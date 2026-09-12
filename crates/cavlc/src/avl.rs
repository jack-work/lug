//! Immutable ordered maps with path-copying AVL updates.
use std::{borrow::Borrow, cmp::Ordering, sync::Arc};

type Link<K, V> = Option<Arc<Node<K, V>>>;

#[derive(Debug)]
struct Node<K, V> {
    entry: Arc<(K, V)>,
    left: Link<K, V>,
    right: Link<K, V>,
    height: usize,
    size: usize,
}

#[derive(Debug)]
pub struct AvlMap<K, V> {
    root: Link<K, V>,
}

impl<K, V> Clone for AvlMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl<K, V> Default for AvlMap<K, V> {
    fn default() -> Self {
        Self { root: None }
    }
}

fn height<K, V>(n: &Link<K, V>) -> usize {
    n.as_ref().map_or(0, |n| n.height)
}
fn size<K, V>(n: &Link<K, V>) -> usize {
    n.as_ref().map_or(0, |n| n.size)
}

fn node<K, V>(entry: Arc<(K, V)>, left: Link<K, V>, right: Link<K, V>) -> Arc<Node<K, V>> {
    Arc::new(Node {
        height: 1 + height(&left).max(height(&right)),
        size: 1 + size(&left) + size(&right),
        entry,
        left,
        right,
    })
}

fn rotate_left<K, V>(n: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let r = n.right.as_ref().expect("right-heavy node");
    node(
        r.entry.clone(),
        Some(node(n.entry.clone(), n.left.clone(), r.left.clone())),
        r.right.clone(),
    )
}

fn rotate_right<K, V>(n: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let l = n.left.as_ref().expect("left-heavy node");
    node(
        l.entry.clone(),
        l.left.clone(),
        Some(node(n.entry.clone(), l.right.clone(), n.right.clone())),
    )
}

fn balance<K, V>(mut n: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    if height(&n.left) > height(&n.right) + 1 {
        let l = n.left.as_ref().unwrap();
        if height(&l.right) > height(&l.left) {
            n = node(
                n.entry.clone(),
                Some(rotate_left(l.clone())),
                n.right.clone(),
            );
        }
        rotate_right(n)
    } else if height(&n.right) > height(&n.left) + 1 {
        let r = n.right.as_ref().unwrap();
        if height(&r.left) > height(&r.right) {
            n = node(
                n.entry.clone(),
                n.left.clone(),
                Some(rotate_right(r.clone())),
            );
        }
        rotate_left(n)
    } else {
        n
    }
}

fn insert<K: Ord, V>(root: &Link<K, V>, entry: Arc<(K, V)>) -> Arc<Node<K, V>> {
    let Some(n) = root else {
        return node(entry, None, None);
    };
    match entry.0.cmp(&n.entry.0) {
        Ordering::Less => balance(node(
            n.entry.clone(),
            Some(insert(&n.left, entry)),
            n.right.clone(),
        )),
        Ordering::Greater => balance(node(
            n.entry.clone(),
            n.left.clone(),
            Some(insert(&n.right, entry)),
        )),
        Ordering::Equal => node(entry, n.left.clone(), n.right.clone()),
    }
}

fn remove_min<K, V>(n: &Arc<Node<K, V>>) -> Link<K, V> {
    match &n.left {
        None => n.right.clone(),
        Some(l) => Some(balance(node(
            n.entry.clone(),
            remove_min(l),
            n.right.clone(),
        ))),
    }
}

fn remove<K, V, Q: Ord + ?Sized>(root: &Link<K, V>, key: &Q) -> (Link<K, V>, bool)
where
    K: Borrow<Q>,
{
    let Some(n) = root else { return (None, false) };
    let result = match key.cmp(n.entry.0.borrow()) {
        Ordering::Less => {
            let (left, found) = remove(&n.left, key);
            if !found {
                return (root.clone(), false);
            }
            Some(balance(node(n.entry.clone(), left, n.right.clone())))
        }
        Ordering::Greater => {
            let (right, found) = remove(&n.right, key);
            if !found {
                return (root.clone(), false);
            }
            Some(balance(node(n.entry.clone(), n.left.clone(), right)))
        }
        Ordering::Equal => match (&n.left, &n.right) {
            (None, _) => n.right.clone(),
            (_, None) => n.left.clone(),
            (_, Some(r)) => {
                let mut successor = r;
                while let Some(l) = &successor.left {
                    successor = l;
                }
                Some(balance(node(
                    successor.entry.clone(),
                    n.left.clone(),
                    remove_min(r),
                )))
            }
        },
    };
    (result, true)
}

impl<K, V> AvlMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        size(&self.root)
    }
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }
    pub fn height(&self) -> usize {
        height(&self.root)
    }
    /// Whether the maps share the same root, without comparing entries.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.root, &other.root) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    pub fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        let mut current = self.root.as_deref();
        while let Some(n) = current {
            match key.cmp(n.entry.0.borrow()) {
                Ordering::Less => current = n.left.as_deref(),
                Ordering::Greater => current = n.right.as_deref(),
                Ordering::Equal => return Some(&n.entry.1),
            }
        }
        None
    }
    /// Returns a new root, sharing untouched subtrees and entry payloads.
    pub fn insert(&self, key: K, value: V) -> Self
    where
        K: Ord,
    {
        Self {
            root: Some(insert(&self.root, Arc::new((key, value)))),
        }
    }
    /// An absent key preserves root identity.
    pub fn remove<Q: Ord + ?Sized>(&self, key: &Q) -> Self
    where
        K: Borrow<Q>,
    {
        Self {
            root: remove(&self.root, key).0,
        }
    }
    pub fn iter(&self) -> Iter<'_, K, V> {
        let mut iter = Iter { stack: Vec::new() };
        iter.push_left(self.root.as_deref());
        iter
    }
}

pub struct Iter<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
}
impl<'a, K, V> Iter<'a, K, V> {
    fn push_left(&mut self, mut n: Option<&'a Node<K, V>>) {
        while let Some(current) = n {
            self.stack.push(current);
            n = current.left.as_deref();
        }
    }
}
impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let n = self.stack.pop()?;
        self.push_left(n.right.as_deref());
        Some((&n.entry.0, &n.entry.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn check(n: &Link<i32, i32>, lo: Option<i32>, hi: Option<i32>) -> (usize, usize) {
        let Some(n) = n else { return (0, 0) };
        assert!(lo.is_none_or(|lo| lo < n.entry.0));
        assert!(hi.is_none_or(|hi| n.entry.0 < hi));
        let (lh, ls) = check(&n.left, lo, Some(n.entry.0));
        let (rh, rs) = check(&n.right, Some(n.entry.0), hi);
        assert!(lh.abs_diff(rh) <= 1);
        assert_eq!(n.height, 1 + lh.max(rh));
        assert_eq!(n.size, 1 + ls + rs);
        (n.height, n.size)
    }

    #[test]
    fn rotations_and_deletions_match_btree() {
        let mut map = AvlMap::new();
        let mut reference = BTreeMap::new();
        let mut seed = 17_u64;
        let mut retained = Vec::new();
        for step in 0..12000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = ((seed >> 32) % 400) as i32;
            if step % 3 == 0 {
                map = map.remove(&key);
                reference.remove(&key);
            } else {
                map = map.insert(key, step);
                reference.insert(key, step);
            }
            check(&map.root, None, None);
            assert_eq!(
                map.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                reference.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
            );
            if step % 1000 == 0 {
                retained.push((map.clone(), reference.clone()));
            }
        }
        for (snapshot, expected) in retained {
            assert_eq!(
                snapshot
                    .iter()
                    .map(|(k, v)| (*k, *v))
                    .collect::<BTreeMap<_, _>>(),
                expected
            );
        }
        for key in reference.keys() {
            map = map.remove(key);
            check(&map.root, None, None);
        }
        assert!(map.is_empty());
    }

    #[test]
    fn ascending_descending_and_double_rotations() {
        for keys in [
            vec![3, 2, 1],
            vec![1, 2, 3],
            vec![3, 1, 2],
            vec![1, 3, 2],
            (0..1000).collect(),
            (0..1000).rev().collect(),
        ] {
            let mut map = AvlMap::new();
            for key in keys {
                map = map.insert(key, key);
                check(&map.root, None, None);
            }
        }
    }

    #[test]
    fn untouched_subtrees_and_payloads_are_shared() {
        let map = AvlMap::new().insert(2, 2).insert(1, 1).insert(3, 3);
        let next = map.insert(1, 10);
        let a = map.root.as_ref().unwrap();
        let b = next.root.as_ref().unwrap();
        assert!(Arc::ptr_eq(
            a.right.as_ref().unwrap(),
            b.right.as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(&a.entry, &b.entry));
        assert!(map.ptr_eq(&map.remove(&9)));
        assert_eq!(map.get(&1), Some(&1));
    }
}
