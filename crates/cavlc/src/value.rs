use crate::avl::AvlMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeMap};
use std::sync::Arc;

/// Immutable JSON. Each object is an AVL map; arrays are atomic leaf values.
/// Cloning any value is O(1). Object edits share every untouched child.
#[derive(Clone, Debug)]
pub struct Value(Arc<Kind>);

#[derive(Debug)]
enum Kind {
    Object(AvlMap<String, Value>),
    Atom(serde_json::Value),
}

impl Default for Value {
    fn default() -> Self {
        Self::object(AvlMap::new())
    }
}

impl Value {
    pub fn object(map: AvlMap<String, Value>) -> Self {
        Self(Arc::new(Kind::Object(map)))
    }
    pub fn as_object(&self) -> Option<&AvlMap<String, Value>> {
        match self.0.as_ref() {
            Kind::Object(map) => Some(map),
            _ => None,
        }
    }
    pub fn as_atom(&self) -> Option<&serde_json::Value> {
        match self.0.as_ref() {
            Kind::Atom(value) => Some(value),
            _ => None,
        }
    }
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_object()?.get(key)
    }
    /// Literal path segments, without dotted-name or JSON Pointer ambiguity.
    pub fn at<'a>(&self, path: impl IntoIterator<Item = &'a str>) -> Option<&Value> {
        let mut value = self;
        for key in path {
            value = value.get(key)?;
        }
        Some(value)
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn to_json(&self) -> serde_json::Value {
        match self.0.as_ref() {
            Kind::Object(map) => serde_json::Value::Object(
                map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect(),
            ),
            Kind::Atom(value) => value.clone(),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        match (self.0.as_ref(), other.0.as_ref()) {
            (Kind::Atom(a), Kind::Atom(b)) => a == b,
            (Kind::Object(a), Kind::Object(b)) => {
                a.ptr_eq(b) || (a.len() == b.len() && a.iter().eq(b.iter()))
            }
            _ => false,
        }
    }
}
impl Eq for Value {}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Object(map) => {
                let mut out = AvlMap::new();
                for (key, value) in map {
                    out = out.insert(key, Self::from(value));
                }
                Self::object(out)
            }
            atom => Self(Arc::new(Kind::Atom(atom))),
        }
    }
}
impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.as_ref() {
            Kind::Atom(value) => value.serialize(serializer),
            Kind::Object(map) => {
                let mut out = serializer.serialize_map(Some(map.len()))?;
                for (key, value) in map.iter() {
                    out.serialize_entry(key, value)?;
                }
                out.end()
            }
        }
    }
}
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde_json::Value::deserialize(deserializer).map(Self::from)
    }
}
