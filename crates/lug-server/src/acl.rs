//! Who may talk to the daemon, and about which log.

use std::collections::{HashMap, HashSet};

/// Default deny for anyone but the owner: an extra uid is let in only if the
/// config named it, and reaches a log only if the log's own list names it.
#[derive(Debug, Clone)]
pub struct Acl {
    pub owner: u32,
    pub allow: HashSet<u32>,
    pub logs: HashMap<String, HashSet<u32>>,
}

impl Acl {
    pub fn new(owner: u32, allow: impl IntoIterator<Item = u32>) -> Self {
        Self { owner, allow: allow.into_iter().collect(), logs: HashMap::new() }
    }

    /// Applied to the peer credentials of an accepted connection.
    pub fn accepts(&self, uid: u32) -> bool {
        uid == self.owner || self.allow.contains(&uid)
    }

    pub fn allows(&self, uid: u32, log: &str) -> bool {
        if uid == self.owner {
            return true;
        }
        self.allow.contains(&uid) && self.logs.get(log).is_some_and(|uids| uids.contains(&uid))
    }
}

/// The uid the daemon runs as.
pub fn own_uid() -> u32 {
    // SAFETY: geteuid is always safe; it reads the calling process's own
    // effective uid and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stranger_needs_both_gates() {
        let mut acl = Acl::new(1000, [1001]);
        assert!(acl.accepts(1001));
        assert!(!acl.accepts(1002));
        assert!(!acl.allows(1001, "orders"));
        acl.logs.insert("orders".into(), HashSet::from([1001]));
        assert!(acl.allows(1001, "orders"));
        assert!(acl.allows(1000, "anything"));
    }
}
