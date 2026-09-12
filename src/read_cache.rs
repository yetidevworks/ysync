//! Bounded scan-to-send handoff. Entries are hints tied to the complete file fingerprint.
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
pub const MAX_FILE: u64 = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 16384;
struct Item {
    stamp: String,
    hash: String,
    bytes: Arc<[u8]>,
    age: u64,
}
#[derive(Default)]
struct State {
    items: HashMap<String, Item>,
    ages: BTreeMap<u64, String>,
    bytes: usize,
    next: u64,
}
pub struct ReadCache {
    limit: usize,
    state: Mutex<State>,
}
impl ReadCache {
    pub fn new(mib: u16) -> Self {
        Self {
            limit: usize::from(mib) * 1024 * 1024,
            state: Mutex::new(State::default()),
        }
    }
    pub fn allows(&self, size: u64) -> bool {
        size > 0 && size <= MAX_FILE && size <= self.limit as u64
    }
    pub fn insert(&self, folder: &str, path: &str, stamp: &str, hash: &str, bytes: Vec<u8>) {
        if !self.allows(bytes.len() as u64) {
            return;
        }
        let key = format!("{folder}/{path}");
        let mut s = self.state.lock().unwrap();
        if let Some(old) = s.items.remove(&key) {
            s.bytes -= old.bytes.len();
            s.ages.remove(&old.age);
        }
        while s.bytes + bytes.len() > self.limit || s.items.len() >= MAX_ENTRIES {
            let Some((_, key)) = s.ages.pop_first() else {
                break;
            };
            if let Some(old) = s.items.remove(&key) {
                s.bytes -= old.bytes.len();
            }
        }
        s.next += 1;
        let age = s.next;
        s.bytes += bytes.len();
        s.ages.insert(age, key.clone());
        s.items.insert(
            key,
            Item {
                stamp: stamp.into(),
                hash: hash.into(),
                bytes: bytes.into(),
                age,
            },
        );
    }
    pub fn get(&self, folder: &str, path: &str, stamp: &str, hash: &str) -> Option<Arc<[u8]>> {
        let s = self.state.lock().unwrap();
        let item = s.items.get(&format!("{folder}/{path}"))?;
        (item.stamp == stamp && item.hash == hash).then(|| item.bytes.clone())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_and_stale_content_never_hits() {
        let c = ReadCache::new(1);
        c.insert("f", "a", "stamp", "hash", vec![1; 700_000]);
        assert!(c.get("f", "a", "changed", "hash").is_none());
        assert!(c.get("f", "a", "stamp", "wrong").is_none());
        c.insert("f", "b", "new", "hash", vec![2; 700_000]);
        assert!(c.get("f", "a", "stamp", "hash").is_none());
        assert_eq!(c.get("f", "b", "new", "hash").unwrap().len(), 700_000);
        assert!(c.state.lock().unwrap().bytes <= 1024 * 1024);
        assert!(!ReadCache::new(0).allows(1));
        assert!(!c.allows(MAX_FILE + 1));
    }
}
