use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};
use unicode_normalization::UnicodeNormalization;

pub type Clock = BTreeMap<String, u64>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Kind {
    File,
    Directory,
    Symlink,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: Kind,
    pub size: u64,
    pub hash: String,
    pub target: Option<String>,
    pub mode: u32,
    pub clock: Clock,
    #[serde(default)]
    pub seq: u64,
    #[serde(skip)]
    pub stamp: String,
}

impl Entry {
    pub fn same_bytes(&self, other: &Self) -> bool {
        self.kind == other.kind && self.hash == other.hash && self.target == other.target
    }
    pub fn same_content(&self, other: &Self) -> bool {
        // Linux symlinks report 0777 while macOS may report 0755. We do not
        // transfer symlink permissions; their target defines their content.
        self.same_bytes(other) && (self.kind == Kind::Symlink || self.mode == other.mode)
    }
    pub fn validate(&self) -> Result<()> {
        validate_path(&self.path)?;
        if self.clock.is_empty() || self.clock.len() > 32 || self.clock.values().any(|x| *x == 0) {
            bail!("invalid version vector");
        }
        if self
            .clock
            .keys()
            .any(|s| s.len() != 64 || hex::decode(s).is_err())
        {
            bail!("invalid device in version vector");
        }
        if self.kind == Kind::File && (self.hash.len() != 64 || hex::decode(&self.hash).is_err()) {
            bail!("invalid content hash");
        }
        if self.mode & !0o777 != 0 {
            bail!("unsupported permission bits");
        }
        if self.size > i64::MAX as u64 {
            bail!("file too large");
        }
        if self.kind == Kind::Symlink {
            let t = self
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing link target"))?;
            if t.contains('\0') || t.len() > 4096 {
                bail!("invalid link target");
            }
            if blake3::hash(t.as_bytes()).to_hex().as_str() != self.hash {
                bail!("link hash mismatch");
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Relation {
    Equal,
    Before,
    After,
    Concurrent,
}

pub fn relation(a: &Clock, b: &Clock) -> Relation {
    let mut less = false;
    let mut greater = false;
    for key in a.keys().chain(b.keys()) {
        let x = a.get(key).copied().unwrap_or(0);
        let y = b.get(key).copied().unwrap_or(0);
        less |= x < y;
        greater |= x > y;
    }
    match (less, greater) {
        (false, false) => Relation::Equal,
        (true, false) => Relation::Before,
        (false, true) => Relation::After,
        _ => Relation::Concurrent,
    }
}
pub fn merge(a: &Clock, b: &Clock) -> Clock {
    let mut c = a.clone();
    for (k, v) in b {
        c.entry(k.clone())
            .and_modify(|x| *x = (*x).max(*v))
            .or_insert(*v);
    }
    c
}
pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > 4096 || path.contains('\0') || path.contains('\\') {
        bail!("invalid relative path");
    }
    if path.split('/').any(|s| {
        s.is_empty()
            || s == "."
            || s == ".."
            || s.eq_ignore_ascii_case(".ysync")
            || s.eq_ignore_ascii_case(".ysyncignore")
    }) {
        bail!("reserved or noncanonical path");
    }
    if Path::new(path)
        .components()
        .any(|c| !matches!(c, Component::Normal(_)))
    {
        bail!("unsafe relative path");
    }
    Ok(())
}
pub fn path_key(path: &str) -> String {
    path.nfc().flat_map(char::to_lowercase).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vectors_detect_offline_conflicts() {
        let a = BTreeMap::from([("a".into(), 2), ("b".into(), 1)]);
        let b = BTreeMap::from([("a".into(), 1), ("b".into(), 2)]);
        assert_eq!(relation(&a, &b), Relation::Concurrent);
        assert_eq!(relation(&a, &merge(&a, &b)), Relation::Before);
        assert_eq!(relation(&merge(&a, &b), &b), Relation::After);
    }
    #[test]
    fn unsafe_paths_rejected() {
        for p in [
            "",
            "../x",
            "/etc/passwd",
            "a/../b",
            "a//b",
            ".ysync/key",
            "a/./b",
            "a\\b",
            "a/.YSYNC/x",
        ] {
            assert!(validate_path(p).is_err(), "{p}");
        }
        assert!(validate_path("src/main.rs").is_ok());
        assert_eq!(path_key("É.txt"), path_key("e\u{301}.TXT"));
    }
}
