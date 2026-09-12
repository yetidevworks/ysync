//! Disposable persistent signatures, bounded by serialized bytes and row count.
use crate::delta::{self, Block, Parameters};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
const MAX_ROWS: i64 = 1024;
const MAX_RECORD: usize = 2 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
struct Record {
    key: String,
    hash: String,
    blocks: Vec<Block>,
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn open(home: &Path) -> Result<Connection> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = home.join("chunk-cache.sqlite");
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)?;
    let c = Connection::open(path)?;
    c.busy_timeout(Duration::from_millis(20))?;
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-2048;
        CREATE TABLE IF NOT EXISTS signatures(key TEXT PRIMARY KEY,data TEXT NOT NULL,checksum TEXT NOT NULL,bytes INTEGER NOT NULL,used INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS signatures_age ON signatures(used);
        CREATE TABLE IF NOT EXISTS feedback(key TEXT PRIMARY KEY,remaining INTEGER NOT NULL,used INTEGER NOT NULL);")?;
    Ok(c)
}
fn key(folder: &str, path: &str, stamp: &str, size: u64, params: Parameters) -> String {
    blake3::hash(&serde_json::to_vec(&(1, folder, path, stamp, size, params.min)).unwrap())
        .to_hex()
        .to_string()
}
pub fn get(
    home: &Path,
    folder: &str,
    path: &str,
    stamp: &str,
    size: u64,
    params: Parameters,
    capacity: u16,
) -> Option<(Vec<Block>, String)> {
    if capacity == 0 {
        return None;
    }
    (|| -> Result<_> {
        let c = open(home)?;
        let key = key(folder, path, stamp, size, params);
        let (data, checksum): (String, String) = c.query_row(
            "SELECT data,checksum FROM signatures WHERE key=?1 AND bytes<=?2 AND length(data)<=?2",
            params![
                key,
                (MAX_RECORD as i64).min(i64::from(capacity) * 1024 * 1024)
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            blake3::hash(data.as_bytes()).to_hex().as_str() == checksum,
            "signature cache checksum mismatch"
        );
        let record: Record = serde_json::from_str(&data)?;
        ensure!(
            record.key == key && record.hash.len() == 64 && hex::decode(&record.hash).is_ok(),
            "invalid cache key/hash"
        );
        delta::validate_basis(&record.blocks, size, params)?;
        let _ = c.execute(
            "UPDATE signatures SET used=?2 WHERE key=?1 AND used<?2-60",
            params![key, now()],
        );
        Ok((record.blocks, record.hash))
    })()
    .ok()
}
#[allow(clippy::too_many_arguments)]
pub fn put(
    home: &Path,
    folder: &str,
    path: &str,
    stamp: &str,
    size: u64,
    params: Parameters,
    blocks: &[Block],
    hash: &str,
    capacity: u16,
) {
    if capacity == 0 {
        return;
    }
    let _ = (|| -> Result<()> {
        delta::validate_basis(blocks, size, params)?;
        let key = key(folder, path, stamp, size, params);
        let data = serde_json::to_string(&Record {
            key: key.clone(),
            hash: hash.into(),
            blocks: blocks.to_vec(),
        })?;
        let limit = i64::from(capacity) * 1024 * 1024;
        ensure!(
            data.len() <= MAX_RECORD && data.len() as i64 <= limit,
            "signature cache record too large"
        );
        let mut c = open(home)?;
        let tx = c.transaction()?;
        tx.execute("INSERT INTO signatures VALUES(?1,?2,?3,?4,?5) ON CONFLICT(key) DO UPDATE SET data=excluded.data,checksum=excluded.checksum,bytes=excluded.bytes,used=excluded.used",params![key,data,blake3::hash(data.as_bytes()).to_hex().to_string(),data.len() as i64,now()])?;
        let (mut bytes, mut rows): (i64, i64) = tx.query_row(
            "SELECT COALESCE(sum(bytes),0),count(*) FROM signatures",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        while bytes > limit || rows > MAX_ROWS {
            let (k, n): (String, i64) = tx.query_row(
                "SELECT key,bytes FROM signatures ORDER BY used,rowid LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            tx.execute("DELETE FROM signatures WHERE key=?1", [k])?;
            bytes -= n;
            rows -= 1;
        }
        tx.commit()?;
        Ok(())
    })();
}
fn feedback_key(peer: &str, folder: &str, path: &str) -> String {
    blake3::hash(&serde_json::to_vec(&(peer, folder, path)).unwrap())
        .to_hex()
        .to_string()
}
/// After an ineffective attempt, stream three subsequent versions before probing again.
pub fn skip_delta(home: &Path, peer: &str, folder: &str, path: &str, capacity: u16) -> bool {
    if capacity == 0 {
        return false;
    }
    (|| -> Result<bool> {
        let mut c = open(home)?;
        let tx = c.transaction()?;
        let key = feedback_key(peer, folder, path);
        let remaining: Option<i64> = tx
            .query_row("SELECT remaining FROM feedback WHERE key=?1", [&key], |r| {
                r.get(0)
            })
            .optional()?;
        if remaining.is_some_and(|n| n > 0) {
            tx.execute(
                "UPDATE feedback SET remaining=remaining-1,used=?2 WHERE key=?1",
                params![key, now()],
            )?;
            tx.commit()?;
            return Ok(true);
        }
        Ok(false)
    })()
    .unwrap_or(false)
}
pub fn feedback(home: &Path, peer: &str, folder: &str, path: &str, useful: bool, capacity: u16) {
    if capacity == 0 {
        return;
    }
    let _ = (|| -> Result<()> {
        let c = open(home)?;
        c.execute("INSERT INTO feedback VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET remaining=excluded.remaining,used=excluded.used",params![feedback_key(peer,folder,path),if useful{0}else{3},now()])?;
        c.execute("DELETE FROM feedback WHERE key IN (SELECT key FROM feedback ORDER BY used DESC,rowid DESC LIMIT -1 OFFSET ?1)",[MAX_ROWS])?;
        Ok(())
    })();
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn serialized_budget_evicts_old_signatures_and_cache_errors_are_misses() {
        let h = tempfile::tempdir().unwrap();
        let size = delta::MAX_BLOCKS as u64;
        let params = Parameters::for_sizes(size, size);
        let blocks: Vec<_> = (0..size)
            .map(|offset| Block {
                offset,
                len: 1,
                hash: "ab".repeat(32),
            })
            .collect();
        for path in ["old", "new"] {
            put(
                h.path(),
                "f",
                path,
                "stamp",
                size,
                params,
                &blocks,
                &"ab".repeat(32),
                1,
            );
        }
        let c = open(h.path()).unwrap();
        let used: i64 = c
            .query_row("SELECT sum(bytes) FROM signatures", [], |r| r.get(0))
            .unwrap();
        assert!(used <= 1024 * 1024);
        assert!(get(h.path(), "f", "old", "stamp", size, params, 1).is_none());
        assert!(get(h.path(), "f", "new", "stamp", size, params, 1).is_some());
        let broken = tempfile::tempdir().unwrap();
        std::fs::write(
            broken.path().join("chunk-cache.sqlite"),
            b"broken sqlite file",
        )
        .unwrap();
        assert!(get(broken.path(), "f", "new", "stamp", size, params, 1).is_none());
        put(
            broken.path(),
            "f",
            "new",
            "stamp",
            size,
            params,
            &blocks,
            &"ab".repeat(32),
            1,
        );
    }
    #[test]
    fn persistent_cache_rejects_corruption_changed_stamps_and_parameters() {
        let h = tempfile::tempdir().unwrap();
        let bytes = vec![7; 2 * 1024 * 1024];
        let params = Parameters::for_sizes(bytes.len() as u64, bytes.len() as u64);
        let mut hash = blake3::Hasher::new();
        let blocks = delta::signature(
            &mut Cursor::new(&bytes),
            0,
            bytes.len() as u64,
            params,
            &mut hash,
            || Ok(()),
        )
        .unwrap();
        put(
            h.path(),
            "f",
            "p",
            "stamp",
            bytes.len() as u64,
            params,
            &blocks,
            hash.finalize().to_hex().as_ref(),
            1,
        );
        assert!(get(h.path(), "f", "p", "stamp", bytes.len() as u64, params, 1).is_some());
        assert!(get(h.path(), "f", "p", "other", bytes.len() as u64, params, 1).is_none());
        let different = Parameters::for_sizes(2 * 1024 * 1024 * 1024, bytes.len() as u64);
        assert!(
            get(
                h.path(),
                "f",
                "p",
                "stamp",
                bytes.len() as u64,
                different,
                1
            )
            .is_none()
        );
        assert!(get(h.path(), "f", "p", "stamp", bytes.len() as u64, params, 0).is_none());
        let cached = get(h.path(), "f", "p", "stamp", bytes.len() as u64, params, 1).unwrap();
        assert_eq!(
            serde_json::to_string(&cached.0).unwrap(),
            serde_json::to_string(&blocks).unwrap()
        );
        let c = open(h.path()).unwrap();
        c.execute(
            "UPDATE signatures SET data=replace(data,'777','888'),checksum='wrong'",
            [],
        )
        .unwrap();
        assert!(get(h.path(), "f", "p", "stamp", bytes.len() as u64, params, 1).is_none());
        feedback(h.path(), "peer", "f", "p", false, 1);
        for _ in 0..3 {
            assert!(skip_delta(h.path(), "peer", "f", "p", 1));
        }
        assert!(!skip_delta(h.path(), "peer", "f", "p", 1));
        assert!(!skip_delta(h.path(), "other-peer", "f", "p", 1));
    }
}
