use crate::model::{Entry, path_key};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use std::{path::Path, time::Duration};

pub fn open(home: &Path) -> Result<Connection> {
    let c = Connection::open(home.join("index.sqlite"))?;
    c.busy_timeout(Duration::from_secs(30))?;
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-32768;
      CREATE TABLE IF NOT EXISTS entries(folder TEXT NOT NULL,path TEXT NOT NULL,path_key TEXT NOT NULL,seq INTEGER NOT NULL,data TEXT NOT NULL,stamp TEXT NOT NULL,seen INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(folder,path),UNIQUE(folder,path_key));
      CREATE INDEX IF NOT EXISTS changes ON entries(folder,seq);
      CREATE TABLE IF NOT EXISTS counters(folder TEXT PRIMARY KEY,value INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS cursors(peer TEXT NOT NULL,folder TEXT NOT NULL,value INTEGER NOT NULL,PRIMARY KEY(peer,folder));
      CREATE TABLE IF NOT EXISTS conflicts(folder TEXT NOT NULL,id TEXT NOT NULL,path TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(folder,id));
      CREATE INDEX IF NOT EXISTS conflicts_path ON conflicts(folder,path);")?;
    Ok(c)
}
pub fn get(c: &Connection, folder: &str, path: &str) -> Result<Option<Entry>> {
    let r: Option<(String, String)> = c
        .prepare_cached("SELECT data,stamp FROM entries WHERE folder=?1 AND path=?2")?
        .query_row(params![folder, path], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    r.map(|(s, stamp)| {
        let mut e: Entry = serde_json::from_str(&s)?;
        e.stamp = stamp;
        Ok(e)
    })
    .transpose()
}
pub fn put(c: &Connection, folder: &str, e: &mut Entry, seen: i64) -> Result<()> {
    let key = path_key(&e.path);
    let alias: Option<String> = c
        .prepare_cached("SELECT path FROM entries WHERE folder=?1 AND path_key=?2")?
        .query_row(params![folder, key], |r| r.get(0))
        .optional()?;
    if alias.as_ref().is_some_and(|p| p != &e.path) {
        bail!("case/Unicode collision: {} and {}", alias.unwrap(), e.path);
    }
    c.prepare_cached(
        "INSERT INTO counters VALUES(?1,1) ON CONFLICT(folder) DO UPDATE SET value=value+1",
    )?
    .execute([folder])?;
    e.seq = c
        .prepare_cached("SELECT value FROM counters WHERE folder=?1")?
        .query_row([folder], |r| r.get::<_, i64>(0))? as u64;
    c.prepare_cached("INSERT INTO entries(folder,path,path_key,seq,data,stamp,seen) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(folder,path) DO UPDATE SET seq=excluded.seq,data=excluded.data,stamp=excluded.stamp,seen=excluded.seen")?.execute(params![folder,e.path,key,i64::try_from(e.seq)?,serde_json::to_string(e)?,e.stamp,seen])?;
    Ok(())
}
pub fn mark_seen(c: &Connection, folder: &str, path: &str, stamp: &str, seen: i64) -> Result<()> {
    c.prepare_cached("UPDATE entries SET stamp=?1,seen=?2 WHERE folder=?3 AND path=?4")?
        .execute(params![stamp, seen, folder, path])?;
    Ok(())
}
pub fn changes(c: &Connection, folder: &str, after: u64, limit: usize) -> Result<Vec<Entry>> {
    let mut q = c.prepare_cached(
        "SELECT data FROM entries WHERE folder=?1 AND seq>?2 ORDER BY seq LIMIT ?3",
    )?;
    let rows = q.query_map(
        params![folder, i64::try_from(after)?, i64::try_from(limit)?],
        |r| r.get::<_, String>(0),
    )?;
    rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
}
pub fn cursor(c: &Connection, peer: &str, folder: &str) -> Result<u64> {
    Ok(c.query_row(
        "SELECT value FROM cursors WHERE peer=?1 AND folder=?2",
        params![peer, folder],
        |r| r.get::<_, i64>(0),
    )
    .optional()?
    .unwrap_or(0) as u64)
}
pub fn set_cursor(c: &Connection, peer: &str, folder: &str, value: u64) -> Result<()> {
    c.execute("INSERT INTO cursors VALUES(?1,?2,?3) ON CONFLICT(peer,folder) DO UPDATE SET value=MAX(value,excluded.value)",params![peer,folder,i64::try_from(value)?])?;
    Ok(())
}
pub fn unseen(c: &Connection, folder: &str, seen: i64, limit: usize) -> Result<Vec<Entry>> {
    let mut q=c.prepare_cached("SELECT data FROM entries WHERE folder=?1 AND seen!=?2 AND json_extract(data,'$.kind')!='Deleted' ORDER BY length(path) DESC LIMIT ?3")?;
    let rows = q.query_map(params![folder, seen, i64::try_from(limit)?], |r| {
        r.get::<_, String>(0)
    })?;
    rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
}
pub fn total(c: &Connection, folder: &str) -> Result<(u64, u64)> {
    c.query_row("SELECT count(*),COALESCE(sum(json_extract(data,'$.size')),0) FROM entries WHERE folder=?1 AND json_extract(data,'$.kind')='File'",[folder],|r|Ok((r.get::<_,i64>(0)? as u64,r.get::<_,i64>(1)? as u64))).context("counting index")
}

/// Restrict reconciliation to one exact path and its descendants using primary-key ranges.
pub fn unseen_under(
    c: &Connection,
    folder: &str,
    path: &str,
    seen: i64,
    limit: usize,
) -> Result<Vec<Entry>> {
    let mut q = c.prepare_cached("SELECT data FROM (
        SELECT path,data,seen FROM entries WHERE folder=?1 AND path=?2
        UNION ALL
        SELECT path,data,seen FROM entries WHERE folder=?1 AND path>=?3 AND path<?4
      ) WHERE seen!=?5 AND json_extract(data,'$.kind')!='Deleted' ORDER BY length(path) DESC LIMIT ?6")?;
    let rows = q.query_map(
        params![
            folder,
            path,
            format!("{path}/"),
            format!("{path}0"),
            seen,
            i64::try_from(limit)?
        ],
        |r| r.get::<_, String>(0),
    )?;
    rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
}
