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
      CREATE INDEX IF NOT EXISTS conflicts_path ON conflicts(folder,path);
      CREATE TABLE IF NOT EXISTS lane_cursors(peer TEXT NOT NULL,folder TEXT NOT NULL,lanes INTEGER NOT NULL,lane INTEGER NOT NULL,value INTEGER NOT NULL,PRIMARY KEY(peer,folder,lanes,lane));
      CREATE INDEX IF NOT EXISTS small_changes ON entries(folder,seq) WHERE json_extract(data,'$.kind')!='File' OR json_extract(data,'$.size')<1048576;
      CREATE INDEX IF NOT EXISTS bulk_changes ON entries(folder,seq) WHERE json_extract(data,'$.kind')='File' AND json_extract(data,'$.size')>=1048576;")?;
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

/// Missing lane/layout cursors start at the last globally durable watermark.
pub fn lane_cursor(
    c: &Connection,
    peer: &str,
    folder: &str,
    lane: crate::lanes::Lane,
) -> Result<u64> {
    lane.validate()?;
    if lane.count == 1 {
        return cursor(c, peer, folder);
    }
    let value: Option<i64> = c
        .prepare_cached(
            "SELECT value FROM lane_cursors WHERE peer=?1 AND folder=?2 AND lanes=?3 AND lane=?4",
        )?
        .query_row(params![peer, folder, lane.count, lane.index], |r| r.get(0))
        .optional()?;
    value
        .map(|v| Ok(v as u64))
        .unwrap_or_else(|| cursor(c, peer, folder))
}
pub fn set_lane_cursor(
    c: &Connection,
    peer: &str,
    folder: &str,
    lane: crate::lanes::Lane,
    value: u64,
) -> Result<()> {
    lane.validate()?;
    if lane.count == 1 {
        return set_cursor(c, peer, folder, value);
    }
    c.prepare_cached("INSERT INTO lane_cursors VALUES(?1,?2,?3,?4,?5) ON CONFLICT(peer,folder,lanes,lane) DO UPDATE SET value=MAX(value,excluded.value)")?.execute(params![peer,folder,lane.count,lane.index,i64::try_from(value)?])?;
    let mut common = value;
    for index in 0..lane.count {
        common = common.min(lane_cursor(
            c,
            peer,
            folder,
            crate::lanes::Lane { index, ..lane },
        )?);
    }
    set_cursor(c, peer, folder, common)
}
pub fn lane_changes(
    c: &Connection,
    folder: &str,
    after: u64,
    lane: crate::lanes::Lane,
) -> Result<(Vec<Entry>, u64)> {
    lane.validate()?;
    if lane.count == 1 {
        let entries = changes(c, folder, after, 128)?;
        let upto = entries.last().map_or(after, |e| e.seq);
        return Ok((entries, upto));
    }
    let high = c.query_row(
        "SELECT COALESCE((SELECT value FROM counters WHERE folder=?1),0)",
        [folder],
        |r| r.get::<_, i64>(0),
    )? as u64;
    let sql = if lane.index == 0 {
        "SELECT data FROM entries WHERE folder=?1 AND seq>?2 AND seq<=?3 AND (json_extract(data,'$.kind')!='File' OR json_extract(data,'$.size')<1048576) ORDER BY seq LIMIT 128"
    } else {
        "SELECT data FROM entries WHERE folder=?1 AND seq>?2 AND seq<=?3 AND json_extract(data,'$.kind')='File' AND json_extract(data,'$.size')>=1048576 ORDER BY seq LIMIT 128"
    };
    let mut q = c.prepare_cached(sql)?;
    let rows = q.query_map(
        params![folder, i64::try_from(after)?, i64::try_from(high)?],
        |r| r.get::<_, String>(0),
    )?;
    let mut entries = vec![];
    let mut seen = 0;
    let mut upto = after;
    for row in rows {
        let e: Entry = serde_json::from_str(&row?)?;
        seen += 1;
        upto = e.seq;
        if lane.includes(&e) {
            entries.push(e);
            if lane.index > 0 {
                break;
            }
        }
    }
    // A bulk lane stops after one matching payload; never skip the unconsumed rows.
    if seen < 128 && (lane.index == 0 || entries.is_empty()) {
        upto = high.max(after);
    }
    Ok((entries, upto))
}

#[cfg(test)]
mod lane_tests {
    use super::*;
    use crate::{lanes::Lane, model::Kind};
    #[test]
    fn every_partition_drains_without_skipping_and_size_changes_move_lanes() {
        let h = tempfile::tempdir().unwrap();
        let c = open(h.path()).unwrap();
        for i in 0..400 {
            let mut e = Entry {
                path: format!("file-{i}"),
                kind: Kind::File,
                size: if i % 5 == 0 { 12 } else { 2 * 1024 * 1024 },
                hash: "ab".repeat(32),
                target: None,
                mode: 0o644,
                clock: Default::default(),
                seq: 0,
                stamp: String::new(),
            };
            put(&c, "f", &mut e, 1).unwrap();
        }
        let mut paths = std::collections::BTreeSet::new();
        for index in 0..3 {
            let lane = Lane { index, count: 3 };
            let mut cursor = 0;
            while cursor < 400 {
                let (entries, upto) = lane_changes(&c, "f", cursor, lane).unwrap();
                assert!(upto > cursor);
                for e in entries {
                    assert!(lane.includes(&e));
                    assert!(paths.insert(e.path));
                }
                cursor = upto;
            }
        }
        assert_eq!(paths.len(), 400);
        let mut e = get(&c, "f", "file-1").unwrap().unwrap();
        e.size = 10;
        put(&c, "f", &mut e, 2).unwrap();
        let (entries, upto) = lane_changes(&c, "f", 400, Lane { index: 0, count: 3 }).unwrap();
        assert_eq!(entries[0].path, "file-1");
        assert_eq!(upto, 401);
        e.kind = Kind::Deleted;
        put(&c, "f", &mut e, 3).unwrap();
        assert_eq!(
            lane_changes(&c, "f", 401, Lane { index: 0, count: 3 })
                .unwrap()
                .0[0]
                .kind,
            Kind::Deleted
        );
    }
    #[test]
    fn layout_changes_never_skip_uncommitted_lanes() {
        let h = tempfile::tempdir().unwrap();
        let c = open(h.path()).unwrap();
        set_cursor(&c, "peer", "f", 10).unwrap();
        let lane = |index, count| Lane { index, count };
        set_lane_cursor(&c, "peer", "f", lane(0, 3), 100).unwrap();
        set_lane_cursor(&c, "peer", "f", lane(1, 3), 80).unwrap();
        assert_eq!(cursor(&c, "peer", "f").unwrap(), 10);
        assert_eq!(lane_cursor(&c, "peer", "f", lane(0, 2)).unwrap(), 10);
        set_lane_cursor(&c, "peer", "f", lane(2, 3), 70).unwrap();
        assert_eq!(cursor(&c, "peer", "f").unwrap(), 70);
        assert_eq!(lane_cursor(&c, "peer", "f", Lane::default()).unwrap(), 70);
        set_lane_cursor(&c, "peer", "f", lane(2, 3), 20).unwrap();
        assert_eq!(lane_cursor(&c, "peer", "f", lane(2, 3)).unwrap(), 70);
        set_cursor(&c, "peer", "f", 200).unwrap();
        // Returning to an old partition may replay metadata, but never advances beyond committed history.
        assert!(lane_cursor(&c, "peer", "f", lane(0, 3)).unwrap() <= 200);
        assert_eq!(lane_cursor(&c, "peer", "f", lane(0, 4)).unwrap(), 200);
    }
}
