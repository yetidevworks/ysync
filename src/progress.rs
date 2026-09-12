//! Disposable outbound delivery observations. Never used to advance sync cursors.
use crate::{config, daemon, engine::Root, lanes::Lane, store};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Pending {
    pub entries: u64,
    pub files: u64,
    /// Logical file sizes, not predicted wire traffic (delta/echoes can save it).
    pub bytes: u64,
    /// False means the bounded sampler stopped early; totals are lower bounds.
    pub complete: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Delivery {
    #[serde(default)]
    pub send_disabled: Option<String>,
    pub active_lanes: usize,
    pub sampled_at: u64,
    pub local_head: u64,
    pub acknowledged: Vec<Option<u64>>,
    pub pending: Option<Pending>,
    pub error: Option<String>,
}

pub type Deliveries = BTreeMap<String, BTreeMap<String, Delivery>>;
type Observations = BTreeMap<String, BTreeMap<String, Vec<Option<u64>>>>;

#[derive(Default)]
pub struct Tracker {
    remote: Mutex<BTreeMap<String, BTreeMap<String, config::FolderMode>>>,
    online: Mutex<BTreeMap<String, std::collections::BTreeSet<u8>>>,
    observed: Mutex<Observations>,
    samples: Mutex<Deliveries>,
}
impl Tracker {
    pub fn remote_modes(&self, peer: &str, modes: BTreeMap<String, config::FolderMode>) {
        self.remote.lock().unwrap().insert(peer.into(), modes);
    }
    pub fn connected(&self, peer: &str, lane: u8, connected: bool) {
        let mut online = self.online.lock().unwrap();
        let lanes = online.entry(peer.into()).or_default();
        if connected {
            lanes.insert(lane);
        } else {
            lanes.remove(&lane);
        }
    }
    pub fn acknowledged(&self, peer: &str, folder: &str, lane: Lane, cursor: u64) {
        let mut observed = self.observed.lock().unwrap();
        let cursors = observed
            .entry(peer.into())
            .or_default()
            .entry(folder.into())
            .or_default();
        if cursors.len() != usize::from(lane.count) {
            *cursors = vec![None; usize::from(lane.count)];
        }
        // Accept rollback on a new handshake; these are observations, not a
        // second durable cursor store. Restart/restore must never look caught up.
        cursors[usize::from(lane.index)] = Some(cursor);
    }
    pub fn forget_lane(&self, peer: &str, lane: Lane) {
        if let Some(folders) = self.observed.lock().unwrap().get_mut(peer) {
            for cursors in folders.values_mut() {
                if cursors.len() == usize::from(lane.count) {
                    cursors[usize::from(lane.index)] = None;
                }
            }
        }
    }
    pub fn snapshot(&self) -> Deliveries {
        let observed = self.observed.lock().unwrap();
        let mut samples = self.samples.lock().unwrap().clone();
        for (peer, folders) in &mut samples {
            for (folder, sample) in folders {
                sample.active_lanes = self
                    .online
                    .lock()
                    .unwrap()
                    .get(peer)
                    .map_or(0, |lanes| lanes.len());
                let current = observed.get(peer).and_then(|f| f.get(folder));
                if !current.is_some_and(|c| {
                    c.len() == sample.acknowledged.len()
                        && c.iter().zip(&sample.acknowledged).all(
                            |(now, prior)| matches!((now, prior), (Some(n), Some(p)) if n >= p),
                        )
                }) {
                    // A stale zero is particularly misleading during reconnect.
                    sample.pending = None;
                }
            }
        }
        samples
    }
}

fn head(c: &rusqlite::Connection, folder: &str) -> Result<u64> {
    Ok(c.query_row(
        "SELECT COALESCE((SELECT value FROM counters WHERE folder=?1),0)",
        [folder],
        |r| r.get::<_, i64>(0),
    )? as u64)
}

fn count(
    c: &rusqlite::Connection,
    root: &Root,
    cursors: &[Option<u64>],
    budget: Duration,
) -> Result<Pending> {
    let deadline = Instant::now() + budget;
    let after = cursors.iter().copied().collect::<Option<Vec<_>>>().unwrap();
    let mut totals = Pending {
        complete: true,
        ..Default::default()
    };
    let mut q = c.prepare_cached("SELECT seq,path,json_extract(data,'$.kind'),json_extract(data,'$.size') FROM entries WHERE folder=?1 AND seq>?2 ORDER BY seq")?;
    let mut rows = q.query(rusqlite::params![
        root.folder.id,
        i64::try_from(after.iter().min().copied().unwrap_or(0))?
    ])?;
    let mut visited = 0;
    while let Some(row) = rows.next()? {
        if visited % 256 == 0 && Instant::now() >= deadline {
            totals.complete = false;
            break;
        }
        visited += 1;
        let seq = u64::try_from(row.get::<_, i64>(0)?)?;
        let path: String = row.get(1)?;
        let kind: String = row.get(2)?;
        let size = u64::try_from(row.get::<_, i64>(3)?)?;
        let lane = Lane::for_file(&path, kind == "File", size, after.len() as u8);
        if seq <= after[usize::from(lane)] || root.excluded(&path) {
            continue;
        }
        totals.entries += 1;
        if kind == "File" {
            totals.files += 1;
            totals.bytes = totals.bytes.saturating_add(size);
        }
    }
    Ok(totals)
}

pub fn run(shared: Arc<daemon::Shared>) {
    // One worker; no folder walk, hashing, or database writes on monitor refresh.
    let Ok(mut c) = store::open(&shared.home) else {
        return;
    };
    let _ = c.busy_timeout(Duration::from_millis(100));
    let mut last_counts: BTreeMap<(String, String), Instant> = BTreeMap::new();
    let mut filters = BTreeMap::new();
    while !shared.stopping() {
        if let Ok(cfg) = config::load(&shared.home) {
            let remote = shared.delivery.remote.lock().unwrap().clone();
            let observed = shared.delivery.observed.lock().unwrap().clone();
            let old = shared.delivery.samples.lock().unwrap().clone();
            let mut samples = Deliveries::new();
            for peer in cfg.peers.iter().filter(|p| p.approved) {
                for folder in cfg.folders.iter().filter(|f| peer.folders.contains(&f.id)) {
                    if shared.stopping() {
                        return;
                    }
                    let cursors = observed
                        .get(&peer.id)
                        .and_then(|f| f.get(&folder.id))
                        .cloned()
                        .unwrap_or_default();
                    let key = (peer.id.clone(), folder.id.clone());
                    let previous = old.get(&peer.id).and_then(|f| f.get(&folder.id));
                    let filter = (
                        folder.clone(),
                        std::fs::metadata(folder.path.join(".ysyncignore"))
                            .ok()
                            .and_then(|m| m.modified().ok().map(|t| (t, m.len()))),
                    );
                    let mut sample = Delivery {
                        sampled_at: daemon::now(),
                        acknowledged: cursors.clone(),
                        send_disabled: if !folder.mode.can_send() {
                            Some("local folder is receive-only".into())
                        } else if remote
                            .get(&peer.id)
                            .and_then(|m| m.get(&folder.id))
                            .is_some_and(|m| !m.can_receive())
                        {
                            Some("peer folder is send-only (last handshake)".into())
                        } else {
                            None
                        },
                        ..Default::default()
                    };
                    let result = (|| -> Result<()> {
                        // A disabled direction is not an empty/delivered queue.
                        if sample.send_disabled.is_some() {
                            return Ok(());
                        }
                        let tx = c.transaction()?;
                        sample.local_head = head(&tx, &folder.id)?;
                        if cursors.is_empty() || cursors.iter().any(Option::is_none) {
                            return Ok(());
                        }
                        if cursors.iter().all(|n| n.unwrap() >= sample.local_head) {
                            sample.pending = Some(Pending {
                                complete: true,
                                ..Default::default()
                            });
                        } else if filters.get(&key) == Some(&filter)
                            && previous.is_some_and(|p| {
                                p.local_head == sample.local_head
                                    && p.acknowledged == cursors
                                    && p.pending.as_ref().is_some_and(|n| n.complete)
                            })
                        {
                            sample.pending = previous.and_then(|p| p.pending.clone());
                        } else if last_counts
                            .get(&key)
                            .is_some_and(|t| t.elapsed() < Duration::from_secs(5))
                        {
                            // Retain the sample's age and watermarks; never relabel old counts as current.
                            if let Some(previous) = previous
                                && previous
                                    .pending
                                    .as_ref()
                                    .is_some_and(|p| p.entries > 0 || !p.complete)
                            {
                                sample = previous.clone();
                            }
                        } else {
                            last_counts.insert(key.clone(), Instant::now());
                            filters.insert(key, filter);
                            let root = Root::open(folder.clone(), shared.gate(&folder.id))?;
                            sample.pending =
                                Some(count(&tx, &root, &cursors, Duration::from_millis(500))?);
                        }
                        Ok(())
                    })();
                    if let Err(e) = result {
                        sample.error = Some(format!("{e:#}"));
                        sample.pending = None;
                    }
                    samples
                        .entry(peer.id.clone())
                        .or_default()
                        .insert(folder.id.clone(), sample);
                }
            }
            *shared.delivery.samples.lock().unwrap() = samples;
        }
        for _ in 0..10 {
            if shared.stopping() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Entry, Kind};

    #[test]
    fn queue_counts_each_current_version_once_using_its_own_lane_cursor() {
        let home = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        config::initialize(home.path(), None, None).unwrap();
        crate::engine::add_folder(home.path(), "f", files.path(), false).unwrap();
        let mut folder = config::load(home.path()).unwrap().folders.remove(0);
        folder.ignores = vec!["ignored".into()];
        let root = Root::open(folder, Default::default()).unwrap();
        let c = store::open(home.path()).unwrap();
        let mut expected = Pending::default();
        let cursors = [Some(15), Some(30), Some(9)];
        for n in 0..100 {
            let mut entry = Entry {
                path: if n % 9 == 0 {
                    format!("ignored/{n}")
                } else {
                    format!("file-{n}")
                },
                kind: if n % 7 == 0 {
                    Kind::Deleted
                } else {
                    Kind::File
                },
                size: if n % 3 == 0 { 123 } else { 2 * 1024 * 1024 },
                hash: String::new(),
                target: None,
                mode: 0o644,
                clock: Default::default(),
                seq: 0,
                stamp: String::new(),
            };
            store::put(&c, "f", &mut entry, 0).unwrap();
            let lane = (0..3)
                .find(|index| {
                    Lane {
                        index: *index,
                        count: 3,
                    }
                    .includes(&entry)
                })
                .unwrap();
            if entry.seq > cursors[lane as usize].unwrap() && !root.excluded(&entry.path) {
                expected.entries += 1;
                if entry.kind == Kind::File {
                    expected.files += 1;
                    expected.bytes += entry.size;
                }
            }
        }
        let actual = count(&c, &root, &cursors, Duration::from_secs(2)).unwrap();
        assert!(actual.complete);
        assert_eq!(
            (actual.entries, actual.files, actual.bytes),
            (expected.entries, expected.files, expected.bytes)
        );
        // An exhausted work budget is never a complete zero or a claim of convergence.
        assert!(!count(&c, &root, &cursors, Duration::ZERO).unwrap().complete);
        let before = head(&c, "f").unwrap();
        let mut entry = store::get(&c, "f", "file-1").unwrap().unwrap();
        entry.size = 17;
        store::put(&c, "f", &mut entry, 0).unwrap();
        let next = count(&c, &root, &[Some(before); 3], Duration::from_secs(2)).unwrap();
        assert_eq!((next.entries, next.files, next.bytes), (1, 1, 17));
    }

    #[test]
    fn disconnect_rollback_and_layout_change_cannot_reuse_a_current_claim() {
        let tracker = Tracker::default();
        let lane = Lane { index: 0, count: 1 };
        tracker.acknowledged("p", "f", lane, 10);
        tracker.connected("p", 0, true);
        tracker
            .samples
            .lock()
            .unwrap()
            .entry("p".into())
            .or_default()
            .insert(
                "f".into(),
                Delivery {
                    local_head: 10,
                    acknowledged: vec![Some(10)],
                    pending: Some(Pending {
                        complete: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        assert_eq!(tracker.snapshot()["p"]["f"].active_lanes, 1);
        tracker.connected("p", 0, false);
        assert_eq!(tracker.snapshot()["p"]["f"].active_lanes, 0);
        tracker.acknowledged("p", "f", lane, 5);
        assert!(tracker.snapshot()["p"]["f"].pending.is_none());
        tracker.acknowledged("p", "f", Lane { count: 3, ..lane }, 10);
        assert!(tracker.snapshot()["p"]["f"].pending.is_none());
        assert!(Tracker::default().snapshot().is_empty());
    }
}
