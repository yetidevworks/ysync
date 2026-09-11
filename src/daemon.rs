use crate::{
    config, engine, protocol,
    scanning::{ScanCancelled, ScanControl, ThermalStatus},
    store,
    watching::{Inbox, Native, Scope},
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::os::unix::fs::OpenOptionsExt;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct FolderStatus {
    #[serde(default)]
    pub pending_conflicts: u64,
    pub phase: String,
    pub scanned: u64,
    pub files: u64,
    pub bytes: u64,
    pub last_scan: Option<u64>,
    pub error: Option<String>,
    #[serde(default)]
    pub watcher: String,
    #[serde(default)]
    pub full_scans: u64,
    #[serde(default)]
    pub scoped_scans: u64,
    #[serde(default)]
    pub checked_entries: u64,
    #[serde(default)]
    pub hashed_files: u64,
    #[serde(default)]
    pub hashed_bytes: u64,
    #[serde(default)]
    pub watch_events: u64,
    #[serde(default)]
    pub coalesced_events: u64,
    #[serde(default)]
    pub ignored_events: u64,
    #[serde(default)]
    pub watch_overflows: u64,
    #[serde(default)]
    pub queued_paths: usize,
    #[serde(default)]
    pub native_watches: usize,
    #[serde(default)]
    pub unwatched_subtrees: usize,
    #[serde(default)]
    pub watch_error: Option<String>,
    #[serde(default)]
    pub watch_retry_at: Option<u64>,
    #[serde(default)]
    pub scan_directories: u64,
    #[serde(default)]
    pub scan_waiting_for_cooling: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Event {
    pub at: u64,
    pub kind: String,
    pub folder: Option<String>,
    pub detail: String,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Status {
    #[serde(default)]
    pub daemon_version: String,
    pub pid: u32,
    pub started: u64,
    pub updated: u64,
    pub device: String,
    pub listen: String,
    pub sent_bytes: u64,
    pub received_bytes: u64,
    #[serde(default)]
    pub resumed_bytes: u64,
    #[serde(default)]
    pub delta_reused_bytes: u64,
    pub sent_entries: u64,
    pub received_entries: u64,
    pub send_bytes_per_sec: f64,
    pub receive_bytes_per_sec: f64,
    pub conflicts: u64,
    #[serde(default)]
    pub thermal: ThermalStatus,
    #[serde(default)]
    pub watch_limits: crate::capacity::Limits,
    pub folders: BTreeMap<String, FolderStatus>,
    pub connected_peers: Vec<String>,
    pub events: VecDeque<Event>,
}
pub struct Shared {
    pub home: PathBuf,
    pub id: String,
    stop: AtomicBool,
    sent: AtomicU64,
    received: AtomicU64,
    status: Mutex<Status>,
    gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    peer_gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    ready: Mutex<HashSet<String>>,
    scan_controls: Mutex<HashMap<String, Arc<ScanControl>>>,
}
impl Shared {
    pub(crate) fn new(home: &Path, id: String, listen: String) -> Self {
        Self {
            home: home.to_owned(),
            id: id.clone(),
            stop: AtomicBool::new(false),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            status: Mutex::new(Status {
                daemon_version: env!("CARGO_PKG_VERSION").into(),
                pid: std::process::id(),
                started: now(),
                device: id.clone(),
                listen,
                watch_limits: crate::capacity::limits(),
                ..Default::default()
            }),
            gates: Mutex::new(HashMap::new()),
            peer_gates: Mutex::new(HashMap::new()),
            ready: Mutex::new(HashSet::new()),
            scan_controls: Mutex::new(HashMap::new()),
        }
    }

    fn update_scan_controls(&self, cfg: &config::Config, thermal: &ThermalStatus) {
        let mut controls = self.scan_controls.lock().unwrap();
        for f in &cfg.folders {
            controls.entry(f.id.clone()).or_default();
        }
        for (id, control) in controls.iter() {
            if self.stopping() {
                control.stop();
            } else {
                control.update(
                    !cfg.folders.iter().any(|f| &f.id == id && !f.paused),
                    thermal.cooling,
                );
            }
        }
    }

    pub fn gate(&self, id: &str) -> Arc<Mutex<()>> {
        self.gates
            .lock()
            .unwrap()
            .entry(id.into())
            .or_default()
            .clone()
    }
    pub fn peer_gate(&self, id: &str) -> Arc<Mutex<()>> {
        self.peer_gates
            .lock()
            .unwrap()
            .entry(id.into())
            .or_default()
            .clone()
    }
    #[cfg(test)]
    pub(crate) fn mark_ready_for_test(&self, id: &str) {
        self.ready.lock().unwrap().insert(id.into());
    }
    pub fn ready(&self, id: &str) -> bool {
        self.ready.lock().unwrap().contains(id)
    }
    pub fn stopping(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
    pub fn bytes_sent(&self, n: u64) {
        self.sent.fetch_add(n, Ordering::Relaxed);
    }
    pub fn bytes_received(&self, n: u64) {
        self.received.fetch_add(n, Ordering::Relaxed);
    }
    pub fn file_sent(&self, folder: &str, path: &str) {
        self.status.lock().unwrap().sent_entries += 1;
        self.event("sent", Some(folder), path);
    }
    pub fn resumed(&self, folder: &str, path: &str, bytes: u64) {
        self.status.lock().unwrap().resumed_bytes += bytes;
        self.event(
            "resumed",
            Some(folder),
            &format!("{path}: reused {bytes} verified bytes"),
        );
    }
    pub fn delta_reused(&self, folder: &str, path: &str, bytes: u64) {
        self.status.lock().unwrap().delta_reused_bytes += bytes;
        self.event(
            "delta",
            Some(folder),
            &format!("{path}: reused {bytes} verified local bytes"),
        );
    }
    pub fn file_received(&self, folder: &str, path: &str) {
        self.status.lock().unwrap().received_entries += 1;
        self.event("received", Some(folder), path);
    }
    pub fn event(&self, kind: &str, folder: Option<&str>, detail: &str) {
        let mut s = self.status.lock().unwrap();
        if kind == "conflict" {
            s.conflicts += 1;
        }
        if s.events.back().is_some_and(|e| {
            e.kind == kind && e.detail == detail && now().saturating_sub(e.at) < 30
        }) {
            return;
        }
        s.events.push_back(Event {
            at: now(),
            kind: kind.into(),
            folder: folder.map(str::to_owned),
            detail: detail.chars().take(500).collect(),
        });
        // Keep index churn from evicting every transfer, error, and conflict message.
        if kind == "received"
            && s.events.iter().filter(|e| e.kind == "received").count() > 16
            && let Some(index) = s.events.iter().position(|e| e.kind == "received")
        {
            s.events.remove(index);
        }
        while s.events.len() > 64 {
            s.events.pop_front();
        }
        if matches!(kind, "error" | "approval" | "conflict") {
            eprintln!("[{kind}] {} {detail}", folder.unwrap_or(""));
        }
    }
    pub fn connected(&self, id: &str, connected: bool) {
        let mut s = self.status.lock().unwrap();
        s.connected_peers.retain(|p| p != id);
        if connected {
            s.connected_peers.push(id.into());
        }
    }
}

fn quota_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<notify::Error>()
        .is_some_and(|e| matches!(e.kind, notify::ErrorKind::MaxFilesWatch))
}
fn watcher_label(native: Option<&Native>) -> &'static str {
    match native {
        Some(w) if w.partial() => "partial native",
        Some(_) => "native",
        None => "polling fallback",
    }
}
fn watch_retry_seconds(error: &anyhow::Error, backoff: u64, rescan_secs: u64) -> u64 {
    if error
        .downcast_ref::<notify::Error>()
        .is_some_and(|e| matches!(e.kind, notify::ErrorKind::MaxFilesWatch))
    {
        // Recreating the root watcher succeeds even while the tree cannot fit in
        // the shared quota. A short retry would repeat an expensive full walk.
        backoff.max(rescan_secs.max(5))
    } else {
        backoff
    }
}

fn scanner(shared: Arc<Shared>, folder_id: String) {
    let mut root: Option<Arc<engine::Root>> = None;
    let mut inbox: Option<Inbox> = None;
    let mut native: Option<Native> = None;
    let mut full = true;
    let mut reload = false;
    let mut last_full = Instant::now();
    let mut next_config = Instant::now();
    let mut next_watch = Instant::now();
    let mut watch_backoff = 30u64;
    let mut full_error = None;
    let mut watch_failure = None;
    let mut quota_limited = false;
    let mut blocked_limit = None;
    let mut cfg = match config::load(&shared.home) {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut retries = 0u32;
    let mut retry: Option<(Instant, crate::watching::Batch)> = None;
    while !shared.stopping() {
        let now_at = Instant::now();
        if now_at >= next_config {
            if let Ok(current) = config::load(&shared.home) {
                cfg = current;
            }
            next_config = now_at + Duration::from_secs(1);
        }
        let Some(folder) = cfg.folders.iter().find(|f| f.id == folder_id).cloned() else {
            break;
        };
        if folder.paused {
            native = None;
            shared.ready.lock().unwrap().remove(&folder_id);
            let mut status = shared.status.lock().unwrap();
            let f = status.folders.entry(folder_id.clone()).or_default();
            f.phase = "paused".into();
            f.native_watches = 0;
            f.unwatched_subtrees = 0;
            f.watch_error = None;
            f.watch_retry_at = None;
            f.watcher = "paused".into();
            drop(status);
            full = true;
            next_watch = Instant::now();
            if let Some(inbox) = &inbox {
                inbox.take();
            }
            thread::sleep(Duration::from_millis(250));
            continue;
        }
        if reload || root.as_ref().is_none_or(|r| r.folder != folder) {
            match engine::Root::open(folder.clone(), shared.gate(&folder_id)) {
                Ok(mut opened) => {
                    opened.scan_control = shared
                        .scan_controls
                        .lock()
                        .unwrap()
                        .get(&folder_id)
                        .cloned();
                    let opened = Arc::new(opened);
                    if let Some(inbox) = &inbox {
                        inbox.update_filter(opened.clone());
                    } else {
                        inbox = Some(Inbox::new(opened.clone()));
                    }
                    root = Some(opened);
                    reload = false;
                    full = true;
                    native = None;
                    next_watch = Instant::now();
                }
                Err(e) => {
                    shared.event("error", Some(&folder_id), &e.to_string());
                    shared.ready.lock().unwrap().remove(&folder_id);
                    let mut status = shared.status.lock().unwrap();
                    let f = status.folders.entry(folder_id.clone()).or_default();
                    f.phase = "error".into();
                    f.error = Some(e.to_string());
                    drop(status);
                    thread::sleep(Duration::from_secs(2));
                    continue;
                }
            }
        }
        let root = root.as_ref().unwrap();
        let inbox = inbox.as_ref().unwrap();
        full |= last_full.elapsed() >= Duration::from_secs(cfg.rescan_secs.max(5));
        let mut batch = if inbox.deadline().is_some_and(|d| Instant::now() >= d) {
            inbox.take()
        } else {
            Default::default()
        };
        if retry.as_ref().is_some_and(|(at, _)| Instant::now() >= *at) {
            let (_, old) = retry.take().unwrap();
            for (p, scope) in old.paths {
                batch
                    .paths
                    .entry(p)
                    .and_modify(|s| {
                        if scope == Scope::Tree {
                            *s = scope;
                        }
                    })
                    .or_insert(scope);
            }
        }
        if batch.reload {
            reload = true;
            full = true;
            continue;
        }
        if batch.restart {
            native = None;
            next_watch = Instant::now() + Duration::from_secs(watch_backoff);
            watch_backoff = (watch_backoff * 2).min(300);
        }
        if batch.full && !batch.restart {
            native = None;
            next_watch = Instant::now();
        }
        full |= batch.full;
        let watch_limit = shared.status.lock().unwrap().watch_limits.max_user_watches;
        let capacity_increased = quota_limited
            && watch_limit
                .zip(blocked_limit)
                .is_some_and(|(now, old)| now > old);
        if let Some(w) = native.as_mut()
            && w.partial()
            && (full || Instant::now() >= next_watch || capacity_increased)
        {
            if !full {
                for path in w.gap_paths() {
                    batch.insert_tree(path);
                }
            }
            w.retry_capacity();
            next_watch = Instant::now() + Duration::from_secs(cfg.rescan_secs.max(5));
            blocked_limit = watch_limit;
        }
        if native.is_none() && (Instant::now() >= next_watch || capacity_increased) {
            match Native::start(root, inbox) {
                Ok(w) => {
                    native = Some(w);
                    full = true;
                }
                Err(e) => {
                    watch_failure = Some(format!("{e:#}"));
                    quota_limited = quota_error(&e);
                    blocked_limit = watch_limit;
                    let delay = watch_retry_seconds(&e, watch_backoff, cfg.rescan_secs);
                    shared.event(
                        "error",
                        Some(&folder_id),
                        &format!("native watcher unavailable; retry in {delay}s: {e}"),
                    );
                    next_watch = Instant::now() + Duration::from_secs(delay);
                    watch_backoff = (watch_backoff * 2).min(300);
                }
            }
        }
        let (events, pending) = inbox.metrics();
        {
            let mut status = shared.status.lock().unwrap();
            let f = status.folders.entry(folder_id.clone()).or_default();
            f.watcher = watcher_label(native.as_ref()).into();
            f.unwatched_subtrees = native.as_ref().map_or(0, Native::gap_count);
            f.watch_error = watch_failure.clone();
            f.watch_retry_at = if watch_failure.is_some() {
                Some(
                    now()
                        + next_watch
                            .saturating_duration_since(Instant::now())
                            .as_secs(),
                )
            } else {
                None
            };
            f.native_watches = native.as_ref().map_or(0, Native::count);
            f.watch_events = events.events;
            f.coalesced_events = events.coalesced;
            f.ignored_events = events.ignored;
            f.watch_overflows = events.overflows;
            f.queued_paths = pending;
        }
        if full || !batch.paths.is_empty() {
            let mut trees = Vec::new();
            let mut entries = Vec::new();
            for (path, scope) in &batch.paths {
                if root.excluded(path) {
                    continue;
                }
                if *scope == Scope::Tree || root.dir.symlink_metadata(path).is_err() {
                    trees.push(path.clone());
                } else {
                    entries.push(path.clone());
                }
            }
            let before = (
                root.hashed_files.load(Ordering::Relaxed),
                root.hashed_bytes.load(Ordering::Relaxed),
            );
            let totals_before = {
                let mut status = shared.status.lock().unwrap();
                let f = status.folders.entry(folder_id.clone()).or_default();
                f.phase = if full { "scanning" } else { "updating" }.into();
                f.scanned = 0;
                f.scan_directories = 0;
                if full {
                    f.full_scans += 1;
                } else {
                    f.scoped_scans += 1;
                }
                (f.checked_entries, f.hashed_files, f.hashed_bytes)
            };
            if let Some(w) = native.as_mut() {
                w.invalidate(root, &trees);
                w.begin();
            }
            let mut watch_error = None;
            let checked = std::cell::Cell::new(0u64);
            let directories = std::cell::Cell::new(0u64);
            let watch_count = std::cell::Cell::new(native.as_ref().map_or(0, Native::count));
            let gap_count = std::cell::Cell::new(native.as_ref().map_or(0, Native::gap_count));
            let report = |count| {
                checked.set(count);
                let mut status = shared.status.lock().unwrap();
                let f = status.folders.entry(folder_id.clone()).or_default();
                f.scanned = count;
                f.checked_entries = totals_before.0 + count;
                f.hashed_files =
                    totals_before.1 + root.hashed_files.load(Ordering::Relaxed) - before.0;
                f.hashed_bytes =
                    totals_before.2 + root.hashed_bytes.load(Ordering::Relaxed) - before.1;
                f.native_watches = watch_count.get();
                f.unwatched_subtrees = gap_count.get();
                f.scan_directories = directories.get();
                drop(status);
                shared.ready.lock().unwrap().insert(folder_id.clone());
            };
            let result = (|| -> Result<()> {
                if full || !trees.is_empty() {
                    engine::scan_with_directories(
                        root,
                        &shared.home,
                        &shared.id,
                        if full { None } else { Some(trees.clone()) },
                        report,
                        |path, meta| {
                            directories.set(directories.get() + 1);
                            if let Some(w) = native.as_mut()
                                && let Err(e) = w.directory(root, path, meta)
                            {
                                let quota = quota_error(&e);
                                let mut status = shared.status.lock().unwrap();
                                let f = status.folders.entry(folder_id.clone()).or_default();
                                f.watch_error = Some(format!("{e:#}"));
                                f.watcher = if quota {
                                    "partial native"
                                } else {
                                    "polling fallback"
                                }
                                .into();
                                watch_error = Some(e);
                                if !quota {
                                    native = None;
                                }
                            }
                            watch_count.set(native.as_ref().map_or(0, Native::count));
                            gap_count.set(native.as_ref().map_or(0, Native::gap_count));
                        },
                    )?;
                }
                if !full && !entries.is_empty() {
                    let offset = checked.get();
                    engine::scan_entries(root, &shared.home, &shared.id, entries, |count| {
                        report(offset + count)
                    })?;
                }
                Ok(())
            })();
            let scanned = checked.get();
            if let Some(error) = watch_error {
                quota_limited = quota_error(&error);
                blocked_limit = watch_limit;
                watch_failure = Some(format!("{error:#}"));
                let delay = watch_retry_seconds(&error, watch_backoff, cfg.rescan_secs);
                shared.event(
                    "error",
                    Some(&folder_id),
                    &format!("native watch registration failed; existing coverage retained where available; retry in {delay}s: {error}"),
                );
                next_watch = Instant::now() + Duration::from_secs(delay);
                watch_backoff = (watch_backoff * 2).min(300);
            }
            if result.is_ok() {
                if let Some(w) = native.as_mut() {
                    w.prune(root, full, &trees);
                    if !w.partial() {
                        watch_backoff = 30;
                        watch_failure = None;
                        quota_limited = false;
                    }
                }
                shared.ready.lock().unwrap().insert(folder_id.clone());
            }
            let totals = if full && result.is_ok() {
                store::open(&shared.home)
                    .and_then(|c| store::total(&c, &folder_id))
                    .ok()
            } else {
                None
            };
            let mut status = shared.status.lock().unwrap();
            let f = status.folders.entry(folder_id.clone()).or_default();
            f.checked_entries = totals_before.0 + scanned;
            f.hashed_files = totals_before.1 + root.hashed_files.load(Ordering::Relaxed) - before.0;
            f.hashed_bytes = totals_before.2 + root.hashed_bytes.load(Ordering::Relaxed) - before.1;
            f.scan_directories = directories.get();
            f.scanned = scanned;
            f.last_scan = Some(now());
            f.native_watches = native.as_ref().map_or(0, Native::count);
            f.watcher = watcher_label(native.as_ref()).into();
            f.unwatched_subtrees = native.as_ref().map_or(0, Native::gap_count);
            f.watch_error = watch_failure.clone();
            f.watch_retry_at = if watch_failure.is_some() {
                Some(
                    now()
                        + next_watch
                            .saturating_duration_since(Instant::now())
                            .as_secs(),
                )
            } else {
                None
            };
            match result {
                Err(e) if e.is::<ScanCancelled>() => {
                    f.phase = "paused".into();
                    // A cancelled pass is incomplete, never a successful full scan.
                    // Resume from a fresh pass after a manual pause/config change.
                    drop(status);
                    shared.ready.lock().unwrap().remove(&folder_id);
                    full = true;
                    next_config = Instant::now();
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Ok(()) => {
                    if full {
                        full_error = None;
                    }
                    f.phase = if full_error.is_some() {
                        "incomplete"
                    } else if native.as_ref().is_some_and(Native::partial) {
                        "partial"
                    } else if native.is_some() {
                        "watching"
                    } else {
                        "polling"
                    }
                    .into();
                    f.error = full_error.clone();
                    if let Some((files, bytes)) = totals {
                        f.files = files;
                        f.bytes = bytes;
                    }
                    retries = 0;
                    retry = None;
                }
                Err(e) => {
                    let detail = format!("{e:#}");
                    if full {
                        full_error = Some(detail.clone());
                    }
                    f.phase = "error".into();
                    f.error = Some(detail.clone());
                    drop(status);
                    shared.event("error", Some(&folder_id), &detail);
                    if !full && retries < 3 {
                        retries += 1;
                        retry = Some((
                            Instant::now() + Duration::from_secs(1 << retries),
                            batch.clone(),
                        ));
                    }
                    if full {
                        last_full = Instant::now();
                    }
                    full = false;
                    continue;
                }
            }
            if full {
                last_full = Instant::now();
                if quota_limited {
                    next_watch = last_full + Duration::from_secs(cfg.rescan_secs.max(5));
                }
            }
            full = false;
        }
        let mut deadline = next_config.min(last_full + Duration::from_secs(cfg.rescan_secs.max(5)));
        if native.is_none() || native.as_ref().is_some_and(Native::partial) {
            deadline = deadline.min(next_watch);
        }
        if let Some(at) = inbox.deadline() {
            deadline = deadline.min(at);
        }
        if let Some((at, _)) = &retry {
            deadline = deadline.min(*at);
        }
        inbox.wait(deadline.saturating_duration_since(Instant::now()));
    }
}

pub fn serve(home: &Path) -> Result<()> {
    let (id, _) = config::identity(home)?;
    let cfg = config::load(home)?;
    if !(1..=64).contains(&cfg.scan_workers) {
        bail!("scan_workers must be between 1 and 64");
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.scan_workers)
        .thread_name(|n| format!("ysync-scan-{n}"))
        .build_global()?;
    let status_db = store::open(home)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(home.join("daemon.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)
        .context("ysync is already running for this state directory")?;
    let listener =
        TcpListener::bind(&cfg.listen).with_context(|| format!("listening on {}", cfg.listen))?;
    listener.set_nonblocking(true)?;
    let shared = Arc::new(Shared::new(home, id.clone(), cfg.listen.clone()));
    let stop = shared.clone();
    ctrlc::set_handler(move || {
        stop.stop.store(true, Ordering::Relaxed);
        for control in stop.scan_controls.lock().unwrap().values() {
            control.stop();
        }
    })?;
    eprintln!(
        "ysync {} • device {id} • listening {}",
        env!("CARGO_PKG_VERSION"),
        cfg.listen
    );
    let active = Arc::new(AtomicU64::new(0));
    let mut scanned = HashSet::new();
    let mut dialing = HashSet::new();
    let mut workers = Vec::new();
    let mut tick = Instant::now() - Duration::from_secs(2);
    let mut prev = (0, 0);
    let mut prev_at = Instant::now();
    let mut thermal = ThermalStatus::default();
    while !shared.stopping() {
        match listener.accept() {
            Ok((stream, _)) => {
                if active.load(Ordering::Relaxed) >= 32 {
                    drop(stream);
                } else {
                    active.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    let s = shared.clone();
                    thread::spawn(move || {
                        if let Err(e) = protocol::session(stream, s.clone(), None, false) {
                            s.event("connection", None, &format!("{e:#}"));
                        }
                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }
        if tick.elapsed() >= Duration::from_secs(1) {
            let cfg = config::load(home)?;
            let reading = if cfg.scan_max_temp_c.is_some() {
                crate::scanning::cpu_temperature()
            } else {
                Ok(0.0)
            };
            let previous = (thermal.cooling, thermal.error.clone(), thermal.max_temp_c);
            thermal.sample(cfg.scan_max_temp_c, reading);
            shared.update_scan_controls(&cfg, &thermal);
            if previous != (thermal.cooling, thermal.error.clone(), thermal.max_temp_c) {
                let detail = if let Some(e) = &thermal.error {
                    format!("scanners paused: {e}")
                } else if thermal.cooling {
                    format!(
                        "scanners cooling at {:.1} C; resume at {} C",
                        thermal.temperature_c.unwrap(),
                        thermal.resume_temp_c.unwrap()
                    )
                } else if let Some(max) = thermal.max_temp_c {
                    format!("scanners enabled; temperature limit {max} C")
                } else {
                    "scanner temperature control disabled".into()
                };
                shared.event("thermal", None, &detail);
            }
            for f in cfg.folders {
                if scanned.insert(f.id.clone()) {
                    let s = shared.clone();
                    workers.push(thread::spawn(move || scanner(s, f.id)));
                }
            }
            for p in cfg.peers {
                if p.approved && p.address.is_some() && dialing.insert(p.id.clone()) {
                    let s = shared.clone();
                    workers.push(thread::spawn(move || {
                        while !s.stopping() {
                            let current = config::load(&s.home).ok().and_then(|c| {
                                c.peers.into_iter().find(|x| x.id == p.id && x.approved)
                            });
                            if let Some(p) = current
                                && let Some(address) = p.address
                            {
                                let gate = s.peer_gate(&p.id);
                                let available = gate.try_lock().is_ok();
                                if available {
                                    let result = (|| -> Result<()> {
                                        let addr = address
                                            .to_socket_addrs()?
                                            .next()
                                            .context("address did not resolve")?;
                                        let stream = TcpStream::connect_timeout(
                                            &addr,
                                            Duration::from_secs(3),
                                        )?;
                                        protocol::session(
                                            stream,
                                            s.clone(),
                                            Some(p.id.clone()),
                                            true,
                                        )
                                    })();
                                    if let Err(e) = result {
                                        s.event("connection", None, &format!("{}: {e:#}", p.name));
                                    }
                                }
                            }
                            let jitter = u64::from_str_radix(&p.id[..2], 16).unwrap_or(0);
                            for _ in 0..(10 + jitter % 10) {
                                if s.stopping() {
                                    break;
                                }
                                thread::sleep(Duration::from_millis(100));
                            }
                        }
                    }));
                }
            }
            let sent = shared.sent.load(Ordering::Relaxed);
            let received = shared.received.load(Ordering::Relaxed);
            let dt = prev_at.elapsed().as_secs_f64();
            let waiting: HashMap<_, _> = shared
                .scan_controls
                .lock()
                .unwrap()
                .iter()
                .map(|(id, c)| (id.clone(), c.waiting()))
                .collect();
            let conflict_counts = crate::conflicts::counts(&status_db).ok();
            let mut status = shared.status.lock().unwrap();
            status.thermal = thermal.clone();
            status.watch_limits = crate::capacity::limits();
            for (id, f) in &mut status.folders {
                if let Some(counts) = &conflict_counts {
                    f.pending_conflicts = counts.get(id).copied().unwrap_or(0);
                }
                f.scan_waiting_for_cooling = waiting.get(id).copied().unwrap_or(false);
            }
            status.updated = now();
            status.sent_bytes = sent;
            status.received_bytes = received;
            status.send_bytes_per_sec = (sent - prev.0) as f64 / dt;
            status.receive_bytes_per_sec = (received - prev.1) as f64 / dt;
            // Monitoring is disposable state; it does not add a disk durability barrier per refresh.
            fs::write(
                home.join("status.tmp"),
                serde_json::to_vec_pretty(&*status)?,
            )?;
            fs::rename(home.join("status.tmp"), home.join("status.json"))?;
            drop(status);
            prev = (sent, received);
            prev_at = Instant::now();
            tick = Instant::now();
        }
        thread::sleep(Duration::from_millis(20));
    }
    // Stop accepting; a process exit releases the daemon lock. In-flight, unacknowledged batches retry.
    shared.event("service", None, "Daemon stopped");
    let mut status = shared.status.lock().unwrap();
    status.updated = now();
    status.pid = 0;
    fs::write(
        home.join("status.json"),
        serde_json::to_vec_pretty(&*status)?,
    )?;
    drop(workers);
    Ok(())
}
pub fn read_status(home: &Path) -> Result<Status> {
    Ok(serde_json::from_slice(
        &fs::read(home.join("status.json")).context("no activity snapshot; start ysync serve")?,
    )?)
}
pub fn monitor(home: &Path, once: bool) -> Result<()> {
    use std::io::{self, IsTerminal, Write};
    if !once && !io::stdout().is_terminal() {
        bail!("monitor requires a terminal; use status --json for scripts");
    }
    loop {
        let s = read_status(home)?;
        let cfg = config::load(home)?;
        if !once {
            print!("\x1b[2J\x1b[H");
        }
        let age = now().saturating_sub(s.updated);
        let alive = s.pid != 0 && age < 5;
        println!(
            "YSYNC  {}  {}\nDevice {}\n{}  PID {}  snapshot {}s ago\n",
            env!("CARGO_PKG_VERSION"),
            cfg.name,
            s.device,
            if alive { "RUNNING" } else { "OFFLINE / STALE" },
            s.pid,
            age
        );
        println!("Scan/hash workers: {}", cfg.scan_workers);
        if let Some(max) = s.thermal.max_temp_c {
            println!(
                "Scanner thermal control: {} | CPU {} C | pause {} C / resume {} C",
                if s.thermal.cooling {
                    "COOLING"
                } else {
                    "ready"
                },
                s.thermal
                    .temperature_c
                    .map_or_else(|| "unavailable".into(), |t| format!("{t:.1}")),
                max,
                s.thermal.resume_temp_c.unwrap_or(max.saturating_sub(5))
            );
            if let Some(e) = &s.thermal.error {
                println!("  {}", sanitize(e));
            }
        } else {
            println!("Scanner thermal control: disabled");
        }
        println!();
        println!(
            "SEND {:>9.2} MB/s   {:>10.2} MB total   {} files",
            s.send_bytes_per_sec / 1e6,
            s.sent_bytes as f64 / 1e6,
            s.sent_entries
        );
        println!(
            "RECV {:>9.2} MB/s   {:>10.2} MB total   {} entries\n",
            s.receive_bytes_per_sec / 1e6,
            s.received_bytes as f64 / 1e6,
            s.received_entries
        );
        println!(
            "RESUME {:>10.2} MB reused this run\n",
            s.resumed_bytes as f64 / 1e6
        );
        println!(
            "DELTA  {:>10.2} MB reused this run\n",
            s.delta_reused_bytes as f64 / 1e6
        );
        if let Some(limit) = s.watch_limits.max_user_watches {
            let owned: usize = s.folders.values().map(|f| f.native_watches).sum();
            println!("INOTIFY {owned} ysync registrations / {limit} shared-user limit");
        }
        println!("FOLDERS");
        for (id, f) in s.folders {
            println!(
                "  {id:<20} {:<10} {:>9} files  {:>8.2} GB  scanned {:>9}",
                f.phase,
                f.files,
                f.bytes as f64 / 1e9,
                f.scanned
            );
            if f.pending_conflicts > 0 {
                println!(
                    "    {} pending conflicts: working files preserved; run ysync conflict list",
                    f.pending_conflicts
                );
            }
            if f.scan_waiting_for_cooling {
                println!(
                    "    COOLING: scan progress retained; waiting for CPU temperature to fall"
                );
            }
            if let Some(e) = &f.watch_error {
                println!(
                    "    WATCH COVERAGE: {} uncovered subtrees; {}",
                    f.unwatched_subtrees,
                    sanitize(e)
                );
                if let Some(at) = f.watch_retry_at {
                    println!(
                        "    coverage retry in {}s; covered paths remain event-driven",
                        at.saturating_sub(now())
                    );
                }
            }
            if let Some(e) = f.error {
                println!("    ERROR: {}", sanitize(&e));
            }
            println!(
                "    {}: {} registrations, {} pending; scans {} full / {} scoped; checked {} entries, hashed {} files ({:.2} MB)",
                f.watcher,
                f.native_watches,
                f.queued_paths,
                f.full_scans,
                f.scoped_scans,
                f.checked_entries,
                f.hashed_files,
                f.hashed_bytes as f64 / 1e6
            );
            println!(
                "    events {} received / {} coalesced / {} ignored; {} overflow signals",
                f.watch_events, f.coalesced_events, f.ignored_events, f.watch_overflows
            );
            if f.phase == "scanning" {
                println!(
                    "    directories visited in current scan: {}",
                    f.scan_directories
                );
            }
        }
        println!("\nDEVICES");
        for p in cfg.peers {
            println!(
                "  {:<20} {}  {}  folders: {}",
                sanitize(&p.name),
                if !p.approved {
                    "PENDING "
                } else if s.connected_peers.contains(&p.id) {
                    "CONNECTED"
                } else {
                    "OFFLINE  "
                },
                &p.id[..12.min(p.id.len())],
                p.folders.join(", ")
            );
        }
        println!("\nRECENT ACTIVITY   {} conflicts this run", s.conflicts);
        for e in s.events.iter().rev().take(10) {
            println!(
                "  {:<10} {:<14} {}",
                if e.kind == "received" {
                    "index"
                } else {
                    &e.kind
                },
                e.folder.as_deref().unwrap_or(""),
                sanitize(&e.detail)
            );
        }
        if once {
            break;
        }
        println!("\nCtrl-C to leave monitoring; the service continues running.");
        io::stdout().flush()?;
        thread::sleep(Duration::from_secs(1));
    }
    Ok(())
}
fn sanitize(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).take(180).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_churn_keeps_non_index_activity_visible() {
        let home = tempfile::tempdir().unwrap();
        let shared = Shared::new(home.path(), "device".into(), "localhost:0".into());
        shared.event("error", None, "connection failed");
        for n in 0..1000 {
            shared.file_received("folder", &format!("path-{n}"));
        }
        let status = shared.status.lock().unwrap();
        assert_eq!(status.received_entries, 1000);
        assert_eq!(
            status
                .events
                .iter()
                .filter(|e| e.kind == "received")
                .count(),
            16
        );
        assert!(status.events.iter().any(|e| e.kind == "error"));
        drop(status);
        for n in 0..100 {
            shared.event("sent", None, &format!("file-{n}"));
        }
        assert_eq!(shared.status.lock().unwrap().events.len(), 64);
    }

    #[test]
    fn watch_quota_exhaustion_waits_for_reconciliation() {
        let quota = anyhow::Error::new(notify::Error::new(notify::ErrorKind::MaxFilesWatch))
            .context("registering directory");
        assert_eq!(watch_retry_seconds(&quota, 30, 3600), 3600);
        assert_eq!(watch_retry_seconds(&quota, 300, 60), 300);
        let transient = anyhow::Error::new(notify::Error::generic("temporarily unavailable"));
        assert_eq!(watch_retry_seconds(&transient, 30, 3600), 30);
    }
}
