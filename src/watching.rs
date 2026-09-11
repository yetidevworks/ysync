//! Bounded event coalescing and native watch ownership. Callbacks never scan the filesystem.
use crate::engine::Root;
use anyhow::Result;
use cap_std::fs::Metadata;
#[cfg(target_os = "linux")]
use cap_std::fs::MetadataExt;
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{AccessKind, AccessMode, ModifyKind},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock, mpsc},
    time::{Duration, Instant},
};

const LIMIT: usize = 4096;
const QUIET: Duration = Duration::from_millis(100);
const MAX_DELAY: Duration = Duration::from_secs(1);
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Entry,
    Tree,
}
#[derive(Clone, Default)]
pub struct Batch {
    pub full: bool,
    pub reload: bool,
    pub restart: bool,
    pub paths: BTreeMap<String, Scope>,
}
impl Batch {
    pub fn insert_tree(&mut self, path: String) {
        if self.full
            || path
                .match_indices('/')
                .any(|(n, _)| self.paths.get(&path[..n]) == Some(&Scope::Tree))
        {
            return;
        }
        let children: Vec<_> = self
            .paths
            .range(format!("{path}/")..format!("{path}0"))
            .map(|(p, _)| p.clone())
            .collect();
        for child in children {
            self.paths.remove(&child);
        }
        self.paths.insert(path, Scope::Tree);
    }
}
#[derive(Clone, Copy, Default)]
pub struct Counts {
    pub events: u64,
    pub ignored: u64,
    pub coalesced: u64,
    pub overflows: u64,
}
#[derive(Default)]
struct Pending {
    batch: Batch,
    counts: Counts,
    first: Option<Instant>,
    last: Option<Instant>,
}
impl Pending {
    fn touch(&mut self, now: Instant) {
        self.first.get_or_insert(now);
        self.last = Some(now);
    }
    fn deadline(&self) -> Option<Instant> {
        Some((self.first? + MAX_DELAY).min(self.last? + QUIET))
    }
    fn full(&mut self, now: Instant) {
        self.batch.full = true;
        self.batch.paths.clear();
        self.touch(now);
    }
    fn insert(&mut self, path: String, scope: Scope, now: Instant) {
        self.touch(now);
        if self.batch.full {
            self.counts.coalesced += 1;
            return;
        }
        if self
            .batch
            .paths
            .get(&path)
            .is_some_and(|s| *s == Scope::Tree || *s == scope)
            || path
                .match_indices('/')
                .any(|(n, _)| self.batch.paths.get(&path[..n]) == Some(&Scope::Tree))
        {
            self.counts.coalesced += 1;
            return;
        }
        if scope == Scope::Tree {
            let children: Vec<_> = self
                .batch
                .paths
                .range(format!("{path}/")..format!("{path}0"))
                .map(|(p, _)| p.clone())
                .collect();
            self.counts.coalesced += children.len() as u64;
            for child in children {
                self.batch.paths.remove(&child);
            }
        }
        self.batch.paths.insert(path, scope);
        if self.batch.paths.len() > LIMIT {
            self.counts.overflows += 1;
            self.full(now);
        }
    }
    fn push(&mut self, event: notify::Result<Event>, root: &Root, now: Instant) {
        self.counts.events += 1;
        let event = match event {
            Ok(e) => e,
            Err(_) => {
                self.batch.restart = true;
                self.full(now);
                return;
            }
        };
        if event.need_rescan() {
            self.counts.overflows += 1;
            self.full(now);
        }
        if matches!(event.kind, EventKind::Access(_))
            && !matches!(
                event.kind,
                EventKind::Access(AccessKind::Close(AccessMode::Write))
            )
        {
            self.counts.ignored += 1;
            return;
        }
        let scope = match event.kind {
            EventKind::Modify(ModifyKind::Data(_) | ModifyKind::Metadata(_))
            | EventKind::Access(_) => Scope::Entry,
            _ => Scope::Tree,
        };
        for path in event.paths {
            let Ok(relative) = path.strip_prefix(&root.folder.path) else {
                continue;
            };
            let Some(relative) = relative.to_str() else {
                self.full(now);
                continue;
            };
            if relative.eq_ignore_ascii_case(".ysyncignore") {
                self.batch.reload = true;
                self.full(now);
                continue;
            }
            if relative.eq_ignore_ascii_case(".ysync/marker")
                || relative.eq_ignore_ascii_case(".ysync")
            {
                // Internal version/tmp churn never schedules a scan; loss of the control directory does.
                if matches!(
                    event.kind,
                    EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
                ) || relative.eq_ignore_ascii_case(".ysync/marker")
                {
                    self.batch.reload = true;
                    self.full(now);
                }
                continue;
            }
            if root.excluded(relative) {
                self.counts.ignored += 1;
                continue;
            }
            if relative.is_empty() {
                if scope == Scope::Tree {
                    self.batch.reload = true;
                    self.full(now);
                } else {
                    self.counts.ignored += 1;
                }
                continue;
            }
            self.insert(relative.to_owned(), scope, now);
        }
    }
}
struct State {
    pending: Mutex<Pending>,
    filter: RwLock<Arc<Root>>,
    wake: mpsc::SyncSender<()>,
}
pub struct Inbox {
    state: Arc<State>,
    rx: mpsc::Receiver<()>,
}
impl Inbox {
    pub fn new(root: Arc<Root>) -> Self {
        let (wake, rx) = mpsc::sync_channel(1);
        Self {
            state: Arc::new(State {
                pending: Mutex::new(Pending::default()),
                filter: RwLock::new(root),
                wake,
            }),
            rx,
        }
    }
    pub fn update_filter(&self, root: Arc<Root>) {
        *self.state.filter.write().unwrap() = root;
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.state.pending.lock().unwrap().deadline()
    }
    pub fn take(&self) -> Batch {
        let mut p = self.state.pending.lock().unwrap();
        p.first = None;
        p.last = None;
        std::mem::take(&mut p.batch)
    }
    pub fn metrics(&self) -> (Counts, usize) {
        let p = self.state.pending.lock().unwrap();
        (p.counts, p.batch.paths.len())
    }
    pub fn wait(&self, timeout: Duration) {
        let _ = self.rx.recv_timeout(timeout);
    }
    fn watcher(&self) -> Result<RecommendedWatcher> {
        let state = self.state.clone();
        Ok(RecommendedWatcher::new(
            move |event| {
                let filter = state.filter.read().unwrap();
                let mut pending = state.pending.lock().unwrap();
                pending.push(event, &filter, Instant::now());
                if pending.first.is_some() {
                    let _ = state.wake.try_send(());
                }
            },
            notify::Config::default().with_follow_symlinks(false),
        )?)
    }
}

pub struct Native {
    watcher: RecommendedWatcher,
    #[cfg(target_os = "linux")]
    dirs: BTreeMap<String, (u64, u64, u64)>,
    #[cfg(target_os = "linux")]
    identities: std::collections::HashMap<(u64, u64), String>,
    generation: u64,
    gaps: BTreeMap<String, u64>,
    quota_blocked: bool,
    #[cfg(all(test, target_os = "linux"))]
    test_watch_limit: Option<usize>,
}
impl Native {
    pub fn start(root: &Root, inbox: &Inbox) -> Result<Self> {
        let mut native = Self {
            watcher: inbox.watcher()?,
            #[cfg(target_os = "linux")]
            dirs: BTreeMap::new(),
            #[cfg(target_os = "linux")]
            identities: std::collections::HashMap::new(),
            generation: 0,
            gaps: BTreeMap::new(),
            quota_blocked: false,
            #[cfg(all(test, target_os = "linux"))]
            test_watch_limit: None,
        };
        #[cfg(target_os = "linux")]
        {
            native.directory(root, "", &root.dir.dir_metadata()?)?;
            native.directory(root, ".ysync", &root.dir.symlink_metadata(".ysync")?)?;
        }
        #[cfg(not(target_os = "linux"))]
        native
            .watcher
            .watch(&root.folder.path, RecursiveMode::Recursive)?;
        Ok(native)
    }
    pub fn partial(&self) -> bool {
        !self.gaps.is_empty()
    }
    pub fn gap_count(&self) -> usize {
        self.gaps.len()
    }
    pub fn gap_paths(&self) -> Vec<String> {
        self.gaps.keys().cloned().collect()
    }
    pub fn retry_capacity(&mut self) {
        self.quota_blocked = false;
    }
    #[cfg(target_os = "linux")]
    fn gap(&mut self, path: &str) {
        if let Some(parent) = path
            .match_indices('/')
            .map(|(n, _)| &path[..n])
            .find(|p| self.gaps.contains_key(*p))
            .map(str::to_owned)
        {
            self.gaps.insert(parent, self.generation);
            return;
        }
        let children: Vec<_> = self
            .gaps
            .range(format!("{path}/")..format!("{path}0"))
            .map(|(p, _)| p.clone())
            .collect();
        for child in children {
            self.gaps.remove(&child);
        }
        self.gaps.insert(path.into(), self.generation);
    }
    pub fn begin(&mut self) {
        self.generation += 1;
    }
    pub fn directory(&mut self, root: &Root, relative: &str, meta: &Metadata) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            let identity = (meta.dev(), meta.ino());
            if let Some(record) = self.dirs.get_mut(relative)
                && (record.0, record.1) == identity
            {
                record.2 = self.generation;
                self.gaps.remove(relative);
                return Ok(());
            }
            if self.quota_blocked {
                if let Some(old) = self.dirs.remove(relative) {
                    self.identities.remove(&(old.0, old.1));
                    let _ = self.watcher.unwatch(&root.folder.path.join(relative));
                }
                self.gap(relative);
                return Ok(());
            }
            if let Some(previous) = self.identities.get(&identity).cloned()
                && previous != relative
            {
                let _ = self.watcher.unwatch(&root.folder.path.join(&previous));
                self.dirs.remove(&previous);
            }
            if let Some(old) = self.dirs.get(relative) {
                self.identities.remove(&(old.0, old.1));
            }
            let absolute = root.folder.path.join(relative);
            if self.dirs.contains_key(relative) {
                let _ = self.watcher.unwatch(&absolute);
            }
            #[cfg(all(test, target_os = "linux"))]
            let injected = self
                .test_watch_limit
                .is_some_and(|limit| self.dirs.len() >= limit);
            #[cfg(not(all(test, target_os = "linux")))]
            let injected = false;
            let result = if injected {
                Err(notify::Error::new(notify::ErrorKind::MaxFilesWatch))
            } else {
                self.watcher.watch(&absolute, RecursiveMode::NonRecursive)
            };
            if let Err(e) = result {
                if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) {
                    self.quota_blocked = true;
                    self.gap(relative);
                    // A replaced directory may have lost its old watch above.
                    self.dirs.remove(relative);
                }
                return Err(e.into());
            }
            self.gaps.remove(relative);
            self.identities.insert(identity, relative.to_owned());
            self.dirs
                .insert(relative.into(), (identity.0, identity.1, self.generation));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (root, relative, meta);
        Ok(())
    }
    pub fn invalidate(&mut self, root: &Root, scopes: &[String]) {
        #[cfg(target_os = "linux")]
        for path in scopes {
            let mut old: Vec<_> = self
                .dirs
                .range(format!("{path}/")..format!("{path}0"))
                .map(|(p, _)| p.clone())
                .collect();
            if self.dirs.contains_key(path) {
                old.push(path.clone());
            }
            for p in old {
                if p.is_empty() || p == ".ysync" {
                    continue;
                }
                let _ = self.watcher.unwatch(&root.folder.path.join(&p));
                if let Some(old) = self.dirs.remove(&p) {
                    self.identities.remove(&(old.0, old.1));
                    self.quota_blocked = false;
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (root, scopes);
    }
    pub fn prune(&mut self, root: &Root, full: bool, scopes: &[String]) {
        #[cfg(target_os = "linux")]
        {
            let mut candidates = Vec::new();
            if full {
                candidates.extend(self.dirs.keys().cloned());
            } else {
                for path in scopes {
                    if self.dirs.contains_key(path) {
                        candidates.push(path.clone());
                    }
                    candidates.extend(
                        self.dirs
                            .range(format!("{path}/")..format!("{path}0"))
                            .map(|(p, _)| p.clone()),
                    );
                }
            }
            for p in candidates {
                if p.is_empty() || p == ".ysync" {
                    continue;
                }
                if self.dirs.get(&p).is_some_and(|r| r.2 != self.generation) {
                    let _ = self.watcher.unwatch(&root.folder.path.join(&p));
                    if let Some(old) = self.dirs.remove(&p) {
                        self.identities.remove(&(old.0, old.1));
                        self.quota_blocked = false;
                    }
                }
            }
        }
        let candidates: Vec<_> = if full {
            self.gaps.keys().cloned().collect()
        } else {
            scopes
                .iter()
                .flat_map(|path| {
                    let mut paths: Vec<_> = self
                        .gaps
                        .range(format!("{path}/")..format!("{path}0"))
                        .map(|(p, _)| p.clone())
                        .collect();
                    if self.gaps.contains_key(path) {
                        paths.push(path.clone());
                    }
                    paths
                })
                .collect()
        };
        for path in candidates {
            if self
                .gaps
                .get(&path)
                .is_some_and(|generation| *generation != self.generation)
            {
                self.gaps.remove(&path);
            }
        }
        if self.gaps.is_empty() {
            self.quota_blocked = false;
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (root, full, scopes);
    }
    pub fn count(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            self.dirs.len()
        }
        #[cfg(not(target_os = "linux"))]
        {
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config, engine};
    use notify::event::{CreateKind, DataChange, Flag, MetadataKind};
    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, Arc<Root>) {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        config::initialize(state.path(), None, None).unwrap();
        engine::add_folder(state.path(), "test", files.path(), true).unwrap();
        let root = Arc::new(
            Root::open(
                config::load(state.path()).unwrap().folders.remove(0),
                Arc::new(Mutex::new(())),
            )
            .unwrap(),
        );
        (state, files, root)
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn quota_retains_coverage_and_recovers_only_missing_subtrees() {
        let (state, files, root) = fixture();
        std::fs::create_dir_all(files.path().join("covered")).unwrap();
        std::fs::create_dir_all(files.path().join("gap/child")).unwrap();
        let inbox = Inbox::new(root.clone());
        let mut native = Native::start(&root, &inbox).unwrap();
        native.test_watch_limit = Some(3); // root, control, and one included directory
        native.begin();
        native
            .directory(&root, "", &root.dir.dir_metadata().unwrap())
            .unwrap();
        native
            .directory(
                &root,
                "covered",
                &root.dir.symlink_metadata("covered").unwrap(),
            )
            .unwrap();
        let error = native
            .directory(&root, "gap", &root.dir.symlink_metadata("gap").unwrap())
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<notify::Error>().unwrap().kind,
            notify::ErrorKind::MaxFilesWatch
        ));
        native
            .directory(
                &root,
                "gap/child",
                &root.dir.symlink_metadata("gap/child").unwrap(),
            )
            .unwrap();
        assert_eq!(native.count(), 3);
        assert_eq!(native.gap_paths(), vec!["gap"]);
        // Native callbacks for a covered directory continue despite exhaustion elsewhere.
        inbox.take();
        std::fs::write(files.path().join("covered/event"), b"still watched").unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            inbox.wait(Duration::from_millis(50));
            if inbox.take().paths.contains_key("covered/event") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "covered directory lost its native watch"
            );
        }
        native.test_watch_limit = None;
        native.retry_capacity();
        native.begin();
        let scopes = native.gap_paths();
        let mut visited = Vec::new();
        engine::scan_with_directories(
            &root,
            state.path(),
            "device",
            Some(scopes.clone()),
            |_| {},
            |path, meta| {
                visited.push(path.to_owned());
                native.directory(&root, path, meta).unwrap();
            },
        )
        .unwrap();
        native.prune(&root, false, &scopes);
        assert_eq!(visited, vec!["gap", "gap/child"]);
        assert!(!native.partial());
        assert_eq!(native.count(), 5);
        assert!(native.dirs.contains_key("covered"));
        inbox.take();
        std::fs::write(files.path().join("gap/child/event"), b"recovered").unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            inbox.wait(Duration::from_millis(50));
            if inbox.take().paths.contains_key("gap/child/event") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "recovered directory did not deliver events"
            );
        }
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn vanished_gap_is_pruned_without_losing_other_missing_scopes() {
        let (_state, files, root) = fixture();
        std::fs::create_dir(files.path().join("missing")).unwrap();
        std::fs::create_dir(files.path().join("other")).unwrap();
        let inbox = Inbox::new(root.clone());
        let mut native = Native::start(&root, &inbox).unwrap();
        native.test_watch_limit = Some(2);
        native.begin();
        assert!(
            native
                .directory(
                    &root,
                    "missing",
                    &root.dir.symlink_metadata("missing").unwrap()
                )
                .is_err()
        );
        native
            .directory(&root, "other", &root.dir.symlink_metadata("other").unwrap())
            .unwrap();
        std::fs::remove_dir(files.path().join("missing")).unwrap();
        native.begin();
        native.prune(&root, false, &["missing".into()]);
        assert_eq!(native.gap_paths(), vec!["other"]);
        assert!(native.partial());
    }
    #[test]
    fn recovery_scopes_merge_without_rewalking_nested_trees() {
        let mut batch = Batch::default();
        batch.paths.insert("parent/entry".into(), Scope::Entry);
        batch.insert_tree("parent/gap".into());
        batch.insert_tree("parent".into());
        batch.insert_tree("parent/gap/child".into());
        batch.insert_tree("parent-other".into());
        assert_eq!(
            batch.paths.keys().cloned().collect::<Vec<_>>(),
            vec!["parent", "parent-other"]
        );
    }
    #[test]
    fn storms_are_bounded_and_coalesce_without_starving_updates() {
        let mut p = Pending::default();
        let start = Instant::now();
        for i in 0..100_000 {
            p.insert(
                "src/main.rs".into(),
                Scope::Entry,
                start + Duration::from_micros(i * 20),
            );
        }
        assert_eq!(p.batch.paths.len(), 1);
        assert_eq!(p.counts.coalesced, 99_999);
        assert_eq!(p.deadline(), Some(start + MAX_DELAY));
        p.insert("src/other.rs".into(), Scope::Entry, start);
        p.insert("src-other/file".into(), Scope::Entry, start);
        p.insert("src".into(), Scope::Tree, start);
        assert_eq!(p.batch.paths.len(), 2);
        assert!(!p.batch.paths.contains_key("src/main.rs"));
        for i in 0..LIMIT + 10 {
            p.insert(format!("many/{i}"), Scope::Entry, start);
        }
        assert!(p.batch.full);
        assert!(p.batch.paths.is_empty());
        assert_eq!(p.counts.overflows, 1);
    }
    #[test]
    fn callback_filters_ignored_work_and_keeps_recovery_signals() {
        let (_s, _f, root) = fixture();
        let at = Instant::now();
        let mut p = Pending::default();
        for path in [
            "node_modules/a/b",
            ".ysync/tmp/partial",
            ".ysync/versions/old",
        ] {
            p.push(
                Ok(Event::new(EventKind::Create(CreateKind::File))
                    .add_path(root.folder.path.join(path))),
                &root,
                at,
            );
        }
        p.push(
            Ok(
                Event::new(EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)))
                    .add_path(root.folder.path.clone()),
            ),
            &root,
            at,
        );
        assert!(p.first.is_none());
        assert!(p.batch.paths.is_empty());
        p.push(
            Ok(
                Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)))
                    .add_path(root.folder.path.join("edited")),
            ),
            &root,
            at,
        );
        assert!(p.batch.paths.contains_key("edited"));
        assert!(!p.batch.full);
        p.push(
            Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)),
            &root,
            at,
        );
        assert!(p.batch.full);
        assert_eq!(p.counts.overflows, 1);
        p.push(Err(notify::Error::generic("watch failure")), &root, at);
        assert!(p.batch.restart);
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_watches_only_included_directories_and_follows_renames() {
        let (state, files, root) = fixture();
        std::fs::create_dir_all(files.path().join("old/sub")).unwrap();
        std::fs::write(files.path().join("old/sub/file"), b"initial").unwrap();
        std::fs::create_dir_all(files.path().join("node_modules/ignored/deep")).unwrap();
        let inbox = Inbox::new(root.clone());
        let mut native = Native::start(&root, &inbox).unwrap();
        let id = config::identity(state.path()).unwrap().0;
        native.begin();
        engine::scan_with_directories(
            &root,
            state.path(),
            &id,
            None,
            |_| {},
            |p, m| native.directory(&root, p, m).unwrap(),
        )
        .unwrap();
        assert_eq!(native.count(), 4); // root, control directory, old, old/sub
        std::fs::rename(files.path().join("old"), files.path().join("new")).unwrap();
        native.begin();
        let regions = vec!["old".into(), "new".into()];
        engine::scan_with_directories(
            &root,
            state.path(),
            &id,
            Some(regions.clone()),
            |_| {},
            |p, m| native.directory(&root, p, m).unwrap(),
        )
        .unwrap();
        native.prune(&root, false, &regions);
        assert_eq!(native.count(), 4);
        inbox.take();
        std::fs::write(files.path().join("new/sub/file"), b"after rename").unwrap();
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            inbox.wait(Duration::from_millis(50));
            if inbox.take().paths.contains_key("new/sub/file") {
                break;
            }
            assert!(Instant::now() < until, "renamed directory lost its watch");
        }
    }
}
