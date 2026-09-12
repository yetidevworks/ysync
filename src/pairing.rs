//! Offline, reviewable enrollment. A seed changes causal history, never source working files.
use crate::{
    config, engine,
    model::{self, Entry, Kind},
    store,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Header {
    format: u32,
    device: String,
    folder: config::Folder,
    ignores: Option<String>,
    mode: String,
    peer: Option<String>,
    counter: u64,
}
#[derive(Debug, Default, Serialize)]
pub struct Summary {
    pub mode: String,
    pub source: String,
    pub receiver: String,
    pub adopt_history: u64,
    pub replace: u64,
    pub delete: u64,
    pub ignored: u64,
    pub conflicts: u64,
    pub send: u64,
    pub receive: u64,
    pub send_new: u64,
}
pub(crate) fn stopped(home: &Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(home.join("daemon.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("stop the ysync daemon first")?;
    Ok(lock)
}
pub(crate) fn root(home: &Path, folder: &str) -> Result<engine::Root> {
    let f = config::load(home)?
        .folders
        .into_iter()
        .find(|f| f.id == folder)
        .context("unknown folder")?;
    engine::Root::open(f, Arc::new(Mutex::new(())))
}
fn read(path: &Path) -> Result<Connection> {
    let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    c.execute_batch("PRAGMA trusted_schema=OFF; BEGIN;")?;
    Ok(c)
}
fn create(path: &Path) -> Result<Connection> {
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    let c = Connection::open(path)?;
    c.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE header(data TEXT NOT NULL); CREATE TABLE items(path TEXT PRIMARY KEY,path_key TEXT UNIQUE NOT NULL,remote TEXT,local TEXT,stamp TEXT,action TEXT NOT NULL);")?;
    Ok(c)
}
fn header(c: &Connection) -> Result<Header> {
    ensure!(
        c.query_row("SELECT count(*) FROM header", [], |r| r.get::<_, i64>(0))? == 1,
        "invalid pairing header"
    );
    let h: Header = serde_json::from_str(
        &c.query_row("SELECT data FROM header", [], |r| r.get::<_, String>(0))?,
    )?;
    ensure!(h.format == 1, "unsupported pairing format");
    config::valid_id(&h.device)?;
    config::valid_folder_id(&h.folder.id)?;
    Ok(h)
}
fn counter(c: &Connection, folder: &str) -> Result<u64> {
    Ok(c.query_row(
        "SELECT COALESCE((SELECT value FROM counters WHERE folder=?1),0)",
        [folder],
        |r| r.get::<_, i64>(0),
    )? as u64)
}
fn artifact(home: &Path, path: &Path) -> Result<()> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = absolute
        .parent()
        .context("missing artifact parent")?
        .canonicalize()?;
    let target = parent.join(absolute.file_name().context("missing artifact filename")?);
    for folder in config::load(home)?.folders {
        ensure!(
            !target.starts_with(folder.path.canonicalize()?),
            "keep pairing artifacts outside synchronized folders"
        );
    }
    Ok(())
}
fn scan(home: &Path, root: &mut engine::Root, device: &str) -> Result<()> {
    let cfg = config::load(home)?;
    // Enrollment is explicit offline work; keep hashing bounded by the configured pool.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.scan_workers.clamp(1, 64))
        .build()?;
    if cfg.scan_max_temp_c.is_none() {
        pool.install(|| engine::scan(root, home, device, None, |_| {}))?;
        return Ok(());
    }
    let control = Arc::new(crate::scanning::ScanControl::default());
    root.scan_control = Some(control.clone());
    let mut thermal = crate::scanning::ThermalStatus::default();
    thermal.sample(cfg.scan_max_temp_c, crate::scanning::cpu_temperature());
    if let Some(error) = thermal.error {
        anyhow::bail!("enrollment scan temperature unavailable: {error}");
    }
    control.update(false, thermal.cooling);
    let result = std::thread::scope(|scope| {
        let work = scope.spawn(|| pool.install(|| engine::scan(root, home, device, None, |_| {})));
        while !work.is_finished() {
            let was = thermal.cooling;
            thermal.sample(cfg.scan_max_temp_c, crate::scanning::cpu_temperature());
            if thermal.error.is_some() {
                control.stop();
            } else {
                control.update(false, thermal.cooling);
            }
            if thermal.cooling != was {
                eprintln!(
                    "Enrollment scan {}",
                    if thermal.cooling {
                        "waiting for cooling"
                    } else {
                        "resuming"
                    }
                );
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        work.join()
            .map_err(|_| anyhow::anyhow!("enrollment scan worker panicked"))?
    });
    root.scan_control = None;
    result.map(|_| ())
}
fn ignores(root: &engine::Root) -> Result<Option<String>> {
    match root.dir.read_to_string(".ysyncignore") {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn finish(c: Connection, path: &Path) -> Result<()> {
    drop(c);
    fs::File::open(path)?.sync_all()?;
    fs::File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
/// Export the receiver before granting the source access. Full scan errors abort export.
pub fn export(home: &Path, folder: &str, output: &Path) -> Result<()> {
    artifact(home, output)?;
    let _lock = stopped(home)?;
    let mut root = root(home, folder)?;
    let device = config::identity(home)?.0;
    scan(home, &mut root, &device)?;
    let c = store::open(home)?;
    let mut dest = create(output)?;
    let tx = dest.transaction()?;
    let h = Header {
        format: 1,
        device,
        folder: root.folder.clone(),
        ignores: ignores(&root)?,
        mode: "receiver".into(),
        peer: None,
        counter: counter(&c, folder)?,
    };
    tx.execute(
        "INSERT INTO header VALUES(?1)",
        [serde_json::to_string(&h)?],
    )?;
    let mut q = c.prepare("SELECT path,data FROM entries WHERE folder=?1 ORDER BY path")?;
    for row in q.query_map([folder], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })? {
        let (path, data) = row?;
        if !root.excluded(&path) {
            tx.execute(
                "INSERT INTO items VALUES(?1,?2,?3,NULL,NULL,'receiver')",
                params![path, model::path_key(&path), data],
            )?;
        }
    }
    tx.commit()?;
    finish(dest, output)
}
/// Preview seed-local or explain the differences normal merge would preserve as conflicts.
pub fn preview(
    home: &Path,
    folder: &str,
    receiver: &Path,
    mode: &str,
    output: &Path,
) -> Result<Summary> {
    ensure!(
        matches!(mode, "merge" | "seed-local"),
        "mode must be merge or seed-local"
    );
    artifact(home, output)?;
    let _lock = stopped(home)?;
    let mut root = root(home, folder)?;
    let device = config::identity(home)?.0;
    let incoming = read(receiver)?;
    let remote_header = header(&incoming)?;
    ensure!(
        remote_header.mode == "receiver"
            && remote_header.folder.id == folder
            && remote_header.device != device,
        "receiver snapshot must be from the other device and the same folder ID"
    );
    scan(home, &mut root, &device)?;
    let c = store::open(home)?;
    let mut plan = create(output)?;
    let tx = plan.transaction()?;
    let h = Header {
        format: 1,
        device: device.clone(),
        folder: root.folder.clone(),
        ignores: ignores(&root)?,
        mode: mode.into(),
        peer: Some(remote_header.device.clone()),
        counter: counter(&c, folder)?,
    };
    let mut summary = Summary {
        mode: mode.into(),
        source: device,
        receiver: remote_header.device,
        ..Default::default()
    };
    let mut q = incoming.prepare("SELECT remote FROM items ORDER BY path")?;
    for row in q.query_map([], |r| r.get::<_, String>(0))? {
        let remote: Entry = serde_json::from_str(&row?)?;
        remote.validate()?;
        if root.excluded(&remote.path) {
            summary.ignored += 1;
            continue;
        }
        let remote = engine::resolve_incoming_paths(&c, &root, &[remote])?.remove(0);
        let local = store::get(&c, folder, &remote.path)?;
        if local.is_none() && remote.kind == Kind::Deleted {
            continue;
        }
        let same = local.as_ref().is_some_and(|l| l.same_content(&remote));
        let relation = local
            .as_ref()
            .map(|l| model::relation(&l.clock, &remote.clock));
        let action = if mode == "merge" {
            if same {
                summary.adopt_history += 1;
                "history"
            } else if matches!(
                relation,
                Some(model::Relation::Concurrent | model::Relation::Equal)
            ) {
                summary.conflicts += 1;
                "conflict"
            } else if matches!(relation, Some(model::Relation::After)) {
                summary.send += 1;
                "send"
            } else {
                summary.receive += 1;
                "receive"
            }
        } else if same {
            summary.adopt_history += 1;
            "history"
        } else if local.as_ref().is_none_or(|l| l.kind == Kind::Deleted) {
            summary.delete += 1;
            "delete"
        } else {
            summary.replace += 1;
            "replace"
        };
        // Replacing a populated directory remains unsupported by the receiver. Don't issue a plan that cannot finish.
        ensure!(
            mode == "merge"
                || local.as_ref().is_none_or(|l| l.kind == remote.kind
                    || l.kind == Kind::Deleted
                    || remote.kind == Kind::Deleted
                    || (l.kind != Kind::Directory && remote.kind != Kind::Directory)),
            "file/directory replacement requires manual review: {}",
            remote.path
        );
        tx.execute(
            "INSERT INTO items VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                remote.path,
                model::path_key(&remote.path),
                serde_json::to_string(&remote)?,
                local.as_ref().map(serde_json::to_string).transpose()?,
                local.as_ref().map(|l| l.stamp.clone()),
                action
            ],
        )?;
    }
    // Include source-only live paths so their publication order follows parent metadata.
    let mut locals=c.prepare("SELECT path,data,stamp FROM entries WHERE folder=?1 AND json_extract(data,'$.kind')!='Deleted' ORDER BY path")?;
    for row in locals.query_map([folder], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        let (path, data, stamp) = row?;
        if root.excluded(&path) {
            continue;
        }
        if !tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM items WHERE path_key=?1)",
            [model::path_key(&path)],
            |r| r.get::<_, bool>(0),
        )? {
            tx.execute(
                "INSERT INTO items VALUES(?1,?2,NULL,?3,?4,'send-new')",
                params![path, model::path_key(&path), data, stamp],
            )?;
            summary.send_new += 1;
        }
    }
    tx.execute(
        "INSERT INTO header VALUES(?1)",
        [serde_json::to_string(&h)?],
    )?;
    tx.commit()?;
    finish(plan, output)?;
    Ok(summary)
}
/// Commit one reviewed seed atomically; subsequent remote edits remain conflicts.
pub fn apply(home: &Path, path: &Path) -> Result<u64> {
    let _lock = stopped(home)?;
    let plan = read(path)?;
    let h = header(&plan)?;
    ensure!(
        h.mode == "seed-local",
        "merge needs no apply step: approve and connect normally"
    );
    ensure!(
        config::identity(home)?.0 == h.device,
        "plan belongs to a different source device"
    );
    config::valid_id(h.peer.as_deref().context("missing receiver")?)?;
    let root = root(home, &h.folder.id)?;
    ensure!(
        root.folder == h.folder && ignores(&root)? == h.ignores,
        "folder configuration changed; preview again"
    );
    let mut c = store::open(home)?;
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    ensure!(
        counter(&tx, &h.folder.id)? == h.counter,
        "source index changed; preview again"
    );
    // Persist the review before publishing any new history. Receiver publication retains replaced/deleted files in .ysync/versions.
    let audit = home.join("pairing");
    fs::create_dir_all(&audit)?;
    let saved = audit.join(format!("{}.sqlite", uuid::Uuid::new_v4()));
    {
        let mut copy = create(&saved)?;
        let copy_tx = copy.transaction()?;
        copy_tx.execute(
            "INSERT INTO header VALUES(?1)",
            [serde_json::to_string(&h)?],
        )?;
        let mut rows = plan
            .prepare("SELECT path,path_key,remote,local,stamp,action FROM items ORDER BY path")?;
        for row in rows.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, String>(5)?,
            ))
        })? {
            let (p, k, r, l, s, a) = row?;
            copy_tx.execute(
                "INSERT INTO items VALUES(?1,?2,?3,?4,?5,?6)",
                params![p, k, r, l, s, a],
            )?;
        }
        copy_tx.commit()?;
        finish(copy, &saved)?;
    }
    fs::File::open(&audit)?.sync_all()?;
    let mut q =
        plan.prepare("SELECT path,remote,local,stamp FROM items ORDER BY CASE WHEN local IS NULL OR json_extract(local,'$.kind')='Deleted' THEN 1 ELSE 0 END, CASE WHEN local IS NULL OR json_extract(local,'$.kind')='Deleted' THEN -length(path) ELSE length(path) END,path")?;
    let mut count = 0;
    let mut flush = Vec::new();
    for row in q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
        ))
    })? {
        let (path, remote, local, stamp) = row?;
        let remote: Option<Entry> = remote.map(|s| serde_json::from_str(&s)).transpose()?;
        if let Some(remote) = &remote {
            remote.validate()?;
            ensure!(remote.path == path, "invalid remote plan path");
        }
        model::validate_path(&path)?;
        ensure!(!root.excluded(&path), "newly ignored plan path");
        let local: Option<Entry> = local.map(|s| serde_json::from_str(&s)).transpose()?;
        ensure!(
            remote.is_some() || local.as_ref().is_some_and(|e| e.kind != Kind::Deleted),
            "invalid source-only plan entry"
        );
        let indexed = store::get(&tx, &h.folder.id, &path)?;
        ensure!(
            match (&local, &indexed) {
                (None, None) => true,
                (Some(a), Some(b)) =>
                    a.path == path
                        && a.same_content(b)
                        && a.clock == b.clock
                        && a.size == b.size
                        && stamp.as_deref() == Some(&b.stamp),
                _ => false,
            },
            "indexed version changed: {path}; preview again"
        );
        // The preview hash is reusable only while the device/inode/size/mtime/ctime fingerprint still matches.
        let observed = engine::observe(&root, &path, indexed.as_ref(), false)?;
        ensure!(
            match (&indexed, &observed) {
                (None, None) => true,
                (Some(a), None) => a.kind == Kind::Deleted,
                (Some(a), Some(b)) => a.same_content(b) && a.size == b.size && a.stamp == b.stamp,
                _ => false,
            },
            "working version changed: {path}; preview again"
        );
        let mut next = indexed.unwrap_or_else(|| Entry {
            path: path.clone(),
            kind: Kind::Deleted,
            size: 0,
            hash: String::new(),
            target: None,
            mode: remote.as_ref().map_or(0, |e| e.mode),
            clock: Default::default(),
            seq: 0,
            stamp: String::new(),
        });
        if let Some(remote) = &remote {
            next.clock = model::merge(&next.clock, &remote.clock);
        }
        let n = next.clock.entry(h.device.clone()).or_default();
        *n = n.checked_add(1).context("version counter exhausted")?;
        next.validate()?;
        flush.push(next.clone());
        if flush.len() == 128 {
            flush_source(&root, &flush)?;
            flush.clear();
        }
        store::put(&tx, &h.folder.id, &mut next, 0)?;
        crate::conflicts::clear_resolved(&tx, &h.folder.id, &next)?;
        count += 1;
    }
    flush_source(&root, &flush)?;
    // Recheck earlier paths after the final flush; an editor could have changed them while later paths were being verified.
    let mut check = plan.prepare("SELECT path FROM items")?;
    for path in check.query_map([], |r| r.get::<_, String>(0))? {
        let path = path?;
        let current = store::get(&tx, &h.folder.id, &path)?.context("missing planned entry")?;
        let observed = engine::observe(&root, &path, Some(&current), false)?;
        ensure!(
            match observed {
                None => current.kind == Kind::Deleted,
                Some(e) => current.same_content(&e) && current.stamp == e.stamp,
            },
            "working version changed during seed: {path}; preview again"
        );
    }
    root.check()?;
    ensure!(
        root.folder
            == config::load(home)?
                .folders
                .into_iter()
                .find(|f| f.id == h.folder.id)
                .context("folder removed")?
            && ignores(&root)? == h.ignores,
        "folder configuration changed during seed"
    );
    tx.commit()?;
    Ok(count)
}

fn flush_source(root: &engine::Root, entries: &[Entry]) -> Result<()> {
    crate::durability::parallel(entries, |e| {
        if matches!(e.kind, Kind::File | Kind::Directory) {
            root.dir.open(&e.path)?.into_std().sync_all()?;
        }
        Ok(())
    })?;
    engine::sync_directories(root, entries)
}
/// Bounded, readable inspection of the exact saved plan.
pub fn show(path: &Path, offset: u64, limit: u16) -> Result<serde_json::Value> {
    ensure!(
        (1..=1000).contains(&limit),
        "limit must be between 1 and 1000"
    );
    let c = read(path)?;
    let h = header(&c)?;
    let mut q =
        c.prepare("SELECT path,action,remote,local FROM items ORDER BY path LIMIT ?1 OFFSET ?2")?;
    let rows=q.query_map(params![limit,i64::try_from(offset)?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?)))?
        .map(|r|{let(p,a,remote,local)=r?;Ok(serde_json::json!({"path":p,"action":a,"receiver":remote.map(|s|serde_json::from_str::<Entry>(&s)).transpose()?,"source":local.map(|s|serde_json::from_str::<Entry>(&s)).transpose()?}))}).collect::<Result<Vec<_>>>()?;
    Ok(
        serde_json::json!({"mode":h.mode,"folder":h.folder.id,"source":h.device,"receiver":h.peer,"offset":offset,"items":rows}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Device {
        home: tempfile::TempDir,
        files: tempfile::TempDir,
        id: String,
    }
    impl Device {
        fn new() -> Self {
            let home = tempfile::tempdir().unwrap();
            let files = tempfile::tempdir().unwrap();
            let id = config::initialize(home.path(), None, None).unwrap();
            engine::add_folder(home.path(), "code", files.path(), false).unwrap();
            Self { home, files, id }
        }
        fn write(&self, path: &str, data: &[u8]) {
            let p = self.files.path().join(path);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, data).unwrap();
        }
        fn root(&self) -> engine::Root {
            root(self.home.path(), "code").unwrap()
        }
        fn scan(&self) {
            scan(self.home.path(), &mut self.root(), &self.id).unwrap();
        }
    }
    fn deliver(a: &Device, b: &Device) {
        let from = store::open(a.home.path()).unwrap();
        let mut to = store::open(b.home.path()).unwrap();
        let root = b.root();
        for e in store::changes(&from, "code", 0, 1000).unwrap() {
            let temp = if e.kind == Kind::File {
                let (p, mut f) = engine::temp_file(&root).unwrap();
                use std::io::Write;
                f.write_all(&fs::read(a.files.path().join(&e.path)).unwrap())
                    .unwrap();
                f.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(e.mode))
                    .unwrap();
                Some(p)
            } else {
                None
            };
            let tx = to.transaction().unwrap();
            engine::apply(&tx, &root, &b.id, &e, temp.as_deref()).unwrap();
            engine::sync_directories(&root, std::slice::from_ref(&e)).unwrap();
            tx.commit().unwrap();
        }
    }
    #[test]
    fn seed_preserves_source_archives_receiver_and_propagates_later_changes() {
        let a = Device::new();
        let b = Device::new();
        let artifacts = tempfile::tempdir().unwrap();
        a.write("shared", b"current source");
        a.write("same", b"same");
        a.write("source-only", b"new");
        b.write("shared", b"old receiver");
        b.write("same", b"same");
        b.write("retired/child", b"keep backup");
        let snapshot = artifacts.path().join("receiver.sqlite");
        let plan = artifacts.path().join("seed.sqlite");
        export(b.home.path(), "code", &snapshot).unwrap();
        let summary = preview(a.home.path(), "code", &snapshot, "seed-local", &plan).unwrap();
        assert_eq!(
            (summary.replace, summary.delete, summary.adopt_history),
            (1, 2, 1)
        );
        assert_eq!(
            show(&plan, 0, 2).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(apply(a.home.path(), &plan).unwrap(), 5);
        assert!(apply(a.home.path(), &plan).is_err());
        assert_eq!(
            fs::read(a.files.path().join("shared")).unwrap(),
            b"current source"
        );
        deliver(&a, &b);
        assert_eq!(
            fs::read(b.files.path().join("shared")).unwrap(),
            b"current source"
        );
        assert_eq!(
            fs::read(b.files.path().join("source-only")).unwrap(),
            b"new"
        );
        assert!(!b.files.path().join("retired").exists());
        assert!(
            crate::conflicts::list(&store::open(b.home.path()).unwrap())
                .unwrap()
                .is_empty()
        );
        let contents: Vec<_> = fs::read_dir(b.files.path().join(".ysync/versions"))
            .unwrap()
            .map(|p| fs::read(p.unwrap().path()).unwrap())
            .collect();
        assert!(contents.iter().any(|x| x == b"old receiver"));
        assert!(contents.iter().any(|x| x == b"keep backup"));
        a.write("shared", b"later source");
        a.scan();
        deliver(&a, &b);
        assert_eq!(
            fs::read(b.files.path().join("shared")).unwrap(),
            b"later source"
        );
    }
    #[test]
    fn restored_directory_metadata_precedes_source_only_children() {
        let a = Device::new();
        let b = Device::new();
        let artifacts = tempfile::tempdir().unwrap();
        b.write("restored/old", b"old");
        b.scan();
        fs::remove_dir_all(b.files.path().join("restored")).unwrap();
        b.scan();
        a.write("restored/new", b"new source child");
        fs::set_permissions(
            a.files.path().join("restored"),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let snapshot = artifacts.path().join("receiver");
        let plan = artifacts.path().join("plan");
        export(b.home.path(), "code", &snapshot).unwrap();
        preview(a.home.path(), "code", &snapshot, "seed-local", &plan).unwrap();
        apply(a.home.path(), &plan).unwrap();
        deliver(&a, &b);
        assert_eq!(
            fs::read(b.files.path().join("restored/new")).unwrap(),
            b"new source child"
        );
        assert!(
            crate::conflicts::list(&store::open(b.home.path()).unwrap())
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn stale_source_rolls_back_every_planned_baseline() {
        let a = Device::new();
        let b = Device::new();
        let artifacts = tempfile::tempdir().unwrap();
        for p in ["first-long-path", "last"] {
            a.write(p, b"source");
            b.write(p, b"receiver");
        }
        let snapshot = artifacts.path().join("receiver");
        let plan = artifacts.path().join("plan");
        export(b.home.path(), "code", &snapshot).unwrap();
        preview(a.home.path(), "code", &snapshot, "seed-local", &plan).unwrap();
        let c = store::open(a.home.path()).unwrap();
        let before = counter(&c, "code").unwrap();
        a.write("last", b"edited after review");
        assert!(
            apply(a.home.path(), &plan)
                .unwrap_err()
                .to_string()
                .contains("working version changed")
        );
        assert_eq!(counter(&c, "code").unwrap(), before);
        assert!(
            !store::get(&c, "code", "first-long-path")
                .unwrap()
                .unwrap()
                .clock
                .contains_key(&b.id)
        );
    }
    #[test]
    fn receiver_edit_after_export_still_conflicts() {
        let a = Device::new();
        let b = Device::new();
        let artifacts = tempfile::tempdir().unwrap();
        a.write("file", b"source");
        b.write("file", b"receiver");
        let snapshot = artifacts.path().join("receiver");
        let plan = artifacts.path().join("plan");
        export(b.home.path(), "code", &snapshot).unwrap();
        preview(a.home.path(), "code", &snapshot, "seed-local", &plan).unwrap();
        apply(a.home.path(), &plan).unwrap();
        b.write("file", b"new receiver edit");
        deliver(&a, &b);
        assert_eq!(
            fs::read(b.files.path().join("file")).unwrap(),
            b"new receiver edit"
        );
        assert_eq!(
            crate::conflicts::list(&store::open(b.home.path()).unwrap())
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn merge_is_non_destructive_and_cannot_be_applied_as_seed() {
        let a = Device::new();
        let b = Device::new();
        let artifacts = tempfile::tempdir().unwrap();
        a.write("file", b"source");
        b.write("file", b"receiver");
        b.write("extra", b"extra");
        let snapshot = artifacts.path().join("receiver");
        let plan = artifacts.path().join("plan");
        export(b.home.path(), "code", &snapshot).unwrap();
        let summary = preview(a.home.path(), "code", &snapshot, "merge", &plan).unwrap();
        assert_eq!(
            (summary.conflicts, summary.receive, summary.delete),
            (1, 1, 0)
        );
        assert!(apply(a.home.path(), &plan).is_err());
        assert!(apply(b.home.path(), &plan).is_err());
        assert_eq!(fs::read(a.files.path().join("file")).unwrap(), b"source");
    }
    #[test]
    fn daemon_lock_and_configuration_changes_reject_enrollment() {
        let a = Device::new();
        let b = Device::new();
        let artifacts = tempfile::tempdir().unwrap();
        a.write("file", b"source");
        b.write("file", b"receiver");
        let snapshot = artifacts.path().join("receiver");
        let plan = artifacts.path().join("plan");
        let lock = stopped(b.home.path()).unwrap();
        assert!(export(b.home.path(), "code", &snapshot).is_err());
        drop(lock);
        export(b.home.path(), "code", &snapshot).unwrap();
        preview(a.home.path(), "code", &snapshot, "seed-local", &plan).unwrap();
        fs::write(a.files.path().join(".ysyncignore"), "file\n").unwrap();
        assert!(apply(a.home.path(), &plan).is_err());
    }
}
