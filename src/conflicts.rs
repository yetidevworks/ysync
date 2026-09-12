//! Durable unresolved versions. Recording a conflict never changes the working file or its clock.
use crate::{config, engine, model, store};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conflict {
    pub id: String,
    pub folder: String,
    pub local: model::Entry,
    pub incoming: model::Entry,
    pub payload: Option<String>,
}

pub fn save(
    c: &Connection,
    root: &engine::Root,
    local: &model::Entry,
    remote: &model::Entry,
    temp: Option<&str>,
) -> Result<()> {
    let mut incoming = remote.clone();
    incoming.seq = 0;
    let id = blake3::hash(&serde_json::to_vec(&incoming)?)
        .to_hex()
        .to_string();
    if c.query_row(
        "SELECT EXISTS(SELECT 1 FROM conflicts WHERE folder=?1 AND id=?2)",
        params![root.folder.id, id],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(());
    }
    let mut record = Conflict {
        id: id.clone(),
        folder: root.folder.id.clone(),
        local: local.clone(),
        incoming,
        payload: None,
    };
    root.dir.create_dir_all(".ysync/conflicts")?;
    if remote.kind == model::Kind::File {
        let destination = format!(".ysync/conflicts/{id}");
        if let Some(temp) = temp {
            // Archive permissions are private; the original mode stays in the record.
            root.dir.set_permissions(
                temp,
                cap_std::fs::Permissions::from_std(fs::Permissions::from_mode(0o600)),
            )?;
            root.dir.rename(temp, &root.dir, &destination)?;
        } else if local.same_bytes(remote) {
            // A permissions-only conflict did not request a payload. Copy it:
            // a hard link would let later in-place edits mutate the saved version.
            root.dir.copy(&remote.path, &root.dir, &destination)?;
            root.dir.set_permissions(
                &destination,
                cap_std::fs::Permissions::from_std(fs::Permissions::from_mode(0o600)),
            )?;
        } else {
            bail!(
                "destination changed during negotiation; retry required to preserve incoming conflict"
            );
        }
        let mut file = root.dir.open(&destination)?.into_std();
        let mut hash = blake3::Hasher::new();
        let bytes = std::io::copy(&mut file, &mut hash)?;
        ensure!(
            bytes == remote.size && hash.finalize().to_hex().as_str() == remote.hash,
            "incoming conflict archive changed; retry required"
        );
        file.sync_all()?;
        record.payload = Some(destination);
    }
    // The manifest is also retained outside SQLite for recovery after a failed commit.
    let manifest = format!(".ysync/conflicts/{id}.json");
    root.dir
        .write(&manifest, serde_json::to_vec_pretty(&record)?)?;
    root.dir.set_permissions(
        &manifest,
        cap_std::fs::Permissions::from_std(fs::Permissions::from_mode(0o600)),
    )?;
    root.dir.open(&manifest)?.into_std().sync_all()?;
    c.execute(
        "INSERT INTO conflicts(folder,id,path,data) VALUES(?1,?2,?3,?4)",
        params![
            record.folder,
            record.id,
            record.incoming.path,
            serde_json::to_string(&record)?
        ],
    )?;
    Ok(())
}

pub fn list(c: &Connection) -> Result<Vec<Conflict>> {
    let mut q = c.prepare("SELECT data FROM conflicts ORDER BY folder,path,id")?;
    q.query_map([], |r| r.get::<_, String>(0))?
        .map(|r| Ok(serde_json::from_str(&r?)?))
        .collect()
}

pub fn counts(c: &Connection) -> Result<std::collections::HashMap<String, u64>> {
    let mut q = c.prepare("SELECT folder,count(*) FROM conflicts GROUP BY folder")?;
    Ok(
        q.query_map([], |r| Ok((r.get(0)?, r.get::<_, i64>(1)? as u64)))?
            .collect::<rusqlite::Result<_>>()?,
    )
}

pub fn clear_resolved(c: &Connection, folder: &str, applied: &model::Entry) -> Result<()> {
    let mut q = c.prepare("SELECT id,data FROM conflicts WHERE folder=?1 AND path=?2")?;
    let records = q
        .query_map(params![folder, applied.path], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, json) in records {
        let record: Conflict = serde_json::from_str(&json)?;
        let includes = |entry: &model::Entry| {
            matches!(
                model::relation(&entry.clock, &applied.clock),
                model::Relation::Before | model::Relation::Equal
            )
        };
        if includes(&record.local) && includes(&record.incoming) {
            c.execute(
                "DELETE FROM conflicts WHERE folder=?1 AND id=?2",
                params![folder, id],
            )?;
        }
    }
    Ok(())
}

/// The user explicitly chooses the current local version, after any manual merge.
/// Stop the daemon first so scanner/receiver transactions cannot race this command.
pub fn keep_local(home: &Path, folder: &str, id: &str) -> Result<()> {
    resolve_local(home, folder, id, None)
}

/// Resolve exactly the indexed version shown in the review panel. Refuse stale reviews.
pub fn keep_local_reviewed(
    home: &Path,
    folder: &str,
    id: &str,
    expected: &model::Entry,
) -> Result<()> {
    resolve_local(home, folder, id, Some(expected))
}

fn resolve_local(
    home: &Path,
    folder: &str,
    id: &str,
    expected: Option<&model::Entry>,
) -> Result<()> {
    ensure!(
        id.len() == 64 && hex::decode(id).is_ok(),
        "use the full conflict ID"
    );
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(home.join("daemon.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)
        .context("stop the ysync daemon before resolving a conflict")?;
    let configured = config::load(home)?
        .folders
        .into_iter()
        .find(|f| f.id == folder)
        .context("unknown folder")?;
    let root = engine::Root::open(configured, Arc::new(Mutex::new(())))?;
    let mut c = store::open(home)?;
    let record: Conflict = serde_json::from_str(
        &c.query_row(
            "SELECT data FROM conflicts WHERE folder=?1 AND id=?2",
            params![folder, id],
            |r| r.get::<_, String>(0),
        )
        .context("unknown or already resolved conflict")?,
    )?;
    ensure!(
        !root.excluded(&record.incoming.path),
        "path is now ignored; review the conflict before changing exclusions"
    );
    let device = config::identity(home)?.0;
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    engine::refresh(&tx, &root, &record.incoming.path, &device, 0, true)?;
    let mut local =
        store::get(&tx, folder, &record.incoming.path)?.context("local version is unavailable")?;
    if let Some(expected) = expected {
        ensure!(
            local.path == expected.path
                && local.same_content(expected)
                && local.size == expected.size
                && local.clock == expected.clock
                && local.stamp == expected.stamp,
            "local version changed since review; refresh and review again"
        );
    }
    if matches!(local.kind, model::Kind::File | model::Kind::Directory) {
        root.dir.open(&local.path)?.into_std().sync_all()?;
    }
    engine::sync_directories(&root, std::slice::from_ref(&local))?;
    if expected.is_some() {
        let observed = engine::observe(&root, &local.path, Some(&local), false)?;
        ensure!(
            match observed {
                Some(e) => e.same_content(&local) && e.size == local.size && e.stamp == local.stamp,
                None => local.kind == model::Kind::Deleted,
            },
            "local version changed during resolution; refresh and review again"
        );
    }
    local.clock = model::merge(&local.clock, &record.incoming.clock);
    let n = local.clock.entry(device).or_default();
    *n = n.checked_add(1).context("version counter exhausted")?;
    store::put(&tx, folder, &mut local, 0)?;
    tx.execute(
        "DELETE FROM conflicts WHERE folder=?1 AND id=?2",
        params![folder, id],
    )?;
    tx.commit()?;
    Ok(())
}

/// One bounded page, loaded only when the user opens or refreshes conflict review.
pub struct Page {
    pub records: Vec<Conflict>,
    pub more: bool,
}
pub fn page(home: &Path, folder: Option<&str>, query: &str, offset: usize) -> Result<Page> {
    let c = read_connection(home)?;
    let mut q = c.prepare("SELECT data FROM conflicts WHERE (?1 IS NULL OR folder=?1) AND (instr(lower(path),lower(?2))>0 OR instr(lower(folder),lower(?2))>0 OR instr(id,?2)>0) ORDER BY folder,path,id LIMIT 51 OFFSET ?3")?;
    let mut records = q
        .query_map(params![folder, query, offset as i64], |r| {
            r.get::<_, String>(0)
        })?
        .map(|r| Ok(serde_json::from_str(&r?)?))
        .collect::<Result<Vec<Conflict>>>()?;
    let more = records.len() > 50;
    records.truncate(50);
    Ok(Page { records, more })
}
fn read_connection(home: &Path) -> Result<Connection> {
    let c = Connection::open_with_flags(
        home.join("index.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    c.busy_timeout(std::time::Duration::from_millis(250))?;
    Ok(c)
}

pub struct Review {
    pub record: Conflict,
    pub current: Option<model::Entry>,
    pub local_preview: String,
    pub incoming_preview: String,
}
pub fn review(home: &Path, folder: &str, id: &str) -> Result<Review> {
    ensure!(
        id.len() == 64 && hex::decode(id).is_ok(),
        "invalid conflict ID"
    );
    let mut c = read_connection(home)?;
    let tx = c.transaction()?;
    let record: Conflict = serde_json::from_str(
        &tx.query_row(
            "SELECT data FROM conflicts WHERE folder=?1 AND id=?2",
            params![folder, id],
            |r| r.get::<_, String>(0),
        )
        .context("conflict was resolved or removed; refresh the list")?,
    )?;
    let indexed = store::get(&tx, folder, &record.incoming.path)?;
    tx.commit()?;
    let configured = config::load(home)?
        .folders
        .into_iter()
        .find(|f| f.id == folder)
        .context("folder is no longer configured; its conflict record is retained")?;
    let root = engine::Root::open(configured, Arc::new(Mutex::new(())))?;
    let current = current_for_review(
        &root,
        &record.incoming.path,
        indexed,
        &config::identity(home)?.0,
    )?;
    let local_preview = current
        .as_ref()
        .map(|e| preview(&root, &e.path, e, false))
        .unwrap_or_else(|| {
            "No current indexed version. Refresh after a scan before resolving.".into()
        });
    let incoming_preview = match (&record.incoming.kind, &record.payload) {
        (model::Kind::File, Some(path)) if path == &format!(".ysync/conflicts/{id}") => {
            preview(&root, path, &record.incoming, true)
        }
        (model::Kind::File, _) => "Incoming archive unavailable; record retained.".into(),
        _ => describe_non_file(&record.incoming),
    };
    Ok(Review {
        record,
        current,
        local_preview,
        incoming_preview,
    })
}
// Observe only the selected path. Predict refresh's clock for an unindexed manual edit
// without changing SQLite; confirmation recomputes it under the daemon lock.
fn current_for_review(
    root: &engine::Root,
    path: &str,
    old: Option<model::Entry>,
    device: &str,
) -> Result<Option<model::Entry>> {
    model::validate_path(path)?;
    if let Ok(meta) = root.dir.symlink_metadata(path)
        && meta.is_file()
        && meta.len() > 16 * 1024 * 1024
        && old.as_ref().is_none_or(|e| e.stamp != engine::stamp(&meta))
    {
        bail!(
            "selected file changed and exceeds the 16 MiB review hash limit; let the scanner index it before reviewing"
        );
    }
    let observed = engine::observe(root, path, old.as_ref(), false)?;
    let mut current = match observed {
        Some(e) => e,
        None => match &old {
            Some(e) if e.kind != model::Kind::Deleted => {
                let mut e = e.clone();
                e.kind = model::Kind::Deleted;
                e.hash.clear();
                e.size = 0;
                e.target = None;
                e.stamp.clear();
                e
            }
            _ => return Ok(old),
        },
    };
    if old.as_ref().is_none_or(|e| !e.same_content(&current)) {
        let n = current.clock.entry(device.into()).or_default();
        *n = n.checked_add(1).context("version counter exhausted")?;
    }
    Ok(Some(current))
}
fn describe_non_file(e: &model::Entry) -> String {
    match e.kind {
        model::Kind::Deleted => "Deleted / absent in this version.".into(),
        model::Kind::Directory => "Directory; descendants are reviewed separately.".into(),
        model::Kind::Symlink => format!(
            "Link target: {}",
            e.target.as_deref().unwrap_or("(missing)")
        ),
        model::Kind::File => String::new(),
    }
}
fn preview(root: &engine::Root, path: &str, e: &model::Entry, archive: bool) -> String {
    if e.kind != model::Kind::File {
        return describe_non_file(e);
    }
    let result = (|| -> Result<String> {
        if !archive {
            model::validate_path(path)?;
        }
        ensure!(
            e.size <= 64 * 1024,
            "preview omitted: file exceeds 64 KiB; compare it externally"
        );
        let meta = root.dir.symlink_metadata(path)?;
        ensure!(
            meta.is_file() && !meta.is_symlink(),
            "preview path is not a regular file"
        );
        let f = root.dir.open(path)?;
        if !archive {
            ensure!(
                engine::stamp(&f.metadata()?) == e.stamp,
                "working file changed since indexing; refresh after a scan"
            );
        }
        let mut data = Vec::new();
        use std::io::Read;
        f.take(64 * 1024 + 1).read_to_end(&mut data)?;
        ensure!(
            data.len() as u64 == e.size && blake3::hash(&data).to_hex().as_str() == e.hash,
            "preview differs from indexed version; refresh after a scan"
        );
        let text = String::from_utf8(data).context("binary content; compare externally")?;
        ensure!(!text.contains('\0'), "binary content; compare externally");
        Ok(if text.is_empty() {
            "(empty file)".into()
        } else {
            text
        })
    })();
    result.unwrap_or_else(|e| format!("Preview unavailable: {e}"))
}

#[cfg(test)]
mod review_tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, engine::Root, String) {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let device = config::initialize(state.path(), None, None).unwrap();
        engine::add_folder(state.path(), "code", files.path(), false).unwrap();
        let root = engine::Root::open(
            config::load(state.path()).unwrap().folders.remove(0),
            Arc::new(Mutex::new(())),
        )
        .unwrap();
        (state, files, root, device)
    }
    fn add(
        c: &Connection,
        root: &engine::Root,
        device: &str,
        path: &str,
        local: &[u8],
        incoming: &[u8],
    ) -> Conflict {
        root.dir.write(path, local).unwrap();
        engine::refresh(c, root, path, device, 0, true).unwrap();
        let local = store::get(c, "code", path).unwrap().unwrap();
        let mut remote = local.clone();
        remote.hash = blake3::hash(incoming).to_hex().to_string();
        remote.size = incoming.len() as u64;
        remote.clock = [("b".repeat(64), 1)].into();
        root.dir.write(".ysync/review-temp", incoming).unwrap();
        save(c, root, &local, &remote, Some(".ysync/review-temp")).unwrap();
        list(c)
            .unwrap()
            .into_iter()
            .find(|r| r.incoming.path == path)
            .unwrap()
    }
    #[test]
    fn review_and_confirm_guard_stale_content_and_preserve_unrelated_records() {
        let (state, files, root, device) = fixture();
        let c = store::open(state.path()).unwrap();
        let record = add(&c, &root, &device, "one.txt", b"local\n", b"incoming\n");
        let other = add(
            &c,
            &root,
            &device,
            "two.txt",
            b"other local",
            b"other incoming",
        );
        let before = store::get(&c, "code", "one.txt").unwrap().unwrap();
        let r = review(state.path(), "code", &record.id).unwrap();
        assert_eq!(r.local_preview, "local\n");
        assert_eq!(r.incoming_preview, "incoming\n");
        assert_eq!(
            store::get(&c, "code", "one.txt").unwrap().unwrap().seq,
            before.seq
        );
        assert_eq!(list(&c).unwrap().len(), 2);
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(state.path().join("daemon.lock"))
            .unwrap();
        fs2::FileExt::lock_exclusive(&lock).unwrap();
        assert!(
            keep_local_reviewed(
                state.path(),
                "code",
                &record.id,
                r.current.as_ref().unwrap()
            )
            .unwrap_err()
            .to_string()
            .contains("stop the ysync daemon")
        );
        drop(lock);
        fs::write(files.path().join("one.txt"), b"manual merge\n").unwrap();
        assert!(
            keep_local_reviewed(
                state.path(),
                "code",
                &record.id,
                r.current.as_ref().unwrap()
            )
            .unwrap_err()
            .to_string()
            .contains("changed since review")
        );
        assert_eq!(
            store::get(&c, "code", "one.txt").unwrap().unwrap().seq,
            before.seq,
            "rejected review must roll back index changes"
        );
        // A manually merged file can be reviewed and resolved while the daemon stays stopped.
        let r = review(state.path(), "code", &record.id).unwrap();
        keep_local_reviewed(
            state.path(),
            "code",
            &record.id,
            r.current.as_ref().unwrap(),
        )
        .unwrap();
        assert_eq!(
            fs::read(files.path().join("one.txt")).unwrap(),
            b"manual merge\n"
        );
        assert_eq!(
            fs::read(files.path().join(record.payload.unwrap())).unwrap(),
            b"incoming\n"
        );
        let pending = list(&c).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, other.id);
    }
    #[test]
    fn changed_clocks_and_recreated_deletions_reject_reviewed_decisions() {
        let (state, files, root, device) = fixture();
        let c = store::open(state.path()).unwrap();
        let record = add(&c, &root, &device, "file", b"local", b"incoming");
        let reviewed = review(state.path(), "code", &record.id)
            .unwrap()
            .current
            .unwrap();
        let mut changed = store::get(&c, "code", "file").unwrap().unwrap();
        changed.clock.insert("e".repeat(64), 1);
        store::put(&c, "code", &mut changed, 0).unwrap();
        assert!(keep_local_reviewed(state.path(), "code", &record.id, &reviewed).is_err());
        fs::remove_file(files.path().join("file")).unwrap();
        let deleted = review(state.path(), "code", &record.id)
            .unwrap()
            .current
            .unwrap();
        assert_eq!(deleted.kind, model::Kind::Deleted);
        fs::write(files.path().join("file"), b"recreated").unwrap();
        assert!(keep_local_reviewed(state.path(), "code", &record.id, &deleted).is_err());
        assert_eq!(fs::read(files.path().join("file")).unwrap(), b"recreated");
        assert_eq!(list(&c).unwrap().len(), 1);
    }

    #[test]
    fn preview_bounds_binary_corruption_and_symlink_archives() {
        let (state, files, root, device) = fixture();
        let c = store::open(state.path()).unwrap();
        let binary = add(&c, &root, &device, "binary", b"\0binary", b"\0incoming");
        assert!(
            review(state.path(), "code", &binary.id)
                .unwrap()
                .incoming_preview
                .contains("binary content")
        );
        let large = add(
            &c,
            &root,
            &device,
            "large",
            &vec![b'x'; 65537],
            &vec![b'y'; 65537],
        );
        assert!(
            review(state.path(), "code", &large.id)
                .unwrap()
                .local_preview
                .contains("64 KiB")
        );
        let regular = add(&c, &root, &device, "text", b"local", b"incoming");
        let archive = files.path().join(regular.payload.as_ref().unwrap());
        fs::write(&archive, b"corrupt").unwrap();
        assert!(
            review(state.path(), "code", &regular.id)
                .unwrap()
                .incoming_preview
                .contains("differs from indexed")
        );
        fs::remove_file(&archive).unwrap();
        std::os::unix::fs::symlink(files.path().join("text"), &archive).unwrap();
        assert!(
            review(state.path(), "code", &regular.id)
                .unwrap()
                .incoming_preview
                .contains("not a regular file")
        );
        assert_eq!(fs::read(files.path().join("text")).unwrap(), b"local");
    }
    #[test]
    fn pages_are_bounded_filtered_and_do_not_create_a_missing_index() {
        let empty = tempfile::tempdir().unwrap();
        assert!(page(empty.path(), None, "", 0).is_err());
        assert!(!empty.path().join("index.sqlite").exists());
        let (state, _files, root, device) = fixture();
        let c = store::open(state.path()).unwrap();
        let template = add(&c, &root, &device, "initial", b"l", b"r");
        for i in 0..60 {
            let mut record = template.clone();
            record.id = format!("{i:064x}");
            record.incoming.path = format!("file-{i:02}");
            c.execute(
                "INSERT INTO conflicts(folder,id,path,data) VALUES('code',?1,?2,?3)",
                params![
                    record.id,
                    record.incoming.path,
                    serde_json::to_string(&record).unwrap()
                ],
            )
            .unwrap();
        }
        let first = page(state.path(), Some("code"), "", 0).unwrap();
        assert_eq!(first.records.len(), 50);
        assert!(first.more);
        let second = page(state.path(), Some("code"), "", 50).unwrap();
        assert_eq!(second.records.len(), 11);
        assert!(!second.more);
        assert_eq!(
            page(state.path(), None, "file-42", 0)
                .unwrap()
                .records
                .len(),
            1
        );
        assert!(
            page(state.path(), Some("missing"), "", 0)
                .unwrap()
                .records
                .is_empty()
        );
    }
}
