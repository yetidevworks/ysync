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
    let _lock = stopped(home, id)?;
    let configured = config::load(home)?
        .folders
        .into_iter()
        .find(|f| f.id == folder)
        .context("unknown folder")?;
    let root = engine::Root::open(configured, Arc::new(Mutex::new(())))?;
    let mut c = store::open(home)?;
    let record = load(&c, folder, id)?;
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

/// The user explicitly replaces the local version with this incoming one and returns
/// where the replaced local version was archived. A receive-only folder never sent its
/// local edit, so the edit's version counter is dropped and later incoming versions apply
/// normally again. Elsewhere the result is a new local version that propagates.
/// Stop the daemon first.
pub fn take_incoming(home: &Path, folder: &str, id: &str) -> Result<Option<String>> {
    let _lock = stopped(home, id)?;
    let configured = config::load(home)?
        .folders
        .into_iter()
        .find(|f| f.id == folder)
        .context("unknown folder")?;
    let receive_only = configured.mode == config::FolderMode::ReceiveOnly;
    let root = engine::Root::open(configured, Arc::new(Mutex::new(())))?;
    let mut c = store::open(home)?;
    let record = load(&c, folder, id)?;
    let path = record.incoming.path.clone();
    ensure!(
        !root.excluded(&path),
        "path is now ignored; review the conflict before changing exclusions"
    );
    let others = for_path(&c, folder, &path)?;
    let newest = others.iter().fold(&record, |newest, r| {
        if model::relation(&newest.incoming.clock, &r.incoming.clock) == model::Relation::Before {
            r
        } else {
            newest
        }
    });
    ensure!(
        newest.id == record.id,
        "a newer incoming version of {path} is pending; take {} instead",
        newest.id
    );
    let temp = match record.incoming.kind {
        model::Kind::File => Some(copy_payload(&root, &record)?),
        _ => None,
    };
    let result = (|| {
        let device = config::identity(home)?.0;
        let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        engine::refresh(&tx, &root, &path, &device, 0, true)?;
        let local = store::get(&tx, folder, &path)?;
        let mut next = record.incoming.clone();
        let mut base = local.as_ref().map(|e| e.clock.clone()).unwrap_or_default();
        if receive_only {
            base.remove(&device);
        }
        next.clock = model::merge(&base, &record.incoming.clock);
        if !receive_only {
            let n = next.clock.entry(device).or_default();
            *n = n.checked_add(1).context("version counter exhausted")?;
        }
        let (stamp, archived) =
            engine::place(&root, &record.incoming, local.as_ref(), temp.as_deref())?;
        let recorded = (|| -> Result<()> {
            if matches!(
                record.incoming.kind,
                model::Kind::File | model::Kind::Directory
            ) {
                root.dir.open(&path)?.into_std().sync_all()?;
            }
            engine::sync_directories(&root, std::slice::from_ref(&record.incoming))?;
            next.stamp = stamp;
            store::put(&tx, folder, &mut next, 0)?;
            store::forget_received(&tx, folder, &path)?;
            // Older records for this path held versions the result includes, against a
            // local version it replaced.
            let includes = |a: &model::Entry, b: &model::Entry| {
                matches!(
                    model::relation(&a.clock, &b.clock),
                    model::Relation::Before | model::Relation::Equal
                )
            };
            for r in &others {
                if r.id == record.id
                    || (includes(&r.incoming, &next)
                        && local.as_ref().is_some_and(|l| includes(&r.local, l)))
                {
                    tx.execute(
                        "DELETE FROM conflicts WHERE folder=?1 AND id=?2",
                        params![folder, r.id],
                    )?;
                }
            }
            tx.commit()?;
            Ok(())
        })();
        // Running the command again indexes the placed file and records the resolution.
        recorded.with_context(|| {
            let kept = archived.as_deref().map_or(String::new(), |a| {
                format!("; the replaced local version is at {a}")
            });
            format!(
                "the incoming version is in place but the resolution was not recorded{kept}. Run this command again before starting the daemon"
            )
        })?;
        Ok(archived)
    })();
    if let Some(temp) = temp {
        // Already gone once published.
        let _ = root.dir.remove_file(temp);
    }
    result
}

fn stopped(home: &Path, id: &str) -> Result<fs::File> {
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
    Ok(lock)
}
fn load(c: &Connection, folder: &str, id: &str) -> Result<Conflict> {
    Ok(serde_json::from_str(
        &c.query_row(
            "SELECT data FROM conflicts WHERE folder=?1 AND id=?2",
            params![folder, id],
            |r| r.get::<_, String>(0),
        )
        .context("unknown or already resolved conflict")?,
    )?)
}
fn for_path(c: &Connection, folder: &str, path: &str) -> Result<Vec<Conflict>> {
    let mut q = c.prepare("SELECT data FROM conflicts WHERE folder=?1 AND path=?2")?;
    q.query_map(params![folder, path], |r| r.get::<_, String>(0))?
        .map(|r| Ok(serde_json::from_str(&r?)?))
        .collect()
}
/// Copy the preserved incoming file to a private temp file, verified against its record.
/// The archive itself stays in place until retention removes it.
fn copy_payload(root: &engine::Root, record: &Conflict) -> Result<String> {
    use std::io::{Read, Write};
    let archive = format!(".ysync/conflicts/{}", record.id);
    ensure!(
        record.payload.as_deref() == Some(archive.as_str()),
        "incoming archive unavailable; the record cannot be taken"
    );
    let meta = root.dir.symlink_metadata(&archive)?;
    ensure!(
        meta.is_file() && !meta.is_symlink(),
        "incoming archive is not a regular file"
    );
    let mut source = root.dir.open(&archive)?;
    let (temp, mut file) = engine::temp_file(root)?;
    let copied = (|| {
        let mut hash = blake3::Hasher::new();
        let mut bytes = 0u64;
        let mut buf = vec![0u8; 262144];
        loop {
            let n = source.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hash.update(&buf[..n]);
            file.write_all(&buf[..n])?;
            bytes += n as u64;
        }
        ensure!(
            bytes == record.incoming.size
                && hash.finalize().to_hex().as_str() == record.incoming.hash,
            "incoming archive differs from its record"
        );
        file.set_permissions(fs::Permissions::from_mode(record.incoming.mode))?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(e) = copied {
        let _ = root.dir.remove_file(&temp);
        return Err(e);
    }
    Ok(temp)
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
    /// Apply a peer's version of `db`; true when it was recorded as a conflict.
    fn incoming(c: &Connection, root: &engine::Root, device: &str, data: &[u8], n: u64) -> bool {
        let (temp, mut f) = engine::temp_file(root).unwrap();
        std::io::Write::write_all(&mut f, data).unwrap();
        f.set_permissions(fs::Permissions::from_mode(0o644))
            .unwrap();
        let e = model::Entry {
            path: "db".into(),
            kind: model::Kind::File,
            size: data.len() as u64,
            hash: blake3::hash(data).to_hex().to_string(),
            target: None,
            mode: 0o644,
            clock: [("b".repeat(64), n)].into(),
            seq: 0,
            stamp: String::new(),
        };
        engine::apply(c, root, device, &e, Some(&temp)).unwrap()
    }
    #[test]
    fn taking_the_newest_incoming_version_ends_repeated_receive_only_conflicts() {
        let (state, files, root, device) = fixture();
        config::edit(state.path(), |c| {
            c.folders[0].mode = config::FolderMode::ReceiveOnly;
            Ok(())
        })
        .unwrap();
        let c = store::open(state.path()).unwrap();
        assert!(!incoming(&c, &root, &device, b"one", 1));
        // A local edit this folder never sends conflicts with every later version.
        fs::write(files.path().join("db"), b"local").unwrap();
        engine::refresh(&c, &root, "db", &device, 0, false).unwrap();
        assert!(incoming(&c, &root, &device, b"two", 2));
        assert!(incoming(&c, &root, &device, b"three", 3));
        let records = list(&c).unwrap();
        let id = |data: &[u8]| {
            let hash = blake3::hash(data).to_hex().to_string();
            records
                .iter()
                .find(|r| r.incoming.hash == hash)
                .unwrap()
                .id
                .clone()
        };
        let older = take_incoming(state.path(), "code", &id(b"two"))
            .unwrap_err()
            .to_string();
        assert!(older.contains(&id(b"three")), "{older}");
        assert_eq!(fs::read(files.path().join("db")).unwrap(), b"local");
        let archived = take_incoming(state.path(), "code", &id(b"three"))
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(files.path().join("db")).unwrap(), b"three");
        assert_eq!(fs::read(files.path().join(archived)).unwrap(), b"local");
        assert!(list(&c).unwrap().is_empty());
        let indexed = store::get(&c, "code", "db").unwrap().unwrap();
        assert_eq!(indexed.clock, [("b".repeat(64), 3)].into());
        assert_eq!(indexed.mode, 0o644);
        assert!(!incoming(&c, &root, &device, b"four", 4));
        assert_eq!(fs::read(files.path().join("db")).unwrap(), b"four");
        assert!(list(&c).unwrap().is_empty());
    }
    fn receive_only_conflict(
        deleted: bool,
    ) -> (tempfile::TempDir, tempfile::TempDir, Connection, String) {
        let (state, files, root, device) = fixture();
        config::edit(state.path(), |c| {
            c.folders[0].mode = config::FolderMode::ReceiveOnly;
            Ok(())
        })
        .unwrap();
        let c = store::open(state.path()).unwrap();
        assert!(!incoming(&c, &root, &device, b"one", 1));
        fs::write(files.path().join("db"), b"local").unwrap();
        engine::refresh(&c, &root, "db", &device, 0, false).unwrap();
        if deleted {
            let mut gone = store::get(&c, "code", "db").unwrap().unwrap();
            gone.kind = model::Kind::Deleted;
            gone.hash.clear();
            gone.size = 0;
            gone.clock = [("b".repeat(64), 2)].into();
            assert!(engine::apply(&c, &root, &device, &gone, None).unwrap());
        } else {
            assert!(incoming(&c, &root, &device, b"two", 2));
        }
        (state, files, c, device)
    }
    #[test]
    fn taking_incoming_again_after_an_unrecorded_attempt_finishes_it() {
        let (state, files, c, device) = receive_only_conflict(false);
        let id = list(&c).unwrap()[0].id.clone();
        c.execute_batch(
            "CREATE TRIGGER stop BEFORE DELETE ON conflicts BEGIN SELECT RAISE(ABORT,'stopped'); END;",
        )
        .unwrap();
        let error = format!(
            "{:#}",
            take_incoming(state.path(), "code", &id).unwrap_err()
        );
        assert!(error.contains("Run this command again"), "{error}");
        assert_eq!(fs::read(files.path().join("db")).unwrap(), b"two");
        assert!(
            store::get(&c, "code", "db")
                .unwrap()
                .unwrap()
                .clock
                .contains_key(&device)
        );
        c.execute_batch("DROP TRIGGER stop").unwrap();
        assert!(take_incoming(state.path(), "code", &id).unwrap().is_none());
        let indexed = store::get(&c, "code", "db").unwrap().unwrap();
        assert_eq!(indexed.clock, [("b".repeat(64), 2)].into());
        assert_eq!(indexed.hash, blake3::hash(b"two").to_hex().to_string());
        assert!(list(&c).unwrap().is_empty());
        let kept = fs::read_dir(files.path().join(".ysync/versions"))
            .unwrap()
            .filter_map(|e| fs::read(e.unwrap().path()).ok())
            .any(|data| data == b"local");
        assert!(kept, "the first attempt's archive must remain");
    }
    #[test]
    fn taking_an_incoming_deletion_archives_the_local_file() {
        let (state, files, c, device) = receive_only_conflict(true);
        let id = list(&c).unwrap()[0].id.clone();
        let archived = take_incoming(state.path(), "code", &id).unwrap().unwrap();
        assert!(!files.path().join("db").exists());
        assert_eq!(fs::read(files.path().join(archived)).unwrap(), b"local");
        let indexed = store::get(&c, "code", "db").unwrap().unwrap();
        assert_eq!(indexed.kind, model::Kind::Deleted);
        assert!(!indexed.clock.contains_key(&device));
        assert!(list(&c).unwrap().is_empty());
    }
    #[test]
    fn taking_incoming_on_a_two_way_folder_is_a_new_local_version() {
        let (state, files, root, device) = fixture();
        let c = store::open(state.path()).unwrap();
        assert!(!incoming(&c, &root, &device, b"one", 1));
        fs::write(files.path().join("db"), b"local").unwrap();
        engine::refresh(&c, &root, "db", &device, 0, false).unwrap();
        assert!(incoming(&c, &root, &device, b"two", 2));
        let id = list(&c).unwrap()[0].id.clone();
        let lock = fs::File::create(state.path().join("daemon.lock")).unwrap();
        fs2::FileExt::lock_exclusive(&lock).unwrap();
        assert!(
            take_incoming(state.path(), "code", &id)
                .unwrap_err()
                .to_string()
                .contains("stop the ysync daemon")
        );
        drop(lock);
        let archive = files.path().join(".ysync/conflicts").join(&id);
        fs::rename(&archive, files.path().join("moved")).unwrap();
        assert!(take_incoming(state.path(), "code", &id).is_err());
        assert_eq!(fs::read(files.path().join("db")).unwrap(), b"local");
        fs::rename(files.path().join("moved"), &archive).unwrap();
        take_incoming(state.path(), "code", &id).unwrap();
        assert_eq!(fs::read(files.path().join("db")).unwrap(), b"two");
        let indexed = store::get(&c, "code", "db").unwrap().unwrap();
        assert_eq!(
            indexed.clock,
            [("b".repeat(64), 2), (device.clone(), 2)].into()
        );
        assert!(list(&c).unwrap().is_empty());
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
