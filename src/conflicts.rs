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
    if matches!(local.kind, model::Kind::File | model::Kind::Directory) {
        root.dir.open(&local.path)?.into_std().sync_all()?;
    }
    engine::sync_directories(&root, std::slice::from_ref(&local))?;
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
