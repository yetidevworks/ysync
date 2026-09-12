use crate::{
    config::Folder,
    model::{self, Entry, Kind, Relation},
    store,
};
use anyhow::{Context, Result, bail};
use cap_std::fs::{Dir, Metadata, MetadataExt, OpenOptions};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rayon::prelude::*;
use rusqlite::Connection;
use std::os::unix::fs::PermissionsExt;
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Root {
    pub folder: Folder,
    pub dir: Dir,
    pub gate: Arc<Mutex<()>>,
    ignores: GlobSet,
    pub hashed_files: AtomicU64,
    pub hashed_bytes: AtomicU64,
    pub scan_control: Option<Arc<crate::scanning::ScanControl>>,
    pub read_cache: Option<Arc<crate::read_cache::ReadCache>>,
}
impl Root {
    pub fn open(folder: Folder, gate: Arc<Mutex<()>>) -> Result<Self> {
        let dir = Dir::open_ambient_dir(&folder.path, cap_std::ambient_authority())?;
        if dir.read_to_string(".ysync/marker")?.trim() != folder.marker {
            bail!("folder marker mismatch: {}", folder.id);
        }
        let mut builder = GlobSetBuilder::new();
        let mut patterns = folder.ignores.clone();
        if let Ok(s) = dir.read_to_string(".ysyncignore") {
            patterns.extend(
                s.lines()
                    .map(str::trim)
                    .filter(|s| !s.is_empty() && !s.starts_with('#'))
                    .map(str::to_owned),
            );
        }
        for p in patterns {
            builder.add(Glob::new(&p)?);
            if !p.contains('/') {
                builder.add(Glob::new(&format!("**/{p}"))?);
            }
        }
        Ok(Self {
            folder,
            dir,
            gate,
            ignores: builder.build()?,
            hashed_files: AtomicU64::new(0),
            hashed_bytes: AtomicU64::new(0),
            scan_control: None,
            read_cache: None,
        })
    }
    fn scan_checkpoint(&self) -> Result<()> {
        if let Some(control) = &self.scan_control {
            control.checkpoint()?;
        }
        Ok(())
    }
    pub fn excluded(&self, path: &str) -> bool {
        let mut prefix = String::new();
        for c in path.split('/') {
            if c.eq_ignore_ascii_case(".ysync") || c.eq_ignore_ascii_case(".ysyncignore") {
                return true;
            }
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(c);
            if self.ignores.is_match(&prefix) {
                return true;
            }
        }
        false
    }
    pub fn check(&self) -> Result<()> {
        // Check both the live mount path and our directory handle before inferring deletions.
        let actual = std::fs::read_to_string(self.folder.path.join(".ysync/marker"))?;
        if actual.trim() != self.folder.marker
            || self.dir.read_to_string(".ysync/marker")?.trim() != self.folder.marker
        {
            bail!("folder unavailable or replaced");
        }
        Ok(())
    }
    pub fn parents(&self, path: &str, create: bool) -> Result<()> {
        self.parent_dir(path, create).map(|_| ())
    }
    fn parent_dir(&self, path: &str, create: bool) -> Result<Option<Dir>> {
        model::validate_path(path)?;
        let mut p = PathBuf::new();
        let mut opened: Option<Dir> = None;
        let components: Vec<_> = Path::new(path).components().collect();
        for part in &components[..components.len() - 1] {
            p.push(part);
            let current = opened.as_ref().unwrap_or(&self.dir);
            let meta = match current.symlink_metadata(part) {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && create => {
                    current.create_dir(part)?;
                    current.symlink_metadata(part)?
                }
                Err(e) => return Err(e.into()),
            };
            if !meta.is_dir() || meta.is_symlink() {
                bail!("path parent is not a real directory: {}", p.display());
            }
            let next = current.open_dir(part)?;
            let actual = next.dir_metadata()?;
            if (meta.dev(), meta.ino()) != (actual.dev(), actual.ino()) {
                bail!("path parent changed while opening: {}", p.display());
            }
            opened = Some(next);
        }
        Ok(opened)
    }
}
pub fn stamp(m: &Metadata) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec()
    )
}
#[cfg(test)]
fn hash_reader(reader: &mut impl Read, root: &Root) -> Result<(String, u64)> {
    let (hash, bytes, _) = hash_collect(reader, root, None)?;
    Ok((hash, bytes))
}
fn hash_collect(
    reader: &mut impl Read,
    root: &Root,
    limit: Option<usize>,
) -> Result<(String, u64, Option<Vec<u8>>)> {
    let mut h = blake3::Hasher::new();
    let mut bytes = 0;
    let mut buf = [0u8; 262144];
    let mut captured = limit.map(Vec::with_capacity);
    loop {
        root.scan_checkpoint()?;
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        bytes += n as u64;
        if let Some(data) = &mut captured {
            if data.len() + n <= limit.unwrap() {
                data.extend_from_slice(&buf[..n]);
            } else {
                captured = None;
            }
        }
    }
    Ok((h.finalize().to_hex().to_string(), bytes, captured))
}
pub fn observe(root: &Root, path: &str, old: Option<&Entry>, force: bool) -> Result<Option<Entry>> {
    root.scan_checkpoint()?;
    model::validate_path(path)?;
    let parent_handle = match root.parent_dir(path, false) {
        Ok(dir) => dir,
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let parent = parent_handle.as_ref().unwrap_or(&root.dir);
    let name = Path::new(path).file_name().context("missing filename")?;
    let meta = match parent.symlink_metadata(name) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let st = stamp(&meta);
    let mode = meta.mode() & 0o777;
    if !force && old.is_some_and(|e| e.kind != Kind::Deleted && e.stamp == st && e.mode == mode) {
        return Ok(old.cloned());
    }
    let (kind, size, hash, target) = if meta.is_symlink() {
        let t = parent
            .read_link_contents(name)?
            .into_os_string()
            .into_string()
            .map_err(|_| anyhow::anyhow!("non-UTF-8 link target"))?;
        (
            Kind::Symlink,
            0,
            blake3::hash(t.as_bytes()).to_hex().to_string(),
            Some(t),
        )
    } else if meta.is_dir() {
        (Kind::Directory, 0, String::new(), None)
    } else if meta.is_file() {
        let mut f = parent.open(name)?;
        let before = stamp(&f.metadata()?);
        if before != st {
            bail!("file changed before hashing: {path}");
        }
        let capture = root
            .read_cache
            .as_ref()
            .filter(|c| c.allows(meta.len()))
            .map(|_| meta.len() as usize);
        let (hash, bytes, captured) = hash_collect(&mut f, root, capture)?;
        root.hashed_files.fetch_add(1, Ordering::Relaxed);
        root.hashed_bytes.fetch_add(bytes, Ordering::Relaxed);
        if stamp(&f.metadata()?) != before || stamp(&root.dir.symlink_metadata(path)?) != before {
            bail!("file changed during hashing: {path}");
        }
        if let (Some(cache), Some(bytes)) = (&root.read_cache, captured) {
            cache.insert(&root.folder.id, path, &st, &hash, bytes);
        }
        (Kind::File, meta.len(), hash, None)
    } else {
        bail!("unsupported special file: {path}");
    };
    Ok(Some(Entry {
        path: path.into(),
        kind,
        size,
        hash,
        target,
        mode,
        clock: old.map(|e| e.clock.clone()).unwrap_or_default(),
        seq: 0,
        stamp: st,
    }))
}
pub(crate) fn refresh(
    c: &Connection,
    root: &Root,
    path: &str,
    device: &str,
    seen: i64,
    force: bool,
) -> Result<()> {
    if root.excluded(path) {
        return Ok(());
    }
    let old = store::get(c, &root.folder.id, path)?;
    let observed = observe(root, path, old.as_ref(), force)?;
    record_observation(c, root, path, old, observed, device, seen)
}
fn record_observation(
    c: &Connection,
    root: &Root,
    path: &str,
    old: Option<Entry>,
    observed: Option<Entry>,
    device: &str,
    seen: i64,
) -> Result<()> {
    let mut fresh = match observed {
        Some(e) => e,
        None => match old.as_ref() {
            Some(e) if e.kind != Kind::Deleted => {
                let mut x = e.clone();
                x.kind = Kind::Deleted;
                x.hash.clear();
                x.size = 0;
                x.target = None;
                x.stamp.clear();
                x
            }
            _ => return Ok(()),
        },
    };
    if old.as_ref().is_some_and(|e| e.same_content(&fresh)) {
        store::mark_seen(c, &root.folder.id, path, &fresh.stamp, seen)?;
    } else {
        *fresh.clock.entry(device.into()).or_default() += 1;
        store::put(c, &root.folder.id, &mut fresh, seen)?;
    }
    Ok(())
}
fn scan_batch(
    c: &mut Connection,
    root: &Root,
    paths: &[String],
    device: &str,
    seen: i64,
    force: bool,
) -> Result<Vec<String>> {
    let inputs = paths
        .iter()
        .filter(|p| !root.excluded(p))
        .map(|p| Ok((p.clone(), store::get(c, &root.folder.id, p)?)))
        .collect::<Result<Vec<_>>>()?;
    // Hash in parallel outside the SQLite write transaction. The caller's folder gate
    // protects the indexed baseline; other folders can commit while these reads run.
    let observations = inputs
        .par_iter()
        .map(|(p, old)| observe(root, p, old.as_ref(), force))
        .collect::<Vec<_>>();
    if observations.iter().any(|r| {
        r.as_ref()
            .is_err_and(|e| e.is::<crate::scanning::ScanCancelled>())
    }) {
        return Err(crate::scanning::ScanCancelled.into());
    }
    root.scan_checkpoint()?;
    root.check()?;
    let mut errors = Vec::new();
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    for ((path, old), observed) in inputs.into_iter().zip(observations) {
        match observed {
            Ok(None) if old.as_ref().is_some_and(|e| e.kind == Kind::Directory) => {
                // A recursive removal can race enumeration. Journal its tombstone in
                // the deepest-first unseen pass, after every child deletion.
            }
            Ok(observed) => record_observation(&tx, root, &path, old, observed, device, seen)?,
            Err(e) => errors.push(format!("{e:#} [path: {path}]")),
        }
    }
    tx.commit()?;
    Ok(errors)
}
pub fn scan(
    root: &Root,
    home: &Path,
    device: &str,
    subpaths: Option<Vec<String>>,
    progress: impl FnMut(u64),
) -> Result<u64> {
    scan_with_directories(root, home, device, subpaths, progress, |_, _| {})
}

pub fn scan_with_directories(
    root: &Root,
    home: &Path,
    device: &str,
    subpaths: Option<Vec<String>>,
    progress: impl FnMut(u64),
    on_directory: impl FnMut(&str, &Metadata),
) -> Result<u64> {
    scan_impl(root, home, device, subpaths, true, progress, on_directory)
}

pub fn scan_entries(
    root: &Root,
    home: &Path,
    device: &str,
    paths: Vec<String>,
    progress: impl FnMut(u64),
) -> Result<u64> {
    scan_impl(root, home, device, Some(paths), false, progress, |_, _| {})
}

fn scan_impl(
    root: &Root,
    home: &Path,
    device: &str,
    subpaths: Option<Vec<String>>,
    recursive: bool,
    mut progress: impl FnMut(u64),
    mut on_directory: impl FnMut(&str, &Metadata),
) -> Result<u64> {
    root.scan_checkpoint()?;
    root.check()?;
    let mut c = store::open(home)?;
    let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64;
    let full = subpaths.is_none();
    let regions = subpaths.clone().unwrap_or_default();
    let mut stack = subpaths.unwrap_or_else(|| vec![String::new()]);
    let mut batch = Vec::new();
    let mut count = 0;
    let mut errors = Vec::new();
    while let Some(path) = stack.pop() {
        root.scan_checkpoint()?;
        if root.excluded(&path) {
            continue;
        }
        let m = if path.is_empty() {
            Some(root.dir.dir_metadata()?)
        } else {
            match root.dir.symlink_metadata(&path) {
                Ok(m) => Some(m),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    if errors.len() < 32 {
                        errors.push(format!("{e} [path: {path}]"));
                    }
                    None
                }
            }
        };
        if recursive && m.as_ref().is_some_and(|m| m.is_dir() && !m.is_symlink()) {
            on_directory(&path, m.as_ref().unwrap());
            let children = (|| -> Result<Vec<String>> {
                let dir = if path.is_empty() {
                    root.dir.try_clone()?
                } else {
                    let parent_handle = root.parent_dir(&path, false)?;
                    let parent = parent_handle.as_ref().unwrap_or(&root.dir);
                    let dir = parent.open_dir(Path::new(&path).file_name().unwrap())?;
                    let actual = dir.dir_metadata()?;
                    let expected = m.as_ref().unwrap();
                    if (actual.dev(), actual.ino()) != (expected.dev(), expected.ino()) {
                        bail!("directory changed while opening");
                    }
                    dir
                };
                let mut children = Vec::new();
                for e in dir.entries()? {
                    root.scan_checkpoint()?;
                    let e = e?;
                    let name = match e.file_name().into_string() {
                        Ok(n) => n,
                        Err(_) => {
                            if errors.len() < 32 {
                                errors.push(format!("non-UTF-8 filename in {path}"));
                            }
                            continue;
                        }
                    };
                    let child = if path.is_empty() {
                        name
                    } else {
                        format!("{path}/{name}")
                    };
                    if !root.excluded(&child) {
                        children.push(child);
                    }
                }
                Ok(children)
            })();
            match children {
                Ok(paths) => stack.extend(paths),
                Err(e) if e.is::<crate::scanning::ScanCancelled>() => return Err(e),
                Err(e) => {
                    if errors.len() < 32 {
                        errors.push(format!("{e:#} [path: {path}]"));
                    }
                }
            }
        }
        if !path.is_empty() {
            batch.push(path);
        }
        if batch.len() >= 128 || stack.is_empty() {
            let _guard = root.gate.lock().unwrap();
            root.check()?;
            let batch_errors = scan_batch(&mut c, root, &batch, device, epoch, false)?;
            errors.extend(
                batch_errors
                    .into_iter()
                    .take(32usize.saturating_sub(errors.len())),
            );
            count += batch.len() as u64;
            batch.clear();
            progress(count);
        }
    }
    if !errors.is_empty() {
        bail!(
            "scan incomplete; accessible files were indexed, deletion inference suspended: {}",
            errors.join("; ")
        );
    }
    {
        let reconcile: Vec<Option<&str>> = if full {
            vec![None]
        } else {
            regions
                .iter()
                .filter(|path| {
                    recursive
                        || root
                            .dir
                            .symlink_metadata(path)
                            .err()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
                })
                .map(|s| Some(s.as_str()))
                .collect()
        };
        for region in reconcile {
            loop {
                let _guard = root.gate.lock().unwrap();
                root.check()?;
                root.scan_checkpoint()?;
                let absent = match region {
                    None => store::unseen(&c, &root.folder.id, epoch, 128)?,
                    Some(path) => store::unseen_under(&c, &root.folder.id, path, epoch, 128)?,
                };
                if absent.is_empty() {
                    break;
                }
                // Observing can wait for cooling or hash a changed file. Never do
                // either while holding the database's shared write transaction.
                let mut observations = Vec::with_capacity(absent.len());
                for e in absent {
                    root.scan_checkpoint()?;
                    let observed = if root.excluded(&e.path) {
                        None
                    } else {
                        Some(observe(root, &e.path, Some(&e), false)?)
                    };
                    observations.push((e, observed));
                }
                root.scan_checkpoint()?;
                root.check()?;
                let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let batch_count = observations.len() as u64;
                for (e, observed) in observations {
                    match observed {
                        None => store::mark_seen(&tx, &root.folder.id, &e.path, &e.stamp, epoch)?,
                        Some(observed) => {
                            let path = e.path.clone();
                            record_observation(&tx, root, &path, Some(e), observed, device, epoch)?;
                        }
                    }
                }
                tx.commit()?;
                count += batch_count;
                progress(count);
            }
        }
    }
    Ok(count)
}

/// Preserve the local spelling of canonically equivalent Unicode paths.
/// Case-only aliases and distinct physical files remain collisions.
pub fn resolve_incoming_paths(
    c: &Connection,
    root: &Root,
    entries: &[Entry],
) -> Result<Vec<Entry>> {
    use unicode_normalization::UnicodeNormalization;
    let mut prefixes = std::collections::BTreeSet::new();
    let mut path_keys = std::collections::HashSet::new();
    let mut prefix_bytes = 0usize;
    for entry in entries {
        entry.validate()?;
        if root.excluded(&entry.path) {
            continue;
        }
        if !path_keys.insert(model::path_key(&entry.path)) {
            bail!("batch contains colliding paths");
        }
        for prefix in Path::new(&entry.path)
            .ancestors()
            .filter_map(Path::to_str)
            .filter(|s| !s.is_empty())
        {
            let key = model::path_key(prefix);
            if !prefixes.contains(&key) {
                prefix_bytes += key.len();
                if prefix_bytes > 4 * 1024 * 1024 {
                    bail!("path prefix metadata too large");
                }
                prefixes.insert(key);
            }
        }
    }
    if prefixes.is_empty() {
        return Ok(entries.to_vec());
    }
    let keys: Vec<_> = prefixes.iter().collect();
    let mut query = c.prepare_cached("SELECT path_key,path FROM entries WHERE folder=?1 AND path_key IN (SELECT value FROM json_each(?2))")?;
    let aliases = query
        .query_map(
            rusqlite::params![root.folder.id, serde_json::to_string(&keys)?],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )?
        .collect::<rusqlite::Result<std::collections::HashMap<_, _>>>()?;
    let metadata = |path: &str| -> Result<Option<Metadata>> {
        let result = root
            .parents(path, false)
            .and_then(|()| Ok(root.dir.symlink_metadata(path)?));
        match result {
            Ok(m) => Ok(Some(m)),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    };
    let mut checked = std::collections::HashSet::new();
    entries
        .iter()
        .map(|entry| {
            let mut resolved = entry.clone();
            if root.excluded(&entry.path) {
                return Ok(resolved);
            }
            for prefix in Path::new(&entry.path)
                .ancestors()
                .filter_map(Path::to_str)
                .filter(|s| !s.is_empty())
            {
                let Some(local) = aliases.get(&model::path_key(prefix)) else {
                    continue;
                };
                if local == prefix {
                    break;
                }
                if !local.nfc().eq(prefix.nfc()) {
                    bail!("case collision: {local} and {prefix}");
                }
                if checked.insert((local.clone(), prefix.to_owned())) {
                    match (metadata(local)?, metadata(prefix)?) {
                        (Some(a), Some(b)) if a.dev() != b.dev() || a.ino() != b.ino() => bail!(
                            "distinct files have canonically equivalent names: {local} and {prefix}"
                        ),
                        (None, Some(_)) => bail!(
                            "local Unicode spelling changed; rescan required: {local} and {prefix}"
                        ),
                        _ => {}
                    }
                }
                resolved.path = format!("{local}{}", &entry.path[prefix.len()..]);
                break;
            }
            Ok(resolved)
        })
        .collect()
}

pub fn wants(c: &Connection, root: &Root, remote: &Entry) -> Result<bool> {
    remote.validate()?;
    if root.excluded(&remote.path) {
        return Ok(false);
    }
    let old = store::get(c, &root.folder.id, &remote.path)?;
    if remote.kind != Kind::File {
        return Ok(false);
    }
    if old.as_ref().is_some_and(|local| {
        matches!(
            model::relation(&local.clock, &remote.clock),
            Relation::After
        ) || (model::relation(&local.clock, &remote.clock) == Relation::Equal
            && local.same_content(remote))
    }) {
        return Ok(false);
    }
    // The destination may have changed before its watcher ran. Recheck its
    // metadata/cache before negotiation so a retry can fetch a missing payload.
    let live = observe(root, &remote.path, old.as_ref(), false)?;
    Ok(live.is_none_or(|local| !local.same_bytes(remote)))
}
fn archive(root: &Root, path: &str, area: &str, remove: bool) -> Result<Option<String>> {
    let m = match root.dir.symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let dest = format!(".ysync/{area}/{}", uuid::Uuid::new_v4());
    root.dir.create_dir_all(format!(".ysync/{area}"))?;
    if m.is_dir() && !m.is_symlink() {
        bail!("directory replacement requires manual resolution: {path}");
    }
    // Link before replacement so the final rename can publish atomically without a missing-path gap.
    if remove {
        root.dir.rename(path, &root.dir, &dest)?;
    } else {
        root.dir.hard_link(path, &root.dir, &dest)?;
    }
    if m.is_file() {
        root.dir.open(&dest)?.into_std().sync_all()?;
    }
    write_metadata(
        root,
        &format!("{dest}.json"),
        &serde_json::json!({"path":path,"saved_at":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()}),
    )?;
    Ok(Some(dest))
}
fn write_metadata(root: &Root, path: &str, value: &impl serde::Serialize) -> Result<()> {
    root.dir.write(path, serde_json::to_vec(value)?)?;
    root.dir.open(path)?.into_std().sync_all()?;
    Ok(())
}
fn check_destination(root: &Root, path: &str, local: Option<&Entry>) -> Result<()> {
    let expected = local
        .filter(|e| e.kind != Kind::Deleted)
        .map(|e| e.stamp.as_str());
    let current = match root.dir.symlink_metadata(path) {
        Ok(meta) => Some(stamp(&meta)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    if current.as_deref() != expected {
        bail!("destination changed before publication; retry required: {path}");
    }
    Ok(())
}

fn publish_new_file(root: &Root, temp: &str, path: &str) -> Result<()> {
    // Atomic creation without replacement. An editor may create the path after
    // the last check; a replacing rename would silently overwrite that save.
    root.dir.hard_link(temp, &root.dir, path)?;
    root.dir.remove_file(temp)?;
    Ok(())
}

pub fn apply(
    c: &Connection,
    root: &Root,
    device: &str,
    remote: &Entry,
    temp: Option<&str>,
) -> Result<bool> {
    remote.validate()?;
    root.check()?;
    if root.excluded(&remote.path) {
        return Ok(false);
    }
    // A replay or echo cannot change this path. Leave local watcher processing to the scanner.
    if store::get(c, &root.folder.id, &remote.path)?.is_some_and(|e| {
        model::relation(&e.clock, &remote.clock) == Relation::After
            || (model::relation(&e.clock, &remote.clock) == Relation::Equal
                && e.same_content(remote))
    }) {
        return Ok(false);
    }
    // Re-read local content immediately before applying: an external editor may have beaten its watcher.
    refresh(c, root, &remote.path, device, 0, true)?;
    let old = store::get(c, &root.folder.id, &remote.path)?;
    let rel = old
        .as_ref()
        .map(|e| model::relation(&e.clock, &remote.clock));
    if matches!(rel, Some(Relation::After))
        || (matches!(rel, Some(Relation::Equal))
            && old.as_ref().is_some_and(|e| e.same_content(remote)))
    {
        return Ok(false);
    }
    if matches!(rel, Some(Relation::Concurrent | Relation::Equal))
        && old.as_ref().is_some_and(|e| !e.same_content(remote))
    {
        crate::conflicts::save(c, root, old.as_ref().unwrap(), remote, temp)?;
        // Do not merge clocks: that would falsely claim different working
        // contents are synchronized and make a later stale copy look newer.
        return Ok(true);
    }
    let mut next = remote.clone();
    if let Some(old) = &old {
        next.clock = model::merge(&old.clock, &remote.clock);
    }
    check_destination(root, &remote.path, old.as_ref())?;
    if old.as_ref().is_some_and(|e| e.same_bytes(remote)) {
        // Matching content still needs its clocks merged, but chmod (even to the
        // existing mode) changes ctime and emits native metadata notifications.
        // During bootstrap those no-op writes can overflow the watcher queue
        // and repeatedly trigger full scans of an otherwise unchanged tree.
        if matches!(remote.kind, Kind::File | Kind::Directory)
            && old.as_ref().is_some_and(|e| e.mode != remote.mode)
        {
            root.dir.set_permissions(
                &remote.path,
                cap_std::fs::Permissions::from_std(std::fs::Permissions::from_mode(remote.mode)),
            )?;
            root.dir.open(&remote.path)?.into_std().sync_all()?;
        }
    } else if !old.as_ref().is_some_and(|e| e.same_content(remote)) {
        // A tombstone may arrive at a fresh receiver that never had the file.
        // Creating its parents invents directories, which the watcher can then
        // journal as independent local creations and conflict with later deletes.
        if remote.kind != Kind::Deleted {
            root.parents(&remote.path, true)?;
        }
        match remote.kind {
            Kind::Deleted => {
                if let Some(e) = &old {
                    if e.kind == Kind::Directory {
                        match root.dir.remove_dir(&remote.path) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => {
                                return Err(e)
                                    .context("directory is not empty; reconcile children first");
                            }
                        }
                    } else {
                        archive(root, &remote.path, "versions", true)?;
                    }
                }
            }
            Kind::Directory => {
                if root
                    .dir
                    .symlink_metadata(&remote.path)
                    .is_ok_and(|m| !m.is_dir() || m.is_symlink())
                {
                    archive(root, &remote.path, "versions", true)?;
                }
                root.dir.create_dir_all(&remote.path)?;
            }
            Kind::Symlink => {
                let temp_link = format!(".ysync/tmp/{}", uuid::Uuid::new_v4());
                root.dir.symlink_contents(
                    remote.target.as_ref().context("missing symlink target")?,
                    &temp_link,
                )?;
                archive(root, &remote.path, "versions", false)?;
                root.dir.rename(&temp_link, &root.dir, &remote.path)?;
            }
            Kind::File => {
                let t = temp.context("file changed during negotiation; retry required")?;
                if old.as_ref().is_none_or(|e| e.kind == Kind::Deleted) {
                    publish_new_file(root, t, &remote.path)?;
                } else {
                    archive(root, &remote.path, "versions", false)?;
                    root.dir.rename(t, &root.dir, &remote.path)?;
                }
            }
        }
        if remote.kind == Kind::Directory {
            root.dir.set_permissions(
                &remote.path,
                cap_std::fs::Permissions::from_std(std::fs::Permissions::from_mode(remote.mode)),
            )?;
        }
    }
    if next.kind != Kind::Deleted {
        next.stamp = stamp(&root.dir.symlink_metadata(&next.path)?);
    } else {
        next.stamp.clear();
    }
    store::put(c, &root.folder.id, &mut next, 0)?;
    crate::conflicts::clear_resolved(c, &root.folder.id, &next)?;
    Ok(false)
}

/// Persist rename/create/delete metadata once per affected directory, before committing the batch cursor.
pub fn sync_directories(root: &Root, entries: &[Entry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut dirs = std::collections::HashSet::<PathBuf>::new();
    for p in [
        "",
        ".ysync",
        ".ysync/tmp",
        ".ysync/versions",
        ".ysync/conflicts",
    ] {
        dirs.insert(p.into());
    }
    for e in entries {
        if root.excluded(&e.path) {
            continue;
        }
        let path = Path::new(&e.path);
        if e.kind == Kind::Directory {
            dirs.insert(path.into());
        }
        for parent in path.ancestors().skip(1) {
            dirs.insert(parent.into());
        }
    }
    let mut dirs: Vec<_> = dirs.into_iter().collect();
    dirs.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    // Siblings can flush together, but every deeper level must be durable
    // before its parents. Sharing an in-flight filesystem commit avoids
    // serial disk waits on trees with many small files and directories.
    let mut remaining = dirs.as_slice();
    while let Some(first) = remaining.first() {
        let depth = first.components().count();
        let count = remaining.partition_point(|p| p.components().count() == depth);
        let (level, rest) = remaining.split_at(count);
        crate::durability::parallel(level, |p| {
            root.scan_checkpoint()?;
            match root.dir.open_dir(p) {
                // cap-std uses O_PATH directory handles on Linux; fsync needs a readable descriptor.
                Ok(dir) => dir
                    .open(".")?
                    .into_std()
                    .sync_all()
                    .with_context(|| format!("flushing directory {}", p.display()))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            Ok(())
        })?;
        remaining = rest;
    }
    Ok(())
}

pub fn temp_file(root: &Root) -> Result<(String, std::fs::File)> {
    root.dir.create_dir_all(".ysync/tmp")?;
    let p = format!(".ysync/tmp/{}", uuid::Uuid::new_v4());
    let f = root
        .dir
        .open_with(&p, OpenOptions::new().write(true).create_new(true))?
        .into_std();
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok((p, f))
}

pub fn add_folder(home: &Path, id: &str, path: &Path, dev: bool) -> Result<()> {
    add_folder_with_mode(home, id, path, dev, Default::default())
}

pub fn add_folder_with_mode(
    home: &Path,
    id: &str,
    path: &Path,
    dev: bool,
    mode: crate::config::FolderMode,
) -> Result<()> {
    crate::config::valid_folder_id(id)?;
    let path = std::fs::canonicalize(path).context("folder must already exist")?;
    if !path.is_dir() {
        bail!("folder must be a directory");
    }
    let home = std::fs::canonicalize(home)?;
    if home.starts_with(&path) || path.starts_with(&home) {
        bail!("sync folder and daemon state must not overlap");
    }
    crate::config::edit(&home, |c| {
        if c.folders
            .iter()
            .any(|f| f.id == id || f.path.starts_with(&path) || path.starts_with(&f.path))
        {
            bail!("duplicate or overlapping folder");
        }
        let dir = Dir::open_ambient_dir(&path, cap_std::ambient_authority())?;
        if dir.symlink_metadata(".ysync").is_ok_and(|m| m.is_symlink()) {
            bail!(".ysync cannot be a symlink");
        }
        dir.create_dir_all(".ysync/tmp")?;
        let marker = if let Ok(m) = dir.read_to_string(".ysync/marker") {
            m.trim().to_string()
        } else {
            let m = uuid::Uuid::new_v4().to_string();
            dir.write(".ysync/marker", &m)?;
            m
        };
        c.folders.push(Folder {
            id: id.into(),
            path,
            marker,
            paused: false,
            mode,
            ignores: if dev {
                crate::config::dev_ignores()
            } else {
                vec![]
            },
        });
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use std::collections::BTreeMap;
    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, Root, String) {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let id = config::initialize(state.path(), None, None).unwrap();
        add_folder(state.path(), "test", files.path(), false).unwrap();
        let root = Root::open(
            config::load(state.path()).unwrap().folders.remove(0),
            Arc::new(Mutex::new(())),
        )
        .unwrap();
        (state, files, root, id)
    }
    #[test]
    fn absent_tombstone_does_not_create_parent_directories() {
        let (state, files, root, id) = fixture();
        let c = store::open(state.path()).unwrap();
        let remote = Entry {
            path: "retired/objects/00/old-object".into(),
            kind: Kind::Deleted,
            size: 0,
            hash: String::new(),
            target: None,
            mode: 0,
            clock: BTreeMap::from([("a".repeat(64), 2)]),
            seq: 1,
            stamp: String::new(),
        };
        assert!(!apply(&c, &root, &id, &remote, None).unwrap());
        sync_directories(&root, std::slice::from_ref(&remote)).unwrap();
        assert!(!files.path().join("retired").exists());
        let stored = store::get(&c, "test", &remote.path).unwrap().unwrap();
        assert_eq!(stored.kind, Kind::Deleted);
        assert_eq!(stored.clock, remote.clock);
        assert!(crate::conflicts::list(&c).unwrap().is_empty());
    }

    #[test]
    fn cooling_preserves_partial_hash_and_stop_interrupts_next_read() {
        use std::{
            sync::mpsc,
            thread,
            time::{Duration, Instant},
        };
        struct Reader {
            data: std::io::Cursor<Vec<u8>>,
            control: Arc<crate::scanning::ScanControl>,
            first: Option<mpsc::Sender<()>>,
        }
        impl Read for Reader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = self.data.read(buf)?;
                if let Some(tx) = self.first.take() {
                    self.control.update(false, true);
                    tx.send(()).unwrap();
                }
                Ok(n)
            }
        }
        for stop in [false, true] {
            let (_state, _files, mut root, _id) = fixture();
            let control = Arc::new(crate::scanning::ScanControl::default());
            root.scan_control = Some(control.clone());
            let data: Vec<u8> = (0..800_000).map(|i| (i % 251) as u8).collect();
            let expected = blake3::hash(&data).to_hex().to_string();
            let (tx, rx) = mpsc::channel();
            let mut reader = Reader {
                data: std::io::Cursor::new(data),
                control: control.clone(),
                first: Some(tx),
            };
            let worker = thread::spawn(move || {
                let result = hash_reader(&mut reader, &root);
                (result, reader.data.position())
            });
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !control.waiting() {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
            if stop {
                control.stop();
            } else {
                control.update(false, false);
            }
            let (result, position) = worker.join().unwrap();
            if stop {
                assert!(result.unwrap_err().is::<crate::scanning::ScanCancelled>());
                assert_eq!(position, 262144);
            } else {
                assert_eq!(result.unwrap(), (expected, 800_000));
                assert_eq!(position, 800_000);
            }
        }
    }
    #[test]
    fn cooling_retains_walk_progress_and_releases_database_writer() {
        use std::{
            sync::mpsc,
            thread,
            time::{Duration, Instant},
        };
        let (state, files, mut root, id) = fixture();
        for n in 0..300 {
            std::fs::write(files.path().join(format!("file-{n}")), b"data").unwrap();
        }
        let control = Arc::new(crate::scanning::ScanControl::default());
        root.scan_control = Some(control.clone());
        let home = state.path().to_owned();
        let (tx, rx) = mpsc::channel();
        let c = control.clone();
        let worker = thread::spawn(move || {
            let visits = std::cell::Cell::new(0);
            let count = scan_with_directories(
                &root,
                &home,
                &id,
                None,
                |count| {
                    if count == 128 {
                        c.update(false, true);
                        tx.send(count).unwrap();
                    }
                },
                |_, _| visits.set(visits.get() + 1),
            )
            .unwrap();
            (
                count,
                visits.get(),
                root.hashed_files.load(Ordering::Relaxed),
            )
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 128);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !control.waiting() {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let mut db = store::open(state.path()).unwrap();
        db.busy_timeout(Duration::from_millis(100)).unwrap();
        db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap()
            .commit()
            .unwrap();
        control.update(false, false);
        assert_eq!(worker.join().unwrap(), (300, 1, 300));
    }
    #[test]
    fn cancelling_partial_scan_does_not_infer_deletions() {
        let (state, files, mut root, id) = fixture();
        std::fs::write(files.path().join("removed"), b"keep baseline").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        std::fs::remove_file(files.path().join("removed")).unwrap();
        for n in 0..300 {
            std::fs::write(files.path().join(format!("file-{n}")), b"data").unwrap();
        }
        let control = Arc::new(crate::scanning::ScanControl::default());
        root.scan_control = Some(control.clone());
        let error = scan(&root, state.path(), &id, None, |_| {
            control.update(true, false);
        })
        .unwrap_err();
        assert!(error.is::<crate::scanning::ScanCancelled>());
        let db = store::open(state.path()).unwrap();
        assert_eq!(
            store::get(&db, "test", "removed").unwrap().unwrap().kind,
            Kind::File
        );
        control.update(false, false);
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        assert_eq!(
            store::get(&db, "test", "removed").unwrap().unwrap().kind,
            Kind::Deleted
        );
    }
    #[test]
    fn detects_deletes_without_recreating_versions_on_rescan() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("x"), b"hello").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let a = store::get(&c, "test", "x").unwrap().unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        assert_eq!(a.seq, store::get(&c, "test", "x").unwrap().unwrap().seq);
        std::fs::remove_file(files.path().join("x")).unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        assert_eq!(
            store::get(&c, "test", "x").unwrap().unwrap().kind,
            Kind::Deleted
        );
    }
    #[test]
    fn identical_peer_entries_merge_clocks_without_filesystem_writes() {
        let (state, files, root, id) = fixture();
        std::fs::create_dir(files.path().join("dir")).unwrap();
        std::fs::write(files.path().join("dir/file"), b"identical content").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        for path in ["dir", "dir/file"] {
            let local = store::get(&c, "test", path).unwrap().unwrap();
            let before = stamp(&root.dir.symlink_metadata(path).unwrap());
            let mut remote = local.clone();
            remote.clock = BTreeMap::from([("a".repeat(64), 1)]);
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(!apply(&c, &root, &id, &remote, None).unwrap());
            assert_eq!(stamp(&root.dir.symlink_metadata(path).unwrap()), before);
            let merged = store::get(&c, "test", path).unwrap().unwrap();
            assert_eq!(merged.clock, model::merge(&local.clock, &remote.clock));
            let mut c = store::open(state.path()).unwrap();
            scan_batch(&mut c, &root, &[path.into()], &id, 0, false).unwrap();
            assert_eq!(
                store::get(&c, "test", path).unwrap().unwrap().seq,
                merged.seq
            );
        }
    }

    #[test]
    fn symlink_mode_differences_do_not_conflict_or_rewrite_links() {
        let (state, files, root, id) = fixture();
        std::os::unix::fs::symlink("target", files.path().join("link")).unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let original = store::get(&c, "test", "link").unwrap().unwrap();
        let before = stamp(&root.dir.symlink_metadata("link").unwrap());
        let mut incoming = original.clone();
        incoming.mode = if original.mode == 0o777 { 0o755 } else { 0o777 };
        incoming.clock = BTreeMap::from([("a".repeat(64), 1)]);
        assert!(!apply(&c, &root, &id, &incoming, None).unwrap());
        let merged = store::get(&c, "test", "link").unwrap().unwrap();
        assert_eq!(merged.clock, model::merge(&original.clock, &incoming.clock));
        assert_eq!(stamp(&root.dir.symlink_metadata("link").unwrap()), before);
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let rescanned = store::get(&c, "test", "link").unwrap().unwrap();
        assert_eq!(rescanned.seq, merged.seq);
        assert!(crate::conflicts::list(&c).unwrap().is_empty());

        // A different link target is still a real concurrent conflict.
        incoming.target = Some("different-target".into());
        incoming.hash = blake3::hash(b"different-target").to_hex().to_string();
        incoming.clock = BTreeMap::from([("b".repeat(64), 1)]);
        assert!(apply(&c, &root, &id, &incoming, None).unwrap());
        assert_eq!(
            std::fs::read_link(files.path().join("link")).unwrap(),
            Path::new("target")
        );
        assert_eq!(stamp(&root.dir.symlink_metadata("link").unwrap()), before);
        assert_eq!(crate::conflicts::list(&c).unwrap().len(), 1);
    }

    #[test]
    fn permission_changes_still_apply_when_bytes_match() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"same bytes").unwrap();
        std::fs::set_permissions(
            files.path().join("file"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let mut remote = store::get(&c, "test", "file").unwrap().unwrap();
        remote.clock.insert("a".repeat(64), 1);
        remote.mode = 0o755;
        assert!(!apply(&c, &root, &id, &remote, None).unwrap());
        assert_eq!(root.dir.metadata("file").unwrap().mode() & 0o777, 0o755);
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"same bytes"
        );
    }

    #[test]
    fn marker_loss_never_becomes_mass_delete() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("x"), b"hello").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        std::fs::remove_file(files.path().join(".ysync/marker")).unwrap();
        assert!(scan(&root, state.path(), &id, None, |_| {}).is_err());
    }

    fn staged_remote(
        root: &Root,
        base: &Entry,
        bytes: &[u8],
        independent: bool,
    ) -> (Entry, String) {
        use std::io::Write;
        let mut remote = base.clone();
        remote.kind = Kind::File;
        remote.size = bytes.len() as u64;
        remote.hash = blake3::hash(bytes).to_hex().to_string();
        if independent {
            remote.clock.clear();
        }
        remote.clock.insert("a".repeat(64), 1);
        let (name, mut file) = temp_file(root).unwrap();
        file.write_all(bytes).unwrap();
        file.set_permissions(std::fs::Permissions::from_mode(remote.mode))
            .unwrap();
        file.sync_all().unwrap();
        (remote, name)
    }

    #[test]
    fn unicode_equivalent_names_preserve_local_spelling_and_conflicts() {
        use unicode_normalization::UnicodeNormalization;
        let (state, files, root, id) = fixture();
        let local_path = "cafe\u{301}/uzbekista\u{301}n.svg";
        std::fs::create_dir(files.path().join("cafe\u{301}")).unwrap();
        std::fs::write(files.path().join(local_path), b"working").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let old = store::get(&c, "test", local_path).unwrap().unwrap();
        let mut incoming = old.clone();
        incoming.path = local_path.nfc().collect();
        incoming.clock = [("a".repeat(64), 1)].into();
        let resolved = resolve_incoming_paths(&c, &root, &[incoming.clone()])
            .unwrap()
            .remove(0);
        assert_eq!(resolved.path, local_path);
        assert!(!wants(&c, &root, &resolved).unwrap());
        assert!(!apply(&c, &root, &id, &resolved, None).unwrap());
        let merged = store::get(&c, "test", local_path).unwrap().unwrap();
        assert_eq!(merged.clock.len(), 2);
        let (mut remote, temp) = staged_remote(&root, &merged, b"later edit", false);
        remote.path = incoming.path.clone();
        remote.clock.insert("a".repeat(64), 2);
        let remote = resolve_incoming_paths(&c, &root, &[remote])
            .unwrap()
            .remove(0);
        assert!(!apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join(local_path)).unwrap(),
            b"later edit"
        );
        let (mut remote, temp) = staged_remote(&root, &merged, b"independent branch", true);
        remote.path = incoming.path;
        remote.clock = [("b".repeat(64), 1)].into();
        let remote = resolve_incoming_paths(&c, &root, &[remote])
            .unwrap()
            .remove(0);
        assert!(apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join(local_path)).unwrap(),
            b"later edit"
        );
        assert_eq!(
            crate::conflicts::list(&c).unwrap()[0].incoming.path,
            local_path
        );
        let mut child = old.clone();
        child.path = "caf\u{e9}/new.txt".into();
        assert_eq!(
            resolve_incoming_paths(&c, &root, &[child]).unwrap()[0].path,
            "cafe\u{301}/new.txt"
        );
        let mut case = old.clone();
        case.path = "Cafe\u{301}/uzbekista\u{301}n.svg".into();
        assert!(resolve_incoming_paths(&c, &root, &[case]).is_err());
    }

    #[test]
    fn incoming_alias_checks_keep_ignored_batch_positions_and_reject_duplicates() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"data").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let mut entry = store::get(&c, "test", "file").unwrap().unwrap();
        entry.path = "cafe\u{301}".into();
        let mut other = entry.clone();
        other.path = "caf\u{e9}".into();
        assert!(resolve_incoming_paths(&c, &root, &[entry, other]).is_err());
        let mut ignored_root = root;
        ignored_root.folder.ignores = vec!["ignored".into()];
        let ignored_root = Root::open(ignored_root.folder, ignored_root.gate).unwrap();
        let mut ignored = store::get(&c, "test", "file").unwrap().unwrap();
        ignored.path = "ignored/file".into();
        let resolved = resolve_incoming_paths(&c, &ignored_root, &[ignored.clone()]).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].path, ignored.path);
        assert!(!wants(&c, &ignored_root, &resolved[0]).unwrap());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn distinct_unicode_files_are_not_merged() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("cafe\u{301}"), b"first").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let mut remote = store::get(&c, "test", "cafe\u{301}").unwrap().unwrap();
        remote.path = "caf\u{e9}".into();
        std::fs::write(files.path().join(&remote.path), b"second").unwrap();
        assert!(
            resolve_incoming_paths(&c, &root, &[remote])
                .unwrap_err()
                .to_string()
                .contains("distinct files")
        );
        assert_eq!(
            std::fs::read(files.path().join("cafe\u{301}")).unwrap(),
            b"first"
        );
        assert_eq!(
            std::fs::read(files.path().join("caf\u{e9}")).unwrap(),
            b"second"
        );
    }

    #[test]
    fn bootstrap_never_replaces_an_unindexed_existing_file() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"new Mac work").unwrap();
        let local = observe(&root, "file", None, true).unwrap().unwrap();
        let (remote, temp) = staged_remote(&root, &local, b"old Linux snapshot", true);
        let c = store::open(state.path()).unwrap();
        assert!(wants(&c, &root, &remote).unwrap());
        assert!(apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"new Mac work"
        );
        let after = store::get(&c, "test", "file").unwrap().unwrap();
        assert_eq!(
            model::relation(&after.clock, &remote.clock),
            Relation::Concurrent
        );
        let pending = crate::conflicts::list(&c).unwrap();
        assert_eq!(pending.len(), 1);
        let saved = files.path().join(pending[0].payload.as_ref().unwrap());
        assert_eq!(std::fs::read(&saved).unwrap(), b"old Linux snapshot");
        drop(c);
        let c = store::open(state.path()).unwrap();
        assert!(apply(&c, &root, &id, &remote, None).unwrap());
        assert_eq!(
            crate::conflicts::list(&c).unwrap().len(),
            1,
            "replays deduplicate"
        );
        std::fs::write(files.path().join("file"), b"manual merge").unwrap();
        crate::conflicts::keep_local(state.path(), "test", &pending[0].id).unwrap();
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"manual merge"
        );
        assert_eq!(std::fs::read(saved).unwrap(), b"old Linux snapshot");
        let resolved = store::get(&c, "test", "file").unwrap().unwrap();
        assert_eq!(
            model::relation(&resolved.clock, &remote.clock),
            Relation::After
        );
        assert!(crate::conflicts::list(&c).unwrap().is_empty());
    }

    #[test]
    fn stale_causal_version_cannot_replace_newer_local_work() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"base").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let stale = store::get(&c, "test", "file").unwrap().unwrap();
        std::fs::write(files.path().join("file"), b"newer").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        assert!(!wants(&c, &root, &stale).unwrap());
        assert!(!apply(&c, &root, &id, &stale, None).unwrap());
        assert_eq!(std::fs::read(files.path().join("file")).unwrap(), b"newer");
    }

    #[test]
    fn edit_during_transfer_becomes_a_conflict_instead_of_being_overwritten() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"base").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let base = store::get(&c, "test", "file").unwrap().unwrap();
        let (remote, temp) = staged_remote(&root, &base, b"remote edit", false);
        assert!(wants(&c, &root, &remote).unwrap());
        // Simulate a save after negotiation, before publication, without a scanner pass.
        std::fs::write(files.path().join("file"), b"new local edit during transfer").unwrap();
        assert!(apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"new local edit during transfer"
        );
        let current = store::get(&c, "test", "file").unwrap().unwrap();
        assert_eq!(
            model::relation(&current.clock, &remote.clock),
            Relation::Concurrent
        );
        assert_eq!(crate::conflicts::list(&c).unwrap().len(), 1);
    }

    #[test]
    fn changed_destination_after_no_payload_negotiation_retries_without_losing_either_version() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"base").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let mut c = store::open(state.path()).unwrap();
        let base = store::get(&c, "test", "file").unwrap().unwrap();
        let (remote, temp) = staged_remote(&root, &base, b"base", false);
        assert!(!wants(&c, &root, &remote).unwrap());
        std::fs::write(files.path().join("file"), b"late edit").unwrap();
        let tx = c.transaction().unwrap();
        assert!(apply(&tx, &root, &id, &remote, None).is_err());
        tx.rollback().unwrap();
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"late edit"
        );
        assert!(
            wants(&c, &root, &remote).unwrap(),
            "retry must request the missing incoming version"
        );
        assert!(apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"late edit"
        );
        assert_eq!(crate::conflicts::list(&c).unwrap().len(), 1);
    }

    #[test]
    fn remote_delete_cannot_remove_an_unscanned_local_edit() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"base").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let mut remote = store::get(&c, "test", "file").unwrap().unwrap();
        remote.clock.insert("a".repeat(64), 1);
        remote.kind = Kind::Deleted;
        remote.hash.clear();
        remote.size = 0;
        std::fs::write(files.path().join("file"), b"new local work").unwrap();
        assert!(apply(&c, &root, &id, &remote, None).unwrap());
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"new local work"
        );
        assert_eq!(
            crate::conflicts::list(&c).unwrap()[0].incoming.kind,
            Kind::Deleted
        );
    }

    #[test]
    fn equal_clocks_with_different_bytes_are_not_treated_as_synchronized() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"keep local").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let local = store::get(&c, "test", "file").unwrap().unwrap();
        let (mut remote, temp) = staged_remote(&root, &local, b"legacy divergence", false);
        remote.clock = local.clock.clone();
        assert!(wants(&c, &root, &remote).unwrap());
        assert!(apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"keep local"
        );
        assert_eq!(crate::conflicts::list(&c).unwrap().len(), 1);
    }

    #[test]
    fn new_file_publication_does_not_replace_a_path_created_after_the_check() {
        use std::io::Write;
        let (_state, files, root, _id) = fixture();
        let (temp, mut file) = temp_file(&root).unwrap();
        file.write_all(b"incoming").unwrap();
        check_destination(&root, "file", None).unwrap();
        std::fs::write(files.path().join("file"), b"new editor save").unwrap();
        assert!(publish_new_file(&root, &temp, "file").is_err());
        assert_eq!(
            std::fs::read(files.path().join("file")).unwrap(),
            b"new editor save"
        );
        assert_eq!(std::fs::read(files.path().join(temp)).unwrap(), b"incoming");
    }
    #[test]
    fn conflicting_permissions_never_chmod_the_working_file_or_link_the_archive() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("file"), b"same bytes").unwrap();
        std::fs::set_permissions(
            files.path().join("file"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let mut remote = store::get(&c, "test", "file").unwrap().unwrap();
        remote.clock = BTreeMap::from([("a".repeat(64), 1)]);
        remote.mode = 0;
        assert!(!wants(&c, &root, &remote).unwrap());
        assert!(apply(&c, &root, &id, &remote, None).unwrap());
        assert_eq!(root.dir.metadata("file").unwrap().mode() & 0o777, 0o644);
        let pending = crate::conflicts::list(&c).unwrap();
        std::fs::write(files.path().join("file"), b"later edit").unwrap();
        assert_eq!(
            std::fs::read(files.path().join(pending[0].payload.as_ref().unwrap())).unwrap(),
            b"same bytes"
        );
    }

    #[test]
    fn independent_type_change_does_not_replace_a_populated_directory() {
        let (state, files, root, id) = fixture();
        std::fs::create_dir(files.path().join("dir")).unwrap();
        std::fs::write(files.path().join("dir/child"), b"keep child").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let c = store::open(state.path()).unwrap();
        let local = store::get(&c, "test", "dir").unwrap().unwrap();
        let (remote, temp) = staged_remote(&root, &local, b"old file at this path", true);
        assert!(apply(&c, &root, &id, &remote, Some(&temp)).unwrap());
        assert_eq!(
            std::fs::read(files.path().join("dir/child")).unwrap(),
            b"keep child"
        );
        assert_eq!(crate::conflicts::list(&c).unwrap().len(), 1);
    }

    #[test]
    fn cached_files_still_reject_symlink_parents_and_detect_replacement() {
        let (_state, files, root, _id) = fixture();
        std::fs::create_dir_all(files.path().join("safe/nested")).unwrap();
        std::fs::write(files.path().join("safe/nested/file"), b"original").unwrap();
        let old = observe(&root, "safe/nested/file", None, false)
            .unwrap()
            .unwrap();
        std::fs::rename(files.path().join("safe"), files.path().join("former")).unwrap();
        assert!(
            observe(&root, "safe/nested/file", Some(&old), false)
                .unwrap()
                .is_none()
        );
        std::os::unix::fs::symlink("former", files.path().join("safe")).unwrap();
        assert!(observe(&root, "safe/nested/file", Some(&old), false).is_err());
        std::fs::remove_file(files.path().join("safe")).unwrap();
        std::fs::create_dir_all(files.path().join("safe/nested")).unwrap();
        std::fs::write(files.path().join("safe/nested/file"), b"replaced").unwrap();
        let new = observe(&root, "safe/nested/file", Some(&old), false)
            .unwrap()
            .unwrap();
        assert_ne!(new.hash, old.hash);
        assert_eq!(new.hash, blake3::hash(b"replaced").to_hex().as_str());
    }

    #[test]
    fn symlink_parents_cannot_escape_root() {
        let (_s, files, root, _id) = fixture();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), files.path().join("escape")).unwrap();
        assert!(root.parents("escape/secret", true).is_err());
        assert!(!outside.path().join("secret").exists());
    }
    #[test]
    fn unsupported_path_does_not_block_other_files_or_infer_deletions() {
        let (state, files, root, id) = fixture();
        std::fs::write(files.path().join("old"), b"keep deletion history").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        std::fs::remove_file(files.path().join("old")).unwrap();
        std::fs::write(files.path().join("good"), b"still index me").unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(files.path().join("socket")).unwrap();
        assert!(scan(&root, state.path(), &id, None, |_| {}).is_err());
        let c = store::open(state.path()).unwrap();
        assert!(store::get(&c, "test", "good").unwrap().is_some());
        assert_eq!(
            store::get(&c, "test", "old").unwrap().unwrap().kind,
            Kind::File
        );
    }
    #[test]
    fn directory_disappearing_during_scan_is_journaled_after_children() {
        let (state, files, root, id) = fixture();
        std::fs::create_dir(files.path().join("dir")).unwrap();
        std::fs::write(files.path().join("dir/child"), b"data").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        std::fs::remove_dir_all(files.path().join("dir")).unwrap();
        let mut c = store::open(state.path()).unwrap();
        scan_batch(&mut c, &root, &["dir".into()], &id, 2, false).unwrap();
        assert_eq!(
            store::get(&c, "test", "dir").unwrap().unwrap().kind,
            Kind::Directory
        );
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let dir = store::get(&c, "test", "dir").unwrap().unwrap();
        let child = store::get(&c, "test", "dir/child").unwrap().unwrap();
        assert_eq!(dir.kind, Kind::Deleted);
        assert_eq!(child.kind, Kind::Deleted);
        assert!(child.seq < dir.seq);
    }
    #[test]
    fn scoped_delete_only_visits_its_subtree_and_journals_children_first() {
        let (state, files, root, id) = fixture();
        std::fs::create_dir_all(files.path().join("gone/sub")).unwrap();
        std::fs::write(files.path().join("gone/sub/child"), b"data").unwrap();
        std::fs::write(files.path().join("gone-neighbor"), b"keep").unwrap();
        for i in 0..200 {
            std::fs::write(files.path().join(format!("unrelated-{i}")), b"same").unwrap();
        }
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let hashes = root.hashed_files.load(Ordering::Relaxed);
        let c = store::open(state.path()).unwrap();
        let old = store::get(&c, "test", "gone-neighbor").unwrap().unwrap();
        std::fs::remove_dir_all(files.path().join("gone")).unwrap();
        let count = scan(&root, state.path(), &id, Some(vec!["gone".into()]), |_| {}).unwrap();
        assert!(
            count <= 4,
            "scoped deletion checked unrelated paths: {count}"
        );
        assert_eq!(root.hashed_files.load(Ordering::Relaxed), hashes);
        let dir = store::get(&c, "test", "gone").unwrap().unwrap();
        let sub = store::get(&c, "test", "gone/sub").unwrap().unwrap();
        let child = store::get(&c, "test", "gone/sub/child").unwrap().unwrap();
        assert_eq!(dir.kind, Kind::Deleted);
        assert!(child.seq < sub.seq && sub.seq < dir.seq);
        assert_eq!(
            old.seq,
            store::get(&c, "test", "gone-neighbor")
                .unwrap()
                .unwrap()
                .seq
        );
    }
    #[test]
    fn directory_metadata_and_echo_events_do_not_rehash_unchanged_files() {
        let (state, files, root, id) = fixture();
        std::fs::create_dir(files.path().join("dir")).unwrap();
        std::fs::write(files.path().join("dir/file"), b"unchanged").unwrap();
        scan(&root, state.path(), &id, None, |_| {}).unwrap();
        let hashes = root.hashed_files.load(Ordering::Relaxed);
        assert_eq!(
            scan_entries(&root, state.path(), &id, vec!["dir".into()], |_| {}).unwrap(),
            1
        );
        scan_entries(&root, state.path(), &id, vec!["dir/file".into()], |_| {}).unwrap();
        scan(&root, state.path(), &id, Some(vec!["dir".into()]), |_| {}).unwrap();
        assert_eq!(root.hashed_files.load(Ordering::Relaxed), hashes);
    }
}
