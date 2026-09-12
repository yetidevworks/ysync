//! Explicit retention for private archives. Unresolved conflicts are never eligible.
use crate::{config, engine, pairing, store};
use anyhow::{Context, Result, ensure};
use cap_std::fs::MetadataExt;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Policy {
    pub automatic: bool,
    pub versions_days: Option<u32>,
    pub versions_max_bytes: Option<u64>,
    pub partial_days: Option<u32>,
    pub resolved_conflicts_days: Option<u32>,
}
#[derive(Debug, Clone, Serialize)]
pub struct Candidate {
    pub path: String,
    pub bytes: u64,
    pub reason: String,
    #[serde(skip)]
    stamp: String,
    #[serde(skip)]
    sidecar: Option<(String, String)>,
    #[serde(skip)]
    age: u64,
}
#[derive(Default, Debug, Serialize)]
pub struct Report {
    pub folder: String,
    pub candidates: Vec<Candidate>,
    pub eligible_bytes: u64,
    pub removed: u64,
    pub removed_bytes: u64,
    pub skipped: u64,
    pub protected_conflicts: u64,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok_and(|u| u.to_string() == s)
}
fn hex_id(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn area(root: &engine::Root, path: &str) -> Result<bool> {
    for p in [".ysync", path] {
        match root.dir.symlink_metadata(p) {
            Ok(m) => ensure!(
                m.is_dir() && !m.is_symlink(),
                "retention area must be a real directory: {p}"
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}
fn modified(m: &cap_std::fs::Metadata) -> Result<u64> {
    Ok(m.modified()?
        .into_std()
        .duration_since(UNIX_EPOCH)?
        .as_secs())
}
fn candidate(
    root: &engine::Root,
    path: String,
    reason: &str,
    age: u64,
    sidecar: Option<String>,
) -> Result<Option<Candidate>> {
    let m = root.dir.symlink_metadata(&path)?;
    // Never follow symlinks or remove unexplained special files. Hard-linked versions may still alias working files; unlinking is safe but count no reclaimed bytes for those.
    if !m.is_file() || m.is_symlink() {
        return Ok(None);
    }
    let sidecar = if let Some(p) = sidecar {
        let sm = root.dir.symlink_metadata(&p)?;
        if !sm.is_file() || sm.is_symlink() {
            return Ok(None);
        }
        Some((p, engine::stamp(&sm)))
    } else {
        None
    };
    Ok(Some(Candidate {
        path,
        bytes: if m.nlink() == 1 { m.len() } else { 0 },
        reason: reason.into(),
        stamp: engine::stamp(&m),
        sidecar,
        age,
    }))
}
fn expired(saved: u64, days: Option<u32>, at: u64) -> bool {
    days.is_some_and(|days| at.saturating_sub(saved) >= u64::from(days) * 86400 && saved <= at)
}
/// Caller holds the folder gate during automatic maintenance, or daemon lock for manual deletion.
pub fn run(home: &Path, root: &engine::Root, policy: &Policy, apply: bool) -> Result<Report> {
    root.check()?;
    let c = rusqlite::Connection::open_with_flags(
        home.join("index.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    c.busy_timeout(std::time::Duration::from_millis(250))?;
    let protected = c.query_row(
        "SELECT count(*) FROM conflicts WHERE folder=?1",
        [&root.folder.id],
        |r| r.get::<_, i64>(0),
    )?;
    let mut report = Report {
        folder: root.folder.id.clone(),
        protected_conflicts: protected as u64,
        ..Default::default()
    };
    let at = now();
    let mut versions = vec![];
    // Disabled categories incur no directory walks.
    for category in ["versions", "tmp", "conflicts"] {
        if (category == "versions"
            && policy.versions_days.is_none()
            && policy.versions_max_bytes.is_none())
            || (category == "tmp" && policy.partial_days.is_none())
            || (category == "conflicts" && policy.resolved_conflicts_days.is_none())
        {
            continue;
        }
        let dir = format!(".ysync/{category}");
        if !area(root, &dir)? {
            continue;
        }
        let mut visited = 0;
        for item in root.dir.read_dir(&dir)? {
            visited += 1;
            ensure!(
                visited <= 100_000,
                "retention directory exceeds 100,000 entries; split maintenance manually"
            );
            let item = item?;
            let Some(name) = item.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let path = format!("{dir}/{name}");
            let m = root.dir.symlink_metadata(&path)?;
            if !m.is_file() || m.is_symlink() {
                continue;
            }
            if category == "versions" && uuid(&name) {
                let side = format!("{path}.json");
                let sm = match root.dir.symlink_metadata(&side) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if !sm.is_file() || sm.is_symlink() || sm.len() > 16384 {
                    continue;
                }
                let data: serde_json::Value = match serde_json::from_slice(&root.dir.read(&side)?) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let Some(saved) = data["saved_at"].as_u64() else {
                    continue;
                };
                let Some(original) = data["path"].as_str() else {
                    continue;
                };
                if crate::model::validate_path(original).is_err() {
                    continue;
                }
                if let Some(item) = candidate(root, path, "version age", saved, Some(side))? {
                    versions.push(item);
                }
            } else if category == "tmp"
                && (name
                    .strip_prefix("resume-")
                    .and_then(|n| n.strip_suffix(".part"))
                    .is_some_and(hex_id)
                    || uuid(&name))
                && expired(modified(&m)?, policy.partial_days, at)
            {
                if let Some(item) =
                    candidate(root, path, "abandoned transfer", modified(&m)?, None)?
                {
                    report.candidates.push(item);
                }
            } else if category == "conflicts" && name.strip_suffix(".json").is_some_and(hex_id) {
                let id = name.trim_end_matches(".json");
                if c.prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM conflicts WHERE folder=?1 AND id=?2)",
                )?
                .query_row(params![root.folder.id, id], |r| r.get::<_, bool>(0))?
                {
                    continue;
                }
                if !expired(modified(&m)?, policy.resolved_conflicts_days, at) || m.len() > 65536 {
                    continue;
                }
                let record: crate::conflicts::Conflict =
                    match serde_json::from_slice(&root.dir.read(&path)?) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                let resolved =
                    store::get(&c, &root.folder.id, &record.incoming.path)?.is_some_and(|e| {
                        [&record.local, &record.incoming].iter().all(|old| {
                            matches!(
                                crate::model::relation(&old.clock, &e.clock),
                                crate::model::Relation::Before | crate::model::Relation::Equal
                            )
                        })
                    });
                if !resolved || record.id != id || record.folder != root.folder.id {
                    continue;
                }
                if let Some(payload) = record.payload {
                    if payload != format!("{dir}/{id}") {
                        continue;
                    }
                    if root.dir.symlink_metadata(&payload).is_err() {
                        continue;
                    }
                    if let Some(item) = candidate(
                        root,
                        payload,
                        "resolved conflict age",
                        modified(&m)?,
                        Some(path),
                    )? {
                        report.candidates.push(item);
                    }
                } else if let Some(item) =
                    candidate(root, path, "resolved conflict age", modified(&m)?, None)?
                {
                    report.candidates.push(item);
                }
            }
        }
    }
    versions.sort_by_key(|v| v.age);
    let mut bytes = versions.iter().map(|v| v.bytes).sum::<u64>();
    for mut v in versions {
        let age = expired(v.age, policy.versions_days, at);
        if age || policy.versions_max_bytes.is_some_and(|cap| bytes > cap) {
            bytes = bytes.saturating_sub(v.bytes);
            if !age {
                v.reason = "version space limit".into();
            }
            report.candidates.push(v);
        }
    }
    report.eligible_bytes = report.candidates.iter().map(|x| x.bytes).sum();
    if apply && !report.candidates.is_empty() {
        for item in &report.candidates {
            root.check()?;
            let category = item.path.split('/').nth(1).context("missing category")?;
            ensure!(
                area(root, &format!(".ysync/{category}"))?,
                "retention directory disappeared"
            );
            let unchanged = |p: &str, s: &str| {
                root.dir
                    .symlink_metadata(p)
                    .is_ok_and(|m| m.is_file() && !m.is_symlink() && engine::stamp(&m) == s)
            };
            if !unchanged(&item.path, &item.stamp)
                || item.sidecar.as_ref().is_some_and(|(p, s)| !unchanged(p, s))
            {
                report.skipped += 1;
                continue;
            }
            let file = root.dir.open(&item.path)?.into_std();
            if fs2::FileExt::try_lock_exclusive(&file).is_err() {
                report.skipped += 1;
                continue;
            }
            if engine::stamp(&cap_std::fs::Metadata::from_file(&file)?) != item.stamp {
                report.skipped += 1;
                continue;
            }
            // For conflicts, recheck the DB immediately before removing any payload.
            if category == "conflicts" {
                let id = item
                    .path
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .trim_end_matches(".json");
                if c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM conflicts WHERE folder=?1 AND id=?2)",
                    params![root.folder.id, id],
                    |r| r.get::<_, bool>(0),
                )? {
                    report.skipped += 1;
                    continue;
                }
            }
            root.dir.remove_file(&item.path)?;
            if let Some((side, _)) = &item.sidecar {
                root.dir.remove_file(side)?;
            }
            report.removed += 1;
            report.removed_bytes += item.bytes;
        }
        for category in ["versions", "tmp", "conflicts"] {
            let dir = format!(".ysync/{category}");
            if area(root, &dir)? {
                root.dir.open_dir(dir)?.open(".")?.into_std().sync_all()?;
            }
        }
    }
    Ok(report)
}
use rusqlite::params;
pub fn command(home: &Path, folder: Option<&str>, apply: bool) -> Result<Vec<Report>> {
    let _lock = if apply {
        Some(pairing::stopped(home)?)
    } else {
        None
    };
    let cfg = config::load(home)?;
    if let Some(f) = folder {
        ensure!(cfg.folders.iter().any(|x| x.id == f), "unknown folder");
    }
    cfg.folders
        .iter()
        .filter(|f| folder.is_none_or(|id| id == f.id))
        .map(|f| run(home, &pairing::root(home, &f.id)?, &cfg.retention, apply))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, time::Duration};
    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, engine::Root) {
        let h = tempfile::tempdir().unwrap();
        let f = tempfile::tempdir().unwrap();
        config::initialize(h.path(), None, None).unwrap();
        engine::add_folder(h.path(), "code", f.path(), false).unwrap();
        store::open(h.path()).unwrap();
        let r = pairing::root(h.path(), "code").unwrap();
        (h, f, r)
    }
    fn version(root: &engine::Root, data: &[u8], saved: u64) -> String {
        root.dir.create_dir_all(".ysync/versions").unwrap();
        let p = format!(".ysync/versions/{}", uuid::Uuid::new_v4());
        root.dir.write(&p, data).unwrap();
        root.dir
            .write(
                format!("{p}.json"),
                serde_json::to_vec(&serde_json::json!({"path":"file","saved_at":saved})).unwrap(),
            )
            .unwrap();
        p
    }
    fn old(path: &Path) {
        fs::File::open(path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1)))
            .unwrap();
    }
    #[test]
    fn retention_defaults_preserve_everything_and_dry_run_does_not_remove() {
        let (h, _f, r) = fixture();
        let p = version(&r, b"old", 1);
        let fresh = version(&r, b"fresh", now());
        assert!(
            run(h.path(), &r, &Policy::default(), true)
                .unwrap()
                .candidates
                .is_empty()
        );
        let policy = Policy {
            versions_days: Some(1),
            ..Default::default()
        };
        let report = run(h.path(), &r, &policy, false).unwrap();
        assert_eq!(report.candidates.len(), 1);
        assert!(r.dir.exists(&p));
        assert_eq!(run(h.path(), &r, &policy, true).unwrap().removed, 1);
        assert!(!r.dir.exists(&p));
        assert!(r.dir.exists(&fresh));
    }
    #[test]
    fn size_cap_removes_oldest_and_keeps_working_hard_links() {
        let (h, f, r) = fixture();
        let first = version(&r, b"12345", 1);
        let last = version(&r, b"12345", 2);
        fs::hard_link(f.path().join(&first), f.path().join("working")).unwrap();
        let report = run(
            h.path(),
            &r,
            &Policy {
                versions_max_bytes: Some(0),
                ..Default::default()
            },
            true,
        )
        .unwrap();
        assert_eq!(report.removed, 2);
        assert_eq!(fs::read(f.path().join("working")).unwrap(), b"12345");
        assert!(!r.dir.exists(last));
    }
    #[test]
    fn active_partial_is_locked_and_unknown_files_and_symlinks_are_preserved() {
        let (h, f, r) = fixture();
        r.dir.create_dir_all(".ysync/tmp").unwrap();
        let p = format!(".ysync/tmp/resume-{}.part", "a".repeat(64));
        r.dir.write(&p, b"partial").unwrap();
        old(&f.path().join(&p));
        let file = fs::File::open(f.path().join(&p)).unwrap();
        fs2::FileExt::lock_exclusive(&file).unwrap();
        r.dir.write(".ysync/tmp/unknown", b"unknown").unwrap();
        let external = tempfile::NamedTempFile::new().unwrap();
        let link = format!(".ysync/tmp/resume-{}.part", "b".repeat(64));
        std::os::unix::fs::symlink(external.path(), f.path().join(&link)).unwrap();
        let policy = Policy {
            partial_days: Some(1),
            ..Default::default()
        };
        let report = run(h.path(), &r, &policy, true).unwrap();
        assert_eq!(report.removed, 0);
        assert_eq!(report.skipped, 1);
        drop(file);
        assert_eq!(run(h.path(), &r, &policy, true).unwrap().removed, 1);
        assert!(r.dir.exists(".ysync/tmp/unknown"));
        assert!(external.path().exists());
        assert!(f.path().join(link).symlink_metadata().unwrap().is_symlink());
    }
    #[test]
    fn unresolved_and_uncommitted_conflict_archives_never_expire() {
        let (h, f, r) = fixture();
        let device = config::identity(h.path()).unwrap().0;
        r.dir.write("file", b"local").unwrap();
        engine::scan(&r, h.path(), &device, None, |_| {}).unwrap();
        let c = store::open(h.path()).unwrap();
        let local = store::get(&c, "code", "file").unwrap().unwrap();
        let mut remote = local.clone();
        remote.clock = [("b".repeat(64), 1)].into();
        remote.hash = blake3::hash(b"remote").to_hex().to_string();
        remote.size = 6;
        let (p, mut tmp) = engine::temp_file(&r).unwrap();
        use std::io::Write;
        tmp.write_all(b"remote").unwrap();
        drop(tmp);
        crate::conflicts::save(&c, &r, &local, &remote, Some(&p)).unwrap();
        let record = crate::conflicts::list(&c).unwrap().remove(0);
        let manifest = format!(".ysync/conflicts/{}.json", record.id);
        old(&f.path().join(&manifest));
        let policy = Policy {
            resolved_conflicts_days: Some(1),
            ..Default::default()
        };
        assert_eq!(run(h.path(), &r, &policy, true).unwrap().removed, 0);
        c.execute("DELETE FROM conflicts", []).unwrap();
        assert_eq!(run(h.path(), &r, &policy, true).unwrap().removed, 0);
        c.execute(
            "INSERT INTO conflicts VALUES(?1,?2,?3,?4)",
            params![
                "code",
                record.id,
                "file",
                serde_json::to_string(&record).unwrap()
            ],
        )
        .unwrap();
        crate::conflicts::keep_local(h.path(), "code", &record.id).unwrap();
        assert_eq!(run(h.path(), &r, &policy, true).unwrap().removed, 1);
        assert!(!r.dir.exists(manifest));
        assert_eq!(fs::read(f.path().join("file")).unwrap(), b"local");
    }
    #[test]
    fn symlinked_archive_area_and_running_manual_cleanup_are_rejected() {
        let (h, _f, r) = fixture();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), r.folder.path.join(".ysync/versions")).unwrap();
        assert!(
            run(
                h.path(),
                &r,
                &Policy {
                    versions_days: Some(1),
                    ..Default::default()
                },
                true
            )
            .is_err()
        );
        let _lock = pairing::stopped(h.path()).unwrap();
        assert!(command(h.path(), None, true).is_err());
    }
}
