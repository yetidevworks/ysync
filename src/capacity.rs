//! Read-only capacity diagnostics. Index estimates never walk synchronized trees.
use crate::{config, engine::Root};
use anyhow::{Context, Result};
use serde::Serialize;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default, Serialize, serde::Deserialize)]
pub struct Limits {
    pub max_user_watches: Option<u64>,
    pub max_user_instances: Option<u64>,
    pub max_queued_events: Option<u64>,
}
#[cfg(target_os = "linux")]
pub fn limits() -> Limits {
    let read = |name: &str| {
        std::fs::read_to_string(Path::new("/proc/sys/fs/inotify").join(name))
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
    };
    Limits {
        max_user_watches: read("max_user_watches"),
        max_user_instances: read("max_user_instances"),
        max_queued_events: read("max_queued_events"),
    }
}
#[cfg(not(target_os = "linux"))]
pub fn limits() -> Limits {
    Limits::default()
}
#[derive(Serialize)]
pub struct FolderEstimate {
    pub id: String,
    pub paused: bool,
    pub indexed_directories: Option<u64>,
    pub estimated_watches: Option<u64>,
    pub error: Option<String>,
}
#[derive(Serialize)]
pub struct Report {
    pub limits: Limits,
    pub folders: Vec<FolderEstimate>,
    pub active_estimated_watches: u64,
    pub all_estimated_watches: u64,
    pub estimates_available: bool,
    pub all_folders_exceed_limit: Option<bool>,
    pub suggested_max_user_watches: Option<u64>,
    pub note: String,
}
fn suggestion(needed: u64, current: u64) -> u64 {
    let headroom = (needed / 4).max(65_536);
    needed
        .saturating_add(headroom)
        .div_ceil(65_536)
        .saturating_mul(65_536)
        .max(current)
}
pub fn report(home: &Path) -> Result<Report> {
    let cfg = config::load(home)?;
    let limits = limits();
    let db = rusqlite::Connection::open_with_flags(
        home.join("index.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .context("capacity estimates need an existing index; run a scanner first")?;
    db.busy_timeout(std::time::Duration::from_secs(2))?;
    let mut folders = Vec::new();
    let mut active = 0;
    let mut all = 0;
    for folder in cfg.folders {
        let mut estimate = FolderEstimate {
            id: folder.id.clone(),
            paused: folder.paused,
            indexed_directories: None,
            estimated_watches: None,
            error: None,
        };
        let result = (|| -> Result<u64> {
            let root = Root::open(folder, Arc::new(Mutex::new(())))?;
            let mut query = db.prepare("SELECT path FROM entries WHERE folder=?1 AND json_extract(data,'$.kind')='Directory'")?;
            let rows = query.query_map([&root.folder.id], |r| r.get::<_, String>(0))?;
            let mut count = 0;
            for path in rows {
                if !root.excluded(&path?) {
                    count += 1;
                }
            }
            Ok(count)
        })();
        match result {
            Ok(n) => {
                estimate.indexed_directories = Some(n);
                estimate.estimated_watches = Some(n + 2);
                all += n + 2;
                if !estimate.paused {
                    active += n + 2;
                }
            }
            Err(e) => estimate.error = Some(format!("{e:#}")),
        }
        folders.push(estimate);
    }
    let available = folders.iter().all(|f| f.error.is_none());
    Ok(Report {
        all_folders_exceed_limit: limits.max_user_watches.map(|limit| all > limit),
        suggested_max_user_watches: limits.max_user_watches.filter(|_| available).map(|limit| suggestion(all, limit)),
        limits, folders, active_estimated_watches: active, all_estimated_watches: all, estimates_available: available,
        note: "Estimates use cached directory records and current exclusions, plus root/control watches. An incomplete or stale index can undercount or overcount. Linux limits are shared with other processes of this user; their usage is not included. No source tree was walked and no kernel settings were changed.".into(),
    })
}
pub fn print_report(home: &Path, json: bool) -> Result<()> {
    let report = report(home)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    println!(
        "Linux inotify watches: {} | instances: {} | event queue: {}",
        report
            .limits
            .max_user_watches
            .map_or_else(|| "unavailable".into(), |n| n.to_string()),
        report
            .limits
            .max_user_instances
            .map_or_else(|| "unavailable".into(), |n| n.to_string()),
        report
            .limits
            .max_queued_events
            .map_or_else(|| "unavailable".into(), |n| n.to_string())
    );
    for folder in &report.folders {
        println!(
            "{}{}: {}",
            folder.id,
            if folder.paused { " (paused)" } else { "" },
            folder.estimated_watches.map_or_else(
                || folder.error.clone().unwrap_or_default(),
                |n| format!("{n} estimated watches")
            )
        );
    }
    println!(
        "Active folders: {} | all folders: {} estimated watches",
        report.active_estimated_watches, report.all_estimated_watches
    );
    if report.all_folders_exceed_limit == Some(true) {
        println!("CAPACITY SHORTFALL: all configured folders exceed the shared watch limit.");
    }
    if let Some(limit) = report.suggested_max_user_watches {
        println!(
            "Suggested watch limit with headroom: {limit} (review other applications' needs too)"
        );
    }
    println!("{}", report.note);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn headroom_rounds_up_and_never_reduces_existing_limit() {
        assert_eq!(suggestion(800_000, 524_288), 1_048_576);
        assert_eq!(suggestion(100, 524_288), 524_288);
    }
    #[test]
    fn estimate_filters_cached_ignores_and_includes_paused_folders() {
        let state = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let id = config::initialize(state.path(), None, None).unwrap();
        crate::engine::add_folder(state.path(), "test", files.path(), false).unwrap();
        std::fs::create_dir_all(files.path().join("ignored/nested")).unwrap();
        std::fs::create_dir(files.path().join("src")).unwrap();
        let root = Root::open(
            config::load(state.path()).unwrap().folders[0].clone(),
            Arc::new(Mutex::new(())),
        )
        .unwrap();
        crate::engine::scan(&root, state.path(), &id, None, |_| {}).unwrap();
        std::fs::write(files.path().join(".ysyncignore"), "ignored\n").unwrap();
        config::edit(state.path(), |c| {
            c.folders[0].paused = true;
            Ok(())
        })
        .unwrap();
        let result = report(state.path()).unwrap();
        assert_eq!(result.active_estimated_watches, 0);
        assert_eq!(result.all_estimated_watches, 3);
        assert_eq!(result.folders[0].indexed_directories, Some(1));
    }
}
