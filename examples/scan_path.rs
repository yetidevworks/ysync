//! Read-only source diagnostic: scan one relative subtree into a disposable index.
use anyhow::{Context, Result, ensure};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use ysync::{config, engine, model};
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 3,
        "usage: scan_path STATE FOLDER_ID RELATIVE_PATH"
    );
    model::validate_path(&args[2])?;
    let folder = config::load(Path::new(&args[0]))?
        .folders
        .into_iter()
        .find(|f| f.id == args[1])
        .context("folder not configured")?;
    let root = engine::Root::open(folder, Arc::new(Mutex::new(())))?;
    let state = tempfile::tempdir()?;
    let id = config::initialize(state.path(), None, None)?;
    let started = std::time::Instant::now();
    let result = engine::scan(&root, state.path(), &id, Some(vec![args[2].clone()]), |n| {
        eprintln!("checked {n} entries");
    });
    eprintln!("elapsed: {:.3}s", started.elapsed().as_secs_f64());
    result?;
    Ok(())
}
