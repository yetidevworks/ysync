//! Separates hashing/indexing from transport and receiver filesystem costs.
use anyhow::Result;
use std::{
    fs,
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::Instant,
};
use ysync::{config, engine, store};

fn main() -> Result<()> {
    let files = tempfile::tempdir()?;
    let file_count = 64u64;
    let size = 16 * 1024 * 1024u64;
    let block: Vec<u8> = (0..1024 * 1024).map(|n| (n % 251) as u8).collect();
    for i in 0..file_count {
        let mut f = fs::File::create(files.path().join(format!("{i:04}.bin")))?;
        for _ in 0..16 {
            f.write_all(&block)?;
        }
    }
    let mut results = Vec::new();
    for workers in [1, 2, 4, 8, 16] {
        // Deliberately warm each input before every trial, so this measures hashing/metadata
        // concurrency rather than which trial happened to populate the disk cache first.
        for i in 0..file_count {
            let mut file = fs::File::open(files.path().join(format!("{i:04}.bin")))?;
            let mut buf = vec![0; 1024 * 1024];
            while file.read(&mut buf)? > 0 {}
        }
        let home = tempfile::tempdir()?;
        let id = config::initialize(home.path(), None, None)?;
        engine::add_folder(home.path(), "bench", files.path(), false)?;
        store::open(home.path())?;
        let root = engine::Root::open(
            config::load(home.path())?.folders.remove(0),
            Arc::new(Mutex::new(())),
        )?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()?;
        let start = Instant::now();
        pool.install(|| engine::scan(&root, home.path(), &id, None, |_| {}))?;
        let seconds = start.elapsed().as_secs_f64();
        assert_eq!(
            store::total(&store::open(home.path())?, "bench")?,
            (file_count, file_count * size)
        );
        let result = serde_json::json!({"workers":workers,"seconds":seconds,"MB_per_second":(file_count*size) as f64/seconds/1e6});
        println!("{result}");
        results.push(result);
    }
    let report = serde_json::json!({"scope":"warm-cache scan/hash/index only; no network or receiver writes","files":file_count,"bytes":file_count*size,"trials":results});
    fs::write(
        "benchmarks/scanner-warm-cache.json",
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(())
}
