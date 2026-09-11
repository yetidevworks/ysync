use std::{collections::HashSet, io::Cursor, time::Instant};
use ysync::delta::{self, Parameters};
fn main() {
    let mut original = vec![0; 32 * 1024 * 1024];
    blake3::Hasher::new()
        .update(b"ysync delta benchmark")
        .finalize_xof()
        .fill(&mut original);
    let mut edit = original.clone();
    edit[16_000_000..16_004_096].fill(0xff);
    let mut insert = original.clone();
    insert.splice(1234..1234, b"unaligned insertion".iter().copied());
    let mut delete = original.clone();
    delete.drain(8_000_003..8_004_099);
    let fixed: HashSet<_> = original.chunks(64 * 1024).map(blake3::hash).collect();
    let mut results = Vec::new();
    for (name, target) in [
        ("4 KiB overwrite", edit),
        ("19 byte insertion", insert),
        ("4096 byte deletion", delete),
    ] {
        let started = Instant::now();
        let params = Parameters::for_sizes(target.len() as u64, original.len() as u64);
        let mut old_hash = blake3::Hasher::new();
        let basis = delta::signature(
            &mut Cursor::new(&original),
            0,
            original.len() as u64,
            params,
            &mut old_hash,
            || Ok(()),
        )
        .unwrap();
        let mut new_hash = blake3::Hasher::new();
        let source = delta::signature(
            &mut Cursor::new(&target),
            0,
            target.len() as u64,
            params,
            &mut new_hash,
            || Ok(()),
        )
        .unwrap();
        let ops = delta::plan(&source, &basis);
        let seconds = started.elapsed().as_secs_f64();
        assert_eq!(new_hash.finalize(), blake3::hash(&target));
        let literal: u64 = ops
            .iter()
            .filter(|op| op.basis_offset.is_none())
            .map(|op| op.len)
            .sum();
        let fixed_bytes: usize = target
            .chunks(64 * 1024)
            .filter(|chunk| !fixed.contains(&blake3::hash(chunk)))
            .map(<[u8]>::len)
            .sum();
        let signatures = serde_json::to_vec(&basis).unwrap().len();
        let plan = serde_json::to_vec(&ops).unwrap().len();
        results.push(serde_json::json!({"change":name,"file_bytes":target.len(),"cdc_literal_bytes":literal,"reused_percent":100.0*(1.0-literal as f64/target.len() as f64),"fixed_64KiB_literal_bytes":fixed_bytes,"basis_and_plan_json_bytes":signatures+plan,"index_and_plan_seconds":seconds}));
    }
    let result = serde_json::json!({"scope":"Synthetic in-memory algorithm comparison; not network throughput. Includes scanning both versions and planning; excludes receiver writes, encryption, and fsync.","results":results});
    let json = serde_json::to_string_pretty(&result).unwrap();
    println!("{json}");
    std::fs::write("benchmarks/delta-synthetic.json", format!("{json}\n")).unwrap();
}
