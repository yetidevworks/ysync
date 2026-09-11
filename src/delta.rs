//! Content-defined deltas. Gear hashing selects boundaries; BLAKE3 verifies content.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom, Write},
    time::{Duration, Instant},
};

pub const THRESHOLD: u64 = 1024 * 1024;
pub const MAX_BLOCKS: usize = 8192;
#[derive(Clone, Copy)]
pub struct Parameters {
    pub min: u64,
    pub max: u64,
    mask: u64,
}
impl Parameters {
    pub fn for_sizes(source: u64, basis: u64) -> Self {
        let min = source
            .max(basis)
            .div_ceil(MAX_BLOCKS as u64)
            .max(16 * 1024)
            .next_power_of_two();
        Self {
            min,
            max: min * 16,
            mask: min * 4 - 1,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Block {
    pub offset: u64,
    pub len: u64,
    pub hash: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub len: u64,
    pub hash: String,
    pub basis_offset: Option<u64>,
}

// Fixed deterministic table; boundary selection is not a cryptographic operation.
const fn gear_table() -> [u64; 256] {
    let mut result = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut n = (i as u64).wrapping_add(0x9e3779b97f4a7c15);
        n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
        result[i] = n ^ (n >> 31);
        i += 1;
    }
    result
}
const GEAR: [u64; 256] = gear_table();

/// Reads only `size` bytes. Memory is bounded by one 256 KiB buffer plus MAX_BLOCKS descriptors.
pub fn signature(
    file: &mut impl Read,
    start: u64,
    size: u64,
    params: Parameters,
    whole: &mut blake3::Hasher,
    mut progress: impl FnMut() -> Result<()>,
) -> Result<Vec<Block>> {
    if start.checked_add(size).is_none() {
        bail!("source range overflow");
    }
    let mut blocks = Vec::new();
    let mut buf = [0; 256 * 1024];
    let mut remaining = size;
    let mut offset = start;
    let mut len = 0u64;
    let mut gear = 0u64;
    let mut hash = blake3::Hasher::new();
    let mut tick = Instant::now();
    progress()?;
    while remaining > 0 {
        let count = remaining.min(buf.len() as u64) as usize;
        let n = file.read(&mut buf[..count])?;
        if n == 0 {
            bail!("file shortened during delta indexing");
        }
        let mut from = 0;
        for (i, &byte) in buf[..n].iter().enumerate() {
            gear = gear.wrapping_shl(1).wrapping_add(GEAR[byte as usize]);
            len += 1;
            if len >= params.min && (gear & params.mask == 0 || len >= params.max) {
                hash.update(&buf[from..=i]);
                whole.update(&buf[from..=i]);
                blocks.push(Block {
                    offset,
                    len,
                    hash: hash.finalize().to_hex().to_string(),
                });
                if blocks.len() > MAX_BLOCKS {
                    bail!("delta signature exceeds block budget");
                }
                offset += len;
                len = 0;
                gear = 0;
                hash = blake3::Hasher::new();
                from = i + 1;
            }
        }
        hash.update(&buf[from..n]);
        whole.update(&buf[from..n]);
        remaining -= n as u64;
        if tick.elapsed() >= Duration::from_secs(1) {
            progress()?;
            tick = Instant::now();
        }
    }
    if len > 0 {
        blocks.push(Block {
            offset,
            len,
            hash: hash.finalize().to_hex().to_string(),
        });
    }
    if blocks.len() > MAX_BLOCKS {
        bail!("delta signature exceeds block budget");
    }
    Ok(blocks)
}
fn valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hex::decode(hash).is_ok()
}
pub fn validate_basis(blocks: &[Block], size: u64, params: Parameters) -> Result<()> {
    if blocks.len() > MAX_BLOCKS {
        bail!("too many basis blocks");
    }
    let mut offset = 0u64;
    for b in blocks {
        if b.offset != offset || b.len == 0 || b.len > params.max || !valid_hash(&b.hash) {
            bail!("invalid basis block");
        }
        offset = offset
            .checked_add(b.len)
            .ok_or_else(|| anyhow::anyhow!("basis range overflow"))?;
        if offset > size {
            bail!("basis exceeds declared size");
        }
    }
    if offset != size {
        bail!("incomplete basis signature");
    }
    Ok(())
}
pub fn plan(source: &[Block], basis: &[Block]) -> Vec<Operation> {
    let mut lookup = HashMap::new();
    for block in basis {
        lookup
            .entry((&block.hash, block.len))
            .or_insert(block.offset);
    }
    source
        .iter()
        .map(|b| Operation {
            len: b.len,
            hash: b.hash.clone(),
            basis_offset: lookup.get(&(&b.hash, b.len)).copied(),
        })
        .collect()
}
pub fn validate_plan(
    ops: &[Operation],
    remaining: u64,
    basis_size: u64,
    params: Parameters,
) -> Result<()> {
    if ops.len() > MAX_BLOCKS {
        bail!("too many delta operations");
    }
    let mut size = 0u64;
    for op in ops {
        if op.len == 0 || op.len > params.max || !valid_hash(&op.hash) {
            bail!("invalid delta operation");
        }
        if op.basis_offset.is_some_and(|offset| {
            offset
                .checked_add(op.len)
                .is_none_or(|end| end > basis_size)
        }) {
            bail!("copy outside basis");
        }
        size = size
            .checked_add(op.len)
            .ok_or_else(|| anyhow::anyhow!("delta length overflow"))?;
        if size > remaining {
            bail!("delta exceeds target size");
        }
    }
    if size != remaining {
        bail!("incomplete delta plan");
    }
    Ok(())
}

/// Copy from the held basis descriptor into staging and reject stale/corrupt content.
pub fn copy_verified(
    basis: &mut (impl Read + Seek),
    output: &mut impl Write,
    operation: &Operation,
    whole: &mut blake3::Hasher,
    mut progress: impl FnMut() -> Result<()>,
) -> Result<()> {
    let offset = operation
        .basis_offset
        .ok_or_else(|| anyhow::anyhow!("not a copy operation"))?;
    basis.seek(SeekFrom::Start(offset))?;
    let mut remaining = operation.len;
    let mut buf = [0; 256 * 1024];
    let mut hash = blake3::Hasher::new();
    while remaining > 0 {
        let count = remaining.min(buf.len() as u64) as usize;
        let n = basis.read(&mut buf[..count])?;
        if n == 0 {
            bail!("basis shortened while copying delta");
        }
        output.write_all(&buf[..n])?;
        whole.update(&buf[..n]);
        hash.update(&buf[..n]);
        remaining -= n as u64;
        progress()?;
    }
    if hash.finalize().to_hex().as_str() != operation.hash {
        bail!("delta copy checksum mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    fn sig(data: &[u8], params: Parameters) -> Vec<Block> {
        let mut whole = blake3::Hasher::new();
        let result = signature(
            &mut Cursor::new(data),
            0,
            data.len() as u64,
            params,
            &mut whole,
            || Ok(()),
        )
        .unwrap();
        assert_eq!(whole.finalize(), blake3::hash(data));
        result
    }
    #[test]
    fn edits_insertions_and_deletions_reuse_shifted_content() {
        let mut basis = vec![0; 8 * 1024 * 1024];
        blake3::Hasher::new()
            .update(b"delta fixture")
            .finalize_xof()
            .fill(&mut basis);
        let mut edited = basis.clone();
        edited[3_000_000..3_004_096].fill(0x62);
        let mut inserted = basis.clone();
        inserted.splice(1024..1024, b"unaligned insertion".iter().copied());
        let mut deleted = basis.clone();
        deleted.drain(2_000_007..2_012_345);
        for target in [edited, inserted, deleted] {
            let params = Parameters::for_sizes(target.len() as u64, basis.len() as u64);
            let old = sig(&basis, params);
            let new = sig(&target, params);
            let ops = plan(&new, &old);
            validate_basis(&old, basis.len() as u64, params).unwrap();
            validate_plan(&ops, target.len() as u64, basis.len() as u64, params).unwrap();
            let mut assembled = Vec::new();
            let mut literals = 0;
            for (chunk, op) in new.iter().zip(ops) {
                let bytes = if let Some(offset) = op.basis_offset {
                    &basis[offset as usize..(offset + op.len) as usize]
                } else {
                    literals += op.len;
                    &target[chunk.offset as usize..(chunk.offset + chunk.len) as usize]
                };
                assert_eq!(blake3::hash(bytes).to_hex().as_str(), op.hash);
                assembled.extend_from_slice(bytes);
            }
            assert_eq!(assembled, target);
            assert!(literals < 512 * 1024, "too many literal bytes: {literals}");
        }
    }
    #[test]
    fn manifests_reject_overflow_gaps_invalid_hashes_and_out_of_bounds_copies() {
        let params = Parameters::for_sizes(10, 10);
        let good = Block {
            offset: 0,
            len: 10,
            hash: blake3::hash(b"0123456789").to_hex().to_string(),
        };
        validate_basis(std::slice::from_ref(&good), 10, params).unwrap();
        let mut bad = good.clone();
        bad.offset = 1;
        assert!(validate_basis(&[bad], 10, params).is_err());
        let mut bad = good.clone();
        bad.hash = "z".repeat(64);
        assert!(validate_basis(&[bad], 10, params).is_err());
        let op = Operation {
            len: 10,
            hash: good.hash,
            basis_offset: Some(u64::MAX),
        };
        assert!(validate_plan(&[op], 10, 10, params).is_err());
        assert!(validate_plan(&[], 10, 10, params).is_err());
        let oversized = vec![
            Block {
                offset: 0,
                len: 1,
                hash: "0".repeat(64)
            };
            MAX_BLOCKS + 1
        ];
        assert!(validate_basis(&oversized, 1, params).is_err());
    }
    #[test]
    fn boundaries_ignore_read_fragmentation_and_handle_empty_and_zero_content() {
        struct Short<'a>(&'a [u8]);
        impl Read for Short<'_> {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = out.len().min(733).min(self.0.len());
                out[..n].copy_from_slice(&self.0[..n]);
                self.0 = &self.0[n..];
                Ok(n)
            }
        }
        let data = vec![0; 1024 * 1024 + 17];
        let params = Parameters::for_sizes(data.len() as u64, data.len() as u64);
        let normal = sig(&data, params);
        let short = signature(
            &mut Short(&data),
            0,
            data.len() as u64,
            params,
            &mut blake3::Hasher::new(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&normal).unwrap(),
            serde_json::to_string(&short).unwrap()
        );
        assert!(sig(&[], params).is_empty());
        assert!(
            signature(
                &mut Cursor::new(b"short"),
                0,
                10,
                params,
                &mut blake3::Hasher::new(),
                || Ok(())
            )
            .is_err()
        );
        let largest = Parameters::for_sizes(i64::MAX as u64, i64::MAX as u64);
        assert!((i64::MAX as u64).div_ceil(largest.min) <= MAX_BLOCKS as u64);
    }
    #[test]
    fn modified_or_truncated_basis_cannot_pass_copy_verification() {
        let original = b"a block of original bytes";
        let op = Operation {
            len: original.len() as u64,
            hash: blake3::hash(original).to_hex().to_string(),
            basis_offset: Some(0),
        };
        let mut output = Vec::new();
        copy_verified(
            &mut Cursor::new(original),
            &mut output,
            &op,
            &mut blake3::Hasher::new(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(output, original);
        let mut modified = original.to_vec();
        modified[4] ^= 0xff;
        assert!(
            copy_verified(
                &mut Cursor::new(modified),
                &mut Vec::new(),
                &op,
                &mut blake3::Hasher::new(),
                || Ok(())
            )
            .is_err()
        );
        assert!(
            copy_verified(
                &mut Cursor::new(&original[..4]),
                &mut Vec::new(),
                &op,
                &mut blake3::Hasher::new(),
                || Ok(())
            )
            .is_err()
        );
    }
}
