//! Stable path partitioning with one metadata/small-file lane and bounded bulk lanes.
use crate::model::{Entry, Kind};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
pub const BULK_BYTES: u64 = 1024 * 1024;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lane {
    pub index: u8,
    pub count: u8,
}
impl Default for Lane {
    fn default() -> Self {
        Self { index: 0, count: 1 }
    }
}
impl Lane {
    pub fn validate(self) -> Result<()> {
        ensure!(
            (1..=8).contains(&self.count) && self.index < self.count,
            "invalid transfer lane"
        );
        Ok(())
    }
    pub fn includes(self, e: &Entry) -> bool {
        self.index == Self::for_file(&e.path, e.kind == Kind::File, e.size, self.count)
    }
    pub(crate) fn for_file(path: &str, is_file: bool, size: u64, count: u8) -> u8 {
        if count == 1 || !is_file || size < BULK_BYTES {
            return 0;
        }
        1 + (u64::from_le_bytes(
            blake3::hash(path.as_bytes()).as_bytes()[..8]
                .try_into()
                .unwrap(),
        ) % u64::from(count - 1)) as u8
    }
}
