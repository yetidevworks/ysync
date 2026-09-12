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
        if self.count == 1 {
            return true;
        }
        if e.kind != Kind::File || e.size < BULK_BYTES {
            return self.index == 0;
        }
        self.index
            == 1 + (u64::from_le_bytes(
                blake3::hash(e.path.as_bytes()).as_bytes()[..8]
                    .try_into()
                    .unwrap(),
            ) % u64::from(self.count - 1)) as u8
    }
}
