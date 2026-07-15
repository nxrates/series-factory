//! `.vol` file format: dense series of `(mts, sigma_pct)` records.
//!
//! File layout is now headerless. Each row is a 14-byte [`VolRecord`] and the
//! file length is always a multiple of that size. Writes go through
//! [`nxr_sdk::AppendLog`] (shared with the aggregator's `.idx` output) so there
//! is one append-only storage primitive across the project instead of a
//! bespoke header+records writer per data type. Reads mmap the file and cast
//! directly to `&[VolRecord]`.
//!
//! ## Record layout (14 B, little-endian, packed)
//!
//! ```text
//! Offset | Field     | Size | Type    | Description
//! -------|-----------|------|---------|--------------------------------------
//! 0      | mts       | 6    | u48 LE  | Record mts (16 us ticks since 2010)
//! 6      | sigma_pct | 8    | f64 LE  | Rogers-Satchell sigma over s10 OHLC, fraction (0.015 = 1.5%)
//! ```
//!
//! Matches the `#[repr(C, packed)]` convention used by `mitch::Index` and
//! other MITCH body types: no trailing pad, unaligned f64 reads via
//! `bytemuck::Pod`. Decode timestamps via [`mitch::timestamp::decode_u48`];
//! encode with [`mitch::timestamp::encode_u48`].

use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use memmap2::Mmap;
use mitch::timestamp;
use nxr_sdk::AppendLog;
use std::fs::File;
use std::path::Path;

/// 30 minutes expressed in MITCH ticks (16 us granularity).
/// 30 min = 1 800 000 000 us / 16 us = 112 500 000 ticks.
const HALF_HOUR_TICKS: u64 = 112_500_000;

/// 14-byte vol record (mts + sigma). See module docs for wire layout.
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C, packed)]
pub struct VolRecord {
    pub mts: [u8; 6],
    pub sigma_pct: f64,
}

const _: () = assert!(std::mem::size_of::<VolRecord>() == 14);

/// Append-only writer for `.vol` files. Thin wrapper over
/// [`nxr_sdk::AppendLog<VolRecord>`].
///
/// Unlike the previous header+stats format, the writer keeps no in-memory
/// metadata: each `write_record` call flushes one 16-byte row to disk. Callers
/// that need aggregate stats (min/max sigma, first/last mts) should compute
/// them from the loaded slice at read time. This matches the policy used
/// elsewhere in the project (`.idx`, `.bars`) where files are just a sequence
/// of POD records.
pub struct VolWriter {
    log: AppendLog<VolRecord>,
}

impl VolWriter {
    pub fn new(path: &Path) -> Result<Self> {
        let log =
            AppendLog::open(path).with_context(|| format!("open vol file: {}", path.display()))?;
        Ok(Self { log })
    }

    /// Append one record. `mts` is a u48 MITCH tick (16 us since 2010).
    pub fn write_record(&mut self, mts: u64, sigma_pct: f64) -> Result<()> {
        let record = VolRecord {
            mts: timestamp::encode_u48(mts),
            sigma_pct,
        };
        self.log.append(&record)
    }

    /// No-op kept for source compatibility. AppendLog writes reach disk as
    /// soon as `append()` returns; OS-level sync happens on drop.
    pub fn finish(self) -> Result<()> {
        Ok(())
    }
}

/// Mmap-backed vol reader - zero-copy access to volatility data.
///
/// Expects a file whose length is a multiple of `size_of::<VolRecord>()`. A
/// zero-byte file yields an empty record slice (valid).
pub struct VolMmap {
    _mmap: Mmap,
    records: &'static [VolRecord],
}

unsafe impl Send for VolMmap {}
unsafe impl Sync for VolMmap {}

impl nxr_sdk::vol::VolSource for VolMmap {
    #[inline]
    fn len(&self) -> usize {
        self.records.len()
    }

    #[inline]
    fn sigma_pct(&self, i: usize) -> f64 {
        self.records.get(i).map(|r| r.sigma_pct).unwrap_or(0.0)
    }

    /// Binary search records by MITCH mts, bucketed to 30-minute periods.
    #[inline]
    fn find_index_for_mts(&self, mts: u64) -> usize {
        let period_mts = (mts / HALF_HOUR_TICKS) * HALF_HOUR_TICKS;
        let res = self.records.binary_search_by(|r| {
            let bytes = r.mts;
            timestamp::decode_u48(&bytes).cmp(&period_mts)
        });
        match res {
            Ok(idx) => idx,
            Err(idx) => idx.min(self.records.len().saturating_sub(1)),
        }
    }
}

impl VolMmap {
    pub fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("open vol file: {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file)? };

        let rec_size = std::mem::size_of::<VolRecord>();
        if mmap.len() % rec_size != 0 {
            anyhow::bail!(
                "vol file size {} not a multiple of {} (corrupt or wrong format): {}",
                mmap.len(),
                rec_size,
                path.display()
            );
        }
        let n = mmap.len() / rec_size;

        let records: &[VolRecord] =
            unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const VolRecord, n) };
        let records: &'static [VolRecord] = unsafe { &*(records as *const [VolRecord]) };

        Ok(Self {
            _mmap: mmap,
            records,
        })
    }

    /// Underlying vol records (timestamp + sigma).
    pub fn records(&self) -> &[VolRecord] {
        self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_size() {
        assert_eq!(std::mem::size_of::<VolRecord>(), 14);
    }

    #[test]
    fn write_then_mmap_roundtrip() {
        let tmp = std::env::temp_dir().join("nxr_vol_roundtrip.vol");
        let _ = std::fs::remove_file(&tmp);

        {
            let mut w = VolWriter::new(&tmp).unwrap();
            w.write_record(1_000, 0.010).unwrap();
            w.write_record(2_000, 0.020).unwrap();
            w.finish().unwrap();
        }

        let m = VolMmap::open(&tmp).unwrap();
        assert_eq!(m.records().len(), 2);
        let sigma = m.records()[1].sigma_pct;
        assert_eq!(sigma, 0.020);
        std::fs::remove_file(&tmp).ok();
    }
}
