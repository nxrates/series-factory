//! Binary volatility format for zero-copy mmap loading.
//!
//! File layout:
//!   [VolHeader 64 bytes][VolRecord 16 bytes][VolRecord 16 bytes]...
//!
//! Design principles:
//! - 64-byte header aligned to cache line
//! - 16-byte records for efficient iteration
//! - mmap-friendly: no pointers, plain data only
//! - Stats embedded for quick validation

use anyhow::{Context, Result};
use bytemuck::{bytes_of, Pod, Zeroable};
use memmap2::Mmap;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

const VOL_MAGIC: u64 = 0x564F4C00_4C4F5600; // "VOL\0LOV\0" - palindromic for validity check
const VOL_VERSION: u32 = 1;

/// 64-byte header for .vol files (one cache line)
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct VolHeader {
    /// Magic number for validation
    pub magic: u64,
    /// Format version
    pub version: u32,
    /// Simple hash of asset name for quick identification
    pub asset_hash: u32,
    /// Number of vol records following the header
    pub n_records: u64,
    /// First timestamp (milliseconds)
    pub start_ts: i64,
    /// Last timestamp (milliseconds)
    pub end_ts: i64,
    /// Minimum sigma percentage (for sanity checks)
    pub min_sigma_pct: f64,
    /// Maximum sigma percentage (for sanity checks)
    pub max_sigma_pct: f64,
    /// Reserved for future use
    pub _reserved: u64,
}

const _: () = assert!(std::mem::size_of::<VolHeader>() == 64);
const _: () = assert!(std::mem::align_of::<VolHeader>() == 8);

impl VolHeader {
    pub fn new(asset_hash: u32) -> Self {
        Self {
            magic: VOL_MAGIC,
            version: VOL_VERSION,
            asset_hash,
            n_records: 0,
            start_ts: i64::MAX,
            end_ts: i64::MIN,
            min_sigma_pct: f64::MAX,
            max_sigma_pct: f64::MIN,
            _reserved: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == VOL_MAGIC && self.version == VOL_VERSION
    }

    /// Hash an asset name to u32 (simple FNV-1a variant)
    pub fn hash_asset(name: &str) -> u32 {
        let mut hash = 0x811c9dc5u32;
        for byte in name.bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(0x01000193);
        }
        hash
    }
}

/// 16-byte vol record (timestamp + percentage)
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct VolRecord {
    /// Timestamp in milliseconds since epoch
    pub timestamp: i64,
    /// Parkinson sigma as percentage (e.g., 0.015 = 1.5%)
    pub sigma_pct: f64,
}

const _: () = assert!(std::mem::size_of::<VolRecord>() == 16);
const _: () = assert!(std::mem::align_of::<VolRecord>() == 8);

/// Streaming vol writer - writes directly to file without buffering all data
pub struct VolWriter {
    writer: BufWriter<File>,
    header: VolHeader,
    record_count: u64,
    min_sigma: f64,
    max_sigma: f64,
    first_ts: Option<i64>,
    last_ts: i64,
}

impl VolWriter {
    /// Create a new vol writer
    pub fn create(path: &Path, asset_name: &str) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create vol file: {}", path.display()))?;

        let mut writer = BufWriter::with_capacity(256 * 1024, file);
        let header = VolHeader::new(VolHeader::hash_asset(asset_name));

        // Write placeholder header (will be rewritten on close)
        writer.write_all(bytes_of(&header))?;

        Ok(Self {
            writer,
            header,
            record_count: 0,
            min_sigma: f64::MAX,
            max_sigma: f64::MIN,
            first_ts: None,
            last_ts: 0,
        })
    }

    /// Write a single vol record
    pub fn write_record(&mut self, timestamp: i64, sigma_pct: f64) -> Result<()> {
        let record = VolRecord { timestamp, sigma_pct };
        self.writer.write_all(bytes_of(&record))?;

        // Update stats
        self.record_count += 1;
        self.min_sigma = self.min_sigma.min(sigma_pct);
        self.max_sigma = self.max_sigma.max(sigma_pct);
        self.last_ts = timestamp;
        if self.first_ts.is_none() {
            self.first_ts = Some(timestamp);
        }

        Ok(())
    }

    /// Finalize the file and update header
    pub fn finish(mut self) -> Result<()> {
        // Update header with final stats
        self.header.n_records = self.record_count;
        self.header.start_ts = self.first_ts.unwrap_or(0);
        self.header.end_ts = self.last_ts;
        self.header.min_sigma_pct = if self.record_count > 0 { self.min_sigma } else { 0.0 };
        self.header.max_sigma_pct = if self.record_count > 0 { self.max_sigma } else { 0.0 };

        // Flush records
        self.writer.flush()?;

        // Rewrite header at the beginning
        let mut file = self.writer.into_inner()?;
        file.seek(std::io::SeekFrom::Start(0))?;

        // Truncate to exact size
        let total_size = std::mem::size_of::<VolHeader>()
            + (self.record_count as usize) * std::mem::size_of::<VolRecord>();
        file.set_len(total_size as u64)?;

        file.write_all(bytes_of(&self.header))?;
        file.sync_all()?;

        Ok(())
    }
}

/// Mmap-backed vol reader - zero-copy access to volatility data
pub struct VolMmap {
    _mmap: Mmap,
    header: &'static VolHeader,
    records: &'static [VolRecord],
}

unsafe impl Send for VolMmap {}
unsafe impl Sync for VolMmap {}

impl VolMmap {
    /// Open a vol file with mmap
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("Failed to open vol file: {}", path.display()))?;

        let mmap = unsafe { Mmap::map(&file)? };

        // Validate minimum size
        if mmap.len() < std::mem::size_of::<VolHeader>() {
            anyhow::bail!("Vol file too small: {}", path.display());
        }

        // Parse header
        let header = unsafe {
            &*(mmap.as_ptr() as *const VolHeader)
        };

        if !header.is_valid() {
            anyhow::bail!("Invalid vol file: bad magic or version");
        }

        // Validate file size matches header
        let expected_size = std::mem::size_of::<VolHeader>()
            + (header.n_records as usize) * std::mem::size_of::<VolRecord>();
        if mmap.len() != expected_size {
            anyhow::bail!(
                "Vol file size mismatch: expected {} bytes, got {}",
                expected_size,
                mmap.len()
            );
        }

        // Get records slice
        let records = unsafe {
            std::slice::from_raw_parts(
                mmap.as_ptr().add(std::mem::size_of::<VolHeader>()) as *const VolRecord,
                header.n_records as usize,
            )
        };

        Ok(Self {
            _mmap: mmap,
            // Extend lifetime to static - safe as long as Self owns the mmap
            header: unsafe { &*(header as *const VolHeader) },
            records: unsafe { &*(records as *const [VolRecord]) },
        })
    }

    pub fn len(&self) -> usize {
        self.header.n_records as usize
    }

    pub fn records(&self) -> &[VolRecord] {
        self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<VolHeader>(), 64);
    }

    #[test]
    fn test_record_size() {
        assert_eq!(std::mem::size_of::<VolRecord>(), 16);
    }

    #[test]
    fn test_asset_hash() {
        let hash1 = VolHeader::hash_asset("BTCUSDT");
        let hash2 = VolHeader::hash_asset("BTCUSDT");
        let hash3 = VolHeader::hash_asset("ETHUSDT");
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_header_validation() {
        let mut header = VolHeader::new(12345);
        assert!(header.is_valid());

        header.magic = 0xDEADBEEF;
        assert!(!header.is_valid());
    }
}
