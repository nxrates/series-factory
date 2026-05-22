//! Daily-shard helpers (Phase 55 W4).
//!
//! Spec: `docs/sharding-spec.md`. All time-series artifacts (`.idx`, `.s10`,
//! `.renko`, `.bars`) are sharded by `open_ts.date_utc()` into per-ticker
//! subdirectories. Each ticker dir owns a `manifest.json` (single source of
//! truth for shard inventory + sha256). This module owns the path math, the
//! atomic shard writer, and the manifest read/write/merge logic.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// UTC date for an epoch-ms timestamp. Used as the shard bucket key for ∀
/// artifact types.
#[inline]
pub fn ts_ms_to_utc_date(ts_ms: i64) -> NaiveDate {
    // ts_ms is i64 epoch milliseconds; div by 86_400_000 = days since epoch.
    // Use chrono's NaiveDateTime to get robust handling of pre-1970 ts (unused
    // in prod but cheap to keep correct).
    let secs = ts_ms.div_euclid(1000);
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
    dt.date_naive()
}

/// Format YYYY-MM-DD (spec-canonical shard filename stem).
#[inline]
pub fn date_stem(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

/// Per-shard manifest entry (matches spec §Manifest format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardEntry {
    pub date: String,
    pub first_ts: i64,
    pub last_ts: i64,
    pub n_records: u64,
    pub size_bytes: u64,
    pub sha256: String,
}

/// Per-ticker shard inventory + integrity hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub ticker: String,
    #[serde(default)]
    pub ticker_id: u64,
    #[serde(default)]
    pub kind: String, // "idx" | "s10" | "renko" | "bars"
    pub shards: Vec<ShardEntry>,
    pub first_ts: i64,
    pub last_ts: i64,
    pub last_updated: i64,
    pub schema_version: u32,
}

impl Manifest {
    pub fn new(ticker: String, ticker_id: u64, kind: &str) -> Self {
        Self {
            ticker,
            ticker_id,
            kind: kind.to_string(),
            shards: Vec::new(),
            first_ts: i64::MAX,
            last_ts: i64::MIN,
            last_updated: 0,
            schema_version: 1,
        }
    }

    /// Insert or replace a shard entry by date. Keeps shards sorted by date.
    pub fn upsert(&mut self, entry: ShardEntry) {
        if let Some(existing) = self.shards.iter_mut().find(|s| s.date == entry.date) {
            *existing = entry;
        } else {
            self.shards.push(entry);
        }
        self.shards.sort_by(|a, b| a.date.cmp(&b.date));
        if let Some(first) = self.shards.first() {
            self.first_ts = first.first_ts;
        }
        if let Some(last) = self.shards.last() {
            self.last_ts = last.last_ts;
        }
        self.last_updated = chrono::Utc::now().timestamp_millis();
    }
}

/// Read manifest if present; else empty manifest stub.
pub fn read_manifest(path: &Path) -> Result<Option<Manifest>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read manifest {}", path.display()))?;
    let m: Manifest = serde_json::from_str(&raw)
        .with_context(|| format!("parse manifest {}", path.display()))?;
    Ok(Some(m))
}

/// Atomic manifest write (`.tmp` then rename).
pub fn write_manifest(path: &Path, m: &Manifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(m)?;
    {
        let mut f = fs::File::create(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(json.as_bytes())?;
        let _ = f.sync_data();
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Compute sha256 of a file (streamed, 1 MiB chunks).
pub fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut f = fs::File::open(path)
        .with_context(|| format!("open for hash {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Atomic shard write: write `bytes` to `<path>.tmp` → fsync → rename → path.
/// Caller pre-computes the bytes (for binary-fixed records typically via
/// `bytemuck::cast_slice`).
pub fn write_shard_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    let tmp = if ext.is_empty() {
        path.with_extension("tmp")
    } else {
        path.with_extension(format!("{ext}.tmp"))
    };
    {
        let mut f = fs::File::create(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(bytes)?;
        let _ = f.sync_data();
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Path of the per-ticker indexes directory (composite shards).
pub fn composite_dir(out_dir: &Path, base: &str, quote: &str) -> PathBuf {
    out_dir
        .join("indexes")
        .join("composite")
        .join(format!("{}-{}", base.to_uppercase(), quote.to_uppercase()))
}

/// Path of the per-ticker bars directory (s10/renko/bars shards).
pub fn bars_dir(out_dir: &Path, base: &str, quote: &str) -> PathBuf {
    out_dir
        .join("bars")
        .join(base.to_uppercase())
        .join(format!(
            "{}{}",
            base.to_uppercase(),
            quote.to_uppercase()
        ))
}

/// Shard path for a given (ticker_dir, date, extension).
pub fn shard_path(ticker_dir: &Path, date: NaiveDate, ext: &str) -> PathBuf {
    ticker_dir.join(format!("{}.{}", date_stem(date), ext))
}

/// Manifest path inside a ticker directory.
pub fn manifest_path(ticker_dir: &Path) -> PathBuf {
    ticker_dir.join("manifest.json")
}

/// List existing daily shards in a ticker dir by extension, sorted by date.
pub fn list_shards(ticker_dir: &Path, ext: &str) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let mut out = Vec::new();
    if !ticker_dir.exists() {
        return Ok(out);
    }
    let suffix = format!(".{}", ext);
    for entry in fs::read_dir(ticker_dir)
        .with_context(|| format!("read_dir {}", ticker_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if !name.ends_with(&suffix) {
            continue;
        }
        let stem = &name[..name.len() - suffix.len()];
        if let Ok(d) = NaiveDate::parse_from_str(stem, "%Y-%m-%d") {
            out.push((d, path));
        }
    }
    out.sort_by_key(|(d, _)| *d);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_bucket_key_round_trips() {
        // 2024-05-21 00:00:00 UTC = 1716249600000 ms
        let d = ts_ms_to_utc_date(1_716_249_600_000);
        assert_eq!(date_stem(d), "2024-05-21");
        // Last ms of the day stays in the same bucket.
        let d2 = ts_ms_to_utc_date(1_716_249_600_000 + 86_399_999);
        assert_eq!(date_stem(d2), "2024-05-21");
        // First ms of next day rolls.
        let d3 = ts_ms_to_utc_date(1_716_249_600_000 + 86_400_000);
        assert_eq!(date_stem(d3), "2024-05-22");
    }

    #[test]
    fn manifest_upsert_sorts_by_date() {
        let mut m = Manifest::new("BTC-USDT".into(), 0, "idx");
        m.upsert(ShardEntry {
            date: "2024-05-21".into(),
            first_ts: 1_716_249_600_000,
            last_ts: 1_716_335_999_999,
            n_records: 100,
            size_bytes: 5_600,
            sha256: "a".into(),
        });
        m.upsert(ShardEntry {
            date: "2024-05-20".into(),
            first_ts: 1_716_163_200_000,
            last_ts: 1_716_249_599_999,
            n_records: 200,
            size_bytes: 11_200,
            sha256: "b".into(),
        });
        assert_eq!(m.shards[0].date, "2024-05-20");
        assert_eq!(m.shards[1].date, "2024-05-21");
        assert_eq!(m.first_ts, 1_716_163_200_000);
        assert_eq!(m.last_ts, 1_716_335_999_999);
    }

    #[test]
    fn write_shard_atomic_creates_file() {
        let tmp_root = std::env::temp_dir().join("sf_shard_atomic_test");
        let _ = fs::remove_dir_all(&tmp_root);
        let ticker_dir = composite_dir(&tmp_root, "BTC", "USDT");
        let p = shard_path(&ticker_dir, NaiveDate::from_ymd_opt(2024, 5, 21).unwrap(), "idx");
        write_shard_atomic(&p, b"hello").unwrap();
        let bytes = fs::read(&p).unwrap();
        assert_eq!(bytes, b"hello");
        let _ = fs::remove_dir_all(&tmp_root);
    }
}
