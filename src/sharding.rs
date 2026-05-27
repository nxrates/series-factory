//! Daily-shard helpers. See `docs/sharding-spec.md`.
//!
//! Spec: `docs/sharding-spec.md`. The canonical shard-storage primitives now
//! live in the `nxr-sdk` crate at `nxr_sdk::shard` (single source of truth for
//! path math, atomic shard/manifest writers, sha256, and the manifest
//! read/write/merge logic). This module re-exports those and adds only the two
//! *legacy* pair-keyed path helpers that the sdk does not provide:
//!
//! - [`composite_dir`] → `<out>/indexes/composite/<BASE>-<QUOTE>`
//! - [`bars_dir_pair`] → `<out>/bars/<BASE>/<BASE><QUOTE>`
//!
//! The sdk's id-keyed `bars_dir(data_root, ticker_id)` is a *different* layout
//! (`<root>/bars/<MITCH_ID>`); the pair-keyed helper is intentionally named
//! `bars_dir_pair` to avoid confusion.

use std::path::{Path, PathBuf};

// Re-export the canonical primitives so existing call sites keep compiling
// unchanged. These are the single source of truth in `nxr_sdk::shard`.
pub use nxr_sdk::shard::{
    date_stem, list_shards, manifest_path, read_manifest, sha256_file, shard_path, ts_ms_to_utc_date,
    write_manifest, write_shard_atomic, Manifest, ShardEntry,
};

/// Legacy per-ticker indexes directory (pair-keyed composite shards):
/// `<out>/indexes/composite/<BASE>-<QUOTE>`.
pub fn composite_dir(out_dir: &Path, base: &str, quote: &str) -> PathBuf {
    out_dir
        .join("indexes")
        .join("composite")
        .join(format!("{}-{}", base.to_uppercase(), quote.to_uppercase()))
}

/// Legacy per-ticker bars directory (pair-keyed s10/renko/bars shards):
/// `<out>/bars/<BASE>/<BASE><QUOTE>`. Distinct from the sdk's id-keyed
/// `bars_dir(data_root, ticker_id)`.
pub fn bars_dir_pair(out_dir: &Path, base: &str, quote: &str) -> PathBuf {
    out_dir
        .join("bars")
        .join(base.to_uppercase())
        .join(format!("{}{}", base.to_uppercase(), quote.to_uppercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use std::fs;

    #[test]
    fn composite_dir_uses_legacy_pair_layout() {
        let p = composite_dir(Path::new("/data"), "btc", "usdt");
        assert_eq!(p, Path::new("/data/indexes/composite/BTC-USDT"));
    }

    #[test]
    fn bars_dir_pair_uses_legacy_pair_layout() {
        let p = bars_dir_pair(Path::new("/data"), "btc", "usdt");
        assert_eq!(p, Path::new("/data/bars/BTC/BTCUSDT"));
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
