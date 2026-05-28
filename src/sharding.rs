//! Daily-shard helpers. See `docs/sharding-spec.md`.
//!
//! Canonical shard primitives live in `nxr_sdk::shard` — single source of
//! truth for path math (MITCH-ID keyed), atomic writers, sha256, and manifest
//! read/write/merge. This module is now a re-export-only thin wrapper.
//!
//! Layout (canonical, post-U3/U4):
//! - indexes: `<root>/indexes/<MITCH_ID>/<YYYY-MM-DD>.idx`  (sdk `idx_dir`)
//! - bars:    `<root>/bars/<MITCH_ID>/<YYYY-MM-DD>.<ext>`  (sdk `bars_dir`)
//!
//! The legacy `composite/<BASE>-<QUOTE>/` + `<BASE>/<BASE><QUOTE>/` pair-keyed
//! directories are REMOVED (operator mandate 2026-05-27: composite/ "should
//! not even exist"). All call sites use `nxr_sdk::shard::{idx_dir,bars_dir}`
//! with `nxr_sdk::resolve_ticker_id(<sym>)`.

// Re-export the canonical primitives so existing call sites keep compiling
// unchanged. These are the single source of truth in `nxr_sdk::shard`.
pub use nxr_sdk::shard::{
    date_stem, idx_dir, bars_dir, list_shards, manifest_path, read_manifest, sha256_file,
    shard_path, ts_ms_to_utc_date, write_manifest, write_shard_atomic, Manifest, ShardEntry,
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use std::fs;
    use std::path::Path;

    #[test]
    fn idx_dir_uses_mitch_id_layout() {
        // BTC/USDT MITCH ID — deterministic via sdk resolver.
        let id = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let p = idx_dir(Path::new("/data"), id);
        assert_eq!(p, Path::new("/data/indexes").join(id.to_string()));
    }

    #[test]
    fn write_shard_atomic_creates_file() {
        let tmp_root = std::env::temp_dir().join("sf_shard_atomic_test");
        let _ = fs::remove_dir_all(&tmp_root);
        let id = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let ticker_dir = idx_dir(&tmp_root, id);
        std::fs::create_dir_all(&ticker_dir).unwrap();
        let p = shard_path(&ticker_dir, NaiveDate::from_ymd_opt(2024, 5, 21).unwrap(), "idx");
        write_shard_atomic(&p, b"hello").unwrap();
        let bytes = fs::read(&p).unwrap();
        assert_eq!(bytes, b"hello");
        let _ = fs::remove_dir_all(&tmp_root);
    }
}
