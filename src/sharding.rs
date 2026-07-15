//! Daily-shard helpers.
//!
//! Canonical shard primitives live in `nxr_sdk::shard` — single source of
//! truth for path math (MITCH-ID keyed), atomic writers, sha256, and manifest
//! read/write/merge. This module is now a re-export-only thin wrapper.
//!
//! Layout (canonical):
//! - indexes: `<root>/indexes/<MITCH_ID>/<YYYY-MM-DD>.idx`  (sdk `idx_dir`)
//! - bars:    `<root>/bars/<MITCH_ID>/<YYYY-MM-DD>.<ext>`  (sdk `bars_dir`)
//!
//! The legacy `composite/<BASE>-<QUOTE>/` + `<BASE>/<BASE><QUOTE>/` pair-keyed
//! directories are REMOVED. All call sites use `nxr_sdk::shard::{idx_dir,bars_dir}`
//! with `nxr_sdk::resolve_ticker_id(<sym>)`.

// Re-export the canonical primitives so existing call sites keep compiling
// unchanged. These are the single source of truth in `nxr_sdk::shard`.
pub use nxr_sdk::shard::{
    bars_dir, date_stem, idx_dir, list_shards, manifest_path, read_manifest, sha256_file,
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
        let p = shard_path(
            &ticker_dir,
            NaiveDate::from_ymd_opt(2024, 5, 21).unwrap(),
            "idx",
        );
        write_shard_atomic(&p, b"hello").unwrap();
        let bytes = fs::read(&p).unwrap();
        assert_eq!(bytes, b"hello");
        let _ = fs::remove_dir_all(&tmp_root);
    }
}

// ── Shared offline .idx shard writing (extracted from merge_idx 2026-07-08) ──

use anyhow::{Context, Result};
use chrono::NaiveDate;
use nxr_sdk::ipc::append_log::AppendLog;
use nxr_sdk::ipc::record::IndexRecord;
use std::path::{Path, PathBuf};

/// Per-day AppendLog rotator. Rolls on UTC date boundary.
///
/// IDEMPOTENT (2026-07-05, D10): the first touch of each date within a run
/// TRUNCATES the target shard before writing. AppendLog opens in append mode,
/// so re-running merge over an existing dir used to APPEND a duplicate day
/// after the previous content — the root cause of the 2026-07-05 prod
/// append-corruption incident (60 BTC shards) and of doubled staging output.
/// The touched-set (not blanket truncate-on-rotate) matters: a near-midnight
/// straggler can rotate BACK to the prior date mid-run, which must append to
/// this run's own output, not wipe it.
pub struct ShardedWriter {
    out_dir: PathBuf,
    current: Option<(NaiveDate, AppendLog<IndexRecord>)>,
    /// Dates already opened (and truncated) by THIS run.
    touched: std::collections::HashSet<NaiveDate>,
    /// Cached `[start, end)` epoch-ms window of `current`'s UTC day. An in-window
    /// ts is provably same-day, so we skip the per-record `ts_ms_to_utc_date`
    /// chrono call and decide "no rotate" with two int compares. UTC days are
    /// exactly MS_PER_DAY ms so the window↔date mapping is exact. `(MAX, MIN)`
    /// sentinel = "no day yet" → first record takes the chrono path.
    cur_day_start_ms: i64,
    cur_day_end_ms: i64,
}

impl ShardedWriter {
    pub fn new(out_dir: PathBuf) -> Self {
        Self {
            out_dir,
            current: None,
            touched: std::collections::HashSet::new(),
            cur_day_start_ms: i64::MAX,
            cur_day_end_ms: i64::MIN,
        }
    }

    pub fn append(&mut self, ts_ms: i64, rec: &IndexRecord) -> Result<()> {
        // Same-day fast path: in-window ts ⇒ no rotation, no chrono conversion.
        if ts_ms >= self.cur_day_start_ms && ts_ms < self.cur_day_end_ms && self.current.is_some() {
            self.current.as_mut().unwrap().1.append(rec)?;
            return Ok(());
        }
        let date = ts_ms_to_utc_date(ts_ms);
        let need_rotate = match &self.current {
            Some((d, _)) => *d != date,
            None => true,
        };
        if need_rotate {
            // close prior shard ∵ AppendLog has Drop fsync
            self.current = None;
            // Refresh cached day window in lock-step with the new shard date.
            self.cur_day_start_ms = nxr_sdk::shard::day_start_ms(date);
            self.cur_day_end_ms = self.cur_day_start_ms + nxr_sdk::shard::MS_PER_DAY;
            let path = shard_path(&self.out_dir, date, "idx");
            // Idempotency: first touch of this date in THIS run replaces any
            // pre-existing shard (re-runs must converge, never append-double).
            if self.touched.insert(date) {
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        tracing::info!(shard = %path.display(), "replacing existing shard (idempotent re-run)")
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(e).with_context(|| format!("truncate shard {}", path.display()))
                    }
                }
            }
            // OFFLINE builder: buffered AppendLog (256 KiB BufWriter). The merge
            // output shard is not tailed by any live reader until the build is
            // done, so coalescing the per-record 56B writes is a pure win.
            // (The LIVE aggregator path keeps the unbuffered `open`.)
            let log = AppendLog::<IndexRecord>::open_buffered(&path)
                .with_context(|| format!("open shard {}", path.display()))?;
            self.current = Some((date, log));
        }
        self.current.as_mut().unwrap().1.append(rec)?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if let Some((_, mut log)) = self.current.take() {
            log.flush()?;
        }
        Ok(())
    }
}

/// Build a manifest entry for a `.idx` shard by streaming the file once.
pub fn shard_entry_for_idx(date: NaiveDate, path: &Path) -> Result<ShardEntry> {
    use std::io::Read;
    let rec_size = core::mem::size_of::<IndexRecord>();
    let size_bytes = std::fs::metadata(path)?.len();
    let n_records = if rec_size == 0 {
        0
    } else {
        size_bytes / rec_size as u64
    };
    let mut f = std::fs::File::open(path)?;
    let mut first_ts: i64 = i64::MAX;
    let mut last_ts: i64 = i64::MIN;
    let mut buf = vec![0u8; 4096 * rec_size];
    loop {
        let mut filled = 0usize;
        while filled < buf.len() {
            match f.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        if filled == 0 {
            break;
        }
        if filled % rec_size != 0 {
            anyhow::bail!("shard {} not aligned to IndexRecord", path.display());
        }
        let recs: &[IndexRecord] = bytemuck::cast_slice(&buf[..filled]);
        if let Some(r) = recs.first() {
            let ts = mitch::timestamp::to_epoch_ms(r.header.get_timestamp());
            if ts < first_ts {
                first_ts = ts;
            }
        }
        if let Some(r) = recs.last() {
            let ts = mitch::timestamp::to_epoch_ms(r.header.get_timestamp());
            if ts > last_ts {
                last_ts = ts;
            }
        }
        if filled < buf.len() {
            break;
        }
    }
    if n_records == 0 {
        first_ts = 0;
        last_ts = 0;
    }
    Ok(ShardEntry {
        date: date.format("%Y-%m-%d").to_string(),
        first_ts,
        last_ts,
        n_records,
        size_bytes,
        sha256: sha256_file(path)?,
    })
}
