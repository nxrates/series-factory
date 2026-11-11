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

// ── Done-ness: what makes a day shard "already on disk" ─────────────────────

/// Days of `[from, to]` the manifest at `dir` does not vouch for: a day is done
/// only when the manifest lists it and the file on disk is exactly the recorded
/// size. A shard a crashed or interrupted
/// writer left behind differs from its entry (or has none) and is redone.
pub fn days_todo(dir: &Path, kind: &str, from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
    let manifest = read_manifest(&manifest_path(dir)).ok().flatten();
    let listed = |d: NaiveDate| {
        manifest.as_ref().is_some_and(|m| {
            let key = d.format("%Y-%m-%d").to_string();
            m.shards.iter().any(|e| {
                e.date == key
                    && std::fs::metadata(shard_path(dir, d, kind))
                        .is_ok_and(|f| f.len() == e.size_bytes)
            })
        })
    };
    let mut out = Vec::new();
    let mut d = from;
    while d <= to {
        if !listed(d) {
            out.push(d);
        }
        d += chrono::Duration::days(1);
    }
    out
}

/// Delete the given shard files and drop their manifest entries, so the days
/// are todo again.
pub fn unlist_shards(dir: &Path, shards: &[PathBuf]) {
    let stems: Vec<String> = shards
        .iter()
        .filter_map(|p| p.file_stem()?.to_str().map(String::from))
        .collect();
    for p in shards {
        let _ = std::fs::remove_file(p);
    }
    let path = manifest_path(dir);
    if let Ok(Some(mut m)) = read_manifest(&path) {
        m.shards.retain(|e| !stems.contains(&e.date));
        let _ = write_manifest(&path, &m);
    }
}

/// Which venues contributed to each finished composite day, beside the manifest.
/// A day finalised with fewer venues than were asked for is visible here, and can
/// be rebuilt later with `--rebuild`. Written atomically (temp + rename). `forget`
/// days (a rebuild that produced nothing) lose their entry. An unparseable file is
/// an error, never an empty history to overwrite.
pub fn record_venues(
    dir: &Path,
    days: &std::collections::BTreeMap<NaiveDate, Vec<String>>,
    forget: &[NaiveDate],
) -> Result<()> {
    let mut all = read_venues(dir)?;
    for d in forget {
        all.remove(&d.to_string());
    }
    all.extend(days.iter().map(|(d, v)| (d.to_string(), v.clone())));
    write_shard_atomic(&dir.join("venues.json"), &serde_json::to_vec_pretty(&all)?)
}

/// The recorded venues per day (`YYYY-MM-DD` -> venue names); empty when the file
/// does not exist, an error when it exists but cannot be read.
pub fn read_venues(dir: &Path) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let path = dir.join("venues.json");
    match std::fs::read(&path) {
        Ok(b) => serde_json::from_slice(&b)
            .with_context(|| format!("{} is corrupt: fix or remove it", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Default::default()),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// A derived shard is fresh when it is a whole, non-empty number of `rec_size`
/// records and not older than the `input` it was built from.
pub fn fresh(out: &Path, input: &Path, rec_size: u64) -> bool {
    let (Ok(o), Ok(i)) = (std::fs::metadata(out), std::fs::metadata(input)) else {
        return false;
    };
    o.len() > 0
        && o.len() % rec_size == 0
        && matches!((o.modified(), i.modified()), (Ok(a), Ok(b)) if a >= b)
}

#[cfg(test)]
mod done_tests {
    use super::*;

    fn day(d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2024, 1, d).unwrap()
    }

    /// Done = listed in the manifest and exactly as recorded; a partial, missing
    /// or unlisted shard is todo.
    #[test]
    fn days_todo_redoes_partial_missing_and_unlisted_shards() {
        let dir = std::env::temp_dir().join(format!("sf-done-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let write = |d: u32, n: usize| {
            std::fs::write(shard_path(&dir, day(d), "idx"), vec![1u8; n]).unwrap()
        };
        write(1, 56 * 3); // done
        write(2, 56 * 3); // done, then truncated mid-record below
        write(3, 56 * 3); // on disk, never listed
        let mut m = Manifest::new("T".into(), 1, "idx");
        let entry = |d: u32| ShardEntry {
            date: day(d).format("%Y-%m-%d").to_string(),
            first_ts: 0,
            last_ts: 0,
            n_records: 3,
            size_bytes: 56 * 3,
            sha256: String::new(),
        };
        m.set_shards_batch(vec![entry(1), entry(2), entry(4)]); // day 4 listed, file missing
        write_manifest(&manifest_path(&dir), &m).unwrap();
        write(2, 56 * 2 + 10);
        assert_eq!(
            days_todo(&dir, "idx", day(1), day(5)),
            [day(2), day(3), day(4), day(5)]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A shard that failed validation is deleted and unlisted: todo again.
    #[test]
    fn unlist_makes_a_done_day_todo_again() {
        let dir = std::env::temp_dir().join(format!("sf-unlist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = shard_path(&dir, day(1), "idx");
        std::fs::write(&p, vec![1u8; 56]).unwrap();
        let mut m = Manifest::new("T".into(), 1, "idx");
        m.set_shards_batch(vec![ShardEntry {
            date: "2024-01-01".into(),
            first_ts: 0,
            last_ts: 0,
            n_records: 1,
            size_bytes: 56,
            sha256: String::new(),
        }]);
        write_manifest(&manifest_path(&dir), &m).unwrap();
        assert!(days_todo(&dir, "idx", day(1), day(1)).is_empty());
        unlist_shards(&dir, &[p.clone()]);
        assert!(!p.exists());
        assert_eq!(days_todo(&dir, "idx", day(1), day(1)), [day(1)]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn venues_are_recorded_per_day_and_merged() {
        let dir = std::env::temp_dir().join(format!("sf-venues-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let one = |d: u32, v: &[&str]| {
            std::collections::BTreeMap::from([(day(d), v.iter().map(|s| s.to_string()).collect())])
        };
        record_venues(&dir, &one(1, &["binance", "okx"]), &[]).unwrap();
        record_venues(&dir, &one(2, &["binance"]), &[]).unwrap();
        record_venues(&dir, &one(1, &["binance", "okx", "bybit"]), &[]).unwrap();
        let got = read_venues(&dir).unwrap();
        assert_eq!(got["2024-01-01"].len(), 3);
        assert_eq!(got["2024-01-02"], ["binance"]);
        // a rebuild that produced nothing forgets the day
        record_venues(&dir, &Default::default(), &[day(2)]).unwrap();
        assert!(!read_venues(&dir).unwrap().contains_key("2024-01-02"));
        // a corrupt sidecar is never replaced by an empty history
        std::fs::write(dir.join("venues.json"), b"{ truncat").unwrap();
        assert!(record_venues(&dir, &one(3, &["binance"]), &[]).is_err());
        assert_eq!(
            std::fs::read(dir.join("venues.json")).unwrap(),
            b"{ truncat"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fresh_needs_whole_records_not_older_than_input() {
        let dir = std::env::temp_dir().join(format!("sf-fresh-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (i, o) = (dir.join("in"), dir.join("out"));
        std::fs::write(&i, b"x").unwrap();
        assert!(!fresh(&o, &i, 96), "missing output");
        std::fs::write(&o, vec![0u8; 100]).unwrap();
        assert!(!fresh(&o, &i, 96), "partial record");
        std::fs::write(&o, vec![0u8; 96]).unwrap();
        assert!(fresh(&o, &i, 96));
        std::fs::write(&i, b"newer").unwrap();
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&i)
            .unwrap()
            .set_modified(later)
            .unwrap();
        assert!(!fresh(&o, &i, 96), "input rebuilt after the output");
        let _ = std::fs::remove_dir_all(&dir);
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
    /// Exclusive `.writer.lock` on `out_dir`, held for this writer's whole life.
    ///
    /// The idempotent-truncate below `fs::remove_file`s the target shard. Without
    /// this lock that unlink raced the LIVE aggregator's `IdxShardWriter` for the
    /// same ticker: the offline run deletes today's shard, the live writer keeps
    /// appending to its now-unlinked fd, and every record the live process wrote
    /// today is gone at the next `list_shards`. `acquire_idx_writer_lock` is the
    /// same flock `IdxShardWriter::open` takes, so whichever side is already
    /// writing wins and the other fails loudly instead of destroying data.
    _writer_lock: std::fs::File,
    /// Cached `[start, end)` epoch-ms window of `current`'s UTC day. An in-window
    /// ts is provably same-day, so we skip the per-record `ts_ms_to_utc_date`
    /// chrono call and decide "no rotate" with two int compares. UTC days are
    /// exactly MS_PER_DAY ms so the window↔date mapping is exact. `(MAX, MIN)`
    /// sentinel = "no day yet" → first record takes the chrono path.
    cur_day_start_ms: i64,
    cur_day_end_ms: i64,
}

impl ShardedWriter {
    /// Fails when the live aggregator (or another offline job) already holds the
    /// ticker's writer lock — see [`Self::_writer_lock`]. Callers that iterate a
    /// roster should log and SKIP that ticker, never fall back to an unlocked
    /// write.
    pub fn new(out_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("create shard dir {}", out_dir.display()))?;
        let lock = nxr_sdk::shard::acquire_idx_writer_lock(&out_dir)
            .with_context(|| format!("writer-lock {}", out_dir.display()))?;
        Ok(Self {
            out_dir,
            current: None,
            touched: std::collections::HashSet::new(),
            cur_day_start_ms: i64::MAX,
            cur_day_end_ms: i64::MIN,
            _writer_lock: lock,
        })
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
            //
            // ⚠ NEVER unlink TODAY's shard. The writer lock above keeps the live
            // aggregator out of this ticker, but a shard for the current UTC day
            // is the one the live process appends to, and "replace" is not a
            // meaningful operation on a series that is still being written —
            // whatever this run rebuilds is immediately incomplete. Offline
            // rebuilds cover CLOSED days; the live tail is the aggregator's.
            let today = ts_ms_to_utc_date(nxr_sdk::now_ms() as i64);
            if date >= today {
                tracing::warn!(
                    shard = %path.display(), %date, %today,
                    "refusing to replace a current-day shard (live aggregator owns the tail); appending instead"
                );
                self.touched.insert(date);
            } else if self.touched.insert(date) {
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
