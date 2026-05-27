//! Migrate all on-disk market data into the canonical MITCH-ID daily-sharded
//! layout and glue historical + live into one continuous series.
//!
//! ```text
//! BEFORE                                          AFTER
//! indexes/<id>.idx               (live, flat)  ┐
//! indexes/composite/<BASE-QUOTE>/<date>.idx    ┼─→ indexes/<id>/<date>.idx
//!                                (backfill)    ┘
//! bars/<BASE>/<BASEQUOTE>/<date>.{s10,renko}   ──→ bars/<id>/<date>.{s10,renko}
//! ```
//!
//! ## Properties
//! - **Self-identifying idx**: every `IndexRecord` carries its `ticker_id` in
//!   the body (`index.ticker`), so idx migration needs no name→id resolution —
//!   the flat file name and the composite record agree by construction.
//! - **Glue**: per ticker, backfill (older) + live (newer) records are merged,
//!   sorted by observation ts, and fed through the delta-gate so the seam is
//!   seamless and deduped.
//! - **Idempotent + incremental**: each ticker self-watermarks from the max ts
//!   already present in `indexes/<id>/`. Re-running ingests only records newer
//!   than the watermark — so the same tool does the bulk pass (old pod live)
//!   AND the delta pass (after the old pod stops, ≤10s cutover window).
//! - **2-year retention**: records/bars older than `--cutoff-days` (default
//!   730) are not migrated, realizing the "keep only 2 years" mandate (legacy
//!   files hold the rest until `--purge-legacy`).
//! - **Torn-write healing**: `read_shard_aligned` truncates a partial trailing
//!   record (the PAXG `short read` corruption) instead of failing.
//!
//! ## Usage
//! ```sh
//! migrate-to-sharded --data-root /data --report-only        # dry run, counts only
//! migrate-to-sharded --data-root /data                       # bulk migrate (idx+bars)
//! migrate-to-sharded --data-root /data                       # re-run = delta pass
//! migrate-to-sharded --data-root /data --purge-legacy        # after verify: drop legacy
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

use nxr_sdk::shard::{self, IdxShardWriter, ShardRecord, FLAG_HISTORICAL_BACKFILL, MS_PER_DAY};
use nxr_sdk::{resolve_ticker_id, Bar, IndexRecord};

#[derive(Parser, Debug)]
#[command(about = "Unify market data into MITCH-ID daily-sharded layout")]
struct Args {
    /// Data root containing `indexes/` and `bars/`.
    #[arg(long, default_value = "/data")]
    data_root: PathBuf,
    /// Retention cap in days; older data is not migrated (default 730 = 2y).
    #[arg(long, default_value_t = 730)]
    cutoff_days: i64,
    /// Optional comma-separated ticker_id allowlist (default: all).
    #[arg(long)]
    tickers: Option<String>,
    /// Migrate only the `.idx` index streams.
    #[arg(long)]
    idx_only: bool,
    /// Migrate only the `.s10`/`.renko` bars.
    #[arg(long)]
    bars_only: bool,
    /// Dry run: report counts, write nothing.
    #[arg(long)]
    report_only: bool,
    /// Rebuild each ticker's `<id>/` dir from scratch (ignores watermark).
    #[arg(long)]
    force: bool,
    /// After migration, delete legacy layout (flat `<id>.idx`+`.bak`,
    /// `indexes/composite/`, `bars/<BASE>/`). Destructive; run only post-verify.
    #[arg(long)]
    purge_legacy: bool,
}

/// Max observation ts already migrated into `indexes/<id>/` (for idempotent
/// incremental runs). `i64::MIN` when the ticker has no shards yet.
fn idx_watermark(idx_dir: &Path) -> i64 {
    shard::list_shards(idx_dir, "idx")
        .ok()
        .and_then(|s| s.last().map(|(_, p)| p.clone()))
        .and_then(|p| shard::read_shard_aligned::<IndexRecord>(&p).ok())
        .and_then(|r| r.last().map(|x| x.shard_ts_ms()))
        .unwrap_or(i64::MIN)
}

fn parse_ticker_filter(s: &Option<String>) -> Option<Vec<u64>> {
    s.as_ref().map(|raw| {
        raw.split(',')
            .filter_map(|t| t.trim().parse::<u64>().ok())
            .collect()
    })
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();
    let root = &args.data_root;
    let cutoff_ms = nxr_sdk::now_ms() as i64 - args.cutoff_days * MS_PER_DAY;
    let allow = parse_ticker_filter(&args.tickers);
    let allowed = |id: u64| allow.as_ref().map(|v| v.contains(&id)).unwrap_or(true);

    println!(
        "migrate-to-sharded: root={} cutoff={}d report_only={} force={} purge={}",
        root.display(),
        args.cutoff_days,
        args.report_only,
        args.force,
        args.purge_legacy
    );

    if !args.bars_only {
        migrate_idx(root, cutoff_ms, &allowed, args.report_only, args.force)?;
    }
    if !args.idx_only {
        migrate_bars(root, cutoff_ms, &allowed, args.report_only, args.force)?;
    }
    if args.purge_legacy && !args.report_only {
        purge_legacy(root)?;
    }
    Ok(())
}

/// Glue + reshape all index data into `indexes/<id>/<date>.idx`.
fn migrate_idx(
    root: &Path,
    cutoff_ms: i64,
    allowed: &dyn Fn(u64) -> bool,
    report_only: bool,
    force: bool,
) -> Result<()> {
    let indexes = root.join("indexes");

    // Discover the ticker set + each ticker's source paths WITHOUT loading any
    // record bodies. Flat live file: indexes/<id>.idx. Backfill: composite/
    // <BASE-QUOTE>/ — self-identifying (read 1 record of its newest shard to
    // get the ticker_id).
    let mut flat: BTreeMap<u64, PathBuf> = BTreeMap::new();
    if indexes.is_dir() {
        for e in fs::read_dir(&indexes).with_context(|| format!("read_dir {}", indexes.display()))? {
            let p = e?.path();
            if !p.is_file() {
                continue;
            }
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.ends_with(".idx") {
                continue; // skips .idx.bak
            }
            if let Ok(id) = name[..name.len() - 4].parse::<u64>() {
                flat.insert(id, p);
            }
        }
    }
    let mut composite: BTreeMap<u64, PathBuf> = BTreeMap::new();
    let comp_root = indexes.join("composite");
    if comp_root.is_dir() {
        for e in fs::read_dir(&comp_root)? {
            let dir = e?.path();
            if !dir.is_dir() {
                continue;
            }
            // Read the id from the first record of the newest shard (cheap).
            if let Some((_d, path)) = shard::list_shards(&dir, "idx")?.last() {
                if let Some(r) = shard::read_shard_aligned::<IndexRecord>(path)?.first() {
                    composite.insert(r.index.ticker, dir);
                }
            }
        }
    }

    let mut ids: Vec<u64> = flat.keys().chain(composite.keys()).copied().collect();
    ids.sort_unstable();
    ids.dedup();

    let mut total_written = 0usize;
    let mut total_seen = 0usize;
    for id in ids {
        if !allowed(id) {
            continue;
        }
        let idx_dir = shard::idx_dir(root, id);
        if force && idx_dir.exists() {
            fs::remove_dir_all(&idx_dir).ok();
        }
        let wm = idx_watermark(&idx_dir);

        // Live flat file (small, ts-ascending). Load it to learn live_start so
        // backfill that overlaps the live window is dropped (live is canonical).
        let flat_recs: Vec<IndexRecord> = match flat.get(&id) {
            Some(p) => shard::read_shard_aligned::<IndexRecord>(p)?,
            None => Vec::new(),
        };
        let live_start = flat_recs.first().map(|r| r.shard_ts_ms()).unwrap_or(i64::MAX);

        // Open writer once per ticker (report_only: simulate the gate inline so
        // we never write). The writer seeds last_bid/ask from the tail (idempotent).
        let mut writer = if report_only {
            None
        } else {
            // manifest=false: skip per-rotation manifest rebuild/sha256/fsync.
            // Feeding 2y of data is ~730 daily rotations/ticker; refreshing a
            // growing manifest.json with an fsync each time is O(n^2) churn on
            // DRBD. The API reads via list_shards (manifest-free); a manifest can
            // be rebuilt cheaply afterward if needed.
            //
            // R1 H9: writer-lock contention means the live aggregator currently
            // owns this ticker's stream. Migration is an offline tool — skip the
            // ticker rather than failing the whole run, and let the operator
            // re-run after scaling deploy/nxr to 0 if a full backfill is needed.
            match IdxShardWriter::open_with(root, id, true, false) {
                Ok(w) => Some(w),
                Err(e) => {
                    let msg = format!("{:#}", e);
                    if msg.contains("writer-lock") {
                        tracing::warn!(ticker_id = id, err = %msg,
                            "skip: live aggregator holds writer-lock; scale deploy/nxr=0 to migrate");
                        continue;
                    }
                    return Err(e).with_context(|| format!("open writer {id}"));
                }
            }
        };
        // Inline gate-simulation state for report mode (mirrors IdxShardWriter:
        // drop only when the full market tuple (bid,ask,vbid,vask,ci) repeats).
        let mut sim_last: Option<(f64, f64, u32, u32, u16)> = None;
        let mut sim_date: Option<chrono::NaiveDate> = None;
        let mut written = 0usize;
        let mut seen = 0usize;

        let mut feed = |rec: &IndexRecord,
                        writer: &mut Option<IdxShardWriter>,
                        written: &mut usize,
                        seen: &mut usize|
         -> Result<()> {
            let ts = rec.shard_ts_ms();
            if ts < cutoff_ms || ts <= wm {
                return Ok(());
            }
            *seen += 1;
            if let Some(w) = writer.as_mut() {
                // R1 H12: tag every migrated record as historical-backfill.
                // This (a) makes the offline-replay origin self-describing
                // for downstream consumers and (b) exempts the row from the
                // R1 H1 future/ancient ts guard on IdxShardWriter (legitimate
                // multi-year backfill writes ts far older than now-2y).
                let mut tagged = *rec;
                tagged.index.flags |= FLAG_HISTORICAL_BACKFILL;
                if w.append(&tagged)? {
                    *written += 1;
                }
            } else {
                // Replicate IdxShardWriter gate for the dry-run count.
                let date = shard::ts_ms_to_utc_date(ts);
                let body = rec.index;
                let (bid, ask, vbid, vask, ci) = (body.bid, body.ask, body.vbid, body.vask, body.ci);
                let new_day = sim_date != Some(date);
                let changed = sim_last
                    .map(|(b, a, vb, va, c)| bid != b || ask != a || vbid != vb || vask != va || ci != c)
                    .unwrap_or(true);
                if changed || new_day {
                    *written += 1;
                    sim_last = Some((bid, ask, vbid, vask, ci));
                }
                sim_date = Some(date);
            }
            Ok(())
        };

        // 1. Backfill (older), date-ordered shards, capped below live_start.
        if let Some(dir) = composite.get(&id) {
            for (_d, path) in shard::list_shards(dir, "idx")? {
                for rec in shard::read_shard_aligned::<IndexRecord>(&path)? {
                    if rec.shard_ts_ms() < live_start {
                        feed(&rec, &mut writer, &mut written, &mut seen)?;
                    }
                }
            }
        }
        // 2. Live flat (newer), canonical for its window.
        for rec in &flat_recs {
            feed(rec, &mut writer, &mut written, &mut seen)?;
        }

        if let Some(mut w) = writer {
            w.flush()?;
        }
        total_written += written;
        total_seen += seen;
        if seen > 0 {
            let verb = if report_only { "would write" } else { "wrote" };
            println!("  [idx] {id}: {seen} in-window → {verb} {written} (delta-gated)");
        }
    }
    let verb = if report_only { "would write" } else { "wrote" };
    println!("[idx] total: {total_seen} in-window, {verb} {total_written} (delta-gated, ~{:.1} MiB)",
        (total_written * 56) as f64 / 1_048_576.0);
    Ok(())
}

/// Re-key bars from `bars/<BASE>/<BASEQUOTE>/` to `bars/<id>/`. Bars carry no
/// ticker id, so resolve `BASE/QUOTE`→id and validate the id has a live index
/// (flat `<id>.idx` or sharded `indexes/<id>/`) to catch resolver mismatches.
fn migrate_bars(
    root: &Path,
    cutoff_ms: i64,
    allowed: &dyn Fn(u64) -> bool,
    report_only: bool,
    force: bool,
) -> Result<()> {
    let bars = root.join("bars");
    if !bars.is_dir() {
        return Ok(());
    }
    let cutoff_date = shard::ts_ms_to_utc_date(cutoff_ms);
    let mut copied = 0usize;
    let mut skipped = 0usize;

    for e in fs::read_dir(&bars)? {
        let base_dir = e?.path();
        if !base_dir.is_dir() {
            continue;
        }
        let base = base_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        // Skip the numeric <id>/ output dirs (idempotent re-runs).
        if base.parse::<u64>().is_ok() {
            continue;
        }
        for pe in fs::read_dir(&base_dir)? {
            let pair_dir = pe?.path();
            if !pair_dir.is_dir() {
                continue;
            }
            let pair = pair_dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !pair.starts_with(&base) || pair.len() <= base.len() {
                continue;
            }
            let quote = &pair[base.len()..];
            let sym = format!("{base}/{quote}");
            let id = resolve_ticker_id(&sym);
            if !allowed(id) {
                continue;
            }
            // Validate: a real ticker must have a live index stream.
            let has_index = root.join("indexes").join(format!("{id}.idx")).exists()
                || shard::idx_dir(root, id).exists();
            if !has_index {
                eprintln!(
                    "  [bars] WARN {sym} resolved to id {id} but no index exists — skipping (resolver mismatch?)"
                );
                skipped += 1;
                continue;
            }
            let dst_dir = shard::bars_dir(root, id);
            if force && dst_dir.exists() {
                fs::remove_dir_all(&dst_dir).ok();
            }
            for ext in ["s10", "renko"] {
                for (date, path) in shard::list_shards(&pair_dir, ext)? {
                    if date < cutoff_date {
                        continue;
                    }
                    let dst = shard::shard_path(&dst_dir, date, ext);
                    if dst.exists() && !force {
                        continue;
                    }
                    let bars_vec = shard::read_shard_aligned::<Bar>(&path)?;
                    if bars_vec.is_empty() {
                        continue;
                    }
                    if report_only {
                        println!(
                            "  [bars] {sym}→{id} {ext} {}: {} bars → {}",
                            shard::date_stem(date),
                            bars_vec.len(),
                            dst.display()
                        );
                        continue;
                    }
                    shard::write_shard_atomic(&dst, bytemuck::cast_slice(&bars_vec))?;
                    copied += 1;
                }
            }
            if !report_only {
                println!("  [bars] {sym}→{id}: shards re-keyed to {}", dst_dir.display());
            }
        }
    }
    println!("[bars] {copied} shards copied, {skipped} skipped (mismatch)");
    Ok(())
}

/// Delete the legacy layout after the sharded layout is verified. Destructive.
fn purge_legacy(root: &Path) -> Result<()> {
    let indexes = root.join("indexes");
    // flat <id>.idx + .bak
    if indexes.is_dir() {
        for e in fs::read_dir(&indexes)? {
            let p = e?.path();
            if !p.is_file() {
                continue;
            }
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.ends_with(".idx") || name.ends_with(".idx.bak") {
                let stem = name.trim_end_matches(".bak").trim_end_matches(".idx");
                if stem.parse::<u64>().is_ok() {
                    fs::remove_file(&p).ok();
                }
            }
        }
    }
    fs::remove_dir_all(indexes.join("composite")).ok();
    // bars/<BASE>/ alpha dirs (keep numeric <id>/)
    let bars = root.join("bars");
    if bars.is_dir() {
        for e in fs::read_dir(&bars)? {
            let p = e?.path();
            if p.is_dir() {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.parse::<u64>().is_err() {
                    fs::remove_dir_all(&p).ok();
                }
            }
        }
    }
    println!("[purge] legacy flat/composite/bars-by-base removed");
    Ok(())
}
