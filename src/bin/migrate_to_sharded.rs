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

use nxr_sdk::shard::{self, IdxShardWriter, ShardRecord};
use nxr_sdk::{resolve_ticker_id, Bar, IndexRecord};

const MS_PER_DAY: i64 = 86_400_000;

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

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
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
    let cutoff_ms = now_ms() - args.cutoff_days * MS_PER_DAY;
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
    let mut per_id: BTreeMap<u64, Vec<IndexRecord>> = BTreeMap::new();

    // 1. Live flat files: indexes/<id>.idx (skip .bak and the <id>/ subdirs).
    if indexes.is_dir() {
        for e in fs::read_dir(&indexes).with_context(|| format!("read_dir {}", indexes.display()))? {
            let p = e?.path();
            if !p.is_file() {
                continue;
            }
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.ends_with(".idx") {
                continue; // skips .idx.bak (ends_with .bak)
            }
            let stem = &name[..name.len() - 4];
            if let Ok(id) = stem.parse::<u64>() {
                if !allowed(id) {
                    continue;
                }
                let recs = shard::read_shard_aligned::<IndexRecord>(&p)?;
                per_id.entry(id).or_default().extend(recs);
            }
        }
    }

    // 2. Backfill composite shards: records self-identify via index.ticker.
    let composite = indexes.join("composite");
    if composite.is_dir() {
        for e in fs::read_dir(&composite)? {
            let dir = e?.path();
            if !dir.is_dir() {
                continue;
            }
            for (_d, path) in shard::list_shards(&dir, "idx")? {
                for r in shard::read_shard_aligned::<IndexRecord>(&path)? {
                    let id = r.index.ticker;
                    if allowed(id) {
                        per_id.entry(id).or_default().push(r);
                    }
                }
            }
        }
    }

    let mut total_in = 0usize;
    let mut total_written = 0usize;
    for (id, mut recs) in per_id {
        let idx_dir = shard::idx_dir(root, id);
        if force && idx_dir.exists() {
            fs::remove_dir_all(&idx_dir).ok();
        }
        let wm = idx_watermark(&idx_dir);
        recs.retain(|r| {
            let ts = r.shard_ts_ms();
            ts >= cutoff_ms && ts > wm
        });
        recs.sort_by_key(|r| r.shard_ts_ms());
        total_in += recs.len();
        if recs.is_empty() {
            continue;
        }
        if report_only {
            println!(
                "  [idx] {id}: {} new records (>{wm}), would write to {}",
                recs.len(),
                idx_dir.display()
            );
            continue;
        }
        let mut writer = IdxShardWriter::open(root, id, true)
            .with_context(|| format!("open writer for {id}"))?;
        let mut written = 0usize;
        for r in &recs {
            if writer.append(r)? {
                written += 1;
            }
        }
        writer.flush()?;
        total_written += written;
        println!(
            "  [idx] {id}: ingested {} records → {written} written (delta-gated) in {}",
            recs.len(),
            idx_dir.display()
        );
    }
    println!("[idx] total: {total_in} ingested, {total_written} written (delta-gated)");
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
