//! Glue/join validator for the backfill ↔ live `.idx` seam.
//!
//! ## Modes
//!
//! **Sharded mode (default, R1 H13 — post-U4 layout):**
//! - One ticker_id → many `<data_root>/indexes/<ticker_id>/<YYYY-MM-DD>.idx` shards.
//! - Validates:
//!   1. Per-shard monotonicity (`ts` non-decreasing inside each file).
//!   2. Cross-shard continuity (last ts of day D vs first ts of D+1 — no
//!      missing date-shards in the date range present).
//!   3. Inter-record gap ≤ `SENTINEL_INTERVAL_MS + 30_000` ms = 90 s (the
//!      live writer emits a sentinel every 60 s when quotes are quiet; a
//!      gap > 90 s with no sentinel inside indicates a real outage).
//!
//! **Legacy flat mode (`--legacy-flat`, pre-U4):**
//! - Two flat `.idx` files per ticker:
//!     `<backfill-dir>/<BASE>-<QUOTE>.idx` (backfilled composite history)
//!     `<live-dir>/<ticker_id>.idx`        (live aggregator output)
//! - Validates the original cohort: backfill monotonicity, no gap at the
//!   join, sample-checked overlap drift (price < 1%, CI < 50%). Inter-record
//!   gap threshold is `2 * cycle_ms` (typically 200 ms).
//!
//! Per-ticker JSON report + aggregate report in `--all` mode. Each ticker's
//! check runs inside `catch_unwind` so one corrupted file does not abort the
//! batch.
//!
//! ## Usage
//!
//! Sharded:
//!   glue-check --ticker-id 12345 --data-root /data
//!   glue-check --all --data-root /data --report /data/glue/last.json
//!
//! Legacy flat:
//!   glue-check --legacy-flat <ticker_id_or_pair>
//!     --backfill-dir /data/backfill/composite
//!     --live-dir     /data/indexes
//!
//! Exit code: 0 if 0 errors; 1 if any errors.

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mitch::timestamp;
use nxr_sdk::shard::{
    idx_dir, list_shards, read_shard_aligned, ShardRecord, SENTINEL_INTERVAL_MS,
};
use nxr_sdk::{ipc::record::IndexRecord, resolve_ticker_id, tdwap::decode_ci_ubp};
use serde::Serialize;
use std::path::PathBuf;
use tracing::{info, warn};

/// Inter-record-gap budget for the sharded mode: 60 s sentinel cadence + 30 s
/// jitter budget (process scheduling, fsync slack, day-boundary rotate). A
/// gap exceeding this with no sentinel inside means the live writer was down.
const SHARDED_GAP_MS: i64 = SENTINEL_INTERVAL_MS + 30_000;

#[derive(Parser, Debug)]
#[command(about = "Validate .idx continuity for one or all tickers (sharded layout default; --legacy-flat for pre-U4).")]
struct Args {
    /// Sharded mode: decimal MITCH ticker id (e.g. 12345). Mutually exclusive
    /// with `--all`. Ignored in `--legacy-flat` mode (use positional `ticker`).
    #[arg(long)]
    ticker_id: Option<u64>,

    /// Sharded mode: data root (contains `indexes/<id>/<YYYY-MM-DD>.idx`).
    #[arg(long, default_value = "/data")]
    data_root: PathBuf,

    /// Legacy mode: `<BASE>-<QUOTE>` (e.g. `BTC-USDT`) or decimal ticker id.
    /// Required unless `--all` (and `--legacy-flat`).
    ticker: Option<String>,

    /// Legacy mode: dir holding backfill composite `<BASE>-<QUOTE>.idx` files.
    #[arg(long)]
    backfill_dir: Option<PathBuf>,

    /// Legacy mode: dir holding live `<ticker_id>.idx` files.
    #[arg(long)]
    live_dir: Option<PathBuf>,

    /// Use the pre-U4 flat-file mode. Default = sharded.
    #[arg(long)]
    legacy_flat: bool,

    /// Legacy mode: aggregator cycle ms (defines the no-gap/no-overlap window).
    #[arg(long, default_value = "100")]
    cycle_ms: i64,

    /// Legacy mode: cap on overlap-window records before the diff sweep truncates.
    #[arg(long, default_value = "1000")]
    max_overlap_records: usize,

    /// Legacy mode: sample size for uniformly-spaced timestamp probes.
    #[arg(long, default_value = "100")]
    sample_size: usize,

    /// Max distance (ms) between a probe target_ts and the nearest record before
    /// the sample is treated as landing in a data gap and skipped from drift
    /// stats. Legacy mode only.
    #[arg(long, default_value = "10000")]
    stale_threshold_ms: i64,

    /// Emit single-ticker output as JSON to stdout (default = human text).
    #[arg(long)]
    json: bool,

    /// Iterate every ticker. In sharded mode, enumerates `<data_root>/indexes/<id>/`
    /// dirs. In legacy mode, enumerates `<backfill-dir>/*.idx` files.
    #[arg(long)]
    all: bool,

    /// In `--all` mode, write aggregate JSON report to this path.
    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
struct TickerReport {
    ticker: String,
    ticker_id: Option<u64>,
    /// Legacy mode only: ts of the last backfill record (join boundary).
    #[serde(default)]
    t_cut: i64,
    /// Legacy mode only: ts_live[0] - t_cut.
    #[serde(default)]
    gap_ms: i64,
    /// Legacy mode only: overlap records considered.
    #[serde(default)]
    overlap_records: usize,
    /// Legacy mode only: max bp drift across overlap samples.
    #[serde(default)]
    max_price_diff_bps: f64,
    /// Legacy mode only: max CI drift across overlap samples.
    #[serde(default)]
    max_ci_diff_pct: f64,
    /// Sharded + legacy: index of the record breaking monotonicity, if any.
    monotone_violation_ix: Option<usize>,
    /// Sharded mode only: number of distinct date-shards present.
    #[serde(default)]
    shards_present: usize,
    /// Sharded mode only: number of date-shards missing between min..max
    /// (i.e. holes in the calendar coverage).
    #[serde(default)]
    shards_missing: usize,
    /// Sharded mode only: largest inter-record dt encountered (ms).
    #[serde(default)]
    max_intra_gap_ms: i64,
    /// Sharded mode only: number of inter-record gaps > SHARDED_GAP_MS.
    #[serde(default)]
    gap_violations: usize,
    /// "ok" | "gap" | "overlap_drift" | "monotone_violation" | "missing_live" |
    /// "missing_backfill" | "missing_shards" | "empty" | "insufficient_samples" |
    /// "error"
    status: String,
    note: Option<String>,
    #[serde(default)]
    live_outage_records: u32,
    #[serde(default)]
    backfill_outage_records: u32,
    #[serde(default)]
    valid_sample_records: u32,
}

#[derive(Debug, Serialize)]
struct AggregateReport {
    total: usize,
    checked: usize,
    passed: usize,
    warned: usize,
    errored: usize,
    tickers: Vec<TickerReport>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();

    if args.legacy_flat {
        return run_legacy(&args);
    }
    run_sharded(&args)
}

// ─── Sharded mode ────────────────────────────────────────────────────────────

fn run_sharded(args: &Args) -> Result<()> {
    if args.all {
        let agg = run_sharded_all(args)?;
        emit_aggregate(&agg, args)?;
        if agg.errored > 0 {
            std::process::exit(1);
        }
        return Ok(());
    }
    let id = args.ticker_id.ok_or_else(|| {
        anyhow!("missing --ticker-id (or pass --all); use --legacy-flat for the old flat-file layout")
    })?;
    let report = check_sharded_safe(id, &args.data_root);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human(&report);
    }
    if matches!(
        report.status.as_str(),
        "monotone_violation" | "missing_shards" | "gap" | "error"
    ) {
        std::process::exit(1);
    }
    Ok(())
}

fn run_sharded_all(args: &Args) -> Result<AggregateReport> {
    let indexes_root = args.data_root.join("indexes");
    let mut ids: Vec<u64> = Vec::new();
    if indexes_root.exists() {
        for entry in std::fs::read_dir(&indexes_root)
            .with_context(|| format!("read_dir {}", indexes_root.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if let Ok(id) = name.parse::<u64>() {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();

    let total = ids.len();
    let mut tickers = Vec::with_capacity(total);
    let mut checked = 0usize;
    let mut passed = 0usize;
    let mut warned = 0usize;
    let mut errored = 0usize;

    for id in ids {
        let r = check_sharded_safe(id, &args.data_root);
        match r.status.as_str() {
            "ok" => {
                checked += 1;
                passed += 1;
            }
            "empty" | "missing_live" => {
                checked += 1;
                warned += 1;
            }
            _ => {
                checked += 1;
                errored += 1;
            }
        }
        tickers.push(r);
    }

    Ok(AggregateReport {
        total,
        checked,
        passed,
        warned,
        errored,
        tickers,
    })
}

fn check_sharded_safe(ticker_id: u64, data_root: &std::path::Path) -> TickerReport {
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_sharded(ticker_id, data_root)
    }));
    match res {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => mk_err(&ticker_id.to_string(), Some(ticker_id), format!("error: {}", e)),
        Err(panic) => {
            let msg = downcast_panic(&panic);
            warn!(ticker_id, panic = %msg, "check panicked");
            mk_err(&ticker_id.to_string(), Some(ticker_id), format!("panic: {}", msg))
        }
    }
}

fn check_sharded(ticker_id: u64, data_root: &std::path::Path) -> Result<TickerReport> {
    let dir = idx_dir(data_root, ticker_id);
    let shards = list_shards(&dir, "idx")?;
    if shards.is_empty() {
        return Ok(TickerReport {
            ticker: ticker_id.to_string(),
            ticker_id: Some(ticker_id),
            status: "missing_live".to_string(),
            note: Some(format!("no shards under {}", dir.display())),
            ..base_report(ticker_id)
        });
    }

    // Calendar gap detection: every date between min..max must exist.
    let min_date = shards.first().unwrap().0;
    let max_date = shards.last().unwrap().0;
    let mut present: std::collections::BTreeSet<NaiveDate> = std::collections::BTreeSet::new();
    for (d, _) in &shards {
        present.insert(*d);
    }
    let mut missing: Vec<NaiveDate> = Vec::new();
    let mut d = min_date;
    while d <= max_date {
        if !present.contains(&d) {
            missing.push(d);
        }
        d = d.succ_opt().unwrap_or(d);
        if d == max_date.succ_opt().unwrap_or(max_date) && d == max_date {
            break;
        }
    }
    let shards_present = shards.len();
    let shards_missing = missing.len();

    // Cross-shard scan: monotonicity, max-gap, gap violations.
    let mut prev_ts: Option<i64> = None;
    let mut max_gap: i64 = 0;
    let mut gap_violations: usize = 0;
    let mut monotone_violation_ix: Option<usize> = None;
    let mut global_ix: usize = 0;
    let mut total_records: usize = 0;
    for (_date, path) in &shards {
        let recs: Vec<IndexRecord> = read_shard_aligned(path)
            .with_context(|| format!("read shard {}", path.display()))?;
        for r in &recs {
            let t = r.shard_ts_ms();
            if let Some(p) = prev_ts {
                let dt = t - p;
                if dt < 0 {
                    monotone_violation_ix = Some(global_ix);
                    break;
                }
                if dt > max_gap {
                    max_gap = dt;
                }
                if dt > SHARDED_GAP_MS {
                    gap_violations += 1;
                }
            }
            prev_ts = Some(t);
            global_ix += 1;
            total_records += 1;
        }
        if monotone_violation_ix.is_some() {
            break;
        }
    }

    let mut status = "ok".to_string();
    let mut note: Option<String> = None;
    if total_records == 0 {
        status = "empty".to_string();
        note = Some("0 records across all shards".to_string());
    } else if monotone_violation_ix.is_some() {
        status = "monotone_violation".to_string();
        note = Some(format!(
            "ts decreased at global record index {}",
            monotone_violation_ix.unwrap()
        ));
    } else if shards_missing > 0 {
        status = "missing_shards".to_string();
        note = Some(format!(
            "{} missing date-shards between {} and {} (first missing: {})",
            shards_missing, min_date, max_date, missing[0]
        ));
    } else if gap_violations > 0 {
        status = "gap".to_string();
        note = Some(format!(
            "{} inter-record gaps > {}ms (max gap = {}ms)",
            gap_violations, SHARDED_GAP_MS, max_gap
        ));
    }

    Ok(TickerReport {
        ticker: ticker_id.to_string(),
        ticker_id: Some(ticker_id),
        shards_present,
        shards_missing,
        max_intra_gap_ms: max_gap,
        gap_violations,
        monotone_violation_ix,
        status,
        note,
        ..base_report(ticker_id)
    })
}

fn base_report(ticker_id: u64) -> TickerReport {
    TickerReport {
        ticker: ticker_id.to_string(),
        ticker_id: Some(ticker_id),
        t_cut: 0,
        gap_ms: 0,
        overlap_records: 0,
        max_price_diff_bps: 0.0,
        max_ci_diff_pct: 0.0,
        monotone_violation_ix: None,
        shards_present: 0,
        shards_missing: 0,
        max_intra_gap_ms: 0,
        gap_violations: 0,
        status: String::new(),
        note: None,
        live_outage_records: 0,
        backfill_outage_records: 0,
        valid_sample_records: 0,
    }
}

// ─── Legacy flat mode ────────────────────────────────────────────────────────

fn run_legacy(args: &Args) -> Result<()> {
    let backfill_dir = args
        .backfill_dir
        .clone()
        .ok_or_else(|| anyhow!("--legacy-flat requires --backfill-dir"))?;
    let live_dir = args
        .live_dir
        .clone()
        .ok_or_else(|| anyhow!("--legacy-flat requires --live-dir"))?;
    let cfg = LegacyCfg {
        backfill_dir,
        live_dir,
        cycle_ms: args.cycle_ms,
        max_overlap_records: args.max_overlap_records,
        sample_size: args.sample_size,
        stale_threshold_ms: args.stale_threshold_ms,
    };

    if args.all {
        let agg = run_legacy_all(&cfg)?;
        emit_aggregate(&agg, args)?;
        info!(
            total = agg.total,
            checked = agg.checked,
            passed = agg.passed,
            warned = agg.warned,
            errored = agg.errored,
            "glue-check --all (legacy) complete"
        );
        if agg.errored > 0 {
            std::process::exit(1);
        }
        return Ok(());
    }
    let ticker = args
        .ticker
        .clone()
        .ok_or_else(|| anyhow!("missing positional <ticker_id_or_pair> for --legacy-flat; pass --all for batch mode"))?;
    let report = check_legacy_safe(&ticker, &cfg);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human(&report);
    }
    if matches!(
        report.status.as_str(),
        "monotone_violation" | "overlap_drift" | "error" | "missing_live" | "missing_backfill"
    ) {
        std::process::exit(1);
    }
    Ok(())
}

struct LegacyCfg {
    backfill_dir: PathBuf,
    live_dir: PathBuf,
    cycle_ms: i64,
    max_overlap_records: usize,
    sample_size: usize,
    stale_threshold_ms: i64,
}

fn run_legacy_all(cfg: &LegacyCfg) -> Result<AggregateReport> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&cfg.backfill_dir)
        .with_context(|| format!("read_dir {}", cfg.backfill_dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("idx"))
        .collect();
    entries.sort();

    let total = entries.len();
    let mut tickers = Vec::with_capacity(total);
    let mut checked = 0usize;
    let mut passed = 0usize;
    let mut warned = 0usize;
    let mut errored = 0usize;

    for p in &entries {
        let stem = p
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        if stem.is_empty() {
            continue;
        }
        let r = check_legacy_safe(&stem, cfg);
        match r.status.as_str() {
            "ok" => {
                checked += 1;
                passed += 1;
            }
            "gap" | "insufficient_samples" => {
                checked += 1;
                warned += 1;
            }
            "missing_live" => {
                info!(ticker = %stem, "skip: live .idx not found");
            }
            "missing_backfill" | "empty" => {
                checked += 1;
                warned += 1;
            }
            _ => {
                checked += 1;
                errored += 1;
            }
        }
        tickers.push(r);
    }

    Ok(AggregateReport {
        total,
        checked,
        passed,
        warned,
        errored,
        tickers,
    })
}

fn check_legacy_safe(ticker: &str, cfg: &LegacyCfg) -> TickerReport {
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check_legacy(ticker, cfg)));
    match res {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => mk_err(ticker, None, format!("error: {}", e)),
        Err(panic) => {
            let msg = downcast_panic(&panic);
            warn!(ticker = %ticker, panic = %msg, "check panicked");
            mk_err(ticker, None, format!("panic: {}", msg))
        }
    }
}

fn mk_err(ticker: &str, ticker_id: Option<u64>, note: String) -> TickerReport {
    let mut r = base_report(ticker_id.unwrap_or(0));
    r.ticker = ticker.to_string();
    r.ticker_id = ticker_id;
    r.status = "error".to_string();
    r.note = Some(note);
    r
}

fn check_legacy(ticker: &str, cfg: &LegacyCfg) -> Result<TickerReport> {
    let (pair_name, ticker_id_opt) = resolve_names(ticker);

    let backfill_path = cfg.backfill_dir.join(format!("{}.idx", pair_name));
    if !backfill_path.exists() {
        let mut r = base_report(ticker_id_opt.unwrap_or(0));
        r.ticker = ticker.to_string();
        r.ticker_id = ticker_id_opt;
        r.status = "missing_backfill".to_string();
        r.note = Some(format!("no file at {}", backfill_path.display()));
        return Ok(r);
    }

    let live_path = match ticker_id_opt {
        Some(id) => cfg.live_dir.join(format!("{}.idx", id)),
        None => {
            let mut r = base_report(0);
            r.ticker = ticker.to_string();
            r.status = "error".to_string();
            r.note = Some("could not resolve ticker_id from pair name".to_string());
            return Ok(r);
        }
    };
    if !live_path.exists() {
        let mut r = base_report(ticker_id_opt.unwrap_or(0));
        r.ticker = ticker.to_string();
        r.ticker_id = ticker_id_opt;
        r.status = "missing_live".to_string();
        r.note = Some(format!("no file at {}", live_path.display()));
        return Ok(r);
    }

    let bf_vec: Vec<IndexRecord> = read_shard_aligned(&backfill_path)?;
    let lv_vec: Vec<IndexRecord> = read_shard_aligned(&live_path)?;
    let bf: &[IndexRecord] = &bf_vec;
    let lv: &[IndexRecord] = &lv_vec;

    if bf.is_empty() || lv.is_empty() {
        let mut r = base_report(ticker_id_opt.unwrap_or(0));
        r.ticker = ticker.to_string();
        r.ticker_id = ticker_id_opt;
        r.status = "empty".to_string();
        r.note = Some(format!("bf_records={}, live_records={}", bf.len(), lv.len()));
        return Ok(r);
    }

    let mut last_ts = ts_ms(&bf[0]);
    let mut monotone_violation_ix: Option<usize> = None;
    for (i, r) in bf.iter().enumerate().skip(1) {
        let t = ts_ms(r);
        if t < last_ts {
            monotone_violation_ix = Some(i);
            break;
        }
        last_ts = t;
    }

    let t_cut = ts_ms(&bf[bf.len() - 1]);
    let t_live0 = ts_ms(&lv[0]);

    if let Some(ix) = monotone_violation_ix {
        let mut r = base_report(ticker_id_opt.unwrap_or(0));
        r.ticker = ticker.to_string();
        r.ticker_id = ticker_id_opt;
        r.t_cut = t_cut;
        r.monotone_violation_ix = Some(ix);
        r.status = "monotone_violation".to_string();
        r.note = Some(format!("backfill ts went backwards at record {}", ix));
        return Ok(r);
    }

    let dt = t_live0 - t_cut;
    if dt > 2 * cfg.cycle_ms {
        let mut r = base_report(ticker_id_opt.unwrap_or(0));
        r.ticker = ticker.to_string();
        r.ticker_id = ticker_id_opt;
        r.t_cut = t_cut;
        r.gap_ms = dt;
        r.status = "gap".to_string();
        r.note = Some(format!(
            "ts_live[0]={} > t_cut + 2×cycle_ms = {}",
            t_live0,
            t_cut + 2 * cfg.cycle_ms
        ));
        return Ok(r);
    }

    if dt >= 0 {
        let mut r = base_report(ticker_id_opt.unwrap_or(0));
        r.ticker = ticker.to_string();
        r.ticker_id = ticker_id_opt;
        r.t_cut = t_cut;
        r.gap_ms = dt;
        r.status = "ok".to_string();
        return Ok(r);
    }

    let t_overlap_start = t_live0;
    let bf_start = bf.partition_point(|r| ts_ms(r) < t_overlap_start);
    let overlap_records = bf
        .len()
        .saturating_sub(bf_start)
        .min(cfg.max_overlap_records);

    let sample_n = cfg.sample_size.max(1).min(overlap_records.max(1));
    let mut max_price_diff_rel = 0.0_f64;
    let mut max_ci_diff_pct = 0.0_f64;
    let stale_threshold_ms = cfg.stale_threshold_ms.max(0);
    let mut live_outage_records: u32 = 0;
    let mut backfill_outage_records: u32 = 0;
    let mut valid_sample_records: u32 = 0;

    for s in 0..sample_n {
        let frac = if sample_n == 1 {
            0.5
        } else {
            s as f64 / (sample_n - 1) as f64
        };
        let span = (t_cut - t_overlap_start).max(0) as f64;
        let target_ts = t_overlap_start + (frac * span).round() as i64;

        let bf_ix = nearest_ix(bf, target_ts);
        let lv_ix = nearest_ix(lv, target_ts);
        let a = &bf[bf_ix];
        let b = &lv[lv_ix];
        let bf_dt = (ts_ms(a) - target_ts).abs();
        let lv_dt = (ts_ms(b) - target_ts).abs();
        let bf_stale = bf_dt > stale_threshold_ms;
        let lv_stale = lv_dt > stale_threshold_ms;
        if bf_stale {
            backfill_outage_records += 1;
        }
        if lv_stale {
            live_outage_records += 1;
        }
        if bf_stale || lv_stale {
            continue;
        }

        let bid_a = a.index.bid;
        let bid_b = b.index.bid;
        let ask_a = a.index.ask;
        let ask_b = b.index.ask;
        let mid_a = (bid_a + ask_a) * 0.5;
        let mid_b = (bid_b + ask_b) * 0.5;
        let ci_a = decode_ci_ubp(a.index.ci);
        let ci_b = decode_ci_ubp(b.index.ci);

        max_price_diff_rel = max_price_diff_rel.max(rel_diff(bid_a, bid_b));
        max_price_diff_rel = max_price_diff_rel.max(rel_diff(ask_a, ask_b));
        max_price_diff_rel = max_price_diff_rel.max(rel_diff(mid_a, mid_b));

        let ci_max = ci_a.abs().max(ci_b.abs());
        if ci_max > 0.0 {
            let d = (ci_a - ci_b).abs() / ci_max;
            if d > max_ci_diff_pct {
                max_ci_diff_pct = d;
            }
        }
        valid_sample_records += 1;
    }

    let max_price_diff_bps = max_price_diff_rel * 10_000.0;
    let mut status = "ok".to_string();
    let mut note = None;
    let min_valid = (sample_n / 4) as u32;
    if valid_sample_records < min_valid {
        status = "insufficient_samples".to_string();
        note = Some(format!(
            "valid={} < sample_size/4 = {} (live_outage={}, backfill_outage={}, stale_threshold_ms={})",
            valid_sample_records, min_valid, live_outage_records, backfill_outage_records, stale_threshold_ms
        ));
    } else if max_price_diff_rel > 0.01 || max_ci_diff_pct > 0.5 {
        status = "overlap_drift".to_string();
        note = Some(format!(
            "max_price_diff_bps={:.2}, max_ci_diff_pct={:.2} (valid_samples={}/{}, live_outage={}, backfill_outage={})",
            max_price_diff_bps,
            max_ci_diff_pct * 100.0,
            valid_sample_records,
            sample_n,
            live_outage_records,
            backfill_outage_records,
        ));
    }

    let mut r = base_report(ticker_id_opt.unwrap_or(0));
    r.ticker = ticker.to_string();
    r.ticker_id = ticker_id_opt;
    r.t_cut = t_cut;
    r.gap_ms = dt;
    r.overlap_records = overlap_records;
    r.max_price_diff_bps = max_price_diff_bps;
    r.max_ci_diff_pct = max_ci_diff_pct * 100.0;
    r.live_outage_records = live_outage_records;
    r.backfill_outage_records = backfill_outage_records;
    r.valid_sample_records = valid_sample_records;
    r.status = status;
    r.note = note;
    Ok(r)
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn emit_aggregate(agg: &AggregateReport, args: &Args) -> Result<()> {
    let json = serde_json::to_string_pretty(agg)?;
    if let Some(p) = args.report.as_ref() {
        std::fs::write(p, &json).with_context(|| format!("write {}", p.display()))?;
        info!(path = %p.display(), "aggregate report written");
    } else {
        println!("{}", json);
    }
    Ok(())
}

fn downcast_panic(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn resolve_names(arg: &str) -> (String, Option<u64>) {
    if let Ok(id) = arg.parse::<u64>() {
        return (arg.to_string(), Some(id));
    }
    let pair = arg.replace('/', "-");
    let symbol = pair.replace('-', "/");
    let id = resolve_ticker_id(&symbol);
    (pair, Some(id))
}

#[inline]
fn ts_ms(r: &IndexRecord) -> i64 {
    timestamp::to_epoch_ms(r.header.get_timestamp())
}

fn nearest_ix(recs: &[IndexRecord], target_ts: i64) -> usize {
    debug_assert!(!recs.is_empty());
    let p = recs.partition_point(|r| ts_ms(r) < target_ts);
    if p == 0 {
        return 0;
    }
    if p >= recs.len() {
        return recs.len() - 1;
    }
    let prev = p - 1;
    let d_prev = (ts_ms(&recs[prev]) - target_ts).abs();
    let d_next = (ts_ms(&recs[p]) - target_ts).abs();
    if d_prev <= d_next {
        prev
    } else {
        p
    }
}

#[inline]
fn rel_diff(a: f64, b: f64) -> f64 {
    let den = a.abs().max(b.abs());
    if den == 0.0 {
        return 0.0;
    }
    (a - b).abs() / den
}

fn print_human(r: &TickerReport) {
    println!("ticker:                {}", r.ticker);
    if let Some(id) = r.ticker_id {
        println!("ticker_id:             {}", id);
    }
    if r.shards_present > 0 || r.shards_missing > 0 || r.max_intra_gap_ms > 0 {
        // Sharded summary.
        println!("shards_present:        {}", r.shards_present);
        println!("shards_missing:        {}", r.shards_missing);
        println!("max_intra_gap_ms:      {}", r.max_intra_gap_ms);
        println!("gap_violations:        {}", r.gap_violations);
    } else {
        // Legacy flat summary.
        println!("t_cut:                 {}", r.t_cut);
        println!("gap_ms:                {}", r.gap_ms);
        println!("overlap_records:       {}", r.overlap_records);
        println!("valid_sample_records:  {}", r.valid_sample_records);
        println!("live_outage_records:   {}", r.live_outage_records);
        println!("backfill_outage_records:{}", r.backfill_outage_records);
        println!("max_price_diff_bps:    {:.4}", r.max_price_diff_bps);
        println!("max_ci_diff_pct:       {:.4}", r.max_ci_diff_pct);
    }
    if let Some(ix) = r.monotone_violation_ix {
        println!("monotone_violation_ix: {}", ix);
    }
    println!("status:                {}", r.status);
    if let Some(n) = r.note.as_ref() {
        println!("note:                  {}", n);
    }
}
