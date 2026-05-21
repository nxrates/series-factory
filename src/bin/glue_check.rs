//! Glue/join validator for the backfill ↔ live `.idx` seam.
//!
//! For each ticker, opens the backfill composite `.idx` (`<backfill-dir>/<BASE>-<QUOTE>.idx`)
//! and the live `.idx` (`<live-dir>/<ticker_id>.idx`), then verifies:
//!
//!   1. Backfill timestamps are monotone non-decreasing.
//!   2. `ts_live[0]` sits in `[t_cut, t_cut + 2 × cycle_ms)` (no gap, no overlap).
//!   3. If overlap (`ts_live[0] < t_cut`): sample-check up to `--sample-size`
//!      timestamps spread uniformly over the overlap; for each, binary-search
//!      both files for the nearest record and bound the max relative diff on
//!      `bid`/`ask`/`mid`/`ci_ubp`. ERROR if any price diff > 1% (5 bps target)
//!      or CI diff > 50%.
//!
//! Per-ticker JSON report + aggregate report in `--all` mode. Each ticker's
//! check runs inside `catch_unwind` so one corrupted file does not abort the
//! batch.
//!
//! Usage:
//!   glue-check <ticker_id_or_pair>
//!     --backfill-dir /data/backfill/composite
//!     --live-dir     /data/indexes
//!     [--cycle-ms 100]
//!     [--max-overlap-records 1000]
//!     [--sample-size 100]
//!     [--json]
//!
//!   glue-check --all
//!     --backfill-dir /data/backfill/composite
//!     --live-dir     /data/indexes
//!     [--report path.json]
//!
//! Exit code: 0 if 0 errors; 1 if any errors.

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use memmap2::Mmap;
use mitch::timestamp;
use nxr_sdk::{ipc::record::IndexRecord, resolve_ticker_id, tdwap::decode_ci_ubp};
use serde::Serialize;
use std::fs::File;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(about = "Validate the backfill ↔ live .idx join for one or all tickers.")]
struct Args {
    /// `<BASE>-<QUOTE>` (e.g. `BTC-USDT`) or decimal ticker id. Required unless `--all`.
    ticker: Option<String>,

    /// Directory holding backfill composite `<BASE>-<QUOTE>.idx` files.
    #[arg(long)]
    backfill_dir: PathBuf,

    /// Directory holding live `<ticker_id>.idx` files.
    #[arg(long)]
    live_dir: PathBuf,

    /// Aggregator cycle in milliseconds (defines the no-gap/no-overlap window).
    #[arg(long, default_value = "100")]
    cycle_ms: i64,

    /// Cap on overlap-window records considered before truncating the diff sweep.
    #[arg(long, default_value = "1000")]
    max_overlap_records: usize,

    /// Sample size for uniformly-spaced timestamp probes within the overlap.
    #[arg(long, default_value = "100")]
    sample_size: usize,

    /// Max distance (ms) between a probe target_ts and the nearest record before
    /// the sample is treated as landing in a data gap and skipped from drift
    /// stats. Default = 10000 ms (10s @ 100ms cadence ≈ 100 cycles). Tune up for
    /// laggier feeds, down for tighter outage detection.
    #[arg(long, default_value = "10000")]
    stale_threshold_ms: i64,

    /// Emit single-ticker output as JSON to stdout (default = human text).
    #[arg(long)]
    json: bool,

    /// Iterate every ticker in `--backfill-dir`. Mutually exclusive with positional ticker.
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
    t_cut: i64,
    gap_ms: i64,
    overlap_records: usize,
    max_price_diff_bps: f64,
    max_ci_diff_pct: f64,
    monotone_violation_ix: Option<usize>,
    status: String, // "ok" | "gap" | "overlap_drift" | "monotone_violation" | "missing_live" | "missing_backfill" | "empty" | "insufficient_samples" | "error"
    note: Option<String>,
    /// Samples skipped because the nearest live record was further than
    /// `stale_threshold_ms` from the probe target_ts (i.e. live aggregator
    /// outage during the overlap window).
    #[serde(default)]
    live_outage_records: u32,
    /// Samples skipped because the nearest backfill record was further than
    /// `stale_threshold_ms` from the probe target_ts.
    #[serde(default)]
    backfill_outage_records: u32,
    /// Samples that actually contributed to drift stats (both sides fresh).
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

    if args.all {
        let agg = run_all(&args)?;
        let json = serde_json::to_string_pretty(&agg)?;
        if let Some(p) = args.report.as_ref() {
            std::fs::write(p, &json).with_context(|| format!("write {}", p.display()))?;
            info!(path = %p.display(), "aggregate report written");
        } else {
            println!("{}", json);
        }
        info!(
            total = agg.total,
            checked = agg.checked,
            passed = agg.passed,
            warned = agg.warned,
            errored = agg.errored,
            "glue-check --all complete"
        );
        if agg.errored > 0 {
            std::process::exit(1);
        }
        return Ok(());
    }

    let ticker = args
        .ticker
        .clone()
        .ok_or_else(|| anyhow!("missing positional <ticker_id_or_pair>; pass `--all` for batch mode"))?;
    let report = check_one_safe(&ticker, &args);
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

/// Iterate every `<BASE>-<QUOTE>.idx` in the backfill dir; for each, resolve to a
/// `<ticker_id>.idx` in the live dir and run `check_one_safe`. Missing live files
/// produce a `missing_live` status (informational, not counted as error).
fn run_all(args: &Args) -> Result<AggregateReport> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&args.backfill_dir)
        .with_context(|| format!("read_dir {}", args.backfill_dir.display()))?
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
        let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
        if stem.is_empty() {
            continue;
        }
        let r = check_one_safe(&stem, args);
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
                // not counted in `checked`; informational
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

/// Panic-safe wrapper. One broken file ! kill the batch.
fn check_one_safe(ticker: &str, args: &Args) -> TickerReport {
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check_one(ticker, args)));
    match res {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => mk_err(ticker, format!("error: {}", e)),
        Err(panic) => {
            let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            warn!(ticker = %ticker, panic = %msg, "check panicked");
            mk_err(ticker, format!("panic: {}", msg))
        }
    }
}

fn mk_err(ticker: &str, note: String) -> TickerReport {
    TickerReport {
        ticker: ticker.to_string(),
        ticker_id: None,
        t_cut: 0,
        gap_ms: 0,
        overlap_records: 0,
        max_price_diff_bps: 0.0,
        max_ci_diff_pct: 0.0,
        monotone_violation_ix: None,
        status: "error".to_string(),
        note: Some(note),
        live_outage_records: 0,
        backfill_outage_records: 0,
        valid_sample_records: 0,
    }
}

fn check_one(ticker: &str, args: &Args) -> Result<TickerReport> {
    // Resolve both files. Accept either decimal id (e.g. "12345") or
    // `<BASE>-<QUOTE>` form (e.g. "BTC-USDT"). Backfill files are named by
    // pair; live files by numeric id.
    let (pair_name, ticker_id_opt) = resolve_names(ticker);

    let backfill_path = args
        .backfill_dir
        .join(format!("{}.idx", pair_name));
    if !backfill_path.exists() {
        return Ok(TickerReport {
            ticker: ticker.to_string(),
            ticker_id: ticker_id_opt,
            t_cut: 0,
            gap_ms: 0,
            overlap_records: 0,
            max_price_diff_bps: 0.0,
            max_ci_diff_pct: 0.0,
            monotone_violation_ix: None,
            status: "missing_backfill".to_string(),
            note: Some(format!("no file at {}", backfill_path.display())),
            live_outage_records: 0,
            backfill_outage_records: 0,
            valid_sample_records: 0,
        });
    }

    let live_path = match ticker_id_opt {
        Some(id) => args.live_dir.join(format!("{}.idx", id)),
        None => {
            return Ok(TickerReport {
                ticker: ticker.to_string(),
                ticker_id: None,
                t_cut: 0,
                gap_ms: 0,
                overlap_records: 0,
                max_price_diff_bps: 0.0,
                max_ci_diff_pct: 0.0,
                monotone_violation_ix: None,
                status: "error".to_string(),
                note: Some("could not resolve ticker_id from pair name".to_string()),
                live_outage_records: 0,
                backfill_outage_records: 0,
                valid_sample_records: 0,
            });
        }
    };
    if !live_path.exists() {
        return Ok(TickerReport {
            ticker: ticker.to_string(),
            ticker_id: ticker_id_opt,
            t_cut: 0,
            gap_ms: 0,
            overlap_records: 0,
            max_price_diff_bps: 0.0,
            max_ci_diff_pct: 0.0,
            monotone_violation_ix: None,
            status: "missing_live".to_string(),
            note: Some(format!("no file at {}", live_path.display())),
            live_outage_records: 0,
            backfill_outage_records: 0,
            valid_sample_records: 0,
        });
    }

    let bf_map = mmap_idx(&backfill_path)?;
    let lv_map = mmap_idx(&live_path)?;
    let bf: &[IndexRecord] = bf_slice(&bf_map.0)?;
    let lv: &[IndexRecord] = bf_slice(&lv_map.0)?;

    if bf.is_empty() || lv.is_empty() {
        return Ok(TickerReport {
            ticker: ticker.to_string(),
            ticker_id: ticker_id_opt,
            t_cut: 0,
            gap_ms: 0,
            overlap_records: 0,
            max_price_diff_bps: 0.0,
            max_ci_diff_pct: 0.0,
            monotone_violation_ix: None,
            status: "empty".to_string(),
            note: Some(format!(
                "bf_records={}, live_records={}",
                bf.len(),
                lv.len()
            )),
            live_outage_records: 0,
            backfill_outage_records: 0,
            valid_sample_records: 0,
        });
    }

    // Backfill monotonicity.
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
        return Ok(TickerReport {
            ticker: ticker.to_string(),
            ticker_id: ticker_id_opt,
            t_cut,
            gap_ms: 0,
            overlap_records: 0,
            max_price_diff_bps: 0.0,
            max_ci_diff_pct: 0.0,
            monotone_violation_ix: Some(ix),
            status: "monotone_violation".to_string(),
            note: Some(format!("backfill ts went backwards at record {}", ix)),
            live_outage_records: 0,
            backfill_outage_records: 0,
            valid_sample_records: 0,
        });
    }

    // Classify seam: gap / overlap / clean.
    let dt = t_live0 - t_cut;
    if dt > 2 * args.cycle_ms {
        return Ok(TickerReport {
            ticker: ticker.to_string(),
            ticker_id: ticker_id_opt,
            t_cut,
            gap_ms: dt,
            overlap_records: 0,
            max_price_diff_bps: 0.0,
            max_ci_diff_pct: 0.0,
            monotone_violation_ix: None,
            status: "gap".to_string(),
            note: Some(format!(
                "ts_live[0]={} > t_cut + 2×cycle_ms = {}",
                t_live0,
                t_cut + 2 * args.cycle_ms
            )),
            live_outage_records: 0,
            backfill_outage_records: 0,
            valid_sample_records: 0,
        });
    }

    if dt >= 0 {
        // Clean join: ts_live[0] ∈ [t_cut, t_cut + 2×cycle_ms].
        return Ok(TickerReport {
            ticker: ticker.to_string(),
            ticker_id: ticker_id_opt,
            t_cut,
            gap_ms: dt,
            overlap_records: 0,
            max_price_diff_bps: 0.0,
            max_ci_diff_pct: 0.0,
            monotone_violation_ix: None,
            status: "ok".to_string(),
            note: None,
            live_outage_records: 0,
            backfill_outage_records: 0,
            valid_sample_records: 0,
        });
    }

    // Overlap window: ts_live[0] < t_cut. Sample-check.
    let t_overlap_start = t_live0;
    // Find first backfill index >= t_overlap_start.
    let bf_start = bf
        .partition_point(|r| ts_ms(r) < t_overlap_start);
    let overlap_records = bf
        .len()
        .saturating_sub(bf_start)
        .min(args.max_overlap_records);

    let sample_n = args.sample_size.max(1).min(overlap_records.max(1));
    let mut max_price_diff_rel = 0.0_f64;
    let mut max_ci_diff_pct = 0.0_f64;
    let stale_threshold_ms = args.stale_threshold_ms.max(0);
    let mut live_outage_records: u32 = 0;
    let mut backfill_outage_records: u32 = 0;
    let mut valid_sample_records: u32 = 0;

    for s in 0..sample_n {
        // Uniformly-spaced timestamps in [t_overlap_start, t_cut].
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

        // Outage detection: if the nearest record is further away than the
        // stale threshold, that side has a data gap at target_ts. Skip the
        // sample from drift stats and tally it separately. ! comparing
        // backfill vs (much-later) live record produces fake "drift".
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

        // CI is a magnitude (>=0). max-normalized diff.
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
    // Insufficient-sample guard fires BEFORE drift judgement: if a live outage
    // ate most of the overlap window, max_price_diff_rel is meaningless even
    // if the few surviving samples happen to disagree.
    let min_valid = (sample_n / 4) as u32;
    if valid_sample_records < min_valid {
        status = "insufficient_samples".to_string();
        note = Some(format!(
            "valid={} < sample_size/4 = {} (live_outage={}, backfill_outage={}, stale_threshold_ms={})",
            valid_sample_records,
            min_valid,
            live_outage_records,
            backfill_outage_records,
            stale_threshold_ms,
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

    Ok(TickerReport {
        ticker: ticker.to_string(),
        ticker_id: ticker_id_opt,
        t_cut,
        gap_ms: dt, // negative ⇒ overlap
        overlap_records,
        max_price_diff_bps,
        max_ci_diff_pct: max_ci_diff_pct * 100.0,
        monotone_violation_ix: None,
        status,
        note,
        live_outage_records,
        backfill_outage_records,
        valid_sample_records,
    })
}

/// Resolve `<arg>` into `(pair_name, ticker_id)`. Accepts either form:
/// - "BTC-USDT" or "BTC/USDT" → pair_name="BTC-USDT", ticker_id from resolver.
/// - "12345" (decimal id) → pair_name same string (caller must have a pair
///   file named "12345.idx" in backfill, otherwise missing_backfill).
fn resolve_names(arg: &str) -> (String, Option<u64>) {
    if let Ok(id) = arg.parse::<u64>() {
        return (arg.to_string(), Some(id));
    }
    // Pair form: accept "BTC-USDT" or "BTC/USDT". Normalize hyphen→slash for resolver.
    let pair = arg.replace('/', "-");
    let symbol = pair.replace('-', "/");
    let id = resolve_ticker_id(&symbol);
    (pair, Some(id))
}

/// mmap the file; returns the `Mmap` (caller pins it for slice lifetime).
fn mmap_idx(path: &Path) -> Result<(Mmap, u64)> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = f.metadata()?.len();
    if len == 0 {
        // memmap2 refuses zero-length maps; create an empty stand-in via a tiny placeholder.
        // Easier: bail w/ note → caller treats as empty.
        return Err(anyhow!("file is empty: {}", path.display()));
    }
    let m = unsafe { Mmap::map(&f) }.with_context(|| format!("mmap {}", path.display()))?;
    Ok((m, len))
}

/// Reinterpret an mmap as `&[IndexRecord]`. Tail bytes (file not %56) are dropped.
fn bf_slice<'a>(m: &'a Mmap) -> Result<&'a [IndexRecord]> {
    let rec = std::mem::size_of::<IndexRecord>();
    let nbytes = m.len();
    let aligned = (nbytes / rec) * rec;
    if aligned == 0 {
        return Ok(&[]);
    }
    Ok(bytemuck::cast_slice::<u8, IndexRecord>(&m[..aligned]))
}

#[inline]
fn ts_ms(r: &IndexRecord) -> i64 {
    timestamp::to_epoch_ms(r.header.get_timestamp())
}

/// Binary-search `recs` for the index whose ts is closest to `target_ts`.
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
    println!("t_cut:                 {}", r.t_cut);
    println!("gap_ms:                {}", r.gap_ms);
    println!("overlap_records:       {}", r.overlap_records);
    println!("valid_sample_records:  {}", r.valid_sample_records);
    println!("live_outage_records:   {}", r.live_outage_records);
    println!("backfill_outage_records:{}", r.backfill_outage_records);
    println!("max_price_diff_bps:    {:.4}", r.max_price_diff_bps);
    println!("max_ci_diff_pct:       {:.4}", r.max_ci_diff_pct);
    if let Some(ix) = r.monotone_violation_ix {
        println!("monotone_violation_ix: {}", ix);
    }
    println!("status:                {}", r.status);
    if let Some(n) = r.note.as_ref() {
        println!("note:                  {}", n);
    }
}
