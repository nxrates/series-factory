//! Glue/join validator for the backfill ↔ live `.idx` seam.
//!
//! ## Sharded mode
//!
//! - One ticker_id → many `<data_root>/indexes/<ticker_id>/<YYYY-MM-DD>.idx` shards.
//! - Validates:
//!   1. Per-shard monotonicity (`ts` non-decreasing inside each file).
//!   2. Cross-shard continuity (last ts of day D vs first ts of D+1 — no
//!      missing date-shards in the date range present).
//!   3. Inter-record gap ≤ `SENTINEL_INTERVAL_MS + 30_000` ms = 90 s (the
//!      live writer emits a sentinel every 60 s when quotes are quiet; a
//!      gap > 90 s with no sentinel inside indicates a real outage).
//!
//! Per-ticker JSON report + aggregate report in `--all` mode. Each ticker's
//! check runs inside `catch_unwind` so one corrupted file does not abort the
//! batch.
//!
//! ## Usage
//!
//!   glue-check --ticker-id 12345 --data-root /data
//!   glue-check --all --data-root /data --report /data/glue/last.json
//!
//! ## Severity
//!
//! Verdicts are not equally actionable, and lumping them made this check
//! permanently red and therefore useless as an alert (186/186 non-pass:
//! 123 `gap`, 55 `missing_shards`, 8 `monotone_violation`). See [`Severity`]
//! for which verdict means corruption and which means expected absence.
//!
//! Exit code, matching `integrity-check`: 0 = clean, 1 = warnings only,
//! 2 = at least one error (or any warning under `--strict`).

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{idx_dir, list_shards, read_shard_aligned, ShardRecord, SENTINEL_INTERVAL_MS};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing::{info, warn};

/// Inter-record-gap budget for the sharded mode: 60 s sentinel cadence + 30 s
/// jitter budget (process scheduling, fsync slack, day-boundary rotate). A
/// gap exceeding this with no sentinel inside means the live writer was down.
const SHARDED_GAP_MS: i64 = SENTINEL_INTERVAL_MS + 30_000;

#[derive(Parser, Debug)]
#[command(about = "Validate .idx continuity for one or all tickers (sharded layout).")]
struct Args {
    /// Decimal MITCH ticker id (e.g. 12345). Mutually exclusive with `--all`.
    #[arg(long)]
    ticker_id: Option<u64>,

    #[clap(flatten)]
    common: series_factory::cli::CommonArgs,

    /// Emit single-ticker output as JSON to stdout (default = human text).
    #[arg(long)]
    json: bool,

    /// Iterate every ticker; enumerates `<data_root>/indexes/<id>/` dirs.
    #[arg(long)]
    all: bool,

    /// In `--all` mode, write aggregate JSON report to this path.
    #[arg(long)]
    report: Option<PathBuf>,

    /// Treat warnings as errors (exit 2 on any non-pass verdict).
    #[arg(long)]
    strict: bool,
}

/// How actionable a verdict is. SINGLE source for the status -> exit-code
/// mapping, shared by the single-ticker and `--all` paths so the taxonomy
/// cannot drift between them (it had: two separate string lists disagreed).
///
/// Rule, same as `integrity_check`: hard structural corruption is an ERROR
/// always; absence that has a benign explanation is a WARN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
enum Severity {
    Pass,
    Warn,
    Error,
}

fn severity(status: &str) -> Severity {
    match status {
        "ok" => Severity::Pass,
        // Expected absence, NOT evidence of corruption:
        // - `gap`: the 90 s budget assumes a sentinel every 60 s, but the
        //   sentinel is emitted from `append()` (sdk shard.rs), i.e. it is
        //   tick-driven, not timer-driven. No inbound tick means no sentinel,
        //   so a quiet ticker, a closed session and an overnight roll all
        //   register as `gap` and are indistinguishable from a real outage.
        // - `missing_shards`: a shard file is created lazily on first append,
        //   so any UTC day with zero traffic for that ticker legitimately has
        //   no file (weekends, holidays, dark venues, delistings).
        // - `empty` / `missing_live` / `insufficient_samples`: coverage
        //   questions, nothing read back wrong.
        "empty" | "missing_live" | "gap" | "missing_shards" | "insufficient_samples" => {
            Severity::Warn
        }
        // `monotone_violation` (record ts went backwards: interleaved writer,
        // bad merge, clock rewind), `error` (the checker could not read the
        // store), and any status not yet classified. Unknown verdicts fail
        // loud rather than silently joining the warn pile.
        _ => Severity::Error,
    }
}

/// 0 clean, 1 warnings only, 2 any error. Under `--strict` a warning is an
/// error, so the cron can be tightened without changing the taxonomy.
fn exit_code(errored: usize, warned: usize, strict: bool) -> i32 {
    if errored > 0 || (strict && warned > 0) {
        2
    } else if warned > 0 {
        1
    } else {
        0
    }
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
    /// Verdict histogram, so "55 missing_shards" is readable without walking
    /// `tickers`. The counts are what make a warn pile triageable.
    by_status: BTreeMap<String, usize>,
    tickers: Vec<TickerReport>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();
    run_sharded(&args)
}

// ─── Sharded mode ────────────────────────────────────────────────────────────

fn run_sharded(args: &Args) -> Result<()> {
    if args.all {
        let agg = run_sharded_all(args)?;
        emit_aggregate(&agg, args)?;
        let code = exit_code(agg.errored, agg.warned, args.strict);
        info!(
            passed = agg.passed,
            warned = agg.warned,
            errored = agg.errored,
            by_status = ?agg.by_status,
            exit = code,
            "glue-check verdict"
        );
        if code != 0 {
            std::process::exit(code);
        }
        return Ok(());
    }
    let id = args.ticker_id.ok_or_else(|| {
        anyhow!(
            "missing --ticker-id (or pass --all); use --legacy-flat for the old flat-file layout"
        )
    })?;
    let report = check_sharded_safe(id, &args.common.data_root);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human(&report);
    }
    let code = match severity(&report.status) {
        Severity::Pass => 0,
        Severity::Warn => exit_code(0, 1, args.strict),
        Severity::Error => 2,
    };
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}

fn run_sharded_all(args: &Args) -> Result<AggregateReport> {
    let indexes_root = args.common.data_root.join("indexes");
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

    let mut by_status: BTreeMap<String, usize> = BTreeMap::new();

    for id in ids {
        let r = check_sharded_safe(id, &args.common.data_root);
        checked += 1;
        match severity(&r.status) {
            Severity::Pass => passed += 1,
            Severity::Warn => warned += 1,
            Severity::Error => errored += 1,
        }
        *by_status.entry(r.status.clone()).or_default() += 1;
        tickers.push(r);
    }

    Ok(AggregateReport {
        total,
        checked,
        passed,
        warned,
        errored,
        by_status,
        tickers,
    })
}

fn check_sharded_safe(ticker_id: u64, data_root: &std::path::Path) -> TickerReport {
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_sharded(ticker_id, data_root)
    }));
    match res {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => mk_err(
            &ticker_id.to_string(),
            Some(ticker_id),
            format!("error: {}", e),
        ),
        Err(panic) => {
            let msg = downcast_panic(&panic);
            warn!(ticker_id, panic = %msg, "check panicked");
            mk_err(
                &ticker_id.to_string(),
                Some(ticker_id),
                format!("panic: {}", msg),
            )
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
        let recs: Vec<IndexRecord> =
            read_shard_aligned(path).with_context(|| format!("read shard {}", path.display()))?;
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

fn mk_err(ticker: &str, ticker_id: Option<u64>, note: String) -> TickerReport {
    let mut r = base_report(ticker_id.unwrap_or(0));
    r.ticker = ticker.to_string();
    r.ticker_id = ticker_id;
    r.status = "error".to_string();
    r.note = Some(note);
    r
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
