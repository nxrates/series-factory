//! Renko continuity verifier — Sprint M1.
//!
//! Cross-shard validation NOT covered by `integrity-check bars`:
//!   - B03 across day boundary: `close[shard_D.last] == open[shard_D+1.first]`
//!     (existing checker validates only WITHIN a single shard).
//!   - Per-ticker bricks/day **median** + MAD (tracks calibrator target;
//!     operator target = median ≈ 300, low avg error, tolerate 5× regime spikes).
//!   - Gap-ms distribution across day boundaries (debug hist↔live restarts).
//!
//! ## Usage
//!
//! ```text
//! renko-continuity-check --data-root /data [--ticker <id>] [--json] [--out report.json]
//! ```
//!
//! Exit codes: 0 = clean, 1 = warnings (gaps > 60s but no B03 violation),
//! 2 = errors (any B03 violation).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use nxr_sdk::shard::{list_shards, read_shard_aligned};
use nxr_sdk::stats as sdk_stats;
use nxr_sdk::Bar;
use serde::{Deserialize, Serialize};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(about = "Verify cross-shard continuity for .renko files (B03 + bricks/day stats).")]
struct Cli {
    #[clap(flatten)]
    common: series_factory::cli::CommonArgs,
    /// Restrict to a single ticker_id (MITCH u64). Default: all tickers.
    #[arg(long)]
    ticker: Option<u64>,
    /// Emit per-ticker JSON to stdout.
    #[arg(long)]
    json: bool,
    /// Optional output file for the full JSON report (regardless of --json).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Gap-ms threshold above which a boundary is flagged as WARN (default 60_000).
    #[arg(long, default_value_t = 60_000)]
    warn_gap_ms: i64,
}

// ── Report shapes ───────────────────────────────────────────────────────────

/// Boundary between adjacent shards (D, D+1).
#[derive(Debug, Serialize, Deserialize)]
struct BoundaryReport {
    day_from: String,
    day_to: String,
    last_close: f64,
    first_open: f64,
    /// Absolute price delta (== 0.0 if B03 holds exactly).
    delta: f64,
    /// Wallclock gap in ms between close[last] and open[first]. Negative means overlap.
    gap_ms: i64,
    /// True if `|delta| > 1e-12 * |last_close|` — i.e. B03 violated beyond float noise.
    b03_violated: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct TickerReport {
    ticker_id: u64,
    shards: usize,
    total_bricks: u64,
    /// Per-shard bricks count (one entry per .renko file, ordered by date).
    bricks_per_day: Vec<u64>,
    /// Median bricks/day across shards.
    median_bpd: f64,
    /// Mean bricks/day.
    mean_bpd: f64,
    /// Median absolute deviation of bricks/day (MAD).
    mad_bpd: f64,
    /// Min/max per-day (regime extremes).
    min_bpd: u64,
    max_bpd: u64,
    /// Number of cross-shard boundaries inspected (= shards - 1).
    boundaries: usize,
    /// Number of B03 violations across boundaries.
    b03_violations: usize,
    /// Number of boundaries with gap_ms > warn_gap_ms.
    large_gaps: usize,
    /// Worst boundary details (top-5 by |delta|, then top-5 by gap_ms).
    worst_b03: Vec<BoundaryReport>,
    worst_gaps: Vec<BoundaryReport>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GlobalReport {
    data_root: String,
    tickers: usize,
    total_bricks: u64,
    total_boundaries: usize,
    total_b03_violations: usize,
    total_large_gaps: usize,
    /// Per-ticker breakdown, keyed by ticker_id (stringified for stable JSON ordering).
    per_ticker: BTreeMap<String, TickerReport>,
}

// ── Core ────────────────────────────────────────────────────────────────────

fn check_ticker(ticker_dir: &Path, ticker_id: u64, warn_gap_ms: i64) -> Result<TickerReport> {
    let shards = list_shards(ticker_dir, "renko")
        .with_context(|| format!("list_shards {}", ticker_dir.display()))?;

    let mut bricks_per_day: Vec<u64> = Vec::with_capacity(shards.len());
    let mut last_bar_of_prev: Option<(chrono::NaiveDate, Bar)> = None;
    let mut all_boundaries: Vec<BoundaryReport> = Vec::new();
    let mut total_bricks: u64 = 0;
    let mut b03_violations: usize = 0;
    let mut large_gaps: usize = 0;

    for (date, path) in &shards {
        let bars: Vec<Bar> = read_shard_aligned(path)
            .with_context(|| format!("read_shard_aligned {}", path.display()))?;
        bricks_per_day.push(bars.len() as u64);
        total_bricks += bars.len() as u64;
        if bars.is_empty() {
            continue;
        }
        if let Some((prev_date, prev_last)) = last_bar_of_prev.as_ref() {
            let first = bars[0];
            // Copy out of packed struct before field math.
            let last_close = prev_last.close;
            let first_open = first.open;
            // B03 via the shared seam check so the binary AND the cert
            // (data-quality-audit) enforce byte-for-byte the same tolerance.
            let seam = series_factory::seam::check_renko_cross_shard(last_close, first_open);
            let delta = seam.delta;
            let b03_violated = seam.violated;
            let gap_ms = first.open_time_ms() - prev_last.close_time_ms();
            if b03_violated {
                b03_violations += 1;
            }
            if gap_ms.abs() > warn_gap_ms {
                large_gaps += 1;
            }
            all_boundaries.push(BoundaryReport {
                day_from: prev_date.to_string(),
                day_to: date.to_string(),
                last_close,
                first_open,
                delta,
                gap_ms,
                b03_violated,
            });
        }
        last_bar_of_prev = Some((*date, *bars.last().unwrap()));
    }

    // Cast u64 → f64 once; sdk stats consume f64 slices.
    let bpd_f64: Vec<f64> = bricks_per_day.iter().map(|&n| n as f64).collect();
    let (median_bpd, mad_bpd) = sdk_stats::median_and_mad(&bpd_f64);
    let mean_bpd = if bricks_per_day.is_empty() {
        0.0
    } else {
        total_bricks as f64 / bricks_per_day.len() as f64
    };

    // Worst-5 by |delta|, then by |gap_ms|.
    let mut sorted_by_delta = all_boundaries
        .iter()
        .filter(|b| b.b03_violated)
        .cloned()
        .collect::<Vec<_>>();
    sorted_by_delta.sort_by(|a, b| b.delta.abs().partial_cmp(&a.delta.abs()).unwrap());
    let worst_b03 = sorted_by_delta.into_iter().take(5).collect();

    let mut sorted_by_gap = all_boundaries.clone();
    sorted_by_gap.sort_by(|a, b| b.gap_ms.abs().cmp(&a.gap_ms.abs()));
    let worst_gaps = sorted_by_gap.into_iter().take(5).collect();

    Ok(TickerReport {
        ticker_id,
        shards: shards.len(),
        total_bricks,
        min_bpd: *bricks_per_day.iter().min().unwrap_or(&0),
        max_bpd: *bricks_per_day.iter().max().unwrap_or(&0),
        bricks_per_day,
        median_bpd,
        mean_bpd,
        mad_bpd,
        boundaries: all_boundaries.len(),
        b03_violations,
        large_gaps,
        worst_b03,
        worst_gaps,
    })
}

impl Clone for BoundaryReport {
    fn clone(&self) -> Self {
        Self {
            day_from: self.day_from.clone(),
            day_to: self.day_to.clone(),
            last_close: self.last_close,
            first_open: self.first_open,
            delta: self.delta,
            gap_ms: self.gap_ms,
            b03_violated: self.b03_violated,
        }
    }
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let cli = Cli::parse();
    let bars_root = cli.common.data_root.join("bars");
    if !bars_root.exists() {
        anyhow::bail!("data root has no bars/ subdirectory: {}", bars_root.display());
    }

    let mut per_ticker: BTreeMap<String, TickerReport> = BTreeMap::new();
    let mut total_bricks: u64 = 0;
    let mut total_boundaries: usize = 0;
    let mut total_b03: usize = 0;
    let mut total_gaps: usize = 0;

    for entry in std::fs::read_dir(&bars_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s,
            None => continue,
        };
        let ticker_id: u64 = match name.parse() {
            Ok(n) => n,
            Err(_) => continue, // skip non-numeric subdirs
        };
        if let Some(want) = cli.ticker {
            if ticker_id != want {
                continue;
            }
        }
        let rep = check_ticker(&path, ticker_id, cli.warn_gap_ms)?;
        total_bricks += rep.total_bricks;
        total_boundaries += rep.boundaries;
        total_b03 += rep.b03_violations;
        total_gaps += rep.large_gaps;
        per_ticker.insert(format!("{:020}", ticker_id), rep);
    }

    let global = GlobalReport {
        data_root: cli.common.data_root.display().to_string(),
        tickers: per_ticker.len(),
        total_bricks,
        total_boundaries,
        total_b03_violations: total_b03,
        total_large_gaps: total_gaps,
        per_ticker,
    };

    // Optional file output
    if let Some(out_path) = &cli.out {
        let json = serde_json::to_string_pretty(&global)?;
        std::fs::write(out_path, json)
            .with_context(|| format!("write report to {}", out_path.display()))?;
    }

    if cli.json {
        let json = serde_json::to_string_pretty(&global)?;
        println!("{}", json);
    } else {
        println!("# Renko continuity check");
        println!("data_root: {}", global.data_root);
        println!(
            "tickers={} total_bricks={} boundaries={} b03_violations={} large_gaps={}",
            global.tickers,
            global.total_bricks,
            global.total_boundaries,
            global.total_b03_violations,
            global.total_large_gaps
        );
        println!();
        println!(
            "{:<22} {:>6} {:>10} {:>9} {:>9} {:>9} {:>5} {:>5} {:>5} {:>5}",
            "ticker_id", "shards", "bricks", "med_bpd", "mean_bpd", "mad_bpd", "min", "max", "b03", "gaps"
        );
        for (_, r) in &global.per_ticker {
            println!(
                "{:<22} {:>6} {:>10} {:>9.1} {:>9.1} {:>9.1} {:>5} {:>5} {:>5} {:>5}",
                r.ticker_id,
                r.shards,
                r.total_bricks,
                r.median_bpd,
                r.mean_bpd,
                r.mad_bpd,
                r.min_bpd,
                r.max_bpd,
                r.b03_violations,
                r.large_gaps,
            );
        }
        // Print B03 violation details (always — these are errors).
        for (_, r) in &global.per_ticker {
            if r.b03_violations > 0 {
                println!("\n## B03 violations — ticker {}", r.ticker_id);
                for b in &r.worst_b03 {
                    println!(
                        "  {} → {}: close={:.10} open={:.10} delta={:.3e} gap_ms={}",
                        b.day_from, b.day_to, b.last_close, b.first_open, b.delta, b.gap_ms
                    );
                }
            }
        }
    }

    let exit_code = if total_b03 > 0 { 2 } else if total_gaps > 0 { 1 } else { 0 };
    std::process::exit(exit_code);
}
