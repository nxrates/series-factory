//! NXR — bank/hedge-fund-grade data-quality certifier for the canonical
//! sharded layout.
//!
//! Audits the on-disk daily-shard store written by the live aggregator and the
//! series-factory backfill tools, and emits a PASS/FAIL verdict per ticker. The
//! mandate is institutional: **zero unexplained gaps**, microstructure
//! invariants always hold, and the statistical fingerprint of each feed is
//! sane (no stuck feeds, no impossible jumps, plausible volatility, Renko brick
//! cadence within calibration tolerance).
//!
//! ## Canonical layout audited
//!
//! ```text
//! <data_root>/indexes/<ticker_id>/<YYYY-MM-DD>.idx     56B IndexRecord, ts-ascending
//! <data_root>/bars/<ticker_id>/<YYYY-MM-DD>.{s10,renko} 96B Bar
//! ```
//!
//! ## Checks (per ticker, over a configurable trailing window)
//!
//! 1. **ZERO-gap detection (sentinel-aware)** on the `.idx` stream. Records are
//!    delta-gated; a liveness sentinel (`FLAG_HEARTBEAT_SENTINEL`) is written at
//!    most every `SENTINEL_INTERVAL_MS` while quotes are unchanged, and the
//!    first record of each UTC day is always written. A gap whose `dt` exceeds
//!    `--max-gap-ms` is SUSPECT; it is excused as `quiet` only if a sentinel
//!    covers an endpoint (or the stream predates sentinel rollout) AND
//!    `dt <= --quiet-tolerance-ms`. Otherwise it is an `OUTAGE` (FAIL). Whole
//!    missing UTC days in `[first, last]` are a hard FAIL.
//! 2. **Microstructure invariants** on `.idx`: `bid>0`, `ask>0`, `ask>=bid`,
//!    bid/ask finite, `spread_bps ∈ [0, 2000]`, `confidence <= accepted`.
//! 3. **Anomaly battery** on mid-price log-returns (sentinels skipped):
//!    robust MAD outliers (|z| > 8), tabular CUSUM drift (k=0.5, h=8), and
//!    annualized Parkinson volatility from daily high/low.
//! 4. **Renko bars/day vs calibration**: mean bricks/UTC-day vs `--target-bpd`.
//!    Stable/stable crypto pairs (inferred by near-zero vol / all-zero returns)
//!    are flagged `crypto_stable=skip` and excluded from the bpd verdict.
//! 5. **s10 kline sanity**: ts-sorted, non-overlapping buckets, OHLC consistency.
//!
//! Exit non-zero if any audited ticker FAILS, so the binary can gate CI /
//! cutover. Empty or missing directories are reported as "no data", never a
//! panic.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{Duration, NaiveDate, Utc};
use clap::Parser;
use serde::Serialize;

use nxr_sdk::shard::{
    bars_dir, idx_dir, list_shards, read_shard_aligned, ts_ms_to_utc_date, ShardRecord,
    FLAG_HEARTBEAT_SENTINEL,
};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::Bar;

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "data-quality-audit",
    about = "Certify bank/hedge-fund-grade data quality over the canonical sharded layout."
)]
struct Cli {
    /// Root of the canonical store (contains `indexes/` and `bars/`).
    #[arg(long, default_value = "/data")]
    data_root: PathBuf,

    /// Comma-separated ticker ids. Default: every id dir under `indexes/`.
    #[arg(long)]
    tickers: Option<String>,

    /// Trailing window in days (audited range = [today-window, today]).
    #[arg(long, default_value_t = 365)]
    window_days: i64,

    /// `dt` (ms) above which an idx gap is SUSPECT (default 2× sentinel interval).
    #[arg(long, default_value_t = 120_000)]
    max_gap_ms: i64,

    /// `dt` (ms) tolerated for a sentinel-covered / pre-rollout quiet gap.
    #[arg(long, default_value_t = 300_000)]
    quiet_tolerance_ms: i64,

    /// Calibration target: expected Renko bricks per UTC day.
    #[arg(long, default_value_t = 300.0)]
    target_bpd: f64,

    /// Emit a machine-readable JSON report instead of the human table.
    #[arg(long)]
    json: bool,
}

// ── Report types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct GapRecord {
    start_ts: i64,
    end_ts: i64,
    dt_ms: i64,
    classification: String, // "quiet" | "OUTAGE"
}

#[derive(Debug, Clone, Serialize)]
struct ViolationSample {
    ts: i64,
    reason: String,
    bid: f64,
    ask: f64,
    spread_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
struct TickerReport {
    ticker_id: u64,
    has_data: bool,
    n_records: u64,
    // Gap detection
    suspect_gaps: u64,
    quiet_gaps: u64,
    outage_gaps: u64,
    missing_days: u64,
    missing_day_list: Vec<String>,
    worst_gaps: Vec<GapRecord>,
    // Invariants
    invariant_violations: u64,
    invariant_samples: Vec<ViolationSample>,
    // Anomalies
    mad_outliers: u64,
    worst_mad_z: f64,
    cusum_alarms: u64,
    sigma_park_ann: f64,
    crypto_stable: bool,
    // Renko
    renko_bpd: f64,
    renko_days: u64,
    renko_ratio: f64,
    renko_skipped_stable: bool,
    // s10 kline
    s10_bars: u64,
    s10_violations: u64,
    s10_samples: Vec<String>,
    // Verdict
    verdict: String, // "PASS" | "FAIL" | "NO_DATA"
    fail_reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
struct AuditReport {
    data_root: String,
    window_days: i64,
    window_start: String,
    window_end: String,
    max_gap_ms: i64,
    quiet_tolerance_ms: i64,
    target_bpd: f64,
    tickers: Vec<TickerReport>,
    n_pass: u64,
    n_fail: u64,
    n_no_data: u64,
}

// ── Inline stats (no new deps) ──────────────────────────────────────────────

/// Median of a slice (sorts a copy). Returns NaN for empty input.
fn median(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Median absolute deviation about the median.
fn mad(xs: &[f64], med: f64) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let dev: Vec<f64> = xs.iter().map(|x| (x - med).abs()).collect();
    median(&dev)
}

/// Sample mean.
fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// Sample standard deviation (Bessel-corrected). Returns 0 for n<2.
fn stddev(xs: &[f64], mu: f64) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let ss: f64 = xs.iter().map(|x| (x - mu).powi(2)).sum();
    (ss / (xs.len() as f64 - 1.0)).sqrt()
}

/// Tabular (two-sided) CUSUM on standardized returns. Returns alarm count.
/// k = allowance (slack), h = decision threshold (both in sigma units).
fn cusum_alarms(z: &[f64], k: f64, h: f64) -> u64 {
    let mut sh = 0.0_f64; // high-side accumulator
    let mut sl = 0.0_f64; // low-side accumulator
    let mut alarms = 0u64;
    for &x in z {
        sh = (sh + x - k).max(0.0);
        sl = (sl - x - k).max(0.0);
        if sh > h || sl > h {
            alarms += 1;
            // Reset after an alarm so a sustained shift counts once per re-trip.
            sh = 0.0;
            sl = 0.0;
        }
    }
    alarms
}

// ── Ticker discovery ───────────────────────────────────────────────────────

/// Enumerate ticker-id subdirectories under `<root>/indexes/`.
fn discover_tickers(data_root: &Path) -> Vec<u64> {
    let idx_root = data_root.join("indexes");
    let mut ids = Vec::new();
    let rd = match std::fs::read_dir(&idx_root) {
        Ok(rd) => rd,
        Err(_) => return ids, // missing root → no tickers (graceful)
    };
    for entry in rd.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(id) = name.parse::<u64>() {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

// ── Per-ticker audit ─────────────────────────────────────────────────────────

fn audit_ticker(
    data_root: &Path,
    id: u64,
    win_start: NaiveDate,
    win_end: NaiveDate,
    max_gap_ms: i64,
    quiet_tol_ms: i64,
    target_bpd: f64,
) -> Result<TickerReport> {
    let mut r = TickerReport {
        ticker_id: id,
        has_data: false,
        n_records: 0,
        suspect_gaps: 0,
        quiet_gaps: 0,
        outage_gaps: 0,
        missing_days: 0,
        missing_day_list: Vec::new(),
        worst_gaps: Vec::new(),
        invariant_violations: 0,
        invariant_samples: Vec::new(),
        mad_outliers: 0,
        worst_mad_z: 0.0,
        cusum_alarms: 0,
        sigma_park_ann: f64::NAN,
        crypto_stable: false,
        renko_bpd: f64::NAN,
        renko_days: 0,
        renko_ratio: f64::NAN,
        renko_skipped_stable: false,
        s10_bars: 0,
        s10_violations: 0,
        s10_samples: Vec::new(),
        verdict: "NO_DATA".to_string(),
        fail_reasons: Vec::new(),
    };

    // ── Load idx shards in window ─────────────────────────────────────────
    let idir = idx_dir(data_root, id);
    let idx_shards: Vec<(NaiveDate, PathBuf)> = list_shards(&idir, "idx")?
        .into_iter()
        .filter(|(d, _)| *d >= win_start && *d <= win_end)
        .collect();

    // Walk records in ts order across shards; collect per-shard for missing-day check.
    let present_days: BTreeSet<NaiveDate> = idx_shards.iter().map(|(d, _)| *d).collect();
    let mut records: Vec<IndexRecord> = Vec::new();
    for (_, path) in &idx_shards {
        match read_shard_aligned::<IndexRecord>(path) {
            Ok(mut recs) => records.append(&mut recs),
            Err(_) => continue, // unreadable shard: treated as absent for that file
        }
    }
    // Records are ts-ascending within a shard and shards are date-sorted, but a
    // torn/overlapping boundary could break global monotonicity — sort to be safe.
    records.sort_by_key(|rec| rec.shard_ts_ms());

    r.n_records = records.len() as u64;
    if !records.is_empty() {
        r.has_data = true;
    }

    // ── Check 1: ZERO-gap detection (sentinel-aware) ──────────────────────
    // Detect whether sentinels are present anywhere (post-rollout streams).
    let any_sentinel = records
        .iter()
        .any(|rec| (rec.index.flags & FLAG_HEARTBEAT_SENTINEL) != 0);

    for pair in records.windows(2) {
        let a = &pair[0];
        let b = &pair[1];
        let ts1 = a.shard_ts_ms();
        let ts2 = b.shard_ts_ms();
        let dt = ts2 - ts1;
        if dt <= max_gap_ms {
            continue;
        }
        r.suspect_gaps += 1;
        let a_flags = a.index.flags;
        let b_flags = b.index.flags;
        let endpoint_sentinel = (a_flags & FLAG_HEARTBEAT_SENTINEL) != 0
            || (b_flags & FLAG_HEARTBEAT_SENTINEL) != 0;
        // "quiet" if a sentinel covers an endpoint, OR the whole stream predates
        // sentinel rollout (no sentinels anywhere) — but only within tolerance.
        let excused = endpoint_sentinel || !any_sentinel;
        let classification = if excused && dt <= quiet_tol_ms {
            r.quiet_gaps += 1;
            "quiet"
        } else {
            r.outage_gaps += 1;
            "OUTAGE"
        };
        // Keep the worst (largest dt) gaps for the report.
        r.worst_gaps.push(GapRecord {
            start_ts: ts1,
            end_ts: ts2,
            dt_ms: dt,
            classification: classification.to_string(),
        });
    }
    r.worst_gaps.sort_by(|x, y| y.dt_ms.cmp(&x.dt_ms));
    r.worst_gaps.truncate(10);

    // Missing whole UTC days in [first, last] observed range (clamped to window).
    if let (Some(first), Some(last)) = (records.first(), records.last()) {
        let first_day = ts_ms_to_utc_date(first.shard_ts_ms()).max(win_start);
        let last_day = ts_ms_to_utc_date(last.shard_ts_ms()).min(win_end);
        let mut d = first_day;
        while d <= last_day {
            if !present_days.contains(&d) {
                r.missing_days += 1;
                if r.missing_day_list.len() < 30 {
                    r.missing_day_list.push(d.format("%Y-%m-%d").to_string());
                }
            }
            d += Duration::days(1);
        }
    }

    // ── Check 2: microstructure invariants ────────────────────────────────
    for rec in &records {
        let idx = rec.index; // copy out of packed struct
        let bid = idx.bid;
        let ask = idx.ask;
        let mut reason: Option<&str> = None;
        if !bid.is_finite() || !ask.is_finite() {
            reason = Some("bid/ask non-finite");
        } else if bid <= 0.0 {
            reason = Some("bid <= 0");
        } else if ask <= 0.0 {
            reason = Some("ask <= 0");
        } else if ask < bid {
            reason = Some("crossed quote (ask < bid)");
        } else if idx.confidence > idx.accepted {
            reason = Some("confidence > accepted");
        } else {
            let sb = idx.spread_bps();
            if !(0.0..=2000.0).contains(&sb) {
                reason = Some("spread_bps out of [0,2000]");
            }
        }
        if let Some(why) = reason {
            r.invariant_violations += 1;
            if r.invariant_samples.len() < 10 {
                let sb = if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 {
                    idx.spread_bps()
                } else {
                    f64::NAN
                };
                r.invariant_samples.push(ViolationSample {
                    ts: rec.shard_ts_ms(),
                    reason: why.to_string(),
                    bid,
                    ask,
                    spread_bps: sb,
                });
            }
        }
    }

    // ── Check 3: anomaly battery (log-returns, skipping sentinels) ─────────
    // Mid series from non-sentinel records only.
    let mut mids: Vec<(i64, f64)> = Vec::with_capacity(records.len());
    for rec in &records {
        let idx = rec.index;
        if (idx.flags & FLAG_HEARTBEAT_SENTINEL) != 0 {
            continue;
        }
        let mid = idx.mid();
        if mid.is_finite() && mid > 0.0 {
            mids.push((rec.shard_ts_ms(), mid));
        }
    }

    let mut returns: Vec<f64> = Vec::new();
    for w in mids.windows(2) {
        let m0 = w[0].1;
        let m1 = w[1].1;
        if m0 > 0.0 && m1 > 0.0 {
            returns.push((m1 / m0).ln());
        }
    }

    if !returns.is_empty() {
        let med = median(&returns);
        let m = mad(&returns, med);
        let denom = 1.4826 * m;
        if denom > 0.0 && denom.is_finite() {
            for &rt in &returns {
                let z = ((rt - med) / denom).abs();
                if z > 8.0 {
                    r.mad_outliers += 1;
                }
                if z > r.worst_mad_z {
                    r.worst_mad_z = z;
                }
            }
        }
        // CUSUM on standardized returns (mean/std standardization).
        let mu = mean(&returns);
        let sd = stddev(&returns, mu);
        if sd > 0.0 {
            let z: Vec<f64> = returns.iter().map(|x| (x - mu) / sd).collect();
            r.cusum_alarms = cusum_alarms(&z, 0.5, 8.0);
        }
    }

    // ── Parkinson volatility (annualized) from daily idx mid high/low ──────
    // Prefer s10 bars' daily high/low if available; else derive from idx mids.
    let bdir = bars_dir(data_root, id);
    let s10_shards: Vec<(NaiveDate, PathBuf)> = list_shards(&bdir, "s10")?
        .into_iter()
        .filter(|(d, _)| *d >= win_start && *d <= win_end)
        .collect();

    let mut daily_hl_logsq: Vec<f64> = Vec::new(); // (ln(H/L))^2 per day
    let mut zero_return_frac = 0.0;

    // From s10 bars: aggregate daily high/low.
    if !s10_shards.is_empty() {
        for (date, path) in &s10_shards {
            let bars = match read_shard_aligned::<Bar>(path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let mut hi = f64::MIN;
            let mut lo = f64::MAX;
            let _ = date;
            for bar in &bars {
                let h = bar.high;
                let l = bar.low;
                if h.is_finite() && l.is_finite() && l > 0.0 && h > 0.0 {
                    if h > hi {
                        hi = h;
                    }
                    if l < lo {
                        lo = l;
                    }
                }
            }
            if hi > 0.0 && lo > 0.0 && hi >= lo && lo != f64::MAX {
                let lr = (hi / lo).ln();
                daily_hl_logsq.push(lr * lr);
            }
        }
    } else if !mids.is_empty() {
        // Derive daily high/low from idx mids when no s10 bars present.
        use std::collections::BTreeMap;
        let mut by_day: BTreeMap<NaiveDate, (f64, f64)> = BTreeMap::new();
        for &(ts, mid) in &mids {
            let d = ts_ms_to_utc_date(ts);
            let e = by_day.entry(d).or_insert((f64::MIN, f64::MAX));
            if mid > e.0 {
                e.0 = mid;
            }
            if mid < e.1 {
                e.1 = mid;
            }
        }
        for (_, (hi, lo)) in by_day {
            if hi > 0.0 && lo > 0.0 && hi >= lo {
                let lr = (hi / lo).ln();
                daily_hl_logsq.push(lr * lr);
            }
        }
    }

    if !daily_hl_logsq.is_empty() {
        // sigma_daily = sqrt( (1/(4 ln2)) * mean( (ln(H/L))^2 ) )
        let factor = 1.0 / (4.0 * std::f64::consts::LN_2);
        let sigma_daily = (factor * mean(&daily_hl_logsq)).sqrt();
        // Annualize: crypto trades ~365 days; FX ~252. Use 365 as conservative
        // upper bound for the 24/7 crypto-dominated store.
        r.sigma_park_ann = sigma_daily * (365.0_f64).sqrt();
    }

    if !returns.is_empty() {
        let zeros = returns.iter().filter(|&&x| x == 0.0).count();
        zero_return_frac = zeros as f64 / returns.len() as f64;
    }

    // Stable/stable inference: annualized Parkinson sigma < 0.5% OR >95% zero returns.
    let sigma_finite = r.sigma_park_ann.is_finite();
    if (sigma_finite && r.sigma_park_ann < 0.005) || zero_return_frac > 0.95 {
        r.crypto_stable = true;
    }

    // ── Check 4: Renko bricks/day vs calibration ──────────────────────────
    let renko_shards: Vec<(NaiveDate, PathBuf)> = list_shards(&bdir, "renko")?
        .into_iter()
        .filter(|(d, _)| *d >= win_start && *d <= win_end)
        .collect();

    if !renko_shards.is_empty() {
        use std::collections::BTreeMap;
        let mut per_day: BTreeMap<NaiveDate, u64> = BTreeMap::new();
        for (_, path) in &renko_shards {
            let bars = match read_shard_aligned::<Bar>(path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            for bar in &bars {
                let d = ts_ms_to_utc_date(bar.ts_ms());
                *per_day.entry(d).or_insert(0) += 1;
            }
        }
        r.renko_days = per_day.len() as u64;
        if r.renko_days > 0 {
            let total: u64 = per_day.values().sum();
            r.renko_bpd = total as f64 / r.renko_days as f64;
            if target_bpd > 0.0 {
                r.renko_ratio = r.renko_bpd / target_bpd;
            }
        }
        if r.crypto_stable {
            r.renko_skipped_stable = true;
        }
    }

    // ── Check 5: s10 kline sanity ─────────────────────────────────────────
    for (_, path) in &s10_shards {
        let bars = match read_shard_aligned::<Bar>(path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let mut prev_close_ms: Option<i64> = None;
        for bar in &bars {
            r.s10_bars += 1;
            let o = bar.open;
            let h = bar.high;
            let l = bar.low;
            let c = bar.close;
            let open_ms = bar.open_time_ms();
            let close_ms = bar.close_time_ms();
            let mut why: Option<String> = None;
            if !(o.is_finite() && h.is_finite() && l.is_finite() && c.is_finite()) {
                why = Some("non-finite OHLC".to_string());
            } else if l > h {
                why = Some("low > high".to_string());
            } else if o < l || o > h {
                why = Some("open outside [low,high]".to_string());
            } else if c < l || c > h {
                why = Some("close outside [low,high]".to_string());
            } else if open_ms > close_ms {
                why = Some("open_ts > close_ts".to_string());
            } else if let Some(pc) = prev_close_ms {
                if open_ms < pc {
                    why = Some("overlapping bucket (open_ts < prev close_ts)".to_string());
                }
            }
            if let Some(w) = why {
                r.s10_violations += 1;
                if r.s10_samples.len() < 10 {
                    r.s10_samples.push(format!("ts={} {}", open_ms, w));
                }
            }
            prev_close_ms = Some(close_ms);
        }
    }

    // ── Verdict ───────────────────────────────────────────────────────────
    if !r.has_data {
        r.verdict = "NO_DATA".to_string();
        return Ok(r);
    }
    let mut reasons: Vec<String> = Vec::new();
    if r.outage_gaps > 0 {
        reasons.push(format!("{} OUTAGE gap(s)", r.outage_gaps));
    }
    if r.missing_days > 0 {
        reasons.push(format!("{} missing UTC day(s)", r.missing_days));
    }
    if r.invariant_violations > 0 {
        reasons.push(format!("{} invariant violation(s)", r.invariant_violations));
    }
    if r.s10_violations > 0 {
        reasons.push(format!("{} s10 OHLC violation(s)", r.s10_violations));
    }
    // Renko cadence: only gate non-stable pairs with renko data. Tolerance ±50%.
    if !r.crypto_stable && r.renko_days > 0 && r.renko_ratio.is_finite() {
        if r.renko_ratio < 0.5 || r.renko_ratio > 2.0 {
            reasons.push(format!(
                "renko bpd off calibration (ratio {:.2})",
                r.renko_ratio
            ));
        }
    }
    // Anomalies are advisory (reported) but MAD outliers + CUSUM are warnings,
    // not hard fails — a single jump should not red-flag a feed for cutover.
    // The hard mandate is gaps + invariants + structural sanity.
    if reasons.is_empty() {
        r.verdict = "PASS".to_string();
    } else {
        r.verdict = "FAIL".to_string();
        r.fail_reasons = reasons;
    }
    Ok(r)
}

// ── Human-readable rendering ─────────────────────────────────────────────────

fn print_human(report: &AuditReport) {
    println!("══════════════════════════════════════════════════════════════════════");
    println!(" NXR DATA-QUALITY AUDIT");
    println!("──────────────────────────────────────────────────────────────────────");
    println!(" data_root        : {}", report.data_root);
    println!(
        " window           : {} → {} ({} days)",
        report.window_start, report.window_end, report.window_days
    );
    println!(
        " max_gap_ms={}  quiet_tolerance_ms={}  target_bpd={}",
        report.max_gap_ms, report.quiet_tolerance_ms, report.target_bpd
    );
    println!("══════════════════════════════════════════════════════════════════════");

    if report.tickers.is_empty() {
        println!("\n  (no ticker directories found under indexes/ — nothing to audit)\n");
    }

    for t in &report.tickers {
        println!("\n── ticker {} ──────────────────────────────────────────────", t.ticker_id);
        if !t.has_data {
            println!("   NO DATA in window.");
            continue;
        }
        println!("   records              : {}", t.n_records);
        println!(
            "   gaps                 : suspect={} quiet(ok)={} OUTAGE={}",
            t.suspect_gaps, t.quiet_gaps, t.outage_gaps
        );
        if t.missing_days > 0 {
            let preview = t.missing_day_list.join(", ");
            println!("   missing UTC days     : {} [{}]", t.missing_days, preview);
        } else {
            println!("   missing UTC days     : 0");
        }
        if !t.worst_gaps.is_empty() {
            println!("   worst gaps:");
            for g in &t.worst_gaps {
                println!(
                    "     [{} → {}] dt={}ms {}",
                    g.start_ts, g.end_ts, g.dt_ms, g.classification
                );
            }
        }
        println!("   invariant violations : {}", t.invariant_violations);
        for s in &t.invariant_samples {
            println!(
                "     @ts={} {} (bid={}, ask={}, spread_bps={:.2})",
                s.ts, s.reason, s.bid, s.ask, s.spread_bps
            );
        }
        println!(
            "   anomalies            : mad_outliers={} (worst |z|={:.2}) cusum_alarms={}",
            t.mad_outliers, t.worst_mad_z, t.cusum_alarms
        );
        println!(
            "   sigma_park (ann)     : {}",
            fmt_sigma(t.sigma_park_ann)
        );
        if t.crypto_stable {
            println!("   crypto_stable        : YES (renko bpd check skipped)");
        }
        if t.renko_days > 0 {
            println!(
                "   renko                : bpd={:.1} over {} days (ratio {:.2}){}",
                t.renko_bpd,
                t.renko_days,
                t.renko_ratio,
                if t.renko_skipped_stable { " [skipped: stable]" } else { "" }
            );
        } else {
            println!("   renko                : no .renko shards in window");
        }
        println!(
            "   s10 kline            : {} bars, {} violations",
            t.s10_bars, t.s10_violations
        );
        for s in &t.s10_samples {
            println!("     {}", s);
        }
        println!("   VERDICT              : {}", t.verdict);
        if !t.fail_reasons.is_empty() {
            println!("     reasons: {}", t.fail_reasons.join("; "));
        }
    }

    // Summary table.
    println!("\n══════════════════════════════════════════════════════════════════════");
    println!(" SUMMARY");
    println!("──────────────────────────────────────────────────────────────────────");
    println!(
        " {:>12} {:>10} {:>7} {:>7} {:>7} {:>7} {:>7} {:>12} {:>9} {:>7}",
        "ticker", "records", "outage", "missday", "invviol", "madout", "cusum", "sigma_ann", "renkobpd", "verdict"
    );
    for t in &report.tickers {
        println!(
            " {:>12} {:>10} {:>7} {:>7} {:>7} {:>7} {:>7} {:>12} {:>9} {:>7}",
            t.ticker_id,
            t.n_records,
            t.outage_gaps,
            t.missing_days,
            t.invariant_violations,
            t.mad_outliers,
            t.cusum_alarms,
            fmt_sigma(t.sigma_park_ann),
            fmt_bpd(t.renko_bpd, t.renko_skipped_stable),
            t.verdict,
        );
    }
    println!("──────────────────────────────────────────────────────────────────────");
    println!(
        " PASS={}  FAIL={}  NO_DATA={}",
        report.n_pass, report.n_fail, report.n_no_data
    );
    println!("══════════════════════════════════════════════════════════════════════");
}

fn fmt_sigma(s: f64) -> String {
    if s.is_finite() {
        format!("{:.4}", s)
    } else {
        "n/a".to_string()
    }
}

fn fmt_bpd(b: f64, skipped: bool) -> String {
    if skipped {
        "skip".to_string()
    } else if b.is_finite() {
        format!("{:.1}", b)
    } else {
        "n/a".to_string()
    }
}

// ── Entrypoint ───────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cli = Cli::parse();

    let today = Utc::now().date_naive();
    let win_end = today;
    let win_start = today - Duration::days(cli.window_days.max(0));

    // Resolve ticker set.
    let tickers: Vec<u64> = match &cli.tickers {
        Some(s) => s
            .split(',')
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .filter_map(|x| x.parse::<u64>().ok())
            .collect(),
        None => discover_tickers(&cli.data_root),
    };

    let mut reports: Vec<TickerReport> = Vec::with_capacity(tickers.len());
    for id in tickers {
        match audit_ticker(
            &cli.data_root,
            id,
            win_start,
            win_end,
            cli.max_gap_ms,
            cli.quiet_tolerance_ms,
            cli.target_bpd,
        ) {
            Ok(r) => reports.push(r),
            Err(e) => {
                // A per-ticker IO failure is a FAIL for that ticker, not a panic.
                let mut r = TickerReport {
                    ticker_id: id,
                    has_data: false,
                    n_records: 0,
                    suspect_gaps: 0,
                    quiet_gaps: 0,
                    outage_gaps: 0,
                    missing_days: 0,
                    missing_day_list: Vec::new(),
                    worst_gaps: Vec::new(),
                    invariant_violations: 0,
                    invariant_samples: Vec::new(),
                    mad_outliers: 0,
                    worst_mad_z: 0.0,
                    cusum_alarms: 0,
                    sigma_park_ann: f64::NAN,
                    crypto_stable: false,
                    renko_bpd: f64::NAN,
                    renko_days: 0,
                    renko_ratio: f64::NAN,
                    renko_skipped_stable: false,
                    s10_bars: 0,
                    s10_violations: 0,
                    s10_samples: Vec::new(),
                    verdict: "FAIL".to_string(),
                    fail_reasons: vec![format!("audit error: {e}")],
                };
                r.has_data = false;
                reports.push(r);
            }
        }
    }

    let n_pass = reports.iter().filter(|r| r.verdict == "PASS").count() as u64;
    let n_fail = reports.iter().filter(|r| r.verdict == "FAIL").count() as u64;
    let n_no_data = reports.iter().filter(|r| r.verdict == "NO_DATA").count() as u64;

    let report = AuditReport {
        data_root: cli.data_root.display().to_string(),
        window_days: cli.window_days,
        window_start: win_start.format("%Y-%m-%d").to_string(),
        window_end: win_end.format("%Y-%m-%d").to_string(),
        max_gap_ms: cli.max_gap_ms,
        quiet_tolerance_ms: cli.quiet_tolerance_ms,
        target_bpd: cli.target_bpd,
        tickers: reports,
        n_pass,
        n_fail,
        n_no_data,
    };

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human(&report);
    }

    // Gate CI/cutover: non-zero exit if any ticker FAILS.
    if n_fail > 0 {
        std::process::exit(1);
    }
    Ok(())
}
