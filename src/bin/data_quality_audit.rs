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
//!    are flagged `crypto_stable=skip` and excluded from the bpd verdict. Parity
//!    extensions (mirroring `.idx` rigor): **R1** cross-shard brick continuity
//!    (last close[D] == first open[D+1], 1e-9 rel; FAIL), **R3** brick magnitude
//!    floor (`|Δ|/open >= renko.min_pct`; FAIL), **R4** calibration correctness
//!    (realized bpd within the calibrator's ±20% accept band AND a `renko_k`
//!    entry exists in `ticker-params.json`; missing k while shards exist =
//!    CRIT `renko_uncalibrated`), **R2** per-day brick distribution drift
//!    (median/MAD/min-day; WARN).
//! 5. **s10 kline sanity**: ts-sorted, non-overlapping buckets, OHLC consistency.
//!    Parity extensions: **K1** cross-shard boundary continuity (delta = positive
//!    multiple of 10_000 ms; off-grid = FAIL, multi-bucket gap = WARN), **K2**
//!    UTC-grid bucket alignment (`open_ts % 10_000 == 0`; FAIL — guards the
//!    s10-from-idx alignment regression), **K3** coverage vs 8640 bars/day
//!    (WARN 0.5–0.8, FAIL < 0.5; mirrors integrity-check strict coverage gate).
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
    FLAG_HEARTBEAT_SENTINEL, MS_PER_DAY,
};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::stats as sdk_stats;
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::Bar;

// ── Calibration / renko config (single source of truth = config.yml) ─────────

/// s10 bucket width: UTC-grid alignment + per-day coverage are computed against
/// this. Matches `integrity-check s10 --bucket-ms` default and the live producer.
const S10_BUCKET_MS: i64 = 10_000;

/// Renko brick floor (`series.renko.min_pct`) from `config.yml`, falling back to
/// `0.0001`. Mirrors `integrity_check::load_renko_bounds` (single canonical
/// resolver). R3 brick-magnitude floor check uses this.
fn load_renko_min_pct() -> f64 {
    use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
    PipelineYml::load_default(ConfigHint::Bin)
        .map(|yml| yml.series.renko.min_pct as f64)
        .unwrap_or(0.0001)
}

/// Relative-bpd accept-band the calibrator's walk-forward accept-gate uses to
/// drop a window (`bar_construction::calibrate.rs`: `rel_bpd_err > 0.20`). R4
/// asserts realized bricks/day lands inside the same band around target_bpd, so
/// the cert mirrors exactly what the calibrator accepted at fit time.
const RENKO_BPD_ACCEPT_TOL: f64 = 0.20;

/// Look up the calibrated Renko k for `ticker_id` in
/// `$NXR_TICKER_PARAMS_PATH` (default `/data/config/ticker-params.json`).
/// Returns `None` if the file is missing/malformed or has no positive-finite
/// entry. Mirrors `renko_from_idx::load_calibrated_k` (same schema + filter).
/// R4 CRIT `renko_uncalibrated` fires when renko shards exist but this is None.
fn load_calibrated_k(ticker_id: u64) -> Option<f64> {
    let cfg = nxr_sdk::NxrConfig::from_env();
    let raw = std::fs::read_to_string(&cfg.ticker_params_path).ok()?;
    let weights: WeightsFile = serde_json::from_str(&raw).ok()?;
    weights
        .renko_k_per_ticker
        .get(&ticker_id.to_string())
        .copied()
        .filter(|k| *k > 0.0 && k.is_finite())
}

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "data-quality-audit",
    about = "Certify bank/hedge-fund-grade data quality over the canonical sharded layout."
)]
struct Cli {
    #[clap(flatten)]
    common: series_factory::cli::CommonArgs,

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
    ///
    /// R1 C6: default lowered from 300_000 → 0 until sentinels ship widely
    /// across all live shards. With 0 tolerance the audit correctly fails
    /// any gap > `max_gap_ms`; auto-quieting 2-5 min outages was masking
    /// real producer downtime. After the sentinel-writing build (R1 C2)
    /// has been live for ≥1 day across every ticker AND the audit confirms
    /// sentinels are present in fresh shards, raise this to 60_000
    /// (1 min slack above SENTINEL_INTERVAL_MS) so a single missed
    /// sentinel doesn't trip a false alarm.
    #[arg(long, default_value_t = 0)]
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
    // Renko parity checks (vs .idx 6.5/10 target)
    renko_b03_violations: u64,          // R1 cross-shard continuity breaks
    renko_brick_floor_violations: u64,  // R3 |Δ|/open < min_pct
    renko_k_present: bool,              // R4 calibrated k exists in ticker-params
    renko_uncalibrated: bool,           // R4 CRIT: shards but no k
    renko_inferred_bpd: f64,            // R4 realized bricks/day
    renko_bpd_off_band: bool,           // R4 |inferred/target-1| > accept tol
    renko_bpd_median: f64,              // R2 per-day distribution
    renko_bpd_mad: f64,
    renko_bpd_min_day: u64,
    renko_dist_drift: bool,             // R2 WARN: MAD > 0.5*median or min < 0.33*median
    renko_samples: Vec<String>,         // dated violation provenance
    // s10 kline
    s10_bars: u64,
    s10_violations: u64,
    s10_samples: Vec<String>,
    // s10 parity checks
    s10_b03_violations: u64,            // K1 cross-shard off-grid boundary
    s10_b03_gaps: u64,                  // K1 multi-bucket gap at boundary (WARN)
    s10_misaligned: u64,                // K2 open_time_ms % 10_000 != 0
    s10_coverage_pct: f64,              // K3 bars/day vs 8640
    s10_coverage_fail: bool,            // K3 < 0.5
    s10_coverage_warn: bool,            // K3 0.5..0.8
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

// ── Stats: median/mean/stddev/mad all sourced from `nxr_sdk::stats` ─────────

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
        renko_b03_violations: 0,
        renko_brick_floor_violations: 0,
        renko_k_present: false,
        renko_uncalibrated: false,
        renko_inferred_bpd: f64::NAN,
        renko_bpd_off_band: false,
        renko_bpd_median: f64::NAN,
        renko_bpd_mad: f64::NAN,
        renko_bpd_min_day: 0,
        renko_dist_drift: false,
        renko_samples: Vec::new(),
        s10_bars: 0,
        s10_violations: 0,
        s10_samples: Vec::new(),
        s10_b03_violations: 0,
        s10_b03_gaps: 0,
        s10_misaligned: 0,
        s10_coverage_pct: f64::NAN,
        s10_coverage_fail: false,
        s10_coverage_warn: false,
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
        let (med, m) = sdk_stats::median_and_mad(&returns);
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
        let mu = sdk_stats::mean(&returns);
        let sd = sdk_stats::std_dev(&returns);
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
        let sigma_daily = (factor * sdk_stats::mean(&daily_hl_logsq)).sqrt();
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
        let renko_min_pct = load_renko_min_pct();
        let mut per_day: BTreeMap<NaiveDate, u64> = BTreeMap::new();
        // R1 cross-shard continuity: last bar.close of day D == first bar.open of
        // D+1. Tracked across shard boundaries (shards are date-sorted).
        let mut prev_shard_close: Option<(NaiveDate, f64)> = None;
        for (date, path) in &renko_shards {
            let bars = match read_shard_aligned::<Bar>(path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let mut day_count = 0u64;
            let first_open = bars.first().map(|b| b.open);
            // R1: boundary continuity vs previous shard's last close.
            if let (Some((pd, pc)), Some(fo)) = (prev_shard_close, first_open) {
                if (pc - fo).abs() > pc.abs() * 1e-9 {
                    r.renko_b03_violations += 1;
                    if r.renko_samples.len() < 10 {
                        r.renko_samples.push(format!(
                            "[{}→{}] R1 cross-shard discontinuity: prev close {} != open {}",
                            pd, date, pc, fo
                        ));
                    }
                }
            }
            for bar in &bars {
                let d = ts_ms_to_utc_date(bar.ts_ms());
                *per_day.entry(d).or_insert(0) += 1;
                day_count += 1;
                // R3 brick magnitude floor: |close-open|/open >= min_pct.
                let open = bar.open;
                let close = bar.close;
                if open > 0.0 && open.is_finite() && close.is_finite() {
                    let brick = ((close - open) / open).abs();
                    if brick < renko_min_pct {
                        r.renko_brick_floor_violations += 1;
                        if r.renko_samples.len() < 10 {
                            r.renko_samples.push(format!(
                                "[{}] R3 brick {:.6} < floor {} (open={}, close={})",
                                date, brick, renko_min_pct, open, close
                            ));
                        }
                    }
                }
            }
            let _ = day_count;
            if let Some(last) = bars.last() {
                prev_shard_close = Some((*date, last.close));
            }
        }
        r.renko_days = per_day.len() as u64;
        if r.renko_days > 0 {
            let total: u64 = per_day.values().sum();
            r.renko_bpd = total as f64 / r.renko_days as f64;
            r.renko_inferred_bpd = r.renko_bpd;
            if target_bpd > 0.0 {
                r.renko_ratio = r.renko_bpd / target_bpd;
            }
            // R2 per-day distribution: median + MAD of bricks/day.
            let counts: Vec<f64> = per_day.values().map(|&c| c as f64).collect();
            let (med, mad) = sdk_stats::median_and_mad(&counts);
            r.renko_bpd_median = med;
            r.renko_bpd_mad = mad;
            r.renko_bpd_min_day = per_day.values().copied().min().unwrap_or(0);
            if med > 0.0
                && (mad > 0.5 * med || (r.renko_bpd_min_day as f64) < 0.33 * med)
            {
                r.renko_dist_drift = true;
            }
        }
        // R4 calibration correctness: realized bpd must land in the calibrator's
        // accept band around target_bpd, AND a calibrated k must exist.
        r.renko_k_present = load_calibrated_k(id).is_some();
        if !r.renko_k_present && r.renko_days > 0 {
            r.renko_uncalibrated = true; // CRIT: shards built w/ stale/bootstrap k
        }
        if !r.crypto_stable && r.renko_days > 0 && target_bpd > 0.0 {
            let rel_err = (r.renko_inferred_bpd / target_bpd - 1.0).abs();
            if rel_err > RENKO_BPD_ACCEPT_TOL {
                r.renko_bpd_off_band = true;
            }
        }
        if r.crypto_stable {
            r.renko_skipped_stable = true;
        }
    }

    // ── Check 5: s10 kline sanity (+ K1 continuity, K2 alignment, K3 coverage)
    {
        use std::collections::BTreeMap;
        let mut prev_close_ms: Option<i64> = None; // across shards (date-sorted)
        let mut buckets_per_day: BTreeMap<NaiveDate, u64> = BTreeMap::new();
        for (date, path) in &s10_shards {
            let bars = match read_shard_aligned::<Bar>(path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            for bar in &bars {
                r.s10_bars += 1;
                let o = bar.open;
                let h = bar.high;
                let l = bar.low;
                let c = bar.close;
                let open_ms = bar.open_time_ms();
                let close_ms = bar.close_time_ms();
                *buckets_per_day
                    .entry(ts_ms_to_utc_date(open_ms))
                    .or_insert(0) += 1;

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
                        r.s10_samples.push(format!("[{}] ts={} {}", date, open_ms, w));
                    }
                }

                // K2 bucket alignment: open_time_ms on the UTC 10s grid.
                if open_ms % S10_BUCKET_MS != 0 {
                    r.s10_misaligned += 1;
                    if r.s10_samples.len() < 10 {
                        r.s10_samples.push(format!(
                            "[{}] K2 off-grid open_ts={} (% {} = {})",
                            date,
                            open_ms,
                            S10_BUCKET_MS,
                            open_ms % S10_BUCKET_MS
                        ));
                    }
                }

                // K1 boundary continuity: first bucket of D+1 must be exactly one
                // bucket after the last close of D. Off-grid delta (non-multiple)
                // = FAIL; multiple>1 = gap (WARN). Same-shard consecutive bars are
                // already covered by the overlap check above; this gate fires on
                // the cross-shard boundary too via the shared prev_close_ms.
                if let Some(pc) = prev_close_ms {
                    let delta = open_ms - pc;
                    if delta > 0 {
                        if delta % S10_BUCKET_MS != 0 {
                            r.s10_b03_violations += 1;
                            if r.s10_samples.len() < 10 {
                                r.s10_samples.push(format!(
                                    "[{}] K1 off-grid boundary: open_ts {} - prev_close {} = {} ms (not mult of {})",
                                    date, open_ms, pc, delta, S10_BUCKET_MS
                                ));
                            }
                        } else if delta / S10_BUCKET_MS > 1 {
                            r.s10_b03_gaps += 1;
                        }
                    }
                }
                prev_close_ms = Some(close_ms);
            }
        }

        // K3 coverage: mean bars/day vs expected (MS_PER_DAY / bucket = 8640).
        if !buckets_per_day.is_empty() {
            let expected = MS_PER_DAY as f64 / S10_BUCKET_MS as f64;
            let total: u64 = buckets_per_day.values().sum();
            let mean_per_day = total as f64 / buckets_per_day.len() as f64;
            r.s10_coverage_pct = mean_per_day / expected;
            if r.s10_coverage_pct < 0.5 {
                r.s10_coverage_fail = true;
            } else if r.s10_coverage_pct < 0.8 {
                r.s10_coverage_warn = true;
            }
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
    // R1 (CRIT): cross-shard renko continuity breaks.
    if r.renko_b03_violations > 0 {
        reasons.push(format!(
            "{} renko cross-shard discontinuity (R1)",
            r.renko_b03_violations
        ));
    }
    // R3 (HIGH): renko brick magnitude below floor.
    if r.renko_brick_floor_violations > 0 {
        reasons.push(format!(
            "{} renko brick(s) below min_pct floor (R3)",
            r.renko_brick_floor_violations
        ));
    }
    // R4 (CRIT): renko shards present but no calibrated k → stale/bootstrap k.
    if r.renko_uncalibrated {
        reasons.push("renko uncalibrated: shards exist but no k in ticker-params (R4)".to_string());
    }
    // R4 (HIGH): realized bpd outside calibrator accept band (skip stable pairs).
    if r.renko_bpd_off_band {
        reasons.push(format!(
            "renko inferred_bpd {:.1} off target {:.1} by >{:.0}% (R4)",
            r.renko_inferred_bpd,
            target_bpd,
            RENKO_BPD_ACCEPT_TOL * 100.0
        ));
    }
    // K1 (CRIT): s10 off-grid boundary (non-multiple delta).
    if r.s10_b03_violations > 0 {
        reasons.push(format!(
            "{} s10 off-grid boundary (K1)",
            r.s10_b03_violations
        ));
    }
    // K2 (HIGH): s10 bucket misalignment to UTC grid (the s10-from-idx regression).
    if r.s10_misaligned > 0 {
        reasons.push(format!("{} s10 misaligned bucket(s) (K2)", r.s10_misaligned));
    }
    // K3 (HIGH): s10 coverage < 50% of expected 8640 bars/day.
    if r.s10_coverage_fail {
        reasons.push(format!(
            "s10 coverage {:.1}% < 50% of expected (K3)",
            r.s10_coverage_pct * 100.0
        ));
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
            println!(
                "   renko calib (R4)     : inferred_bpd={:.1} target={:.1} k_present={}{}{}",
                t.renko_inferred_bpd,
                report.target_bpd,
                t.renko_k_present,
                if t.renko_uncalibrated { " [UNCALIBRATED]" } else { "" },
                if t.renko_bpd_off_band { " [OFF-BAND]" } else { "" }
            );
            println!(
                "   renko cont/floor     : R1_disc={} R3_floor_viol={}",
                t.renko_b03_violations, t.renko_brick_floor_violations
            );
            println!(
                "   renko dist (R2)      : median={} MAD={} min_day={}{}",
                fmt_sigma(t.renko_bpd_median),
                fmt_sigma(t.renko_bpd_mad),
                t.renko_bpd_min_day,
                if t.renko_dist_drift { " [WARN: drift]" } else { "" }
            );
            for s in &t.renko_samples {
                println!("     {}", s);
            }
        } else {
            println!("   renko                : no .renko shards in window");
        }
        println!(
            "   s10 kline            : {} bars, {} OHLC violations",
            t.s10_bars, t.s10_violations
        );
        println!(
            "   s10 parity           : K1_offgrid={} K1_gaps={} K2_misaligned={} coverage={}{}",
            t.s10_b03_violations,
            t.s10_b03_gaps,
            t.s10_misaligned,
            if t.s10_coverage_pct.is_finite() {
                format!("{:.1}%", t.s10_coverage_pct * 100.0)
            } else {
                "n/a".to_string()
            },
            if t.s10_coverage_fail {
                " [FAIL]"
            } else if t.s10_coverage_warn {
                " [WARN]"
            } else {
                ""
            }
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
        None => discover_tickers(&cli.common.data_root),
    };

    let mut reports: Vec<TickerReport> = Vec::with_capacity(tickers.len());
    for id in tickers {
        match audit_ticker(
            &cli.common.data_root,
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
                    renko_b03_violations: 0,
                    renko_brick_floor_violations: 0,
                    renko_k_present: false,
                    renko_uncalibrated: false,
                    renko_inferred_bpd: f64::NAN,
                    renko_bpd_off_band: false,
                    renko_bpd_median: f64::NAN,
                    renko_bpd_mad: f64::NAN,
                    renko_bpd_min_day: 0,
                    renko_dist_drift: false,
                    renko_samples: Vec::new(),
                    s10_bars: 0,
                    s10_violations: 0,
                    s10_samples: Vec::new(),
                    s10_b03_violations: 0,
                    s10_b03_gaps: 0,
                    s10_misaligned: 0,
                    s10_coverage_pct: f64::NAN,
                    s10_coverage_fail: false,
                    s10_coverage_warn: false,
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
        data_root: cli.common.data_root.display().to_string(),
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
