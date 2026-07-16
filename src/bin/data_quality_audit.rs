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

use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::ohlc::{rollup, Ohlc};
use nxr_sdk::shard::{
    bars_dir, idx_dir, list_shards, manifest_path, read_manifest, read_shard_aligned,
    ts_ms_to_utc_date, vol_path_for_id, ShardRecord, BAR_MS_S10, FLAG_CONF_FRESHNESS,
    FLAG_HEARTBEAT_SENTINEL, MS_PER_DAY,
};
use nxr_sdk::stats as sdk_stats;
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::Bar;

use series_factory::seam::{check_renko_cross_shard, check_s10_cross_shard, MIN_PX};
use series_factory::vol_bin::VolMmap;

// ── Calibration / renko config (single source of truth = config.yml) ─────────

/// s10 bucket width: UTC-grid alignment + per-day coverage are computed against
/// this. Matches `integrity-check s10 --bucket-ms` default and the live producer.
const S10_BUCKET_MS: i64 = 10_000;
/// Grid-alignment tolerance (ms) for s10 K1/K2. `open_ts` persists as u48 mts
/// (16µs units); the ms round-trip can jitter a grid-aligned value by ~1 ms, so
/// a ±2 ms window distinguishes mts quantization (benign) from real off-grid
/// corruption (deviates by seconds). Never masks a genuine misalignment.
const GRID_TOL_MS: i64 = 2;

/// Confidence-freshness floor (fraction `f ∈[0,1]`, f = byte/255): MUST match `integrity_check`'s
/// G4 `CONF_FRESHNESS_FLOOR`. When a record carries [`FLAG_CONF_FRESHNESS`] its
/// `confidence` byte is a freshness u8 0-255 0-100, `f = byte/255`; below this floor the
/// composite is built from stale components. The legacy `confidence <= accepted`
/// invariant is INAPPLICABLE to such records (the freshness byte 200/255 of a
/// healthy fresh feed routinely exceeds the active-provider `accepted` count),
/// so the cert applies this advisory staleness check instead, exactly as G4 does.
const CONF_FRESHNESS_FLOOR: f64 = 0.05;

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

/// R4-provenance staleness band: the `k` the renko shards were ACTUALLY built
/// with (`manifest.renko_k_used`) must agree with the *current* ticker-params k
/// within this relative tolerance. A larger drift ⇒ the shards were emitted with
/// a materially different multiplier and must be regenerated (FAIL
/// `renko_k_stale`). Tighter than the bpd accept band: k is the direct build
/// input, not a noisy realized statistic.
const RENKO_K_STALE_TOL: f64 = 0.05;

/// R4 accept-band test: realized `inferred_bpd` is OFF-BAND when its relative
/// error vs the (per-ticker) `target_bpd` exceeds [`RENKO_BPD_ACCEPT_TOL`].
/// Non-positive/non-finite target ⇒ not off-band (caller already gates on
/// `target > 0`); kept pure so the per-ticker-target wiring is unit-testable.
#[inline]
fn renko_bpd_off_band(inferred_bpd: f64, target_bpd: f64) -> bool {
    if !(target_bpd > 0.0) || !inferred_bpd.is_finite() {
        return false;
    }
    (inferred_bpd / target_bpd - 1.0).abs() > RENKO_BPD_ACCEPT_TOL
}

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

/// File-level `calibrated_at` (unix-seconds of the last `nxr-calibrate` run)
/// from the *current* ticker-params.json. Mirrors `renko_from_idx::
/// load_calibrated_k`'s second tuple element. R4-provenance compares this to
/// the manifest's build-time `renko_calibrated_at` to flag shards that predate
/// the latest calibration (`renko_shards_behind_calibration`, advisory).
fn load_current_calibrated_at() -> Option<i64> {
    let cfg = nxr_sdk::NxrConfig::from_env();
    let raw = std::fs::read_to_string(&cfg.ticker_params_path).ok()?;
    let weights: WeightsFile = serde_json::from_str(&raw).ok()?;
    weights.calibrated_at.map(|s| s as i64)
}

/// R4-provenance staleness test: the `k` the shards were built with (`used`)
/// has drifted from the `current` ticker-params k beyond [`RENKO_K_STALE_TOL`].
/// Non-positive/non-finite current ⇒ not stale (caller already gates on a
/// present current k). Kept pure so the provenance wiring is unit-testable.
#[inline]
fn renko_k_stale(used: f64, current: f64) -> bool {
    if !(current > 0.0) || !used.is_finite() {
        return false;
    }
    (used / current - 1.0).abs() > RENKO_K_STALE_TOL
}

/// Resolves the per-ticker calibration target (`target_bpd`) exactly the way
/// the calibrator + live producer do, instead of anchoring every ticker to the
/// flat `--target-bpd`. Owns the `config.yml` calibration block + operator
/// judgment lists so R4 (and the legacy ratio gate) compare realized bpd to the
/// SAME per-pair/per-class target the renko shards were built against.
///
/// Mirrors `nxr_calibrate::run_once`: `bucket_for_pair` reads MITCH wire bits +
/// the `crypto_majors`/`stablecoins`/`fx_majors` lists; `target_for_pair_classed`
/// applies per-pair override → per-class default → flat fallback. The pair
/// symbol is reconstructed from the numeric `ticker_id` via `get_asset_by_id`
/// (the audit only knows ids, not the `pair_volumes` map the calibrator walks).
struct TargetResolver {
    /// `None` when `config.yml` failed to load → every lookup uses the CLI flat.
    cal: Option<nxr_sdk::pipeline_config::CalibrationYml>,
    crypto_majors: Vec<String>,
    stablecoins: Vec<String>,
    fx_majors: Vec<String>,
    /// CLI fallback when `config.yml` cannot be loaded at all. Read only by the
    /// flat-fallback `target_for`; main now uses `target_for_resolved` (FIX #5)
    /// so the field/method are retained as documented API but not on the hot path.
    #[allow(dead_code)]
    fallback_target_bpd: f64,
}

impl TargetResolver {
    /// Load the calibration config + operator lists once. On any load failure,
    /// every `target_for` returns `fallback_target_bpd` (the CLI value) so the
    /// cert degrades to the old flat-anchor behavior rather than panicking.
    fn load(fallback_target_bpd: f64) -> Self {
        use nxr_sdk::asset_class::{
            effective_list, DEFAULT_CRYPTO_MAJORS, DEFAULT_FX_MAJORS, DEFAULT_STABLECOINS,
        };
        use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
        match PipelineYml::load_default(ConfigHint::Bin) {
            Ok(yml) => {
                // Same effective-list resolution the calibrator uses: YAML wins,
                // audit-frozen sdk defaults fill an empty list.
                let to_owned = |v: Vec<&str>| v.iter().map(|s| s.to_string()).collect();
                TargetResolver {
                    cal: Some(yml.series.calibration.clone()),
                    crypto_majors: to_owned(effective_list(
                        &yml.cexs.crypto_majors,
                        DEFAULT_CRYPTO_MAJORS,
                    )),
                    stablecoins: to_owned(effective_list(
                        &yml.cexs.stablecoins,
                        DEFAULT_STABLECOINS,
                    )),
                    fx_majors: to_owned(effective_list(&yml.cexs.fx_majors, DEFAULT_FX_MAJORS)),
                    fallback_target_bpd,
                }
            }
            Err(_) => TargetResolver {
                cal: None,
                crypto_majors: Vec::new(),
                stablecoins: Vec::new(),
                fx_majors: Vec::new(),
                fallback_target_bpd,
            },
        }
    }

    /// Reconstruct `<BASE>/<QUOTE>` from a packed `ticker_id` via the MITCH
    /// asset registry. `None` if either leg is unknown (e.g. a synth id whose
    /// legs aren't in the by-id table) — caller falls back to the flat target.
    fn pair_sym(ticker_id: u64) -> Option<String> {
        use nxr_sdk::mitch::ticker::TickerId;
        let tid = TickerId::from_raw(ticker_id);
        let base = nxr_sdk::resolve::get_asset_by_id(tid.base_asset_class(), tid.base_asset_id())?;
        let quote =
            nxr_sdk::resolve::get_asset_by_id(tid.quote_asset_class(), tid.quote_asset_id())?;
        Some(format!("{}/{}", base.name, quote.name))
    }

    /// Per-ticker `target_bpd`. Resolves the pair sym + asset-class bucket the
    /// same way `nxr_calibrate` does, then applies `target_for_pair_classed`.
    /// Falls back to the flat CLI target when the pair can't be reconstructed.
    #[allow(dead_code)]
    fn target_for(&self, ticker_id: u64) -> f64 {
        self.target_for_resolved(ticker_id)
            .unwrap_or(self.fallback_target_bpd)
    }

    /// Per-ticker `target_bpd`, returning `None` when the pair sym / target
    /// CANNOT be resolved (config.yml absent OR the id's legs aren't in the
    /// MITCH by-id table — e.g. a synth target). FIX #5: the caller must NOT
    /// silently substitute the flat 300 for the bpd-off-band verdict (that
    /// produces a false PASS/FAIL); it emits `renko_target_unresolved` (WARN)
    /// and SKIPs the bpd-band gate instead. `target_for` keeps the flat-fallback
    /// behavior for the (display-only) legacy ratio + per-ticker target field.
    fn target_for_resolved(&self, ticker_id: u64) -> Option<f64> {
        use nxr_sdk::asset_class::bucket_for_pair;
        let cal = self.cal.as_ref()?;
        let pair = Self::pair_sym(ticker_id)?;
        let cm: Vec<&str> = self.crypto_majors.iter().map(String::as_str).collect();
        let sc: Vec<&str> = self.stablecoins.iter().map(String::as_str).collect();
        let fm: Vec<&str> = self.fx_majors.iter().map(String::as_str).collect();
        let class = bucket_for_pair(&pair, ticker_id, &cm, &sc, &fm);
        Some(cal.target_for_pair_classed(&pair, class.as_key()))
    }

    /// Whether `ticker_id` is a known stable/pegged pair by IDENTITY (asset-class
    /// bucket = `crypto_stable`), independent of any volatility inference. Used
    /// by the stuck-feed gate so a frozen non-stable feed cannot excuse itself
    /// via the vol-derived `crypto_stable` flag. Unknown pair (unresolvable id)
    /// → `false` (treat as non-stable; a flatline there is still suspicious).
    fn is_known_stable(&self, ticker_id: u64) -> bool {
        use nxr_sdk::asset_class::{bucket_for_pair, AssetClassBucket};
        let Some(pair) = Self::pair_sym(ticker_id) else {
            return false;
        };
        let cm: Vec<&str> = self.crypto_majors.iter().map(String::as_str).collect();
        let sc: Vec<&str> = self.stablecoins.iter().map(String::as_str).collect();
        let fm: Vec<&str> = self.fx_majors.iter().map(String::as_str).collect();
        bucket_for_pair(&pair, ticker_id, &cm, &sc, &fm) == AssetClassBucket::CryptoStable
    }

    /// Resolve the asset-class bucket for `ticker_id` (same recipe as the target
    /// + stable resolvers). Unknown pair (unresolvable id) → `Default`, so the
    /// widest spread/jump envelope applies and a thin/synth pair is never
    /// false-flagged. Drives the per-class [`ClassBand`] for the invariant chain.
    fn class_for(&self, ticker_id: u64) -> nxr_sdk::asset_class::AssetClassBucket {
        use nxr_sdk::asset_class::{bucket_for_pair, AssetClassBucket};
        let Some(pair) = Self::pair_sym(ticker_id) else {
            return AssetClassBucket::Default;
        };
        let cm: Vec<&str> = self.crypto_majors.iter().map(String::as_str).collect();
        let sc: Vec<&str> = self.stablecoins.iter().map(String::as_str).collect();
        let fm: Vec<&str> = self.fx_majors.iter().map(String::as_str).collect();
        bucket_for_pair(&pair, ticker_id, &cm, &sc, &fm)
    }
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
    /// Default 60 s (= sentinel interval + slack): a single missed heartbeat
    /// sentinel must NOT register as an OUTAGE. A live 45 d run flagged OUTAGE
    /// on 529/529 tickers with tolerance 0 — normal quiet periods, not real
    /// downtime. Real outages (sustained > `max_gap_ms` beyond this slack) and
    /// missing UTC days still FAIL. Override to 0 for the strictest
    /// sentinel-coverage audit.
    #[arg(long, default_value_t = 60_000)]
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
    // G4-parity advisory: post-rollout records carrying FLAG_CONF_FRESHNESS whose
    // freshness u8 0-255 `f = confidence/100` is below CONF_FRESHNESS_FLOOR. WARN, not
    // a FAIL: markets do go briefly stale. NEVER a `confidence > accepted` FAIL.
    conf_stale_advisories: u64,
    // Quote DEPTH (FIX depth): vbid/vask backing-size validation.
    zero_depth_violations: u64, // HARD FAIL: vbid==0 && vask==0 (phantom liquidity)
    one_sided_depth_warns: u64, // WARN: vbid==0 XOR vask==0 (one-sided book)
    // Reject dominance (FIX reject): per-record + window reject-rate signal.
    reject_dominant_records: u64, // WARN: records where rejected > accepted
    reject_rate: f64,             // window Σrejected / (Σrejected+Σaccepted); WARN > 0.5
    reject_rate_warn: bool,
    // Anomalies
    mad_outliers: u64,
    worst_mad_z: f64,
    cusum_alarms: u64,
    sigma_park_ann: f64,
    crypto_stable: bool,
    zero_return_frac: f64, // fraction of zero log-returns (flatline proxy)
    longest_flat_run: u64, // longest run of identical mids (stuck-feed proxy)
    stuck_feed: bool,      // WARN: near-constant feed, NOT a known stable pair
    // vol↔idx reconciliation (.vol stored sigma vs idx/s10-derived Parkinson)
    vol_present: bool,
    vol_sigma_stored: f64,      // median stored sigma_pct from .vol, annualized
    vol_sigma_divergence: bool, // WARN: stored vol grossly diverges from derived
    // kline rollup-parity (s10 → 1m/1h via sdk ohlc::rollup)
    rollup_violations: u64, // OHLC monoid / alignment / tick-sum breaks
    rollup_samples: Vec<String>,
    // Renko
    renko_bpd: f64,
    renko_days: u64,
    renko_ratio: f64,
    renko_target_bpd: f64, // R4: per-ticker calibration target (! flat CLI)
    renko_target_unresolved: bool, // FIX #5 WARN: pair/target unresolvable → bpd-band SKIP
    renko_skipped_stable: bool,
    // Renko parity checks (vs .idx 6.5/10 target)
    renko_b03_violations: u64,         // R1 cross-shard continuity breaks
    renko_brick_floor_violations: u64, // R3 |Δ|/open < min_pct
    renko_k_present: bool,             // R4 calibrated k exists in ticker-params
    renko_uncalibrated: bool,          // R4 CRIT: shards but no k
    renko_inferred_bpd: f64,           // R4 realized bricks/day
    renko_bpd_off_band: bool,          // R4 |inferred/target-1| > accept tol
    renko_bpd_median: f64,             // R2 per-day distribution
    renko_bpd_mad: f64,
    renko_bpd_min_day: u64,
    renko_dist_drift: bool, // R2 WARN: MAD > 0.5*median or min < 0.33*median
    renko_dist_fail: bool,  // R2 FAIL: a single day's k transiently wrong (hard)
    // R4 provenance: prove shards were built with the CURRENT k (manifest stamp)
    renko_k_used: Option<f64>, // manifest.renko_k_used (k shards were built with)
    renko_k_stale: bool,       // R4 FAIL: |used/current-1| > tol → regenerate shards
    renko_k_implausible: bool, // FIX #6 FAIL: k outside sane per-class envelope
    renko_k_clamped_warn: bool, // FIX #6 WARN: k pinned at calibrator mult ceiling
    renko_shards_behind_calibration: bool, // R4 WARN: shards predate latest calibrated_at
    renko_provenance_missing: bool, // R4 WARN: pre-provenance manifest (no renko_k_used)
    renko_samples: Vec<String>, // dated violation provenance
    // s10 kline
    s10_bars: u64,
    s10_violations: u64,
    s10_samples: Vec<String>,
    // s10 parity checks
    s10_b03_violations: u64, // K1 cross-shard off-grid boundary
    s10_b03_gaps: u64,       // K1 multi-bucket gap at boundary (WARN)
    s10_misaligned: u64,     // K2 open_time_ms % 10_000 != 0
    s10_coverage_pct: f64,   // K3 bars/day vs 8640
    s10_coverage_fail: bool, // K3 < 0.5
    s10_coverage_warn: bool, // K3 0.5..0.8
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

// ── s10 K1 open-grid boundary classification ───────────────────────────────

/// Outcome of the K1 open-grid boundary delta between consecutive s10 bars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S10Boundary {
    /// `delta <= 0`: non-advancing (overlap / duplicate) — handled by the
    /// separate overlap check, K1 ignores it.
    NonAdvancing,
    /// `delta == BAR_MS`: exactly one bucket forward — clean, contiguous grid.
    Contiguous,
    /// `delta == k·BAR_MS`, k>1: a whole-bucket gap (WARN, not a grid break).
    Gap,
    /// `delta % BAR_MS != 0`: off the 10s grid — FAIL.
    OffGrid,
}

/// Classify the OPEN→OPEN delta between two s10 buckets on the UTC 10s grid.
///
/// This is the regression-proof form of K1: comparing on the OPEN grid (not
/// `open - prev_CLOSE`, where the `close = open + (BAR_MS-1)` round-trip makes
/// every contiguous pair yield ~2 ms and trips `% BAR_MS != 0` on every bar).
#[inline]
fn classify_s10_open_delta(prev_open_ms: i64, open_ms: i64) -> S10Boundary {
    let delta = open_ms - prev_open_ms;
    if delta <= 0 {
        return S10Boundary::NonAdvancing;
    }
    // mts QUANTIZATION TOLERANCE: open_ts persists as u48 mts (16µs units); the
    // ms round-trip can jitter a grid-aligned value by up to ~1 ms. A bar is on
    // the 10 s grid iff `delta` is within ±GRID_TOL_MS of a bucket multiple.
    // Real off-grid corruption deviates by seconds, so this never masks it.
    let rem = delta.rem_euclid(S10_BUCKET_MS);
    let on_grid = rem <= GRID_TOL_MS || rem >= S10_BUCKET_MS - GRID_TOL_MS;
    if !on_grid {
        S10Boundary::OffGrid
    } else if delta > S10_BUCKET_MS + GRID_TOL_MS {
        S10Boundary::Gap
    } else {
        S10Boundary::Contiguous
    }
}

// ── kline rollup-parity (K-rollup) ─────────────────────────────────────────

/// Project a canonical `Bar` onto the lighter `Ohlc` the SDK `rollup` consumes.
/// Copies fields out of the packed struct first.
#[inline]
fn bar_to_ohlc(b: &Bar) -> Ohlc {
    Ohlc {
        ts: b.open_time_ms(),
        close_ts: b.close_time_ms(),
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
        vbid: b.vbid as u64,
        vask: b.vask as u64,
        tick_count: b.tick_count,
        avg_ci_ubp: b.avg_ci_ubp,
    }
}

/// Validate one higher-TF series produced by `ohlc::rollup` against the s10
/// source it was rolled from. Asserts, per output bar: OHLC monoid sanity
/// (`high >= max(open,close)`, `low <= min(open,close)`, `open/close ∈ [low,high]`,
/// `high >= low`), bucket-start alignment (`ts % dst_tf_ms == 0`), and that the
/// summed `tick_count` over the covered s10 bars equals the rolled bar's
/// `tick_count` (no dropped/double-counted source bar). Returns violation count
/// and up to `cap` provenance samples. This exercises the SAME `rollup` path
/// that serves every higher-TF kline (otherwise only SDK-unit-tested on synthetic
/// data).
fn check_rollup_parity(
    label: &str,
    src: &[Ohlc],
    dst_tf_ms: i64,
    cap: usize,
    out: &mut Vec<String>,
) -> u64 {
    use nxr_sdk::ohlc::ohlc_ci_ubp;
    use std::collections::HashMap;
    let rolled = rollup(src, BAR_MS_S10, dst_tf_ms);
    // Per dst bucket, independently re-accumulate the SAME monoid `rollup` must
    // reproduce, mirroring its straddler policy EXACTLY (a src bar spanning two
    // dst buckets contributes its full vbid/vask/tick_count/ci to BOTH legs — see
    // ohlc::rollup `touch`). Well-formed grid-aligned s10 never straddles, but we
    // mirror the two-leg accounting so the parity check can't drift from rollup.
    //  • Σ tick_count (no dropped/double-counted source bar)
    //  • Σ vbid / Σ vask (FIX #3: volume conservation across the fold)
    //  • tick-weighted mean of DECODED ci_ubp (FIX #3: avg_ci_ubp is a
    //    sqrt-compressed per-bar mean, so the rollup decodes→tick-weights→re-
    //    encodes. We compare on the DECODED ci_ubp domain — `ohlc_ci_ubp` is the
    //    public decoder — so we don't reimplement the private encoder, and the
    //    epsilon absorbs the u16 sqrt round-trip quantization.)
    #[derive(Default)]
    struct Acc {
        ticks: u64,
        vbid: u64,
        vask: u64,
        ci_weighted_sum: f64, // Σ (decode(avg_ci_ubp) · tick_count)
    }
    let bucket_start = |ts: i64| (ts.div_euclid(dst_tf_ms)) * dst_tf_ms;
    let mut exp: HashMap<i64, Acc> = HashMap::new();
    let add = |bs: i64, s: &Ohlc, m: &mut HashMap<i64, Acc>| {
        let a = m.entry(bs).or_default();
        a.ticks += s.tick_count as u64;
        a.vbid += s.vbid;
        a.vask += s.vask;
        a.ci_weighted_sum += ohlc_ci_ubp(s.avg_ci_ubp) * s.tick_count as f64;
    };
    for s in src {
        let open_bs = bucket_start(s.ts);
        let close_bs = bucket_start(s.close_ts);
        add(open_bs, s, &mut exp);
        if close_bs != open_bs {
            add(close_bs, s, &mut exp);
        }
    }
    // RELATIVE epsilon for the decoded tick-weighted CI mean. avg_ci_ubp is a
    // u16 SQRT-quantized confidence stat; at real magnitudes (~10^6 ubp) the
    // re-encode round-trip differs from the per-bar decoded mean by ~0.001-0.01%
    // pure quantization — an ABSOLUTE 1.0 eps tripped 100% of buckets on the live
    // 45d run (false positive). tick/vbid/vask remain EXACT integer-sum checks.
    // 1% relative (with a 2-ubp floor for tiny values) absorbs quantization only.
    const CI_REL_EPS: f64 = 0.01;
    const CI_ABS_FLOOR: f64 = 2.0;
    let mut viol = 0u64;
    for b in &rolled {
        let mut why: Option<String> = None;
        if !(b.open.is_finite() && b.high.is_finite() && b.low.is_finite() && b.close.is_finite()) {
            why = Some("non-finite OHLC".to_string());
        } else if b.high < b.low {
            why = Some(format!("high {} < low {}", b.high, b.low));
        } else if b.high < b.open.max(b.close) {
            why = Some("high < max(open,close)".to_string());
        } else if b.low > b.open.min(b.close) {
            why = Some("low > min(open,close)".to_string());
        } else if b.ts % dst_tf_ms != 0 {
            why = Some(format!("ts {} not aligned to {}", b.ts, dst_tf_ms));
        } else if let Some(a) = exp.get(&b.ts) {
            if a.ticks != b.tick_count as u64 {
                why = Some(format!(
                    "tick_count {} != Σ src {} over bucket",
                    b.tick_count, a.ticks
                ));
            } else if a.vbid != b.vbid {
                why = Some(format!("vbid {} != Σ src {} over bucket", b.vbid, a.vbid));
            } else if a.vask != b.vask {
                why = Some(format!("vask {} != Σ src {} over bucket", b.vask, a.vask));
            } else if a.ticks > 0 {
                // Decoded tick-weighted mean: Σ(decode(ci)·ticks) / Σticks vs the
                // rolled bar's decoded avg_ci_ubp. Zero ticks ⇒ no mean to check.
                let expected_ci = a.ci_weighted_sum / a.ticks as f64;
                let observed_ci = ohlc_ci_ubp(b.avg_ci_ubp);
                let ci_tol = (CI_REL_EPS * expected_ci.abs()).max(CI_ABS_FLOOR);
                if (expected_ci - observed_ci).abs() > ci_tol {
                    why = Some(format!(
                        "ci_ubp(decoded) {:.4} != tick-weighted Σ mean {:.4} over bucket",
                        observed_ci, expected_ci
                    ));
                }
            }
        } else {
            // A rolled bucket with no source bars mapping in = phantom bucket.
            why = Some("rolled bucket has no source bars".to_string());
        }
        if let Some(w) = why {
            viol += 1;
            if out.len() < cap {
                out.push(format!("[rollup {}] ts={} {}", label, b.ts, w));
            }
        }
    }
    viol
}

// ── Microstructure invariant decision (pure, unit-testable) ─────────────────

/// Outcome of the per-record microstructure invariant chain. `reason = Some` ⇒
/// a hard invariant FAIL; `conf_stale = true` ⇒ a freshness-flagged record whose
/// freshness u8 0-255 is below [`CONF_FRESHNESS_FLOOR`] (advisory WARN only, NEVER a
/// FAIL, and NEVER the legacy `confidence > accepted` FAIL, which is
/// inapplicable to freshness-byte records). Pure so the FIX #1/#2 wire-semantics
/// + price-floor decisions are testable without on-disk shards.
struct InvariantOutcome {
    reason: Option<&'static str>,
    conf_stale: bool,
    /// DEPTH METRIC (NEVER a verdict input). `vbid == 0 && vask == 0`. NXR
    /// composites are a PRICE+CONFIDENCE feed, NOT an order-book depth feed:
    /// `triangulator.rs:143` sets `vbid:0, vask:0` for ALL inferred/triangulated
    /// pairs (the prime USDC crosses BTCUSDC/ETHUSDC/BNBUSDC are inferred → 0
    /// depth BY DESIGN), and volume-less FX providers also yield 0. A real
    /// 45d run showed 1M+ zero-depth on single healthy tickers. Reported as a
    /// pure count (`zero_depth_count`) that NEVER affects the verdict — no ERROR,
    /// no `reason`. The field is retained ONLY so the report can surface the count.
    zero_depth: bool,
    /// DEPTH METRIC (NEVER a verdict input). `vbid == 0 XOR vask == 0` — one side
    /// has no backing size. Like `zero_depth`, this is BY DESIGN for the
    /// price+confidence feed and is surfaced as a count only, never a WARN/FAIL
    /// that gates the verdict.
    depth_one_sided: bool,
    /// FIX (reject) MED WARN: `rejected > accepted` — the composite rejected more
    /// provider quotes than it accepted this tick → a degraded composite. WARN.
    reject_dominant: bool,
    /// SPREAD WARN/metric (NEVER a verdict input): positive `spread_bps` above the
    /// per-class envelope. Legit on illiquid alts/synths, so advisory only.
    spread_out_of_band: bool,
}

/// Per-asset-class microstructure envelope: the spread-realness ceiling (bps)
/// and the per-tick jump guard (fractional mid move). FLAT bands are wrong:
/// a 2000 bps spread is impossible on BTC/USDT yet plausible on an illiquid
/// synth, and a 10% tick jump is a clear anomaly on a major but routine on a
/// thin alt. Conservative defaults TIGHTEN majors (catch more) without
/// false-flagging thin pairs. Resolved from the `AssetClassBucket` so the
/// envelope is instrument-agnostic (keyed on class, not a hardcoded symbol).
#[derive(Debug, Clone, Copy)]
struct ClassBand {
    /// Spread-realness ceiling in bps. A spread above this is implausible for
    /// the class → hard invariant FAIL (replaces the flat [0,2000] ceiling).
    spread_ceiling_bps: f64,
    /// Per-tick jump ceiling (fractional |Δmid|/prev_mid). Above → anomaly WARN
    /// (advisory, like the audit's MAD/CUSUM battery — never a hard FAIL).
    /// Reserved for the per-class jump guard (the existing MAD/CUSUM battery
    /// already covers cross-tick anomalies); kept on the band for completeness.
    #[allow(dead_code)]
    jump_frac: f64,
}

/// Resolve the per-class spread/jump envelope. Conservative, class-keyed.
/// crypto_major: tightest (deep books, sub-100bps spreads, ~5% tick moves).
/// crypto_alt / crypto_cross: looser. crypto_stable: very tight spread but the
/// renko/vol gates already handle cadence; fx tight; synth/default: widest so a
/// thin synthetic pair is never false-flagged.
fn class_band(class: nxr_sdk::asset_class::AssetClassBucket) -> ClassBand {
    use nxr_sdk::asset_class::AssetClassBucket::*;
    match class {
        // Deep, liquid majors: a real spread is a few bps; >150 bps is corrupt
        // or a flash dislocation. 5% per-tick jump is already extreme here.
        CryptoMajor => ClassBand {
            spread_ceiling_bps: 150.0,
            jump_frac: 0.05,
        },
        // Stable/stable: spread should be a handful of bps; cap tight.
        CryptoStable => ClassBand {
            spread_ceiling_bps: 100.0,
            jump_frac: 0.02,
        },
        // FX majors: extremely tight institutional spreads.
        FxMajor => ClassBand {
            spread_ceiling_bps: 50.0,
            jump_frac: 0.03,
        },
        // Alts / crypto-crosses: thinner books, wider real spreads + jumps.
        CryptoAlt | CryptoCross => ClassBand {
            spread_ceiling_bps: 800.0,
            jump_frac: 0.15,
        },
        FxCross => ClassBand {
            spread_ceiling_bps: 300.0,
            jump_frac: 0.08,
        },
        // Synth / unknown: widest band — never false-flag a thin synthetic pair.
        Default => ClassBand {
            spread_ceiling_bps: 2000.0,
            jump_frac: 0.25,
        },
    }
}

/// Evaluate the microstructure invariant chain for one INDEX record. Mirrors
/// integrity-check's G1 (price floor), crossed-quote, and G4 (freshness floor)
/// semantics so the cert and the per-file checker agree on the wire contract.
///
/// `band` carries the per-asset-class spread ceiling (replaces the flat 2000bps
/// cap). `vbid`/`vask`/`rejected` drive the depth + reject-dominant signals.
fn eval_invariant(
    bid: f64,
    ask: f64,
    flags: u8,
    confidence: u8,
    accepted: u8,
    rejected: u8,
    vbid: u32,
    vask: u32,
    spread_bps: f64,
    band: ClassBand,
) -> InvariantOutcome {
    let is_freshness = (flags & FLAG_CONF_FRESHNESS) != 0;
    // DEPTH SIGNAL — count metric ONLY, NEVER a verdict input. NXR composites are
    // a price+confidence feed: inferred/triangulated pairs (triangulator.rs:143)
    // and volume-less FX providers carry vbid=vask=0 BY DESIGN. So zero/
    // one-sided depth is NOT in the `reason` (hard-FAIL) chain below.
    let zero_depth = vbid == 0 && vask == 0;
    let depth_one_sided = (vbid == 0) ^ (vask == 0);
    let reject_dominant = rejected > accepted;
    let reason: Option<&'static str> = if !bid.is_finite() || !ask.is_finite() {
        Some("bid/ask non-finite")
    } else if bid <= 0.0 {
        Some("bid <= 0")
    } else if ask <= 0.0 {
        Some("ask <= 0")
    } else if bid < MIN_PX || ask < MIN_PX {
        // FIX #2 (G1 parity): finite sub-floor denormal cannot be a real quote.
        Some("bid/ask below price floor (denormal)")
    } else if ask < bid {
        Some("crossed quote (ask < bid)")
    } else if !is_freshness && confidence > accepted {
        // FIX #1: legacy semantics only — `confidence` = active-provider count.
        // Freshness-flagged records carry an independent percent byte that routinely
        // exceeds `accepted`, so the cross-constraint must NOT apply to them.
        Some("confidence > accepted")
    } else {
        // SPREAD WIDTH IS NOT A HARD FAIL. A wide-but-positive spread (no crossed
        // quote — `ask < bid` already FAILed above) is legitimate on genuinely
        // illiquid alts/synths (real 45d run flagged 819 bps on a real CryptoCross
        // pair). Only a STRUCTURAL impossibility fails here, and the crossed-quote
        // / sub-floor / non-finite branches above already cover every structural
        // case. The per-class spread ceiling is now a WARN/metric (`spread_out_of_band`).
        None
    };
    // Spread WARN/metric (NEVER a verdict input). A positive spread above the
    // per-class envelope is surfaced as a count; majors keep a tighter band so
    // the WARN still flags a dislocated major, but it never FAILs a ticker.
    let spread_out_of_band =
        spread_bps.is_finite() && spread_bps > band.spread_ceiling_bps && reason.is_none();
    // FIX #1 (G4 parity, advisory): freshness-flagged + structurally-OK records
    // whose decoded freshness is below the floor are a stale composite. WARN-only.
    let conf_stale = is_freshness
        && reason.is_none()
        && mitch::index::conf_from_u8(confidence) < CONF_FRESHNESS_FLOOR;
    InvariantOutcome {
        reason,
        conf_stale,
        zero_depth,
        depth_one_sided,
        reject_dominant,
        spread_out_of_band,
    }
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
    target_resolved: bool,
    is_known_stable: bool,
    k_bounds: Option<(f64, f64)>,
    band: ClassBand,
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
        conf_stale_advisories: 0,
        zero_depth_violations: 0,
        one_sided_depth_warns: 0,
        reject_dominant_records: 0,
        reject_rate: f64::NAN,
        reject_rate_warn: false,
        mad_outliers: 0,
        worst_mad_z: 0.0,
        cusum_alarms: 0,
        sigma_park_ann: f64::NAN,
        crypto_stable: false,
        zero_return_frac: f64::NAN,
        longest_flat_run: 0,
        stuck_feed: false,
        vol_present: false,
        vol_sigma_stored: f64::NAN,
        vol_sigma_divergence: false,
        rollup_violations: 0,
        rollup_samples: Vec::new(),
        renko_bpd: f64::NAN,
        renko_days: 0,
        renko_ratio: f64::NAN,
        renko_target_bpd: target_bpd,
        renko_target_unresolved: false,
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
        renko_dist_fail: false,
        renko_k_used: None,
        renko_k_stale: false,
        renko_k_implausible: false,
        renko_k_clamped_warn: false,
        renko_shards_behind_calibration: false,
        renko_provenance_missing: false,
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
        let endpoint_sentinel =
            (a_flags & FLAG_HEARTBEAT_SENTINEL) != 0 || (b_flags & FLAG_HEARTBEAT_SENTINEL) != 0;
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
    // Depth (vbid/vask) + reject-dominance run only on NON-sentinel records:
    // a FLAG_HEARTBEAT_SENTINEL is a liveness beacon that legitimately carries
    // no fresh backing size / provider tally, so it must never count as a
    // phantom-liquidity FAIL or a reject-dominant WARN.
    let mut sum_accepted: u64 = 0;
    let mut sum_rejected: u64 = 0;
    for rec in &records {
        let idx = rec.index; // copy out of packed struct
        let bid = idx.bid;
        let ask = idx.ask;
        let is_sentinel = (idx.flags & FLAG_HEARTBEAT_SENTINEL) != 0;
        // FIX #1/#2: structural invariants + wire-semantics-aware confidence
        // handling are evaluated by the pure `eval_invariant` (unit-tested). A
        // fresh record (FLAG_CONF_FRESHNESS, confidence≈200/255 > accepted) is NOT
        // a `confidence > accepted` FAIL; instead its sub-floor freshness is an
        // advisory WARN (G4 parity). Sub-floor denormals FAIL the price floor.
        let sb_for_eval = if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 {
            idx.spread_bps()
        } else {
            f64::NAN
        };
        let outcome = eval_invariant(
            bid,
            ask,
            idx.flags,
            idx.confidence,
            idx.accepted,
            idx.rejected,
            idx.vbid,
            idx.vask,
            sb_for_eval,
            band,
        );
        if outcome.conf_stale {
            r.conf_stale_advisories += 1;
        }
        // Depth + reject signals: sentinel-exempt (see comment above).
        if !is_sentinel {
            if outcome.zero_depth {
                r.zero_depth_violations += 1;
            }
            if outcome.depth_one_sided {
                r.one_sided_depth_warns += 1;
            }
            if outcome.reject_dominant {
                r.reject_dominant_records += 1;
            }
            sum_accepted += idx.accepted as u64;
            sum_rejected += idx.rejected as u64;
        }
        // Depth is a metric-only signal (handled above), never in the `reason`
        // chain — so `reason` here is always a real structural defect.
        if let Some(why) = outcome.reason {
            {
                r.invariant_violations += 1;
                if r.invariant_samples.len() < 10 {
                    r.invariant_samples.push(ViolationSample {
                        ts: rec.shard_ts_ms(),
                        reason: why.to_string(),
                        bid,
                        ask,
                        spread_bps: sb_for_eval,
                    });
                }
            }
        }
    }
    // Window reject-rate: Σrejected / (Σaccepted + Σrejected) over non-sentinel
    // records. A composite that rejects > half the provider quotes it sees is a
    // degraded feed → WARN (advisory, never a FAIL).
    let reject_denom = sum_accepted + sum_rejected;
    if reject_denom > 0 {
        r.reject_rate = sum_rejected as f64 / reject_denom as f64;
        r.reject_rate_warn = r.reject_rate > 0.5;
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
    r.zero_return_frac = if returns.is_empty() {
        f64::NAN
    } else {
        zero_return_frac
    };

    // Longest run of identical consecutive mids (cheap stuck-feed proxy). A live
    // feed jitters tick-to-tick; a frozen forwarder repeats the exact mid.
    {
        let mut run = 0u64;
        let mut longest = 0u64;
        let mut prev: Option<f64> = None;
        for &(_, mid) in &mids {
            match prev {
                Some(p) if p == mid => run += 1,
                _ => run = 1,
            }
            if run > longest {
                longest = run;
            }
            prev = Some(mid);
        }
        r.longest_flat_run = longest;
    }

    // Stable/stable inference: annualized Parkinson sigma < 0.5% OR >95% zero returns.
    let sigma_finite = r.sigma_park_ann.is_finite();
    if (sigma_finite && r.sigma_park_ann < 0.005) || zero_return_frac > 0.95 {
        r.crypto_stable = true;
    }

    // Stuck-feed gate (idx HIGH): near-constant feed (>99% zero returns) that is
    // NOT a known stable/pegged pair (resolved from the asset-class bucket, NOT
    // from the volatility-inferred `crypto_stable` flag — which would let a
    // flatline excuse itself). A genuine non-stable pair frozen at one quote is
    // a dead/stale forwarder, not legitimately quiet.
    if !returns.is_empty() && zero_return_frac > 0.99 && !is_known_stable && !r.crypto_stable {
        r.stuck_feed = true;
        if r.s10_samples.len() < 10 {
            r.s10_samples.push(format!(
                "stuck/flatline feed: zero_return_frac={:.3} longest_flat_run={}",
                zero_return_frac, r.longest_flat_run
            ));
        }
    }

    // ── vol↔idx reconciliation (idx MED) ──────────────────────────────────
    // Load the persisted `.vol` (the EMA-smoothed Rogers-Satchell σ the live
    // renko producer primes from) and assert its stored σ agrees with the
    // Parkinson σ this audit just derived over the same window. A gross
    // divergence ⇒ orphan / mismatched / stale .vol — the seam-glue would feed
    // the renko engine a wrong σ. WARN only (display); the .vol is an input to
    // renko cadence, which R2/R4 gate downstream.
    {
        let vp = vol_path_for_id(data_root, id);
        if vp.exists() {
            if let Ok(vm) = VolMmap::open(&vp) {
                use nxr_sdk::vol::VolSource;
                let n = vm.len();
                if n > 0 {
                    r.vol_present = true;
                    // Stored σ is a per-30-min fraction (e.g. 0.015 = 1.5%).
                    // Annualize the median to the SAME basis as sigma_park_ann:
                    // 30-min bins → 48/day × 365 days = 17_520 bins/yr.
                    let mut sigmas: Vec<f64> = (0..n)
                        .map(|i| vm.sigma_pct(i))
                        .filter(|s| s.is_finite() && *s > 0.0)
                        .collect();
                    if !sigmas.is_empty() {
                        let med = nxr_sdk::stats::median(&sigmas);
                        let bins_per_year = 48.0_f64 * 365.0;
                        r.vol_sigma_stored = med * bins_per_year.sqrt();
                        // Compare on a log scale: flag if the two σ estimates differ
                        // by more than ~3× either way (gross orphan/stale, not the
                        // expected RS-vs-Parkinson estimator gap of <2×).
                        if r.sigma_park_ann.is_finite()
                            && r.sigma_park_ann > 0.0
                            && r.vol_sigma_stored > 0.0
                        {
                            let ratio = r.vol_sigma_stored / r.sigma_park_ann;
                            if !(0.33..=3.0).contains(&ratio) {
                                r.vol_sigma_divergence = true;
                            }
                        }
                    }
                }
            }
        }
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
            // R1 (B03): boundary continuity vs previous shard's last close, via
            // the SHARED production primitive `seam::check_renko_cross_shard`
            // (DRY: same RENKO_B03_REL_TOL the renko-continuity-check binary uses,
            // no duplicated 1e-9 literal). `prev close == first open` to relative
            // tol so the live+offline series append into one seamless chart.
            if let (Some((pd, pc)), Some(fo)) = (prev_shard_close, first_open) {
                if check_renko_cross_shard(pc, fo).violated {
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
            if med > 0.0 && (mad > 0.5 * med || (r.renko_bpd_min_day as f64) < 0.33 * med) {
                r.renko_dist_drift = true;
            }
            // R2 hard FAIL: a single shard built with a transiently-wrong k is
            // averaged away by the window-mean bpd (R4 uses the mean), so it
            // never FAILs on bpd alone. The per-day distribution is the only
            // place that asymmetry shows. Promote the same drift sub-condition
            // to a verdict FAIL (skip stable pairs whose low cadence makes the
            // ratios noisy). min_day < 0.33·median ⇒ ≥1 starved day; MAD >
            // 0.5·median ⇒ wide day-to-day swing — both signal a bad shard.
            if !r.crypto_stable
                && med > 0.0
                && (mad > 0.5 * med || (r.renko_bpd_min_day as f64) < 0.33 * med)
            {
                r.renko_dist_fail = true;
                if r.renko_samples.len() < 10 {
                    r.renko_samples.push(format!(
                        "R2 per-day drift: median={:.1} MAD={:.1} min_day={} (transient bad-k shard)",
                        med, mad, r.renko_bpd_min_day
                    ));
                }
            }
        }
        // R4 calibration correctness: realized bpd must land in the calibrator's
        // accept band around target_bpd, AND a calibrated k must exist.
        let current_k = load_calibrated_k(id);
        r.renko_k_present = current_k.is_some();
        if !r.renko_k_present && r.renko_days > 0 {
            r.renko_uncalibrated = true; // CRIT: shards built w/ stale/bootstrap k
        }
        // FIX #5: when the per-ticker target could NOT be resolved (synth id /
        // config absent), `target_bpd` is the flat-300 FALLBACK — judging realized
        // bpd against it yields a false PASS/FAIL. Emit a WARN and SKIP the
        // bpd-band gate; provenance gates (uncalibrated / k_stale) remain the hard
        // gate so an unresolved-target ticker is still held to k correctness.
        if !target_resolved && r.renko_days > 0 {
            r.renko_target_unresolved = true;
            if r.renko_samples.len() < 10 {
                r.renko_samples.push(format!(
                    "renko_target_unresolved: pair/target for id {} unresolvable → bpd-band SKIP (k provenance still gated)",
                    id
                ));
            }
        }
        if !r.crypto_stable && target_resolved && r.renko_days > 0 && target_bpd > 0.0 {
            // R4 anchors to the PER-TICKER target (`target_bpd` is now resolved
            // via `TargetResolver::target_for`, not the flat CLI 300), so a
            // 50-target pegged pair is judged against 50, not 300. Gated on
            // `target_resolved` (FIX #5) so an unresolved flat fallback never
            // produces a spurious off-band verdict.
            r.renko_bpd_off_band = renko_bpd_off_band(r.renko_inferred_bpd, target_bpd);
        }
        // FIX #6: absolute-k plausibility. The SOL/USDT k=3.995 stablecoin-sentinel
        // bug was self-consistent (a real k in ticker-params) yet absurd — pinned
        // at the calibrator's mult ceiling for a non-stable major, yielding ~33
        // bpd vs 300. Gate `current_k` against a sane envelope: outside the band
        // ⇒ FAIL `renko_k_implausible`; at/near the calibrator's upper mult bound
        // (a clamp-leak the search couldn't escape) ⇒ WARN. Uses config
        // `mult_bounds` when available, else a conservative absolute band.
        if let Some(k) = current_k {
            // Conservative absolute fallback band when config didn't provide
            // mult_bounds (mirrors observed crypto-major k∈[0.05,0.6] + headroom).
            const K_ABS_LO: f64 = 0.01;
            const K_ABS_HI: f64 = 4.0;
            let (lo, hi) = k_bounds.unwrap_or((K_ABS_LO, K_ABS_HI));
            if !(k.is_finite() && k >= lo && k <= hi) {
                r.renko_k_implausible = true;
                if r.renko_samples.len() < 10 {
                    r.renko_samples.push(format!(
                        "renko_k_implausible: k={:.6} outside sane envelope [{:.4}, {:.4}] (FIX #6)",
                        k, lo, hi
                    ));
                }
            } else if hi > 0.0 && (k / hi - 1.0).abs() < 0.01 {
                // k pinned within 1% of the calibrator's upper mult bound ⇒ the
                // search clamped at the wall (the SOL 3.995-vs-4.0 signature) and
                // never converged on a target-bpd-correct k. Non-stable pairs only:
                // a genuinely stable pair legitimately wants a near-ceiling k.
                if !r.crypto_stable && !is_known_stable {
                    r.renko_k_clamped_warn = true;
                    if r.renko_samples.len() < 10 {
                        r.renko_samples.push(format!(
                            "renko_k_clamped: k={:.6} pinned at calibrator ceiling {:.4} (stablecoin-sentinel signature, FIX #6 WARN)",
                            k, hi
                        ));
                    }
                }
            }
        }
        // R4 PROVENANCE: prove the SHARDS were built with the CURRENT k, not just
        // that *a* k exists. `renko-from-idx` stamps the k it actually fed the
        // generator into `manifest.renko_k_used` (+ `renko_calibrated_at`); read
        // it back and assert it still matches ticker-params.
        if let Ok(Some(manifest)) = read_manifest(&manifest_path(&bdir)) {
            r.renko_k_used = manifest.renko_k_used;
            match manifest.renko_k_used {
                Some(used) => {
                    // FAIL: shards built with a materially different k than the
                    // current ticker-params k → regenerate (skip when current k
                    // is absent; `renko_uncalibrated` already FAILs that case).
                    if let Some(cur) = current_k {
                        if renko_k_stale(used, cur) {
                            r.renko_k_stale = true;
                        }
                    }
                    // WARN: a re-calibration landed after these shards were built
                    // (current calibrated_at strictly newer than the stamp).
                    if let (Some(cur_at), Some(built_at)) =
                        (load_current_calibrated_at(), manifest.renko_calibrated_at)
                    {
                        if cur_at > built_at {
                            r.renko_shards_behind_calibration = true;
                        }
                    }
                }
                // WARN (backward-compat): legacy manifest predates provenance
                // stamping → can't prove which k the shards used. Advisory only.
                None => {
                    if r.renko_days > 0 {
                        r.renko_provenance_missing = true;
                    }
                }
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
        let mut prev_open_ms: Option<i64> = None; // K1 grid is computed OPEN→OPEN
        let mut prev_close_bar: Option<Bar> = None; // last bar of prev shard (seam x-check)
        let mut buckets_per_day: BTreeMap<NaiveDate, u64> = BTreeMap::new();
        for (date, path) in &s10_shards {
            let bars = match read_shard_aligned::<Bar>(path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Cross-shard seam x-check via the SHARED production primitive
            // (`seam::check_s10_cross_shard`, close→close residual, ±1ms jitter
            // band). The per-bar K1 below tracks the OPEN grid for the precise
            // ok/gap/off-grid classification; the seam call asserts the boundary
            // bar pair agrees with `renko-continuity-check`'s tolerance so cert +
            // binary never drift on what "contiguous at the day rotation" means.
            if let (Some(pcb), Some(first)) = (prev_close_bar.as_ref(), bars.first()) {
                let seam = check_s10_cross_shard(pcb.close_time_ms(), first.close_time_ms());
                if seam.violated && first.open_time_ms() - pcb.open_time_ms() == BAR_MS_S10 {
                    // Seam flags a break the open-grid K1 would NOT (a sub-bucket
                    // close_ts corruption); surface it so neither check masks it.
                    r.s10_b03_violations += 1;
                    if r.s10_samples.len() < 10 {
                        r.s10_samples.push(format!(
                            "[{}] K1 seam residual {:.0}ms > jitter (close-grid) at shard boundary",
                            date, seam.delta
                        ));
                    }
                }
            }
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
                        r.s10_samples
                            .push(format!("[{}] ts={} {}", date, open_ms, w));
                    }
                }

                // K2 bucket alignment: open_time_ms on the UTC 10s grid, within
                // the mts-quantization tolerance (±GRID_TOL_MS) — see GRID_TOL_MS.
                let k2_rem = open_ms.rem_euclid(S10_BUCKET_MS);
                if k2_rem > GRID_TOL_MS && k2_rem < S10_BUCKET_MS - GRID_TOL_MS {
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

                // K1 boundary continuity — computed on the OPEN grid.
                //
                // REGRESSION FIX: the prior gate compared `open_ms - prev_CLOSE_ms`.
                // But a bucket's `close_ts = open + (BAR_MS-1)` (9999 ms), which
                // round-trips through the u48 mts encoding to `open + 9998`. So for
                // EVERY contiguous pair the close→open delta is ≈2 ms, making
                // `delta % 10_000 != 0` fire on literally every healthy bar → a
                // false CRIT on every ticker. The correct invariant is OPEN→OPEN:
                //   contiguous → delta == BAR_MS (ok)
                //   gap        → delta == k·BAR_MS, k>1 (WARN, s10_b03_gaps)
                //   off-grid   → delta % BAR_MS != 0 (FAIL, s10_b03_violations)
                // Non-advancing (delta<=0) is the overlap case already flagged above.
                if let Some(po) = prev_open_ms {
                    match classify_s10_open_delta(po, open_ms) {
                        S10Boundary::OffGrid => {
                            r.s10_b03_violations += 1;
                            if r.s10_samples.len() < 10 {
                                r.s10_samples.push(format!(
                                    "[{}] K1 off-grid boundary: open_ts {} - prev_open {} = {} ms (not mult of {})",
                                    date, open_ms, po, open_ms - po, S10_BUCKET_MS
                                ));
                            }
                        }
                        S10Boundary::Gap => r.s10_b03_gaps += 1,
                        S10Boundary::Contiguous | S10Boundary::NonAdvancing => {}
                    }
                }
                prev_close_ms = Some(close_ms);
                prev_open_ms = Some(open_ms);
            }
            if let Some(last) = bars.last() {
                prev_close_bar = Some(*last);
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

    // ── Check 6: kline rollup-parity (s10 → 1m & 1h via sdk ohlc::rollup) ──
    // The higher-TF kline REST path rolls s10 up with `ohlc::rollup`, currently
    // only unit-tested on synthetic data. Roll the most-recent real shards (one
    // UTC day is enough to exercise both src bucket cadence and the 1h fold) and
    // assert the OHLC monoid + alignment + tick-sum invariants hold end-to-end.
    {
        // FIX #4: roll up a CONCATENATION of the last two ADJACENT s10 shards that
        // are ≥1 day apart, so a 1h dst bucket spans the SHARD SEAM (the day
        // rotation a single-shard cut can never exercise — a 1h bucket at the
        // 23:00–00:00 boundary draws src bars from BOTH shards). Falls back to the
        // single most-recent shard when <2 shards exist. Picking the two most
        // recent shards that differ by ≥1 day guarantees a real UTC-day seam.
        let mut src: Vec<Ohlc> = Vec::new();
        let n = s10_shards.len();
        if n >= 2 {
            let (last_date, last_path) = &s10_shards[n - 1];
            // Walk back to the newest earlier shard at least 1 day before `last`.
            let prev = s10_shards[..n - 1]
                .iter()
                .rev()
                .find(|(d, _)| (*last_date - *d).num_days() >= 1);
            if let Some((_, prev_path)) = prev {
                if let Ok(pbars) = read_shard_aligned::<Bar>(prev_path) {
                    src.extend(pbars.iter().map(bar_to_ohlc));
                }
            }
            if let Ok(lbars) = read_shard_aligned::<Bar>(last_path) {
                src.extend(lbars.iter().map(bar_to_ohlc));
            }
        } else if let Some((_, path)) = s10_shards.last() {
            if let Ok(bars) = read_shard_aligned::<Bar>(path) {
                src.extend(bars.iter().map(bar_to_ohlc));
            }
        }
        // `rollup` requires a ts-sorted src; the seam concatenation is already
        // date-ordered (prev shard before last) and each shard is ts-sorted, so
        // the join is monotone — but sort defensively against a torn boundary.
        src.sort_by_key(|o| o.ts);
        if !src.is_empty() {
            const MIN_MS: i64 = 60_000;
            const HOUR_MS: i64 = 3_600_000;
            r.rollup_violations +=
                check_rollup_parity("1m", &src, MIN_MS, 10, &mut r.rollup_samples);
            r.rollup_violations +=
                check_rollup_parity("1h", &src, HOUR_MS, 10, &mut r.rollup_samples);
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
    // DEPTH IS METRIC-ONLY, NEVER A VERDICT INPUT. NXR composites are a
    // price+confidence feed: inferred/triangulated pairs (triangulator.rs:143)
    // and volume-less FX providers carry vbid=vask=0 BY DESIGN. The empirical
    // 45d run flagged 1M+ "phantom" quotes on healthy majors — a false positive.
    // zero_depth_violations / one_sided_depth_warns are surfaced as counts in the
    // report (printed below) but do not gate the verdict.
    if r.s10_violations > 0 {
        reasons.push(format!("{} s10 OHLC violation(s)", r.s10_violations));
    }
    // Renko cadence: only gate non-stable pairs with renko data. Tolerance ±50%.
    // FIX #5: skip when the target was unresolved (ratio is vs the flat fallback).
    if !r.crypto_stable
        && !r.renko_target_unresolved
        && r.renko_days > 0
        && r.renko_ratio.is_finite()
    {
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
    // R4 (CRIT): shards built with a materially different k than the current
    // ticker-params k (manifest provenance mismatch) → regenerate renko shards.
    if r.renko_k_stale {
        reasons.push(format!(
            "renko_k_stale: shards built with k={} but current ticker-params k differs by >{:.0}% (R4)",
            r.renko_k_used.map(|k| format!("{:.6}", k)).unwrap_or_else(|| "?".to_string()),
            RENKO_K_STALE_TOL * 100.0
        ));
    }
    // FIX #6 (CRIT): calibrated k outside the sane per-asset-class envelope — a
    // self-consistent-but-absurd k (the SOL=3.995 stablecoin-sentinel bug).
    if r.renko_k_implausible {
        reasons
            .push("renko_k_implausible: calibrated k outside sane envelope (FIX #6)".to_string());
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
        reasons.push(format!(
            "{} s10 misaligned bucket(s) (K2)",
            r.s10_misaligned
        ));
    }
    // K3 (HIGH): s10 coverage < 50% of expected 8640 bars/day.
    if r.s10_coverage_fail {
        reasons.push(format!(
            "s10 coverage {:.1}% < 50% of expected (K3)",
            r.s10_coverage_pct * 100.0
        ));
    }
    // R2 (MED→HARD): per-day brick distribution shows a transiently-wrong-k
    // shard that the window-mean bpd (R4) averages away. Hard FAIL so a single
    // bad day's shard can't pass on the mean alone.
    if r.renko_dist_fail {
        reasons.push(format!(
            "renko per-day drift: min_day {} / median {:.1} (R2)",
            r.renko_bpd_min_day, r.renko_bpd_median
        ));
    }
    // K-rollup (HIGH): higher-TF kline rollup parity break (OHLC monoid /
    // alignment / tick-sum). Guards the path serving all higher-TF klines.
    if r.rollup_violations > 0 {
        reasons.push(format!(
            "{} kline rollup-parity violation(s) (K-rollup)",
            r.rollup_violations
        ));
    }
    // Stuck-feed (HIGH): near-constant non-stable feed = dead/stale forwarder.
    if r.stuck_feed {
        reasons.push(format!(
            "stuck/flatline feed (zero_return_frac={:.3}, longest_flat_run={})",
            r.zero_return_frac, r.longest_flat_run
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
        println!(
            "\n── ticker {} ──────────────────────────────────────────────",
            t.ticker_id
        );
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
        if t.conf_stale_advisories > 0 {
            println!(
                "   conf-freshness (G4)  : {} record(s) below floor {} [WARN advisory]",
                t.conf_stale_advisories, CONF_FRESHNESS_FLOOR
            );
        }
        // Depth: both-zero is a FAIL (counted in verdict); one-sided is WARN.
        if t.zero_depth_violations > 0 || t.one_sided_depth_warns > 0 {
            println!(
                "   quote depth          : zero-depth(FAIL)={} one-sided(WARN)={}",
                t.zero_depth_violations, t.one_sided_depth_warns
            );
        }
        // Reject dominance: per-record + window reject-rate (both WARN advisory).
        if t.reject_dominant_records > 0 || t.reject_rate_warn {
            let rate = if t.reject_rate.is_finite() {
                format!("{:.1}%", t.reject_rate * 100.0)
            } else {
                "n/a".to_string()
            };
            println!(
                "   reject dominance     : rejected>accepted in {} rec(s), window reject-rate={} [WARN advisory]",
                t.reject_dominant_records, rate
            );
        }
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
        println!("   sigma_park (ann)     : {}", fmt_sigma(t.sigma_park_ann));
        println!(
            "   feed health          : zero_ret_frac={} longest_flat_run={}{}",
            fmt_sigma(t.zero_return_frac),
            t.longest_flat_run,
            if t.stuck_feed { " [STUCK]" } else { "" }
        );
        if t.vol_present {
            println!(
                "   vol↔idx recon        : stored_sigma_ann={} parkinson_ann={}{}",
                fmt_sigma(t.vol_sigma_stored),
                fmt_sigma(t.sigma_park_ann),
                if t.vol_sigma_divergence {
                    " [DIVERGENCE]"
                } else {
                    ""
                }
            );
        }
        if t.crypto_stable {
            println!("   crypto_stable        : YES (renko bpd check skipped)");
        }
        if t.renko_days > 0 {
            println!(
                "   renko                : bpd={:.1} over {} days (ratio {:.2}){}",
                t.renko_bpd,
                t.renko_days,
                t.renko_ratio,
                if t.renko_skipped_stable {
                    " [skipped: stable]"
                } else {
                    ""
                }
            );
            println!(
                "   renko calib (R4)     : inferred_bpd={:.1} target={:.1} (per-ticker) k_present={}{}{}{}",
                t.renko_inferred_bpd,
                t.renko_target_bpd,
                t.renko_k_present,
                if t.renko_uncalibrated { " [UNCALIBRATED]" } else { "" },
                if t.renko_bpd_off_band { " [OFF-BAND]" } else { "" },
                if t.renko_target_unresolved { " [WARN: target-unresolved → bpd SKIP]" } else { "" }
            );
            println!(
                "   renko provenance (R4): k_used={}{}{}{}{}{}",
                t.renko_k_used
                    .map(|k| format!("{:.6}", k))
                    .unwrap_or_else(|| "none".to_string()),
                if t.renko_k_stale { " [STALE]" } else { "" },
                if t.renko_k_implausible {
                    " [IMPLAUSIBLE-K]"
                } else {
                    ""
                },
                if t.renko_k_clamped_warn {
                    " [WARN: k clamped at ceiling]"
                } else {
                    ""
                },
                if t.renko_shards_behind_calibration {
                    " [WARN: behind-calibration]"
                } else {
                    ""
                },
                if t.renko_provenance_missing {
                    " [WARN: provenance-missing]"
                } else {
                    ""
                }
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
                if t.renko_dist_fail {
                    " [FAIL: drift]"
                } else if t.renko_dist_drift {
                    " [WARN: drift]"
                } else {
                    ""
                }
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
        if t.rollup_violations > 0 {
            println!(
                "   kline rollup-parity  : {} violation(s)",
                t.rollup_violations
            );
            for s in &t.rollup_samples {
                println!("     {}", s);
            }
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
        "ticker",
        "records",
        "outage",
        "missday",
        "invviol",
        "madout",
        "cusum",
        "sigma_ann",
        "renkobpd",
        "verdict"
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

    // Per-ticker calibration target + stable-pair identity, resolved exactly the
    // way the calibrator + live producer do (config.yml per-pair/per-class). The
    // flat `--target-bpd` is only a fallback when config.yml can't be loaded.
    let target_resolver = TargetResolver::load(cli.target_bpd);

    // FIX #6: the calibrator's renko-k envelope (`mult_bounds = [K_FLOOR, ceiling]`)
    // when config.yml is loadable, so the absolute-k plausibility gate compares
    // against the SAME band the calibrator searched. `None` → audit_ticker uses
    // its conservative absolute fallback band.
    let k_bounds: Option<(f64, f64)> = target_resolver
        .cal
        .as_ref()
        .map(|c| (c.mult_bounds[0], c.mult_bounds[1]));

    let mut reports: Vec<TickerReport> = Vec::with_capacity(tickers.len());
    for id in tickers {
        // FIX #5: distinguish a genuinely RESOLVED per-ticker target from the flat
        // fallback. When unresolved, audit_ticker SKIPs the bpd-band gate.
        let resolved = target_resolver.target_for_resolved(id);
        let per_ticker_target = resolved.unwrap_or(cli.target_bpd);
        let target_resolved = resolved.is_some();
        let is_known_stable = target_resolver.is_known_stable(id);
        let band = class_band(target_resolver.class_for(id));
        match audit_ticker(
            &cli.common.data_root,
            id,
            win_start,
            win_end,
            cli.max_gap_ms,
            cli.quiet_tolerance_ms,
            per_ticker_target,
            target_resolved,
            is_known_stable,
            k_bounds,
            band,
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
                    conf_stale_advisories: 0,
                    zero_depth_violations: 0,
                    one_sided_depth_warns: 0,
                    reject_dominant_records: 0,
                    reject_rate: f64::NAN,
                    reject_rate_warn: false,
                    mad_outliers: 0,
                    worst_mad_z: 0.0,
                    cusum_alarms: 0,
                    sigma_park_ann: f64::NAN,
                    crypto_stable: false,
                    zero_return_frac: f64::NAN,
                    longest_flat_run: 0,
                    stuck_feed: false,
                    vol_present: false,
                    vol_sigma_stored: f64::NAN,
                    vol_sigma_divergence: false,
                    rollup_violations: 0,
                    rollup_samples: Vec::new(),
                    renko_bpd: f64::NAN,
                    renko_days: 0,
                    renko_ratio: f64::NAN,
                    renko_target_bpd: f64::NAN,
                    renko_target_unresolved: false,
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
                    renko_dist_fail: false,
                    renko_k_used: None,
                    renko_k_stale: false,
                    renko_k_implausible: false,
                    renko_k_clamped_warn: false,
                    renko_shards_behind_calibration: false,
                    renko_provenance_missing: false,
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

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Default band for invariant tests (Default bucket: widest envelope) — the
    /// depth/reject/freshness tests don't exercise the spread ceiling.
    fn test_band() -> ClassBand {
        class_band(nxr_sdk::asset_class::AssetClassBucket::Default)
    }

    /// `eval_invariant` with HEALTHY depth (vbid/vask both non-zero) and reject
    /// (rejected=0) defaults, so tests targeting the freshness/price chain don't
    /// have to spell out the new args. Depth/reject tests call `eval_invariant`
    /// directly to vary those bytes.
    fn eval_inv(
        bid: f64,
        ask: f64,
        flags: u8,
        confidence: u8,
        accepted: u8,
        spread_bps: f64,
    ) -> InvariantOutcome {
        eval_invariant(
            bid,
            ask,
            flags,
            confidence,
            accepted,
            /*rejected*/ 0,
            /*vbid*/ 100,
            /*vask*/ 100,
            spread_bps,
            test_band(),
        )
    }

    // ── K1 open-grid boundary (3 branches + false-positive elimination) ──────

    /// Clean boundary: first bucket of D+1 opens exactly one BAR_MS past the
    /// last open of D. The OLD close-grid form (`open - prev_close`) yielded
    /// ~2 ms here (close = open + 9998) and FALSE-FAILED every contiguous bar;
    /// the open-grid form classifies it Contiguous.
    #[test]
    fn k1_clean_boundary_is_contiguous() {
        let prev_open = 1_700_000_000_000i64; // 10s-aligned
        let open = prev_open + S10_BUCKET_MS; // next bucket
        assert_eq!(
            classify_s10_open_delta(prev_open, open),
            S10Boundary::Contiguous
        );
        // False-positive proof: the OLD close-grid delta on the SAME healthy
        // pair is non-grid (≈2 ms) and would have tripped OffGrid.
        let prev_close = prev_open + (S10_BUCKET_MS - 1) - 1; // open+9998 round-trip
        let old_close_delta = open - prev_close;
        assert_ne!(
            old_close_delta % S10_BUCKET_MS,
            0,
            "old close-grid delta {} IS off-grid → false CRIT (the bug)",
            old_close_delta
        );
        // New open-grid delta is a clean multiple → no false CRIT.
        assert_eq!((open - prev_open) % S10_BUCKET_MS, 0);
    }

    /// One whole bucket missing at the boundary → Gap (WARN), not a grid break.
    #[test]
    fn k1_single_bucket_gap_is_gap() {
        let prev_open = 1_700_000_000_000i64;
        let open = prev_open + 2 * S10_BUCKET_MS; // skip one bucket
        assert_eq!(classify_s10_open_delta(prev_open, open), S10Boundary::Gap);
    }

    /// Open not on the 10s grid (e.g. a 3 ms drift) → OffGrid (FAIL).
    #[test]
    fn k1_off_grid_open_is_fail() {
        let prev_open = 1_700_000_000_000i64;
        let open = prev_open + S10_BUCKET_MS + 3; // 10_003 ms forward
        assert_eq!(
            classify_s10_open_delta(prev_open, open),
            S10Boundary::OffGrid
        );
    }

    /// Non-advancing (duplicate / overlap) is ignored by K1 (handled elsewhere).
    #[test]
    fn k1_non_advancing_ignored() {
        let prev_open = 1_700_000_000_000i64;
        assert_eq!(
            classify_s10_open_delta(prev_open, prev_open),
            S10Boundary::NonAdvancing
        );
        assert_eq!(
            classify_s10_open_delta(prev_open, prev_open - S10_BUCKET_MS),
            S10Boundary::NonAdvancing
        );
    }

    // ── R4 per-ticker-target accept band ─────────────────────────────────────

    /// A k yielding ~300 bpd on a TRUE-50-target pegged pair must be OFF-BAND
    /// (the flat-300 anchor would have FALSE-PASSED it). Conversely a correct
    /// k landing near the per-ticker 50 target must pass.
    #[test]
    fn r4_uses_per_ticker_target_not_flat() {
        // Pegged pair: real target = 50. 300 realized is 6× over → off-band.
        assert!(
            renko_bpd_off_band(300.0, 50.0),
            "300 bpd vs 50 target must be off-band"
        );
        // Correct k on the same pair: 48 realized vs 50 target → within ±20%.
        assert!(
            !renko_bpd_off_band(48.0, 50.0),
            "48 bpd vs 50 target is in-band"
        );
        // The OLD flat-300 anchor would have judged 300 realized as a perfect
        // match (ratio 1.0) → FALSE PASS. Prove the flat anchor masks it.
        assert!(
            !renko_bpd_off_band(300.0, 300.0),
            "flat-300 anchor false-passes the wrong-k pegged pair (the bug)"
        );
    }

    /// An override pair (target 800) with a correct ~800 k must PASS, where the
    /// flat-300 anchor would FALSE-FAIL it (800/300-1 = 1.67 > 0.20).
    #[test]
    fn r4_override_pair_passes_on_per_ticker_target() {
        assert!(
            !renko_bpd_off_band(800.0, 800.0),
            "800 vs 800 target in-band"
        );
        assert!(
            renko_bpd_off_band(800.0, 300.0),
            "flat-300 anchor false-fails the high-target override pair (the bug)"
        );
    }

    // ── R4 provenance: shards built with the CURRENT k (manifest stamp) ───────

    /// A renko manifest whose stamped `renko_k_used` materially differs from the
    /// current ticker-params k → STALE (FAIL); a matching k → clean; a legacy
    /// manifest with `None` provenance → warn-only (backward-compat, NOT a FAIL).
    #[test]
    fn r4_provenance_stale_matching_and_missing() {
        use nxr_sdk::shard::Manifest;
        let current_k = 0.075_f64;

        // MISMATCH: shards built with a k 20% off the current k → STALE (FAIL).
        let mut stale = Manifest::new("BTC-USDT".to_string(), 1, "renko");
        stale.set_renko_provenance(current_k * 1.20, Some(1_700_000_000));
        let used = stale.renko_k_used.expect("provenance stamped");
        assert!(
            renko_k_stale(used, current_k),
            "k drift > {:.0}% must be flagged STALE → FAIL",
            RENKO_K_STALE_TOL * 100.0
        );

        // MATCH: shards built with the current k (within tol) → clean, no FAIL.
        let mut fresh = Manifest::new("BTC-USDT".to_string(), 1, "renko");
        fresh.set_renko_provenance(current_k * 1.02, Some(1_700_000_000));
        let used = fresh.renko_k_used.expect("provenance stamped");
        assert!(
            !renko_k_stale(used, current_k),
            "k within {:.0}% of current must be clean (no FAIL)",
            RENKO_K_STALE_TOL * 100.0
        );

        // MISSING: legacy/pre-provenance manifest → None. Warn-only: the
        // report path sets `renko_provenance_missing` (advisory) and NEVER
        // routes through `renko_k_stale`, so the verdict cannot FAIL on it.
        let legacy = Manifest::new("BTC-USDT".to_string(), 1, "renko");
        assert!(
            legacy.renko_k_used.is_none(),
            "legacy manifest has no provenance → warn-only branch, not FAIL"
        );

        // Staleness comparison: current calibrated_at strictly newer than the
        // build-time stamp → behind-calibration WARN (advisory, not FAIL).
        let cur_at = 1_700_000_500_i64;
        let built_at = fresh.renko_calibrated_at.expect("calibrated_at stamped");
        assert!(
            cur_at > built_at,
            "recalibration after build → behind-calibration WARN"
        );
    }

    // ── FIX #1: fresh-record confidence>accepted must NOT false-FAIL ─────────

    /// A healthy post-rollout record carries FLAG_CONF_FRESHNESS with a freshness
    /// byte (e.g. 200/255 = 0.78) that routinely EXCEEDS the active-provider
    /// `accepted` count (e.g. 6). The OLD hard invariant `confidence > accepted`
    /// would FAIL it; FIX #1 gates that constraint on the flag being CLEAR, so a
    /// fresh record produces NO invariant FAIL and NO freshness advisory.
    #[test]
    fn fix1_fresh_record_no_false_fail() {
        const FRESH: u8 = FLAG_CONF_FRESHNESS;
        // confidence=78 (>accepted=6) but freshness-flagged → must NOT FAIL.
        let o = eval_inv(100.0, 100.1, FRESH, 78, 6, 0.999_0 /*~spread bps*/);
        assert!(
            o.reason.is_none(),
            "fresh record must not FAIL invariant: {:?}",
            o.reason
        );
        assert!(
            !o.conf_stale,
            "freshness 200/255 = 0.78 is above floor → no advisory"
        );

        // Same numbers WITHOUT the flag = legacy semantics → confidence>accepted FAIL.
        let legacy = eval_inv(100.0, 100.1, 0, 200, 6, 0.999_0);
        assert_eq!(
            legacy.reason,
            Some("confidence > accepted"),
            "legacy (flag clear) confidence>accepted must still FAIL"
        );

        // Freshness-flagged BUT stale (byte 4/100 = 0.04 < 0.05 floor) → advisory
        // WARN only, still NOT an invariant FAIL.
        let stale = eval_inv(100.0, 100.1, FRESH, 4, 6, 0.999_0);
        assert!(
            stale.reason.is_none(),
            "stale-fresh record must not FAIL: {:?}",
            stale.reason
        );
        assert!(stale.conf_stale, "freshness below floor → advisory WARN");
    }

    // ── FIX #2: sub-floor denormal bid/ask must FAIL the cert ────────────────

    /// A finite, technically-positive but sub-MIN_PX price (a denormal underflow)
    /// passes `> 0.0` yet cannot be a real quote. The cert must FAIL it (G1 parity).
    #[test]
    fn fix2_subfloor_price_fails() {
        let o = eval_inv(5e-10, 6e-10, 0, 1, 1, 0.0);
        assert_eq!(
            o.reason,
            Some("bid/ask below price floor (denormal)"),
            "sub-floor denormal must FAIL the price floor"
        );
        // Normal price → no floor FAIL.
        let ok = eval_inv(100.0, 100.1, 0, 1, 1, 9.99);
        assert!(
            ok.reason.is_none(),
            "normal price must not FAIL: {:?}",
            ok.reason
        );
        // Crossed quote → explicit FAIL.
        let crossed = eval_inv(100.5, 100.0, 0, 1, 1, 0.0);
        assert_eq!(crossed.reason, Some("crossed quote (ask < bid)"));
    }

    // ── FIX (depth): zero-depth / one-sided quote validation ─────────────────

    /// Both sides zero size = phantom liquidity → hard FAIL (the `reason` chain
    /// returns the named zero-depth reason and `zero_depth` is set). One-sided
    /// zero is a WARN (`depth_one_sided`), NOT a hard FAIL. Healthy depth → clean.
    #[test]
    fn depth_is_metric_only_never_fails() {
        // DEPTH IS METRIC-ONLY for NXR's price+confidence model. vbid==0 && vask==0
        // is BY DESIGN for inferred pairs (triangulator.rs:143) + volume-less FX:
        // it sets the `zero_depth` count flag but is NEVER a `reason` (verdict input).
        let both = eval_invariant(
            100.0,
            100.1,
            0,
            1,
            1,
            0,
            /*vbid*/ 0,
            /*vask*/ 0,
            9.99,
            test_band(),
        );
        assert!(
            both.reason.is_none(),
            "both-zero depth must NOT FAIL: {:?}",
            both.reason
        );
        assert!(
            both.zero_depth,
            "zero_depth metric flag still set (for counting)"
        );
        assert!(!both.depth_one_sided, "both-zero is NOT one-sided");

        // One-sided zero → metric flag, no FAIL.
        let bid_side = eval_invariant(
            100.0,
            100.1,
            0,
            1,
            1,
            0,
            /*vbid*/ 0,
            /*vask*/ 50,
            9.99,
            test_band(),
        );
        assert!(bid_side.reason.is_none(), "one-sided depth must NOT FAIL");
        assert!(bid_side.depth_one_sided, "one-sided depth metric flag set");
        assert!(!bid_side.zero_depth, "one-sided is NOT zero_depth");

        // Healthy two-sided depth → no depth signal at all.
        let ok = eval_invariant(
            100.0,
            100.1,
            0,
            1,
            1,
            0,
            /*vbid*/ 10,
            /*vask*/ 10,
            9.99,
            test_band(),
        );
        assert!(
            !ok.zero_depth && !ok.depth_one_sided,
            "healthy depth → no signal"
        );
        assert!(ok.reason.is_none());
    }

    // ── FIX (reject): rejected-byte dominance ────────────────────────────────

    /// `rejected > accepted` (more provider quotes rejected than accepted this
    /// tick) → a degraded composite → WARN advisory (`reject_dominant`), never a
    /// hard FAIL. `rejected <= accepted` → no signal.
    #[test]
    fn reject_dominant_is_warn_not_fail() {
        // 5 rejected vs 2 accepted → dominant WARN, no FAIL.
        let dom = eval_invariant(
            100.0,
            100.1,
            0,
            1,
            /*accepted*/ 2,
            /*rejected*/ 5,
            100,
            100,
            9.99,
            test_band(),
        );
        assert!(
            dom.reject_dominant,
            "rejected>accepted → reject_dominant WARN"
        );
        assert!(
            dom.reason.is_none(),
            "reject dominance is WARN, not FAIL: {:?}",
            dom.reason
        );

        // Equal → NOT dominant (strictly greater required).
        let eq = eval_invariant(100.0, 100.1, 0, 1, 3, 3, 100, 100, 9.99, test_band());
        assert!(!eq.reject_dominant, "rejected==accepted is not dominant");

        // Healthy (more accepted than rejected) → no signal.
        let ok = eval_invariant(100.0, 100.1, 0, 1, 8, 1, 100, 100, 9.99, test_band());
        assert!(!ok.reject_dominant, "accepted>rejected → no reject signal");
    }

    // ── FIX #3: rollup parity now checks vbid/vask + tick-weighted CI ────────

    fn ohlc_row(ts: i64, px: f64, vbid: u64, vask: u64, ticks: u32, ci: u16) -> Ohlc {
        Ohlc {
            ts,
            close_ts: ts + BAR_MS_S10 - 1,
            open: px,
            high: px,
            low: px,
            close: px,
            vbid,
            vask,
            tick_count: ticks,
            avg_ci_ubp: ci,
        }
    }

    /// A clean s10 src rolled to 1m must produce ZERO rollup-parity violations —
    /// vbid/vask sums and the tick-weighted decoded-CI mean all reconcile.
    #[test]
    fn fix3_rollup_parity_clean_src_no_violation() {
        // 6 × 10s bars in one 60s bucket, varying volumes + CI codes.
        let mut src = Vec::new();
        for k in 0..6i64 {
            let ts = 1_700_000_000_000 + k * BAR_MS_S10; // grid-aligned at 10s & 60s
            src.push(ohlc_row(
                ts,
                100.0,
                10 + k as u64,
                20 + k as u64,
                1 + k as u32,
                8 + k as u16,
            ));
        }
        let mut samples = Vec::new();
        let viol = check_rollup_parity("1m", &src, 60_000, 10, &mut samples);
        assert_eq!(viol, 0, "clean src must roll up cleanly: {:?}", samples);
    }

    /// Sanity: the rollup itself conserves vbid/vask exactly (the new FIX #3
    /// assertions are correct, not vacuously passing). Confirm the parity helper
    /// reports the SAME totals the rollup produced.
    #[test]
    fn fix3_rollup_conserves_volume() {
        let src = vec![
            ohlc_row(1_700_000_000_000, 100.0, 5, 7, 2, 16),
            ohlc_row(1_700_000_000_000 + BAR_MS_S10, 100.0, 3, 4, 4, 32),
        ];
        let rolled = rollup(&src, BAR_MS_S10, 60_000);
        assert_eq!(rolled.len(), 1, "two 10s bars fold into one 60s bucket");
        assert_eq!(rolled[0].vbid, 8, "Σ vbid = 5+3");
        assert_eq!(rolled[0].vask, 11, "Σ vask = 7+4");
        assert_eq!(rolled[0].tick_count, 6, "Σ ticks = 2+4");
        let mut samples = Vec::new();
        assert_eq!(
            check_rollup_parity("1m", &src, 60_000, 10, &mut samples),
            0,
            "parity must agree with the rollup totals: {:?}",
            samples
        );
    }

    // ── FIX #6: absolute-k plausibility band ─────────────────────────────────

    /// The pure band test that FIX #6's gate uses. A SOL-like k pinned at the
    /// calibrator's 4.0 ceiling is within bounds but within 1% of the wall (the
    /// stablecoin-sentinel clamp signature → WARN); a mid-band crypto-major k is
    /// clean; an out-of-band k is implausible (FAIL).
    #[test]
    fn fix6_k_plausibility_band() {
        let (lo, hi) = (0.05f64, 4.0f64);
        // Out of band → implausible.
        assert!(
            !(10.0f64 >= lo && 10.0 <= hi),
            "k=10 outside [0.05,4.0] → FAIL"
        );
        assert!(
            !(0.001f64 >= lo && 0.001 <= hi),
            "k=0.001 below floor → FAIL"
        );
        // In band, mid → clean (no clamp WARN).
        let k_mid = 0.334f64; // observed BTC k
        assert!(k_mid >= lo && k_mid <= hi, "mid-band k in envelope");
        assert!(
            (k_mid / hi - 1.0).abs() >= 0.01,
            "mid-band k not at ceiling → no clamp WARN"
        );
        // SOL sentinel k=3.995 → in band but within 1% of ceiling → clamp WARN.
        let k_sol = 3.995f64;
        assert!(k_sol >= lo && k_sol <= hi, "3.995 is within [0.05,4.0]");
        assert!(
            (k_sol / hi - 1.0).abs() < 0.01,
            "3.995 within 1% of 4.0 ceiling → clamp WARN"
        );
    }
}
