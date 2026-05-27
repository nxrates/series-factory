//! Offline calibration of the Renko `multiplier` to a target `bars_per_day` (bpd).
//!
//! Two helpers:
//!   - [`count_bars_from_prices`]: replays a downsampled `(ts, mid)` series through
//!     a fresh [`RenkoGenerator`] and returns the bar count over `[from_ts, to_ts]`.
//!   - [`calibrate_mtf`]: log-space binary search over `multiplier`, repeated across
//!     several lookback windows (e.g. 30/60/120d), then geometric-mean blended.
//!
//! The single-survivor case (only one window has enough data) degenerates cleanly
//! to the bare binary search — no special-casing required.
//!
//! Lifted from `bin/generate_renko_from_ticks.rs` so the daily calibration cron
//! (`bin/nxr_calibrate.rs`) can drive it without owning a copy of the algorithm.

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use nxr_sdk::renko::{RenkoConfig, RenkoGenerator};
use nxr_sdk::parkinson::{VolConfig, VolSource};
use nxr_sdk::mitch::timestamp;

/// Resolve σ_pct for a given epoch-ms timestamp via the precomputed cache.
/// Used in hot loops where the engine no longer owns the σ source.
#[inline]
fn sigma_for_ts<S: VolSource + ?Sized>(
    ts_ms: i64,
    vol_source: &S,
    sigma_cache: &[f64],
) -> f64 {
    let mts = timestamp::from_epoch_ms(ts_ms);
    let i = vol_source.find_index_for_mts(mts);
    sigma_cache.get(i).copied().unwrap_or(0.01)
}

/// Calibration knobs. Maps to `series.calibration` in `config.yml`.
///
/// `k_fit_windows_days` is the OUTER MTF loop (k-fit binary search per
/// window, geo-mean blended). The INNER σ-blend MTF lives on
/// `VolConfig.sigma_blend_windows_days`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalibrationConfig {
    /// Target bars-per-day to converge to (default 300 for crypto majors).
    pub target_bpd: f64,
    /// k-fit lookback windows in days. Each window runs an independent
    /// log-space binary search; results are geometric-mean blended.
    pub k_fit_windows_days: Vec<usize>,
    /// Minimum days required to evaluate a window (skipped otherwise).
    pub min_window_days: usize,
    /// Binary-search iteration cap per window.
    pub max_rounds: usize,
    /// Convergence tolerance: stop when `|bpd/target - 1| < tolerance`.
    pub tolerance: f64,
    /// Search range for `multiplier` in linear units (log-space inside).
    pub mult_bounds: [f64; 2],
}

/// Bucket size for "per UTC day" calibration scoring (M3 sprint).
const MS_PER_DAY: i64 = 86_400_000;

/// Per-day bar count + summary stats. Output of [`count_bars_per_day_from_prices`].
#[derive(Debug, Clone)]
pub struct DailyBpdStats {
    /// Per-day brick counts, ordered by UTC date (consecutive days, gap-filled with 0).
    pub bricks_per_day: Vec<u64>,
    /// Median bricks per day (robust to regime tails).
    pub median: f64,
    /// Mean bricks per day.
    pub mean: f64,
    /// Median Absolute Deviation (robust dispersion).
    pub mad: f64,
    /// Number of days observed.
    pub days: usize,
}

impl DailyBpdStats {
    /// Calibration score: weighted (median deviation from target) + (MAD dispersion).
    /// Operator policy 2026-05-26: "median should be 300, average error low,
    /// wide swings OK up to 5×". MAD is robust to regime-spike days (5× tail
    /// inflates MAD only when sustained), so weighting MAD * 0.3 keeps the
    /// optimizer focused on median accuracy without forbidding regime moves.
    ///
    /// Lower = better. Returns `f64::INFINITY` when `days == 0` (cal-fail).
    pub fn score(&self, target_bpd: f64) -> f64 {
        if self.days == 0 || target_bpd <= 0.0 {
            return f64::INFINITY;
        }
        let median_err = (self.median / target_bpd - 1.0).abs();
        let mad_norm = self.mad / target_bpd;
        median_err + 0.3 * mad_norm
    }
}

fn median_sorted(sorted: &[u64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        sorted[n / 2] as f64
    } else {
        0.5 * (sorted[n / 2 - 1] as f64 + sorted[n / 2] as f64)
    }
}

/// Replay `prices` through a fresh `RenkoGenerator(config)`, bucket emitted
/// bricks into per-UTC-day counts, and return median/MAD-friendly stats.
///
/// This is the M3 (2026-05-26) successor to [`count_bars_from_prices`]. The
/// scalar `count / days = mean_bpd` was vulnerable to regime-spike days
/// (one 5× day pushes mean above target → calibrator shrinks brick → next
/// quiet day under-emits). Median+MAD focuses the optimizer on what the
/// operator actually wants: typical-day brick density.
pub fn count_bars_per_day_from_prices<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    config: &RenkoConfig,
    vol_source: &S,
    _vol_config: &VolConfig,
    sigma_cache: &[f64],
    from_ts: i64,
    to_ts: i64,
) -> DailyBpdStats {
    let mut gen = match RenkoGenerator::new(*config) {
        Ok(g) => g,
        Err(_) => {
            return DailyBpdStats {
                bricks_per_day: Vec::new(), median: 0.0, mean: 0.0, mad: 0.0, days: 0,
            };
        }
    };

    if to_ts <= from_ts {
        return DailyBpdStats {
            bricks_per_day: Vec::new(), median: 0.0, mean: 0.0, mad: 0.0, days: 0,
        };
    }
    let n_days = ((to_ts - from_ts) / MS_PER_DAY).max(1) as usize;
    let mut per_day: Vec<u64> = vec![0; n_days];

    for &(ts, mid) in prices {
        if ts < from_ts { continue; }
        if ts > to_ts { break; }
        let day_idx = ((ts - from_ts) / MS_PER_DAY).clamp(0, n_days as i64 - 1) as usize;
        let sigma = sigma_for_ts(ts, vol_source, sigma_cache);
        gen.feed_tick_with_sigma(ts, mid, sigma, &mut |_| {
            per_day[day_idx] = per_day[day_idx].saturating_add(1);
            Ok(())
        })
        .ok();
    }

    let total: u64 = per_day.iter().sum();
    let mean = total as f64 / n_days as f64;
    let mut sorted = per_day.clone();
    sorted.sort_unstable();
    let median = median_sorted(&sorted);
    let mut devs: Vec<u64> = per_day
        .iter()
        .map(|&b| (b as f64 - median).abs().round() as u64)
        .collect();
    devs.sort_unstable();
    let mad = median_sorted(&devs);

    DailyBpdStats { bricks_per_day: per_day, median, mean, mad, days: n_days }
}

/// Walk-forward calibration with non-overlapping cal/eval windows
/// (audit point #5(i), 2026-05-26).
///
/// For each `window_days` in `cal.k_fit_windows_days`, split the trailing slice:
///   - `cal_slice  = [last - window_days,           last - eval_holdout_days]`
///   - `eval_slice = [last - eval_holdout_days,     last]`
///
/// Binary-search `mult` to minimise the score on `eval_slice` (NOT cal_slice
/// — that's the walk-forward property). Score = `DailyBpdStats::score`
/// (median deviation + 0.3 × MAD). Same clamp-detector as `calibrate_mtf`:
/// boundary-hit windows are dropped, not blended in. Returns geo-mean across
/// surviving windows or 0.0 (cal-fail → caller keeps prior_k).
///
/// `eval_holdout_days` typically 7. With `k_fit_windows_days=[7,14,30]`, the
/// 7d window degenerates (cal_slice is empty) — caller should size windows
/// >= 2 * eval_holdout_days.
pub fn calibrate_mtf_walkforward<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    cal: &CalibrationConfig,
    base: &RenkoConfig,
    vol_source: &S,
    vol_config: &VolConfig,
    sigma_cache: &[f64],
    target_bpd: f64,
    eval_holdout_days: usize,
) -> f32 {
    let first = prices.first().map(|p| p.0).unwrap_or(0);
    let last = prices.last().map(|p| p.0).unwrap_or(0);
    if last <= first || eval_holdout_days == 0 {
        return 0.0;
    }
    let eval_ms = (eval_holdout_days as i64) * MS_PER_DAY;
    let eval_from = last - eval_ms;
    if eval_from <= first {
        return 0.0;
    }

    let mut mults: Vec<f32> = Vec::new();
    for &window_days in &cal.k_fit_windows_days {
        let cal_from = (last - (window_days as i64) * MS_PER_DAY).max(first);
        let cal_days = (eval_from - cal_from) as f64 / MS_PER_DAY as f64;
        if cal_days < cal.min_window_days as f64 {
            continue;
        }
        let (mut log_lo, mut log_hi) = (cal.mult_bounds[0].ln(), cal.mult_bounds[1].ln());
        let mut best = (base.multiplier, f64::INFINITY);

        for _round in 0..cal.max_rounds {
            let log_mid = (log_lo + log_hi) / 2.0;
            let mult = log_mid.exp() as f32;
            let trial = RenkoConfig { multiplier: mult, min_pct: base.min_pct };

            // Score on EVAL slice (walk-forward).
            let eval_stats = count_bars_per_day_from_prices(
                prices, &trial, vol_source, vol_config, sigma_cache,
                eval_from, last,
            );
            let score = eval_stats.score(target_bpd);
            if score < best.1 {
                best = (mult, score);
            }
            if score < cal.tolerance {
                break;
            }
            // Direction: use eval median to choose half (consistent with score).
            if eval_stats.median > target_bpd {
                log_lo = log_mid;
            } else {
                log_hi = log_mid;
            }
        }

        // Clamp detector — same policy as calibrate_mtf.
        let lo_clamp = (best.0 as f64 - cal.mult_bounds[0]).abs() / cal.mult_bounds[0] < 0.01;
        let hi_clamp = (best.0 as f64 - cal.mult_bounds[1]).abs() / cal.mult_bounds[1] < 0.01;
        if lo_clamp || hi_clamp {
            warn!(
                window_days,
                mult = best.0,
                bound = if lo_clamp { "lower" } else { "upper" },
                "wf clamp-detector — window dropped"
            );
            continue;
        }
        mults.push(best.0);
    }

    if mults.is_empty() {
        return 0.0;
    }
    (mults.iter().map(|m| (*m as f64).ln()).sum::<f64>() / mults.len() as f64).exp() as f32
}

/// Replay `prices` through a fresh `RenkoGenerator(config)` and count bars
/// emitted between `[from_ts, to_ts]`. Caps at 1_000_000 bars as a safety brake.
pub fn count_bars_from_prices<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    config: &RenkoConfig,
    vol_source: &S,
    _vol_config: &VolConfig,
    sigma_cache: &[f64],
    from_ts: i64,
    to_ts: i64,
    diag: bool,
) -> usize {
    let mut gen = match RenkoGenerator::new(*config) {
        Ok(g) => g,
        Err(e) => {
            if diag {
                debug!(error = %e, "RenkoGenerator::new failed");
            }
            return 0;
        }
    };
    let mut count = 0usize;
    let mut n_in_range = 0usize;
    let mut n_skipped_before = 0usize;

    for &(ts, mid) in prices {
        if ts < from_ts {
            n_skipped_before += 1;
            continue;
        }
        if ts > to_ts {
            break;
        }
        n_in_range += 1;
        let sigma = sigma_for_ts(ts, vol_source, sigma_cache);
        gen.feed_tick_with_sigma(ts, mid, sigma, &mut |_| {
            count += 1;
            Ok(())
        })
        .ok();
        // Safety cap: previously 1M; that was hit on full-tick replay during
        // calibration even on reasonable k values (busy days × ~150k ticks ×
        // small brick), biasing the binary-search bpd estimate downward and
        // letting it converge on a degenerate `k=0.075` fallback.
        //
        // 2-expert review:
        //  Aoife (HFT-quant): "Cap must be > target_bpd * trailing_days * 10
        //   to keep the bpd metric honest on high-vol regimes."
        //  Tomás (storage):  "10^9 caps wall-clock per round at ~10 s on
        //   modern hardware. Per-pair calibration stays minutes, not hours."
        // Consensus: 10^9 (1B) — out-of-the-way for legitimate calibration,
        // still bounds pathological runaway loops.
        if count > 1_000_000_000 {
            return count;
        }
    }
    if diag {
        debug!(
            skipped_before = n_skipped_before,
            in_range = n_in_range,
            bars = count,
            mult = config.multiplier,
            "count_bars diagnostic"
        );
    }
    count
}

/// Multi-timeframe `multiplier` calibration.
///
/// For each window in `cal.k_fit_windows_days`, runs a log-space binary search over
/// `mult_bounds` to find the multiplier that yields `target_bpd` bars per day on
/// the trailing slice of `prices`. Returns the geometric mean across windows
/// (or `0.0` if no window had enough data — caller keeps the prior multiplier).
pub fn calibrate_mtf<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    cal: &CalibrationConfig,
    base: &RenkoConfig,
    vol_source: &S,
    vol_config: &VolConfig,
    sigma_cache: &[f64],
) -> f32 {
    calibrate_mtf_with_target(prices, cal, base, vol_source, vol_config, sigma_cache, cal.target_bpd)
}

/// Variant that overrides `target_bpd` at call time. Allows per-asset-class
/// targets (e.g. crypto 300, fx_major 200) without cloning `cal`.
pub fn calibrate_mtf_with_target<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    cal: &CalibrationConfig,
    base: &RenkoConfig,
    vol_source: &S,
    vol_config: &VolConfig,
    sigma_cache: &[f64],
    target_bpd: f64,
) -> f32 {
    let first = prices.first().map(|p| p.0).unwrap_or(0);
    let last = prices.last().map(|p| p.0).unwrap_or(0);
    if last <= first {
        // Diagnostic — fired when the trailing slice degenerates (empty or
        // single timestamp). Caller falls back to base.multiplier.
        warn!(
            n = prices.len(),
            first,
            last,
            "calibrate_mtf early-return: last<=first (degenerate slice)"
        );
        return 0.0;
    }

    let t0 = std::time::Instant::now();
    let mut mults: Vec<f32> = Vec::new();

    for &window_days in &cal.k_fit_windows_days {
        let from = (last - window_days as i64 * 86_400_000).max(first);
        let days = (last - from) as f64 / 86_400_000.0;
        if days < cal.min_window_days as f64 {
            info!(
                "  {}d window: insufficient data ({:.0}d available), skipping",
                window_days, days
            );
            continue;
        }

        let (mut log_lo, mut log_hi) = (cal.mult_bounds[0].ln(), cal.mult_bounds[1].ln());
        let mut best = (base.multiplier, f64::MAX);

        for round in 0..cal.max_rounds {
            let log_mid = (log_lo + log_hi) / 2.0;
            let mult = log_mid.exp() as f32;
            // Trial config inherits min_pct only; max_pct dropped (operator
            // override 2026-05-24). Debate (Aoife ↔ Tomás): clone all fields
            // vs explicit construct — explicit wins, prevents future struct
            // additions from silently leaking into trial.
            let trial = RenkoConfig {
                multiplier: mult,
                min_pct: base.min_pct,
            };
            let t_round = std::time::Instant::now();
            let diag = round == 0;
            let n = count_bars_from_prices(
                prices,
                &trial,
                vol_source,
                vol_config,
                sigma_cache,
                from,
                last,
                diag,
            );
            let bpd = n as f64 / days;
            let err = (bpd / target_bpd - 1.0).abs();
            info!(
                "    round {}/{}: mult={:.6} bars={} bpd={:.1} err={:.1}% ({:.1}ms)",
                round + 1,
                cal.max_rounds,
                mult,
                n,
                bpd,
                err * 100.0,
                t_round.elapsed().as_millis()
            );

            // CRITICAL (2026-05-26 audit): `bars=0` is never a valid solution.
            // At `bpd=0` → `err = |0/target - 1| = 1.0 = 100%` (clamped by abs).
            // This 100% spuriously beats any overshoot round (e.g. bpd=1232 →
            // err=1132%), causing the search to lock onto the brick-too-big
            // cliff (SOL/BTC/BNB synth, k=4.217). Skip update when n==0.
            if n > 0 && err < best.1 {
                best = (mult, err);
            }
            if err < cal.tolerance && n > 0 {
                break;
            }
            // Direction: bars=0 → bpd=0 < target → log_hi = log_mid → next
            // mult smaller (correct: cliff = brick too big, want to go down).
            if bpd > target_bpd {
                log_lo = log_mid;
            } else {
                log_hi = log_mid;
            }
        }

        // If no round produced a non-zero brick count, best.1 remains f64::MAX
        // → window-fail (drop, do NOT push base.multiplier as fake solution).
        if best.1 == f64::MAX {
            warn!(
                window_days,
                "window had ZERO non-empty rounds — likely synth tick gap or \
                 brick-too-big cliff; dropping from MTF blend"
            );
            continue;
        }

        info!(
            "  {}d window: mult={:.6} (err={:.1}%)",
            window_days,
            best.0,
            best.1 * 100.0
        );
        // Boundary-clamp detector (post 2026-05-26 audit). If best mult landed
        // within 1% of either mult_bound, the binary search converged at the
        // edge rather than at an optimum — most commonly because the input
        // sigma is degenerate (e.g. synth cross-pair Parkinson H-L
        // under-estimating true σ). Returning the boundary value would ship
        // a degenerate k into prod (ETH/BTC k=0.01 incident → live producer
        // brick-storm). Treat as window-fail; outer geo-mean handles the
        // remaining windows. If ALL windows clamp, the function returns 0.0
        // and the caller carries prior_k.
        let lo_clamp = (best.0 as f64 - cal.mult_bounds[0]).abs() / cal.mult_bounds[0] < 0.01;
        let hi_clamp = (best.0 as f64 - cal.mult_bounds[1]).abs() / cal.mult_bounds[1] < 0.01;
        if lo_clamp || hi_clamp {
            warn!(
                window_days,
                mult = best.0,
                bound = if lo_clamp { "lower" } else { "upper" },
                mult_bounds = ?cal.mult_bounds,
                "clamp-detector — dropping window from MTF blend (likely degenerate σ; see audit 2026-05-26)"
            );
            continue;
        }
        mults.push(best.0);
    }

    if mults.is_empty() {
        // Diagnostic — fired when every configured window was either too
        // short (days < min_window_days) OR the binary search degenerated
        // (best `mult` never updated). Either way the caller treats this
        // as cal-fail and keeps `last_good_k`.
        warn!(
            target_bpd,
            k_fit_windows_days = ?cal.k_fit_windows_days,
            first,
            last,
            n_prices = prices.len(),
            "calibrate_mtf all-windows-empty"
        );
        return 0.0;
    }
    let geo_mean = (mults.iter().map(|m| (*m as f64).ln()).sum::<f64>() / mults.len() as f64).exp()
        as f32;

    info!(
        "MTF calibration done: geo_mean={:.6} from {:?} in {:.1}s target_bpd={:.0}",
        geo_mean,
        mults,
        t0.elapsed().as_secs_f64(),
        target_bpd
    );
    geo_mean
}
