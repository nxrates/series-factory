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
use tracing::info;

use crate::bar_construction::{RenkoConfig, RenkoGenerator, VolConfig};
use crate::bar_construction::parkinson::VolSource;

/// Calibration knobs. Maps to `series.calibration` in `config.yml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalibrationConfig {
    /// Target bars-per-day to converge to (default 300 for crypto majors).
    pub target_bpd: f64,
    /// Lookback windows in days. Each window runs an independent log-space
    /// binary search; results are geometric-mean blended.
    pub windows_days: Vec<usize>,
    /// Minimum days required to evaluate a window (skipped otherwise).
    pub min_window_days: usize,
    /// Binary-search iteration cap per window.
    pub max_rounds: usize,
    /// Convergence tolerance: stop when `|bpd/target - 1| < tolerance`.
    pub tolerance: f64,
    /// Search range for `multiplier` in linear units (log-space inside).
    pub mult_bounds: [f64; 2],
}

/// Replay `prices` through a fresh `RenkoGenerator(config)` and count bars
/// emitted between `[from_ts, to_ts]`. Caps at 1_000_000 bars as a safety brake.
pub fn count_bars_from_prices<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    config: &RenkoConfig,
    vol_source: &S,
    vol_config: &VolConfig,
    sigma_cache: &[f64],
    from_ts: i64,
    to_ts: i64,
    diag: bool,
) -> usize {
    let mut gen = match RenkoGenerator::new(*config, vol_source, vol_config.clone()) {
        Ok(g) => g,
        Err(e) => {
            if diag {
                eprintln!("  [diag] RenkoGenerator::new failed: {}", e);
            }
            return 0;
        }
    };
    gen.set_sigma_cache(sigma_cache);
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
        gen.feed_tick(ts, mid, &mut |_| {
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
        eprintln!(
            "  [diag] count_bars: skipped_before={} in_range={} bars={} mult={:.6}",
            n_skipped_before, n_in_range, count, config.multiplier
        );
    }
    count
}

/// Multi-timeframe `multiplier` calibration.
///
/// For each window in `cal.windows_days`, runs a log-space binary search over
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
        eprintln!(
            "  [diag] calibrate_mtf early-return last<=first  n={} first={} last={}",
            prices.len(), first, last
        );
        return 0.0;
    }

    let t0 = std::time::Instant::now();
    let mut mults: Vec<f32> = Vec::new();

    for &window_days in &cal.windows_days {
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
            // cliff (incident: SOL/BTC synth converged on k=4.217 / 0 bars).
            // Skip best-update when n==0; direction logic below still steers
            // search downward (bpd=0 < target → log_hi=log_mid → smaller mult).
            if n > 0 && err < best.1 {
                best = (mult, err);
            }
            if err < cal.tolerance {
                break;
            }
            if bpd > target_bpd {
                log_lo = log_mid;
            } else {
                log_hi = log_mid;
            }
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
            eprintln!(
                "  [clamp-detector] window={}d mult={:.6} at-{} bound mult_bounds={:?} — \
                 dropping from MTF blend (likely degenerate σ; see audit 2026-05-26)",
                window_days,
                best.0,
                if lo_clamp { "lower" } else { "upper" },
                cal.mult_bounds
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
        eprintln!(
            "  [diag] calibrate_mtf all-windows-empty target_bpd={} windows_days={:?} \
             first={} last={} n_prices={}",
            target_bpd, cal.windows_days, first, last, prices.len()
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
