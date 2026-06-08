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

use nxr_sdk::renko::{RenkoConfig, RenkoGenerator, K_FLOOR, SIGMA_FALLBACK};
use nxr_sdk::vol::{VolConfig, VolSource};
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
    sigma_cache.get(i).copied().unwrap_or(SIGMA_FALLBACK)
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

use nxr_sdk::shard::MS_PER_DAY;

/// bpd-accept gate tolerance: a window's chosen k is rejected when its
/// FULL-history median bpd deviates from target by more than this fraction.
/// Tightened 0.20 → 0.08 (2026-06-09) now that the accept objective is anchored
/// to the full-history median (FIX 1) and the score is pure-median (FIX 2b), so
/// the fit objective == the operator's full-history measurement.
const RENKO_BPD_ACCEPT_TOL: f64 = 0.05;

/// PART B (2026-06-09 accuracy redesign): walk-forward fold-agreement guard.
/// The per-window search is DEMOTED to an overfit guard: each fold still picks a
/// k_w, but we no longer geo-mean them into the answer. If the folds disagree by
/// more than this ratio (`max(k_w)/min(k_w) > K_AGREEMENT_MAX`) the windows are
/// fitting different regimes → reject (return 0.0, caller drops the entry).
const K_AGREEMENT_MAX: f64 = 1.5;

/// PART B selection stop criterion: log-k bracket half-width at which the
/// full-history-median bisection halts. `ln(1.005)` ⇒ the bracket [exp(lo),
/// exp(hi)] is ≤ 0.5 % wide in k. We STOP ON BRACKET WIDTH, not on score: the
/// snap_to_25_grid lattice + integer-day median make bpd(k) a STAIRCASE, so a
/// score-stop stalls on a flat tread while the true crossing sits at the rung
/// edge. The two-sided rung probe then picks the closer of the two bracketing
/// rungs.
const K_BRACKET_LN_EPS: f64 = 0.004988; // ln(1.005) → 0.5% k-width

/// Dispersion side-guard bound (FIX 2b, 2026-06-09): reject a window when its
/// MAD/median exceeds this. Kept as a SEPARATE diagnostic guard — NOT folded
/// into the minimized score (the old `0.3*MAD/target` term biased k upward by
/// rewarding the dispersion-compressing effect of bigger bricks). 1.0 = MAD as
/// large as the median itself, a sane "this window is pure noise" ceiling.
const RENKO_DISPERSION_MAX: f64 = 1.0;

/// Spike/gap quarantine for a per-day brick-count vector (RCA ROOT2c).
///
/// Drops gap days (zero counts) and spike days (outside `[median/3, median*3]`
/// of the non-zero median), then returns the surviving "clean" days. Returns
/// `None` when too few clean days remain (the slice is too gappy/spiky to
/// trust) — the caller then marks the window dead (`days=0`) so it is dropped
/// rather than fitting noise.
///
/// `min_clean = max(3, n_days/2)` capped at `n_days`: require at least 3 clean
/// days, ideally half the observed window.
fn quarantine_clean_days(per_day: &[u64], n_days: usize) -> Option<Vec<u64>> {
    let nonzero: Vec<u64> = per_day.iter().copied().filter(|&b| b > 0).collect();
    if nonzero.is_empty() {
        return None;
    }
    let mut nz_sorted = nonzero.clone();
    nz_sorted.sort_unstable();
    let prov_median = median_sorted(&nz_sorted);
    let (lo, hi) = (prov_median / 3.0, prov_median * 3.0);
    let clean: Vec<u64> = nonzero
        .into_iter()
        .filter(|&b| (b as f64) >= lo && (b as f64) <= hi)
        .collect();
    let min_clean = 3usize.max(n_days / 2).min(n_days);
    if clean.len() < min_clean {
        None
    } else {
        Some(clean)
    }
}

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
    /// Calibration score: PURE median deviation from target — `|median/target-1|`.
    ///
    /// FIX 2b (2026-06-09): removed the `0.3*MAD/target` term that previously
    /// contaminated the objective. Larger bricks mechanically compress per-day
    /// dispersion, so a MAD penalty inside the MINIMIZED score biased k upward
    /// (smaller bpd) — pulling the full-history median above target. The pure
    /// median objective now equals the operator's full-history measurement.
    /// MAD is retained as a SEPARATE side-guard via [`dispersion_ok`], not here.
    ///
    /// Lower = better. Returns `f64::INFINITY` when `days == 0` (cal-fail).
    pub fn score(&self, target_bpd: f64) -> f64 {
        if self.days == 0 || target_bpd <= 0.0 {
            return f64::INFINITY;
        }
        (self.median / target_bpd - 1.0).abs()
    }

    /// Dispersion side-guard (FIX 2b, 2026-06-09): `true` when the window's
    /// per-day brick spread is sane (`MAD/median ≤ RENKO_DISPERSION_MAX`). Used
    /// as a window REJECT gate, deliberately OUTSIDE the minimized [`score`] so
    /// it cannot bias k. A `days==0` (dead) or non-positive-median window is not
    /// OK. MAD computation itself is unchanged in `count_bars_per_day_from_prices`.
    pub fn dispersion_ok(&self) -> bool {
        if self.days == 0 || self.median <= 0.0 {
            return false;
        }
        self.mad / self.median <= RENKO_DISPERSION_MAX
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
/// Successor to [`count_bars_from_prices`]. The scalar `count / days =
/// mean_bpd` was vulnerable to regime-spike days (one 5× day pushes mean
/// above target → calibrator shrinks brick → next quiet day under-emits).
/// Median+MAD focuses the optimizer on what the operator actually wants:
/// typical-day brick density.
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

    // PERF A1 (2026-06-09): `prices` is ts-ASCENDING. The eval-search replays
    // a 7–40d trailing slice ~60×/ticker but previously iterated the WHOLE
    // (up-to-247M) Vec, skipping ~99 % via `ts < from_ts`. Binary-search the
    // slice bounds ONCE so we iterate only the in-range window. Inclusivity is
    // pinned to the ORIGINAL guards (`ts >= from_ts`, `ts <= to_ts`): `lo`
    // drops everything strictly before `from_ts`; `hi` keeps everything `≤
    // to_ts`. `day_idx` is still derived from the unchanged `from_ts`, so brick
    // bucketing (and thus the fitted k) is byte-identical to the linear scan.
    let lo = prices.partition_point(|p| p.0 < from_ts);
    let hi = prices.partition_point(|p| p.0 <= to_ts);
    let window = &prices[lo..hi];

    for &(ts, mid) in window {
        let day_idx = ((ts - from_ts) / MS_PER_DAY).clamp(0, n_days as i64 - 1) as usize;
        let sigma = sigma_for_ts(ts, vol_source, sigma_cache);
        gen.feed_tick_with_sigma(ts, mid, sigma, &mut |_| {
            per_day[day_idx] = per_day[day_idx].saturating_add(1);
            Ok(())
        })
        .ok();
    }

    // Spike/gap quarantine (RCA ROOT2c, 2026-06-01). The raw eval slice was
    // contaminated by spike days (regime bursts) and gap days (zero-record /
    // .idx.stub.bak / 56B-truncated shards surface as 0-count days). Pure logic
    // is in `quarantine_clean_days` so it can be unit-tested.
    let clean_stats = quarantine_clean_days(&per_day, n_days);

    let Some(clean) = clean_stats else {
        // Dead window — insufficient clean eval days. days=0 → score()=INFINITY
        // → window dropped by the calibrator (no fit on contaminated data).
        return DailyBpdStats {
            bricks_per_day: per_day, median: 0.0, mean: 0.0, mad: 0.0, days: 0,
        };
    };

    let clean_total: u64 = clean.iter().sum();
    let mean = clean_total as f64 / clean.len() as f64;
    let mut sorted = clean.clone();
    sorted.sort_unstable();
    let median = median_sorted(&sorted);
    let mut devs: Vec<u64> = clean
        .iter()
        .map(|&b| (b as f64 - median).abs().round() as u64)
        .collect();
    devs.sort_unstable();
    let mad = median_sorted(&devs);

    // `days` reflects the count of CLEAN days actually scored, so score()
    // returns INFINITY when the clean set degenerated above.
    DailyBpdStats { bricks_per_day: per_day, median, mean, mad, days: clean.len() }
}

/// Walk-forward calibration with non-overlapping cal/eval windows
/// (audit point #5(i), 2026-05-26).
///
/// For each `window_days` in `cal.k_fit_windows_days`, split the trailing slice:
///   - `cal_slice  = [last - window_days,           last - eval_holdout_days]`
///   - `eval_slice = [last - eval_holdout_days,     last]`
///
/// PART B (2026-06-09 accuracy redesign — drives full-history median bpd error
/// ≤ 5 %): the walk-forward search is DEMOTED to an OVERFIT GUARD. Each fold
/// still binary-searches `mult` on its eval slice to produce a `k_w`, but the
/// folds are NO LONGER geo-mean blended into the answer and the per-window
/// full-history recompute/accept is removed from inside the loop. After the
/// loop, if the folds disagree (`max(k_w)/min(k_w) > K_AGREEMENT_MAX`) the
/// windows are fitting incompatible regimes → reject (return 0.0).
///
/// DECOUPLE FIX (2026-06-09): Stage A (walk-forward folds) is a GUARD, not a
/// PREREQUISITE. The full-history bisection (PART B2) is the PRIMARY selector
/// and runs UNCONDITIONALLY — even when EVERY walk-forward fold was
/// quarantine-dropped (e.g. a re-backfilled handover window, thin alts, crosses
/// whose short recent windows are all spike/gap). The agreement guard fires
/// ONLY when ≥2 folds survived AND disagree. "no window had enough data" no
/// longer fails the ticker on its own; failure now requires the full-history
/// bisection itself to be unable to bracket/accept the target.
///
/// The ANSWER comes from a SEPARATE log-k bisection on the FULL-history median
/// over `[first, last]` (the exact quantity the operator + data_quality_audit
/// measure). bpd(k) is monotone-decreasing but STAIR-STEPPED (snap_to_25_grid
/// lattice × integer-day median); the bisection brackets the median==target
/// crossing to a 0.5 %-wide k-window, then a two-sided rung probe picks the
/// bracketing rung (exp(lo) vs exp(hi)) with the smaller `|median/target-1|`.
/// The achieved error is that structural-floor minimum — logged per ticker so
/// the operator can spot tickers needing a per-pair forced-k override.
///
/// Final gate: accept `k_star` iff `achieved_err ≤ RENKO_BPD_ACCEPT_TOL` AND
/// dispersion_ok AND `k_star ≥ K_FLOOR` AND no clamp; else 0.0 (caller drops).
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
    let mut mults: Vec<f32> = Vec::new();
    for &window_days in &cal.k_fit_windows_days {
        let cal_from = (last - (window_days as i64) * MS_PER_DAY).max(first);

        // PER-WINDOW eval slice (RCA ROOT2c, 2026-06-01). Previously `eval_from`
        // was hoisted out of this loop, so all 3 MTF windows scored the SAME
        // trailing 7d slice — `k_fit_windows_days` was a no-op (every window
        // returned the same k) AND that one slice was the contaminated one.
        // Scale the holdout with the window (≥ eval_holdout_days, ≤ a third of
        // the window) so a 7d / 14d / 30d window each scores a DIFFERENT eval
        // slice and yields a genuinely different k.
        let window_span_days = (last - cal_from) as f64 / MS_PER_DAY as f64;
        let eval_days_w = (eval_holdout_days as f64)
            .max(window_span_days / 3.0)
            .min(window_span_days)
            .floor() as i64;
        if eval_days_w <= 0 {
            continue;
        }
        let eval_from = last - eval_days_w * MS_PER_DAY;
        let cal_days = (eval_from - cal_from) as f64 / MS_PER_DAY as f64;
        if cal_days < cal.min_window_days as f64 || eval_from <= first {
            continue;
        }
        let (mut log_lo, mut log_hi) = (cal.mult_bounds[0].ln(), cal.mult_bounds[1].ln());
        let mut best = (base.multiplier, f64::INFINITY);
        // Track the chosen mult's clean-eval-day count for the degenerate-window
        // guard below (PART B no longer needs the eval median — selection is the
        // post-loop full-history bisection).
        let mut best_eval_days = 0usize;
        // Two best log-mults seen (for the post-search midpoint refine). bpd is
        // monotone-decreasing in k, so the median==target crossing lives between
        // the two lowest-scoring trials; their geometric midpoint is the best
        // single extra probe to tighten convergence.
        let mut best_log_mults: [f64; 2] = [f64::NAN, f64::NAN];

        // Re-usable trial evaluator (search loop + the midpoint refine below).
        let eval_mult = |mult: f32| -> DailyBpdStats {
            let trial = RenkoConfig { multiplier: mult, min_pct: base.min_pct };
            // Score on this window's EVAL slice (walk-forward). The slice is
            // spike/gap-quarantined inside count_bars_per_day_from_prices.
            count_bars_per_day_from_prices(
                prices, &trial, vol_source, vol_config, sigma_cache,
                eval_from, last,
            )
        };

        for _round in 0..cal.max_rounds {
            let log_mid = (log_lo + log_hi) / 2.0;
            let mult = log_mid.exp() as f32;

            let eval_stats = eval_mult(mult);
            let score = eval_stats.score(target_bpd);
            if score < best.1 {
                best = (mult, score);
                best_eval_days = eval_stats.days;
                best_log_mults[1] = best_log_mults[0];
                best_log_mults[0] = log_mid;
            }
            if score < cal.tolerance {
                break;
            }
            // Direction: use eval median to choose half. bpd is monotone-
            // decreasing in k, so median > target ⇒ k too small ⇒ search the
            // higher half; median < target ⇒ search the lower half. This always
            // steps TOWARD median==target, the same point score() rewards.
            if eval_stats.median > target_bpd {
                log_lo = log_mid;
            } else {
                log_hi = log_mid;
            }
        }

        // CONVERGENCE TIGHTENING (2026-06-07): the binary search halts on the
        // score-tolerance OR round budget, which can stop one step shy of the
        // true median==target crossing. Probe the geometric midpoint of the two
        // best-scoring k's — that bracket straddles the crossing — and adopt it
        // ONLY if it does not worsen the score. Still useful as part of the
        // overfit-guard k_w estimate (PART B no longer blends k_w into the
        // answer, but a tighter k_w sharpens the agreement check).
        if best_log_mults[0].is_finite() && best_log_mults[1].is_finite() {
            let mid_log = (best_log_mults[0] + best_log_mults[1]) / 2.0;
            let mid_mult = mid_log.exp() as f32;
            let mid_stats = eval_mult(mid_mult);
            let mid_score = mid_stats.score(target_bpd);
            if mid_score <= best.1 && mid_stats.days > 0 {
                best = (mid_mult, mid_score);
                best_eval_days = mid_stats.days;
            }
        }

        // If every round degenerated (clean-eval days == 0 each time), best.1
        // stays INFINITY → drop the window (the quarantine killed the slice).
        if best.1 == f64::INFINITY || best_eval_days == 0 {
            warn!(
                window_days,
                "wf window had no scorable clean-eval round — dropped (spike/gap quarantine)"
            );
            continue;
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
        // EMERGENCY 2026-06-01 T0.1: sub-K_FLOOR reject (wf branch). See
        // calibrate_mtf_with_target above for rationale.
        if (best.0 as f64) < K_FLOOR {
            warn!(
                window_days,
                mult = best.0,
                k_floor = K_FLOOR,
                "wf sub-K_FLOOR reject — window dropped (runtime would silently clamp)"
            );
            continue;
        }
        // PART B1 (2026-06-09): this fold's k_w is now ONLY an overfit-guard
        // datum. The per-window full-history recompute + dispersion + accept gate
        // that used to live here is REMOVED — the answer comes from the post-loop
        // full-history log-k bisection (PART B2), which measures the same
        // full-history median once, at the bracketing rungs, instead of once per
        // fold on each fold's overfit k.
        mults.push(best.0);
    }

    // ── PART B1: fold-agreement overfit guard (CONDITIONAL) ──────────────────
    // DECOUPLE FIX (2026-06-09): Stage A (the per-window walk-forward folds) is a
    // robustness GUARD, not a hard PREREQUISITE. It must NOT by itself fail the
    // ticker. The actual k selector is the full-history log-k bisection (PART B2)
    // below; it runs UNCONDITIONALLY now. The agreement check only fires when
    // ≥2 folds actually survived quarantine and DISAGREE — a genuine
    // overfit/regime split. 0 or 1 surviving fold ⇒ skip the agreement check
    // (reduced overfit protection, but Stage B + dispersion_ok still gate the
    // result) rather than rejecting on "no window had enough data".
    if mults.is_empty() {
        warn!(
            "wf all walk-forward folds quarantine-dropped (spike/gap/short) — \
             agreement guard SKIPPED (reduced overfit protection); relying on \
             full-history bisection (PART B2) + dispersion guard"
        );
    } else if mults.len() == 1 {
        info!(
            k_w = mults[0],
            "wf only one walk-forward fold survived quarantine — agreement guard \
             SKIPPED (need ≥2 folds); relying on full-history bisection + dispersion"
        );
    } else {
        let k_min = mults.iter().cloned().fold(f32::INFINITY, f32::min) as f64;
        let k_max = mults.iter().cloned().fold(f32::NEG_INFINITY, f32::max) as f64;
        if k_min > 0.0 && k_max / k_min > K_AGREEMENT_MAX {
            warn!(
                k_min,
                k_max,
                ratio = k_max / k_min,
                agreement_max = K_AGREEMENT_MAX,
                "wf fold-agreement guard — folds disagree (max/min > bound), rejecting (overfit/regime split)"
            );
            return 0.0;
        }
    }

    // ── PART B2: log-k bisection on the FULL-history median ───────────────────
    // bpd(k) is monotone-decreasing in k over [first, last]; g(k) = median(k) -
    // target therefore crosses zero exactly once. Bisect in log-k (k spans ~2
    // decades, so log-space halving is uniform in relative k). The median is a
    // STAIRCASE (snap_to_25_grid × integer-day median), so we stop on BRACKET
    // WIDTH, not score, then pick the better of the two bracketing rungs.
    let median_bpd = |k: f64| -> DailyBpdStats {
        count_bars_per_day_from_prices(
            prices,
            &RenkoConfig { multiplier: k as f32, min_pct: base.min_pct },
            vol_source, vol_config, sigma_cache, first, last,
        )
    };

    let mut lo = cal.mult_bounds[0].ln();
    let mut hi = cal.mult_bounds[1].ln();
    let g_lo = median_bpd(lo.exp());
    let g_hi = median_bpd(hi.exp());
    // Bracket validity: smaller k (lo bound) must OVERSHOOT target (g(lo)>0) and
    // larger k (hi bound) must UNDERSHOOT (g(hi)<0). If either degenerate or the
    // crossing isn't bracketed, the target is structurally unreachable in
    // [mult_bounds] → Failed (caller drops the entry).
    if g_lo.days == 0 || g_hi.days == 0
        || !(g_lo.median - target_bpd > 0.0)
        || !(g_hi.median - target_bpd < 0.0)
    {
        warn!(
            lo_median = g_lo.median,
            hi_median = g_hi.median,
            target_bpd,
            "wf bisection bracket invalid (target not bracketed by mult_bounds) — returning 0.0"
        );
        return 0.0;
    }

    for _ in 0..cal.max_rounds {
        if (hi - lo) < K_BRACKET_LN_EPS {
            break;
        }
        let mid = (lo + hi) / 2.0;
        let m = median_bpd(mid.exp());
        // median > target ⇒ k too small ⇒ move lo up (toward larger k);
        // median ≤ target ⇒ k too large ⇒ move hi down (toward smaller k).
        if m.median > target_bpd {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    // ── PART B2 two-sided rung probe ─────────────────────────────────────────
    // lo is the LAST k whose median was > target (the small-k / overshoot rung);
    // hi is the LAST k whose median was ≤ target (the large-k / undershoot rung).
    // Re-measure both and keep whichever lands closer to target — the staircase
    // makes the true achievable optimum one of these two bracketing rungs.
    let small_k = lo.exp(); // ≥ target side
    let large_k = hi.exp(); // ≤ target side
    let s_small = median_bpd(small_k);
    let s_large = median_bpd(large_k);
    let err_small = if s_small.days == 0 { f64::INFINITY } else { (s_small.median / target_bpd - 1.0).abs() };
    let err_large = if s_large.days == 0 { f64::INFINITY } else { (s_large.median / target_bpd - 1.0).abs() };
    let (k_star, achieved_err, full_stats) = if err_small <= err_large {
        (small_k, err_small, s_small)
    } else {
        (large_k, err_large, s_large)
    };

    info!(
        k_star,
        achieved_err_pct = achieved_err * 100.0,
        full_median = full_stats.median,
        target_bpd,
        accept_tol_pct = RENKO_BPD_ACCEPT_TOL * 100.0,
        "wf full-history rung selection (structural floor = achieved_err)"
    );

    // ── PART B3: final gate ──────────────────────────────────────────────────
    if full_stats.days == 0 {
        warn!(k_star, "wf selected rung had no scorable clean day — returning 0.0");
        return 0.0;
    }
    if achieved_err > RENKO_BPD_ACCEPT_TOL {
        warn!(
            k_star,
            achieved_err_pct = achieved_err * 100.0,
            accept_tol_pct = RENKO_BPD_ACCEPT_TOL * 100.0,
            full_median = full_stats.median,
            target_bpd,
            "wf accept gate — structural floor exceeds tol (needs per-pair renko_k override), returning 0.0"
        );
        return 0.0;
    }
    if !full_stats.dispersion_ok() {
        warn!(
            k_star,
            full_median = full_stats.median,
            full_mad = full_stats.mad,
            dispersion_max = RENKO_DISPERSION_MAX,
            "wf dispersion side-guard — pure-noise full history, returning 0.0"
        );
        return 0.0;
    }
    if k_star < K_FLOOR {
        warn!(k_star, k_floor = K_FLOOR, "wf k_star < K_FLOOR — returning 0.0 (caller drops entry)");
        return 0.0;
    }
    // Clamp-detector on the FINAL k_star: a selection parked at either bound is
    // a degenerate σ / unreachable-target artifact, not a fit.
    let lo_clamp = (k_star - cal.mult_bounds[0]).abs() / cal.mult_bounds[0] < 0.01;
    let hi_clamp = (k_star - cal.mult_bounds[1]).abs() / cal.mult_bounds[1] < 0.01;
    if lo_clamp || hi_clamp {
        warn!(
            k_star,
            bound = if lo_clamp { "lower" } else { "upper" },
            "wf final clamp-detector — k_star parked at mult bound, returning 0.0"
        );
        return 0.0;
    }

    k_star as f32
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
        let from = (last - window_days as i64 * MS_PER_DAY as i64).max(first);
        let days = (last - from) as f64 / MS_PER_DAY as f64;
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
        // bpd-target acceptance gate (EMERGENCY 2026-06-01 T0.1; re-anchored to
        // FULL-history median by FIX 1, tolerance tightened to 0.08 by FIX 2a).
        // FIX 1 (2026-06-09): `best.1` is the relative bpd error on the trailing
        // WINDOW slice — not what the operator measures (full-history median).
        // Recompute the chosen k's median over [first, last] and gate on THAT,
        // so the fit objective == the operator's measurement (eliminates the
        // ~1.6-2× median overshoot). Reject windows beyond RENKO_BPD_ACCEPT_TOL.
        let full_stats = count_bars_per_day_from_prices(
            prices,
            &RenkoConfig { multiplier: best.0, min_pct: base.min_pct },
            vol_source, vol_config, sigma_cache, first, last,
        );
        let rel_bpd_err = if full_stats.days == 0 {
            f64::INFINITY
        } else {
            (full_stats.median / target_bpd - 1.0).abs()
        };
        if rel_bpd_err > RENKO_BPD_ACCEPT_TOL {
            warn!(
                window_days,
                mult = best.0,
                full_median = full_stats.median,
                window_err_pct = best.1 * 100.0,
                target_bpd,
                rel_bpd_err_pct = rel_bpd_err * 100.0,
                accept_tol_pct = RENKO_BPD_ACCEPT_TOL * 100.0,
                "bpd-target accept gate — dropping window (full-history |median-target|/target > tol)"
            );
            continue;
        }
        // EMERGENCY T0.1: sub-K_FLOOR reject. If the per-window best mult lands
        // below K_FLOOR (0.05), the runtime would silently clamp to K_FLOOR at
        // 3 sites (renko.rs:188, bars_renko.rs:223, synth_backfill:402). Drop the
        // window so geo_mean reflects only valid k values; if ALL windows fall
        // below K_FLOOR, geo_mean returns 0.0 → calibrate fail → P0.4 logic drops
        // the entry from ticker-params.json (no stale carry).
        if (best.0 as f64) < K_FLOOR {
            warn!(
                window_days,
                mult = best.0,
                k_floor = K_FLOOR,
                "sub-K_FLOOR reject — dropping window (runtime would silently clamp; violates feedback_no_k_fallback)"
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
    // EMERGENCY 2026-06-01 T0.1: final safety net — if the geo_mean lands
    // below K_FLOOR despite per-window gating, signal calibrate-fail rather
    // than ship a clamped k. Per P0.4 the caller drops the entry on 0.0.
    if (geo_mean as f64) < K_FLOOR {
        warn!(geo_mean, k_floor = K_FLOOR, target_bpd,
            "MTF geo_mean < K_FLOOR — returning 0.0 (caller drops entry per P0.4)");
        return 0.0;
    }
    geo_mean
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantine_drops_gaps_and_spikes() {
        // 10 days: 8 "normal" ~100/day, one gap (0), one spike (1000).
        // n_days=10 → min_clean = max(3, 5) = 5. 8 normals survive.
        let per_day = vec![100, 98, 102, 0, 101, 99, 1000, 100, 103, 97];
        let clean = quarantine_clean_days(&per_day, per_day.len())
            .expect("8 clean days >= min_clean(5)");
        assert_eq!(clean.len(), 8, "gap(0) and spike(1000) excluded");
        assert!(clean.iter().all(|&b| b > 0 && b < 1000));
    }

    #[test]
    fn quarantine_dead_window_when_too_gappy() {
        // 10 days, only 2 non-zero → below min_clean(5) → None (dead window).
        let per_day = vec![0, 0, 0, 100, 0, 0, 102, 0, 0, 0];
        assert!(
            quarantine_clean_days(&per_day, per_day.len()).is_none(),
            "2 clean days < min_clean(5) → dead window"
        );
    }

    #[test]
    fn quarantine_all_zero_is_dead() {
        assert!(quarantine_clean_days(&vec![0u64; 7], 7).is_none());
    }

    #[test]
    fn dead_window_scores_infinity() {
        // A dead (days=0) DailyBpdStats must score INFINITY so the bpd-accept
        // gate / search never selects it.
        let dead = DailyBpdStats {
            bricks_per_day: vec![], median: 0.0, mean: 0.0, mad: 0.0, days: 0,
        };
        assert_eq!(dead.score(300.0), f64::INFINITY);
    }

    /// Constant-σ VolSource for the regression test below. The brick size is
    /// then `max(k * σ, min_pct) * price`, so k alone drives bpd on a fixed path.
    struct ConstSigma(f64);
    impl VolSource for ConstSigma {
        fn len(&self) -> usize { 1 }
        fn sigma_pct(&self, _i: usize) -> f64 { self.0 }
        fn find_index_for_mts(&self, _mts: u64) -> usize { 0 }
    }

    /// Deterministic GBM-ish tick path: `n_days` of `ticks_per_day` points at
    /// even ~`MS_PER_DAY/ticks_per_day` cadence, driven by a tiny LCG so the
    /// test is reproducible without an RNG dependency. Returns `(ts, mid)`.
    fn synth_gbm_path(n_days: usize, ticks_per_day: usize, sigma_step: f64, seed: u64) -> Vec<(i64, f64)> {
        let dt_ms = (MS_PER_DAY / ticks_per_day as i64).max(1);
        let mut state = seed;
        let mut next_u = || {
            // xorshift64* — uniform in (0,1).
            state ^= state >> 12; state ^= state << 25; state ^= state >> 27;
            ((state.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64) / ((1u64 << 53) as f64)
        };
        let total = n_days * ticks_per_day;
        let mut out = Vec::with_capacity(total);
        let mut price = 100.0f64;
        let t0: i64 = 1_700_000_000_000; // arbitrary epoch-ms anchor
        for i in 0..total {
            // Symmetric multiplicative step → full-tick path with intra-"minute"
            // wiggle that a 1-min last-mid downsample would discard.
            let z = next_u() - 0.5;
            price *= (sigma_step * z).exp();
            out.push((t0 + i as i64 * dt_ms, price));
        }
        out
    }

    /// Collapse a full-tick path to 1-min last-mid buckets (the OLD calibrator
    /// granularity) so the test can prove the granularity gap.
    fn downsample_1min_last(prices: &[(i64, f64)]) -> Vec<(i64, f64)> {
        use std::collections::BTreeMap;
        let mut m: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
        for &(ts, mid) in prices {
            let bucket = (ts / 60_000) * 60_000;
            let e = m.entry(bucket).or_insert((ts, mid));
            if ts >= e.0 { *e = (ts, mid); }
        }
        m.into_iter().map(|(_, (ts, mid))| (ts, mid)).collect()
    }

    /// REGRESSION GUARD (2026-06-06 brick-storm RCA): the k chosen by
    /// `calibrate_mtf_walkforward` on a FULL-TICK path, when applied via the SAME
    /// `RenkoGenerator` over the SAME full-tick path, must yield bpd within
    /// `RENKO_BPD_ACCEPT_TOL` (0.08 post FIX 2a) of target. This pins
    /// calibrate-input == apply-input AND, with FIX 1, asserts that the accept
    /// objective is the FULL-history median (the gate now recomputes over
    /// [first,last]). A 1-min downsample (the old bug) undercounts crossings on
    /// the identical k, which is asserted as the negative control.
    #[test]
    fn full_tick_calibrate_matches_full_tick_apply() {
        let target_bpd = 300.0;
        // ~40d, 4000 ticks/day (~21s cadence) — dense enough that intra-minute
        // extremes exist, small enough to keep the test < 1s. sigma_step tuned so
        // target_bpd=300 lands at k≈0.5 (comfortably > K_FLOOR, < mult_bounds hi).
        let prices = synth_gbm_path(40, 4000, 0.006, 0xC0FFEE);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;

        let vol = ConstSigma(0.01); // 1% σ_pct, flat
        let sigma_cache = vec![0.01];
        let vol_cfg = VolConfig {
            ema_period: 1,
            sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 1,
            recompute_cooldown_ms: 0,
            // Spike-responsive σ OFF for the granularity/convergence regression
            // tests (they isolate calibrate-input == apply-input parity, which
            // is orthogonal to the spike floor). Default OFF anyway.
            ..VolConfig::default()
        };
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![14, 30],
            min_window_days: 7,
            max_rounds: 40,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],   // PART B: production K_FLOOR..MULT_UPPER_BOUND (bracket must hold)
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(k > 0.0, "calibration must produce a valid k (got {k})");

        // Apply the chosen k over the SAME full-tick path (identical generator).
        let applied = RenkoConfig { multiplier: k, min_pct: 0.0 };
        let stats = count_bars_per_day_from_prices(
            &prices, &applied, &vol, &vol_cfg, &sigma_cache, first, last,
        );
        // FIX 1: the calibrator now gates the chosen k on the FULL-history median
        // over [first,last]; this assertion measures exactly that, so it must
        // land within the (tightened) module accept tol.
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "full-tick apply bpd (median {:.1}) within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median, RENKO_BPD_ACCEPT_TOL * 100.0, target_bpd, rel_err
        );

        // NEGATIVE CONTROL: the SAME k on a 1-min last-mid downsample of the
        // SAME path under-emits — proving the old (downsampled) calibrate input
        // would have mis-measured bpd and selected a too-small k. Path crossings
        // along full ticks >> crossings along 1-min last-mid.
        let coarse = downsample_1min_last(&prices);
        let coarse_stats = count_bars_per_day_from_prices(
            &coarse, &applied, &vol, &vol_cfg, &sigma_cache, first, last,
        );
        assert!(
            coarse_stats.median < stats.median,
            "1-min downsample must undercount bricks vs full-tick for the same k \
             (coarse median {:.1} < full {:.1})",
            coarse_stats.median, stats.median
        );
    }

    /// CONVERGENCE TIGHTENING GUARD (2026-06-07): the post-search midpoint
    /// refine must never push the selected k's bpd FURTHER from target than the
    /// binary search alone would have. We assert the walk-forward result lands
    /// within the bpd-accept tol — i.e. the midpoint probe only ever helps or is
    /// a no-op (it is adopted only when its score ≤ the search best). Same synth
    /// path as the full-tick regression so behaviour is pinned end-to-end.
    #[test]
    fn midpoint_refine_does_not_overshoot_target() {
        let target_bpd = 300.0;
        let prices = synth_gbm_path(40, 4000, 0.006, 0xC0FFEE);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;

        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vol_cfg = VolConfig {
            ema_period: 1,
            sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 1,
            recompute_cooldown_ms: 0,
            // Spike-responsive σ OFF for the granularity/convergence regression
            // tests (they isolate calibrate-input == apply-input parity, which
            // is orthogonal to the spike floor). Default OFF anyway.
            ..VolConfig::default()
        };
        // Tight round budget so the raw search is more likely to stop short of
        // the crossing — this is exactly when the midpoint refine should engage.
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![14, 30],
            min_window_days: 7,
            max_rounds: 8,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],   // PART B: production K_FLOOR..MULT_UPPER_BOUND (bracket must hold)
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(k > 0.0, "calibration must produce a valid k (got {k})");

        let applied = RenkoConfig { multiplier: k, min_pct: 0.0 };
        let stats = count_bars_per_day_from_prices(
            &prices, &applied, &vol, &vol_cfg, &sigma_cache, first, last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "refined k median {:.1} within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median, RENKO_BPD_ACCEPT_TOL * 100.0, target_bpd, rel_err
        );
    }

    #[test]
    fn bpd_accept_gate_threshold_math() {
        // The gate drops a window when |median/target - 1| > RENKO_BPD_ACCEPT_TOL
        // (tightened 0.20 → 0.08 by FIX 2a).
        let target = 300.0;
        // 33 bpd vs 300 target → rel-err ≈ 0.89 → dropped (the SOL/USDT k≈4 case).
        let rel_err_bad = (33.0_f64 / target - 1.0).abs();
        assert!(rel_err_bad > RENKO_BPD_ACCEPT_TOL);
        // 290 bpd vs 300 → rel-err ≈ 0.033 → accepted under the tight 0.08 gate.
        let rel_err_ok = (290.0_f64 / target - 1.0).abs();
        assert!(rel_err_ok <= RENKO_BPD_ACCEPT_TOL);
        // 330 bpd vs 300 → rel-err = 0.10 → now DROPPED (would have passed @0.20).
        let rel_err_was_ok_now_bad = (330.0_f64 / target - 1.0).abs();
        assert!(rel_err_was_ok_now_bad > RENKO_BPD_ACCEPT_TOL);
    }

    #[test]
    fn score_is_pure_median_no_mad_term() {
        // FIX 2b: score must be exactly |median/target - 1|, independent of MAD.
        let target = 300.0;
        let lo_disp = DailyBpdStats {
            bricks_per_day: vec![], median: 300.0, mean: 300.0, mad: 5.0, days: 10,
        };
        let hi_disp = DailyBpdStats {
            bricks_per_day: vec![], median: 300.0, mean: 300.0, mad: 120.0, days: 10,
        };
        // Same median ⇒ identical score regardless of MAD (old objective would
        // have penalised hi_disp by 0.3*120/300 = 0.12).
        assert_eq!(lo_disp.score(target), hi_disp.score(target));
        assert!((lo_disp.score(target) - 0.0).abs() < 1e-12);
        let off = DailyBpdStats {
            bricks_per_day: vec![], median: 330.0, mean: 330.0, mad: 0.0, days: 10,
        };
        assert!((off.score(target) - 0.10).abs() < 1e-12);
    }

    #[test]
    fn dispersion_guard_rejects_pure_noise() {
        // FIX 2b side-guard: MAD/median > RENKO_DISPERSION_MAX ⇒ reject.
        let ok = DailyBpdStats {
            bricks_per_day: vec![], median: 300.0, mean: 300.0, mad: 100.0, days: 10,
        };
        assert!(ok.dispersion_ok(), "MAD/median=0.33 ≤ bound ⇒ ok");
        let noisy = DailyBpdStats {
            bricks_per_day: vec![], median: 100.0, mean: 100.0, mad: 150.0, days: 10,
        };
        assert!(!noisy.dispersion_ok(), "MAD/median=1.5 > bound ⇒ reject");
        let dead = DailyBpdStats {
            bricks_per_day: vec![], median: 0.0, mean: 0.0, mad: 0.0, days: 0,
        };
        assert!(!dead.dispersion_ok(), "dead window not ok");
    }

    /// PART B2: the full-history log-k bisection + two-sided rung probe must
    /// return the k whose full-history median is the CLOSER of the two
    /// bracketing rungs. We reproduce the bracket independently and assert the
    /// returned k lands on the better rung (its err ≤ the other rung's err).
    #[test]
    fn rootfind_lands_on_optimal_rung() {
        let target_bpd = 300.0;
        let prices = synth_gbm_path(40, 4000, 0.006, 0xC0FFEE);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;

        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vol_cfg = VolConfig {
            ema_period: 1,
            sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 1,
            recompute_cooldown_ms: 0,
            ..VolConfig::default()
        };
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![14, 30],
            min_window_days: 7,
            max_rounds: 20,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(k > 0.0, "must produce a valid k (got {k})");

        // The selected k's full-history median err IS the achieved structural
        // floor; it must be ≤ RENKO_BPD_ACCEPT_TOL (gate) AND ≤ the err at a
        // k one bracket-rung away on each side (it is the rung minimum).
        let median_err = |kk: f64| -> f64 {
            let s = count_bars_per_day_from_prices(
                &prices, &RenkoConfig { multiplier: kk as f32, min_pct: 0.0 },
                &vol, &vol_cfg, &sigma_cache, first, last,
            );
            if s.days == 0 { f64::INFINITY } else { (s.median / target_bpd - 1.0).abs() }
        };
        let err_star = median_err(k as f64);
        assert!(err_star <= RENKO_BPD_ACCEPT_TOL,
            "achieved_err {err_star} must be within accept tol");
        // Step a full bracket-width (0.5%) to each side: the chosen rung is the
        // local optimum, so neither neighbor beats it.
        let step = (K_BRACKET_LN_EPS).exp(); // ×1.005
        let err_up = median_err(k as f64 * step);
        let err_dn = median_err(k as f64 / step);
        assert!(err_star <= err_up && err_star <= err_dn,
            "k={k} sits on the optimal rung (err_star={err_star} up={err_up} dn={err_dn})");
    }

    /// DECOUPLE FIX (2026-06-09): a ticker whose SHORT trailing walk-forward
    /// windows are entirely quarantine-dropped (spike/gap contamination in the
    /// recent eval slices) but whose FULL history cleanly brackets the target
    /// must STILL receive a valid k from the full-history bisection (PART B2) —
    /// NOT 0.0. This pins the bug fix: Stage A (folds) is a guard, not a hard
    /// prerequisite. We build a 40d path that is calm+clean for the first ~32d,
    /// then make the trailing ~8d a spike/gap minefield so every short eval slice
    /// is dropped by `quarantine_clean_days`, leaving `mults` empty — yet the
    /// full-history median (dominated by the 32 clean days) still brackets target.
    #[test]
    fn quarantined_short_windows_still_get_full_history_k() {
        let target_bpd = 300.0;
        // 40 calm clean days at the SAME path/σ the full-tick regression uses
        // (so the full history brackets target near k≈0.5).
        let mut prices = synth_gbm_path(40, 4000, 0.006, 0xC0FFEE);
        let dt = prices[1].0 - prices[0].0;
        let ticks_per_day = (MS_PER_DAY / dt) as usize;
        let last_ts = prices.last().unwrap().0;

        // Contaminate ONLY the trailing ~8 days so the short walk-forward eval
        // slices (trailing 7–10d) are quarantine-killed while the 40d full
        // history keeps ≥32 clean days. For each of the last 8 day-buckets:
        //  - even day → GAP: strip its ticks (0-count day)
        //  - odd  day → SPIKE: inject a few ticks that emit a huge brick burst
        //    (a ~30 % jump per tick at k≈0.5 forces many crossings → >3× median).
        // Quarantine's [median/3, median*3] band then rejects every trailing day.
        let day0 = last_ts - 8 * MS_PER_DAY;
        // Drop all ticks in the trailing 8 days, then re-synthesise per-day.
        prices.retain(|&(ts, _)| ts < day0);
        let base_price = prices.last().map(|p| p.1).unwrap_or(100.0);
        for d in 0..8i64 {
            let day_start = day0 + d * MS_PER_DAY;
            if d % 2 == 0 {
                // GAP day: emit nothing → 0-count day.
                continue;
            }
            // SPIKE day: a burst of violent zig-zag ticks → far-above-median
            // brick count for that single day. Few ticks, large moves.
            let mut p = base_price;
            for j in 0..(ticks_per_day.max(64)) as i64 {
                let dir = if j % 2 == 0 { 1.30 } else { 1.0 / 1.30 };
                p *= dir;
                prices.push((day_start + j * dt, p));
            }
        }
        prices.sort_by_key(|x| x.0);

        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vol_cfg = VolConfig {
            ema_period: 1,
            sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 1,
            recompute_cooldown_ms: 0,
            ..VolConfig::default()
        };
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![14, 30],
            min_window_days: 7,
            max_rounds: 20,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(
            k > 0.0,
            "trailing-contaminated short windows (all quarantine-dropped) must \
             still yield a full-history k, not 0.0 (got {k})"
        );
        assert!((k as f64) >= K_FLOOR, "k must be ≥ K_FLOOR (got {k})");

        // The chosen k's FULL-history median must land within accept tol — the
        // full-history bisection is the selector, the spike/gap trailing days are
        // quarantined out of the [first,last] median too.
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let stats = count_bars_per_day_from_prices(
            &prices, &RenkoConfig { multiplier: k, min_pct: 0.0 },
            &vol, &vol_cfg, &sigma_cache, first, last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "full-history median {:.1} within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median, RENKO_BPD_ACCEPT_TOL * 100.0, target_bpd, rel_err
        );
    }

    /// PART B1: when the per-fold overfit-guard k_w's disagree by more than
    /// `K_AGREEMENT_MAX` (1.5×) the function must REJECT (return 0.0). We craft a
    /// path whose recent week is far higher-vol than the older history, so the
    /// short fold fits a much smaller k than the long fold (path crossings drive
    /// k under flat σ), tripping the agreement guard.
    #[test]
    fn agreement_guard_rejects_divergent_folds() {
        // 35 calm days then 5 EXTREMELY turbulent days (15× step). Holdout
        // scales with window: the 14d fold scores a ~4.7d trailing eval slice
        // (fully inside the 5d spike → needs a LARGE k to hit target) while the
        // 30d fold scores a ~10d trailing slice (half spike, half calm → much
        // SMALLER k). The two k_w then differ by > 1.5× ⇒ agreement guard fires.
        let calm = synth_gbm_path(35, 4000, 0.0015, 0xABCDEF);
        let mut turbulent = synth_gbm_path(5, 4000, 0.0225, 0x123456);
        // Re-anchor the turbulent segment's timestamps to follow `calm`.
        let dt = calm[1].0 - calm[0].0;
        let t_after = calm.last().unwrap().0 + dt;
        for (i, p) in turbulent.iter_mut().enumerate() {
            p.0 = t_after + i as i64 * dt;
        }
        let mut prices = calm;
        prices.extend(turbulent);

        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vol_cfg = VolConfig {
            ema_period: 1,
            sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 1,
            recompute_cooldown_ms: 0,
            ..VolConfig::default()
        };
        let cal = CalibrationConfig {
            target_bpd: 300.0,
            k_fit_windows_days: vec![14, 30],
            min_window_days: 7,
            max_rounds: 20,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, 300.0, 7,
        );
        assert_eq!(k, 0.0,
            "divergent folds (recent turbulence vs calm history) must trip the \
             agreement guard and reject (got k={k})");
    }
}
