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

use nxr_sdk::renko::{RenkoConfig, RenkoGenerator, K_FLOOR, K_MAX_SAFETY, SIGMA_FALLBACK};
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

/// Structure guard — MODAL-MASS floor (FIX 2c, 2026-06-09, storm-robust redesign).
///
/// WHY THE OLD `MAD/median ≤ 1.0` SPREAD GUARD WAS WRONG (same bug-class as the
/// removed spike-clip): the operator principle is "storms are FINE — a
/// 2000-brick storm day among many ~250-brick calm days is expected; the MEDIAN
/// is what matters and is robust to storms." A SPREAD measure (MAD/median,
/// std/mean, p75/p25) is INFLATED by exactly that legitimate storm tail, so a
/// healthy storm-heavy crypto pair (BNB/SOL) could be rejected as "pure noise"
/// for variance the operator explicitly declared sound. (Empirically MAD itself
/// is robust enough that the old 1.0 bound rarely tripped — but it conflated
/// "many storm days" with "no signal", which is the wrong CONCEPT, and was
/// redundant with the real degenerate guards below.)
///
/// NEW MEASURE (storm-robust): instead of penalising tail SPREAD, REQUIRE a
/// concentration of MODAL MASS near the median. `structure_ok()` passes when at
/// least [`STRUCTURE_MODAL_MASS_MIN`] of clean days fall inside the band
/// `[median/STRUCTURE_BAND_FACTOR, median*STRUCTURE_BAND_FACTOR]`. A storm-heavy
/// pair has its calm-day bulk clustered tightly around the median (storm days
/// sit ABOVE the band but are a minority) ⇒ high modal mass ⇒ PASS. GENUINELY
/// structureless / pure-noise data has days scattered with no modal cluster ⇒
/// low modal mass ⇒ REJECT — the only thing this guard should still catch. This
/// keeps the storm principle (median-targeting already makes storms harmless)
/// while still rejecting actual garbage.
///
/// `BAND_FACTOR = 3` ⇒ a day counts as "near the median" within a 3× window
/// (the same 3× scale the legacy gap heuristic used). `MODAL_MASS_MIN = 0.5` ⇒
/// a majority of days must cluster — a storm tail can be up to (nearly) half the
/// days and the pair still passes, well beyond any realistic crypto storm
/// fraction.
const STRUCTURE_BAND_FACTOR: f64 = 3.0;
const STRUCTURE_MODAL_MASS_MIN: f64 = 0.5;

/// min_pct clamp-detector fraction ceiling (2026-06-09, expert caveat #1: "the
/// only path to a passing-but-wrong k"). `compute_brick_size` floors the brick
/// at `min_pct`: `brick = max(k·σ_pct, min_pct)·price`. When `k_star·σ_pct ≤
/// min_pct` the brick is INDEPENDENT of k — bpd(k) goes flat and the bisection's
/// monotonicity assumption silently breaks, so it can "accept" a k whose brick
/// is actually min_pct-clamped (unresponsive to the fitted k). If MORE than this
/// fraction of the calibration window is min_pct-clamped at `k_star`, the k is
/// meaningless → REJECT (caller drops → per-ticker `renko_k_overrides`).
const MIN_PCT_CLAMP_MAX_FRAC: f64 = 0.5;

/// Fraction of `[first, last]` price samples whose brick at `k_star` is
/// min_pct-clamped (`k_star · σ_pct(ts) ≤ min_pct`). Pure (no Renko replay) so
/// the accept-gate guard is unit-testable. Returns 0.0 when the window is empty
/// (no samples ⇒ nothing to clamp).
fn min_pct_clamped_fraction<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    k_star: f64,
    min_pct: f32,
    vol_source: &S,
    sigma_cache: &[f64],
    first: i64,
    last: i64,
) -> f64 {
    let lo = prices.partition_point(|p| p.0 < first);
    let hi = prices.partition_point(|p| p.0 <= last);
    let window = &prices[lo..hi];
    if window.is_empty() {
        return 0.0;
    }
    let clamped = window
        .iter()
        .filter(|&&(ts, _)| {
            let sigma_pct = sigma_for_ts(ts, vol_source, sigma_cache);
            k_star * sigma_pct <= min_pct as f64
        })
        .count();
    clamped as f64 / window.len() as f64
}

/// Gap quarantine for a per-day brick-count vector (RCA ROOT2c).
///
/// Drops ONLY genuinely-corrupt GAP days (zero counts: zero-record /
/// .idx.stub.bak / 56B-truncated shards surface as 0-count days) and returns
/// the surviving "clean" days. Returns `None` when too few clean days remain
/// (the slice is too gappy to trust) — the caller then marks the window dead
/// (`days=0`) so it is dropped rather than fitting noise.
///
/// FIX (2026-06-09, operator spec): the legacy `[prov_median/3, prov_median*3]`
/// SPIKE clip is REMOVED. The objective statistic is the MEDIAN bpd, which is
/// already robust to a handful of 2000-brick storm days among many ~100-brick
/// calm days — the median absorbs them. Clipping high-bpd storm days here
/// corrupted the objective in two ways:
///   1. It made the calibrator fit the CALM-day median (≈ target at a small k)
///      while the operator's true full-history (storms-included) median at that
///      k was far above target → the accept gate then rejected the ticker
///      (`full_median=535 ≫ target=300 @ k=0.252`, BTC/USDT + 82 bases + 5
///      crosses). A correct k (larger, storms-included median = target) exists
///      but the clipped objective never converged to it.
///   2. The clip band is a MOVING function of k (`prov_median` depends on k), so
///      the set of "clean" days changed non-monotonically with k → `bpd(k)` was
///      no longer monotone-decreasing → the log-k bisection stalled on a
///      non-monotone surface instead of bracketing `median == target`.
/// Removing the clip makes `bpd(k)` monotone-decreasing in k (bigger k → bigger
/// bricks → fewer bricks/day) over the whole [first,last] history, so the
/// bisection converges to the k where the FULL-history (storms-included) median
/// == target — exactly the operator's measurement. Genuine data corruption is
/// still caught: gap days drop here, and `structure_ok` / the min_pct
/// clamp-detector remain as separate side-guards.
///
/// `min_clean = max(3, n_days/2)` capped at `n_days`: require at least 3 clean
/// (non-gap) days, ideally half the observed window.
fn quarantine_clean_days(per_day: &[u64], n_days: usize) -> Option<Vec<u64>> {
    // Keep every NON-GAP day. Storm/high-bpd days are LEGITIMATE and the median
    // absorbs them — do NOT clip them out of the objective (operator spec).
    let clean: Vec<u64> = per_day.iter().copied().filter(|&b| b > 0).collect();
    if clean.is_empty() {
        return None;
    }
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
    /// MAD remains available but the active side-guard is now [`structure_ok`]
    /// (storm-robust modal-mass), not a MAD/median spread ratio.
    ///
    /// Lower = better. Returns `f64::INFINITY` when `days == 0` (cal-fail).
    pub fn score(&self, target_bpd: f64) -> f64 {
        if self.days == 0 || target_bpd <= 0.0 {
            return f64::INFINITY;
        }
        (self.median / target_bpd - 1.0).abs()
    }

    /// Fraction of clean (non-gap) days whose brick count falls inside the modal
    /// band `[median/STRUCTURE_BAND_FACTOR, median*STRUCTURE_BAND_FACTOR]`. This
    /// is the storm-robust replacement for the old MAD/median spread ratio: a
    /// minority storm tail sits ABOVE the band and lowers this fraction only
    /// slightly, whereas truly structureless data (no modal cluster) scatters
    /// outside the band and drives it down. Returns 0.0 for a dead/degenerate
    /// window (no median ⇒ no structure). Computed off `bricks_per_day` (gap
    /// days, count 0, are excluded — they are never "near" a positive median).
    pub fn modal_mass(&self) -> f64 {
        if self.days == 0 || self.median <= 0.0 {
            return 0.0;
        }
        let lo = self.median / STRUCTURE_BAND_FACTOR;
        let hi = self.median * STRUCTURE_BAND_FACTOR;
        let clean: Vec<u64> =
            self.bricks_per_day.iter().copied().filter(|&b| b > 0).collect();
        if clean.is_empty() {
            return 0.0;
        }
        let in_band = clean
            .iter()
            .filter(|&&b| (b as f64) >= lo && (b as f64) <= hi)
            .count();
        in_band as f64 / clean.len() as f64
    }

    /// Structure side-guard (FIX 2c, 2026-06-09 — storm-robust): `true` when a
    /// MAJORITY of clean days cluster near the median ([`modal_mass`] ≥
    /// [`STRUCTURE_MODAL_MASS_MIN`]). Replaces the old `MAD/median ≤ 1.0` spread
    /// gate, which penalised the legitimate storm tail the operator declared
    /// sound (same bug-class as the removed spike-clip). Used as a window REJECT
    /// gate, deliberately OUTSIDE the minimized [`score`] so it cannot bias k. A
    /// `days==0` (dead) or non-positive-median window is not OK.
    pub fn structure_ok(&self) -> bool {
        if self.days == 0 || self.median <= 0.0 {
            return false;
        }
        self.modal_mass() >= STRUCTURE_MODAL_MASS_MIN
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

    // Gap quarantine (RCA ROOT2c, 2026-06-01; spike-clip REMOVED 2026-06-09).
    // Drops ONLY zero-count GAP days (zero-record / .idx.stub.bak / 56B-truncated
    // shards). High-bpd STORM days are kept — the median absorbs them and the
    // operator's full-history median target requires they stay in (see
    // `quarantine_clean_days` doc). Pure logic lives there so it is unit-testable.
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
/// Final gate: `k_star` is ALWAYS returned as the closest-achievable k (k>0).
/// `RENKO_BPD_ACCEPT_TOL` is ADVISORY — exceeding it only WARNs (snap-grid
/// structural floor), it does NOT drop the ticker (operator 2026-06-09: "k must
/// be the PRODUCT of calibration, never give up to a manual override"). Only the
/// genuinely-degenerate guards return 0.0: structure !ok (pure-noise full
/// history), median at K_FLOOR ≤ target (too flat to reach target at any k),
/// degenerate-σ / no crossing below K_MAX_SAFETY, min_pct-clamp fraction > floor.
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
    // Median σ_pct over the full sigma cache — a discriminating diagnostic shared
    // by EVERY degenerate-reject guard below (too-flat, K_MAX-degenerate-σ,
    // min_pct-clamp): the reject line then shows WHETHER the σ basis itself
    // collapsed vs. the k landing in a clamp band. Computed once.
    let median_sigma_pct = {
        let mut s: Vec<f64> = sigma_cache.iter().copied().filter(|x| x.is_finite()).collect();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if s.is_empty() { f64::NAN } else { s[s.len() / 2] }
    };
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

        // LOWER clamp detector (UPPER rejection removed 2026-06-09: "K should
        // not have a max"). A fold parked at the K_FLOOR edge IS a degenerate-σ
        // artifact and still drops the window.
        let lo_clamp = (best.0 as f64 - cal.mult_bounds[0]).abs() / cal.mult_bounds[0] < 0.01;
        if lo_clamp {
            warn!(
                window_days,
                mult = best.0,
                bound = "lower",
                "wf clamp-detector — window dropped (parked at K_FLOOR edge)"
            );
            continue;
        }
        // UPPER-bracket-PARKED fold: the per-fold search brackets ONLY
        // [mult_bounds] (it does NOT auto-expand — only the full-history
        // bisection does). When a fold's true k_w sits ABOVE the initial bracket
        // (a storming eval slice), the fold parks at the upper edge. That parked
        // value is an UNCONVERGED estimate, NOT a genuine fit — it must NOT be
        // counted as a disagreeing datum in the fold-AGREEMENT guard (else it
        // spuriously trips max/min on a wall artifact). This is NOT a ticker
        // rejection (the operator's no-max directive): the answer still comes
        // from the upward-auto-expanding full-history bisection (PART B2). We
        // simply OMIT the unconverged fold from the agreement set.
        let hi_parked = (best.0 as f64 - cal.mult_bounds[1]).abs() / cal.mult_bounds[1] < 0.01;
        if hi_parked {
            info!(
                window_days,
                mult = best.0,
                upper_bracket = cal.mult_bounds[1],
                "wf fold parked at upper bracket (true k_w above hint) — omitted from \
                 agreement set (NOT rejected); full-history bisection auto-expands upward"
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
        // datum. The per-window full-history recompute + structure + accept gate
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
    // (reduced overfit protection, but Stage B + structure_ok still gate the
    // result) rather than rejecting on "no window had enough data".
    if mults.is_empty() {
        warn!(
            "wf all walk-forward folds quarantine-dropped (spike/gap/short) — \
             agreement guard SKIPPED (reduced overfit protection); relying on \
             full-history bisection (PART B2) + structure guard"
        );
    } else if mults.len() == 1 {
        info!(
            k_w = mults[0],
            "wf only one walk-forward fold survived quarantine — agreement guard \
             SKIPPED (need ≥2 folds); relying on full-history bisection + structure"
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
                n_folds = mults.len(),
                "GUARD reject: fold-agreement — folds disagree (k_max/k_min {:.2} > {:.2}), overfit/regime split — returning 0.0",
                k_max / k_min, K_AGREEMENT_MAX
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
    let mut g_hi = median_bpd(hi.exp());

    // LOWER side (PRESERVED FLOOR — operator directive: "Maybe a minimum"). The
    // smallest k (lo bound = K_FLOOR) must OVERSHOOT target (g(lo) > 0). If even
    // the smallest brick yields TOO FEW bricks/day (g(lo) ≤ 0) the asset is so
    // flat it CANNOT reach target — legitimately unreachable. We do NOT
    // auto-expand downward; drop it (return 0.0 → caller routes to a per-pair
    // target/k override). A dead lo window is likewise unfittable.
    if g_lo.days == 0 || !(g_lo.median - target_bpd > 0.0) {
        warn!(
            lo_median = g_lo.median,
            target_bpd,
            mult_lo = cal.mult_bounds[0],
            median_sigma_pct,
            lo_days = g_lo.days,
            "GUARD reject: median at K_FLOOR ({:.1}) ≤ target ({:.1}) — asset too flat to reach target at any k (floor preserved, per-pair override needed) — returning 0.0",
            g_lo.median, target_bpd
        );
        return 0.0;
    }

    // UPPER side (NO CEILING — operator directive 2026-06-09: "do not clamp K. K
    // should not have a max"). The largest k (initial hi = mult_bounds[1]) must
    // UNDERSHOOT target (g(hi) < 0) to bracket the crossing. If even the biggest
    // brick still emits TOO MANY bricks/day (g(hi) ≥ target) the crossing sits
    // ABOVE the initial bracket — a storming crypto that needs k beyond the old
    // 4.0 wall. Instead of rejecting, EXPAND UPWARD: double k_hi until the
    // median drops below target OR the numeric safety cap K_MAX_SAFETY is hit.
    while g_hi.days != 0 && g_hi.median - target_bpd >= 0.0 && hi.exp() < K_MAX_SAFETY {
        let new_hi_k = (hi.exp() * 2.0).min(K_MAX_SAFETY);
        hi = new_hi_k.ln();
        g_hi = median_bpd(hi.exp());
        debug!(
            hi_k = new_hi_k,
            hi_median = g_hi.median,
            target_bpd,
            "wf bisection: doubling upper bracket (storming crypto, crossing above initial hi)"
        );
    }
    // After expansion, the hi bound must bracket the crossing (median < target).
    // It can still fail to bracket only when: (a) the hi eval degenerated
    // (days==0), or (b) the safety cap was reached and median STILL ≥ target
    // (a degenerate flat-σ series whose median never drops). Both ⇒ unfittable.
    if g_hi.days == 0 || !(g_hi.median - target_bpd < 0.0) {
        warn!(
            hi_median = g_hi.median,
            hi_k = hi.exp(),
            target_bpd,
            k_max_safety = K_MAX_SAFETY,
            median_sigma_pct,
            hi_days = g_hi.days,
            "GUARD reject: at upper bracket k={:.4} median={:.1} {} target {:.1} (degenerate σ / no crossing below K_MAX_SAFETY) — returning 0.0",
            hi.exp(), g_hi.median,
            if g_hi.days == 0 { "DEAD hi-window vs" } else { "still ≥" }, target_bpd
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
        warn!(
            k_star,
            full_median = full_stats.median,
            target_bpd,
            median_sigma_pct,
            "GUARD reject: selected rung had no scorable clean day (full-history quarantine emptied the window) — returning 0.0"
        );
        return 0.0;
    }
    // ACCEPT-TOL is now ADVISORY (operator directive 2026-06-09: "k must be the
    // PRODUCT of calibration — always find the closest-possible k, never give up
    // to a manual override"). The two-sided rung probe already selected k_star =
    // closest achievable rung. The snap_to_25_grid × integer-day-median staircase
    // can leave the best rung structurally a few % off target (BNB/USDT, SOL/USDT
    // full-2yr cannot land within 5% of 300). That is a STRUCTURAL FLOOR, NOT an
    // "no valid k" condition — so we ACCEPT k_star and only WARN. The genuinely
    // degenerate guards below (structure, K_FLOOR, min_pct-clamp) still 0.0.
    if achieved_err > RENKO_BPD_ACCEPT_TOL {
        warn!(
            k_star,
            full_median = full_stats.median,
            target_bpd,
            achieved_err_pct = achieved_err * 100.0,
            accept_tol_pct = RENKO_BPD_ACCEPT_TOL * 100.0,
            "wf accept gate (advisory) — best-achievable rung {:.1}% off target — accepting closest k (snap-grid structural floor)",
            achieved_err * 100.0
        );
        // fall through — do NOT return 0.0; k_star is the closest achievable k.
    }
    // STRUCTURE side-guard (FIX 2c, storm-robust): reject ONLY genuinely
    // structureless data (no modal cluster). A storm-heavy-but-median-sound
    // crypto pair (BNB/SOL) has its calm-day bulk clustered near the median and
    // PASSES — the storm tail is the operator-sanctioned variance, NOT noise.
    // Always log the modal-mass + MAD/median ratio so a reject is fully diagnosed.
    let modal_mass = full_stats.modal_mass();
    let mad_over_median = if full_stats.median > 0.0 { full_stats.mad / full_stats.median } else { f64::NAN };
    if !full_stats.structure_ok() {
        warn!(
            k_star,
            full_median = full_stats.median,
            full_mad = full_stats.mad,
            mad_over_median,
            modal_mass,
            modal_mass_min = STRUCTURE_MODAL_MASS_MIN,
            band_factor = STRUCTURE_BAND_FACTOR,
            days = full_stats.days,
            "GUARD reject: structure side-guard — modal mass {:.2} < min {:.2} (structureless / pure-noise full history, no day-cluster near median) — returning 0.0",
            modal_mass, STRUCTURE_MODAL_MASS_MIN
        );
        return 0.0;
    }
    if k_star < K_FLOOR {
        warn!(
            k_star,
            k_floor = K_FLOOR,
            full_median = full_stats.median,
            target_bpd,
            "GUARD reject: k_star < K_FLOOR (runtime would silently clamp) — returning 0.0 (caller drops entry)"
        );
        return 0.0;
    }
    // min_pct CLAMP-DETECTOR (2026-06-09, expert caveat #1). The bisection assumes
    // bpd(k) is monotone in k, but `compute_brick_size` floors the brick at
    // min_pct: once `k_star·σ_pct ≤ min_pct` the brick is min_pct-clamped and
    // INDEPENDENT of k, so bpd goes flat and the search can "accept" a k whose
    // brick is unresponsive to the fitted k (a passing-but-meaningless k). If a
    // MATERIAL fraction of the full history is min_pct-clamped at k_star, FAIL the
    // ticker so it routes to a per-ticker `renko_k_overrides` entry instead of
    // shipping a k the renko engine ignores.
    let clamp_frac = min_pct_clamped_fraction(
        prices, k_star, base.min_pct, vol_source, sigma_cache, first, last,
    );
    if clamp_frac > MIN_PCT_CLAMP_MAX_FRAC {
        warn!(
            k_star,
            min_pct = base.min_pct,
            clamp_frac_pct = clamp_frac * 100.0,
            clamp_max_pct = MIN_PCT_CLAMP_MAX_FRAC * 100.0,
            median_sigma_pct,
            k_star_x_median_sigma = k_star * median_sigma_pct,
            full_median = full_stats.median,
            "GUARD reject: selected brick min_pct-clamped over {:.1}% of history (> {:.0}% ceiling); k*median_σ={:.5} ≤ min_pct={} ⇒ brick floored at min_pct, k unresponsive — failing ticker for per-ticker override",
            clamp_frac * 100.0, MIN_PCT_CLAMP_MAX_FRAC * 100.0, k_star * median_sigma_pct, base.min_pct
        );
        return 0.0;
    }
    // LOWER clamp-detector on the FINAL k_star (UPPER rejection REMOVED, operator
    // directive 2026-06-09: "K should not have a max" — a large k is VALID, not
    // degenerate; the upper bracket auto-expands so a k near/above mult_bounds[1]
    // is the genuine fit, not a parked artifact). The lower side is kept: a k
    // parked at the K_FLOOR bracket edge is a degenerate-σ / unreachable-target
    // artifact (the floor preservation side).
    let lo_clamp = (k_star - cal.mult_bounds[0]).abs() / cal.mult_bounds[0] < 0.01;
    if lo_clamp {
        warn!(
            k_star,
            bound = "lower",
            mult_lo = cal.mult_bounds[0],
            full_median = full_stats.median,
            target_bpd,
            "GUARD reject: k_star parked at K_FLOOR bracket edge (degenerate-σ / unreachable-target artifact) — returning 0.0"
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
    fn quarantine_drops_gaps_keeps_storms() {
        // FIX (2026-06-09, operator spec): quarantine drops ONLY gap (0) days;
        // high-bpd STORM days are LEGITIMATE and kept (the median absorbs them).
        // 10 days: 8 "normal" ~100/day, one gap (0), one storm (1000).
        // n_days=10 → min_clean = max(3, 5) = 5. 9 non-gap days survive.
        let per_day = vec![100, 98, 102, 0, 101, 99, 1000, 100, 103, 97];
        let clean = quarantine_clean_days(&per_day, per_day.len())
            .expect("9 non-gap days >= min_clean(5)");
        assert_eq!(clean.len(), 9, "only gap(0) excluded; storm(1000) KEPT");
        assert!(clean.iter().all(|&b| b > 0), "all surviving days are non-gap");
        assert!(clean.contains(&1000), "storm day retained in objective");
        // Median is robust to the single storm: 9 days {97,98,99,100,100,101,
        // 102,103,1000} → median = 100, unmoved by the storm.
        let mut s = clean.clone();
        s.sort_unstable();
        assert_eq!(median_sorted(&s), 100.0, "median absorbs the storm day");
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

    /// Deterministic BIMODAL tick path: `n_calm` calm days interleaved with
    /// `n_storm` high-vol "storm" days, both at `ticks_per_day` cadence. Calm
    /// days use `sigma_calm`, storm days `sigma_storm` (≫ calm). Days are laid
    /// out so that calm and storm days ALTERNATE (storm days are NOT a tiny
    /// minority) — this is the regime that produced the BTC/USDT bug: with a
    /// substantial storm fraction the UNclipped (operator) full-history median
    /// sits well above target at the small k where the CLIPPED (calm-only)
    /// median equals target. Returns `(ts, mid)` ascending.
    fn synth_bimodal_path(
        n_days: usize,
        ticks_per_day: usize,
        sigma_calm: f64,
        sigma_storm: f64,
        storm_every: usize,
        seed: u64,
    ) -> Vec<(i64, f64)> {
        let dt_ms = (MS_PER_DAY / ticks_per_day as i64).max(1);
        let mut state = seed;
        let mut next_u = || {
            state ^= state >> 12; state ^= state << 25; state ^= state >> 27;
            ((state.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64) / ((1u64 << 53) as f64)
        };
        let mut out = Vec::with_capacity(n_days * ticks_per_day);
        let mut price = 100.0f64;
        let t0: i64 = 1_700_000_000_000;
        for d in 0..n_days {
            // Storm day every `storm_every`-th day (e.g. 2 → alternate).
            let sigma_step = if storm_every > 0 && d % storm_every == 0 {
                sigma_storm
            } else {
                sigma_calm
            };
            for j in 0..ticks_per_day {
                let z = next_u() - 0.5;
                price *= (sigma_step * z).exp();
                let i = (d * ticks_per_day + j) as i64;
                out.push((t0 + i * dt_ms, price));
            }
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

    /// OPERATOR-PRINCIPLE GUARD (2026-06-09 brick-storm calibration RCA): the
    /// MEDIAN bpd is the target and it is ROBUST to storm days. A path with many
    /// calm days AND a substantial set of high-vol STORM days must STILL
    /// calibrate to the k where the FULL-history (storms-INCLUDED) median ==
    /// target — it must NOT be rejected. This directly encodes the operator's
    /// spec and pins the fix: the legacy `[median/3, median*3]` SPIKE CLIP inside
    /// `quarantine_clean_days` dropped storm days from the objective, so the
    /// search fitted the CALM-day median (≈ target at a small k) while the
    /// operator's storms-included full median at that k was far above target →
    /// the accept gate then rejected the ticker (the `full_median=535 ≫
    /// target=300 @ k=0.252` symptom on BTC/USDT + 82 bases + 5 crosses).
    ///
    /// This test FAILS on the pre-fix code (clip present): the bisection's
    /// median surface is the clipped/calm median, so it converges to a too-small
    /// k whose storms-included full median overshoots → 0.0. With the clip
    /// removed, `bpd(k)` is monotone on the storms-included median and the
    /// bisection lands on the correct (larger) k. We assert BOTH:
    ///   (a) the calibrator returns k>0 with full-history median within tol, and
    ///   (b) the OLD clipped objective at that SAME k would have read a LOWER
    ///       median than the (correct) full median — the mechanism the fix kills.
    #[test]
    fn storming_ticker_calibrates_on_full_history_median() {
        let target_bpd = 300.0;
        // 40d, 4000 ticks/day. Storm day every 2nd day (a SUBSTANTIAL storm
        // fraction, not a tiny minority) at ~6× the calm step. At the target k
        // the calm days emit ~150 bricks/day and storm days ~600+, so the
        // storms-included median sits at target while the calm-only (clipped)
        // median would sit well below it → the old clip-fitted k overshoots.
        let prices = synth_bimodal_path(40, 4000, 0.0006, 0.0035, 2, 0xB1B0DA1);
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
        // Single fit window: the alternating-storm regime legitimately makes the
        // 14d vs 30d eval folds disagree (>1.5×), which is the fold-AGREEMENT
        // guard's job — orthogonal to the clip fix under test. One window
        // isolates the full-history-median selector (PART B2). Production windows
        // are config-driven, so this is a valid CalibrationConfig.
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![30],
            min_window_days: 7,
            max_rounds: 40,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(
            k > 0.0,
            "storming ticker must calibrate (storms-included median == target k \
             exists) — must NOT return 0.0 (got {k})"
        );
        assert!((k as f64) >= K_FLOOR, "derived k must be ≥ K_FLOOR (got {k})");

        // (a) The derived k's FULL-history (storms-included) median is the
        // operator's measurement and must be within accept tol.
        let full = count_bars_per_day_from_prices(
            &prices, &RenkoConfig { multiplier: k, min_pct: 0.0 },
            &vol, &vol_cfg, &sigma_cache, first, last,
        );
        let full_rel_err = (full.median / target_bpd - 1.0).abs();
        assert!(
            full_rel_err <= RENKO_BPD_ACCEPT_TOL,
            "storms-included full median {:.1} within {:.0}% of target {:.0}? \
             rel_err={:.3}",
            full.median, RENKO_BPD_ACCEPT_TOL * 100.0, target_bpd, full_rel_err
        );

        // (b) `count_bars_per_day_from_prices` now reports the storms-INCLUDED
        // median: `full.median` equals the median of ALL non-gap days, no spike
        // clip. Verify directly off `bricks_per_day`, so a future re-introduction
        // of the clip (which would make `full.median` read the calm-only value)
        // trips this guard.
        let nonzero: Vec<u64> =
            full.bricks_per_day.iter().copied().filter(|&b| b > 0).collect();
        let mut nz_sorted = nonzero.clone();
        nz_sorted.sort_unstable();
        let storms_included_median = median_sorted(&nz_sorted);
        assert_eq!(
            full.median, storms_included_median,
            "reported median must be the storms-INCLUDED (unclipped) median \
             ({} vs {})",
            full.median, storms_included_median
        );
    }

    /// OPERATOR-PRINCIPLE UNIT GUARD (2026-06-09): `quarantine_clean_days` must
    /// drop ONLY gap (zero) days and KEEP every storm day, so the median it feeds
    /// the calibration objective is the operator's storms-INCLUDED median. Pins
    /// the exact fix: the legacy `[prov_median/3, prov_median*3]` spike clip would
    /// have dropped the storm days and produced a different (calm-only) median —
    /// the wrong objective the pre-fix bisection optimised, which then made the
    /// storms-included accept gate reject a correct-k ticker.
    #[test]
    fn quarantine_objective_is_storms_included_median() {
        // 12 days: 8 calm ~120/day + 4 STORM ~900/day (substantial storm
        // fraction, all > 3× the calm provisional median ⇒ the OLD clip drops
        // all four). No gap days.
        let per_day = vec![118u64, 122, 900, 119, 950, 121, 880, 120, 910, 123, 117, 124];
        let n = per_day.len();

        // FIXED: only gap days drop (none here) ⇒ all 12 survive.
        let clean = quarantine_clean_days(&per_day, n).expect("non-empty");
        assert_eq!(clean.len(), 12, "no gap days ⇒ all kept, storms included");
        let mut cs = clean.clone();
        cs.sort_unstable();
        let kept_median = median_sorted(&cs);

        let mut all = per_day.clone();
        all.sort_unstable();
        let full_median = median_sorted(&all);
        assert_eq!(kept_median, full_median, "kept-set median == full-set median");

        // LEGACY clip reconstruction drops the 4 storm days ⇒ strictly DIFFERENT
        // (calm-only) median = the objective the pre-fix search wrongly used.
        let (clo, chi) = (full_median / 3.0, full_median * 3.0);
        let clipped: Vec<u64> = per_day
            .iter()
            .copied()
            .filter(|&b| (b as f64) >= clo && (b as f64) <= chi)
            .collect();
        assert!(clipped.len() < per_day.len(), "legacy clip drops the storm days");
        let mut clp = clipped.clone();
        clp.sort_unstable();
        let clipped_median = median_sorted(&clp);
        assert_ne!(
            clipped_median, kept_median,
            "legacy clipped (calm-only) median {} must differ from the \
             storms-included median {} the fix now uses",
            clipped_median, kept_median
        );
    }

    #[test]
    fn bpd_accept_gate_threshold_math() {
        // ADVISORY tol (2026-06-09): `RENKO_BPD_ACCEPT_TOL` no longer DROPS a
        // ticker — exceeding it only WARNs (snap-grid structural floor) while
        // `k_star` (the closest achievable rung) is still returned. This test pins
        // the THRESHOLD ARITHMETIC (which side of the advisory line each err lands
        // on) — the constant comparison is unchanged; only the consequence (warn
        // vs return-0.0) changed in the gate.
        let target = 300.0;
        // 33 bpd vs 300 → rel-err ≈ 0.89 → ADVISORY warn (the SOL/USDT k≈4 case);
        // pre-fix this dropped the ticker, post-fix it accepts the closest k.
        let rel_err_bad = (33.0_f64 / target - 1.0).abs();
        assert!(rel_err_bad > RENKO_BPD_ACCEPT_TOL);
        // 290 bpd vs 300 → rel-err ≈ 0.033 → within tol (no warn).
        let rel_err_ok = (290.0_f64 / target - 1.0).abs();
        assert!(rel_err_ok <= RENKO_BPD_ACCEPT_TOL);
        // 330 bpd vs 300 → rel-err = 0.10 → above tol ⇒ advisory warn (still k>0).
        let rel_err_warn = (330.0_f64 / target - 1.0).abs();
        assert!(rel_err_warn > RENKO_BPD_ACCEPT_TOL);
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

    /// Helper: build a DailyBpdStats from a per-day vector, computing the same
    /// median/mad/days the production counter does, so structure_ok/modal_mass
    /// can be exercised on realistic day distributions.
    fn stats_from_days(per_day: &[u64]) -> DailyBpdStats {
        let clean: Vec<u64> = per_day.iter().copied().filter(|&b| b > 0).collect();
        let mut sorted = clean.clone();
        sorted.sort_unstable();
        let median = median_sorted(&sorted);
        let mut devs: Vec<u64> =
            clean.iter().map(|&b| (b as f64 - median).abs().round() as u64).collect();
        devs.sort_unstable();
        let mad = median_sorted(&devs);
        let mean = if clean.is_empty() { 0.0 } else { clean.iter().sum::<u64>() as f64 / clean.len() as f64 };
        DailyBpdStats {
            bricks_per_day: per_day.to_vec(), median, mean, mad, days: clean.len(),
        }
    }

    #[test]
    fn structure_guard_storm_robust_rejects_only_noise() {
        // FIX 2c (storm-robust): the guard passes STORM-HEAVY-but-median-sound
        // crypto (operator principle) and rejects ONLY structureless / pure-noise
        // data. Measure = modal mass (fraction of days within [med/3, med*3]).

        // (a) STORM-HEAVY healthy crypto: ~30 calm days clustered near 300 + a
        //     handful of extreme storm days (~3000, 10× median). MUST PASS — the
        //     storm tail is the operator-sanctioned variance, NOT noise.
        let mut storm = vec![
            270, 280, 285, 290, 295, 298, 300, 300, 302, 305, 308, 310, 312, 315,
            265, 275, 288, 292, 296, 301, 303, 307, 311, 318, 320, 322, 285, 293,
        ];
        storm.extend_from_slice(&[3000, 3200, 3100, 2900, 3300]); // storm tail
        let storm_stats = stats_from_days(&storm);
        assert!(
            storm_stats.structure_ok(),
            "storm-heavy crypto (median {:.0}, modal_mass {:.2}) must PASS — storms \
             are operator-sanctioned variance, NOT noise",
            storm_stats.median, storm_stats.modal_mass()
        );
        // And the legacy MAD/median ratio it REPLACED is NOT what gates it:
        assert!(
            storm_stats.modal_mass() >= STRUCTURE_MODAL_MASS_MIN,
            "modal mass {:.2} ≥ {:.2}", storm_stats.modal_mass(), STRUCTURE_MODAL_MASS_MIN
        );

        // (b) GENUINE pure noise: days scattered across orders of magnitude with
        //     NO modal cluster near the median. MUST REJECT.
        let noise = vec![1u64, 2, 5, 12, 40, 120, 400, 1200, 3000, 9000];
        let noise_stats = stats_from_days(&noise);
        assert!(
            !noise_stats.structure_ok(),
            "structureless data (modal_mass {:.2}) must REJECT",
            noise_stats.modal_mass()
        );

        // (c) Dead/degenerate window is not ok.
        let dead = DailyBpdStats {
            bricks_per_day: vec![], median: 0.0, mean: 0.0, mad: 0.0, days: 0,
        };
        assert!(!dead.structure_ok(), "dead window not ok");
        assert_eq!(dead.modal_mass(), 0.0, "dead window has zero modal mass");

        // (d) Tight calm-only window (no storms): trivially passes.
        let calm = stats_from_days(&[290, 295, 300, 300, 305, 310, 298, 302]);
        assert!(calm.structure_ok(), "tight calm window passes");
        assert_eq!(calm.modal_mass(), 1.0, "all calm days in band ⇒ modal_mass=1");
    }

    /// Per-tick σ VolSource: `sigma_cache[i]` indexed by tick ORDER (one cache
    /// slot per tick), so a path can mix clamped and unclamped regions. Holds the
    /// exact per-tick `mts` list and recovers `i` by exact match (the test stamps
    /// distinct, ≥16µs-apart tick mts so the map is a bijection). Used by
    /// `min_pct_clamped_brick_is_rejected`.
    struct PerTickSigma {
        mts: Vec<u64>,
    }
    impl VolSource for PerTickSigma {
        fn len(&self) -> usize { self.mts.len() }
        fn sigma_pct(&self, i: usize) -> f64 { i as f64 } // unused by calibrate (cache wins)
        fn find_index_for_mts(&self, mts: u64) -> usize {
            // Last index whose mts ≤ query (ticks are ascending) — the same
            // "most-recent-σ-as-of-ts" semantics the real VolSource uses.
            self.mts.partition_point(|&m| m <= mts).saturating_sub(1)
        }
    }

    /// EXPERT CAVEAT #1 (2026-06-09): the min_pct CLAMP-DETECTOR. When
    /// `k_star · σ_pct ≤ min_pct` the brick floors at `min_pct·price` and becomes
    /// INDEPENDENT of k — bpd(k) goes flat, the bisection's monotonicity breaks,
    /// and a "passing-but-wrong" k can slip through. The accept gate must REJECT
    /// (return 0.0) when a MATERIAL fraction of the series is min_pct-clamped at
    /// the selected k, so the ticker routes to a per-ticker `renko_k_overrides`.
    #[test]
    fn min_pct_clamped_brick_is_rejected() {
        // ── Part A: the pure detector arithmetic ─────────────────────────────
        // sigma_cache[i] is the σ_pct AT tick i. Ticks are stamped 1s apart from a
        // post-MITCH-epoch anchor so each maps to a distinct mts; PerTickSigma
        // recovers tick i by mts. 10 ticks: first 8 LOW σ (0.001), last 2 HIGH
        // σ (0.02).
        let sigma_cache = vec![
            0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.02, 0.02,
        ];
        let t0: i64 = 1_700_000_000_000; // well past MITCH epoch (2024-01-01)
        let prices: Vec<(i64, f64)> = (0..sigma_cache.len() as i64)
            .map(|i| (t0 + i * 1000, 100.0))
            .collect();
        let mts: Vec<u64> = prices
            .iter()
            .map(|&(ts, _)| timestamp::from_epoch_ms(ts))
            .collect();
        let vol = PerTickSigma { mts };
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;

        // k_star = 0.5, min_pct = 0.002. Clamp when k·σ ≤ min_pct ⇒ σ ≤ 0.004.
        // 8/10 ticks have σ=0.001 (clamped); 2/10 have σ=0.02 (unclamped).
        let frac = min_pct_clamped_fraction(
            &prices, 0.5, 0.002, &vol, &sigma_cache, first, last,
        );
        assert!((frac - 0.8).abs() < 1e-9, "8/10 ticks clamped (got {frac})");
        assert!(frac > MIN_PCT_CLAMP_MAX_FRAC, "0.8 > 0.5 ⇒ gate would reject");

        // Raise k so the brick is k-responsive everywhere: k=10, min_pct=0.002 ⇒
        // clamp when σ ≤ 0.0002 — no tick qualifies ⇒ fraction 0 (accept).
        let frac_ok = min_pct_clamped_fraction(
            &prices, 10.0, 0.002, &vol, &sigma_cache, first, last,
        );
        assert_eq!(frac_ok, 0.0, "no tick clamped at large k ⇒ accept");

        // ── Part B: the gate REJECTS end-to-end ──────────────────────────────
        // A calm, LOW-σ full-tick path with a min_pct set ABOVE k·σ for the whole
        // series. Every brick floors at min_pct ⇒ bpd is flat in k ⇒ the search
        // cannot bracket target AND/OR lands on a min_pct-clamped k_star. Either
        // way the calibrator MUST NOT ship a (meaningless) positive k.
        let prices2 = synth_gbm_path(40, 4000, 0.0005, 0xC0FFEE); // tiny σ_step
        let vol2 = ConstSigma(0.0008); // 0.08% σ_pct, flat
        let sigma_cache2 = vec![0.0008];
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
        // min_pct = 1% — far above k·σ for any k ∈ [0.05,4]·0.0008 = [0.00004,0.0032].
        // Every brick is min_pct-clamped over the whole search ⇒ k is unresponsive.
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.01 };

        let k = calibrate_mtf_walkforward(
            &prices2, &cal, &base, &vol2, &vol_cfg, &sigma_cache2, 300.0, 7,
        );
        assert_eq!(
            k, 0.0,
            "min_pct-dominated series must FAIL (return 0.0 → per-ticker override), not ship a clamped k"
        );
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

    /// SYNTH SELECTOR PARITY (2026-06-09): production synth calibration
    /// (`bin/nxr_calibrate.rs::calibrate_one_synth`) feeds an event-merged
    /// cross price series (`leg_a / leg_b`) into the SAME
    /// `calibrate_mtf_walkforward` the base path uses — there is NO separate
    /// geo-mean / eval-slice selector for synths. This test pins that contract:
    /// a SYNTH-SHAPED series (a cross reconstructed from two USDT-quoted leg
    /// GBM paths, exactly the construction in `calibrate_one_synth`) whose SHORT
    /// trailing walk-forward windows are all quarantine-dropped must STILL reach
    /// the full-history log-k bisection (PART B2) and return a valid k>0 — never
    /// fail just because the recent eval folds were killed. If a future change
    /// re-routes synths to a non-bisection selector, this fails.
    #[test]
    fn synth_cross_reaches_full_history_bisection() {
        let target_bpd = 300.0;
        // Reconstruct a synth cross EXACTLY as `calibrate_one_synth` does:
        // merge two leg paths and emit `mid = leg_a / leg_b` per event. Both legs
        // share the tick grid here (same dt) so the merge is a 1:1 zip — the
        // ratio path is itself a GBM with the two legs' vols compounded.
        let leg_a = synth_gbm_path(40, 4000, 0.0042, 0xA11CE);   // ETH/USDT-like
        let leg_b = synth_gbm_path(40, 4000, 0.0042, 0xB0B);     // BTC/USDT-like
        let n = leg_a.len().min(leg_b.len());
        let mut prices: Vec<(i64, f64)> = (0..n)
            .map(|i| {
                let (ts, a) = leg_a[i];
                let (_, b) = leg_b[i];
                (ts, a / b) // synth cross mid (worst-case-spread collapses to ratio at mid)
            })
            .collect();
        // sigma_step 0.0042 per leg → ratio σ ≈ sqrt(2)*0.0042 ≈ 0.006, the same
        // crossing density the base full-tick regression hits at k≈0.5. Sanity:
        // ensure the merged path is non-degenerate before contaminating it.
        assert!(prices.len() > 30_000, "merged synth path too short");

        // Contaminate ONLY the trailing ~8 days (gap+spike) so every SHORT
        // walk-forward eval fold is quarantine-killed, forcing the answer to come
        // from the full-history bisection — same shape as
        // `quarantined_short_windows_still_get_full_history_k`, but on a
        // SYNTH-constructed (ratio) series.
        let dt = prices[1].0 - prices[0].0;
        let ticks_per_day = (MS_PER_DAY / dt) as usize;
        let last_ts = prices.last().unwrap().0;
        let day0 = last_ts - 8 * MS_PER_DAY;
        prices.retain(|&(ts, _)| ts < day0);
        let base_price = prices.last().map(|p| p.1).unwrap_or(1.0);
        for d in 0..8i64 {
            let day_start = day0 + d * MS_PER_DAY;
            if d % 2 == 0 {
                continue; // GAP day → 0-count
            }
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
            "synth cross with all-quarantined short folds must reach the \
             full-history bisection and return k>0, not fail (got {k})"
        );
        assert!((k as f64) >= K_FLOOR, "synth k must be ≥ K_FLOOR (got {k})");

        // The selected k's FULL-history median (the quantity the operator + DQ
        // audit measure on the live synth renko) must land within accept tol —
        // proving the synth path's selector IS the bisection, not the old
        // eval-slice geo-mean that produced all-fail crosses.
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let stats = count_bars_per_day_from_prices(
            &prices, &RenkoConfig { multiplier: k, min_pct: 0.0 },
            &vol, &vol_cfg, &sigma_cache, first, last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "synth full-history median {:.1} within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median, RENKO_BPD_ACCEPT_TOL * 100.0, target_bpd, rel_err
        );
    }

    /// NO-UPPER-CAP (2026-06-09, operator directive "K should not have a max"):
    /// a STORMING series whose median==target crossing sits ABOVE the initial
    /// upper bracket (`mult_bounds[1]`) must AUTO-EXPAND the bracket upward and
    /// return THAT (large) k with the full-history median within tol — it must
    /// NOT be bracket-invalid-rejected nor clamp-dropped. Pre-fix this returned
    /// 0.0 ("bracket invalid: median(hi) ≥ target"); post-fix the bisection
    /// doubles `k_hi` past the wall until it brackets the crossing.
    #[test]
    fn storming_series_above_initial_bracket_auto_expands_upward() {
        let target_bpd = 300.0;
        // A VERY volatile path with a DELIBERATELY SMALL initial upper bracket
        // (mult_bounds[1] = 0.5) so the true crossing is far above it. With
        // ConstSigma σ=0.01, brick_pct = k·0.01; at the bracket hi k=0.5 the
        // brick is 0.5 % — far too small for this storm ⇒ median ≫ target ⇒ the
        // old code returned "bracket invalid". The fix doubles k_hi upward until
        // the (much larger) brick brings the median down to target.
        let prices = synth_gbm_path(40, 4000, 0.012, 0x5704);
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
        // INITIAL upper bracket hint = 0.5 (small ON PURPOSE). The fix must climb
        // above it. mult_bounds[0] stays at K_FLOOR (lower floor preserved).
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![30],
            min_window_days: 7,
            max_rounds: 40,
            tolerance: 0.02,
            mult_bounds: [0.05, 0.5],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(
            k > 0.0,
            "storming series above the initial bracket must auto-expand upward \
             and calibrate — NOT return 0.0 (got {k})"
        );
        // The whole point: the returned k must sit ABOVE the initial bracket hi,
        // proving the upward auto-expansion (not a parked-at-bound artifact).
        assert!(
            (k as f64) > cal.mult_bounds[1],
            "fitted k={k} must exceed the initial upper bracket {} (auto-expanded)",
            cal.mult_bounds[1]
        );

        let stats = count_bars_per_day_from_prices(
            &prices, &RenkoConfig { multiplier: k, min_pct: 0.0 },
            &vol, &vol_cfg, &sigma_cache, first, last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "auto-expanded k median {:.1} within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median, RENKO_BPD_ACCEPT_TOL * 100.0, target_bpd, rel_err
        );
    }

    /// FLOOR PRESERVED (2026-06-09, operator directive "Maybe a minimum"): a
    /// TOO-QUIET series whose median is BELOW target even at the smallest k
    /// (K_FLOOR) legitimately cannot reach target — the bisection must NOT expand
    /// downward; it returns 0.0 so the ticker routes to a per-pair target/k
    /// override. Only the MAX side was unclamped; the MIN side is unchanged.
    /// ADVISORY ACCEPT-TOL (2026-06-09, operator directive "k must be the PRODUCT
    /// of calibration — always find the closest-possible k, never give up to a
    /// manual override"): when the snap_to_25_grid × integer-day-median STAIRCASE
    /// leaves the best bracketing rung structurally OFF target by MORE than
    /// `RENKO_BPD_ACCEPT_TOL` (the BNB/USDT + SOL/USDT full-2yr case), the
    /// calibrator must STILL return that closest-achievable k (k>0) — it must NOT
    /// reject-to-0.0. The 5% tol is ADVISORY (warn-only), not a drop gate.
    ///
    /// Construction: a calm path at a VERY LOW target so consecutive achievable
    /// per-day-median rungs (snap_to_25_grid × integer-day median) are coarse in
    /// RELATIVE terms. At target=5.5 the two bracketing rungs are median=6 and
    /// median=5 — BOTH ~9.1% off — so no rung lands within 5% (structural floor).
    /// Pre-fix this returned 0.0 (accept-gate reject); post-fix it returns the
    /// closest rung's k (k>0) with `achieved_err` in the 6-10% band.
    #[test]
    fn best_rung_off_target_accepts_closest_k_not_zero() {
        // target between two adjacent integer rungs (5 and 6): err = 0.5/5.5 ≈
        // 9.1% on EITHER side ⇒ the closest achievable rung is structurally >5%
        // off — the exact BNB/USDT + SOL/USDT staircase-floor regime.
        let target_bpd = 5.5;
        let prices = synth_gbm_path(40, 400, 0.0025, 0xFEED);
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
        // Upper bracket 0.65 lands in a VALID region (median≈4-5, days≥38 < target
        // 5.5) so the bracket holds; K_FLOOR overshoots (median=335 > 5.5). The
        // two bracketing rungs straddling 5.5 are median=6 and median=5 — both
        // ~9% off — the snap-grid structural floor under test.
        let cal = CalibrationConfig {
            target_bpd,
            k_fit_windows_days: vec![30],
            min_window_days: 7,
            max_rounds: 40,
            tolerance: 0.02,
            mult_bounds: [0.05, 0.65],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        // PRIMARY assertion: the closest-achievable rung k is returned, NOT 0.0.
        assert!(
            k > 0.0,
            "best rung off-target by >5% (snap-grid structural floor) must return \
             the closest k, NOT reject to 0.0 (got {k}) — operator: k is the \
             PRODUCT of calibration, never give up to a manual override"
        );
        assert!((k as f64) >= K_FLOOR, "returned k must be ≥ K_FLOOR (got {k})");

        // The structural floor (achieved_err) must EXCEED the advisory tol — this
        // is exactly the regime the old gate REJECTED. Confirm we are testing the
        // >5%-off branch (6-10% band), and that the returned k is the closer rung.
        let stats = count_bars_per_day_from_prices(
            &prices, &RenkoConfig { multiplier: k, min_pct: 0.0 },
            &vol, &vol_cfg, &sigma_cache, first, last,
        );
        let achieved_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            achieved_err > RENKO_BPD_ACCEPT_TOL,
            "this test must exercise the >5%-off advisory branch — achieved_err \
             {:.3} must exceed tol {:.3} (median {} vs target {})",
            achieved_err, RENKO_BPD_ACCEPT_TOL, stats.median, target_bpd
        );
        assert!(
            achieved_err < 0.12,
            "structural floor in the 6-10% band (got {:.1}% — median {} vs {})",
            achieved_err * 100.0, stats.median, target_bpd
        );
        // The returned k must be the OPTIMAL rung: neither bracket neighbour beats
        // its err (it IS the closest-achievable k, just structurally off target).
        let median_err = |kk: f64| -> f64 {
            let s = count_bars_per_day_from_prices(
                &prices, &RenkoConfig { multiplier: kk as f32, min_pct: 0.0 },
                &vol, &vol_cfg, &sigma_cache, first, last,
            );
            if s.days == 0 { f64::INFINITY } else { (s.median / target_bpd - 1.0).abs() }
        };
        let step = K_BRACKET_LN_EPS.exp();
        assert!(
            achieved_err <= median_err(k as f64 * step)
                && achieved_err <= median_err(k as f64 / step),
            "returned k sits on the optimal (closest) rung despite being off target"
        );
    }

    #[test]
    fn too_quiet_series_below_target_at_floor_returns_zero() {
        let target_bpd = 300.0;
        // A near-flat path: tiny per-tick step ⇒ very few brick crossings even at
        // the SMALLEST k (K_FLOOR). With ConstSigma σ=0.01, at k=K_FLOOR=0.05 the
        // brick is 0.05 % — yet this path barely moves, so the full-history median
        // sits well BELOW target. g(lo) = median(K_FLOOR) - target < 0 ⇒ the lower
        // bracket does NOT overshoot ⇒ unreachable ⇒ 0.0 (floor preserved).
        let prices = synth_gbm_path(40, 4000, 0.00005, 0xDEAD);
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
            k_fit_windows_days: vec![30],
            min_window_days: 7,
            max_rounds: 40,
            tolerance: 0.02,
            mult_bounds: [0.05, 4.0],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };

        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert_eq!(
            k, 0.0,
            "too-quiet series (median < target even at K_FLOOR) must return 0.0 \
             (floor preserved, NO downward expansion) — got {k}"
        );
    }

    /// TASK-2 REGRESSION (2026-06-09, BNB/SOL storm-rejection bug-class): a
    /// storm-heavy-but-median-sound crypto path (many calm days clustered near
    /// target + a MINORITY of extreme storm days, ~10× median) must CALIBRATE
    /// (k > 0). Under the OLD `MAD/median ≤ 1.0` spread guard the legitimate
    /// storm variance could (in the bug-class the operator flagged) reject the
    /// pair as "pure noise"; the storm-robust modal-mass guard (FIX 2c) passes
    /// it. The full-history median at the derived k must still equal target
    /// (storms are absorbed by the median — operator principle).
    #[test]
    fn storm_heavy_crypto_calibrates_not_dispersion_rejected() {
        let target_bpd = 300.0;
        // 60d, storms every 6th day (~17% storm fraction, a realistic crypto
        // storm cadence) at ~10× the calm step. Calm days emit a tight cluster
        // near target at the fitted k; storm days form an extreme upper tail.
        let prices = synth_bimodal_path(60, 4000, 0.004, 0.04, 6, 0xB1B0DA1);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vol_cfg = VolConfig {
            ema_period: 1, sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95], winsorize_min_samples: 1,
            recompute_cooldown_ms: 0, ..VolConfig::default()
        };
        let cal = CalibrationConfig {
            target_bpd, k_fit_windows_days: vec![30], min_window_days: 7,
            max_rounds: 40, tolerance: 0.02, mult_bounds: [0.05, 4.0],
        };
        let base = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };
        let k = calibrate_mtf_walkforward(
            &prices, &cal, &base, &vol, &vol_cfg, &sigma_cache, target_bpd, 7,
        );
        assert!(
            k > 0.0,
            "storm-heavy crypto must CALIBRATE (storms are operator-sanctioned \
             variance, NOT a dispersion-reject) — got k={k}"
        );

        let full = count_bars_per_day_from_prices(
            &prices, &RenkoConfig { multiplier: k, min_pct: 0.0 },
            &vol, &vol_cfg, &sigma_cache, first, last,
        );
        // The storm tail is real: MAD/median can be large, but the median is at
        // target and modal mass is high ⇒ structure_ok passes. Pin BOTH facts so
        // a regression that re-introduces a spread-based reject is caught.
        let full_rel_err = (full.median / target_bpd - 1.0).abs();
        assert!(
            full_rel_err <= RENKO_BPD_ACCEPT_TOL,
            "storms-included full median {:.1} within {:.0}% of target",
            full.median, RENKO_BPD_ACCEPT_TOL * 100.0
        );
        assert!(
            full.structure_ok(),
            "storm-heavy series must pass structure_ok (modal_mass {:.2})",
            full.modal_mass()
        );
        // A storm tail DOES inflate the legacy MAD/median spread well above the
        // old 1.0 bound on some seeds — proving the spread measure was the wrong
        // gate. We don't assert its exact value (seed-dependent) but DO assert
        // the modal-mass guard is what (correctly) admits the pair.
        assert!(
            full.modal_mass() >= STRUCTURE_MODAL_MASS_MIN,
            "modal mass {:.2} ≥ {:.2}", full.modal_mass(), STRUCTURE_MODAL_MASS_MIN
        );
    }
}
