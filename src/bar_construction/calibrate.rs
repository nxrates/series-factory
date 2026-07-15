//! Offline calibration of the Renko `multiplier` (k) to a target `bars_per_day`
//! (bpd). LEAN solver (2026-06-10, `docs/renko-methodology.md`): the multi-fold
//! MTF log-k bisection + guard tangle is replaced by ONE direct
//! SCALE-TO-TARGET solver, [`scale_to_target_k`].
//!
//! Model (UNCHANGED): `brick_size(t) = snap_to_25_grid(price · max(k·σ_pct(t),
//! min_pct))`. The σ estimator (`sdk/rust/src/vol.rs`) is untouched; the ONLY
//! change is HOW k is found.
//!
//! `bpd(k)` is a monotone-decreasing STAIRCASE (snap_to_25_grid lattice +
//! 30-min recompute + min_pct floor), NOT exact `1/k`. So the analytic step
//! `k1 = k0·(m0/target)` is a high-quality WARM START — refined by re-count, a
//! bounded log-k bracket fallback (the wrong-tread safety net), and a ±1-rung
//! snap probe. See methodology §4.
//!
//! Objective: MEDIAN over the trailing `rolling_window_days` of per-UTC-day
//! brick counts (gap/zero days DROPPED via [`quarantine_clean_days`]; STORM days
//! KEPT) == resolved `target_bpd`, counted on the FULL-TICK path via
//! [`count_bars_per_day_from_prices`] (the granularity-parity guarantee).

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use nxr_sdk::mitch::timestamp;
use nxr_sdk::renko::{RenkoConfig, RenkoGenerator, K_FLOOR, K_MAX_SAFETY, SIGMA_FALLBACK};
use nxr_sdk::vol::{VolConfig, VolSource};

/// Resolve σ_pct for a given epoch-ms timestamp via the precomputed cache.
/// Used in hot loops where the engine no longer owns the σ source.
#[inline]
fn sigma_for_ts<S: VolSource + ?Sized>(ts_ms: i64, vol_source: &S, sigma_cache: &[f64]) -> f64 {
    let mts = timestamp::from_epoch_ms(ts_ms);
    let i = vol_source.find_index_for_mts(mts);
    sigma_cache.get(i).copied().unwrap_or(SIGMA_FALLBACK)
}

/// Calibration knobs. Maps to `series.calibration` in `config.yml`.
///
/// One trailing window (`rolling_window_days`), one median objective, one
/// direct solver. The σ-blend MTF (the entire regime-adaptive layer) lives on
/// `VolConfig.sigma_blend_windows_days` and is unrelated to this.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalibrationConfig {
    /// Target bars-per-day to converge to (default 300 for crypto majors).
    pub target_bpd: f64,
    /// Single trailing window (days) over which the per-UTC-day brick-count
    /// MEDIAN is driven to `target_bpd`. Replaces the legacy MTF
    /// `k_fit_windows_days` blend.
    pub rolling_window_days: usize,
    /// Minimum days of trailing history required to attempt a fit.
    pub min_window_days: usize,
    /// Iteration cap for the BOUNDED log-k bracket fallback (methodology §4
    /// step 6 — the wrong-tread safety net). Renamed from `max_rounds`.
    pub bracket_max_iters: usize,
    /// Warm-start accept tolerance: the direct scale step accepts when
    /// `|median/target − 1| ≤ accept_tol`. ALSO the advisory achieved-err warn
    /// threshold (NOT a drop gate). Renamed from `tolerance`.
    pub accept_tol: f64,
    /// `[K_FLOOR, initial-upper-bracket-hint]`. `[0]` is a hard lower floor (no
    /// downward expansion); `[1]` only seeds the bracket fallback's upper side
    /// (which doubles up to `K_MAX_SAFETY`).
    pub mult_bounds: [f64; 2],
}

use nxr_sdk::shard::MS_PER_DAY;

/// Canonical default bpd-accept tolerance (the warm-start accept threshold + the
/// advisory achieved-err warn threshold). The live solver reads the per-config
/// `CalibrationConfig::accept_tol` (default 0.05 == this); this constant is the
/// documented default and the test oracle.
#[allow(dead_code)]
const RENKO_BPD_ACCEPT_TOL: f64 = 0.05;

/// Zero/near-zero median guard threshold (methodology §4 step 3). When the
/// warm-start count yields `median < EPS_BPD` (≈ no bricks) the `m0/target`
/// divide would explode; the solver instead falls straight to the bounded
/// bracket search from scratch. This preserves the `bars≈0 → 100%-err` cliff
/// lock that the legacy `calibrate_mtf_with_target` n==0 guard provided.
const EPS_BPD: f64 = 1.0;

/// Bounded bracket-fallback STOP criterion: log-k bracket half-width at which
/// the wrong-tread bisection halts. `ln(1.005)` ⇒ the bracket [exp(lo),
/// exp(hi)] is ≤ 0.5 % wide in k. We STOP ON BRACKET WIDTH, not on score: the
/// snap_to_25_grid lattice + integer-day median make bpd(k) a STAIRCASE, so a
/// score-stop stalls on a flat tread while the true crossing sits at the rung
/// edge. The two-sided rung probe then picks the closer of the two bracketing
/// rungs.
const K_BRACKET_LN_EPS: f64 = 0.004988; // ln(1.005) → 0.5% k-width

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

/// Per-day bar count + median. Output of [`count_bars_per_day_from_prices`].
///
/// LEAN (2026-06-10): `mean`/`mad` fields + the `score`/`modal_mass`/
/// `structure_ok` methods are removed — the direct solver uses the MEDIAN only
/// (storms-included, gap-quarantined), with no search-score and no
/// modal-mass/dispersion structure guard.
#[derive(Debug, Clone)]
pub struct DailyBpdStats {
    /// Per-day brick counts, ordered by UTC date (consecutive days, gap-filled with 0).
    pub bricks_per_day: Vec<u64>,
    /// Median bricks per day (robust to regime tails / storm days).
    pub median: f64,
    /// Number of CLEAN (non-gap) days scored. `0` ⇒ dead/degenerate window.
    pub days: usize,
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
/// bricks into per-UTC-day counts, and return the storms-included MEDIAN.
///
/// The ONE counter the solver calls. Drives `RenkoGenerator` over the FULL-TICK
/// path (the calibrate==apply granularity-parity guarantee); drops gap
/// (count==0) days via [`quarantine_clean_days`]; KEEPS storm days (the median
/// absorbs them — operator spec).
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
                bricks_per_day: Vec::new(),
                median: 0.0,
                days: 0,
            };
        }
    };

    if to_ts <= from_ts {
        return DailyBpdStats {
            bricks_per_day: Vec::new(),
            median: 0.0,
            days: 0,
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
        // Dead window — insufficient clean days. days=0 ⇒ the solver treats it
        // as unfittable (no fit on contaminated data).
        return DailyBpdStats {
            bricks_per_day: per_day,
            median: 0.0,
            days: 0,
        };
    };

    let mut sorted = clean.clone();
    sorted.sort_unstable();
    let median = median_sorted(&sorted);

    // `days` reflects the count of CLEAN days actually scored, so a degenerate
    // (gappy) window surfaces as days==0.
    DailyBpdStats {
        bricks_per_day: per_day,
        median,
        days: clean.len(),
    }
}

/// Direct SCALE-TO-TARGET k solver (2026-06-10, `docs/renko-methodology.md` §4).
/// Replaces `calibrate_mtf_walkforward` + the multi-fold log-k bisection.
///
/// Objective: MEDIAN over the trailing `cal.rolling_window_days` of per-UTC-day
/// brick counts (gap days DROPPED, STORM days KEPT) == `target_bpd`, counted on
/// the FULL-TICK path via [`count_bars_per_day_from_prices`].
///
/// `bpd(k)` is monotone-decreasing in k but a STAIRCASE (snap_to_25_grid lattice
/// × 30-min recompute × min_pct floor), NOT exact `1/k`. So the analytic step
/// `k1 = k0·(m0/target)` is a high-quality WARM START — the point the old
/// bisection converged toward — refined by re-counts, a bounded bracket
/// fallback for the wrong-tread case, and a ±1-rung snap probe.
///
/// Algorithm (typical 2-3 brick counts; worst 7-9):
///  1. `k0 = clamp(prior_k.unwrap_or(0.5), K_FLOOR, K_MAX_SAFETY)`.
///  2. `m0 = median_bpd(k0)`  (count #1).
///  3. ZERO/CLIFF GUARD: if `days==0 || m0 < EPS_BPD` → fall straight to the
///     bounded bracket search from scratch (do NOT divide by ~0). Preserves the
///     `bars≈0 → 100%-err` cliff lock.
///  4. `k1 = clamp(k0·(m0/target), …)`; `m1 = median_bpd(k1)` (count #2); accept
///     k1 if `|m1/target−1| ≤ accept_tol`.
///  5. else one more scale step `k2 = clamp(k1·(m1/target), …)`; `m2` (count #3);
///     accept k2 if within tol.
///  6. else BOUNDED log-k BRACKET fallback (≤ `bracket_max_iters` iters): seed
///     from {k0,k1,k2}, expand the UPPER side (double up to K_MAX_SAFETY) until
///     it brackets the crossing; lower side is K_FLOOR (never expanded down);
///     bisect, STOP ON BRACKET WIDTH (`K_BRACKET_LN_EPS`).
///  7. ±1-RUNG snap probe: count the two adjacent k that shift the snapped brick
///     ±1 grid increment from `k_star`; keep the rung with min `|median/target−1|`.
///  8. K_FLOOR clamp + log `achieved_err`. `accept_tol` is ADVISORY on the
///     achieved-err warn — coarse-grid/high-price tickers (BTC) have a structural
///     rung floor that can exceed 5%; WARN, still return the best k.
///  9. min_pct CLAMPED-FRACTION degeneracy check: if > `MIN_PCT_CLAMP_MAX_FRAC`
///     of the σ cache is min_pct-bound at `k_star`, the k is unresponsive → drop
///     (return 0.0 → caller routes to `renko_k_overrides`).
///
/// Degenerate-reject (return 0.0): empty window; median below target even at the
/// K_FLOOR overshoot side (too flat to reach target at any k); no crossing below
/// K_MAX_SAFETY; k parked at the K_FLOOR edge; min_pct-clamp fraction over floor.
pub fn scale_to_target_k<S: VolSource + ?Sized>(
    prices: &[(i64, f64)],
    cal: &CalibrationConfig,
    base: &RenkoConfig,
    vol_source: &S,
    vol_config: &VolConfig,
    sigma_cache: &[f64],
    target_bpd: f64,
    prior_k: Option<f32>,
) -> f32 {
    let first = prices.first().map(|p| p.0).unwrap_or(0);
    let last = prices.last().map(|p| p.0).unwrap_or(0);
    if last <= first || target_bpd <= 0.0 {
        warn!(
            n = prices.len(),
            first,
            last,
            target_bpd,
            "scale_to_target_k early-return: degenerate window or non-positive target"
        );
        return 0.0;
    }

    // Median σ_pct over the σ cache — a discriminating diagnostic shared by the
    // degenerate-reject log lines (too-flat / no-crossing / min_pct-clamp).
    let median_sigma_pct = {
        let mut s: Vec<f64> = sigma_cache
            .iter()
            .copied()
            .filter(|x| x.is_finite())
            .collect();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if s.is_empty() {
            f64::NAN
        } else {
            s[s.len() / 2]
        }
    };

    // The ONE counter the solver calls: storms-included, gap-quarantined median
    // over the FULL window [first, last], on the full-tick path.
    let median_bpd = |k: f64| -> DailyBpdStats {
        count_bars_per_day_from_prices(
            prices,
            &RenkoConfig {
                multiplier: k as f32,
                min_pct: base.min_pct,
            },
            vol_source,
            vol_config,
            sigma_cache,
            first,
            last,
        )
    };

    let k_lo = cal.mult_bounds[0]; // == K_FLOOR (asserted by assert_bounds_consistent)
    let k_hi_seed = cal.mult_bounds[1];

    // ── Step 1-2: warm start ─────────────────────────────────────────────────
    let k0 = (prior_k.map(|k| k as f64).unwrap_or(0.5)).clamp(K_FLOOR, K_MAX_SAFETY);
    let s0 = median_bpd(k0); // count #1
    let m0 = s0.median;

    // ── Step 3: ZERO/CLIFF GUARD ─────────────────────────────────────────────
    // bars≈0 (days==0 OR median < EPS_BPD) → do NOT divide by ~0; bracket from
    // scratch. (The old calibrate_mtf n==0 cliff-lock, preserved.)
    let zero_or_cliff = s0.days == 0 || m0 < EPS_BPD;

    // Candidate set for the bracket-fallback seed (filled by the scale steps).
    let mut k1_opt: Option<f64> = None;
    let mut k2_opt: Option<f64> = None;
    let mut k_star: Option<f64> = None;

    if !zero_or_cliff {
        // ── Step 4: first scale step ─────────────────────────────────────────
        let k1 = (k0 * (m0 / target_bpd)).clamp(K_FLOOR, K_MAX_SAFETY);
        let s1 = median_bpd(k1); // count #2
        k1_opt = Some(k1);
        if s1.days != 0 && (s1.median / target_bpd - 1.0).abs() <= cal.accept_tol {
            k_star = Some(k1);
        } else if s1.days != 0 && s1.median >= EPS_BPD {
            // ── Step 5: second scale step ────────────────────────────────────
            let k2 = (k1 * (s1.median / target_bpd)).clamp(K_FLOOR, K_MAX_SAFETY);
            let s2 = median_bpd(k2); // count #3
            k2_opt = Some(k2);
            if s2.days != 0 && (s2.median / target_bpd - 1.0).abs() <= cal.accept_tol {
                k_star = Some(k2);
            }
        }
    }

    // ── Step 6: BOUNDED log-k BRACKET fallback (the wrong-tread safety net) ───
    // Reached when the warm start missed (or the zero/cliff guard fired). Reuses
    // the proven bracket-by-width logic: lower side fixed at K_FLOOR (never
    // expanded down), upper side doubles up to K_MAX_SAFETY; bisect, stop on
    // bracket width.
    if k_star.is_none() {
        let mut lo = k_lo.ln();
        // Seed the upper bracket from the largest scale-step candidate (or the
        // config hint) so a storming ticker starts above its true crossing.
        let mut hi = k1_opt
            .into_iter()
            .chain(k2_opt)
            .fold(k_hi_seed, f64::max)
            .max(k_lo * 2.0)
            .ln();

        let g_lo = median_bpd(lo.exp());
        let mut g_hi = median_bpd(hi.exp());

        // LOWER side (floor preserved): the smallest k (K_FLOOR) must OVERSHOOT
        // target. If even the smallest brick yields too few bricks/day the asset
        // is too flat to reach target — drop (route to a per-pair override).
        if g_lo.days == 0 || !(g_lo.median - target_bpd > 0.0) {
            warn!(
                lo_median = g_lo.median, target_bpd, mult_lo = k_lo,
                median_sigma_pct, lo_days = g_lo.days,
                "GUARD reject: median at K_FLOOR ({:.1}) ≤ target ({:.1}) — too flat to reach target at any k (floor preserved) — returning 0.0",
                g_lo.median, target_bpd
            );
            return 0.0;
        }

        // UPPER side (NO ceiling): double k_hi until the median drops below
        // target OR the numeric safety cap is hit.
        while g_hi.days != 0 && g_hi.median - target_bpd >= 0.0 && hi.exp() < K_MAX_SAFETY {
            let new_hi_k = (hi.exp() * 2.0).min(K_MAX_SAFETY);
            hi = new_hi_k.ln();
            g_hi = median_bpd(hi.exp());
        }
        // A zero-brick hi window (days==0 ⇒ median 0) is a VALID upper bracket:
        // m(hi)=0 < target, and the K_FLOOR overshoot was already verified
        // above — the crossing lies inside (lo, hi] and bisection finds it.
        // Slow crosses (ETH/BTC et al.) legitimately emit 0 bricks/365d at the
        // mult_bounds[1] seed; rejecting them as "DEAD" was the 2026-06-09
        // synth-calibration wipeout. The only genuine no-crossing case is a
        // median still ≥ target after expansion capped at K_MAX_SAFETY.
        if g_hi.days != 0 && !(g_hi.median - target_bpd < 0.0) {
            warn!(
                hi_median = g_hi.median, hi_k = hi.exp(), target_bpd,
                k_max_safety = K_MAX_SAFETY, median_sigma_pct, hi_days = g_hi.days,
                "GUARD reject: at upper bracket k={:.4} median={:.1} still ≥ target {:.1} (no crossing below K_MAX_SAFETY) — returning 0.0",
                hi.exp(), g_hi.median, target_bpd
            );
            return 0.0;
        }

        for _ in 0..cal.bracket_max_iters {
            if (hi - lo) < K_BRACKET_LN_EPS {
                break;
            }
            let mid = (lo + hi) / 2.0;
            let m = median_bpd(mid.exp());
            // median > target ⇒ k too small ⇒ move lo up; else move hi down.
            if m.median > target_bpd {
                lo = mid;
            } else {
                hi = mid;
            }
        }

        // Two-sided bracket-rung selection: lo is the last k whose median was >
        // target (overshoot rung); hi the last ≤ target (undershoot rung). Keep
        // whichever lands closer.
        let small_k = lo.exp();
        let large_k = hi.exp();
        let s_small = median_bpd(small_k);
        let s_large = median_bpd(large_k);
        let err_small = if s_small.days == 0 {
            f64::INFINITY
        } else {
            (s_small.median / target_bpd - 1.0).abs()
        };
        let err_large = if s_large.days == 0 {
            f64::INFINITY
        } else {
            (s_large.median / target_bpd - 1.0).abs()
        };
        k_star = Some(if err_small <= err_large {
            small_k
        } else {
            large_k
        });
    }

    let mut k_star = k_star.expect("k_star set by warm-start or bracket fallback");

    // ── Step 7: ±1-rung snap probe ───────────────────────────────────────────
    // Probe the two adjacent k that shift the snapped brick by one grid
    // increment; keep the rung with min |median/target−1|. K_BRACKET_LN_EPS is
    // the proven 0.5%-k step that moves one snap rung at production grids.
    {
        let step = K_BRACKET_LN_EPS.exp(); // ×1.005
        let probe = |k: f64| -> f64 {
            let s = median_bpd(k);
            if s.days == 0 {
                f64::INFINITY
            } else {
                (s.median / target_bpd - 1.0).abs()
            }
        };
        let center = probe(k_star);
        let up_k = (k_star * step).min(K_MAX_SAFETY);
        let dn_k = (k_star / step).max(K_FLOOR);
        let up = probe(up_k);
        let dn = probe(dn_k);
        if up < center && up <= dn {
            k_star = up_k;
        } else if dn < center {
            k_star = dn_k;
        }
    }

    // ── Step 8: K_FLOOR clamp + achieved-err (ADVISORY accept-tol) ────────────
    k_star = k_star.max(K_FLOOR);
    let full_stats = median_bpd(k_star);
    if full_stats.days == 0 {
        warn!(
            k_star, full_median = full_stats.median, target_bpd, median_sigma_pct,
            "GUARD reject: selected k had no scorable clean day (window quarantine emptied) — returning 0.0"
        );
        return 0.0;
    }
    let achieved_err = (full_stats.median / target_bpd - 1.0).abs();
    info!(
        k_star,
        achieved_err_pct = achieved_err * 100.0,
        full_median = full_stats.median,
        target_bpd,
        accept_tol_pct = cal.accept_tol * 100.0,
        "scale_to_target_k result (achieved_err = structural floor)"
    );
    if achieved_err > cal.accept_tol {
        // ADVISORY (operator: "k must be the PRODUCT of calibration, never give
        // up to 0"). Coarse-grid / high-price tickers (BTC) have a structural
        // rung floor that can exceed accept_tol — WARN, still return k_star.
        warn!(
            k_star, full_median = full_stats.median, target_bpd,
            achieved_err_pct = achieved_err * 100.0, accept_tol_pct = cal.accept_tol * 100.0,
            "accept gate (advisory) — best-achievable rung {:.1}% off target — accepting closest k (snap-grid structural floor)",
            achieved_err * 100.0
        );
    }

    // Lower-edge clamp detector: a k parked at the K_FLOOR bracket edge is a
    // degenerate-σ / unreachable-target artifact (the floor preservation side).
    if (k_star - k_lo).abs() / k_lo < 0.01 {
        warn!(
            k_star, bound = "lower", mult_lo = k_lo,
            full_median = full_stats.median, target_bpd,
            "GUARD reject: k_star parked at K_FLOOR bracket edge (degenerate-σ / unreachable-target) — returning 0.0"
        );
        return 0.0;
    }

    // ── Step 9: min_pct CLAMPED-FRACTION degeneracy check ────────────────────
    // When k_star·σ_pct ≤ min_pct the brick floors at min_pct and is INDEPENDENT
    // of k (bpd flat). The MEDIAN is robust to exactly the low-σ tail that
    // clamps, so it HIDES per-bin clamping — hence the per-bin fraction check
    // (NOT a median-σ comparison). If > MIN_PCT_CLAMP_MAX_FRAC of the window is
    // clamped, the k is unresponsive → drop → per-ticker renko_k_overrides.
    let clamp_frac = min_pct_clamped_fraction(
        prices,
        k_star,
        base.min_pct,
        vol_source,
        sigma_cache,
        first,
        last,
    );
    if clamp_frac > MIN_PCT_CLAMP_MAX_FRAC {
        warn!(
            k_star, min_pct = base.min_pct,
            clamp_frac_pct = clamp_frac * 100.0, clamp_max_pct = MIN_PCT_CLAMP_MAX_FRAC * 100.0,
            median_sigma_pct, k_star_x_median_sigma = k_star * median_sigma_pct,
            full_median = full_stats.median,
            "GUARD reject: brick min_pct-clamped over {:.1}% of window (> {:.0}% ceiling); k unresponsive — failing ticker for per-ticker override",
            clamp_frac * 100.0, MIN_PCT_CLAMP_MAX_FRAC * 100.0
        );
        return 0.0;
    }

    k_star as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Quarantine (gap-only; storms KEPT) ───────────────────────────────────

    #[test]
    fn quarantine_drops_gaps_keeps_storms() {
        // Drops ONLY gap (0) days; high-bpd STORM days are KEPT (median absorbs).
        // 10 days: 8 ~100/day, one gap (0), one storm (1000). n_days=10 →
        // min_clean = max(3,5) = 5; 9 non-gap days survive.
        let per_day = vec![100, 98, 102, 0, 101, 99, 1000, 100, 103, 97];
        let clean =
            quarantine_clean_days(&per_day, per_day.len()).expect("9 non-gap days >= min_clean(5)");
        assert_eq!(clean.len(), 9, "only gap(0) excluded; storm(1000) KEPT");
        assert!(clean.contains(&1000), "storm day retained in objective");
        let mut s = clean.clone();
        s.sort_unstable();
        assert_eq!(median_sorted(&s), 100.0, "median absorbs the storm day");
    }

    #[test]
    fn quarantine_dead_window_when_too_gappy() {
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
    fn quarantine_objective_is_storms_included_median() {
        // STORM-robust median objective: gap-only quarantine keeps all 12
        // (no gaps), storms included.
        let per_day = vec![
            118u64, 122, 900, 119, 950, 121, 880, 120, 910, 123, 117, 124,
        ];
        let n = per_day.len();
        let clean = quarantine_clean_days(&per_day, n).expect("non-empty");
        assert_eq!(clean.len(), 12, "no gap days ⇒ all kept, storms included");
        let mut cs = clean.clone();
        cs.sort_unstable();
        let kept_median = median_sorted(&cs);
        let mut all = per_day.clone();
        all.sort_unstable();
        assert_eq!(
            kept_median,
            median_sorted(&all),
            "kept median == full-set median"
        );
        // A legacy [median/3, median*3] clip would have dropped the 4 storm days
        // → a strictly different (calm-only) median. The fix keeps storms.
        let (clo, chi) = (kept_median / 3.0, kept_median * 3.0);
        let clipped: Vec<u64> = per_day
            .iter()
            .copied()
            .filter(|&b| (b as f64) >= clo && (b as f64) <= chi)
            .collect();
        assert!(
            clipped.len() < per_day.len(),
            "legacy clip drops the storm days"
        );
    }

    // ── Synthetic price paths ────────────────────────────────────────────────

    /// Constant-σ VolSource: brick = `max(k·σ, min_pct)·price`, so k alone drives
    /// bpd on a fixed path.
    struct ConstSigma(f64);
    impl VolSource for ConstSigma {
        fn len(&self) -> usize {
            1
        }
        fn sigma_pct(&self, _i: usize) -> f64 {
            self.0
        }
        fn find_index_for_mts(&self, _mts: u64) -> usize {
            0
        }
    }

    /// Deterministic GBM-ish full-tick path (xorshift64* LCG, reproducible).
    fn synth_gbm_path(
        n_days: usize,
        ticks_per_day: usize,
        sigma_step: f64,
        seed: u64,
    ) -> Vec<(i64, f64)> {
        let dt_ms = (MS_PER_DAY / ticks_per_day as i64).max(1);
        let mut state = seed;
        let mut next_u = || {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            ((state.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64) / ((1u64 << 53) as f64)
        };
        let total = n_days * ticks_per_day;
        let mut out = Vec::with_capacity(total);
        let mut price = 100.0f64;
        let t0: i64 = 1_700_000_000_000;
        for i in 0..total {
            let z = next_u() - 0.5;
            price *= (sigma_step * z).exp();
            out.push((t0 + i as i64 * dt_ms, price));
        }
        out
    }

    /// Deterministic BIMODAL path: storm day every `storm_every`-th day.
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
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            ((state.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64) / ((1u64 << 53) as f64)
        };
        let mut out = Vec::with_capacity(n_days * ticks_per_day);
        let mut price = 100.0f64;
        let t0: i64 = 1_700_000_000_000;
        for d in 0..n_days {
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
    /// granularity) — the granularity-gap negative control.
    fn downsample_1min_last(prices: &[(i64, f64)]) -> Vec<(i64, f64)> {
        use std::collections::BTreeMap;
        let mut m: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
        for &(ts, mid) in prices {
            let bucket = (ts / 60_000) * 60_000;
            let e = m.entry(bucket).or_insert((ts, mid));
            if ts >= e.0 {
                *e = (ts, mid);
            }
        }
        m.into_iter().map(|(_, (ts, mid))| (ts, mid)).collect()
    }

    fn vol_cfg() -> VolConfig {
        VolConfig {
            ema_period: 1,
            sigma_blend_windows_days: vec![1],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 1,
            recompute_cooldown_ms: 0,
            ..VolConfig::default()
        }
    }

    fn cal(target_bpd: f64) -> CalibrationConfig {
        CalibrationConfig {
            target_bpd,
            rolling_window_days: 365,
            min_window_days: 7,
            bracket_max_iters: 12,
            accept_tol: 0.05,
            mult_bounds: [0.05, 4.0],
        }
    }

    // ── calibrate == apply granularity parity (KEPT) ─────────────────────────

    /// REGRESSION GUARD (brick-storm RCA): k fit on the FULL-TICK path, applied
    /// via the SAME RenkoGenerator over the SAME path, lands within accept_tol;
    /// the 1-min downsample under-counts (negative control).
    #[test]
    fn full_tick_calibrate_matches_full_tick_apply() {
        let target_bpd = 300.0;
        let prices = synth_gbm_path(40, 4000, 0.006, 0xC0FFEE);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };

        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            None,
        );
        assert!(k > 0.0, "calibration must produce a valid k (got {k})");

        let applied = RenkoConfig {
            multiplier: k,
            min_pct: 0.0,
        };
        let stats =
            count_bars_per_day_from_prices(&prices, &applied, &vol, &vc, &sigma_cache, first, last);
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "full-tick apply median {:.1} within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median,
            RENKO_BPD_ACCEPT_TOL * 100.0,
            target_bpd,
            rel_err
        );

        let coarse = downsample_1min_last(&prices);
        let coarse_stats =
            count_bars_per_day_from_prices(&coarse, &applied, &vol, &vc, &sigma_cache, first, last);
        assert!(
            coarse_stats.median < stats.median,
            "1-min downsample must undercount bricks vs full-tick (coarse {:.1} < full {:.1})",
            coarse_stats.median,
            stats.median
        );
    }

    /// Storms-included median: a path with many calm days + a substantial storm
    /// set calibrates to the k where the storms-INCLUDED full median == target
    /// (must NOT be rejected). The reported median is the unclipped one.
    #[test]
    fn storming_ticker_calibrates_on_full_history_median() {
        let target_bpd = 300.0;
        let prices = synth_bimodal_path(40, 4000, 0.0006, 0.0035, 2, 0xB1B0DA1);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };

        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            None,
        );
        assert!(
            k > 0.0,
            "storming ticker must calibrate, NOT return 0.0 (got {k})"
        );
        assert!((k as f64) >= K_FLOOR, "k ≥ K_FLOOR (got {k})");

        let full = count_bars_per_day_from_prices(
            &prices,
            &RenkoConfig {
                multiplier: k,
                min_pct: 0.0,
            },
            &vol,
            &vc,
            &sigma_cache,
            first,
            last,
        );
        let rel_err = (full.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "storms-included full median {:.1} within tol? rel_err={:.3}",
            full.median,
            rel_err
        );
        // Reported median is the storms-INCLUDED (unclipped) median.
        let nonzero: Vec<u64> = full
            .bricks_per_day
            .iter()
            .copied()
            .filter(|&b| b > 0)
            .collect();
        let mut nz = nonzero.clone();
        nz.sort_unstable();
        assert_eq!(
            full.median,
            median_sorted(&nz),
            "reported median is storms-included"
        );
    }

    // ── (a) NEW: convergence in ≤3 counts within accept_tol ──────────────────

    /// `scale_to_target_k` lands within `accept_tol` via the warm-start scale
    /// steps (≤3 full-history brick counts) on a synthetic GBM path — the direct
    /// solver does NOT need the bracket fallback on a well-behaved path.
    #[test]
    fn scale_to_target_converges_within_tol_on_gbm() {
        let target_bpd = 300.0;
        let prices = synth_gbm_path(40, 4000, 0.006, 0xC0FFEE);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };

        // Seed prior_k near the true k so the warm start converges immediately.
        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            Some(0.5),
        );
        assert!(k > 0.0 && (k as f64) >= K_FLOOR, "valid k (got {k})");
        let stats = count_bars_per_day_from_prices(
            &prices,
            &RenkoConfig {
                multiplier: k,
                min_pct: 0.0,
            },
            &vol,
            &vc,
            &sigma_cache,
            first,
            last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "converged median {:.1} within {:.0}% of target {:.0}? rel_err={:.3}",
            stats.median,
            RENKO_BPD_ACCEPT_TOL * 100.0,
            target_bpd,
            rel_err
        );

        // No prior (k0=0.5 default) must also converge — exercises the cold path.
        let k_cold = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            None,
        );
        let cold = count_bars_per_day_from_prices(
            &prices,
            &RenkoConfig {
                multiplier: k_cold,
                min_pct: 0.0,
            },
            &vol,
            &vc,
            &sigma_cache,
            first,
            last,
        );
        assert!(
            (cold.median / target_bpd - 1.0).abs() <= RENKO_BPD_ACCEPT_TOL,
            "cold-start median {:.1} within tol",
            cold.median
        );
    }

    // ── (b) NEW: 1/k staircase / homogeneity departure ───────────────────────

    /// The continuous relation `bpd ∝ 1/k` holds only above the min_pct floor and
    /// ignoring the snap_to_25_grid lattice. On a COARSE-grid (high-price) level
    /// the lattice quantizes brick_size, so `median_bpd(2·k0) ≠ median_bpd(k0)/2`
    /// exactly. This documents the staircase departure AND validates that the
    /// warm-start + fallback solver still converges where a pure analytic step
    /// would not.
    #[test]
    fn one_over_k_staircase_departs_then_solver_converges() {
        let target_bpd = 50.0;
        // High price level (~84k) so snap_to_25_grid rungs are RELATIVELY coarse.
        let mut prices = synth_gbm_path(60, 2000, 0.004, 0xBADC0DE);
        for p in prices.iter_mut() {
            p.1 *= 840.0;
        } // ~100 → ~84k
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0001,
        };

        let mb = |k: f64| {
            count_bars_per_day_from_prices(
                &prices,
                &RenkoConfig {
                    multiplier: k as f32,
                    min_pct: 0.0001,
                },
                &vol,
                &vc,
                &sigma_cache,
                first,
                last,
            )
            .median
        };
        let k0 = 0.5_f64;
        let m_k0 = mb(k0);
        let m_2k0 = mb(2.0 * k0);
        assert!(m_k0 > 0.0 && m_2k0 > 0.0, "non-degenerate medians");
        // Departure from exact 1/k: the pure-analytic prediction is m_k0/2;
        // on the coarse grid the realized median differs by a measurable amount.
        let analytic = m_k0 / 2.0;
        let departure = (m_2k0 - analytic).abs() / analytic.max(1.0);
        assert!(
            departure > 0.0,
            "staircase departs from exact 1/k (m(2k0)={} vs analytic {})",
            m_2k0,
            analytic
        );

        // Despite the staircase, the solver still lands the best achievable rung
        // (k>0); achieved_err is the structural floor (may exceed accept_tol).
        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            None,
        );
        assert!(
            k > 0.0 && (k as f64) >= K_FLOOR,
            "warm-start + fallback converges to a valid rung k (got {k})"
        );
        // The returned k is the closest rung: neither ±1 grid neighbour beats it.
        let err = |kk: f64| (mb(kk) / target_bpd - 1.0).abs();
        let step = K_BRACKET_LN_EPS.exp();
        let e0 = err(k as f64);
        assert!(
            e0 <= err(k as f64 * step) + 1e-9 && e0 <= err(k as f64 / step) + 1e-9,
            "returned k sits on the optimal rung (e0={e0})"
        );
    }

    // ── (c) NEW: zero/cliff guard ────────────────────────────────────────────

    /// A thin near-empty window (median ≈ 0 at the warm-start k) must NOT divide
    /// by ~0 and push k toward K_MAX_SAFETY — the zero/cliff guard routes it to
    /// the bounded bracket, which (lower side fixed at K_FLOOR, never expanded
    /// down) either finds a valid rung or rejects to 0.0. It must NEVER return a
    /// runaway near-K_MAX_SAFETY k.
    #[test]
    fn zero_cliff_guard_does_not_runaway_to_k_max() {
        let target_bpd = 300.0;
        // Near-flat path: barely moves ⇒ at the warm-start k0=0.5 (brick 0.5%)
        // the median is ~0 (cliff). The guard must NOT scale k up by m0/target≈0.
        let prices = synth_gbm_path(40, 4000, 0.00002, 0xDEAD);
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };

        // Seed a LARGE prior so a naive m0/target scale (m0≈0) would collapse k —
        // the guard instead brackets from K_FLOOR upward.
        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            Some(2.0),
        );
        // Too-flat to reach target even at K_FLOOR ⇒ legitimately 0.0 (floor
        // preserved). The KEY invariant: k is NEVER a runaway near K_MAX_SAFETY.
        assert!(
            k == 0.0 || ((k as f64) >= K_FLOOR && (k as f64) < 1000.0),
            "zero/cliff window must drop (0.0) or return a sane k — never runaway to K_MAX_SAFETY (got {k})"
        );
        assert!(
            (k as f64) < K_MAX_SAFETY as f64 * 0.001,
            "k must not approach K_MAX_SAFETY ({k})"
        );
    }

    /// FLOOR PRESERVED: a too-quiet series whose median is below target even at
    /// K_FLOOR cannot reach target — returns 0.0 (no downward expansion).
    #[test]
    fn too_quiet_series_below_target_at_floor_returns_zero() {
        let target_bpd = 300.0;
        let prices = synth_gbm_path(40, 4000, 0.00005, 0xDEAD);
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };
        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            None,
        );
        assert_eq!(
            k, 0.0,
            "too-quiet series (median < target even at K_FLOOR) → 0.0 (floor preserved) — got {k}"
        );
    }

    // ── No upper cap: storming series auto-expands upward ────────────────────

    #[test]
    fn storming_series_above_initial_bracket_auto_expands_upward() {
        let target_bpd = 300.0;
        let prices = synth_gbm_path(40, 4000, 0.012, 0x5704);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        // Deliberately small initial upper bracket so the true crossing is above
        // it ⇒ the bracket fallback must double k_hi upward. No prior, and the
        // warm start lands below tol-fail so the bracket engages.
        let mut c = cal(target_bpd);
        c.mult_bounds = [0.05, 0.5];
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };
        let k = scale_to_target_k(
            &prices,
            &c,
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            Some(0.05),
        );
        assert!(
            k > 0.0,
            "storming series must auto-expand and calibrate (got {k})"
        );
        let stats = count_bars_per_day_from_prices(
            &prices,
            &RenkoConfig {
                multiplier: k,
                min_pct: 0.0,
            },
            &vol,
            &vc,
            &sigma_cache,
            first,
            last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "auto-expanded k median {:.1} within tol? rel_err={:.3}",
            stats.median,
            rel_err
        );
    }

    /// Dead hi-seed bracket: a slow cross emits ZERO bricks/day at the
    /// mult_bounds[1] hi seed (days==0 ⇒ median 0). That is a VALID upper
    /// bracket — m(hi)=0 < target while K_FLOOR overshoots — so the solver
    /// must bisect into the crossing, not reject as "DEAD hi-window".
    /// Regression for the 2026-06-09 synth wipeout (ETH/BTC, SOL/BTC,
    /// BNB/BTC, SOL/ETH all returned 0.0 from a bisectable window).
    #[test]
    fn dead_hi_seed_is_valid_bracket_and_calibrates() {
        let target_bpd = 100.0;
        let prices = synth_gbm_path(40, 4000, 0.0008, 0xC205);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };
        // Prior k parked at the (dead) hi seed: m0 has days==0 → zero/cliff
        // guard → bracket fallback with g_hi dead at mult_bounds[1]=4.0.
        let k = scale_to_target_k(
            &prices,
            &cal(target_bpd),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            Some(4.0),
        );
        assert!(
            k > 0.0,
            "dead-hi-seed series must bisect, not reject (got {k})"
        );
        let stats = count_bars_per_day_from_prices(
            &prices,
            &RenkoConfig {
                multiplier: k,
                min_pct: 0.0,
            },
            &vol,
            &vc,
            &sigma_cache,
            first,
            last,
        );
        let rel_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            rel_err <= RENKO_BPD_ACCEPT_TOL,
            "bisected k median {:.1} off target, rel_err={:.3}",
            stats.median,
            rel_err
        );
    }

    /// ADVISORY accept-tol: when the snap-grid staircase leaves the best rung
    /// structurally OFF target by >accept_tol, the solver STILL returns the
    /// closest k (k>0) — accept_tol is warn-only, NOT a drop gate.
    #[test]
    fn best_rung_off_target_accepts_closest_k_not_zero() {
        let target_bpd = 5.5; // between integer rungs 5 and 6 → ~9% off either way
        let prices = synth_gbm_path(40, 400, 0.0025, 0xFEED);
        let first = prices.first().unwrap().0;
        let last = prices.last().unwrap().0;
        let vol = ConstSigma(0.01);
        let sigma_cache = vec![0.01];
        let vc = vol_cfg();
        let mut c = cal(target_bpd);
        c.mult_bounds = [0.05, 0.65];
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.0,
        };
        let k = scale_to_target_k(
            &prices,
            &c,
            &base,
            &vol,
            &vc,
            &sigma_cache,
            target_bpd,
            None,
        );
        assert!(
            k > 0.0,
            "best rung off-target by >5% must return closest k, NOT 0.0 (got {k})"
        );
        assert!((k as f64) >= K_FLOOR, "k ≥ K_FLOOR (got {k})");
        let stats = count_bars_per_day_from_prices(
            &prices,
            &RenkoConfig {
                multiplier: k,
                min_pct: 0.0,
            },
            &vol,
            &vc,
            &sigma_cache,
            first,
            last,
        );
        let achieved_err = (stats.median / target_bpd - 1.0).abs();
        assert!(
            achieved_err > RENKO_BPD_ACCEPT_TOL,
            "must exercise the >5%-off advisory branch (err {:.3})",
            achieved_err
        );
    }

    #[test]
    fn bpd_accept_gate_threshold_math() {
        let target = 300.0;
        assert!((33.0_f64 / target - 1.0).abs() > RENKO_BPD_ACCEPT_TOL);
        assert!((290.0_f64 / target - 1.0).abs() <= RENKO_BPD_ACCEPT_TOL);
        assert!((330.0_f64 / target - 1.0).abs() > RENKO_BPD_ACCEPT_TOL);
    }

    // ── min_pct clamp-detector (KEPT) ────────────────────────────────────────

    /// Per-tick σ VolSource: `sigma_cache[i]` indexed by tick ORDER.
    struct PerTickSigma {
        mts: Vec<u64>,
    }
    impl VolSource for PerTickSigma {
        fn len(&self) -> usize {
            self.mts.len()
        }
        fn sigma_pct(&self, i: usize) -> f64 {
            i as f64
        }
        fn find_index_for_mts(&self, mts: u64) -> usize {
            self.mts.partition_point(|&m| m <= mts).saturating_sub(1)
        }
    }

    #[test]
    fn min_pct_clamped_fraction_detector_arithmetic() {
        // 10 ticks: first 8 LOW σ (0.001), last 2 HIGH σ (0.02).
        let sigma_cache = vec![
            0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.02, 0.02,
        ];
        let t0: i64 = 1_700_000_000_000;
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
        // k=0.5, min_pct=0.002 ⇒ clamp when σ ≤ 0.004 ⇒ 8/10 clamped.
        let frac = min_pct_clamped_fraction(&prices, 0.5, 0.002, &vol, &sigma_cache, first, last);
        assert!((frac - 0.8).abs() < 1e-9, "8/10 ticks clamped (got {frac})");
        assert!(frac > MIN_PCT_CLAMP_MAX_FRAC, "0.8 > 0.5 ⇒ gate rejects");
        // k=10 ⇒ clamp when σ ≤ 0.0002 ⇒ none ⇒ accept.
        let frac_ok =
            min_pct_clamped_fraction(&prices, 10.0, 0.002, &vol, &sigma_cache, first, last);
        assert_eq!(frac_ok, 0.0, "no tick clamped at large k ⇒ accept");
    }

    #[test]
    fn min_pct_clamped_series_is_rejected_end_to_end() {
        // A calm LOW-σ path with min_pct set above k·σ for the whole series:
        // every brick floors at min_pct ⇒ k unresponsive ⇒ must FAIL (0.0).
        let prices = synth_gbm_path(40, 4000, 0.0005, 0xC0FFEE);
        let vol = ConstSigma(0.0008);
        let sigma_cache = vec![0.0008];
        let vc = vol_cfg();
        // min_pct = 1% — far above k·σ for any k ∈ [0.05,4]·0.0008.
        let base = RenkoConfig {
            multiplier: 0.075,
            min_pct: 0.01,
        };
        let k = scale_to_target_k(
            &prices,
            &cal(300.0),
            &base,
            &vol,
            &vc,
            &sigma_cache,
            300.0,
            None,
        );
        assert_eq!(
            k, 0.0,
            "min_pct-dominated series must FAIL (→ per-ticker override), not ship a clamped k"
        );
    }
}
