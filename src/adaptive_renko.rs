//! Adaptive Renko bar generation with fixed multi-timeframe Parkinson volatility.
//!
//! Brick size formula:
//!   b_t = p_t * clamp(multiplier * σ_blend(t), min_pct, max_pct)
//!
//! where:
//!   - σ_blend = Σ w_i * winsorized_mean(σ_park over lookback_i)
//!   - w_i = inverse-variance weights (auto-computed, no tunable params)
//!   - σ_park = sqrt(ln(H/L)² / (4·ln2))  (Parkinson 1980, 30-min)
//!   - EMA smoothing with period 28 reduces noise
//!   - Winsorized mean clips at 5th/95th percentile for outlier robustness
//!
//! Lookbacks configurable via VolConfig (pipeline.yml)
//! Optimizable: multiplier (controls bars/day)
//!
//! Design priorities:
//! - Streaming: never hold all bars in RAM
//! - Simple: fixed lookbacks, auto-weighting, no over-fitting
//! - Correct: proper brick formation with continuity invariants

use crate::vol_bin::VolMmap;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Volatility calculation config (from pipeline.yml `vol` section).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolConfig {
    pub ema_period: usize,
    pub mtf_lookback_days: Vec<usize>,
    pub winsorize_pct: [f64; 2],
    pub winsorize_min_samples: usize,
}

impl Default for VolConfig {
    fn default() -> Self {
        Self {
            ema_period: 28,
            mtf_lookback_days: vec![30, 60, 180],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 5,
        }
    }
}

/// Adaptive Renko configuration.
///
/// `multiplier` controls bars/day via brick_pct = multiplier * σ_blend
/// (auto-calibrated via --target-bpd).
/// `min_pct` is a safety floor only (0.0001 = 0.01%).
/// Lookbacks and winsorize params come from VolConfig (pipeline.yml).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RenkoConfig {
    /// σ multiplier (controls bars/day, e.g., 0.05–0.30)
    pub multiplier: f32,

    /// Minimum brick size as percentage of price
    pub min_pct: f32,

    /// Maximum brick size as percentage of price
    pub max_pct: f32,

}

impl Default for RenkoConfig {
    fn default() -> Self {
        Self {
            multiplier: 0.075,
            min_pct: 0.001,  // 0.1%
            max_pct: 0.10,   // 10%
        }
    }
}

impl RenkoConfig {
    /// Create a unique identifier for this config (for file naming)
    pub fn id(&self) -> String {
        format!(
            "m{:04}_mp{:04}",
            (self.multiplier * 10000.0) as u16,
            (self.min_pct * 1_000_000.0) as u16,
        )
    }

    pub fn validate(&self) -> Result<()> {
        if !(0.001..=1.0).contains(&self.multiplier) {
            anyhow::bail!("multiplier out of range: {}", self.multiplier);
        }
        if self.min_pct >= self.max_pct {
            anyhow::bail!("min_pct must be < max_pct");
        }
        Ok(())
    }
}

/// Renko bar with all required fields for downstream enrichment.
#[derive(Debug, Clone, Copy)]
pub struct RenkoBar {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// +1 for up, -1 for down
    pub direction: i8,
    /// Brick size in absolute price units
    pub brick_size: f64,
    /// Number of ticks/data points that contributed
    pub tick_count: u32,
    /// Time elapsed since previous bar (milliseconds)
    pub duration_ms: u32,
}

/// Multi-timeframe Parkinson volatility blender.
///
/// Reads 30-min Parkinson σ = sqrt(ln(H/L)² / (4·ln2)) from vol mmap,
/// then blends across configurable lookback windows using
/// inverse-variance weighting and winsorized mean for robustness.
pub struct MtfParkinsonCalculator<'a> {
    vol_mmap: &'a VolMmap,
    vol_config: VolConfig,
    /// Reusable buffer to avoid per-call allocations in compute_sigma
    buf: Vec<f64>,
}

impl<'a> MtfParkinsonCalculator<'a> {
    pub fn new(vol_mmap: &'a VolMmap, vol_config: VolConfig) -> Self {
        Self { vol_mmap, vol_config, buf: Vec::new() }
    }

    /// Precompute all sigma values into a flat cache for O(1) lookup.
    /// Call this once, then use the returned Vec instead of compute_sigma()
    /// in hot loops (calibration). Avoids repeated sort-based winsorized mean.
    pub fn precompute_sigma_cache(&mut self) -> Vec<f64> {
        let n = self.vol_mmap.records().len();
        let mut cache = Vec::with_capacity(n);
        for i in 0..n {
            cache.push(self.compute_sigma(i));
        }
        cache
    }

    /// Find the record index for a given timestamp (binary search)
    fn find_index_for_ts(&self, timestamp_ms: i64) -> usize {
        let records = self.vol_mmap.records();
        let period_ts = (timestamp_ms / 1_800_000) * 1_800_000; // 30-min buckets
        match records.binary_search_by_key(&period_ts, |r| r.timestamp) {
            Ok(idx) => idx,
            Err(idx) => idx.min(records.len().saturating_sub(1)),
        }
    }

    /// Compute blended Parkinson σ for a given hour index.
    ///
    /// Returns the inverse-variance weighted blend of winsorized means
    /// across the three fixed lookback windows.
    pub fn compute_sigma(&mut self, hour_idx: usize) -> f64 {
        let records = self.vol_mmap.records();
        if hour_idx >= records.len() {
            return 0.01; // fallback
        }

        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;

        for &lookback_days in &self.vol_config.mtf_lookback_days {
            let lookback_periods = lookback_days * 48; // 48 half-hours per day
            let start_idx = hour_idx.saturating_sub(lookback_periods);

            if start_idx >= hour_idx || hour_idx - start_idx < 48 {
                continue; // not enough data for this timeframe
            }

            let window = &records[start_idx..=hour_idx];
            self.buf.clear();
            self.buf.extend(window.iter().map(|r| r.sigma_pct));

            let min_samples = self.vol_config.winsorize_min_samples;
            let [lo_pct, hi_pct] = self.vol_config.winsorize_pct;
            let (wmean, variance) = winsorized_mean_and_var_inplace(&mut self.buf, min_samples, lo_pct, hi_pct);
            if variance <= 0.0 || wmean <= 0.0 {
                continue;
            }

            // Inverse-variance weighting: w_i = 1 / Var(σ_i)
            let inv_var = 1.0 / variance;
            weighted_sum += inv_var * wmean;
            weight_sum += inv_var;
        }

        if weight_sum > 0.0 {
            weighted_sum / weight_sum
        } else {
            // Fallback to current hour's value
            records.get(hour_idx).map(|r| r.sigma_pct).unwrap_or(0.01)
        }
    }
}

/// Winsorized mean and variance, in-place.
///
/// Sorts the input buffer, clips to [lo_pct, hi_pct] percentile boundaries,
/// then computes mean and variance. Reuses the buffer to avoid allocation.
fn winsorized_mean_and_var_inplace(values: &mut [f64], min_samples: usize, lo_pct: f64, _hi_pct: f64) -> (f64, f64) {
    let n = values.len();
    if n < min_samples {
        let mean = values.iter().sum::<f64>() / n.max(1) as f64;
        let var = if n > 1 {
            values.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>() / (n - 1) as f64
        } else { 0.0 };
        return (mean, var);
    }

    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let lo = (n as f64 * lo_pct) as usize;
    let hi = n - 1 - lo; // symmetric clipping
    let lo_val = values[lo];
    let hi_val = values[hi];

    // Winsorize in-place: clamp to [lo_val, hi_val]
    let mut sum = 0.0;
    for v in values.iter_mut() {
        *v = v.clamp(lo_val, hi_val);
        sum += *v;
    }
    let mean = sum / n as f64;
    let var = values.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>() / (n - 1) as f64;

    (mean, var)
}

/// Snap a positive value to a 4-significant-figure grid with 2/5 multiples.
///
/// Algorithm:
///   1. Compute the unit at the 4th significant figure: `u = 10^(floor(log10(x)) - 3)`
///   2. Try two candidate grid steps: `2*u` and `5*u`
///   3. Snap to whichever grid gives less rounding error
///
/// This produces "visually satisfying" round numbers that don't overfit to noise.
/// Max rounding error is ~0.15% of value — well below Parkinson vol (1-3% per bar).
///
/// E.g. snap_to_25_grid(174.38)  = 174.4  (4sf, grid step 0.2)
///      snap_to_25_grid(347.83)  = 348.0  (4sf, grid step 0.5)
///      snap_to_25_grid(84234.0) = 84200  (4sf, grid step 200)
///      snap_to_25_grid(0.00347) = 0.003470 (4sf, grid step 0.000002)
///      snap_to_25_grid(12.34)   = 12.34  (4sf, grid step 0.02)
#[inline]
fn snap_to_25_grid(value: f64) -> f64 {
    if value <= 0.0 || !value.is_finite() {
        return value;
    }
    let d = value.log10().floor();
    let unit = 10f64.powf(d - 3.0);
    let step2 = unit * 2.0;
    let step5 = unit * 5.0;
    let snapped2 = (value / step2).round() * step2;
    let snapped5 = (value / step5).round() * step5;
    if (value - snapped2).abs() <= (value - snapped5).abs() {
        snapped2
    } else {
        snapped5
    }
}

/// Compute the grid step for a brick size snapped via `snap_to_25_grid`.
///
/// The grid step is the finer of `2*unit` and `5*unit` at 4 significant figures.
/// Since we always pick the closer snap, the effective grid is `2*unit`
/// (the finer of the two), which gives step sizes like:
///   brick=174.4 → step=0.2, brick=84200 → step=20, brick=12.34 → step=0.02
///
/// Brick boundaries snap to multiples of this step to ensure consistency
/// regardless of aggregation start time.
#[inline]
fn grid_step_for_brick(brick_size: f64) -> f64 {
    if brick_size <= 0.0 || !brick_size.is_finite() {
        return 1.0;
    }
    let d = brick_size.log10().floor();
    // Use the finer grid (2*unit) — all 5*unit multiples are also 2*unit multiples
    // at the boundaries that matter.
    10f64.powf(d - 3.0) * 2.0
}

/// Snap a price to the nearest multiple of `step`.
#[inline]
fn snap_to_grid(price: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return price;
    }
    (price / step).round() * step
}

/// Renko bar generator with adaptive brick sizing.
///
/// Brick size = price * clamp(multiplier * σ_blend, min_pct, max_pct).
/// Brick sizes are snapped to a 4-significant-figure grid with 2/5 multiples
/// (see `snap_to_25_grid`).  Brick boundaries snap to the implied grid step.
/// This ensures bricks land on consistent price levels regardless of
/// aggregation start time, and reduces curve-fitting noise.
///
/// No hysteresis — the MTF blending (30/60/180d inverse-variance weighted
/// winsorized means) already provides sufficient smoothing. Previous
/// hysteresis implementation caused a clamp-boundary discontinuity that
/// made bar count non-monotonic in min_pct.
pub struct RenkoGenerator<'a> {
    config: RenkoConfig,
    sigma_calc: MtfParkinsonCalculator<'a>,
    /// Precomputed sigma values for O(1) lookup (avoids sort-based winsorized mean).
    /// Set via `set_sigma_cache()` — used by calibration for speed.
    /// Borrowed to avoid cloning ~500K f64s per calibration iteration.
    sigma_cache: Option<&'a [f64]>,
    current_brick_size: f64,
    last_recompute_period: i64,
    // Persistent state for incremental tick feeding
    initialized: bool,
    last_close: f64,
    pending_high: f64,
    pending_low: f64,
    bar_start_ts: i64,
    tick_count: u32,
    n_bars: usize,
    total_duration_ms: u64,
}

impl<'a> RenkoGenerator<'a> {
    pub fn new(config: RenkoConfig, vol_mmap: &'a VolMmap, vol_config: VolConfig) -> Result<Self> {
        config.validate()?;
        let sigma_calc = MtfParkinsonCalculator::new(vol_mmap, vol_config);
        Ok(Self {
            config,
            sigma_calc,
            sigma_cache: None,
            current_brick_size: 0.0,
            last_recompute_period: i64::MIN,
            initialized: false,
            last_close: 0.0,
            pending_high: 0.0,
            pending_low: 0.0,
            bar_start_ts: 0,
            tick_count: 0,
            n_bars: 0,
            total_duration_ms: 0,
        })
    }

    /// Set precomputed sigma cache for O(1) sigma lookups.
    /// When set, compute_brick_size skips the expensive winsorized mean computation.
    /// Use for calibration where sigma values are constant across iterations.
    /// Takes a borrow — no allocation per calibration iteration.
    pub fn set_sigma_cache(&mut self, cache: &'a [f64]) {
        self.sigma_cache = Some(cache);
    }

    /// Return cumulative bar count and total duration.
    pub fn stats(&self) -> (usize, u64) {
        (self.n_bars, self.total_duration_ms)
    }

    /// Compute brick size for a given price and timestamp.
    ///
    /// Pure function of (price, σ_blend, config). Recomputed every 30 min.
    /// Uses sigma_cache when available (O(1) lookup vs O(n log n) sort).
    fn compute_brick_size(&mut self, price: f64, timestamp_ms: i64) -> f64 {
        let hour_idx = self.sigma_calc.find_index_for_ts(timestamp_ms);
        let sigma = if let Some(ref cache) = self.sigma_cache {
            cache.get(hour_idx).copied().unwrap_or(0.01)
        } else {
            self.sigma_calc.compute_sigma(hour_idx)
        };

        let raw_pct = self.config.multiplier as f64 * sigma;
        let clamped_pct = raw_pct.clamp(self.config.min_pct as f64, self.config.max_pct as f64);
        let raw_brick = price * clamped_pct;
        // Snap to 4-sigfig 2/5-multiple grid: reduces noise, aligns with natural
        // price levels, and ensures consistency regardless of aggregation start
        // time.  Max rounding error ~0.15% — well below Parkinson vol.
        let brick_size = snap_to_25_grid(raw_brick);
        self.current_brick_size = brick_size;
        self.last_recompute_period = timestamp_ms / 1_800_000; // 30-min periods
        brick_size
    }

    /// Feed a single tick into the generator, emitting bars via callback.
    ///
    /// State persists across calls, so this can be called across multiple
    /// tick files without resetting the renko generator.
    pub fn feed_tick<F>(&mut self, ts: i64, price: f64, write_bar: &mut F) -> Result<()>
    where
        F: FnMut(&RenkoBar) -> Result<()>,
    {
        if !self.initialized {
            self.compute_brick_size(price, ts);
            // Snap initial price to grid so all subsequent brick boundaries
            // land on clean levels (multiples of the brick's LSB).
            let grid = grid_step_for_brick(self.current_brick_size);
            self.last_close = snap_to_grid(price, grid);
            self.pending_high = price;
            self.pending_low = price;
            self.bar_start_ts = ts;
            self.tick_count = 1;
            self.initialized = true;
            return Ok(());
        }

        self.tick_count += 1;

        // Recompute brick size every 30 min (by wall clock, not bar time)
        let current_half_hour = ts / 1_800_000;
        if current_half_hour > self.last_recompute_period {
            self.compute_brick_size(price, ts);
            // Re-snap last_close to new grid when brick size changes,
            // preventing misalignment between old and new grid steps.
            let new_grid = grid_step_for_brick(self.current_brick_size);
            self.last_close = snap_to_grid(self.last_close, new_grid);
        }

        let sz = self.current_brick_size;

        // Skip degenerate prices and brick sizes — zero/NaN/negative
        // can cause infinite loops in the while loops below.
        if sz <= 0.0 || !sz.is_finite() || !price.is_finite() || price <= 0.0 {
            return Ok(());
        }

        self.pending_high = self.pending_high.max(price);
        self.pending_low = self.pending_low.min(price);

        // Grid step for snapping brick boundaries (2*unit at 4-sigfig precision)
        let grid = grid_step_for_brick(sz);

        // Max bricks per tick: safety guard against infinite loops from
        // floating-point edge cases in snap_to_grid convergence.
        // BTC's biggest daily move ~40% / min_pct 0.1% = 400 bricks max.
        // 10,000 is extremely generous.
        const MAX_BRICKS_PER_TICK: usize = 10_000;
        let mut bricks_this_tick = 0usize;

        // Emit UP bricks
        let mut first_in_seq = true;
        while price - self.last_close >= sz {
            let close = snap_to_grid(self.last_close + sz, grid);
            // Guard: if snap didn't advance, break to avoid infinite loop
            if close <= self.last_close || bricks_this_tick >= MAX_BRICKS_PER_TICK { break; }
            bricks_this_tick += 1;
            let duration = (ts - self.bar_start_ts) as u32;
            self.total_duration_ms += duration as u64;

            let l = if first_in_seq { self.pending_low.min(self.last_close) } else { self.last_close };

            let bar = RenkoBar {
                timestamp: ts,
                open: self.last_close,
                high: close,
                low: l,
                close,
                direction: 1,
                brick_size: sz,
                // First bar in sequence gets accumulated ticks; gap-fill bars get 0
                // (no new ticks contributed — purely mechanical price gap-fill)
                tick_count: if first_in_seq { self.tick_count } else { 0 },
                duration_ms: duration,
            };

            first_in_seq = false;
            write_bar(&bar)?;
            self.n_bars += 1;

            self.last_close = close;
            self.pending_high = close;
            self.pending_low = close;
            self.bar_start_ts = ts;
            self.tick_count = 0;
        }

        // Emit DOWN bricks
        first_in_seq = true;
        while self.last_close - price >= sz {
            let close = snap_to_grid(self.last_close - sz, grid);
            // Guard: if snap didn't advance, break to avoid infinite loop
            if close >= self.last_close || bricks_this_tick >= MAX_BRICKS_PER_TICK { break; }
            bricks_this_tick += 1;
            let duration = (ts - self.bar_start_ts) as u32;
            self.total_duration_ms += duration as u64;

            let h = if first_in_seq { self.pending_high.max(self.last_close) } else { self.last_close };

            let bar = RenkoBar {
                timestamp: ts,
                open: self.last_close,
                high: h,
                low: close,
                close,
                direction: -1,
                brick_size: sz,
                tick_count: if first_in_seq { self.tick_count } else { 0 },
                duration_ms: duration,
            };

            first_in_seq = false;
            write_bar(&bar)?;
            self.n_bars += 1;

            self.last_close = close;
            self.pending_high = close;
            self.pending_low = close;
            self.bar_start_ts = ts;
            self.tick_count = 0;
        }

        Ok(())
    }

    /// Generate Renko bars from a price iterator.
    ///
    /// Renko invariants enforced:
    ///   1. Single-sided wick: bullish → high == close, bearish → low == close
    ///   2. Wick bounded by brick size
    ///   3. Continuity: open[i] == close[i-1]
    ///   4. Multi-brick gaps: while loops emit N bars
    ///
    /// State persists across calls — can be called multiple times for
    /// incremental feeding (e.g., across multiple tick files).
    pub fn generate<F>(
        &mut self,
        price_iter: impl Iterator<Item = (i64, f64)>,
        mut write_bar: F,
    ) -> Result<(usize, u64)>
    where
        F: FnMut(&RenkoBar) -> Result<()>,
    {
        let bars_before = self.n_bars;
        let dur_before = self.total_duration_ms;

        for (ts, price) in price_iter {
            self.feed_tick(ts, price, &mut write_bar)?;
        }

        Ok((self.n_bars - bars_before, self.total_duration_ms - dur_before))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = RenkoConfig::default();
        assert!(config.validate().is_ok());

        let bad = RenkoConfig { multiplier: 0.0, ..Default::default() };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_config_id() {
        let config = RenkoConfig { multiplier: 0.075, min_pct: 0.000830, ..Default::default() };
        let id = config.id();
        assert_eq!(id, "m0750_mp0830");
    }

    #[test]
    fn test_snap_to_25_grid() {
        // 4 sigfigs with 2/5 multiples
        // 174.38: unit=0.1, step2=0.2, step5=0.5 → 174.4 (err 0.02) vs 174.5 (err 0.12) → 174.4
        assert!((snap_to_25_grid(174.38) - 174.4).abs() < 1e-10);
        // 347.83: unit=0.1, step2=0.2 → 347.8, step5=0.5 → 348.0 → 347.8 (err 0.03)
        assert!((snap_to_25_grid(347.83) - 347.8).abs() < 1e-10);
        // 84234: unit=10, step2=20, step5=50 → 84240 vs 84250 → 84240
        assert!((snap_to_25_grid(84234.0) - 84240.0).abs() < 1e-10);
        // 0.00347: unit=0.000001, step2=0.000002 → 0.003470, step5=0.000005 → 0.003470
        assert!((snap_to_25_grid(0.00347) - 0.00347).abs() < 1e-10);
        // 12.34: unit=0.01, step2=0.02 → 12.34, step5=0.05 → 12.35 → 12.34
        assert!((snap_to_25_grid(12.34) - 12.34).abs() < 1e-10);
        // Edge: exact power of 10
        assert!((snap_to_25_grid(1000.0) - 1000.0).abs() < 1e-10);
        // Already on-grid: 7.53 has 4sf, unit=0.001, step2=0.002 → 7.530
        assert!((snap_to_25_grid(7.53) - 7.530).abs() < 1e-10);
        // Off-grid: 7.531 → nearest 0.002 is 7.532, nearest 0.005 is 7.530 → 7.530 (err 0.001 < 0.002)
        assert!((snap_to_25_grid(7.531) - 7.530).abs() < 1e-10);
    }

    #[test]
    fn test_grid_step() {
        // 4sf grid step = 2 * 10^(floor(log10(x)) - 3)
        let grid = grid_step_for_brick(174.4);  // floor(log10(174.4))=2, step = 2*10^(2-3) = 0.2
        assert!((grid - 0.2).abs() < 1e-10);
        assert!((snap_to_grid(79823.45, grid) - 79823.4).abs() < 1e-10);

        let grid2 = grid_step_for_brick(84240.0);  // floor(log10)=4, step = 2*10^(4-3) = 20
        assert!((grid2 - 20.0).abs() < 1e-10);
        assert!((snap_to_grid(84234.0, grid2) - 84240.0).abs() < 1e-10);
    }

    #[test]
    fn test_winsorized_mean() {
        // Need >= 20 values for 5th/95th percentile to have effect
        let mut values: Vec<f64> = (1..=20).map(|i| i as f64).collect();
        values[19] = 1000.0; // outlier at position 19
        let (mean, _var) = winsorized_mean_and_var_inplace(&mut values, 5, 0.05, 0.95);
        // The outlier (1000.0) should be clipped to sorted[18]=19.0
        assert!(mean < 20.0, "winsorized mean should suppress outlier: got {}", mean);
        // Without winsorizing: (1+2+...+19+1000)/20 = 59.5
        // With winsorizing the outlier clipped to 19: (1+2+...+19+19)/20 = 10.45
        assert!(mean < 12.0, "winsorized mean should be well below raw mean: got {}", mean);
    }
}
