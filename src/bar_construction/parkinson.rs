//! Multi-timeframe Parkinson volatility.
//!
//! Inverse-variance weighted blend of winsorized means across configurable
//! lookback windows. Source data (per-bin sigma_pct) is provided by anything
//! that implements [`VolSource`]: a memory-mapped .vol file (backtest), an
//! in-memory ring buffer (real-time), or a test fixture.

use serde::{Deserialize, Serialize};

/// Abstract source of per-bin Parkinson sigma values.
///
/// Bins are typically 30 minutes wide. Implementors provide O(1) length and
/// O(1) sigma lookup, plus binary-search-style timestamp-to-index mapping.
pub trait VolSource {
    /// Number of bins stored.
    fn len(&self) -> usize;

    /// `sigma_pct` for bin at index `i`. Returns 0.0 if out of range.
    fn sigma_pct(&self, i: usize) -> f64;

    /// Bin index for the given MITCH mts (u48 ticks since 2010). Clamps to
    /// `len().saturating_sub(1)` on overshoot.
    fn find_index_for_mts(&self, mts: u64) -> usize;

    /// True when no bins are available.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Volatility calculation config (typically from pipeline.yml `vol` section).
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
            mtf_lookback_days: vec![14, 60, 180],
            winsorize_pct: [0.05, 0.95],
            winsorize_min_samples: 5,
        }
    }
}

/// Multi-timeframe Parkinson sigma blender.
///
/// Reads 30-min Parkinson sigma from a `VolSource`, then blends across
/// configurable lookback windows using inverse-variance weighting and
/// winsorized mean for robustness.
pub struct MtfParkinsonCalculator<'a, S: VolSource + ?Sized> {
    source: &'a S,
    config: VolConfig,
    buf: Vec<f64>,
}

impl<'a, S: VolSource + ?Sized> MtfParkinsonCalculator<'a, S> {
    pub fn new(source: &'a S, config: VolConfig) -> Self {
        Self { source, config, buf: Vec::new() }
    }

    /// Precompute sigma for every bin into a flat cache for O(1) lookup.
    /// Use this once, then pass the result to hot loops (e.g. calibration).
    pub fn precompute_sigma_cache(&mut self) -> Vec<f64> {
        let n = self.source.len();
        let mut cache = Vec::with_capacity(n);
        for i in 0..n {
            cache.push(self.compute_sigma(i));
        }
        cache
    }

    /// Delegate to the source.
    #[inline]
    pub fn find_index_for_mts(&self, mts: u64) -> usize {
        self.source.find_index_for_mts(mts)
    }

    /// Blended Parkinson sigma at bin `hour_idx`.
    ///
    /// Returns the inverse-variance weighted blend of winsorized means
    /// across all configured lookback windows. Falls back to the bin's
    /// raw sigma if no window yields a valid sample.
    pub fn compute_sigma(&mut self, hour_idx: usize) -> f64 {
        let n = self.source.len();
        if hour_idx >= n {
            return 0.01;
        }

        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;

        for &lookback_days in &self.config.mtf_lookback_days {
            let lookback_periods = lookback_days * 48;
            let start_idx = hour_idx.saturating_sub(lookback_periods);
            if start_idx >= hour_idx || hour_idx - start_idx < 48 {
                continue;
            }

            self.buf.clear();
            self.buf.reserve(hour_idx - start_idx + 1);
            for i in start_idx..=hour_idx {
                self.buf.push(self.source.sigma_pct(i));
            }

            let min_samples = self.config.winsorize_min_samples;
            let [lo_pct, hi_pct] = self.config.winsorize_pct;
            let (wmean, variance) =
                winsorized_mean_and_var_inplace(&mut self.buf, min_samples, lo_pct, hi_pct);
            if variance <= 0.0 || wmean <= 0.0 {
                continue;
            }

            let inv_var = 1.0 / variance;
            weighted_sum += inv_var * wmean;
            weight_sum += inv_var;
        }

        if weight_sum > 0.0 {
            weighted_sum / weight_sum
        } else {
            self.source.sigma_pct(hour_idx).max(0.01)
        }
    }
}

/// Winsorized mean and variance, in-place.
///
/// Sorts the buffer, clips to `[lo_pct, hi_pct]` percentile boundaries, then
/// returns (mean, variance). Reuses the buffer to avoid allocation.
fn winsorized_mean_and_var_inplace(
    values: &mut [f64],
    min_samples: usize,
    lo_pct: f64,
    hi_pct: f64,
) -> (f64, f64) {
    let n = values.len();
    if n < min_samples {
        let mean = values.iter().sum::<f64>() / n.max(1) as f64;
        let var = if n > 1 {
            values.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>() / (n - 1) as f64
        } else {
            0.0
        };
        return (mean, var);
    }

    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let lo = (n as f64 * lo_pct) as usize;
    let hi = ((n as f64 * hi_pct) as usize).saturating_sub(1).min(n - 1);
    let lo_val = values[lo];
    let hi_val = values[hi];

    let mut sum = 0.0;
    for v in values.iter_mut() {
        *v = v.clamp(lo_val, hi_val);
        sum += *v;
    }
    let mean = sum / n as f64;
    let var = values.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>() / (n - 1) as f64;
    (mean, var)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticSource(Vec<f64>);

    impl VolSource for StaticSource {
        fn len(&self) -> usize { self.0.len() }
        fn sigma_pct(&self, i: usize) -> f64 { self.0.get(i).copied().unwrap_or(0.0) }
        fn find_index_for_mts(&self, _mts: u64) -> usize { self.0.len().saturating_sub(1) }
    }

    #[test]
    fn winsorized_mean_suppresses_outlier() {
        let mut values: Vec<f64> = (1..=20).map(|i| i as f64).collect();
        values[19] = 1000.0;
        let (mean, _var) = winsorized_mean_and_var_inplace(&mut values, 5, 0.05, 0.95);
        assert!(mean < 12.0, "winsorized mean should suppress outlier: got {}", mean);
    }

    #[test]
    fn calc_returns_fallback_on_small_source() {
        let src = StaticSource(vec![0.02]);
        let mut calc = MtfParkinsonCalculator::new(&src, VolConfig::default());
        assert!((calc.compute_sigma(0) - 0.02).abs() < 1e-9);
    }
}
