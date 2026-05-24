//! Adaptive Renko bar generation.
//!
//! Brick size formula:
//!   b_t = p_t * max(multiplier * sigma_blend(t), min_pct)
//!
//! where sigma_blend comes from [`super::parkinson::MtfParkinsonCalculator`]
//! over any [`super::parkinson::VolSource`] (mmap for backtest, ring buffer
//! for real-time). NO upper ceiling — operator directive 2026-05-24
//! ("markets be markets"): an adaptive Renko that caps brick % on high-σ
//! days biases calibration's binary search downward (the search assumes the
//! clamp does not fire). Removing `max_pct` keeps `k(σ)` honest. The only
//! remaining safety is `min_pct` (floor against div-by-zero / sigma=0).
//!
//! Debate (Aoife HFT-quant ↔ Tomás storage):
//!   - Aoife: "Ceiling distorts calibration on 2020-style σ spikes —
//!     bpd undershoots because the brick is too small relative to k."
//!   - Tomás: "But 10% absolute brick on a 5σ day still survives our 96B
//!     Bar layout; nothing downstream breaks if pct grows."
//!   - Consensus: drop the ceiling, keep min_pct=0.0001 floor.
//!
//! Design:
//!   * Streaming, never holds all bars in RAM
//!   * Fixed lookbacks and auto-weighting, no over-fitting
//!   * Continuity invariants enforced (single-sided wick, open[i]=close[i-1])
//!   * Emits `mitch::Bar` with `kind = BarKind::Renko as u8`.

use anyhow::Result;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use serde::{Deserialize, Serialize};

use super::grid::{grid_step_for_brick, snap_to_25_grid, snap_to_grid};
use super::parkinson::{MtfParkinsonCalculator, VolConfig, VolSource};

/// Adaptive Renko configuration.
///
/// `multiplier` controls bars/day via `brick_pct = multiplier * sigma_blend`
/// (auto-calibrated via target bars/day). `min_pct` is a safety floor.
/// No upper ceiling — see module docstring.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RenkoConfig {
    pub multiplier: f32,
    pub min_pct: f32,
}

impl Default for RenkoConfig {
    fn default() -> Self {
        Self { multiplier: 0.075, min_pct: 0.0001 }
    }
}

impl RenkoConfig {
    /// Unique identifier for this config (used for output file naming).
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
        // Debate (Aoife ↔ Tomás): allow min_pct == 0 (no floor) vs require > 0
        // (div-by-zero guard). Consensus: must be ≥ 0; the brick formula's
        // `max(_, min_pct)` already handles the zero case safely.
        if self.min_pct < 0.0 {
            anyhow::bail!("min_pct must be >= 0");
        }
        Ok(())
    }
}

/// Streaming Renko bar generator with adaptive brick sizing.
///
/// Emits `mitch::Bar` with `kind = BarKind::Renko as u8`. Enrichment fields
/// (dispersion, drift, vol_imbalance, ...) are left at zero: downstream
/// consumers accumulate them from ticks.
pub struct RenkoGenerator<'a, S: VolSource + ?Sized> {
    config: RenkoConfig,
    sigma_calc: MtfParkinsonCalculator<'a, S>,
    sigma_cache: Option<&'a [f64]>,
    current_brick_size: f64,
    /// Grid step derived from `current_brick_size`; recomputed only when the
    /// brick size changes (every 30 min or on init), so the hot path reads it
    /// with no arithmetic.
    current_grid_step: f64,
    last_recompute_period: i64,
    initialized: bool,
    last_close: f64,
    pending_high: f64,
    pending_low: f64,
    bar_start_ts: i64,
    tick_count: u32,
    n_bars: usize,
    total_duration_ms: u64,
}

impl<'a, S: VolSource + ?Sized> RenkoGenerator<'a, S> {
    pub fn new(config: RenkoConfig, source: &'a S, vol_config: VolConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            sigma_calc: MtfParkinsonCalculator::new(source, vol_config),
            sigma_cache: None,
            current_brick_size: 0.0,
            current_grid_step: 0.0,
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

    /// Use a precomputed sigma cache for O(1) lookups.
    pub fn set_sigma_cache(&mut self, cache: &'a [f64]) {
        self.sigma_cache = Some(cache);
    }

    /// Cumulative bar count and total duration (ms).
    pub fn stats(&self) -> (usize, u64) {
        (self.n_bars, self.total_duration_ms)
    }

    fn compute_brick_size(&mut self, price: f64, timestamp_ms: i64) -> f64 {
        let mts = timestamp::from_epoch_ms(timestamp_ms);
        let hour_idx = self.sigma_calc.find_index_for_mts(mts);
        let sigma = if let Some(cache) = self.sigma_cache {
            cache.get(hour_idx).copied().unwrap_or(0.01)
        } else {
            self.sigma_calc.compute_sigma(hour_idx)
        };

        let raw_pct = self.config.multiplier as f64 * sigma;
        // Floor only — no ceiling (markets be markets, see module doc).
        // Debate (Aoife ↔ Tomás): without a cap, a flash crash could mint
        // a 50% brick. But that's the right answer for adaptive renko —
        // calibration's binary search needs a monotone pct→bpd curve.
        let clamped_pct = raw_pct.max(self.config.min_pct as f64);
        let raw_brick = price * clamped_pct;
        let brick_size = snap_to_25_grid(raw_brick);
        self.current_brick_size = brick_size;
        self.current_grid_step = grid_step_for_brick(brick_size);
        self.last_recompute_period = timestamp_ms / 1_800_000;
        brick_size
    }

    #[inline]
    fn emit_bar<F>(
        &mut self,
        open_ts: i64,
        close_ts: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        tick_count: u32,
        write_bar: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&Bar) -> Result<()>,
    {
        let open_mts = timestamp::from_epoch_ms(open_ts);
        let close_mts = timestamp::from_epoch_ms(close_ts);
        let mut bar = Bar::new_ohlcv(open_mts, close_mts, open, high, low, close, 0, 0, tick_count);
        bar.kind = BarKind::Renko as u8;
        write_bar(&bar)?;
        self.n_bars += 1;
        Ok(())
    }

    /// Feed one tick, emitting any produced bars via the callback.
    pub fn feed_tick<F>(&mut self, ts: i64, price: f64, write_bar: &mut F) -> Result<()>
    where
        F: FnMut(&Bar) -> Result<()>,
    {
        if !self.initialized {
            self.compute_brick_size(price, ts);
            self.last_close = snap_to_grid(price, self.current_grid_step);
            self.pending_high = price;
            self.pending_low = price;
            self.bar_start_ts = ts;
            self.tick_count = 1;
            self.initialized = true;
            return Ok(());
        }

        self.tick_count += 1;

        let current_half_hour = ts / 1_800_000;
        if current_half_hour > self.last_recompute_period {
            self.compute_brick_size(price, ts);
            self.last_close = snap_to_grid(self.last_close, self.current_grid_step);
        }

        let sz = self.current_brick_size;
        if sz <= 0.0 || !sz.is_finite() || !price.is_finite() || price <= 0.0 {
            return Ok(());
        }

        self.pending_high = self.pending_high.max(price);
        self.pending_low = self.pending_low.min(price);

        let grid = self.current_grid_step;

        const MAX_BRICKS_PER_TICK: usize = 10_000;
        let mut bricks_this_tick = 0usize;

        let mut first_in_seq = true;
        while price - self.last_close >= sz {
            let close = snap_to_grid(self.last_close + sz, grid);
            if close <= self.last_close || bricks_this_tick >= MAX_BRICKS_PER_TICK {
                break;
            }
            bricks_this_tick += 1;
            let duration = (ts - self.bar_start_ts) as u64;
            self.total_duration_ms += duration;

            let low = if first_in_seq { self.pending_low.min(self.last_close) } else { self.last_close };
            let tick_count_for_bar = if first_in_seq { self.tick_count } else { 0 };
            self.emit_bar(self.bar_start_ts, ts, self.last_close, close, low, close, tick_count_for_bar, write_bar)?;

            first_in_seq = false;
            self.last_close = close;
            self.pending_high = close;
            self.pending_low = close;
            self.bar_start_ts = ts;
            self.tick_count = 0;
        }

        first_in_seq = true;
        while self.last_close - price >= sz {
            let close = snap_to_grid(self.last_close - sz, grid);
            if close >= self.last_close || bricks_this_tick >= MAX_BRICKS_PER_TICK {
                break;
            }
            bricks_this_tick += 1;
            let duration = (ts - self.bar_start_ts) as u64;
            self.total_duration_ms += duration;

            let high = if first_in_seq { self.pending_high.max(self.last_close) } else { self.last_close };
            let tick_count_for_bar = if first_in_seq { self.tick_count } else { 0 };
            self.emit_bar(self.bar_start_ts, ts, self.last_close, high, close, close, tick_count_for_bar, write_bar)?;

            first_in_seq = false;
            self.last_close = close;
            self.pending_high = close;
            self.pending_low = close;
            self.bar_start_ts = ts;
            self.tick_count = 0;
        }

        Ok(())
    }

    /// Feed many ticks from an iterator.
    pub fn generate<F>(
        &mut self,
        price_iter: impl Iterator<Item = (i64, f64)>,
        mut write_bar: F,
    ) -> Result<(usize, u64)>
    where
        F: FnMut(&Bar) -> Result<()>,
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
    fn config_validation() {
        assert!(RenkoConfig::default().validate().is_ok());
        let bad = RenkoConfig { multiplier: 0.0, ..Default::default() };
        assert!(bad.validate().is_err());
        // min_pct=0 is now allowed (no floor); negative is still rejected.
        let zero_floor = RenkoConfig { multiplier: 0.075, min_pct: 0.0 };
        assert!(zero_floor.validate().is_ok());
        let neg = RenkoConfig { multiplier: 0.075, min_pct: -1e-6 };
        assert!(neg.validate().is_err());
    }

    #[test]
    fn config_id() {
        let config = RenkoConfig { multiplier: 0.075, min_pct: 0.000830 };
        assert_eq!(config.id(), "m0750_mp0830");
    }
}
