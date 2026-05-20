//! Two-stage aggregation mirroring the production pipeline.
//!
//! Stage 1 (per-provider, inner cycle at `cycle_ms`):
//!   raw ticks -> `nxr_sdk::TickAccumulator` -> per-provider `Index`
//!   matches what `nxr-crypto` / `nxr-fx` forwarders emit every 50 ms.
//!
//! Stage 2 (cross-provider, same inner cycle):
//!   per-provider `Index`s -> `nxr_sdk::compute_vwap_at` -> composite `Index`
//!   matches what the core `nxr` aggregator emits to WebSocket / .idx.
//!
//! Stage 3 (bar construction, outer bucket at `agg_step`):
//!   composite `Index` stream -> `nxr_sdk::BarAccumulator` -> `mitch::Bar`
//!   adds the microstructure enrichment (realized var, bipower, drift, OFI,
//!   avg_spread_bps, avg_ci_ubp, reject_rate, ...).
//!
//! Per-provider state lives in a dense `Vec<ProviderSlot>` kept sorted by
//! provider id. Three concerns (accumulator, z-stats, TDWAP entry) coexist in
//! one slot so the per-tick hot path performs a single linear scan over the
//! handful of providers in play rather than three independent `HashMap`
//! probes. A per-slot `dirty` flag gates stage 2 + stage 3 so cycles with
//! no new input cost only the scan (matching prod `core::aggregator` which
//! only recomputes TDWAP for tickers in `dirty_scratch`).

use crate::types::{AggregationMode, Config, TickFrame};
use mitch::bar::Bar;
use nxr_sdk::tdwap::decode_ci_ubp;
use nxr_sdk::{
    compute_vwap_at, is_valid_tick, resolve_ticker_id,
    BarAccumulator, ProviderEntry, RunningStats, TickAccumulator,
};
use std::time::{Duration, Instant};

/// Everything we track about a single upstream provider. Kept in
/// `Aggregator::slots` sorted by `id` for deterministic iteration order in
/// `compute_vwap_at` (FP sums are order-sensitive, so stable ordering matters
/// for reproducible backtests).
struct ProviderSlot {
    id: u16,
    acc: TickAccumulator,
    stats: RunningStats,
    /// Latest TDWAP-ready snapshot. `None` until the first successful flush.
    entry: Option<ProviderEntry>,
    /// Set on every accepted ingest, cleared after each flush attempt.
    /// When every slot is clean the outer cycle short-circuits.
    dirty: bool,
}

impl ProviderSlot {
    fn new(id: u16, ticker_id: u64) -> Self {
        Self {
            id,
            acc: TickAccumulator::new(ticker_id),
            stats: RunningStats::default(),
            entry: None,
            dirty: false,
        }
    }
}

pub struct Aggregator {
    config: Config,
    ticker_id: u64,
    cycle_ms: i64,
    stale_secs: f64,
    z_threshold: f64,

    slots: Vec<ProviderSlot>,

    anchor: Option<(Instant, i64)>,
    next_cycle_ms: i64,
    last_tick_ms: i64,

    bar_acc: BarAccumulator,
    bucket_start_ms: i64,
    last_close: f64,
}

impl Aggregator {
    pub fn new(config: Config) -> Self {
        let ticker_id = resolve_ticker_id(&format!("{}/{}", config.base, config.quote));
        Self {
            ticker_id,
            cycle_ms: config.cycle_ms as i64,
            stale_secs: config.stale_secs,
            z_threshold: config.z_threshold,
            slots: Vec::new(),
            anchor: None,
            next_cycle_ms: -1,
            last_tick_ms: -1,
            bar_acc: BarAccumulator::new(),
            bucket_start_ms: -1,
            last_close: 0.0,
            config,
        }
    }

    /// Locate the slot for `provider_id`, inserting a new one in sorted
    /// position if we have not seen this provider before. Returns a `&mut`
    /// so the caller can run the z-gate and accumulator ingest through the
    /// same borrow, keeping the hot path to a single linear scan.
    #[inline]
    fn slot_mut(&mut self, provider_id: u16) -> &mut ProviderSlot {
        if let Some(idx) = self.slots.iter().position(|s| s.id == provider_id) {
            return &mut self.slots[idx];
        }
        let insert_at = self
            .slots
            .iter()
            .position(|s| s.id > provider_id)
            .unwrap_or(self.slots.len());
        self.slots
            .insert(insert_at, ProviderSlot::new(provider_id, self.ticker_id));
        &mut self.slots[insert_at]
    }

    /// Map a tick's epoch_ms onto the simulated clock anchored at the first tick.
    /// Instant supports `start_instant + Duration`, so arithmetic works even
    /// though we never actually sleep wall-clock for the gap.
    #[inline]
    fn sim_now(&self, tick_ms: i64) -> Instant {
        let (anchor_instant, anchor_tick_ms) = self
            .anchor
            .expect("sim_now called before anchor initialized");
        let dt_ms = (tick_ms - anchor_tick_ms).max(0) as u64;
        anchor_instant + Duration::from_millis(dt_ms)
    }

    /// Run one aggregation cycle at the given boundary timestamp.
    /// 1. Flush every dirty `TickAccumulator` into its `ProviderEntry`.
    /// 2. If no slot flushed, return immediately (the dirty gate: match prod
    ///    `core::aggregator` which only recomputes TDWAP for dirty tickers).
    /// 3. Run cross-provider TDWAP via the simulated clock.
    /// 4. Emit any full outer bars (time or tick mode) before ingesting the
    ///    composite, so bar boundaries reflect the period's observations
    ///    rather than the boundary tick itself.
    /// 5. Ingest the composite `Index` into the `BarAccumulator`.
    fn run_cycle(&mut self, now_tick_ms: i64, bars: &mut Vec<Bar>) {
        let now = self.sim_now(now_tick_ms);

        let mut any_flush = false;
        for slot in &mut self.slots {
            if !slot.dirty {
                continue;
            }
            slot.dirty = false;
            if let Some(index) = slot.acc.flush() {
                match &mut slot.entry {
                    Some(e) => e.update_at(index, now),
                    None => slot.entry = Some(ProviderEntry::new_at(index, 1.0, now)),
                }
                any_flush = true;
            }
        }

        if !any_flush {
            return;
        }

        let Some(composite) = compute_vwap_at(
            self.ticker_id,
            self.slots.iter().filter_map(|s| s.entry.as_ref()),
            self.stale_secs,
            now,
        ) else {
            return;
        };
        if !is_valid_tick(composite.bid, composite.ask) {
            return;
        }
        let composite_mid = (composite.bid + composite.ask) * 0.5;

        self.maybe_emit_bar(now_tick_ms, composite_mid, bars);

        let ci_ubp = decode_ci_ubp(composite.ci);
        self.bar_acc.ingest(
            composite.bid,
            composite.ask,
            composite.vbid,
            composite.vask,
            now_tick_ms,
            ci_ubp,
            composite.accepted as u32,
            composite.rejected as u32,
        );
    }

    /// Outer bucketing: flush the `BarAccumulator` at time-bucket boundaries
    /// (time mode) or when the composite mid crosses the tick-mode band.
    ///
    /// Time-mode bars snap `open_mts` / `close_mts` to the bucket grid so
    /// downstream consumers see strict `step`-aligned timestamps regardless
    /// of which cycle within the bucket got the last composite ingest.
    /// Without the snap, sparse real-market data produces bars with
    /// close_times that float inside the bucket (e.g. 9950 vs 19000) and
    /// breaks consumers that assume grid alignment.
    fn maybe_emit_bar(&mut self, now_tick_ms: i64, composite_mid: f64, bars: &mut Vec<Bar>) {
        match self.config.agg_mode {
            AggregationMode::Time => {
                let step = self.config.agg_step as i64;
                if step <= 0 {
                    return;
                }
                while now_tick_ms >= self.bucket_start_ms + step {
                    if let Some(mut bar) = self.bar_acc.flush() {
                        bar.set_open_mts(mitch::timestamp::from_epoch_ms(self.bucket_start_ms));
                        bar.set_close_mts(mitch::timestamp::from_epoch_ms(self.bucket_start_ms + step));
                        self.last_close = bar.close;
                        bars.push(bar);
                    }
                    self.bucket_start_ms += step;
                }
            }
            AggregationMode::Tick => {
                if self.last_close == 0.0 {
                    self.last_close = composite_mid;
                    return;
                }
                let r = self.config.agg_step;
                let upper = self.last_close * (1.0 + r);
                let lower = self.last_close / (1.0 + r);
                if self.bar_acc.count() > 0
                    && (composite_mid >= upper || composite_mid <= lower)
                {
                    if let Some(bar) = self.bar_acc.flush() {
                        if bar.close > 0.0 {
                            self.last_close = bar.close;
                            bars.push(bar);
                        }
                    }
                }
            }
        }
    }

    pub fn process_ticks(&mut self, ticks: &[TickFrame]) -> Vec<Bar> {
        let mut bars = Vec::new();
        for tick in ticks {
            let tick_ms = tick.timestamp_ms();
            let body = tick.body;
            if !is_valid_tick(body.bid, body.ask) {
                continue;
            }

            if self.anchor.is_none() {
                self.anchor = Some((Instant::now(), tick_ms));
                self.next_cycle_ms = tick_ms + self.cycle_ms;
                if self.config.agg_mode == AggregationMode::Time {
                    let step = self.config.agg_step as i64;
                    self.bucket_start_ms = if step > 0 { (tick_ms / step) * step } else { tick_ms };
                }
            }
            self.last_tick_ms = tick_ms;

            // Drain expired cycles. On a long quiet window every iteration
            // sees clean slots; fast-forward `next_cycle_ms` to the cycle
            // containing the current tick instead of spinning 72k times for
            // a one-hour gap.
            while tick_ms >= self.next_cycle_ms {
                let any_dirty = self.slots.iter().any(|s| s.dirty);
                if any_dirty {
                    let cycle_ts = self.next_cycle_ms;
                    self.run_cycle(cycle_ts, &mut bars);
                    self.next_cycle_ms += self.cycle_ms;
                } else {
                    let cycles = (tick_ms - self.next_cycle_ms) / self.cycle_ms + 1;
                    self.next_cycle_ms += cycles * self.cycle_ms;
                    break;
                }
            }

            let provider_id = tick.provider_id();
            let mid = (body.bid + body.ask) * 0.5;
            let z_threshold = self.z_threshold;
            let slot = self.slot_mut(provider_id);
            let z = slot.stats.update(mid);
            if z > z_threshold {
                slot.acc.reject();
                continue;
            }
            slot.acc.ingest(body.bid, body.ask, body.vbid, body.vask);
            slot.dirty = true;
        }
        bars
    }

    /// Drain terminal state: run one final cycle at the LAST tick timestamp
    /// (not the next cycle boundary, which would be past the last observation
    /// and inflate `sigma_stale`) then flush the `BarAccumulator`.
    ///
    /// The tail bar covers a partial bucket at end-of-data. In Time mode we
    /// still snap its timestamps to the bucket grid so consumers observing
    /// `close_time_ms` diffs see a uniform `step` everywhere, not a short
    /// stub at the end.
    pub fn finalize(&mut self) -> Vec<Bar> {
        let mut tail = Vec::new();
        if self.anchor.is_some() {
            self.run_cycle(self.last_tick_ms, &mut tail);
        }
        if let Some(mut bar) = self.bar_acc.flush() {
            if self.config.agg_mode == AggregationMode::Time {
                let step = self.config.agg_step as i64;
                if step > 0 {
                    bar.set_open_mts(mitch::timestamp::from_epoch_ms(self.bucket_start_ms));
                    bar.set_close_mts(mitch::timestamp::from_epoch_ms(self.bucket_start_ms + step));
                }
            }
            self.last_close = bar.close;
            tail.push(bar);
        }
        tail
    }
}
