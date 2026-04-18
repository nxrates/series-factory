//! Time/tick-based aggregation backed by SDK's BarAccumulator.
//!
//! Series-factory's job is orchestration (bucketing, multi-source merge, gap fill).
//! The actual per-bar accumulation and microstructure enrichment is in nxr-sdk.

use crate::types::{AggregationMode, Config, TickFrame};
use mitch::bar::Bar;
use nxr_sdk::BarAccumulator;

pub struct Aggregator {
    config: Config,
    current_bucket: Vec<TickFrame>,
    bucket_start_time: i64,
    last_close: f64,
    acc: BarAccumulator,
}

impl Aggregator {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            current_bucket: Vec::new(),
            bucket_start_time: -1,
            last_close: 0.0,
            acc: BarAccumulator::new(),
        }
    }

    pub fn process_ticks(&mut self, ticks: &[TickFrame]) -> Vec<Bar> {
        let mut bars = Vec::new();

        for tick in ticks {
            match self.config.agg_mode {
                AggregationMode::Time => {
                    if self.bucket_start_time < 0 {
                        let step = self.config.agg_step as i64;
                        let aligned = if step > 0 { (tick.timestamp_ms() / step) * step } else { tick.timestamp_ms() };
                        self.bucket_start_time = aligned;
                    }

                    let step = self.config.agg_step as i64;
                    while tick.timestamp_ms() >= self.bucket_start_time + step {
                        bars.push(self.flush_bucket());
                        self.bucket_start_time += step;
                    }

                    self.current_bucket.push(*tick);
                }
                AggregationMode::Tick => {
                    let mid = tick.mid_price();
                    if self.last_close == 0.0 {
                        self.last_close = mid;
                    }
                    let step_ratio = self.config.agg_step;
                    let upper = self.last_close * (1.0 + step_ratio);
                    let lower = self.last_close / (1.0 + step_ratio);

                    if !self.current_bucket.is_empty() && (mid >= upper || mid <= lower) {
                        let bar = self.flush_bucket();
                        if bar.close > 0.0 {
                            bars.push(bar);
                        }
                        self.last_close = mid;
                    }

                    self.current_bucket.push(*tick);
                }
            }
        }

        bars
    }

    pub fn finalize(&mut self) -> Option<Bar> {
        if self.current_bucket.is_empty() && self.last_close == 0.0 {
            return None;
        }
        let bar = self.flush_bucket();
        Some(bar)
    }

    /// Drain current_bucket through the BarAccumulator and flush to a Bar.
    fn flush_bucket(&mut self) -> Bar {
        let max_dev = self.config.tick_max_deviation;
        let close_for_filter = self.current_bucket.last().map(|t| t.mid_price()).unwrap_or(0.0);

        for tick in self.current_bucket.drain(..) {
            let mid = tick.mid_price();
            // Outlier rejection: skip ticks deviating too far from the bucket's close
            if close_for_filter > 0.0 {
                let deviation = (mid - close_for_filter).abs() / close_for_filter;
                if deviation > max_dev {
                    continue;
                }
            }
            let body = tick.body;
            self.acc.ingest(body.bid, body.ask, body.vbid, body.vask, tick.timestamp_ms());
        }

        match self.acc.flush() {
            Some(bar) => {
                self.last_close = bar.close;
                bar
            }
            None => nxr_sdk::flat_bar(0, self.last_close),
        }
    }
}

