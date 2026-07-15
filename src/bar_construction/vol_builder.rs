//! Canonical 30-min Rogers-Satchell sigma builder over s10 OHLC.
//!
//! Single source of truth for the `.vol` file construction shared by every
//! offline pipeline (`nxr-calibrate` and the renko-from-idx bins). The ratified vol basis
//! (2026-06): the canonical per-bin σ is the Rogers-Satchell range estimator
//! over s10-resampled 30-min OHLC, with `offline == live` byte-for-byte.
//!
//! Pipeline (ONE function, [`build_vol_from_s10`]):
//!   1. Stream the gapless `.s10` bars (96B `mitch::Bar`, `kind = Kline`).
//!   2. Convert each to [`nxr_sdk::ohlc::Ohlc`] and roll up
//!      `ohlc::rollup(10_000, 1_800_000)` into 30-min OHLC bins (O=first
//!      s10.open, H=max s10.high, L=min s10.low, C=last s10.close).
//!   3. Per bin: σ = `vol_estimator::rs_sigma_from_ohlc(o,h,l,c)`.
//!   4. EMA(`vol_cfg.ema_period`, default 28) smoothing — first `ema_period`
//!      bins use an expanding-mean seed (identical to the live `LiveVolRing`).
//!   5. Write one EMA-smoothed σ row per bin to the `.vol` `VolWriter`.
//!
//! The s10 input MUST be gapless (flat-fill on quiet windows, matching the live
//! `bars_s10` producer) so offline σ == live σ on quiet windows.

use anyhow::Result;
use mitch::timestamp;
use nxr_sdk::ohlc::{bar_to_ohlc, rollup, Ohlc};
use nxr_sdk::shard::{ShardStream, BAR_MS_S10, MS_PER_30MIN};
use nxr_sdk::vol::VolConfig;
use nxr_sdk::vol_estimator::rs_sigma_from_ohlc;
use nxr_sdk::Bar;
use std::path::PathBuf;

use crate::vol_bin::VolWriter;

/// Streaming reader over a ticker's date-ordered `.s10` shards.
///
/// Bounded memory: one `ShardStream<Bar>` working buffer at a time. Feeds
/// [`build_vol_from_s10`] via [`Self::next_bar`]. Malformed shards are skipped
/// (best-effort: a single corrupt day must not abort the whole vol build).
pub struct S10ShardIter {
    shards: std::collections::VecDeque<PathBuf>,
    cur: Option<ShardStream<Bar>>,
}

impl S10ShardIter {
    /// `shards` = `(date, path)` pairs from `list_shards(dir, "s10")`, ascending.
    pub fn new<I>(shards: I) -> Self
    where
        I: IntoIterator<Item = (chrono::NaiveDate, PathBuf)>,
    {
        Self {
            shards: shards.into_iter().map(|(_, p)| p).collect(),
            cur: None,
        }
    }

    /// Next s10 `Bar` in chronological order, or `None` at end of all shards.
    pub fn next_bar(&mut self) -> Option<Bar> {
        loop {
            if let Some(stream) = self.cur.as_mut() {
                match stream.next() {
                    Ok(Some(bar)) => return Some(bar),
                    Ok(None) | Err(_) => {
                        self.cur = None;
                    }
                }
            }
            let path = self.shards.pop_front()?;
            self.cur = ShardStream::<Bar>::open(&path).ok();
        }
    }
}

/// Build a `.vol` file from a sequence of s10 `Bar`s.
///
/// `next_bar` yields s10 bars in chronological order (returns `None` at EOF) —
/// e.g. draining a `ShardStream<Bar>` over the daily `.s10` shards. Returns the
/// number of `.vol` rows written (one per 30-min bin).
pub fn build_vol_from_s10<F>(
    mut next_bar: F,
    vol_cfg: &VolConfig,
    writer: &mut VolWriter,
) -> Result<usize>
where
    F: FnMut() -> Option<Bar>,
{
    // Collect s10 candles, then roll up to 30-min OHLC bins. The rollup monoid
    // (first/max/min/last) is byte-identical to the live ring's intra-bin OHLC.
    let mut s10: Vec<Ohlc> = Vec::new();
    while let Some(bar) = next_bar() {
        let o = bar_to_ohlc(&bar);
        if !(o.open > 0.0 && o.high > 0.0 && o.low > 0.0 && o.close > 0.0) {
            continue;
        }
        s10.push(o);
    }
    let bins = rollup(&s10, BAR_MS_S10, MS_PER_30MIN);
    write_vol_records_from_ohlc(&bins, vol_cfg, writer)
}

/// Build a `.vol` file from a stream of `(epoch_ms, mid)` price observations.
///
/// Reconstructs the gapless 10s-OHLC mid series in-memory (flat-fill on quiet
/// 10s buckets, matching the live `bars_s10` producer), rolls it up to 30-min
/// OHLC, then writes EMA-smoothed RS σ rows. Use this from offline bins that
/// have a mid/tick stream but no persisted `.s10` (research/sweep tools, synth
/// reconstruction). Returns the number of `.vol` rows written.
pub fn build_vol_from_mid_ticks<I>(
    mids: I,
    vol_cfg: &VolConfig,
    writer: &mut VolWriter,
) -> Result<usize>
where
    I: IntoIterator<Item = (i64, f64)>,
{
    use std::collections::BTreeMap;
    let mut s10: BTreeMap<i64, Ohlc> = BTreeMap::new();
    for (ts, mid) in mids {
        if !(mid.is_finite() && mid > 0.0) {
            continue;
        }
        let key = (ts / BAR_MS_S10) * BAR_MS_S10;
        s10.entry(key)
            .and_modify(|o| {
                if mid > o.high {
                    o.high = mid;
                }
                if mid < o.low {
                    o.low = mid;
                }
                o.close = mid;
                o.tick_count = o.tick_count.saturating_add(1);
            })
            .or_insert(Ohlc {
                ts: key,
                close_ts: key + BAR_MS_S10 - 1,
                open: mid,
                high: mid,
                low: mid,
                close: mid,
                vbid: 0,
                vask: 0,
                tick_count: 1,
                avg_ci_ubp: 0,
            });
    }
    // Gapless flat-fill across empty 10s buckets.
    let mut series: Vec<Ohlc> = Vec::with_capacity(s10.len());
    let mut prev_ts: Option<i64> = None;
    let mut last_close = 0.0;
    for (&ts, &o) in &s10 {
        if let Some(pt) = prev_ts {
            let mut b = pt + BAR_MS_S10;
            while b < ts {
                series.push(Ohlc {
                    ts: b,
                    close_ts: b + BAR_MS_S10 - 1,
                    open: last_close,
                    high: last_close,
                    low: last_close,
                    close: last_close,
                    vbid: 0,
                    vask: 0,
                    tick_count: 0,
                    avg_ci_ubp: 0,
                });
                b += BAR_MS_S10;
            }
        }
        series.push(o);
        last_close = o.close;
        prev_ts = Some(ts);
    }
    let bins = rollup(&series, BAR_MS_S10, MS_PER_30MIN);
    write_vol_records_from_ohlc(&bins, vol_cfg, writer)
}

/// Write EMA-smoothed RS σ rows from a slice of 30-min OHLC bins (ts-ascending).
///
/// Shared finalize path: σ = RS(o,h,l,c) per bin, then expanding-mean-seeded
/// EMA(`ema_period`). Exposed so callers that already hold rolled-up 30-min
/// OHLC bins can write `.vol` without re-streaming s10.
pub fn write_vol_records_from_ohlc(
    bins: &[Ohlc],
    vol_cfg: &VolConfig,
    writer: &mut VolWriter,
) -> Result<usize> {
    let ema_period = vol_cfg.ema_period.max(1);
    let alpha = 2.0 / (ema_period as f64 + 1.0);
    let mut prev_ema: Option<f64> = None;
    let mut count = 0usize;

    for (i, bin) in bins.iter().enumerate() {
        let sigma = rs_sigma_from_ohlc(bin.open, bin.high, bin.low, bin.close);
        let ema = if i < ema_period {
            bins[..=i]
                .iter()
                .map(|b| rs_sigma_from_ohlc(b.open, b.high, b.low, b.close))
                .sum::<f64>()
                / (i + 1) as f64
        } else {
            alpha * sigma + (1.0 - alpha) * prev_ema.unwrap_or(sigma)
        };
        prev_ema = Some(ema);
        writer.write_record(timestamp::from_epoch_ms(bin.ts), ema)?;
        count += 1;
    }

    Ok(count)
}
