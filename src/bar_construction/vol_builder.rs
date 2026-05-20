//! Canonical 30-min Parkinson sigma builder.
//!
//! Single source of truth for the `.vol` file construction shared by every
//! offline pipeline (`nxr-calibrate`, `generate-renko-from-ticks`,
//! `optimize-renko-stats`). Previously each call site re-implemented the same
//! 30-min HLC bucketing + EMA-smoothed Parkinson sigma loop, which caused at
//! least one bug (`optimize_renko_stats` used 1h buckets + hardcoded ema=14).
//!
//! Canonical parameters:
//!   - Bucket width: 30 minutes (1_800_000 ms). Matches `VolMmap` consumer.
//!   - EMA period:   `vol_cfg.ema_period` (default 28, see `VolConfig`).
//!
//! The bucket H/L are derived from raw mid prices supplied by the caller.
//! Sigma is computed via `parkinson_sigma(high, low)` then EMA-smoothed.
//! The first `ema_period` samples use a simple expanding mean to seed the EMA.

use anyhow::Result;
use mitch::timestamp;
use nxr_sdk::{ipc::record::IndexRecord, parkinson_sigma};
use std::collections::BTreeMap;

use crate::bar_construction::VolConfig;
use crate::vol_bin::VolWriter;

/// 30-min bucket width in milliseconds.
pub const BUCKET_MS: i64 = 1_800_000;

/// Build a `.vol` file from a stream of `IndexRecord`s.
///
/// Streams records from `next_record` (returns `None` at EOF), aggregates
/// them into 30-minute HLC buckets using `mid = (bid + ask) / 2` (with H/L
/// taken from `ask` / `bid` respectively, matching `nxr_calibrate.rs`
/// semantics), then writes EMA-smoothed Parkinson sigma rows to `writer`.
///
/// Returns the number of records written.
pub fn build_vol_from_records<F>(
    mut next_record: F,
    vol_cfg: &VolConfig,
    writer: &mut VolWriter,
) -> Result<usize>
where
    F: FnMut() -> Option<IndexRecord>,
{
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();

    while let Some(rec) = next_record() {
        let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
        let bid = rec.index.bid;
        let ask = rec.index.ask;
        let mid = (bid + ask) * 0.5;
        if !(mid.is_finite() && mid > 0.0) {
            continue;
        }

        let key = (ts / BUCKET_MS) * BUCKET_MS;
        let entry = hlc.entry(key).or_insert((ask.max(mid), bid.min(mid)));
        if ask > entry.0 {
            entry.0 = ask;
        }
        if bid < entry.1 && bid > 0.0 {
            entry.1 = bid;
        }
    }

    write_vol_records_from_hlc(&hlc, vol_cfg, writer)
}

/// Build a `.vol` file from prebuilt 30-min HLC buckets.
///
/// Use when the caller has already aggregated raw ticks into HLC buckets
/// (e.g. `generate_renko_from_ticks.rs`, `optimize_renko_stats.rs`).
/// Buckets must be keyed by 30-min-aligned epoch_ms.
pub fn build_vol_from_hlc(
    hlc: &BTreeMap<i64, (f64, f64)>,
    vol_cfg: &VolConfig,
    writer: &mut VolWriter,
) -> Result<usize> {
    write_vol_records_from_hlc(hlc, vol_cfg, writer)
}

fn write_vol_records_from_hlc(
    hlc: &BTreeMap<i64, (f64, f64)>,
    vol_cfg: &VolConfig,
    writer: &mut VolWriter,
) -> Result<usize> {
    let hours: Vec<(i64, f64, f64)> = hlc.iter().map(|(&ts, &(h, l))| (ts, h, l)).collect();
    let ema_period = vol_cfg.ema_period.max(1);
    let alpha = 2.0 / (ema_period as f64 + 1.0);
    let mut prev_ema: Option<f64> = None;
    let mut count = 0usize;

    for (i, &(ts, high, low)) in hours.iter().enumerate() {
        let sigma = parkinson_sigma(high, low);
        let ema = if i < ema_period {
            hours[..=i].iter().map(|&(_, h, l)| parkinson_sigma(h, l)).sum::<f64>()
                / (i + 1) as f64
        } else {
            alpha * sigma + (1.0 - alpha) * prev_ema.unwrap_or(sigma)
        };
        prev_ema = Some(ema);
        writer.write_record(timestamp::from_epoch_ms(ts), ema)?;
        count += 1;
    }

    Ok(count)
}
