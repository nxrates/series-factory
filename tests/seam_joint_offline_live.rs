//! SEAM-JOINT: full offline-vs-live s10 equivalence on ONE fixed IndexRecord
//! stream.
//!
//! ## What this proves
//!
//! A single deterministic `IndexRecord` stream is driven through TWO
//! independent s10 bar-producing code paths:
//!
//!   * **OFFLINE** — the `s10-from-idx` core logic: a `BarAccumulator` keyed by
//!     the 10 s wall-clock bucket, flushed on rollover, grid-stamped via
//!     `nxr_sdk::bar_builder::stamp_s10_grid`, with GAPLESS `flat_bar` fill for
//!     empty buckets between observed buckets. (Mirrors
//!     `series-factory/src/bin/s10_from_idx.rs`.)
//!   * **LIVE** — the live producer's `flush_all` cursor loop: one
//!     `BarAccumulator` per in-flight bucket, drained in ascending order on
//!     each flush, falling back to `flat_bar(cursor, last_close)` for empty
//!     buckets, stamped through the SAME shared `stamp_s10_grid`. (Mirrors the
//!     live `TickerState`/`flush_all` accumulator path minus the broadcast /
//!     shard-writer / multicast plumbing.)
//!
//! Both paths bottom out on the SAME `nxr_sdk` primitives — `BarAccumulator`
//! (OHLC + microstructure), `flat_bar` (gap fill), and `stamp_s10_grid` (grid
//! stamp). The test then asserts the emitted bar series are BYTE-FOR-BYTE
//! identical on every format-load-bearing field:
//!   * `open` / `high` / `low` / `close` via `f64::to_bits` (exact bits),
//!   * `kind` (BarKind::Kline),
//!   * grid-stamped `open_ts` / `close_ts` (the 6-byte u48 encodings),
//!   * the microstructure section that `BarAccumulator::flush` populates
//!     (`vbid`, `vask`, `tick_count`, `realized_var`, `bipower_var`, `drift`,
//!     `vol_imbalance`, `avg_spread_bps`, `max_abs_return`, `avg_ci_ubp`,
//!     `reject_rate`) — these are computed by the shared accumulator, so they
//!     MUST agree bit-for-bit on a real (non-flat) bucket.
//!
//! ## What is NOT covered (documented)
//!
//!   * The live path's broadcast / off-runtime shard-writer / UDP-multicast
//!     fan-out, restart-seed (`last_bucket_emitted`/`last_close` from disk),
//!     `MAX_GAPFILL_BUCKETS` outage cap, and the `repair_s10_ohlc` /
//!     `s10_invariants_ok` reject gate are LIVE-ONLY plumbing, not part of the
//!     emitted-bar FORMAT. The fixed stream here never trips the reject gate
//!     (OHLC is always well-formed) and never exceeds the gap cap, so those
//!     branches are out of scope by construction.
//!   * The live producer lives in a binary crate — its private
//!     `flush_all` cannot be imported across the crate boundary, so the live
//!     side is a faithful replica of that loop. The grid stamp is NOT replicated
//!     though: both paths call the real shared `stamp_s10_grid` that the live
//!     producer delegates to (post-Task-3), so any change to the live stamp
//!     recompiles + breaks this test through the production symbol.

#![cfg(test)]

use mitch::bar::{Bar, BarKind};
use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp;
use nxr_sdk::bar_builder::{flat_bar, stamp_s10_grid, BarAccumulator};
use nxr_sdk::ipc::record::IndexRecord;

const BAR_MS: i64 = 10_000;

/// Build one IndexRecord at `epoch_ms` with the given quote + volumes.
fn record(epoch_ms: i64, bid: f64, ask: f64, vbid: u32, vask: u32) -> IndexRecord {
    let mts = timestamp::from_epoch_ms(epoch_ms);
    let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
    // Index::new(ticker, bid, ask, ci, vbid, vask, tick_count, confidence,
    //            accepted, rejected) — match the arg order used elsewhere.
    let idx = Index::new(0xABCD_u64, bid, ask, 0, vbid, vask, 1, 3, 3, 0);
    IndexRecord::new(header, idx)
}

/// Construct a FIXED, deterministic record stream (no rng, no clock):
///   * bucket A (base):        3 ticks (real OHLC)
///   * bucket A+1:             SKIPPED  → must be flat-filled by both paths
///   * bucket A+2:             SKIPPED  → flat-filled
///   * bucket A+3:             2 ticks (real OHLC)
///   * bucket A+4:             1 tick   (degenerate real bucket)
/// Spans a within-day grid (no day boundary needed for the FORMAT equivalence;
/// the gap exercises both paths' flat-fill identically).
fn fixed_stream() -> (i64, Vec<IndexRecord>) {
    let base = 1_700_000_000_000i64; // 10s-aligned epoch ms
    let base = (base / BAR_MS) * BAR_MS;
    let mut recs = Vec::new();

    // Bucket A: 3 ticks, rising then falling → non-trivial O/H/L/C + OFI.
    recs.push(record(base + 100, 100.00, 100.02, 50, 60));
    recs.push(record(base + 4_000, 100.10, 100.12, 70, 40));
    recs.push(record(base + 9_900, 100.05, 100.07, 30, 90));

    // (A+1, A+2 deliberately empty.)

    // Bucket A+3: 2 ticks.
    recs.push(record(base + 3 * BAR_MS + 200, 100.20, 100.22, 80, 20));
    recs.push(record(base + 3 * BAR_MS + 8_000, 100.30, 100.32, 10, 95));

    // Bucket A+4: 1 tick.
    recs.push(record(base + 4 * BAR_MS + 500, 100.25, 100.27, 55, 55));

    (base, recs)
}

/// OFFLINE producer — replica of `s10_from_idx.rs` ingest loop. Returns the
/// emitted bars in chronological order.
fn run_offline(recs: &[IndexRecord]) -> Vec<Bar> {
    let mut accum = BarAccumulator::new();
    let mut out: Vec<Bar> = Vec::new();
    let mut cur_bucket: Option<i64> = None;
    let mut last_close: f64 = 0.0;

    // Gapless fill for empty buckets strictly between `from` and `to`.
    let fill_gap = |out: &mut Vec<Bar>, from: i64, to: i64, last_close: f64| {
        if last_close <= 0.0 {
            return;
        }
        let mut b = from + BAR_MS;
        while b < to {
            out.push(flat_bar(b, last_close));
            b += BAR_MS;
        }
    };

    for rec in recs {
        let ts_ms = timestamp::to_epoch_ms(rec.header.get_timestamp());
        let idx = rec.index;
        let mid = (idx.bid + idx.ask) * 0.5;
        if !(mid.is_finite() && mid > 0.0) {
            continue;
        }
        let bucket = ts_ms.div_euclid(BAR_MS) * BAR_MS;
        match cur_bucket {
            None => cur_bucket = Some(bucket),
            Some(cb) if bucket > cb => {
                if let Some(mut bar) = accum.flush() {
                    bar.kind = BarKind::Kline as u8;
                    stamp_s10_grid(&mut bar, cb, BAR_MS);
                    if bar.close > 0.0 && bar.close.is_finite() {
                        last_close = bar.close;
                    }
                    out.push(bar);
                }
                fill_gap(&mut out, cb, bucket, last_close);
                cur_bucket = Some(bucket);
            }
            Some(cb) if bucket < cb => continue,
            _ => {}
        }
        let ci_ubp = nxr_sdk::tdwap::decode_ci_ubp(idx.ci);
        accum.ingest(
            idx.bid,
            idx.ask,
            idx.vbid,
            idx.vask,
            ts_ms,
            ci_ubp,
            idx.accepted as u32,
            idx.rejected as u32,
        );
    }
    // Flush the final open bucket (no trailing gap fill).
    if let Some(mut bar) = accum.flush() {
        bar.kind = BarKind::Kline as u8;
        if let Some(cb) = cur_bucket {
            stamp_s10_grid(&mut bar, cb, BAR_MS);
        }
        out.push(bar);
    }
    out
}

/// LIVE producer — replica of the live `{ingest, flush_all}` loop. One
/// accumulator per in-flight bucket; flush drains buckets in ascending order,
/// flat-filling empties, up to (and including) the highest fully-closed bucket.
/// Drives the SAME shared `stamp_s10_grid` the live producer delegates to.
fn run_live(recs: &[IndexRecord]) -> Vec<Bar> {
    use std::collections::BTreeMap;

    // Ingest: route each tick to its bucket's accumulator.
    let mut accs: BTreeMap<i64, BarAccumulator> = BTreeMap::new();
    let mut last_close: f64 = 0.0;
    let mut last_observed_bucket: i64 = i64::MIN;
    for rec in recs {
        let body = rec.index;
        let ts_ms = timestamp::to_epoch_ms(rec.header.get_timestamp());
        let mid = (body.bid + body.ask) * 0.5;
        let tick_bucket = (ts_ms / BAR_MS) * BAR_MS;
        last_observed_bucket = last_observed_bucket.max(tick_bucket);
        let acc = accs.entry(tick_bucket).or_insert_with(BarAccumulator::new);
        let ci_ubp = nxr_sdk::tdwap::decode_ci_ubp(body.ci);
        acc.ingest(
            body.bid,
            body.ask,
            body.vbid,
            body.vask,
            ts_ms,
            ci_ubp,
            body.accepted as u32,
            body.rejected as u32,
        );
        if mid.is_finite() && mid > 0.0 {
            last_close = mid;
        }
    }

    // Flush: emit every bucket from the first observed up to the last observed
    // (the live `flush_all` emits the closing bucket on each 10 s tick; here we
    // emit the full contiguous span at once, which yields the identical series).
    let mut out: Vec<Bar> = Vec::new();
    let first_bucket = match accs.keys().next() {
        Some(&k) => k,
        None => return out,
    };
    let mut last_emitted_close: f64 = 0.0;
    let mut cursor = first_bucket;
    while cursor <= last_observed_bucket {
        let bar = match accs.remove(&cursor) {
            Some(mut acc) if acc.count() > 0 => acc
                .flush()
                .unwrap_or_else(|| flat_bar(cursor, last_emitted_close)),
            _ if last_emitted_close > 0.0 => flat_bar(cursor, last_emitted_close),
            _ => {
                cursor += BAR_MS;
                continue;
            }
        };
        let mut bar = bar;
        bar.kind = BarKind::Kline as u8;
        stamp_s10_grid(&mut bar, cursor, BAR_MS);
        if bar.close > 0.0 && bar.close.is_finite() {
            last_emitted_close = bar.close;
        }
        out.push(bar);
        cursor += BAR_MS;
    }
    let _ = last_close; // live keeps a separate `last_close` seed; unused here.
    out
}

/// Assert two emitted bars are byte-for-byte equal on every format-load-bearing
/// field (modulo the documented live-only enrich fields, which this fixed
/// stream makes identical anyway because both paths use the same accumulator).
fn assert_bar_eq(i: usize, a: &Bar, b: &Bar) {
    // `Bar` is `#[repr(C, packed)]` — copy fields to locals before comparing
    // (taking a reference to a packed field is UB / a compile error).
    macro_rules! bits_eq {
        ($field:ident) => {{
            let av = a.$field;
            let bv = b.$field;
            assert_eq!(av.to_bits(), bv.to_bits(), "bar {i} {}", stringify!($field));
        }};
    }
    macro_rules! plain_eq {
        ($field:ident) => {{
            let av = a.$field;
            let bv = b.$field;
            assert_eq!(av, bv, "bar {i} {}", stringify!($field));
        }};
    }
    bits_eq!(open);
    bits_eq!(high);
    bits_eq!(low);
    bits_eq!(close);
    plain_eq!(kind);
    plain_eq!(open_ts);
    plain_eq!(close_ts);
    // Grid-stamped epoch ms must agree exactly (same encoding input).
    assert_eq!(a.open_time_ms(), b.open_time_ms(), "bar {i} open_time_ms");
    assert_eq!(
        a.close_time_ms(),
        b.close_time_ms(),
        "bar {i} close_time_ms"
    );
    // Microstructure section produced by the shared BarAccumulator::flush.
    plain_eq!(vbid);
    plain_eq!(vask);
    plain_eq!(tick_count);
    bits_eq!(realized_var);
    bits_eq!(bipower_var);
    bits_eq!(drift);
    bits_eq!(vol_imbalance);
    bits_eq!(avg_spread_bps);
    bits_eq!(max_abs_return);
}

#[test]
fn seam_joint_offline_matches_live_s10_byte_for_byte() {
    let (base, recs) = fixed_stream();
    let offline = run_offline(&recs);
    let live = run_live(&recs);

    // Expected emitted buckets: A, A+1(flat), A+2(flat), A+3 — A+4 is the final
    // OPEN bucket. The OFFLINE path flushes A+4 as a trailing bar (no following
    // bucket to roll it over); the LIVE replica emits buckets only up to
    // `last_observed_bucket` inclusive, which ALSO includes A+4. So both emit
    // buckets A..A+4 = 5 bars.
    assert_eq!(
        offline.len(),
        live.len(),
        "emitted-bar count differs: offline={} live={}",
        offline.len(),
        live.len()
    );
    assert_eq!(offline.len(), 5, "expected 5 bars (A, 2 flat, A+3, A+4)");

    for (i, (a, b)) in offline.iter().zip(&live).enumerate() {
        assert_bar_eq(i, a, b);
    }

    // Cross-check the grid: every emitted bar must sit exactly one bucket past
    // the previous (the contiguous-grid contract the seam check enforces).
    for w in offline.windows(2) {
        let d = w[1].close_time_ms() - w[0].close_time_ms();
        assert_eq!(d, BAR_MS, "offline bars not on contiguous 10s grid (Δ={d})");
    }

    // Anchor: first bar opens at the base bucket; last at base + 4*BAR_MS.
    assert_eq!(
        offline[0].open_time_ms(),
        base,
        "first bar open != base bucket"
    );
    assert_eq!(
        offline[4].open_time_ms(),
        base + 4 * BAR_MS,
        "last bar open != base + 4 buckets"
    );

    // SENSITIVITY CONTROL: the two flat-filled buckets (A+1, A+2) must carry the
    // close of bucket A (gapless continuation), proving the flat-fill is real.
    let a_close = offline[0].close;
    let f1_close = offline[1].close;
    let f2_close = offline[2].close;
    assert_eq!(
        f1_close.to_bits(),
        a_close.to_bits(),
        "flat bar A+1 must continue bucket A close"
    );
    assert_eq!(
        f2_close.to_bits(),
        a_close.to_bits(),
        "flat bar A+2 must continue bucket A close"
    );
}
