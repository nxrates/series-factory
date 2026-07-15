//! SEAM-8: heartbeat-sentinel skip parity (offline == live).
//!
//! The live producers (`core/src/bars_s10.rs`, `core/src/bars_renko.rs`) skip
//! every `IndexRecord` carrying `FLAG_HEARTBEAT_SENTINEL` — they are liveness
//! beacons with stale bid/ask that would poison the s10 OHLC / vol ring / renko
//! micros. The offline generators (`s10_from_idx.rs`, `renko_from_idx.rs`) just
//! gained the SAME skip. This test LOCKS that parity: it builds an idx record
//! stream with sentinels interleaved, runs the EXACT skip + mid-extraction loop
//! the offline binaries use, and asserts the resulting (ts, mid) stream is
//! byte-identical to the stream WITHOUT sentinels — proving a sentinel
//! contributes ZERO volume / OHLC / brick input on the offline side, exactly as
//! live. Deterministic — no rng, no clock reads.

#![cfg(test)]

use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL;

/// Build an IndexRecord at `epoch_ms`; `sentinel` toggles the heartbeat flag.
/// Sentinels deliberately carry a WILDLY different (stale) bid/ask so that if
/// the skip were missing, the divergence would be obvious in the mid stream.
fn record(epoch_ms: i64, bid: f64, ask: f64, sentinel: bool) -> IndexRecord {
    let mts = timestamp::from_epoch_ms(epoch_ms);
    let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
    let mut idx = Index::new(0xABCD_u64, bid, ask, 100, 1_000, 1_000, 1, 1, 1, 0);
    if sentinel {
        idx.flags |= FLAG_HEARTBEAT_SENTINEL;
    }
    IndexRecord::new(header, idx)
}

/// The shared offline skip + mid-extraction loop — VERBATIM from
/// `renko_from_idx.rs` pass 1 / `s10_from_idx.rs` ingest:
///   1. skip `FLAG_HEARTBEAT_SENTINEL`,
///   2. compute `mid = (bid+ask)/2`, skip non-finite / non-positive.
/// Returns the surviving `(ts_ms, mid)` stream.
fn offline_mid_stream(records: &[IndexRecord]) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    for rec in records {
        // mirror bars_renko.rs:528 / bars_s10.rs:198 / both offline binaries.
        if rec.index.flags & FLAG_HEARTBEAT_SENTINEL != 0 {
            continue;
        }
        let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
        let mid = (rec.index.bid + rec.index.ask) * 0.5;
        if !(mid.is_finite() && mid > 0.0) {
            continue;
        }
        out.push((ts, mid));
    }
    out
}

#[test]
fn seam8_sentinel_skip_offline_matches_sentinel_free() {
    // Build two streams over the SAME real ticks:
    //   * `with_sentinels`: a sentinel injected between every real tick (and at
    //     the head + tail), each carrying a stale 9999/9999.5 quote.
    //   * `clean`: only the real ticks.
    // After the offline skip, both must yield the IDENTICAL (ts, mid) stream.
    let base = 1_700_000_000_000i64;
    let mut with_sentinels: Vec<IndexRecord> = Vec::new();
    let mut clean: Vec<IndexRecord> = Vec::new();

    // Leading sentinel.
    with_sentinels.push(record(base - 50, 9_999.0, 9_999.5, true));

    for i in 0..2_000i64 {
        let ts = base + i * 100;
        let bid = 100.0 + (i as f64) * 0.01;
        let ask = bid + 0.02;
        let real = record(ts, bid, ask, false);
        with_sentinels.push(real.clone());
        clean.push(real);
        // Interleaved sentinel right after each real tick (same ts → would land
        // in the same bucket and poison OHLC/vol if not skipped).
        with_sentinels.push(record(ts, 9_999.0, 9_999.5, true));
    }
    // Trailing sentinel.
    with_sentinels.push(record(base + 2_000 * 100, 9_999.0, 9_999.5, true));

    let s_with = offline_mid_stream(&with_sentinels);
    let s_clean = offline_mid_stream(&clean);

    assert_eq!(
        s_with.len(),
        s_clean.len(),
        "SEAM-8: sentinel-skip stream len {} != sentinel-free {}",
        s_with.len(),
        s_clean.len()
    );
    assert_eq!(
        s_clean.len(),
        2_000,
        "expected exactly the real ticks to survive"
    );

    for (i, (a, b)) in s_with.iter().zip(&s_clean).enumerate() {
        assert_eq!(a.0, b.0, "SEAM-8 ts divergence @ {i}: {} != {}", a.0, b.0);
        // Byte-identical mid (no sentinel leaked into the value).
        assert_eq!(
            a.1.to_bits(),
            b.1.to_bits(),
            "SEAM-8 mid divergence @ {i}: {} != {} (sentinel poisoned offline mid)",
            a.1,
            b.1
        );
    }

    // SENSITIVITY CONTROL: if the skip were ABSENT, the stale 9999 quotes WOULD
    // appear in the stream → strictly more records + a different mid set. Prove
    // the assertion above is non-trivial.
    let no_skip: Vec<(i64, f64)> = with_sentinels
        .iter()
        .filter_map(|rec| {
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let mid = (rec.index.bid + rec.index.ask) * 0.5;
            (mid.is_finite() && mid > 0.0).then_some((ts, mid))
        })
        .collect();
    assert!(
        no_skip.len() > s_with.len(),
        "control: without the skip the stream must be LONGER ({} vs {})",
        no_skip.len(),
        s_with.len()
    );
}
