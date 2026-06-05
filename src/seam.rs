//! Cross-shard SEAM continuity checks — the production invariant shared by the
//! `renko-continuity-check` binary AND the `data-quality-audit` cert.
//!
//! These functions encode the day-boundary continuity contract that
//! `integrity-check bars` (single-shard only) cannot see:
//!
//!   * **Renko B03**: `close[shard_D.last] == open[shard_D+1.first]` to a tight
//!     relative tolerance (1e-9). The live producer guarantees the body
//!     `close[i-1] == open[i]` invariant within a shard; B03 extends it ACROSS
//!     the day rotation so the live+offline series append into ONE seamless
//!     chart with no price step at midnight.
//!   * **s10 grid continuity**: `close_ts[shard_D.last] + BAR_MS == open_ts[shard_D+1.first]`.
//!     s10 bars are a contiguous 10s grid; across a day boundary the last
//!     bucket of day D closes at `D_end - 1` ms and the first bucket of D+1
//!     opens at `D_end` ms, so the close→open delta is exactly one bucket
//!     (`BAR_MS`). A mismatch means a dropped/duplicated bucket at the seam.
//!
//! Both checks take an ordered list of `(close_last, open_first, ...)` boundary
//! tuples so callers can feed either mmap'd shards (binary) or already-loaded
//! bars (cert) without this module owning the I/O.

use nxr_sdk::shard::BAR_MS_S10;
use nxr_sdk::Bar;

/// Relative tolerance for the renko B03 price-continuity check. Writer/reader
/// bytes are identical for the same `f64`, so in practice the delta is exactly
/// `0.0`; `1e-9` × `|close|` is a conservative band that still catches any real
/// brick-grid break while permitting fp round-trip noise.
pub const RENKO_B03_REL_TOL: f64 = 1e-9;

/// Absolute price floor shared by the structural microstructure invariants of
/// BOTH the `integrity-check` (G1) AND the `data-quality-audit` cert. Any
/// finite, technically-positive bid/ask below this cannot be a real quote (it
/// underflowed to a denormal); `Index::validate()` only rejects `<= 0.0`, so a
/// value like `1e-15` would slip through unless this floor is enforced. The two
/// validators MUST agree on the floor or the cutover gate and the per-file
/// checker would disagree on what "structurally corrupt price" means — hence a
/// single source here rather than a duplicated `1e-9` literal in each binary.
pub const MIN_PX: f64 = 1e-9;

/// Outcome of one cross-shard boundary continuity check.
#[derive(Debug, Clone, Copy)]
pub struct SeamBoundary {
    /// `close` (renko) or `close_ts` ms (s10) of the last bar in shard D.
    pub last: f64,
    /// `open` (renko) or `open_ts` ms (s10) of the first bar in shard D+1.
    pub first: f64,
    /// Signed delta `first - last` (price for renko; ms for s10 — already net
    /// of the expected one-bucket step).
    pub delta: f64,
    /// True when the boundary violates the continuity invariant beyond tol.
    pub violated: bool,
}

/// Renko B03 across one shard boundary: `open[D+1.first]` must equal
/// `close[D.last]` to within `RENKO_B03_REL_TOL × |close|` (abs floor 1e-15 so a
/// zero close can't make the band degenerate).
#[inline]
pub fn check_renko_cross_shard(prev_close: f64, next_open: f64) -> SeamBoundary {
    let delta = next_open - prev_close;
    let tol = (prev_close.abs() * RENKO_B03_REL_TOL).max(1e-15);
    SeamBoundary {
        last: prev_close,
        first: next_open,
        delta,
        violated: delta.abs() > tol,
    }
}

/// Convenience wrapper: B03 directly from the last `Bar` of shard D and the
/// first `Bar` of shard D+1. Copies fields out of the packed struct first.
#[inline]
pub fn check_renko_cross_shard_bars(prev_last: &Bar, next_first: &Bar) -> SeamBoundary {
    let prev_close = prev_last.close;
    let next_open = next_first.open;
    check_renko_cross_shard(prev_close, next_open)
}

/// Maximum tolerated jitter (ms) on the decoded s10 seam delta. The mitch
/// timestamp wire format quantizes to 16µs ticks (`mts = µs >> 4`), so the
/// `to_epoch_ms` round-trip of a `close_ts` can shift by ≤1 ms after integer
/// ms truncation. A real dropped/duplicated bucket is off by a full `BAR_MS`
/// (10_000 ms), so a 1 ms band cleanly separates grid jitter from a true
/// discontinuity.
pub const S10_SEAM_JITTER_MS: i64 = 1;

/// s10 grid continuity across one shard boundary. The first bucket of shard D+1
/// must be exactly one bucket (`BAR_MS_S10`) past the last bucket of shard D.
/// Expressed close-to-close (the SAME idiom `integrity-check` uses WITHIN a
/// shard: `close_ts[i] - close_ts[i-1] == bucket_ms`), which is identical to the
/// `close_ts[D.last] + BAR_MS == open_ts[D+1.first]` contract because
/// `open_ts == close_ts - (BAR_MS - 1)`: the `-1` cancels on both sides, making
/// this the quantization-stable form. `delta` is the residual
/// `(close_ts[D+1] - close_ts[D]) - BAR_MS` (≈0 ⇒ perfect grid; |delta| >
/// `S10_SEAM_JITTER_MS` ⇒ dropped/duplicated bucket at the day seam).
///
/// Takes BOTH shards' `close_ts` (epoch ms). The convenience wrapper
/// [`check_s10_cross_shard_bars`] derives them from the boundary `Bar`s.
#[inline]
pub fn check_s10_cross_shard(prev_close_ts_ms: i64, next_close_ts_ms: i64) -> SeamBoundary {
    let residual = (next_close_ts_ms - prev_close_ts_ms) - BAR_MS_S10;
    SeamBoundary {
        last: prev_close_ts_ms as f64,
        first: next_close_ts_ms as f64,
        delta: residual as f64,
        violated: residual.abs() > S10_SEAM_JITTER_MS,
    }
}

/// Convenience wrapper: s10 grid continuity directly from the last `Bar` of
/// shard D and the first `Bar` of shard D+1 (close-to-close).
#[inline]
pub fn check_s10_cross_shard_bars(prev_last: &Bar, next_first: &Bar) -> SeamBoundary {
    check_s10_cross_shard(prev_last.close_time_ms(), next_first.close_time_ms())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nxr_sdk::mitch::timestamp;

    fn mk_s10_bar(bucket_open_ms: i64, open: f64, close: f64) -> Bar {
        let open_mts = timestamp::from_epoch_ms(bucket_open_ms);
        let close_mts = timestamp::from_epoch_ms(bucket_open_ms + BAR_MS_S10 - 1);
        let mut b = Bar::new_ohlcv(open_mts, close_mts, open, open.max(close), open.min(close), close, 0, 0, 0);
        b.open_ts = timestamp::encode_u48(open_mts);
        b.close_ts = timestamp::encode_u48(close_mts);
        b
    }

    #[test]
    fn renko_b03_holds_when_equal() {
        let r = check_renko_cross_shard(84_234.5, 84_234.5);
        assert!(!r.violated, "identical close/open must not violate B03");
        assert_eq!(r.delta, 0.0);
    }

    #[test]
    fn renko_b03_violates_on_step() {
        // A 1-tick price step at the seam (well above 1e-9 rel tol).
        let r = check_renko_cross_shard(84_234.5, 84_240.0);
        assert!(r.violated, "a price step at the seam must violate B03");
    }

    #[test]
    fn renko_b03_tolerates_fp_noise() {
        let close = 0.003_47_f64;
        let noisy = close + close * 1e-10; // within 1e-9 band
        let r = check_renko_cross_shard(close, noisy);
        assert!(!r.violated, "sub-tol fp noise must not trip B03");
    }

    #[test]
    fn s10_grid_holds_across_boundary() {
        // Last bucket of day D, first bucket of day D+1, contiguous grid.
        let day_end = 1_700_000_000_000i64; // arbitrary 10s-aligned ms
        let last = mk_s10_bar(day_end - BAR_MS_S10, 100.0, 100.5);
        let first = mk_s10_bar(day_end, 100.5, 101.0);
        let r = check_s10_cross_shard_bars(&last, &first);
        assert!(!r.violated, "contiguous s10 grid must not violate (delta={})", r.delta);
        assert!(r.delta.abs() <= S10_SEAM_JITTER_MS as f64, "delta {} within jitter", r.delta);
    }

    #[test]
    fn s10_grid_violates_on_dropped_bucket() {
        let day_end = 1_700_000_000_000i64;
        let last = mk_s10_bar(day_end - BAR_MS_S10, 100.0, 100.5);
        // Skip one bucket: first opens at day_end + BAR_MS instead of day_end →
        // close-to-close delta is 2×BAR_MS, residual ≈ +BAR_MS.
        let first = mk_s10_bar(day_end + BAR_MS_S10, 100.5, 101.0);
        let r = check_s10_cross_shard_bars(&last, &first);
        assert!(r.violated, "a dropped bucket at the seam must violate s10 grid");
        assert!(
            (r.delta - BAR_MS_S10 as f64).abs() <= S10_SEAM_JITTER_MS as f64,
            "dropped-bucket residual {} should be ≈ +BAR_MS",
            r.delta
        );
    }
}
