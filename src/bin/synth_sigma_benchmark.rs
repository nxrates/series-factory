//! Empirical benchmark of synthetic cross-pair Parkinson σ methodology.
//!
//! Triggered by the 2026-05-26 renko-synth audit
//! (`docs/internal/renko-synth-audit-2026-05-26.md`): the current cross-pair
//! σ pipeline reads two leg .idx tick streams, bins each into 30-min HLC, then
//! computes Parkinson σ on the **leg-product mid at bucket-close**. Tanaka's
//! diagnosis is that this under-estimates the cross's true σ because leg
//! highs and lows rarely co-occur within the same 30-min window — the
//! synthesised H-L of the cross collapses toward the diagonal.
//!
//! This binary measures the under-estimate quantitatively by comparing three
//! σ estimators per day over a date range:
//!
//!   - **Method A** (current production proxy): cross_mid = leg_a_mid /
//!     leg_b_mid sampled at 30-min bucket-close timestamps; Parkinson σ on
//!     the H-L of the resulting 30-min bars (each bar's H/L is just its
//!     close — flat bars, no intra-bar range — so σ derives from the
//!     bucket-to-bucket diff; we approximate the production behaviour by
//!     building a per-bucket H/L from the bucket's leg-product mid AND the
//!     bucket open's leg-product mid). Equivalent to what `mtf_sweep.rs`
//!     would feed the calibrator if invoked on a synth cross.
//!
//!   - **Method B** (event-driven ground truth): min-heap merge both leg
//!     .idx tick streams by ts; on every tick of either leg, recompute
//!     cross_mid using last-known of the other leg; bucket synth ticks into
//!     30-min H-L bars (true H, true L within each bin); Parkinson σ.
//!
//!   - **Method C** (native cross-pair, if `indexes/<cross_id>/<date>.idx`
//!     exists with records): the cross's own native .idx, bucketed and σ'd
//!     identically to Method B. This is the gold reference.
//!
//! For each method, the binary also simulates what `calibrate_mtf_with_target`
//! produces (k) when fed that method's tick stream as input. The output
//! tells the operator:
//!   - how much the current method under-estimates σ (median ratio_B/A)
//!   - whether event-driven matches native (ratio_C/B near 1.0)
//!   - whether Method A drives k to the lower mult_bound (`0.01`) while
//!     Method B / C produce sane k in the 0.05-0.5 range.
//!
//! Parkinson formula (per bar):
//!   σ² = (1 / 4ln2) * (ln H/L)²
//! Daily σ = sqrt(mean of bar variances within the day).
//!
//! Runs on the cluster only — no local .idx data on the operator's Mac.
//! Build: `cargo check --bin synth-sigma-benchmark`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use clap::Parser;
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{
    idx_dir, list_shards, ShardStream, MS_PER_30MIN, MS_PER_DAY,
};
use series_factory::bar_construction::{
    build_vol_from_hlc, calibrate_mtf_with_target, CalibrationConfig,
};
use nxr_sdk::parkinson::{MtfParkinsonCalculator, VolConfig};
use nxr_sdk::renko::RenkoConfig;
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

const LN2: f64 = std::f64::consts::LN_2;

/// Parkinson per-bar variance: σ² = (ln(H/L))² / (4 ln 2).
#[inline]
fn parkinson_var(high: f64, low: f64) -> f64 {
    if high <= 0.0 || low <= 0.0 || high < low {
        return 0.0;
    }
    let lhl = (high / low).ln();
    (lhl * lhl) / (4.0 * LN2)
}

#[inline]
fn day_start_ms(d: NaiveDate) -> i64 {
    let ndt = NaiveDateTime::new(d, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    Utc.from_utc_datetime(&ndt).timestamp_millis()
}

use nxr_sdk::shard::parse_utc_date as parse_date;

#[derive(Parser, Debug)]
#[command(
    about = "Benchmark current synth cross-pair Parkinson σ (Method A) against \
             event-driven synth (Method B) and native (Method C). Outputs a \
             daily ratio table + calibrator-simulated k for each method."
)]
struct Args {
    /// MITCH ticker id of leg A (numerator). e.g. ETH/USDT = 650828851044155392.
    #[arg(long)]
    leg_a: u64,

    /// MITCH ticker id of leg B (denominator). e.g. BTC/USDT = 435315775907037184.
    #[arg(long)]
    leg_b: u64,

    /// MITCH ticker id of the cross pair (e.g. ETH/BTC = 650828835420372992).
    /// Used only for Method C (native ground truth, if shards exist).
    #[arg(long)]
    cross: u64,

    /// Data root that contains `indexes/<id>/<date>.idx` shards.
    /// Defaults to the `NXR_DATA_ROOT` env var or `/data` if unset.
    #[arg(long)]
    data_root: Option<PathBuf>,

    /// Inclusive UTC start date (YYYY-MM-DD).
    #[arg(long)]
    from: String,

    /// Inclusive UTC end date (YYYY-MM-DD).
    #[arg(long)]
    to: String,

    /// Target bpd for the calibrator simulation. Default 100 (cross-pair
    /// target proposed by the audit). Override e.g. 300 to see what the
    /// pre-audit configuration would have produced.
    #[arg(long, default_value_t = 100.0_f64)]
    target_bpd: f64,

    /// Lower mult_bound for the calibrator binary search. Default 0.01 —
    /// matches `config.yml`. Method A is expected to clamp here.
    #[arg(long, default_value_t = 0.01_f64)]
    mult_lo: f64,

    /// Upper mult_bound for the calibrator binary search. Default 10.0.
    #[arg(long, default_value_t = 10.0_f64)]
    mult_hi: f64,

    /// MTF windows (days) for the calibrator simulation, comma-separated.
    /// Default "30,60,120" — the shipped post-revert config.
    #[arg(long, default_value = "30,60,120")]
    k_fit_windows_days: String,

    /// If set, emit per-day rows as JSON on stdout in addition to the table.
    #[arg(long, default_value_t = false)]
    json: bool,
}

/// Streaming snapshot: ts (epoch ms), bid, ask, mid.
#[derive(Debug, Clone, Copy)]
struct LegTick {
    ts: i64,
    mid: f64,
    bid: f64,
    ask: f64,
}

/// Load ticks from shards within `[from_date, to_date]` (inclusive), sorted
/// ascending by ts. Returns `Vec<LegTick>` so downstream consumers can iterate
/// freely. Shards outside the range are skipped at the filename level to keep
/// peak RAM proportional to the requested window — not to total history.
fn load_leg_ticks(
    data_root: &Path,
    ticker_id: u64,
    from_date: NaiveDate,
    to_date: NaiveDate,
) -> Result<Vec<LegTick>> {
    let dir = idx_dir(data_root, ticker_id);
    let shards = list_shards(&dir, "idx")
        .with_context(|| format!("list shards {}", dir.display()))?;
    if shards.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for (d, path) in shards {
        if d < from_date || d > to_date {
            continue;
        }
        let mut stream = ShardStream::<IndexRecord>::open(&path)
            .with_context(|| format!("open idx {}", path.display()))?;
        while let Some(rec) = stream.next()? {
            let header = rec.header;
            let ts = timestamp::to_epoch_ms(header.get_timestamp());
            let body = rec.index;
            let bid = body.bid;
            let ask = body.ask;
            if !(bid.is_finite() && ask.is_finite()) {
                continue;
            }
            if bid <= 0.0 || ask <= 0.0 {
                continue;
            }
            let mid = (bid + ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                continue;
            }
            out.push(LegTick { ts, mid, bid, ask });
        }
    }
    // Per-shard order is ascending by construction (delta-gated writer) but
    // we don't trust cross-shard splices implicitly — sort once.
    out.sort_by_key(|t| t.ts);
    Ok(out)
}

/// Build 30-min H/L cross bars using Method A semantics:
///
///   For each 30-min bucket key K (epoch ms, aligned), find the last leg-A
///   tick with ts ∈ [K, K + MS_PER_30MIN) and last leg-B tick in the same
///   interval. Cross_close(K) = leg_a_close / leg_b_close. Method A then
///   takes "high" and "low" of the bucket as the cross_close at bucket
///   OPEN and bucket CLOSE (because the production pipeline only retains
///   one snapshot per bucket, so the bar is effectively flat). To make
///   Method A a fair representation of the production proxy, we treat the
///   sequence of bucket-close cross_mid values as the only available
///   high-fidelity signal: H/L per bucket = (max, min) of that bucket's
///   leg_a_mid / leg_b_mid evaluated at the two snapshot times we'd have
///   in the production flow: bucket OPEN (last tick ≤ K) and bucket CLOSE
///   (last tick ≤ K + MS_PER_30MIN - 1).
///
///   This mirrors `mtf_sweep.rs:359-389`'s 30-min HLC build, which uses
///   `ask.max(mid)` / `bid.min(mid)` only — i.e., quote-derived range
///   from a single bucket — and is the exact path that
///   `build_vol_from_hlc` consumes when fed a synthetic cross.
fn build_method_a_hlc(
    leg_a: &[LegTick],
    leg_b: &[LegTick],
    from_ms: i64,
    to_ms_excl: i64,
) -> BTreeMap<i64, (f64, f64)> {
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    if leg_a.is_empty() || leg_b.is_empty() {
        return hlc;
    }
    let mut bucket = (from_ms / MS_PER_30MIN) * MS_PER_30MIN;
    if bucket < from_ms {
        bucket += MS_PER_30MIN;
    }
    while bucket < to_ms_excl {
        // last tick at or before bucket open
        let open_ts = bucket;
        // last tick at or before bucket close - 1
        let close_ts = bucket + MS_PER_30MIN - 1;

        let a_open = last_at_or_before(leg_a, open_ts);
        let b_open = last_at_or_before(leg_b, open_ts);
        let a_close = last_at_or_before(leg_a, close_ts);
        let b_close = last_at_or_before(leg_b, close_ts);

        // Both legs must have at least one tick by the bucket close, else skip.
        if let (Some(ac), Some(bc)) = (a_close, b_close) {
            // Production proxy: use bid/ask of each leg at bucket close to
            // construct the synth H/L exactly as `mtf_sweep.rs` does:
            //   cross_ask = leg_a.ask / leg_b.bid   (worst-case spread for buyer)
            //   cross_bid = leg_a.bid / leg_b.ask
            //   cross_mid = (cross_ask + cross_bid) / 2
            // Then within the bucket, take H = max(cross_ask, cross_mid),
            //                          L = min(cross_bid, cross_mid).
            // The "second snapshot" at bucket OPEN is used to widen H/L:
            // if the open snapshot's mid exceeds close H, lift H; symmetric
            // for L. This is the closest faithful proxy to the production
            // path that mtf_sweep ingests.
            let cross_close_ask = ac.ask / bc.bid;
            let cross_close_bid = ac.bid / bc.ask;
            let cross_close_mid = 0.5 * (cross_close_ask + cross_close_bid);
            let mut hi = cross_close_ask.max(cross_close_mid);
            let mut lo = cross_close_bid.min(cross_close_mid);

            if let (Some(ao), Some(bo)) = (a_open, b_open) {
                let cao = ao.ask / bo.bid;
                let cbo = ao.bid / bo.ask;
                let cmo = 0.5 * (cao + cbo);
                hi = hi.max(cao.max(cmo));
                if cbo > 0.0 && cmo > 0.0 {
                    lo = lo.min(cbo.min(cmo));
                }
            }
            if hi.is_finite() && lo.is_finite() && hi > 0.0 && lo > 0.0 {
                hlc.insert(bucket, (hi, lo));
            }
        }
        bucket += MS_PER_30MIN;
    }
    hlc
}

/// Binary-search the last tick at or before `ts`. Returns `None` if `ts` is
/// before the first tick.
fn last_at_or_before(ticks: &[LegTick], ts: i64) -> Option<LegTick> {
    if ticks.is_empty() {
        return None;
    }
    // partition_point: first index where ts > target
    let i = ticks.partition_point(|t| t.ts <= ts);
    if i == 0 {
        return None;
    }
    Some(ticks[i - 1])
}

/// Method B's event-driven merger: returns the full sequence of synthetic
/// cross-pair ticks `(ts, cross_mid, cross_bid, cross_ask)`.
///
/// Algorithm:
///   - Min-heap of (ts, leg_idx, tick_idx) over both legs.
///   - Maintain last-known bid/ask for each leg.
///   - On every pop, advance that leg's last-known, then if BOTH legs are
///     primed, emit `(ts, cross_mid)` where cross_mid is computed from
///     last-known leg quotes using the same A.ask/B.bid worst-case spread
///     used in Method A (so the comparison is apples-to-apples — the
///     difference is purely "did we resolve intra-bucket H/L from real
///     ticks vs from bucket-aligned snapshots").
#[derive(Debug, Clone, Copy)]
struct SynthTick {
    ts: i64,
    mid: f64,
    bid: f64,
    ask: f64,
}

fn merge_event_driven(leg_a: &[LegTick], leg_b: &[LegTick]) -> Vec<SynthTick> {
    let mut out: Vec<SynthTick> = Vec::with_capacity(leg_a.len() + leg_b.len());
    if leg_a.is_empty() || leg_b.is_empty() {
        return out;
    }
    // (ts, which_leg [0=a, 1=b], idx)
    let mut heap: BinaryHeap<Reverse<(i64, u8, usize)>> = BinaryHeap::with_capacity(2);
    heap.push(Reverse((leg_a[0].ts, 0u8, 0usize)));
    heap.push(Reverse((leg_b[0].ts, 1u8, 0usize)));

    let mut a_last: Option<LegTick> = None;
    let mut b_last: Option<LegTick> = None;

    while let Some(Reverse((ts, which, idx))) = heap.pop() {
        // Advance the corresponding leg's last-known state.
        match which {
            0 => {
                a_last = Some(leg_a[idx]);
                let next = idx + 1;
                if next < leg_a.len() {
                    heap.push(Reverse((leg_a[next].ts, 0, next)));
                }
            }
            1 => {
                b_last = Some(leg_b[idx]);
                let next = idx + 1;
                if next < leg_b.len() {
                    heap.push(Reverse((leg_b[next].ts, 1, next)));
                }
            }
            _ => unreachable!(),
        }

        // Only emit once both legs are primed.
        if let (Some(a), Some(b)) = (a_last, b_last) {
            // Cross quote convention identical to Method A so the H/L
            // construction is comparable.
            let cross_ask = a.ask / b.bid;
            let cross_bid = a.bid / b.ask;
            if !(cross_ask.is_finite() && cross_bid.is_finite()) {
                continue;
            }
            if cross_ask <= 0.0 || cross_bid <= 0.0 {
                continue;
            }
            let cross_mid = 0.5 * (cross_ask + cross_bid);
            out.push(SynthTick {
                ts,
                mid: cross_mid,
                bid: cross_bid,
                ask: cross_ask,
            });
        }
    }
    out
}

/// Bucket a tick stream into 30-min H/L bars over `[from_ms, to_ms_excl)`.
/// H = max(ask, mid), L = min(bid, mid) — same convention as mtf_sweep so
/// the comparison is apples-to-apples.
fn bucket_synth_hlc(
    ticks: &[SynthTick],
    from_ms: i64,
    to_ms_excl: i64,
) -> BTreeMap<i64, (f64, f64)> {
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    for t in ticks {
        if t.ts < from_ms || t.ts >= to_ms_excl {
            continue;
        }
        let key = (t.ts / MS_PER_30MIN) * MS_PER_30MIN;
        let entry = hlc.entry(key).or_insert((t.ask.max(t.mid), t.bid.min(t.mid)));
        let hi_candidate = t.ask.max(t.mid);
        let lo_candidate = t.bid.min(t.mid);
        if hi_candidate > entry.0 {
            entry.0 = hi_candidate;
        }
        if lo_candidate > 0.0 && lo_candidate < entry.1 {
            entry.1 = lo_candidate;
        }
    }
    hlc
}

/// Native cross HLC bucketing — identical convention but operates on
/// LegTick (which carries bid/ask).
fn bucket_native_hlc(
    ticks: &[LegTick],
    from_ms: i64,
    to_ms_excl: i64,
) -> BTreeMap<i64, (f64, f64)> {
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    for t in ticks {
        if t.ts < from_ms || t.ts >= to_ms_excl {
            continue;
        }
        let key = (t.ts / MS_PER_30MIN) * MS_PER_30MIN;
        let entry = hlc.entry(key).or_insert((t.ask.max(t.mid), t.bid.min(t.mid)));
        let hi = t.ask.max(t.mid);
        let lo = t.bid.min(t.mid);
        if hi > entry.0 {
            entry.0 = hi;
        }
        if lo > 0.0 && lo < entry.1 {
            entry.1 = lo;
        }
    }
    hlc
}

/// Daily Parkinson σ from a 30-min HLC map. For each UTC day in
/// `[from_date, to_date]`, average the per-bucket Parkinson variance over
/// every bucket whose key falls in [day_start, day_end), then take √.
fn daily_sigma_from_hlc(
    hlc: &BTreeMap<i64, (f64, f64)>,
    from_date: NaiveDate,
    to_date: NaiveDate,
) -> BTreeMap<NaiveDate, f64> {
    let mut out: BTreeMap<NaiveDate, f64> = BTreeMap::new();
    let mut d = from_date;
    while d <= to_date {
        let day_start = day_start_ms(d);
        let day_end = day_start + MS_PER_DAY;
        let mut sum_var = 0.0;
        let mut n = 0usize;
        for (&ts, &(h, l)) in hlc.range(day_start..day_end) {
            let _ = ts;
            let v = parkinson_var(h, l);
            if v > 0.0 {
                sum_var += v;
                n += 1;
            }
        }
        let sigma = if n > 0 { (sum_var / n as f64).sqrt() } else { 0.0 };
        out.insert(d, sigma);
        d = d.succ_opt().unwrap_or(d);
    }
    out
}

/// Run the calibrator on a single tick stream + HLC pair, returning the
/// resulting k (geo-mean across windows, or 0.0 if all windows failed).
///
/// We materialise a temporary `.vol` file from the HLC (just like the
/// production pipeline) and feed it to `calibrate_mtf_with_target`.
fn simulate_calibrator(
    label: &str,
    prices: &[(i64, f64)],
    hlc: &BTreeMap<i64, (f64, f64)>,
    vol_cfg: &VolConfig,
    target_bpd: f64,
    k_fit_windows_days: &[usize],
    mult_bounds: [f64; 2],
) -> Result<f64> {
    if prices.is_empty() || hlc.is_empty() {
        warn!(method = label, "calibrator skip: empty input");
        return Ok(0.0);
    }
    let vol_path = std::env::temp_dir().join(format!(
        "nxr-synth-sigma-bench-{}-{}.vol",
        label,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&vol_path);
    {
        let mut writer = VolWriter::new(&vol_path)?;
        build_vol_from_hlc(hlc, vol_cfg, &mut writer)?;
        writer.finish()?;
    }
    let vol_mmap = VolMmap::open(&vol_path)?;
    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let cal = CalibrationConfig {
        target_bpd,
        k_fit_windows_days: k_fit_windows_days.to_vec(),
        min_window_days: 7,
        max_rounds: 12,
        tolerance: 0.05,
        mult_bounds,
    };
    let base = RenkoConfig {
        multiplier: 0.4_f32,
        min_pct: 0.0001,
    };
    let k = calibrate_mtf_with_target(
        prices,
        &cal,
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
    ) as f64;

    let _ = std::fs::remove_file(&vol_path);
    Ok(k)
}

/// Convert a sorted tick stream + 30-min HLC into the calibrator's expected
/// `prices: Vec<(i64, f64)>` shape. For Method A we sample one mid per
/// bucket-close; for B/C we use the full event stream (close-of-bucket OR
/// every synthetic tick — choice is the caller's).
fn prices_from_synth(ticks: &[SynthTick]) -> Vec<(i64, f64)> {
    ticks.iter().map(|t| (t.ts, t.mid)).collect()
}

fn prices_from_native(ticks: &[LegTick]) -> Vec<(i64, f64)> {
    ticks.iter().map(|t| (t.ts, t.mid)).collect()
}

fn prices_from_method_a(hlc: &BTreeMap<i64, (f64, f64)>) -> Vec<(i64, f64)> {
    // Method A's only available mid signal is one snapshot per bucket-close.
    // Use the bucket key + MS_PER_30MIN - 1 as the timestamp and (H+L)/2 as
    // the mid — this is exactly what the production proxy makes visible to
    // the calibrator if a synth-cross were threaded through `mtf_sweep`.
    hlc.iter()
        .map(|(&k, &(h, l))| (k + MS_PER_30MIN - 1, 0.5 * (h + l)))
        .collect()
}

fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn parse_windows(s: &str) -> Result<Vec<usize>> {
    s.split(',')
        .map(|t| {
            t.trim()
                .parse::<usize>()
                .with_context(|| format!("bad window {}", t))
        })
        .collect()
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();

    let from_date = parse_date(&args.from)?;
    let to_date = parse_date(&args.to)?;
    if to_date < from_date {
        anyhow::bail!("--to must be >= --from");
    }
    let k_fit_windows_days = parse_windows(&args.k_fit_windows_days)?;
    let mult_bounds = [args.mult_lo, args.mult_hi];

    let data_root = args
        .data_root
        .clone()
        .unwrap_or_else(|| {
            std::env::var("NXR_DATA_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/data"))
        });

    info!(
        leg_a = args.leg_a,
        leg_b = args.leg_b,
        cross = args.cross,
        from = %from_date,
        to = %to_date,
        data_root = %data_root.display(),
        target_bpd = args.target_bpd,
        ?k_fit_windows_days,
        ?mult_bounds,
        "synth-sigma-benchmark start"
    );

    // ── Load leg ticks (date-filtered to keep peak RAM bounded) ─────────────
    let leg_a = load_leg_ticks(&data_root, args.leg_a, from_date, to_date)
        .with_context(|| format!("load leg A id={}", args.leg_a))?;
    let leg_b = load_leg_ticks(&data_root, args.leg_b, from_date, to_date)
        .with_context(|| format!("load leg B id={}", args.leg_b))?;
    info!(
        n_leg_a = leg_a.len(),
        n_leg_b = leg_b.len(),
        "leg ticks loaded"
    );
    if leg_a.is_empty() || leg_b.is_empty() {
        anyhow::bail!("missing leg shards; cannot benchmark");
    }

    // Restrict the analysis range to [from, to + 1day) in ms.
    let range_from_ms = day_start_ms(from_date);
    let range_to_ms_excl = day_start_ms(to_date) + MS_PER_DAY;

    // ── Method A: leg-product mid at 30-min bucket snapshots ────────────────
    let hlc_a = build_method_a_hlc(&leg_a, &leg_b, range_from_ms, range_to_ms_excl);
    let sigma_a = daily_sigma_from_hlc(&hlc_a, from_date, to_date);
    info!(buckets_a = hlc_a.len(), "method A built");

    // ── Method B: event-driven merge → 30-min H/L ───────────────────────────
    let synth_b = merge_event_driven(&leg_a, &leg_b);
    let hlc_b = bucket_synth_hlc(&synth_b, range_from_ms, range_to_ms_excl);
    let sigma_b = daily_sigma_from_hlc(&hlc_b, from_date, to_date);
    info!(
        n_synth_ticks = synth_b.len(),
        buckets_b = hlc_b.len(),
        "method B built"
    );

    // ── Method C: native cross-pair, if available ───────────────────────────
    let native_cross =
        load_leg_ticks(&data_root, args.cross, from_date, to_date).unwrap_or_default();
    let (hlc_c, sigma_c, native_available) = if native_cross.is_empty() {
        info!(
            cross = args.cross,
            "no native cross-pair shards found; Method C unavailable"
        );
        (BTreeMap::new(), BTreeMap::new(), false)
    } else {
        let hlc = bucket_native_hlc(&native_cross, range_from_ms, range_to_ms_excl);
        let sigma = daily_sigma_from_hlc(&hlc, from_date, to_date);
        info!(
            cross = args.cross,
            n_native = native_cross.len(),
            buckets_c = hlc.len(),
            "method C (native) built"
        );
        (hlc, sigma, true)
    };

    // ── Per-day table ───────────────────────────────────────────────────────
    println!(
        "\n{:<12}  {:>16}  {:>16}  {:>16}  {:>10}  {:>10}",
        "date", "sigma_A_current", "sigma_B_event", "sigma_C_native", "B/A", "C/B"
    );
    let mut ratios_ba: Vec<f64> = Vec::new();
    let mut ratios_cb: Vec<f64> = Vec::new();
    let mut d = from_date;
    while d <= to_date {
        let a = sigma_a.get(&d).copied().unwrap_or(0.0);
        let b = sigma_b.get(&d).copied().unwrap_or(0.0);
        let c = sigma_c.get(&d).copied().unwrap_or(0.0);
        let ba = if a > 0.0 { b / a } else { 0.0 };
        let cb = if native_available && b > 0.0 {
            c / b
        } else {
            0.0
        };
        if a > 0.0 && b > 0.0 {
            ratios_ba.push(ba);
        }
        if native_available && b > 0.0 && c > 0.0 {
            ratios_cb.push(cb);
        }
        let c_str = if native_available {
            format!("{:>16.6}", c)
        } else {
            format!("{:>16}", "n/a")
        };
        let cb_str = if native_available {
            format!("{:>10.3}", cb)
        } else {
            format!("{:>10}", "n/a")
        };
        println!(
            "{:<12}  {:>16.6}  {:>16.6}  {}  {:>10.3}  {}",
            d, a, b, c_str, ba, cb_str
        );
        if args.json {
            let rec = serde_json::json!({
                "date": d.to_string(),
                "sigma_a_current": a,
                "sigma_b_event": b,
                "sigma_c_native": if native_available { Some(c) } else { None },
                "ratio_b_over_a": ba,
                "ratio_c_over_b": if native_available { Some(cb) } else { None },
            });
            println!("JSON {}", rec);
        }
        d = d.succ_opt().unwrap_or(d);
    }

    // ── Aggregate ratio stats ───────────────────────────────────────────────
    ratios_ba.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    ratios_cb.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean_ba = if ratios_ba.is_empty() {
        0.0
    } else {
        ratios_ba.iter().sum::<f64>() / ratios_ba.len() as f64
    };
    let median_ba = quantile_sorted(&ratios_ba, 0.5);
    let p95_ba = quantile_sorted(&ratios_ba, 0.95);
    println!("\n────── ratio_B/A (event-driven / current) ──────");
    println!(
        "n={}  mean={:.3}  median={:.3}  p95={:.3}  → Method A under-estimates σ by ~{:.2}× (median)",
        ratios_ba.len(),
        mean_ba,
        median_ba,
        p95_ba,
        median_ba
    );

    if native_available {
        let mean_cb = if ratios_cb.is_empty() {
            0.0
        } else {
            ratios_cb.iter().sum::<f64>() / ratios_cb.len() as f64
        };
        let median_cb = quantile_sorted(&ratios_cb, 0.5);
        println!(
            "\n────── ratio_C/B (native / event-driven) ──────\n\
             n={}  mean={:.3}  median={:.3}  → event-driven {} native (closer to 1.0 = better)",
            ratios_cb.len(),
            mean_cb,
            median_cb,
            if (median_cb - 1.0).abs() < 0.10 {
                "MATCHES"
            } else {
                "DIFFERS FROM"
            }
        );
    }

    // ── Calibrator simulation ───────────────────────────────────────────────
    println!("\n────── calibrator k simulation ──────");
    let vol_cfg = VolConfig::default();

    let prices_a = prices_from_method_a(&hlc_a);
    let k_a = simulate_calibrator(
        "A",
        &prices_a,
        &hlc_a,
        &vol_cfg,
        args.target_bpd,
        &k_fit_windows_days,
        mult_bounds,
    )
    .unwrap_or(0.0);

    let prices_b = prices_from_synth(&synth_b);
    let k_b = simulate_calibrator(
        "B",
        &prices_b,
        &hlc_b,
        &vol_cfg,
        args.target_bpd,
        &k_fit_windows_days,
        mult_bounds,
    )
    .unwrap_or(0.0);

    let k_c = if native_available {
        let prices_c = prices_from_native(&native_cross);
        simulate_calibrator(
            "C",
            &prices_c,
            &hlc_c,
            &vol_cfg,
            args.target_bpd,
            &k_fit_windows_days,
            mult_bounds,
        )
        .unwrap_or(0.0)
    } else {
        f64::NAN
    };

    let lo = mult_bounds[0];
    let a_clamped = k_a > 0.0 && (k_a - lo).abs() / lo < 0.01;
    let b_clamped = k_b > 0.0 && (k_b - lo).abs() / lo < 0.01;
    println!(
        "Method A (current proxy)         k = {:.6}{}",
        k_a,
        if a_clamped {
            " [CLAMPED at lower bound — degenerate σ]"
        } else {
            ""
        }
    );
    println!(
        "Method B (event-driven)          k = {:.6}{}",
        k_b,
        if b_clamped {
            " [CLAMPED at lower bound]"
        } else {
            ""
        }
    );
    if native_available {
        let c_clamped = k_c > 0.0 && (k_c - lo).abs() / lo < 0.01;
        println!(
            "Method C (native cross)          k = {:.6}{}",
            k_c,
            if c_clamped {
                " [CLAMPED at lower bound]"
            } else {
                ""
            }
        );
    } else {
        println!("Method C (native cross)          k = n/a (no shards)");
    }

    println!(
        "\n──────  SUMMARY  ──────\n\
         • If median ratio_B/A >> 1.0, Method A under-estimates σ as Tanaka diagnosed.\n\
         • If Method A's k is clamped at {:.3} and Method B's k is in [0.05, 0.5], the\n   under-estimate is what drives k to the boundary in production.\n\
         • If native is available and ratio_C/B ≈ 1.0, the event-driven synth is a\n   faithful substitute for the native series and should be the new pipeline default.",
        mult_bounds[0]
    );

    Ok(())
}
