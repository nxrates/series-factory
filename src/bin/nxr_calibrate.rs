//! Daily offline calibration of Renko `multiplier` per ticker.
//!
//! Pipeline:
//!   1. Load `config.yml` and the current `ticker-params.json`.
//!   2. For each `(provider, ticker_id)` in `pair_volumes`:
//!        - Infer asset class (fx/crypto · major/alt/stable · cross).
//!        - Look up `target_bpd` for the class; `skip` ⇒ continue.
//!        - Stream the consensus `.idx` file (one per ticker_id) and build 30-min
//!          Parkinson HLC + EMA-smoothed sigma.
//!        - Run MTF binary-search calibration on a 1-min mid downsample.
//!   3. Merge results into `ticker-params.json` (preserving existing fields) and
//!      stamp `calibrated_at`. Atomic write via `nxr_sdk::ipc::write_atomic`.
//!
//! The aggregator picks up the new multipliers on its next mtime check (see
//! `core/src/weights.rs::maybe_reload`).
//!
//! Usage: `nxr-calibrate [--once] [--parallel N]`. `--once` exits after one run
//! (default for k8s CronJob); without it the binary sleeps 24h between runs.

use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::Parser;
use mitch::common::InstrumentType;
use mitch::timestamp;
use nxr_sdk::asset_class::{
    bucket_for_pair, effective_list, AssetClassBucket, DEFAULT_CRYPTO_MAJORS, DEFAULT_FX_MAJORS,
    DEFAULT_STABLECOINS,
};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::renko::{RenkoConfig, K_FLOOR, K_MAX_SAFETY};
use nxr_sdk::shard::{list_shards, ShardStream};
use nxr_sdk::vol::{MtfVolCalculator, VolConfig};
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::{resolve_ticker, resolve_ticker_id};
use rayon::prelude::*;
use series_factory::bar_construction::{
    build_vol_from_s10, scale_to_target_k, CalibrationConfig, S10ShardIter,
};
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

/// Drift-gate sub-window (days): k is also fit on the first `DRIFT_SUBWINDOW_DAYS`
/// vs the last `DRIFT_SUBWINDOW_DAYS` of the rolling window; `k_drift =
/// |k_end−k_start|/k_start`. `k_drift > DRIFT_GATE_MAX` ⇒ WARN "needs
/// point-in-time rebuild" (does NOT block — just surfaces the single-latest-k
/// look-ahead bound).
const DRIFT_SUBWINDOW_DAYS: i64 = 90;
const DRIFT_GATE_MAX: f64 = 0.05;

/// Upper clamp on the up-front `Vec::<(i64,f64)>::with_capacity` reservation for
/// the full-tick mid path (PERF A2 pre-sizing).
///
/// **Why a cap (OOM RCA, 2026-06-09):** the pre-size estimate is
/// `Σ shard_bytes / 56` — an *accurate* upper bound of the records on disk, but
/// only an upper bound of the records actually *kept*. Heartbeat-sentinel and
/// non-finite-mid records are filtered in the push loop, and on sentinel-heavy
/// tickers (BTC/USDT: ~763M on-disk records vs ~247M finite mids, ≈3.1×) the
/// raw estimate over-reserves by 3×. At 16 B/elem that is a single ~12 GB
/// up-front allocation which OOMs the 22Gi calibrator pod (`memory allocation
/// of 12211304480 bytes failed`) once the σ cache + other allocations are
/// added. We keep the estimate (it is correct, and cheap reallocation is only
/// paid on the few huge sentinel-heavy tickers) but clamp the *reservation* so
/// a large or skewed estimate can never trigger a giant up-front alloc; above
/// the cap the Vec starts empty and grows normally.
///
/// 64M elements × 16 B ≈ 1 GiB — comfortably below the per-worker budget while
/// still pre-sizing the common case (most tickers are << 64M finite mids).
const MAX_PRERESERVE_TICKS: usize = 64_000_000;

/// Clamp a pre-size estimate to [`MAX_PRERESERVE_TICKS`]. Pure sizing helper —
/// never affects which records are loaded or their order; only the initial Vec
/// capacity. Above the cap returns `MAX_PRERESERVE_TICKS` so the Vec grows by
/// realloc rather than OOMing on the up-front reservation.
#[inline]
fn clamp_prereserve(estimate: usize) -> usize {
    estimate.min(MAX_PRERESERVE_TICKS)
}

// Synth pair registry — canonical source @ nxr_sdk::synth::pairs.
use nxr_sdk::pipeline_config::SynthPairYml;
use nxr_sdk::synth::pairs::{SynthPairSpec, DEFAULT_INITIAL_SYNTH_PAIRS};

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(about = "Daily Renko k calibration cron.")]
struct Args {
    /// Run once and exit (default for k8s CronJob).
    #[arg(long)]
    once: bool,
    /// Rayon worker count for the per-ticker loop. Keep low to bound RAM
    /// (each ticker mmaps a .idx and builds a sigma cache).
    #[arg(long, default_value_t = 4)]
    parallel: usize,
}

// ── Config (subset of config.yml) ────────────────────────────────────────────

use nxr_sdk::pipeline_config::{CalibrationYml, PipelineYml};

/// Convert the shared calibration block into the inner `CalibrationConfig`
/// the direct scale-to-target solver consumes.
fn calibration_inner(c: &CalibrationYml) -> CalibrationConfig {
    CalibrationConfig {
        target_bpd: c.target_bpd,
        rolling_window_days: c.rolling_window_days,
        min_window_days: c.min_window_days,
        bracket_max_iters: c.bracket_max_iters,
        accept_tol: c.accept_tol,
        mult_bounds: c.mult_bounds,
    }
}

/// Drift gate: fit k on the FIRST vs the LAST
/// `DRIFT_SUBWINDOW_DAYS` of the (already window-trimmed) `prices`, log
/// `k_drift = |k_end−k_start|/k_start` at INFO, and WARN (does NOT block) when it
/// exceeds `DRIFT_GATE_MAX` — flagging the ticker for point-in-time rebuild. A
/// sub-window that fails to fit (returns 0.0) is logged and skipped (drift
/// unknown, not a block).
fn drift_gate<S: nxr_sdk::vol::VolSource + ?Sized>(
    label: &str,
    ticker_id: u64,
    prices: &[(i64, f64)],
    cal: &CalibrationConfig,
    base: &RenkoConfig,
    vol_source: &S,
    vol_cfg: &VolConfig,
    sigma_cache: &[f64],
    target_bpd: f64,
    fitted_k: f64,
) {
    let first = match prices.first() {
        Some(p) => p.0,
        None => return,
    };
    let last = match prices.last() {
        Some(p) => p.0,
        None => return,
    };
    const MS_PER_DAY: i64 = 86_400_000;
    let span_days = (last - first) / MS_PER_DAY;
    if span_days < 2 * DRIFT_SUBWINDOW_DAYS {
        info!(
            label,
            ticker_id, span_days, "drift gate: window < 2× sub-window — skipped"
        );
        return;
    }
    let start_lo = prices.partition_point(|p| p.0 < first);
    let start_hi = prices.partition_point(|p| p.0 <= first + DRIFT_SUBWINDOW_DAYS * MS_PER_DAY);
    let end_lo = prices.partition_point(|p| p.0 < last - DRIFT_SUBWINDOW_DAYS * MS_PER_DAY);
    let end_hi = prices.partition_point(|p| p.0 <= last);
    let start_slice = &prices[start_lo..start_hi];
    let end_slice = &prices[end_lo..end_hi];
    // Seed each sub-fit with the full-window fitted_k so the warm start is cheap.
    let k_start = scale_to_target_k(
        start_slice,
        cal,
        base,
        vol_source,
        vol_cfg,
        sigma_cache,
        target_bpd,
        Some(fitted_k as f32),
    ) as f64;
    let k_end = scale_to_target_k(
        end_slice,
        cal,
        base,
        vol_source,
        vol_cfg,
        sigma_cache,
        target_bpd,
        Some(fitted_k as f32),
    ) as f64;
    if !(k_start > 0.0 && k_end > 0.0) {
        info!(
            label,
            ticker_id,
            k_start,
            k_end,
            "drift gate: a sub-window failed to fit — k_drift unknown (not a block)"
        );
        return;
    }
    let k_drift = (k_end - k_start).abs() / k_start;
    info!(
        label,
        ticker_id,
        k_start,
        k_end,
        k_drift,
        fitted_k,
        "drift gate: first-{DRIFT_SUBWINDOW_DAYS}d vs last-{DRIFT_SUBWINDOW_DAYS}d k drift"
    );
    if k_drift > DRIFT_GATE_MAX {
        warn!(label, ticker_id, k_start, k_end, k_drift, drift_max = DRIFT_GATE_MAX,
            "drift gate: k_drift > {:.0}% — ticker needs point-in-time rebuild (single-latest-k look-ahead exceeds bound; NOT blocking)",
            DRIFT_GATE_MAX * 100.0);
    }
}

/// Resolve `target_bpd` for a given pair: per-pair overrides only,
/// flat default for all unlisted pairs. Class arg retained for log context.
fn target_for_pair(c: &CalibrationYml, pair: &str, class: AssetClassBucket) -> f64 {
    c.target_for_pair_classed(pair, class.as_key())
}

// ── Asset-class bucket detection ─────────────────────────────────────────────
//
// Bucket detection is owned by `nxr_sdk::asset_class::bucket_for_pair`,
// which reads the MITCH wire bits (`TickerId::base_asset_class()` /
// `quote_asset_class()`) and applies the operator-defined `crypto_majors`
// list for the major-vs-alt judgment within `AssetClass::CR`. No local
// string lists or bit-shift duplication.

// ── Per-ticker calibration ───────────────────────────────────────────────────

#[derive(Debug)]
enum CalOutcome {
    Ok { ticker_id: u64, k: f64 },
    Skipped { ticker_id: u64, reason: String },
    Failed { ticker_id: u64, reason: String },
}

fn calibrate_one(
    ticker_id: u64,
    pair: &str,
    class: AssetClassBucket,
    idx_dir: &Path,
    bars_root: &Path,
    cal_ext: &CalibrationYml,
    target_bpd: f64,
    vol_cfg: &VolConfig,
    renko_yml: &nxr_sdk::pipeline_config::RenkoYml,
    prior_k: Option<f32>,
) -> CalOutcome {
    // Sharded layout: shards live at `<idx_dir>/<ticker_id>/<YYYY-MM-DD>.idx`.
    // Enumerate via `list_shards`, then stream each shard via `ShardStream` so
    // memory stays bounded (one shard's working buffer, not the full history).
    let ticker_dir = idx_dir.join(ticker_id.to_string());
    let shards = match list_shards(&ticker_dir, "idx") {
        Ok(v) => v,
        Err(e) => {
            return CalOutcome::Skipped {
                ticker_id,
                reason: format!("no shards under {}: {}", ticker_dir.display(), e),
            }
        }
    };
    if shards.is_empty() {
        return CalOutcome::Skipped {
            ticker_id,
            reason: format!("no .idx shards under {}", ticker_dir.display()),
        };
    }

    // Pass 1: stream every .idx shard in date order → FULL-TICK mid path for the
    // in-memory calibration walk-forward. CRITICAL (2026-06-06 brick-storm RCA):
    // the calibrator MUST fit/measure k on the SAME granularity the applier
    // (`renko_from_idx.rs`) and the live renko producer
    // emit bricks from — the full ~100ms idx mid stream — NOT a 1-min last-mid
    // downsample. A renko brick forms on each price-level crossing along the
    // PATH; 1-min last-mid discards all intra-minute extremes → the calibrator
    // counts FAR fewer crossings → its bpd-accept-gate believes a too-small k
    // yields ~target bpd, but the full-tick applier then over-emits ~3.3×
    // (measured: BTC k=0.374 → 992 bpd applied vs ~300 target). Pushing every
    // finite mid in ts order to a Vec preserves the path: shards are date-ordered
    // and within-shard records are append-order (ts-ascending) from upstream.
    //
    // SEAM PARITY: skip heartbeat sentinels exactly as the applier
    // (`renko_from_idx.rs:199`) and live producer (`bars_renko.rs:528`) do —
    // they are not real mid moves and would inject phantom path points.
    //
    // The vol basis is built separately from the gapless `.s10` shards (RS over
    // s10 OHLC), NOT from idx-HLC — see the s10 vol build below.
    // PERF A2 (2026-06-09): pre-size the tick Vec from the on-disk shard byte
    // sizes (each IndexRecord = 56 B) so the per-tick push loop over up to
    // ~247M records does NOT pay log-N reallocation churn (~6 GB of memmove on
    // a cold ticker). Upper bound only — heartbeat sentinels / non-finite mids
    // are filtered below, so the Vec ends ≤ this reservation; no correctness
    // impact, purely a capacity hint.
    let est_ticks: usize = shards
        .iter()
        .filter_map(|(_d, p)| std::fs::metadata(p).ok().map(|m| m.len() as usize / 56))
        .sum();
    // Clamp the reservation: `est_ticks` is an accurate upper bound of on-disk
    // records but heartbeat sentinels / non-finite mids are filtered below, so
    // on sentinel-heavy tickers it over-reserves ~3× → a single multi-GB alloc
    // OOMs the pod. See `MAX_PRERESERVE_TICKS`.
    let mut tick_prices: Vec<(i64, f64)> = Vec::with_capacity(clamp_prereserve(est_ticks));
    for (_date, shard_path) in &shards {
        let mut stream = match ShardStream::<IndexRecord>::open(shard_path) {
            Ok(s) => s,
            Err(e) => {
                return CalOutcome::Failed {
                    ticker_id,
                    reason: format!("open shard {}: {}", shard_path.display(), e),
                }
            }
        };
        loop {
            let rec = match stream.next() {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(e) => {
                    return CalOutcome::Failed {
                        ticker_id,
                        reason: format!("read shard {}: {}", shard_path.display(), e),
                    }
                }
            };
            if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 {
                continue;
            }
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let bid = rec.index.bid;
            let ask = rec.index.ask;
            let mid = (bid + ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                continue;
            }
            tick_prices.push((ts, mid));
        }
    }

    if tick_prices.is_empty() {
        return CalOutcome::Skipped {
            ticker_id,
            reason: "empty .idx".into(),
        };
    }

    // Build the .vol file (tmp, deleted at end of fn) from the gapless `.s10`
    // shards via the canonical RS-over-s10-OHLC builder. offline == live.
    let vol_path = std::env::temp_dir().join(format!(
        "nxr-calibrate-{}-{}.vol",
        ticker_id,
        std::process::id()
    ));
    {
        let mut writer = match VolWriter::new(&vol_path) {
            Ok(w) => w,
            Err(e) => {
                return CalOutcome::Failed {
                    ticker_id,
                    reason: format!("vol writer: {}", e),
                }
            }
        };
        let s10_dir = nxr_sdk::shard::bars_dir(bars_root, ticker_id);
        let s10_shards = list_shards(&s10_dir, "s10").unwrap_or_default();
        if s10_shards.is_empty() {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Skipped {
                ticker_id,
                reason: format!("no .s10 shards under {}", s10_dir.display()),
            };
        }
        let mut s10_iter = S10ShardIter::new(s10_shards);
        if let Err(e) = build_vol_from_s10(|| s10_iter.next_bar(), vol_cfg, &mut writer) {
            return CalOutcome::Failed {
                ticker_id,
                reason: format!("vol build: {}", e),
            };
        }
        if let Err(e) = writer.finish() {
            return CalOutcome::Failed {
                ticker_id,
                reason: format!("vol finish: {}", e),
            };
        }
    }

    let vol_mmap = match VolMmap::open(&vol_path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Failed {
                ticker_id,
                reason: format!("vol mmap: {}", e),
            };
        }
    };

    let sigma_cache = {
        let mut calc = MtfVolCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let base = RenkoConfig {
        multiplier: RenkoConfig::default().multiplier,
        min_pct: renko_yml.min_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed {
            ticker_id,
            reason: format!("base renko cfg: {}", e),
        };
    }

    // Trim to the trailing rolling window (methodology §3): the .idx may hold
    // more history than `rolling_window_days`; the median objective is over the
    // single trailing window only.
    let cal_inner = calibration_inner(cal_ext);
    let window = trailing_window(&tick_prices, cal_inner.rolling_window_days);

    info!(
        ticker_id,
        pair,
        class = class.as_key(),
        target_bpd,
        n_ticks = window.len(),
        window_days = cal_inner.rolling_window_days,
        "calibrating (direct scale-to-target, full-tick)"
    );
    // Direct SCALE-TO-TARGET solver (methodology §4). prior_k (yesterday's k from
    // the weights file) is the warm-start seed.
    let mult = scale_to_target_k(
        window,
        &cal_inner,
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
        prior_k,
    );

    if mult > 0.0 && (mult as f64).is_finite() {
        // Drift gate (§6): bound the single-latest-k look-ahead. Logging only.
        drift_gate(
            "base",
            ticker_id,
            window,
            &cal_inner,
            &base,
            &vol_mmap,
            vol_cfg,
            &sigma_cache,
            target_bpd,
            mult as f64,
        );
    }

    let _ = std::fs::remove_file(&vol_path);

    if !(mult > 0.0 && (mult as f64).is_finite()) {
        return CalOutcome::Failed {
            ticker_id,
            reason: "calibration returned 0 (degenerate window / unreachable target)".into(),
        };
    }
    CalOutcome::Ok {
        ticker_id,
        k: mult as f64,
    }
}

/// Trailing-window slice: the last `window_days` of the ts-ascending `prices`
/// (by the LAST timestamp). The median objective is over this single window.
fn trailing_window(prices: &[(i64, f64)], window_days: usize) -> &[(i64, f64)] {
    const MS_PER_DAY: i64 = 86_400_000;
    let Some(&(last, _)) = prices.last() else {
        return prices;
    };
    let from = last - (window_days as i64) * MS_PER_DAY;
    let lo = prices.partition_point(|p| p.0 < from);
    &prices[lo..]
}

// ── Synth-pair calibration ───────────────────────────────────────────────────
//
// For each configured synth cross (e.g. ETH/BTC), reconstruct synth ticks
// from the two underlying USDT-quoted leg `.idx` files via event-driven
// min-heap merge, then run the SAME MTF calibrator that the base path uses.
// Output is a single `renko_k` value per synth ticker_id, written to
// `ticker-params.json` alongside base entries; live `bars_renko_synth`
// picks it up via the existing weights hot-reload path.
//
// **Why NOT persist a synth `.idx`:** the kernel design keeps synth on the
// wire only —
// disk has bars + σ, never synth ticks. Calibration is the one place we
// reconstruct ticks transiently in memory.
//
// **Why NOT K_FLOOR fallback on synth:** Method-B σ from event-merged
// ticks is the operator's quality target; if calibrate fails (e.g.
// clamp-detector drops every window), `Failed` is the honest outcome and
// the caller carries the prior value rather than fabricating one.

/// Streaming reader over one leg's date-ordered `.idx` shards.
///
/// Yields `(ts, bid, ask)` triples in ascending order across shards. Memory
/// footprint is bounded to **one ShardStream working buffer at a time** (~150 KB)
/// instead of the full leg history (was: 24 B/tick × tens of millions × 2 legs
/// → 16Gi pod OOM at ~7-9 min into the synth pass, 2026-05-30 incident).
///
/// `idx_root` must point at the indexes directory itself (e.g. `/data/indexes`,
/// NOT `/data` — `nxr-calibrate`'s NxrConfig::indexes_dir already includes
/// the `indexes/` suffix). Per-ticker shards live at
/// `<idx_root>/<ticker_id>/<YYYY-MM-DD>.idx`.
struct LegStream {
    shards: std::vec::IntoIter<(chrono::NaiveDate, PathBuf)>,
    cur: Option<ShardStream<IndexRecord>>,
}

/// PERF A2: upper-bound tick count for one synth leg from its `.idx` shard
/// byte sizes (each IndexRecord = 56 B). Used only to pre-size the merged synth
/// tick Vec — a missing dir / unreadable shard simply contributes 0, so the
/// reservation degrades to the old grow-on-push behavior, never wrong.
fn est_leg_ticks(idx_root: &Path, ticker_id: u64) -> usize {
    let dir = idx_root.join(ticker_id.to_string());
    list_shards(&dir, "idx")
        .unwrap_or_default()
        .iter()
        .filter_map(|(_d, p)| std::fs::metadata(p).ok().map(|m| m.len() as usize / 56))
        .sum()
}

impl LegStream {
    fn open(idx_root: &Path, ticker_id: u64) -> Result<Self> {
        let dir = idx_root.join(ticker_id.to_string());
        let shards =
            list_shards(&dir, "idx").with_context(|| format!("list shards {}", dir.display()))?;
        if shards.is_empty() {
            anyhow::bail!("no .idx shards under {}", dir.display());
        }
        Ok(Self {
            shards: shards.into_iter(),
            cur: None,
        })
    }

    /// Next valid `(ts_ms, IndexRecord)` across all shards, or `Ok(None)` at end.
    /// Skips heartbeat sentinels and records with non-finite/non-positive
    /// bid/ask (matches prior filter). The FULL record is returned (not just
    /// bid/ask) so the gated reconstruction (`SynthReplayState`) can read the
    /// leg's confidence / ci / volumes — identical inputs to the backfill gate.
    fn next_tick(&mut self) -> Result<Option<(i64, IndexRecord)>> {
        loop {
            if self.cur.is_none() {
                match self.shards.next() {
                    Some((_d, path)) => {
                        let s = ShardStream::<IndexRecord>::open(&path)
                            .with_context(|| format!("open idx {}", path.display()))?;
                        self.cur = Some(s);
                    }
                    None => return Ok(None),
                }
            }
            let stream = self.cur.as_mut().unwrap();
            match stream.next()? {
                Some(rec) => {
                    // SEAM PARITY: drop heartbeat sentinels (mirror native path
                    // + applier) so synth legs carry only real mid moves.
                    if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 {
                        continue;
                    }
                    let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
                    let bid = rec.index.bid;
                    let ask = rec.index.ask;
                    if !(bid.is_finite() && ask.is_finite()) {
                        continue;
                    }
                    if bid <= 0.0 || ask <= 0.0 {
                        continue;
                    }
                    return Ok(Some((ts, rec)));
                }
                None => {
                    // End of current shard; advance to next.
                    self.cur = None;
                }
            }
        }
    }
}

fn calibrate_one_synth(
    synth_id: u64,
    synth_sym: &str,
    leg_a_id: u64,
    leg_b_id: u64,
    idx_root: &Path,
    bars_root: &Path,
    cal_ext: &CalibrationYml,
    target_bpd: f64,
    vol_cfg: &VolConfig,
    renko_yml: &nxr_sdk::pipeline_config::RenkoYml,
    prior_k: Option<f32>,
) -> CalOutcome {
    // ── 1. Open both leg streams (no materialization) ───────────────────────
    // Streams pull one tick at a time from on-disk shards. Memory bounded
    // to two ShardStream buffers (~300 KB total) instead of full history
    // (was 16Gi pod OOM, 2026-05-30 incident).
    let mut leg_a_stream = match LegStream::open(idx_root, leg_a_id) {
        Ok(s) => s,
        Err(e) => {
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("leg_a={} {}", leg_a_id, e),
            }
        }
    };
    let mut leg_b_stream = match LegStream::open(idx_root, leg_b_id) {
        Ok(s) => s,
        Err(e) => {
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("leg_b={} {}", leg_b_id, e),
            }
        }
    };

    // Prime both legs' look-ahead slot. If either leg has zero valid ticks → skip.
    let mut a_next: Option<(i64, IndexRecord)> = match leg_a_stream.next_tick() {
        Ok(v) => v,
        Err(e) => {
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("read leg_a: {}", e),
            }
        }
    };
    let mut b_next: Option<(i64, IndexRecord)> = match leg_b_stream.next_tick() {
        Ok(v) => v,
        Err(e) => {
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("read leg_b: {}", e),
            }
        }
    };
    if a_next.is_none() || b_next.is_none() {
        return CalOutcome::Skipped {
            ticker_id: synth_id,
            reason: "empty leg".into(),
        };
    }

    // ── 2. Event-driven 2-stream merge → synth (ts, bid, ask, mid) ───────────
    // At every leg tick we update last-known of that leg, then if both legs
    // primed emit a synth tick using the worst-case-spread convention:
    //   synth.bid = leg_a.bid / leg_b.ask
    //   synth.ask = leg_a.ask / leg_b.bid
    // (mirrors core/src/synth_kernel.rs:185 + triangulator.rs:17).
    //
    // Each leg stream is monotone-ascending (shards sorted by date,
    // within-shard records are append-order from upstream), so the merge
    // reduces to "pick whichever side has the earlier next-ts" — no heap.
    // SEAM PARITY (R3): the synth `.vol` is built from the SAME persisted `.s10`
    // shards the live `bars_renko_synth` ring consumes (written by the synth s10
    // producer — live `bars_s10::spawn(Synth)` / offline `synth-backfill-from-idx`),
    // NOT an in-memory mid reconstruction. The old reconstruction min/max/last on
    // raw mids ≠ the real s10 producer's `BarAccumulator` microstructure-weighted
    // OHLC + flat-fill timing → train/serve skew on synths. The leg merge below
    // is now ONLY for the FULL-TICK `tick_prices` (the calibration tick stream),
    // exactly as the native path uses its full `.idx` mid path. CRITICAL
    // (2026-06-06 brick-storm RCA): calibrate granularity MUST == apply
    // granularity; a 1-min last-mid downsample undercounts crossings → too-small
    // k → live over-emit. Each leg event emits one synth path point.
    //
    // §5 PARITY FIX (2026-06-10 RCA #1763): the reconstruction is now routed
    // through the SAME gated state machine the backfill driver uses
    // (`nxr_sdk::synth::SynthReplayState::feed_leg_tick`). Previously this loop
    // merged the two legs UNGATED — emitting a mid for every leg event regardless
    // of leg staleness / confidence / sanity — while backfill + live gate through
    // `compute_synth_index` (5 s leg-TTL + conf + sanity). That fit `k` on a
    // DENSER tick stream than it was applied to → synth-cross median bpd
    // collapsed ~90 %. Feeding the gated state machine here (now_ms = inbound
    // tick ts, push mid only on `Some`) restores hist==live for synths.
    // PERF A2 (2026-06-09): pre-size the synth tick Vec. The gated emit produces
    // ≤ one point per leg event (once both legs primed + within TTL), so the
    // length is bounded by (leg_a_ticks + leg_b_ticks). Estimate each leg's tick
    // count from its on-disk shard bytes (56 B/IndexRecord). Upper bound — gated
    // drops + skip records trim it — so capacity is reserved once, no churn.
    let est_synth_ticks: usize =
        est_leg_ticks(idx_root, leg_a_id).saturating_add(est_leg_ticks(idx_root, leg_b_id));
    // Clamp the reservation (see `MAX_PRERESERVE_TICKS`): two sentinel-heavy legs
    // (e.g. BTC) summed can exceed the budget → a single multi-GB up-front alloc.
    let mut tick_prices: Vec<(i64, f64)> = Vec::with_capacity(clamp_prereserve(est_synth_ticks));
    // leg_a == base, leg_b == quote (see `resolve_synth_work`). Tie-break favors
    // base on equal ts — identical to backfill's `merge_pop` (ta <= tb → base).
    let mut merge_state = nxr_sdk::synth::SynthReplayState::new(synth_id, leg_a_id, leg_b_id);
    loop {
        // Pick side with smaller ts (ties favor a/base → matches backfill merge).
        let take_a = match (&a_next, &b_next) {
            (Some(a), Some(b)) => a.0 <= b.0,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        let (ts, rec) = if take_a {
            let cur = a_next.take().expect("a_next primed");
            a_next = match leg_a_stream.next_tick() {
                Ok(v) => v,
                Err(e) => {
                    return CalOutcome::Failed {
                        ticker_id: synth_id,
                        reason: format!("read leg_a: {}", e),
                    }
                }
            };
            cur
        } else {
            let cur = b_next.take().expect("b_next primed");
            b_next = match leg_b_stream.next_tick() {
                Ok(v) => v,
                Err(e) => {
                    return CalOutcome::Failed {
                        ticker_id: synth_id,
                        reason: format!("read leg_b: {}", e),
                    }
                }
            };
            cur
        };

        // GATED reconstruction: feed the leg tick through the shared state
        // machine with now_ms = this tick's ts (purely leg-to-leg TTL, never
        // replay-clock drift — identical to backfill pass B). A synth tick is
        // pushed ONLY when both legs are live, within TTL, and pass conf/sanity.
        if let Some(synth_rec) = merge_state.feed_leg_tick(&rec, ts) {
            let body = synth_rec.index;
            let mid = (body.bid + body.ask) * 0.5;
            // Full-tick synth path point (the calibration tick stream). The leg
            // merge is monotone-ascending in ts (both legs are ts-ordered and we
            // always advance the earlier side), so push preserves path order.
            tick_prices.push((ts, mid));
        }
    }
    // Drop leg streams now — frees the two ShardStream buffers before the
    // vol-write + sigma-cache + calibrator stage runs.
    drop(leg_a_stream);
    drop(leg_b_stream);

    if tick_prices.is_empty() {
        return CalOutcome::Skipped {
            ticker_id: synth_id,
            reason: "empty merged stream".into(),
        };
    }

    // ── 3. Build .vol from the persisted synth `.s10` shards (identical to the
    // native base path) → offline σ == live σ on the SAME real s10 artifact.
    let vol_path = std::env::temp_dir().join(format!(
        "nxr-calibrate-synth-{}-{}.vol",
        synth_id,
        std::process::id()
    ));
    {
        let mut writer = match VolWriter::new(&vol_path) {
            Ok(w) => w,
            Err(e) => {
                return CalOutcome::Failed {
                    ticker_id: synth_id,
                    reason: format!("vol writer: {}", e),
                }
            }
        };
        let s10_dir = nxr_sdk::shard::bars_dir(bars_root, synth_id);
        let s10_shards = list_shards(&s10_dir, "s10").unwrap_or_default();
        if s10_shards.is_empty() {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Skipped {
                ticker_id: synth_id,
                reason: format!(
                    "no synth .s10 shards under {} (run synth-backfill-from-idx first)",
                    s10_dir.display()
                ),
            };
        }
        let mut s10_iter = S10ShardIter::new(s10_shards);
        if let Err(e) = build_vol_from_s10(|| s10_iter.next_bar(), vol_cfg, &mut writer) {
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("vol build: {}", e),
            };
        }
        if let Err(e) = writer.finish() {
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("vol finish: {}", e),
            };
        }
    }
    let vol_mmap = match VolMmap::open(&vol_path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("vol mmap: {}", e),
            };
        }
    };
    let sigma_cache = {
        let mut calc = MtfVolCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let base = RenkoConfig {
        multiplier: RenkoConfig::default().multiplier,
        min_pct: renko_yml.min_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed {
            ticker_id: synth_id,
            reason: format!("base renko cfg: {}", e),
        };
    }

    let cal_inner = calibration_inner(cal_ext);
    let window = trailing_window(&tick_prices, cal_inner.rolling_window_days);
    info!(
        synth_id,
        synth_sym,
        leg_a_id,
        leg_b_id,
        target_bpd,
        n_ticks = window.len(),
        window_days = cal_inner.rolling_window_days,
        "calibrating synth (direct scale-to-target, full-tick)"
    );
    // Same direct solver the base path uses (methodology §4). prior_k = yesterday's.
    let mult = scale_to_target_k(
        window,
        &cal_inner,
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
        prior_k,
    );

    if mult > 0.0 && (mult as f64).is_finite() {
        drift_gate(
            "synth",
            synth_id,
            window,
            &cal_inner,
            &base,
            &vol_mmap,
            vol_cfg,
            &sigma_cache,
            target_bpd,
            mult as f64,
        );
    }

    let _ = std::fs::remove_file(&vol_path);

    if !(mult > 0.0 && (mult as f64).is_finite()) {
        return CalOutcome::Failed {
            ticker_id: synth_id,
            reason: "synth calibration returned 0 (degenerate window / unreachable target)".into(),
        };
    }
    CalOutcome::Ok {
        ticker_id: synth_id,
        k: mult as f64,
    }
}

/// Resolve all synth-pair entries (from YAML or audit-frozen fallback) to
/// `(synth_id, sym, leg_a_id, leg_b_id)`. Entries that fail to resolve any
/// leg are dropped with a warn.
fn resolve_synth_work(yml_pairs: &[SynthPairYml]) -> Vec<(u64, &'static str, u64, u64)> {
    // Build a `'static`-lifetime spec view: leaked owned strings from YAML,
    // or direct reference to the sdk default array.
    let owned: Vec<SynthPairSpec>;
    let specs: &[SynthPairSpec] = if yml_pairs.is_empty() {
        warn!("synths.initial_pairs empty in YAML — falling back to DEFAULT_INITIAL_SYNTH_PAIRS");
        DEFAULT_INITIAL_SYNTH_PAIRS
    } else {
        owned = yml_pairs
            .iter()
            .map(|y| SynthPairSpec {
                synth_sym: Box::leak(y.synth_sym.clone().into_boxed_str()),
                base_sym: Box::leak(y.base_sym.clone().into_boxed_str()),
                quote_sym: Box::leak(y.quote_sym.clone().into_boxed_str()),
            })
            .collect();
        &owned
    };

    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        let resolve = |sym: &str| -> Option<u64> {
            match resolve_ticker(sym, InstrumentType::SPOT) {
                Ok(m) => Some(m.ticker.id),
                Err(e) => {
                    warn!(sym, err = ?e, "synth pair resolve failed; skipping");
                    None
                }
            }
        };
        let synth_id = match resolve(spec.synth_sym) {
            Some(v) => v,
            None => continue,
        };
        let leg_a_id = match resolve(spec.base_sym) {
            Some(v) => v,
            None => continue,
        };
        let leg_b_id = match resolve(spec.quote_sym) {
            Some(v) => v,
            None => continue,
        };
        out.push((synth_id, spec.synth_sym, leg_a_id, leg_b_id));
    }
    out
}

// ── Main run ─────────────────────────────────────────────────────────────────

fn run_once(args: &Args) -> Result<()> {
    let root: PipelineYml = PipelineYml::load_default(nxr_sdk::pipeline_config::ConfigHint::Bin)?;
    let series = &root.series;
    // Operator judgment lists (YAML override w/ audit-frozen sdk fallback).
    // Used for within-MITCH-class buckets that the wire bits don't encode
    // (major-vs-alt within CR, stablecoin pairs within CR, major-vs-cross
    // within FX). Empty YAML → fallback + warn so cfg drift is visible.
    if root.cexs.crypto_majors.is_empty() {
        warn!("cexs.crypto_majors empty in YAML — falling back to DEFAULT_CRYPTO_MAJORS");
    }
    if root.cexs.stablecoins.is_empty() {
        warn!("cexs.stablecoins empty in YAML — falling back to DEFAULT_STABLECOINS");
    }
    if root.cexs.fx_majors.is_empty() {
        warn!("cexs.fx_majors empty in YAML — falling back to DEFAULT_FX_MAJORS");
    }
    let crypto_majors = effective_list(&root.cexs.crypto_majors, DEFAULT_CRYPTO_MAJORS);
    let stablecoins = effective_list(&root.cexs.stablecoins, DEFAULT_STABLECOINS);
    let fx_majors = effective_list(&root.cexs.fx_majors, DEFAULT_FX_MAJORS);

    let nxr_cfg = nxr_sdk::NxrConfig::from_env();
    let params_path = PathBuf::from(&nxr_cfg.ticker_params_path);
    let idx_dir = PathBuf::from(&nxr_cfg.indexes_dir);
    // Bars root holds the per-ticker `.s10` shards (the canonical vol basis).
    // `bars_dir` = `<root>/bars`; sharding helpers want the data root, so use
    // the parent (mirrors s10_from_idx.rs derivation).
    let bars_root = Path::new(&nxr_cfg.bars_dir)
        .parent()
        .unwrap_or(Path::new("/data"))
        .to_path_buf();
    let cfg_path = nxr_sdk::pipeline_config::PipelineYml::resolve_path(
        nxr_sdk::pipeline_config::ConfigHint::Bin,
    );

    info!(
        cfg = %cfg_path.display(),
        params = %params_path.display(),
        idx = %idx_dir.display(),
        parallel = args.parallel,
        "nxr-calibrate starting"
    );

    // Load the existing weights file so we can preserve volumes/exchanges/etc.
    let mut weights_file: WeightsFile = if params_path.exists() {
        let raw = std::fs::read_to_string(&params_path)
            .with_context(|| format!("read {}", params_path.display()))?;
        serde_json::from_str(&raw).with_context(|| format!("parse {}", params_path.display()))?
    } else {
        warn!(path = %params_path.display(), "ticker-params.json missing — starting from scratch");
        WeightsFile::default()
    };

    // Build the work list: (pair, ticker_id, class). De-dupe across providers
    // since the same ticker_id appears under multiple exchanges in pair_volumes.
    let mut seen: HashMap<u64, (String, AssetClassBucket)> = HashMap::new();
    for pairs in weights_file.pair_volumes.values() {
        for pair in pairs.keys() {
            let ticker_id = resolve_ticker_id(pair);
            let class = bucket_for_pair(pair, ticker_id, &crypto_majors, &stablecoins, &fx_majors);
            seen.entry(ticker_id)
                .or_insert_with(|| (pair.clone(), class));
        }
    }
    let work: Vec<(u64, String, AssetClassBucket)> =
        seen.into_iter().map(|(id, (p, c))| (id, p, c)).collect();
    info!(n_tickers = work.len(), "ticker universe assembled");

    // Configure rayon worker count.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.parallel.max(1))
        .build()
        .with_context(|| "build rayon pool")?;

    let cal_ext = &series.calibration;
    let vol_cfg = &series.vol;
    let renko_yml = &series.renko;

    // Prior-day k seeds (warm start for the direct scale-to-target solver), read
    // from the existing weights file BEFORE it is mutated. Keyed by ticker_id
    // string. Absent ⇒ the solver uses its k0=0.5 cold-start default. This is a
    // SEARCH SEED only — never an emit fallback (renko_k starts empty).
    let prior_k_map: BTreeMap<String, f64> = weights_file.renko_k_per_ticker.clone();
    let prior_k_for =
        |id: u64| -> Option<f32> { prior_k_map.get(&id.to_string()).map(|&k| k as f32) };

    // Fail fast if config's mult_bounds disagree with the SDK's single-source
    // renko ceiling/floor (RCA ROOT2a). A mismatch makes the clamp-detector
    // watch the wrong wall and the search park at a lattice artifact.
    cal_ext
        .assert_bounds_consistent()
        .map_err(|e| anyhow::anyhow!(e))?;

    let results: Mutex<Vec<CalOutcome>> = Mutex::new(Vec::with_capacity(work.len()));

    pool.install(|| {
        work.par_iter().for_each(|(ticker_id, pair, class)| {
            // Per-pair override → per-class default (e.g. crypto_stable → 50,
            // detected from the already-computed bucket) → flat default. No skip
            // path — operator policy: never skip a day, always return a target.
            let target_bpd = target_for_pair(cal_ext, pair, *class);

            // PART B4 (2026-06-09): per-pair FORCED renko-k escape hatch. If the
            // operator pinned a k for this pair (e.g. a structural-floor ticker
            // the staircase keeps out of accept tol), emit it DIRECTLY and skip
            // the fit — provided it is within [K_FLOOR, MULT_UPPER_BOUND].
            if let Some(&forced_k) = cal_ext.renko_k_overrides.get(pair) {
                if (K_FLOOR..=K_MAX_SAFETY).contains(&forced_k) {
                    info!(
                        ticker_id = *ticker_id,
                        pair, forced_k, "renko_k override — skipping fit (operator-forced k)"
                    );
                    results.lock().unwrap().push(CalOutcome::Ok {
                        ticker_id: *ticker_id,
                        k: forced_k,
                    });
                    return;
                }
                warn!(
                    ticker_id = *ticker_id,
                    pair,
                    forced_k,
                    k_floor = K_FLOOR,
                    k_max_safety = K_MAX_SAFETY,
                    "renko_k override out of [K_FLOOR, K_MAX_SAFETY] — ignoring, running fit"
                );
            }

            // Panic-safe: one bad ticker (malformed .idx, OOM in sigma cache,
            // ...) must not abort the whole cron. AssertUnwindSafe is sound
            // here because nothing inside is moved across the boundary.
            let pair_clone = pair.clone();
            let class_clone = *class;
            let prior_k = prior_k_for(*ticker_id);
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                calibrate_one(
                    *ticker_id,
                    &pair_clone,
                    class_clone,
                    &idx_dir,
                    &bars_root,
                    cal_ext,
                    target_bpd,
                    vol_cfg,
                    renko_yml,
                    prior_k,
                )
            }))
            .unwrap_or_else(|p| {
                let msg = if let Some(s) = p.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = p.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                CalOutcome::Failed {
                    ticker_id: *ticker_id,
                    reason: format!("panic: {}", msg),
                }
            });

            results.lock().unwrap().push(outcome);
        });
    });

    // Tally.
    let outcomes = results.into_inner().unwrap();
    let (mut passed, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    // Start renko_k EMPTY. Only Ok outcomes populate.
    // Failed/Skipped tickers are NOT carried over from the prior weights file.
    // Policy: skip the day when calibration fails; never bootstrap a k. Carrying
    // forward stale k corrupts the live renko engine for tickers whose σ regime
    // has shifted since last successful calibration (renko_k cohort 2026-06-01
    // found 91 % of base tickers using prior-run k due to today's pass=17/188).
    let prior_count = weights_file.renko_k_per_ticker.len();
    let mut renko_k: BTreeMap<String, f64> = BTreeMap::new();

    for o in &outcomes {
        match o {
            CalOutcome::Ok { ticker_id, k } => {
                passed += 1;
                renko_k.insert(ticker_id.to_string(), *k);
            }
            CalOutcome::Skipped { ticker_id, reason } => {
                skipped += 1;
                info!(ticker_id, %reason, "skipped");
            }
            CalOutcome::Failed { ticker_id, reason } => {
                failed += 1;
                warn!(ticker_id, %reason, "calibration failed");
            }
        }
    }

    info!(
        passed,
        skipped,
        failed,
        total = outcomes.len(),
        prior_entries = prior_count,
        kept_entries = renko_k.len(),
        dropped_stale = prior_count.saturating_sub(renko_k.len()),
        "calibration summary (base; stale entries dropped, never carried forward)"
    );

    // k-STABILITY DIAGNOSTIC (2026-06-09): log the DISTRIBUTION of accepted k
    // values so the operator can see at a glance whether k is stable/clustered
    // (good σ — k is the intended mostly-stable per-ticker normalization) or
    // wild (σ problem — the daily adaptiveness has leaked into k). Now that k has
    // NO upper cap, a fat right tail here is the canary for a σ regression.
    {
        let mut ks: Vec<f64> = renko_k.values().copied().collect();
        ks.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if ks.is_empty() {
            warn!("k-stability (base): no accepted k values");
        } else {
            let n = ks.len();
            let k_min = ks[0];
            let k_max = ks[n - 1];
            let k_median = if n % 2 == 1 {
                ks[n / 2]
            } else {
                0.5 * (ks[n / 2 - 1] + ks[n / 2])
            };
            info!(
                k_count = n,
                k_min,
                k_median,
                k_max,
                spread = k_max / k_min.max(f64::MIN_POSITIVE),
                "k-stability distribution (base) — clustered=good σ, wide=σ problem"
            );
        }
    }

    // SLA-CRITICAL: write ticker-params.json AFTER base pass so
    // renko emission unblocks for base tickers even if synth pass hangs/fails.
    // Synth pass below re-writes with synth k's added.
    weights_file.renko_k_per_ticker = renko_k.clone();
    weights_file.calibrated_at = Some(nxr_sdk::now_sec());
    let json_base = serde_json::to_string_pretty(&weights_file)?;
    write_atomic_string(&params_path, &json_base)?;
    info!(path = %params_path.display(), bytes = json_base.len(), base_k_count = renko_k.len(), "ticker-params.json updated (base pass)");

    // ── Synth-pair pass (5 crosses) ──────────────────────────────────────────
    // Runs unconditionally after the base pass. Cheap (~10 pairs, mostly
    // bound by leg .idx I/O which the base pass already warmed in page
    // cache). Synths route through the SAME `scale_to_target_k` direct solver
    // the base pass uses (warm start + bounded bracket fallback + ±1-rung probe;
    // see `calibrate_one_synth`); the K_FLOOR / min_pct-clamp / unreachable-target
    // guards inside it drop degenerate windows. If the fit fails, k is NOT
    // persisted (caller keeps prior). Per-pair override or flat default per
    // synth.
    let synth_work = {
        let pairs = nxr_sdk::synth::pipeline_pairs::synth_pipeline_pairs(&root);
        resolve_synth_work(&pairs)
    };
    info!(
        n_synth = synth_work.len(),
        "synth calibration pass starting"
    );
    let (mut s_passed, mut s_skipped, mut s_failed) = (0usize, 0usize, 0usize);
    for (synth_id, synth_sym, leg_a_id, leg_b_id) in synth_work {
        // Class-detect the synth pair too (stable/stable crosses like USD1/USDC
        // → crypto_stable → 50) instead of relying on a manual override entry.
        let synth_class = bucket_for_pair(
            synth_sym,
            synth_id,
            &crypto_majors,
            &stablecoins,
            &fx_majors,
        );
        let synth_target = target_for_pair(cal_ext, synth_sym, synth_class);
        // PART B4: synth pairs honor the same per-pair forced-k escape hatch.
        if let Some(&forced_k) = cal_ext.renko_k_overrides.get(synth_sym) {
            if (K_FLOOR..=K_MAX_SAFETY).contains(&forced_k) {
                info!(
                    synth_id,
                    synth_sym,
                    forced_k,
                    "synth renko_k override — skipping fit (operator-forced k)"
                );
                s_passed += 1;
                renko_k.insert(synth_id.to_string(), forced_k);
                continue;
            }
            warn!(
                synth_id,
                synth_sym,
                forced_k,
                "synth renko_k override out of [K_FLOOR, K_MAX_SAFETY] — ignoring, running fit"
            );
        }
        let synth_prior_k = prior_k_for(synth_id);
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            calibrate_one_synth(
                synth_id,
                synth_sym,
                leg_a_id,
                leg_b_id,
                &idx_dir,
                &bars_root,
                cal_ext,
                synth_target,
                vol_cfg,
                renko_yml,
                synth_prior_k,
            )
        }))
        .unwrap_or_else(|p| {
            let msg = if let Some(s) = p.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = p.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            CalOutcome::Failed {
                ticker_id: synth_id,
                reason: format!("panic: {}", msg),
            }
        });
        match outcome {
            CalOutcome::Ok { ticker_id, k } => {
                s_passed += 1;
                info!(synth_id = ticker_id, synth_sym, k, "synth calibrated");
                renko_k.insert(ticker_id.to_string(), k);
            }
            CalOutcome::Skipped { ticker_id, reason } => {
                s_skipped += 1;
                info!(synth_id = ticker_id, synth_sym, %reason, "synth skipped");
            }
            CalOutcome::Failed { ticker_id, reason } => {
                s_failed += 1;
                warn!(synth_id = ticker_id, synth_sym, %reason, "synth failed");
            }
        }
    }
    info!(s_passed, s_skipped, s_failed, "calibration summary (synth)");

    // ── Inferred xxx/USDC fallback (2026-06-10) ──────────────────────────────
    // Inferred USDC-quoted tickers only materialize live `.idx` since the
    // migration (≈2026-06-03), so their base fit fails on span for weeks and
    // the stale-drop policy (no stale-k carry-over) then wipes their k —
    // silencing live renko for pairs downstream consumers need (ETH/USDC).
    // Until the inferred span covers the rolling window, derive k synth-style
    // from the USDT legs — the identical math the live inference uses
    // (xxx/USDT × 1/(USDC/USDT)). Guarded: only when the USDT leg itself
    // calibrated this run (healthy legs), never for stable/USDC pairs (those
    // route to overrides), and never overwriting an accepted base fit.
    if let Ok(q) = resolve_ticker("USDC/USDT", InstrumentType::SPOT) {
        let quote_leg_id = q.ticker.id;
        let mut inferred_fallbacks = 0usize;
        for (ticker_id, pair, class) in &work {
            if renko_k.contains_key(&ticker_id.to_string()) {
                continue;
            }
            let Some(base_sym) = pair.strip_suffix("/USDC") else {
                continue;
            };
            if stablecoins.iter().any(|s| s.eq_ignore_ascii_case(base_sym)) {
                continue;
            }
            let leg_pair = format!("{}/USDT", base_sym);
            let Ok(leg) = resolve_ticker(&leg_pair, InstrumentType::SPOT) else {
                continue;
            };
            if !renko_k.contains_key(&leg.ticker.id.to_string()) {
                continue; // unhealthy leg ⇒ no basis for a derived k
            }
            let target = target_for_pair(cal_ext, pair, *class);
            let prior = prior_k_for(*ticker_id);
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                calibrate_one_synth(
                    *ticker_id,
                    pair,
                    leg.ticker.id,
                    quote_leg_id,
                    &idx_dir,
                    &bars_root,
                    cal_ext,
                    target,
                    vol_cfg,
                    renko_yml,
                    prior,
                )
            }))
            .unwrap_or_else(|_| CalOutcome::Failed {
                ticker_id: *ticker_id,
                reason: "panic in inferred-USDC fallback".into(),
            });
            match outcome {
                CalOutcome::Ok { ticker_id, k } => {
                    inferred_fallbacks += 1;
                    info!(ticker_id, pair = %pair, k, "inferred xxx/USDC k derived from USDT legs (span fallback)");
                    renko_k.insert(ticker_id.to_string(), k);
                }
                CalOutcome::Skipped { ticker_id, reason } => {
                    info!(ticker_id, pair = %pair, %reason, "inferred-USDC fallback skipped");
                }
                CalOutcome::Failed { ticker_id, reason } => {
                    warn!(ticker_id, pair = %pair, %reason, "inferred-USDC fallback failed");
                }
            }
        }
        info!(
            inferred_fallbacks,
            "inferred xxx/USDC fallback pass complete"
        );
    }

    // k-STABILITY DIAGNOSTIC (2026-06-09): final distribution over ALL accepted
    // base+synth k values — the operator's at-a-glance σ-health check now that k
    // is uncapped.
    {
        let mut ks: Vec<f64> = renko_k.values().copied().collect();
        ks.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if ks.is_empty() {
            warn!("k-stability (all): no accepted k values");
        } else {
            let n = ks.len();
            let k_min = ks[0];
            let k_max = ks[n - 1];
            let k_median = if n % 2 == 1 {
                ks[n / 2]
            } else {
                0.5 * (ks[n / 2 - 1] + ks[n / 2])
            };
            info!(
                k_count = n,
                k_min,
                k_median,
                k_max,
                spread = k_max / k_min.max(f64::MIN_POSITIVE),
                "k-stability distribution (base+synth) — clustered=good σ, wide=σ problem"
            );
        }
    }

    weights_file.renko_k_per_ticker = renko_k;
    weights_file.calibrated_at = Some(nxr_sdk::now_sec());

    let json = serde_json::to_string_pretty(&weights_file)?;
    // write_atomic requires Pod; we have a String → emit via a tiny helper
    // that mirrors its tmp+rename semantics for the JSON case.
    write_atomic_string(&params_path, &json)?;
    info!(path = %params_path.display(), bytes = json.len(), "ticker-params.json updated");

    Ok(())
}

/// Atomic JSON-string write: `<path>.tmp` + rename. Mirrors
/// `nxr_sdk::ipc::write_atomic` but for non-Pod payloads.
fn write_atomic_string(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create_dir_all {:?}", parent))?;
    }
    let tmp = {
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if ext.is_empty() {
            path.with_extension("tmp")
        } else {
            path.with_extension(format!("{ext}.tmp"))
        }
    };
    std::fs::write(&tmp, contents).with_context(|| format!("write {:?}", tmp))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename {:?} -> {:?}", tmp, path))?;
    Ok(())
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();

    loop {
        if let Err(e) = run_once(&args) {
            warn!(err = %e, "calibration run failed");
        }
        if args.once {
            break;
        }
        info!("sleeping 24h until next calibration");
        std::thread::sleep(std::time::Duration::from_secs(nxr_sdk::shard::SECS_PER_DAY));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records-per-byte sizing must match the on-disk `IndexRecord` stride the
    /// loader reads (`ShardStream` reads `file_bytes / size_of::<IndexRecord>()`
    /// with no file header), so `bytes / 56` is the exact pre-filter count.
    #[test]
    fn index_record_is_56_bytes() {
        assert_eq!(core::mem::size_of::<IndexRecord>(), 56);
    }

    /// The pre-size estimate (`Σ shard_bytes / 56`) is an upper bound on records
    /// kept; for a realistic, lightly-filtered shard set it stays within ~1.2× of
    /// the actual finite-mid count. Model a 1 GB shard (≈18.7M records on disk)
    /// where ~10% are heartbeat sentinels: estimate / actual ≈ 1.11 ≤ 1.2.
    #[test]
    fn estimate_within_1_2x_of_actual_when_lightly_filtered() {
        let on_disk_records = 1_073_741_824usize / 56; // 1 GiB of IndexRecords
        let actual_kept = (on_disk_records as f64 * 0.90) as usize; // 10% sentinels
        let estimate = on_disk_records; // bytes/56 == record count
        let ratio = estimate as f64 / actual_kept as f64;
        assert!(
            ratio <= 1.2,
            "estimate {} / actual {} = {:.3} > 1.2",
            estimate,
            actual_kept,
            ratio
        );
        // And the clamp is a no-op here (well under the cap).
        assert_eq!(clamp_prereserve(estimate), estimate);
    }

    /// The clamp must never let the reservation exceed `MAX_PRERESERVE_TICKS`,
    /// even for the BTC OOM case (~763M estimated → 12 GB at 16 B/elem).
    #[test]
    fn clamp_never_exceeds_cap() {
        // BTC OOM reproduction: 12211304480 bytes / 16 = 763_206_530 elements.
        let btc_oom_estimate = 12_211_304_480usize / 16;
        assert!(btc_oom_estimate > MAX_PRERESERVE_TICKS);
        assert_eq!(clamp_prereserve(btc_oom_estimate), MAX_PRERESERVE_TICKS);

        // Arbitrary huge / saturating estimates also clamp.
        assert_eq!(clamp_prereserve(usize::MAX), MAX_PRERESERVE_TICKS);
        assert_eq!(
            clamp_prereserve(MAX_PRERESERVE_TICKS + 1),
            MAX_PRERESERVE_TICKS
        );

        // At/below the cap the estimate passes through unchanged.
        assert_eq!(clamp_prereserve(MAX_PRERESERVE_TICKS), MAX_PRERESERVE_TICKS);
        assert_eq!(clamp_prereserve(0), 0);
        assert_eq!(clamp_prereserve(1_000), 1_000);
    }

    /// Capped reservation byte ceiling: 64M × 16 B ≈ 1 GiB, comfortably below the
    /// per-worker budget that the raw 12 GB estimate blew past.
    #[test]
    fn capped_reservation_byte_ceiling_is_about_1gib() {
        let bytes = MAX_PRERESERVE_TICKS * core::mem::size_of::<(i64, f64)>();
        assert_eq!(core::mem::size_of::<(i64, f64)>(), 16);
        assert!(
            bytes <= 1_100_000_000,
            "capped reservation {} B > ~1 GiB",
            bytes
        );
    }

    // ── §5 PARITY GUARD (RCA #1763) ─────────────────────────────────────────
    // Regression test: calibrate's synth reconstruction must produce the
    // BYTE-IDENTICAL gated synth tick sequence the backfill driver produces.
    // Both now drive `nxr_sdk::synth::SynthReplayState::feed_leg_tick` over the
    // same ts-ascending leg merge (tie → base), with now_ms = inbound tick ts.
    // The historical bug: calibrate merged the legs UNGATED → it counted synth
    // crossings during stale-leg / low-conf windows that the gated backfill
    // stream never emits → k fit on a denser stream than applied → median bpd
    // collapse. This test fails if the two reconstructions ever diverge again.

    use mitch::header::MitchHeader;
    use mitch::index::Index;

    const T_BASE_ID: u64 = 0xAAAA_AAAA_AAAA_AAAA;
    const T_QUOTE_ID: u64 = 0xBBBB_BBBB_BBBB_BBBB;
    const T_SYNTH_ID: u64 = 0xCCCC_CCCC_CCCC_CCCC;

    fn mk_rec(ticker: u64, bid: f64, ask: f64, conf: u8, ts_ms: i64) -> IndexRecord {
        let mts = timestamp::from_epoch_ms(ts_ms);
        let header = MitchHeader::new(mitch::common::message_type::INDEX, 1, mts, 1);
        let idx = Index {
            ticker,
            bid,
            ask,
            vbid: 100,
            vask: 100,
            ci: 0,
            tick_count: 1,
            confidence: conf,
            accepted: conf,
            rejected: 0,
            flags: 0,
        };
        IndexRecord::new(header, idx)
    }

    /// Drive a state machine the way CALIBRATE does (in-line ts-merge of two
    /// look-ahead slots; tie favors base; feed with now_ms = inbound ts;
    /// collect `(ts, mid)` only on a gated `Some`). Mirrors the loop in
    /// `calibrate_one_synth` exactly — kept in lock-step with it.
    fn reconstruct_calibrate(base: &[IndexRecord], quote: &[IndexRecord]) -> Vec<(i64, f64)> {
        let to_slot =
            |r: &IndexRecord| (timestamp::to_epoch_ms(r.header.get_timestamp()), r.clone());
        let mut bi = base.iter();
        let mut qi = quote.iter();
        let mut a_next = bi.next().map(to_slot);
        let mut b_next = qi.next().map(to_slot);
        let mut st = nxr_sdk::synth::SynthReplayState::new(T_SYNTH_ID, T_BASE_ID, T_QUOTE_ID);
        let mut out = Vec::new();
        loop {
            let take_a = match (&a_next, &b_next) {
                (Some(a), Some(b)) => a.0 <= b.0,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let (ts, rec) = if take_a {
                let cur = a_next.take().unwrap();
                a_next = bi.next().map(to_slot);
                cur
            } else {
                let cur = b_next.take().unwrap();
                b_next = qi.next().map(to_slot);
                cur
            };
            if let Some(s) = st.feed_leg_tick(&rec, ts) {
                let b = s.index;
                out.push((ts, (b.bid + b.ask) * 0.5));
            }
        }
        out
    }

    /// Drive the SAME state machine the way BACKFILL pass B does: `merge_pop`
    /// (peek both, pop older, tie → base) then `feed_leg_tick(rec, rec_ts_ms)`.
    fn reconstruct_backfill(base: &[IndexRecord], quote: &[IndexRecord]) -> Vec<(i64, f64)> {
        let mut bi = base.iter().peekable();
        let mut qi = quote.iter().peekable();
        let ts_of = |r: &IndexRecord| timestamp::to_epoch_ms(r.header.get_timestamp());
        let mut st = nxr_sdk::synth::SynthReplayState::new(T_SYNTH_ID, T_BASE_ID, T_QUOTE_ID);
        let mut out = Vec::new();
        loop {
            let rec = match (bi.peek(), qi.peek()) {
                (None, None) => break,
                (Some(_), None) => bi.next().unwrap(),
                (None, Some(_)) => qi.next().unwrap(),
                (Some(b), Some(q)) => {
                    if ts_of(b) <= ts_of(q) {
                        bi.next().unwrap()
                    } else {
                        qi.next().unwrap()
                    }
                }
            };
            let ts = ts_of(rec);
            if let Some(s) = st.feed_leg_tick(rec, ts) {
                let b = s.index;
                out.push((ts, (b.bid + b.ask) * 0.5));
            }
        }
        out
    }

    #[test]
    fn calibrate_backfill_reconstruction_parity() {
        // 16 ms grid (above mts 16 us granularity) so ms round-trips exactly.
        let t0: i64 = 1_700_000_000_000;
        let s: i64 = 16;
        // Interleaved legs with a deliberate STALE window: quote goes silent
        // from t0+2s..t0+9s (> 5 s TTL) while base keeps ticking. An ungated
        // merge would emit a synth on every base tick in that window; the gate
        // must drop them — and BOTH paths must drop the SAME ones.
        let base = vec![
            mk_rec(T_BASE_ID, 3000.0, 3001.0, 3, t0 + 1 * s),
            mk_rec(T_BASE_ID, 3002.0, 3003.0, 3, t0 + 2_000),
            mk_rec(T_BASE_ID, 3004.0, 3005.0, 3, t0 + 4_000), // quote stale > 5s? no (gap 2s)
            mk_rec(T_BASE_ID, 3006.0, 3007.0, 3, t0 + 8_000), // quote (t0+1) stale by 8s ⇒ drop
            mk_rec(T_BASE_ID, 3008.0, 3009.0, 3, t0 + 10_000),
            // low-confidence leg → sanity/conf gate drop:
            mk_rec(T_BASE_ID, 3010.0, 3011.0, 0, t0 + 12_000),
            mk_rec(T_BASE_ID, 3012.0, 3013.0, 3, t0 + 14_000),
        ];
        let quote = vec![
            mk_rec(T_QUOTE_ID, 60_000.0, 60_010.0, 3, t0 + 1 * s),
            mk_rec(T_QUOTE_ID, 60_002.0, 60_012.0, 3, t0 + 2_000),
            mk_rec(T_QUOTE_ID, 60_004.0, 60_014.0, 3, t0 + 9_000),
            mk_rec(T_QUOTE_ID, 60_006.0, 60_016.0, 3, t0 + 11_000),
            mk_rec(T_QUOTE_ID, 60_008.0, 60_018.0, 3, t0 + 13_000),
            mk_rec(T_QUOTE_ID, 60_010.0, 60_020.0, 3, t0 + 15_000),
        ];

        let cal = reconstruct_calibrate(&base, &quote);
        let bkf = reconstruct_backfill(&base, &quote);
        assert_eq!(
            cal, bkf,
            "calibrate vs backfill gated reconstruction MUST be byte-identical"
        );

        // Guard the gate actually fired: an UNGATED merge (old calibrate bug)
        // emits strictly MORE synth ticks than the gated path. If this ever
        // becomes equal, the TTL/conf gate has been removed from calibrate.
        let ungated = {
            let mut bi = base.iter().peekable();
            let mut qi = quote.iter().peekable();
            let ts_of = |r: &IndexRecord| timestamp::to_epoch_ms(r.header.get_timestamp());
            let (mut lb, mut lq): (Option<Index>, Option<Index>) = (None, None);
            let mut n = 0usize;
            loop {
                let rec = match (bi.peek(), qi.peek()) {
                    (None, None) => break,
                    (Some(_), None) => bi.next().unwrap(),
                    (None, Some(_)) => qi.next().unwrap(),
                    (Some(b), Some(q)) => {
                        if ts_of(b) <= ts_of(q) {
                            bi.next().unwrap()
                        } else {
                            qi.next().unwrap()
                        }
                    }
                };
                if rec.index.ticker == T_BASE_ID {
                    lb = Some(rec.index);
                } else {
                    lq = Some(rec.index);
                }
                if let (Some(b), Some(q)) = (lb, lq) {
                    let bid = b.bid / q.ask;
                    let ask = b.ask / q.bid;
                    if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 {
                        n += 1;
                    }
                }
            }
            n
        };
        assert!(
            ungated > cal.len(),
            "gate must prune stale/low-conf ticks: ungated={} gated={}",
            ungated,
            cal.len()
        );
    }
}
