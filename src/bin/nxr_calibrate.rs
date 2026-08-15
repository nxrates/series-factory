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
use nxr_sdk::synth::pairs::SynthPairSpec;

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(about = "Daily Renko k calibration cron.")]
struct Args {
    /// Run once and exit (default for k8s CronJob).
    #[arg(long)]
    once: bool,
    /// Rayon worker count for the per-ticker loop. Keep low to bound RAM
    /// (each ticker mmaps a .idx and builds a sigma cache). Default 2, NOT 4:
    /// `--parallel 4` drove node load to 322 and took the public feed down
    /// (2026-06 outage). The k8s CronJob passes 2 explicitly; this default now
    /// matches it so an ad-hoc manual run cannot reintroduce the outage.
    #[arg(long, default_value_t = 2)]
    parallel: usize,
    /// Override the trailing fit window (`calibration.rolling_window_days`, YAML
    /// default 730). Exists to answer "is 730 d actually needed?" — the fit is a
    /// volatility-scale estimate, so a shorter window may land the same k for a
    /// fraction of the RAM/IO/time. k is only comparable across runs at EQUAL
    /// window, so the value used is recorded in `last_run.window_days`.
    #[arg(long)]
    window_days: Option<u32>,
}

// ── ticker-params.json store ─────────────────────────────────────────────────

/// Serialized read-modify-atomic-rename access to `ticker-params.json`.
///
/// The file is SHARED with `nxr-weights` (hourly): it owns `generated_at` /
/// `pair_volumes` / `exchanges`, we own the `renko_k*` / `calibration_status` /
/// `last_run` fields. The old code loaded the file once at startup and
/// serialized the whole struct up to 90 minutes later, silently reverting every
/// weights run in between — a lost update. Every write now re-reads immediately
/// before mutating, under TWO locks: an in-process mutex so the `--parallel`
/// workers cannot lose each other's updates (same bug class, one level down),
/// and a `ParamsLock` flock so the SEPARATE `nxr-weights` process cannot either
/// (the mutex is invisible to it — it is the original lost-update racer).
/// Mutex first, flock second: that ordering means only one thread of this
/// process ever holds the flock, so the per-open-fd flock cannot self-deadlock.
struct ParamsStore {
    path: PathBuf,
    lock: std::sync::Mutex<()>,
}

impl ParamsStore {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            lock: std::sync::Mutex::new(()),
        }
    }

    fn read(&self) -> Result<WeightsFile> {
        if !self.path.exists() {
            warn!(path = %self.path.display(), "ticker-params.json missing — starting from scratch");
            return Ok(WeightsFile::default());
        }
        let raw = std::fs::read_to_string(&self.path)
            .with_context(|| format!("read {}", self.path.display()))?;
        serde_json::from_str(&raw).with_context(|| format!("parse {}", self.path.display()))
    }

    /// Re-read, mutate ONLY our own fields inside `f`, atomic-rename. The whole
    /// read-modify-rename runs under the cross-process `ParamsLock`; a lock we
    /// cannot take is an error (the caller logs it and the ticker is retried next
    /// cycle) — never a silently dropped write.
    fn update<F: FnOnce(&mut WeightsFile)>(&self, f: F) -> Result<()> {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let _flock = nxr_sdk::weights_schema::ParamsLock::acquire(&self.path)
            .with_context(|| format!("lock {}", self.path.display()))?;
        let mut w = self.read()?;
        f(&mut w);
        write_atomic_string(&self.path, &serde_json::to_string_pretty(&w)?)
    }
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
        // SKIPPED, not Failed: semantically identical to the K_FLOOR bracket skip
        // — the asset has no brick structure to fit (pegged stable, thin
        // FX-quoted book). Which code path noticed is an implementation detail, so
        // it must not reach `consecutive_failures`: all 19 "failures" of the
        // 2026-07-25 run were this, and alerting on them would have started the
        // >=3 alert with 19 permanent false positives. The message keeps the
        // diagnostic granularity.
        return CalOutcome::Skipped {
            ticker_id,
            reason: "unreachable target (solver returned 0 / degenerate window)".into(),
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
        // Skipped, not Failed — same reasoning as the base pass above.
        return CalOutcome::Skipped {
            ticker_id: synth_id,
            reason: "synth unreachable target (solver returned 0 / degenerate window)".into(),
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
        // No fallback list: the hardcoded five were a second, divergent source
        // next to the derived set. An empty YAML means nothing to calibrate.
        warn!("synths.initial_pairs empty in YAML: nothing to calibrate");
        &[]
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

/// Persist ONE outcome before moving on: staged k (never the live map) +
/// per-ticker diagnostics, so a kill after this point keeps the work. Serialized
/// by `ParamsStore` (in-process mutex + flock) so `--parallel` workers and the
/// `nxr-weights` process cannot lose each other's entries. 35 KB file, so the
/// per-ticker cost is noise next to a fit.
///
/// Shared by all three passes (base loop, synth, inferred-USDC fallback) — the
/// synth and fallback work used to be unstaged, so a kill in either phase threw
/// it away every cycle. `count_in_run` is true for the BASE loop only, because
/// `tickers_total` is the base roster: the fallback re-fits ids the base loop
/// already counted (ratio > 1), and the synth universe is the full cross catalog,
/// most of which is structurally skipped for want of `.s10` (ratio → 0). Either
/// would make `nxr_calibrate_coverage_ratio` (= succeeded/total) lie.
///
/// `Skipped` is STRUCTURAL (no shards / window too short / target unreachable at
/// K_FLOOR — a pegged stable may be flat forever), so it must not feed
/// `consecutive_failures` or the >=3 alert would sit permanently red on assets
/// renko is simply not offered for.
fn stage_outcome(store: &ParamsStore, outcome: &CalOutcome, count_in_run: bool) {
    let now = nxr_sdk::now_sec();
    let (id_key, k_ok, err, skip) = match outcome {
        CalOutcome::Ok { ticker_id, k } => (ticker_id.to_string(), Some(*k), None, None),
        CalOutcome::Skipped { ticker_id, reason } => {
            (ticker_id.to_string(), None, None, Some(reason.clone()))
        }
        CalOutcome::Failed { ticker_id, reason } => {
            (ticker_id.to_string(), None, Some(reason.clone()), None)
        }
    };
    if let Err(e) = store.update(|w| {
        if let Some(k) = k_ok {
            w.renko_k_staged.insert(id_key.clone(), k);
        }
        let st = w.calibration_status.entry(id_key.clone()).or_default();
        st.last_attempt = Some(now);
        if k_ok.is_some() {
            st.last_success = Some(now);
            st.consecutive_failures = 0;
            st.last_error = None;
            st.last_skip_reason = None;
        } else if let Some(reason) = skip {
            st.last_skip_reason = Some(reason);
        } else {
            st.consecutive_failures = st.consecutive_failures.saturating_add(1);
            st.last_error = err;
        }
        if count_in_run {
            if let Some(r) = w.last_run.as_mut() {
                r.tickers_attempted += 1;
                if k_ok.is_some() {
                    r.tickers_succeeded += 1;
                }
            }
        }
    }) {
        warn!(ticker_id = %id_key, err = %e, "staged-result write failed (fit result lost for this ticker)");
    }
}

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

    // Snapshot the file for the ROSTER + prior-k reads only. Every WRITE goes
    // through `store`, which re-reads first (see ParamsStore).
    let store = ParamsStore::new(params_path.clone());
    let weights_file: WeightsFile = store.read()?;

    // Run identity = UTC date + window. The 4 daily cycles therefore SHARE an id,
    // so a cycle killed by activeDeadlineSeconds is resumed by the next one
    // instead of restarting from zero; a window change starts a fresh staging set
    // because k is not comparable across windows.
    let window_days = args
        .window_days
        .map(|d| d as usize)
        .unwrap_or(series.calibration.rolling_window_days);
    let run_id = format!("{}-w{}", chrono::Utc::now().format("%Y-%m-%d"), window_days);
    let started_at = nxr_sdk::now_sec();

    // Build the work list: (pair, ticker_id, class). De-dupe by ticker_id since
    // the same pair appears under multiple exchanges in pair_volumes.
    //
    // Roster = CEX-volume pairs UNION the config's declared symbols
    // (`PipelineYml::configured_symbols`, shared with core's
    // `register_config_symbols`), filtered to ids that actually have `.idx` on
    // disk. `pair_volumes` alone was the wrong key: the fit reads ONLY `.idx`
    // ticks (`calibrate_one`) and never volume, so Pyth-only stables/metals/FX
    // were excluded forever despite having full tick history — 17 of the 23 DEX
    // pool assets, silently uncalibrated (2026-07-25). Union rather than replace,
    // so nothing currently calibrated is dropped.
    //
    // MINUS the synth/cross universe, which the synth pass below owns: resolve it
    // FIRST so the base roster can exclude it. `configured_symbols()` includes
    // `cexs.cross_pairs`, and a cross's only correct basis is the event-merged pair
    // of USDT leg `.idx` streams (a cross `.idx`, where one exists at all, is a
    // stale live-inference artifact — crosses are RAM-only on the wire). Without
    // this exclusion any cross with an on-disk `.idx` was fitted TWICE per run on
    // two different bases: once in the base loop, then overwritten by the synth
    // pass. Each id is now fitted exactly once, on its own basis.
    let synth_work = {
        let pairs = nxr_sdk::synth::pipeline_pairs::synth_pipeline_pairs(&root);
        resolve_synth_work(&pairs)
    };
    let synth_ids: std::collections::HashSet<u64> = synth_work.iter().map(|(id, ..)| *id).collect();
    let synth_count = synth_work.len();
    let mut seen: HashMap<u64, (String, AssetClassBucket)> = HashMap::new();
    let volume_pairs: Vec<String> = weights_file
        .pair_volumes
        .values()
        .flat_map(|pairs| pairs.keys().cloned())
        .collect();
    let mut no_shards = 0usize;
    let mut deferred_to_synth = 0usize;
    for pair in volume_pairs
        .iter()
        .cloned()
        .chain(root.configured_symbols())
    {
        let ticker_id = resolve_ticker_id(&pair);
        if seen.contains_key(&ticker_id) {
            continue;
        }
        // Owned by the synth pass (correct basis = the two USDT leg streams).
        if synth_ids.contains(&ticker_id) {
            deferred_to_synth += 1;
            continue;
        }
        // A declared symbol with no `.idx` yet (gated Lazer feed, brand-new
        // listing) is not an error — it simply has nothing to fit. Skip it here
        // instead of spending a worker to reach the same conclusion.
        if !idx_dir.join(ticker_id.to_string()).is_dir() {
            no_shards += 1;
            continue;
        }
        let class = bucket_for_pair(&pair, ticker_id, &crypto_majors, &stablecoins, &fx_majors);
        seen.insert(ticker_id, (pair.clone(), class));
    }
    info!(
        from_volumes = volume_pairs.len(),
        declared = root.configured_symbols().len(),
        skipped_no_idx = no_shards,
        deferred_to_synth,
        n_synth = synth_work.len(),
        "roster sources merged (volume pairs ∪ config-declared, minus synth crosses, filtered to on-disk .idx)"
    );
    let roster: Vec<(u64, String, AssetClassBucket)> =
        seen.into_iter().map(|(id, (p, c))| (id, p, c)).collect();
    let roster_ids: std::collections::HashSet<String> =
        roster.iter().map(|(id, ..)| id.to_string()).collect();

    // Arm the run stamp + staging set BEFORE any work, and drop a stale staging
    // set (different run_id ⇒ different day or window). `finished_at: None` here
    // is what makes a killed run detectable: a stamp older than one cycle with no
    // finish time is a run that died, which used to be invisible because
    // ttlSecondsAfterFinished reaps the Job object.
    let resume: BTreeMap<String, f64> = {
        let mut resumed = BTreeMap::new();
        store.update(|w| {
            if w.staged_run_id.as_deref() == Some(run_id.as_str()) {
                resumed = w.renko_k_staged.clone();
            } else {
                w.renko_k_staged.clear();
                w.staged_run_id = Some(run_id.clone());
            }
            let resumed_roster = resumed.keys().filter(|k| roster_ids.contains(*k)).count();
            // Counters START at what earlier cycles of this run_id already staged
            // for ROSTER ids — otherwise `coverage_ratio` (= succeeded/total)
            // reads near-zero on a resumed cycle that in fact finished the roster.
            // Roster-only, matching `tickers_total`: staged synth ids are not in it.
            w.last_run = Some(nxr_sdk::weights_schema::CalibrateRun {
                run_id: run_id.clone(),
                started_at,
                finished_at: None,
                tickers_total: roster.len(),
                tickers_attempted: resumed_roster,
                tickers_succeeded: resumed_roster,
                exit_reason: "partial".to_string(),
                window_days: Some(window_days as u32),
            });
        })?;
        resumed
    };

    // Resume: skip tickers this run_id already staged. Partial work from a killed
    // cycle is therefore never repeated, which is what lets 184 tickers finish
    // across cycles instead of restarting into the same deadline every time.
    let work: Vec<(u64, String, AssetClassBucket)> = roster
        .iter()
        .filter(|(id, _, _)| !resume.contains_key(&id.to_string()))
        .cloned()
        .collect();
    info!(
        n_tickers = work.len(),
        roster = roster.len(),
        resumed = resume.len(),
        run_id = %run_id,
        window_days,
        "ticker universe assembled"
    );

    // Configure rayon worker count.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.parallel.max(1))
        .build()
        .with_context(|| "build rayon pool")?;

    // `--window-days` applies here, so BOTH passes (base + synth) and every
    // solver call see the same window the run records in `last_run.window_days`.
    let cal_owned = {
        let mut c = series.calibration.clone();
        c.rolling_window_days = window_days;
        c
    };
    let cal_ext = &cal_owned;
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
            // detected from the already-computed bucket) → flat default.
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
                    let forced = CalOutcome::Ok {
                        ticker_id: *ticker_id,
                        k: forced_k,
                    };
                    stage_outcome(&store, &forced, true);
                    results.lock().unwrap().push(forced);
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

            stage_outcome(&store, &outcome, true);

            results.lock().unwrap().push(outcome);
        });
    });

    // Tally.
    let outcomes = results.into_inner().unwrap();
    let (mut passed, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    // Seed renko_k from `resume` — THIS run_id's staged Ok outcomes from an earlier
    // cycle that `activeDeadlineSeconds` killed — and nothing else. Never from the
    // prior weights file: Failed/Skipped tickers are NOT carried over. Policy:
    // skip the day when calibration fails; never bootstrap a k. Carrying forward
    // stale k corrupts the live renko engine for tickers whose σ regime has shifted
    // since last successful calibration (renko_k cohort 2026-06-01 found 91 % of
    // base tickers using prior-run k due to today's pass=17/188).
    //
    // Seeding is what makes the staged/promote feature work at all: `work` excludes
    // resumed tickers, so without it every id an earlier cycle staged is absent
    // from `renko_k` and the final base+synth write drops it — the promoted set
    // silently shrinks back to one cycle's worth. It also makes the two downstream
    // readers of `renko_k` correct for resumed ids: the inferred-USDC fallback no
    // longer re-fits an id that already has a k, and a resumed USDT leg no longer
    // reads as an "unhealthy leg". Same-run outcomes only ⇒ the no-k-fallback
    // policy is intact: a killed run still promotes nothing new and no default k
    // (0.075 or otherwise) can enter here.
    let prior_count = weights_file.renko_k_per_ticker.len();
    let mut renko_k: BTreeMap<String, f64> = resume.clone();

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
            let k_median = nxr_sdk::stats::median(&ks);
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

    // PROMOTE: the base pass covered every roster ticker (this run's outcomes +
    // whatever an earlier killed cycle staged under the same run_id), so the
    // staged set is now a COMPLETE pass and may replace the live map wholesale.
    //
    // Whole-set replace, never per-ticker merge: the live map's contract is "only
    // this pass's Ok outcomes". Merging incrementally would leave unreached or
    // failed tickers on prior-run k — the stale-k corruption the `no k fallback`
    // policy forbids (renko_k cohort 2026-06-01: 91% of base tickers were running
    // on prior-run k). A killed run therefore promotes NOTHING and the live map
    // keeps the last complete pass, which is the safe direction.
    // `renko_k` is already `resume ∪ this run's Ok` (see the seed above), so it IS
    // the promotable set.
    let base_at = nxr_sdk::now_sec();
    let promoted = renko_k.clone();
    let promoted_count = promoted.len();
    store.update(|w| {
        w.renko_k_per_ticker = promoted;
        w.calibrated_at = Some(base_at);
        // Staging set is NOT cleared here: the synth + inferred-fallback phases
        // still stage into it, so a kill after this point resumes those instead of
        // redoing them. Cleared only by the final write (whole run complete) —
        // which is also the only place `exit_reason`/`finished_at` are stamped, so
        // `nxr_calibrate_run_completed` cannot read 1 while the synth phase is
        // still outstanding.
    })?;
    info!(
        path = %params_path.display(),
        promoted = promoted_count,
        resumed = resume.len(),
        "ticker-params.json updated (base pass promoted; weights fields preserved)"
    );

    // ── Synth-pair pass (crosses; roster excludes these ids) ─────────────────
    // Runs unconditionally after the base pass, over the `synth_work` resolved
    // before the roster. Synths route through the SAME `scale_to_target_k` direct
    // solver the base pass uses (warm start + bounded bracket fallback + ±1-rung
    // probe; see `calibrate_one_synth`); the K_FLOOR / min_pct-clamp /
    // unreachable-target guards inside it drop degenerate windows. If the fit
    // fails, k is NOT persisted (caller keeps prior). Per-pair override or flat
    // default per synth.
    info!(n_synth = synth_count, "synth calibration pass starting");
    let (mut s_passed, mut s_skipped, mut s_failed) = (0usize, 0usize, 0usize);
    for (synth_id, synth_sym, leg_a_id, leg_b_id) in synth_work {
        // Already staged by an earlier cycle of this run_id (seeded into renko_k);
        // synth ids are disjoint from the base roster, so a hit here can only be a
        // resume. Never re-fit it.
        if renko_k.contains_key(&synth_id.to_string()) {
            continue;
        }
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
                stage_outcome(
                    &store,
                    &CalOutcome::Ok {
                        ticker_id: synth_id,
                        k: forced_k,
                    },
                    false,
                );
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
        // count_in_run = false: `tickers_total` is the base roster (see stage_outcome).
        stage_outcome(&store, &outcome, false);
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
            // count_in_run = false: already counted by the base loop.
            stage_outcome(&store, &outcome, false);
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
            let k_median = nxr_sdk::stats::median(&ks);
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

    // Final write (base + synth k). Through the store, so `nxr-weights` runs that
    // landed while this pass was working are preserved instead of being reverted
    // to the snapshot this process read at startup.
    let final_at = nxr_sdk::now_sec();
    let k_count = renko_k.len();
    store.update(|w| {
        w.renko_k_per_ticker = renko_k;
        w.calibrated_at = Some(final_at);
        // Whole run done ⇒ retire the staging set; the next run_id starts fresh.
        w.renko_k_staged.clear();
        w.staged_run_id = None;
        if let Some(r) = w.last_run.as_mut() {
            r.exit_reason = "completed".to_string();
            r.finished_at = Some(final_at);
        }
    })?;
    info!(path = %params_path.display(), k_count, "ticker-params.json updated (base+synth)");

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
