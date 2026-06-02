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
use mitch::timestamp;
use mitch::common::InstrumentType;
use nxr_sdk::asset_class::{
    bucket_for_pair, effective_list, AssetClassBucket,
    DEFAULT_CRYPTO_MAJORS, DEFAULT_FX_MAJORS, DEFAULT_STABLECOINS,
};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{list_shards, ShardStream, MS_PER_MIN};
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::{resolve_ticker, resolve_ticker_id};
use rayon::prelude::*;
use series_factory::bar_construction::{
    build_vol_from_s10, calibrate_mtf_walkforward, CalibrationConfig, S10ShardIter,
};
use nxr_sdk::vol::{MtfVolCalculator, VolConfig};
use nxr_sdk::renko::RenkoConfig;
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

/// Walk-forward calibration holdout window. Used by both the direct and the
/// synth calibration paths; defined once at module scope to keep the two
/// branches in lock-step (audit point #5(i), 2026-05-26 — `feedback_no_k_fallback`).
const EVAL_HOLDOUT_DAYS: usize = 7;

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

// ── Config (subset of nxrates.yml) ───────────────────────────────────────────

use nxr_sdk::pipeline_config::{CalibrationYml, PipelineYml};

/// Convert the shared calibration block into the inner `CalibrationConfig`
/// the MTF calibrator consumes.
fn calibration_inner(c: &CalibrationYml) -> CalibrationConfig {
    CalibrationConfig {
        target_bpd: c.target_bpd,
        k_fit_windows_days: c.k_fit_windows_days.clone(),
        min_window_days: c.min_window_days,
        max_rounds: c.max_rounds,
        tolerance: c.tolerance,
        mult_bounds: c.mult_bounds,
    }
}

/// Resolve `target_bpd` for a given pair. Phase 60.π: per-pair overrides only,
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
) -> CalOutcome {
    // Phase 55 sharded layout: shards live at `<idx_dir>/<ticker_id>/<YYYY-MM-DD>.idx`.
    // Enumerate via `list_shards`, then stream each shard via `ShardStream` so
    // memory stays bounded (one shard's working buffer, not the full history).
    let ticker_dir = idx_dir.join(ticker_id.to_string());
    let shards = match list_shards(&ticker_dir, "idx") {
        Ok(v) => v,
        Err(e) => return CalOutcome::Skipped {
            ticker_id,
            reason: format!("no shards under {}: {}", ticker_dir.display(), e),
        },
    };
    if shards.is_empty() {
        return CalOutcome::Skipped {
            ticker_id,
            reason: format!("no .idx shards under {}", ticker_dir.display()),
        };
    }

    // Pass 1: stream every .idx shard in date order → 1-min last-mid downsample
    // for the in-memory calibration walk-forward. The vol basis is built
    // separately from the gapless `.s10` shards (RS over s10 OHLC), NOT from
    // idx-HLC — see the s10 vol build below.
    let mut price_buckets: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
    for (_date, shard_path) in &shards {
        let mut stream = match ShardStream::<IndexRecord>::open(shard_path) {
            Ok(s) => s,
            Err(e) => return CalOutcome::Failed {
                ticker_id,
                reason: format!("open shard {}: {}", shard_path.display(), e),
            },
        };
        loop {
            let rec = match stream.next() {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(e) => return CalOutcome::Failed {
                    ticker_id,
                    reason: format!("read shard {}: {}", shard_path.display(), e),
                },
            };
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let bid = rec.index.bid;
            let ask = rec.index.ask;
            let mid = (bid + ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) { continue; }

            // 1-min last-mid bucket for in-memory calibration.
            let bucket = (ts / MS_PER_MIN) * MS_PER_MIN;
            let pe = price_buckets.entry(bucket).or_insert((ts, mid));
            if ts >= pe.0 { *pe = (ts, mid); }
        }
    }

    if price_buckets.is_empty() {
        return CalOutcome::Skipped { ticker_id, reason: "empty .idx".into() };
    }

    // Build the .vol file (tmp, deleted at end of fn) from the gapless `.s10`
    // shards via the canonical RS-over-s10-OHLC builder. offline == live.
    let vol_path = std::env::temp_dir().join(format!("nxr-calibrate-{}-{}.vol", ticker_id, std::process::id()));
    {
        let mut writer = match VolWriter::new(&vol_path) {
            Ok(w) => w,
            Err(e) => return CalOutcome::Failed { ticker_id, reason: format!("vol writer: {}", e) },
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
            return CalOutcome::Failed { ticker_id, reason: format!("vol build: {}", e) };
        }
        if let Err(e) = writer.finish() {
            return CalOutcome::Failed { ticker_id, reason: format!("vol finish: {}", e) };
        }
    }

    let vol_mmap = match VolMmap::open(&vol_path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Failed { ticker_id, reason: format!("vol mmap: {}", e) };
        }
    };

    let sigma_cache = {
        let mut calc = MtfVolCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let tick_prices: Vec<(i64, f64)> = price_buckets
        .into_iter()
        .map(|(_, (ts, mid))| (ts, mid))
        .collect();

    let base = RenkoConfig {
        multiplier: RenkoConfig::default().multiplier,
        min_pct: renko_yml.min_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed { ticker_id, reason: format!("base renko cfg: {}", e) };
    }

    info!(ticker_id, pair, class = class.as_key(), target_bpd, "calibrating (walk-forward)");
    // Walk-forward calibration (7d holdout non-overlapping with the training
    // slice) per audit point #5(i) 2026-05-26. Eliminates the regime-leak
    // overfit that produced k≈0.01 boundary-clamps on cross-pairs (live
    // brick-storm root cause). Holdout const hoisted to module scope.
    let mult = calibrate_mtf_walkforward(
        &tick_prices,
        &calibration_inner(cal_ext),
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
        EVAL_HOLDOUT_DAYS,
    );

    let _ = std::fs::remove_file(&vol_path);

    if !(mult > 0.0 && (mult as f64).is_finite()) {
        return CalOutcome::Failed {
            ticker_id,
            reason: "calibration returned 0 (no window had enough data)".into(),
        };
    }
    CalOutcome::Ok { ticker_id, k: mult as f64 }
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
// **Why NOT persist a synth `.idx`:** the kernel design (see audit doc
// `synth-pipeline-design-2026-05-26.md`) keeps synth on the wire only —
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

impl LegStream {
    fn open(idx_root: &Path, ticker_id: u64) -> Result<Self> {
        let dir = idx_root.join(ticker_id.to_string());
        let shards = list_shards(&dir, "idx")
            .with_context(|| format!("list shards {}", dir.display()))?;
        if shards.is_empty() {
            anyhow::bail!("no .idx shards under {}", dir.display());
        }
        Ok(Self { shards: shards.into_iter(), cur: None })
    }

    /// Next valid `(ts, bid, ask)` triple across all shards, or `Ok(None)` at end.
    /// Skips records with non-finite/non-positive bid/ask (matches prior filter).
    fn next_tick(&mut self) -> Result<Option<(i64, f64, f64)>> {
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
                    let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
                    let bid = rec.index.bid;
                    let ask = rec.index.ask;
                    if !(bid.is_finite() && ask.is_finite()) { continue; }
                    if bid <= 0.0 || ask <= 0.0 { continue; }
                    return Ok(Some((ts, bid, ask)));
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
) -> CalOutcome {
    // ── 1. Open both leg streams (no materialization) ───────────────────────
    // Streams pull one tick at a time from on-disk shards. Memory bounded
    // to two ShardStream buffers (~300 KB total) instead of full history
    // (was 16Gi pod OOM, 2026-05-30 incident).
    let mut leg_a_stream = match LegStream::open(idx_root, leg_a_id) {
        Ok(s) => s,
        Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("leg_a={} {}", leg_a_id, e) },
    };
    let mut leg_b_stream = match LegStream::open(idx_root, leg_b_id) {
        Ok(s) => s,
        Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("leg_b={} {}", leg_b_id, e) },
    };

    // Prime both legs' look-ahead slot. If either leg has zero valid ticks → skip.
    let mut a_next: Option<(i64, f64, f64)> = match leg_a_stream.next_tick() {
        Ok(v) => v,
        Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("read leg_a: {}", e) },
    };
    let mut b_next: Option<(i64, f64, f64)> = match leg_b_stream.next_tick() {
        Ok(v) => v,
        Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("read leg_b: {}", e) },
    };
    if a_next.is_none() || b_next.is_none() {
        return CalOutcome::Skipped { ticker_id: synth_id, reason: "empty leg".into() };
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
    let mut a_last: Option<(f64, f64)> = None;
    let mut b_last: Option<(f64, f64)> = None;
    // SEAM PARITY (R3): the synth `.vol` is built from the SAME persisted `.s10`
    // shards the live `bars_renko_synth` ring consumes (written by the synth s10
    // producer — live `bars_s10::spawn(Synth)` / offline `synth-backfill-from-idx`),
    // NOT an in-memory mid reconstruction. The old reconstruction min/max/last on
    // raw mids ≠ the real s10 producer's `BarAccumulator` microstructure-weighted
    // OHLC + flat-fill timing → train/serve skew on synths. The leg merge below
    // is now ONLY for the 1-min last-mid `price_buckets` (the calibration tick
    // stream), exactly as the native path uses its `.idx` mids.
    let mut price_buckets: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
    loop {
        // Pick side with smaller ts (ties favor a → deterministic).
        let take_a = match (&a_next, &b_next) {
            (Some(a), Some(b)) => a.0 <= b.0,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        let (ts, b_px, a_px) = if take_a {
            let cur = a_next.take().expect("a_next primed");
            a_next = match leg_a_stream.next_tick() {
                Ok(v) => v,
                Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("read leg_a: {}", e) },
            };
            a_last = Some((cur.1, cur.2));
            cur
        } else {
            let cur = b_next.take().expect("b_next primed");
            b_next = match leg_b_stream.next_tick() {
                Ok(v) => v,
                Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("read leg_b: {}", e) },
            };
            b_last = Some((cur.1, cur.2));
            cur
        };
        let _ = (b_px, a_px); // silence unused-binding lint; consumed via *_last

        // Both legs primed → emit synth tick.
        if let (Some((ab, aa)), Some((bb, ba))) = (a_last, b_last) {
            // Worst-case spread compounding (mirrors live kernel).
            let synth_bid = ab / ba;
            let synth_ask = aa / bb;
            if !(synth_bid.is_finite() && synth_ask.is_finite()
                && synth_bid > 0.0 && synth_ask > 0.0) {
                continue;
            }
            let mid = (synth_bid + synth_ask) * 0.5;
            // 1-min last-mid downsample (the calibration tick stream).
            let bucket = (ts / MS_PER_MIN) * MS_PER_MIN;
            let pe = price_buckets.entry(bucket).or_insert((ts, mid));
            if ts >= pe.0 { *pe = (ts, mid); }
        }
    }
    // Drop leg streams now — frees the two ShardStream buffers before the
    // vol-write + sigma-cache + calibrator stage runs.
    drop(leg_a_stream);
    drop(leg_b_stream);

    if price_buckets.is_empty() {
        return CalOutcome::Skipped { ticker_id: synth_id, reason: "empty merged stream".into() };
    }

    // ── 3. Build .vol from the persisted synth `.s10` shards (identical to the
    // native base path) → offline σ == live σ on the SAME real s10 artifact.
    let vol_path = std::env::temp_dir()
        .join(format!("nxr-calibrate-synth-{}-{}.vol", synth_id, std::process::id()));
    {
        let mut writer = match VolWriter::new(&vol_path) {
            Ok(w) => w,
            Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol writer: {}", e) },
        };
        let s10_dir = nxr_sdk::shard::bars_dir(bars_root, synth_id);
        let s10_shards = list_shards(&s10_dir, "s10").unwrap_or_default();
        if s10_shards.is_empty() {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Skipped {
                ticker_id: synth_id,
                reason: format!("no synth .s10 shards under {} (run synth-backfill-from-idx first)", s10_dir.display()),
            };
        }
        let mut s10_iter = S10ShardIter::new(s10_shards);
        if let Err(e) = build_vol_from_s10(|| s10_iter.next_bar(), vol_cfg, &mut writer) {
            return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol build: {}", e) };
        }
        if let Err(e) = writer.finish() {
            return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol finish: {}", e) };
        }
    }
    let vol_mmap = match VolMmap::open(&vol_path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol mmap: {}", e) };
        }
    };
    let sigma_cache = {
        let mut calc = MtfVolCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let tick_prices: Vec<(i64, f64)> = price_buckets
        .into_iter()
        .map(|(_, (ts, mid))| (ts, mid))
        .collect();

    let base = RenkoConfig {
        multiplier: RenkoConfig::default().multiplier,
        min_pct: renko_yml.min_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed { ticker_id: synth_id, reason: format!("base renko cfg: {}", e) };
    }

    info!(synth_id, synth_sym, leg_a_id, leg_b_id, target_bpd, "calibrating synth (walk-forward)");
    // Walk-forward 7d holdout (matches direct path above; const at module scope).
    let mult = calibrate_mtf_walkforward(
        &tick_prices,
        &calibration_inner(cal_ext),
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
        EVAL_HOLDOUT_DAYS,
    );
    let _ = std::fs::remove_file(&vol_path);

    if !(mult > 0.0 && (mult as f64).is_finite()) {
        return CalOutcome::Failed {
            ticker_id: synth_id,
            reason: "synth calibration returned 0 (clamp-dropped windows or insufficient data)".into(),
        };
    }
    CalOutcome::Ok { ticker_id: synth_id, k: mult as f64 }
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
        let synth_id = match resolve(spec.synth_sym) { Some(v) => v, None => continue };
        let leg_a_id = match resolve(spec.base_sym) { Some(v) => v, None => continue };
        let leg_b_id = match resolve(spec.quote_sym) { Some(v) => v, None => continue };
        out.push((synth_id, spec.synth_sym, leg_a_id, leg_b_id));
    }
    out
}

// ── Main run ─────────────────────────────────────────────────────────────────

fn run_once(args: &Args) -> Result<()> {
    let root: PipelineYml = PipelineYml::load_default(
        nxr_sdk::pipeline_config::ConfigHint::Bin,
    )?;
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
        serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", params_path.display()))?
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
            seen.entry(ticker_id).or_insert_with(|| (pair.clone(), class));
        }
    }
    let work: Vec<(u64, String, AssetClassBucket)> = seen
        .into_iter()
        .map(|(id, (p, c))| (id, p, c))
        .collect();
    info!(n_tickers = work.len(), "ticker universe assembled");

    // Configure rayon worker count.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.parallel.max(1))
        .build()
        .with_context(|| "build rayon pool")?;

    let cal_ext = &series.calibration;
    let vol_cfg = &series.vol;
    let renko_yml = &series.renko;

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

            // Panic-safe: one bad ticker (malformed .idx, OOM in sigma cache,
            // ...) must not abort the whole cron. AssertUnwindSafe is sound
            // here because nothing inside is moved across the boundary.
            let pair_clone = pair.clone();
            let class_clone = *class;
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
                CalOutcome::Failed { ticker_id: *ticker_id, reason: format!("panic: {}", msg) }
            });

            results.lock().unwrap().push(outcome);
        });
    });

    // Tally.
    let outcomes = results.into_inner().unwrap();
    let (mut passed, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    // EMERGENCY 2026-06-01 fix (per docs/EMERGENCY-2026-06-01.md P0.4 + memory
    // feedback_no_k_fallback): start renko_k EMPTY. Only Ok outcomes populate.
    // Failed/Skipped tickers are NOT carried over from the prior weights file.
    // Operator policy is "skip day if calibrate fails; never bootstrap". Carrying
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
        "calibration summary (base; stale entries dropped per feedback_no_k_fallback)"
    );

    // SLA-CRITICAL (phase60.η): write ticker-params.json AFTER base pass so
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
    // cache). The clamp-detector inside `calibrate_mtf_with_target` drops
    // degenerate windows; if all windows fail, k is NOT persisted (caller
    // keeps prior). Phase 60.π: per-pair override or flat default per synth.
    let synth_work = resolve_synth_work(&root.synths.initial_pairs);
    info!(n_synth = synth_work.len(), "synth calibration pass starting");
    let (mut s_passed, mut s_skipped, mut s_failed) = (0usize, 0usize, 0usize);
    for (synth_id, synth_sym, leg_a_id, leg_b_id) in synth_work {
        // Class-detect the synth pair too (stable/stable crosses like USD1/USDC
        // → crypto_stable → 50) instead of relying on a manual override entry.
        let synth_class =
            bucket_for_pair(synth_sym, synth_id, &crypto_majors, &stablecoins, &fx_majors);
        let synth_target = target_for_pair(cal_ext, synth_sym, synth_class);
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            calibrate_one_synth(
                synth_id, synth_sym, leg_a_id, leg_b_id,
                &idx_dir, &bars_root, cal_ext, synth_target, vol_cfg, renko_yml,
            )
        }))
        .unwrap_or_else(|p| {
            let msg = if let Some(s) = p.downcast_ref::<&str>() { s.to_string() }
                      else if let Some(s) = p.downcast_ref::<String>() { s.clone() }
                      else { "unknown panic".to_string() };
            CalOutcome::Failed { ticker_id: synth_id, reason: format!("panic: {}", msg) }
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
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {:?}", parent))?;
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
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {:?} -> {:?}", tmp, path))?;
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
        if args.once { break; }
        info!("sleeping 24h until next calibration");
        std::thread::sleep(std::time::Duration::from_secs(nxr_sdk::shard::SECS_PER_DAY));
    }
    Ok(())
}
