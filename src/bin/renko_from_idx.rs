//! Generate renko bars from a sharded cross-provider composite `.idx`
//! directory, emitting daily-sharded `.renko` files.
//!
//! Two-pass, streaming:
//!   Pass 1 — sweep ∀ input shards (chronological) to build 30 min Parkinson
//!           HLC + an EMA-smoothed sigma `.vol` file.
//!   Pass 2 — sweep input shards again, feed each record's mid price to
//!           `RenkoGenerator`. The generator perpetually re-calibrates its
//!           brick size every 30 min from the vol file.
//!
//! Inputs:  `$NXR_DATA_INDEXES/<MITCH_TICKER_ID>/<YYYY-MM-DD>.idx`
//! Output:  `$NXR_DATA_BARS/<MITCH_TICKER_ID>/<YYYY-MM-DD>.renko`
//!         + merged into `$NXR_DATA_BARS/<MITCH_TICKER_ID>/manifest.json`.

use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::renko::{RenkoConfig, RenkoGenerator, SIGMA_FALLBACK};
use nxr_sdk::shard::{ShardStream, MS_PER_30MIN, MS_PER_DAY};
use nxr_sdk::vol::{MtfVolCalculator, VolSource};
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::{resolve_ticker_id, BarAccumulator};
use series_factory::sharding::{
    bars_dir, idx_dir, list_shards, manifest_path, read_manifest, shard_path, ts_ms_to_utc_date,
    write_manifest, write_shard_atomic, Manifest,
};
use series_factory::{
    bar_construction::{build_vol_from_s10, S10ShardIter},
    vol_bin::{VolMmap, VolWriter},
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(about = "Build renko shards from a sharded composite idx dir.")]
struct Args {
    /// Path to nxrates.yml (reads `series.{renko,vol,calibration,pipeline}`).
    config: PathBuf,
    /// Base asset symbol (e.g. BTC).
    base: String,
    /// Quote asset symbol (e.g. USDT).
    quote: String,
    /// Override the input composite shard dir.
    #[arg(long = "in-dir")]
    input_dir: Option<PathBuf>,
    /// Override the output shard dir.
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,
}

use nxr_sdk::pipeline_config::PipelineYml;

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let root: PipelineYml = PipelineYml::load(&args.config)?;
    let yml = root.series;

    let cfg = nxr_sdk::NxrConfig::from_env();
    let base = args.base.to_uppercase();
    let quote = args.quote.to_uppercase();
    let data_root_idx = Path::new(&cfg.indexes_dir)
        .parent()
        .unwrap_or(Path::new("/data"))
        .to_path_buf();
    let data_root_bars = Path::new(&cfg.bars_dir)
        .parent()
        .unwrap_or(Path::new("/data"))
        .to_path_buf();
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let in_dir = args
        .input_dir
        .clone()
        .unwrap_or_else(|| idx_dir(&data_root_idx, ticker_id));
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| bars_dir(&data_root_bars, ticker_id));

    info!(in_dir = %in_dir.display(), out_dir = %out_dir.display(), "renko-from-idx starting (sharded)");

    let shards = list_shards(&in_dir, "idx")?;
    if shards.is_empty() {
        anyhow::bail!("no input shards in {}", in_dir.display());
    }
    info!(input_shards = shards.len(), "input shard scan done");

    // ═══ PASS 1: scan composite idx for the first valid mts → bootstrap anchor ═══
    let t0 = std::time::Instant::now();
    let mut first_ts: i64 = 0;
    let mut pass1_count: u64 = 0;
    'outer: for (_, path) in &shards {
        let mut stream = ShardStream::<nxr_sdk::IndexRecord>::open(path)?;
        while let Some(chunk) = stream.next_chunk()? {
            for rec in chunk {
                // SEAM PARITY: skip heartbeat-sentinel records exactly like the live
                // producers (core/src/bars_renko.rs:528, bars_s10.rs:198). Sentinels
                // carry stale bid/ask liveness beacons; ingesting them offline (but
                // not live) poisons the σ basis → hist↔live seam drift.
                if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 {
                    continue;
                }
                let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
                let mid = (rec.index.bid + rec.index.ask) * 0.5;
                if !(mid.is_finite() && mid > 0.0) {
                    continue;
                }
                pass1_count += 1;
                first_ts = (ts / MS_PER_30MIN) * MS_PER_30MIN;
                break 'outer;
            }
        }
    }
    info!(
        pass1_records = pass1_count,
        "pass 1: bootstrap anchor found in {}ms",
        t0.elapsed().as_millis()
    );

    // ═══ Build vol (.vol) file (RS over the PERSISTED s10 shards) ═══
    // VOL-BASIS PARITY: σ MUST be built from the real `.s10` shards the live
    // producer persisted (nxr_calibrate.rs:199-211), NOT reconstructed from idx
    // mids. Reconstructing s10 from idx mids diverges from the live/calibrator σ
    // basis → backfilled .renko bricks miss target bpd (BTC regen at correct k
    // still emitted 992 bpd). Fail clearly when `.s10` is absent rather than
    // silently falling back, matching the calibrator's "no .s10 shards" skip.
    std::fs::create_dir_all(&out_dir)?;
    // vol file lives alongside the shard dir; transient (deleted post-write).
    let vol_path = out_dir.join("_renko.vol");
    let s10_dir = bars_dir(&data_root_bars, ticker_id);
    let s10_shards = list_shards(&s10_dir, "s10").unwrap_or_default();
    if s10_shards.is_empty() {
        anyhow::bail!(
            "no .s10 shards under {} — run s10-from-idx first (vol basis MUST match live/calibrator)",
            s10_dir.display()
        );
    }
    let mut vol_writer = VolWriter::new(&vol_path)?;
    let mut s10_iter = S10ShardIter::new(s10_shards);
    let n_vol = build_vol_from_s10(|| s10_iter.next_bar(), &yml.vol, &mut vol_writer)?;
    vol_writer.finish()?;
    let vol_mmap = VolMmap::open(&vol_path)?;
    info!(
        vol_records = n_vol,
        "vol file written (from persisted .s10)"
    );

    // ═══ PASS 2: feed composite mid → RenkoGenerator ═══
    let bootstrap_end = first_ts + yml.pipeline.bootstrap_days * MS_PER_DAY;

    // Read the calibrated k from ticker-params.json. A prior hardcoded
    // `multiplier: 0.075` was the root cause of a 22× bars/day overshoot on
    // majors (BTC 8645, ETH 6786, BNB 6114): nxr-calibrate writes per-ticker
    // k values into `ticker-params.json`, and this offline emitter must
    // honour them.
    //
    // Per durable rule `feedback_no_k_fallback` we ABORT when no calibrated k
    // exists rather than bootstrapping a degenerate 0.075. Skipping is the
    // right call: a missing entry means nxr-calibrate has not yet processed
    // this ticker (new ticker, fresh deploy, or calibrator failed for the
    // day). Running with 0.075 produces brick-storm overshoots we already
    // paid for once.
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let (calibrated_k, calibrated_at) = load_calibrated_k(ticker_id);
    let multiplier = match calibrated_k {
        Some(k) => k as f32,
        None => {
            tracing::warn!(
                ticker_id,
                pair = %format!("{}/{}", base, quote),
                "no calibrated k in ticker-params.json — skipping renko build (run nxr-calibrate first)"
            );
            return Ok(());
        }
    };
    info!(
        ticker_id,
        k = multiplier,
        source = "ticker-params.json",
        "renko k resolved"
    );

    let renko_config = RenkoConfig {
        multiplier,
        min_pct: yml.renko.min_pct,
    };
    renko_config.validate()?;

    let sigma_cache = {
        let mut calc = MtfVolCalculator::new(&vol_mmap, yml.vol.clone());
        calc.precompute_sigma_cache()
    };
    let sigma_at = |ts: i64| -> f64 {
        let mts = timestamp::from_epoch_ms(ts);
        let i = vol_mmap.find_index_for_mts(mts);
        sigma_cache.get(i).copied().unwrap_or(SIGMA_FALLBACK)
    };

    let t1 = std::time::Instant::now();
    let mut generator = RenkoGenerator::new(renko_config)?;

    let mut bars_by_date: BTreeMap<NaiveDate, Vec<Bar>> = BTreeMap::new();
    let mut accum = BarAccumulator::new();
    let mut pending: Vec<Bar> = Vec::new();
    let mut pass2_count: u64 = 0;
    let mut post_bootstrap: u64 = 0;
    let mut total_bars: usize = 0;

    for (_, path) in &shards {
        let mut stream = ShardStream::<nxr_sdk::IndexRecord>::open(path)?;
        while let Some(chunk) = stream.next_chunk()? {
            for rec in chunk {
                // SEAM PARITY: skip heartbeat sentinels (mirror live bars_renko.rs:528).
                if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 {
                    continue;
                }
                let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
                let idx = rec.index;
                let mid = (idx.bid + idx.ask) * 0.5;
                if !(mid.is_finite() && mid > 0.0) {
                    continue;
                }
                pass2_count += 1;

                if ts < bootstrap_end {
                    let sigma = sigma_at(ts);
                    generator.feed_tick_with_sigma(ts, mid, sigma, &mut |_: &Bar| Ok(()))?;
                    continue;
                }
                post_bootstrap += 1;

                let ci_ubp = nxr_sdk::tdwap::decode_ci_ubp(idx.ci);
                accum.ingest(
                    idx.bid,
                    idx.ask,
                    idx.vbid,
                    idx.vask,
                    ts,
                    ci_ubp,
                    idx.accepted as u32,
                    idx.rejected as u32,
                );
                let sigma = sigma_at(ts);
                generator.feed_tick_with_sigma(ts, mid, sigma, &mut |bar: &Bar| {
                    pending.push(*bar);
                    Ok(())
                })?;

                if !pending.is_empty() {
                    let enrich = accum.flush();
                    let n_pending = pending.len() as u32;
                    for mut bar in pending.drain(..) {
                        bar.kind = BarKind::Renko as u8;
                        if let Some(ref e) = enrich {
                            bar.vbid = e.vbid;
                            bar.vask = e.vask;
                            bar.tick_count = if n_pending > 0 {
                                e.tick_count / n_pending
                            } else {
                                0
                            };
                            bar.realized_var = e.realized_var;
                            bar.bipower_var = e.bipower_var;
                            bar.drift = e.drift;
                            bar.vol_imbalance = e.vol_imbalance;
                            bar.avg_spread_bps = e.avg_spread_bps;
                            bar.max_abs_return = e.max_abs_return;
                            bar.avg_ci_ubp = e.avg_ci_ubp;
                            bar.reject_rate = e.reject_rate;
                        }
                        let date = ts_ms_to_utc_date(bar.open_time_ms());
                        bars_by_date.entry(date).or_default().push(bar);
                        total_bars += 1;
                    }
                    if total_bars > yml.pipeline.max_bars {
                        anyhow::bail!("bar count exceeds {} safety limit", yml.pipeline.max_bars);
                    }
                }
            }
        }
    }

    info!(
        bars = total_bars,
        pass2_records = pass2_count,
        post_bootstrap,
        out_shards = bars_by_date.len(),
        "pass 2 done in {}ms",
        t1.elapsed().as_millis()
    );

    // ═══ WRITE SHARDS ═══
    for (date, bars) in &bars_by_date {
        let path = shard_path(&out_dir, *date, "renko");
        let bytes: &[u8] = bytemuck::cast_slice(bars);
        write_shard_atomic(&path, bytes)?;
        info!(date = %date, n = bars.len(), path = %path.display(), "wrote renko shard");
    }
    // Persist id-keyed `.vol` so the live renko producer can prime its RS ring
    // on restart (seam-glue; mirrors renko-trailing-from-idx).
    {
        let persist_path = nxr_sdk::shard::vol_path_for_id(&data_root_bars, ticker_id);
        if let Some(parent) = persist_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let tmp = persist_path.with_extension("vol.tmp");
        match fs::copy(&vol_path, &tmp).and_then(|_| fs::rename(&tmp, &persist_path)) {
            Ok(_) => info!(
                vol = %persist_path.display(),
                vol_records = n_vol,
                "persistent id-keyed .vol written (live-prime source)"
            ),
            Err(e) => warn!(
                err = %e,
                vol = %persist_path.display(),
                "persistent .vol write failed (live prime will warm from ticks)"
            ),
        }
    }
    let _ = fs::remove_file(&vol_path);

    // ═══ MANIFEST ═══
    let ticker_str = format!("{}-{}", base, quote);
    let mpath = manifest_path(&out_dir);
    let mut manifest = read_manifest(&mpath)?
        .unwrap_or_else(|| Manifest::new(ticker_str.clone(), ticker_id, "renko"));
    manifest.ticker = ticker_str;
    manifest.ticker_id = ticker_id;
    manifest.refresh_kind::<Bar>(&out_dir, "renko")?;
    // PROVENANCE: stamp the k actually fed to RenkoGenerator (the f32
    // `multiplier`, promoted back to f64 so the recorded value matches the
    // exact bits the generator saw) + the `calibrated_at` epoch-seconds read
    // from ticker-params.json. The cert asserts manifest.renko_k_used ==
    // current ticker-params k within tol, and flags `shards stale vs latest
    // calibration` when ticker-params calibrated_at is newer than this stamp.
    manifest.set_renko_provenance(multiplier as f64, calibrated_at);
    info!(
        ticker_id,
        renko_k_used = multiplier as f64,
        renko_calibrated_at = ?calibrated_at,
        "renko provenance stamped into manifest"
    );
    write_manifest(&mpath, &manifest)?;

    info!(out_dir = %out_dir.display(), manifest = %mpath.display(), "manifest updated");
    Ok(())
}

/// Look up the calibrated Renko multiplier for `ticker_id` in
/// `$NXR_TICKER_PARAMS_PATH` (default `/data/config/ticker-params.json`),
/// together with the file-level `calibrated_at` (unix-seconds of the last
/// `nxr-calibrate` run) for build-time PROVENANCE stamping into the manifest.
///
/// Returns `(None, _)` and logs a single warning if the file is missing,
/// malformed, or has no entry for this ticker — callers ABORT the renko build
/// (no bootstrap-k fallback, per `feedback_no_k_fallback`). The second tuple
/// element is the `calibrated_at` epoch-seconds (`None` for pre-calibration /
/// legacy files); it is recorded even when k resolves so the cert can compare
/// it against the *current* ticker-params `calibrated_at` to detect staleness.
fn load_calibrated_k(ticker_id: u64) -> (Option<f64>, Option<i64>) {
    let cfg = nxr_sdk::NxrConfig::from_env();
    let path = PathBuf::from(&cfg.ticker_params_path);
    let raw = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(path = %path.display(), err = %e, "ticker-params.json read failed; using bootstrap k");
            return (None, None);
        }
    };
    let weights: WeightsFile = match serde_json::from_str(&raw) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(path = %path.display(), err = %e, "ticker-params.json parse failed; using bootstrap k");
            return (None, None);
        }
    };
    let calibrated_at = weights.calibrated_at.map(|s| s as i64);
    let k = weights
        .renko_k_per_ticker
        .get(&ticker_id.to_string())
        .copied()
        .filter(|k| *k > 0.0 && k.is_finite());
    (k, calibrated_at)
}
