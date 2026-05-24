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
//! Inputs:  `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>/<YYYY-MM-DD>.idx`
//! Output:  `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>/<YYYY-MM-DD>.renko`
//!         + merged into `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>/manifest.json`.

use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::shard::{ShardStream, MS_PER_30MIN, MS_PER_DAY};
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::{BarAccumulator, resolve_ticker_id};
use serde::Deserialize;
use series_factory::sharding::{
    bars_dir_pair, composite_dir, list_shards, manifest_path, read_manifest, shard_path,
    ts_ms_to_utc_date, write_manifest, write_shard_atomic, Manifest,
};
use series_factory::{
    bar_construction::{build_vol_from_hlc, MtfParkinsonCalculator, RenkoConfig, RenkoGenerator, VolConfig},
    vol_bin::{VolMmap, VolWriter},
};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

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

#[derive(Deserialize)]
struct NxratesYml {
    series: SeriesYml,
}
#[derive(Deserialize)]
struct SeriesYml {
    renko: RenkoYml,
    vol: VolConfig,
    pipeline: PipelineYml,
}
#[derive(Deserialize)]
struct RenkoYml {
    min_pct: f32,
    max_pct: f32,
}
#[derive(Deserialize)]
struct PipelineYml {
    bootstrap_days: i64,
    max_bars: usize,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let root: NxratesYml = serde_yaml::from_str(&fs::read_to_string(&args.config)?)?;
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
    let in_dir = args
        .input_dir
        .clone()
        .unwrap_or_else(|| composite_dir(&data_root_idx, &base, &quote));
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| bars_dir_pair(&data_root_bars, &base, &quote));

    info!(in_dir = %in_dir.display(), out_dir = %out_dir.display(), "renko-from-idx starting (sharded)");

    let shards = list_shards(&in_dir, "idx")?;
    if shards.is_empty() {
        anyhow::bail!("no input shards in {}", in_dir.display());
    }
    info!(input_shards = shards.len(), "input shard scan done");

    // ═══ PASS 1: 30-min HLC from composite mid ═══
    let t0 = std::time::Instant::now();
    let mut hlc: HashMap<i64, (f64, f64, f64)> = HashMap::new();
    let mut pass1_count: u64 = 0;
    for (_, path) in &shards {
        let mut stream = ShardStream::<nxr_sdk::IndexRecord>::open(path)?;
        while let Some(rec) = stream.next()? {
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let mid = (rec.index.bid + rec.index.ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                continue;
            }
            let key = (ts / MS_PER_30MIN) * MS_PER_30MIN;
            let e = hlc.entry(key).or_insert((mid, mid, mid));
            if mid > e.0 {
                e.0 = mid;
            }
            if mid < e.1 {
                e.1 = mid;
            }
            e.2 = mid;
            pass1_count += 1;
        }
    }
    info!(
        pass1_records = pass1_count,
        buckets = hlc.len(),
        "pass 1: 30-min HLC built in {}ms",
        t0.elapsed().as_millis()
    );

    // ═══ Build vol (.vol) file ═══
    std::fs::create_dir_all(&out_dir)?;
    // vol file lives alongside the shard dir; transient (deleted post-write).
    let vol_path = out_dir.join("_renko.vol");
    // Drop the close component → canonical (ts, H, L) BTreeMap for the shared
    // EMA-Parkinson builder. BTreeMap sorts by ts so output is monotone.
    let hlc_sorted: BTreeMap<i64, (f64, f64)> =
        hlc.iter().map(|(&ts, &(h, l, _))| (ts, (h, l))).collect();
    let first_ts = hlc_sorted.keys().next().copied().unwrap_or(0);
    let n_vol = hlc_sorted.len();
    let mut vol_writer = VolWriter::new(&vol_path)?;
    build_vol_from_hlc(&hlc_sorted, &yml.vol, &mut vol_writer)?;
    vol_writer.finish()?;
    let vol_mmap = VolMmap::open(&vol_path)?;
    info!(vol_records = n_vol, "vol file written");

    // ═══ PASS 2: feed composite mid → RenkoGenerator ═══
    let bootstrap_end = first_ts + yml.pipeline.bootstrap_days * MS_PER_DAY;

    // Phase 55 W3.C: read the calibrated k from ticker-params.json. The prior
    // hardcoded `multiplier: 0.075` was the root cause of the 22× bars/day
    // overshoot on majors (BTC 8645, ETH 6786, BNB 6114): nxr-calibrate writes
    // per-ticker k values into `ticker-params.json`, but this offline emitter
    // ignored them and ran every pair at the bootstrap k=0.075. Fall back to
    // 0.075 only when the calibration table has no entry (new tickers /
    // first-ever build before the calibrator runs).
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let calibrated_k = load_calibrated_k(ticker_id);
    let multiplier = calibrated_k.unwrap_or(0.075) as f32;
    info!(
        ticker_id,
        k = multiplier,
        source = if calibrated_k.is_some() { "ticker-params.json" } else { "default" },
        "renko k resolved"
    );

    let renko_config = RenkoConfig {
        multiplier,
        min_pct: yml.renko.min_pct,
        max_pct: yml.renko.max_pct,
    };
    renko_config.validate()?;

    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, yml.vol.clone());
        calc.precompute_sigma_cache()
    };

    let t1 = std::time::Instant::now();
    let mut generator = RenkoGenerator::new(renko_config, &vol_mmap, yml.vol.clone())?;
    generator.set_sigma_cache(&sigma_cache);

    let mut bars_by_date: BTreeMap<NaiveDate, Vec<Bar>> = BTreeMap::new();
    let mut accum = BarAccumulator::new();
    let mut pending: Vec<Bar> = Vec::new();
    let mut pass2_count: u64 = 0;
    let mut post_bootstrap: u64 = 0;
    let mut total_bars: usize = 0;

    for (_, path) in &shards {
        let mut stream = ShardStream::<nxr_sdk::IndexRecord>::open(path)?;
        while let Some(rec) = stream.next()? {
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let idx = rec.index;
            let mid = (idx.bid + idx.ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                continue;
            }
            pass2_count += 1;

            if ts < bootstrap_end {
                generator.feed_tick(ts, mid, &mut |_: &Bar| Ok(()))?;
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
            generator.feed_tick(ts, mid, &mut |bar: &Bar| {
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
                        bar.tick_count = if n_pending > 0 { e.tick_count / n_pending } else { 0 };
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
    // remove transient vol scratch file
    let _ = fs::remove_file(&vol_path);

    // ═══ MANIFEST ═══
    let ticker_str = format!("{}-{}", base, quote);
    let mpath = manifest_path(&out_dir);
    let mut manifest = read_manifest(&mpath)?
        .unwrap_or_else(|| Manifest::new(ticker_str.clone(), ticker_id, "renko"));
    manifest.ticker = ticker_str;
    manifest.ticker_id = ticker_id;
    manifest.refresh_kind::<Bar>(&out_dir, "renko")?;
    write_manifest(&mpath, &manifest)?;

    info!(out_dir = %out_dir.display(), manifest = %mpath.display(), "manifest updated");
    Ok(())
}

/// Look up the calibrated Renko multiplier for `ticker_id` in
/// `$NXR_TICKER_PARAMS_PATH` (default `/data/config/ticker-params.json`).
/// Returns `None` and logs a single warning if the file is missing, malformed,
/// or has no entry for this ticker — callers fall back to the bootstrap k.
fn load_calibrated_k(ticker_id: u64) -> Option<f64> {
    let cfg = nxr_sdk::NxrConfig::from_env();
    let path = PathBuf::from(&cfg.ticker_params_path);
    let raw = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(path = %path.display(), err = %e, "ticker-params.json read failed; using bootstrap k");
            return None;
        }
    };
    let weights: WeightsFile = match serde_json::from_str(&raw) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(path = %path.display(), err = %e, "ticker-params.json parse failed; using bootstrap k");
            return None;
        }
    };
    weights
        .renko_k_per_ticker
        .get(&ticker_id.to_string())
        .copied()
        .filter(|k| *k > 0.0 && k.is_finite())
}
