//! Calibration-window sweep: walk-forward error vs rolling-window length.
//!
//! Answers ONE question with data instead of judgment (operator ask,
//! 2026-06-09): which single global `rolling_window_days` minimizes the
//! bricks-per-day calibration error across the main tickers — especially for
//! regime shifters (ETH) where a long window may average structurally
//! different epochs and a short one may whipsaw on one vol regime?
//!
//! Method (per ticker, per candidate window `w`):
//!   1. Walk-forward folds: at each anchor `t_i` (non-overlapping, stepping
//!      back `forward_days` from the end of history), fit
//!      `k = scale_to_target_k(prices[t_i − w .. t_i])`, then measure
//!      `median bpd` on the UNSEEN forward slice `prices[t_i .. t_i + fwd]`.
//!      fwd_err = |median/target − 1|. This is the LIVE-quality estimate: it
//!      is exactly what the daily cron + live engine experience.
//!   2. Hist-gate fold: fit on the trailing `w` ending at history end, measure
//!      median over the FULL span (the stored-history single-latest-k gate).
//!
//! Output: CSV on stdout (`kind,ticker,window_days,anchor,k,median,target,err`)
//! plus an aggregate summary per window (mean/max fwd_err across tickers ×
//! anchors). The σ layer is shared verbatim (RS-over-s10, EMA, blend, spike
//! floor) — strictly causal, so per-anchor fits see only past σ.
//!
//! Usage:
//!   window-sweep <config.yml> --tickers BTC/USDT,ETH/USDT,BNB/USDT,SOL/USDT \
//!     --windows 90,180,365,730 --anchors 6 --forward-days 90

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{info, warn};

use mitch::common::InstrumentType;
use nxr_sdk::asset_class::{bucket_for_pair, effective_list, DEFAULT_CRYPTO_MAJORS, DEFAULT_FX_MAJORS, DEFAULT_STABLECOINS};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
use nxr_sdk::renko::RenkoConfig;
use nxr_sdk::resolve_ticker;
use nxr_sdk::shard::{list_shards, ShardStream};
use nxr_sdk::timestamp;
use nxr_sdk::vol::{MtfVolCalculator, VolConfig};
use series_factory::bar_construction::{
    count_bars_per_day_from_prices, scale_to_target_k, CalibrationConfig, S10ShardIter,
    build_vol_from_s10,
};
use series_factory::vol_bin::{VolMmap, VolWriter};
use std::path::{Path, PathBuf};

const MS_PER_DAY: i64 = 86_400_000;

#[derive(Parser, Debug)]
#[command(about = "Walk-forward sweep of calibration rolling-window length")]
struct Args {
    /// Pipeline config yml (targets, vol cfg, renko min_pct, data roots).
    config: PathBuf,
    /// Comma-separated BASE/QUOTE pairs to sweep.
    #[arg(long, default_value = "BTC/USDT,ETH/USDT,BNB/USDT,SOL/USDT")]
    tickers: String,
    /// Comma-separated candidate window lengths (days).
    #[arg(long, default_value = "90,180,365,730")]
    windows: String,
    /// Number of non-overlapping walk-forward anchors (newest first).
    #[arg(long, default_value_t = 6)]
    anchors: usize,
    /// Forward evaluation slice length per anchor (days).
    #[arg(long, default_value_t = 90)]
    forward_days: usize,
}

struct Fold {
    kind: &'static str,
    window_days: usize,
    anchor_date: String,
    k: f64,
    median: f64,
    err: f64,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let root = PipelineYml::load(&args.config)
        .or_else(|_| PipelineYml::load(&PipelineYml::resolve_path(ConfigHint::Bin)))
        .context("load pipeline yml")?;
    let series = &root.series;
    let cal_yml = &series.calibration;
    let vol_cfg: VolConfig = series.vol.clone();
    let renko_min_pct = series.renko.min_pct;

    let crypto_majors = effective_list(&root.cexs.crypto_majors, DEFAULT_CRYPTO_MAJORS);
    let stablecoins = effective_list(&root.cexs.stablecoins, DEFAULT_STABLECOINS);
    let fx_majors = effective_list(&root.cexs.fx_majors, DEFAULT_FX_MAJORS);

    // Data roots: same env-driven resolution as nxr_calibrate (NXR_DATA_*).
    let nxr_cfg = nxr_sdk::NxrConfig::from_env();
    let idx_root = PathBuf::from(&nxr_cfg.indexes_dir);
    let bars_root = Path::new(&nxr_cfg.bars_dir)
        .parent()
        .unwrap_or(Path::new("/data"))
        .to_path_buf();

    let windows: Vec<usize> = args.windows.split(',')
        .filter_map(|s| s.trim().parse().ok()).collect();
    let pairs: Vec<String> = args.tickers.split(',')
        .map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    anyhow::ensure!(!windows.is_empty() && !pairs.is_empty(), "empty sweep axes");

    println!("kind,ticker,window_days,anchor,k,median,target,err_pct");
    let mut all: Vec<(String, Fold)> = Vec::new();

    for pair in &pairs {
        let m = match resolve_ticker(pair, InstrumentType::SPOT) {
            Ok(m) => m,
            Err(e) => { warn!(pair, ?e, "resolve failed; skipping"); continue; }
        };
        let ticker_id = m.ticker.id;
        let class = bucket_for_pair(pair, ticker_id, &crypto_majors, &stablecoins, &fx_majors);
        let target = cal_yml.target_for_pair_classed(pair, class.as_key());

        let prices = match load_full_tick(&idx_root, ticker_id) {
            Ok(p) if !p.is_empty() => p,
            Ok(_) => { warn!(pair, "empty idx; skipping"); continue; }
            Err(e) => { warn!(pair, ?e, "idx load failed; skipping"); continue; }
        };

        // σ basis: full-history RS-over-s10 (causal per bin → safe to share
        // across anchors; each fold only reads σ(ts) for ts inside its slice).
        let vol_path = std::env::temp_dir()
            .join(format!("nxr-wsweep-{}-{}.vol", ticker_id, std::process::id()));
        if let Err(e) = build_vol(&bars_root, ticker_id, &vol_cfg, &vol_path) {
            warn!(pair, ?e, "vol build failed; skipping");
            continue;
        }
        let vol_mmap = VolMmap::open(&vol_path).context("vol mmap")?;
        let sigma_cache = {
            let mut calc = MtfVolCalculator::new(&vol_mmap, vol_cfg.clone());
            calc.precompute_sigma_cache()
        };
        let base = RenkoConfig { multiplier: RenkoConfig::default().multiplier, min_pct: renko_min_pct };

        let last_ts = prices.last().unwrap().0;
        let first_ts = prices.first().unwrap().0;
        let fwd_ms = (args.forward_days as i64) * MS_PER_DAY;

        for &w in &windows {
            let mut cal = CalibrationConfig {
                target_bpd: cal_yml.target_bpd,
                rolling_window_days: w,
                min_window_days: cal_yml.min_window_days,
                bracket_max_iters: cal_yml.bracket_max_iters,
                accept_tol: cal_yml.accept_tol,
                mult_bounds: cal_yml.mult_bounds,
            };
            cal.rolling_window_days = w;
            let w_ms = (w as i64) * MS_PER_DAY;

            // 1. Walk-forward folds (newest first, non-overlapping fwd slices).
            for i in 1..=args.anchors {
                let t = last_ts - (i as i64) * fwd_ms;
                if t - w_ms < first_ts {
                    // window would underflow history — skip (logged, not silent).
                    info!(pair, w, anchor = i, "fold skipped: window underflows history");
                    continue;
                }
                let fit = slice_range(&prices, t - w_ms, t);
                let fwd = slice_range(&prices, t, t + fwd_ms);
                if fit.is_empty() || fwd.is_empty() { continue; }
                let k = scale_to_target_k(fit, &cal, &base, &vol_mmap, &vol_cfg, &sigma_cache, target, None::<f32>);
                let anchor_date = fmt_date(t);
                if !(k > 0.0) {
                    all.push((pair.clone(), Fold { kind: "fwd", window_days: w, anchor_date, k: 0.0, median: f64::NAN, err: f64::NAN }));
                    continue;
                }
                let stats = count_bars_per_day_from_prices(
                    fwd, &RenkoConfig { multiplier: k, min_pct: renko_min_pct },
                    &vol_mmap, &vol_cfg, &sigma_cache, t, t + fwd_ms);
                let err = if stats.days == 0 { f64::NAN } else { (stats.median / target - 1.0).abs() };
                all.push((pair.clone(), Fold { kind: "fwd", window_days: w, anchor_date, k: k as f64, median: stats.median, err }));
            }

            // 2. Hist-gate fold: trailing-w fit applied to the FULL span.
            let fit = slice_range(&prices, last_ts - w_ms, last_ts);
            if !fit.is_empty() {
                let k = scale_to_target_k(fit, &cal, &base, &vol_mmap, &vol_cfg, &sigma_cache, target, None);
                if k > 0.0 {
                    let stats = count_bars_per_day_from_prices(
                        &prices, &RenkoConfig { multiplier: k, min_pct: renko_min_pct },
                        &vol_mmap, &vol_cfg, &sigma_cache, first_ts, last_ts);
                    let err = if stats.days == 0 { f64::NAN } else { (stats.median / target - 1.0).abs() };
                    all.push((pair.clone(), Fold { kind: "hist", window_days: w, anchor_date: fmt_date(last_ts), k: k as f64, median: stats.median, err }));
                }
            }
        }
        let _ = std::fs::remove_file(&vol_path);

        for (p, f) in all.iter().filter(|(p, _)| p == pair) {
            println!("{},{},{},{},{:.6},{:.1},{:.0},{:.2}",
                f.kind, p, f.window_days, f.anchor_date, f.k, f.median, target, f.err * 100.0);
        }
    }

    // Aggregate: mean + worst fwd err per window across tickers × anchors.
    println!("--- aggregate (fwd folds) ---");
    println!("window_days,n_folds,n_failed,mean_err_pct,max_err_pct");
    for &w in &windows {
        let folds: Vec<&Fold> = all.iter()
            .filter(|(_, f)| f.kind == "fwd" && f.window_days == w)
            .map(|(_, f)| f).collect();
        let ok: Vec<f64> = folds.iter().filter(|f| f.err.is_finite()).map(|f| f.err).collect();
        let failed = folds.len() - ok.len();
        if ok.is_empty() {
            println!("{},{},{},NaN,NaN", w, folds.len(), failed);
            continue;
        }
        let mean = ok.iter().sum::<f64>() / ok.len() as f64;
        let max = ok.iter().cloned().fold(0.0_f64, f64::max);
        println!("{},{},{},{:.2},{:.2}", w, folds.len(), failed, mean * 100.0, max * 100.0);
    }
    Ok(())
}

fn slice_range(prices: &[(i64, f64)], from: i64, to: i64) -> &[(i64, f64)] {
    let lo = prices.partition_point(|p| p.0 < from);
    let hi = prices.partition_point(|p| p.0 <= to);
    &prices[lo..hi]
}

fn fmt_date(ts_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_ms)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| ts_ms.to_string())
}

/// Stream every .idx shard → ts-ascending full-tick mid path (heartbeat
/// sentinels + non-finite mids skipped — seam parity with the applier/live).
fn load_full_tick(idx_root: &Path, ticker_id: u64) -> Result<Vec<(i64, f64)>> {
    let dir = idx_root.join(ticker_id.to_string());
    let shards = list_shards(&dir, "idx")?;
    anyhow::ensure!(!shards.is_empty(), "no .idx shards under {}", dir.display());
    let est: usize = shards.iter()
        .filter_map(|(_d, p)| std::fs::metadata(p).ok().map(|m| m.len() as usize / 56))
        .sum();
    let mut out: Vec<(i64, f64)> = Vec::with_capacity(est.min(64_000_000));
    for (_d, path) in &shards {
        let mut s = ShardStream::<IndexRecord>::open(path)?;
        while let Some(rec) = s.next()? {
            if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 { continue; }
            let mid = (rec.index.bid + rec.index.ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) { continue; }
            out.push((timestamp::to_epoch_ms(rec.header.get_timestamp()), mid));
        }
    }
    Ok(out)
}

fn build_vol(bars_root: &Path, ticker_id: u64, vol_cfg: &VolConfig, vol_path: &Path) -> Result<()> {
    let mut writer = VolWriter::new(vol_path)?;
    let s10_dir = nxr_sdk::shard::bars_dir(bars_root, ticker_id);
    let s10_shards = list_shards(&s10_dir, "s10")?;
    anyhow::ensure!(!s10_shards.is_empty(), "no .s10 shards under {}", s10_dir.display());
    let mut iter = S10ShardIter::new(s10_shards);
    build_vol_from_s10(|| iter.next_bar(), vol_cfg, &mut writer)?;
    writer.finish()?;
    Ok(())
}
