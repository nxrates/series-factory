//! Empirical sweep of MTF calibration `k_fit_windows_days` configs.
//!
//! For each `(pair, candidate_config)`, replays a stratified sample of UTC days
//! through `calibrate_mtf_with_target` (using strict-no-leak trailing prices)
//! and `count_bars_from_prices` (counting bricks emitted ∈ [D_start, D_end))
//! to measure realised bpd. No bricks are written to disk.
//!
//! Output is a single newline-delimited JSON stream on stdout, one record per
//! (pair, config, day), plus aggregated SWEEP summary lines on stderr.
//!
//! Goal: find a single `k_fit_windows_days` config that minimises mean |bpd - target|
//! across the launch symbol set, without per-asset overfitting.
//!
//! Lifted heavily from `bin/renko_trailing_from_idx.rs` — same pass-1 loading
//! (full-tick stream + vol/sigma cache), but per-day inner loop iterates the
//! candidate-config list instead of writing renko shards.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use clap::Parser;
use mitch::timestamp;
use nxr_sdk::asset_class::{
    classify_ticker, effective_list,
    DEFAULT_CRYPTO_MAJORS, DEFAULT_FX_MAJORS, DEFAULT_STABLECOINS,
};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::resolve_ticker_id;
use nxr_sdk::shard::{
    bars_dir, idx_dir, list_shards, ShardStream, MS_PER_30MIN, MS_PER_DAY,
};
use serde::Serialize;
use series_factory::bar_construction::{
    build_vol_from_hlc, calibrate_mtf_with_target, count_bars_from_prices, CalibrationConfig,
};
use nxr_sdk::parkinson::MtfParkinsonCalculator;
use nxr_sdk::renko::RenkoConfig;
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

// Launch symbol set (operator brief 2026-05-25 rev-3): universal config,
// cross-ticker mean-error minimization, no per-sym overfitting. 13 syms
// (5 volatile USDT-quoted + 5 crypto crosses + 3 stable/USDT) — see
// `config.yml::pipeline.sweep.pairs`. Audit-frozen fallback in
// `nxr_sdk::synth::pairs::DEFAULT_SWEEP_PAIRS`.

// ── Candidate k_fit_windows_days configs (7, chosen pre-sweep, no per-sym tuning) ─
//
// Operator brief 2026-05-25:
//   - baseline:        [30, 60, 120]  (currently shipped)
//   - speculative:     [14, 30, 90]   (intuition pick — needs validation)
//   - responsive:      [7,  30, 90]
//   - compressed:      [14, 30, 60]
//   - two-window long: [30, 90]
//   - two-window alt:  [14, 60]
//   - heavy short:     [7,  14, 30]
const CANDIDATES: &[(&str, &[usize])] = &[
    ("baseline_30_60_120", &[30, 60, 120]),
    ("spec_14_30_90",      &[14, 30, 90]),
    ("resp_7_30_90",       &[7,  30, 90]),
    ("compr_14_30_60",     &[14, 30, 60]),
    ("two_30_90",          &[30, 90]),
    ("two_14_60",          &[14, 60]),
    ("short_7_14_30",      &[7,  14, 30]),
];

#[derive(Parser, Debug)]
#[command(
    about = "Empirical MTF k_fit_windows_days sweep. Replays calibrate+count over a stratified \
             day sample for each candidate; emits NDJSON on stdout."
)]
struct Args {
    /// Path to nxrates.yml (provides renko/vol/calibration knobs).
    #[arg(long)]
    config: PathBuf,

    /// Stratified day stride. Default 5 → ~1 in 5 days sampled (~30 days/pair
    /// over a ~150d window).
    #[arg(long, default_value_t = 5usize)]
    stride: usize,

    /// Limit days to this number of MOST-RECENT closed days. 0 = use the full
    /// available range (anchored at last shard).
    #[arg(long, default_value_t = 120usize)]
    max_days: usize,

    /// Process only this pair (eg `BTC/USDT`). Comma-separated list also OK.
    /// Default: full `SWEEP_PAIRS` set.
    #[arg(long)]
    pairs: Option<String>,

    /// Restrict to a subset of candidate keys (comma-separated). Default: all.
    #[arg(long)]
    candidates: Option<String>,

    /// Inclusive end date (UTC, `YYYY-MM-DD`). Defaults to yesterday (closed).
    #[arg(long)]
    to: Option<String>,
}

use nxr_sdk::pipeline_config::PipelineYml;

// ── Asset-class bucketing — delegated to SDK ────────────────────────────────
// Was: hardcoded CRYPTO_MAJORS (22) + STABLE_SYMBOLS (11) + class_for_pair
// branching. Now reads `cexs.{crypto_majors,stablecoins,fx_majors}` from
// YAML w/ audit-frozen sdk fallback. Matches `nxr_calibrate.rs` policy so
// the sweep + the cron stay bit-for-bit aligned on bucket assignment.
fn class_for_pair(yml_cexs: &nxr_sdk::pipeline_config::CexsYml, base: &str, quote: &str) -> &'static str {
    use mitch::common::InstrumentType;
    let pair = format!("{}/{}", base.to_uppercase(), quote.to_uppercase());
    let ticker_id = match nxr_sdk::resolve_ticker(&pair, InstrumentType::SPOT) {
        Ok(m) => mitch::ticker::TickerId::from_raw(m.ticker.id),
        Err(_) => return "default",
    };
    let majors = effective_list(&yml_cexs.crypto_majors, DEFAULT_CRYPTO_MAJORS);
    let stables = effective_list(&yml_cexs.stablecoins, DEFAULT_STABLECOINS);
    let fx_m = effective_list(&yml_cexs.fx_majors, DEFAULT_FX_MAJORS);
    classify_ticker(&ticker_id, base, quote, &majors, &stables, &fx_m).as_key()
}

use nxr_sdk::shard::{day_start_ms, parse_utc_date as parse_date};

/// NDJSON record on stdout — one per (pair, candidate, day) sample.
#[derive(Serialize)]
struct SweepRecord<'a> {
    pair: &'a str,
    candidate: &'a str,
    k_fit_windows_days: &'a [usize],
    date: String,
    target_bpd: f64,
    k: f64,
    bpd_actual: f64,
    err_abs: f64,
    err_rel: f64,
    trailing_n: usize,
    day_ticks: usize,
}

#[derive(Default)]
struct CandidateSummary {
    samples: Vec<f64>, // bpd_actual per day
}

impl CandidateSummary {
    fn finalize(&self, target_bpd: f64) -> (f64, f64, usize) {
        if self.samples.is_empty() {
            return (0.0, 0.0, 0);
        }
        let mut s = self.samples.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mean = s.iter().sum::<f64>() / s.len() as f64;
        let median = s[s.len() / 2];
        // OOB window = [target/3, target*2] (mirrors renko-trailing's bracket).
        let lo = (target_bpd / 3.0).max(50.0);
        let hi = target_bpd * 2.0;
        let oob = s.iter().filter(|b| **b < lo || **b > hi).count();
        (mean, median, oob)
    }
}

fn parse_pairs(
    arg: Option<&str>,
    yml_pairs: &[nxr_sdk::pipeline_config::PairSpec],
) -> Vec<(String, String)> {
    match arg {
        None => {
            if yml_pairs.is_empty() {
                warn!(
                    "pipeline.sweep.pairs empty in config.yml — falling back to \
                     audit-frozen DEFAULT_SWEEP_PAIRS"
                );
                nxr_sdk::synth::pairs::DEFAULT_SWEEP_PAIRS
                    .iter()
                    .map(|(b, q)| (b.to_string(), q.to_string()))
                    .collect()
            } else {
                yml_pairs
                    .iter()
                    .map(|p| (p.base.to_uppercase(), p.quote.to_uppercase()))
                    .collect()
            }
        }
        Some(s) => s
            .split(',')
            .filter_map(|tok| {
                let tok = tok.trim();
                nxr_sdk::split_pair_multi(tok, &['/', '-'])
                    .map(|(b, q)| (b.to_uppercase(), q.to_uppercase()))
            })
            .collect(),
    }
}

fn parse_candidates(arg: Option<&str>) -> Vec<(&'static str, &'static [usize])> {
    let filter: Option<BTreeSet<String>> = arg.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    });
    CANDIDATES
        .iter()
        .filter(|(k, _)| match &filter {
            Some(set) => set.contains(*k),
            None => true,
        })
        .copied()
        .collect()
}

fn run_pair(
    args: &Args,
    yml: &nxr_sdk::pipeline_config::SeriesYml,
    cexs: &nxr_sdk::pipeline_config::CexsYml,
    base: &str,
    quote: &str,
    candidates: &[(&'static str, &'static [usize])],
) -> Result<BTreeMap<String, CandidateSummary>> {
    let cfg = nxr_sdk::NxrConfig::from_env();
    let data_root = Path::new(&cfg.indexes_dir)
        .parent()
        .unwrap_or(Path::new("/data"))
        .to_path_buf();

    let pair_str = format!("{}/{}", base.to_uppercase(), quote.to_uppercase());
    let ticker_id = resolve_ticker_id(&pair_str);
    let class_key = class_for_pair(cexs, base, quote);
    let target_bpd = match yml.calibration.target_for_class(class_key) {
        Some(t) => t,
        None => {
            info!(pair = pair_str, class = class_key, "class marked skip");
            return Ok(BTreeMap::new());
        }
    };

    let idx_directory = idx_dir(&data_root, ticker_id);
    let _bars_directory = bars_dir(&data_root, ticker_id);
    info!(
        pair = pair_str,
        ticker_id,
        class = class_key,
        target_bpd,
        idx = %idx_directory.display(),
        "sweep: pair start"
    );

    let shards = list_shards(&idx_directory, "idx")?;
    if shards.is_empty() {
        warn!(pair = pair_str, "no idx shards found, skipping");
        return Ok(BTreeMap::new());
    }

    let first_shard_date = shards.first().unwrap().0;
    let last_shard_date = shards.last().unwrap().0;
    let today = Utc::now().date_naive();
    let yesterday = today.pred_opt().unwrap_or(today);
    let default_to = last_shard_date.min(yesterday);

    let to_date = args
        .to
        .as_deref()
        .map(parse_date)
        .transpose()?
        .map(|d| d.min(default_to))
        .unwrap_or(default_to);

    // Choose evaluation days: last `max_days` closed days, taking every `stride`-th.
    let from_date = {
        let max_back = (args.max_days as i64).max(1);
        let candidate = to_date - chrono::Duration::days(max_back - 1);
        candidate.max(first_shard_date)
    };

    // ── Determine longest window across candidates → retention budget ───────
    let max_window_days_in_sweep = candidates
        .iter()
        .flat_map(|(_, w)| w.iter().copied())
        .max()
        .unwrap_or(120);

    let from_d_start = day_start_ms(from_date);
    let tick_retain_from = from_d_start - (max_window_days_in_sweep as i64) * MS_PER_DAY;

    // ── Pass 1: HLC + tick stream ───────────────────────────────────────────
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    let mut tick_stream: Vec<(i64, f64)> = Vec::new();
    let mut total_records: u64 = 0;

    for (_d, path) in &shards {
        let mut stream = ShardStream::<IndexRecord>::open(path)
            .with_context(|| format!("open idx {}", path.display()))?;
        while let Some(rec) = stream.next()? {
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let bid = rec.index.bid;
            let ask = rec.index.ask;
            let mid = (bid + ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                continue;
            }
            total_records += 1;

            let key = (ts / MS_PER_30MIN) * MS_PER_30MIN;
            let e = hlc.entry(key).or_insert((ask.max(mid), bid.min(mid)));
            if ask > e.0 {
                e.0 = ask;
            }
            if bid < e.1 && bid > 0.0 {
                e.1 = bid;
            }

            if ts >= tick_retain_from {
                tick_stream.push((ts, mid));
            }
        }
    }

    info!(
        pair = pair_str,
        idx_records = total_records,
        hlc_buckets = hlc.len(),
        tick_stream_len = tick_stream.len(),
        retain_days = max_window_days_in_sweep,
        "pass 1 done"
    );

    if hlc.is_empty() || tick_stream.is_empty() {
        warn!(pair = pair_str, "no usable mid quotes after scan");
        return Ok(BTreeMap::new());
    }

    // ── Vol + sigma cache (one per pair, shared across candidates) ─────────
    let vol_path = std::env::temp_dir().join(format!(
        "nxr-mtf-sweep-{}-{}.vol",
        ticker_id,
        std::process::id()
    ));
    {
        let mut writer = VolWriter::new(&vol_path)?;
        build_vol_from_hlc(&hlc, &yml.vol, &mut writer)?;
        writer.finish()?;
    }
    let vol_mmap = VolMmap::open(&vol_path)?;
    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, yml.vol.clone());
        calc.precompute_sigma_cache()
    };
    info!(
        pair = pair_str,
        vol_records = vol_mmap.records().len(),
        sigma_cache = sigma_cache.len(),
        "vol+sigma built"
    );

    let prices: Vec<(i64, f64)> = tick_stream;

    // ── Pick stratified day set ─────────────────────────────────────────────
    // Use range [from_date, to_date], every `stride` days, ascending.
    let mut days: Vec<NaiveDate> = Vec::new();
    {
        let mut d = from_date;
        let mut i = 0usize;
        while d <= to_date {
            if i % args.stride == 0 {
                days.push(d);
            }
            i += 1;
            d = d.succ_opt().unwrap_or(d);
        }
    }
    info!(
        pair = pair_str,
        n_days = days.len(),
        from = %from_date,
        to = %to_date,
        stride = args.stride,
        "stratified day set built"
    );

    let mut by_cand: BTreeMap<String, CandidateSummary> = BTreeMap::new();
    for (key, _) in candidates {
        by_cand.insert((*key).to_string(), CandidateSummary::default());
    }

    let cal_base = &yml.calibration;
    let stdout = std::io::stdout();

    for d in &days {
        let d_start = day_start_ms(*d);
        let d_end = d_start + MS_PER_DAY;
        let cutoff_idx = prices.partition_point(|(ts, _)| *ts < d_start);
        let trailing: &[(i64, f64)] = &prices[..cutoff_idx];
        // Count day-D ticks once (info-only, NOT used for sweep math).
        let day_lo = prices.partition_point(|(ts, _)| *ts < d_start);
        let day_hi = prices.partition_point(|(ts, _)| *ts < d_end);
        let day_ticks = day_hi.saturating_sub(day_lo);

        if trailing.len() < 2 || day_ticks == 0 {
            // Skip days without enough trailing context or no day-D ticks.
            continue;
        }

        for (key, windows) in candidates {
            let cal = CalibrationConfig {
                target_bpd,
                k_fit_windows_days: windows.to_vec(),
                min_window_days: cal_base.min_window_days,
                max_rounds: cal_base.max_rounds,
                tolerance: cal_base.tolerance,
                mult_bounds: cal_base.mult_bounds,
            };
            // Prior multiplier — match renko-trailing default (mid of bounds).
            let base_cfg = RenkoConfig {
                multiplier: 0.4_f32,
                min_pct: yml.renko.min_pct,
            };

            let k = calibrate_mtf_with_target(
                trailing,
                &cal,
                &base_cfg,
                &vol_mmap,
                &yml.vol,
                &sigma_cache,
                target_bpd,
            ) as f64;

            if !(k > 0.0 && k.is_finite()) {
                continue;
            }
            // Apply K_FLOOR explicitly so the reported k matches the engine's
            // effective k. The engine
            // (`nxr_sdk::renko::RenkoGenerator::compute_brick_size`) already
            // clamps k → max(k, K_FLOOR) internally; without this explicit
            // clamp on the sweep side the candidate logs k=0.01 but the brick
            // count reflects k=0.05 — accounting fraud.
            let k_clamped = k.max(nxr_sdk::renko::K_FLOOR);
            let renko_cfg = RenkoConfig {
                multiplier: k_clamped as f32,
                min_pct: yml.renko.min_pct,
            };
            if renko_cfg.validate().is_err() {
                continue;
            }

            // Count bricks for day D under k. `count_bars_from_prices`
            // iterates linearly; pass a sub-slice [day_lo..day_hi] so we
            // only touch O(day_ticks) entries instead of O(all_prices)
            // per (config, day). Massive speedup w/ retain=120d ticks.
            let day_slice: &[(i64, f64)] = &prices[day_lo..day_hi];
            let bars = count_bars_from_prices(
                day_slice,
                &renko_cfg,
                &vol_mmap,
                &yml.vol,
                &sigma_cache,
                d_start,
                d_end - 1,
                false,
            );
            let bpd_actual = bars as f64;
            let err_abs = (bpd_actual - target_bpd).abs();
            let err_rel = if target_bpd > 0.0 {
                err_abs / target_bpd
            } else {
                0.0
            };

            let rec = SweepRecord {
                pair: &pair_str,
                candidate: key,
                k_fit_windows_days: windows,
                date: d.to_string(),
                target_bpd,
                k,
                bpd_actual,
                err_abs,
                err_rel,
                trailing_n: trailing.len(),
                day_ticks,
            };
            let line = serde_json::to_string(&rec)?;
            {
                use std::io::Write;
                let mut h = stdout.lock();
                writeln!(h, "{}", line)?;
            }
            by_cand
                .get_mut(*key)
                .expect("preinit")
                .samples
                .push(bpd_actual);
        }
    }

    let _ = std::fs::remove_file(&vol_path);
    Ok(by_cand)
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let root: PipelineYml = PipelineYml::load(&args.config)?;
    let sweep_pairs = root.pipeline.sweep.pairs;
    let yml = root.series;
    let cexs = root.cexs;

    if cexs.crypto_majors.is_empty() {
        warn!("cexs.crypto_majors empty in YAML — falling back to DEFAULT_CRYPTO_MAJORS");
    }
    if cexs.stablecoins.is_empty() {
        warn!("cexs.stablecoins empty in YAML — falling back to DEFAULT_STABLECOINS");
    }
    if cexs.fx_majors.is_empty() {
        warn!("cexs.fx_majors empty in YAML — falling back to DEFAULT_FX_MAJORS");
    }

    let pairs = parse_pairs(args.pairs.as_deref(), &sweep_pairs);
    let candidates = parse_candidates(args.candidates.as_deref());
    info!(
        n_pairs = pairs.len(),
        n_candidates = candidates.len(),
        "mtf-sweep starting"
    );

    // Aggregate (candidate → (pair → CandidateSummary)).
    let mut combined: BTreeMap<String, BTreeMap<String, CandidateSummary>> = BTreeMap::new();
    for (k, _) in &candidates {
        combined.insert((*k).to_string(), BTreeMap::new());
    }

    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (b, q) in pairs {
        let pair_key = format!("{}/{}", b.to_uppercase(), q.to_uppercase());
        if !seen.insert(pair_key.clone()) {
            continue;
        }
        match run_pair(&args, &yml, &cexs, &b, &q, &candidates) {
            Ok(by_cand) => {
                for (cand_key, sum) in by_cand {
                    combined
                        .entry(cand_key)
                        .or_default()
                        .insert(pair_key.clone(), sum);
                }
            }
            Err(e) => warn!(pair = pair_key, err = %e, "pair run failed"),
        }
    }

    // ── Emit SWEEP summary on stderr — per-(candidate, pair) plus composite ─
    eprintln!("\n────── mtf-sweep summary ──────");
    let target = yml.calibration.target_bpd;
    // Per-(candidate, pair) lines + per-candidate aggregate.
    for (cand_key, by_pair) in &combined {
        let mut means_per_pair: Vec<f64> = Vec::new();
        let mut oobs_per_pair: Vec<f64> = Vec::new();
        for (pair_key, sum) in by_pair {
            let (mean, median, oob) = sum.finalize(target);
            means_per_pair.push(mean);
            oobs_per_pair.push(oob as f64);
            eprintln!(
                "SWEEP  candidate={} pair={} n={} bpd_mean={:.1} bpd_median={:.1} oob={} err_abs={:.1}",
                cand_key,
                pair_key,
                sum.samples.len(),
                mean,
                median,
                oob,
                (mean - target).abs()
            );
        }
        if !means_per_pair.is_empty() {
            let score_mean = means_per_pair
                .iter()
                .map(|m| (m - target).abs())
                .sum::<f64>()
                / means_per_pair.len() as f64;
            let mu = means_per_pair.iter().sum::<f64>() / means_per_pair.len() as f64;
            let var = means_per_pair
                .iter()
                .map(|m| (m - mu).powi(2))
                .sum::<f64>()
                / means_per_pair.len() as f64;
            let score_var = var.sqrt();
            let score_oob = oobs_per_pair.iter().sum::<f64>() / oobs_per_pair.len() as f64;
            // OOB weight 5.0 (raised from 0.0 per cohort audit). Previously
            // the sweep's "composite" picked candidates with low mean error
            // even when 30%+ of days fell outside [target/3, 2*target] —
            // a regime-leaky candidate could beat a stable one because OOB
            // carried zero weight.
            let composite = score_mean + 0.5 * score_var + 5.0 * score_oob;
            eprintln!(
                "AGG    candidate={} score_mean={:.2} score_var={:.2} score_oob={:.2} composite={:.2}",
                cand_key, score_mean, score_var, score_oob, composite
            );
        }
    }
    Ok(())
}
