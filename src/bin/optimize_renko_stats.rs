//! Renko configuration optimizer - pure statistical objective.
//!
//! Selects the volatility parameters (multiplier, min/max pct) that yield
//! the highest-quality bar series for downstream ML.
//!
//! Objective (zero trading metrics):
//!   J_stats = 0.30*STAT + 0.30*IID + 0.20*HOMO + 0.05*NORM + 0.15*ROBUST
//!
//! Search strategy: Phase A = parallel random exploration → Phase B = batched TPE.
//! Bar generation: full MTF Parkinson vol via RenkoGenerator.
//! Fold scoring: 5 non-overlapping temporal folds on the resulting bar series.
//!
//! After optimization, the best config's bars are enriched with microstructure
//! features from raw tick data and written as 96-byte mitch::Bar files - no
//! separate generation step required.
//!
//! Memory safety:
//!   - Dynamic RAM detection (macOS sysctl / fallback)
//!   - Hard RLIMIT_DATA cap prevents OOM crashes
//!   - Tick files processed in small batches (controlled concurrency)
//!   - Only 1-minute downsampled prices kept in RAM (~7 MB per year)
//!   - Rayon thread pool sized to fit memory budget
//!
//! Usage:
//!   optimize-renko-stats <BASE> <QUOTE> <CACHE_DIR> <OUTPUT_DIR> [N_TRIALS] [MAX_MEM_GB] [BARS_DIR]

use anyhow::Result;
use rayon::prelude::*;
use mitch::bar::{Bar, BarKind};
use nxr_sdk::BarAccumulator;
use series_factory::{
    bar_construction::build_vol_from_mid_ticks,
    vol_bin::{VolMmap, VolWriter},
    sampler::{SearchConfig, SearchState},
    read_tick_file,
    stats::{aggregate_fold_scores, compute_returns, score_fold, GateSpec, StatAggregateScore},
};
use nxr_sdk::vol::{VolConfig, VolSource};
use nxr_sdk::renko::{RenkoConfig, RenkoGenerator, SIGMA_FALLBACK};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};
use tracing::info;

/// Interval for downsampled price series (1 minute).
const DOWNSAMPLE_INTERVAL_MS: i64 = 60_000;

/// TPE batch size - generate this many configs per batch, score in parallel.
const TPE_BATCH_SIZE: usize = 32;

/// Estimated peak RAM per tick file being processed (Vec<TickFrame>).
/// Conservative: ~400 MB for a large daily file.
const ESTIMATED_FILE_RAM_MB: usize = 400;

/// RAM reserved for OS + the optimizer's own data structures.
const RESERVED_RAM_MB: usize = 2048;

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "Usage: {} <BASE> <QUOTE> <TICKS_DIR> <STATS_DIR> [N_TRIALS] [MAX_MEM_GB] [BARS_DIR]",
            args[0]
        );
        eprintln!("  TICKS_DIR  raw tick archives (falls back to $NXR_DATA_TICKS)");
        eprintln!("  STATS_DIR  optimizer stats/params output (independent of .bars)");
        eprintln!("  N_TRIALS   default: 512");
        eprintln!("  MAX_MEM_GB default: auto-detect (50% of physical RAM)");
        eprintln!("  BARS_DIR   if set, writes enriched .bars file directly here");
        std::process::exit(1);
    }

    let base = &args[1];
    let quote = &args[2];
    let ticks_dir = PathBuf::from(&args[3]);
    let stats_dir = PathBuf::from(&args[4]);
    let n_trials: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(512);
    let max_mem_gb: usize = args
        .get(6)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or_else(|| {
            // Default: follow the shared NXR cap helper (env var or 60%
            // physical). Converted back to GB so the rayon sizing math below
            // keeps working unchanged.
            let cap_bytes = nxr_sdk::memory::default_cap_bytes();
            ((cap_bytes / (1024 * 1024 * 1024)) as usize).max(4)
        });
    let bars_dir: Option<PathBuf> = args.get(7).map(PathBuf::from);

    // ── Enforce memory limit at OS level ─────────────────────────────────────
    // Honour the explicit CLI arg / detected cap, not the env default, so
    // operators can bound the optimizer tighter than the stack-wide default.
    nxr_sdk::memory::apply_rlimit_bytes((max_mem_gb as u64) * 1024 * 1024 * 1024);

    // ── Size rayon thread pool to fit memory budget ──────────────────────────
    let n_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let available_for_files_mb = (max_mem_gb * 1024).saturating_sub(RESERVED_RAM_MB);
    let max_concurrent_files = (available_for_files_mb / ESTIMATED_FILE_RAM_MB).max(1);
    let n_threads = n_cpus.min(max_concurrent_files).max(1);

    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build_global()
        .ok();

    // Output goes directly to stats_dir (no nested BASE/ subdirectory)
    fs::create_dir_all(&stats_dir)?;

    info!("=== Renko Statistical Optimizer  pair={}{} ===", base, quote);
    info!(
        "Trials: {}  CPUs: {}  threads: {}  mem_limit: {} GB (detected)",
        n_trials, n_cpus, n_threads, max_mem_gb
    );
    if let Some(ref bd) = bars_dir {
        info!("Bars output: {}", bd.display());
    }

    // ── Discover tick files ──────────────────────────────────────────────────
    let pair_name = format!("{}{}", base, quote);
    let exchange_dir = ticks_dir.join("binance").join(&pair_name);

    let mut tick_files: Vec<PathBuf> = fs::read_dir(&exchange_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ticks"))
        .collect();
    tick_files.sort();
    info!("Found {} tick files", tick_files.len());

    // ── Phase 1: Build Parkinson vol (batched parallel) ────────────────────────────────
    let vol_cfg = VolConfig::default();
    let vol_path = stats_dir.join(format!("_temp_vol_{}.vol", base));
    build_vol_batched(&tick_files, &vol_path, max_concurrent_files, &vol_cfg)?;
    let vol_mmap = VolMmap::open(&vol_path)?;
    info!("Vol: {} records", vol_mmap.len());

    // ── Phase 2: Downsample prices (batched parallel) ────────────────────────
    let tick_prices =
        load_downsampled_prices_batched(&tick_files, DOWNSAMPLE_INTERVAL_MS, max_concurrent_files)?;
    let n_prices = tick_prices.len();
    let mem_mb = (n_prices * 16) as f64 / (1024.0 * 1024.0);
    info!("Downsampled prices: {} points ({:.1} MB)", n_prices, mem_mb);

    let duration_ms = tick_prices.last().map(|t| t.0).unwrap_or(0)
        - tick_prices.first().map(|t| t.0).unwrap_or(0);
    let days = duration_ms as f64 / nxr_sdk::shard::MS_PER_DAY as f64;
    info!("Duration: {:.1} days ({:.1} years)", days, days / 365.25);

    // ── Gate spec (density gate removed - J_stats decides what's best) ────────
    let gate_spec = GateSpec {
        max_reversal_delay_pct: 0.5,
    };
    info!("Gates: LAG < 50% (no density gate - optimizer explores freely)");

    // ── Phase A: Parallel random exploration ──────────────────────────────────
    let n_phase_a = (n_trials * 3 / 4).max(16);
    let n_phase_b = n_trials - n_phase_a;
    info!(
        "Phase A: {} exploration trials (parallel)  Phase B: {} refinement trials",
        n_phase_a, n_phase_b
    );

    let mut search = SearchState::new(SearchConfig {
        n_explore: n_phase_a,
        n_refine: n_phase_b,
        seed: 42,
    });
    let phase_a_configs: Vec<RenkoConfig> = (0..n_phase_a)
        .filter_map(|_| search.next_config())
        .collect();

    let phase_a_results: Vec<(RenkoConfig, StatAggregateScore, f64)> = phase_a_configs
        .par_iter()
        .map(|config| {
            let bars = generate_bars(&tick_prices, config, &vol_mmap);
            let score = if bars.len() >= 100 {
                score_config(&bars, duration_ms, &gate_spec)
            } else {
                score_config(&[], duration_ms, &gate_spec)
            };
            let obj = if score.passed_gates {
                score.objective
            } else {
                0.0
            };
            (*config, score, obj)
        })
        .collect();

    let mut best_score = f64::NEG_INFINITY;
    let mut best_result: Option<(RenkoConfig, StatAggregateScore)> = None;

    for (i, (config, score, obj)) in phase_a_results.iter().enumerate() {
        if i % 50 == 0 || (*obj > best_score && score.passed_gates) {
            info!(
                "[A {}/{}] J={:.4}  STAT={:.3} IID={:.3} HOMO={:.3} NORM={:.3} ROBUST={:.3} \
                 bars={:.0}/d  gates={}",
                i + 1,
                n_phase_a,
                score.objective,
                score.median_stat,
                score.median_iid,
                score.homo,
                score.median_norm,
                score.robust,
                score.bars_per_day,
                if score.passed_gates { "YES" } else { "NO" }
            );
        }
        if *obj > best_score {
            best_score = *obj;
            best_result = Some((*config, score.clone()));
            info!("  * new best J={:.4}", best_score);
        }
    }

    // Feed Phase A results into search state for Phase B refinement
    for (config, _, obj) in &phase_a_results {
        search.update(*config, *obj);
    }

    info!(
        "Phase A complete: best J={:.4} from {} trials",
        best_score, n_phase_a
    );

    // ── Phase B: Local refinement around best ─────────────────────────────────
    let mut no_improve = 0usize;
    const MAX_NO_IMPROVE: usize = 100;

    let mut phase_b_done = 0;
    while phase_b_done < n_phase_b {
        let batch_size = TPE_BATCH_SIZE.min(n_phase_b - phase_b_done);

        let mut batch_configs = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match search.next_config() {
                Some(c) => batch_configs.push(c),
                None => break,
            }
        }
        if batch_configs.is_empty() {
            break;
        }

        let batch_results: Vec<(RenkoConfig, StatAggregateScore, f64)> = batch_configs
            .par_iter()
            .map(|config| {
                let bars = generate_bars(&tick_prices, config, &vol_mmap);
                let score = if bars.len() >= 100 {
                    score_config(&bars, duration_ms, &gate_spec)
                } else {
                    score_config(&[], duration_ms, &gate_spec)
                };
                let obj = if score.passed_gates {
                    score.objective
                } else {
                    0.0
                };
                (*config, score, obj)
            })
            .collect();

        let mut batch_improved = false;
        for (config, score, obj) in &batch_results {
            search.update(*config, *obj);
            phase_b_done += 1;

            if *obj > best_score {
                best_score = *obj;
                best_result = Some((*config, score.clone()));
                batch_improved = true;
                info!(
                    "[B {}/{}] * J={:.4}  STAT={:.3} IID={:.3} HOMO={:.3} bars={:.0}/d",
                    phase_b_done, n_phase_b, score.objective, score.median_stat, score.median_iid,
                    score.homo, score.bars_per_day,
                );
            }
        }

        if batch_improved {
            no_improve = 0;
        } else {
            no_improve += batch_size;
            if no_improve >= MAX_NO_IMPROVE {
                info!(
                    "Plateau: {} trials without improvement - stopping Phase B",
                    MAX_NO_IMPROVE
                );
                break;
            }
        }
    }

    // ── Save results ──────────────────────────────────────────────────────────
    if let Some((ref config, ref score)) = best_result {
        let config_path = stats_dir.join("bar-params.json");
        fs::write(&config_path, serde_json::to_string_pretty(config)?)?;

        let summary_path = stats_dir.join("best_summary.txt");
        let mut f = fs::File::create(&summary_path)?;
        writeln!(f, "J_stats:    {:.4}", score.objective)?;
        writeln!(f, "  STAT:     {:.4}", score.median_stat)?;
        writeln!(f, "  IID:      {:.4}", score.median_iid)?;
        writeln!(f, "  HOMO:     {:.4}", score.homo)?;
        writeln!(f, "  NORM:     {:.4}", score.median_norm)?;
        writeln!(f, "  ROBUST:   {:.4}", score.robust)?;
        writeln!(f, "bars/day:   {:.1}", score.bars_per_day)?;
        writeln!(f, "reversal:   {:.3}", score.reversal_delay)?;
        writeln!(f, "multiplier:     {:.6}", config.multiplier)?;
        writeln!(f, "min_pct:        {:.6}", config.min_pct)?;
        // max_pct removed 2026-05-24 (operator: no cap on adaptive renko).
        writeln!(f, "total_trials:   {}", n_phase_a + phase_b_done)?;

        info!("=== Optimisation complete ===");
        info!("Best J_stats = {:.4}", score.objective);
        info!("Config saved to: {}", config_path.display());
    } else {
        info!("No valid configuration found (all configs failed hard gates).");
    }

    // ── Phase F: Generate enriched .bars file ────────────────────────────────
    // If BARS_DIR is set and we found a valid config, generate the production
    // bars file directly - no separate binary needed.
    if let (Some(ref bars_dir), Some((ref config, _))) = (&bars_dir, &best_result) {
        info!("=== Generating enriched .bars file ===");

        // Re-generate bars with the best config (fast - uses downsampled prices already in RAM).
        let mut bars = generate_bars(&tick_prices, config, &vol_mmap);
        info!("Re-generated {} bars with best config", bars.len());

        // Build bar close-time boundaries for assigning ticks to bars.
        // Each bar covers (prev_close_ms, this_close_ms]. The first bar's lower
        // bound is 0 (captures any pre-first-close ticks).
        let bar_boundaries: Vec<(i64, i64)> = bars
            .iter()
            .enumerate()
            .map(|(i, bar)| {
                let start = if i > 0 { bars[i - 1].close_time_ms() } else { 0 };
                (start, bar.close_time_ms())
            })
            .collect();

        // Stream ticks → per-bar BarAccumulator → overlay enrichment on Renko bars.
        let mut accumulators: Vec<BarAccumulator> = (0..bars.len())
            .map(|_| BarAccumulator::new())
            .collect();

        for (chunk_idx, chunk) in tick_files.chunks(max_concurrent_files).enumerate() {
            if chunk_idx % 10 == 0 {
                let n_batches = (tick_files.len() + max_concurrent_files - 1) / max_concurrent_files;
                info!(
                    "  Enrichment batch {}/{}...",
                    chunk_idx + 1,
                    n_batches
                );
            }
            for tick_file in chunk {
                let ticks = match read_tick_file(tick_file) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                for t in &ticks {
                    let ts = t.timestamp_ms();
                    let bar_idx = match bar_boundaries.binary_search_by(|&(start, end)| {
                        if ts <= start {
                            std::cmp::Ordering::Greater
                        } else if ts > end {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    }) {
                        Ok(idx) => idx,
                        Err(_) => continue,
                    };
                    if bar_idx < accumulators.len() {
                        let body = t.body;
                        accumulators[bar_idx].ingest(
                            body.bid, body.ask, body.vbid, body.vask, ts,
                            0.0, 1, 0,
                        );
                    }
                }
            }
        }

        // Overlay enrichment and mark kind.
        for (i, accum) in accumulators.iter_mut().enumerate() {
            bars[i].kind = BarKind::Renko as u8;
            if let Some(e) = accum.flush() {
                bars[i].vbid = e.vbid;
                bars[i].vask = e.vask;
                bars[i].tick_count = e.tick_count;
                bars[i].realized_var = e.realized_var;
                bars[i].bipower_var = e.bipower_var;
                bars[i].drift = e.drift;
                bars[i].vol_imbalance = e.vol_imbalance;
                bars[i].avg_spread_bps = e.avg_spread_bps;
                bars[i].max_abs_return = e.max_abs_return;
                bars[i].avg_ci_ubp = e.avg_ci_ubp;
                bars[i].reject_rate = e.reject_rate;
            }
        }

        // Write 96-byte mitch::Bar file atomically (tmp + rename) so readers
        // never observe a partial write.
        fs::create_dir_all(bars_dir)?;
        let bars_path = bars_dir.join(format!("{}.bars", pair_name.to_uppercase()));
        nxr_sdk::ipc::write_atomic::<Bar>(&bars_path, &bars)?;
        let file_size_mb = bars_path.metadata()?.len() as f64 / 1024.0 / 1024.0;
        info!(
            "Bars written: {} bars ({:.1} MB) -> {}",
            bars.len(),
            file_size_mb,
            bars_path.display()
        );
    }

    // ── Clean up temp vol ─────────────────────────────────────────────────────
    let _ = fs::remove_file(&vol_path);

    Ok(())
}

// ── Scoring ───────────────────────────────────────────────────────────────────

fn score_config(bars: &[Bar], duration_ms: i64, gate_spec: &GateSpec) -> StatAggregateScore {
    const N_FOLDS: usize = 5;
    const MIN_PER_FOLD: usize = 30;

    if bars.len() < N_FOLDS * MIN_PER_FOLD {
        return aggregate_fold_scores(&[], 0, 0, bars, gate_spec);
    }

    let fold_size = bars.len() / N_FOLDS;
    let mut fold_scores = Vec::with_capacity(N_FOLDS);

    for f in 0..N_FOLDS {
        let start = f * fold_size;
        let end = if f == N_FOLDS - 1 {
            bars.len()
        } else {
            start + fold_size
        };
        let returns = compute_returns(&bars[start..end], 1);
        if returns.len() < MIN_PER_FOLD {
            continue;
        }
        fold_scores.push(score_fold(&returns));
    }

    aggregate_fold_scores(&fold_scores, bars.len(), duration_ms, bars, gate_spec)
}

// ── Bar generation ────────────────────────────────────────────────────────────

fn generate_bars(
    tick_prices: &[(i64, f64)],
    config: &RenkoConfig,
    vol_mmap: &VolMmap,
) -> Vec<Bar> {
    let mut generator = match RenkoGenerator::new(*config) {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    // Resolve σ per tick via the vol mmap directly (this binary's hot path
    // is single-pass over already-decoded tick prices, no cache).
    let mut calc = nxr_sdk::vol::MtfVolCalculator::new(vol_mmap, VolConfig::default());
    let sigma_cache = calc.precompute_sigma_cache();
    let mut bars = Vec::new();
    let iter = tick_prices.iter().map(|&(ts, mid)| {
        let mts = nxr_sdk::mitch::timestamp::from_epoch_ms(ts);
        let i = vol_mmap.find_index_for_mts(mts);
        let sigma = sigma_cache.get(i).copied().unwrap_or(SIGMA_FALLBACK);
        (ts, mid, sigma)
    });
    let _ = generator.generate(iter, |bar: &Bar| {
        bars.push(*bar);
        Ok(())
    });
    bars
}

// ── Batched-parallel vol construction ────────────────────────────────────────

fn build_vol_batched(
    tick_files: &[PathBuf],
    path: &Path,
    batch_size: usize,
    vol_cfg: &VolConfig,
) -> Result<()> {
    info!(
        "Building Rogers-Satchell vol (over gapless s10 OHLC) from {} files (batch_size={})...",
        tick_files.len(),
        batch_size
    );

    // Collect the full (ts, mid) stream (parallel per-file read), then build the
    // vol via the canonical RS-over-s10-OHLC builder (matches every other
    // pipeline + the live ring). Previous code built 30-min HLC here.
    let mut mids: Vec<(i64, f64)> = Vec::new();

    for (chunk_idx, chunk) in tick_files.chunks(batch_size).enumerate() {
        if chunk_idx % 5 == 0 {
            info!(
                "  Vol batch {}/{}...",
                chunk_idx + 1,
                (tick_files.len() + batch_size - 1) / batch_size
            );
        }

        let partials: Vec<Vec<(i64, f64)>> = chunk
            .par_iter()
            .filter_map(|tick_file| {
                let ticks = read_tick_file(tick_file).ok()?;
                let mut v: Vec<(i64, f64)> = Vec::with_capacity(ticks.len());
                for t in &ticks {
                    v.push((t.timestamp_ms(), t.mid_price()));
                }
                Some(v)
            })
            .collect();

        for partial in partials {
            mids.extend(partial);
        }
    }

    // Ensure chronological order before the gapless 10s-bucket fill.
    mids.sort_unstable_by_key(|&(ts, _)| ts);

    let mut writer = VolWriter::new(path)?;
    let n = build_vol_from_mid_ticks(mids.iter().copied(), vol_cfg, &mut writer)?;
    writer.finish()?;
    info!("Vol built: {} 30-min records", n);
    Ok(())
}

// ── Batched-parallel downsampled price loading ───────────────────────────────

fn load_downsampled_prices_batched(
    tick_files: &[PathBuf],
    interval_ms: i64,
    batch_size: usize,
) -> Result<Vec<(i64, f64)>> {
    info!(
        "Loading downsampled prices ({}ms interval, batch_size={})...",
        interval_ms, batch_size
    );

    let mut buckets: BTreeMap<i64, (i64, f64)> = BTreeMap::new();

    for (chunk_idx, chunk) in tick_files.chunks(batch_size).enumerate() {
        if chunk_idx % 5 == 0 {
            info!(
                "  Price batch {}/{}...",
                chunk_idx + 1,
                (tick_files.len() + batch_size - 1) / batch_size
            );
        }

        let partial_buckets: Vec<BTreeMap<i64, (i64, f64)>> = chunk
            .par_iter()
            .filter_map(|tick_file| {
                let ticks = read_tick_file(tick_file).ok()?;
                let mut b: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
                for t in &ticks {
                    let ts = t.timestamp_ms();
                    let bucket = (ts / interval_ms) * interval_ms;
                    let mid = t.mid_price();
                    let entry = b.entry(bucket).or_insert((ts, mid));
                    if ts >= entry.0 {
                        *entry = (ts, mid);
                    }
                }
                Some(b)
            })
            .collect();

        for partial in partial_buckets {
            for (bucket, (ts, mid)) in partial {
                let entry = buckets.entry(bucket).or_insert((ts, mid));
                if ts >= entry.0 {
                    *entry = (ts, mid);
                }
            }
        }
    }

    let prices: Vec<(i64, f64)> = buckets.into_iter().map(|(_, (ts, mid))| (ts, mid)).collect();
    info!("Downsampled: {} price points", prices.len());
    Ok(prices)
}
