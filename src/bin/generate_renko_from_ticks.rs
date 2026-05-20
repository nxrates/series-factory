//! Generate enriched Renko bars from raw MITCH tick data.
//!
//! Usage: generate-renko-from-ticks <nxrates.yml> [PAIR]
//!
//! Pipeline (2 tick-file scans):
//!   Scan 1: Stream ticks → 30-min Parkinson vol + 1-min downsampled prices
//!   Calibration: Binary search on in-memory prices (no disk I/O)
//!   Scan 2: Stream ticks → RenkoGenerator → enriched 96-byte mitch::Bar files
//!
//! ALL parameters come from nxrates.yml `series` section - nothing hardcoded.

use anyhow::Result;
use bytemuck::bytes_of;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::{parkinson_sigma, BarAccumulator};
use serde::Deserialize;
use series_factory::{
    bar_construction::{MtfParkinsonCalculator, RenkoConfig, RenkoGenerator, VolConfig},
    vol_bin::{VolMmap, VolWriter},
    TickFrame,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
};
use tracing::info;

// ── Pipeline config (deserialized from nxrates.yml `series` section) ────────

#[derive(Debug, Deserialize)]
struct NxratesYml {
    series: PipelineYml,
}

#[derive(Debug, Deserialize)]
struct PipelineYml {
    renko: RenkoYml,
    vol: VolConfig,
    calibration: CalibrationConfig,
    pipeline: PipelineParams,
}

#[derive(Debug, Deserialize)]
struct RenkoYml {
    min_pct: f32,
    max_pct: f32,
}

#[derive(Debug, Deserialize)]
struct CalibrationConfig {
    target_bpd: f64,
    windows_days: Vec<usize>,
    min_window_days: usize,
    max_rounds: usize,
    tolerance: f64,
    mult_bounds: [f64; 2],
}

#[derive(Debug, Deserialize)]
struct PipelineParams {
    bootstrap_days: i64,
    max_bars: usize,
    max_mem_gb: usize,
    exchanges: Vec<String>,
    pairs: Vec<String>,
}

// ── Tick file helpers ────────────────────────────────────────────────────────

fn discover_tick_files(ticks_dir: &PathBuf, pair: &str, exchanges: &[String]) -> Vec<PathBuf> {
    let symbol = format!("{}USDT", pair);
    let mut tick_files: Vec<PathBuf> = Vec::new();
    for exchange in exchanges {
        let dir = ticks_dir.join(exchange).join(&symbol);
        if !dir.exists() { continue; }
        let mut files: Vec<PathBuf> = fs::read_dir(&dir).into_iter().flatten()
            .filter_map(|e| e.ok()).map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ticks")
                && !p.file_name().and_then(|n| n.to_str()).map(|n| n.starts_with("._")).unwrap_or(false))
            .collect();
        info!("  {}: {} tick files in {}", exchange, files.len(), dir.display());
        tick_files.append(&mut files);
    }
    // Convert any old-format files before sorting
    for p in &tick_files {
        let file_len = std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0);
        let frame_size = std::mem::size_of::<TickFrame>();
        if file_len > 0 && file_len % frame_size != 0 && file_len % OLD_TICK_SIZE == 0 {
            maybe_convert_old_format(p);
        }
    }

    // Sort by first timestamp for chronological order
    let mut ts_files: Vec<(i64, PathBuf)> = tick_files.into_iter().filter_map(|p| {
        let f = File::open(&p).ok()?;
        let m = unsafe { memmap2::Mmap::map(&f).ok()? };
        if m.len() < std::mem::size_of::<TickFrame>() { return None; }
        let frame: &TickFrame = bytemuck::from_bytes(&m[..std::mem::size_of::<TickFrame>()]);
        Some((frame.timestamp_ms(), p))
    }).collect();
    ts_files.sort_unstable_by_key(|&(ts, _)| ts);
    ts_files.into_iter().map(|(_, p)| p).collect()
}

/// Old pre-MITCH tick format: 32 bytes per record.
/// Layout: [timestamp_ms: i64][bid: f64][ask: f64][bid_vol: u32][ask_vol: u32]
const OLD_TICK_SIZE: usize = 32;

/// Detect old-format tick file and convert to modern TickFrame in-place.
fn maybe_convert_old_format(path: &PathBuf) -> bool {
    let frame_size = std::mem::size_of::<TickFrame>();
    let file_len = match std::fs::metadata(path) {
        Ok(m) => m.len() as usize,
        Err(_) => return false,
    };
    // Already new format
    if file_len % frame_size == 0 { return false; }
    // Check if old 32-byte format
    if file_len % OLD_TICK_SIZE != 0 { return false; }

    let n_ticks = file_len / OLD_TICK_SIZE;
    info!("  Converting old-format tick file: {} ({} ticks)", path.display(), n_ticks);

    let f = match File::open(path) { Ok(f) => f, Err(_) => return false };
    let m = match unsafe { memmap2::Mmap::map(&f) } { Ok(m) => m, Err(_) => return false };

    let tmp = path.with_extension("ticks.converting");
    let out = match File::create(&tmp) { Ok(f) => f, Err(_) => return false };
    let mut writer = BufWriter::with_capacity(256 * 1024, out);

    for i in 0..n_ticks {
        let off = i * OLD_TICK_SIZE;
        let epoch_ms = i64::from_le_bytes(m[off..off+8].try_into().unwrap());
        let bid = f64::from_le_bytes(m[off+8..off+16].try_into().unwrap());
        let ask = f64::from_le_bytes(m[off+16..off+24].try_into().unwrap());
        let bvol = u32::from_le_bytes(m[off+24..off+28].try_into().unwrap());
        let avol = u32::from_le_bytes(m[off+28..off+32].try_into().unwrap());

        let tick = mitch::tick::Tick {
            ticker: 0,
            bid,
            ask,
            vbid: bvol,
            vask: avol,
        };
        let frame = TickFrame::new(0, mitch::timestamp::from_epoch_ms(epoch_ms), tick);
        if writer.write_all(bytes_of(&frame)).is_err() {
            let _ = fs::remove_file(&tmp);
            return false;
        }
    }
    drop(m);
    drop(f);

    if writer.flush().is_err() {
        let _ = fs::remove_file(&tmp);
        return false;
    }
    drop(writer);

    // Atomic replace
    if fs::rename(&tmp, path).is_err() {
        let _ = fs::remove_file(&tmp);
        return false;
    }
    info!("  Converted: {} → {} bytes", file_len, n_ticks * frame_size);
    true
}

/// Mmap a tick file and return the TickFrame slice (zero-copy).
/// Auto-converts old 32-byte format to modern 40-byte TickFrame if needed.
fn mmap_tick_file(path: &PathBuf) -> Option<(memmap2::Mmap, usize)> {
    let frame_size = std::mem::size_of::<TickFrame>();

    // Check if conversion needed before opening
    {
        let file_len = std::fs::metadata(path).ok()?.len() as usize;
        if file_len % frame_size != 0 && file_len % OLD_TICK_SIZE == 0 {
            maybe_convert_old_format(path);
        }
    }

    let f = File::open(path).ok()?;
    let m = unsafe { memmap2::Mmap::map(&f).ok()? };
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    unsafe { libc::madvise(m.as_ptr() as *mut _, m.len(), libc::MADV_SEQUENTIAL); }
    let n_frames = m.len() / frame_size;
    if n_frames == 0 { return None; }
    Some((m, n_frames))
}

fn release_mmap(mmap: &memmap2::Mmap) {
    unsafe { libc::madvise(mmap.as_ptr() as *mut _, mmap.len(), libc::MADV_DONTNEED); }
}

/// Get TickFrame slice from mmap (zero-copy).
#[inline]
fn frames_from_mmap(mmap: &memmap2::Mmap, n_frames: usize) -> &[TickFrame] {
    let frame_size = std::mem::size_of::<TickFrame>();
    let bytes = &mmap[..n_frames * frame_size];
    bytemuck::cast_slice(bytes)
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    // Shared RLIMIT_AS cap (60% physical or NXR_MAX_MEM_GB) — replaces the
    // per-binary set_memory_limit helper that used to live here. The yml
    // `pipeline.max_mem_gb` field is advisory (used for e.g. rayon sizing
    // if wired later); the process-wide cap comes from apply_safe_cap.
    nxr_sdk::memory::apply_safe_cap();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <nxrates.yml> [PAIR]", args[0]);
        std::process::exit(1);
    }

    let root: NxratesYml = serde_yaml::from_str(&fs::read_to_string(&args[1])?)?;
    let yml = root.series;

    // Storage paths come from the unified NXR_DATA_* taxonomy:
    //   NXR_DATA_TICKS  (raw tick archives, default <root>/ticks)
    //   NXR_DATA_BARS   (generated .bars files, default <root>/bars)
    let cfg = nxr_sdk::NxrConfig::from_env();
    let ticks_dir = PathBuf::from(&cfg.ticks_dir);
    let output_base = PathBuf::from(&cfg.bars_dir);

    let pairs: Vec<String> = if args.len() >= 3 {
        vec![args[2].to_uppercase()]
    } else {
        yml.pipeline.pairs.iter().map(|p| p.to_uppercase()).collect()
    };

    for pair in &pairs {
        info!("═══ Processing {} ═══", pair);
        let output_path = output_base.join(pair).join(format!("{}USDT.bars", pair));
        let config_path = output_base.join(pair).join("config.json");

        let mut config: RenkoConfig = if config_path.exists() {
            serde_json::from_str(&fs::read_to_string(&config_path)?)?
        } else {
            RenkoConfig { multiplier: 0.075, min_pct: yml.renko.min_pct, max_pct: yml.renko.max_pct }
        };
        config.min_pct = yml.renko.min_pct;
        config.max_pct = yml.renko.max_pct;
        config.validate()?;

        let tick_files = discover_tick_files(&ticks_dir, pair, &yml.pipeline.exchanges);
        info!("{} tick files across {} exchanges", tick_files.len(), yml.pipeline.exchanges.len());

        if tick_files.is_empty() {
            info!("No tick files for {}, skipping", pair);
            continue;
        }

        run_pipeline(&tick_files, &mut config, &yml, &output_path, &config_path)?;
    }

    Ok(())
}

fn run_pipeline(
    tick_files: &[PathBuf], config: &mut RenkoConfig,
    yml: &PipelineYml, output_path: &PathBuf, config_path: &PathBuf,
) -> Result<()> {

    // ═══ PASS 1: Build 30-min Parkinson vol + downsample 1-min prices ═══
    // Fused scan: one pass through all tick files builds both the vol series
    // (needed for RenkoGenerator) and 1-min downsampled prices (used for
    // in-memory calibration, eliminating ~15 redundant tick file re-reads).
    let t0 = std::time::Instant::now();
    let mut hlc: HashMap<i64, (f64, f64, f64)> = HashMap::new();
    let mut price_buckets: BTreeMap<i64, (i64, f64)> = BTreeMap::new();

    for (fi, path) in tick_files.iter().enumerate() {
        let Some((mmap, n)) = mmap_tick_file(path) else { continue };
        if fi % 10 == 0 { info!("  Pass 1: file {}/{}", fi + 1, tick_files.len()); }
        for frame in frames_from_mmap(&mmap, n) {
            let ts = frame.timestamp_ms();
            let mid = frame.mid_price();

            // Vol: 30-min HLC buckets
            let key = (ts / 1_800_000) * 1_800_000;
            let e = hlc.entry(key).or_insert((mid, mid, mid));
            if mid > e.0 { e.0 = mid; }
            if mid < e.1 { e.1 = mid; }
            e.2 = mid;

            // Price: 1-min last-close for calibration (~25 MB for 3 years)
            let bucket = (ts / 60_000) * 60_000;
            let pe = price_buckets.entry(bucket).or_insert((ts, mid));
            if ts >= pe.0 { *pe = (ts, mid); }
        }
        release_mmap(&mmap);
    }

    let parkinson = parkinson_sigma;

    let vol_path = output_path.with_extension("vol");
    let mut hours: Vec<(i64, f64, f64)> = hlc.into_iter().map(|(ts, (h, l, _))| (ts, h, l)).collect();
    hours.sort_unstable_by_key(|&(ts, _, _)| ts);
    let ema_period = yml.vol.ema_period;
    let alpha = 2.0 / (ema_period as f64 + 1.0);
    let mut vol_writer = VolWriter::new(&vol_path)?;
    let mut prev_ema: Option<f64> = None;

    for (i, &(ts, high, low)) in hours.iter().enumerate() {
        let sigma = parkinson(high, low);
        let ema = if i < ema_period {
            hours[..=i].iter().map(|&(_, h, l)| parkinson(h, l)).sum::<f64>() / (i + 1) as f64
        } else {
            alpha * sigma + (1.0 - alpha) * prev_ema.unwrap_or(sigma)
        };
        prev_ema = Some(ema);
        vol_writer.write_record(timestamp::from_epoch_ms(ts), ema)?;
    }
    vol_writer.finish()?;
    let vol_mmap = VolMmap::open(&vol_path)?;

    // Convert price buckets to sorted vec
    let tick_prices: Vec<(i64, f64)> = price_buckets.into_iter()
        .map(|(_, (ts, mid))| (ts, mid))
        .collect();
    let pass1_ms = t0.elapsed().as_millis();
    let price_mb = (tick_prices.len() * 16) as f64 / (1024.0 * 1024.0);
    info!("Pass 1 done: {} vol records, {} prices ({:.1} MB) in {}ms",
        hours.len(), tick_prices.len(), price_mb, pass1_ms);

    // Diagnostic: dump first/last prices and check for zeros
    if let Some(first) = tick_prices.first() {
        info!("  prices[0]: ts={} mid={:.2}", first.0, first.1);
    }
    if let Some(last) = tick_prices.last() {
        info!("  prices[last]: ts={} mid={:.2}", last.0, last.1);
    }
    let n_zero = tick_prices.iter().filter(|p| p.1 <= 0.0 || !p.1.is_finite()).count();
    let n_valid = tick_prices.len() - n_zero;
    info!("  valid={}, zero/nan={}", n_valid, n_zero);
    if let (Some(first), Some(last)) = (tick_prices.first(), tick_prices.last()) {
        let days = (last.0 - first.0) as f64 / 86_400_000.0;
        info!("  span: {:.1} days", days);
    }

    // ═══ CALIBRATION (in-memory from downsampled prices) ═══
    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, yml.vol.clone());
        let t = std::time::Instant::now();
        let c = calc.precompute_sigma_cache();
        let min_s = c.iter().cloned().fold(f64::MAX, f64::min);
        let max_s = c.iter().cloned().fold(f64::MIN, f64::max);
        let mean_s = c.iter().sum::<f64>() / c.len().max(1) as f64;
        info!("Sigma cache: {} entries in {}ms (min={:.6} max={:.6} mean={:.6})",
            c.len(), t.elapsed().as_millis(), min_s, max_s, mean_s);
        c
    };

    {
        let mult = calibrate_mtf(&tick_prices, &yml.calibration, config, &vol_mmap, &yml.vol, &sigma_cache);
        if mult > 0.0 { config.multiplier = mult; }
        info!("Calibrated: multiplier={:.6} (target {:.0} bpd)", config.multiplier, yml.calibration.target_bpd);
        if let Some(parent) = config_path.parent() { fs::create_dir_all(parent)?; }
        fs::write(config_path, serde_json::to_string_pretty(&config)?)?;
        info!("Updated config: {}", config_path.display());
    }

    // ═══ PASS 2: Generate bars + inline enrichment via BarAccumulator ═══
    // BarAccumulator ingests every post-bootstrap tick, then we flush it at each
    // emitted Renko bar to overlay the enrichment fields. Geometry (OHLC + ts)
    // comes from RenkoGenerator; enrichment (dispersion, drift, ...) from accum.
    let first_price_ts = tick_prices.first().map(|p| p.0).unwrap_or(0);
    let bootstrap_end = first_price_ts + yml.pipeline.bootstrap_days * 86_400_000;
    let t1 = std::time::Instant::now();
    let mut bars: Vec<Bar> = Vec::new();
    let mut accum = BarAccumulator::new();
    let mut pending: Vec<Bar> = Vec::new();
    let mut generator = RenkoGenerator::new(*config, &vol_mmap, yml.vol.clone())?;
    let mut pass2_tick_count = 0u64;
    let mut pass2_post_bootstrap = 0u64;

    for (fi, path) in tick_files.iter().enumerate() {
        let Some((mmap, n)) = mmap_tick_file(path) else { continue };
        if fi % 10 == 0 { info!("  Pass 2: file {}/{}", fi + 1, tick_files.len()); }

        for frame in frames_from_mmap(&mmap, n) {
            let ts = frame.timestamp_ms();
            let mid = frame.mid_price();
            let body = frame.body;
            pass2_tick_count += 1;

            if pass2_tick_count <= 3 || (pass2_tick_count == 100_000) {
                let spread = frame.spread();
                eprintln!("  [diag] tick #{}: ts={} mid={:.2} spread={:.4}",
                    pass2_tick_count, ts, mid, spread);
            }

            if ts < bootstrap_end {
                generator.feed_tick(ts, mid, &mut |_: &Bar| Ok(()))?;
                continue;
            }

            pass2_post_bootstrap += 1;
            if pass2_post_bootstrap == 1 {
                eprintln!("  [diag] first post-bootstrap tick: ts={} mid={:.2} (bootstrap_end={})", ts, mid, bootstrap_end);
                let (nb, _) = generator.stats();
                eprintln!("  [diag] generator state after bootstrap: n_bars={}", nb);
            }

            accum.ingest(body.bid, body.ask, body.vbid, body.vask, ts, 0.0, 1, 0);
            generator.feed_tick(ts, mid, &mut |bar: &Bar| { pending.push(*bar); Ok(()) })?;

            if !pending.is_empty() {
                // Flush accumulator once per burst of emissions (enrichment
                // attributed to the first bar in the burst; subsequent bars in
                // the same tick have zero enrichment since no additional ticks
                // fell in their time window).
                let enrich = accum.flush();
                for (j, mut bar) in pending.drain(..).enumerate() {
                    bar.kind = BarKind::Renko as u8;
                    if j == 0 {
                        if let Some(e) = enrich {
                            bar.vbid = e.vbid;
                            bar.vask = e.vask;
                            bar.tick_count = e.tick_count;
                            bar.realized_var = e.realized_var;
                            bar.bipower_var = e.bipower_var;
                            bar.drift = e.drift;
                            bar.vol_imbalance = e.vol_imbalance;
                            bar.avg_spread_bps = e.avg_spread_bps;
                            bar.max_abs_return = e.max_abs_return;
                            bar.avg_ci_ubp = e.avg_ci_ubp;
                            bar.reject_rate = e.reject_rate;
                        }
                    }
                    bars.push(bar);
                }
                if bars.len() % 50_000 == 0 { eprintln!("    ... {} bars generated", bars.len()); }
                if bars.len() > yml.pipeline.max_bars { anyhow::bail!("Bar count exceeds {} safety limit", yml.pipeline.max_bars); }
            }
        }
        release_mmap(&mmap);
    }

    info!("Generated {} bars in {}ms (ticks={} post_bootstrap={})",
        bars.len(), t1.elapsed().as_millis(), pass2_tick_count, pass2_post_bootstrap);

    // ═══ WRITE OUTPUT ═══
    // Atomic tmp+rename: readers (btr/prime engine) holding an old FD continue
    // reading the prior inode until they re-open, rather than seeing a
    // truncated-mid-write buffer.
    nxr_sdk::ipc::write_atomic::<Bar>(&output_path, &bars)?;
    let _ = fs::remove_file(&vol_path);

    let total_ms = t0.elapsed().as_millis();
    let file_mb = output_path.metadata()?.len() as f64 / 1024.0 / 1024.0;
    info!("=== Done: {} bars, {:.1} MB in {:.1}s (pass1={:.1}s pass2={:.1}s) ===",
        bars.len(), file_mb, total_ms as f64 / 1000.0,
        pass1_ms as f64 / 1000.0, t1.elapsed().as_millis() as f64 / 1000.0);

    Ok(())
}

// ── Calibration (in-memory from downsampled prices) ─────────────────────────

fn count_bars_from_prices(
    prices: &[(i64, f64)], config: &RenkoConfig, vol_mmap: &VolMmap,
    vol_config: &VolConfig, sigma_cache: &[f64], from_ts: i64, to_ts: i64,
    diag: bool,
) -> usize {
    let mut gen = match RenkoGenerator::new(*config, vol_mmap, vol_config.clone()) {
        Ok(g) => g,
        Err(e) => {
            if diag { eprintln!("  [diag] RenkoGenerator::new failed: {}", e); }
            return 0;
        }
    };
    gen.set_sigma_cache(sigma_cache);
    let mut count = 0usize;
    let mut n_in_range = 0usize;
    let mut n_skipped_before = 0usize;

    for &(ts, mid) in prices {
        if ts < from_ts { n_skipped_before += 1; continue; }
        if ts > to_ts { break; }
        n_in_range += 1;
        gen.feed_tick(ts, mid, &mut |_| { count += 1; Ok(()) }).ok();
        if count > 1_000_000 { return count; }
    }
    if diag {
        eprintln!("  [diag] count_bars: skipped_before={} in_range={} bars={} mult={:.6}",
            n_skipped_before, n_in_range, count, config.multiplier);
    }
    count
}

fn calibrate_mtf(
    prices: &[(i64, f64)], cal: &CalibrationConfig, base: &RenkoConfig,
    vol_mmap: &VolMmap, vol_config: &VolConfig, sigma_cache: &[f64],
) -> f32 {
    let first = prices.first().map(|p| p.0).unwrap_or(0);
    let last = prices.last().map(|p| p.0).unwrap_or(0);
    if last <= first { return 0.0; }

    let t0 = std::time::Instant::now();
    let mut mults: Vec<f32> = Vec::new();

    for &window_days in &cal.windows_days {
        let from = (last - window_days as i64 * 86_400_000).max(first);
        let days = (last - from) as f64 / 86_400_000.0;
        if days < cal.min_window_days as f64 {
            info!("  {}d window: insufficient data ({:.0}d available), skipping", window_days, days);
            continue;
        }

        let (mut log_lo, mut log_hi) = (cal.mult_bounds[0].ln(), cal.mult_bounds[1].ln());
        let mut best = (base.multiplier, f64::MAX);

        for round in 0..cal.max_rounds {
            let log_mid = (log_lo + log_hi) / 2.0;
            let mult = log_mid.exp() as f32;
            let trial = RenkoConfig { multiplier: mult, min_pct: base.min_pct, ..*base };
            let t_round = std::time::Instant::now();
            let diag = round == 0; // first round of each window gets diagnostics
            let n = count_bars_from_prices(prices, &trial, vol_mmap, vol_config, sigma_cache, from, last, diag);
            let bpd = n as f64 / days;
            let err = (bpd / cal.target_bpd - 1.0).abs();
            info!("    round {}/{}: mult={:.6} bars={} bpd={:.1} err={:.1}% ({:.1}ms)",
                round + 1, cal.max_rounds, mult, n, bpd, err * 100.0, t_round.elapsed().as_millis());

            if err < best.1 { best = (mult, err); }
            if err < cal.tolerance { break; }
            if bpd > cal.target_bpd { log_lo = log_mid; } else { log_hi = log_mid; }
        }

        info!("  {}d window: mult={:.6} (err={:.1}%)", window_days, best.0, best.1 * 100.0);
        mults.push(best.0);
    }

    if mults.is_empty() { return 0.0; }
    let geo_mean = (mults.iter().map(|m| (*m as f64).ln()).sum::<f64>() / mults.len() as f64).exp() as f32;

    info!("MTF calibration done: geo_mean={:.6} from {:?} in {:.1}s",
        geo_mean, mults, t0.elapsed().as_secs_f64());
    geo_mean
}
