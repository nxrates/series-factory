//! Generate renko bars from a single cross-provider composite `.idx` file.
//!
//! Two-pass, streaming:
//!   Pass 1 — sweep the composite `.idx` to build 30 min Parkinson HLC + an
//!           EMA-smoothed sigma `.vol` file. Optionally use the same sweep
//!           to calibrate the renko `multiplier` against `target_bpd`
//!           (currently opt-in via `--calibrate`).
//!   Pass 2 — sweep the composite `.idx` again, feed each record's mid
//!           price to `RenkoGenerator`. The generator perpetually
//!           re-calibrates its brick size every 30 min from the vol file
//!           (`bar_construction/renko.rs:179-183`).
//!
//! Output: `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>.bars` (prod layout).

use anyhow::{Context, Result};
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::{
    BarAccumulator, ipc::record::IndexRecord, parkinson_sigma,
};
use serde::Deserialize;
use series_factory::{
    bar_construction::{MtfParkinsonCalculator, RenkoConfig, RenkoGenerator, VolConfig},
    vol_bin::{VolMmap, VolWriter},
};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
#[command(about = "Build renko .bars from a composite .idx.")]
struct Args {
    /// Path to nxrates.yml (reads `series.{renko,vol,calibration,pipeline}`).
    config: PathBuf,
    /// Base asset symbol (e.g. BTC).
    base: String,
    /// Quote asset symbol (e.g. USDT).
    quote: String,
    /// Override the input composite `.idx` path.
    /// Default: `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>.idx`.
    #[arg(long)]
    idx: Option<PathBuf>,
    /// Override the output `.bars` path.
    /// Default: `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>.bars`.
    #[arg(long)]
    out: Option<PathBuf>,
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
    let idx_path = args.idx.unwrap_or_else(|| {
        PathBuf::from(&cfg.indexes_dir)
            .join("composite")
            .join(format!(
                "{}-{}.idx",
                args.base.to_uppercase(),
                args.quote.to_uppercase()
            ))
    });
    let out_path = args.out.unwrap_or_else(|| {
        PathBuf::from(&cfg.bars_dir)
            .join(args.base.to_uppercase())
            .join(format!(
                "{}{}.bars",
                args.base.to_uppercase(),
                args.quote.to_uppercase()
            ))
    });

    info!(idx = %idx_path.display(), out = %out_path.display(), "renko-from-idx starting");

    // ═══ PASS 1: 30-min HLC from composite mid ═══
    let t0 = std::time::Instant::now();
    let mut hlc: HashMap<i64, (f64, f64, f64)> = HashMap::new();
    let mut stream = IdxStream::open(&idx_path)?;
    let mut pass1_count: u64 = 0;
    while let Some(rec) = stream.next_record()? {
        let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
        let mid = (rec.index.bid + rec.index.ask) * 0.5;
        if !(mid.is_finite() && mid > 0.0) {
            continue;
        }
        let key = (ts / 1_800_000) * 1_800_000;
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
    info!(
        pass1_records = pass1_count,
        buckets = hlc.len(),
        "pass 1: 30-min HLC built in {}ms",
        t0.elapsed().as_millis()
    );

    // ═══ Build vol (.vol) file ═══
    let vol_path = out_path.with_extension("vol");
    if let Some(parent) = vol_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut hours: Vec<(i64, f64, f64)> =
        hlc.into_iter().map(|(ts, (h, l, _))| (ts, h, l)).collect();
    hours.sort_unstable_by_key(|&(ts, _, _)| ts);
    let ema_period = yml.vol.ema_period;
    let alpha = 2.0 / (ema_period as f64 + 1.0);
    let mut vol_writer = VolWriter::new(&vol_path)?;
    let mut prev_ema: Option<f64> = None;
    for (i, &(ts, high, low)) in hours.iter().enumerate() {
        let sigma = parkinson_sigma(high, low);
        let ema = if i < ema_period {
            hours[..=i]
                .iter()
                .map(|&(_, h, l)| parkinson_sigma(h, l))
                .sum::<f64>()
                / (i + 1) as f64
        } else {
            alpha * sigma + (1.0 - alpha) * prev_ema.unwrap_or(sigma)
        };
        prev_ema = Some(ema);
        vol_writer.write_record(timestamp::from_epoch_ms(ts), ema)?;
    }
    vol_writer.finish()?;
    let vol_mmap = VolMmap::open(&vol_path)?;
    info!(vol_records = hours.len(), "vol file written");

    // ═══ PASS 2: feed composite mid → RenkoGenerator ═══
    // RenkoGenerator already recomputes brick_size every 30 min from the
    // VolMmap (renko.rs:179-183) — the prod "perpetual re-calibration".
    // We also overlay micro-structure (realized_var, drift, ...) via
    // BarAccumulator fed from the composite's bid/ask/vbid/vask.
    let first_ts = hours.first().map(|&(ts, _, _)| ts).unwrap_or(0);
    let bootstrap_end = first_ts + yml.pipeline.bootstrap_days * 86_400_000;

    let renko_config = RenkoConfig {
        multiplier: 0.075,
        min_pct: yml.renko.min_pct,
        max_pct: yml.renko.max_pct,
    };
    renko_config.validate()?;

    // Precompute sigma cache so the generator does not re-read the vol
    // mmap on every tick (prod-identical: see `set_sigma_cache`).
    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, yml.vol.clone());
        calc.precompute_sigma_cache()
    };

    let t1 = std::time::Instant::now();
    let mut generator = RenkoGenerator::new(renko_config, &vol_mmap, yml.vol.clone())?;
    generator.set_sigma_cache(&sigma_cache);

    let mut bars: Vec<Bar> = Vec::new();
    let mut accum = BarAccumulator::new();
    let mut pending: Vec<Bar> = Vec::new();
    let mut stream = IdxStream::open(&idx_path)?;
    let mut pass2_count: u64 = 0;
    let mut post_bootstrap: u64 = 0;

    while let Some(rec) = stream.next_record()? {
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

        // Composite Index already carries aggregated (bid, ask, vbid, vask,
        // rejected, accepted). We feed those directly into BarAccumulator
        // so enrichment reflects cross-provider consensus, not one tick's
        // noise.
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
            // Multi-brick batch fix: when one breakout tick produces N bricks,
            // ALL N bricks share the same microstructure window
            // [prev_emission_ts, current_ts]. Copy enrichment to every bar in
            // the batch instead of zeroing bars 1..N (which would teach ML
            // models a spurious inverse-velocity signal).
            let enrich = accum.flush();
            let n_pending = pending.len() as u32;
            for mut bar in pending.drain(..) {
                bar.kind = BarKind::Renko as u8;
                if let Some(ref e) = enrich {
                    bar.vbid = e.vbid;
                    bar.vask = e.vask;
                    // tick_count and additive volume fields are split evenly
                    // so per-bar sums still aggregate to the true window total.
                    bar.tick_count = if n_pending > 0 { e.tick_count / n_pending } else { 0 };
                    // Variance/return/drift/spread/ci/reject are intensive
                    // properties of the window, not extensive — copy as-is.
                    bar.realized_var = e.realized_var;
                    bar.bipower_var = e.bipower_var;
                    bar.drift = e.drift;
                    bar.vol_imbalance = e.vol_imbalance;
                    bar.avg_spread_bps = e.avg_spread_bps;
                    bar.max_abs_return = e.max_abs_return;
                    bar.avg_ci_ubp = e.avg_ci_ubp;
                    bar.reject_rate = e.reject_rate;
                }
                bars.push(bar);
            }
            if bars.len() > yml.pipeline.max_bars {
                anyhow::bail!("bar count exceeds {} safety limit", yml.pipeline.max_bars);
            }
        }
    }

    info!(
        bars = bars.len(),
        pass2_records = pass2_count,
        post_bootstrap,
        "pass 2 done in {}ms",
        t1.elapsed().as_millis()
    );

    // ═══ WRITE OUTPUT ═══
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    nxr_sdk::ipc::write_atomic::<Bar>(&out_path, &bars)?;
    let _ = fs::remove_file(&vol_path);
    info!(out = %out_path.display(), "wrote {} renko bars", bars.len());
    Ok(())
}

/// Buffered streaming reader for an `AppendLog<IndexRecord>` file.
struct IdxStream {
    file: File,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
    eof: bool,
}

impl IdxStream {
    fn open(path: &std::path::Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            file,
            buf: vec![0u8; 4096 * core::mem::size_of::<IndexRecord>()],
            pos: 0,
            filled: 0,
            eof: false,
        })
    }

    fn refill(&mut self) -> Result<()> {
        if self.eof {
            return Ok(());
        }
        self.filled = 0;
        self.pos = 0;
        while self.filled < self.buf.len() {
            match self.file.read(&mut self.buf[self.filled..])? {
                0 => {
                    self.eof = true;
                    break;
                }
                n => self.filled += n,
            }
        }
        if self.filled % core::mem::size_of::<IndexRecord>() != 0 {
            anyhow::bail!(
                "idx stream short read {} not aligned to IndexRecord {}",
                self.filled,
                core::mem::size_of::<IndexRecord>()
            );
        }
        Ok(())
    }

    fn next_record(&mut self) -> Result<Option<IndexRecord>> {
        let rec_size = core::mem::size_of::<IndexRecord>();
        // Refill on exhaustion. Re-check pos against filled afterwards so a
        // refill that hits EOF (filled stays at the prior full buffer because
        // `refill` short-circuits when `eof` is already set) still returns
        // None instead of re-reading stale bytes past the file end.
        if self.pos >= self.filled {
            self.refill()?;
            if self.pos >= self.filled {
                return Ok(None);
            }
        }
        let slice = &self.buf[self.pos..self.pos + rec_size];
        let rec: IndexRecord = *bytemuck::from_bytes(slice);
        self.pos += rec_size;
        Ok(Some(rec))
    }
}
