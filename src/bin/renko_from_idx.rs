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

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::{BarAccumulator, ipc::record::IndexRecord, parkinson_sigma, resolve_ticker_id};
use serde::Deserialize;
use series_factory::sharding::{
    bars_dir_pair, composite_dir, list_shards, manifest_path, read_manifest, sha256_file,
    shard_path, ts_ms_to_utc_date, write_manifest, write_shard_atomic, Manifest, ShardEntry,
};
use series_factory::{
    bar_construction::{MtfParkinsonCalculator, RenkoConfig, RenkoGenerator, VolConfig},
    vol_bin::{VolMmap, VolWriter},
};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::Read;
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
        let mut stream = IdxStream::open(path)?;
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
    let first_ts = hours.first().map(|&(ts, _, _)| ts).unwrap_or(0);
    let bootstrap_end = first_ts + yml.pipeline.bootstrap_days * 86_400_000;

    let renko_config = RenkoConfig {
        multiplier: 0.075,
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
        let mut stream = IdxStream::open(path)?;
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
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let mpath = manifest_path(&out_dir);
    let mut manifest = read_manifest(&mpath)?
        .unwrap_or_else(|| Manifest::new(ticker_str.clone(), ticker_id, "renko"));
    manifest.ticker = ticker_str;
    manifest.ticker_id = ticker_id;
    if manifest.kind.is_empty() {
        manifest.kind = "renko".into();
    } else if !manifest.kind.contains("renko") {
        manifest.kind = format!("{},renko", manifest.kind);
    }
    let existing = list_shards(&out_dir, "renko")?;
    manifest.shards.retain(|s| {
        !existing.iter().any(|(d, _)| d.format("%Y-%m-%d").to_string() == s.date)
    });
    for (date, path) in existing {
        manifest.upsert(build_shard_entry_bar(date, &path)?);
    }
    write_manifest(&mpath, &manifest)?;

    info!(out_dir = %out_dir.display(), manifest = %mpath.display(), "manifest updated");
    Ok(())
}

/// Manifest entry for a 96B Bar shard.
fn build_shard_entry_bar(date: NaiveDate, path: &Path) -> Result<ShardEntry> {
    let rec_size = core::mem::size_of::<Bar>();
    let size_bytes = std::fs::metadata(path)?.len();
    let n_records = if rec_size == 0 { 0 } else { size_bytes / rec_size as u64 };
    let mut first_ts: i64 = 0;
    let mut last_ts: i64 = 0;
    if n_records > 0 {
        use std::io::Seek;
        let mut f = std::fs::File::open(path)?;
        let mut head = vec![0u8; rec_size];
        f.read_exact(&mut head)?;
        let bar0: Bar = *bytemuck::from_bytes(&head);
        first_ts = bar0.open_time_ms();
        f.seek(std::io::SeekFrom::End(-(rec_size as i64)))?;
        let mut tail = vec![0u8; rec_size];
        f.read_exact(&mut tail)?;
        let bar_n: Bar = *bytemuck::from_bytes(&tail);
        last_ts = bar_n.open_time_ms();
    }
    Ok(ShardEntry {
        date: date.format("%Y-%m-%d").to_string(),
        first_ts,
        last_ts,
        n_records,
        size_bytes,
        sha256: sha256_file(path)?,
    })
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
