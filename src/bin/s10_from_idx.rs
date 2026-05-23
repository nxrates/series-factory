//! Generate uniform 10-second OHLC bars from a sharded cross-provider
//! composite `.idx` directory, emitting daily-sharded `.s10` files (96B
//! `mitch::Bar`, `kind = BarKind::Kline`) with FULL microstructure section.
//!
//! Streaming, single-pass: shards iterated in chronological order, each
//! `IndexRecord` fed to `nxr_sdk::BarAccumulator` keyed by 10s wall-clock
//! bucket. On bucket rollover, the previous accumulator is flushed → routed
//! to the daily output shard keyed by `open_ts.date_utc()`.
//!
//! Inputs:  `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>/<YYYY-MM-DD>.idx`
//! Output:  `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>/<YYYY-MM-DD>.s10`
//!         + `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>/manifest.json` (kind merged)
//!
//! Higher-TF series (1m, 5m, 1h, ...) are produced on the fly by API
//! readers via the OHLC monoid rollup (`nxr_sdk::ohlc::rollup`).

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::{BarAccumulator, ipc::record::IndexRecord, resolve_ticker_id};
use series_factory::sharding::{
    bars_dir_pair, composite_dir, list_shards, manifest_path, read_manifest, sha256_file,
    shard_path, ts_ms_to_utc_date, write_manifest, write_shard_atomic, Manifest, ShardEntry,
};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tracing::info;

#[derive(Parser, Debug)]
#[command(about = "Build sharded 10s OHLC .s10 from a sharded composite idx dir.")]
struct Args {
    /// Path to nxrates.yml (reserved for future overrides; not currently read).
    config: PathBuf,
    /// `BASE-QUOTE` pair (e.g. `BTC-USDT`).
    pair: String,
    /// Bucket size in milliseconds. Default 10000 (10 s).
    #[arg(long, default_value_t = 10_000)]
    bucket_ms: i64,
    /// Override the input composite shard dir.
    /// Default: `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>/`.
    #[arg(long = "in-dir")]
    input_dir: Option<PathBuf>,
    /// Override the output shard dir.
    /// Default: `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>/`.
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,
}

fn split_pair(s: &str) -> Result<(String, String)> {
    let mut it = s.splitn(2, '-');
    let base = it
        .next()
        .ok_or_else(|| anyhow::anyhow!("bad pair {}: missing base", s))?
        .to_uppercase();
    let quote = it
        .next()
        .ok_or_else(|| anyhow::anyhow!("bad pair {}: missing quote", s))?
        .to_uppercase();
    if base.is_empty() || quote.is_empty() {
        anyhow::bail!("bad pair {}: empty base or quote", s);
    }
    Ok((base, quote))
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let (base, quote) = split_pair(&args.pair)?;
    if !args.config.exists() {
        anyhow::bail!("config not found: {}", args.config.display());
    }
    if args.bucket_ms <= 0 {
        anyhow::bail!("--bucket-ms must be positive (got {})", args.bucket_ms);
    }

    let cfg = nxr_sdk::NxrConfig::from_env();
    // cfg.indexes_dir = `<root>/indexes`; cfg.bars_dir = `<root>/bars`.
    // sharding helpers expect data root → use parent of those.
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

    info!(
        in_dir = %in_dir.display(),
        out_dir = %out_dir.display(),
        bucket_ms = args.bucket_ms,
        "s10-from-idx starting (sharded)"
    );

    let shards = list_shards(&in_dir, "idx")?;
    if shards.is_empty() {
        anyhow::bail!("no input shards in {}", in_dir.display());
    }
    info!(input_shards = shards.len(), "input shard scan done");

    let t0 = std::time::Instant::now();
    let mut accum = BarAccumulator::new();
    let mut bars_by_date: BTreeMap<NaiveDate, Vec<Bar>> = BTreeMap::new();
    let mut cur_bucket: Option<i64> = None;
    let mut n_input: u64 = 0;
    let mut n_skipped: u64 = 0;

    let flush_bar = |bars_by_date: &mut BTreeMap<NaiveDate, Vec<Bar>>, bar: Bar| {
        // route by open_ts.utc_date()
        let date = ts_ms_to_utc_date(bar.open_time_ms());
        bars_by_date.entry(date).or_default().push(bar);
    };

    for (date, path) in &shards {
        info!(date = %date, path = %path.display(), "reading input shard");
        let mut stream = IdxStream::open(path)?;
        while let Some(rec) = stream.next_record()? {
            let ts_ms = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let idx = rec.index;
            let bid = idx.bid;
            let ask = idx.ask;
            let mid = (bid + ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                n_skipped += 1;
                continue;
            }
            n_input += 1;

            let bucket = ts_ms.div_euclid(args.bucket_ms) * args.bucket_ms;
            match cur_bucket {
                None => {
                    cur_bucket = Some(bucket);
                }
                Some(cb) if bucket > cb => {
                    if let Some(mut bar) = accum.flush() {
                        bar.kind = BarKind::Kline as u8;
                        flush_bar(&mut bars_by_date, bar);
                    }
                    cur_bucket = Some(bucket);
                }
                Some(cb) if bucket < cb => {
                    n_skipped += 1;
                    continue;
                }
                _ => {}
            }

            let ci_ubp = nxr_sdk::tdwap::decode_ci_ubp(idx.ci);
            accum.ingest(
                bid,
                ask,
                idx.vbid,
                idx.vask,
                ts_ms,
                ci_ubp,
                idx.accepted as u32,
                idx.rejected as u32,
            );
        }
    }
    // flush final open bucket
    if let Some(mut bar) = accum.flush() {
        bar.kind = BarKind::Kline as u8;
        flush_bar(&mut bars_by_date, bar);
    }

    let total_bars: usize = bars_by_date.values().map(|v| v.len()).sum();
    info!(
        input_records = n_input,
        skipped = n_skipped,
        bars = total_bars,
        out_shards = bars_by_date.len(),
        "s10 pass done in {}ms",
        t0.elapsed().as_millis()
    );

    // write daily shards atomically
    std::fs::create_dir_all(&out_dir)?;
    for (date, bars) in &bars_by_date {
        let path = shard_path(&out_dir, *date, "s10");
        let bytes: &[u8] = bytemuck::cast_slice(bars);
        write_shard_atomic(&path, bytes)?;
        info!(date = %date, n = bars.len(), path = %path.display(), "wrote s10 shard");
    }

    // update manifest (merge w/ any existing renko/bars entries)
    let ticker_str = format!("{}-{}", base, quote);
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let mpath = manifest_path(&out_dir);
    let mut manifest = read_manifest(&mpath)?
        .unwrap_or_else(|| Manifest::new(ticker_str.clone(), ticker_id, "s10"));
    // ticker fields = canonical re-stamp
    manifest.ticker = ticker_str;
    manifest.ticker_id = ticker_id;
    // ! overwrite kind: bars dir hosts multiple kinds (s10 + renko/bars). We
    // track per-shard kind via extension on disk; manifest.kind = mixed
    // marker.
    if manifest.kind.is_empty() {
        manifest.kind = "s10".into();
    } else if !manifest.kind.contains("s10") {
        manifest.kind = format!("{},s10", manifest.kind);
    }
    // re-scan ALL s10 shards (dir may contain shards from previous runs)
    let existing = list_shards(&out_dir, "s10")?;
    // drop stale s10 entries from manifest; rebuild from disk
    manifest.shards.retain(|s| {
        // keep non-s10 marker shards? manifest stores all in one Vec; we tag
        // by date string only. Until split is needed, rebuild s10 entries
        // and merge w/ existing-date entries from other kinds.
        // Simplest: keep all that ! match existing s10 dates; we'll re-add.
        !existing.iter().any(|(d, _)| d.format("%Y-%m-%d").to_string() == s.date)
    });
    for (date, path) in existing {
        let entry = build_shard_entry_bar(date, &path)?;
        manifest.upsert(entry);
    }
    write_manifest(&mpath, &manifest)?;

    info!(out_dir = %out_dir.display(), manifest = %mpath.display(), "manifest updated");
    Ok(())
}

/// Build a manifest entry for a Bar-shard (.s10/.renko/.bars). Reads bar
/// records to extract first/last open_ts + count.
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
        // seek to last record
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
