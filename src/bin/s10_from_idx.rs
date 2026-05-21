//! Generate uniform 10-second OHLC bars from a single cross-provider
//! composite `.idx` file, emitting the canonical 96B `mitch::Bar`
//! (`kind = BarKind::Kline`) with FULL microstructure section.
//!
//! Streaming, single-pass: each `IndexRecord` is fed to
//! `nxr_sdk::BarAccumulator` keyed by 10s wall-clock bucket. On bucket
//! rollover, the previous bucket's accumulator is flushed to a `Bar`
//! (same math used by `renko-from-idx`'s enrichment overlay → identical
//! microstructure between `.s10` and `.renko` artifacts).
//!
//! Output: `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>.s10` (prod layout).
//!
//! Higher-TF series (1m, 5m, 1h, ...) are produced on the fly by API
//! readers via the OHLC monoid rollup (`nxr_sdk::ohlc::rollup`).

use anyhow::{Context, Result};
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::{BarAccumulator, ipc::record::IndexRecord};
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
#[command(about = "Build uniform 10s OHLC .s10 from a composite .idx.")]
struct Args {
    /// Path to nxrates.yml (reserved for future overrides; not currently read).
    config: PathBuf,
    /// `BASE-QUOTE` pair (e.g. `BTC-USDT`).
    pair: String,
    /// Bucket size in milliseconds. Default 10000 (10 s).
    #[arg(long, default_value_t = 10_000)]
    bucket_ms: i64,
    /// Override the input composite `.idx` path.
    /// Default: `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>.idx`.
    #[arg(long = "in")]
    input: Option<PathBuf>,
    /// Override the output `.s10` path.
    /// Default: `$NXR_DATA_BARS/<BASE>/<BASE><QUOTE>.s10`.
    #[arg(long)]
    out: Option<PathBuf>,
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
    // Touch config path so a typo fails early (mirrors renko-from-idx).
    if !args.config.exists() {
        anyhow::bail!("config not found: {}", args.config.display());
    }

    if args.bucket_ms <= 0 {
        anyhow::bail!("--bucket-ms must be positive (got {})", args.bucket_ms);
    }

    let cfg = nxr_sdk::NxrConfig::from_env();
    let idx_path = args.input.unwrap_or_else(|| {
        PathBuf::from(&cfg.indexes_dir)
            .join("composite")
            .join(format!("{}-{}.idx", base, quote))
    });
    let out_path = args.out.unwrap_or_else(|| {
        PathBuf::from(&cfg.bars_dir)
            .join(&base)
            .join(format!("{}{}.s10", base, quote))
    });

    info!(
        idx = %idx_path.display(),
        out = %out_path.display(),
        bucket_ms = args.bucket_ms,
        "s10-from-idx starting"
    );

    let t0 = std::time::Instant::now();
    let mut stream = IdxStream::open(&idx_path)?;
    let mut accum = BarAccumulator::new();
    let mut bars: Vec<Bar> = Vec::new();
    let mut cur_bucket: Option<i64> = None;
    let mut n_input: u64 = 0;
    let mut n_skipped: u64 = 0;

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
                // Bucket rollover: flush the previous accumulator.
                if let Some(mut bar) = accum.flush() {
                    bar.kind = BarKind::Kline as u8;
                    bars.push(bar);
                }
                cur_bucket = Some(bucket);
            }
            Some(cb) if bucket < cb => {
                // Out-of-order record older than the open bucket; skip
                // (caller is expected to feed monotone .idx).
                n_skipped += 1;
                continue;
            }
            _ => { /* same bucket — fall through */ }
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

    // Flush the final open bucket.
    if let Some(mut bar) = accum.flush() {
        bar.kind = BarKind::Kline as u8;
        bars.push(bar);
    }

    info!(
        input_records = n_input,
        skipped = n_skipped,
        bars = bars.len(),
        "s10 pass done in {}ms",
        t0.elapsed().as_millis()
    );

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    nxr_sdk::ipc::write_atomic::<Bar>(&out_path, &bars)?;
    info!(out = %out_path.display(), "wrote {} s10 bars", bars.len());
    Ok(())
}

/// Buffered streaming reader for an `AppendLog<IndexRecord>` file.
/// Copy of the reader used by `renko_from_idx.rs` (no public API yet).
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
