//! Generate uniform 10-second OHLC bars from a sharded cross-provider
//! composite `.idx` directory, emitting daily-sharded `.s10` files (96B
//! `mitch::Bar`, `kind = BarKind::Kline`) with FULL microstructure section.
//!
//! Streaming, single-pass: shards iterated in chronological order, each
//! `IndexRecord` fed to `nxr_sdk::BarAccumulator` keyed by 10s wall-clock
//! bucket. On bucket rollover, the previous accumulator is flushed → routed
//! to the daily output shard keyed by `open_ts.date_utc()`.
//!
//! Inputs:  `$NXR_DATA_INDEXES/<MITCH_TICKER_ID>/<YYYY-MM-DD>.idx`
//! Output:  `$NXR_DATA_BARS/<MITCH_TICKER_ID>/<YYYY-MM-DD>.s10`
//!         + `$NXR_DATA_BARS/<MITCH_TICKER_ID>/manifest.json` (kind merged)
//!
//! Higher-TF series (1m, 5m, 1h, ...) are produced on the fly by API
//! readers via the OHLC monoid rollup (`nxr_sdk::ohlc::rollup`).

use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::bar_builder::flat_bar;
use nxr_sdk::shard::ShardStream;
use nxr_sdk::{BarAccumulator, resolve_ticker_id};
use series_factory::sharding::{
    bars_dir, idx_dir, list_shards, manifest_path, read_manifest, shard_path,
    ts_ms_to_utc_date, write_manifest, write_shard_atomic, Manifest,
};
use std::collections::BTreeMap;
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
    /// Default: `$NXR_DATA_INDEXES/<MITCH_ID>/`.
    #[arg(long = "in-dir")]
    input_dir: Option<PathBuf>,
    /// Override the output shard dir.
    /// Default: `$NXR_DATA_BARS/<MITCH_ID>/`.
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let (base, quote) = nxr_sdk::split_pair_multi(&args.pair, &['/', '-'])
        .map(|(b, q)| (b.to_uppercase(), q.to_uppercase()))
        .ok_or_else(|| anyhow::anyhow!("bad pair {}: expected BASE-QUOTE", args.pair))?;
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
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let in_dir = args
        .input_dir
        .clone()
        .unwrap_or_else(|| idx_dir(&data_root_idx, ticker_id));
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| bars_dir(&data_root_bars, ticker_id));

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
    // Last emitted close — seeds `flat_bar` for empty (zero-tick) buckets so the
    // offline series is GAPLESS, byte-identical to the live `bars_s10` producer
    // (core/src/bars_s10.rs flush_all). Without this, quiet windows drop buckets
    // and offline-s10-resampled vol diverges from live on those windows.
    let mut last_close: f64 = 0.0;
    let mut n_input: u64 = 0;
    let mut n_skipped: u64 = 0;
    let mut n_flat: u64 = 0;

    let flush_bar = |bars_by_date: &mut BTreeMap<NaiveDate, Vec<Bar>>, bar: Bar| {
        // route by open_ts.utc_date()
        let date = ts_ms_to_utc_date(bar.open_time_ms());
        bars_by_date.entry(date).or_default().push(bar);
    };

    // Emit flat bars for every empty 10s bucket in `(from, to)` (exclusive both
    // ends), each seeded with `last_close`. Mirrors the live producer, which
    // fires one `flat_bar` per ticker per 10s boundary that received no ticks.
    let fill_gap = |bars_by_date: &mut BTreeMap<NaiveDate, Vec<Bar>>,
                    from: i64,
                    to: i64,
                    last_close: f64,
                    n_flat: &mut u64| {
        if last_close <= 0.0 {
            return;
        }
        let mut b = from + args.bucket_ms;
        while b < to {
            flush_bar(bars_by_date, flat_bar(b, last_close));
            *n_flat += 1;
            b += args.bucket_ms;
        }
    };

    for (date, path) in &shards {
        info!(date = %date, path = %path.display(), "reading input shard");
        let mut stream = ShardStream::<nxr_sdk::IndexRecord>::open(path)?;
        while let Some(rec) = stream.next()? {
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
                        if bar.close > 0.0 && bar.close.is_finite() {
                            last_close = bar.close;
                        }
                        flush_bar(&mut bars_by_date, bar);
                    }
                    // GAPLESS fill: emit a flat bar for each empty bucket strictly
                    // between the just-closed bucket and the new one.
                    fill_gap(&mut bars_by_date, cb, bucket, last_close, &mut n_flat);
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
    // flush final open bucket (no trailing gap-fill: gapless only spans
    // first..last observed, matching live which fills only past boundaries).
    if let Some(mut bar) = accum.flush() {
        bar.kind = BarKind::Kline as u8;
        flush_bar(&mut bars_by_date, bar);
    }

    let total_bars: usize = bars_by_date.values().map(|v| v.len()).sum();
    info!(
        input_records = n_input,
        skipped = n_skipped,
        flat_filled = n_flat,
        bars = total_bars,
        out_shards = bars_by_date.len(),
        "s10 pass done (gapless) in {}ms",
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
    // re-scan ALL s10 shards (dir may contain shards from previous runs); merge
    // `s10` into the comma-joined `kind` field.
    manifest.refresh_kind::<Bar>(&out_dir, "s10")?;
    write_manifest(&mpath, &manifest)?;

    info!(out_dir = %out_dir.display(), manifest = %mpath.display(), "manifest updated");
    Ok(())
}
