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

/// Grid-stamp a flushed s10 bar's open/close timestamps to the bucket boundary,
/// mirroring `core/src/bars_s10.rs::stamp_s10_bucket` so the offline series is
/// byte-identical (10s-grid-aligned) to the live producer. `BarAccumulator`
/// stamps the raw first/last-tick ts; without this re-stamp the on-disk
/// `close_ts` deltas jitter off the bucket grid, tripping the integrity-check
/// s10 invariant ("spacing not a multiple of bucket"). This was the offline-only
/// defect behind every `s10-from-idx`-generated (migration / backfill) shard;
/// live-written shards were always clean because the producer already grid-stamps.
fn stamp_grid(bar: &mut Bar, bucket_open: i64, bucket_ms: i64) {
    // Delegate to the CANONICAL shared stamp so offline == live by construction
    // (both call `nxr_sdk::bar_builder::stamp_s10_grid`).
    nxr_sdk::bar_builder::stamp_s10_grid(bar, bucket_open, bucket_ms);
}

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
            // SEAM PARITY: skip heartbeat sentinels (mirror live bars_s10.rs:198).
            // Sentinels carry stale bid/ask; ingesting offline (but not live)
            // poisons the s10 OHLC → vol-ring σ → hist↔live seam drift.
            if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 {
                n_skipped += 1;
                continue;
            }
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
                        // Grid-snap to the closed bucket `cb` (offline == live).
                        stamp_grid(&mut bar, cb, args.bucket_ms);
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
        if let Some(cb) = cur_bucket {
            stamp_grid(&mut bar, cb, args.bucket_ms);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// LIVE-path stamper, expressed by calling the SAME canonical symbol the
    /// live producer now delegates to. Post-Task-3 the live
    /// `core/src/bars_s10.rs::stamp_s10_bucket` is a thin wrapper over
    /// `nxr_sdk::bar_builder::stamp_s10_grid(bar, bucket_open, BAR_MS)`; calling
    /// that real symbol here (instead of a hand-copied body) means any future
    /// change to the live grid stamp recompiles + re-exercises this test
    /// through the actual production code path. The offline `stamp_grid` ALSO
    /// delegates to the same symbol, so SEAM-4 now proves the offline binary
    /// and the live producer share one stamp implementation by construction.
    fn live_stamp_s10_bucket(bar: &mut Bar, bucket_open: i64) {
        const BAR_MS: i64 = 10_000;
        nxr_sdk::bar_builder::stamp_s10_grid(bar, bucket_open, BAR_MS);
    }

    fn blank_bar() -> Bar {
        let z = timestamp::from_epoch_ms(0);
        Bar::new_ohlcv(z, z, 1.0, 1.0, 1.0, 1.0, 0, 0, 0)
    }

    /// SEAM-4: the offline `stamp_grid` (s10-from-idx) and the live
    /// `stamp_s10_bucket` MUST produce BYTE-IDENTICAL `open_ts`/`close_ts` for
    /// every 10s bucket boundary. Deterministic sweep, no rng. This permanently
    /// guards the grid-stamp bug just fixed: a post-write integrity check ran
    /// too late; this is a FORMAT-PARITY assertion at the stamping function.
    #[test]
    fn seam4_s10_grid_stamp_parity_offline_vs_live() {
        const BAR_MS: i64 = 10_000;
        // Sweep a wide, deterministic range of 10s-aligned bucket opens:
        //   * an epoch-near band (DST/leap-second-irrelevant UTC ms),
        //   * a present-day band (~2023-2024 epoch),
        //   * a far-future band (u48 tick headroom check).
        // Plus a dense contiguous run to exercise day-boundary adjacency.
        // All bands are post-2010 (the mitch epoch); pre-epoch inputs saturate
        // to 0 in `from_epoch_ms`, which is irrelevant to live↔offline parity.
        let bands: [i64; 3] = [
            1_300_000_000_000,       // ~2011-03 (just past the mitch epoch)
            1_700_000_000_000,       // ~2023-11
            4_100_000_000_000,       // ~2099
        ];
        let mut checked = 0u64;
        for &base in &bands {
            // 5000 consecutive buckets per band (spans > 13h — crosses no DST
            // because these are raw UTC ms, but does cross the within-day grid
            // and, for the dense run, day boundaries).
            let base = (base / BAR_MS) * BAR_MS; // ensure 10s-aligned
            for i in 0..5_000i64 {
                let bucket_open = base + i * BAR_MS;

                let mut off = blank_bar();
                stamp_grid(&mut off, bucket_open, BAR_MS);

                let mut live = blank_bar();
                live_stamp_s10_bucket(&mut live, bucket_open);

                assert_eq!(
                    off.open_ts, live.open_ts,
                    "SEAM-4 open_ts mismatch @ bucket_open={bucket_open}: offline={:?} live={:?}",
                    off.open_ts, live.open_ts
                );
                assert_eq!(
                    off.close_ts, live.close_ts,
                    "SEAM-4 close_ts mismatch @ bucket_open={bucket_open}: offline={:?} live={:?}",
                    off.close_ts, live.close_ts
                );
                // Decoded epoch_ms must land on the bucket grid within the 16µs
                // tick quantization (≤1 ms) — guards that the encode itself is
                // grid-aligned, not just that the two stampers agree.
                assert!(
                    (off.open_time_ms() - bucket_open).abs() <= 1,
                    "offline open_ts decode {} off grid bucket_open {bucket_open}",
                    off.open_time_ms()
                );
                assert!(
                    (off.close_time_ms() - (bucket_open + BAR_MS - 1)).abs() <= 1,
                    "offline close_ts decode {} off grid bucket_close {}",
                    off.close_time_ms(),
                    bucket_open + BAR_MS - 1
                );
                checked += 1;
            }
        }
        assert!(checked >= 15_000, "SEAM-4 must sweep ≥15k boundaries, got {checked}");
    }
}
