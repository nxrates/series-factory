//! Convert raw per-exchange `.ticks` files into a single per-provider `.idx`
//! AppendLog of 56-byte `IndexRecord` entries (50 ms aggregated by default).
//!
//! This is the offline equivalent of the prod forwarder's inner loop:
//!   raw tick  →  `TickAccumulator`  →  `Index` per cycle  →  `.idx`
//!
//! Once all four providers have their own `.idx` we can fan them into a
//! single cross-provider composite `.idx` with the `merge-idx` binary, and
//! then the renko step reads that composite.
//!
//! Usage:
//!   ticks-to-idx <EXCHANGE> <BASE> <QUOTE> [--cycle-ms 50] [--z 6.0]
//!
//! Paths come from the standard env:
//!   `$NXR_DATA_TICKS/<exchange>/<BASESYMBOL>/*.ticks`
//!   `$NXR_DATA_INDEXES/<exchange>/<BASE>-<QUOTE>.idx`
//!
//! Where `<BASESYMBOL>` is the exchange-specific directory convention used
//! by the fetcher (binance/bybit/bitget = `BTCUSDT`, okx = `BTCUSDT` too
//! since `dir_name` follows `"{base}{quote}"` in its TickSource impl).

use anyhow::{Context, Result};
use clap::Parser;
use mitch::header::MitchHeader;
use mitch::common::message_type;
use nxr_sdk::{
    agg::{RunningStats, TickAccumulator},
    ipc::append_log::AppendLog,
    ipc::record::IndexRecord,
    resolve_ticker_id,
};
use series_factory::TickFrame;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(about = "Replay raw .ticks into a per-provider .idx AppendLog.")]
struct Args {
    /// Exchange name as used in the raw cache directory (binance / bybit / okx / bitget).
    exchange: String,
    /// Base asset symbol (e.g. BTC).
    base: String,
    /// Quote asset symbol (e.g. USDT).
    quote: String,
    /// Aggregation cycle in milliseconds. Default matches prod forwarder
    /// cadence (50 ms = 20 Hz). Raise it (e.g. 200, 1000) to trade temporal
    /// resolution for smaller `.idx` files on long replays.
    #[arg(long, default_value = "50")]
    cycle_ms: u64,
    /// Z-score outlier gate on the mid price — ticks beyond `z` stddevs from
    /// the local EMA are dropped from the ingest and counted against
    /// `Index.rejected` on flush. Prod crypto default is 6.0.
    #[arg(long, default_value = "6.0")]
    z: f64,
    /// Override the provider id used in the output MITCH header. Normally
    /// the exchange name is resolved via `nxr_sdk::providers`.
    #[arg(long)]
    provider_id: Option<u16>,
    /// Override the output `.idx` path. Default:
    ///   `$NXR_DATA_INDEXES/<exchange>/<BASE>-<QUOTE>.idx`
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let cfg = nxr_sdk::NxrConfig::from_env();

    // --- Provider id ---
    let provider_id = match args.provider_id {
        Some(p) => p,
        None => nxr_sdk::providers::get_market_provider_id_by_name(&args.exchange)
            .with_context(|| format!("unknown exchange: {}", args.exchange))?,
    };
    let ticker_id = resolve_ticker_id(&format!("{}/{}", args.base, args.quote));

    // --- I/O paths ---
    // Cache layout: `<ticks_dir>/<exchange>/<base+quote>/*.ticks`.
    // All four exchanges currently concatenate base+quote for the directory
    // (OKX uses `BTC-USDT` for the filename prefix but still `BTCUSDT` for
    // the dir), which matches `fetch_monthly_daily`'s `dir_name` argument.
    let sym_dir = format!("{}{}", args.base.to_uppercase(), args.quote.to_uppercase());
    let ticks_dir = PathBuf::from(&cfg.ticks_dir)
        .join(&args.exchange)
        .join(&sym_dir);
    let out_path = args.out.unwrap_or_else(|| {
        PathBuf::from(&cfg.indexes_dir)
            .join(&args.exchange)
            .join(format!(
                "{}-{}.idx",
                args.base.to_uppercase(),
                args.quote.to_uppercase()
            ))
    });

    info!(
        exchange = %args.exchange,
        provider_id,
        ticker_id,
        cycle_ms = args.cycle_ms,
        ticks_dir = %ticks_dir.display(),
        out = %out_path.display(),
        "starting ticks-to-idx"
    );

    // --- Discover + sort input files so their timestamps advance monotonically ---
    let files = discover_sorted_tick_files(&ticks_dir)
        .with_context(|| format!("discover tick files in {}", ticks_dir.display()))?;
    if files.is_empty() {
        // Soft-skip: this (sym, exchange) has no archive coverage. The merge
        // step downstream will combine whatever other exchanges DID return
        // data. A hard error here used to fail entire tickers (XMR-USDT,
        // USDe-USDT, USDG-USDT) when 1 of 3 exchanges had no .ticks files.
        tracing::warn!(
            ticks_dir = %ticks_dir.display(),
            exchange = %args.exchange,
            ticker_id,
            "no .ticks files — soft-skipping this (sym,exchange); merge will use other exchanges"
        );
        return Ok(());
    }
    info!(n_files = files.len(), "discovered tick files");

    // --- State ---
    let mut acc = TickAccumulator::new(ticker_id);
    let mut stats = RunningStats::default();
    let mut log: AppendLog<IndexRecord> = AppendLog::open(&out_path)
        .with_context(|| format!("open AppendLog {}", out_path.display()))?;
    let cycle_ms = args.cycle_ms as i64;
    let z_threshold = args.z;

    let mut next_cycle_ms: Option<i64> = None;
    let mut last_tick_ms: i64 = 0;
    let mut ingested: u64 = 0;
    let mut rejected: u64 = 0;
    let mut records_written: u64 = 0;

    // --- Main loop ---
    for (i, path) in files.iter().enumerate() {
        if i % 10 == 0 {
            info!(
                file = %path.display(),
                progress = format!("{}/{}", i + 1, files.len()),
                records_written,
                "processing"
            );
        }
        // Stream the file in BATCH_SIZE-tick chunks so an entire monthly
        // file (up to ~2 GiB of TickFrames) never sits resident at once.
        if let Err(e) = stream_file_into_acc(
            path,
            provider_id,
            &mut acc,
            &mut stats,
            &mut log,
            &mut next_cycle_ms,
            &mut last_tick_ms,
            &mut ingested,
            &mut rejected,
            &mut records_written,
            z_threshold,
            cycle_ms,
        ) {
            warn!(file = %path.display(), err = %e, "stream failed; continuing");
        }
    }

    // Final flush — close out whatever is in the last partial cycle.
    if let Some(index) = acc.flush() {
        let mts = mitch::timestamp::from_epoch_ms(last_tick_ms.max(next_cycle_ms.unwrap_or(0)));
        let header = MitchHeader::new(message_type::INDEX, provider_id, mts, 1);
        log.append(&IndexRecord { header, index })
            .with_context(|| "AppendLog final append failed")?;
        records_written += 1;
    }
    log.flush().context("AppendLog final flush")?;

    info!(
        records_written,
        ingested,
        rejected,
        files = files.len(),
        out = %out_path.display(),
        "ticks-to-idx complete"
    );
    Ok(())
}

/// Stream a single `.ticks` file through the TickAccumulator. Chunked
/// `File::read` into a ~480 KiB buffer keeps resident memory at O(1)
/// regardless of file size (monthly bybit/binance files can exceed 2 GiB).
#[allow(clippy::too_many_arguments)]
fn stream_file_into_acc(
    path: &Path,
    provider_id: u16,
    acc: &mut TickAccumulator,
    stats: &mut RunningStats,
    log: &mut AppendLog<IndexRecord>,
    next_cycle_ms: &mut Option<i64>,
    last_tick_ms: &mut i64,
    ingested: &mut u64,
    rejected: &mut u64,
    records_written: &mut u64,
    z_threshold: f64,
    cycle_ms: i64,
) -> Result<()> {
    const BATCH: usize = 10_000;
    let rec_size = core::mem::size_of::<TickFrame>();
    let mut file = File::open(path)?;
    let len = file.metadata()?.len() as usize;
    if len == 0 {
        return Ok(());
    }
    if len % rec_size != 0 {
        anyhow::bail!(
            "{} size {} is not a multiple of TickFrame ({})",
            path.display(),
            len,
            rec_size
        );
    }
    let mut buf = vec![0u8; BATCH * rec_size];
    loop {
        let mut filled = 0usize;
        while filled < buf.len() {
            match file.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        if filled == 0 {
            break;
        }
        if filled % rec_size != 0 {
            anyhow::bail!("short read {} not aligned to TickFrame {}", filled, rec_size);
        }
        let frames: &[TickFrame] = bytemuck::cast_slice(&buf[..filled]);
        for tf in frames {
            let ts_ms = tf.timestamp_ms();
            let body = tf.body;
            if !(body.bid > 0.0 && body.ask > 0.0 && body.ask >= body.bid) {
                continue;
            }
            if next_cycle_ms.is_none() {
                *next_cycle_ms = Some(ts_ms + cycle_ms);
            }
            while ts_ms >= next_cycle_ms.unwrap() {
                let boundary = next_cycle_ms.unwrap();
                if let Some(index) = acc.flush() {
                    let mts = mitch::timestamp::from_epoch_ms(boundary);
                    let header = MitchHeader::new(message_type::INDEX, provider_id, mts, 1);
                    log.append(&IndexRecord { header, index })?;
                    *records_written += 1;
                }
                *next_cycle_ms = Some(boundary + cycle_ms);
            }
            let mid = (body.bid + body.ask) * 0.5;
            let z = stats.update(mid);
            if z > z_threshold {
                acc.reject();
                *rejected += 1;
                continue;
            }
            acc.ingest(body.bid, body.ask, body.vbid, body.vask);
            *ingested += 1;
            *last_tick_ms = ts_ms;
        }
        if filled < buf.len() {
            break;
        }
    }
    Ok(())
}

/// Collect every `*.ticks` file under `dir` and sort them lexicographically.
/// Our exchange filename conventions (`YYYY-MM`, `YYYY-MM-DD`,
/// `YYYYMMDD_NNN`) all sort in time order as plain strings.
fn discover_sorted_tick_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|s| s.to_str()) == Some("ticks")
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("._"))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    Ok(files)
}
