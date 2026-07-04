//! Convert raw per-exchange `.ticks` files into a single per-provider `.idx`
//! AppendLog of 56-byte `IndexRecord` entries (200 ms / 5 Hz aggregated by
//! default — the production `network.aggregation_interval_ms` cadence).
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
    /// cadence (200 ms = 5 Hz, `network.aggregation_interval_ms`). MUST match
    /// live; a finer value over-samples intra-cycle and skews renko bpd.
    #[arg(long, default_value = "200")]
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
    /// Delete each raw `.ticks` file the instant it has been fully folded into
    /// the `.idx` AppendLog. This bounds the raw footprint to ~1 file per
    /// exchange in flight (true delete-as-you-go) instead of letting a whole
    /// fetch window of `.ticks` accumulate until a separate cleanup pass.
    /// Off by default so plain re-runs / forensic inspection keep the raw;
    /// the backfill orchestrator sets it for production disk-bounding.
    #[arg(long)]
    delete_after_convert: bool,
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
    // Backfill applies the SAME hard exclusion as live aggregation (sdk single
    // source) — regenerated history must be 100% distribution-compatible with
    // the live feed: an excluded venue must not exist in EITHER. RCA 2026-07-04.
    anyhow::ensure!(
        !nxr_sdk::providers::is_excluded_provider(provider_id),
        "exchange '{}' (provider {}) is HARD-EXCLUDED from index construction \
         (nxr_sdk::providers::EXCLUDED_PROVIDERS — fabricated L1 sizes)",
        args.exchange, provider_id
    );
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
    // OFFLINE builder: buffered AppendLog (256 KiB BufWriter) coalesces the
    // per-record 56B writes. Safe here ∵ nothing tails this shard until the
    // build finishes (unlike the live aggregator path which MUST stay
    // unbuffered for forwarder reader-visibility).
    let mut log: AppendLog<IndexRecord> = AppendLog::open_buffered(&out_path)
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
        let folded = match stream_file_into_acc(
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
            Ok(()) => true,
            Err(e) => {
                warn!(file = %path.display(), err = %e, "stream failed; continuing");
                false
            }
        };
        // Delete-as-you-go: drop this raw `.ticks` the instant it is folded so
        // peak raw on disk never exceeds ~1 file per exchange. `flush_idx`
        // fdatasyncs the AppendLog FIRST so every record from this file is
        // durably on disk before its source is removed — a crash thereafter
        // leaves the `.idx` ⊇ this file's records, and the orchestrator's
        // fresh-build path (stale `.idx` removed → window re-fetched) keeps
        // resume duplicate-free. Only delete on a clean fold; a failed stream
        // keeps the raw for retry.
        maybe_delete_folded_tick(args.delete_after_convert, folded, path, &mut |p| {
            log.flush().with_context(|| format!("pre-delete idx flush {}", p.display()))
        });
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
    let rec_size = core::mem::size_of::<TickFrame>();
    let file = File::open(path)?;
    let len = file.metadata()?.len() as usize;
    if len == 0 {
        return Ok(());
    }
    // mmap + zero-copy cast (mirror generate_renko_from_ticks::mmap_tick_file).
    // Replaces the old 480 KiB chunked-read loop: the kernel pages the file in
    // on demand, so resident memory stays O(1) without an explicit batch buffer.
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    unsafe {
        libc::madvise(mmap.as_ptr() as *mut _, mmap.len(), libc::MADV_SEQUENTIAL);
    }
    // Torn-tail guard: keep ONLY whole TickFrames. A monthly file killed
    // mid-write can end with a partial frame (1..rec_size bytes); discard it
    // rather than mis-cast. Same effect as the prior `len % rec_size` bail,
    // but tolerant instead of fatal (one record lost, never corrupt).
    let n_frames = len / rec_size;
    if n_frames == 0 {
        return Ok(());
    }
    let frames: &[TickFrame] = bytemuck::cast_slice(&mmap[..n_frames * rec_size]);
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
    Ok(())
}

/// Delete-as-you-go gate for one folded `.ticks` file.
///
/// When `enabled` && `folded`, durably flush the `.idx` via `flush_idx` (so the
/// file's records survive a crash before its source disappears) and then remove
/// the raw file. A flush error keeps the raw (logged); a remove error is logged
/// but non-fatal (defensive month sweep / startup sweep will catch it). Returns
/// `true` iff the file was actually removed — exposed for unit testing the
/// "raw count shrinks as files are folded" invariant without standing up a real
/// AppendLog.
fn maybe_delete_folded_tick(
    enabled: bool,
    folded: bool,
    path: &Path,
    flush_idx: &mut dyn FnMut(&Path) -> Result<()>,
) -> bool {
    if !(enabled && folded) {
        return false;
    }
    if let Err(e) = flush_idx(path) {
        warn!(file = %path.display(), err = %e, "pre-delete idx flush failed; keeping raw");
        return false;
    }
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(e) => {
            warn!(file = %path.display(), err = %e, "delete-after-convert remove failed");
            false
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn touch(dir: &Path, name: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, b"\0\0\0\0").unwrap();
        p
    }

    fn count_ticks(dir: &Path) -> usize {
        discover_sorted_tick_files(dir).unwrap().len()
    }

    /// Delete-as-you-go: simulate t2i folding a multi-file window. After each
    /// file is folded the raw count must drop by 1, so at no point do more than
    /// (n - i) raw files coexist — the peak per (sym,exchange) is ~1 file in
    /// flight, never the whole window. This is the disk-bound invariant the
    /// orchestrator relies on (replaces the prior monthly batch-delete).
    #[test]
    fn delete_after_convert_shrinks_raw_per_file() {
        let tmp = std::env::temp_dir().join(format!("t2i_dag_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // a fetch window landed 5 daily `.ticks` for one exchange
        let files: Vec<PathBuf> = (1..=5)
            .map(|d| touch(&tmp, &format!("2024-01-{:02}.ticks", d)))
            .collect();
        assert_eq!(count_ticks(&tmp), 5);

        // fold + delete each in turn; flush stub always succeeds (records durable)
        let flushes = AtomicUsize::new(0);
        let mut flush = |_p: &Path| -> Result<()> {
            flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        };
        for (i, f) in files.iter().enumerate() {
            let removed = maybe_delete_folded_tick(true, true, f, &mut flush);
            assert!(removed, "folded file must be deleted as we go");
            // INVARIANT: remaining raw == files not yet folded (never grows).
            let remaining = count_ticks(&tmp);
            assert_eq!(remaining, files.len() - (i + 1));
            assert!(remaining <= files.len() - (i + 1));
        }
        assert_eq!(count_ticks(&tmp), 0, "all raw purged once window folded");
        // every delete was preceded by exactly one durability flush
        assert_eq!(flushes.load(Ordering::Relaxed), files.len());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Escape hatch: with `--delete-after-convert` OFF (i.e. `--keep-staging`
    /// at the orchestrator) every raw file is retained for forensic re-runs.
    #[test]
    fn keep_staging_retains_all_raw() {
        let tmp = std::env::temp_dir().join(format!("t2i_keep_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let f = touch(&tmp, "2024-02-01.ticks");
        let mut flush = |_p: &Path| -> Result<()> { Ok(()) };
        // enabled=false → no delete even though folded=true
        let removed = maybe_delete_folded_tick(false, true, &f, &mut flush);
        assert!(!removed);
        assert_eq!(count_ticks(&tmp), 1, "raw kept when delete-as-you-go off");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// A file whose fold errored (folded=false) is NOT deleted — it stays for
    /// retry / the defensive month sweep, never silently lost.
    #[test]
    fn failed_fold_keeps_raw_for_retry() {
        let tmp = std::env::temp_dir().join(format!("t2i_fail_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let f = touch(&tmp, "2024-03-01.ticks");
        let mut flush = |_p: &Path| -> Result<()> { Ok(()) };
        let removed = maybe_delete_folded_tick(true, false, &f, &mut flush);
        assert!(!removed);
        assert_eq!(count_ticks(&tmp), 1, "unfolded raw retained");
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
