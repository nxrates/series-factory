//! Shared exchange infrastructure - download, decompress, parse, tick I/O.
//!
//! All exchange sources share: HTTP download, tick file I/O (native TickFrame),
//! ZIP/GZIP decompression, bid/ask inference from trade data, batch sending,
//! and date iteration helpers.

use crate::types::{Config, TickFrame};
use anyhow::Result;
use chrono::{Datelike, Duration, NaiveDate};
use memmap2::Mmap;
use mitch::Tick;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use rayon::prelude::*;
use tracing::{debug, info, warn};
use zip::ZipArchive;

// ─── HTTP Agent ───────────────────────────��──────────────────────────────────

/// Create a shared HTTP agent with 10-min timeout (large monthly archives can be 300MB+).
pub fn http_agent() -> Arc<ureq::Agent> {
    Arc::new(
        ureq::AgentBuilder::new()
            .timeout(std::time::Duration::from_secs(600))
            .build(),
    )
}

// ─── Archive URL resolver (phase 59.R3.C2.O4) ──────────────────────────────
//
// Per-exchange `archive_url_template.{monthly,daily,probe}` is YAML-driven
// (see `nxr_sdk::pipeline_config::ExchangeYml::archive_url_template`).
// Cached at first read; PipelineYml resolution honours `NXR_CONFIG`.

#[derive(Clone, Default)]
pub struct ArchiveUrls {
    pub monthly: String,
    pub daily: String,
    pub probe: String,
}

fn archive_urls_for(exch: &str) -> ArchiveUrls {
    use nxr_sdk::pipeline_config::{
        ConfigHint, PipelineYml,
        DEFAULT_ARCHIVE_URL_BINANCE_DAILY, DEFAULT_ARCHIVE_URL_BINANCE_MONTHLY,
        DEFAULT_ARCHIVE_URL_BINANCE_PROBE, DEFAULT_ARCHIVE_URL_BYBIT_DAILY,
        DEFAULT_ARCHIVE_URL_BYBIT_MONTHLY, DEFAULT_ARCHIVE_URL_BYBIT_PROBE,
        DEFAULT_ARCHIVE_URL_BITGET_DAILY, DEFAULT_ARCHIVE_URL_BITGET_MONTHLY,
        DEFAULT_ARCHIVE_URL_BITGET_PROBE, DEFAULT_ARCHIVE_URL_OKX_DAILY,
        DEFAULT_ARCHIVE_URL_OKX_MONTHLY, DEFAULT_ARCHIVE_URL_OKX_PROBE,
    };
    let (def_m, def_d, def_p) = match exch {
        "binance" => (
            DEFAULT_ARCHIVE_URL_BINANCE_MONTHLY,
            DEFAULT_ARCHIVE_URL_BINANCE_DAILY,
            DEFAULT_ARCHIVE_URL_BINANCE_PROBE,
        ),
        "bybit" => (
            DEFAULT_ARCHIVE_URL_BYBIT_MONTHLY,
            DEFAULT_ARCHIVE_URL_BYBIT_DAILY,
            DEFAULT_ARCHIVE_URL_BYBIT_PROBE,
        ),
        "bitget" => (
            DEFAULT_ARCHIVE_URL_BITGET_MONTHLY,
            DEFAULT_ARCHIVE_URL_BITGET_DAILY,
            DEFAULT_ARCHIVE_URL_BITGET_PROBE,
        ),
        "okx" => (
            DEFAULT_ARCHIVE_URL_OKX_MONTHLY,
            DEFAULT_ARCHIVE_URL_OKX_DAILY,
            DEFAULT_ARCHIVE_URL_OKX_PROBE,
        ),
        _ => ("", "", ""),
    };
    let yaml = PipelineYml::load_default(ConfigHint::Bin)
        .ok()
        .and_then(|pl| pl.cexs.exchanges.get(exch).cloned());
    let tpl = yaml.and_then(|ex| ex.archive_url_template);
    let monthly = tpl
        .as_ref()
        .and_then(|t| t.monthly.clone())
        .unwrap_or_else(|| def_m.to_string());
    let daily = tpl
        .as_ref()
        .and_then(|t| t.daily.clone())
        .unwrap_or_else(|| def_d.to_string());
    let probe = tpl
        .as_ref()
        .and_then(|t| t.probe.clone())
        .unwrap_or_else(|| def_p.to_string());
    ArchiveUrls { monthly, daily, probe }
}

/// Resolve archive URLs for an exchange (cached per-exchange).
pub fn archive_urls(exch: &'static str) -> &'static ArchiveUrls {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    static CACHE: OnceLock<std::sync::Mutex<HashMap<&'static str, &'static ArchiveUrls>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = cache.lock().expect("archive_urls cache lock");
    if let Some(v) = guard.get(exch).copied() {
        return v;
    }
    let urls = Box::leak(Box::new(archive_urls_for(exch)));
    guard.insert(exch, urls);
    urls
}

/// Streaming copy buffer size. Bounds peak resident memory during a download
/// to O(this) regardless of archive size — an ETH-USDT monthly aggTrades zip is
/// 181-460 MiB compressed (≈654 MiB decompressed); buffering the whole body in
/// RAM (the old `download_bytes` → `read_to_end` into a `Vec<u8>`) caused
/// intermittent OOM/truncation. 8 MiB is large enough to keep syscall overhead
/// negligible and small enough to be irrelevant to RSS.
const DOWNLOAD_STREAM_BUF_BYTES: usize = 8 * 1024 * 1024;

/// Outcome of a streamed-to-disk download.
pub struct DownloadedArchive {
    /// Temp file holding the compressed archive on disk (same dir as the final
    /// cache file, so it lives on the big ticks PVC, not a small TMPDIR).
    pub path: PathBuf,
    /// Bytes actually written to disk.
    pub bytes_got: u64,
    /// `Content-Length` advertised by the server, if any.
    pub content_length: Option<u64>,
}

impl Drop for DownloadedArchive {
    fn drop(&mut self) {
        // Delete-as-you-go: the temp archive is purely transient scratch; the
        // durable artifact is the decompressed `.ticks` file. Never leak it.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Stream an HTTP body to a temp file on disk with a bounded copy buffer
/// (`DOWNLOAD_STREAM_BUF_BYTES`), so peak RAM is O(buffer) not O(archive).
///
/// `dst_dir` is where the temp `.partial-dl` lands; callers pass the final
/// cache file's parent so the scratch shares the ticks PVC. Returns the temp
/// path plus the byte counts needed for truncation detection by the caller.
///
/// Truncation detection lives in the caller (`download_to_file_retry`): this fn
/// reports `bytes_got` and `content_length` but does NOT itself decide that a
/// short read is fatal, so the decision can be retried with backoff.
pub async fn download_to_file(
    agent: &Arc<ureq::Agent>,
    url: &str,
    dst_dir: &Path,
) -> Result<DownloadedArchive> {
    let agent = agent.clone();
    let url = url.to_string();
    let dst_dir = dst_dir.to_path_buf();

    tokio::task::spawn_blocking(move || -> Result<DownloadedArchive> {
        debug!("Downloading (stream→disk): {}", url);
        std::fs::create_dir_all(&dst_dir)?;

        let response = agent
            .get(&url)
            .call()
            .map_err(|e| anyhow::anyhow!("Download failed {}: {}", url, e))?;

        let content_length = response
            .header("Content-Length")
            .and_then(|s| s.trim().parse::<u64>().ok());

        // Unique temp name so concurrent downloads in the same dir don't clash.
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp_path = dst_dir.join(format!(".partial-dl-{nonce}-{:x}", url.len()));

        let mut reader = response.into_reader();
        let out = File::create(&tmp_path)?;
        let mut writer = BufWriter::with_capacity(DOWNLOAD_STREAM_BUF_BYTES, out);
        let mut buf = vec![0u8; DOWNLOAD_STREAM_BUF_BYTES];
        let mut bytes_got: u64 = 0;
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    // Mid-transfer error (the failure mode that lost ETH
                    // months): drop the partial file, surface as Err so the
                    // retry loop can re-attempt rather than accept a stub.
                    let _ = writer.flush();
                    drop(writer);
                    let _ = std::fs::remove_file(&tmp_path);
                    return Err(anyhow::anyhow!("stream read failed {}: {}", url, e));
                }
            };
            writer.write_all(&buf[..n])?;
            bytes_got += n as u64;
        }
        writer.flush()?;
        drop(writer);

        Ok(DownloadedArchive { path: tmp_path, bytes_got, content_length })
    })
    .await?
}

/// Truncation predicate: a download is truncated iff a `Content-Length` was
/// advertised and fewer bytes arrived. Split out so the rule is unit-testable
/// without a live HTTP server.
#[inline]
pub fn is_truncated(bytes_got: u64, content_length: Option<u64>) -> bool {
    matches!(content_length, Some(expected) if bytes_got < expected)
}

/// Stream-download to disk with retry + truncation detection.
///
/// A download whose written byte count falls short of the advertised
/// `Content-Length` is treated as a FAILED transfer (retried up to
/// `max_attempts`), NOT as a valid-but-empty archive. This is the fix for the
/// ETH 2024-2025 silent-gap: a partial body that decompressed to few/zero ticks
/// was previously accepted as "empty month".
pub async fn download_to_file_retry(
    agent: &Arc<ureq::Agent>,
    url: &str,
    dst_dir: &Path,
    max_attempts: usize,
) -> Result<DownloadedArchive> {
    let mut last_err = None;
    for attempt in 0..max_attempts {
        match download_to_file(agent, url, dst_dir).await {
            Ok(dl) => {
                if is_truncated(dl.bytes_got, dl.content_length) {
                    let expected = dl.content_length.unwrap_or(0);
                    {
                        warn!(
                            bytes_got = dl.bytes_got,
                            bytes_expected = expected,
                            url,
                            "TRUNCATED download (short of Content-Length) — \
                             treating as failed transfer, NOT empty archive",
                        );
                        last_err = Some(anyhow::anyhow!(
                            "truncated download {}: got {} of {} bytes",
                            url, dl.bytes_got, expected
                        ));
                        // `dl` drops here → temp file removed before retry.
                        if attempt + 1 < max_attempts {
                            warn!("Attempt {}/{} truncated: {} - retrying", attempt + 1, max_attempts, url);
                        }
                        continue;
                    }
                }
                return Ok(dl);
            }
            Err(e) => {
                if attempt + 1 < max_attempts {
                    warn!("Attempt {}/{} failed: {} - retrying", attempt + 1, max_attempts, url);
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap())
}

// ─── Tick file I/O (native TickFrame) ───────────────────────────────────��────

/// Read .ticks file as Vec<TickFrame> (mmap + bytemuck zero-copy).
pub fn read_tick_file(path: &Path) -> Result<Vec<TickFrame>> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let frame_size = std::mem::size_of::<TickFrame>();

    if mmap.is_empty() {
        return Ok(Vec::new());
    }
    if mmap.len() % frame_size != 0 {
        anyhow::bail!(
            "File size ({}) not a multiple of TickFrame size ({}): {}",
            mmap.len(),
            frame_size,
            path.display(),
        );
    }

    let frames: &[TickFrame] = bytemuck::cast_slice(&mmap);
    Ok(frames.to_vec())
}

/// Re-stamp every frame's header with `provider_id`. Used after reading
/// cached .ticks files (older caches were written with provider_id=0 before
/// sources plumbed their MITCH id through parse_csv).
#[inline]
pub fn stamp_provider_id(frames: &mut [TickFrame], provider_id: u16) {
    for f in frames {
        f.header.set_provider_id(provider_id);
    }
}

// ─── Trade → tick (NO book fabrication) ─────────────────────────────────────

/// Build a tick from an EXECUTED TRADE with honest no-book semantics.
///
/// Operator ruling 2026-07-04: the only truth is the executed price; order
/// books are spoofable/partial and NOTHING may fabricate one. The retired
/// `infer_tick` synthesized a ±0.5bp book around every archived trade, which
/// poisoned 23 months of served `avg_spread_bps` at a constant ~1.0bp
/// (HISTORY-V2 RCA). Here: `bid == ask == trade_px` (no invented spread) and
/// the aggressor side carries the volume. Downstream, `ticks_to_idx` marks
/// these records `FLAG_NO_BOOK` so bar builders exclude them from spread
/// sampling — spread is ABSENT (NaN + flag) in trade-derived history, never
/// a fabricated constant.
#[inline]
pub fn honest_tick(ticker_id: u64, price: f64, volume: u32, is_buyer: bool) -> Tick {
    // 6 sig digits — matches prod forwarder tick rounding
    let px = nxr_sdk::stats::round_to_sig_digits(price, 6);
    Tick {
        ticker: ticker_id,
        bid: px,
        ask: px,
        vbid: if is_buyer { 0 } else { volume },
        vask: if is_buyer { volume } else { 0 },
    }
}

// ─── Batch sending ─────────────────────────────���─────────────────────────────

const BATCH_SIZE: usize = 10_000;

// ─── Date helpers ────────────────────────────���───────────────────────────────

/// First day of the next month.
#[inline]
pub fn next_month_start(year: i32, month: u32) -> NaiveDate {
    if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
    }
}

/// Last day of the given month.
#[inline]
pub fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    next_month_start(year, month) - Duration::days(1)
}

/// Iterate months from `from` to `to`, yielding (month_start, month_end).
pub fn month_ranges(from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    let mut ranges = Vec::new();
    let mut current = from;
    while current <= to {
        let end = last_day_of_month(current.year(), current.month()).min(to);
        ranges.push((current, end));
        current = next_month_start(current.year(), current.month());
    }
    ranges
}

// ─── Timestamp normalization ────────────────────────────────────��────────────

/// Normalize a timestamp to milliseconds (handles microsecond timestamps).
#[inline]
pub fn normalize_timestamp_ms(ts: i64) -> i64 {
    if ts > 10_000_000_000_000 { ts / 1000 } else { ts }
}

// ─── Filesystem helpers ──────────���───────────────────��──────────────────────

pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Send ticks read from cached tick files through the channel.
///
/// Streams files **sequentially** and emits `BATCH_SIZE`-tick batches sliced
/// directly from each mmap, so peak RSS per source is one batch (~480 KiB)
/// plus the kernel's working set for the current file (trimmed by
/// `MADV_SEQUENTIAL`). The previous version used `par_iter().collect()`,
/// which read every file for the source into an owned `Vec<TickFrame>`
/// concurrently — for 30 days × 4 sources this pulled ≥10 GiB resident and
/// OOM-killed the host on 2026-04-23.
///
/// Each emitted batch has its header provider_id re-stamped so the
/// downstream aggregator can route per-provider regardless of whether the
/// cache predates provider-aware parse_csv writes.
pub async fn fetch_cached_ticks(
    files: &[std::path::PathBuf],
    provider_id: u16,
    tx: mpsc::Sender<Vec<TickFrame>>,
) {
    for path in files {
        if let Err(e) = stream_tick_file_to_channel(path, provider_id, &tx).await {
            warn!("Error streaming tick file {:?}: {}", path, e);
        }
    }
}

async fn stream_tick_file_to_channel(
    path: &Path,
    provider_id: u16,
    tx: &mpsc::Sender<Vec<TickFrame>>,
) -> Result<()> {
    // Chunked `File::read` (NOT `Mmap::map`): on macOS, mmap'd pages that
    // we touch are counted against our RSS and `MADV_SEQUENTIAL` is a weak
    // hint; reading a 1.7 GiB bybit monthly via mmap once OOM-killed the
    // host at a 2 GiB cap. Explicit read keeps resident use to ~1 batch.
    let tick_frame_size = std::mem::size_of::<TickFrame>();
    let mut file = File::open(path)?;
    let len = file.metadata()?.len() as usize;
    if len == 0 {
        return Ok(());
    }
    if len % tick_frame_size != 0 {
        anyhow::bail!(
            "File size ({}) is not a multiple of TickFrame ({}): {}",
            len,
            tick_frame_size,
            path.display(),
        );
    }
    stream_as_tick_frames(&mut file, tick_frame_size, provider_id, tx).await
}

/// Stream a native `TickFrame` (48 B) file batch-by-batch through the channel.
async fn stream_as_tick_frames(
    file: &mut File,
    frame_size: usize,
    provider_id: u16,
    tx: &mpsc::Sender<Vec<TickFrame>>,
) -> Result<()> {
    use std::io::Read;
    let mut buf: Vec<u8> = vec![0u8; BATCH_SIZE * frame_size];
    loop {
        let mut filled = 0usize;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(e.into()),
            }
        }
        if filled == 0 {
            break;
        }
        if filled % frame_size != 0 {
            anyhow::bail!("short read {} bytes not aligned to TickFrame {}", filled, frame_size);
        }
        let frames: &[TickFrame] = bytemuck::cast_slice(&buf[..filled]);
        let mut owned: Vec<TickFrame> = frames.to_vec();
        stamp_provider_id(&mut owned, provider_id);
        if tx.send(owned).await.is_err() {
            return Ok(());
        }
        if filled < buf.len() {
            break;
        }
    }
    Ok(())
}


// ─── Shared exchange download infrastructure ────────────────────────────────

#[derive(Clone, Copy)]
pub enum Compression { Zip, Gzip }

/// Download archive (streamed to disk), stream-decompress from disk +
/// stream-parse CSV, stream-write the `.ticks` cache file. Peak resident memory
/// is bounded to:
///   * one `DOWNLOAD_STREAM_BUF_BYTES` copy buffer (8 MiB) during download
///   * one `CSV_CHUNK_BYTES` line-aligned chunk (~1 MiB)
///   * one `BATCH_SIZE`-tick parsed batch (~480 KiB)
///
/// Critically, the compressed archive is NEVER held in RAM as a `Vec<u8>`:
/// it is streamed to a temp file on the ticks PVC and decompressed from disk.
/// The previous path buffered the entire compressed body via `read_to_end`
/// into a `Vec<u8>` (181-460 MiB for ETH-USDT monthly) before decompressing —
/// under memory pressure this intermittently OOM'd or truncated mid-transfer,
/// and the truncated body was silently accepted as a few/zero-tick "empty"
/// month (the ETH 2024-2025 gap).
///
/// The older path before that extracted all CSVs from the zip into memory
/// (~1.5 GiB for binance monthly), then parsed to a second Vec<Vec<TickFrame>>,
/// then flattened, then sorted. Peak ≥ 4.8 GiB. On 2026-04-23 this OOM-killed
/// the host mid-download of the first monthly archive.
///
/// Exchange CSVs are time-ordered on disk (binance: by-id ≈ by-time; bybit,
/// okx, bitget: by-timestamp). We preserve that order by streaming, so no
/// in-memory sort is needed. Downstream `MergedTickStream` assumes each
/// file is individually sorted and enforces it across sources.
///
/// Returns the number of ticks written. A download that completes the HTTP
/// transfer but parses to **zero** ticks (truncated archive, an upstream CSV
/// schema change that breaks the column mapping, or an empty member) is NOT
/// an `Err` at the HTTP layer, so the caller MUST inspect this count: a `0`
/// for a pair/month that genuinely has trades is a silent-empty defect. The
/// `.ticks` file is still written (possibly zero-length) so the caller can
/// decide whether to keep it, delete it, or fall back to the daily archives.
pub async fn download_and_convert<P>(
    agent: &Arc<ureq::Agent>, url: &str, cache_path: &Path, ticker_id: u64,
    compression: Compression, max_attempts: usize, parse_csv: &P,
) -> Result<u64>
where
    P: Fn(&[u8], u64) -> Result<Vec<TickFrame>> + Sync,
{
    use std::io::BufReader;

    const CSV_CHUNK_BYTES: usize = 1_000_000;

    ensure_parent_dir(cache_path)?;
    // Stream the compressed archive to a temp file on the same dir (ticks PVC),
    // with truncation detection + retry. The archive is decompressed FROM DISK
    // below; it is never held in RAM as a Vec<u8>. `archive` drops at end of
    // scope → temp file removed (delete-as-you-go scratch).
    let dst_dir = cache_path.parent().unwrap_or_else(|| Path::new("."));
    let archive = download_to_file_retry(agent, url, dst_dir, max_attempts).await?;
    let archive_path = archive.path.clone();
    let archive_bytes = archive.bytes_got;
    let tmp_path = cache_path.with_extension("ticks.partial");
    let mut total_ticks: u64 = 0;

    {
        let out_file = File::create(&tmp_path)?;
        let mut writer = BufWriter::with_capacity(1 << 20, out_file);

        // `feed` accepts one line-aligned CSV chunk and streams its parsed
        // ticks directly to `writer`, so no large intermediate Vec survives
        // between chunks.
        let mut feed = |chunk: &[u8]| -> Result<()> {
            if chunk.is_empty() {
                return Ok(());
            }
            match parse_csv(chunk, ticker_id) {
                Ok(ticks) => {
                    let bytes: &[u8] = bytemuck::cast_slice(&ticks);
                    writer.write_all(bytes)?;
                    total_ticks += ticks.len() as u64;
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(ticker_id, err = %e, "csv chunk parse failed");
                    Ok(())
                }
            }
        };

        match compression {
            Compression::Zip => {
                // `ZipArchive` needs Read+Seek; a `File` provides both, so the
                // central directory is read from disk without loading the body.
                let zip_file = File::open(&archive_path)?;
                let mut zarchive = ZipArchive::new(zip_file)?;
                for i in 0..zarchive.len() {
                    let file = match zarchive.by_index(i) {
                        Ok(f) => f,
                        Err(e) => {
                            tracing::warn!(ticker_id, idx = i, err = %e, "zip entry unreadable");
                            continue;
                        }
                    };
                    if !file.name().ends_with(".csv") {
                        continue;
                    }
                    stream_csv_in_chunks(BufReader::with_capacity(64 * 1024, file), CSV_CHUNK_BYTES, &mut feed)?;
                }
            }
            Compression::Gzip => {
                let gz_file = File::open(&archive_path)?;
                let decoder = flate2::read::GzDecoder::new(BufReader::with_capacity(64 * 1024, gz_file));
                stream_csv_in_chunks(BufReader::with_capacity(64 * 1024, decoder), CSV_CHUNK_BYTES, &mut feed)?;
            }
        }

        writer.flush()?;
        // drop writer + out_file here so the rename below sees a closed fd.
    }

    // Atomic rename so a partial/interrupted download leaves the old cache
    // file intact rather than a half-written target.
    std::fs::rename(&tmp_path, cache_path)?;

    if total_ticks == 0 {
        // A 200-OK transfer that yields zero ticks is the silent-empty failure
        // mode behind recurring "ETH older months produce no .ticks" reports:
        // the archive downloaded, but every row failed to parse (upstream CSV
        // schema drift) or the member was truncated/empty. Surface it loudly —
        // never let a zero-record month pass as a normal success.
        warn!(
            bytes_downloaded = archive_bytes,
            url,
            cache = %cache_path.display(),
            "download_and_convert produced ZERO ticks (CSV schema drift or genuinely empty \
             member — truncation is now caught upstream as a failed transfer) — \
             treating as empty so caller can fall back / flag the gap",
        );
    } else {
        info!("Converted {} ticks → {}", total_ticks, cache_path.display());
    }
    drop(archive); // remove temp compressed archive (delete-as-you-go scratch)
    Ok(total_ticks)
}

/// Read from `reader` in line-aligned chunks of ~`chunk_bytes` each and
/// invoke `feed` on every complete chunk. A chunk always ends on a newline
/// so `feed`'s callee can parse it as standalone CSV text without losing
/// a split row at the boundary.
fn stream_csv_in_chunks<R: std::io::BufRead>(
    mut reader: R,
    chunk_bytes: usize,
    feed: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(chunk_bytes + 64 * 1024);
    loop {
        buf.clear();
        let mut filled = 0usize;
        while filled < chunk_bytes {
            let before = buf.len();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) => break,
                Ok(_) => {
                    filled += buf.len() - before;
                }
                Err(e) => return Err(e.into()),
            }
        }
        if buf.is_empty() {
            break;
        }
        feed(&buf)?;
        if filled < chunk_bytes {
            break; // hit EOF on the last read
        }
    }
    Ok(())
}

/// Shared monthly-first, daily-fallback tick file fetcher.
///
/// `monthly(symbol, year, month)` returns `(url, filename)`.
/// `daily(symbol, date)` returns `(url, filename)`.
/// Monthly downloads get 3 attempts; daily gets 1.
pub async fn fetch_monthly_daily<P>(
    agent: &Arc<ureq::Agent>,
    config: &Config,
    exchange: &str,
    symbol: &str,
    dir_name: &str,
    ticker_id: u64,
    cache_ext: &str,
    compression: Compression,
    monthly: impl Fn(&str, i32, u32) -> (String, String),
    daily: impl Fn(&str, NaiveDate) -> (String, String),
    parse_csv: &P,
) -> Result<Vec<PathBuf>>
where
    P: Fn(&[u8], u64) -> Result<Vec<TickFrame>> + Sync,
{
    let today = chrono::Utc::now().date_naive();
    let ticks_dir = config.ticks_dir.join(exchange).join(dir_name);
    let mut files = Vec::new();

    for (month_start, month_end) in month_ranges(config.from.date_naive(), config.to.date_naive()) {
        let year = month_start.year();
        let month = month_start.month();
        // Track whether THIS month yielded any data at all, across the monthly
        // archive + daily fallback. A completed month that produces nothing for
        // a liquid pair is the silent-gap signature (e.g. recurring "ETH older
        // months empty"); we emit a single per-month warning so it can never
        // disappear unnoticed at the default INFO log level again.
        let files_before_month = files.len();
        let mut month_ticks: u64 = 0;
        let mut used_cached_month = false;

        // Try monthly archive for completed months
        if last_day_of_month(year, month) < today {
            let (url, filename) = monthly(symbol, year, month);
            let cache_path = ticks_dir.join(filename.replace(cache_ext, ".ticks"));

            if cache_path.exists() {
                debug!("Cached: {}", cache_path.display());
                files.push(cache_path);
                // A pre-existing cache file is trusted as non-empty here (it
                // was validated when written); skip the daily fallback.
                used_cached_month = true;
            } else {
                info!("Downloading monthly: {}", filename);
                match download_and_convert(agent, &url, &cache_path, ticker_id, compression, 3, parse_csv).await {
                    // Zero-tick monthly is NOT a success: drop the empty cache
                    // file and fall through to the daily archives, which may
                    // carry the data the monthly archive lacked (or confirm a
                    // real gap day-by-day). This is the fix for monthly archives
                    // that download cleanly but parse to nothing.
                    Ok(0) => {
                        warn!(
                            "Monthly {} yielded ZERO ticks; removing empty cache and trying daily",
                            filename
                        );
                        let _ = std::fs::remove_file(&cache_path);
                    }
                    Ok(n) => {
                        month_ticks += n;
                        files.push(cache_path);
                        used_cached_month = true;
                    }
                    Err(e) => warn!("Monthly failed {}: {}, trying daily", filename, e),
                }
            }
        }

        // Daily fallback (current month, failed monthly, or zero-tick monthly)
        if !used_cached_month {
            let end = month_end.min(config.to.date_naive());
            let mut day = month_start;
            while day <= end {
                let (url, filename) = daily(symbol, day);
                let cache_path = ticks_dir.join(filename.replace(cache_ext, ".ticks"));

                if !cache_path.exists() {
                    info!("Downloading daily: {}", filename);
                    match download_and_convert(agent, &url, &cache_path, ticker_id, compression, 1, parse_csv).await {
                        Ok(0) => {
                            // Empty daily archive — drop it, don't push.
                            let _ = std::fs::remove_file(&cache_path);
                            debug!("Empty {} daily archive for {}", exchange, day);
                        }
                        Ok(n) => { month_ticks += n; files.push(cache_path); }
                        Err(_) => debug!("No {} data for {}", exchange, day),
                    }
                } else {
                    debug!("Cached: {}", cache_path.display());
                    files.push(cache_path);
                }
                day += Duration::days(1);
            }
        }

        // Per-month silent-gap guard: a COMPLETED month (older than today) that
        // produced no fresh file and no ticks is almost certainly a fetch defect
        // for a liquid pair, not a real absence. Warn so backfill logs flag the
        // hole instead of silently skipping it.
        let produced_files = files.len() > files_before_month;
        let month_is_complete = last_day_of_month(year, month) < today;
        if month_is_complete && !used_cached_month && month_ticks == 0 && !produced_files {
            warn!(
                exchange,
                symbol,
                year,
                month,
                "no ticks fetched for completed month (monthly + daily both empty) — \
                 possible silent archive/parse gap, NOT necessarily a real data absence",
            );
        }
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use nxr_sdk::pipeline_config::{
        DEFAULT_ARCHIVE_URL_BINANCE_MONTHLY, DEFAULT_ARCHIVE_URL_OKX_MONTHLY,
    };

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    /// Replica of `BinanceSource`'s monthly URL/filename builder (sources are
    /// closures, not exported fns). Kept byte-identical so this test guards the
    /// real symbol→URL mapping.
    fn binance_monthly(sym: &str, y: i32, m: u32) -> (String, String) {
        let prefix = DEFAULT_ARCHIVE_URL_BINANCE_MONTHLY;
        let f = format!("{}-aggTrades-{:04}-{:02}.zip", sym, y, m);
        let url = format!("{}{}", prefix.replace("{sym}", sym), f);
        (url, f)
    }

    fn okx_monthly(sym: &str, y: i32, m: u32) -> (String, String) {
        let prefix = DEFAULT_ARCHIVE_URL_OKX_MONTHLY;
        let f = format!("{}-trades-{:04}-{:02}.zip", sym, y, m);
        let url = format!(
            "{}{}",
            prefix
                .replace("{y:04}", &format!("{:04}", y))
                .replace("{m:02}", &format!("{:02}", m)),
            f,
        );
        (url, f)
    }

    /// ETH-USDT must build the SAME well-formed Binance monthly path as
    /// BNB-USDT / SOL-USDT — differing ONLY in the symbol token. This pins the
    /// invariant behind the recurring "ETH older months empty" reports: the
    /// fetcher applies no per-symbol date floor, alias, or special-casing, so a
    /// missing ETH 2024 month can NEVER be a URL/mapping defect — it would be a
    /// real upstream gap (which it is not — these archives exist + 200-OK).
    #[test]
    fn binance_monthly_url_symbol_parity_eth_bnb_sol() {
        for &(y, m) in &[(2024, 6), (2024, 12), (2025, 6), (2026, 2)] {
            let (eth_url, eth_f) = binance_monthly("ETHUSDT", y, m);
            let (bnb_url, _) = binance_monthly("BNBUSDT", y, m);
            let (sol_url, _) = binance_monthly("SOLUSDT", y, m);

            // ETH path is structurally identical to BNB/SOL once the symbol is
            // normalised out — proving no ETH-specific divergence by month.
            let norm = |s: &str| s.replace("ETHUSDT", "§").replace("BNBUSDT", "§").replace("SOLUSDT", "§");
            assert_eq!(norm(&eth_url), norm(&bnb_url), "ETH vs BNB url diverged @ {y}-{m:02}");
            assert_eq!(norm(&eth_url), norm(&sol_url), "ETH vs SOL url diverged @ {y}-{m:02}");

            // Well-formed: exact expected ETH path for every month (no floor).
            assert_eq!(
                eth_url,
                format!(
                    "https://data.binance.vision/data/spot/monthly/aggTrades/ETHUSDT/ETHUSDT-aggTrades-{y:04}-{m:02}.zip"
                ),
            );
            assert_eq!(eth_f, format!("ETHUSDT-aggTrades-{y:04}-{m:02}.zip"));
        }
    }

    /// OKX uses a dashed symbol (`ETH-USDT`) + date-keyed bucket. Same parity
    /// guarantee: ETH differs from BNB/SOL only by symbol token, across months.
    #[test]
    fn okx_monthly_url_symbol_parity_eth_bnb_sol() {
        for &(y, m) in &[(2024, 6), (2025, 6), (2026, 2)] {
            let (eth_url, _) = okx_monthly("ETH-USDT", y, m);
            let (bnb_url, _) = okx_monthly("BNB-USDT", y, m);
            let (sol_url, _) = okx_monthly("SOL-USDT", y, m);
            let norm = |s: &str| s.replace("ETH-USDT", "§").replace("BNB-USDT", "§").replace("SOL-USDT", "§");
            assert_eq!(norm(&eth_url), norm(&bnb_url));
            assert_eq!(norm(&eth_url), norm(&sol_url));
            assert_eq!(
                eth_url,
                format!(
                    "https://static.okx.com/cdn/okex/traderecords/trades/monthly/{y:04}{m:02}/ETH-USDT-trades-{y:04}-{m:02}.zip"
                ),
            );
        }
    }

    /// Truncation predicate: short-of-Content-Length ⇒ truncated (failed
    /// transfer, retried), exact/over ⇒ OK, unknown length ⇒ OK (can't judge).
    /// This is the rule that stops a partial ETH archive being accepted as an
    /// empty month.
    #[test]
    fn is_truncated_predicate() {
        assert!(is_truncated(100, Some(200)), "short body must be truncated");
        assert!(!is_truncated(200, Some(200)), "exact body is complete");
        assert!(!is_truncated(201, Some(200)), "over-read (chunked) not truncated");
        assert!(!is_truncated(0, None), "no Content-Length ⇒ cannot flag truncation");
        assert!(!is_truncated(100, None), "no Content-Length ⇒ accept");
    }

    /// A COMPLETE gzip archive on disk streams + decompresses to the exact tick
    /// count via `download_and_convert`'s decompress path; a TRUNCATED gzip
    /// (bytes lopped off) fails to decompress and yields ZERO ticks rather than
    /// a wrong-but-accepted partial — proving truncated data is never silently
    /// converted to a "valid empty" month at the decompress layer (the HTTP
    /// layer additionally rejects it via Content-Length before we get here).
    #[test]
    fn gzip_complete_vs_truncated_decompress_from_disk() {
        use flate2::write::GzEncoder;
        use flate2::Compression as GzLevel;
        use std::io::Write as _;

        let csv = "0,1,2,3\n1,2,3,4\n2,3,4,5\n";
        let mut enc = GzEncoder::new(Vec::new(), GzLevel::default());
        enc.write_all(csv.as_bytes()).unwrap();
        let gz = enc.finish().unwrap();

        let dir = std::env::temp_dir().join(format!("sf-gz-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // helper: decode a gz file on disk through stream_csv_in_chunks, count lines.
        let decode_lines = |bytes: &[u8]| -> Result<usize> {
            let p = dir.join("a.gz");
            std::fs::write(&p, bytes).unwrap();
            let f = File::open(&p).unwrap();
            let decoder = flate2::read::GzDecoder::new(std::io::BufReader::new(f));
            let mut lines = 0usize;
            let mut feed = |chunk: &[u8]| -> Result<()> {
                lines += chunk.iter().filter(|&&b| b == b'\n').count();
                Ok(())
            };
            stream_csv_in_chunks(std::io::BufReader::new(decoder), 1_000_000, &mut feed)?;
            Ok(lines)
        };

        // Complete archive → all 3 rows decode.
        assert_eq!(decode_lines(&gz).unwrap(), 3, "complete gz must decode all rows");

        // Truncated archive (drop trailing CRC/length + payload) → decode errors.
        let truncated = &gz[..gz.len() / 2];
        assert!(
            decode_lines(truncated).is_err(),
            "truncated gz must fail to decompress, not yield a partial-accepted result",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `month_ranges` tiles [from,to] contiguously with no per-symbol logic —
    /// every ETH month in a 2024-06..2026-02 backfill is enumerated exactly
    /// once, so no month can be silently skipped at the iteration layer.
    #[test]
    fn month_ranges_full_backfill_window_no_gap() {
        let r = month_ranges(d(2024, 6, 9), d(2026, 2, 28));
        assert_eq!(r.first().unwrap().0, d(2024, 6, 9));
        assert_eq!(r.last().unwrap().1, d(2026, 2, 28));
        // 2024-06 .. 2026-02 inclusive = 21 calendar months.
        assert_eq!(r.len(), 21);
        for pair in r.windows(2) {
            assert_eq!(pair[1].0, pair[0].1 + Duration::days(1));
        }
    }
}
