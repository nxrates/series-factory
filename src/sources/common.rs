//! Shared exchange infrastructure - download, decompress, parse, tick I/O.
//!
//! All exchange sources share: HTTP download, tick file I/O (native TickFrame),
//! ZIP/GZIP decompression, bid/ask inference from trade data, batch sending,
//! and date iteration helpers.

use crate::types::{round_to_6_sig_digits, Config, TickFrame};
use anyhow::Result;
use chrono::{Datelike, Duration, NaiveDate};
use memmap2::Mmap;
use mitch::Tick;
use std::fs::{self, File};
use std::io::{BufWriter, Cursor, Read, Write};
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

/// Download URL to memory (blocking, runs on spawn_blocking).
pub async fn download_bytes(agent: &Arc<ureq::Agent>, url: &str) -> Result<Vec<u8>> {
    let agent = agent.clone();
    let url = url.to_string();

    tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        debug!("Downloading: {}", url);
        let response = agent
            .get(&url)
            .call()
            .map_err(|e| anyhow::anyhow!("Download failed {}: {}", url, e))?;
        let mut data = Vec::new();
        response.into_reader().read_to_end(&mut data)?;
        Ok(data)
    })
    .await?
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

// ─── Bid/ask inference ───────────────────────────────────────────────────────

/// Infer bid/ask from a trade price and side.
///
/// Market buy → price is ask, bid inferred 1 bps below.
/// Market sell → price is bid, ask inferred 1 bps above.
#[inline]
pub fn infer_tick(ticker_id: u64, price: f64, volume: u32, is_buyer: bool) -> Tick {
    if is_buyer {
        Tick {
            ticker: ticker_id,
            bid: round_to_6_sig_digits(price * 0.9999),
            ask: round_to_6_sig_digits(price),
            vbid: 0,
            vask: volume,
        }
    } else {
        Tick {
            ticker: ticker_id,
            bid: round_to_6_sig_digits(price),
            ask: round_to_6_sig_digits(price * 1.0001),
            vbid: volume,
            vask: 0,
        }
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

/// Download with retry (immediate retry, up to `max_attempts`).
pub async fn download_bytes_retry(agent: &Arc<ureq::Agent>, url: &str, max_attempts: usize) -> Result<Vec<u8>> {
    let mut last_err = None;
    for attempt in 0..max_attempts {
        match download_bytes(agent, url).await {
            Ok(data) => return Ok(data),
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

/// Download archive, stream-decompress + stream-parse CSV, stream-write the
/// `.ticks` cache file. Peak resident memory is bounded to:
///   * the downloaded archive bytes (compressed, ~50-300 MiB for monthly)
///   * one `CSV_CHUNK_BYTES` line-aligned chunk (~1 MiB)
///   * one `BATCH_SIZE`-tick parsed batch (~480 KiB)
///
/// The old path extracted all CSVs from the zip into memory (~1.5 GiB for
/// binance monthly), then parsed to a second Vec<Vec<TickFrame>>, then
/// flattened, then sorted. Peak ≥ 4.8 GiB. On 2026-04-23 this OOM-killed
/// the host mid-download of the first monthly archive.
///
/// Exchange CSVs are time-ordered on disk (binance: by-id ≈ by-time; bybit,
/// okx, bitget: by-timestamp). We preserve that order by streaming, so no
/// in-memory sort is needed. Downstream `MergedTickStream` assumes each
/// file is individually sorted and enforces it across sources.
pub async fn download_and_convert<P>(
    agent: &Arc<ureq::Agent>, url: &str, cache_path: &Path, ticker_id: u64,
    compression: Compression, max_attempts: usize, parse_csv: &P,
) -> Result<()>
where
    P: Fn(&[u8], u64) -> Result<Vec<TickFrame>> + Sync,
{
    use std::io::{BufRead, BufReader};

    const CSV_CHUNK_BYTES: usize = 1_000_000;

    let data = download_bytes_retry(agent, url, max_attempts).await?;
    ensure_parent_dir(cache_path)?;
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
                let mut archive = ZipArchive::new(Cursor::new(&data))?;
                for i in 0..archive.len() {
                    let file = match archive.by_index(i) {
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
                let decoder = flate2::read::GzDecoder::new(Cursor::new(&data));
                stream_csv_in_chunks(BufReader::with_capacity(64 * 1024, decoder), CSV_CHUNK_BYTES, &mut feed)?;
            }
        }

        writer.flush()?;
        // drop writer + out_file here so the rename below sees a closed fd.
    }

    // Atomic rename so a partial/interrupted download leaves the old cache
    // file intact rather than a half-written target.
    std::fs::rename(&tmp_path, cache_path)?;

    info!("Converted {} ticks → {}", total_ticks, cache_path.display());
    drop(data);
    Ok(())
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

        // Try monthly archive for completed months
        if last_day_of_month(year, month) < today {
            let (url, filename) = monthly(symbol, year, month);
            let cache_path = ticks_dir.join(filename.replace(cache_ext, ".ticks"));

            if cache_path.exists() {
                debug!("Cached: {}", cache_path.display());
                files.push(cache_path);
                continue;
            }

            info!("Downloading monthly: {}", filename);
            match download_and_convert(agent, &url, &cache_path, ticker_id, compression, 3, parse_csv).await {
                Ok(_) => { files.push(cache_path); continue; }
                Err(e) => warn!("Monthly failed {}: {}, trying daily", filename, e),
            }
        }

        // Daily fallback (current month or failed monthly)
        let end = month_end.min(config.to.date_naive());
        let mut day = month_start;
        while day <= end {
            let (url, filename) = daily(symbol, day);
            let cache_path = ticks_dir.join(filename.replace(cache_ext, ".ticks"));

            if !cache_path.exists() {
                info!("Downloading daily: {}", filename);
                match download_and_convert(agent, &url, &cache_path, ticker_id, compression, 1, parse_csv).await {
                    Ok(_) => files.push(cache_path),
                    Err(_) => debug!("No {} data for {}", exchange, day),
                }
            } else {
                debug!("Cached: {}", cache_path.display());
                files.push(cache_path);
            }
            day += Duration::days(1);
        }
    }

    Ok(files)
}
