//! Shared exchange infrastructure — download, decompress, parse, tick I/O.
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

/// Write .ticks file from &[TickFrame] (zero-copy bytemuck).
pub fn write_tick_file(path: &Path, frames: &[TickFrame]) -> Result<()> {
    ensure_parent_dir(path)?;
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);
    let bytes: &[u8] = bytemuck::cast_slice(frames);
    writer.write_all(bytes)?;
    writer.flush()?;
    Ok(())
}

// ─── Decompression ──────────────────────────────────��────────────────────────

/// Extract all CSV files from a ZIP archive, return their raw bytes.
pub fn extract_csvs_from_zip(zip_data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let cursor = Cursor::new(zip_data);
    let mut archive = ZipArchive::new(cursor)?;
    let mut csvs = Vec::new();

    for i in 0..archive.len() {
        let mut file = match archive.by_index(i) {
            Ok(f) => f,
            Err(_) => continue,
        };
        if !file.name().ends_with(".csv") {
            continue;
        }
        let mut data = Vec::new();
        if file.read_to_end(&mut data).is_ok() {
            csvs.push(data);
        }
    }
    Ok(csvs)
}

/// Decompress gzip data.
pub fn decompress_gzip(gz_data: &[u8]) -> Result<Vec<u8>> {
    nxr_sdk::compress::decode_gzip_bytes(gz_data)
        .ok_or_else(|| anyhow::anyhow!("gzip decompression failed"))
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

/// Send ticks through channel in owned batches (no .to_vec() copy).
pub async fn send_tick_batches(
    mut ticks: Vec<TickFrame>,
    tx: &mpsc::Sender<Vec<TickFrame>>,
) {
    // Drain in BATCH_SIZE chunks, transferring ownership
    while ticks.len() > BATCH_SIZE {
        let rest = ticks.split_off(BATCH_SIZE);
        let batch = ticks;
        ticks = rest;
        if tx.send(batch).await.is_err() {
            return;
        }
    }
    if !ticks.is_empty() {
        let _ = tx.send(ticks).await;
    }
}

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
/// Shared implementation for all exchange fetch_ticks().
pub async fn fetch_cached_ticks(
    files: &[std::path::PathBuf],
    tx: mpsc::Sender<Vec<TickFrame>>,
) {
    let results: Vec<Result<Vec<TickFrame>>> = files
        .par_iter()
        .map(|path| {
            read_tick_file(path).map_err(|e| {
                warn!("Error reading tick file {:?}: {}", path, e);
                e
            })
        })
        .collect();

    for result in results {
        if let Ok(ticks) = result {
            send_tick_batches(ticks, &tx).await;
        }
    }
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
                    warn!("Attempt {}/{} failed: {} — retrying", attempt + 1, max_attempts, url);
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap())
}

/// Download archive, decompress, parse CSV, sort, write .ticks cache file.
pub async fn download_and_convert<P>(
    agent: &Arc<ureq::Agent>, url: &str, cache_path: &Path, ticker_id: u64,
    compression: Compression, max_attempts: usize, parse_csv: &P,
) -> Result<()>
where
    P: Fn(&[u8], u64) -> Result<Vec<TickFrame>> + Sync,
{
    let data = download_bytes_retry(agent, url, max_attempts).await?;
    let mut ticks = match compression {
        Compression::Zip => {
            let csvs = extract_csvs_from_zip(&data)?;
            let batches: Vec<Vec<TickFrame>> = csvs
                .par_iter()
                .filter_map(|csv| parse_csv(csv, ticker_id).ok())
                .collect();
            batches.into_iter().flatten().collect()
        }
        Compression::Gzip => {
            let csv_data = decompress_gzip(&data)?;
            parse_csv(&csv_data, ticker_id)?
        }
    };
    ticks.par_sort_unstable_by_key(|t| t.timestamp_ms());
    write_tick_file(cache_path, &ticks)?;
    info!("Converted {} ticks → {}", ticks.len(), cache_path.display());
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
    let cache_dir = config.cache_dir.join(exchange).join(dir_name);
    let mut files = Vec::new();

    for (month_start, month_end) in month_ranges(config.from.date_naive(), config.to.date_naive()) {
        let year = month_start.year();
        let month = month_start.month();

        // Try monthly archive for completed months
        if last_day_of_month(year, month) < today {
            let (url, filename) = monthly(symbol, year, month);
            let cache_path = cache_dir.join(filename.replace(cache_ext, ".ticks"));

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
            let cache_path = cache_dir.join(filename.replace(cache_ext, ".ticks"));

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
