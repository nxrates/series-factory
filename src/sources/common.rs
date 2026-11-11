//! Shared archive-source infrastructure: one code path for every venue and market.
//!
//! A venue file (`binance.rs`, ...) only states its archive layout (file-name
//! patterns), the symbol spelling and a row mapper; everything else lives here:
//! URL expansion, monthly -> daily granularity, download + retry + missing
//! classification, CSV -> tick conversion (validation, quote volume, ordering),
//! caching and availability probing.

use crate::sources::{provider_id_for, series_key, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate};
use memmap2::Mmap;
use mitch::Tick;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, OnceCell};
use tracing::{debug, info, warn};
use zip::ZipArchive;

// ─── HTTP Agent ───────────────────────────────────────────────────────────────

/// Shared egress client with 10-min timeout (large monthly archives can be 300MB+): the process-wide
/// btr-rpc set, direct when no egress is configured.
pub fn http_agent() -> Arc<btr_rpc::Http> {
    // Archive CDNs answer a missing object 403 (bitget) or 404: a normal reply, never an IP park.
    // Patience: a parked IP after a 5xx is waited out instead of failing "every IP backing off".
    Arc::new(
        btr_rpc::env_http(std::time::Duration::from_secs(600))
            .expect("egress config unusable")
            .missing_403()
            .patience(std::time::Duration::from_secs(90)),
    )
}

// ─── Archive layout ───────────────────────────────────────────────────────────
//
// URL = stem + file. The stem (everything up to the file name) is YAML, spot in
// `cexs.exchanges.<name>.archive_url_template.{monthly,daily}`, perps in
// `...archive_url_template.perp.{monthly,daily}`; the file-name pattern is the
// venue's layout and lives in its source file. Both use the same placeholders:
// `{sym}` `{y:04}` `{m:02}` `{d:02}` `{ds}` (YYYYMMDD) `{seq:03}` `{product}`.

#[derive(Clone, Default, Deserialize)]
pub struct Stems {
    #[serde(default)]
    pub monthly: String,
    #[serde(default)]
    pub daily: String,
}

#[derive(Default, Deserialize)]
struct Template {
    #[serde(flatten)]
    spot: Stems,
    #[serde(default)]
    perp: Stems,
}

#[derive(Default, Deserialize)]
struct Exchange {
    #[serde(default)]
    archive_url_template: Template,
    #[serde(default)]
    exclude_symbols: Vec<String>,
}

#[derive(Default, Deserialize)]
struct Cexs {
    #[serde(default)]
    exchanges: BTreeMap<String, Exchange>,
}

#[derive(Default, Deserialize)]
struct Doc {
    #[serde(default)]
    cexs: Cexs,
}

/// Is `BASE/QUOTE` (quote spelled as in the series, `USDT:PERP` for perps) in the
/// venue's `exclude_symbols`? Such a pair is an unrelated asset on that venue, so
/// it never enters a composite: not probed, not fetched.
pub fn excluded(exchange: &str, base: &str, quote: &str) -> Result<bool> {
    use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
    let path = PipelineYml::resolve_path(ConfigHint::Bin);
    let text =
        fs::read_to_string(&path).with_context(|| format!("read config {}", path.display()))?;
    let doc: Doc =
        serde_yml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;
    let pair = format!("{}/{}", base.to_uppercase(), quote.to_uppercase());
    Ok(doc.cexs.exchanges.get(exchange).is_some_and(|e| {
        e.exclude_symbols
            .iter()
            .any(|x| x.eq_ignore_ascii_case(&pair))
    }))
}

/// URL stems of one venue × market from the pipeline config (`NXR_CONFIG`, else
/// `config.yml`). A config that cannot be read is an error, never a silent default.
fn stems(exchange: &str, perp: bool) -> Result<Stems> {
    use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
    let path = PipelineYml::resolve_path(ConfigHint::Bin);
    let text =
        fs::read_to_string(&path).with_context(|| format!("read config {}", path.display()))?;
    let mut doc: Doc =
        serde_yml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;
    let t = doc
        .cexs
        .exchanges
        .remove(exchange)
        .unwrap_or_default()
        .archive_url_template;
    Ok(if perp { t.perp } else { t.spot })
}

/// File-name patterns of one venue × market; `""` = that granularity is not published.
#[derive(Clone, Copy)]
pub struct Files {
    pub monthly: &'static str,
    pub daily: &'static str,
    /// Hours east of UTC at which the venue cuts its archive days and months: the
    /// file of day `D` covers UTC `[D - tz_h, D + 1d - tz_h)` (0 for UTC archives).
    pub tz_h: i64,
}

/// One CSV row as a venue states it. `size` is in the venue's own unit (base
/// units for spot, contracts for contract-sized swaps); [`Venue::scale`] turns it into base.
pub struct Row {
    pub us: u64,
    pub price: f64,
    pub size: f64,
    pub buyer: bool,
}

pub type RowFn = fn(&csv::StringRecord) -> Option<Row>;
type Extract = Box<dyn Fn(&serde_json::Value) -> Result<f64> + Send + Sync>;

/// Everything a venue contributes for one (base, quote): the sections above.
pub struct Venue {
    pub name: &'static str,
    pub sym: String,
    pub perp: bool,
    pub files: Files,
    pub stems: Stems,
    pub row: RowFn,
    /// Bitget's `{product}` path segment (UMCBL / CMCBL); empty elsewhere.
    pub product: &'static str,
    /// Resolution (us) of the venue's timestamps when coarser than a millisecond
    /// (Bitget stamps whole seconds); 0 otherwise. Trades sharing a stamp are
    /// spread evenly across that interval instead of all landing at its start,
    /// which would lead the true time by up to the resolution.
    pub ts_res_us: u64,
    /// Public REST endpoint + extractor for the base units per contract.
    pub contract: Option<(String, Extract)>,
}

impl Venue {
    pub fn new(
        name: &'static str,
        perp: bool,
        sym: String,
        files: Files,
        row: RowFn,
    ) -> Result<Self> {
        let stems = stems(name, perp)?;
        for (kind, file, stem) in [
            ("monthly", files.monthly, &stems.monthly),
            ("daily", files.daily, &stems.daily),
        ] {
            anyhow::ensure!(
                file.is_empty() || (stem.ends_with('/') && Compression::of(file).is_some()),
                "{name}: config cexs.exchanges.{name}.archive_url_template{}.{kind} must be a URL stem ending in '/' (and the file pattern {file:?} .zip or .csv.gz)",
                if perp { ".perp" } else { "" },
            );
        }
        Ok(Self {
            name,
            sym,
            perp,
            files,
            stems,
            row,
            product: "",
            ts_res_us: 0,
            contract: None,
        })
    }

    pub fn with_ts_res(mut self, us: u64) -> Self {
        self.ts_res_us = us;
        self
    }

    pub fn with_product(mut self, product: &'static str) -> Self {
        self.product = product;
        self
    }

    pub fn with_contract(
        mut self,
        url: String,
        extract: impl Fn(&serde_json::Value) -> Result<f64> + Send + Sync + 'static,
    ) -> Self {
        self.contract = Some((url, Box::new(extract)));
        self
    }

    fn url(&self, stem: &str, file: &str, day: NaiveDate, seq: u32) -> (String, String) {
        let file = expand(file, self, day, seq);
        (format!("{}{file}", expand(stem, self, day, seq)), file)
    }

    /// `(url, file name)` of the monthly archive of `first`'s month; `None` when not published.
    pub fn monthly(&self, first: NaiveDate) -> Option<(String, String)> {
        (!self.files.monthly.is_empty())
            .then(|| self.url(&self.stems.monthly, self.files.monthly, first, 1))
    }

    /// `(url, file name)` of the daily archive (chunk `seq` when chunked).
    pub fn daily(&self, day: NaiveDate, seq: u32) -> Option<(String, String)> {
        (!self.files.daily.is_empty())
            .then(|| self.url(&self.stems.daily, self.files.daily, day, seq))
    }

    /// Daily archives come in numbered chunks (`_001.zip`, `_002.zip`, ...).
    fn chunked(&self) -> bool {
        self.files.daily.contains("{seq")
    }
}

fn expand(t: &str, v: &Venue, d: NaiveDate, seq: u32) -> String {
    t.replace("{sym}", &v.sym)
        .replace("{product}", v.product)
        .replace("{y:04}", &format!("{:04}", d.year()))
        .replace("{m:02}", &format!("{:02}", d.month()))
        .replace("{d:02}", &format!("{:02}", d.day()))
        .replace("{ds}", &d.format("%Y%m%d").to_string())
        .replace("{seq:03}", &format!("{seq:03}"))
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Compression {
    Zip,
    Gzip,
}

impl Compression {
    /// Compression of an archive name and the name without its extension.
    fn of(name: &str) -> Option<(Self, &str)> {
        if let Some(s) = name.strip_suffix(".csv.gz") {
            Some((Self::Gzip, s))
        } else {
            name.strip_suffix(".zip").map(|s| (Self::Zip, s))
        }
    }
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
    http: &Arc<btr_rpc::Http>,
    url: &str,
    dst_dir: &Path,
) -> Result<DownloadedArchive> {
    debug!("Downloading (stream→disk): {}", url);
    std::fs::create_dir_all(dst_dir)?;

    let (status, mut resp) = http
        .get_stream(url, &[])
        .await
        .map_err(|e| anyhow::anyhow!("Download failed {}: {}", url, e))?;
    if !(200..300).contains(&status) {
        return Err(anyhow::anyhow!("Download failed {}: HTTP {}", url, status));
    }

    let content_length = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok());

    // Unique temp name so concurrent downloads in the same dir don't clash.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_path = dst_dir.join(format!(".partial-dl-{nonce}-{:x}", url.len()));

    let mut writer = BufWriter::with_capacity(DOWNLOAD_STREAM_BUF_BYTES, File::create(&tmp_path)?);
    let mut bytes_got: u64 = 0;
    loop {
        match resp.chunk().await {
            Ok(None) => break,
            Ok(Some(chunk)) => {
                if let Err(e) = writer.write_all(&chunk) {
                    drop(writer);
                    let _ = std::fs::remove_file(&tmp_path);
                    return Err(e.into());
                }
                bytes_got += chunk.len() as u64;
            }
            Err(e) => {
                // Mid-transfer error (the failure mode that lost ETH
                // months): drop the partial file, surface as Err so the
                // retry loop can re-attempt rather than accept a stub.
                let _ = writer.flush();
                drop(writer);
                let _ = std::fs::remove_file(&tmp_path);
                return Err(anyhow::anyhow!("stream read failed {}: {}", url, e));
            }
        }
    }
    writer.flush()?;
    drop(writer);

    Ok(DownloadedArchive {
        path: tmp_path,
        bytes_got,
        content_length,
    })
}

/// Truncation predicate: a download is truncated iff a `Content-Length` was
/// advertised and fewer bytes arrived. Split out so the rule is unit-testable
/// without a live HTTP server.
#[inline]
pub fn is_truncated(bytes_got: u64, content_length: Option<u64>) -> bool {
    matches!(content_length, Some(expected) if bytes_got < expected)
}

/// A 404/403 download error: the archive has no such object (a normal reply
/// from these CDNs), as opposed to a transfer fault worth a warning.
pub fn is_missing(e: &anyhow::Error) -> bool {
    let msg = e.to_string();
    msg.ends_with("HTTP 404") || msg.ends_with("HTTP 403")
}
/// Attempts per request: the one retry policy of every archive and REST call.
const ATTEMPTS: usize = 3;
const BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

/// Run `op` up to `ATTEMPTS` times, sleeping `backoff * attempt` between tries.
/// A missing object ([`is_missing`]) is final and returned at once; any other
/// error is retried and the last one returned.
async fn with_retry<T, F, Fut>(backoff: std::time::Duration, what: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last = None;
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(backoff * attempt as u32).await;
        }
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if is_missing(&e) => return Err(e),
            Err(e) => {
                warn!("Attempt {}/{ATTEMPTS} failed: {what}: {e:#}", attempt + 1);
                last = Some(e);
            }
        }
    }
    Err(last.expect("ATTEMPTS > 0"))
}

/// Stream-download to disk with retry + truncation detection.
///
/// A download whose written byte count falls short of the advertised
/// `Content-Length` is a FAILED transfer (retried), NOT a valid-but-empty
/// archive: the ETH 2024-2025 silent gap was a partial body that decompressed
/// to few/zero ticks and was accepted as an empty month.
pub async fn download_to_file_retry(
    agent: &Arc<btr_rpc::Http>,
    url: &str,
    dst_dir: &Path,
) -> Result<DownloadedArchive> {
    with_retry(BACKOFF, url, || async {
        let dl = download_to_file(agent, url, dst_dir).await?;
        // A truncated `dl` drops here, removing its temp file before the retry.
        anyhow::ensure!(
            !is_truncated(dl.bytes_got, dl.content_length),
            "truncated download {url}: got {} of {} bytes",
            dl.bytes_got,
            dl.content_length.unwrap_or(0)
        );
        Ok(dl)
    })
    .await
}

/// JSON of a public REST endpoint, with the archive retry policy.
async fn rest_json(agent: &Arc<btr_rpc::Http>, url: &str) -> Result<serde_json::Value> {
    with_retry(BACKOFF, url, || async {
        let reply = agent.get(url, &[]).await?;
        anyhow::ensure!(reply.status == 200, "{url}: HTTP {}", reply.status);
        Ok(serde_json::from_slice(&reply.body)?)
    })
    .await
}

/// Does the archive object exist? 404/403 = no; any other failure is an error
/// (unknown), never a "no". A streamed GET whose body is dropped unread: some
/// CDNs answer HEAD with the whole body, which the client refuses over its cap.
async fn head_exists(agent: &Arc<btr_rpc::Http>, url: &str) -> Result<bool> {
    let r = with_retry(BACKOFF, url, || async {
        let (status, _body) = agent.get_stream(url, &[]).await?;
        anyhow::ensure!((200..300).contains(&status), "{url}: HTTP {status}");
        Ok(true)
    })
    .await;
    match r {
        Err(e) if is_missing(&e) => Ok(false),
        r => r,
    }
}

// ─── Tick file I/O (native TickFrame) ─────────────────────────────────────────

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
/// Parse one CSV chunk into ticks. `row` maps a record to a [`Row`], or `None`
/// to skip it: header rows, short rows and unparseable fields all land there,
/// so a chunk that starts on the header or is cut mid-row never panics or
/// poisons the parse. Every venue shares the rest: a non-finite or non-positive
/// price, or a non-finite or negative size, is dropped, and the quote volume is
/// `price * size * scale` (`scale` = base units per contract), truncated to whole
/// quote units, at least 1 (a trade is never sizeless: a cycle of only sub-unit
/// trades must not read as a zero-depth phantom quote) and saturating at `u32::MAX`.
pub fn csv_ticks(csv_data: &[u8], ticker_id: u64, v: &Venue, scale: f64) -> Result<Vec<TickFrame>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true) // column counts vary (Bybit `RPI`)
        .from_reader(std::io::Cursor::new(csv_data));
    let pid = provider_id_for(v.name);
    let mut rows = Vec::new();
    let mut r = csv::StringRecord::new();
    while rdr.read_record(&mut r)? {
        let Some(row) = (v.row)(&r) else {
            continue;
        };
        let notional = row.price * row.size * scale;
        if row.price.is_finite() && row.price > 0.0 && notional.is_finite() && notional >= 0.0 {
            rows.push((row, notional));
        }
    }
    if v.ts_res_us > 0 {
        spread_stamps(&mut rows, v.ts_res_us);
    }
    Ok(rows
        .into_iter()
        .map(|(row, notional)| {
            TickFrame::new(
                pid,
                mitch::timestamp::from_epoch_us(row.us),
                honest_tick(ticker_id, row.price, (notional as u32).max(1), row.buyer),
            )
        })
        .collect())
}

/// Spread each run of rows sharing one stamp evenly over `[stamp, stamp + res)`,
/// in file order, each at the middle of its slot.
fn spread_stamps(rows: &mut [(Row, f64)], res: u64) {
    let mut i = 0;
    while i < rows.len() {
        let us = rows[i].0.us;
        let n = rows[i..].iter().take_while(|(r, _)| r.us == us).count();
        for (k, (row, _)) in rows[i..i + n].iter_mut().enumerate() {
            row.us = us + (2 * k as u64 + 1) * res / (2 * n as u64);
        }
        i += n;
    }
}

/// Epoch ms (a microsecond input is normalised to ms first) -> epoch microseconds.
pub fn ms_to_us(ts: i64) -> u64 {
    normalize_timestamp_ms(ts).max(0) as u64 * 1000
}

/// Fractional epoch seconds -> epoch microseconds; `None` for a non-finite or non-positive value.
pub fn secs_to_us(ts: f64) -> Option<u64> {
    (ts.is_finite() && ts > 0.0).then(|| (ts * 1e6).round() as u64)
}

/// Field `i` of a record parsed as `T`; `None` when missing or malformed.
pub fn field<T: std::str::FromStr>(r: &csv::StringRecord, i: usize) -> Option<T> {
    r.get(i)?.parse().ok()
}

// ─── Date helpers ──────────────────────────────────────────────────────────────

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

// ─── Timestamp normalization ──────────────────────────────────────────────────

/// Normalize a timestamp to milliseconds (handles microsecond timestamps).
#[inline]
pub fn normalize_timestamp_ms(ts: i64) -> i64 {
    if ts > 10_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

// ─── Filesystem helpers ────────────────────────────────────────────────────────

pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

// ─── Download + convert ──────────────────────────────────────────────────────

/// Download archive (streamed to disk), stream-decompress from disk +
/// stream-parse CSV, stream-write the `.ticks` cache file. Peak resident memory
/// is bounded to:
///   * one `DOWNLOAD_STREAM_BUF_BYTES` copy buffer (8 MiB) during download
///   * one `CSV_CHUNK_BYTES` line-aligned chunk (~1 MiB)
///   * one parsed CSV chunk
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
    agent: &Arc<btr_rpc::Http>,
    url: &str,
    cache_path: &Path,
    ticker_id: u64,
    compression: Compression,
    parse_csv: &P,
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
    let archive = download_to_file_retry(agent, url, dst_dir).await?;
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
                    stream_csv_in_chunks(
                        BufReader::with_capacity(64 * 1024, file),
                        CSV_CHUNK_BYTES,
                        &mut feed,
                    )?;
                }
            }
            Compression::Gzip => {
                let gz_file = File::open(&archive_path)?;
                let decoder =
                    flate2::read::GzDecoder::new(BufReader::with_capacity(64 * 1024, gz_file));
                stream_csv_in_chunks(
                    BufReader::with_capacity(64 * 1024, decoder),
                    CSV_CHUNK_BYTES,
                    &mut feed,
                )?;
            }
        }

        writer.flush()?;
        // drop writer + out_file here so the rename below sees a closed fd.
    }

    reverse_if_descending(&tmp_path)?;
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

/// Flip a `.ticks` file written newest-first (Bybit perp archives) to the
/// oldest-first order every downstream reader assumes. Streams backward in
/// bounded blocks, so memory stays O(block) whatever the day's trade count.
fn reverse_if_descending(path: &Path) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    const F: usize = std::mem::size_of::<TickFrame>();
    const BLOCK_FRAMES: usize = 16_384;
    let mut src = File::open(path)?;
    let len = src.metadata()?.len();
    if (len as usize) < 2 * F {
        return Ok(());
    }
    let mut frame = [0u8; F];
    src.read_exact(&mut frame)?;
    let first = bytemuck::pod_read_unaligned::<TickFrame>(&frame).timestamp();
    src.seek(SeekFrom::End(-(F as i64)))?;
    src.read_exact(&mut frame)?;
    if first <= bytemuck::pod_read_unaligned::<TickFrame>(&frame).timestamp() {
        return Ok(());
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".reversed");
    let tmp = PathBuf::from(tmp);
    let mut flipped = || -> Result<()> {
        let mut out = BufWriter::with_capacity(1 << 20, File::create(&tmp)?);
        let mut block: Vec<TickFrame> = vec![bytemuck::Zeroable::zeroed(); BLOCK_FRAMES];
        let mut end = len as usize;
        while end > 0 {
            let n = end.min(BLOCK_FRAMES * F);
            src.seek(SeekFrom::Start((end - n) as u64))?;
            let frames = &mut block[..n / F];
            src.read_exact(bytemuck::cast_slice_mut(frames))?;
            frames.reverse();
            out.write_all(bytemuck::cast_slice(frames))?;
            end -= n;
        }
        out.flush()?;
        Ok(())
    };
    if let Err(e) = flipped() {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, path)?;
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
// ─── Archive fetch: plan, cache, download ────────────────────────────────────

/// Network side of a fetch, injectable so the planning logic runs offline.
#[async_trait::async_trait]
pub trait Net: Sync {
    /// Base units per contract (1.0 when the venue sizes in base units).
    async fn scale(&self, v: &Venue) -> Result<f64>;
    /// Download `url`, convert it to `cache`, return the tick count.
    async fn download(&self, v: &Venue, url: &str, cache: &Path, scale: f64) -> Result<u64>;
    /// Does the archive object exist?
    async fn exists(&self, url: &str) -> Result<bool>;
}

/// The real network.
pub struct Live {
    pub agent: Arc<btr_rpc::Http>,
    pub ticker_id: u64,
}

#[async_trait::async_trait]
impl Net for Live {
    async fn scale(&self, v: &Venue) -> Result<f64> {
        let Some((url, extract)) = &v.contract else {
            return Ok(1.0);
        };
        let k = extract(&rest_json(&self.agent, url).await?)?;
        anyhow::ensure!(
            k.is_finite() && k > 0.0,
            "{}: contract size {k} from {url}",
            v.name
        );
        Ok(k)
    }

    async fn download(&self, v: &Venue, url: &str, cache: &Path, scale: f64) -> Result<u64> {
        let (compression, _) =
            Compression::of(url).context("archive url is not .zip or .csv.gz")?;
        download_and_convert(
            &self.agent,
            url,
            cache,
            self.ticker_id,
            compression,
            &|c, t| csv_ticks(c, t, v, scale),
        )
        .await
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        head_exists(&self.agent, url).await
    }
}

/// Monthly archive to use for `[start, end]` (clipped to one month), if any: a
/// completed month, fetched whole when the window covers it or when the venue
/// has no daily archive to cut it with.
fn monthly_for(
    v: &Venue,
    start: NaiveDate,
    end: NaiveDate,
    today: NaiveDate,
) -> Option<(String, String)> {
    let first = start.with_day(1)?;
    let last = last_day_of_month(start.year(), start.month());
    let covered = start == first && end == last;
    (last < today && (covered || v.files.daily.is_empty()))
        .then(|| v.monthly(first))
        .flatten()
}

fn cache_of(dir: &Path, file: &str) -> PathBuf {
    let (_, stem) = Compression::of(file).expect("validated in Venue::new");
    dir.join(format!("{stem}.ticks"))
}

fn cached(p: &Path) -> bool {
    fs::metadata(p).is_ok_and(|m| m.len() > 0)
}

/// Fetch `[from, to]` (inclusive days) into `dir` and return the `.ticks` files
/// covering it, oldest first. Per calendar month: the monthly archive when the
/// window covers the whole completed month, else daily archives; a monthly that
/// is missing, failed or empty falls back to daily. A file already in `dir` is
/// not fetched again; a zero-tick download is never kept. 404/403 is quiet
/// (`debug`), any other failure that survives the retries is a `warn`.
pub async fn fetch_archive(
    net: &dyn Net,
    v: &Venue,
    dir: &Path,
    from: NaiveDate,
    to: NaiveDate,
    today: NaiveDate,
) -> Result<Fetched> {
    let scale = OnceCell::new();
    // The contract size is looked up on the first download only: a fully cached window needs none.
    let scale = &scale;
    let get = move |url: String, cache: PathBuf| async move {
        let k = *scale.get_or_try_init(|| net.scale(v)).await?;
        Ok::<_, anyhow::Error>(net.download(v, &url, &cache, k).await)
    };
    let mut out = Fetched::default();
    // One outcome per unit fetched, in time order: a monthly file, or a day.
    let mut units: Vec<(String, Unit)> = Vec::new();
    for (start, end) in month_ranges(from, to) {
        let (year, month) = (start.year(), start.month());
        let complete = last_day_of_month(year, month) < today;
        let before = out.files.len();
        let mut ticks = 0u64;
        let mut monthly_fault = None;

        let first = start.with_day(1).expect("day 1 exists");
        // A cached monthly is a superset of any window inside it.
        if let Some((_, file)) = complete.then(|| v.monthly(first)).flatten() {
            let cache = cache_of(dir, &file);
            if cached(&cache) {
                out.files.push(cache);
                units.push((format!("{year}-{month:02}"), Unit::Got));
                continue;
            }
        }
        if let Some((url, file)) = monthly_for(v, start, end, today) {
            let cache = cache_of(dir, &file);
            info!("Downloading monthly: {file}");
            match get(url, cache.clone()).await? {
                Ok(0) => {
                    warn!(
                        "Monthly {file} yielded ZERO ticks; removing empty cache and trying daily"
                    );
                    let _ = fs::remove_file(&cache);
                }
                Ok(_) => {
                    out.files.push(cache);
                    units.push((format!("{year}-{month:02}"), Unit::Got));
                    continue;
                }
                Err(e) if is_missing(&e) => debug!("No monthly {file}, trying daily"),
                Err(e) => {
                    warn!("Monthly failed {file}: {e:#}, trying daily");
                    monthly_fault = Some(format!("monthly {file} failed: {e:#}"));
                }
            }
        }

        if v.files.daily.is_empty() {
            // Monthly-only venue: a month it has not published yet (the current
            // one, or a just-finished one) is held, never finalised without it.
            let lag = last_day_of_month(year, month) + Duration::days(PUBLISH_LAG_DAYS) >= today;
            if let Some(f) = monthly_fault {
                out.faults.push(f);
            } else if out.files.len() == before && (!complete || lag) {
                out.faults.push(format!(
                    "{} publishes monthly files only: {year}-{month:02} is not published yet",
                    v.name
                ));
            }
        } else {
            let mut day = start;
            while day <= end {
                let (n, unit) = fetch_day(&get, v, dir, day, &mut out.files).await?;
                ticks += n;
                units.push((day.to_string(), unit));
                day += Duration::days(1);
            }
        }

        // A completed month that produced nothing is the silent-gap signature of a
        // fetch defect for a liquid pair, not necessarily a real absence: say so.
        if complete && out.files.len() == before && ticks == 0 {
            warn!(
                exchange = v.name,
                symbol = %v.sym,
                year,
                month,
                "no ticks fetched for completed month (monthly + daily both empty) - \
                 possible silent archive/parse gap, NOT necessarily a real data absence",
            );
        }
    }
    // A venue cutting its days east of UTC ends its last file before the window
    // does: the next day's file holds the rest, and the last day is not complete
    // without it.
    let tail = to + Duration::days(1);
    if v.files.tz_h > 0 && tail <= today {
        let (_, unit) = fetch_day(&get, v, dir, tail, &mut out.files).await?;
        let last_got = matches!(units.last(), Some((_, Unit::Got)));
        match unit {
            Unit::Got => {}
            Unit::Failed(e) => out.faults.push(format!("tail day {tail}: {e}")),
            Unit::Missing if last_got => out.faults.push(format!(
                "tail day {tail} is not published, so {to} is incomplete"
            )),
            Unit::Missing => {}
        }
    }
    out.faults.extend(unit_faults(&units));
    Ok(out)
}

/// What a fetch produced: the `.ticks` files covering the window, and why it may
/// be incomplete. Any fault means the venue's data for the window must not be
/// treated as final.
#[derive(Default)]
pub struct Fetched {
    pub files: Vec<PathBuf>,
    pub faults: Vec<String>,
}

/// Archive units are final a few days after their period ends; a missing one
/// inside this lag is "not published yet", not "absent".
const PUBLISH_LAG_DAYS: i64 = 5;

/// Outcome of one fetched unit.
enum Unit {
    Got,
    /// 404/403, or an empty archive.
    Missing,
    /// A failure that survived the retries.
    Failed(String),
}

/// Faults over a run of units: every failure, and every run of two or more
/// missing units between present ones. A single missing day is an archive hole;
/// a run is what a block looks like (403 is also how Bitget says "missing", so
/// status alone cannot tell them apart). Missing units before the first or after
/// the last present one are the venue's listing and archive ends, not faults.
fn unit_faults(units: &[(String, Unit)]) -> Vec<String> {
    let mut faults: Vec<String> = units
        .iter()
        .filter_map(|(l, u)| match u {
            Unit::Failed(e) => Some(format!("{l}: {e}")),
            _ => None,
        })
        .collect();
    let present = |u: &Unit| matches!(u, Unit::Got);
    let (Some(first), Some(last)) = (
        units.iter().position(|(_, u)| present(u)),
        units.iter().rposition(|(_, u)| present(u)),
    ) else {
        return faults;
    };
    let mut run = 0;
    for (l, u) in &units[first..=last] {
        if matches!(u, Unit::Missing) {
            run += 1;
            if run == 2 {
                faults.push(format!(
                    "missing run inside coverage from {l} (block or archive hole)"
                ));
            }
        } else {
            run = 0;
        }
    }
    faults
}

/// Fetch one archive day (one file, or numbered chunks until one is missing or
/// empty) into `files`; returns the ticks downloaded and the day's outcome.
async fn fetch_day<G, Fut>(
    get: &G,
    v: &Venue,
    dir: &Path,
    day: NaiveDate,
    files: &mut Vec<PathBuf>,
) -> Result<(u64, Unit)>
where
    G: Fn(String, PathBuf) -> Fut,
    Fut: std::future::Future<Output = Result<Result<u64>>>,
{
    let mut ticks = 0;
    let mut seq = 1;
    let mut got = false;
    while let Some((url, file)) = v.daily(day, seq) {
        let cache = cache_of(dir, &file);
        if cached(&cache) {
            files.push(cache);
            got = true;
        } else {
            match get(url, cache.clone()).await? {
                Ok(0) => {
                    let _ = fs::remove_file(&cache);
                    debug!("Empty {} archive {file}", v.name);
                    break;
                }
                Ok(n) => {
                    ticks += n;
                    files.push(cache);
                    got = true;
                }
                Err(e) if is_missing(&e) => {
                    debug!("No {} data for {day} chunk {seq}", v.name);
                    break;
                }
                Err(e) => {
                    warn!("{} {} {day} failed after retries: {e:#}", v.name, v.sym);
                    return Ok((ticks, Unit::Failed(format!("{file}: {e:#}"))));
                }
            }
        }
        if !v.chunked() {
            break;
        }
        seq += 1;
    }
    Ok((ticks, if got { Unit::Got } else { Unit::Missing }))
}

/// Upper bound on probe requests per (venue, symbol): a longer window is sampled
/// at an even stride so a dead symbol costs a bounded number of HEADs.
const PROBE_MAX: usize = 120;

/// First and last archive date found in `[from, to]`, `None` when the venue
/// publishes nothing for the symbol there. Probes the units [`fetch_archive`]
/// would fetch (monthly where it would, else daily), newest first then oldest
/// first, stopping at the first hit; `Err` means a transport fault, not "absent".
pub async fn probe_archive(
    net: &dyn Net,
    v: &Venue,
    from: NaiveDate,
    to: NaiveDate,
    today: NaiveDate,
) -> Result<Option<(NaiveDate, NaiveDate)>> {
    let mut units: Vec<(NaiveDate, String)> = Vec::new();
    for (start, end) in month_ranges(from, to) {
        if let Some((url, _)) = monthly_for(v, start, end, today) {
            units.push((start.with_day(1).expect("day 1 exists"), url));
        } else {
            let mut day = start;
            while day <= end {
                units.extend(v.daily(day, 1).map(|(url, _)| (day, url)));
                day += Duration::days(1);
            }
        }
    }
    let stride = units.len().div_ceil(PROBE_MAX).max(1);
    let n = units.len();
    let sample: Vec<&(NaiveDate, String)> = units
        .iter()
        .enumerate()
        .filter(|(i, _)| i % stride == 0 || *i == n - 1)
        .map(|(_, u)| u)
        .collect();
    for last in sample.iter().rev() {
        if net.exists(&last.1).await? {
            for first in &sample {
                if net.exists(&first.1).await? {
                    return Ok(Some((first.0, last.0)));
                }
            }
        }
    }
    Ok(None)
}

/// The single [`TickSource`] of every archive venue; the venue picks its layout per (base, quote).
pub struct ArchiveSource {
    agent: Arc<btr_rpc::Http>,
    /// `Ok(None)`: the venue publishes no archive for this market.
    venue: fn(&Config) -> Result<Option<Venue>>,
}

impl ArchiveSource {
    pub fn new(venue: fn(&Config) -> Result<Option<Venue>>) -> Self {
        Self {
            agent: http_agent(),
            venue,
        }
    }
}

#[async_trait::async_trait]
impl TickSource for ArchiveSource {
    async fn fetch_ticks(&self, config: &Config, _tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        let v = (self.venue)(config)?
            .with_context(|| format!("no archive for {} on this venue", config.quote))?;
        anyhow::ensure!(
            !excluded(v.name, &config.base, &config.quote)?,
            "{}/{} is in {}.exclude_symbols",
            config.base,
            config.quote,
            v.name
        );
        let (dir, ticker_id) = series_key(config);
        info!("Fetching {} data for {}", v.name, v.sym);
        let net = Live {
            agent: self.agent.clone(),
            ticker_id,
        };
        let fetched = fetch_archive(
            &net,
            &v,
            &config.ticks_dir.join(v.name).join(dir),
            config.from.date_naive(),
            config.to.date_naive(),
            chrono::Utc::now().date_naive(),
        )
        .await?;
        info!("{} tick files for {}", fetched.files.len(), v.sym);
        // The files are the output; no tick batches are sent. A fault fails the
        // fetch so the caller never treats the window as final.
        anyhow::ensure!(
            fetched.faults.is_empty(),
            "{} {} incomplete: {}",
            v.name,
            v.sym,
            fetched.faults.join("; ")
        );
        Ok(())
    }

    async fn probe(&self, config: &Config) -> Result<Option<(NaiveDate, NaiveDate)>> {
        let Some(v) = (self.venue)(config)? else {
            return Ok(None);
        };
        if excluded(v.name, &config.base, &config.quote)? {
            return Ok(None);
        }
        let net = Live {
            agent: self.agent.clone(),
            ticker_id: series_key(config).1,
        };
        probe_archive(
            &net,
            &v,
            config.from.date_naive(),
            config.to.date_naive(),
            chrono::Utc::now().date_naive(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    /// Truncation predicate: short-of-Content-Length ⇒ truncated (failed
    /// transfer, retried), exact/over ⇒ OK, unknown length ⇒ OK (can't judge).
    /// This is the rule that stops a partial ETH archive being accepted as an
    /// empty month.
    #[test]
    fn is_truncated_predicate() {
        assert!(is_truncated(100, Some(200)), "short body must be truncated");
        assert!(!is_truncated(200, Some(200)), "exact body is complete");
        assert!(
            !is_truncated(201, Some(200)),
            "over-read (chunked) not truncated"
        );
        assert!(
            !is_truncated(0, None),
            "no Content-Length ⇒ cannot flag truncation"
        );
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
        assert_eq!(
            decode_lines(&gz).unwrap(),
            3,
            "complete gz must decode all rows"
        );

        // Truncated archive (drop trailing CRC/length + payload) → decode errors.
        let truncated = &gz[..gz.len() / 2];
        assert!(
            decode_lines(truncated).is_err(),
            "truncated gz must fail to decompress, not yield a partial-accepted result",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn frames(ms: &[i64]) -> Vec<TickFrame> {
        ms.iter()
            .map(|&t| {
                TickFrame::new(
                    1,
                    mitch::timestamp::from_epoch_ms(t),
                    honest_tick(7, 100.0, 1, true),
                )
            })
            .collect()
    }

    /// Write `ms` as a `.ticks` file, run the flip, return the timestamps and
    /// whether the `.reversed` scratch file is gone.
    fn flip(ms: &[i64]) -> (Vec<i64>, bool) {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "sf-rev-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("d.ticks.partial");
        std::fs::write(&p, bytemuck::cast_slice(&frames(ms))).unwrap();
        reverse_if_descending(&p).unwrap();
        let got = read_tick_file(&p)
            .unwrap()
            .iter()
            .map(|f| f.timestamp_ms())
            .collect();
        let clean = !dir.join("d.ticks.partial.reversed").exists();
        let _ = std::fs::remove_dir_all(&dir);
        (got, clean)
    }

    const T0: i64 = 1_800_000_000_000;

    /// A newest-first file (Bybit perp) is flipped across several blocks.
    #[test]
    fn descending_ticks_file_is_reversed() {
        let desc: Vec<i64> = (0..40_000).rev().map(|i| T0 + i * 1000).collect();
        let mut asc = desc.clone();
        asc.reverse();
        assert_eq!(flip(&desc), (asc, true));
    }

    #[test]
    fn ascending_single_and_equal_timestamp_files_are_untouched() {
        let asc: Vec<i64> = (0..10).map(|i| T0 + i * 1000).collect();
        assert_eq!(flip(&asc), (asc.clone(), true));
        assert_eq!(flip(&[T0]), (vec![T0], true));
        let same = vec![T0; 5];
        assert_eq!(flip(&same), (same.clone(), true));
    }

    /// Newest-first with ties and one out-of-place row: first > last flips the
    /// whole file; no row is lost or invented.
    #[test]
    fn partially_descending_file_is_flipped_whole() {
        let v = vec![T0 + 5000, T0 + 4000, T0 + 4000, T0 + 4500, T0 + 2000, T0];
        let mut want = v.clone();
        want.reverse();
        assert_eq!(flip(&v), (want, true));
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
    fn http_err(code: u16) -> anyhow::Error {
        anyhow::anyhow!("http u: HTTP {code}")
    }

    /// Retry/missing classification: the missing codes are final, anything else is
    /// retried up to `ATTEMPTS` and the last error surfaces.
    #[tokio::test]
    async fn retry_stops_at_missing_and_exhausts_on_faults() {
        let zero = std::time::Duration::ZERO;
        for (code, want) in [(404, 1), (403, 1), (500, ATTEMPTS), (429, ATTEMPTS)] {
            let n = AtomicUsize::new(0);
            let r: Result<()> = with_retry(zero, "t", || async {
                n.fetch_add(1, Ordering::Relaxed);
                Err(http_err(code))
            })
            .await;
            assert_eq!(n.load(Ordering::Relaxed), want, "HTTP {code}");
            assert_eq!(is_missing(&r.unwrap_err()), want == 1);
        }
        let n = AtomicUsize::new(0);
        let r = with_retry(zero, "t", || async {
            if n.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(http_err(502))
            } else {
                Ok(7)
            }
        })
        .await;
        assert_eq!((r.unwrap(), n.load(Ordering::Relaxed)), (7, 2));
    }

    /// Offline network: replies by file name, records every request.
    struct Fake {
        reply: fn(&str) -> Result<u64>,
        calls: Mutex<Vec<String>>,
        scales: AtomicUsize,
    }

    impl Fake {
        fn new(reply: fn(&str) -> Result<u64>) -> Self {
            Self {
                reply,
                calls: Mutex::new(vec![]),
                scales: AtomicUsize::new(0),
            }
        }
        fn calls(&self) -> Vec<String> {
            std::mem::take(&mut self.calls.lock().unwrap())
        }
    }

    #[async_trait::async_trait]
    impl Net for Fake {
        async fn scale(&self, _: &Venue) -> Result<f64> {
            self.scales.fetch_add(1, Ordering::Relaxed);
            Ok(1.0)
        }
        async fn download(&self, _: &Venue, url: &str, cache: &Path, _: f64) -> Result<u64> {
            self.calls
                .lock()
                .unwrap()
                .push(url.rsplit('/').next().unwrap().into());
            let n = (self.reply)(url)?;
            if n > 0 {
                ensure_parent_dir(cache)?;
                fs::write(cache, b"t")?;
            }
            Ok(n)
        }
        async fn exists(&self, url: &str) -> Result<bool> {
            self.calls
                .lock()
                .unwrap()
                .push(url.rsplit('/').next().unwrap().into());
            match (self.reply)(url) {
                Ok(_) => Ok(true),
                Err(e) if is_missing(&e) => Ok(false),
                Err(e) => Err(e),
            }
        }
    }

    const BOTH: Files = Files {
        monthly: "{sym}-{y:04}-{m:02}.zip",
        daily: "{sym}-{y:04}-{m:02}-{d:02}.zip",
        tz_h: 0,
    };

    fn venue(files: Files) -> Venue {
        Venue {
            name: "x",
            sym: "BTCUSDT".into(),
            perp: false,
            files,
            stems: Stems {
                monthly: "http://h/m/{sym}/".into(),
                daily: "http://h/d/{sym}/".into(),
            },
            row: |_| None,
            product: "",
            ts_res_us: 0,
            contract: None,
        }
    }

    fn tmp(tag: &str) -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "sf-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn ok(_: &str) -> Result<u64> {
        Ok(5)
    }

    /// Run one fetch; returns (requests, cached file names).
    async fn run(
        files: Files,
        reply: fn(&str) -> Result<u64>,
        from: NaiveDate,
        to: NaiveDate,
        today: NaiveDate,
    ) -> (Vec<String>, Vec<String>) {
        let (net, dir) = (Fake::new(reply), tmp("fetch"));
        let got = fetch_archive(&net, &venue(files), &dir, from, to, today)
            .await
            .unwrap()
            .files;
        let _ = fs::remove_dir_all(&dir);
        let names = got
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into())
            .collect();
        (net.calls(), names)
    }

    /// A completed month the window covers is one monthly request; a partial
    /// window inside it is cut with daily archives and never downloads the month.
    #[tokio::test]
    async fn granularity_follows_window_coverage() {
        let today = d(2024, 3, 1);
        let (calls, files) = run(BOTH, ok, d(2024, 1, 1), d(2024, 1, 31), today).await;
        assert_eq!(calls, ["BTCUSDT-2024-01.zip"]);
        assert_eq!(files, ["BTCUSDT-2024-01.ticks"]);
        let (calls, files) = run(BOTH, ok, d(2024, 1, 5), d(2024, 1, 6), today).await;
        assert_eq!(calls, ["BTCUSDT-2024-01-05.zip", "BTCUSDT-2024-01-06.zip"]);
        assert_eq!(files.len(), 2);
    }

    /// A monthly that is missing, failed or empty falls back to the dailies.
    #[tokio::test]
    async fn monthly_miss_fault_or_empty_falls_back_to_daily() {
        let today = d(2024, 3, 1);
        let cases: [fn(&str) -> Result<u64>; 3] = [
            |u| {
                if u.contains("/m/") {
                    Err(http_err(404))
                } else {
                    Ok(5)
                }
            },
            |u| {
                if u.contains("/m/") {
                    Err(http_err(500))
                } else {
                    Ok(5)
                }
            },
            |u| Ok(if u.contains("/m/") { 0 } else { 5 }),
        ];
        for reply in cases {
            let (calls, files) = run(BOTH, reply, d(2024, 2, 1), d(2024, 2, 29), today).await;
            assert_eq!(calls[0], "BTCUSDT-2024-02.zip");
            assert_eq!(files.len(), 29, "29 daily files after the monthly miss");
        }
    }

    /// The current month has no monthly archive; a day the archive lacks (today) is skipped.
    #[tokio::test]
    async fn current_month_is_daily_and_a_missing_day_is_skipped() {
        let reply: fn(&str) -> Result<u64> = |u| {
            if u.ends_with("03-03.zip") {
                Err(http_err(404))
            } else {
                Ok(5)
            }
        };
        let (calls, files) = run(BOTH, reply, d(2024, 3, 1), d(2024, 3, 3), d(2024, 3, 3)).await;
        assert_eq!(calls.len(), 3);
        assert!(calls
            .iter()
            .all(|c| c.len() == "BTCUSDT-2024-03-01.zip".len()));
        assert_eq!(files.len(), 2);
    }

    /// A monthly-only venue fetches the whole month for any window inside it,
    /// and has nothing for a month that is not complete.
    #[tokio::test]
    async fn monthly_only_venue() {
        let only = Files { daily: "", ..BOTH };
        let (calls, _) = run(only, ok, d(2024, 1, 5), d(2024, 1, 6), d(2024, 3, 1)).await;
        assert_eq!(calls, ["BTCUSDT-2024-01.zip"]);
        let (calls, files) = run(only, ok, d(2024, 3, 1), d(2024, 3, 2), d(2024, 3, 3)).await;
        assert!((calls, files) == (vec![], vec![]));
    }

    /// Chunked daily archives are read in order until a chunk is missing or empty.
    #[tokio::test]
    async fn chunked_daily_stops_at_missing_or_empty_chunk() {
        let chunked = Files {
            monthly: "",
            daily: "{ds}_{seq:03}.zip",
            tz_h: 0,
        };
        let reply: fn(&str) -> Result<u64> = |u| match u.rsplit('_').next().unwrap() {
            "001.zip" | "002.zip" => Ok(5),
            "003.zip" => Ok(0),
            _ => Err(http_err(403)),
        };
        let (calls, files) = run(chunked, reply, d(2024, 1, 5), d(2024, 1, 5), d(2024, 3, 1)).await;
        assert_eq!(
            calls,
            ["20240105_001.zip", "20240105_002.zip", "20240105_003.zip"]
        );
        assert_eq!(files, ["20240105_001.ticks", "20240105_002.ticks"]);
    }

    /// A venue whose days run east of UTC also needs the day after the window,
    /// unless that day has not started yet.
    #[tokio::test]
    async fn east_of_utc_venue_fetches_the_next_day_too() {
        let east = Files { tz_h: 8, ..BOTH };
        let (calls, _) = run(east, ok, d(2024, 1, 5), d(2024, 1, 6), d(2024, 3, 1)).await;
        assert_eq!(calls.len(), 3);
        assert!(calls[2].ends_with("01-07.zip"));
        let (calls, _) = run(east, ok, d(2024, 3, 1), d(2024, 3, 2), d(2024, 3, 2)).await;
        assert_eq!(calls.len(), 2, "03-03 has not started");
    }

    /// Faults of one fetch.
    async fn faults(
        files: Files,
        reply: fn(&str) -> Result<u64>,
        from: NaiveDate,
        to: NaiveDate,
        today: NaiveDate,
    ) -> Vec<String> {
        let (net, dir) = (Fake::new(reply), tmp("faults"));
        let got = fetch_archive(&net, &venue(files), &dir, from, to, today)
            .await
            .unwrap();
        let _ = fs::remove_dir_all(&dir);
        got.faults
    }

    /// A failure that survives the retries makes the fetch incomplete, in the
    /// middle of a chunked day as well as on a plain day.
    #[tokio::test]
    async fn a_failed_download_is_a_fault() {
        let today = d(2024, 3, 1);
        let f = faults(
            BOTH,
            |u| {
                if u.ends_with("01-06.zip") {
                    Err(http_err(500))
                } else {
                    Ok(5)
                }
            },
            d(2024, 1, 5),
            d(2024, 1, 7),
            today,
        )
        .await;
        assert_eq!(f.len(), 1, "{f:?}");
        let chunked = Files {
            monthly: "",
            daily: "{ds}_{seq:03}.zip",
            tz_h: 0,
        };
        let f = faults(
            chunked,
            |u| match u.rsplit('_').next().unwrap() {
                "001.zip" => Ok(5),
                _ => Err(http_err(502)),
            },
            d(2024, 1, 5),
            d(2024, 1, 5),
            today,
        )
        .await;
        assert_eq!(f.len(), 1, "half a day is a fault: {f:?}");
    }

    /// One missing day between present ones is an archive hole; two in a row look
    /// like a block. Missing days before the first or after the last present one
    /// are the venue's listing and archive ends.
    #[tokio::test]
    async fn missing_runs_inside_coverage_are_faults() {
        let today = d(2024, 3, 1);
        let run = |reply: fn(&str) -> Result<u64>| {
            faults(BOTH, reply, d(2024, 1, 3), d(2024, 1, 9), today)
        };
        let miss =
            |u: &str, days: &[&str]| days.iter().any(|d| u.ends_with(&format!("01-{d}.zip")));
        let hole = run(|u| {
            if u.ends_with("01-05.zip") {
                Err(http_err(404))
            } else {
                Ok(5)
            }
        })
        .await;
        assert!(hole.is_empty(), "{hole:?}");
        let block = run(|u| {
            if u.ends_with("01-05.zip") || u.ends_with("01-06.zip") {
                Err(http_err(403))
            } else {
                Ok(5)
            }
        })
        .await;
        assert_eq!(block.len(), 1, "{block:?}");
        let ends = run(|u| {
            if u.ends_with("01-03.zip")
                || u.ends_with("01-04.zip")
                || u.ends_with("01-08.zip")
                || u.ends_with("01-09.zip")
            {
                Err(http_err(403))
            } else {
                Ok(5)
            }
        })
        .await;
        assert!(ends.is_empty(), "{ends:?}");
    }

    /// An east-of-UTC venue's last day is incomplete without the next day's file.
    #[tokio::test]
    async fn missing_tail_file_is_a_fault() {
        let east = Files { tz_h: 8, ..BOTH };
        let today = d(2024, 3, 1);
        let f = faults(
            east,
            |u| {
                if u.ends_with("01-07.zip") {
                    Err(http_err(403))
                } else {
                    Ok(5)
                }
            },
            d(2024, 1, 5),
            d(2024, 1, 6),
            today,
        )
        .await;
        assert_eq!(f.len(), 1, "{f:?}");
        assert!(faults(east, ok, d(2024, 1, 5), d(2024, 1, 6), today)
            .await
            .is_empty());
    }

    /// A monthly-only venue's month is held until it is published and old enough.
    #[tokio::test]
    async fn monthly_only_month_is_held_until_published() {
        let only = Files { daily: "", ..BOTH };
        let miss: fn(&str) -> Result<u64> = |_| Err(http_err(404));
        // current month, and a month that ended 2 days ago: held
        assert_eq!(
            faults(only, miss, d(2024, 3, 1), d(2024, 3, 2), d(2024, 3, 3))
                .await
                .len(),
            1
        );
        assert_eq!(
            faults(only, miss, d(2024, 2, 1), d(2024, 2, 29), d(2024, 3, 2))
                .await
                .len(),
            1
        );
        // an old month it never had: absent, not held
        assert!(
            faults(only, miss, d(2024, 1, 1), d(2024, 1, 31), d(2024, 3, 20))
                .await
                .is_empty()
        );
        assert!(
            faults(only, ok, d(2024, 2, 1), d(2024, 2, 29), d(2024, 3, 2))
                .await
                .is_empty()
        );
    }

    /// Cached files are never refetched; the contract size is looked up once, on
    /// the first download, and not at all for a fully cached window.
    #[tokio::test]
    async fn cache_skips_download_and_contract_lookup() {
        let (net, dir) = (Fake::new(ok), tmp("cache"));
        let v = venue(BOTH);
        let window = (d(2024, 1, 5), d(2024, 1, 7), d(2024, 3, 1));
        for expect in [(3, 1), (0, 1)] {
            fetch_archive(&net, &v, &dir, window.0, window.1, window.2)
                .await
                .unwrap();
            assert_eq!(
                (net.calls().len(), net.scales.load(Ordering::Relaxed)),
                expect
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Probe: first and last archive in the window; none for a dead symbol; a
    /// transport fault is an error, never "absent".
    #[tokio::test]
    async fn probe_reports_coverage_and_faults() {
        let v = venue(BOTH);
        let (from, to, today) = (d(2024, 1, 5), d(2024, 1, 9), d(2024, 3, 1));
        let net = Fake::new(|u| {
            if u.ends_with("01-07.zip") || u.ends_with("01-08.zip") {
                Ok(1)
            } else {
                Err(http_err(404))
            }
        });
        let got = probe_archive(&net, &v, from, to, today).await.unwrap();
        assert_eq!(got, Some((d(2024, 1, 7), d(2024, 1, 8))));
        let dead = Fake::new(|_| Err(http_err(403)));
        assert_eq!(
            probe_archive(&dead, &v, from, to, today).await.unwrap(),
            None
        );
        let flaky = Fake::new(|_| Err(http_err(503)));
        assert!(probe_archive(&flaky, &v, from, to, today).await.is_err());
    }

    /// Trades sharing a coarse stamp are spread over it, in order, mid-slot.
    #[test]
    fn coarse_stamps_are_spread_over_their_interval() {
        let v = Venue {
            name: "binance",
            row: |r| {
                Some(Row {
                    us: ms_to_us(field(r, 0)?),
                    price: 100.0,
                    size: 1.0,
                    buyer: true,
                })
            },
            ..venue(BOTH).with_ts_res(1_000_000)
        };
        let t = csv_ticks(
            b"1700000000000\n1700000000000\n1700000000000\n1700000000000\n1700000001000\n",
            7,
            &v,
            1.0,
        )
        .unwrap();
        let ms: Vec<i64> = t.iter().map(|f| f.timestamp_ms()).collect();
        let want: Vec<i64> = [125, 375, 625, 875, 1500]
            .iter()
            .map(|d| 1_700_000_000_000 + d)
            .collect();
        assert!(
            ms.iter().zip(&want).all(|(a, b)| (a - b).abs() <= 1),
            "{ms:?}"
        );
    }

    /// Placeholders expand identically in stems and file names.
    #[test]
    fn placeholders_expand() {
        let v = Venue {
            product: "UMCBL",
            ..venue(Files {
                monthly: "",
                daily: "{ds}_{seq:03}.zip",
                tz_h: 0,
            })
        };
        let day = d(2024, 1, 5);
        assert_eq!(
            expand(
                "{product}/{sym}/{y:04}{m:02}{d:02}/{ds}_{seq:03}",
                &v,
                day,
                2
            ),
            "UMCBL/BTCUSDT/20240105/20240105_002"
        );
    }

    /// Shared validation: non-finite or non-positive prices, non-finite or
    /// negative sizes and non-finite timestamps never become ticks.
    #[test]
    fn non_finite_and_invalid_fields_are_rejected() {
        let v = Venue {
            name: "binance",
            row: |r| {
                Some(Row {
                    us: secs_to_us(field(r, 0)?)?,
                    price: field(r, 1)?,
                    size: field(r, 2)?,
                    buyer: true,
                })
            },
            ..venue(BOTH)
        };
        let csv = "1700000000.5,100,1\n\
                   inf,100,1\n\
                   NaN,100,1\n\
                   1700000000.5,inf,1\n\
                   1700000000.5,NaN,1\n\
                   1700000000.5,0,1\n\
                   1700000000.5,-5,1\n\
                   1700000000.5,100,inf\n\
                   1700000000.5,100,NaN\n\
                   1700000000.5,100,-1\n\
                   1700000000.5,100,1e400\n\
                   1700000000.5,1e308,1e308\n";
        let t = csv_ticks(csv.as_bytes(), 7, &v, 1.0).unwrap();
        assert_eq!(t.len(), 1, "only the first row is valid");
        assert_eq!({ t[0].body.vask }, 100);
        let t = csv_ticks(b"1700000000.5,100,0.001\n", 7, &v, 1.0).unwrap();
        assert_eq!(
            { t[0].body.vask },
            1,
            "a sub-unit trade keeps one unit of size"
        );
    }
}
