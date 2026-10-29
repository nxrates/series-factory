//! Peer gap fill: refill holes in local `.idx` / `.s10` shards from other NXR
//! nodes' range reads (`/v1/idx`, `/v1/bars`).
//!
//! Rules (each one is enforced in code and covered by a test):
//! - Gap only: existing records are never rewritten or reordered, only merged
//!   with rows that fall strictly inside a hole. Verified before the rename.
//! - Never today's shard (live writer owns it) and never a day >= today.
//! - Whole hole per source, never stitched across sources. Sources are tried in
//!   the order given; a source whose rows fail any check is rejected and the
//!   next is tried. No source passes => the hole is reported `uncovered`.
//! - Trust is self-consistency (shards are not byte-identical across nodes):
//!   whole records, in window, monotone, stride, ticker, sane prices, both
//!   hole edges covered, price at each seam within tolerance of the local
//!   neighbour.
//! - Closed days only: a hole is applied only once its day ended
//!   `CLOSE_GRACE_MS` ago, so the live writer (which owns today's shard and,
//!   across a rollover, the tail of yesterday's) can never touch the same
//!   bytes. The directory-wide `.writer.lock` is NOT taken: the live
//!   aggregator holds it for its whole life, so honouring it made apply
//!   impossible on a running node. Concurrent peer-fill runs serialise on a
//!   private `.peerfill.lock`; a held lock skips the ticker.
//! - Every filled range lands in the `backfill.json` sidecar next to the shards.
//!
//! Light (`sign_only`) peers persist `.idx` only for the retention window
//! (`signed_quotes.retention_days`, 1 on today's signer configs) and only for
//! tickers that are not composed. Outside that window or set `/v1/idx`
//! answers `200 []`, which a light peer is reported as `empty` for and never
//! fills. `/v1/ohlc` is derived from idx/bars and has no shard on disk, so it
//! is not a fill source.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;
use mitch::bar::Bar;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{
    date_stem, day_start_ms, list_shards, manifest_path, read_manifest, read_shard_aligned,
    shard_entry, shard_path, write_manifest, Manifest, ShardRecord, MS_PER_DAY,
};
use serde::{Deserialize, Serialize};

/// Server page cap (`MAX_LIMIT` in core `rest.rs`).
const PAGE: usize = 10_000;
const MAX_PAGES: usize = 500;
const MAX_BODY: u64 = 64 << 20;
/// A day is closed for peer-fill this long after midnight UTC: covers the live
/// writer's rollover flush and `finalize_manifest` of the finished day.
pub const CLOSE_GRACE_MS: i64 = 600_000;
const PEERFILL_LOCK: &str = ".peerfill.lock";

/// Exclusive non-blocking lock serialising peer-fill runs on one ticker dir.
/// Deliberately not `.writer.lock` (see module docs).
fn acquire_peerfill_lock(dir: &Path) -> Result<fs::File> {
    use fs2::FileExt;
    fs::create_dir_all(dir).with_context(|| format!("create_dir_all {}", dir.display()))?;
    let p = dir.join(PEERFILL_LOCK);
    let f = fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&p)
        .with_context(|| format!("open {}", p.display()))?;
    f.try_lock_exclusive().map_err(|e| anyhow!("{} held by another peer-fill run: {e}", p.display()))?;
    Ok(f)
}

// -- row abstraction ----------------------------------------------------------

pub trait PeerRow: ShardRecord + Copy {
    /// Shard extension and manifest kind.
    const EXT: &'static str;
    /// `/v1/<path>` segment builder.
    fn url(sym: &str) -> String;
    /// Slack (ms) between a record's shard ts and the key the server filters on.
    const PAD_MS: i64;
    /// Key the server pages on (`from`/`to`).
    fn cursor_ms(&self) -> i64;
    fn px(&self) -> f64;
    fn ticker(&self) -> Option<u64>;
    /// Field sanity (finite, positive, ordered).
    fn sane(&self) -> bool;
    /// Allowed step from the previous accepted row.
    fn step_ok(prev: &Self, cur: &Self) -> bool;
}

impl PeerRow for IndexRecord {
    const EXT: &'static str = "idx";
    const PAD_MS: i64 = 0;
    fn url(sym: &str) -> String {
        format!("/v1/idx/{sym}")
    }
    fn cursor_ms(&self) -> i64 {
        self.shard_ts_ms()
    }
    fn px(&self) -> f64 {
        let (b, a) = (self.index.bid, self.index.ask);
        (b + a) / 2.0
    }
    fn ticker(&self) -> Option<u64> {
        Some(self.index.ticker)
    }
    fn sane(&self) -> bool {
        let (b, a) = (self.index.bid, self.index.ask);
        b.is_finite() && a.is_finite() && b > 0.0 && a > 0.0
    }
    fn step_ok(prev: &Self, cur: &Self) -> bool {
        cur.shard_ts_ms() >= prev.shard_ts_ms()
    }
}

impl PeerRow for Bar {
    const EXT: &'static str = "s10";
    const PAD_MS: i64 = 10_000;
    fn url(sym: &str) -> String {
        format!("/v1/bars/{sym}/kline")
    }
    fn cursor_ms(&self) -> i64 {
        self.open_time_ms()
    }
    fn px(&self) -> f64 {
        self.close
    }
    fn ticker(&self) -> Option<u64> {
        None
    }
    fn sane(&self) -> bool {
        let (o, h, l, c) = (self.open, self.high, self.low, self.close);
        [o, h, l, c].iter().all(|v| v.is_finite() && *v > 0.0)
            && h >= l
            && o >= l
            && o <= h
            && c >= l
            && c <= h
            && self.close_time_ms() > self.open_time_ms()
    }
    fn step_ok(prev: &Self, cur: &Self) -> bool {
        let d = cur.open_time_ms() - prev.open_time_ms();
        d > 0 && d % 10_000 == 0
    }
}

// -- gaps ---------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Anchor {
    pub ts: i64,
    pub px: f64,
    pub ticker: Option<u64>,
}

fn anchor<T: PeerRow>(r: &T) -> Anchor {
    Anchor { ts: r.shard_ts_ms(), px: r.px(), ticker: r.ticker() }
}

/// A hole `[start, end)` (ms) inside one UTC day, with the local records on
/// each side (when they exist).
#[derive(Clone, Debug)]
pub struct Gap {
    pub date: NaiveDate,
    pub start: i64,
    pub end: i64,
    pub left: Option<Anchor>,
    pub right: Option<Anchor>,
}

fn first_anchor<T: PeerRow>(p: &Path) -> Option<Anchor> {
    read_shard_aligned::<T>(p).ok()?.first().map(anchor)
}
fn last_anchor<T: PeerRow>(p: &Path) -> Option<Anchor> {
    read_shard_aligned::<T>(p).ok()?.last().map(anchor)
}

/// Holes in `dir`'s `T` shards for closed days in `[from, min(to, today-1)]`.
///
/// A hole is a missing day, a head/tail shortfall, or an interior ts step wider
/// than `min_gap_ms`. Days before the first local shard are pre-history, not
/// holes. Today and later are never reported.
pub fn detect_gaps<T: PeerRow>(
    dir: &Path,
    from: NaiveDate,
    to: NaiveDate,
    today: NaiveDate,
    min_gap_ms: i64,
) -> Result<Vec<Gap>> {
    let shards = list_shards(dir, T::EXT)?;
    let Some((first_date, _)) = shards.first() else {
        return Ok(Vec::new());
    };
    let by_date: BTreeMap<NaiveDate, &PathBuf> = shards.iter().map(|(d, p)| (*d, p)).collect();
    let last_day = to.min(today.pred_opt().unwrap_or(today));
    let mut out = Vec::new();
    let mut d = from.max(*first_date);
    while d <= last_day {
        let day_start = day_start_ms(d);
        let day_end = day_start + MS_PER_DAY;
        let prev = shards.iter().rev().find(|(sd, _)| *sd < d);
        let next = shards.iter().find(|(sd, _)| *sd > d);
        let left_day = prev.and_then(|(_, p)| last_anchor::<T>(p));
        let right_day = next.and_then(|(_, p)| first_anchor::<T>(p));
        let recs = match by_date.get(&d) {
            Some(p) => read_shard_aligned::<T>(p)?,
            None => Vec::new(),
        };
        if recs.is_empty() {
            if prev.is_some() {
                out.push(Gap { date: d, start: day_start, end: day_end, left: left_day, right: right_day });
            }
        } else {
            let ts: Vec<i64> = recs.iter().map(|r| r.shard_ts_ms()).collect();
            if prev.is_some() && ts[0] - day_start > min_gap_ms {
                out.push(Gap {
                    date: d,
                    start: day_start,
                    end: ts[0],
                    left: left_day,
                    right: Some(anchor(&recs[0])),
                });
            }
            for i in 1..recs.len() {
                if ts[i] - ts[i - 1] > min_gap_ms {
                    out.push(Gap {
                        date: d,
                        start: ts[i - 1] + 1,
                        end: ts[i],
                        left: Some(anchor(&recs[i - 1])),
                        right: Some(anchor(&recs[i])),
                    });
                }
            }
            let last = *ts.last().unwrap();
            if day_end - last > min_gap_ms {
                out.push(Gap {
                    date: d,
                    start: last + 1,
                    end: day_end,
                    left: Some(anchor(recs.last().unwrap())),
                    right: right_day,
                });
            }
        }
        d = d.succ_opt().unwrap();
    }
    Ok(out)
}

// -- peer transport -----------------------------------------------------------

pub struct PeerResponse {
    pub body: Vec<u8>,
    /// `x-nxr-composed: 1` (bars synthesised on read, not persisted).
    pub composed: bool,
}

pub trait PeerClient {
    /// One range read; `Err` = unreachable / non-200.
    fn get(&self, peer: &str, path_and_query: &str) -> Result<PeerResponse>;
}

/// `ureq` client. The key is read from the env var NAMED by the caller and only
/// ever placed in the `x-nxr-key` header; it is never logged or echoed.
pub struct HttpPeer {
    agent: ureq::Agent,
    key: Option<String>,
}

impl HttpPeer {
    pub fn new(key_env: &str) -> Self {
        let key = std::env::var(key_env).ok().filter(|k| !k.trim().is_empty());
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(3))
            .timeout_read(Duration::from_secs(30))
            .build();
        Self { agent, key }
    }
    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }
}

impl PeerClient for HttpPeer {
    fn get(&self, peer: &str, pq: &str) -> Result<PeerResponse> {
        let url = format!("{}{}", peer.trim_end_matches('/'), pq);
        let mut req = self.agent.get(&url).set("accept", "application/octet-stream");
        if let Some(k) = &self.key {
            req = req.set("x-nxr-key", k);
        }
        let resp = req.call().map_err(|e| match e {
            ureq::Error::Status(c, _) => anyhow!("http {c}"),
            ureq::Error::Transport(t) => anyhow!("transport: {}", t.kind()),
        })?;
        let composed = resp.header("x-nxr-composed") == Some("1");
        let mut body = Vec::new();
        std::io::Read::read_to_end(&mut std::io::Read::take(resp.into_reader(), MAX_BODY), &mut body)?;
        Ok(PeerResponse { body, composed })
    }
}

// -- fetch + validate ---------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Tol {
    pub min_gap_ms: i64,
    /// Seam price tolerance: `base + per_sqrt_h * sqrt(hours of distance)` bps.
    pub seam_base_bps: f64,
    pub seam_per_sqrt_h_bps: f64,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Reject {
    Unreachable(String),
    Torn,
    Empty,
    OutOfWindow,
    NonMonotone,
    WrongTicker,
    BadRow,
    ShortHead(i64),
    ShortTail(i64),
    SeamJump { side: &'static str, bps: i64 },
    Pager,
}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reject::Unreachable(e) => write!(f, "unreachable: {e}"),
            Reject::Torn => write!(f, "torn body (len not a multiple of stride)"),
            Reject::Empty => write!(f, "empty (peer holds no rows for this range)"),
            Reject::OutOfWindow => write!(f, "row outside requested window"),
            Reject::NonMonotone => write!(f, "non-monotone / bad stride"),
            Reject::WrongTicker => write!(f, "ticker id mismatch"),
            Reject::BadRow => write!(f, "non-finite / non-positive / inconsistent row"),
            Reject::ShortHead(ms) => write!(f, "short source: first row {ms} ms after hole start"),
            Reject::ShortTail(ms) => write!(f, "short source: last row {ms} ms before hole end"),
            Reject::SeamJump { side, bps } => write!(f, "{side} seam price jump {bps} bps"),
            Reject::Pager => write!(f, "pager did not advance"),
        }
    }
}

fn decode<T: PeerRow>(body: &[u8]) -> Result<Vec<T>, Reject> {
    let stride = std::mem::size_of::<T>();
    if body.len() % stride != 0 {
        return Err(Reject::Torn);
    }
    // `T` is packed (align 1), so a byte slice casts safely.
    Ok(bytemuck::cast_slice::<u8, T>(body).to_vec())
}

/// Page the whole hole from one peer. Returns raw decoded rows (unvalidated).
fn fetch_rows<T: PeerRow>(
    c: &dyn PeerClient,
    peer: &str,
    sym: &str,
    gap: &Gap,
    pause: Duration,
) -> Result<(Vec<T>, bool), Reject> {
    let mut rows: Vec<T> = Vec::new();
    let mut cursor = gap.start - T::PAD_MS;
    let to = gap.end - 1 + T::PAD_MS;
    let mut composed = false;
    for _ in 0..MAX_PAGES {
        let pq = format!("{}?from={}&to={}&limit={}", T::url(sym), cursor, to, PAGE);
        let resp = c.get(peer, &pq).map_err(|e| Reject::Unreachable(e.to_string()))?;
        composed |= resp.composed;
        let page = decode::<T>(&resp.body)?;
        let n = page.len();
        if n == 0 {
            break;
        }
        let last = page[n - 1].cursor_ms();
        if last < cursor {
            return Err(Reject::Pager);
        }
        rows.extend(page);
        if n < PAGE || last >= to {
            break;
        }
        cursor = last + 1;
        std::thread::sleep(pause);
    }
    Ok((rows, composed))
}

fn seam_ok(px: f64, a: &Anchor, dist_ms: i64, tol: &Tol, side: &'static str) -> Result<(), Reject> {
    let hours = (dist_ms.max(0) as f64) / 3_600_000.0;
    let allow = tol.seam_base_bps + tol.seam_per_sqrt_h_bps * hours.sqrt();
    let bps = ((px / a.px - 1.0).abs() * 1e4).round();
    if !(bps <= allow) {
        return Err(Reject::SeamJump { side, bps: bps as i64 });
    }
    Ok(())
}

/// Validate one source's rows for `gap`; on success returns the rows that fall
/// inside `[start, end)`.
pub fn validate_rows<T: PeerRow>(
    rows: Vec<T>,
    gap: &Gap,
    ticker_id: u64,
    tol: &Tol,
) -> Result<Vec<T>, Reject> {
    if rows.is_empty() {
        return Err(Reject::Empty);
    }
    let want_ticker = gap
        .left
        .as_ref()
        .and_then(|a| a.ticker)
        .or_else(|| gap.right.as_ref().and_then(|a| a.ticker))
        .unwrap_or(ticker_id);
    for (i, r) in rows.iter().enumerate() {
        let t = r.shard_ts_ms();
        if t < gap.start - T::PAD_MS || t >= gap.end + T::PAD_MS {
            return Err(Reject::OutOfWindow);
        }
        if let Some(tk) = r.ticker() {
            if tk != want_ticker {
                return Err(Reject::WrongTicker);
            }
        }
        if !r.sane() {
            return Err(Reject::BadRow);
        }
        if i > 0 && !T::step_ok(&rows[i - 1], r) {
            return Err(Reject::NonMonotone);
        }
    }
    let inside: Vec<T> = rows
        .into_iter()
        .filter(|r| (gap.start..gap.end).contains(&r.shard_ts_ms()))
        .collect();
    let (Some(first), Some(last)) = (inside.first(), inside.last()) else {
        return Err(Reject::Empty);
    };
    let head = first.shard_ts_ms() - gap.start;
    let tail = gap.end - last.shard_ts_ms();
    if head > tol.min_gap_ms {
        return Err(Reject::ShortHead(head));
    }
    if tail > tol.min_gap_ms {
        return Err(Reject::ShortTail(tail));
    }
    if let Some(l) = &gap.left {
        seam_ok(first.px(), l, first.shard_ts_ms() - l.ts, tol, "left")?;
    }
    if let Some(r) = &gap.right {
        seam_ok(last.px(), r, r.ts - last.shard_ts_ms(), tol, "right")?;
    }
    Ok(inside)
}

pub struct Fill<T> {
    pub gap: Gap,
    pub rows: Vec<T>,
    pub peer: String,
    pub composed: bool,
}

pub enum Outcome<T> {
    Filled(Fill<T>),
    Uncovered(Vec<(String, Reject)>),
}

/// Try `peers` in order; the first whose whole-hole rows validate wins.
pub fn fill_gap<T: PeerRow>(
    c: &dyn PeerClient,
    peers: &[String],
    sym: &str,
    ticker_id: u64,
    gap: &Gap,
    tol: &Tol,
    pause: Duration,
) -> Outcome<T> {
    let mut why = Vec::new();
    for p in peers {
        let got = fetch_rows::<T>(c, p, sym, gap, pause)
            .and_then(|(rows, composed)| validate_rows::<T>(rows, gap, ticker_id, tol).map(|r| (r, composed)));
        match got {
            Ok((rows, composed)) => {
                return Outcome::Filled(Fill { gap: gap.clone(), rows, peer: p.clone(), composed })
            }
            Err(e) => why.push((p.clone(), e)),
        }
    }
    Outcome::Uncovered(why)
}

// -- apply --------------------------------------------------------------------

fn bytes_of<T: PeerRow>(v: &[T]) -> &[u8] {
    bytemuck::cast_slice(v)
}

/// Merge `fills` (all for `date`) into the day's shard. Existing records are
/// kept byte-for-byte and in order; the result is verified before the rename.
/// Returns `(rows_added, path)`.
pub fn apply_day<T: PeerRow>(dir: &Path, date: NaiveDate, fills: &[Fill<T>], today: NaiveDate) -> Result<(usize, PathBuf)> {
    if date >= today {
        bail!("refusing {date}: current/future day belongs to the live writer");
    }
    let path = shard_path(dir, date, T::EXT);
    let stride = std::mem::size_of::<T>();
    let old_bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    if old_bytes.len() % stride != 0 {
        bail!("{} has a torn tail; heal it first (refusing to rewrite existing bytes)", path.display());
    }
    let old: Vec<T> = bytemuck::cast_slice::<u8, T>(&old_bytes).to_vec();
    if old.windows(2).any(|w| w[1].shard_ts_ms() < w[0].shard_ts_ms()) {
        bail!("{} is not ts-ordered; heal it first", path.display());
    }
    let mut add: Vec<T> = fills.iter().flat_map(|f| f.rows.iter().copied()).collect();
    add.sort_by_key(|r| r.shard_ts_ms());
    // Rows must not collide with an existing ts (holes are open intervals).
    let have: std::collections::HashSet<i64> = old.iter().map(|r| r.shard_ts_ms()).collect();
    if add.iter().any(|r| have.contains(&r.shard_ts_ms())) {
        bail!("{}: fill row collides with an existing record", path.display());
    }
    let mut merged: Vec<T> = Vec::with_capacity(old.len() + add.len());
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < add.len() {
        if j >= add.len() || (i < old.len() && old[i].shard_ts_ms() <= add[j].shard_ts_ms()) {
            merged.push(old[i]);
            i += 1;
        } else {
            merged.push(add[j]);
            j += 1;
        }
    }
    // Verify: every old record present, in order, byte-identical.
    let mb = bytes_of(&merged);
    let (mut k, mut m) = (0usize, 0usize);
    while k < old_bytes.len() && m < mb.len() {
        if mb[m..m + stride] == old_bytes[k..k + stride] {
            k += stride;
        }
        m += stride;
    }
    if k != old_bytes.len() || merged.len() != old.len() + add.len() {
        bail!("{}: merge verification failed; nothing written", path.display());
    }
    let part = path.with_extension(format!("{}.part", T::EXT));
    {
        let mut f = fs::File::create(&part).with_context(|| format!("create {}", part.display()))?;
        f.write_all(mb)?;
        f.sync_all()?;
    }
    fs::rename(&part, &path).with_context(|| format!("rename {}", part.display()))?;
    Ok((add.len(), path))
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SidecarEntry {
    pub kind: String,
    pub date: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub rows: usize,
    pub source: String,
    pub composed: bool,
    pub filled_at_ms: i64,
}

pub fn sidecar_path(dir: &Path) -> PathBuf {
    dir.join("backfill.json")
}

pub fn append_sidecar(dir: &Path, new: &[SidecarEntry]) -> Result<()> {
    let p = sidecar_path(dir);
    let mut all: Vec<SidecarEntry> = match fs::read_to_string(&p) {
        Ok(s) => serde_json::from_str(&s).with_context(|| format!("parse {}", p.display()))?,
        Err(_) => Vec::new(),
    };
    all.extend_from_slice(new);
    let tmp = p.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(&all)?)?;
    fs::rename(&tmp, &p)?;
    Ok(())
}

/// Refresh the manifest entry of each touched day.
pub fn refresh_manifest<T: PeerRow>(dir: &Path, name: &str, ticker_id: u64, dates: &[NaiveDate]) -> Result<()> {
    let mp = manifest_path(dir);
    let mut m = read_manifest(&mp)?.unwrap_or_else(|| Manifest::new(name.to_string(), ticker_id, T::EXT));
    for d in dates {
        m.upsert(shard_entry::<T>(*d, &shard_path(dir, *d, T::EXT))?);
    }
    if !m.kind.split(',').any(|k| k == T::EXT) {
        m.kind = if m.kind.is_empty() { T::EXT.into() } else { format!("{},{}", m.kind, T::EXT) };
    }
    write_manifest(&mp, &m)
}

// -- per ticker/kind driver ---------------------------------------------------

#[derive(Serialize, Debug, Clone)]
pub struct FilledRange {
    pub date: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub rows: usize,
    pub source: String,
    pub composed: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct UncoveredRange {
    pub date: String,
    pub start_ms: i64,
    pub end_ms: i64,
    /// `peer: reason` per source tried.
    pub reasons: Vec<String>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct KindReport {
    pub kind: String,
    pub holes: usize,
    pub filled: Vec<FilledRange>,
    pub uncovered: Vec<UncoveredRange>,
    pub applied: bool,
    pub skipped: Option<String>,
}

pub struct Run<'a> {
    pub client: &'a dyn PeerClient,
    pub peers: &'a [String],
    pub tol: &'a Tol,
    pub pause: Duration,
    pub apply: bool,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub today: NaiveDate,
    /// Wall clock (ms) the closed-day rule is judged against.
    pub now_ms: i64,
}

/// Detect, fetch, validate and (with `run.apply`) write the holes of one
/// ticker's `T` series. `dir` is the ticker's shard dir; `sym` the peer symbol.
pub fn run_series<T: PeerRow>(run: &Run, dir: &Path, name: &str, sym: &str, ticker_id: u64) -> Result<KindReport> {
    let mut rep = KindReport { kind: T::EXT.into(), ..Default::default() };
    let gaps = detect_gaps::<T>(dir, run.from, run.to, run.today, run.tol.min_gap_ms)?;
    rep.holes = gaps.len();
    let mut fills: Vec<Fill<T>> = Vec::new();
    for g in &gaps {
        match fill_gap::<T>(run.client, run.peers, sym, ticker_id, g, run.tol, run.pause) {
            Outcome::Filled(f) => fills.push(f),
            Outcome::Uncovered(why) => rep.uncovered.push(UncoveredRange {
                date: date_stem(g.date),
                start_ms: g.start,
                end_ms: g.end,
                reasons: why.into_iter().map(|(p, r)| format!("{p}: {r}")).collect(),
            }),
        }
    }
    if fills.is_empty() {
        return Ok(rep);
    }
    let mut note = |f: &Fill<T>| FilledRange {
        date: date_stem(f.gap.date),
        start_ms: f.gap.start,
        end_ms: f.gap.end,
        rows: f.rows.len(),
        source: f.peer.clone(),
        composed: f.composed,
    };
    if !run.apply {
        rep.filled = fills.iter().map(&mut note).collect();
        return Ok(rep);
    }
    // Closed days only. Anything fetched for a day still inside the grace is
    // left a hole for the next run.
    let (fills, open): (Vec<Fill<T>>, Vec<Fill<T>>) = fills
        .into_iter()
        .partition(|f| day_start_ms(f.gap.date) + MS_PER_DAY + CLOSE_GRACE_MS <= run.now_ms);
    for f in &open {
        rep.uncovered.push(UncoveredRange {
            date: date_stem(f.gap.date),
            start_ms: f.gap.start,
            end_ms: f.gap.end,
            reasons: vec!["day not closed yet".into()],
        });
    }
    if fills.is_empty() {
        return Ok(rep);
    }
    let _lock = match acquire_peerfill_lock(dir) {
        Ok(l) => l,
        Err(e) => {
            rep.skipped = Some(format!("{e}"));
            // Fetched but unwritten: these holes are still holes.
            for f in &fills {
                rep.uncovered.push(UncoveredRange {
                    date: date_stem(f.gap.date),
                    start_ms: f.gap.start,
                    end_ms: f.gap.end,
                    reasons: vec!["peer-fill lock held".into()],
                });
            }
            return Ok(rep);
        }
    };
    let mut by_day: BTreeMap<NaiveDate, Vec<Fill<T>>> = BTreeMap::new();
    for f in fills {
        by_day.entry(f.gap.date).or_default().push(f);
    }
    let now = nxr_sdk::now_ms() as i64;
    let mut touched = Vec::new();
    let mut side = Vec::new();
    for (d, fs_) in &by_day {
        apply_day::<T>(dir, *d, fs_, run.today)?;
        touched.push(*d);
        for f in fs_ {
            rep.filled.push(note(f));
            side.push(SidecarEntry {
                kind: T::EXT.into(),
                date: date_stem(*d),
                start_ms: f.gap.start,
                end_ms: f.gap.end,
                rows: f.rows.len(),
                source: f.peer.clone(),
                composed: f.composed,
                filled_at_ms: now,
            });
        }
    }
    refresh_manifest::<T>(dir, name, ticker_id, &touched)?;
    append_sidecar(dir, &side)?;
    rep.applied = true;
    Ok(rep)
}

// -- tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mitch::common::message_type;
    use mitch::header::MitchHeader;
    use mitch::index::Index;
    use mitch::timestamp::from_epoch_ms;
    use std::cell::RefCell;
    use std::collections::HashMap;

    const SEC: i64 = 1000;
    fn d(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2024, 5, day).unwrap()
    }
    fn tol() -> Tol {
        Tol { min_gap_ms: 300 * SEC, seam_base_bps: 100.0, seam_per_sqrt_h_bps: 300.0 }
    }
    fn tid() -> u64 {
        nxr_sdk::resolve_ticker_id("BTC/USDT")
    }
    fn irec(ts: i64, mid: f64) -> IndexRecord {
        let header = MitchHeader::new(message_type::INDEX, 0, from_epoch_ms(ts), 1);
        let index = Index {
            ticker: tid(),
            bid: mid * 0.99995,
            ask: mid * 1.00005,
            vbid: 1,
            vask: 1,
            ci: 100,
            tick_count: 1,
            confidence: 50,
            accepted: 1,
            rejected: 0,
            flags: 0,
        };
        IndexRecord::new(header, index)
    }
    fn brec(open: i64, px: f64) -> Bar {
        let mut b: Bar = bytemuck::Zeroable::zeroed();
        b.set_open_mts(from_epoch_ms(open));
        b.set_close_mts(from_epoch_ms(open + 9_998));
        b.open = px;
        b.high = px * 1.0001;
        b.low = px * 0.9999;
        b.close = px;
        b
    }
    /// 1 row / 60 s over `[a, b)` at slowly drifting price.
    fn series(a: i64, b: i64) -> Vec<IndexRecord> {
        (0..).map(|i| a + i * 60 * SEC).take_while(|t| *t < b).map(|t| irec(t, 60_000.0 + (t % 7) as f64)).collect()
    }
    fn write<T: PeerRow>(dir: &Path, date: NaiveDate, v: &[T]) {
        fs::create_dir_all(dir).unwrap();
        fs::write(shard_path(dir, date, T::EXT), bytes_of(v)).unwrap();
    }
    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("peer_fill_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    /// Serves canned rows per peer, honouring from/to/limit like the real route.
    struct Mock {
        rows: HashMap<String, Vec<IndexRecord>>,
        mutate: HashMap<String, Box<dyn Fn(Vec<u8>) -> Vec<u8>>>,
        calls: RefCell<Vec<String>>,
    }
    impl Mock {
        fn new() -> Self {
            Mock { rows: HashMap::new(), mutate: HashMap::new(), calls: RefCell::new(vec![]) }
        }
    }
    fn q(pq: &str, k: &str) -> i64 {
        pq.split(['?', '&']).find_map(|kv| kv.strip_prefix(&format!("{k}="))).unwrap().parse().unwrap()
    }
    impl PeerClient for Mock {
        fn get(&self, peer: &str, pq: &str) -> Result<PeerResponse> {
            self.calls.borrow_mut().push(peer.to_string());
            let Some(rows) = self.rows.get(peer) else { bail!("http 503") };
            let (from, to, lim) = (q(pq, "from"), q(pq, "to"), q(pq, "limit") as usize);
            let v: Vec<IndexRecord> = rows
                .iter()
                .filter(|r| (from..=to).contains(&r.shard_ts_ms()))
                .take(lim)
                .copied()
                .collect();
            let mut body = bytes_of(&v).to_vec();
            if let Some(m) = self.mutate.get(peer) {
                body = m(body);
            }
            Ok(PeerResponse { body, composed: false })
        }
    }

    fn day_ms(day: u32) -> i64 {
        day_start_ms(d(day))
    }

    /// Local: day 20 full, day 21 missing, day 22 full. Peers have day 21.
    fn setup(name: &str) -> (PathBuf, Vec<IndexRecord>) {
        let dir = tmp(name);
        write(&dir, d(20), &series(day_ms(20), day_ms(21)));
        write(&dir, d(22), &series(day_ms(22), day_ms(23)));
        (dir, series(day_ms(21), day_ms(22)))
    }
    fn run<'a>(c: &'a dyn PeerClient, peers: &'a [String], t: &'a Tol, apply: bool) -> Run<'a> {
        Run { client: c, peers, tol: t, pause: Duration::ZERO, apply, from: d(18), to: d(22), today: d(26), now_ms: day_ms(26) + 3600 * SEC }
    }
    fn peers(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn detects_missing_day_head_tail_interior_and_skips_today() {
        let dir = tmp("detect");
        // day 20: starts 1 h late (head gap) and has a 20 min interior hole, ends early (tail)
        let mut v = series(day_ms(20) + 3600 * SEC, day_ms(20) + 8 * 3600 * SEC);
        v.extend(series(day_ms(20) + 8 * 3600 * SEC + 20 * 60 * SEC, day_ms(20) + 20 * 3600 * SEC));
        write(&dir, d(19), &series(day_ms(19), day_ms(20))); // anchor for head gap
        write(&dir, d(20), &v);
        // day 21 missing, day 22 present (also "today" below => must be skipped)
        write(&dir, d(22), &series(day_ms(22), day_ms(22) + 3600 * SEC));
        let g = detect_gaps::<IndexRecord>(&dir, d(18), d(25), d(22), 300 * SEC).unwrap();
        let days: Vec<_> = g.iter().map(|x| x.date.day0() + 1).collect();
        use chrono::Datelike;
        assert_eq!(days, vec![20, 20, 20, 21], "{g:?}");
        assert!(g.iter().all(|x| x.date < d(22)), "today's shard must never be a hole");
        assert_eq!(g[0].start, day_ms(20)); // head
        assert!(g[3].left.is_some() && g[3].right.is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn fills_hole_from_first_good_peer_and_preserves_existing() {
        let (dir, truth) = setup("happy");
        let before20 = fs::read(shard_path(&dir, d(20), "idx")).unwrap();
        let before22 = fs::read(shard_path(&dir, d(22), "idx")).unwrap();
        let mut m = Mock::new();
        m.rows.insert("V".into(), truth.clone());
        let ps = peers(&["V", "A"]);
        let t = tol();
        let rep = run_series::<IndexRecord>(&run(&m, &ps, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.applied && rep.uncovered.is_empty(), "{rep:?}");
        assert_eq!(rep.filled.len(), 1);
        assert_eq!(rep.filled[0].source, "V");
        assert_eq!(*m.calls.borrow(), vec!["V"], "stopped at first good source");
        let got = read_shard_aligned::<IndexRecord>(&shard_path(&dir, d(21), "idx")).unwrap();
        assert_eq!(bytes_of(&got), bytes_of(&truth));
        assert_eq!(fs::read(shard_path(&dir, d(20), "idx")).unwrap(), before20);
        assert_eq!(fs::read(shard_path(&dir, d(22), "idx")).unwrap(), before22);
        let side: Vec<SidecarEntry> = serde_json::from_str(&fs::read_to_string(sidecar_path(&dir)).unwrap()).unwrap();
        assert_eq!((side.len(), side[0].source.as_str(), side[0].rows), (1, "V", truth.len()));
        let man = read_manifest(&manifest_path(&dir)).unwrap().unwrap();
        assert!(man.shards.iter().any(|s| s.date == "2024-05-21" && s.n_records as usize == truth.len()));
        // second run: nothing left to do, nothing rewritten
        let rep2 = run_series::<IndexRecord>(&run(&m, &ps, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert_eq!(rep2.holes, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn interior_hole_merge_keeps_existing_records_in_place() {
        let dir = tmp("interior");
        let full = series(day_ms(20), day_ms(21));
        let hole: Vec<_> = full.iter().filter(|r| (10 * 3600 * SEC..11 * 3600 * SEC).contains(&(r.shard_ts_ms() - day_ms(20)))).copied().collect();
        let kept: Vec<_> = full.iter().filter(|r| !hole.iter().any(|h| h.shard_ts_ms() == r.shard_ts_ms())).copied().collect();
        write(&dir, d(19), &series(day_ms(19), day_ms(20)));
        write(&dir, d(20), &kept);
        let mut m = Mock::new();
        m.rows.insert("V".into(), full.clone());
        let ps = peers(&["V"]);
        let t = tol();
        let r = Run { from: d(20), to: d(20), ..run(&m, &ps, &t, true) };
        let rep = run_series::<IndexRecord>(&r, &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.applied && rep.uncovered.is_empty(), "{rep:?}");
        let got = read_shard_aligned::<IndexRecord>(&shard_path(&dir, d(20), "idx")).unwrap();
        assert_eq!(bytes_of(&got), bytes_of(&full));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tampered_sources_rejected_next_source_used() {
        let (dir, truth) = setup("tamper");
        let t = tol();
        // each tamper must fail on its own; the clean peer "Z" then wins.
        type Tamper = Box<dyn Fn(Vec<u8>) -> Vec<u8>>;
        let cases: Vec<(&str, Tamper, &str)> = vec![
            ("scale", Box::new(|b| rescale(b, 2.0)), "seam price jump"),
            ("swap", Box::new(|b| swap_rows(b, 5, 6)), "non-monotone"),
            ("torn", Box::new(|mut b| { b.truncate(b.len() - 7); b }), "torn"),
            ("offday", Box::new(|b| shift_ts(b, 0, 2 * 86_400_000)), "outside requested window"),
            ("nan", Box::new(|b| nan_row(b, 3)), "non-finite"),
        ];
        for (name, f, want) in cases {
            let mut m = Mock::new();
            m.rows.insert("bad".into(), truth.clone());
            m.mutate.insert("bad".into(), f);
            m.rows.insert("Z".into(), truth.clone());
            let ps = peers(&["bad", "Z"]);
            let mut rep = run_series::<IndexRecord>(&run(&m, &ps, &t, false), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
            assert_eq!(rep.filled.len(), 1, "{name}: {rep:?}");
            assert_eq!(rep.filled.pop().unwrap().source, "Z", "{name}");
            // and the reason was recorded when the bad source stands alone
            let ps1 = peers(&["bad"]);
            let rep1 = run_series::<IndexRecord>(&run(&m, &ps1, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
            assert!(rep1.filled.is_empty() && !rep1.applied, "{name}: {rep1:?}");
            assert!(rep1.uncovered[0].reasons[0].contains(want), "{name}: {:?}", rep1.uncovered);
            assert!(!shard_path(&dir, d(21), "idx").exists(), "{name}: tampered source wrote data");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wrong_ticker_rejected() {
        let (dir, truth) = setup("ticker");
        let mut m = Mock::new();
        m.rows.insert("bad".into(), truth);
        m.mutate.insert("bad".into(), Box::new(|b| set_ticker(b, 4, 7)));
        let ps = peers(&["bad"]);
        let t = tol();
        let rep = run_series::<IndexRecord>(&run(&m, &ps, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.uncovered[0].reasons[0].contains("ticker id mismatch"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn short_and_empty_sources_leave_hole_uncovered_and_write_nothing() {
        let (dir, truth) = setup("short");
        let mut m = Mock::new();
        // short tail: peer stops 6 h early; light peer: 200 [] ; dead peer: 503
        m.rows.insert("short".into(), truth[..truth.len() - 360].to_vec());
        m.rows.insert("light".into(), vec![]);
        let ps = peers(&["short", "light", "dead"]);
        let t = tol();
        let rep = run_series::<IndexRecord>(&run(&m, &ps, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.filled.is_empty() && !rep.applied);
        assert_eq!(rep.uncovered.len(), 1);
        let r = &rep.uncovered[0].reasons;
        assert!(r[0].contains("short source"), "{r:?}");
        assert!(r[1].contains("empty"), "{r:?}");
        assert!(r[2].contains("unreachable"), "{r:?}");
        assert!(!shard_path(&dir, d(21), "idx").exists());
        assert!(!sidecar_path(&dir).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dry_run_writes_nothing_and_reports_would_fill() {
        let (dir, truth) = setup("dry");
        let mut m = Mock::new();
        m.rows.insert("V".into(), truth);
        let ps = peers(&["V"]);
        let t = tol();
        let rep = run_series::<IndexRecord>(&run(&m, &ps, &t, false), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert_eq!(rep.filled.len(), 1);
        assert!(!rep.applied);
        assert!(!shard_path(&dir, d(21), "idx").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_refuses_today_and_future() {
        let dir = tmp("today");
        let gap = Gap { date: d(26), start: day_ms(26), end: day_ms(26) + 1000, left: None, right: None };
        let f = Fill { gap, rows: vec![irec(day_ms(26) + 10, 1.0)], peer: "V".into(), composed: false };
        assert!(apply_day::<IndexRecord>(&dir, d(26), &[f], d(26)).is_err());
        assert!(!shard_path(&dir, d(26), "idx").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn peerfill_lock_held_skips_ticker() {
        let (dir, truth) = setup("lock");
        let _held = acquire_peerfill_lock(&dir).unwrap();
        let mut m = Mock::new();
        m.rows.insert("V".into(), truth);
        let ps = peers(&["V"]);
        let t = tol();
        let rep = run_series::<IndexRecord>(&run(&m, &ps, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.skipped.is_some() && !rep.applied && rep.uncovered.len() == 1);
        assert!(!shard_path(&dir, d(21), "idx").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    /// The live aggregator holds `.writer.lock` for its whole life; apply on a
    /// closed day must still go through (it never touches today's shard).
    #[test]
    fn live_writer_lock_does_not_block_closed_day() {
        use fs2::FileExt;
        let (dir, truth) = setup("livelock");
        let live = fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(dir.join(".writer.lock")).unwrap();
        live.try_lock_exclusive().unwrap();
        let mut m = Mock::new();
        m.rows.insert("V".into(), truth);
        let ps = peers(&["V"]);
        let t = tol();
        let rep = run_series::<IndexRecord>(&run(&m, &ps, &t, true), &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.applied && rep.skipped.is_none(), "{rep:?}");
        assert!(shard_path(&dir, d(21), "idx").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn day_inside_close_grace_is_left_a_hole() {
        let (dir, truth) = setup("grace");
        let mut m = Mock::new();
        m.rows.insert("V".into(), truth);
        let ps = peers(&["V"]);
        let t = tol();
        // day 21 ended 1 s ago: live writer may still be flushing its tail.
        let mut r = run(&m, &ps, &t, true);
        r.now_ms = day_ms(22) + SEC;
        let rep = run_series::<IndexRecord>(&r, &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(!rep.applied && rep.uncovered.iter().any(|u| u.reasons[0] == "day not closed yet"), "{rep:?}");
        assert!(!shard_path(&dir, d(21), "idx").exists());
        // past the grace it applies
        r.now_ms = day_ms(22) + CLOSE_GRACE_MS;
        let rep = run_series::<IndexRecord>(&r, &dir, "BTC-USDT", "BTC-USDT", tid()).unwrap();
        assert!(rep.applied, "{rep:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bars_stride_and_seam_checked() {
        const B: i64 = 1_716_000_000_000;
        let g = Gap { date: d(21), start: B, end: B + 100_000, left: Some(Anchor { ts: B - 1_000, px: 100.0, ticker: None }), right: None };
        let good: Vec<Bar> = (0..10).map(|i| brec(B + i * 10_000, 100.0)).collect();
        let t = tol();
        assert_eq!(validate_rows::<Bar>(good.clone(), &g, 0, &t).unwrap().len(), 10);
        let mut off = good.clone();
        off[4] = brec(B + 4 * 10_000 + 3_000, 100.0);
        assert_eq!(validate_rows::<Bar>(off, &g, 0, &t).unwrap_err(), Reject::NonMonotone);
        let jump: Vec<Bar> = (0..10).map(|i| brec(B + i * 10_000, 150.0)).collect();
        assert!(matches!(validate_rows::<Bar>(jump, &g, 0, &t), Err(Reject::SeamJump { .. })));
        assert!(matches!(validate_rows::<Bar>(good[..5].to_vec(), &g, 0, &Tol { min_gap_ms: 20_000, ..t.clone() }), Err(Reject::ShortTail(_))));
    }

    #[test]
    fn http_client_sends_key_header_and_pages() {
        use std::io::{Read, Write};
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let rec = irec(1_000, 5.0);
        let body = bytes_of(&[rec]).to_vec();
        let h = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut buf = [0u8; 2048];
            let n = s.read(&mut buf).unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_lowercase();
            let ok = req.contains("x-nxr-key: synthetic") && req.contains("application/octet-stream");
            let hdr = format!("HTTP/1.1 {}\r\ncontent-length: {}\r\nx-nxr-composed: 1\r\nconnection: close\r\n\r\n", if ok { "200 OK" } else { "403 Forbidden" }, body.len());
            s.write_all(hdr.as_bytes()).unwrap();
            s.write_all(&body).unwrap();
        });
        std::env::set_var("PEER_FILL_TEST_KEY", "synthetic");
        let c = HttpPeer::new("PEER_FILL_TEST_KEY");
        let r = c.get(&format!("http://{addr}"), "/v1/idx/X?from=0").unwrap();
        assert_eq!(r.body.len(), 56);
        assert!(r.composed);
        h.join().unwrap();
    }

    // -- byte-level tamper helpers (56 B IndexRecord) --
    fn rescale(mut b: Vec<u8>, k: f64) -> Vec<u8> {
        for c in b.chunks_exact_mut(56) {
            let r: &mut IndexRecord = bytemuck::from_bytes_mut(c);
            let (bid, ask) = (r.index.bid, r.index.ask);
            r.index.bid = bid * k;
            r.index.ask = ask * k;
        }
        b
    }
    fn swap_rows(mut b: Vec<u8>, i: usize, j: usize) -> Vec<u8> {
        let (a, c) = (b[i * 56..i * 56 + 56].to_vec(), b[j * 56..j * 56 + 56].to_vec());
        b[i * 56..i * 56 + 56].copy_from_slice(&c);
        b[j * 56..j * 56 + 56].copy_from_slice(&a);
        b
    }
    fn shift_ts(mut b: Vec<u8>, i: usize, by: i64) -> Vec<u8> {
        let r: IndexRecord = *bytemuck::from_bytes(&b[i * 56..i * 56 + 56]);
        let moved = irec(r.shard_ts_ms() + by, r.px());
        b[i * 56..i * 56 + 56].copy_from_slice(bytemuck::bytes_of(&moved));
        b
    }
    fn nan_row(mut b: Vec<u8>, i: usize) -> Vec<u8> {
        let r: &mut IndexRecord = bytemuck::from_bytes_mut(&mut b[i * 56..i * 56 + 56]);
        r.index.bid = f64::NAN;
        b
    }
    fn set_ticker(mut b: Vec<u8>, i: usize, t: u64) -> Vec<u8> {
        let r: &mut IndexRecord = bytemuck::from_bytes_mut(&mut b[i * 56..i * 56 + 56]);
        r.index.ticker = t;
        b
    }
}
