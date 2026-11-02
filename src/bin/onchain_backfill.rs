//! Offline backfill of `.idx` for on-chain-only assets (MUSD, SYRUPUSDC, USDAT) from HyperSync
//! pool logs, using the live poller's math (`nxr_onchain::{pool, composite}`, params from
//! `config.yml onchain:`), never a copy of it.
//!
//! ```text
//! HYPERSYNC_API_KEY=... onchain-backfill --config config.yml --asset USDAT \
//!     --from 2026-10-01 --to 2026-10-31 --out-root /staging/root [--force]
//! ```
//! Writes `<out-root>/indexes/<id of ASSET/USD>/<YYYY-MM-DD>.idx` + `manifest.json` and a sidecar
//! `<out-root>/onchain-backfill.<ASSET>.json`. `--to` must be before today (UTC). `--out-root` has no
//! default and existing shards/sidecar are refused without `--force`. The target dir is symlink-checked,
//! flock'd (`.writer.lock`) and refused if it holds a shard dated today or later. Never point it at a
//! live data dir; to promote, copy the `*.idx` files only (the manifest here is a staging manifest).
//!
//! Bars from the staged idx (do not reimplement; `s10-from-idx` is the producer):
//! ```text
//! s10-from-idx config.yml USDAT-USD --in-dir <out-root>/indexes/<id> \
//!     --out-dir <out-root>/bars/<id> --to-date <to>
//! ```
//!
//! Deviations from live (all approximations, recorded in the sidecar):
//! * Samples are the pool state after the last event at or before each 1 s grid second, held
//!   (live polls slot0 each second; the state only moves on a swap). A mint/burn with no swap
//!   leaves `liquidity` stale until the next swap.
//! * V3/V4 depth = `cl_band_depth` over +-`depth_band_bps` with the event `liquidity` constant
//!   across the band (empty tick nets); live walks the initialized ticks.
//! * Curve: price from each `TokenExchange` (out/in, un-biased by the configured `fee_bps`; swaps
//!   under 1 unit ignored); live uses get_dy at 1 unit both ways. Depth = 4 x depth floor (live:
//!   0 / 1x / 4x by a probe swap); AddLiquidity/RemoveLiquidity ignored. Token decimals are 6.
//! * A leg whose last swap is older than `--hold-cap-hours` (default 6) is dropped from the composite.
//! * Curve fills used for price are capped at 1% of 4 x the depth floor (larger swaps carry impact).
//! * Venue trust, chain-dark state and block-age gates are not modeled (always trusted/fresh).
//! * SYRUPUSDC: no vault-rate leg and no rate anchor (no free archive RPC): pool composite only,
//!   peg-band gate skipped.
//! * Heartbeat: a record is emitted on a mid change and at most 5 s apart. Header provider 0
//!   (composite), flags BACKFILL | NO_BOOK | CONF_ACTIVE.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{NaiveDate, Utc};
use clap::Parser;
use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use nxr_onchain::composite::{composite, PoolView};
use nxr_onchain::pool::{cl_band_depth, twap, v3_price_sqrtpx96, Sample};
use nxr_onchain::registry::{load_section, PoolDesc, PoolKind, RpcRow};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::resolve_ticker_id;
use nxr_sdk::shard::{
    day_start_ms, FLAG_CONF_ACTIVE, FLAG_HISTORICAL_BACKFILL, FLAG_NO_BOOK, MS_PER_DAY,
};
use serde_json::{json, Value};
use series_factory::sharding::{
    idx_dir, list_shards, manifest_path, shard_entry_for_idx, shard_path, write_manifest,
    write_shard_atomic, Manifest,
};
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const Q96: f64 = 79228162514264337593543950336.0;
const DEC: u8 = 6;
const LOOKBACK_MS: u64 = 7 * 86_400_000;
/// Owner cap on total HyperSync requests per second.
const HS_RPS: u32 = 5;
const HEARTBEAT_MS: u64 = 5_000;
const MIN_CURVE_RAW: u128 = 1_000_000;
const MAX_QUOTE_GAP_MS: u64 = 300_000;
/// Static sanity band for rate-anchored assets without a peg (SYRUPUSDC, USDC per token).
const RATE_SANITY: (f64, f64) = (1.0, 1.3);
const ASSETS: [&str; 3] = ["MUSD", "SYRUPUSDC", "USDAT"];
const V4_SWAP: &str = "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)";
const CURVE_XCH: &str = "TokenExchange(address,int128,uint256,int128,uint256)";
const CURVE_COINS: &str = "0xc6610657";
const DECIMALS_CALL: &str = "0x313ce567";

#[derive(Parser, Debug)]
#[command(about = "Backfill .idx for on-chain-only assets from HyperSync pool logs.")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    asset: String,
    /// First UTC day, inclusive.
    #[arg(long)]
    from: NaiveDate,
    /// Last UTC day, inclusive; strictly before today (UTC).
    #[arg(long)]
    to: NaiveDate,
    /// Staging root (required; never a live data dir).
    #[arg(long)]
    out_root: PathBuf,
    #[arg(long, default_value = "https://api.nxrates.com")]
    quote_ohlc_url: String,
    /// Replace existing shards.
    #[arg(long)]
    force: bool,
    /// Curve base coin index; default read from the pool (`coins(k)`) over the config RPCs.
    #[arg(long)]
    curve_base_idx: Option<u8>,
    /// Drop a leg from the composite once its last swap is older than this.
    #[arg(long, default_value_t = 6)]
    hold_cap_hours: u64,
}

// ---------- pure helpers ----------

fn validate_range(from: NaiveDate, to: NaiveDate, today: NaiveDate) -> Result<()> {
    if to >= today {
        bail!("--to {to} must be strictly before today {today} (UTC): the live tail is not ours");
    }
    if from > to {
        bail!("--from {from} is after --to {to}");
    }
    Ok(())
}

fn topic0(sig: &str) -> String {
    format!("0x{}", hex::encode(Keccak256::digest(sig.as_bytes())))
}

fn word(d: &[u8], i: usize) -> Option<&[u8]> {
    d.get(32 * i..32 * i + 32)
}

fn u128_at(d: &[u8], i: usize) -> Option<u128> {
    let w = word(d, i)?;
    w[..16]
        .iter()
        .all(|b| *b == 0)
        .then(|| u128::from_be_bytes(w[16..].try_into().unwrap()))
}

/// Canonically sign-extended 128-bit signed word (int24 / int128).
fn i128_at(d: &[u8], i: usize) -> Option<i128> {
    let w = word(d, i)?;
    let v = i128::from_be_bytes(w[16..].try_into().ok()?);
    let fill = if v < 0 { 0xff } else { 0 };
    w[..16].iter().all(|b| *b == fill).then_some(v)
}

/// v4 `Swap` data: (amount0, amount1, sqrtPriceX96, liquidity, tick, fee) -> (sqrt, liquidity, tick).
fn parse_v4_swap(d: &[u8]) -> Option<(u128, u128, i32)> {
    Some((
        u128_at(d, 2)?,
        u128_at(d, 3)?,
        i32::try_from(i128_at(d, 4)?).ok()?,
    ))
}

/// Curve `TokenExchange` data: (sold_id, tokens_sold, bought_id, tokens_bought).
fn parse_curve(d: &[u8]) -> Option<(i128, u128, i128, u128)> {
    Some((
        i128_at(d, 0)?,
        u128_at(d, 1)?,
        i128_at(d, 2)?,
        u128_at(d, 3)?,
    ))
}

/// One pool, as the events need it.
struct PoolCfg {
    id: String,
    kind: PoolKind,
    hooked: bool,
    base_is_t0: bool,
    band: f64,
    floor: f64,
    fee: f64,
    base_idx: i128,
    /// Curve: largest input (raw) whose fill price is used.
    max_sold_raw: u128,
}

/// (price, depth) of base in quote after a v4 swap: the live `sample` math, constant-L depth.
fn v4_state(p: &PoolCfg, sqrt: u128, liq: u128, tick: i32) -> Option<(f64, f64)> {
    let px = v3_price_sqrtpx96(sqrt, DEC, DEC, p.base_is_t0)?;
    let depth = cl_band_depth(sqrt as f64 / Q96, tick, liq, &[], p.band, p.base_is_t0)?;
    let depth = depth / 10f64.powi(DEC as i32);
    depth.is_finite().then_some((px, depth))
}

fn curve_state(
    p: &PoolCfg,
    (sid, sold, bid, bought): (i128, u128, i128, u128),
) -> Option<(f64, f64)> {
    if sold.min(bought) < MIN_CURVE_RAW || sold > p.max_sold_raw {
        return None;
    }
    let px = if sid == p.base_idx {
        bought as f64 / sold as f64 / (1.0 - p.fee)
    } else if bid == p.base_idx {
        sold as f64 / bought as f64 * (1.0 - p.fee)
    } else {
        return None;
    };
    px.is_finite().then_some((px, 4.0 * p.floor))
}

#[derive(Debug, Clone, PartialEq)]
struct Log {
    block: u64,
    idx: u64,
    topics: Vec<String>,
    data: Vec<u8>,
}

fn qty(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => match s.strip_prefix("0x") {
            Some(h) => u64::from_str_radix(h, 16).ok(),
            None => s.parse().ok(),
        },
        _ => None,
    }
}

/// One HyperSync response -> (logs with block ts in seconds, next_block).
fn parse_response(resp: &Value) -> Result<(Vec<(Log, u64)>, u64)> {
    let mut ts: HashMap<u64, u64> = HashMap::new();
    let mut logs = vec![];
    for page in resp["data"].as_array().into_iter().flatten() {
        for b in page["blocks"].as_array().into_iter().flatten() {
            if let (Some(n), Some(t)) = (qty(&b["number"]), qty(&b["timestamp"])) {
                ts.insert(n, t);
            }
        }
        for l in page["logs"].as_array().into_iter().flatten() {
            let topics: Vec<String> = match l["topics"].as_array() {
                Some(a) => a
                    .iter()
                    .filter_map(|t| t.as_str())
                    .map(str::to_lowercase)
                    .collect(),
                None => (0..4)
                    .filter_map(|i| l[format!("topic{i}")].as_str())
                    .map(str::to_lowercase)
                    .collect(),
            };
            let data = hex::decode(l["data"].as_str().unwrap_or("").trim_start_matches("0x"))?;
            let block =
                qty(&l["block_number"]).ok_or_else(|| anyhow!("log without block_number"))?;
            let idx = qty(&l["log_index"]).ok_or_else(|| anyhow!("log without log_index"))?;
            logs.push((
                Log {
                    block,
                    idx,
                    topics,
                    data,
                },
                block,
            ));
        }
    }
    let next = qty(&resp["next_block"]).ok_or_else(|| anyhow!("no next_block"))?;
    let out = logs
        .into_iter()
        .map(|(l, b)| {
            ts.get(&b)
                .map(|t| (l, *t))
                .ok_or_else(|| anyhow!("no timestamp for block {b}"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((out, next))
}

// ---------- grid ----------

struct Params {
    twap_ms: u64,
    max_age_ms: u64,
    max_dev_bps: u32,
    min_depth: f64,
    warmup_ms: u64,
    min_legs: usize,
    /// (peg, band_bps); None = no anchor available.
    peg: Option<(f64, u32)>,
    /// Static USDC-per-token band when there is no peg.
    sanity: Option<(f64, f64)>,
    hold_cap_ms: u64,
}

struct Leg {
    id: String,
    v4: bool,
    hooked: bool,
    /// (ts_ms, price, depth), ts-ascending.
    ev: Vec<(u64, f64, f64)>,
}

/// Quote -> USD marks (ts_ms, open), ts-ascending.
struct Quote(Vec<(u64, f64)>);

impl Quote {
    /// Bars must cover `[start, end)` with no gap over 5 min and sit in 0.98..1.02.
    fn new(bars: Vec<(u64, f64)>, start: u64, end: u64) -> Result<Self> {
        let first = bars.first().ok_or_else(|| anyhow!("no USDC-USD bars"))?;
        if first.0 > start {
            bail!("USDC-USD bars start after the grid start");
        }
        if bars.iter().any(|b| !(0.98..=1.02).contains(&b.1)) {
            bail!("USDC-USD bar outside 0.98..1.02");
        }
        if bars.windows(2).any(|w| w[1].0 - w[0].0 > MAX_QUOTE_GAP_MS) {
            bail!("USDC-USD bar gap over 5 min");
        }
        if bars.last().unwrap().0 + MAX_QUOTE_GAP_MS < end {
            bail!("USDC-USD bars end more than 5 min before the range end");
        }
        Ok(Self(bars))
    }

    fn at(&self, t: u64) -> f64 {
        let i = self.0.partition_point(|b| b.0 <= t);
        self.0[i.saturating_sub(1)].1
    }
}

/// The live compose loop at a 1 s cadence over `[from_ms - warmup - twap, end_ms)`, emitting
/// from `from_ms`: per-leg TWAP views -> `composite` -> DQ gate (legs, V4 rule, peg band,
/// continuous warmup) -> record on mid change or every `HEARTBEAT_MS`.
fn run_grid(
    legs: &[Leg],
    q: &Quote,
    p: &Params,
    ticker: u64,
    from_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, IndexRecord)>, GridStats) {
    let mut stats = GridStats {
        held_pct: vec![0.0; legs.len()],
        dropped_pct: vec![0.0; legs.len()],
    };
    let (mut held, mut dropped, mut total) = (vec![0u64; legs.len()], vec![0u64; legs.len()], 0u64);
    let start = from_ms.saturating_sub(p.warmup_ms + p.twap_ms) / 1000 * 1000;
    let mut cur = vec![0usize; legs.len()];
    let mut last: Vec<Option<(f64, f64)>> = vec![None; legs.len()];
    let mut last_ts = vec![0u64; legs.len()];
    let mut samples: Vec<Vec<Sample>> = vec![vec![]; legs.len()];
    let (mut green_since, mut last_mid, mut last_emit): (Option<u64>, Option<f64>, u64) =
        (None, None, 0);
    let mut out = vec![];
    let mut t = start;
    while t < end_ms {
        let mut views = vec![];
        total += u64::from(t >= from_ms);
        for (i, l) in legs.iter().enumerate() {
            while cur[i] < l.ev.len() && l.ev[cur[i]].0 <= t {
                last[i] = Some((l.ev[cur[i]].1, l.ev[cur[i]].2));
                last_ts[i] = l.ev[cur[i]].0;
                cur[i] += 1;
            }
            let Some((price, depth_usd)) = last[i] else {
                continue;
            };
            let age = t - last_ts[i];
            if t >= from_ms {
                held[i] += u64::from(age > p.max_age_ms);
            }
            if age > p.hold_cap_ms {
                dropped[i] += u64::from(t >= from_ms);
                continue;
            }
            samples[i].push(Sample {
                ts_ms: t,
                price,
                depth_usd,
            });
            let lo = samples[i].partition_point(|s| s.ts_ms + p.twap_ms + p.max_age_ms < t);
            if let Some((px, d)) = twap(&samples[i][lo..], t.saturating_sub(p.twap_ms), t) {
                views.push(PoolView {
                    id: l.id.clone(),
                    twap: px,
                    depth_usd: d,
                    age_ms: 0,
                    quote_px: q.at(t),
                });
            }
        }
        let c = composite(&views, p.max_age_ms, p.max_dev_bps, p.min_depth);
        let green = c.as_ref().is_some_and(|c| {
            let enough = c.active >= p.min_legs
                && c.kept.iter().any(|id| {
                    legs.iter()
                        .any(|l| l.id == *id && (!l.v4 || (p.min_legs == 1 && !l.hooked)))
                });
            // The live band test is on the pool-quote mid (USDC), before the USD bridge.
            let usdc = c.mid / q.at(t);
            let band = match (p.peg, p.sanity) {
                (Some((a, bps)), _) => ((usdc / a - 1.0) * 1e4).abs() <= bps as f64,
                (None, Some((lo, hi))) => (lo..=hi).contains(&usdc),
                (None, None) => true,
            };
            enough && band
        });
        green_since = if green { green_since.or(Some(t)) } else { None };
        if let (Some(c), Some(s)) = (c.filter(|_| green), green_since) {
            if t >= from_ms
                && t - s >= p.warmup_ms
                && (last_mid != Some(c.mid) || t - last_emit >= HEARTBEAT_MS)
            {
                let mut idx = Index::new(
                    ticker,
                    c.mid,
                    c.mid,
                    c.ci_enc,
                    0,
                    0,
                    1,
                    c.conf,
                    c.active.min(255) as u8,
                    c.total.saturating_sub(c.active).min(255) as u8,
                );
                idx.flags = FLAG_HISTORICAL_BACKFILL | FLAG_NO_BOOK | FLAG_CONF_ACTIVE;
                let header = MitchHeader::new(
                    message_type::INDEX,
                    0,
                    mitch::timestamp::from_epoch_ms(t as i64),
                    1,
                );
                out.push((t, IndexRecord { header, index: idx }));
                (last_mid, last_emit) = (Some(c.mid), t);
            }
        }
        t += 1000;
    }
    let pct = |v: &[u64]| {
        v.iter()
            .map(|x| 100.0 * *x as f64 / total.max(1) as f64)
            .collect()
    };
    stats.held_pct = pct(&held);
    stats.dropped_pct = pct(&dropped);
    (out, stats)
}

/// Per leg, % of emitted-range grid seconds on a held price older than `max_age_ms`, and
/// % dropped by the hold cap.
struct GridStats {
    held_pct: Vec<f64>,
    dropped_pct: Vec<f64>,
}

// ---------- output ----------

fn sidecar_path(out_root: &Path, asset: &str) -> PathBuf {
    out_root.join(format!("onchain-backfill.{asset}.json"))
}

fn is_symlink(p: &Path) -> bool {
    std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink())
}

/// Everything that can refuse a run, before any fetch or write.
fn precheck(
    out_root: &Path,
    ticker_id: u64,
    asset: &str,
    from: NaiveDate,
    to: NaiveDate,
    force: bool,
    today: NaiveDate,
) -> Result<PathBuf> {
    let dir = idx_dir(out_root, ticker_id);
    for p in [out_root, &out_root.join("indexes"), &dir] {
        if is_symlink(p) {
            bail!("refusing symlink {}", p.display());
        }
    }
    if dir.exists() {
        if !dir.canonicalize()?.starts_with(out_root.canonicalize()?) {
            bail!("refusing path outside out-root: {}", dir.display());
        }
        if let Some((d, _)) = list_shards(&dir, "idx")?
            .into_iter()
            .find(|(d, _)| *d >= today)
        {
            bail!(
                "{} holds a shard dated {d} (today or later): live dir, refusing",
                dir.display()
            );
        }
    }
    let mut d = from;
    while d <= to {
        let path = shard_path(&dir, d, "idx");
        if path.exists() && !force {
            bail!("refusing to overwrite {} (use --force)", path.display());
        }
        d = d.succ_opt().ok_or_else(|| anyhow!("date overflow"))?;
    }
    let sp = sidecar_path(out_root, asset);
    if sp.exists() && !force {
        bail!("refusing to overwrite {} (use --force)", sp.display());
    }
    Ok(dir)
}

/// Writes one shard per day with rows; returns (written, days without rows). With `force` a
/// stale shard on a row-less day is deleted. Fails when nothing is written.
#[allow(clippy::too_many_arguments)]
fn write_days(
    out_root: &Path,
    ticker: &str,
    ticker_id: u64,
    asset: &str,
    recs: &[(u64, IndexRecord)],
    from: NaiveDate,
    to: NaiveDate,
    force: bool,
) -> Result<(Vec<(NaiveDate, PathBuf)>, Vec<NaiveDate>)> {
    let today = Utc::now().date_naive();
    precheck(out_root, ticker_id, asset, from, to, force, today)?;
    let dir = idx_dir(out_root, ticker_id);
    std::fs::create_dir_all(&dir)?;
    precheck(out_root, ticker_id, asset, from, to, force, today)?;
    let _lock = nxr_sdk::shard::acquire_idx_writer_lock(&dir)
        .with_context(|| format!("writer lock held on {}", dir.display()))?;
    let (mut jobs, mut skipped) = (vec![], vec![]);
    let mut d = from;
    while d <= to {
        let (lo, hi) = (
            day_start_ms(d) as u64,
            (day_start_ms(d) + MS_PER_DAY) as u64,
        );
        let rows: Vec<IndexRecord> = recs
            .iter()
            .filter(|r| r.0 >= lo && r.0 < hi)
            .map(|r| r.1)
            .collect();
        let path = shard_path(&dir, d, "idx");
        if rows.is_empty() {
            tracing::warn!(%d, "no records (DQ gate never green); no shard written");
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            skipped.push(d);
        } else {
            jobs.push((d, path, rows));
        }
        d = d.succ_opt().ok_or_else(|| anyhow!("date overflow"))?;
    }
    if jobs.is_empty() {
        bail!("nothing written: the DQ gate was never green in {from}..{to}");
    }
    let mut written = vec![];
    for (d, path, rows) in &jobs {
        write_shard_atomic(path, bytemuck::cast_slice(rows))?;
        written.push((*d, path.clone()));
    }
    let mut m = Manifest::new(ticker.to_string(), ticker_id, "idx");
    let mut entries = vec![];
    for (date, p) in list_shards(&dir, "idx")? {
        entries.push(shard_entry_for_idx(date, &p)?);
    }
    m.set_shards_batch(entries);
    write_manifest(&manifest_path(&dir), &m)?;
    Ok((written, skipped))
}

// ---------- network ----------

/// Process-wide HyperSync pacing: calls start at least `gap` apart (5 req/s, burst 1).
struct Limiter {
    gap: std::time::Duration,
    next: std::sync::Mutex<std::time::Instant>,
}

impl Limiter {
    fn new(per_sec: u32) -> Self {
        Self {
            gap: std::time::Duration::from_millis(1000 / per_sec as u64),
            next: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    /// Reserve the next start slot at or after `now`; returns how long to wait.
    fn reserve(&self, now: std::time::Instant) -> std::time::Duration {
        let mut n = self.next.lock().unwrap();
        let at = (*n).max(now);
        *n = at + self.gap;
        at - now
    }

    fn wait(&self) {
        std::thread::sleep(self.reserve(std::time::Instant::now()));
    }

    /// Push every later call back (429 / Retry-After).
    fn back_off(&self, d: std::time::Duration) {
        let mut n = self.next.lock().unwrap();
        *n = (*n).max(std::time::Instant::now() + d);
    }
}

/// 429, 408 and 5xx are worth another try; any other 4xx is a request or auth fault.
fn retryable(code: u16) -> bool {
    matches!(code, 408 | 429) || code >= 500
}

struct Hs {
    key: String,
    agent: ureq::Agent,
    lim: Limiter,
}

impl Hs {
    fn new() -> Result<Self> {
        let key =
            std::env::var("HYPERSYNC_API_KEY").map_err(|_| anyhow!("HYPERSYNC_API_KEY unset"))?;
        let agent = ureq::AgentBuilder::new()
            .timeout(std::time::Duration::from_secs(60))
            .build();
        Ok(Self {
            key,
            agent,
            lim: Limiter::new(HS_RPS),
        })
    }

    /// One paced request with retry: 429/5xx/transport back off (Retry-After honoured), other 4xx fail.
    fn call(
        &self,
        what: &str,
        mut f: impl FnMut() -> Result<ureq::Response, ureq::Error>,
    ) -> Result<Value> {
        let mut last = String::new();
        for n in 0..4 {
            if n > 0 {
                self.lim.back_off(std::time::Duration::from_secs(2 * n));
            }
            self.lim.wait();
            match f() {
                Ok(r) => return Ok(serde_json::from_str(&r.into_string()?)?),
                Err(ureq::Error::Status(c, r)) => {
                    if let Some(s) = r.header("retry-after").and_then(|v| v.parse::<u64>().ok()) {
                        self.lim
                            .back_off(std::time::Duration::from_secs(s.min(120)));
                    }
                    if !retryable(c) {
                        bail!("hypersync {what}: http {c}");
                    }
                    last = format!("http {c}");
                }
                Err(_) => last = "transport error".into(),
            }
        }
        bail!("hypersync {what}: {last}")
    }

    fn post(&self, chain: u64, body: &Value) -> Result<Value> {
        let url = format!("https://{chain}.hypersync.xyz/query");
        let b = body.to_string();
        self.call(&format!("chain {chain}"), || {
            self.agent
                .post(&url)
                .set("Authorization", &format!("Bearer {}", self.key))
                .send_string(&b)
        })
    }

    fn height(&self, chain: u64) -> Result<u64> {
        let url = format!("https://{chain}.hypersync.xyz/height");
        let v = self.call(&format!("height {chain}"), || self.agent.get(&url).call())?;
        qty(&v["height"]).ok_or_else(|| anyhow!("no height"))
    }

    fn block_ts(&self, chain: u64, n: u64) -> Result<Option<u64>> {
        let r = self.post(
            chain,
            &json!({"from_block": n, "to_block": n + 1, "include_all_blocks": true,
                    "field_selection": {"block": ["number", "timestamp"]}}),
        )?;
        Ok(r["data"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|p| p["blocks"].as_array().into_iter().flatten())
            .find_map(|b| qty(&b["timestamp"])))
    }

    /// Lowest block whose timestamp is >= `ts_sec` (height + 1 if none).
    fn first_block_at(&self, chain: u64, ts_sec: u64, height: u64) -> Result<u64> {
        let (mut lo, mut hi) = (0, height + 1);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.block_ts(chain, mid)? {
                Some(t) if t < ts_sec => lo = mid + 1,
                _ => hi = mid,
            }
        }
        Ok(lo)
    }

    fn logs(
        &self,
        chain: u64,
        from: u64,
        to: u64,
        address: Option<&str>,
        t0: &str,
        t1: Option<&str>,
    ) -> Result<Vec<(Log, u64)>> {
        let mut sel = json!({"topics": [[t0]]});
        if let Some(t1) = t1 {
            sel["topics"] = json!([[t0], [t1]]);
        }
        if let Some(a) = address {
            sel["address"] = json!([a]);
        }
        let mut out = paginate(from, to, |cur| {
            let r = self.post(
                chain,
                &json!({"from_block": cur, "to_block": to, "logs": [sel],
                        "field_selection": {
                            "log": ["block_number", "log_index", "data", "topic0", "topic1", "topic2", "topic3"],
                            "block": ["number", "timestamp"]}}),
            )?;
            parse_response(&r)
        })?;
        out.sort_by_key(|(l, _)| (l.block, l.idx));
        Ok(out)
    }
}

/// Pages `[from, to)` by `next_block`; no progress or a short walk is an error, never a quiet stop.
fn paginate<T>(
    from: u64,
    to: u64,
    mut fetch: impl FnMut(u64) -> Result<(Vec<T>, u64)>,
) -> Result<Vec<T>> {
    let (mut cur, mut out) = (from, vec![]);
    while cur < to {
        let (mut page, next) = fetch(cur)?;
        out.append(&mut page);
        if next <= cur {
            bail!("hypersync made no progress at block {cur} (next_block {next}, to {to})");
        }
        cur = next;
    }
    if cur < to {
        bail!("pagination stopped at {cur} before {to}");
    }
    Ok(out)
}

fn fetch_quote(url: &str, from_ms: u64, to_ms: u64, start: u64) -> Result<Quote> {
    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(60))
        .build();
    let mut bars: Vec<(u64, f64)> = vec![];
    let mut s = from_ms;
    while s < to_ms {
        // the API caps a reply at 10_000 rows: 5 d of 1 m bars = 7_200
        let e = (s + 5 * MS_PER_DAY as u64).min(to_ms);
        let body = agent
            .get(&format!(
                "{}/v1/ohlc/USDC-USD?tf=60&from={s}&to={e}",
                url.trim_end_matches('/')
            ))
            .set("Accept", "application/json")
            .set("User-Agent", "curl/8")
            .call()
            .map_err(|e| anyhow!("quote ohlc: {e}"))?
            .into_string()?;
        let v: Value = serde_json::from_str(&body)?;
        let arr = v
            .as_array()
            .or_else(|| v["data"].as_array())
            .ok_or_else(|| anyhow!("quote ohlc: not an array"))?;
        for b in arr {
            if let (Some(ts), Some(o)) = (qty(&b["ts"]), b["open"].as_f64()) {
                bars.push((if ts < 100_000_000_000 { ts * 1000 } else { ts }, o));
            }
        }
        s = e;
    }
    bars.sort_by_key(|b| b.0);
    bars.dedup_by_key(|b| b.0);
    Quote::new(bars, start, to_ms)
}

/// `eth_call` at latest over the config RPCs: first answer wins.
fn eth_call(rpcs: &[RpcRow], to: &str, data: &str) -> Result<String> {
    for (i, row) in rpcs.iter().enumerate() {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                          "params": [{"to": to, "data": data}, "latest"]});
        let Ok(r) = ureq::post(&row.endpoint().url)
            .timeout(std::time::Duration::from_secs(15))
            .send_string(&body.to_string())
        else {
            tracing::warn!(rpc = i, "eth_call failed");
            continue;
        };
        let v: Value = serde_json::from_str(&r.into_string()?)?;
        if let Some(res) = v["result"].as_str() {
            return Ok(res.to_string());
        }
    }
    bail!("eth_call {to} failed on every rpc")
}

fn same_addr(word_hex: &str, token: &str) -> bool {
    word_hex.len() >= 66
        && word_hex[word_hex.len() - 40..].eq_ignore_ascii_case(token.trim_start_matches("0x"))
}

/// Base coin index of a Curve pool: `coins(0)` / `coins(1)`.
fn curve_base_idx(rpcs: &[RpcRow], pool: &str, base_token: &str) -> Result<i128> {
    for k in 0..2u8 {
        if same_addr(
            &eth_call(rpcs, pool, &format!("{CURVE_COINS}{k:064x}"))?,
            base_token,
        ) {
            return Ok(k as i128);
        }
    }
    bail!("could not resolve Curve base coin index for {pool}; pass --curve-base-idx")
}

fn assert_decimals_6(rpcs: &[RpcRow], token: &str) -> Result<()> {
    let r = eth_call(rpcs, token, DECIMALS_CALL)?;
    if u64::from_str_radix(r.trim_start_matches("0x"), 16).ok() != Some(DEC as u64) {
        bail!("token {token} decimals != {DEC}: the backfill math assumes 6");
    }
    Ok(())
}

fn pool_manager(chain: &str) -> Result<&'static str> {
    Ok(match chain {
        "ethereum" => "0x000000000004444c5dc75cb358380d2e3de08a90",
        "monad" => "0x188d586ddcf52439676ca21a244753fa19f9ea8e",
        c => bail!("no v4 PoolManager known for chain {c}"),
    })
}

fn chain_id(chain: &str) -> Result<u64> {
    Ok(match chain {
        "ethereum" => 1,
        "monad" => 143,
        c => bail!("no HyperSync chain id for {c}"),
    })
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let a = Args::parse();
    let today = Utc::now().date_naive();
    validate_range(a.from, a.to, today)?;
    let asset = a.asset.to_uppercase();
    if !ASSETS.contains(&asset.as_str()) {
        bail!("--asset must be one of {ASSETS:?}");
    }
    let sec = load_section(&a.config)?;
    let descs: Vec<&PoolDesc> = sec
        .pools
        .iter()
        .filter(|p| {
            p.asset.eq_ignore_ascii_case(&asset) && matches!(p.kind, PoolKind::V4 | PoolKind::Curve)
        })
        .collect();
    if descs.is_empty() {
        bail!("no v4/curve pools for {asset} in {}", a.config.display());
    }
    let ticker = format!("{asset}/USD");
    let ticker_id = resolve_ticker_id(&ticker);
    // Refuse before any network call.
    std::fs::create_dir_all(&a.out_root)?;
    precheck(&a.out_root, ticker_id, &asset, a.from, a.to, a.force, today)?;
    let q = sec.quality.get(&asset);
    let rate_excluded = q.is_some_and(|q| q.rate.is_some());
    let peg = q.and_then(|q| q.peg.map(|p| (p, q.band_bps)));
    let params = Params {
        twap_ms: sec.twap_secs * 1000,
        max_age_ms: sec.max_age_ms,
        max_dev_bps: sec.max_dev_bps,
        min_depth: sec.depth_floor(&asset, &descs[0].quote),
        warmup_ms: sec.warmup_secs * 1000,
        min_legs: q.and_then(|q| q.min_legs).unwrap_or(sec.min_legs),
        peg,
        sanity: (peg.is_none() && rate_excluded).then_some(RATE_SANITY),
        hold_cap_ms: a.hold_cap_hours * 3_600_000,
    };
    if rate_excluded {
        eprintln!("warning: {asset}: vault-rate leg excluded (no free archive RPC); pool composite only, static band {RATE_SANITY:?} instead of a rate anchor");
    }
    // Config sanity: USDC-quoted pools with both tokens named, 6 decimals on-chain.
    let mut checked = std::collections::HashSet::new();
    for d in &descs {
        let (Some(bt), Some(qt)) = (d.base_token.as_deref(), d.quote_token.as_deref()) else {
            bail!(
                "pool {} lacks base_token/quote_token",
                d.locator().unwrap_or("")
            );
        };
        if !d.quote.eq_ignore_ascii_case("USDC") {
            bail!(
                "pool {} quote is {}, not USDC",
                d.locator().unwrap_or(""),
                d.quote
            );
        }
        let rpcs = &sec
            .chains
            .get(&d.chain)
            .ok_or_else(|| anyhow!("no chain {}", d.chain))?
            .rpcs;
        for t in [bt, qt] {
            if checked.insert((d.chain.clone(), t.to_lowercase())) {
                assert_decimals_6(rpcs, t)?;
            }
        }
    }
    let (from_ms, end_ms) = (
        day_start_ms(a.from) as u64,
        (day_start_ms(a.to) + MS_PER_DAY) as u64,
    );
    let hs = Hs::new()?;
    let mut bounds: HashMap<u64, (u64, u64)> = HashMap::new();
    let mut legs = vec![];
    let mut used = vec![];
    for d in &descs {
        let cid = chain_id(&d.chain)?;
        let (b0, b1) = match bounds.get(&cid) {
            Some(b) => *b,
            None => {
                let h = hs.height(cid)?;
                let tip = hs
                    .block_ts(cid, h)?
                    .ok_or_else(|| anyhow!("no tip block on chain {cid}"))?;
                if tip * 1000 < end_ms {
                    bail!("HyperSync tip of chain {cid} (ts {tip}) is before the end of --to: data would be truncated");
                }
                let b = (
                    hs.first_block_at(cid, (from_ms - LOOKBACK_MS) / 1000, h)?,
                    hs.first_block_at(cid, end_ms / 1000, h)?,
                );
                bounds.insert(cid, b);
                b
            }
        };
        let (bt, qt) = (
            d.base_token.as_deref().unwrap_or_default().to_lowercase(),
            d.quote_token.as_deref().unwrap_or_default().to_lowercase(),
        );
        let hooked = d
            .hooks
            .as_deref()
            .is_some_and(|h| h.trim_start_matches("0x").bytes().any(|b| b != b'0'));
        let floor = sec.depth_floor(&d.asset, &d.quote);
        let mut cfg = PoolCfg {
            id: format!("{}:{}:{}", d.chain, d.dex, d.locator().unwrap_or("")),
            kind: d.kind,
            hooked,
            base_is_t0: bt < qt,
            band: sec.depth_band_bps as f64 / 1e4,
            floor,
            fee: d.fee_bps as f64 / 1e4,
            base_idx: 0,
            max_sold_raw: (0.01 * 4.0 * floor * 10f64.powi(DEC as i32)) as u128,
        };
        let logs = if d.kind == PoolKind::V4 {
            let id = d
                .pool_id
                .as_deref()
                .ok_or_else(|| anyhow!("v4 pool without pool_id"))?
                .to_lowercase();
            hs.logs(
                cid,
                b0,
                b1,
                Some(pool_manager(&d.chain)?),
                &topic0(V4_SWAP),
                Some(&id),
            )?
        } else {
            let addr = d
                .address
                .as_deref()
                .ok_or_else(|| anyhow!("curve pool without address"))?
                .to_lowercase();
            cfg.base_idx = match a.curve_base_idx {
                Some(i) => i as i128,
                None => curve_base_idx(&sec.chains[&d.chain].rpcs, &addr, &bt)?,
            };
            hs.logs(cid, b0, b1, Some(&addr), &topic0(CURVE_XCH), None)?
        };
        let ev: Vec<(u64, f64, f64)> = logs
            .iter()
            .filter_map(|(l, ts)| {
                let s = if cfg.kind == PoolKind::V4 {
                    let (sq, li, ti) = parse_v4_swap(&l.data)?;
                    v4_state(&cfg, sq, li, ti)?
                } else {
                    curve_state(&cfg, parse_curve(&l.data)?)?
                };
                Some((ts * 1000, s.0, s.1))
            })
            .collect();
        eprintln!("{}: {} events", cfg.id, ev.len());
        used.push(cfg.id.clone());
        legs.push(Leg {
            id: cfg.id,
            v4: cfg.kind == PoolKind::V4,
            hooked: cfg.hooked,
            ev,
        });
    }
    let grid_start = from_ms.saturating_sub(params.warmup_ms + params.twap_ms) / 1000 * 1000;
    let quote = fetch_quote(
        &a.quote_ohlc_url,
        grid_start.saturating_sub(120_000),
        end_ms,
        grid_start,
    )?;
    let (recs, stats) = run_grid(&legs, &quote, &params, ticker_id, from_ms, end_ms);
    let (written, skipped) = write_days(
        &a.out_root,
        &ticker,
        ticker_id,
        &asset,
        &recs,
        a.from,
        a.to,
        a.force,
    )?;
    let shards: Vec<Value> = written
        .iter()
        .map(|(d, p)| Ok(serde_json::to_value(shard_entry_for_idx(*d, p)?)?))
        .collect::<Result<_>>()?;
    let side = json!({
        "asset": asset, "ticker": ticker, "ticker_id": ticker_id,
        "range": {"from": a.from.to_string(), "to": a.to.to_string()},
        "legs_used": used,
        "legs_excluded": if rate_excluded { vec!["vault rate convertToAssets (no free archive RPC)"] } else { vec![] },
        "peg_band_gate": params.peg.is_some(),
        "static_sanity_band": params.sanity,
        "leg_held_price_pct": used.iter().zip(&stats.held_pct).collect::<Vec<_>>(),
        "leg_dropped_by_hold_cap_pct": used.iter().zip(&stats.dropped_pct).collect::<Vec<_>>(),
        "hold_cap_hours": a.hold_cap_hours,
        "curve_swap_cap": "input <= 1% of 4 x depth floor; dust < 1 unit ignored",
        "skipped_days": skipped.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
        "shards": shards,
        "records": recs.len(),
        "notes": ["depth: constant event liquidity over the band (v4), optimistic vs live tick walk; curve depth = 4 x floor",
                  "curve price is a fill price from TokenExchange amounts, fee-unbiased by fee_bps",
                  "no venue trust / chain-dark gates; no warmup stall-credit (any red second restarts warmup)"],
        "weights": "default", "backfill": true, "hypersync_max_rps": HS_RPS,
        "config_sha256": series_factory::sharding::sha256_file(&a.config)?,
        "tool_commit": option_env!("SF_GIT_SHA").unwrap_or("unknown"),
        "tool_dirty": option_env!("SF_GIT_DIRTY").unwrap_or("unknown"),
        "ts": Utc::now().to_rfc3339(), "source": "hypersync",
    });
    let sp = sidecar_path(&a.out_root, &asset);
    write_shard_atomic(&sp, serde_json::to_string_pretty(&side)?.as_bytes())
        .with_context(|| sp.display().to_string())?;
    eprintln!(
        "{asset}: {} records, {} shards -> {}",
        recs.len(),
        written.len(),
        a.out_root.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use series_factory::sharding::read_manifest;

    fn pcfg(base_is_t0: bool) -> PoolCfg {
        PoolCfg {
            id: "p".into(),
            kind: PoolKind::V4,
            hooked: false,
            base_is_t0,
            band: 0.005,
            floor: 50_000.0,
            fee: 0.0001,
            base_idx: 0,
            max_sold_raw: 2_000_000_000,
        }
    }

    fn w(v: i128) -> String {
        let pad = if v < 0 { "ff" } else { "00" };
        format!("{}{:032x}", pad.repeat(16), v as u128)
    }

    fn v4_data(sqrt: u128, liq: u128, tick: i32) -> String {
        format!(
            "0x{}{}{}{}{}{}",
            w(-5),
            w(5),
            w(sqrt as i128),
            w(liq as i128),
            w(tick as i128),
            w(0)
        )
    }

    fn response() -> Value {
        let sqrt = (Q96 * 1.0005f64.sqrt()) as u128;
        json!({"data": [{
            "blocks": [{"number": "0x10", "timestamp": "0x6720c000"}],
            "logs": [{"block_number": "0x10", "log_index": "0x2", "topics": ["0xAB", "0xCD"],
                      "data": v4_data(sqrt, 3_000_000_000_000, -7)}]}],
            "next_block": "0x20"})
    }

    #[test]
    fn decodes_price_and_orientation() {
        assert!((v3_price_sqrtpx96(Q96 as u128, 6, 6, true).unwrap() - 1.0).abs() < 1e-12);
        let sqrt = (Q96 * 1.0005f64.sqrt()) as u128;
        let (p0, p1) = (
            v3_price_sqrtpx96(sqrt, 6, 6, true).unwrap(),
            v3_price_sqrtpx96(sqrt, 6, 6, false).unwrap(),
        );
        assert!((p0 - 1.0005).abs() < 1e-9 && (p0 * p1 - 1.0).abs() < 1e-12);
    }

    #[test]
    fn parses_canned_hypersync_page() {
        let (logs, next) = parse_response(&response()).unwrap();
        assert_eq!((logs.len(), next, logs[0].1), (1, 0x20, 0x6720c000));
        let l = &logs[0].0;
        assert_eq!((l.block, l.idx, l.topics[0].as_str()), (16, 2, "0xab"));
        let (sq, liq, tick) = parse_v4_swap(&l.data).unwrap();
        assert_eq!((liq, tick), (3_000_000_000_000, -7));
        let (px, depth) = v4_state(&pcfg(true), sq, liq, tick).unwrap();
        assert!((px - 1.0005).abs() < 1e-9 && depth > 0.0);
    }

    #[test]
    fn curve_price_is_fee_unbiased_and_ignores_dust() {
        let mut c = pcfg(true);
        c.kind = PoolKind::Curve;
        let sell = curve_state(&c, (0, 1_000_000_000, 1, 999_900_000))
            .unwrap()
            .0;
        let buy = curve_state(&c, (1, 1_000_100_000, 0, 1_000_000_000))
            .unwrap()
            .0;
        assert!(
            (sell - 1.0).abs() < 1e-9 && (buy - 1.0).abs() < 1e-4,
            "{sell} {buy}"
        );
        assert!(curve_state(&c, (0, 10, 1, 9)).is_none());
    }

    #[test]
    fn limiter_spaces_calls_and_honours_backoff() {
        let l = Limiter::new(5);
        let t = std::time::Instant::now();
        let waits: Vec<_> = (0..4).map(|_| l.reserve(t)).collect();
        let ms: Vec<u128> = waits.iter().map(|d| d.as_millis()).collect();
        assert_eq!(ms, vec![0, 200, 400, 600]);
        // never more than 5 starts in any 1 s window
        assert!(waits[4 - 1] - waits[0] >= std::time::Duration::from_millis(600));
        l.back_off(std::time::Duration::from_secs(3));
        assert!(l.reserve(std::time::Instant::now()) >= std::time::Duration::from_millis(2900));
    }

    #[test]
    fn refuses_today_and_inverted_range() {
        let d = |s: &str| s.parse::<NaiveDate>().unwrap();
        assert!(validate_range(d("2026-10-01"), d("2026-11-02"), d("2026-11-02")).is_err());
        assert!(validate_range(d("2026-10-05"), d("2026-10-01"), d("2026-11-02")).is_err());
        assert!(validate_range(d("2026-10-01"), d("2026-11-01"), d("2026-11-02")).is_ok());
    }

    fn params() -> Params {
        Params {
            twap_ms: 60_000,
            max_age_ms: 10_000,
            max_dev_bps: 50,
            min_depth: 50_000.0,
            warmup_ms: 300_000,
            min_legs: 1,
            peg: Some((1.0, 10)),
            sanity: None,
            hold_cap_ms: 6 * 3_600_000,
        }
    }

    fn legs(day0: u64) -> Vec<Leg> {
        // Pool a drifts, pool b flat; a goes off-peg (30 bp) mid-day.
        let a = vec![
            (day0, 1.0, 1e6),
            (day0 + 3_600_000, 1.0002, 1e6),
            (day0 + 7_200_000, 1.003, 1e6),
        ];
        let b = vec![(day0 - 3_600_000, 1.0, 2e6)];
        vec![
            Leg {
                id: "a".into(),
                v4: false,
                hooked: false,
                ev: a,
            },
            Leg {
                id: "b".into(),
                v4: true,
                hooked: false,
                ev: b,
            },
        ]
    }

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "onchain-backfill-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn quote_at(day0: u64) -> Quote {
        Quote(vec![(day0 - 3_600_000, 1.0)])
    }

    fn day() -> (NaiveDate, u64) {
        let d: NaiveDate = "2026-10-01".parse().unwrap();
        (d, day_start_ms(d) as u64)
    }

    fn grid(legs: &[Leg], p: &Params, day0: u64) -> (Vec<(u64, IndexRecord)>, GridStats) {
        let id = resolve_ticker_id("USDAT/USD");
        run_grid(legs, &quote_at(day0), p, id, day0, day0 + MS_PER_DAY as u64)
    }

    fn mid(r: &(u64, IndexRecord)) -> f64 {
        let i = r.1.index;
        i.bid
    }

    #[test]
    fn grid_is_deterministic_and_gated() {
        let (_, day0) = day();
        let (r1, _) = grid(&legs(day0), &params(), day0);
        let (r2, _) = grid(&legs(day0), &params(), day0);
        let bytes = |r: &Vec<(u64, IndexRecord)>| {
            r.iter()
                .flat_map(|x| bytemuck::bytes_of(&x.1).to_vec())
                .collect::<Vec<u8>>()
        };
        assert!(!r1.is_empty() && bytes(&r1) == bytes(&r2));
        // first record at day start: the warmup was pre-rolled.
        assert_eq!(r1[0].0, day0);
        assert!((mid(&r1[0]) - 1.0).abs() < 1e-6);
        // no record may leave the 10 bp peg band.
        assert!(r1.iter().all(|r| (mid(r) - 1.0).abs() <= 0.0010 + 1e-9));
    }

    #[test]
    fn flat_market_heartbeats_every_5s_and_reports_held_price() {
        let (_, day0) = day();
        let flat = vec![Leg {
            id: "a".into(),
            v4: false,
            hooked: false,
            ev: vec![(day0 - 3_600_000, 1.0, 1e6)],
        }];
        let mut p = params();
        p.hold_cap_ms = 48 * 3_600_000;
        let (r, st) = grid(&flat, &p, day0);
        assert!(r.windows(2).all(|w| w[1].0 - w[0].0 == HEARTBEAT_MS));
        assert_eq!(r.len(), MS_PER_DAY as usize / 5000);
        // the only swap is 1 h old: the price is held (older than max_age) for the whole day.
        assert!(st.held_pct[0] > 99.9 && st.dropped_pct[0] == 0.0);
    }

    #[test]
    fn hold_cap_drops_a_silent_leg() {
        let (_, day0) = day();
        let one = vec![Leg {
            id: "a".into(),
            v4: false,
            hooked: false,
            ev: vec![(day0 - 3_600_000, 1.0, 1e6)],
        }];
        let mut p = params();
        p.hold_cap_ms = 2 * 3_600_000;
        let (r, st) = grid(&one, &p, day0);
        // alive until 1 h into the day, then dropped (no leg => no composite).
        assert!(r.iter().all(|x| x.0 <= day0 + 3_600_000));
        assert!(st.dropped_pct[0] > 90.0);
    }

    #[test]
    fn static_band_gates_a_peg_less_asset() {
        let (_, day0) = day();
        let mk = |px: f64| {
            vec![Leg {
                id: "a".into(),
                v4: false,
                hooked: false,
                ev: vec![(day0 - 3_600_000, px, 1e6)],
            }]
        };
        let mut p = params();
        (p.peg, p.sanity) = (None, Some(RATE_SANITY));
        assert!(!grid(&mk(1.18), &p, day0).0.is_empty());
        assert!(grid(&mk(0.95), &p, day0).0.is_empty());
        assert!(grid(&mk(1.5), &p, day0).0.is_empty());
    }

    #[test]
    fn paginate_walks_to_the_end_and_bails_without_progress() {
        let ok = paginate(0, 10, |c| Ok((vec![c], (c + 4).min(10)))).unwrap();
        assert_eq!(ok, vec![0, 4, 8]);
        let stuck = paginate(0, 10, |c| Ok((vec![c], c)));
        assert!(stuck.unwrap_err().to_string().contains("no progress"));
        assert!(paginate::<u8>(0, 10, |c| Ok((vec![], c.saturating_sub(1)))).is_err());
    }

    #[test]
    fn only_rate_limit_and_server_errors_retry() {
        assert!(retryable(429) && retryable(503) && retryable(408));
        assert!(!retryable(400) && !retryable(401) && !retryable(404));
    }

    #[test]
    fn quote_needs_cover_no_big_gap_and_a_sane_range() {
        let m = 60_000u64;
        let bars = |r: std::ops::Range<u64>, v: f64| r.map(|i| (i * m, v)).collect::<Vec<_>>();
        assert!(Quote::new(bars(10..100, 1.0), 10 * m, 99 * m).is_ok());
        assert!(
            Quote::new(bars(11..100, 1.0), 10 * m, 99 * m).is_err(),
            "starts late"
        );
        assert!(
            Quote::new(bars(10..100, 1.05), 10 * m, 99 * m).is_err(),
            "range"
        );
        let mut gap = bars(10..30, 1.0);
        gap.extend(bars(40..100, 1.0));
        assert!(Quote::new(gap, 10 * m, 99 * m).is_err(), "gap");
        assert!(
            Quote::new(bars(10..50, 1.0), 10 * m, 99 * m).is_err(),
            "ends early"
        );
    }

    fn recs_for(day0: u64) -> Vec<(u64, IndexRecord)> {
        grid(&legs(day0), &params(), day0).0
    }

    #[test]
    fn e2e_writes_shard_manifest_and_refuses_overwrite() {
        let (d, day0) = day();
        let id = resolve_ticker_id("USDAT/USD");
        let recs = recs_for(day0);
        let root = tmp("e2e");
        let (w, skipped) = write_days(&root, "USDAT/USD", id, "USDAT", &recs, d, d, false).unwrap();
        assert!(skipped.is_empty() && w.len() == 1);
        let bytes = std::fs::read(&w[0].1).unwrap();
        assert_eq!(bytes.len(), recs.len() * 56);
        let back: &[IndexRecord] = bytemuck::cast_slice(&bytes);
        let (t, f) = {
            let i = back[0].index;
            (i.ticker, i.flags)
        };
        assert_eq!(t, id);
        assert_eq!(
            f,
            FLAG_HISTORICAL_BACKFILL | FLAG_NO_BOOK | FLAG_CONF_ACTIVE
        );
        assert!(manifest_path(&idx_dir(&root, id)).exists());
        assert!(write_days(&root, "USDAT/USD", id, "USDAT", &recs, d, d, false).is_err());
        assert!(write_days(&root, "USDAT/USD", id, "USDAT", &recs, d, d, true).is_ok());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn force_deletes_a_stale_shard_on_a_rowless_day_and_empty_run_fails() {
        let (d, day0) = day();
        let d2 = d.succ_opt().unwrap();
        let id = resolve_ticker_id("USDAT/USD");
        let recs = recs_for(day0); // rows on d only
        let root = tmp("stale");
        write_days(&root, "USDAT/USD", id, "USDAT", &recs, d, d, false).unwrap();
        let stale = shard_path(&idx_dir(&root, id), d2, "idx");
        std::fs::write(&stale, [0u8; 56]).unwrap();
        let (w, skipped) = write_days(&root, "USDAT/USD", id, "USDAT", &recs, d, d2, true).unwrap();
        assert_eq!((w.len(), skipped), (1, vec![d2]));
        assert!(!stale.exists());
        let m = read_manifest(&manifest_path(&idx_dir(&root, id)))
            .unwrap()
            .unwrap();
        assert_eq!(m.shards.len(), 1);
        let err = write_days(&root, "USDAT/USD", id, "USDAT", &[], d, d, true).unwrap_err();
        assert!(err.to_string().contains("nothing written"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn refuses_a_held_writer_lock_a_symlink_and_a_live_dir() {
        let (d, day0) = day();
        let id = resolve_ticker_id("USDAT/USD");
        let recs = recs_for(day0);
        // writer lock held elsewhere
        let root = tmp("lock");
        let dir = idx_dir(&root, id);
        std::fs::create_dir_all(&dir).unwrap();
        let _held = nxr_sdk::shard::acquire_idx_writer_lock(&dir).unwrap();
        let e = write_days(&root, "USDAT/USD", id, "USDAT", &recs, d, d, false).unwrap_err();
        assert!(e.to_string().contains("writer lock"), "{e}");
        // symlinked ticker dir
        let root2 = tmp("link");
        let real = tmp("real");
        std::fs::create_dir_all(root2.join("indexes")).unwrap();
        std::os::unix::fs::symlink(&real, idx_dir(&root2, id)).unwrap();
        let e = write_days(&root2, "USDAT/USD", id, "USDAT", &recs, d, d, false).unwrap_err();
        assert!(e.to_string().contains("symlink"), "{e}");
        // a shard dated today marks a live dir
        let root3 = tmp("live");
        let dir3 = idx_dir(&root3, id);
        std::fs::create_dir_all(&dir3).unwrap();
        let today = Utc::now().date_naive();
        std::fs::write(shard_path(&dir3, today, "idx"), [0u8; 56]).unwrap();
        assert!(precheck(&root3, id, "USDAT", d, d, false, today).is_err());
        // an existing sidecar blocks without --force
        let root4 = tmp("side");
        std::fs::write(sidecar_path(&root4, "USDAT"), "{}").unwrap();
        assert!(precheck(&root4, id, "USDAT", d, d, false, today).is_err());
        assert!(precheck(&root4, id, "USDAT", d, d, true, today).is_ok());
        for r in [root, root2, real, root3, root4] {
            let _ = std::fs::remove_dir_all(r);
        }
    }

    #[test]
    fn shipped_config_selects_the_three_assets_pools() {
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("../config.yml");
        let Ok(sec) = load_section(&p) else { return };
        for (asset, n) in [("MUSD", 2), ("SYRUPUSDC", 2), ("USDAT", 1)] {
            let c = sec
                .pools
                .iter()
                .filter(|x| {
                    x.asset.eq_ignore_ascii_case(asset)
                        && matches!(x.kind, PoolKind::V4 | PoolKind::Curve)
                })
                .count();
            assert_eq!(c, n, "{asset}");
        }
    }
}
