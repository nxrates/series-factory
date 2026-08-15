//! Fetch historical OHLC candles for a Pyth Lazer feed and emit a `.ticks`
//! file, compatible with the SAME downstream pipeline `fetch-crypto-history`
//! feeds (`ticks-to-idx` -> `merge-idx` -> `s10-from-idx`).
//!
//! Why this exists (2026-07-21 backfill mandate): Pyth Lazer's live WS stream
//! (`wss://pyth-lazer-N.dourolabs.app/v1/stream`) has no history replay - it
//! only pushes the current tick. Historical OHLC lives on a SEPARATE REST
//! service, `https://pyth.dourolabs.app/v1/{channel}/history` (TradingView-UDF
//! shape: `{s,t,o,h,l,c,v}`, `t` in Unix SECONDS). This is NOT the retired
//! legacy Pythnet-backed benchmarks shim (`benchmarks.pyth.network`, hex feed
//! ids, fully removed from this stack 2026-07-21) - that is a different host
//! with a different id space and must never come back into this pipeline.
//!
//! Granularity floor: Pyth's finest `resolution` is `1` (1 minute). Sub-minute
//! backfill is NOT possible from this source - do not attempt to fabricate
//! finer buckets. Newer stablecoin feeds (U, USDG, USDF, BFUSD, USDTB) only
//! have ~2 weeks of real history behind them regardless of how far back
//! `--from` reaches; a wide window is NOT an error, Pyth just returns
//! whatever it actually has (empty tail, no error code).
//!
//! No real order book behind an index candle -> `bid == ask == close`
//! (`honest_tick` policy, operator ruling 2026-07-04: never fabricate a
//! spread). Volume has no signed side (unlike a CEX trade tape) so it is
//! split evenly across `vbid`/`vask` rather than guessing a side - `merge-idx`
//! already auto-flags `FLAG_NO_BOOK` whenever `bid == ask`, and separately
//! tags every row `FLAG_HISTORICAL_BACKFILL`, so this is the same non-live
//! provenance marking every other offline backfill gets. Nothing here needs a
//! new flag or a new server code path.
//!
//! Usage:
//!   fetch-pyth-history <BASE> [--quote USD] [--resolution 1]
//!     [--from-unix <secs>] [--to-unix <secs>] [--channel fixed_rate@200ms]
//!
//! Output: `$NXR_DATA_TICKS/pyth/<BASE><QUOTE>/history.ticks`
//! (re-running overwrites this one file - idempotent, no partial-accumulation
//! risk, matches the "re-fetch is safe" precedent in fetch-crypto-history).

use anyhow::{bail, Context, Result};
use clap::Parser;
use mitch::{Tick, TickFrame};
use series_factory::sources::{common::ensure_parent_dir, provider_id_for};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(about = "Fetch Pyth Lazer historical OHLC and emit a .ticks file.")]
struct Args {
    /// Base asset symbol as used in `oracles.providers.pyth.symbols` (e.g. U, USDG, EURC).
    base: String,
    /// Quote (always USD for the stablecoin/USD pegs this tool targets).
    #[arg(long, default_value = "USD")]
    quote: String,
    /// Pyth history resolution in minutes. `1` is the finest available -
    /// sub-minute is NOT served by this endpoint, do not lower this.
    #[arg(long, default_value = "1")]
    resolution: String,
    /// Window start, Unix seconds. Default: 30 days back (Pyth silently caps
    /// to whatever it actually retains - a wide window is not an error).
    #[arg(long)]
    from_unix: Option<i64>,
    /// Window end, Unix seconds. Default: now.
    #[arg(long)]
    to_unix: Option<i64>,
    /// Lazer channel - MUST match the feed's `min_channel` from
    /// `/v1/symbols` (config.yml already pins this per-provider: all our
    /// stablecoin feeds are `fixed_rate@200ms`).
    #[arg(long, default_value = "fixed_rate@200ms")]
    channel: String,
    /// Pyth feed family prefix, i.e. the part of the upstream symbol before the
    /// dot (`Crypto.XAUT/USD`, `FX.USD/BRL`, `Metal.XAU/USD`,
    /// `Commodities.Index.NATGAS/USD`). Was hardcoded to `Crypto`, which made
    /// every FX, metal and commodity feed in `oracles.providers.pyth.symbols`
    /// unreachable by this tool: the request 404'd and no history was ever
    /// backfilled for them. Verify against
    /// https://history.pyth-lazer.dourolabs.app/history/v1/symbols.
    #[arg(long, default_value = "Crypto")]
    family: String,
    /// Override output path (default: `$NXR_DATA_TICKS/pyth/<BASE><QUOTE>/history.ticks`).
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(serde::Deserialize, Debug)]
struct HistoryResponse {
    s: String,
    #[serde(default)]
    t: Vec<i64>,
    #[serde(default)]
    #[allow(dead_code)]
    o: Vec<f64>,
    #[serde(default)]
    #[allow(dead_code)]
    h: Vec<f64>,
    #[serde(default)]
    #[allow(dead_code)]
    l: Vec<f64>,
    #[serde(default)]
    c: Vec<f64>,
    #[serde(default)]
    v: Vec<f64>,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");

    let args = Args::parse();
    let to = args.to_unix.unwrap_or_else(now_unix);
    let from = args.from_unix.unwrap_or(to - 30 * 86_400);

    let base_uc = args.base.to_uppercase();
    let quote_uc = args.quote.to_uppercase();
    let symbol = format!("{}.{base_uc}/{quote_uc}", args.family);
    let ticker_id = nxr_sdk::resolve_ticker_id(&format!("{base_uc}/{quote_uc}"));
    let provider_id = provider_id_for("pyth");
    anyhow::ensure!(
        !nxr_sdk::providers::is_excluded_provider(provider_id),
        "pyth (provider {provider_id}) is HARD-EXCLUDED - refusing to backfill"
    );

    let url = format!("https://pyth.dourolabs.app/v1/{}/history", args.channel);
    info!(symbol, url, from, to, resolution = %args.resolution, "fetching Pyth history");

    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(30))
        .build();
    let mut req = agent
        .get(&url)
        .query("symbol", &symbol)
        .query("resolution", &args.resolution)
        .query("from", &from.to_string())
        .query("to", &to.to_string());
    // Auth becomes mandatory 2026-07-24 (config.yml oracle-relay note) - wire
    // it in now so this tool keeps working past the cutover with no edit.
    if let Ok(token) = std::env::var("NXR_ORACLE_TOKEN_PYTH") {
        req = req.set("Authorization", &format!("Bearer {token}"));
    }
    let resp = req
        .call()
        .with_context(|| format!("GET {url} (symbol={symbol})"))?;
    let raw = resp
        .into_string()
        .with_context(|| format!("read history response body for {symbol}"))?;
    let body: HistoryResponse = serde_json::from_str(&raw)
        .with_context(|| format!("parse history JSON for {symbol}: {raw}"))?;

    if body.s != "ok" {
        bail!("Pyth history returned status '{}' for {symbol}", body.s);
    }
    if body.t.is_empty() {
        warn!(symbol, "Pyth returned zero history rows - nothing to write");
        return Ok(());
    }
    anyhow::ensure!(
        body.t.len() == body.c.len(),
        "t/c array length mismatch for {symbol}: {} vs {}",
        body.t.len(),
        body.c.len()
    );

    let mut frames: Vec<TickFrame> = Vec::with_capacity(body.t.len());
    for i in 0..body.t.len() {
        let close = body.c[i];
        if !(close.is_finite() && close > 0.0) {
            continue; // corrupt row - skip, never fabricate a price
        }
        // No signed trade flow behind an index candle - split evenly rather
        // than guess a side (see module doc: no-fabrication policy).
        let vol_half = (body.v.get(i).copied().unwrap_or(0.0) / 2.0).max(0.0) as u32;
        let px = nxr_sdk::stats::round_to_sig_digits(close, 6);
        let tick = Tick::new_unchecked(ticker_id, px, px, vol_half, vol_half);
        let ts_ms = body.t[i] * 1000;
        frames.push(TickFrame::new(
            provider_id,
            mitch::timestamp::from_epoch_ms(ts_ms),
            tick,
        ));
    }
    anyhow::ensure!(
        !frames.is_empty(),
        "every row for {symbol} was corrupt/non-finite"
    );

    let cfg = nxr_sdk::NxrConfig::from_env();
    let sym_dir = format!("{base_uc}{quote_uc}");
    let out_path = args.out.unwrap_or_else(|| {
        PathBuf::from(&cfg.ticks_dir)
            .join("pyth")
            .join(&sym_dir)
            .join("history.ticks")
    });
    ensure_parent_dir(&out_path)?;
    let bytes: &[u8] = bytemuck::cast_slice(&frames);
    std::fs::write(&out_path, bytes).with_context(|| format!("write {}", out_path.display()))?;

    let first_ms = frames.first().map(|f| f.timestamp_ms()).unwrap_or(0);
    let last_ms = frames.last().map(|f| f.timestamp_ms()).unwrap_or(0);
    info!(
        symbol,
        ticker_id,
        rows = frames.len(),
        first_ts = %chrono::DateTime::from_timestamp_millis(first_ms).map(|d| d.to_rfc3339()).unwrap_or_default(),
        last_ts = %chrono::DateTime::from_timestamp_millis(last_ms).map(|d| d.to_rfc3339()).unwrap_or_default(),
        out = %out_path.display(),
        "wrote .ticks"
    );
    Ok(())
}
