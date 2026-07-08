//! Backfill `.idx` daily shards for pyth-oracle tickers from Pyth Benchmarks.
//!
//! Benchmarks resolution reality (probed 2026-07-08): the TradingView shim
//! floor is 1-minute OHLC; the 1-second `/v1/updates/price/{ts}/{interval}`
//! store is hard-capped at 60 s/request with VAA-heavy payloads (~123 KB per
//! symbol-minute) - a 6-month 1 s backfill would be TB-scale, so 1-minute is
//! the deep-history source. Each 1-minute bar becomes FOUR IndexRecords
//! (O,H,L,C at +0/15/30/45 s) so range survives into s10/renko/vol; records
//! carry `FLAG_HISTORICAL_BACKFILL | FLAG_NO_BOOK` (oracle feeds have no
//! book depth - bid==ask, vbid==vask==0).
//!
//! Feed manifest comes from `config.yml oracles.providers.*.symbols`; the
//! benchmarks catalog maps feed id -> TV symbol, so no extra config.
//!
//! Never touches today's shard (live producer owns it): `--to` is clamped to
//! yesterday UTC. Existing shard dates are skipped unless `--overwrite`
//! (idempotent per-date replace via `ShardedWriter`, cf merge-idx D10).
//!
//! Usage:
//!   pyth-backfill <config.yml> [--symbols all|A,B] [--days 730]
//!     [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--overwrite]
//!     [--benchmarks-url https://benchmarks.pyth.network] [--rate-ms 300]

use anyhow::{Context, Result, bail};
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use clap::Parser;
use mitch::common::message_type;
use mitch::header::MitchHeader;
use nxr_sdk::Index;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{FLAG_HISTORICAL_BACKFILL, FLAG_NO_BOOK, idx_dir};
use series_factory::sharding::{
    Manifest, ShardedWriter, list_shards, manifest_path, shard_entry_for_idx, write_manifest,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tracing::{info, warn};

/// TV-shim empirical max ≈ 7200 bars/request (10 d returns empty) - 5 d is
/// the safe chunk.
const CHUNK_DAYS: i64 = 5;
const FETCH_RETRIES: u32 = 3;

#[derive(Parser, Debug)]
struct Args {
    /// Path to config.yml (oracles section = feed manifest)
    config: String,
    /// "all" or comma-separated canonical symbols (USDT/USD or USDT-USD)
    #[arg(long, default_value = "all")]
    symbols: String,
    /// Lookback window when --from is absent (crypto backfill parity: 730)
    #[arg(long, default_value_t = 730)]
    days: i64,
    #[arg(long)]
    from: Option<NaiveDate>,
    /// Clamped to yesterday UTC - today's shard is owned by the live writer
    #[arg(long)]
    to: Option<NaiveDate>,
    /// Re-fetch + replace dates that already have shards
    #[arg(long, default_value_t = false)]
    overwrite: bool,
    #[arg(long, default_value = "https://benchmarks.pyth.network")]
    benchmarks_url: String,
    /// Sleep between HTTP requests (public infra politeness)
    #[arg(long, default_value_t = 300)]
    rate_ms: u64,
    #[arg(long)]
    data_root: Option<String>,
}

#[derive(serde::Deserialize)]
struct TvHistory {
    s: String,
    #[serde(default)]
    t: Vec<i64>,
    #[serde(default)]
    o: Vec<f64>,
    #[serde(default)]
    h: Vec<f64>,
    #[serde(default)]
    l: Vec<f64>,
    #[serde(default)]
    c: Vec<f64>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();

    let pyml = nxr_sdk::pipeline_config::PipelineYml::load(Path::new(&args.config))
        .with_context(|| format!("loading {}", args.config))?;
    let data_root = match &args.data_root {
        Some(p) => std::path::PathBuf::from(p),
        None => nxr_sdk::config::NxrConfig::from_env().data_root(),
    };
    let data_root = data_root.as_path();

    let want: Option<BTreeSet<String>> = if args.symbols == "all" {
        None
    } else {
        Some(
            args.symbols
                .split(',')
                .map(|s| s.trim().to_uppercase().replace('-', "/"))
                .collect(),
        )
    };

    // (nxr_sym, feed_id) across all oracle providers.
    let mut feeds: Vec<(String, String)> = Vec::new();
    for prov in pyml.oracles.providers.values() {
        for (sym, feed) in &prov.symbols {
            let sym = sym.to_uppercase();
            if want.as_ref().is_none_or(|w| w.contains(&sym)) {
                feeds.push((sym, feed.trim_start_matches("0x").to_ascii_lowercase()));
            }
        }
    }
    if feeds.is_empty() {
        bail!("no oracle symbols matched --symbols {}", args.symbols);
    }

    let today = Utc::now().date_naive();
    let yesterday = today - ChronoDuration::days(1);
    let to = args.to.unwrap_or(yesterday).min(yesterday);
    let from = args.from.unwrap_or(to - ChronoDuration::days(args.days));
    if from > to {
        bail!("empty window: from {from} > to {to}");
    }

    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(60))
        .build();

    // Benchmarks catalog: feed id -> TV symbol string.
    let catalog: Vec<serde_json::Value> = serde_json::from_reader(
        agent
            .get(&format!("{}/v1/price_feeds/", args.benchmarks_url))
            .call()
            .context("benchmarks catalog fetch")?
            .into_reader(),
    )
    .context("benchmarks catalog parse")?;
    let id_to_tv: BTreeMap<String, String> = catalog
        .iter()
        .filter_map(|f| {
            Some((
                f.get("id")?.as_str()?.to_ascii_lowercase(),
                f.pointer("/attributes/symbol")?.as_str()?.to_string(),
            ))
        })
        .collect();

    info!(feeds = feeds.len(), %from, %to, overwrite = args.overwrite, "pyth-backfill starting");

    let mut summary: Vec<serde_json::Value> = Vec::new();
    for (sym, feed_id) in &feeds {
        let Some(tv_sym) = id_to_tv.get(feed_id) else {
            warn!(%sym, %feed_id, "feed id not in benchmarks catalog, skipping");
            continue;
        };
        let Some(ticker_id) = nxr_sdk::try_resolve_ticker_id(sym) else {
            bail!("unresolvable oracle symbol {sym} (stale mitch ids?)");
        };
        match backfill_one(&args, &agent, sym, tv_sym, ticker_id, data_root, from, to) {
            Ok(v) => summary.push(v),
            Err(e) => {
                warn!(%sym, err = %e, "backfill failed for symbol");
                summary.push(serde_json::json!({"symbol": sym, "error": e.to_string()}));
            }
        }
    }
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn backfill_one(
    args: &Args,
    agent: &ureq::Agent,
    sym: &str,
    tv_sym: &str,
    ticker_id: u64,
    data_root: &Path,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<serde_json::Value> {
    let dir = idx_dir(data_root, ticker_id);
    std::fs::create_dir_all(&dir)?;
    let existing: BTreeSet<NaiveDate> = if args.overwrite {
        BTreeSet::new()
    } else {
        list_shards(&dir, "idx")?.into_iter().map(|(d, _)| d).collect()
    };

    // Contiguous missing-date runs, each fetched in <= CHUNK_DAYS windows.
    let mut missing: Vec<NaiveDate> = Vec::new();
    let mut d = from;
    while d <= to {
        if !existing.contains(&d) {
            missing.push(d);
        }
        d += ChronoDuration::days(1);
    }
    if missing.is_empty() {
        info!(%sym, "nothing to do (all dates covered)");
        return Ok(serde_json::json!({"symbol": sym, "ticker_id": ticker_id.to_string(), "days": 0}));
    }

    let mut writer = ShardedWriter::new(dir.clone());
    let mut total_bars = 0usize;
    let mut total_recs = 0usize;
    let mut days_with_data: BTreeSet<NaiveDate> = BTreeSet::new();

    let mut run_start = 0usize;
    while run_start < missing.len() {
        // Extend to the end of this contiguous run.
        let mut run_end = run_start;
        while run_end + 1 < missing.len()
            && missing[run_end + 1] == missing[run_end] + ChronoDuration::days(1)
        {
            run_end += 1;
        }
        let mut chunk_start = missing[run_start];
        let run_last = missing[run_end];
        while chunk_start <= run_last {
            let chunk_end = (chunk_start + ChronoDuration::days(CHUNK_DAYS - 1)).min(run_last);
            let from_ts = chunk_start.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
            let to_ts = chunk_end.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp();
            let hist = fetch_history(agent, &args.benchmarks_url, tv_sym, from_ts, to_ts)?;
            std::thread::sleep(std::time::Duration::from_millis(args.rate_ms));
            if hist.s == "ok" && !hist.t.is_empty() {
                for i in 0..hist.t.len() {
                    let bar_ts_ms = hist.t[i] * 1000;
                    // O,H,L,C at +0/15/30/45 s: intra-bar ORDER of H/L is
                    // unknowable from OHLC; the fixed sequence preserves the
                    // bar's range (what renko/vol need) at 1-min granularity.
                    for (k, px) in
                        [(0i64, hist.o[i]), (15_000, hist.h[i]), (30_000, hist.l[i]), (45_000, hist.c[i])]
                    {
                        if !(px.is_finite() && px > 0.0) {
                            continue;
                        }
                        let ts_ms = bar_ts_ms + k;
                        let index = Index {
                            ticker: ticker_id,
                            bid: px,
                            ask: px,
                            vbid: 0,
                            vask: 0,
                            ci: 0,
                            tick_count: 1,
                            confidence: 0,
                            accepted: 1,
                            rejected: 0,
                            flags: FLAG_HISTORICAL_BACKFILL | FLAG_NO_BOOK,
                        };
                        let header = MitchHeader::new(
                            message_type::INDEX,
                            0, // composite record, mirrors merge-idx
                            mitch::timestamp::from_epoch_ms(ts_ms),
                            1,
                        );
                        writer.append(ts_ms, &IndexRecord { header, index })?;
                        total_recs += 1;
                    }
                    days_with_data.insert(chunk_start); // approx; refined by manifest scan
                }
                total_bars += hist.t.len();
            }
            chunk_start = chunk_end + ChronoDuration::days(1);
        }
        run_start = run_end + 1;
    }
    writer.close()?;

    // Manifest: full-dir rescan, same as merge-idx.
    let shards = list_shards(&dir, "idx")?;
    let mut entries = Vec::with_capacity(shards.len());
    for (date, path) in &shards {
        entries.push(shard_entry_for_idx(*date, path)?);
    }
    let mpath = manifest_path(&dir);
    let mut manifest = Manifest::new(sym.to_string(), ticker_id, "idx");
    manifest.set_shards_batch(entries);
    write_manifest(&mpath, &manifest)?;

    info!(%sym, ticker_id, bars = total_bars, recs = total_recs, "backfill complete");
    Ok(serde_json::json!({
        "symbol": sym,
        "ticker_id": ticker_id.to_string(),
        "tv_symbol": tv_sym,
        "bars": total_bars,
        "records": total_recs,
    }))
}

fn fetch_history(
    agent: &ureq::Agent,
    base: &str,
    tv_sym: &str,
    from_ts: i64,
    to_ts: i64,
) -> Result<TvHistory> {
    let url = format!("{base}/v1/shims/tradingview/history");
    let mut last_err = None;
    for attempt in 0..FETCH_RETRIES {
        match agent
            .get(&url)
            .query("symbol", tv_sym)
            .query("resolution", "1")
            .query("from", &from_ts.to_string())
            .query("to", &to_ts.to_string())
            .call()
        {
            Ok(resp) => {
                let h: TvHistory =
                    serde_json::from_reader(resp.into_reader()).context("history parse")?;
                if h.s == "ok" || h.s == "no_data" {
                    return Ok(h);
                }
                last_err = Some(anyhow::anyhow!("history status {}", h.s));
            }
            Err(e) => last_err = Some(e.into()),
        }
        std::thread::sleep(std::time::Duration::from_millis(1000 * (attempt as u64 + 1)));
    }
    Err(last_err.unwrap())
}
