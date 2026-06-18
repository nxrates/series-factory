//! Fetch historical crypto aggTrades/ticks for renko calibration.
//!
//! Populates `$NXR_DATA_TICKS/<exchange>/<SYMBOL>/*.ticks` for every
//! (pair x exchange) pair in the series pipeline config, covering
//! `[to - days, to)` where `to` is midnight UTC of the current day.
//!
//! Downstream (`generate-renko-from-ticks`) read from
//! the same directory. Re-running is safe: the monthly/daily cache skips
//! archives that already exist on disk.
//!
//! Usage: fetch-crypto-history <nxrates.yml> [--pairs BTC,ETH] [--exchanges binance,bybit]
//!                              [--days 545] [--quote USDT] [--parallelism 4]
//!
//! FX has no public historical source, so calibration is crypto-only. Run this
//! bin before any binary that depends on pre-populated `.ticks` archives.

use anyhow::Result;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};
use clap::Parser;
use serde::Serialize;
use series_factory::{
    sources::{create_source, TickSource},
    types::{AggregationMode, Config, DataSource, TickFrame},
};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(about = "Fetch historical crypto ticks for renko calibration.")]
struct Args {
    /// Path to nxrates.yml (reads `series.pipeline.{pairs,exchanges}`).
    config: PathBuf,

    /// Comma-separated pairs (override config).
    #[arg(long)]
    pairs: Option<String>,

    /// Comma-separated exchanges (override config).
    #[arg(long)]
    exchanges: Option<String>,

    /// History window in days (from = to - days).
    /// Default covers `bootstrap_days + rolling_window_days` for full calibration.
    #[arg(long)]
    days: Option<i64>,

    /// Explicit window start (YYYY-MM-DD, inclusive). When set together with
    /// `--to-date` this overrides the `--days`-from-today window. Used by
    /// `backfill-all`'s monthly stream-and-delete loop to fetch exactly one
    /// calendar month at a time so raw `.ticks` never accumulate the full range.
    #[arg(long)]
    from_date: Option<String>,

    /// Explicit window end (YYYY-MM-DD, inclusive). Pairs with `--from-date`.
    #[arg(long)]
    to_date: Option<String>,

    /// Quote asset (most crypto aggTrades archives are vs USDT).
    #[arg(long, default_value = "USDT")]
    quote: String,

    /// Max concurrent (pair x exchange) fetchers.
    #[arg(long, default_value = "4")]
    parallelism: usize,

    /// Probe-only mode: HEAD-check archive coverage per (pair, exchange) and
    /// emit JSON `[{ticker, exchange, has_data, first_date, last_date}]` on
    /// stdout. Skips actual downloads. Used by `backfill-all` to drop
    /// uncovered exchanges before paying download cost.
    #[arg(long)]
    probe: bool,
}

#[derive(Debug, Serialize)]
struct ProbeResult {
    ticker: String,
    exchange: String,
    has_data: bool,
    first_date: Option<String>,
    last_date: Option<String>,
}

/// HEAD-check archive availability for one (pair, exchange) over the window.
/// Returns earliest + latest month w/ a 200 OK monthly archive. Bounded:
/// scans at most `MAX_PROBE_MONTHS` chronological months in each direction.
async fn probe_one(
    pair: &str,
    quote: &str,
    exchange: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ProbeResult {
    use chrono::Datelike;
    const MAX_PROBE_MONTHS: i32 = 72; // 6y cap

    let sym = format!("{}{}", pair.to_uppercase(), quote.to_uppercase());
    let agent = std::sync::Arc::new(
        ureq::AgentBuilder::new()
            .timeout(std::time::Duration::from_secs(15))
            .build(),
    );

    // Probe URL template sourced from YAML
    // `cexs.exchanges.<exch>.archive_url_template.probe` (phase 59.R3.C2.O4,
    // 2026-05-30). `{sym}` / `{y:04}` / `{m:02}` are filled at probe time.
    let probe_tpl: Option<String> = match exchange {
        "binance" | "bybit" => {
            let t = series_factory::sources::common::archive_urls(if exchange == "binance" {
                "binance"
            } else {
                "bybit"
            })
            .probe
            .clone();
            if t.is_empty() { None } else { Some(t) }
        }
        // bitget/okx have no easily-HEADable monthly archive convention
        // (per-day only) — return None to skip probing; fetcher itself
        // will discover coverage on attempt.
        _ => None,
    };
    let url_for = |y: i32, m: u32| -> Option<String> {
        probe_tpl.as_ref().map(|t| {
            t.replace("{sym}", &sym)
                .replace("{y:04}", &format!("{:04}", y))
                .replace("{m:02}", &format!("{:02}", m))
        })
    };

    // walk forward to find first available month
    let mut first: Option<NaiveDate> = None;
    let mut last: Option<NaiveDate> = None;
    let mut cursor = NaiveDate::from_ymd_opt(from.year(), from.month(), 1).unwrap();
    let stop = NaiveDate::from_ymd_opt(to.year(), to.month(), 1).unwrap();
    let mut scanned = 0;
    while cursor <= stop && scanned < MAX_PROBE_MONTHS {
        scanned += 1;
        let url = match url_for(cursor.year(), cursor.month()) {
            Some(u) => u,
            None => {
                // unknown probe mapping → bail w/ has_data=true (don't block)
                return ProbeResult {
                    ticker: pair.to_string(),
                    exchange: exchange.to_string(),
                    has_data: true,
                    first_date: None,
                    last_date: None,
                };
            }
        };
        let ok = head_ok(&agent, &url).await;
        if ok {
            first = Some(cursor);
            break;
        }
        cursor = next_month(cursor);
    }
    if first.is_none() {
        return ProbeResult {
            ticker: pair.to_string(),
            exchange: exchange.to_string(),
            has_data: false,
            first_date: None,
            last_date: None,
        };
    }
    // walk backward from `to` to find last available
    let mut cursor = stop;
    let mut scanned = 0;
    while scanned < MAX_PROBE_MONTHS {
        scanned += 1;
        let url = match url_for(cursor.year(), cursor.month()) {
            Some(u) => u,
            None => break,
        };
        if head_ok(&agent, &url).await {
            last = Some(cursor);
            break;
        }
        cursor = prev_month(cursor);
        if cursor < first.unwrap() {
            break;
        }
    }
    ProbeResult {
        ticker: pair.to_string(),
        exchange: exchange.to_string(),
        has_data: true,
        first_date: first.map(|d| d.format("%Y-%m-%d").to_string()),
        last_date: last.map(|d| d.format("%Y-%m-%d").to_string()),
    }
}

fn next_month(d: NaiveDate) -> NaiveDate {
    if d.month() == 12 {
        NaiveDate::from_ymd_opt(d.year() + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(d.year(), d.month() + 1, 1).unwrap()
    }
}

fn prev_month(d: NaiveDate) -> NaiveDate {
    if d.month() == 1 {
        NaiveDate::from_ymd_opt(d.year() - 1, 12, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(d.year(), d.month() - 1, 1).unwrap()
    }
}

async fn head_ok(agent: &std::sync::Arc<ureq::Agent>, url: &str) -> bool {
    let agent = agent.clone();
    let url = url.to_string();
    tokio::task::spawn_blocking(move || {
        let resp = agent.head(&url).call();
        match resp {
            Ok(r) => r.status() >= 200 && r.status() < 300,
            Err(_) => false,
        }
    })
    .await
    .unwrap_or(false)
}

use nxr_sdk::pipeline_config::PipelineYml as YmlRoot;

fn midnight_utc_today() -> DateTime<Utc> {
    Utc::now()
        .date_naive()
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .and_utc()
}

fn parse_csv(arg: Option<&str>, fallback: &[String]) -> Vec<String> {
    match arg {
        Some(s) => s.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect(),
        None => fallback.to_vec(),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let root: YmlRoot = YmlRoot::load(&args.config)?;

    let pairs = parse_csv(args.pairs.as_deref(), &root.series.pipeline.pairs);
    let exchanges = parse_csv(args.exchanges.as_deref(), &root.series.pipeline.exchanges);

    if pairs.is_empty() {
        anyhow::bail!("no pairs to fetch (empty config.series.pipeline.pairs and no --pairs)");
    }
    if exchanges.is_empty() {
        anyhow::bail!("no exchanges to fetch (empty config.series.pipeline.exchanges and no --exchanges)");
    }

    let max_window: i64 = root.series.calibration.rolling_window_days as i64;
    let default_days = root.series.pipeline.bootstrap_days + max_window;
    let days = args.days.unwrap_or(default_days);

    // Window resolution: explicit `--from-date`/`--to-date` (used by the
    // monthly stream-and-delete loop) take precedence over the `--days`
    // -from-today default. Both bounds are inclusive calendar days.
    let (from, to) = match (args.from_date.as_deref(), args.to_date.as_deref()) {
        (Some(f), Some(t)) => {
            let parse_day = |s: &str| -> Result<DateTime<Utc>> {
                let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| anyhow::anyhow!("bad date {s:?}: {e}"))?;
                Ok(d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc())
            };
            let from = parse_day(f)?;
            // `--to-date` is inclusive; `fetch_monthly_daily`'s daily loop runs
            // `day <= config.to.date_naive()`, so the naive date of `to` must be
            // the last day we want. Midnight of that day satisfies that.
            let to = parse_day(t)?;
            (from, to)
        }
        _ => {
            let to = midnight_utc_today();
            (to - Duration::days(days), to)
        }
    };

    let nxr_cfg = nxr_sdk::NxrConfig::from_env();
    let ticks_dir = PathBuf::from(&nxr_cfg.ticks_dir);

    // probe-only mode: HEAD-check every (pair, exchange) → JSON stdout
    if args.probe {
        let mut results: Vec<ProbeResult> = Vec::new();
        for pair in &pairs {
            for exchange in &exchanges {
                let r = probe_one(pair, &args.quote, exchange, from, to).await;
                info!(
                    ticker = %r.ticker,
                    exchange = %r.exchange,
                    has_data = r.has_data,
                    first_date = ?r.first_date,
                    last_date = ?r.last_date,
                    "probe"
                );
                results.push(r);
            }
        }
        println!("{}", serde_json::to_string(&results)?);
        return Ok(());
    }

    info!(
        "fetching {} pairs x {} exchanges, window={} days [{} -> {}), ticks_dir={}",
        pairs.len(),
        exchanges.len(),
        days,
        from.format("%Y-%m-%d"),
        to.format("%Y-%m-%d"),
        ticks_dir.display(),
    );

    let sem = Arc::new(Semaphore::new(args.parallelism.max(1)));
    let mut handles = Vec::new();

    for pair in &pairs {
        for exchange in &exchanges {
            let cfg = Config {
                base: pair.to_uppercase(),
                quote: args.quote.to_uppercase(),
                sources: vec![exchange.clone()],
                from,
                to,
                agg_mode: AggregationMode::Time,
                agg_step: 60_000.0,
                cycle_ms: 100,
                stale_secs: 30.0,
                z_threshold: 6.0,
                ticks_dir: ticks_dir.clone(),
                bars_dir: PathBuf::from(&nxr_cfg.bars_dir),
            };
            let permit = sem.clone().acquire_owned().await.unwrap();
            let exchange = exchange.clone();
            let pair = pair.clone();
            handles.push(tokio::spawn(async move {
                let _p = permit;
                let result = fetch_one(&cfg, &exchange).await;
                match &result {
                    Ok(n) => info!("[{}/{}] ok ({} tick batches)", exchange, pair, n),
                    Err(e) => error!("[{}/{}] failed: {:#}", exchange, pair, e),
                }
                (pair, exchange, result)
            }));
        }
    }

    let mut ok = 0usize;
    let mut failed = 0usize;
    for h in handles {
        match h.await {
            Ok((_p, _e, Ok(_))) => ok += 1,
            Ok((_, _, Err(_))) => failed += 1,
            Err(e) => {
                warn!("task join error: {:#}", e);
                failed += 1;
            }
        }
    }

    info!("done: {} ok, {} failed", ok, failed);
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn fetch_one(cfg: &Config, exchange: &str) -> Result<usize> {
    let source = create_source(&DataSource::Exchange(exchange.to_string())).await?;
    let (tx, mut rx) = mpsc::channel::<Vec<TickFrame>>(32);

    let cfg_cloned = cfg.clone();
    let fetch = tokio::spawn(async move {
        drive_source(source, &cfg_cloned, tx).await
    });

    let mut batches = 0usize;
    while rx.recv().await.is_some() {
        batches += 1;
    }
    fetch.await??;
    Ok(batches)
}

async fn drive_source(
    source: Box<dyn TickSource>,
    cfg: &Config,
    tx: mpsc::Sender<Vec<TickFrame>>,
) -> Result<()> {
    source.fetch_ticks(cfg, tx).await
}
