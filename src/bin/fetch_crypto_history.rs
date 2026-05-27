//! Fetch historical crypto aggTrades/ticks for renko calibration.
//!
//! Populates `$NXR_DATA_TICKS/<exchange>/<SYMBOL>/*.ticks` for every
//! (pair x exchange) pair in the series pipeline config, covering
//! `[to - days, to)` where `to` is midnight UTC of the current day.
//!
//! Downstream (`generate-renko-from-ticks`, `optimize-renko-stats`) read from
//! the same directory. Re-running is safe: the monthly/daily cache skips
//! archives that already exist on disk.
//!
//! Usage: fetch-crypto-history <nxrates.yml> [--pairs BTC,ETH] [--exchanges binance,bybit]
//!                              [--days 545] [--quote USDT] [--parallelism 4]
//!
//! FX has no public historical source, so calibration is crypto-only. Run this
//! bin before any binary that depends on pre-populated `.ticks` archives.

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};
use clap::Parser;
use serde::Serialize;
use series_factory::{
    sources::{create_source, TickSource},
    types::{AggregationMode, Config, DataSource, TickFrame},
};
use std::{fs, path::PathBuf, sync::Arc};
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
    /// Default covers `bootstrap_days + max(k_fit_windows_days)` for full calibration.
    #[arg(long)]
    days: Option<i64>,

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

    let url_for = |y: i32, m: u32| -> Option<String> {
        match exchange {
            "binance" => Some(format!(
                "https://data.binance.vision/data/spot/monthly/aggTrades/{s}/{s}-aggTrades-{y:04}-{m:02}.zip",
                s = sym
            )),
            "bybit" => Some(format!(
                "https://public.bybit.com/trading/{s}/{s}{y:04}-{m:02}.csv.gz",
                s = sym
            )),
            // bitget/okx have no easily-HEADable monthly archive convention
            // (per-day only) — return None to skip probing; fetcher itself
            // will discover coverage on attempt.
            _ => None,
        }
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
    let root: YmlRoot = serde_yaml::from_str(
        &fs::read_to_string(&args.config)
            .with_context(|| format!("reading {}", args.config.display()))?,
    )?;

    let pairs = parse_csv(args.pairs.as_deref(), &root.series.pipeline.pairs);
    let exchanges = parse_csv(args.exchanges.as_deref(), &root.series.pipeline.exchanges);

    if pairs.is_empty() {
        anyhow::bail!("no pairs to fetch (empty config.series.pipeline.pairs and no --pairs)");
    }
    if exchanges.is_empty() {
        anyhow::bail!("no exchanges to fetch (empty config.series.pipeline.exchanges and no --exchanges)");
    }

    let max_window: i64 = root.series.calibration.k_fit_windows_days.iter().max().copied().unwrap_or(180) as i64;
    let default_days = root.series.pipeline.bootstrap_days + max_window;
    let days = args.days.unwrap_or(default_days);

    let to = midnight_utc_today();
    let from = to - Duration::days(days);

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
                cycle_ms: 50,
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
