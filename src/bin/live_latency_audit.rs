//! Real-time live pipeline latency audit: idx → s10 (klines) → renko.
//!
//! Reads today's on-disk shard tips for pipeline primaries and compares wall-clock
//! lag against SLA budgets from `docs/scope.md`.
//!
//! Exit 0 = all pass, 1 = warn, 2 = fail.

use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use clap::Parser;
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::pipeline_config::PipelineYml;
use nxr_sdk::resolve_ticker_id;
use nxr_sdk::shard::{bars_dir, idx_dir, read_shard_aligned, shard_path};
use nxr_sdk::Bar;
use serde::Serialize;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

const IDX_FAIL_MS: i64 = 10_000;
const IDX_WARN_MS: i64 = 2_000;
const S10_FAIL_MS: i64 = 30_000;
const S10_WARN_MS: i64 = 15_000;
const RENKO_STALE_FAIL_MS: i64 = 600_000;
const RENKO_STALE_WARN_MS: i64 = 300_000;
const RENKO_PRICE_DRIFT_BPS: f64 = 50.0;

#[derive(Parser, Debug)]
#[command(about = "Audit live idx/s10/renko tip latency vs SLA (real-time generation).")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    pair: Option<String>,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    report: Option<PathBuf>,
    #[arg(long, default_value = "/data/live-audit/last.json")]
    default_report: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Level {
    Ok,
    Warn,
    Fail,
}

#[derive(Debug, Serialize)]
struct PairAudit {
    pair: String,
    ticker_id: u64,
    audited_at_utc: String,
    idx_tip_ms: Option<i64>,
    idx_lag_ms: Option<i64>,
    idx_mid: Option<f64>,
    s10_close_ms: Option<i64>,
    s10_lag_ms: Option<i64>,
    s10_spread_bps: Option<f32>,
    renko_close_ms: Option<i64>,
    renko_lag_ms: Option<i64>,
    renko_close: Option<f64>,
    renko_spread_bps: Option<f32>,
    renko_tick_count: Option<u32>,
    idx_to_s10_ms: Option<i64>,
    mid_to_renko_close_bps: Option<f64>,
    status: Level,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Report {
    audited_at_utc: String,
    pairs: Vec<PairAudit>,
    worst: Level,
}

fn level_max(a: Level, b: Level) -> Level {
    match (a, b) {
        (Level::Fail, _) | (_, Level::Fail) => Level::Fail,
        (Level::Warn, _) | (_, Level::Warn) => Level::Warn,
        _ => Level::Ok,
    }
}

fn idx_tip(data_root: &Path, ticker_id: u64, today: NaiveDate) -> Result<Option<(i64, f64)>> {
    let path = shard_path(&idx_dir(data_root, ticker_id), today, "idx");
    if !path.exists() {
        return Ok(None);
    }
    let recs = read_shard_aligned::<IndexRecord>(&path)?;
    let Some(rec) = recs.last() else {
        return Ok(None);
    };
    if rec.index.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL != 0 {
        return Ok(None);
    }
    let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
    let mid = (rec.index.bid + rec.index.ask) * 0.5;
    if mid.is_finite() && mid > 0.0 {
        Ok(Some((ts, mid)))
    } else {
        Ok(None)
    }
}

fn bar_tip(data_root: &Path, ticker_id: u64, kind: &str, today: NaiveDate) -> Result<Option<Bar>> {
    let path = shard_path(&bars_dir(data_root, ticker_id), today, kind);
    if !path.exists() {
        return Ok(None);
    }
    let recs = read_shard_aligned::<Bar>(&path)?;
    Ok(recs.last().copied())
}

fn audit_pair(data_root: &Path, base: &str, quote: &str, now_ms: i64) -> Result<PairAudit> {
    let pair = format!("{}/{}", base, quote);
    let ticker_id = resolve_ticker_id(&pair);
    let today = Utc::now().date_naive();
    let mut notes = Vec::new();
    let mut status = Level::Ok;

    let idx = idx_tip(data_root, ticker_id, today)?;
    let s10 = bar_tip(data_root, ticker_id, "s10", today)?;
    let renko = bar_tip(data_root, ticker_id, "renko", today)?;

    let (idx_tip_ms, idx_mid) = idx.unzip();
    let idx_lag_ms = idx_tip_ms.map(|t| now_ms.saturating_sub(t));

    let s10_close_ms = s10.as_ref().map(|b| b.close_time_ms());
    let s10_lag_ms = s10_close_ms.map(|t| now_ms.saturating_sub(t));
    let s10_spread_bps = s10.as_ref().map(|b| b.avg_spread_bps);

    let renko_close_ms = renko.as_ref().map(|b| b.close_time_ms());
    let renko_lag_ms = renko_close_ms.map(|t| now_ms.saturating_sub(t));
    let renko_close = renko.as_ref().map(|b| b.close);
    let renko_spread_bps = renko.as_ref().map(|b| b.avg_spread_bps);
    let renko_tick_count = renko.as_ref().map(|b| b.tick_count);

    let idx_to_s10_ms = match (idx_tip_ms, s10_close_ms) {
        (Some(i), Some(s)) => Some(i.saturating_sub(s)),
        _ => None,
    };

    let mid_to_renko_close_bps = match (idx_mid, renko_close) {
        (Some(m), Some(r)) if m > 0.0 && r > 0.0 => Some(((m - r).abs() / m) * 10_000.0),
        _ => None,
    };

    if idx_lag_ms.is_none() {
        notes.push("no idx tip today".into());
        status = level_max(status, Level::Fail);
    } else if let Some(lag) = idx_lag_ms {
        if lag > IDX_FAIL_MS {
            notes.push(format!("idx lag {lag}ms > fail {IDX_FAIL_MS}ms"));
            status = level_max(status, Level::Fail);
        } else if lag > IDX_WARN_MS {
            notes.push(format!("idx lag {lag}ms > warn {IDX_WARN_MS}ms"));
            status = level_max(status, Level::Warn);
        }
    }

    if s10_lag_ms.is_none() {
        notes.push("no s10 tip today".into());
        status = level_max(status, Level::Fail);
    } else if let Some(lag) = s10_lag_ms {
        if lag > S10_FAIL_MS {
            notes.push(format!("s10 lag {lag}ms > fail {S10_FAIL_MS}ms"));
            status = level_max(status, Level::Fail);
        } else if lag > S10_WARN_MS {
            notes.push(format!("s10 lag {lag}ms > warn {S10_WARN_MS}ms"));
            status = level_max(status, Level::Warn);
        }
    }

    if let Some(spread) = s10_spread_bps {
        if !spread.is_finite() || spread <= 0.0 {
            notes.push(format!("s10 avg_spread_bps invalid ({spread})"));
            status = level_max(status, Level::Fail);
        }
    }

    if renko_lag_ms.is_none() {
        notes.push("no renko tip today".into());
        status = level_max(status, Level::Fail);
    } else if let Some(lag) = renko_lag_ms {
        if lag > RENKO_STALE_FAIL_MS {
            notes.push(format!("renko tip stale {lag}ms > fail {RENKO_STALE_FAIL_MS}ms"));
            status = level_max(status, Level::Fail);
        } else if lag > RENKO_STALE_WARN_MS {
            notes.push(format!("renko tip stale {lag}ms > warn {RENKO_STALE_WARN_MS}ms"));
            status = level_max(status, Level::Warn);
        }
    }

    if let (Some(drift), Some(_)) = (mid_to_renko_close_bps, idx_lag_ms.filter(|&l| l < IDX_WARN_MS))
    {
        if drift > RENKO_PRICE_DRIFT_BPS {
            notes.push(format!(
                "mid vs last renko close {drift:.1}bps > {RENKO_PRICE_DRIFT_BPS}bps (awaiting brick cross)"
            ));
            if status == Level::Ok {
                status = Level::Warn;
            }
        }
    }

    if let Some(lag) = idx_to_s10_ms {
        if lag > 20_000 {
            notes.push(format!("idx ahead of s10 by {lag}ms (>20s)"));
            status = level_max(status, Level::Fail);
        } else if lag > 12_000 {
            notes.push(format!("idx ahead of s10 by {lag}ms (>12s)"));
            status = level_max(status, Level::Warn);
        }
    }

    Ok(PairAudit {
        pair,
        ticker_id,
        audited_at_utc: Utc::now().to_rfc3339(),
        idx_tip_ms: idx_tip_ms,
        idx_lag_ms,
        idx_mid,
        s10_close_ms,
        s10_lag_ms,
        s10_spread_bps,
        renko_close_ms,
        renko_lag_ms,
        renko_close,
        renko_spread_bps,
        renko_tick_count,
        idx_to_s10_ms,
        mid_to_renko_close_bps,
        status,
        notes,
    })
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();
    let yml = PipelineYml::load(&args.config)?;
    let cfg = nxr_sdk::NxrConfig::from_env();
    let data_root = PathBuf::from(&cfg.bars_dir)
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data"));

    let pairs: Vec<(String, String)> = if args.all {
        yml.series
            .pipeline
            .pairs
            .iter()
            .map(|b| (b.to_uppercase(), "USDT".to_string()))
            .collect()
    } else if let Some(p) = args.pair.as_deref() {
        let (b, q) = nxr_sdk::split_pair_multi(p, &['/', '-'])
            .ok_or_else(|| anyhow::anyhow!("bad pair {p}"))?;
        vec![(b.to_uppercase(), q.to_uppercase())]
    } else {
        vec![("BTC".into(), "USDT".into())]
    };

    let now_ms = Utc::now().timestamp_millis();
    let mut out = Vec::new();
    let mut worst = Level::Ok;
    for (base, quote) in pairs {
        match audit_pair(&data_root, &base, &quote, now_ms) {
            Ok(row) => {
                worst = level_max(worst, row.status);
                if row.status != Level::Ok {
                    warn!(
                        pair = %row.pair,
                        ?row.status,
                        idx_lag_ms = ?row.idx_lag_ms,
                        s10_lag_ms = ?row.s10_lag_ms,
                        renko_lag_ms = ?row.renko_lag_ms,
                        notes = ?row.notes,
                        "live latency audit"
                    );
                } else {
                    info!(
                        pair = %row.pair,
                        idx_lag_ms = ?row.idx_lag_ms,
                        s10_lag_ms = ?row.s10_lag_ms,
                        renko_lag_ms = ?row.renko_lag_ms,
                        "live latency ok"
                    );
                }
                out.push(row);
            }
            Err(e) => {
                worst = Level::Fail;
                warn!(base, quote, err = %e, "audit error");
                out.push(PairAudit {
                    pair: format!("{base}/{quote}"),
                    ticker_id: resolve_ticker_id(&format!("{base}/{quote}")),
                    audited_at_utc: Utc::now().to_rfc3339(),
                    idx_tip_ms: None,
                    idx_lag_ms: None,
                    idx_mid: None,
                    s10_close_ms: None,
                    s10_lag_ms: None,
                    s10_spread_bps: None,
                    renko_close_ms: None,
                    renko_lag_ms: None,
                    renko_close: None,
                    renko_spread_bps: None,
                    renko_tick_count: None,
                    idx_to_s10_ms: None,
                    mid_to_renko_close_bps: None,
                    status: Level::Fail,
                    notes: vec![format!("{e:#}")],
                });
            }
        }
    }

    let report = Report {
        audited_at_utc: Utc::now().to_rfc3339(),
        pairs: out,
        worst,
    };

    let json = serde_json::to_string_pretty(&report)?;
    println!("{json}");

    let report_path = args.report.unwrap_or(args.default_report);
    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&report_path, &json).with_context(|| format!("write {}", report_path.display()))?;

    match worst {
        Level::Ok => Ok(()),
        Level::Warn => std::process::exit(1),
        Level::Fail => std::process::exit(2),
    }
}
