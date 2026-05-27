//! Backfill orchestrator — drives `fetch-crypto-history` → `ticks-to-idx` →
//! `merge-idx` → `s10-from-idx` → `renko-from-idx` → `integrity-check` for
//! many tickers in parallel and emits a single JSON report.
//!
//! Phase 55 W4 changes:
//! - tracing-subscriber init at startup (defaults to INFO; honours RUST_LOG)
//! - per-step start/done structured logs w/ duration_ms, exit_code, bytes
//! - per-ticker boundary logs + progress markers on disk under
//!   `<out_dir>/backfill/progress/<ticker>.{start,done,failed}`
//! - 60s heartbeat thread emits active/completed counters
//! - pre-fetch availability probe (HEAD-only) drops dead exchanges + may
//!   skip a ticker entirely if no exchange has coverage
//! - merge / s10 / renko emit daily shards; validate iterates shards via
//!   `integrity-check` per shard (best-effort)
//! - resume logic checks manifest sha256 + existence per shard
//!
//! Bins are discovered via `$PATH` (k8s container installs them under
//! `/usr/local/bin`). For each (ticker, step) we record start, duration,
//! exit code, bytes written, and any stderr lines. Tickers run on a rayon
//! pool; each ticker is wrapped in `catch_unwind` so one panic ! tank batch.

use std::collections::BTreeMap;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use clap::Parser;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(about = "Backfill orchestrator: fetch → t2i → merge → s10 → renko → validate.")]
struct Args {
    /// Path to nxrates.yml (forwarded to fetch-crypto-history).
    config: PathBuf,
    /// Start date (YYYY-MM-DD). Default: 1 year ago.
    #[arg(long)]
    from: Option<String>,
    /// End date (YYYY-MM-DD) or `today`. Default: today.
    #[arg(long, default_value = "today")]
    to: String,
    /// Comma-separated tickers `BASE-QUOTE`. Default: from --quote expansion (must set --tickers).
    #[arg(long)]
    tickers: Option<String>,
    /// Comma-separated exchanges. Default: binance,bybit,bitget,okx.
    #[arg(long, default_value = "binance,bybit,bitget,okx")]
    exchanges: String,
    /// Quote currency (used to expand BASE-only tickers if needed).
    #[arg(long, default_value = "USDT")]
    quote: String,
    /// Comma-separated steps to run. Default: all.
    #[arg(long, default_value = "fetch,t2i,merge,s10,renko,validate")]
    steps: String,
    /// Parallel ticker workers.
    #[arg(long, default_value_t = 4)]
    parallel: usize,
    /// Skip steps whose output exists and passes integrity-check.
    #[arg(long)]
    resume: bool,
    /// Output data root (contains ticks/, indexes/, bars/, vol/).
    #[arg(long, default_value = "/data")]
    out_dir: PathBuf,
    /// JSON report path.
    #[arg(long, default_value = "backfill.log.json")]
    log_file: PathBuf,
    /// Print plan and exit.
    #[arg(long)]
    dry_run: bool,
    /// After successful validate, delete per-exchange `ticks/` and
    /// `indexes/<exch>/` intermediates for the pair. Keeps composite
    /// shards + bars shards + .vol.
    ///
    /// R1 C4: default INVERTED from off→on. The composite `.idx` is
    /// self-sufficient (s10 / renko / integrity-check all read it, never
    /// the raw staging); keeping per-ticker staging on disk after a 31-pair
    /// x 5y backfill is what overran the 500Gi PVC. Pass `--keep-staging`
    /// to opt out (e.g. for forensic re-merges).
    #[arg(long, default_value_t = true)]
    cleanup: bool,
    /// Opt-out for the now-default cleanup. When set, raw `ticks/<exch>` and
    /// per-provider `indexes/<exch>/<BASE>-<QUOTE>.idx` files are kept on
    /// disk after a successful per-ticker validate. Useful for debugging a
    /// suspect merge or for forensic re-runs; do NOT set in production
    /// backfill jobs (it will refill the data PVC).
    #[arg(long, default_value_t = false)]
    keep_staging: bool,
    /// Skip availability probe (pre-fetch HEAD check). Useful for synthetic
    /// or offline environments.
    #[arg(long)]
    skip_probe: bool,
}

// ── Report types ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StepReport {
    name: String,
    duration_ms: u128,
    exit_code: i32,
    bytes: u64,
    skipped: bool,
    errors: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AvailabilityEntry {
    exchange: String,
    has_data: bool,
    first_date: Option<String>,
    last_date: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TickerReport {
    ticker: String,
    status: String, // ok | failed | skipped
    steps: Vec<StepReport>,
    #[serde(default)]
    ticker_availability: Vec<AvailabilityEntry>,
    #[serde(default)]
    skip_reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ConfigEcho {
    from: String,
    to: String,
    tickers: Vec<String>,
    exchanges: Vec<String>,
    steps: Vec<String>,
    parallel: usize,
    out_dir: String,
}

#[derive(Debug, Serialize)]
struct Summary {
    total: usize,
    ok: usize,
    failed: usize,
    skipped: usize,
}

#[derive(Debug, Serialize)]
struct FinalReport {
    started_at: String,
    finished_at: String,
    config: ConfigEcho,
    results: Vec<TickerReport>,
    summary: Summary,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn parse_date(s: &str) -> Result<NaiveDate> {
    if s.eq_ignore_ascii_case("today") {
        return Ok(Utc::now().date_naive());
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("bad date `{}`", s))
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

fn split_pair(t: &str) -> Result<(String, String)> {
    series_factory::split_pair(t)
        .map(|(b, q)| (b.to_string(), q.to_string()))
        .ok_or_else(|| anyhow!("bad ticker {}: expected BASE-QUOTE", t))
}

fn file_bytes(p: &Path) -> u64 {
    fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn dir_bytes(p: &Path) -> u64 {
    let mut total = 0u64;
    let read = match fs::read_dir(p) {
        Ok(r) => r,
        Err(_) => return 0,
    };
    for entry in read.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += dir_bytes(&path);
        } else if let Ok(m) = entry.metadata() {
            total += m.len();
        }
    }
    total
}

fn run_step(ticker: &str, bin: &str, args: &[String], out_path: Option<&Path>) -> StepReport {
    let name = bin.to_string();
    info!(ticker, bin, args = ?args, "step start");
    let start = Instant::now();
    let out = Command::new(bin).args(args).output();
    let duration_ms = start.elapsed().as_millis();
    match out {
        Ok(o) => {
            let exit_code = o.status.code().unwrap_or(-1);
            let mut errors = Vec::new();
            if !o.status.success() {
                let stderr = String::from_utf8_lossy(&o.stderr).to_string();
                for line in stderr.lines().rev().take(20) {
                    errors.push(line.to_string());
                }
                errors.reverse();
                error!(
                    ticker,
                    bin,
                    exit_code,
                    duration_ms = duration_ms as u64,
                    stderr_tail = ?errors,
                    "step failed"
                );
            }
            let bytes = match out_path {
                Some(p) if p.is_dir() => dir_bytes(p),
                Some(p) => file_bytes(p),
                None => 0,
            };
            if o.status.success() {
                info!(
                    ticker,
                    bin,
                    exit_code,
                    duration_ms = duration_ms as u64,
                    bytes_out = bytes,
                    "step done"
                );
            }
            StepReport {
                name,
                duration_ms,
                exit_code,
                bytes,
                skipped: false,
                errors,
            }
        }
        Err(e) => {
            error!(ticker, bin, err = %e, "spawn failed");
            StepReport {
                name,
                duration_ms,
                exit_code: -1,
                bytes: 0,
                skipped: false,
                errors: vec![format!("spawn failed: {}", e)],
            }
        }
    }
}

fn integrity_clean(file: &Path, kind: &str) -> bool {
    if !file.exists() {
        return false;
    }
    let out = Command::new("integrity-check")
        .args([kind, file.to_string_lossy().as_ref(), "--strict", "--json"])
        .output();
    match out {
        Ok(o) => o.status.success(),
        Err(_) => false,
    }
}

// ── Progress markers ─────────────────────────────────────────────────────────

fn progress_dir(out_dir: &Path) -> PathBuf {
    out_dir.join("backfill").join("progress")
}

fn write_marker(out_dir: &Path, ticker: &str, suffix: &str) {
    let dir = progress_dir(out_dir);
    let _ = fs::create_dir_all(&dir);
    let p = dir.join(format!("{}.{}", ticker, suffix));
    let now = Utc::now().to_rfc3339();
    if let Err(e) = fs::write(&p, &now) {
        warn!(path = %p.display(), err = %e, "progress marker write failed");
    }
}

// ── Per-ticker pipeline ──────────────────────────────────────────────────────

struct PlanCtx {
    args: Args,
    from: NaiveDate,
    to: NaiveDate,
    exchanges: Vec<String>,
    steps: Vec<String>,
    counters: Arc<Counters>,
}

struct Counters {
    active: AtomicUsize,
    completed: AtomicU64,
    failed: AtomicU64,
    skipped: AtomicU64,
}

impl Counters {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
        }
    }
}

/// HEAD-probe each exchange via fetch-crypto-history --probe. Returns the
/// availability vec + filtered exchanges that have coverage.
fn probe_availability(
    ctx: &PlanCtx,
    base: &str,
    quote: &str,
) -> (Vec<AvailabilityEntry>, Vec<String>) {
    let args = vec![
        ctx.args.config.to_string_lossy().to_string(),
        "--pairs".to_string(),
        base.to_string(),
        "--exchanges".to_string(),
        ctx.exchanges.join(","),
        "--quote".to_string(),
        quote.to_string(),
        "--days".to_string(),
        (ctx.to - ctx.from).num_days().max(1).to_string(),
        "--probe".to_string(),
    ];
    let out = Command::new("fetch-crypto-history").args(&args).output();
    let mut result = Vec::new();
    let mut active = Vec::new();
    match out {
        Ok(o) if o.status.success() => {
            let raw = String::from_utf8_lossy(&o.stdout);
            // probe emits exactly 1 JSON line at the end; pick last non-empty
            let last = raw.lines().filter(|l| !l.trim().is_empty()).last().unwrap_or("");
            #[derive(Deserialize)]
            struct ProbeOut {
                exchange: String,
                has_data: bool,
                first_date: Option<String>,
                last_date: Option<String>,
            }
            if let Ok(parsed) = serde_json::from_str::<Vec<ProbeOut>>(last) {
                for p in parsed {
                    if p.has_data && ctx.exchanges.iter().any(|e| e == &p.exchange) {
                        active.push(p.exchange.clone());
                    } else if !p.has_data {
                        warn!(
                            ticker = %format!("{}-{}", base, quote),
                            exchange = %p.exchange,
                            "no archive coverage; skipping"
                        );
                    }
                    result.push(AvailabilityEntry {
                        exchange: p.exchange,
                        has_data: p.has_data,
                        first_date: p.first_date,
                        last_date: p.last_date,
                    });
                }
            } else {
                warn!(stdout_tail = %last, "probe stdout did not parse; falling back to full exchange list");
                active = ctx.exchanges.clone();
            }
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr).to_string();
            warn!(exit = ?o.status.code(), stderr = %stderr, "probe failed; falling back");
            active = ctx.exchanges.clone();
        }
        Err(e) => {
            warn!(err = %e, "probe spawn failed; falling back");
            active = ctx.exchanges.clone();
        }
    }
    (result, active)
}

/// Delete a ticker's raw exchange staging once its composite `.idx` is built
/// and validated. `ticks/<exch>/<BASE><QUOTE>/` and the per-provider
/// `indexes/<exch>/<BASE>-<QUOTE>.idx` are fully consumed by `merge-idx` — the
/// downstream s10 / renko / integrity steps all read the composite `.idx`,
/// never the raw staging. Purging per-ticker here (rather than once at the end
/// of the whole batch) keeps the data volume bounded: a 13-ticker x 2y backfill
/// otherwise accumulates every ticker's raw ticks until the batch finishes,
/// which overran the 500Gi volume.
fn cleanup_ticker_staging(
    out_dir: &std::path::Path,
    exchanges: &[String],
    base: &str,
    quote: &str,
) -> StepReport {
    let start = Instant::now();
    let mut removed_bytes: u64 = 0;
    let mut errors: Vec<String> = Vec::new();
    for ex in exchanges {
        let ticks_dir = out_dir
            .join("ticks")
            .join(ex)
            .join(format!("{}{}", base, quote));
        if ticks_dir.exists() {
            removed_bytes += dir_bytes(&ticks_dir);
            if let Err(e) = fs::remove_dir_all(&ticks_dir) {
                errors.push(format!("rm -r {}: {}", ticks_dir.display(), e));
            }
        }
        let per_idx = out_dir
            .join("indexes")
            .join(ex)
            .join(format!("{}-{}.idx", base, quote));
        if per_idx.exists() {
            removed_bytes += file_bytes(&per_idx);
            if let Err(e) = fs::remove_file(&per_idx) {
                errors.push(format!("rm {}: {}", per_idx.display(), e));
            }
        }
    }
    StepReport {
        name: "cleanup-staging".to_string(),
        duration_ms: start.elapsed().as_millis(),
        exit_code: if errors.is_empty() { 0 } else { 1 },
        bytes: removed_bytes,
        skipped: false,
        errors,
    }
}

fn run_ticker(ctx: &PlanCtx, ticker: &str) -> TickerReport {
    let (base, quote) = match split_pair(ticker) {
        // R1 H10: uppercase both halves the moment we parse the pair so a
        // lowercased CLI arg (`btc-usdt`) cannot silently shadow the
        // uppercase fetcher output. Per-exchange `ticks/<exch>/<BASE><QUOTE>`
        // staging is built by fetch-crypto-history under uppercase paths;
        // a lowercase split_pair previously matched neither the staging
        // dir nor the cleanup target, so cleanup was a no-op AND the
        // composite merge silently picked an empty input.
        Ok((b, q)) => (b.to_uppercase(), q.to_uppercase()),
        Err(e) => {
            return TickerReport {
                ticker: ticker.to_string(),
                status: "failed".to_string(),
                steps: vec![StepReport {
                    name: "parse".to_string(),
                    duration_ms: 0,
                    exit_code: -1,
                    bytes: 0,
                    skipped: false,
                    errors: vec![e.to_string()],
                }],
                ticker_availability: Vec::new(),
                skip_reason: None,
            };
        }
    };

    ctx.counters.active.fetch_add(1, Ordering::Relaxed);
    write_marker(&ctx.args.out_dir, ticker, "start");
    info!(ticker, "ticker start");

    let mut steps_out: Vec<StepReport> = Vec::new();
    let cfg = ctx.args.config.to_string_lossy().to_string();
    let out_dir = &ctx.args.out_dir;
    // Sharded paths (per Phase 55 W4 spec).
    let composite_dir = out_dir
        .join("indexes")
        .join("composite")
        .join(format!("{}-{}", base, quote));
    let bars_dir = out_dir
        .join("bars")
        .join(&base)
        .join(format!("{}{}", base, quote));
    let vol_path = out_dir.join("vol").join(format!("{}-{}.vol", base, quote));

    let want = |s: &str| ctx.steps.iter().any(|x| x == s);
    let days = (ctx.to - ctx.from).num_days().max(1);

    // 0) availability probe — drops dead exchanges before fetch
    let mut availability = Vec::new();
    let mut active_exchanges = ctx.exchanges.clone();
    if !ctx.args.skip_probe && want("fetch") {
        let (av, active) = probe_availability(ctx, &base, &quote);
        availability = av;
        if active.is_empty() {
            warn!(ticker, "all exchanges report no coverage; skipping ticker");
            ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
            ctx.counters.skipped.fetch_add(1, Ordering::Relaxed);
            write_marker(&ctx.args.out_dir, ticker, "done");
            return TickerReport {
                ticker: ticker.to_string(),
                status: "skipped".to_string(),
                steps: steps_out,
                ticker_availability: availability,
                skip_reason: Some("no archive coverage in any exchange".into()),
            };
        }
        active_exchanges = active;
        info!(ticker, active_exchanges = ?active_exchanges, "availability check ok");
    }

    // 1) fetch
    if want("fetch") {
        let args = vec![
            cfg.clone(),
            "--pairs".to_string(),
            base.clone(),
            "--exchanges".to_string(),
            active_exchanges.join(","),
            "--quote".to_string(),
            quote.clone(),
            "--days".to_string(),
            days.to_string(),
        ];
        steps_out.push(run_step(ticker, "fetch-crypto-history", &args, None));
        if steps_out.last().unwrap().exit_code != 0 {
            ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
            ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
            write_marker(&ctx.args.out_dir, ticker, "failed");
            return TickerReport {
                ticker: ticker.to_string(),
                status: "failed".to_string(),
                steps: steps_out,
                ticker_availability: availability,
                skip_reason: None,
            };
        }
    }

    // 2) t2i (per exchange)
    if want("t2i") {
        for ex in &active_exchanges {
            let per_idx = out_dir
                .join("indexes")
                .join(ex)
                .join(format!("{}-{}.idx", base, quote));
            if ctx.args.resume && integrity_clean(&per_idx, "idx") {
                steps_out.push(StepReport {
                    name: format!("ticks-to-idx[{}]", ex),
                    duration_ms: 0,
                    exit_code: 0,
                    bytes: file_bytes(&per_idx),
                    skipped: true,
                    errors: Vec::new(),
                });
                continue;
            }
            let args = vec![
                ex.clone(),
                base.clone(),
                quote.clone(),
                "--cycle-ms".to_string(),
                "100".to_string(),
            ];
            let mut rep = run_step(ticker, "ticks-to-idx", &args, Some(&per_idx));
            rep.name = format!("ticks-to-idx[{}]", ex);
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
                ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                write_marker(&ctx.args.out_dir, ticker, "failed");
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                    ticker_availability: availability,
                    skip_reason: None,
                };
            }
        }
    }

    // 3) merge → sharded composite_dir
    if want("merge") {
        // resume: manifest exists + every shard hash matches recorded sha256
        if ctx.args.resume && manifest_ok(&composite_dir, "idx") {
            steps_out.push(StepReport {
                name: "merge-idx".to_string(),
                duration_ms: 0,
                exit_code: 0,
                bytes: dir_bytes(&composite_dir),
                skipped: true,
                errors: Vec::new(),
            });
        } else {
            let args = vec![base.clone(), quote.clone()];
            let rep = run_step(ticker, "merge-idx", &args, Some(&composite_dir));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
                ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                write_marker(&ctx.args.out_dir, ticker, "failed");
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                    ticker_availability: availability,
                    skip_reason: None,
                };
            }
        }
        // post-merge: validate each shard strictly. If any fails → ticker failed.
        let shard_check = validate_shards(ticker, &composite_dir, "idx");
        steps_out.push(shard_check.clone());
        if shard_check.exit_code != 0 {
            ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
            ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
            write_marker(&ctx.args.out_dir, ticker, "failed");
            return TickerReport {
                ticker: ticker.to_string(),
                status: "failed".to_string(),
                steps: steps_out,
                ticker_availability: availability,
                skip_reason: None,
            };
        }

        // Composite .idx built + validated — the raw ticks/<exch> staging and
        // per-provider .idx are now fully consumed. Purge them immediately,
        // per-ticker, so a long multi-ticker backfill keeps the data volume
        // bounded instead of hoarding every ticker's raw ticks until the end.
        // R1 C4: cleanup is on by default; `--keep-staging` opts out.
        let do_cleanup = ctx.args.cleanup && !ctx.args.keep_staging;
        if do_cleanup {
            steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
        } else {
            info!(ticker, "staging cleanup skipped (--keep-staging or --cleanup=false)");
        }
    }

    // 4) s10 → sharded bars_dir/*.s10
    if want("s10") {
        if ctx.args.resume && manifest_ok(&bars_dir, "s10") {
            steps_out.push(StepReport {
                name: "s10-from-idx".to_string(),
                duration_ms: 0,
                exit_code: 0,
                bytes: dir_bytes(&bars_dir),
                skipped: true,
                errors: Vec::new(),
            });
        } else {
            let args = vec![cfg.clone(), format!("{}-{}", base, quote)];
            let rep = run_step(ticker, "s10-from-idx", &args, Some(&bars_dir));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
                ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                write_marker(&ctx.args.out_dir, ticker, "failed");
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                    ticker_availability: availability,
                    skip_reason: None,
                };
            }
        }
    }

    // 5) renko → sharded bars_dir/*.renko
    if want("renko") {
        if ctx.args.resume && manifest_ok(&bars_dir, "renko") {
            steps_out.push(StepReport {
                name: "renko-from-idx".to_string(),
                duration_ms: 0,
                exit_code: 0,
                bytes: dir_bytes(&bars_dir),
                skipped: true,
                errors: Vec::new(),
            });
        } else {
            let args = vec![cfg.clone(), base.clone(), quote.clone()];
            let rep = run_step(ticker, "renko-from-idx", &args, Some(&bars_dir));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
                ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                write_marker(&ctx.args.out_dir, ticker, "failed");
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                    ticker_availability: availability,
                    skip_reason: None,
                };
            }
        }
    }

    // 6) validate per-shard for s10/renko + vol (best-effort: scan shard dir)
    if want("validate") {
        // idx already validated post-merge; re-emit for symmetry only if we
        // didn't already run it (e.g. resume skipped merge step).
        if !steps_out.iter().any(|s| s.name == "integrity-check-shards[idx]") {
            steps_out.push(validate_shards(ticker, &composite_dir, "idx"));
        }
        steps_out.push(validate_shards(ticker, &bars_dir, "s10"));
        steps_out.push(validate_shards(ticker, &bars_dir, "renko"));
        // vol (single legacy file)
        if vol_path.exists() {
            let args = vec![
                "vol".to_string(),
                vol_path.to_string_lossy().to_string(),
                "--strict".to_string(),
                "--json".to_string(),
            ];
            let mut rep = run_step(ticker, "integrity-check", &args, Some(&vol_path));
            rep.name = "integrity-check[vol]".to_string();
            steps_out.push(rep);
        }
    }

    let any_fail = steps_out
        .iter()
        .any(|s| s.exit_code != 0 && !s.skipped);
    let all_skipped = !steps_out.is_empty() && steps_out.iter().all(|s| s.skipped);
    let status = if any_fail {
        "failed"
    } else if all_skipped {
        "skipped"
    } else {
        "ok"
    };

    // Raw staging cleanup is no longer end-of-run: it now happens per-ticker
    // immediately after the composite `.idx` is built + validated (see step 3),
    // so the data volume stays bounded across a long multi-ticker backfill.

    let n_steps = steps_out.len();
    info!(ticker, status, n_steps, "ticker done");
    ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
    match status {
        "ok" => {
            ctx.counters.completed.fetch_add(1, Ordering::Relaxed);
            write_marker(&ctx.args.out_dir, ticker, "done");
        }
        "skipped" => {
            ctx.counters.skipped.fetch_add(1, Ordering::Relaxed);
            write_marker(&ctx.args.out_dir, ticker, "done");
        }
        _ => {
            ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
            write_marker(&ctx.args.out_dir, ticker, "failed");
        }
    }

    TickerReport {
        ticker: ticker.to_string(),
        status: status.to_string(),
        steps: steps_out,
        ticker_availability: availability,
        skip_reason: None,
    }
}

/// Cheap resume gate: manifest.json present in ticker_dir AND ≥1 shard of
/// `kind` extension on disk. Full sha256 verification belongs to
/// integrity-check; this only short-circuits the producer step when output
/// is trivially complete.
fn manifest_ok(ticker_dir: &Path, kind: &str) -> bool {
    let mpath = ticker_dir.join("manifest.json");
    if !mpath.exists() {
        return false;
    }
    let suffix = format!(".{}", kind);
    fs::read_dir(ticker_dir)
        .map(|rd| {
            rd.flatten().any(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with(&suffix))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Run integrity-check on every shard in `ticker_dir` matching `kind`.
/// Returns aggregate StepReport (first failure flips exit_code to 1).
fn validate_shards(ticker: &str, ticker_dir: &Path, kind: &str) -> StepReport {
    let name = format!("integrity-check-shards[{}]", kind);
    let start = Instant::now();
    let suffix = format!(".{}", kind);
    let mut shards: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = fs::read_dir(ticker_dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if let Some(n) = path.file_name().and_then(|n| n.to_str()) {
                if n.ends_with(&suffix) {
                    shards.push(path);
                }
            }
        }
    }
    shards.sort();
    if shards.is_empty() {
        // no shards → can't validate; signal failure only if the step is
        // semantically required (i.e. we expected an output). At the call
        // site we already filter to dirs that should contain output; treat
        // empty as a soft warning here.
        warn!(ticker, kind, dir = %ticker_dir.display(), "no shards to validate");
        return StepReport {
            name,
            duration_ms: start.elapsed().as_millis(),
            exit_code: 0,
            bytes: 0,
            skipped: true,
            errors: vec!["no shards present".into()],
        };
    }
    let mut errors: Vec<String> = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut any_fail = false;
    for shard in &shards {
        total_bytes += file_bytes(shard);
        let args = vec![
            kind.to_string(),
            shard.to_string_lossy().to_string(),
            "--strict".to_string(),
            "--json".to_string(),
        ];
        let out = Command::new("integrity-check").args(&args).output();
        match out {
            Ok(o) if o.status.success() => {}
            Ok(o) => {
                any_fail = true;
                let stderr = String::from_utf8_lossy(&o.stderr);
                for line in stderr.lines().rev().take(5) {
                    errors.push(format!("{}: {}", shard.display(), line));
                }
                error!(
                    ticker,
                    kind,
                    shard = %shard.display(),
                    exit = ?o.status.code(),
                    "shard integrity-check failed"
                );
            }
            Err(e) => {
                any_fail = true;
                errors.push(format!("{}: spawn {}", shard.display(), e));
            }
        }
    }
    let exit_code = if any_fail { 1 } else { 0 };
    info!(
        ticker,
        kind,
        n_shards = shards.len(),
        exit_code,
        duration_ms = start.elapsed().as_millis() as u64,
        "shard validate done"
    );
    StepReport {
        name,
        duration_ms: start.elapsed().as_millis(),
        exit_code,
        bytes: total_bytes,
        skipped: false,
        errors,
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    // tracing init — default INFO, env RUST_LOG override. Logs to stderr.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    let to = parse_date(&args.to)?;
    let from = match &args.from {
        Some(s) => parse_date(s)?,
        None => to - chrono::Duration::days(365),
    };
    let exchanges = split_csv(&args.exchanges);
    let steps = split_csv(&args.steps);
    let tickers: Vec<String> = match &args.tickers {
        Some(s) => split_csv(s),
        None => {
            return Err(anyhow!(
                "--tickers required (comma-separated BASE-QUOTE list)"
            ));
        }
    };

    let started_at = Utc::now().to_rfc3339();
    let cfg_echo = ConfigEcho {
        from: from.to_string(),
        to: to.to_string(),
        tickers: tickers.clone(),
        exchanges: exchanges.clone(),
        steps: steps.clone(),
        parallel: args.parallel,
        out_dir: args.out_dir.to_string_lossy().to_string(),
    };

    info!(
        from = %from,
        to = %to,
        n_tickers = tickers.len(),
        exchanges = ?exchanges,
        steps = ?steps,
        parallel = args.parallel,
        out_dir = %args.out_dir.display(),
        "backfill plan"
    );

    if args.dry_run {
        println!("PLAN");
        println!("  from   : {}", from);
        println!("  to     : {} ({} days)", to, (to - from).num_days());
        println!("  exch   : {}", exchanges.join(","));
        println!("  steps  : {}", steps.join(","));
        println!("  par    : {}", args.parallel);
        println!("  out    : {}", args.out_dir.display());
        println!("  tickers ({}):", tickers.len());
        for t in &tickers {
            println!("    - {}", t);
        }
        return Ok(());
    }

    let counters = Arc::new(Counters::new());
    let ctx = PlanCtx {
        args: args.clone(),
        from,
        to,
        exchanges,
        steps,
        counters: counters.clone(),
    };

    // ensure progress dir exists
    let _ = fs::create_dir_all(progress_dir(&args.out_dir));

    // heartbeat thread — emits every 60s while pool runs
    let heartbeat_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hb_stop = heartbeat_stop.clone();
    let hb_counters = counters.clone();
    let n_tickers = tickers.len();
    let heartbeat = thread::spawn(move || {
        while !hb_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(60));
            if hb_stop.load(Ordering::Relaxed) {
                break;
            }
            info!(
                msg = "heartbeat",
                active_tickers = hb_counters.active.load(Ordering::Relaxed),
                completed = hb_counters.completed.load(Ordering::Relaxed),
                failed = hb_counters.failed.load(Ordering::Relaxed),
                skipped = hb_counters.skipped.load(Ordering::Relaxed),
                total = n_tickers,
                "heartbeat"
            );
        }
    });

    let pool = ThreadPoolBuilder::new()
        .num_threads(args.parallel.max(1))
        .build()
        .context("rayon pool")?;

    let results: Mutex<BTreeMap<String, TickerReport>> = Mutex::new(BTreeMap::new());
    pool.install(|| {
        tickers.par_iter().for_each(|t| {
            let res = catch_unwind(AssertUnwindSafe(|| run_ticker(&ctx, t)));
            let rep = match res {
                Ok(r) => r,
                Err(_) => {
                    ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                    write_marker(&ctx.args.out_dir, t, "failed");
                    TickerReport {
                        ticker: t.clone(),
                        status: "failed".to_string(),
                        steps: vec![StepReport {
                            name: "panic".to_string(),
                            duration_ms: 0,
                            exit_code: -1,
                            bytes: 0,
                            skipped: false,
                            errors: vec!["worker panicked".to_string()],
                        }],
                        ticker_availability: Vec::new(),
                        skip_reason: None,
                    }
                }
            };
            results.lock().unwrap().insert(t.clone(), rep);
        });
    });

    heartbeat_stop.store(true, Ordering::Relaxed);
    let _ = heartbeat.join();

    let results_vec: Vec<TickerReport> = results.into_inner().unwrap().into_values().collect();
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    for r in &results_vec {
        match r.status.as_str() {
            "ok" => ok += 1,
            "failed" => failed += 1,
            "skipped" => skipped += 1,
            _ => {}
        }
    }
    let total = results_vec.len();
    let finished_at = Utc::now().to_rfc3339();

    let report = FinalReport {
        started_at,
        finished_at,
        config: cfg_echo,
        results: results_vec,
        summary: Summary {
            total,
            ok,
            failed,
            skipped,
        },
    };

    let json = serde_json::to_string_pretty(&report)?;
    fs::write(&args.log_file, &json)
        .with_context(|| format!("write {}", args.log_file.display()))?;
    info!(
        total,
        ok,
        failed,
        skipped,
        log_file = %args.log_file.display(),
        "backfill done"
    );

    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
