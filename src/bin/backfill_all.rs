//! Backfill orchestrator — drives `fetch-crypto-history` → `ticks-to-idx` →
//! `merge-idx` → `renko-from-idx` → `integrity-check` for many tickers in
//! parallel and emits a single JSON report.
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
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use clap::Parser;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(about = "Backfill orchestrator: fetch → t2i → merge → renko → validate.")]
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
    #[arg(long, default_value = "fetch,t2i,merge,renko,validate")]
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
struct TickerReport {
    ticker: String,
    status: String, // ok | failed | skipped
    steps: Vec<StepReport>,
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
    let mut it = t.splitn(2, '-');
    let base = it.next().ok_or_else(|| anyhow!("bad ticker {}", t))?;
    let quote = it.next().ok_or_else(|| anyhow!("ticker missing quote: {}", t))?;
    Ok((base.to_string(), quote.to_string()))
}

fn file_bytes(p: &Path) -> u64 {
    fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn run_step(
    bin: &str,
    args: &[String],
    out_file: Option<&Path>,
) -> StepReport {
    let name = bin.to_string();
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
            }
            let bytes = out_file.map(file_bytes).unwrap_or(0);
            StepReport {
                name,
                duration_ms,
                exit_code,
                bytes,
                skipped: false,
                errors,
            }
        }
        Err(e) => StepReport {
            name,
            duration_ms,
            exit_code: -1,
            bytes: 0,
            skipped: false,
            errors: vec![format!("spawn failed: {}", e)],
        },
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

// ── Per-ticker pipeline ──────────────────────────────────────────────────────

struct PlanCtx {
    args: Args,
    from: NaiveDate,
    to: NaiveDate,
    exchanges: Vec<String>,
    steps: Vec<String>,
}

fn run_ticker(ctx: &PlanCtx, ticker: &str) -> TickerReport {
    let (base, quote) = match split_pair(ticker) {
        Ok(x) => x,
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
            };
        }
    };

    let mut steps_out: Vec<StepReport> = Vec::new();
    let cfg = ctx.args.config.to_string_lossy().to_string();
    let out_dir = &ctx.args.out_dir;
    let composite_idx = out_dir
        .join("indexes")
        .join("composite")
        .join(format!("{}-{}.idx", base, quote));
    let bars_path = out_dir
        .join("bars")
        .join(&base)
        .join(format!("{}{}.bars", base, quote));
    let vol_path = out_dir.join("vol").join(format!("{}-{}.vol", base, quote));

    let want = |s: &str| ctx.steps.iter().any(|x| x == s);
    let days = (ctx.to - ctx.from).num_days().max(1);

    // 1) fetch
    if want("fetch") {
        let args = vec![
            cfg.clone(),
            "--pairs".to_string(),
            base.clone(),
            "--exchanges".to_string(),
            ctx.exchanges.join(","),
            "--quote".to_string(),
            quote.clone(),
            "--days".to_string(),
            days.to_string(),
        ];
        steps_out.push(run_step("fetch-crypto-history", &args, None));
        if steps_out.last().unwrap().exit_code != 0 {
            return TickerReport {
                ticker: ticker.to_string(),
                status: "failed".to_string(),
                steps: steps_out,
            };
        }
    }

    // 2) t2i (per exchange)
    if want("t2i") {
        for ex in &ctx.exchanges {
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
            let mut rep = run_step("ticks-to-idx", &args, Some(&per_idx));
            rep.name = format!("ticks-to-idx[{}]", ex);
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                };
            }
        }
    }

    // 3) merge
    if want("merge") {
        if ctx.args.resume && integrity_clean(&composite_idx, "idx") {
            steps_out.push(StepReport {
                name: "merge-idx".to_string(),
                duration_ms: 0,
                exit_code: 0,
                bytes: file_bytes(&composite_idx),
                skipped: true,
                errors: Vec::new(),
            });
        } else {
            let args = vec![base.clone(), quote.clone()];
            let rep = run_step("merge-idx", &args, Some(&composite_idx));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                };
            }
        }
    }

    // 4) renko
    if want("renko") {
        if ctx.args.resume && integrity_clean(&bars_path, "bars") {
            steps_out.push(StepReport {
                name: "renko-from-idx".to_string(),
                duration_ms: 0,
                exit_code: 0,
                bytes: file_bytes(&bars_path),
                skipped: true,
                errors: Vec::new(),
            });
        } else {
            let args = vec![format!("{}-{}", base, quote)];
            let rep = run_step("renko-from-idx", &args, Some(&bars_path));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                return TickerReport {
                    ticker: ticker.to_string(),
                    status: "failed".to_string(),
                    steps: steps_out,
                };
            }
        }
    }

    // 5) validate
    if want("validate") {
        for (kind, path) in [
            ("idx", composite_idx.clone()),
            ("bars", bars_path.clone()),
            ("vol", vol_path.clone()),
        ] {
            let args = vec![
                kind.to_string(),
                path.to_string_lossy().to_string(),
                "--strict".to_string(),
                "--json".to_string(),
            ];
            let mut rep = run_step("integrity-check", &args, Some(&path));
            rep.name = format!("integrity-check[{}]", kind);
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
    TickerReport {
        ticker: ticker.to_string(),
        status: status.to_string(),
        steps: steps_out,
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
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

    let ctx = PlanCtx {
        args: args.clone(),
        from,
        to,
        exchanges,
        steps,
    };

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
                Err(_) => TickerReport {
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
                },
            };
            results.lock().unwrap().insert(t.clone(), rep);
        });
    });

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
    println!(
        "backfill done: total={} ok={} failed={} skipped={} → {}",
        total,
        ok,
        failed,
        skipped,
        args.log_file.display()
    );

    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
