//! Backfill orchestrator — the SINGLE entry point for populating, verifying,
//! gap-checking and repairing every series. Drives `fetch-crypto-history` /
//! `fetch-pyth-history` → `ticks-to-idx` → `merge-idx` → `s10-from-idx` →
//! `renko-from-idx` → `integrity-check` → `data-quality-audit` for many tickers
//! in parallel and emits a single JSON report.
//!
//! Canonical invocation (nothing per-asset, nothing ad hoc):
//!
//! ```text
//! backfill-all /etc/nxr/config.yml --tickers native --from "365 days ago" \
//!   --to today --parallel 2 --min-free-gib 120 --out-dir /data
//! ```
//!
//! `--tickers native` is the CEX subscription manifest ∪ the oracle symbol
//! keys — the same set `core` materializes. Crosses are composed on read and
//! are never backfilled. Default `--steps` already covers populate → verify →
//! detect → repair, so gap repair is part of the pipeline, not a runbook.
//!
//! Observability + shard emission:
//! - tracing-subscriber init at startup (defaults to INFO; honours RUST_LOG)
//! - per-step start/done structured logs w/ duration_ms, exit_code, bytes
//! - per-ticker boundary logs + progress markers on disk under
//!   `<out_dir>/backfill/progress/<ticker>.{start,done,failed}`
//! - 60s heartbeat thread emits active/completed counters
//! - pre-fetch availability probe (HEAD-only) drops dead exchanges + may
//!   skip a ticker entirely if no exchange has coverage
//! - merge / s10 / renko emit daily shards; validate iterates shards via
//!   `integrity-check` per shard (best-effort)
//! - lazy: a day whose composite/s10 shard is on disk is never rebuilt, and its
//!   sources are never refetched (`--rebuild` overrides)
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
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use clap::Parser;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(about = "Backfill orchestrator: fetch → t2i → merge → s10 → renko → validate.")]
struct Args {
    /// Path to config.yml (forwarded to fetch-crypto-history).
    config: PathBuf,
    /// Start date (YYYY-MM-DD). Default: 1 year ago.
    #[arg(long)]
    from: Option<String>,
    /// End date (YYYY-MM-DD) or `today`. Default: today.
    #[arg(long, default_value = "today")]
    to: String,
    /// Comma-separated tickers `BASE-QUOTE`, or the sentinel `native` for
    /// every natively aggregated ticker (CEX subscription manifest ∪
    /// `oracles.providers.*.symbols`) — the same set `core` treats as native,
    /// so no derived/cross series is ever backfilled to disk.
    #[arg(long)]
    tickers: Option<String>,
    /// Comma-separated exchanges. Default: binance,bybit,bitget,okx.
    #[arg(long, default_value = "binance,bybit,bitget,okx")]
    exchanges: String,
    /// Quote currency (used to expand BASE-only tickers if needed).
    #[arg(long, default_value = "USDT")]
    quote: String,
    /// Comma-separated steps to run. Default: the whole pipeline — populate
    /// (`fetch`/`pyth` → `t2i` → `merge` → `s10` → `renko`), verify
    /// (`validate`), detect (`gaps`) and repair (`repair`). One invocation is
    /// the contract: gap repair is NOT a separate manual runbook.
    #[arg(
        long,
        default_value = "fetch,pyth,t2i,merge,s10,renko,validate,gaps,repair"
    )]
    steps: String,
    /// Parallel ticker workers. Default 2: each
    /// in-flight ticker holds up to 1 month × n_exch of raw `.ticks` during its
    /// monthly stream-and-delete loop, so the peak raw footprint scales with
    /// this value. 2 keeps the data PVC bounded while still overlapping
    /// download (I/O) with t2i (CPU). The HEAVY_WORK_RAM_CAP semaphore still
    /// independently bounds concurrent heavy subprocesses for RAM.
    #[arg(long, default_value_t = 2)]
    parallel: usize,
    /// Accepted and ignored: reruns are always lazy (see `--rebuild`).
    #[arg(long, hide = true)]
    resume: bool,
    /// Rebuild every day, even days whose composite shard, `.s10` and `.renko`
    /// are already on disk. By default nothing already on disk is fetched or
    /// rebuilt again.
    #[arg(long)]
    rebuild: bool,
    /// Output data root (contains ticks/, indexes/, bars/, vol/).
    #[arg(long, default_value = "/data")]
    out_dir: PathBuf,
    /// JSON report path.
    #[arg(long, default_value = "backfill.log.json")]
    log_file: PathBuf,
    /// Print plan and exit.
    #[arg(long)]
    dry_run: bool,
    /// Keep the sources of each month (raw `.ticks`, per-provider `.idx`) after
    /// its composite shards are built. Default OFF: they are deleted as soon as
    /// the composite is built and listed, so peak disk is about one month of
    /// every venue. The composite `.idx` is self-sufficient (s10 / renko /
    /// integrity-check read it, never the sources). With the flag, only the last
    /// processed month's per-provider `.idx` survive (each month starts them
    /// fresh); raw `.ticks` all stay. For audits; not for production backfills.
    #[arg(long, default_value_t = false)]
    keep_staging: bool,
    /// Skip availability probe (pre-fetch HEAD check). Useful for synthetic
    /// or offline environments.
    #[arg(long)]
    skip_probe: bool,
    /// Trailing window (days) the `gaps` step audits. Kept separate from
    /// `--from`: a 365 d populate is normal, a 365 d re-audit of every ticker
    /// every run is not (the `nxr-dq-cert` CronJob already blew a 50 min
    /// deadline doing exactly that).
    #[arg(long, default_value_t = 45)]
    gap_window_days: i64,
    /// Hard floor on free space (GiB) for the filesystem backing `--out-dir`.
    /// Checked before every ticker and by the heartbeat: the run ABORTS rather
    /// than writing the node into disk pressure. A cascade on 2026-07-30
    /// evicted 61 pods and took price serving down; a backfill must never be
    /// able to cause that.
    #[arg(long, default_value_t = 120)]
    min_free_gib: u64,
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

use nxr_sdk::shard::parse_utc_date_or_today as parse_date;

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

fn file_bytes(p: &Path) -> u64 {
    fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

/// True if `step` is in the configured step list (free-fn variant of the
/// per-ticker `want` closure, for use in `main` before `ctx` is consumed).
fn want_step(steps: &[String], step: &str) -> bool {
    steps.iter().any(|s| s == step)
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

/// Upper bound on concurrent HEAVY work units (`ticks-to-idx` / `merge-idx` /
/// `s10-from-idx` / `renko-from-idx` subprocesses). The outer ticker pool and
/// the inner per-exchange `par_iter` share one rayon pool, so without a global
/// gate up to `parallel × n_exchanges` heavy procs could spawn at once — each
/// can mmap a multi-GiB monthly tick file, OOM-killing a 16 GiB box. We cap the
/// *total* in-flight heavy procs at this many regardless of pool fan-out.
const HEAVY_WORK_RAM_CAP: usize = 4;

/// Process-global gate bounding concurrent heavy `run_step` subprocesses to
/// `min(available_parallelism, HEAVY_WORK_RAM_CAP)`. A counting semaphore built
/// on `Mutex<usize>` + `Condvar` (no extra dep). Scheduling-only — it changes
/// *when* heavy procs run, never their output.
struct HeavySemaphore {
    permits: Mutex<usize>,
    cv: Condvar,
}

impl HeavySemaphore {
    fn new(permits: usize) -> Self {
        Self {
            permits: Mutex::new(permits.max(1)),
            cv: Condvar::new(),
        }
    }
    fn acquire(&self) -> HeavyPermit<'_> {
        let mut p = self.permits.lock().unwrap();
        while *p == 0 {
            p = self.cv.wait(p).unwrap();
        }
        *p -= 1;
        HeavyPermit { sem: self }
    }
}

/// RAII permit — releases its slot and wakes one waiter on drop.
struct HeavyPermit<'a> {
    sem: &'a HeavySemaphore,
}

impl Drop for HeavyPermit<'_> {
    fn drop(&mut self) {
        let mut p = self.sem.permits.lock().unwrap();
        *p += 1;
        self.sem.cv.notify_one();
    }
}

/// Lazily-initialised global heavy-work gate. Sized on first use from
/// `available_parallelism` floored by [`HEAVY_WORK_RAM_CAP`].
static HEAVY_GATE: OnceLock<HeavySemaphore> = OnceLock::new();

fn heavy_gate() -> &'static HeavySemaphore {
    HEAVY_GATE.get_or_init(|| {
        let par = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(HEAVY_WORK_RAM_CAP);
        HeavySemaphore::new(par.min(HEAVY_WORK_RAM_CAP))
    })
}

fn run_step(ticker: &str, bin: &str, args: &[String], out_path: Option<&Path>) -> StepReport {
    // Bound total concurrent heavy subprocesses (OOM guard). Permit held for
    // the lifetime of the child process; released on scope exit.
    let _permit = heavy_gate().acquire();
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

// ── Monthly window helpers (raw-footprint bound) ───────────────────────────────

/// First day of the next calendar month.
fn next_month_start(d: NaiveDate) -> NaiveDate {
    use chrono::Datelike;
    if d.month() == 12 {
        NaiveDate::from_ymd_opt(d.year() + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(d.year(), d.month() + 1, 1).unwrap()
    }
}

/// Split `[from, to]` (inclusive) into per-calendar-month `(start, end)` windows,
/// both bounds inclusive. The orchestrator fetches + folds + deletes raw one
/// window at a time so peak raw `.ticks` is bounded to ~1 month × n_exch instead
/// of the full backfill range.
fn month_windows(from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    use chrono::Datelike;
    let mut out = Vec::new();
    let mut cur = NaiveDate::from_ymd_opt(from.year(), from.month(), 1)
        .unwrap()
        .max(from);
    while cur <= to {
        let month_end = next_month_start(cur) - chrono::Duration::days(1);
        out.push((cur, month_end.min(to)));
        cur = next_month_start(cur);
    }
    out
}

/// Delete all raw `.ticks` for a ticker across every exchange staging dir.
/// Called immediately after each month's t2i append succeeds so the raw never
/// accumulates beyond the current month. Touches ONLY `ticks/<exch>/<BASE><QUOTE>`;
/// never the per-provider/composite `.idx`, bars, or `.vol`.
fn delete_month_raw_ticks(out_dir: &Path, exchanges: &[String], base: &str, quote: &str) -> u64 {
    let mut freed = 0u64;
    let sym_dir = format!("{}{}", base, quote);
    for ex in exchanges {
        let dir = out_dir.join("ticks").join(ex).join(&sym_dir);
        if !dir.exists() {
            continue;
        }
        if let Ok(rd) = fs::read_dir(&dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                let is_tick = p
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|e| e == "ticks")
                    .unwrap_or(false)
                    || p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.ends_with(".ticks.partial"))
                        .unwrap_or(false);
                if is_tick {
                    freed += file_bytes(&p);
                    if let Err(e) = fs::remove_file(&p) {
                        warn!(path = %p.display(), err = %e, "month raw delete failed");
                    }
                }
            }
        }
    }
    freed
}

// ── Startup sweep + pre-flight disk guard (P1.4 / P1.5 / P2.2) ─────────────────

/// Recursively delete every `*.ticks.partial` and `.partial-dl-*` under `ticks/`.
/// These are torn half-downloads left by a killed `download_and_convert`: the
/// atomic rename never completed. They are never read by any step and otherwise
/// orphan forever. Returns bytes freed.
fn sweep_partial_ticks(out_dir: &Path) -> u64 {
    fn walk(dir: &Path, freed: &mut u64) {
        let rd = match fs::read_dir(dir) {
            Ok(r) => r,
            Err(_) => return,
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, freed);
            } else if p
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".ticks.partial") || n.starts_with(".partial-dl-"))
                .unwrap_or(false)
            {
                *freed += file_bytes(&p);
                if let Err(e) = fs::remove_file(&p) {
                    warn!(path = %p.display(), err = %e, "partial-ticks sweep delete failed");
                }
            }
        }
    }
    let mut freed = 0u64;
    walk(&out_dir.join("ticks"), &mut freed);
    freed
}

/// Free bytes available on the filesystem backing `path`, via `statvfs(3)`.
/// Returns `None` if the syscall fails.
fn free_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return None;
    }
    // Available blocks (f_bavail) for non-root × fragment size (f_frsize).
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

/// Raw `.ticks` of a perp series per exchange-day against the spot figure
/// `bytes_per_exchange_day` is sized for: BTC perp on a calm day is already ~0.9x
/// that spot bound on OKX (measured), and busy days run several times calm ones.
// ponytail: fixed factor; move into `series.pipeline.backfill` once the SDK struct can carry it.
const PERP_DAY_FACTOR: u64 = 3;

/// In-flight raw peak: each month window fetches every day of it for every
/// exchange before t2i folds and deletes it, so
/// `(window_days + 1) × n_exch × parallel × bytes_per_exchange_day` (× `PERP_DAY_FACTOR`),
/// where `window_days` is the longest month window of the run (up to 31) and the
/// `+ 1` is the extra day a venue cut at UTC+8 fetches.
fn projected_peak(
    bytes_per_exchange_day: u64,
    n_exch: usize,
    parallel: usize,
    window_days: u64,
    perp: bool,
) -> u64 {
    bytes_per_exchange_day
        .saturating_mul(if perp { PERP_DAY_FACTOR } else { 1 })
        .saturating_mul(n_exch as u64)
        .saturating_mul(parallel.max(1) as u64)
        .saturating_mul(window_days + 1)
}

/// Pre-flight disk-headroom guard (P2.2): abort if [`projected_peak`] exceeds
/// `free * headroom_safety_factor`. Knobs are YAML-sourced
/// (`series.pipeline.backfill`), never hardcoded.
fn preflight_disk_guard(
    out_dir: &Path,
    n_exch: usize,
    parallel: usize,
    window_days: u64,
    perp: bool,
    cfg: &nxr_sdk::pipeline_config::BackfillDiskYml,
) -> Result<()> {
    let projected = projected_peak(cfg.bytes_per_exchange_day, n_exch, parallel, window_days, perp);
    let free = match free_bytes(out_dir) {
        Some(f) => f,
        None => {
            warn!(out_dir = %out_dir.display(), "statvfs failed; skipping pre-flight disk guard");
            return Ok(());
        }
    };
    let budget = (free as f64 * cfg.headroom_safety_factor) as u64;
    let gib = |b: u64| b as f64 / (1024.0 * 1024.0 * 1024.0);
    info!(
        projected_peak_gib = gib(projected),
        free_gib = gib(free),
        budget_gib = gib(budget),
        safety_factor = cfg.headroom_safety_factor,
        n_exch,
        parallel,
        window_days,
        perp,
        "pre-flight disk headroom check"
    );
    if projected > budget {
        return Err(anyhow!(
            "pre-flight disk guard: projected in-flight raw peak {:.1} GiB > {:.1} GiB \
             (free {:.1} GiB × safety {:.2}). Lower --parallel, free space, or tune \
             series.pipeline.backfill.{{bytes_per_exchange_day,headroom_safety_factor}}.",
            gib(projected),
            gib(budget),
            gib(free),
            cfg.headroom_safety_factor
        ));
    }
    Ok(())
}

// ── Per-ticker pipeline ──────────────────────────────────────────────────────

/// Days of `[from, to]` whose composite `.idx` shard is not yet done (all of
/// them under `--rebuild`).
fn todo_days(dir: &Path, from: NaiveDate, to: NaiveDate, rebuild: bool) -> Vec<NaiveDate> {
    let rec = std::mem::size_of::<nxr_sdk::IndexRecord>() as u64;
    if rebuild {
        return from.iter_days().take_while(|d| *d <= to).collect();
    }
    series_factory::sharding::days_todo(dir, "idx", rec, from, to)
}

/// Dates of the `<date>.<kind>` shards in `dir`.
fn list_days(dir: &Path, kind: &str) -> Vec<NaiveDate> {
    let suffix = format!(".{kind}");
    let mut days: Vec<NaiveDate> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_suffix(&suffix)?.parse().ok()
        })
        .collect();
    days.sort_unstable();
    days
}

struct PlanCtx {
    args: Args,
    from: NaiveDate,
    to: NaiveDate,
    exchanges: Vec<String>,
    steps: Vec<String>,
    counters: Arc<Counters>,
    /// 0 = normal pass, 1 = the narrowed re-run a `repair` spawned. Bounds the
    /// recursion at one level: a day that is still missing after one repair is
    /// unrecoverable from the sources we have, and retrying cannot change that.
    repair_pass: u8,
}

/// Missing UTC days + outage windows for one ticker, as reported by
/// `data-quality-audit`.
///
/// Detection is NOT reimplemented here: `data-quality-audit` already does
/// sentinel-aware gap classification (`suspect`/`quiet`/`OUTAGE`) plus
/// missing-day enumeration over the canonical sharded layout. This just runs it
/// per ticker and turns its verdict into repair work.
#[derive(Default)]
struct GapVerdict {
    missing_days: Vec<NaiveDate>,
    outage_gaps: u64,
    worst: Vec<String>,
}

/// Severity of a gap, by duration. Drives whether the run FAILS.
fn gap_severity(dt_ms: i64) -> &'static str {
    match dt_ms {
        d if d >= 3_600_000 => "CRITICAL", // ≥1h: a shard-scale hole
        d if d >= 300_000 => "MAJOR",      // ≥5m: the 2026-07-30 sink outage class
        d if d >= 60_000 => "MINOR",
        _ => "INFO",
    }
}

/// Run `data-quality-audit` for one ticker id and parse its gap verdict.
fn detect_gaps(
    ticker: &str,
    ticker_id: u64,
    out_dir: &Path,
    window_days: i64,
) -> (StepReport, GapVerdict) {
    let start = Instant::now();
    let mut errors = Vec::new();
    let mut verdict = GapVerdict::default();
    let out = Command::new("data-quality-audit")
        .arg("--tickers")
        .arg(ticker_id.to_string())
        .arg("--window-days")
        .arg(window_days.to_string())
        .arg("--data-root")
        .arg(out_dir)
        .arg("--json")
        .output();
    let exit_code = match out {
        Err(e) => {
            errors.push(format!("spawn data-quality-audit: {e}"));
            -1
        }
        Ok(o) => {
            // A FAIL verdict is a non-zero exit; that is the SIGNAL, not an
            // error, so the JSON is parsed either way.
            match serde_json::from_slice::<serde_json::Value>(&o.stdout) {
                Ok(v) => {
                    for t in v["tickers"].as_array().into_iter().flatten() {
                        verdict.outage_gaps += t["outage_gaps"].as_u64().unwrap_or(0);
                        // Flat-filled outage: the s10 producer emits a bar per
                        // bucket regardless of ticks, so downtime shows up as a
                        // run of identical mids, NOT as missing rows. Prod
                        // 2026-07-30: an 8.5 min sink outage on BTC/USDT left 0
                        // missing rows and 52 zero-tick bars.
                        let flat = t["longest_flat_run"].as_u64().unwrap_or(0);
                        if flat > 30 {
                            verdict
                                .worst
                                .push(format!("FLATLINE run of {flat} identical mids"));
                        }
                        for d in t["missing_day_list"].as_array().into_iter().flatten() {
                            if let Some(d) = d.as_str().and_then(|d| d.parse::<NaiveDate>().ok()) {
                                verdict.missing_days.push(d);
                            }
                        }
                        for g in t["worst_gaps"].as_array().into_iter().flatten() {
                            let dt = g["dt_ms"].as_i64().unwrap_or(0);
                            verdict.worst.push(format!(
                                "{} gap {}ms [{}..{}] ({})",
                                gap_severity(dt),
                                dt,
                                g["start_ts"].as_i64().unwrap_or(0),
                                g["end_ts"].as_i64().unwrap_or(0),
                                g["classification"].as_str().unwrap_or("?"),
                            ));
                        }
                    }
                }
                Err(e) => errors.push(format!("parse dq json: {e}")),
            }
            0
        }
    };
    verdict.missing_days.sort_unstable();
    verdict.missing_days.dedup();
    if !verdict.missing_days.is_empty() || verdict.outage_gaps > 0 {
        // Loud by construction: this lands in the JSON report AND the log.
        warn!(
            ticker,
            missing_days = verdict.missing_days.len(),
            outage_gaps = verdict.outage_gaps,
            "GAPS DETECTED"
        );
        errors.extend(verdict.worst.iter().take(8).cloned());
        if !verdict.missing_days.is_empty() {
            errors.push(format!(
                "missing days: {}",
                verdict
                    .missing_days
                    .iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
    }
    (
        StepReport {
            name: "gaps".to_string(),
            duration_ms: start.elapsed().as_millis(),
            exit_code,
            bytes: 0,
            skipped: false,
            errors,
        },
        verdict,
    )
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
    exchanges: &[String],
    base: &str,
    quote: &str,
) -> (Vec<AvailabilityEntry>, Vec<String>) {
    let args = vec![
        ctx.args.config.to_string_lossy().to_string(),
        "--pairs".to_string(),
        base.to_string(),
        "--exchanges".to_string(),
        exchanges.join(","),
        "--quote".to_string(),
        quote.to_string(),
        "--from-date".to_string(),
        ctx.from.to_string(),
        "--to-date".to_string(),
        ctx.to.to_string(),
        "--probe".to_string(),
    ];
    let out = Command::new("fetch-crypto-history").args(&args).output();
    let mut result = Vec::new();
    let mut active = Vec::new();
    match out {
        Ok(o) if o.status.success() => {
            let raw = String::from_utf8_lossy(&o.stdout);
            // probe emits exactly 1 JSON line at the end; pick last non-empty
            let last = raw
                .lines()
                .filter(|l| !l.trim().is_empty())
                .last()
                .unwrap_or("");
            #[derive(Deserialize)]
            struct ProbeOut {
                exchange: String,
                has_data: bool,
                first_date: Option<String>,
                last_date: Option<String>,
            }
            if let Ok(parsed) = serde_json::from_str::<Vec<ProbeOut>>(last) {
                for p in parsed {
                    if p.has_data && exchanges.iter().any(|e| e == &p.exchange) {
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
                active = exchanges.to_vec();
            }
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr).to_string();
            warn!(exit = ?o.status.code(), stderr = %stderr, "probe failed; falling back");
            active = exchanges.to_vec();
        }
        Err(e) => {
            warn!(err = %e, "probe spawn failed; falling back");
            active = exchanges.to_vec();
        }
    }
    (result, active)
}

/// A one-step failed report for a ticker that never started.
fn failed_report(ctx: &PlanCtx, ticker: &str, step: &str, error: String) -> TickerReport {
    ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
    write_marker(&ctx.args.out_dir, ticker, "failed");
    TickerReport {
        ticker: ticker.to_string(),
        status: "failed".to_string(),
        steps: vec![StepReport {
            name: step.to_string(),
            duration_ms: 0,
            exit_code: -1,
            bytes: 0,
            skipped: false,
            errors: vec![error],
        }],
        ticker_availability: Vec::new(),
        skip_reason: None,
    }
}

fn run_ticker(ctx: &PlanCtx, ticker: &str) -> TickerReport {
    let (base, quote) = match nxr_sdk::split_pair_multi(ticker, &['/', '-'])
        .map(|(b, q)| (b.to_string(), q.to_string()))
        .ok_or_else(|| anyhow!("bad ticker {}: expected BASE-QUOTE", ticker))
    {
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

    let perp = series_factory::sources::split_quote(&quote).1;
    ctx.counters.active.fetch_add(1, Ordering::Relaxed);
    write_marker(&ctx.args.out_dir, ticker, "start");
    info!(ticker, "ticker start");

    let mut steps_out: Vec<StepReport> = Vec::new();
    let cfg = ctx.args.config.to_string_lossy().to_string();
    // Aggregation cadence — SINGLE source of truth: network.aggregation_interval_ms
    // (the exact value the live aggregator runs at, 200ms = 5Hz). Historical
    // backfill MUST use the identical cadence for both the per-provider
    // (ticks-to-idx) and composite (merge-idx) TDWAP, else the rebuilt .idx is
    // denser than live and renko bpd calibrates off by the cadence ratio
    // (the 100ms/10Hz hardcode produced ~2× the target median).
    let cycle_ms = nxr_sdk::pipeline_config::PipelineYml::load(&ctx.args.config)
        .ok()
        .and_then(|y| y.network.aggregation_interval_ms)
        .unwrap_or(200)
        .to_string();
    let out_dir = &ctx.args.out_dir;
    // Sharded paths — MITCH-ID keyed (canonical).
    let ticker_id = nxr_sdk::resolve_ticker_id(&format!("{}/{}", base, quote));
    let composite_dir = nxr_sdk::shard::idx_dir(out_dir, ticker_id);
    let bars_dir = nxr_sdk::shard::bars_dir(out_dir, ticker_id);
    let vol_path = out_dir.join("vol").join(format!("{}-{}.vol", base, quote));

    let want = |s: &str| ctx.steps.iter().any(|x| x == s);

    // Disk floor — checked per ticker, not just pre-flight. A long run must
    // notice the node filling under it and stop, never push it into pressure.
    if free_bytes(out_dir).is_some_and(|f| f < ctx.args.min_free_gib * 1024 * 1024 * 1024) {
        let free = free_bytes(out_dir).unwrap_or(0);
        error!(
            ticker,
            free_gib = free / (1024 * 1024 * 1024),
            floor_gib = ctx.args.min_free_gib,
            "DISK FLOOR breached; aborting ticker"
        );
        ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
        ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
        write_marker(&ctx.args.out_dir, ticker, "failed");
        return TickerReport {
            ticker: ticker.to_string(),
            status: "failed".to_string(),
            steps: vec![StepReport {
                name: "disk-floor".to_string(),
                duration_ms: 0,
                exit_code: -1,
                bytes: 0,
                skipped: false,
                errors: vec![format!(
                    "free {} GiB < floor {} GiB",
                    free / (1024 * 1024 * 1024),
                    ctx.args.min_free_gib
                )],
            }],
            ticker_availability: Vec::new(),
            skip_reason: None,
        };
    }

    // 0) availability probe — drops dead exchanges before fetch
    let mut availability = Vec::new();
    // A venue that lists this pair as an unrelated asset (`exclude_symbols`) never contributes.
    let venues: Vec<String> = match ctx
        .exchanges
        .iter()
        .filter_map(|e| match series_factory::sources::excluded(e, &base, &quote) {
            Ok(true) => None,
            Ok(false) => Some(Ok(e.clone())),
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<_>>()
    {
        Ok(v) => v,
        Err(err) => return failed_report(ctx, ticker, "config", format!("{err:#}")),
    };
    let mut active_exchanges = venues.clone();
    // An oracle-relayed symbol (FX, metals, equities, stablecoin pegs) has no
    // CEX book at all. Before the `pyth` step existed this was a hard skip, so
    // the "backfill everything" entry point silently covered crypto only.
    let mut pyth_only = false;
    if !ctx.args.skip_probe && want("fetch") {
        let (av, active) = probe_availability(ctx, &venues, &base, &quote);
        availability = av;
        // Pyth history is spot-only: a perp series with no coverage is skipped, never re-sourced.
        if active.is_empty() && want("pyth") && !perp {
            info!(ticker, "no CEX coverage; routing to pyth history");
            pyth_only = true;
            active_exchanges = vec!["pyth".to_string()];
        } else if active.is_empty() {
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
        } else {
            active_exchanges = active;
        }
        info!(ticker, active_exchanges = ?active_exchanges, "availability check ok");
    }

    // 1-3) Per month window: fetch -> t2i -> merge -> dispose.
    //
    // LAZY: a day is done when its composite `.idx` shard is in the manifest and
    // on disk exactly as recorded (`days_todo`). A month whose days are all done
    // is skipped outright, whether or not its raw archives or per-provider `.idx`
    // still exist: a finished composite never triggers a refetch. Otherwise only
    // the first..last missing day of the month is fetched, folded and merged.
    //
    // BOUNDED: the sources of a month (raw `.ticks`, per-provider `.idx`) are
    // deleted the moment its composite shards are built and listed, so peak disk
    // is about one month of every venue, never the whole window. t2i also
    // deletes each `.ticks` as it folds it. `--keep-staging` keeps them.
    //
    // Each month folds cold (the per-provider and composite accumulators restart
    // at the month's first record); the only artifact is the first seconds of a
    // month blending just the venues that have traded so far.
    let fail = |steps: Vec<StepReport>| -> TickerReport {
        ctx.counters.active.fetch_sub(1, Ordering::Relaxed);
        ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
        write_marker(&ctx.args.out_dir, ticker, "failed");
        TickerReport {
            ticker: ticker.to_string(),
            status: "failed".to_string(),
            steps,
            ticker_availability: availability.clone(),
            skip_reason: None,
        }
    };
    let mut rebuilt: Vec<NaiveDate> = Vec::new();
    if want("fetch") || want("t2i") || want("merge") {
        let per_idx_paths: Vec<PathBuf> = active_exchanges
            .iter()
            .map(|ex| {
                out_dir
                    .join("indexes")
                    .join(ex)
                    .join(format!("{}-{}.idx", base, quote))
            })
            .collect();
        // Today's shard belongs to the live writer and is never complete.
        let last_day = ctx.to.min(Utc::now().date_naive() - chrono::Duration::days(1));
        let (mut fetch_ms, mut merge_ms) = (0u128, 0u128);
        let mut t2i_ms: BTreeMap<String, u128> = BTreeMap::new();
        let (mut fetch_err, mut t2i_err, mut merge_err) = (Vec::new(), Vec::new(), Vec::new());
        let mut months_run = 0usize;
        let mut hard_fail = false;

        'months: for (m_start, m_end) in month_windows(ctx.from, ctx.to) {
            let todo = todo_days(&composite_dir, m_start, m_end.min(last_day), ctx.args.rebuild);
            let (Some(&lo), Some(&hi)) = (todo.first(), todo.last()) else {
                continue;
            };
            months_run += 1;
            let (lo_s, hi_s) = (lo.to_string(), hi.to_string());
            // A leftover per-provider `.idx` is from an interrupted run: refold.
            for p in &per_idx_paths {
                let _ = fs::remove_file(p);
            }

            if pyth_only && want("pyth") {
                // Oracle-sourced ticker: Pyth Lazer history is the only source
                // (no CEX book exists), and it emits `.ticks` in the SAME shape
                // fetch-crypto-history does, so everything downstream is
                // unchanged. 1-minute resolution is the floor Pyth serves.
                let unix = |d: NaiveDate, h, m, s| {
                    d.and_hms_opt(h, m, s)
                        .map(|t| t.and_utc().timestamp())
                        .unwrap_or(0)
                        .to_string()
                };
                let args = vec![
                    base.clone(),
                    "--quote".to_string(),
                    quote.clone(),
                    "--from-unix".to_string(),
                    unix(lo, 0, 0, 0),
                    "--to-unix".to_string(),
                    unix(hi, 23, 59, 59),
                ];
                let rep = run_step(ticker, "fetch-pyth-history", &args, None);
                fetch_ms += rep.duration_ms;
                if rep.exit_code == -1 {
                    fetch_err.extend(rep.errors.clone());
                    hard_fail = true;
                    break 'months;
                }
            } else if want("fetch") {
                let args = vec![
                    cfg.clone(),
                    "--pairs".to_string(),
                    base.clone(),
                    "--exchanges".to_string(),
                    active_exchanges.join(","),
                    "--quote".to_string(),
                    quote.clone(),
                    "--from-date".to_string(),
                    lo_s.clone(),
                    "--to-date".to_string(),
                    hi_s.clone(),
                ];
                let rep = run_step(ticker, "fetch-crypto-history", &args, None);
                fetch_ms += rep.duration_ms;
                // A whole-month fetch failure is non-fatal here: some exchanges
                // legitimately lack a month (sparse new pairs); t2i soft-skips
                // empty dirs and merge uses whatever landed. Only a spawn failure
                // (exit -1) tanks.
                if rep.exit_code == -1 {
                    fetch_err.extend(rep.errors.clone());
                    hard_fail = true;
                    break 'months;
                } else if rep.exit_code != 0 {
                    fetch_err.push(format!("{lo_s}..{hi_s}: fetch exit {}", rep.exit_code));
                }
            }

            if want("t2i") {
                let reps: Vec<(String, StepReport)> = active_exchanges
                    .par_iter()
                    .map(|ex| {
                        let per_idx = out_dir
                            .join("indexes")
                            .join(ex)
                            .join(format!("{}-{}.idx", base, quote));
                        let mut args = vec![
                            ex.clone(),
                            base.clone(),
                            quote.clone(),
                            "--cycle-ms".to_string(),
                            cycle_ms.clone(),
                            "--from-date".to_string(),
                            lo_s.clone(),
                            "--to-date".to_string(),
                            hi_s.clone(),
                        ];
                        if !ctx.args.keep_staging {
                            args.push("--delete-after-convert".to_string());
                        }
                        (ex.clone(), run_step(ticker, "ticks-to-idx", &args, Some(&per_idx)))
                    })
                    .collect();
                for (ex, rep) in reps {
                    *t2i_ms.entry(ex.clone()).or_insert(0) += rep.duration_ms;
                    if rep.exit_code != 0 {
                        t2i_err.push(format!("{lo_s} [{ex}]: t2i exit {}", rep.exit_code));
                        t2i_err.extend(rep.errors.clone());
                        hard_fail = true;
                    }
                }
                if hard_fail {
                    break 'months;
                }
            }

            if want("merge") {
                if !per_idx_paths.iter().any(|p| p.exists()) {
                    warn!(ticker, %lo, %hi, "no exchange data in window; nothing to merge");
                    continue;
                }
                let mut args = vec![
                    base.clone(),
                    quote.clone(),
                    "--cycle-ms".to_string(),
                    cycle_ms.clone(),
                    "--from-date".to_string(),
                    lo_s.clone(),
                    "--to-date".to_string(),
                    hi_s.clone(),
                ];
                for ex in &active_exchanges {
                    args.push("--exchange".to_string());
                    args.push(ex.clone());
                }
                let rep = run_step(ticker, "merge-idx", &args, Some(&composite_dir));
                merge_ms += rep.duration_ms;
                if rep.exit_code != 0 {
                    merge_err.extend(rep.errors.clone());
                    hard_fail = true;
                    break 'months;
                }
                // Verified = the manifest now vouches for the days it built; a
                // day still todo had no data, and `gaps` reports it.
                let still = todo_days(&composite_dir, lo, hi, false);
                rebuilt.extend(todo.iter().filter(|d| !still.contains(d)));
                if !still.is_empty() {
                    warn!(ticker, days = ?still, "days without composite data in window");
                }
            }

            // Dispose: the month's sources are no longer needed.
            if !ctx.args.keep_staging {
                let freed = delete_month_raw_ticks(out_dir, &active_exchanges, &base, &quote);
                for p in &per_idx_paths {
                    let _ = fs::remove_file(p);
                }
                info!(ticker, %lo, %hi, freed_bytes = freed, "month sources disposed");
            }
        }

        if want("fetch") {
            steps_out.push(StepReport {
                name: "fetch-crypto-history[monthly]".to_string(),
                duration_ms: fetch_ms,
                exit_code: if hard_fail && !fetch_err.is_empty() { -1 } else { 0 },
                bytes: 0,
                skipped: months_run == 0,
                errors: fetch_err,
            });
        }
        if want("t2i") {
            for ex in &active_exchanges {
                let failed = t2i_err.iter().any(|e| e.contains(&format!("[{ex}]")));
                steps_out.push(StepReport {
                    name: format!("ticks-to-idx[{ex}]"),
                    duration_ms: *t2i_ms.get(ex).unwrap_or(&0),
                    exit_code: i32::from(failed),
                    bytes: 0,
                    skipped: months_run == 0,
                    errors: Vec::new(),
                });
            }
        }
        if want("merge") {
            steps_out.push(StepReport {
                name: "merge-idx".to_string(),
                duration_ms: merge_ms,
                exit_code: if merge_err.is_empty() { 0 } else { 1 },
                bytes: dir_bytes(&composite_dir),
                skipped: months_run == 0,
                errors: merge_err,
            });
        }
        if hard_fail {
            return fail(steps_out);
        }
        if want("merge") && !manifest_ok(&composite_dir, "idx") {
            steps_out.push(StepReport {
                name: "merge-idx".to_string(),
                duration_ms: 0,
                exit_code: 1,
                bytes: 0,
                skipped: false,
                errors: vec!["no composite shards: no exchange had data in the window".into()],
            });
            return fail(steps_out);
        }
        if want("merge") && !rebuilt.is_empty() {
            // post-merge: validate the shards just built. Any failure fails the ticker.
            let shard_check = validate_shards(ticker, &composite_dir, "idx", Some(&rebuilt));
            steps_out.push(shard_check.clone());
            if shard_check.exit_code != 0 {
                return fail(steps_out);
            }
        }
    }

    // 4) s10 -> sharded bars_dir/*.s10. s10-from-idx leaves fresh days alone; the
    // step is skipped outright when every day is fresh.
    let idx_days = |dir: &Path| -> Vec<NaiveDate> {
        list_days(dir, "idx").into_iter().filter(|d| *d >= ctx.from && *d <= ctx.to).collect()
    };
    let mut s10_days: Vec<NaiveDate> = Vec::new();
    if want("s10") {
        let stale: Vec<NaiveDate> = idx_days(&composite_dir)
            .into_iter()
            .filter(|d| {
                ctx.args.rebuild
                    || !series_factory::sharding::fresh(
                        &bars_dir.join(format!("{d}.s10")),
                        &composite_dir.join(format!("{d}.idx")),
                        96,
                    )
            })
            .collect();
        if stale.is_empty() {
            steps_out.push(StepReport {
                name: "s10-from-idx".to_string(),
                duration_ms: 0,
                exit_code: 0,
                bytes: dir_bytes(&bars_dir),
                skipped: true,
                errors: Vec::new(),
            });
        } else {
            // Today's `.s10` belongs to the live `bars_s10` producer. Without
            // `--to-date` the regen rewrites it atomically and leaves that
            // writer appending to an unlinked inode, so the day's live bars
            // vanish until the pod restarts. Stop at yesterday, always.
            let mut args = vec![
                cfg.clone(),
                format!("{}-{}", base, quote),
                "--to-date".to_string(),
                (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).to_string(),
            ];
            if ctx.args.rebuild {
                args.push("--force".to_string());
            }
            let rep = run_step(ticker, "s10-from-idx", &args, Some(&bars_dir));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                return fail(steps_out);
            }
            s10_days = stale;
        }
    }

    // 5) renko -> sharded bars_dir/*.renko. Fresh when its shards are not older
    // than the newest composite shard or the calibration they were built with.
    let mut renko_ran = false;
    if want("renko") {
        let newest = |dir: &Path, ext: &str| -> Option<std::time::SystemTime> {
            fs::read_dir(dir)
                .ok()?
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(ext))
                .filter_map(|e| e.metadata().ok()?.modified().ok())
                .max()
        };
        let oldest_renko = fs::read_dir(&bars_dir).ok().and_then(|rd| {
            rd.flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(".renko"))
                .filter_map(|e| e.metadata().ok()?.modified().ok())
                .min()
        });
        let params = fs::metadata(&nxr_sdk::NxrConfig::from_env().ticker_params_path)
            .and_then(|m| m.modified())
            .ok();
        let inputs = newest(&composite_dir, ".idx").max(params);
        if !ctx.args.rebuild && oldest_renko.is_some() && oldest_renko >= inputs {
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
            let mut rep = run_step(ticker, "renko-from-idx", &args, Some(&bars_dir));
            // renko-from-idx exits 0 without bricks for a series with no calibrated k
            // (or inside the vol bootstrap): say so instead of reporting a quiet "ok".
            if rep.exit_code == 0 && newest(&bars_dir, ".renko").is_none() {
                let msg = "no renko shards: no calibrated k for this series or the window is inside the vol bootstrap (run nxr-calibrate, then --steps renko)";
                warn!(ticker, "{msg}");
                rep.errors.push(msg.to_string());
            }
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                return fail(steps_out);
            }
            renko_ran = true;
        }
    }

    // 6) validate what was built this run: idx shards were checked right after
    // the merge; s10 and renko shards are checked when their step ran.
    if want("validate") {
        if !s10_days.is_empty() {
            steps_out.push(validate_shards(ticker, &bars_dir, "s10", Some(&s10_days)));
        }
        if renko_ran {
            steps_out.push(validate_shards(ticker, &bars_dir, "renko", None));
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
    }

    // 7) gaps → repair. Detection runs on the persisted series (so it sees the
    // holes a live sink outage left, not just fetch failures); repair re-drives
    // the SAME producer chain over the missing days only.
    if want("gaps") {
        let window_days = (ctx.to - ctx.from)
            .num_days()
            .clamp(1, ctx.args.gap_window_days);
        let (rep, verdict) = detect_gaps(ticker, ticker_id, out_dir, window_days);
        let has_gaps = !verdict.missing_days.is_empty() || verdict.outage_gaps > 0;
        steps_out.push(rep);
        if has_gaps && want("repair") && ctx.repair_pass == 0 && !verdict.missing_days.is_empty() {
            let (lo, hi) = (
                *verdict.missing_days.first().unwrap(),
                *verdict.missing_days.last().unwrap(),
            );
            info!(ticker, %lo, %hi, n_days = verdict.missing_days.len(), "repairing gaps");
            let sub = PlanCtx {
                args: ctx.args.clone(),
                from: lo,
                to: hi + chrono::Duration::days(1),
                exchanges: ctx.exchanges.clone(),
                steps: ["fetch", "pyth", "t2i", "merge", "s10", "renko", "gaps"]
                    .iter()
                    .filter(|s| want(s) || **s == "gaps")
                    .map(|s| s.to_string())
                    .collect(),
                counters: Arc::clone(&ctx.counters),
                repair_pass: 1,
            };
            let sub_rep = run_ticker(&sub, ticker);
            // The post-repair `gaps` step is the proof: anything it still
            // reports is PERMANENTLY LOST (no source retains it) and must stay
            // visible rather than being quietly marked repaired.
            for mut s in sub_rep.steps {
                s.name = format!("repair:{}", s.name);
                steps_out.push(s);
            }
        }
    }

    let any_fail = steps_out.iter().any(|s| s.exit_code != 0 && !s.skipped);
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

/// Cheap gate: manifest.json present in ticker_dir AND ≥1 shard of
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
fn validate_shards(
    ticker: &str,
    ticker_dir: &Path,
    kind: &str,
    only: Option<&[NaiveDate]>,
) -> StepReport {
    let name = format!("integrity-check-shards[{}]", kind);
    let start = Instant::now();
    let suffix = format!(".{}", kind);
    let mut shards: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = fs::read_dir(ticker_dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if let Some(n) = path.file_name().and_then(|n| n.to_str()) {
                let wanted = only.is_none_or(|days| {
                    n.strip_suffix(&suffix)
                        .and_then(|d| d.parse::<NaiveDate>().ok())
                        .is_some_and(|d| days.contains(&d))
                });
                if n.ends_with(&suffix) && wanted {
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
            "--json".to_string(),
        ];
        // exit 0 = clean, 1 = warnings only (sparse stablecoin gaps, etc.),
        // 2 = errors. Drop --strict so warnings ! tank tickers.
        let out = Command::new("integrity-check").args(&args).output();
        match out {
            Ok(o) if matches!(o.status.code(), Some(0) | Some(1)) => {}
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
                    "shard integrity-check failed (exit=2, errors)"
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

/// Every natively aggregated ticker: the CEX subscription manifest
/// (`NxrConfig::symbol_list`, i.e. `NXR_SYMBOLS` or its compiled default) ∪
/// `PipelineYml::relay_symbols()` (`oracles.providers.*` ∪
/// `ctrader.providers.*`).
///
/// This is the SAME union `core` calls `native_ids` (`core/src/main.rs`), and
/// deliberately excludes `cexs.cross_pairs`: crosses are a pure function of
/// their legs, are composed on read, and are never materialized. Backfilling
/// one would recreate exactly the 2 657 derived trees that filled the node.
///
/// The oracle-only walk here skipped every `ctrader` broker symbol, so
/// `--tickers native` covered crypto + Pyth and silently omitted the FX /
/// metals / energy / softs / index primaries the moment that section landed:
/// the same section-blind union bug already fixed in `configured_symbols`.
fn native_tickers(config: &Path) -> Result<Vec<String>> {
    let mut out: Vec<String> = nxr_sdk::NxrConfig::from_env().symbol_list();
    if let Ok(y) = nxr_sdk::pipeline_config::PipelineYml::load(config) {
        out.extend(y.relay_symbols());
    }
    out.iter_mut().for_each(|s| *s = s.replace('/', "-"));
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err(anyhow!("--tickers native resolved to an empty set"));
    }
    Ok(out)
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    nxr_sdk::pipeline_config::PipelineYml::check_config_and_exit_if_requested(nxr_sdk::pipeline_config::ConfigHint::Bin);
    // tracing init — default INFO, RUST_LOG override honoured via sdk's init.
    nxr_sdk::logging::init("info");

    let args = Args::parse();

    let to = parse_date(&args.to)?;
    let from = match &args.from {
        Some(s) => parse_date(s)?,
        None => to - chrono::Duration::days(365),
    };
    let exchanges = split_csv(&args.exchanges);
    let steps = split_csv(&args.steps);
    let tickers: Vec<String> = match args.tickers.as_deref() {
        // `all` is what the deployed `nxr-backfill` CronJob already passes; it
        // was never a sentinel, so it parsed as a literal ticker named "all"
        // and the job could only ever fail. Same set as `native` — a cross is
        // never backfilled.
        Some("native") | Some("all") => native_tickers(&args.config)?,
        Some(s) => split_csv(s),
        None => {
            return Err(anyhow!(
                "--tickers required (comma-separated BASE-QUOTE list, or `native`)"
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
        repair_pass: 0,
    };

    // ensure progress dir exists
    let _ = fs::create_dir_all(progress_dir(&args.out_dir));

    // P1.5 — prune stale progress markers from prior runs. They are append-only
    // crumbs (`<ticker>.{start,done,failed}`) that never get cleared, so they
    // grow unbounded across re-backfills. Clear them at run start so the
    // directory only reflects THIS run.
    {
        let pdir = progress_dir(&args.out_dir);
        let mut pruned = 0usize;
        if let Ok(rd) = fs::read_dir(&pdir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_file() && fs::remove_file(&p).is_ok() {
                    pruned += 1;
                }
            }
        }
        if pruned > 0 {
            info!(pruned, dir = %pdir.display(), "pruned stale progress markers");
        }
    }

    // P1.4 — startup orphan sweep. Kill torn `*.ticks.partial` everywhere, then
    // drop raw staging for any planned ticker whose composite manifest already
    // exists+validates (dead weight from a prior killed run). Never touches
    // composite/.idx/bars/.vol.
    {
        let part_freed = sweep_partial_ticks(&args.out_dir);
        if part_freed > 0 {
            info!(partial_bytes = part_freed, "startup sweep reclaimed torn downloads");
        }
    }

    // P2.2 — pre-flight disk-headroom guard. With delete-as-you-go the raw peak
    // is bounded to ~1 archive file per exchange in flight; abort early if even
    // that (tiny) projection won't fit free space (× safety factor). Knobs are
    // YAML-sourced (no hardcode).
    if want_step(&ctx.steps, "fetch") {
        let disk_cfg = nxr_sdk::pipeline_config::PipelineYml::load(&args.config)
            .map(|y| y.series.pipeline.backfill)
            .unwrap_or_default();
        let window_days = month_windows(from, to)
            .iter()
            .map(|(a, b)| (*b - *a).num_days() as u64 + 1)
            .max()
            .unwrap_or(0);
        let perp = tickers.iter().any(|t| {
            nxr_sdk::split_pair_multi(t, &['/', '-'])
                .is_some_and(|(_, q)| series_factory::sources::split_quote(q).1)
        });
        preflight_disk_guard(
            &args.out_dir,
            ctx.exchanges.len(),
            args.parallel,
            window_days,
            perp,
            &disk_cfg,
        )?;
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    /// A day listed in the manifest and on disk is never todo again, even with
    /// every source gone; `--rebuild` makes every day todo.
    #[test]
    fn done_days_are_not_todo_unless_rebuilding() {
        use series_factory::sharding::{manifest_path, shard_path, write_manifest, Manifest, ShardEntry};
        let dir = std::env::temp_dir().join(format!("bf-todo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let rec = std::mem::size_of::<nxr_sdk::IndexRecord>();
        let day = |n| NaiveDate::from_ymd_opt(2024, 1, n).unwrap();
        fs::write(shard_path(&dir, day(1), "idx"), vec![0u8; rec * 2]).unwrap();
        let mut m = Manifest::new("T".into(), 1, "idx");
        m.set_shards_batch(vec![ShardEntry {
            date: "2024-01-01".into(),
            first_ts: 0,
            last_ts: 0,
            n_records: 2,
            size_bytes: (rec * 2) as u64,
            sha256: String::new(),
        }]);
        write_manifest(&manifest_path(&dir), &m).unwrap();
        assert_eq!(todo_days(&dir, day(1), day(3), false), [day(2), day(3)]);
        assert_eq!(todo_days(&dir, day(1), day(3), true), [day(1), day(2), day(3)]);
        assert_eq!(list_days(&dir, "idx"), [day(1)]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The disk projection follows the window (not a fixed month) and is larger for perps.
    #[test]
    fn projected_peak_scales_with_window_and_perp() {
        let spot = |days| projected_peak(100, 4, 2, days, false);
        assert_eq!(spot(30), 100 * 4 * 2 * 31);
        assert_eq!(spot(2), 100 * 4 * 2 * 3);
        assert_eq!(projected_peak(100, 4, 2, 30, true), spot(30) * PERP_DAY_FACTOR);
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn month_windows_full_calendar_months() {
        // Jan 1 .. Mar 31 → 3 inclusive month windows.
        let w = month_windows(d(2024, 1, 1), d(2024, 3, 31));
        assert_eq!(
            w,
            vec![
                (d(2024, 1, 1), d(2024, 1, 31)),
                (d(2024, 2, 1), d(2024, 2, 29)), // 2024 is a leap year
                (d(2024, 3, 1), d(2024, 3, 31)),
            ]
        );
    }

    #[test]
    fn month_windows_partial_first_and_last() {
        // Mid-month start + mid-month end clamp to the requested bounds.
        let w = month_windows(d(2023, 1, 15), d(2023, 3, 10));
        assert_eq!(
            w,
            vec![
                (d(2023, 1, 15), d(2023, 1, 31)),
                (d(2023, 2, 1), d(2023, 2, 28)),
                (d(2023, 3, 1), d(2023, 3, 10)),
            ]
        );
    }

    #[test]
    fn month_windows_single_partial_month() {
        let w = month_windows(d(2025, 6, 5), d(2025, 6, 9));
        assert_eq!(w, vec![(d(2025, 6, 5), d(2025, 6, 9))]);
    }

    #[test]
    fn month_windows_year_boundary() {
        let w = month_windows(d(2023, 12, 1), d(2024, 1, 31));
        assert_eq!(
            w,
            vec![
                (d(2023, 12, 1), d(2023, 12, 31)),
                (d(2024, 1, 1), d(2024, 1, 31)),
            ]
        );
    }

    #[test]
    fn windows_cover_range_contiguously_no_gap_no_overlap() {
        // Property: windows tile [from,to] exactly — each next start = prev end+1.
        let w = month_windows(d(2022, 3, 17), d(2023, 5, 2));
        assert_eq!(w.first().unwrap().0, d(2022, 3, 17));
        assert_eq!(w.last().unwrap().1, d(2023, 5, 2));
        for pair in w.windows(2) {
            assert_eq!(pair[1].0, pair[0].1 + chrono::Duration::days(1));
        }
    }
}
