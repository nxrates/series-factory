//! Backfill orchestrator — drives `fetch-crypto-history` → `ticks-to-idx` →
//! `merge-idx` → `s10-from-idx` → `renko-from-idx` → `integrity-check` for
//! many tickers in parallel and emits a single JSON report.
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
    /// Parallel ticker workers. Default lowered 4→2 (R-disk, 2026-06-09): each
    /// in-flight ticker holds up to 1 month × n_exch of raw `.ticks` during its
    /// monthly stream-and-delete loop, so the peak raw footprint scales with
    /// this value. 2 keeps the data PVC bounded while still overlapping
    /// download (I/O) with t2i (CPU). The HEAVY_WORK_RAM_CAP semaphore still
    /// independently bounds concurrent heavy subprocesses for RAM.
    #[arg(long, default_value_t = 2)]
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
    /// SINGLE escape-hatch for raw-tick / per-provider-`.idx` purge. Default
    /// OFF ⇒ purge-as-you-go is ON: t2i deletes each `.ticks` the instant it
    /// folds it (file-granular) AND `cleanup_ticker_staging` drops the raw dir
    /// + per-provider `indexes/<exch>/<BASE>-<QUOTE>.idx` once the composite is
    /// built+validated. The composite `.idx` is self-sufficient (s10 / renko /
    /// integrity-check all read it, never the raw staging); retaining staging
    /// after a 31-pair × 5y backfill is what overran the data PVC.
    ///
    /// Set `--keep-staging` to retain ALL raw `.ticks` + per-provider `.idx`
    /// for forensic re-merges. It is the ONLY purge knob — the old separate
    /// `--cleanup` flag was folded into this one (two flags gating one
    /// behaviour was the redundancy). Do NOT set in production backfill jobs.
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
        Self { permits: Mutex::new(permits.max(1)), cv: Condvar::new() }
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

fn integrity_clean(file: &Path, kind: &str) -> bool {
    if !file.exists() {
        return false;
    }
    let out = Command::new("integrity-check")
        .args([kind, file.to_string_lossy().as_ref(), "--json"])
        .output();
    // exit 0 = clean, 1 = warnings only (e.g. sparse ticker w/ >60s gaps —
    // legitimate for stablecoins like USDS/USDe/USDG/PYUSD; not a real error).
    // exit 2 = errors. Treat 0+1 as clean, only 2 as broken.
    match out {
        Ok(o) => matches!(o.status.code(), Some(0) | Some(1)),
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
    let mut cur = NaiveDate::from_ymd_opt(from.year(), from.month(), 1).unwrap().max(from);
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
fn delete_month_raw_ticks(
    out_dir: &Path,
    exchanges: &[String],
    base: &str,
    quote: &str,
) -> u64 {
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

/// Recursively delete every `*.ticks.partial` under `ticks/`. These are torn
/// half-downloads left by a killed `download_and_convert` (common.rs:383) — the
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
                .map(|n| n.ends_with(".ticks.partial"))
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

/// Startup orphan sweep: for every planned (ticker × exchange), if the composite
/// `indexes/<MITCH-ID>/manifest.json` already exists (the ticker is fully built),
/// the raw `ticks/<exch>/<BASE><QUOTE>` staging is dead weight from a prior
/// interrupted or killed run. Delete it. Never touches the composite `.idx`,
/// bars, `.vol`, or any manifest. Returns bytes freed.
fn sweep_orphaned_staging(out_dir: &Path, tickers: &[String], exchanges: &[String]) -> u64 {
    let mut freed = 0u64;
    for t in tickers {
        let (base, quote) = match nxr_sdk::split_pair_multi(t, &['/', '-']) {
            Some((b, q)) => (b.to_uppercase(), q.to_uppercase()),
            None => continue,
        };
        let ticker_id = nxr_sdk::resolve_ticker_id(&format!("{}/{}", base, quote));
        let manifest = nxr_sdk::shard::idx_dir(out_dir, ticker_id).join("manifest.json");
        if !manifest.exists() {
            continue; // ticker not yet built → its staging may be live; keep.
        }
        freed += delete_month_raw_ticks(out_dir, exchanges, &base, &quote);
        // Also drop the now-redundant per-exch .idx (the composite supersedes it).
        for ex in exchanges {
            let per_idx = out_dir
                .join("indexes")
                .join(ex)
                .join(format!("{}-{}.idx", base, quote));
            if per_idx.exists() {
                freed += file_bytes(&per_idx);
                let _ = fs::remove_file(&per_idx);
            }
        }
    }
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

/// Pre-flight disk-headroom guard (P2.2). With delete-as-you-go (t2i removes
/// each `.ticks` the instant it folds it into the `.idx`) the peak raw
/// footprint is bounded to roughly ONE archive file per exchange in flight:
///   1 file × n_exch × parallel × bytes_per_exchange_day × PEAK_FILE_DAYS,
/// where `PEAK_FILE_DAYS` sizes the single largest archive unit. The worst
/// case is a completed-month monthly archive (~31 days of one exchange's
/// ticks in one file). Abort if that projection exceeds
/// `free * headroom_safety_factor`. Knobs are YAML-sourced
/// (`series.pipeline.backfill`), never hardcoded.
fn preflight_disk_guard(
    out_dir: &Path,
    n_exch: usize,
    parallel: usize,
    cfg: &nxr_sdk::pipeline_config::BackfillDiskYml,
) -> Result<()> {
    // One monthly archive is the largest single `.ticks` unit a fetch can land
    // before t2i folds+deletes it. A daily-fallback file is 1 day; a completed
    // month is up to 31. Size the in-flight peak at the worst single file.
    const PEAK_FILE_DAYS: u64 = 31;
    let projected = cfg.bytes_per_exchange_day
        .saturating_mul(n_exch as u64)
        .saturating_mul(parallel.max(1) as u64)
        .saturating_mul(PEAK_FILE_DAYS);
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
        "pre-flight disk headroom check"
    );
    if projected > budget {
        return Err(anyhow!(
            "pre-flight disk guard: projected in-flight raw peak {:.1} GiB > {:.1} GiB \
             (free {:.1} GiB × safety {:.2}). Lower --parallel, free space, or tune \
             series.pipeline.backfill.{{bytes_per_exchange_day,headroom_safety_factor}}.",
            gib(projected), gib(budget), gib(free), cfg.headroom_safety_factor
        ));
    }
    Ok(())
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
    // Sharded paths — MITCH-ID keyed (canonical, U3/U4). See `docs/sharding-spec.md`.
    let ticker_id = nxr_sdk::resolve_ticker_id(&format!("{}/{}", base, quote));
    let composite_dir = nxr_sdk::shard::idx_dir(out_dir, ticker_id);
    let bars_dir = nxr_sdk::shard::bars_dir(out_dir, ticker_id);
    let vol_path = out_dir.join("vol").join(format!("{}-{}.vol", base, quote));

    let want = |s: &str| ctx.steps.iter().any(|x| x == s);

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

    // 1+2) fetch + t2i — DELETE-AS-YOU-GO at FILE granularity (R-disk, 2026-06-09).
    //
    // Previously fetch downloaded the WHOLE [from,to) range of raw `.ticks` for
    // every exchange up-front, and t2i folded them afterwards — so for a 2y x
    // n-exch backfill the full raw range coexisted on the PVC per ticker (peak
    // ≈ range × n_exch). That overran the data volume. A first fix bounded it
    // to ~1 month via a per-month batch delete; the operator (correctly) asked
    // why we batch at all — we should delete as we go.
    //
    // We still iterate per calendar month so fetch never runs more than ~1
    // month ahead of t2i, but the disk bound is now FILE-granular: t2i runs
    // with `--delete-after-convert`, removing each `.ticks` the instant it has
    // been folded into the per-exch `.idx`. So peak raw on disk ≈ 1 archive
    // file per exchange in flight (the one t2i is currently folding + at most
    // the one fetch is currently writing), NEVER a whole month or range.
    //   for each month M in [from,to]:
    //     fetch-crypto-history --from-date M.start --to-date M.end  (all exch)
    //     ticks-to-idx <exch> --delete-after-convert  (folds + deletes per file)
    //     defensive delete of any raw t2i could not remove  (before next month)
    //
    // t2i's AppendLog::open_buffered appends, and it discovers the .ticks files
    // present in the dir at call time (ticks_to_idx.rs:136,275), so per-month
    // invocation composes into one cumulative .idx. The TDWAP accumulator
    // restarts at each month's first tick; the only artifact is that a single
    // ≤cycle_ms aggregation cycle straddling a month boundary is split — a
    // sub-200ms effect on contiguous months, negligible vs the disk win.
    //
    // --resume: if the per-exch .idx already exists + integrity-clean for all
    // active exchanges, skip the whole fetch+t2i loop (preserves prior resume
    // semantics via the same integrity gate). Otherwise we rebuild fresh: any
    // stale/partial per-exch .idx is removed first so months are not appended
    // on top of a half-built log (which would duplicate records). Because t2i
    // fdatasync-flushes the .idx before deleting each raw file, a crash leaves
    // the .idx ⊇ every deleted file's records; the fresh-build path then
    // re-fetches the whole window from empty, so resume never double-counts.
    if want("fetch") || want("t2i") {
        let per_idx_paths: Vec<PathBuf> = active_exchanges
            .iter()
            .map(|ex| {
                out_dir
                    .join("indexes")
                    .join(ex)
                    .join(format!("{}-{}.idx", base, quote))
            })
            .collect();

        // Resume gate: all per-exch .idx already clean ⇒ skip fetch+t2i entirely.
        let all_clean = ctx.args.resume
            && want("t2i")
            && !per_idx_paths.is_empty()
            && per_idx_paths.iter().all(|p| integrity_clean(p, "idx"));

        if all_clean {
            for (ex, p) in active_exchanges.iter().zip(&per_idx_paths) {
                steps_out.push(StepReport {
                    name: format!("ticks-to-idx[{}]", ex),
                    duration_ms: 0,
                    exit_code: 0,
                    bytes: file_bytes(p),
                    skipped: true,
                    errors: Vec::new(),
                });
            }
            // Defensive: drop any orphaned raw from a prior interrupted run.
            delete_month_raw_ticks(out_dir, &active_exchanges, &base, &quote);
        } else {
            // Fresh build: remove any stale per-exch .idx so monthly appends
            // start from empty (no record duplication on re-run).
            if want("t2i") {
                for p in &per_idx_paths {
                    if p.exists() {
                        let _ = fs::remove_file(p);
                    }
                }
            }

            let windows = month_windows(ctx.from, ctx.to);
            let mut fetch_ms: u128 = 0;
            let mut fetch_err: Vec<String> = Vec::new();
            let mut t2i_ms: BTreeMap<String, u128> = BTreeMap::new();
            let mut t2i_err: Vec<String> = Vec::new();
            let mut total_freed: u64 = 0;
            let mut hard_fail = false;

            'months: for (m_start, m_end) in &windows {
                let m_start_s = m_start.format("%Y-%m-%d").to_string();
                let m_end_s = m_end.format("%Y-%m-%d").to_string();

                // fetch this month (all active exchanges)
                if want("fetch") {
                    let args = vec![
                        cfg.clone(),
                        "--pairs".to_string(),
                        base.clone(),
                        "--exchanges".to_string(),
                        active_exchanges.join(","),
                        "--quote".to_string(),
                        quote.clone(),
                        "--from-date".to_string(),
                        m_start_s.clone(),
                        "--to-date".to_string(),
                        m_end_s.clone(),
                    ];
                    let rep = run_step(ticker, "fetch-crypto-history", &args, None);
                    fetch_ms += rep.duration_ms;
                    // A whole-month fetch failure is non-fatal here: some
                    // exchanges legitimately lack a given month (sparse new
                    // pairs). t2i soft-skips empty dirs; merge uses whatever
                    // landed. Only a hard spawn failure (exit -1) tanks.
                    if rep.exit_code == -1 {
                        fetch_err.extend(rep.errors.clone());
                        hard_fail = true;
                        break 'months;
                    } else if rep.exit_code != 0 {
                        fetch_err.push(format!(
                            "{}: monthly fetch exit {}",
                            m_start_s, rep.exit_code
                        ));
                    }
                }

                // fold this month into each per-exch .idx (parallel across exch)
                if want("t2i") {
                    // delete-as-you-go: unless the operator asked to retain raw
                    // (`--keep-staging`), t2i deletes each `.ticks` the instant
                    // it folds it into the `.idx`, so peak raw never exceeds ~1
                    // file per exchange in flight (true file-granular cleanup,
                    // not a post-window batch delete).
                    let delete_as_you_go = !ctx.args.keep_staging;
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
                            ];
                            if delete_as_you_go {
                                args.push("--delete-after-convert".to_string());
                            }
                            let rep = run_step(ticker, "ticks-to-idx", &args, Some(&per_idx));
                            (ex.clone(), rep)
                        })
                        .collect();
                    for (ex, rep) in reps {
                        *t2i_ms.entry(ex.clone()).or_insert(0) += rep.duration_ms;
                        if rep.exit_code != 0 {
                            t2i_err.push(format!("{} [{}]: t2i exit {}", m_start_s, ex, rep.exit_code));
                            t2i_err.extend(rep.errors.clone());
                            hard_fail = true;
                        }
                    }
                }

                // Defensive sweep: t2i with --delete-after-convert already
                // removed each `.ticks` the instant it folded it, so the real
                // footprint bound is ~1 file per exchange in flight. This mops
                // up anything t2i could not delete (e.g. a file whose stream
                // errored mid-fold and was kept for retry, or the whole step
                // being skipped) before the next month is fetched.
                total_freed += delete_month_raw_ticks(out_dir, &active_exchanges, &base, &quote);

                if hard_fail {
                    break 'months;
                }
            }

            // Emit a single fetch + per-exch t2i StepReport summarising the loop.
            if want("fetch") {
                steps_out.push(StepReport {
                    name: "fetch-crypto-history[monthly]".to_string(),
                    duration_ms: fetch_ms,
                    exit_code: if hard_fail && !fetch_err.is_empty() { -1 } else { 0 },
                    bytes: total_freed,
                    skipped: false,
                    errors: fetch_err,
                });
            }
            if want("t2i") {
                for ex in &active_exchanges {
                    let per_idx = out_dir
                        .join("indexes")
                        .join(ex)
                        .join(format!("{}-{}.idx", base, quote));
                    let exit_code = if t2i_err.iter().any(|e| e.contains(&format!("[{}]", ex))) {
                        1
                    } else {
                        0
                    };
                    steps_out.push(StepReport {
                        name: format!("ticks-to-idx[{}]", ex),
                        duration_ms: *t2i_ms.get(ex).unwrap_or(&0),
                        exit_code,
                        bytes: file_bytes(&per_idx),
                        skipped: false,
                        errors: Vec::new(),
                    });
                }
            }

            if hard_fail {
                // P1.4: clean raw staging even on the failure path so a killed
                // ticker doesn't orphan its raw `.ticks` forever.
                if !ctx.args.keep_staging {
                    steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
                }
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
            let args = vec![
                base.clone(),
                quote.clone(),
                "--cycle-ms".to_string(),
                cycle_ms.clone(),
            ];
            let rep = run_step(ticker, "merge-idx", &args, Some(&composite_dir));
            let failed = rep.exit_code != 0;
            steps_out.push(rep);
            if failed {
                // P1.4: sweep raw + per-exch .idx even on failure (never the
                // composite/bars/.vol). Raw was already month-deleted; this
                // mops up the per-provider .idx + any orphan.
                if !ctx.args.keep_staging {
                    steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
                }
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
            if !ctx.args.keep_staging {
                steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
            }
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
        // Purge ON by default; the single `--keep-staging` flag opts out.
        if !ctx.args.keep_staging {
            steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
        } else {
            info!(ticker, "staging cleanup skipped (--keep-staging)");
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
                if !ctx.args.keep_staging {
                    steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
                }
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
                if !ctx.args.keep_staging {
                    steps_out.push(cleanup_ticker_staging(out_dir, &active_exchanges, &base, &quote));
                }
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

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
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
        let orphan_freed = sweep_orphaned_staging(&args.out_dir, &tickers, &ctx.exchanges);
        let total = part_freed + orphan_freed;
        if total > 0 {
            info!(
                partial_bytes = part_freed,
                orphan_bytes = orphan_freed,
                total_gib = total as f64 / (1024.0 * 1024.0 * 1024.0),
                "startup staging sweep reclaimed"
            );
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
        preflight_disk_guard(&args.out_dir, ctx.exchanges.len(), args.parallel, &disk_cfg)?;
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
