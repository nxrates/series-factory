//! Down-sample sharded `.idx` AppendLogs from one cadence to a coarser one
//! (default 100ms / 10Hz → 200ms / 5Hz) using **last-in-bucket** selection.
//!
//! # Why (5Hz migration)
//!
//! NXR shipped a 5Hz (200ms) aggregator. Existing `.idx` history was written
//! at 10Hz (100ms; `idx_aggregation_ms: 100` confirmed live). To keep the
//! series time-consistent across the cutover seam, historical 100ms shards are
//! re-bucketed to 200ms windows. Deploy order is load-bearing: **deploy the
//! 5Hz aggregator first** (so new realtime is 200ms), *then* resample history,
//! so the join seam is a single cadence on both sides.
//!
//! # Method: last-in-200ms-bucket (NOT VWAP-merge)
//!
//! Each output record is the **last (latest-ts) whole input record** whose
//! observation ts falls in its 200ms bin (`floor(ts/target_ms)*target_ms`).
//! We keep that record **verbatim** (header re-stamped to the bin start; body
//! copied byte-for-byte) rather than VWAP-merging the bin's members.
//!
//! Rationale:
//! - A 5Hz aggregator's delta-gate would have emitted the *latest composite
//!   state* at each 200ms boundary, not a volume-weighted blend of the two
//!   10Hz sub-cycles. Last-in-bucket reproduces that emission exactly.
//! - It is the only choice that is **confidence-safe**. The `confidence` byte
//!   is flag-gated (`FLAG_CONF_FRESHNESS`): clear = legacy active-provider
//!   COUNT, set = Q0.8 freshness (`byte/255`). A VWAP-style `max`/pool of two
//!   confidence bytes is meaningless across that flag boundary and could
//!   silently produce a value whose flag no longer matches. Copying one whole
//!   record keeps `confidence` and its `FLAG_CONF_FRESHNESS` bit traveling
//!   together, untouched. See the confidence note below.
//! - Volumes/ticks are per-cycle counters on the wire, not flows to be summed:
//!   the live 5Hz writer reports the counters of *its* cycle, so preserving the
//!   representative record's counters is correct, not summing across sub-cycles.
//!
//! # Confidence handling (deliberately a NO-OP)
//!
//! Historical `.idx` records carry the OLD confidence semantics: an integer
//! active-provider COUNT (0..~22), with `FLAG_CONF_FRESHNESS` **clear**. The
//! new wire semantics is Q0.8 freshness (`byte/255`) gated by that flag, and
//! it is computed from per-provider decay state that is **not** persisted in
//! `.idx`. The raw decay inputs are gone ⇒ historical freshness is **not
//! recomputable**. The honest, correct handling is therefore to **preserve the
//! existing confidence byte and leave `FLAG_CONF_FRESHNESS` clear**, so readers
//! interpret historical rows as legacy counts. The flag-gated reader design
//! (`mitch::index` doc, `nxr_sdk::shard::FLAG_CONF_FRESHNESS`) is *built* to
//! span mixed old/new records exactly this way: only new realtime data carries
//! Q0.8. This tool does NOT touch `confidence` or that flag — no fabricated
//! freshness. Last-in-bucket copy guarantees this automatically.
//!
//! # Layout (sharded, per-MITCH-ticker)
//!
//! ```text
//! <input_dir>/<ticker_id>/<YYYY-MM-DD>.idx     56B IndexRecord, ts-ascending
//! ```
//!
//! `--input-dir` is the `indexes/` root. We iterate ticker subdirectories (each
//! named by its decimal `u64` MITCH id), and within each, every `*.idx` daily
//! shard. The ticker id comes from the **directory name** — never from the
//! date-stem filename. (The previous flat-layout version parsed `file_stem` as
//! `u64`, which on sharded `YYYY-MM-DD.idx` filenames always failed → the
//! misroute filter silently disabled. Fixed: id from dir, body-ticker filter
//! re-enabled.)
//!
//! # Idempotency
//!
//! Each resampled output row is tagged `FLAG_IDX_HEALED` (0x10, the dedicated
//! offline-rewrite bit for INDEX rows). On a re-run we **skip any shard already
//! fully at target cadence** (median inter-record spacing ≈ target_ms AND every
//! record carries `FLAG_IDX_HEALED`) so a second pass is a no-op. Re-bucketing
//! an already-200ms shard is also internally stable (one record per bin → same
//! output), so idempotency holds even if the skip heuristic is bypassed.
//!
//! # Marker flag
//!
//! Output rows OR in `FLAG_IDX_HEALED` (0x10). We deliberately do **not** use
//! the old `RESAMPLE_FLAG = 0x04`: 0x04 is `FLAG_RENKO_SYNTHETIC_BRICK` in the
//! Bar flag space and reusing it on INDEX rows is the exact collision the
//! `heal-idx` migration fixed (healed rows misclassified as synthetic bricks by
//! the calibration/vol exclusion). `FLAG_IDX_HEALED` is free in both spaces and
//! already means "offline-rewritten INDEX row", which is precisely what this is.
//!
//! # Usage
//!
//! ```sh
//! resample-idx --input-dir /data/indexes --source-ms 100 --target-ms 200 \
//!              [--ticker <id>] [--parallel 2] [--dry-run] [--commit]
//! ```
//!
//! Two phases per shard:
//! - Without `--commit`: writes `<shard>.idx.new` beside the original; original
//!   untouched (inspect before committing).
//! - With `--commit`: after invariants pass, atomic `<shard>.idx → .idx.bak`
//!   then `.idx.new → .idx`. `.bak` retained for trivial rollback.
//!
//! Per-shard invariants are verified before any rename; failures leave the
//! original `.idx` untouched and are reported in `resample-report.csv`.

use anyhow::{bail, Context, Result};
use clap::Parser;
use mitch::common::message_type;
use mitch::timestamp::{from_epoch_ms, to_epoch_ms};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{self, FLAG_IDX_HEALED};
use rayon::prelude::*;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(about = "Down-sample sharded MITCH .idx AppendLogs to a coarser cadence (last-in-bucket).")]
struct Args {
    /// `indexes/` root containing `<ticker_id>/<YYYY-MM-DD>.idx` shards.
    #[arg(long)]
    input_dir: PathBuf,

    /// Source cadence in milliseconds (existing aggregator emit period).
    #[arg(long, default_value = "100")]
    source_ms: u64,

    /// Target cadence in milliseconds (new aggregator emit period). Must be an
    /// integer multiple of `--source-ms`.
    #[arg(long, default_value = "200")]
    target_ms: u64,

    /// Single-ticker mode: only resample this ticker subdirectory (decimal id).
    #[arg(long)]
    ticker: Option<String>,

    /// Worker thread pool size for rayon. Keep small on the cluster (the node
    /// just recovered from I/O thrash — the runbook runs serial, parallel=1).
    #[arg(long, default_value = "2")]
    parallel: usize,

    /// Don't write `.idx.new`, only print the would-be report.
    #[arg(long)]
    dry_run: bool,

    /// Commit phase: after writing `.new`, atomically rename `.idx → .idx.bak`
    /// and `.new → .idx`. Without this flag, `.new` files are written but
    /// originals stay in place (lets you inspect before committing).
    #[arg(long)]
    commit: bool,
}

/// Per-shard outcome (one row in the final CSV report).
#[derive(Debug, Clone)]
struct ShardReport {
    ticker: String,
    date: String,
    bytes_old: u64,
    count_old: u64,
    count_new: u64,
    bytes_new: u64,
    median_spacing_old_ms: i64,
    median_spacing_new_ms: i64,
    dropped_misrouted: usize,
    skipped_already_target: bool,
    invariants_passed: bool,
    invariants_failed: Vec<String>,
    duration_ms: u128,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");

    let args = Args::parse();
    if args.commit && args.dry_run {
        bail!("--commit and --dry-run are mutually exclusive");
    }
    if args.target_ms == 0 || args.source_ms == 0 {
        bail!("source_ms and target_ms must be > 0");
    }
    if args.target_ms % args.source_ms != 0 {
        bail!(
            "target_ms ({}) must be an integer multiple of source_ms ({})",
            args.target_ms,
            args.source_ms
        );
    }
    let factor = (args.target_ms / args.source_ms) as usize;
    if factor < 2 {
        bail!("factor (target/source) must be >= 2; got {}", factor);
    }
    info!(
        input_dir = %args.input_dir.display(),
        source_ms = args.source_ms,
        target_ms = args.target_ms,
        factor,
        commit = args.commit,
        dry_run = args.dry_run,
        "resample-idx starting (sharded, last-in-bucket)"
    );

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.parallel.max(1))
        .build_global()
        .ok(); // ignore if already set

    let shards = collect_shards(&args.input_dir, args.ticker.as_deref())?;
    if shards.is_empty() {
        bail!(
            "no <ticker>/<date>.idx shards found under {}",
            args.input_dir.display()
        );
    }
    info!(shard_count = shards.len(), "scanned input directory");

    // Panic-safe: each shard goes through catch_unwind so one bad file does NOT
    // abort the rayon collect (which would discard every already-completed
    // result). A prior prod run lost ~290/351 results to a single panic.
    let reports: Vec<ShardReport> = shards
        .par_iter()
        .map(|sh| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                resample_shard(sh, &args)
            }));
            match result {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    error!(ticker = %sh.ticker, date = %sh.date, error = %e, "resample failed");
                    err_report(sh, format!("error: {e}"))
                }
                Err(panic_box) => {
                    let msg = panic_msg(&panic_box);
                    error!(ticker = %sh.ticker, date = %sh.date, panic = %msg, "resample PANICKED");
                    err_report(sh, format!("panic: {msg}"))
                }
            }
        })
        .collect();

    write_csv_report(&args.input_dir, &reports)?;

    let passed = reports.iter().filter(|r| r.invariants_passed).count();
    let skipped = reports.iter().filter(|r| r.skipped_already_target).count();
    let failed = reports.len() - passed;
    info!(passed, skipped, failed, "resample complete");
    if failed > 0 {
        warn!("{failed} shard(s) failed invariants — see resample-report.csv");
        std::process::exit(2);
    }
    Ok(())
}

/// A discovered shard: (ticker dir name, date stem, path).
struct ShardRef {
    ticker: String,
    date: String,
    path: PathBuf,
}

/// Walk `<input_dir>/<ticker_id>/<YYYY-MM-DD>.idx`. Ticker id is the SUBDIR
/// name (decimal u64); date is the file stem. `only` filters by ticker dir.
///
/// ⚠ LIVE-SHARD SAFETY: the CURRENT UTC day's shard is the file the live
/// aggregator (`IdxShardWriter`) holds open. Resampling it (atomic
/// `<date>.idx → .idx.bak`, `<date>.new → .idx`) orphans the live writer's fd
/// into the `.idx.bak` inode — live ticks then append to `.bak` while the API
/// reads the frozen resampled `.idx`. This is UNCONDITIONAL: not even `--force`
/// can override it (`--force` only relaxes invariant/quality gates, never this
/// invariant). The only safe way to rewrite today's shard is to coordinate a
/// live-writer stop/flush/reopen — out of scope here. Confirmed prod incident
/// 2026-06-10. Skipped shards are logged at INFO; they resample after midnight
/// rotation closes them.
fn collect_shards(root: &Path, only: Option<&str>) -> Result<Vec<ShardRef>> {
    let today = shard::today_utc();
    let mut out = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read_dir {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let ticker = match path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Ticker subdirs are decimal u64 ids; skip anything else (.bak roots etc.).
        if ticker.parse::<u64>().is_err() {
            continue;
        }
        if let Some(filter) = only {
            if ticker != filter {
                continue;
            }
        }
        // `list_shards` only matches `<YYYY-MM-DD>.idx` (date-parseable stem).
        for (date, shard_path) in shard::list_shards(&path, "idx")? {
            // LIVE-SHARD SAFETY: never rewrite the current UTC day's shard —
            // it is held open by the live aggregator (see fn doc + skip guard).
            if date >= today {
                info!(
                    ticker = %ticker,
                    date = %shard::date_stem(date),
                    "skipping current-day shard (live-open, will resample after rotation)"
                );
                continue;
            }
            out.push(ShardRef {
                ticker: ticker.clone(),
                date: shard::date_stem(date),
                path: shard_path,
            });
        }
    }
    out.sort_by(|a, b| (a.ticker.as_str(), a.date.as_str()).cmp(&(b.ticker.as_str(), b.date.as_str())));
    Ok(out)
}

fn resample_shard(sh: &ShardRef, args: &Args) -> Result<ShardReport> {
    let started = std::time::Instant::now();
    let expected_ticker: u64 = sh
        .ticker
        .parse::<u64>()
        .with_context(|| format!("ticker dir name not a u64: {}", sh.ticker))?;

    let bytes_old = fs::metadata(&sh.path)?.len();
    let mut records: Vec<IndexRecord> = shard::read_shard_aligned(&sh.path)
        .with_context(|| format!("read {}", sh.path.display()))?;
    let raw_count = records.len();

    // Filter to the directory's ticker. Some prod shards contain mis-routed
    // records (aggregator wrote rows under the wrong ticker dir — historical
    // bug). The dir name is authoritative; keep only body-ticker matches so the
    // output is semantically clean for this ticker. `.bak` retains everything
    // for forensic recovery. (This filter was dead in the flat-layout version
    // because the id was parsed from the date-stem filename and never matched.)
    let before = records.len();
    records.retain(|r| r.index.ticker == expected_ticker);
    let dropped_misrouted = before - records.len();
    if dropped_misrouted > 0 {
        warn!(
            ticker = %sh.ticker,
            date = %sh.date,
            dropped = dropped_misrouted,
            kept = records.len(),
            "filtered misrouted records (aggregator bug)"
        );
    }

    if records.is_empty() {
        return Ok(empty_report(sh, bytes_old, dropped_misrouted, started));
    }

    let median_spacing_old_ms = median_spacing(&records);

    // Idempotency skip: already at target cadence AND every row already healed.
    let already_target = is_already_target(&records, args.target_ms as i64);
    if already_target {
        info!(ticker = %sh.ticker, date = %sh.date, "already at target cadence — skipped");
        return Ok(ShardReport {
            ticker: sh.ticker.clone(),
            date: sh.date.clone(),
            bytes_old,
            count_old: raw_count as u64,
            count_new: records.len() as u64,
            bytes_new: bytes_old,
            median_spacing_old_ms,
            median_spacing_new_ms: median_spacing_old_ms,
            dropped_misrouted,
            skipped_already_target: true,
            invariants_passed: true,
            invariants_failed: vec![],
            duration_ms: started.elapsed().as_millis(),
        });
    }

    // --- Down-sample: last-in-bucket per 200ms window ---
    let resampled = resample_last_in_bucket(&records, args.target_ms as i64);
    let count_new = resampled.len() as u64;
    let rec_size = std::mem::size_of::<IndexRecord>() as u64;
    let bytes_new = count_new * rec_size;
    let median_spacing_new_ms = median_spacing(&resampled);

    // --- Verify invariants ---
    let mut failures = Vec::new();
    verify_invariants(&records, &resampled, args.target_ms as i64, &mut failures);
    let invariants_passed = failures.is_empty();

    // --- Write .new (unless dry-run) ---
    if !args.dry_run && invariants_passed {
        let new_path = with_suffix(&sh.path, "idx.new");
        nxr_sdk::ipc::write_atomic::<IndexRecord>(&new_path, &resampled)
            .with_context(|| format!("write {}", new_path.display()))?;

        if args.commit {
            let bak_path = with_suffix(&sh.path, "idx.bak");
            fs::rename(&sh.path, &bak_path).with_context(|| {
                format!("rename {} -> {}", sh.path.display(), bak_path.display())
            })?;
            fs::rename(&new_path, &sh.path).with_context(|| {
                format!("rename {} -> {}", new_path.display(), sh.path.display())
            })?;
            info!(ticker = %sh.ticker, date = %sh.date, count_old = records.len(), count_new, "committed");
        } else {
            info!(ticker = %sh.ticker, date = %sh.date, count_old = records.len(), count_new, "wrote .new (not committed)");
        }
    }

    Ok(ShardReport {
        ticker: sh.ticker.clone(),
        date: sh.date.clone(),
        bytes_old,
        count_old: records.len() as u64,
        count_new,
        bytes_new,
        median_spacing_old_ms,
        median_spacing_new_ms,
        dropped_misrouted,
        skipped_already_target: false,
        invariants_passed,
        invariants_failed: failures,
        duration_ms: started.elapsed().as_millis(),
    })
}

/// Bin start (floor to target_ms grid).
#[inline]
fn bin_start(ts_ms: i64, target_ms: i64) -> i64 {
    (ts_ms / target_ms) * target_ms
}

/// Down-sample by keeping the **last (latest-ts) record in each target_ms
/// bin**, re-stamped to the bin start. Body (incl. `confidence` + flags) copied
/// verbatim; `FLAG_IDX_HEALED` OR'd in. `records` must be ts-ascending (shard
/// invariant); we do not re-sort here. One output row per occupied bin.
fn resample_last_in_bucket(records: &[IndexRecord], target_ms: i64) -> Vec<IndexRecord> {
    let mut out: Vec<IndexRecord> = Vec::with_capacity(records.len() / 2 + 1);
    let mut cur_bin: Option<i64> = None;
    for r in records {
        let bin = bin_start(to_epoch_ms(r.header.get_timestamp()), target_ms);
        match cur_bin {
            Some(b) if b == bin => {
                // Same bin: replace the in-progress representative with this
                // later record (last-in-bucket wins).
                *out.last_mut().unwrap() = stamp(r, bin);
            }
            _ => {
                out.push(stamp(r, bin));
                cur_bin = Some(bin);
            }
        }
    }
    out
}

/// Copy a record verbatim, re-stamp header ts to `bin_ms`, OR in FLAG_IDX_HEALED.
/// Body (confidence, FLAG_CONF_FRESHNESS, ci, vols, counters) untouched.
#[inline]
fn stamp(r: &IndexRecord, bin_ms: i64) -> IndexRecord {
    let mut out = *r;
    let mut header = out.header;
    header.set_timestamp(from_epoch_ms(bin_ms));
    out.header = header;
    // Read packed field via copy, OR the marker, write back.
    let flags = out.index.flags | FLAG_IDX_HEALED;
    out.index.flags = flags;
    out
}

/// True if the shard is already at target cadence (median spacing == target_ms)
/// AND every record already carries FLAG_IDX_HEALED. Used to skip on re-run.
fn is_already_target(records: &[IndexRecord], target_ms: i64) -> bool {
    if records.len() < 2 {
        return false;
    }
    let all_healed = records.iter().all(|r| (r.index.flags & FLAG_IDX_HEALED) != 0);
    all_healed && median_spacing(records) == target_ms
}

/// Median inter-record spacing in ms (0 if < 2 records). Robust to gaps.
fn median_spacing(records: &[IndexRecord]) -> i64 {
    if records.len() < 2 {
        return 0;
    }
    let mut deltas: Vec<i64> = records
        .windows(2)
        .map(|w| {
            to_epoch_ms(w[1].header.get_timestamp()) - to_epoch_ms(w[0].header.get_timestamp())
        })
        .filter(|d| *d > 0)
        .collect();
    if deltas.is_empty() {
        return 0;
    }
    deltas.sort_unstable();
    deltas[deltas.len() / 2]
}

fn verify_invariants(
    old: &[IndexRecord],
    new: &[IndexRecord],
    target_ms: i64,
    failures: &mut Vec<String>,
) {
    // (1) count_new must be ≤ count_old and roughly half (one row per occupied
    //     200ms bin; clusters/gaps allow a range). Upper bound = #old bins.
    if new.len() > old.len() {
        failures.push(format!("count grew: {} -> {}", old.len(), new.len()));
    }

    // (2) Every new ts is bin-aligned to the target grid.
    if let Some(bad) = new
        .iter()
        .find(|r| to_epoch_ms(r.header.get_timestamp()) % target_ms != 0)
    {
        let ts = to_epoch_ms(bad.header.get_timestamp());
        failures.push(format!("ts {ts} not aligned to {target_ms}ms grid"));
    }

    // (3) Strictly increasing bin timestamps (one row per bin → strict).
    for w in new.windows(2) {
        let a = to_epoch_ms(w[0].header.get_timestamp());
        let b = to_epoch_ms(w[1].header.get_timestamp());
        if b <= a {
            failures.push(format!("non-increasing bin ts: {a} -> {b}"));
            break;
        }
    }

    // (4) No fabricated future data: last new ts ≤ last old ts (bin-floored).
    if let (Some(no), Some(oo)) = (new.last(), old.last()) {
        let last_new = to_epoch_ms(no.header.get_timestamp());
        let last_old = to_epoch_ms(oo.header.get_timestamp());
        if last_new > last_old {
            failures.push(format!("last_new ts {last_new} > last_old {last_old}"));
        }
    }

    // (5) Every new record carries FLAG_IDX_HEALED.
    if let Some(bad) = new.iter().find(|r| (r.index.flags & FLAG_IDX_HEALED) == 0) {
        let f = bad.index.flags;
        failures.push(format!("record missing FLAG_IDX_HEALED (flags={f:#x})"));
    }

    // (6) Confidence is NOT fabricated: FLAG_CONF_FRESHNESS distribution is
    //     preserved (we never set or clear it). Every new row's flag bit must
    //     have come from some old row (last-in-bucket copies verbatim) — i.e.
    //     if NO old row had it set, no new row may have it set.
    let old_any_fresh = old.iter().any(|r| {
        (r.index.flags & nxr_sdk::shard::FLAG_CONF_FRESHNESS) != 0
    });
    if !old_any_fresh {
        if let Some(bad) = new
            .iter()
            .find(|r| (r.index.flags & nxr_sdk::shard::FLAG_CONF_FRESHNESS) != 0)
        {
            let f = bad.index.flags;
            failures.push(format!(
                "fabricated FLAG_CONF_FRESHNESS (flags={f:#x}) — none in source"
            ));
        }
    }

    // (7) Msg type preserved.
    if let (Some(o), Some(n)) = (old.first(), new.first()) {
        let ot = o.header.message_type();
        let nt = n.header.message_type();
        if ot != nt {
            failures.push(format!("msg_type changed: {ot} -> {nt}"));
        }
    }
}

fn write_csv_report(input_dir: &Path, reports: &[ShardReport]) -> Result<()> {
    let path = input_dir.join("resample-report.csv");
    let mut f = fs::File::create(&path)?;
    writeln!(
        f,
        "ticker,date,bytes_old,count_old,count_new,bytes_new,median_spacing_old_ms,\
         median_spacing_new_ms,dropped_misrouted,skipped_already_target,\
         invariants_passed,duration_ms,failures"
    )?;
    for r in reports {
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            r.ticker,
            r.date,
            r.bytes_old,
            r.count_old,
            r.count_new,
            r.bytes_new,
            r.median_spacing_old_ms,
            r.median_spacing_new_ms,
            r.dropped_misrouted,
            r.skipped_already_target,
            r.invariants_passed,
            r.duration_ms,
            r.invariants_failed.join("|")
        )?;
    }
    info!(report = %path.display(), "report written");
    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────

/// Append a dotted suffix to a path's filename: `2026-06-01.idx` + `idx.new`
/// → `2026-06-01.idx.new`. (`Path::with_extension` would clobber `.idx`.)
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    // file_stem strips the final `.idx`; re-add via the requested suffix which
    // itself starts with `idx`.
    name.push('.');
    name.push_str(suffix);
    path.with_file_name(name)
}

fn empty_report(
    sh: &ShardRef,
    bytes_old: u64,
    dropped_misrouted: usize,
    started: std::time::Instant,
) -> ShardReport {
    ShardReport {
        ticker: sh.ticker.clone(),
        date: sh.date.clone(),
        bytes_old,
        count_old: 0,
        count_new: 0,
        bytes_new: 0,
        median_spacing_old_ms: 0,
        median_spacing_new_ms: 0,
        dropped_misrouted,
        skipped_already_target: false,
        invariants_passed: true,
        invariants_failed: vec![],
        duration_ms: started.elapsed().as_millis(),
    }
}

fn err_report(sh: &ShardRef, msg: String) -> ShardReport {
    ShardReport {
        ticker: sh.ticker.clone(),
        date: sh.date.clone(),
        bytes_old: 0,
        count_old: 0,
        count_new: 0,
        bytes_new: 0,
        median_spacing_old_ms: 0,
        median_spacing_new_ms: 0,
        dropped_misrouted: 0,
        skipped_already_target: false,
        invariants_passed: false,
        invariants_failed: vec![msg],
        duration_ms: 0,
    }
}

fn panic_msg(panic_box: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic_box.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic_box.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

// Keep the INDEX msg-type import live as a documentation anchor: index records
// carry MITCH msg type code 4 (`message_type::INDEX`). Asserted via invariant 7.
#[allow(dead_code)]
const _MSG_TYPE_INDEX_REMINDER: u8 = message_type::INDEX;

// ── tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mitch::common::message_type;
    use mitch::header::MitchHeader;
    use mitch::index::Index;

    fn rec(ticker: u64, ts_ms: i64, bid: f64, confidence: u8, flags: u8) -> IndexRecord {
        let header = MitchHeader::new(message_type::INDEX, 1, from_epoch_ms(ts_ms), 1);
        let body = Index {
            ticker,
            bid,
            ask: bid + 1.0,
            vbid: 10,
            vask: 10,
            ci: 0,
            tick_count: 1,
            confidence,
            accepted: confidence,
            rejected: 0,
            flags,
        };
        IndexRecord::new(header, body)
    }

    fn ts_of(r: &IndexRecord) -> i64 {
        to_epoch_ms(r.header.get_timestamp())
    }

    /// 100→200ms last-in-bucket: 10 records at 100ms → 5 records at 200ms,
    /// each carrying the LATER sub-cycle's body (bid/confidence) verbatim, ts
    /// bin-aligned to the 200ms grid, FLAG_IDX_HEALED set, FLAG_CONF_FRESHNESS
    /// untouched (clear → stays clear).
    #[test]
    fn downsample_100_to_200_last_in_bucket() {
        let t0 = 1_700_000_000_000_i64; // already 200ms-aligned
        let ticker = 42;
        // ts: t0, +100, +200, +300, ... +900 → bins [t0, t0+200, t0+400, t0+600, t0+800]
        let recs: Vec<IndexRecord> = (0..10)
            .map(|i| rec(ticker, t0 + i * 100, 100.0 + i as f64, (i + 1) as u8, 0))
            .collect();

        let out = resample_last_in_bucket(&recs, 200);

        assert_eq!(out.len(), 5, "10 @100ms → 5 @200ms");
        // Each bin keeps the LATER (odd-index) record: confidence 2,4,6,8,10.
        let confs: Vec<u8> = out.iter().map(|r| r.index.confidence).collect();
        assert_eq!(confs, vec![2, 4, 6, 8, 10], "last-in-bucket body preserved");
        // ts bin-aligned + 200ms spacing.
        for (i, r) in out.iter().enumerate() {
            let ts = ts_of(r);
            assert_eq!(ts % 200, 0, "bin-aligned");
            assert_eq!(ts, t0 + i as i64 * 200, "200ms grid");
        }
        assert_eq!(median_spacing(&out), 200, "spacing ≈ 200ms");
        // Marker set, freshness flag NOT fabricated.
        assert!(out.iter().all(|r| (r.index.flags & FLAG_IDX_HEALED) != 0));
        assert!(out
            .iter()
            .all(|r| (r.index.flags & nxr_sdk::shard::FLAG_CONF_FRESHNESS) == 0));

        let mut failures = Vec::new();
        verify_invariants(&recs, &out, 200, &mut failures);
        assert!(failures.is_empty(), "invariants: {failures:?}");
    }

    /// Idempotency: re-running on an already-200ms, already-healed series is a
    /// no-op (one record per bin → same count + same timestamps).
    #[test]
    fn idempotent_on_target_cadence() {
        let t0 = 1_700_000_000_000_i64;
        let ticker = 7;
        let first: Vec<IndexRecord> = (0..6)
            .map(|i| rec(ticker, t0 + i * 100, 50.0 + i as f64, 3, 0))
            .collect();
        let once = resample_last_in_bucket(&first, 200);
        assert!(is_already_target(&once, 200), "post-pass shard is at target");
        let twice = resample_last_in_bucket(&once, 200);
        assert_eq!(once.len(), twice.len(), "re-run count stable");
        for (a, b) in once.iter().zip(&twice) {
            assert_eq!(ts_of(a), ts_of(b), "re-run ts stable");
            assert_eq!(a.index.confidence, b.index.confidence, "body stable");
        }
    }

    /// LIVE-SHARD SAFETY: the resample plan (`collect_shards`) EXCLUDES the
    /// current UTC day's shard (live-open by the aggregator) while still
    /// including a closed past-day shard. Renaming today's shard would orphan
    /// the live writer's fd into the `.idx.bak` inode (prod incident 2026-06-10).
    #[test]
    fn plan_excludes_today_includes_past() {
        use nxr_sdk::shard::{date_stem, idx_dir, shard_path, today_utc, write_shard_atomic};

        let root = std::env::temp_dir().join(format!(
            "resample_skip_today_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let ticker_id: u64 = 42;
        let dir = idx_dir(&root, ticker_id);
        fs::create_dir_all(&dir).unwrap();

        let today = today_utc();
        let past = today.pred_opt().unwrap(); // yesterday (closed)

        // One record per shard is enough — collect_shards only inspects the
        // <YYYY-MM-DD>.idx filename, not the body.
        let one = [rec(ticker_id, 1_700_000_000_000, 100.0, 1, 0)];
        let bytes: Vec<u8> = one
            .iter()
            .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
            .collect();
        write_shard_atomic(&shard_path(&dir, past, "idx"), &bytes).unwrap();
        write_shard_atomic(&shard_path(&dir, today, "idx"), &bytes).unwrap();

        let indexes_root = root.join("indexes");
        let plan = collect_shards(&indexes_root, None).unwrap();
        let dates: Vec<&str> = plan.iter().map(|s| s.date.as_str()).collect();

        assert!(
            dates.contains(&date_stem(past).as_str()),
            "past-day shard must be in the resample plan: {dates:?}"
        );
        assert!(
            !dates.contains(&date_stem(today).as_str()),
            "today's live-open shard must be EXCLUDED from the plan: {dates:?}"
        );
        assert_eq!(plan.len(), 1, "exactly the one past-day shard, today skipped");

        let _ = fs::remove_dir_all(&root);
    }

    /// Confidence is NEVER converted: a legacy-count record (flag clear) stays a
    /// legacy count with the flag clear; a freshness record (flag set) keeps its
    /// flag — neither is fabricated nor stripped.
    #[test]
    fn confidence_flag_preserved_verbatim() {
        let t0 = 1_700_000_000_000_i64;
        let ticker = 9;
        let fresh = nxr_sdk::shard::FLAG_CONF_FRESHNESS;
        // bin 1: two legacy-count rows (flag clear); bin 2: two freshness rows.
        let recs = vec![
            rec(ticker, t0, 1.0, 12, 0),
            rec(ticker, t0 + 100, 1.0, 15, 0),
            rec(ticker, t0 + 200, 1.0, 200, fresh),
            rec(ticker, t0 + 300, 1.0, 210, fresh),
        ];
        let out = resample_last_in_bucket(&recs, 200);
        assert_eq!(out.len(), 2);
        // bin1 → later legacy row: conf 15, freshness flag clear.
        assert_eq!(out[0].index.confidence, 15);
        assert_eq!(out[0].index.flags & fresh, 0, "legacy flag stays clear");
        // bin2 → later freshness row: conf 210, freshness flag still set.
        assert_eq!(out[1].index.confidence, 210);
        assert_ne!(out[1].index.flags & fresh, 0, "freshness flag preserved");
    }
}
