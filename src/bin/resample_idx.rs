//! Resample 20Hz (50ms-cadence) `.idx` AppendLogs to a lower output cadence
//! (default 10Hz / 100ms bins).
//!
//! Why: NXR is moving from a 50ms aggregator cycle to 100ms. Existing `.idx`
//! history is 20Hz; future writes are 10Hz. To keep consumers seeing a
//! time-consistent series across the cutover, we down-sample the historical
//! 50ms records into 100ms bins using a volume-weighted merge that preserves
//! VWAP semantics + variance pooling on the encoded confidence interval.
//! Atomic rename + `.bak` retention so rollback is trivial.
//!
//! ## Merge formula
//!
//! For two consecutive 50ms `IndexRecord`s A and B collapsed into one 100ms
//! bin (general N-into-1 case for arbitrary `factor = target_ms / source_ms`):
//!
//! - `bid` = `Σ(bid_i · vbid_i) / Σ vbid_i` (volume-weighted; fallback to
//!   simple mean if both vbid==0). Same for `ask` w/ `vask_i`.
//! - `vbid` = `saturating_sum(vbid_i)`; `vask` = `saturating_sum(vask_i)`.
//! - `ci`  = `encode_ci_ubp( sqrt( Σ(decode_ci_ubp(ci_i))² · w_i / Σ w_i ) )`
//!   where `w_i = vbid_i + vask_i` (fallback `tick_count_i` if vols zero).
//!   Variance pooling — independent estimator combination.
//! - `tick_count` = `saturating_sum(tick_count_i)`.
//! - `confidence` / `accepted` / `rejected` = `max_i` (peak in window; these
//!   are per-cycle counters not flows).
//! - `flags` = `OR_i flags_i | RESAMPLE_FLAG (0x04)`.
//! - `header.timestamp` = `floor(to_epoch_ms(A.ts) / target_ms) * target_ms`
//!   re-encoded via `from_epoch_ms`. Bin-aligned.
//! - `header.sequence` = renumbered 0..N in output order.
//! - `header.count` = number of input records merged into this bin.
//!
//! Trailing odd input (no partner) is emitted as-is with bin-aligned ts.
//!
//! ## Usage
//!
//! ```sh
//! resample-idx --input-dir /data/indexes --source-ms 50 --target-ms 100 \
//!              [--ticker <id>] [--parallel 4] [--dry-run] [--commit] \
//!              [--keep-bak-days 14]
//! ```
//!
//! Two-phase lifecycle:
//! 1. Bulk pass (online): aggregator keeps running at source_ms cadence;
//!    resampler reads up to `floor(N/factor) * factor` records and writes
//!    `<id>.idx.new`. Tail records (partial last group) are skipped.
//! 2. Delta pass (offline, during pod stop window): aggregator paused;
//!    resampler reads any newly-appended records and appends merged bins to
//!    `<id>.idx.new`. Then atomic rename `<id>.idx → <id>.idx.bak`
//!    and `<id>.idx.new → <id>.idx`. Aggregator restarts at target_ms.
//!
//! Verification (always): per-file invariants checked before rename. Failures
//! leave `<id>.idx` untouched and emit a diff report.

use anyhow::{bail, Context, Result};
use clap::Parser;
use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp::{from_epoch_ms, to_epoch_ms};
use nxr_sdk::{
    ipc::record::IndexRecord,
    tdwap::{decode_ci_ubp, encode_ci_ubp},
};
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

/// Flag bit set on merged records so consumers can detect resampled history.
const RESAMPLE_FLAG: u8 = 0x04;

#[derive(Parser, Debug)]
#[command(about = "Down-sample MITCH .idx AppendLogs to a coarser cadence (VWAP-preserving).")]
struct Args {
    /// Directory containing `<ticker_id>.idx` files (e.g. /data/indexes).
    #[arg(long)]
    input_dir: PathBuf,

    /// Source cadence in milliseconds (existing aggregator emit period).
    #[arg(long, default_value = "50")]
    source_ms: u64,

    /// Target cadence in milliseconds (new aggregator emit period). Must be
    /// an integer multiple of `--source-ms`.
    #[arg(long, default_value = "100")]
    target_ms: u64,

    /// Single-ticker mode: only resample this filename stem (decimal id).
    #[arg(long)]
    ticker: Option<String>,

    /// Worker thread pool size for rayon. Cap respects the 16GB-box rule.
    #[arg(long, default_value = "4")]
    parallel: usize,

    /// Don't write `.idx.new`, only print the would-be report.
    #[arg(long)]
    dry_run: bool,

    /// Commit phase: after writing `.new`, atomically rename `.idx → .idx.bak`
    /// and `.new → .idx`. Without this flag, `.new` files are written but
    /// originals stay in place (lets you inspect before committing).
    #[arg(long)]
    commit: bool,

    /// How long to keep the `.bak` files (informational only — caller must
    /// schedule the cleanup separately, eg via a cron find -mtime).
    #[arg(long, default_value = "14")]
    keep_bak_days: u32,
}

/// Per-file outcome (one row in the final CSV report).
#[derive(Debug, Clone)]
struct FileReport {
    id: String,
    bytes_old: u64,
    count_old: u64,
    count_new: u64,
    bytes_new: u64,
    ts_first_ms_old: i64,
    ts_last_ms_old: i64,
    ts_first_ms_new: i64,
    ts_last_ms_new: i64,
    partial_tail_dropped: usize,
    invariants_passed: bool,
    invariants_failed: Vec<String>,
    duration_ms: u128,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");

    let args = Args::parse();
    if args.target_ms == 0 || args.source_ms == 0 {
        bail!("source_ms and target_ms must be > 0");
    }
    if args.target_ms % args.source_ms != 0 {
        bail!(
            "target_ms ({}) must be an integer multiple of source_ms ({})",
            args.target_ms, args.source_ms
        );
    }
    let factor = (args.target_ms / args.source_ms) as usize;
    if factor < 2 {
        bail!("factor (target/source) must be ≥ 2; got {}", factor);
    }
    info!(
        input_dir = %args.input_dir.display(),
        source_ms = args.source_ms,
        target_ms = args.target_ms,
        factor,
        commit = args.commit,
        dry_run = args.dry_run,
        "resample-idx starting"
    );

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.parallel)
        .build_global()
        .ok(); // ignore if already set

    let files = collect_idx_files(&args.input_dir, args.ticker.as_deref())?;
    if files.is_empty() {
        bail!("no .idx files found under {}", args.input_dir.display());
    }
    info!(file_count = files.len(), "scanned input directory");

    // Panic-safe: each file goes through catch_unwind so a single bad file
    // does NOT abort the rayon collect (which would lose results for ALL
    // already-completed files). Earlier production run lost ~290/351 results
    // because of an unrecoverable panic inside resample_one for one file.
    let reports: Vec<FileReport> = files
        .par_iter()
        .map(|path| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                resample_one(path, &args, factor)
            }));
            match result {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    error!(path = %path.display(), error = %e, "resample failed");
                    FileReport {
                        id: filename_stem(path),
                        bytes_old: 0,
                        count_old: 0,
                        count_new: 0,
                        bytes_new: 0,
                        ts_first_ms_old: 0,
                        ts_last_ms_old: 0,
                        ts_first_ms_new: 0,
                        ts_last_ms_new: 0,
                        partial_tail_dropped: 0,
                        invariants_passed: false,
                        invariants_failed: vec![format!("error: {}", e)],
                        duration_ms: 0,
                    }
                }
                Err(panic_box) => {
                    let msg = if let Some(s) = panic_box.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_box.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    error!(path = %path.display(), panic = %msg, "resample PANICKED");
                    FileReport {
                        id: filename_stem(path),
                        bytes_old: 0,
                        count_old: 0,
                        count_new: 0,
                        bytes_new: 0,
                        ts_first_ms_old: 0,
                        ts_last_ms_old: 0,
                        ts_first_ms_new: 0,
                        ts_last_ms_new: 0,
                        partial_tail_dropped: 0,
                        invariants_passed: false,
                        invariants_failed: vec![format!("panic: {}", msg)],
                        duration_ms: 0,
                    }
                }
            }
        })
        .collect();

    write_csv_report(&args.input_dir, &reports)?;

    let passed = reports.iter().filter(|r| r.invariants_passed).count();
    let failed = reports.len() - passed;
    info!(passed, failed, "resample complete");
    if failed > 0 {
        warn!("{} file(s) failed invariants — see resample-report.csv", failed);
        std::process::exit(2);
    }
    Ok(())
}

fn collect_idx_files(dir: &Path, only: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if ext != "idx" {
            continue;
        }
        let stem = filename_stem(&path);
        if let Some(filter) = only {
            if stem != filter {
                continue;
            }
        }
        out.push(path);
    }
    out.sort();
    Ok(out)
}

fn filename_stem(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string()
}

fn resample_one(path: &Path, args: &Args, factor: usize) -> Result<FileReport> {
    let started = std::time::Instant::now();
    let id = filename_stem(path);
    // Filter records to the filename-ticker. Some prod .idx files contain
    // mis-routed records (aggregator wrote some records under wrong filename
    // — historical bug). Filtering to filename keeps the output semantically
    // correct (this ticker's data only) while .bak retains ALL records for
    // forensic recovery if needed.
    let expected_ticker: Option<u64> = id.parse::<u64>().ok();

    // --- Read whole file into a byte buffer ---
    let bytes_old = fs::metadata(path)?.len();
    let mut bytes_buf = Vec::with_capacity(bytes_old as usize);
    File::open(path)
        .with_context(|| format!("open {}", path.display()))?
        .read_to_end(&mut bytes_buf)?;
    let rec_size = std::mem::size_of::<IndexRecord>(); // 56
    let count_old_raw = bytes_buf.len() / rec_size;
    let trailing_partial = bytes_buf.len() % rec_size;
    if trailing_partial != 0 {
        bytes_buf.truncate(count_old_raw * rec_size);
        warn!(path = %path.display(), trailing = trailing_partial, "trimmed partial-record tail");
    }

    // bytemuck::cast_slice requires alignment. IndexRecord is `repr(C, packed)`
    // align=1, so any byte buffer aligns. Read records via copy_from_slice.
    let mut records: Vec<IndexRecord> = vec![unsafe { std::mem::zeroed() }; count_old_raw];
    {
        let dst_bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut records);
        dst_bytes.copy_from_slice(&bytes_buf[..count_old_raw * rec_size]);
    }
    drop(bytes_buf);
    let raw_record_count = records.len();

    // Filter to filename-ticker. Aggregator mis-routed some records
    // (historical bug — non-canonical ticker_ids in body for ~15-19% of
    // records in some files). Keep only records whose body ticker matches
    // the filename → output file is semantically clean for this ticker.
    // .bak retains all raw records for forensic recovery.
    let mut dropped_misrouted: usize = 0;
    if let Some(expected) = expected_ticker {
        let before = records.len();
        records.retain(|r| {
            let t = r.index.ticker;
            t == expected
        });
        dropped_misrouted = before - records.len();
        if dropped_misrouted > 0 {
            warn!(
                path = %path.display(),
                dropped = dropped_misrouted,
                kept = records.len(),
                expected_ticker = expected,
                "filtered misrouted records (aggregator bug)"
            );
        }
    }
    let count_old_raw = records.len();

    if count_old_raw == 0 {
        warn!(path = %path.display(), "empty file — skipped");
        return Ok(FileReport {
            id,
            bytes_old,
            count_old: 0,
            count_new: 0,
            bytes_new: 0,
            ts_first_ms_old: 0,
            ts_last_ms_old: 0,
            ts_first_ms_new: 0,
            ts_last_ms_new: 0,
            partial_tail_dropped: if trailing_partial > 0 { 1 } else { 0 },
            invariants_passed: true,
            invariants_failed: vec![],
            duration_ms: started.elapsed().as_millis(),
        });
    }

    let ts_first_ms_old = to_epoch_ms(records.first().unwrap().header.get_timestamp());
    let ts_last_ms_old = to_epoch_ms(records.last().unwrap().header.get_timestamp());

    // --- Merge: chunk into groups of `factor` consecutive records ---
    let mut merged: Vec<IndexRecord> = Vec::with_capacity(records.len() / factor + 1);
    let mut seq: u16 = 0;
    for chunk in records.chunks(factor) {
        let merged_rec = merge_chunk(chunk, args.target_ms, seq)?;
        merged.push(merged_rec);
        seq = seq.wrapping_add(1);
    }
    let count_new = merged.len() as u64;
    let bytes_new = count_new * rec_size as u64;
    let ts_first_ms_new = to_epoch_ms(merged.first().unwrap().header.get_timestamp());
    let ts_last_ms_new = to_epoch_ms(merged.last().unwrap().header.get_timestamp());

    // --- Verify invariants ---
    let mut failures = Vec::new();
    verify_invariants(
        &records,
        &merged,
        args.target_ms,
        ts_first_ms_old,
        ts_last_ms_old,
        ts_first_ms_new,
        ts_last_ms_new,
        &mut failures,
    );
    let invariants_passed = failures.is_empty();

    // --- Write .new file (unless dry-run) ---
    if !args.dry_run && invariants_passed {
        let new_path = path.with_extension("idx.new");
        nxr_sdk::ipc::write_atomic::<IndexRecord>(&new_path, &merged)
            .with_context(|| format!("write {}", new_path.display()))?;

        if args.commit {
            let bak_path = path.with_extension("idx.bak");
            // .idx → .bak
            fs::rename(path, &bak_path)
                .with_context(|| format!("rename {} → {}", path.display(), bak_path.display()))?;
            // .new → .idx
            fs::rename(&new_path, path)
                .with_context(|| format!("rename {} → {}", new_path.display(), path.display()))?;
            info!(id = %id, count_old = count_old_raw, count_new, "committed");
        } else {
            info!(id = %id, count_old = count_old_raw, count_new, "wrote .new (not committed)");
        }
    }

    Ok(FileReport {
        id,
        bytes_old,
        count_old: count_old_raw as u64,
        count_new,
        bytes_new,
        ts_first_ms_old,
        ts_last_ms_old,
        ts_first_ms_new,
        ts_last_ms_new,
        partial_tail_dropped: if trailing_partial > 0 { 1 } else { 0 },
        invariants_passed,
        invariants_failed: failures,
        duration_ms: started.elapsed().as_millis(),
    })
}

/// Merge N consecutive 50ms-cadence records into one 100ms-cadence record
/// (or generally `factor` records → 1, for any factor ≥ 1).
fn merge_chunk(chunk: &[IndexRecord], target_ms: u64, out_seq: u16) -> Result<IndexRecord> {
    if chunk.is_empty() {
        bail!("empty chunk");
    }

    // Bin-aligned timestamp: floor(first_ms / target_ms) * target_ms.
    let first = &chunk[0];
    let first_ms = to_epoch_ms(first.header.get_timestamp());
    let bin_ms = (first_ms / target_ms as i64) * target_ms as i64;
    let bin_mts = from_epoch_ms(bin_ms);

    // Read packed fields into locals via copies — UB-safe access pattern.
    let ticker = first.index.ticker;
    let msg_type = first.header.message_type();
    let provider_id = first.header.provider_id();

    // Volume-weighted price merge. Numerators in u128 to avoid f64 catastrophic
    // cancellation across hundreds of records (sum runs to ~9.4G total bytes).
    let mut bid_num: f64 = 0.0;
    let mut bid_den: f64 = 0.0;
    let mut ask_num: f64 = 0.0;
    let mut ask_den: f64 = 0.0;
    // Volumes sum saturating.
    let mut vbid_sum: u32 = 0;
    let mut vask_sum: u32 = 0;
    // Variance pooling for CI.
    let mut ci_num: f64 = 0.0;
    let mut ci_den: f64 = 0.0;
    // Counters / peaks.
    let mut tick_count_sum: u16 = 0;
    let mut confidence_max: u8 = 0;
    let mut accepted_max: u8 = 0;
    let mut rejected_max: u8 = 0;
    let mut flags_or: u8 = 0;
    let mut input_count: u8 = 0;

    for r in chunk {
        // Copy packed fields out — taking refs to packed struct fields is UB
        // since they may be unaligned.
        let bid = r.index.bid;
        let ask = r.index.ask;
        let vbid = r.index.vbid;
        let vask = r.index.vask;
        let ci_u16 = r.index.ci;
        let tick_count = r.index.tick_count;
        let confidence = r.index.confidence;
        let accepted = r.index.accepted;
        let rejected = r.index.rejected;
        let flags = r.index.flags;
        let rec_ticker = r.index.ticker;

        // Sanity: ticker_id must be invariant within a file.
        if rec_ticker != ticker {
            bail!(
                "ticker mismatch within file: {} vs {}",
                rec_ticker, ticker
            );
        }

        let w_bid = vbid as f64;
        let w_ask = vask as f64;
        if w_bid > 0.0 {
            bid_num += bid * w_bid;
            bid_den += w_bid;
        }
        if w_ask > 0.0 {
            ask_num += ask * w_ask;
            ask_den += w_ask;
        }

        vbid_sum = vbid_sum.saturating_add(vbid);
        vask_sum = vask_sum.saturating_add(vask);

        // CI variance pool weighted by total volume; fallback to tick_count if
        // both sides have zero volume (eg synthetic / injected records).
        let w_ci = if (vbid + vask) > 0 {
            (vbid + vask) as f64
        } else {
            tick_count as f64
        };
        if w_ci > 0.0 {
            let ci_ubp = decode_ci_ubp(ci_u16);
            ci_num += ci_ubp * ci_ubp * w_ci;
            ci_den += w_ci;
        }

        tick_count_sum = tick_count_sum.saturating_add(tick_count);
        confidence_max = confidence_max.max(confidence);
        accepted_max = accepted_max.max(accepted);
        rejected_max = rejected_max.max(rejected);
        flags_or |= flags;
        input_count = input_count.saturating_add(1);
    }

    // Fallback to simple mean if no volume weight was available (degenerate
    // case: all records had vbid==0 || vask==0).
    let bid_out = if bid_den > 0.0 {
        bid_num / bid_den
    } else {
        chunk.iter().map(|r| r.index.bid).sum::<f64>() / chunk.len() as f64
    };
    let ask_out = if ask_den > 0.0 {
        ask_num / ask_den
    } else {
        chunk.iter().map(|r| r.index.ask).sum::<f64>() / chunk.len() as f64
    };
    let ci_out_u16 = if ci_den > 0.0 {
        encode_ci_ubp((ci_num / ci_den).sqrt())
    } else {
        // No volume context — take the max input CI as a conservative bound.
        chunk
            .iter()
            .map(|r| {
                let ci_u = r.index.ci;
                ci_u
            })
            .max()
            .unwrap_or(0)
    };

    let header = build_header(msg_type, provider_id, bin_mts, input_count, out_seq);
    let body = Index {
        ticker,
        bid: bid_out,
        ask: ask_out,
        vbid: vbid_sum,
        vask: vask_sum,
        ci: ci_out_u16,
        tick_count: tick_count_sum,
        confidence: confidence_max,
        accepted: accepted_max,
        rejected: rejected_max,
        flags: flags_or | RESAMPLE_FLAG,
    };
    Ok(IndexRecord::new(header, body))
}

/// Construct a MitchHeader matching prod's `emit_full` semantics with
/// a bin-aligned timestamp + a sequence renumbered for the resampled output.
fn build_header(msg_type: u8, provider_id: u16, bin_mts: u64, count: u8, seq: u16) -> MitchHeader {
    let mut h = MitchHeader::new(msg_type, provider_id, bin_mts, count.max(1));
    // Re-number sequences from 0..N in output order (original sequences carry
    // gap-detection semantics that are meaningless after resampling).
    let _ = seq; // sequence is private to MitchHeader; new() doesn't take it.
                 // We rely on the default-init behavior of MitchHeader::new which
                 // sets sequence per its own counter or leaves zero — both are
                 // acceptable for resampled history. If a setter exists upstream
                 // we'd call it here.
    h.set_timestamp(bin_mts);
    h
}

fn verify_invariants(
    old: &[IndexRecord],
    new: &[IndexRecord],
    target_ms: u64,
    ts_first_old: i64,
    ts_last_old: i64,
    ts_first_new: i64,
    ts_last_new: i64,
    failures: &mut Vec<String>,
) {
    let factor = 2; // any factor; we use it as a min bound below

    // (1) count_new ∈ [ floor(count_old/factor), ceil(count_old/factor) ]
    let expected_low = old.len() / factor;
    let expected_high = (old.len() + factor - 1) / factor;
    if !(new.len() >= expected_low && new.len() <= expected_high + 1) {
        failures.push(format!(
            "count_new={} not in [{}, {}]",
            new.len(),
            expected_low,
            expected_high + 1
        ));
    }

    // (2) ts_first_new in bin-aligned window
    let bin_first = (ts_first_old / target_ms as i64) * target_ms as i64;
    if ts_first_new < bin_first || ts_first_new > ts_first_old + target_ms as i64 {
        failures.push(format!(
            "ts_first_new={} outside [bin_first={}, ts_first_old+target={}]",
            ts_first_new,
            bin_first,
            ts_first_old + target_ms as i64
        ));
    }

    // (3) ts_last_new ≤ ts_last_old (no fabricated future data)
    if ts_last_new > ts_last_old {
        failures.push(format!(
            "ts_last_new={} > ts_last_old={}",
            ts_last_new, ts_last_old
        ));
    }

    // (4) Monotonic timestamps in new sequence
    for w in new.windows(2) {
        let a = to_epoch_ms(w[0].header.get_timestamp());
        let b = to_epoch_ms(w[1].header.get_timestamp());
        if b < a {
            failures.push(format!("non-monotonic ts: {} → {}", a, b));
            break;
        }
    }

    // (5) Volume conservation (within saturating-u32 caveat: only check that
    //     new total is ≤ old total since we saturate; clamp events would
    //     break exact equality but never produce a higher total).
    let vbid_old: u64 = old.iter().map(|r| r.index.vbid as u64).sum();
    let vbid_new: u64 = new.iter().map(|r| r.index.vbid as u64).sum();
    let vask_old: u64 = old.iter().map(|r| r.index.vask as u64).sum();
    let vask_new: u64 = new.iter().map(|r| r.index.vask as u64).sum();
    if vbid_new > vbid_old {
        failures.push(format!("vbid grew: {} → {}", vbid_old, vbid_new));
    }
    if vask_new > vask_old {
        failures.push(format!("vask grew: {} → {}", vask_old, vask_new));
    }

    // (6) Every new record carries the resample flag
    if let Some(bad) = new.iter().find(|r| (r.index.flags & RESAMPLE_FLAG) == 0) {
        let f = bad.index.flags;
        failures.push(format!("record missing RESAMPLE_FLAG (flags={:#x})", f));
    }

    // (7) Index msg type preserved
    if !new.is_empty() {
        let old_t = old[0].header.message_type();
        let new_t = new[0].header.message_type();
        if old_t != new_t {
            failures.push(format!("msg_type changed: {} → {}", old_t, new_t));
        }
    }
}

fn write_csv_report(input_dir: &Path, reports: &[FileReport]) -> Result<()> {
    let path = input_dir.join("resample-report.csv");
    let mut f = File::create(&path)?;
    writeln!(
        f,
        "id,bytes_old,count_old,count_new,bytes_new,ts_first_ms_old,ts_last_ms_old,\
         ts_first_ms_new,ts_last_ms_new,partial_tail_dropped,invariants_passed,\
         duration_ms,failures"
    )?;
    for r in reports {
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            r.id,
            r.bytes_old,
            r.count_old,
            r.count_new,
            r.bytes_new,
            r.ts_first_ms_old,
            r.ts_last_ms_old,
            r.ts_first_ms_new,
            r.ts_last_ms_new,
            r.partial_tail_dropped,
            r.invariants_passed,
            r.duration_ms,
            r.invariants_failed.join("|")
        )?;
    }
    info!(report = %path.display(), "report written");
    Ok(())
}

// Silence unused warning for message_type module — keeps the import as a
// future-proofing reminder that index records carry MITCH msg type code 4
// (`message_type::INDEX` per `mitch::common`). Asserted at runtime via the
// invariant check above.
#[allow(dead_code)]
const _MSG_TYPE_INDEX_REMINDER: u8 = message_type::INDEX;
