//! NXR file integrity test binary.
//!
//! Validates on-disk `.idx`, `.bars`, `.vol` files produced by the series
//! factory pipeline. Designed to run both as a CI smoke gate and as a
//! post-pipeline cron assertion (see `nxr-backfill`, `nxr-glue-check`).
//!
//! ## Subcommands
//!
//! ```text
//! integrity-check idx  <path.idx>  [--strict] [--json]
//! integrity-check s10  <path.s10>  [--strict] [--json]
//! integrity-check bars <path.bars> [--strict] [--json]
//! integrity-check vol  <path.vol>  [--strict] [--json]
//! integrity-check dir  <data_root> [--parallel 4] [--report path]
//! ```
//!
//! ## Exit codes
//!
//! - `0` — clean (no errors, no warnings).
//! - `1` — warnings only (or `--strict` not set; warnings non-fatal by default).
//! - `2` — errors present, or `--strict` and any warning.
//!
//! ## Invariants enforced
//!
//! Per file type (see plan §3.2). Briefly:
//!
//! - `.idx` (`IndexRecord`, 56 B): length % 56 == 0; `header.message_type ==
//!   INDEX`; ts non-decreasing; large gaps → WARN; `Index::validate()` passes;
//!   stuck/flatline (G9: bit-identical mid run ≥ STUCK_RUN, or distinct-mid
//!   fraction below floor over a large sample) → WARN/strict-ERROR;
//!   confidence-freshness floor (G4: only when `FLAG_CONF_FRESHNESS` set) →
//!   WARN/strict-ERROR; strict → mean(ci_price/mid) < 100 bps, frac(spread >
//!   500 bps) < 0.5 %.
//! - `.bars` (`mitch::Bar`, 96 B): length % 96 == 0; `open_ts <= close_ts`;
//!   `high >= max(o,c,l)`, `low <= min(o,c,h)`; `kind ∈ {0,1,2,3}`;
//!   `realized_var, bipower_var >= 0`. Renko (`kind == 1`):
//!   `close[i-1] == open[i]`; `|(close-open)/open|` ≥ `min_pct` (no ceiling
//!   post 2026-05-24: adaptive renko has no max_pct cap, see config.yml).
//!   from `config.yml::series.renko`. Kline (`kind == 0`):
//!   `close_ts[i-1] == open_ts[i]`. Strict → bars/day ∈ `[10, 2000]`.
//! - `.vol` (`series_factory::vol_bin::VolRecord`, 14 B): length % 14 == 0;
//!   `mts` strictly increasing; `sigma_pct` ∈ `[0, 0.5]`; no NaN/Inf; max-gap
//!   (> `IDX_MAX_GAP_MS`) → WARN.
//!
//! `.s10` additionally enforces K2 UTC grid alignment (`open_ts % bucket_ms ==
//!   0`) per record → ERROR (the s10-from-idx misalignment regression).
//!
//! ## Internal structure
//!
//! The four file-type validators share an identical skeleton (open mmap →
//! length-multiple guard → `cast_slice` → per-record loop → ts/stat fold →
//! assemble `FileReport`). That skeleton lives once in [`validate_file`]; the
//! genuinely-differing logic (record type, per-record field checks, ts-gap
//! thresholds, stat accumulators, strict post-checks) is implemented per type
//! via the [`RecordValidator`] trait. The `check_*` fns are thin wrappers.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytemuck::{cast_slice, Pod};
use clap::{Parser, Subcommand};
use memmap2::Mmap;
use mitch::common::message_type;
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;
use rayon::prelude::*;
use serde::Serialize;
use series_factory::vol_bin::VolRecord;
use tracing::{info, warn};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(about = "Validate .idx / .bars / .vol files against pipeline invariants.")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Check a single `.idx` file (56B `IndexRecord` rows).
    Idx {
        path: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    /// Check a single `.bars` file (96B `mitch::Bar` rows).
    Bars {
        path: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    /// Check a single `.s10` file (96B `mitch::Bar` rows, kind=Kline, 10s buckets).
    S10 {
        path: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
        /// Bucket size in milliseconds (default 10_000 = 10 s).
        #[arg(long, default_value_t = 10_000)]
        bucket_ms: i64,
    },
    /// Check a single `.vol` file (14B `VolRecord` rows).
    Vol {
        path: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    /// Check a single `.renko` file (alias for `.bars`; 96B renko bricks).
    Renko {
        path: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    /// Recursively check all `.idx`, `.bars`, `.vol` files under a directory.
    Dir {
        path: PathBuf,
        /// Rayon worker count.
        #[arg(long, default_value_t = 4)]
        parallel: usize,
        /// Optional aggregate JSON report path.
        #[arg(long)]
        report: Option<PathBuf>,
        #[arg(long)]
        strict: bool,
    },
}

// ── Findings model ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct Finding {
    /// Record index in the file (or `None` for whole-file findings).
    record_ix: Option<usize>,
    msg: String,
}

#[derive(Debug, Serialize, Default)]
struct Stats {
    ts_first_ms: Option<i64>,
    ts_last_ms: Option<i64>,
    span_hours: Option<f64>,
    /// File-type-specific aggregates.
    #[serde(skip_serializing_if = "Option::is_none")]
    idx_stats: Option<IdxStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bars_stats: Option<BarsStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vol_stats: Option<VolStats>,
}

#[derive(Debug, Serialize, Default)]
struct IdxStats {
    mean_ci_over_mid: f64,
    frac_spread_gt_500_bps: f64,
    mean_spread_bps: f64,
    max_spread_bps: f64,
}

#[derive(Debug, Serialize, Default)]
struct BarsStats {
    bars_per_day: f64,
    n_renko: usize,
    n_kline: usize,
    n_other: usize,
}

#[derive(Debug, Serialize, Default)]
struct VolStats {
    min_sigma_pct: f64,
    max_sigma_pct: f64,
    mean_sigma_pct: f64,
}

#[derive(Debug, Serialize)]
struct FileReport {
    path: String,
    kind: &'static str,
    bytes: u64,
    records: usize,
    errors: Vec<Finding>,
    warnings: Vec<Finding>,
    stats: Stats,
}

impl FileReport {
    fn ok(&self) -> bool { self.errors.is_empty() && self.warnings.is_empty() }
    fn clean(&self) -> bool { self.errors.is_empty() }
}

// ── Optional renko bounds from config.yml ──────────────────────────────────

/// Load `min_pct` from `config.yml`, falling back to `0.0001`.
/// Reads via the canonical [`nxr_sdk::pipeline_config::PipelineYml::load_default`]
/// resolver (single source of truth for config discovery — was a divergent
/// 3-candidate shim until phase 59.R3.C3.O7, 2026-05-30).
///
/// Debate (Aoife ↔ Tomás): post-2026-05-24 max_pct is gone; integrity_check
/// loses its upper-bound enforcement. Aoife: "Replace with anomaly log only."
/// Tomás: "Floor still matters — a brick < min_pct means generator math
/// underflowed and the bar is invalid storage-wise." Consensus: keep floor
/// check, drop ceiling.
fn load_renko_bounds() -> f64 {
    use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
    PipelineYml::load_default(ConfigHint::Bin)
        .map(|yml| yml.series.renko.min_pct as f64)
        .unwrap_or(0.0001)
}

// ── Mmap helper ─────────────────────────────────────────────────────────────

fn open_mmap(path: &Path) -> Result<Mmap> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mmap = unsafe { Mmap::map(&f)? };
    Ok(mmap)
}

// ── Generic validation skeleton ─────────────────────────────────────────────

/// Shared mmap+loop+report skeleton, parameterised over the record type `R` and
/// a `body` closure carrying the per-type logic.
///
/// The driver owns everything identical across the four file types: open the
/// mmap, reject truncated/empty files (with the matching `FileReport`), cast to
/// `&[R]`, drive the per-record loop, fill `span_hours`, and assemble the final
/// `FileReport`. `kind`/`label` are the file-type tag and the human record-type
/// name used in the truncated-file message.
///
/// The per-type accumulator block (prev-ts, running sums, gap counters, …) is an
/// opaque `state: S` owned and threaded by the driver: `body` gets `&mut state`
/// each record, `finish` takes `state` by value once after the loop (after
/// `span_hours` is set) to compute derived stats + strict-mode whole-file checks.
/// Threading the state through the driver (rather than capturing it in the
/// closures) sidesteps the double-`&mut`-borrow that two co-capturing closures
/// would otherwise hit, while still avoiding any per-impl trait boilerplate.
fn validate_file<R, S, Body, Finish>(
    path: &Path,
    kind: &'static str,
    label: &str,
    mut state: S,
    mut body: Body,
    finish: Finish,
) -> Result<FileReport>
where
    R: Pod,
    Body: FnMut(&mut S, usize, &R, &mut Vec<Finding>, &mut Vec<Finding>, &mut Stats),
    Finish: FnOnce(S, usize, &mut Vec<Finding>, &mut Vec<Finding>, &mut Stats),
{
    let mmap = open_mmap(path)?;
    let bytes = mmap.len() as u64;
    let rec_size = std::mem::size_of::<R>();

    let mut errors: Vec<Finding> = Vec::new();
    let mut warnings: Vec<Finding> = Vec::new();
    let mut stats = Stats::default();

    let report = |records, errors, warnings, stats| FileReport {
        path: path.display().to_string(),
        kind,
        bytes,
        records,
        errors,
        warnings,
        stats,
    };

    if mmap.len() % rec_size != 0 {
        errors.push(Finding {
            record_ix: None,
            msg: format!(
                "truncated: file size {} not a multiple of {} ({})",
                mmap.len(),
                rec_size,
                label,
            ),
        });
        return Ok(report(mmap.len() / rec_size, errors, warnings, stats));
    }

    if mmap.is_empty() {
        return Ok(report(0, errors, warnings, stats));
    }

    let records: &[R] = cast_slice(&mmap[..]);
    let n = records.len();

    for (i, rec) in records.iter().enumerate() {
        body(&mut state, i, rec, &mut errors, &mut warnings, &mut stats);
    }

    if let (Some(a), Some(b)) = (stats.ts_first_ms, stats.ts_last_ms) {
        stats.span_hours = Some((b - a) as f64 / (nxr_sdk::shard::MS_PER_HOUR as f64));
    }

    finish(state, n, &mut errors, &mut warnings, &mut stats);

    Ok(report(n, errors, warnings, stats))
}

// ── Shared `mitch::Bar` OHLC core ───────────────────────────────────────────

/// Fields copied out of a packed `mitch::Bar`, shared by the `.bars` and `.s10`
/// validators (both back onto `mitch::Bar`).
struct BarFields {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    open_ts_ms: i64,
    close_ts_ms: i64,
}

/// Run the OHLC sanity block common to `.bars` and `.s10`: copy packed fields,
/// advance `ts_first_ms`/`ts_last_ms`, check `open_ts <= close_ts`, finiteness,
/// `high >= max(o,c,l)`, `low <= min(o,c,h)`, and `realized_var/bipower_var >=
/// 0`. Returns the copied fields plus `ohlc_finite`: when `false`, OHLC was
/// non-finite, the "non-finite OHLC" error was already pushed, and the caller
/// must skip its OHLC-dependent continuity checks (preserving the original
/// early-`continue` behaviour while still letting each caller update its own
/// prev-state).
fn check_bar_ohlc(
    i: usize,
    bar: &mitch::bar::Bar,
    errors: &mut Vec<Finding>,
    stats: &mut Stats,
) -> (BarFields, bool) {
    // Copy fields out of packed struct.
    let f = BarFields {
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        open_ts_ms: bar.open_time_ms(),
        close_ts_ms: bar.close_time_ms(),
    };

    if stats.ts_first_ms.is_none() {
        stats.ts_first_ms = Some(f.open_ts_ms);
    }
    stats.ts_last_ms = Some(f.close_ts_ms);

    if f.open_ts_ms > f.close_ts_ms {
        errors.push(Finding {
            record_ix: Some(i),
            msg: format!("open_ts {} > close_ts {}", f.open_ts_ms, f.close_ts_ms),
        });
    }

    if !f.open.is_finite() || !f.high.is_finite() || !f.low.is_finite() || !f.close.is_finite() {
        errors.push(Finding {
            record_ix: Some(i),
            msg: "non-finite OHLC".into(),
        });
        return (f, false);
    }

    let max_ocl = f.open.max(f.close).max(f.low);
    let min_och = f.open.min(f.close).min(f.high);
    if f.high < max_ocl {
        errors.push(Finding {
            record_ix: Some(i),
            msg: format!("high {} < max(o,c,l) {}", f.high, max_ocl),
        });
    }
    if f.low > min_och {
        errors.push(Finding {
            record_ix: Some(i),
            msg: format!("low {} > min(o,c,h) {}", f.low, min_och),
        });
    }

    let realized_var = bar.realized_var;
    let bipower_var = bar.bipower_var;
    if realized_var < 0.0 {
        errors.push(Finding {
            record_ix: Some(i),
            msg: format!("realized_var {} < 0", realized_var),
        });
    }
    if bipower_var < 0.0 {
        errors.push(Finding {
            record_ix: Some(i),
            msg: format!("bipower_var {} < 0", bipower_var),
        });
    }

    (f, true)
}

// ── .idx check ──────────────────────────────────────────────────────────────

/// Max permitted gap between consecutive records before WARN (60 s).
const IDX_MAX_GAP_MS: i64 = 60_000;

// ── Institutional-grade data-quality constants (G1/G3/G5/G6/G7/G8) ───────────
//
// Design rule (see module head): HARD structural corruption → ERROR always.
// Statistical/heuristic anomalies (jumps, per-record CI) → WARN by default,
// ERROR only under `--strict`, so the cert does NOT false-FAIL on legitimate
// volatile-but-real crypto data.

/// G1 — absolute price floor. Any finite, technically-positive bid/ask below
/// this is treated as structural corruption (e.g. a price that underflowed to a
/// denormal). `Index::validate()` only rejects `<= 0.0`; a value like `1e-15`
/// passes it yet cannot be a real quote. ERROR always.
///
/// Sourced from `series_factory::seam::MIN_PX` so the cert (`data-quality-audit`
/// invariant chain) and this per-file checker share ONE floor — they must never
/// disagree on what a structurally-corrupt price is.
use series_factory::seam::MIN_PX;

/// G6 — lower timestamp bound: 2000-01-01T00:00:00Z in epoch ms. Any record ts
/// below this is a corrupt/garbage clock. Defensive guard: a valid u48 mts
/// decodes to ≥ 2010-01-01 (`mitch::timestamp::from_epoch_ms` floors pre-epoch
/// inputs to 0 → `EPOCH_MS`), so this bound is unreachable through normal
/// encoding — it backstops a non-mts-encoded garbage clock. ERROR.
const EPOCH_2000_MS: i64 = 946_684_800_000;

/// G6 — small future slack (ms) added to "now" before flagging a ts as being
/// from the future. Absorbs benign clock skew between the writer host and this
/// checker (5 min). Beyond this → corrupt clock → ERROR.
const FUTURE_SLACK_MS: i64 = 300_000;

/// G8 — sane upper bound on `Index::accepted` (distinct providers contributing
/// to the composite). `accepted` is a `u8` so it is non-negative and ≤ 255 by
/// type; a value this large is metadata corruption, not a real provider count.
/// ERROR on overrun. (`confidence` is now an independent Q0.8 freshness byte —
/// see `mitch::index` — so no `confidence <= accepted` cross-constraint here.)
const MAX_ACCEPTED_PROVIDERS: u8 = 64;

/// G5 — price-jump heuristic ceiling: `|mid - prev_mid| / prev_mid`. A move of
/// more than this between consecutive records is *flagged* — but real crypto
/// prints genuinely jump this much on thin books, so this is a WARN by default
/// and only an ERROR under `--strict`. 10 %.
const JUMP_PCT: f64 = 0.10;

/// G3 — per-record confidence-interval ceiling as a fraction of mid: a record
/// whose `ci_price > mid * CI_MAX_FRAC` has a degenerate (~useless) interval.
/// Heuristic → WARN by default, ERROR under `--strict`. 1 % of mid.
const CI_MAX_FRAC: f64 = 0.01;

/// G7 — spread-consistency tolerance (bps). `spread_bps()` is derived from
/// `(ask-bid)/mid`, so the cross-check is on the *reconstruction*: recompute
/// `(ask-bid)/mid*1e4` independently and require it match the reported value.
/// A gross mismatch (or a non-finite / negative reconstructed spread that the
/// MITCH `validate()` happened not to catch) signals bid/ask encoding
/// corruption. ERROR on gross mismatch. 0.5 bps absolute slack.
const SPREAD_CONSISTENCY_BPS: f64 = 0.5;

/// G9 — stuck/flatline detector: a run of this many *bit-identical* consecutive
/// mids signals a frozen forwarder (the doc promises "no stuck feeds" but no
/// check enforced it — a feed repeating one price could certify PASS). At the
/// live 10 Hz cadence 600 records ≈ 60 s of an unmoving book. A genuinely
/// quiet-but-moving stable pair re-prints sub-tick noise far more often than
/// once a minute, so only a *true* freeze (identical to the bit) trips this.
/// Heuristic → WARN by default, ERROR under `--strict`.
const STUCK_RUN: usize = 600;

/// G9 — distinct-mid floor: over a *large* sample (≥ `STUCK_MIN_SAMPLE`
/// finite mids) the fraction of distinct mid values must exceed this. A
/// near-degenerate distinct fraction over thousands of records is a partially
/// stuck / quantised feed even when no single run hits `STUCK_RUN`. Kept very
/// conservative (0.2 %) so a legitimately calm pair that still wanders never
/// trips it. Heuristic → WARN by default, ERROR under `--strict`.
const STUCK_MIN_DISTINCT_FRAC: f64 = 0.002;

/// G9 — minimum finite-mid sample before the distinct-fraction gate is allowed
/// to fire. Below this the statistic is too noisy (e.g. a 32-row smoke fixture
/// of one constant quote is legitimate and must never FAIL). 2000 records.
const STUCK_MIN_SAMPLE: usize = 2000;

/// G4 — confidence-freshness floor (Q0.8, ∈[0,1]). When a record carries
/// `FLAG_CONF_FRESHNESS` (bit 3) its `confidence` byte is a Q0.8 freshness
/// `f = byte/255`; an `f` below this floor means the composite is built from
/// stale components. Heuristic (real markets do go briefly stale) → WARN by
/// default, ERROR under `--strict`. When the flag is *clear* the byte is the
/// legacy active-provider count and this gate is skipped entirely. 0.05.
const CONF_FRESHNESS_FLOOR: f64 = 0.05;

/// Per-file accumulators threaded through the `.idx` validator.
#[derive(Default)]
struct IdxState {
    prev_ts_ms: Option<i64>,
    /// G5 — previous record mid for the price-jump heuristic.
    prev_mid: Option<f64>,
    sum_ci_over_mid: f64,
    n_finite: usize,
    n_wide: usize,
    sum_spread_bps: f64,
    max_spread_bps: f64,
    /// G9 — stuck-feed detector. `prev_mid_bits` is the bit pattern of the last
    /// finite mid; `cur_run`/`max_run` track the longest bit-identical run;
    /// `n_distinct_mids` counts distinct finite mid bit patterns (so the
    /// distinct-fraction floor is over the same denominator as `n_finite`).
    prev_mid_bits: Option<u64>,
    cur_identical_run: usize,
    max_identical_run: usize,
    distinct_mids: std::collections::HashSet<u64>,
    /// G9 — count of NON-sentinel finite mids (FIX #7). This is the denominator
    /// for the distinct-mid fraction so it matches `distinct_mids` (which also
    /// excludes sentinels); using the all-records `n_finite` would depress the
    /// fraction on a sentinel-dense feed and falsely trip the stuck-feed gate.
    n_nonsentinel_finite: usize,
    /// G4 — # records that carried `FLAG_CONF_FRESHNESS` with `f` below floor.
    n_stale_conf: usize,
}

fn check_idx(path: &Path, strict: bool) -> Result<FileReport> {
    validate_file::<IndexRecord, _, _, _>(
        path,
        "idx",
        "IndexRecord",
        IdxState::default(),
        |s: &mut IdxState, i, rec, errors, warnings, stats| {
            let mt = rec.header.message_type();
            if mt != message_type::INDEX {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("header.message_type=0x{:02x} != INDEX (0x{:02x})", mt, message_type::INDEX),
                });
            }

            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            if stats.ts_first_ms.is_none() {
                stats.ts_first_ms = Some(ts);
            }
            stats.ts_last_ms = Some(ts);

            // G6 — timestamp epoch sanity: must be within [2000-01-01, now+slack].
            // Out-of-range = corrupt clock = structural corruption → ERROR always.
            let now_ms = nxr_sdk::agg::now_ms() as i64;
            if ts < EPOCH_2000_MS || ts > now_ms + FUTURE_SLACK_MS {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!(
                        "ts {} ms outside sane epoch [{}, now+{}]",
                        ts, EPOCH_2000_MS, FUTURE_SLACK_MS
                    ),
                });
            }

            // Forward inter-row gap (ms) when ts is monotone; None on the first
            // record or a backward ts. FIX #7: the G5 jump guard is suppressed
            // when this gap exceeds IDX_MAX_GAP_MS — a "jump" across a multi-minute
            // outage is the feed resuming at a new fair price, NOT a tick-to-tick
            // print anomaly, so it must not WARN.
            let mut inter_row_gap: Option<i64> = None;
            if let Some(prev) = s.prev_ts_ms {
                if ts < prev {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!("ts non-monotone: {} < prev {}", ts, prev),
                    });
                } else {
                    let gap = ts - prev;
                    inter_row_gap = Some(gap);
                    if gap > IDX_MAX_GAP_MS {
                        warnings.push(Finding {
                            record_ix: Some(i),
                            msg: format!(
                                "gap {} ms between rows {}..{} (>{} ms)",
                                gap,
                                i - 1,
                                i,
                                IDX_MAX_GAP_MS
                            ),
                        });
                    }
                }
            }
            s.prev_ts_ms = Some(ts);

            // Copy out of packed before any field access.
            let idx_body = rec.index;
            // FIX #7: a liveness sentinel re-prints the prior mid by design
            // (FLAG_HEARTBEAT_SENTINEL). Feeding it into the G5 jump-guard or the
            // G9 stuck-run / distinct-fraction accounting would inflate the
            // identical-mid run and depress the distinct fraction → a spurious
            // stuck-feed WARN on a perfectly healthy, sentinel-dense quiet feed.
            // It is skipped in those three accumulators (but still validated for
            // structural invariants G1/G6/G7/G8 above + below).
            let is_sentinel = (idx_body.flags & nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL) != 0;
            let bid = idx_body.bid;
            let ask = idx_body.ask;

            // G1 — absolute price floor (structural corruption → ERROR always).
            // Runs before validate(): validate() only rejects non-finite / <=0,
            // so a finite, technically-positive sub-floor px would slip through.
            let mut floor_bad = false;
            if !bid.is_finite() || bid < MIN_PX {
                floor_bad = true;
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("bid {} below price floor {:e} (or non-finite)", bid, MIN_PX),
                });
            }
            if !ask.is_finite() || ask < MIN_PX {
                floor_bad = true;
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("ask {} below price floor {:e} (or non-finite)", ask, MIN_PX),
                });
            }

            // FIX #8: explicit crossed-quote ERROR (ask < bid) at the invariant
            // level — parity with the audit's invariant chain, which flags it
            // directly. integrity-check previously only caught this transitively
            // via G7 (the reconstructed spread goes negative); surface it as its
            // own structural finding so the message is unambiguous and the check
            // does not depend on the spread-realness path also firing.
            if bid.is_finite() && ask.is_finite() && ask < bid {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("crossed quote: ask {} < bid {} (structural)", ask, bid),
                });
            }

            // G8 — metadata sanity: `accepted` must be a real provider count.
            // u8 ⇒ non-negative by type; an absurdly large value is corruption.
            let accepted = idx_body.accepted;
            if accepted > MAX_ACCEPTED_PROVIDERS {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!(
                        "accepted={} exceeds sane max {} (metadata corruption)",
                        accepted, MAX_ACCEPTED_PROVIDERS
                    ),
                });
            }

            if let Err(e) = idx_body.validate() {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("Index::validate failed: {}", e),
                });
                return;
            }
            // If the floor was breached the body is structurally corrupt; the
            // ERROR is already recorded — skip the derived/heuristic math.
            if floor_bad {
                return;
            }
            let mid = idx_body.mid();
            if mid > 0.0 && mid.is_finite() {
                let spread_bps = idx_body.spread_bps();
                let ci_price = idx_body.ci_price();

                // G7 — spread realness: recompute (ask-bid)/mid*1e4 independently
                // and require it match the reported spread_bps. A gross mismatch,
                // or a non-finite / negative reconstructed spread that validate()
                // missed, indicates bid/ask encoding corruption → ERROR.
                let recomputed_spread_bps = (ask - bid) / mid * 10_000.0;
                if !recomputed_spread_bps.is_finite()
                    || recomputed_spread_bps < 0.0
                    || (recomputed_spread_bps - spread_bps).abs() > SPREAD_CONSISTENCY_BPS
                {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!(
                            "spread inconsistent: (ask-bid)/mid={:.4} bps vs reported {:.4} bps (bid/ask corruption)",
                            recomputed_spread_bps, spread_bps
                        ),
                    });
                }

                // G3 — per-record CI ceiling. Heuristic → WARN, strict → ERROR.
                if ci_price.is_finite() && ci_price > mid * CI_MAX_FRAC {
                    let msg = format!(
                        "ci_price {:.6} > mid*{} = {:.6} (degenerate interval)",
                        ci_price, CI_MAX_FRAC, mid * CI_MAX_FRAC
                    );
                    if strict {
                        errors.push(Finding { record_ix: Some(i), msg });
                    } else {
                        warnings.push(Finding { record_ix: Some(i), msg });
                    }
                }

                // G5 — price-jump guard. Heuristic → WARN, strict → ERROR.
                // FIX #7: skip sentinels (a re-printed prior mid has a ~0 jump
                // anyway, but skipping keeps prev_mid anchored to the last REAL
                // quote) and suppress the check across a gap > IDX_MAX_GAP_MS (a
                // post-outage resume at a new fair price is not a print anomaly).
                if !is_sentinel {
                    if let Some(prev_mid) = s.prev_mid {
                        let gap_ok = inter_row_gap.map(|g| g <= IDX_MAX_GAP_MS).unwrap_or(true);
                        if prev_mid > 0.0 && gap_ok {
                            let jump = (mid - prev_mid).abs() / prev_mid;
                            if jump > JUMP_PCT {
                                let msg = format!(
                                    "mid jump {:.2}% between rows {}..{} (>{:.0}%)",
                                    jump * 100.0,
                                    i.saturating_sub(1),
                                    i,
                                    JUMP_PCT * 100.0
                                );
                                if strict {
                                    errors.push(Finding { record_ix: Some(i), msg });
                                } else {
                                    warnings.push(Finding { record_ix: Some(i), msg });
                                }
                            }
                        }
                    }
                    // Anchor prev_mid to the last REAL quote only — a sentinel must
                    // not become the jump baseline.
                    s.prev_mid = Some(mid);
                }

                // G9 — stuck/flatline run tracking. Compare on the raw bit
                // pattern so only a truly frozen feed (identical to the bit)
                // extends a run; any sub-tick wander resets it. Counted over
                // finite mids only (same denominator as `n_finite`).
                // FIX #7: sentinels are EXCLUDED — a sentinel deliberately
                // re-prints the prior mid, which would extend the identical run
                // and shrink the distinct-mid set, falsely tripping the stuck-feed
                // gate on a healthy sentinel-dense quiet feed.
                if !is_sentinel {
                    let bits = mid.to_bits();
                    s.distinct_mids.insert(bits);
                    s.n_nonsentinel_finite += 1;
                    match s.prev_mid_bits {
                        Some(pb) if pb == bits => s.cur_identical_run += 1,
                        _ => s.cur_identical_run = 1,
                    }
                    if s.cur_identical_run > s.max_identical_run {
                        s.max_identical_run = s.cur_identical_run;
                    }
                    s.prev_mid_bits = Some(bits);
                }

                // G4 — confidence-freshness floor. Only meaningful when the
                // record opts into the Q0.8 freshness wire semantics via
                // `FLAG_CONF_FRESHNESS`; otherwise the byte is the legacy
                // active-provider count and carries no staleness meaning.
                if (idx_body.flags & nxr_sdk::shard::FLAG_CONF_FRESHNESS) != 0 {
                    let f = mitch::index::conf_from_u8(idx_body.confidence);
                    if f < CONF_FRESHNESS_FLOOR {
                        s.n_stale_conf += 1;
                        let msg = format!(
                            "confidence freshness {:.3} < floor {} (stale composite)",
                            f, CONF_FRESHNESS_FLOOR
                        );
                        if strict {
                            errors.push(Finding { record_ix: Some(i), msg });
                        } else {
                            warnings.push(Finding { record_ix: Some(i), msg });
                        }
                    }
                }

                let ratio = ci_price / mid;
                if ratio.is_finite() {
                    s.sum_ci_over_mid += ratio;
                    s.n_finite += 1;
                }
                s.sum_spread_bps += spread_bps;
                if spread_bps > s.max_spread_bps { s.max_spread_bps = spread_bps; }
                if spread_bps > 500.0 { s.n_wide += 1; }
            }
        },
        |s: IdxState, _n, errors, warnings, stats| {
            let mut idx_stats = IdxStats::default();
            if s.n_finite > 0 {
                idx_stats.mean_ci_over_mid = s.sum_ci_over_mid / s.n_finite as f64;
                idx_stats.mean_spread_bps = s.sum_spread_bps / s.n_finite as f64;
                idx_stats.max_spread_bps = s.max_spread_bps;
                idx_stats.frac_spread_gt_500_bps = s.n_wide as f64 / s.n_finite as f64;
            }

            // G9 — stuck/flatline whole-file verdict. Heuristic → WARN by
            // default, ERROR under `--strict`. (a) a single bit-identical run
            // ≥ STUCK_RUN = a sustained freeze; (b) over a large sample, a
            // distinct-mid fraction below the floor = a partially-stuck feed.
            // Conservative thresholds: a calm-but-moving stable pair re-prints
            // sub-tick noise well within both bounds and never trips.
            if s.max_identical_run >= STUCK_RUN {
                let msg = format!(
                    "stuck feed: {} consecutive bit-identical mids (>= {})",
                    s.max_identical_run, STUCK_RUN
                );
                if strict {
                    errors.push(Finding { record_ix: None, msg });
                } else {
                    warnings.push(Finding { record_ix: None, msg });
                }
            }
            // FIX #7: denominator is the NON-sentinel finite count (matches the
            // sentinel-excluded `distinct_mids`), so a sentinel-dense quiet feed
            // is not falsely flagged by an artificially depressed fraction.
            if s.n_nonsentinel_finite >= STUCK_MIN_SAMPLE {
                let distinct_frac =
                    s.distinct_mids.len() as f64 / s.n_nonsentinel_finite as f64;
                if distinct_frac < STUCK_MIN_DISTINCT_FRAC {
                    let msg = format!(
                        "stuck feed: only {} distinct mids over {} non-sentinel finite ({:.4} < {})",
                        s.distinct_mids.len(),
                        s.n_nonsentinel_finite,
                        distinct_frac,
                        STUCK_MIN_DISTINCT_FRAC
                    );
                    if strict {
                        errors.push(Finding { record_ix: None, msg });
                    } else {
                        warnings.push(Finding { record_ix: None, msg });
                    }
                }
            }

            if strict {
                if idx_stats.mean_ci_over_mid >= 0.001 {
                    errors.push(Finding {
                        record_ix: None,
                        msg: format!(
                            "strict: mean(ci/mid)={:.6} >= 0.001 (avg CI > 100 bps)",
                            idx_stats.mean_ci_over_mid
                        ),
                    });
                }
                if idx_stats.frac_spread_gt_500_bps >= 0.005 {
                    errors.push(Finding {
                        record_ix: None,
                        msg: format!(
                            "strict: frac(spread>500bps)={:.4} >= 0.005",
                            idx_stats.frac_spread_gt_500_bps
                        ),
                    });
                }
            }

            stats.idx_stats = Some(idx_stats);
        },
    )
}

// ── .bars check ─────────────────────────────────────────────────────────────

/// Per-file accumulators threaded through the `.bars` validator.
#[derive(Default)]
struct BarsState {
    bars_stats: BarsStats,
    prev_close_ts_ms: Option<i64>,
    prev_close: Option<f64>,
    prev_kind: Option<u8>,
}

fn check_bars(path: &Path, strict: bool) -> Result<FileReport> {
    let renko_min_pct = load_renko_bounds();

    validate_file::<mitch::bar::Bar, _, _, _>(
        path,
        "bars",
        "Bar",
        BarsState::default(),
        |s: &mut BarsState, i, bar, errors, _warnings, stats| {
            let kind = bar.kind;
            match kind {
                0 => s.bars_stats.n_kline += 1,
                1 => s.bars_stats.n_renko += 1,
                2 | 3 => s.bars_stats.n_other += 1,
                _ => {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!("invalid kind={} (expected 0..=3)", kind),
                    });
                }
            }

            // Shared OHLC core (ts span, open<=close, finiteness, high/low, vars).
            let (f, ohlc_finite) = check_bar_ohlc(i, bar, errors, stats);
            let open = f.open;
            let close = f.close;
            let open_ts_ms = f.open_ts_ms;
            let close_ts_ms = f.close_ts_ms;
            if !ohlc_finite {
                s.prev_close = Some(close);
                s.prev_close_ts_ms = Some(close_ts_ms);
                s.prev_kind = Some(kind);
                return;
            }
            // reject_rate is u16 by type, range [0,65535] is guaranteed; no check.

            // Per-kind continuity invariants.
            if let (Some(prev_ts), Some(prev_k)) = (s.prev_close_ts_ms, s.prev_kind) {
                // Kline: close_ts[i-1] == open_ts[i].
                if kind == 0 && prev_k == 0 && prev_ts != open_ts_ms {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!(
                            "kline gap: prev close_ts {} != this open_ts {}",
                            prev_ts, open_ts_ms
                        ),
                    });
                }
            }
            if let (Some(pc), Some(prev_k)) = (s.prev_close, s.prev_kind) {
                // Renko: close[i-1] == open[i] (continuity).
                if kind == 1 && prev_k == 1 && (pc - open).abs() > pc.abs() * 1e-12 + 1e-12 {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!(
                            "renko discontinuity: prev close {} != this open {}",
                            pc, open
                        ),
                    });
                }
            }

            // Renko brick magnitude must be ≥ min_pct (floor only — no ceiling
            // post 2026-05-24: adaptive renko has no max_pct cap).
            if kind == 1 && open > 0.0 {
                let brick = ((close - open) / open).abs();
                if brick < renko_min_pct {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!(
                            "renko brick {:.6} below floor {}",
                            brick, renko_min_pct
                        ),
                    });
                }
            }

            s.prev_close_ts_ms = Some(close_ts_ms);
            s.prev_close = Some(close);
            s.prev_kind = Some(kind);
        },
        |mut s: BarsState, n, errors, _warnings, stats| {
            if let Some(span_h) = stats.span_hours {
                if span_h > 0.0 {
                    s.bars_stats.bars_per_day = n as f64 / (span_h / 24.0);
                }
            }

            if strict && s.bars_stats.bars_per_day > 0.0 {
                if s.bars_stats.bars_per_day < 10.0 || s.bars_stats.bars_per_day > 2000.0 {
                    errors.push(Finding {
                        record_ix: None,
                        msg: format!(
                            "strict: bars/day {:.2} outside [10, 2000]",
                            s.bars_stats.bars_per_day
                        ),
                    });
                }
            }

            stats.bars_stats = Some(s.bars_stats);
        },
    )
}

// ── .s10 check ──────────────────────────────────────────────────────────────

/// Validate a `.s10` file: 96 B `mitch::Bar` rows, all `kind == Kline`,
/// close_ts monotone with `close_ts[i] - close_ts[i-1] == bucket_ms`
/// modulo gap (gap = no-data bucket; reported as WARN, never ERROR).
/// Per-file accumulators threaded through the `.s10` validator.
#[derive(Default)]
struct S10State {
    bars_stats: BarsStats,
    prev_close_ts_ms: Option<i64>,
    n_gaps: usize,
}

fn check_s10(path: &Path, strict: bool, bucket_ms: i64) -> Result<FileReport> {
    validate_file::<mitch::bar::Bar, _, _, _>(
        path,
        "s10",
        "Bar",
        S10State::default(),
        |s: &mut S10State, i, bar, errors, warnings, stats| {
            let kind = bar.kind;
            if kind != mitch::bar::BarKind::Kline as u8 {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!(
                        "s10: kind={} != Kline ({})",
                        kind,
                        mitch::bar::BarKind::Kline as u8
                    ),
                });
            } else {
                s.bars_stats.n_kline += 1;
            }

            // Shared OHLC core (ts span, open<=close, finiteness, high/low, vars).
            let (f, ohlc_finite) = check_bar_ohlc(i, bar, errors, stats);
            let open_ts_ms = f.open_ts_ms;
            let close_ts_ms = f.close_ts_ms;

            // K2 — UTC grid alignment: every s10 bucket's open_ts must sit on the
            // bucket grid (`open_ts % bucket_ms == 0`). A nonzero residue is the
            // s10-from-idx misalignment regression. Previously only
            // `data_quality_audit` caught it; porting it here lets a single-file
            // `integrity-check s10` invocation flag it. Structural → ERROR.
            if bucket_ms > 0 && open_ts_ms % bucket_ms != 0 {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!(
                        "s10 off-grid: open_ts {} % bucket {} = {} (!= 0)",
                        open_ts_ms,
                        bucket_ms,
                        open_ts_ms % bucket_ms
                    ),
                });
            }

            if !ohlc_finite {
                s.prev_close_ts_ms = Some(close_ts_ms);
                return;
            }

            // Sanity bounds on microstructure (warn — not enough to fail).
            let drift = bar.drift;
            let vol_imb = bar.vol_imbalance;
            let spread = bar.avg_spread_bps;
            let max_ret = bar.max_abs_return;
            if drift.is_finite() && drift.abs() > 1.0 {
                warnings.push(Finding {
                    record_ix: Some(i),
                    msg: format!("drift {:.4} |x|>1 (suspicious)", drift),
                });
            }
            if vol_imb.is_finite() && vol_imb.abs() > 1.0 + 1e-3 {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("vol_imbalance {:.4} outside [-1,1]", vol_imb),
                });
            }
            if spread.is_finite() && spread < 0.0 {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("avg_spread_bps {} < 0", spread),
                });
            }
            if max_ret.is_finite() && max_ret < 0.0 {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("max_abs_return {} < 0", max_ret),
                });
            }

            if let Some(prev) = s.prev_close_ts_ms {
                if close_ts_ms < prev {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!("s10 ts non-monotone: {} < prev {}", close_ts_ms, prev),
                    });
                } else {
                    let delta = close_ts_ms - prev;
                    if delta != bucket_ms {
                        // Allow only integer-multiples of bucket_ms (= gap = no-data).
                        if bucket_ms > 0 && delta > 0 && delta % bucket_ms == 0 {
                            s.n_gaps += (delta / bucket_ms - 1) as usize;
                            warnings.push(Finding {
                                record_ix: Some(i),
                                msg: format!(
                                    "s10 gap: {} ms between rows {}..{} ({} missing buckets)",
                                    delta,
                                    i - 1,
                                    i,
                                    delta / bucket_ms - 1,
                                ),
                            });
                        } else {
                            errors.push(Finding {
                                record_ix: Some(i),
                                msg: format!(
                                    "s10 spacing {} ms not a multiple of bucket {} ms",
                                    delta, bucket_ms
                                ),
                            });
                        }
                    }
                }
            }
            s.prev_close_ts_ms = Some(close_ts_ms);
        },
        |mut s: S10State, n, errors, warnings, stats| {
            if let Some(span_h) = stats.span_hours {
                if span_h > 0.0 {
                    s.bars_stats.bars_per_day = n as f64 / (span_h / 24.0);
                }
            }

            // Expected ≈ 8640 bars/day for 10s buckets when no gaps.
            if strict && s.bars_stats.bars_per_day > 0.0 {
                let expected = nxr_sdk::shard::MS_PER_DAY as f64 / bucket_ms as f64;
                let frac = s.bars_stats.bars_per_day / expected;
                if !(0.5..=1.0 + 1e-6).contains(&frac) {
                    errors.push(Finding {
                        record_ix: None,
                        msg: format!(
                            "strict: bars/day {:.1} = {:.1}% of expected {:.1} (gap fraction high)",
                            s.bars_stats.bars_per_day,
                            frac * 100.0,
                            expected
                        ),
                    });
                }
            }
            if s.n_gaps > 0 {
                warnings.push(Finding {
                    record_ix: None,
                    msg: format!("total missing buckets: {}", s.n_gaps),
                });
            }

            stats.bars_stats = Some(s.bars_stats);
        },
    )
}

// ── .vol check ──────────────────────────────────────────────────────────────

/// Absurd-volatility ceiling: a per-bar `sigma_pct` (fraction) at or above this
/// is physically impossible (500 %/bar) and signals structural corruption, not
/// a real-but-extreme print → ERROR always. The tight `[0, 0.5]` band below is
/// the *expected* operating range; this is the hard backstop that must never be
/// crossed even for the most volatile legitimate crypto bar.
const VOL_MAX: f64 = 5.0;

/// Per-file accumulators threaded through the `.vol` validator.
struct VolState {
    vol_stats: VolStats,
    prev_mts: Option<u64>,
    /// Previous record ts in epoch ms, for the max-gap WARN (mirrors `.idx`).
    prev_ts_ms: Option<i64>,
    sum_sigma: f64,
}

impl Default for VolState {
    fn default() -> Self {
        Self {
            vol_stats: VolStats {
                min_sigma_pct: f64::INFINITY,
                max_sigma_pct: f64::NEG_INFINITY,
                mean_sigma_pct: 0.0,
            },
            prev_mts: None,
            prev_ts_ms: None,
            sum_sigma: 0.0,
        }
    }
}

fn check_vol(path: &Path, _strict: bool) -> Result<FileReport> {
    validate_file::<VolRecord, _, _, _>(
        path,
        "vol",
        "VolRecord",
        VolState::default(),
        |s: &mut VolState, i, r, errors, warnings, stats| {
            let mts_bytes = r.mts;
            let mts = timestamp::decode_u48(&mts_bytes);
            let sigma = r.sigma_pct;

            let ts_ms = timestamp::to_epoch_ms(mts);
            if i == 0 {
                stats.ts_first_ms = Some(ts_ms);
            }
            stats.ts_last_ms = Some(ts_ms);

            // G6 — timestamp epoch sanity (mirrors `.idx`): corrupt clock → ERROR.
            let now_ms = nxr_sdk::agg::now_ms() as i64;
            if ts_ms < EPOCH_2000_MS || ts_ms > now_ms + FUTURE_SLACK_MS {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!(
                        "mts {} ms outside sane epoch [{}, now+{}]",
                        ts_ms, EPOCH_2000_MS, FUTURE_SLACK_MS
                    ),
                });
            }

            if let Some(prev) = s.prev_mts {
                if mts <= prev {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!("mts not strictly increasing: {} <= prev {}", mts, prev),
                    });
                }
            }
            s.prev_mts = Some(mts);

            // Max-gap WARN (mirrors `IDX_MAX_GAP_MS`): vol bins should not have
            // unbounded holes. A gap beyond the ceiling is a coverage hole, not
            // structural corruption → WARN (never false-FAILs a real-but-sparse
            // series; strict still promotes it to a fatal exit globally).
            if let Some(prev_ms) = s.prev_ts_ms {
                let gap = ts_ms - prev_ms;
                if gap > IDX_MAX_GAP_MS {
                    warnings.push(Finding {
                        record_ix: Some(i),
                        msg: format!(
                            "vol gap {} ms between rows {}..{} (>{} ms)",
                            gap,
                            i.saturating_sub(1),
                            i,
                            IDX_MAX_GAP_MS
                        ),
                    });
                }
            }
            s.prev_ts_ms = Some(ts_ms);

            if !sigma.is_finite() {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("sigma_pct non-finite ({})", sigma),
                });
                return;
            }
            // Hard structural band: negative or absurdly large (>= VOL_MAX =
            // 500 %/bar) is impossible even for the most volatile real crypto
            // bar → structural corruption → ERROR always.
            if sigma < 0.0 || sigma >= VOL_MAX {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("sigma_pct {} outside sane band [0, {})", sigma, VOL_MAX),
                });
                return;
            }
            // Expected operating range. A value here is finite, non-negative and
            // sub-VOL_MAX but still outside the normal regime → ERROR (kept as
            // the historical tight gate; structural, not heuristic).
            if !(0.0..=0.5).contains(&sigma) {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("sigma_pct {} outside [0, 0.5]", sigma),
                });
            }

            s.sum_sigma += sigma;
            if sigma < s.vol_stats.min_sigma_pct { s.vol_stats.min_sigma_pct = sigma; }
            if sigma > s.vol_stats.max_sigma_pct { s.vol_stats.max_sigma_pct = sigma; }
        },
        |mut s: VolState, n, _errors, _warnings, stats| {
            if n > 0 {
                s.vol_stats.mean_sigma_pct = s.sum_sigma / n as f64;
                if !s.vol_stats.min_sigma_pct.is_finite() { s.vol_stats.min_sigma_pct = 0.0; }
                if !s.vol_stats.max_sigma_pct.is_finite() { s.vol_stats.max_sigma_pct = 0.0; }
            }

            stats.vol_stats = Some(s.vol_stats);
        },
    )
}

// ── Output ──────────────────────────────────────────────────────────────────

fn print_summary(r: &FileReport) {
    println!(
        "{} kind={} records={} bytes={} errors={} warnings={}",
        r.path,
        r.kind,
        r.records,
        r.bytes,
        r.errors.len(),
        r.warnings.len()
    );
    for e in &r.errors {
        eprintln!(
            "ERROR {} [{}]: {}",
            r.path,
            e.record_ix
                .map(|x| x.to_string())
                .unwrap_or_else(|| "*".to_string()),
            e.msg
        );
    }
    for w in &r.warnings {
        eprintln!(
            "WARN  {} [{}]: {}",
            r.path,
            w.record_ix
                .map(|x| x.to_string())
                .unwrap_or_else(|| "*".to_string()),
            w.msg
        );
    }
}

fn emit_report(r: &FileReport, json: bool) {
    if json {
        match serde_json::to_string_pretty(r) {
            Ok(s) => println!("{}", s),
            Err(e) => eprintln!("json serialization failed: {}", e),
        }
    } else {
        print_summary(r);
    }
}

fn exit_code(reports: &[FileReport], strict: bool) -> i32 {
    let any_err = reports.iter().any(|r| !r.clean());
    let any_warn = reports.iter().any(|r| !r.ok());
    if any_err { 2 } else if any_warn { if strict { 2 } else { 1 } } else { 0 }
}

// ── Directory walk ──────────────────────────────────────────────────────────

fn collect_files(root: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let read = match std::fs::read_dir(dir) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %dir.display(), err = %e, "read_dir failed");
                return;
            }
        };
        for entry in read.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, out);
            } else if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
                if matches!(ext, "idx" | "bars" | "s10" | "vol" | "renko") {
                    out.push(p);
                }
            }
        }
    }
    walk(root, &mut out);
    out.sort();
    out
}

#[derive(Debug, Serialize)]
struct AggregateReport {
    root: String,
    files: Vec<FileReport>,
    summary: AggregateSummary,
}

#[derive(Debug, Serialize, Default)]
struct AggregateSummary {
    n_files: usize,
    n_clean: usize,
    n_with_errors: usize,
    n_with_warnings: usize,
    total_records: usize,
    total_bytes: u64,
}

fn check_dir(root: &Path, parallel: usize, strict: bool) -> Result<AggregateReport> {
    let files = collect_files(root);
    info!(root = %root.display(), n = files.len(), "integrity-check dir");

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parallel.max(1))
        .build()
        .with_context(|| "build rayon pool")?;

    let reports: Vec<FileReport> = pool.install(|| {
        files
            .par_iter()
            .map(|p| {
                let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
                let res = match ext {
                    "idx" => check_idx(p, strict),
                    "bars" => check_bars(p, strict),
                    "s10" => check_s10(p, strict, 10_000),
                    "renko" => check_bars(p, strict),
                    "vol" => check_vol(p, strict),
                    other => Err(anyhow::anyhow!("unhandled extension: {}", other)),
                };
                match res {
                    Ok(r) => r,
                    Err(e) => FileReport {
                        path: p.display().to_string(),
                        kind: "error",
                        bytes: 0,
                        records: 0,
                        errors: vec![Finding {
                            record_ix: None,
                            msg: format!("open/check failed: {}", e),
                        }],
                        warnings: vec![],
                        stats: Stats::default(),
                    },
                }
            })
            .collect()
    });

    let mut summary = AggregateSummary::default();
    summary.n_files = reports.len();
    for r in &reports {
        summary.total_records += r.records;
        summary.total_bytes += r.bytes;
        if !r.errors.is_empty() {
            summary.n_with_errors += 1;
        } else if !r.warnings.is_empty() {
            summary.n_with_warnings += 1;
        } else {
            summary.n_clean += 1;
        }
    }

    Ok(AggregateReport {
        root: root.display().to_string(),
        files: reports,
        summary,
    })
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Idx { path, strict, json } => {
            let r = check_idx(&path, strict)?;
            emit_report(&r, json);
            std::process::exit(exit_code(std::slice::from_ref(&r), strict));
        }
        Cmd::Bars { path, strict, json } => {
            let r = check_bars(&path, strict)?;
            emit_report(&r, json);
            std::process::exit(exit_code(std::slice::from_ref(&r), strict));
        }
        Cmd::S10 { path, strict, json, bucket_ms } => {
            let r = check_s10(&path, strict, bucket_ms)?;
            emit_report(&r, json);
            std::process::exit(exit_code(std::slice::from_ref(&r), strict));
        }
        Cmd::Renko { path, strict, json } => {
            let r = check_bars(&path, strict)?;
            emit_report(&r, json);
            std::process::exit(exit_code(std::slice::from_ref(&r), strict));
        }
        Cmd::Vol { path, strict, json } => {
            let r = check_vol(&path, strict)?;
            emit_report(&r, json);
            std::process::exit(exit_code(std::slice::from_ref(&r), strict));
        }
        Cmd::Dir {
            path,
            parallel,
            report,
            strict,
        } => {
            let agg = check_dir(&path, parallel, strict)?;
            for r in &agg.files {
                print_summary(r);
            }
            println!(
                "── summary ── files={} clean={} errors={} warnings={} records={} bytes={}",
                agg.summary.n_files,
                agg.summary.n_clean,
                agg.summary.n_with_errors,
                agg.summary.n_with_warnings,
                agg.summary.total_records,
                agg.summary.total_bytes,
            );
            if let Some(rp) = report {
                let json = serde_json::to_string_pretty(&agg)?;
                std::fs::write(&rp, json)
                    .with_context(|| format!("write report {}", rp.display()))?;
                info!(report = %rp.display(), "aggregate report written");
            }
            std::process::exit(exit_code(&agg.files, strict));
        }
    }
}

// The smoke test binary (`tests/integrity_smoke.rs`) invokes this binary as a
// subprocess via the `CARGO_BIN_EXE_integrity-check` env var. No public Rust
// API is needed.

// ── Unit tests for the institutional-grade ERROR-class checks ────────────────
//
// These craft in-memory `IndexRecord` / `VolRecord` rows, write them to a temp
// file, and call `check_idx` / `check_vol` directly (mirroring the fixture
// style in `tests/integrity_smoke.rs`). They assert ERROR presence/absence for
// the new HARD checks (price floor, ts epoch, accepted bound, spread realness,
// vol band) and WARN-vs-ERROR-vs-strict behaviour for the heuristic checks
// (price jump, per-record CI).
#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::bytes_of;
    use mitch::header::MitchHeader;
    use mitch::index::Index;

    /// Build a valid `IndexRecord` at `epoch_ms` with the given bid/ask.
    fn good_record(epoch_ms: i64, bid: f64, ask: f64) -> IndexRecord {
        let mts = timestamp::from_epoch_ms(epoch_ms);
        let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
        let index = Index::new(
            0xDEAD_BEEF_u64,
            bid, ask, 100, /*ci*/ 1_000, /*vbid*/ 1_000, /*vask*/ 10, /*tick_count*/
            1, /*confidence*/ 1, /*accepted*/ 0, /*rejected*/
        );
        IndexRecord::new(header, index)
    }

    /// Build a `VolRecord` at `epoch_ms` with the given sigma fraction.
    fn vol_record(epoch_ms: i64, sigma_pct: f64) -> VolRecord {
        VolRecord {
            mts: timestamp::encode_u48(timestamp::from_epoch_ms(epoch_ms)),
            sigma_pct,
        }
    }

    fn write_tmp(name: &str, bytes: &[u8]) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "nxr-integrity-unit-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            name,
        ));
        std::fs::write(&p, bytes).expect("write tmp fixture");
        p
    }

    fn write_idx(name: &str, recs: &[IndexRecord]) -> PathBuf {
        let mut buf = Vec::with_capacity(recs.len() * std::mem::size_of::<IndexRecord>());
        for r in recs { buf.extend_from_slice(bytes_of(r)); }
        write_tmp(name, &buf)
    }

    fn write_vol(name: &str, recs: &[VolRecord]) -> PathBuf {
        let mut buf = Vec::with_capacity(recs.len() * std::mem::size_of::<VolRecord>());
        for r in recs { buf.extend_from_slice(bytes_of(r)); }
        write_tmp(name, &buf)
    }

    fn write_bars(name: &str, bars: &[mitch::bar::Bar]) -> PathBuf {
        let mut buf = Vec::with_capacity(bars.len() * std::mem::size_of::<mitch::bar::Bar>());
        for b in bars { buf.extend_from_slice(bytes_of(b)); }
        write_tmp(name, &buf)
    }

    /// Build an s10 Kline bar (`kind == 0`) over `[open_ms, close_ms]`.
    fn s10_bar(open_ms: i64, close_ms: i64, px: f64) -> mitch::bar::Bar {
        let open_mts = timestamp::from_epoch_ms(open_ms);
        let close_mts = timestamp::from_epoch_ms(close_ms);
        let mut b =
            mitch::bar::Bar::new_ohlcv(open_mts, close_mts, px, px, px, px, 0, 0, 1);
        b.kind = mitch::bar::BarKind::Kline as u8;
        b
    }

    /// Build an `IndexRecord` carrying `FLAG_CONF_FRESHNESS` with a given Q0.8
    /// freshness byte (so `check_idx` reads `confidence` as freshness, not a
    /// provider count).
    fn fresh_record(epoch_ms: i64, bid: f64, ask: f64, conf_byte: u8) -> IndexRecord {
        let mts = timestamp::from_epoch_ms(epoch_ms);
        let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
        let mut index = Index::new(0xDEAD_BEEF_u64, bid, ask, 100, 1_000, 1_000, 10, conf_byte, 1, 0);
        index.flags |= nxr_sdk::shard::FLAG_CONF_FRESHNESS;
        IndexRecord::new(header, index)
    }

    /// Build an `IndexRecord` carrying `FLAG_HEARTBEAT_SENTINEL` (a liveness
    /// sentinel that re-prints the prior mid by design). FIX #7 must EXCLUDE these
    /// from the G5 jump + G9 stuck-run / distinct-fraction accounting.
    fn sentinel_record(epoch_ms: i64, bid: f64, ask: f64) -> IndexRecord {
        let mts = timestamp::from_epoch_ms(epoch_ms);
        let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
        let mut index = Index::new(0xDEAD_BEEF_u64, bid, ask, 100, 1_000, 1_000, 10, 1, 1, 0);
        index.flags |= nxr_sdk::shard::FLAG_HEARTBEAT_SENTINEL;
        IndexRecord::new(header, index)
    }

    fn has_err(r: &FileReport, needle: &str) -> bool {
        r.errors.iter().any(|f| f.msg.contains(needle))
    }
    fn has_warn(r: &FileReport, needle: &str) -> bool {
        r.warnings.iter().any(|f| f.msg.contains(needle))
    }

    // A safely-past, in-range epoch for baseline rows (2022-01-01).
    const T0: i64 = 1_640_995_200_000;

    // ── G1 price floor ──────────────────────────────────────────────────────

    #[test]
    fn g1_subfloor_price_is_error() {
        // Finite, technically-positive but below MIN_PX → ERROR (and validate()
        // alone would NOT catch it).
        let rec = good_record(T0, 5e-10, 6e-10);
        let p = write_idx("g1-subfloor.idx", &[rec]);
        let r = check_idx(&p, false).unwrap();
        assert!(has_err(&r, "price floor"), "sub-floor px must ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn g1_normal_price_no_floor_error() {
        let rec = good_record(T0, 100.0, 100.1);
        let p = write_idx("g1-ok.idx", &[rec]);
        let r = check_idx(&p, false).unwrap();
        assert!(!has_err(&r, "price floor"), "normal px must not ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    // ── G6 timestamp epoch sanity ────────────────────────────────────────────

    #[test]
    fn g6_min_mts_does_not_false_fail() {
        // mts=0 decodes to exactly EPOCH_MS (2010-01-01), the smallest ts a u48
        // mts can represent — and it is ABOVE EPOCH_2000_MS, so the lower bound
        // must never false-fail legitimate min-ts data. (The lower bound is a
        // defensive guard against a non-mts-encoded garbage clock; it is
        // unreachable via valid u48 mts, which floors at 2010 — see
        // `mitch::timestamp::from_epoch_ms`.)
        let header = MitchHeader::new(message_type::INDEX, 1, 0, 1);
        let index = Index::new(0xDEAD_BEEF, 100.0, 100.1, 100, 1, 1, 1, 1, 1, 0);
        let rec = IndexRecord::new(header, index);
        let p = write_idx("g6-minmts.idx", &[rec]);
        let r = check_idx(&p, false).unwrap();
        assert!(!has_err(&r, "outside sane epoch"), "min mts must not ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn g6_future_ts_is_error() {
        // now + 1 day → beyond FUTURE_SLACK_MS → ERROR.
        let future_ms = nxr_sdk::agg::now_ms() as i64 + nxr_sdk::shard::MS_PER_DAY;
        let rec = good_record(future_ms, 100.0, 100.1);
        let p = write_idx("g6-future.idx", &[rec]);
        let r = check_idx(&p, false).unwrap();
        assert!(has_err(&r, "outside sane epoch"), "future ts must ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    // ── G8 accepted bound ────────────────────────────────────────────────────

    #[test]
    fn g8_absurd_accepted_is_error() {
        let mts = timestamp::from_epoch_ms(T0);
        let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
        // accepted = MAX+1 → metadata corruption.
        let index = Index::new(
            0xDEAD_BEEF, 100.0, 100.1, 100, 1, 1, 1, 1, MAX_ACCEPTED_PROVIDERS + 1, 0,
        );
        let rec = IndexRecord::new(header, index);
        let p = write_idx("g8-accepted.idx", &[rec]);
        let r = check_idx(&p, false).unwrap();
        assert!(has_err(&r, "exceeds sane max"), "absurd accepted must ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    // ── G5 price-jump guard (heuristic: WARN default, ERROR strict) ───────────

    #[test]
    fn g5_jump_warns_default_errors_strict() {
        // mid 100 → mid 120 = 20% jump (> JUMP_PCT 10%).
        let recs = [
            good_record(T0, 99.95, 100.05),
            good_record(T0 + 1_000, 119.95, 120.05),
        ];
        let p = write_idx("g5-jump.idx", &recs);

        let r = check_idx(&p, false).unwrap();
        assert!(has_warn(&r, "mid jump"), "jump must WARN (non-strict): {:?}", r.warnings);
        assert!(!has_err(&r, "mid jump"), "jump must NOT ERROR (non-strict): {:?}", r.errors);

        let r2 = check_idx(&p, true).unwrap();
        assert!(has_err(&r2, "mid jump"), "jump must ERROR (strict): {:?}", r2.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn g5_small_move_no_jump() {
        let recs = [
            good_record(T0, 100.0, 100.1),
            good_record(T0 + 1_000, 100.5, 100.6), // ~0.5% move < 10%
        ];
        let p = write_idx("g5-nojump.idx", &recs);
        let r = check_idx(&p, false).unwrap();
        assert!(!has_warn(&r, "mid jump"), "small move must not WARN: {:?}", r.warnings);
        std::fs::remove_file(&p).ok();
    }

    // ── G3 per-record CI ceiling (heuristic: WARN default, ERROR strict) ──────

    #[test]
    fn g3_wide_ci_warns_default_errors_strict() {
        // ci wire value chosen large so ci_price > mid * CI_MAX_FRAC (1%).
        let mts = timestamp::from_epoch_ms(T0);
        let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
        let index = Index::new(
            0xDEAD_BEEF, 100.0, 100.1, 60_000, /*ci near u16 sat → very wide*/
            1, 1, 1, 1, 1, 0,
        );
        let rec = IndexRecord::new(header, index);
        let p = write_idx("g3-ci.idx", &[rec]);

        let r = check_idx(&p, false).unwrap();
        assert!(has_warn(&r, "degenerate interval"), "wide CI must WARN: {:?}", r.warnings);

        let r2 = check_idx(&p, true).unwrap();
        assert!(has_err(&r2, "degenerate interval"), "wide CI must ERROR (strict): {:?}", r2.errors);
        std::fs::remove_file(&p).ok();
    }

    // ── .vol band ─────────────────────────────────────────────────────────────

    #[test]
    fn vol_absurd_sigma_is_error() {
        // sigma >= VOL_MAX (5.0) → structural ERROR.
        let recs = [vol_record(T0, VOL_MAX + 1.0)];
        let p = write_vol("vol-absurd.vol", &recs);
        let r = check_vol(&p, false).unwrap();
        assert!(has_err(&r, "sane band"), "absurd sigma must ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn vol_normal_sigma_ok() {
        let recs = [vol_record(T0, 0.02), vol_record(T0 + 1_000, 0.03)];
        let p = write_vol("vol-ok.vol", &recs);
        let r = check_vol(&p, false).unwrap();
        assert!(r.errors.is_empty(), "normal vol must be clean: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    // ── G9 stuck/flatline detector ───────────────────────────────────────────

    #[test]
    fn g9_frozen_feed_warns_default_errors_strict() {
        // STUCK_RUN+1 bit-identical mids at a fixed quote → a frozen forwarder.
        // Spread tiny + finite so no other gate fires; spacing 100 ms (< gap).
        let n = STUCK_RUN + 1;
        let recs: Vec<IndexRecord> = (0..n)
            .map(|i| good_record(T0 + (i as i64) * 100, 100.0, 100.1))
            .collect();
        let p = write_idx("g9-frozen.idx", &recs);

        let r = check_idx(&p, false).unwrap();
        assert!(has_warn(&r, "stuck feed"), "frozen feed must WARN: {:?}", r.warnings);
        assert!(!has_err(&r, "stuck feed"), "frozen feed must NOT ERROR (non-strict): {:?}", r.errors);

        let r2 = check_idx(&p, true).unwrap();
        assert!(has_err(&r2, "stuck feed"), "frozen feed must ERROR (strict): {:?}", r2.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn g9_clean_moving_feed_no_stuck() {
        // STUCK_RUN+50 records, each a distinct mid (tiny but real wander).
        // Must NOT flag stuck under either mode (no false-FAIL on volatile data).
        let n = STUCK_RUN + 50;
        let recs: Vec<IndexRecord> = (0..n)
            .map(|i| {
                let drift = 100.0 + (i as f64) * 0.01; // strictly increasing → all distinct
                good_record(T0 + (i as i64) * 100, drift, drift + 0.1)
            })
            .collect();
        let p = write_idx("g9-moving.idx", &recs);

        let r = check_idx(&p, false).unwrap();
        assert!(!has_warn(&r, "stuck feed"), "moving feed must not WARN: {:?}", r.warnings);
        let r2 = check_idx(&p, true).unwrap();
        assert!(!has_err(&r2, "stuck feed"), "moving feed must not ERROR (strict): {:?}", r2.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn g9_small_constant_sample_no_false_fail() {
        // The 32-row all-identical-quote smoke fixture shape: below STUCK_RUN and
        // below STUCK_MIN_SAMPLE → must never flag stuck (no false-FAIL).
        let recs: Vec<IndexRecord> = (0..32)
            .map(|i| good_record(T0 + (i as i64) * 100, 100.0, 100.1))
            .collect();
        let p = write_idx("g9-smoke-shape.idx", &recs);
        let r = check_idx(&p, true).unwrap();
        assert!(!has_err(&r, "stuck feed"), "small constant sample must not ERROR (strict): {:?}", r.errors);
        assert!(!has_warn(&r, "stuck feed"), "small constant sample must not WARN: {:?}", r.warnings);
        std::fs::remove_file(&p).ok();
    }

    // ── FIX #7: sentinel-dense feed must NOT false-trip the stuck-feed gate ────

    /// A healthy quiet feed where every record between real prints is a liveness
    /// SENTINEL (re-printing the prior mid). The OLD accounting counted sentinels
    /// into the identical-run + distinct-fraction denominators → a guaranteed
    /// spurious stuck-feed WARN. FIX #7 excludes them: a feed with enough DISTINCT
    /// real mids (interleaved with dense sentinels) must NOT flag stuck.
    #[test]
    fn fix7_sentinel_dense_no_false_stuck() {
        // STUCK_MIN_SAMPLE+ real moving prints, each separated by a long run of
        // sentinels re-printing that print's mid. Real mids are all distinct.
        let mut recs: Vec<IndexRecord> = Vec::new();
        let mut t = T0;
        let n_real = STUCK_MIN_SAMPLE + 50;
        for k in 0..n_real {
            let px = 100.0 + (k as f64) * 0.001; // strictly increasing → distinct
            recs.push(good_record(t, px, px + 0.1));
            t += 100;
            // 5 sentinels re-printing the SAME mid between real prints. Under the
            // old logic these 5×N identical sentinels would form a run ≫ STUCK_RUN
            // AND crush the distinct fraction → false stuck.
            for _ in 0..5 {
                recs.push(sentinel_record(t, px, px + 0.1));
                t += 100;
            }
        }
        let p = write_idx("fix7-sentinel-dense.idx", &recs);
        let r = check_idx(&p, true).unwrap(); // strict: would ERROR if it fired
        assert!(
            !has_err(&r, "stuck feed"),
            "sentinel-dense healthy feed must NOT ERROR stuck (strict): {:?}",
            r.errors
        );
        assert!(
            !has_warn(&r, "stuck feed"),
            "sentinel-dense healthy feed must NOT WARN stuck: {:?}",
            r.warnings
        );
        std::fs::remove_file(&p).ok();
    }

    /// A genuinely frozen feed whose ONLY records are sentinels at one quote must
    /// NOT trip the run/distinct gates either (no real mids to judge) — sentinels
    /// are liveness markers, not quote evidence. (Stuck detection on the REAL
    /// stream is unchanged; `g9_frozen_feed_*` still covers a true freeze.)
    #[test]
    fn fix7_all_sentinels_no_stuck_verdict() {
        let n = STUCK_RUN + 10;
        let recs: Vec<IndexRecord> = (0..n)
            .map(|i| sentinel_record(T0 + (i as i64) * 100, 100.0, 100.1))
            .collect();
        let p = write_idx("fix7-all-sentinel.idx", &recs);
        let r = check_idx(&p, true).unwrap();
        assert!(!has_err(&r, "stuck feed"), "all-sentinel run must not ERROR stuck: {:?}", r.errors);
        assert!(!has_warn(&r, "stuck feed"), "all-sentinel run must not WARN stuck: {:?}", r.warnings);
        std::fs::remove_file(&p).ok();
    }

    /// FIX #7 gap-aware jump: a >10% mid move ACROSS an inter-row gap exceeding
    /// IDX_MAX_GAP_MS is a post-outage resume, not a print anomaly → must NOT
    /// WARN. The same move within a tight gap still WARNs (g5 unchanged there).
    #[test]
    fn fix7_jump_across_gap_suppressed() {
        // Row 1 → Row 2 separated by 2× IDX_MAX_GAP_MS with a 20% move.
        let recs = [
            good_record(T0, 99.95, 100.05),
            good_record(T0 + IDX_MAX_GAP_MS * 2, 119.95, 120.05),
        ];
        let p = write_idx("fix7-jump-gap.idx", &recs);
        let r = check_idx(&p, false).unwrap();
        assert!(
            !has_warn(&r, "mid jump"),
            "20% move across a >max-gap outage must NOT WARN jump: {:?}",
            r.warnings
        );
        std::fs::remove_file(&p).ok();
    }

    /// FIX #7: a sentinel must not become the jump baseline. A real print, then a
    /// dense sentinel run at that mid, then a real print 5% away (tight spacing)
    /// → the jump is measured print-to-print (5% < 10%) → no WARN. (Anchoring on a
    /// sentinel would not change the value here, but this proves prev_mid tracks
    /// the last REAL quote.)
    #[test]
    fn fix7_sentinel_not_jump_baseline() {
        let mut recs = vec![good_record(T0, 100.0, 100.1)];
        for k in 1..=5 {
            recs.push(sentinel_record(T0 + k * 100, 100.0, 100.1));
        }
        recs.push(good_record(T0 + 600, 105.0, 105.1)); // 5% from the real 100
        let p = write_idx("fix7-sentinel-baseline.idx", &recs);
        let r = check_idx(&p, false).unwrap();
        assert!(!has_warn(&r, "mid jump"), "5% real-to-real move must not WARN: {:?}", r.warnings);
        std::fs::remove_file(&p).ok();
    }

    // ── FIX #8: explicit crossed-quote ERROR ──────────────────────────────────

    /// `ask < bid` is surfaced as its OWN structural ERROR (parity with the audit
    /// invariant chain), not only transitively via G7's negative reconstructed
    /// spread. The message names "crossed quote".
    #[test]
    fn fix8_crossed_quote_explicit_error() {
        let rec = good_record(T0, 100.5, 100.0); // ask 100.0 < bid 100.5
        let p = write_idx("fix8-crossed.idx", &[rec]);
        let r = check_idx(&p, false).unwrap();
        assert!(
            has_err(&r, "crossed quote"),
            "crossed quote must raise its own structural ERROR: {:?}",
            r.errors
        );
        std::fs::remove_file(&p).ok();
    }

    // ── G4 confidence-freshness floor ─────────────────────────────────────────

    #[test]
    fn g4_stale_freshness_warns_default_errors_strict() {
        // conf byte 0 with FLAG_CONF_FRESHNESS set → f = 0 < floor → flagged.
        let rec = fresh_record(T0, 100.0, 100.1, 0);
        let p = write_idx("g4-stale.idx", &[rec]);

        let r = check_idx(&p, false).unwrap();
        assert!(has_warn(&r, "confidence freshness"), "stale freshness must WARN: {:?}", r.warnings);

        let r2 = check_idx(&p, true).unwrap();
        assert!(has_err(&r2, "confidence freshness"), "stale freshness must ERROR (strict): {:?}", r2.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn g4_fresh_or_legacy_no_false_fail() {
        // (a) flag set + full freshness (byte 255 → f=1.0) → no flag.
        let fresh = fresh_record(T0, 100.0, 100.1, 255);
        let p = write_idx("g4-fresh.idx", &[fresh]);
        let r = check_idx(&p, true).unwrap();
        assert!(!has_err(&r, "confidence freshness"), "full freshness must not ERROR: {:?}", r.errors);
        assert!(!has_warn(&r, "confidence freshness"), "full freshness must not WARN: {:?}", r.warnings);
        std::fs::remove_file(&p).ok();

        // (b) flag CLEAR + low conf byte → legacy provider-count semantics,
        // freshness gate skipped entirely (must not flag).
        let legacy = good_record(T0, 100.0, 100.1); // confidence=1, flag clear
        let p2 = write_idx("g4-legacy.idx", &[legacy]);
        let r2 = check_idx(&p2, true).unwrap();
        assert!(!has_err(&r2, "confidence freshness"), "legacy conf must not ERROR: {:?}", r2.errors);
        assert!(!has_warn(&r2, "confidence freshness"), "legacy conf must not WARN: {:?}", r2.warnings);
        std::fs::remove_file(&p2).ok();
    }

    // ── K2 s10 grid alignment ─────────────────────────────────────────────────

    #[test]
    fn k2_s10_misaligned_open_is_error() {
        // open_ts off the 10s grid (T0 + 3000 → % 10_000 = 3000) → ERROR.
        let off = T0 + 3_000;
        let bar = s10_bar(off, off + 10_000, 100.0);
        let p = write_bars("k2-misaligned.s10", &[bar]);
        let r = check_s10(&p, false, 10_000).unwrap();
        assert!(has_err(&r, "off-grid"), "misaligned s10 open must ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn k2_s10_aligned_open_no_error() {
        // T0 (= ...200_000) is grid-aligned at 10s; two consecutive buckets.
        let b1 = s10_bar(T0, T0 + 10_000, 100.0);
        let b2 = s10_bar(T0 + 10_000, T0 + 20_000, 100.0);
        let p = write_bars("k2-aligned.s10", &[b1, b2]);
        let r = check_s10(&p, false, 10_000).unwrap();
        assert!(!has_err(&r, "off-grid"), "aligned s10 must not ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    // ── .vol gap WARN ─────────────────────────────────────────────────────────

    #[test]
    fn vol_gap_warns() {
        // Two bins > IDX_MAX_GAP_MS (60s) apart → coverage-hole WARN, no ERROR.
        let recs = [vol_record(T0, 0.02), vol_record(T0 + 120_000, 0.03)];
        let p = write_vol("vol-gap.vol", &recs);
        let r = check_vol(&p, false).unwrap();
        assert!(has_warn(&r, "vol gap"), "vol gap must WARN: {:?}", r.warnings);
        assert!(r.errors.is_empty(), "vol gap must not ERROR: {:?}", r.errors);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn vol_no_gap_no_warn() {
        let recs = [vol_record(T0, 0.02), vol_record(T0 + 1_000, 0.03)];
        let p = write_vol("vol-nogap.vol", &recs);
        let r = check_vol(&p, false).unwrap();
        assert!(!has_warn(&r, "vol gap"), "tight spacing must not WARN: {:?}", r.warnings);
        std::fs::remove_file(&p).ok();
    }
}
