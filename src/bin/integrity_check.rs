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
//!   strict → mean(ci_price/mid) < 100 bps, frac(spread > 500 bps) < 0.5 %.
//! - `.bars` (`mitch::Bar`, 96 B): length % 96 == 0; `open_ts <= close_ts`;
//!   `high >= max(o,c,l)`, `low <= min(o,c,h)`; `kind ∈ {0,1,2,3}`;
//!   `realized_var, bipower_var >= 0`. Renko (`kind == 1`):
//!   `close[i-1] == open[i]`; `|(close-open)/open|` ≥ `min_pct` (no ceiling
//!   post 2026-05-24: adaptive renko has no max_pct cap, see config.yml).
//!   from `config.yml::series.renko`. Kline (`kind == 0`):
//!   `close_ts[i-1] == open_ts[i]`. Strict → bars/day ∈ `[10, 2000]`.
//! - `.vol` (`series_factory::vol_bin::VolRecord`, 14 B): length % 14 == 0;
//!   `mts` strictly increasing; `sigma_pct` ∈ `[0, 0.5]`; no NaN/Inf.
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

/// Per-file accumulators threaded through the `.idx` validator.
#[derive(Default)]
struct IdxState {
    prev_ts_ms: Option<i64>,
    sum_ci_over_mid: f64,
    n_finite: usize,
    n_wide: usize,
    sum_spread_bps: f64,
    max_spread_bps: f64,
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

            if let Some(prev) = s.prev_ts_ms {
                if ts < prev {
                    // Demoted err→warn (2026-05-28): merge-idx produces rare
                    // reverse-ts records from cross-provider stream skew (1 in
                    // ~300k = 0.0003% on LTC 2024-05-28). Data is salvageable;
                    // readers can sort. Only --strict promotes to error.
                    warnings.push(Finding {
                        record_ix: Some(i),
                        msg: format!("ts non-monotone: {} < prev {}", ts, prev),
                    });
                } else {
                    let gap = ts - prev;
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

            // Copy out of packed before validate().
            let idx_body = rec.index;
            if let Err(e) = idx_body.validate() {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("Index::validate failed: {}", e),
                });
                return;
            }
            let mid = idx_body.mid();
            if mid > 0.0 && mid.is_finite() {
                let spread_bps = idx_body.spread_bps();
                let ci_price = idx_body.ci_price();
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
        |s: IdxState, _n, errors, _warnings, stats| {
            let mut idx_stats = IdxStats::default();
            if s.n_finite > 0 {
                idx_stats.mean_ci_over_mid = s.sum_ci_over_mid / s.n_finite as f64;
                idx_stats.mean_spread_bps = s.sum_spread_bps / s.n_finite as f64;
                idx_stats.max_spread_bps = s.max_spread_bps;
                idx_stats.frac_spread_gt_500_bps = s.n_wide as f64 / s.n_finite as f64;
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
            let close_ts_ms = f.close_ts_ms;
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

/// Per-file accumulators threaded through the `.vol` validator.
struct VolState {
    vol_stats: VolStats,
    prev_mts: Option<u64>,
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
        |s: &mut VolState, i, r, errors, _warnings, stats| {
            let mts_bytes = r.mts;
            let mts = timestamp::decode_u48(&mts_bytes);
            let sigma = r.sigma_pct;

            if i == 0 {
                stats.ts_first_ms = Some(timestamp::to_epoch_ms(mts));
            }
            stats.ts_last_ms = Some(timestamp::to_epoch_ms(mts));

            if let Some(prev) = s.prev_mts {
                if mts <= prev {
                    errors.push(Finding {
                        record_ix: Some(i),
                        msg: format!("mts not strictly increasing: {} <= prev {}", mts, prev),
                    });
                }
            }
            s.prev_mts = Some(mts);

            if !sigma.is_finite() {
                errors.push(Finding {
                    record_ix: Some(i),
                    msg: format!("sigma_pct non-finite ({})", sigma),
                });
                return;
            }
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
                if matches!(ext, "idx" | "bars" | "s10" | "vol") {
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
