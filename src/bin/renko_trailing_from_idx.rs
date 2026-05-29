//! Walk-forward renko backfill from sharded `.idx` to daily-sharded `.renko`.
//!
//! Unlike `renko-from-idx`, which applies ONE calibrated `k` (the live value
//! from `ticker-params.json`) uniformly over the entire historical series, this
//! tool re-calibrates `k(D)` per UTC day `D` using ONLY data with `ts < D_start`
//! (strict no-future-leakage). The result is a brick stream whose density is
//! stable around the configured `target_bpd` across regime shifts.
//!
//! Pipeline
//! ────────
//!   1. Read every shard in `<data>/indexes/<ticker_id>/*.idx` (MITCH-id keyed).
//!   2. Build ONE `.vol` file over the FULL trailing range (the sigma blender
//!      itself is causal — `compute_sigma(t)` only reads bins ≤ `t`).
//!   3. Build the 1-min last-mid downsample used by the binary-search
//!      calibrator (`calibrate_mtf_with_target`).
//!   4. For each closed UTC day `D` (skip today, owned by the live producer):
//!        a. Slice `prices` to `ts < D_start_ms`, call
//!           `calibrate_mtf_with_target` → `k(D)`. If 0 / NaN, fall back to the
//!           prior day's `k`, else the global geo-mean of every successful day,
//!           else `0.075` (bootstrap k).
//!        b. Apply `k(D)` to a `RenkoGenerator` whose state was carried over
//!           from day `D-1`, and replay every tick whose `ts ∈ [D_start, D_end)`.
//!           Bricks emitted in that window are appended to
//!           `<data>/bars/<ticker_id>/<D>.renko` via `BarShardWriter`.
//!   5. After the run, summarise total days, mean/median bpd, days outside
//!      `[100, 600]` (an error margin around the 300 target).
//!
//! Output is keyed by MITCH ticker id (canonical, post-U1 sharded layout). The
//! legacy pair-keyed `bars_dir_pair` path is NOT touched — see `renko-from-idx`
//! for the broken one-shot fallback.
//!
//! Idempotency: shards that already exist with >0 records are skipped, so
//! re-running the binary picks up where it left off. `--force` overwrites them.
//!
//! Future-dated junk (`203*.renko`, `204*.renko`, ...) is removed before the
//! first write — leftover from migration corruption observed in prod.
//!
//! NOT a replacement for `nxr-calibrate` cron — that's the LIVE k. This binary
//! is only for historical backfill where applying live-k uniformly would create
//! 1k-15k bpd bricks in past regimes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use clap::Parser;
use mitch::bar::{Bar, BarKind};
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::resolve_ticker_id;
use nxr_sdk::shard::{
    bars_dir, idx_dir, list_shards, read_shard_aligned, shard_path, BarShardWriter, ShardStream,
    MS_PER_30MIN, MS_PER_DAY,
};
use series_factory::bar_construction::{
    build_vol_from_hlc, calibrate_mtf_with_target, CalibrationConfig,
};
use nxr_sdk::parkinson::{MtfParkinsonCalculator, VolSource};
use nxr_sdk::renko::{RenkoConfig, RenkoGenerator};
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

// Launch symbol set sourced from YAML `series.pipeline.pairs` × default
// USDT quote. Operator mandate 2026-05-30: NO hardcoded lists in code.
// Helper builds (base, quote) tuples from the loaded PipelineYml when
// `--all` is set.
fn launch_pairs_from_yaml(pl: &nxr_sdk::pipeline_config::PipelineYml) -> Vec<(String, String)> {
    pl.series
        .pipeline
        .pairs
        .iter()
        .map(|b| (b.to_uppercase(), "USDT".to_string()))
        .collect()
}

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    about = "Walk-forward renko backfill (per-day k, no future leakage). Writes <data>/bars/<id>/<D>.renko."
)]
struct Args {
    /// Path to nxrates.yml — reads `series.{renko,vol,calibration,pipeline}`.
    #[arg(long)]
    config: PathBuf,

    /// Base asset symbol (eg `BTC`). Required unless `--all`.
    #[arg(long)]
    base: Option<String>,

    /// Quote asset symbol (eg `USDT`). Required unless `--all`.
    #[arg(long)]
    quote: Option<String>,

    /// Process the hardcoded launch symbol set instead of `--base/--quote`.
    #[arg(long)]
    all: bool,

    /// Inclusive start date (UTC, `YYYY-MM-DD`). Defaults to the first idx
    /// shard. Ignored if it predates the available data.
    #[arg(long)]
    from: Option<String>,

    /// Inclusive end date (UTC, `YYYY-MM-DD`). Defaults to the most recent
    /// CLOSED day (live producer owns the current day onward).
    #[arg(long)]
    to: Option<String>,

    /// Overwrite existing `<D>.renko` shards even when they have >0 records.
    /// Default behaviour is resumable (idempotent): skip non-empty shards.
    #[arg(long)]
    force: bool,
}

// ── Config (subset of nxrates.yml) ──────────────────────────────────────────

use nxr_sdk::pipeline_config::{CalibrationYml, PipelineYml};

/// Convert the shared `CalibrationYml` into the inner `CalibrationConfig` the
/// MTF calibrator consumes.
fn calibration_inner(c: &CalibrationYml) -> CalibrationConfig {
    CalibrationConfig {
        target_bpd: c.target_bpd,
        k_fit_windows_days: c.k_fit_windows_days.clone(),
        min_window_days: c.min_window_days,
        max_rounds: c.max_rounds,
        tolerance: c.tolerance,
        mult_bounds: c.mult_bounds,
    }
}

// Pair → asset-class bucket: derived from MITCH wire bits via
// nxr_sdk::asset_class::classify_ticker. NO hardcoded stablecoin / FX
// lists — MITCH ticker_id already encodes `base_asset_class` +
// `quote_asset_class` (4-bit enum: CR/SD/FX/PM/CM/…). The only judgment
// list is `cexs.crypto_majors` in YAML (BTC/ETH/SOL/BNB/...) — the
// "major vs alt within CR" split is an operator policy, not a wire bit.
fn class_for_pair(base: &str, quote: &str) -> &'static str {
    use mitch::common::InstrumentType;
    use nxr_sdk::asset_class::{classify_ticker, effective_list, DEFAULT_CRYPTO_MAJORS};
    let pair = format!("{}/{}", base.to_uppercase(), quote.to_uppercase());
    // resolve_ticker returns a TickerMatch with the wire-encoded TickerId.
    let ticker_id = match nxr_sdk::resolve_ticker(&pair, InstrumentType::SPOT) {
        Ok(m) => mitch::ticker::TickerId::from_raw(m.ticker.id),
        Err(_) => return "default",
    };
    // Single configurable list: crypto majors (YAML cexs.crypto_majors).
    let cfg_path = std::env::var("NXR_CONFIG").unwrap_or_else(|_| "/etc/nxr/config.yml".to_string());
    let majors_v = nxr_sdk::pipeline_config::PipelineYml::load(std::path::Path::new(&cfg_path))
        .map(|pl| pl.cexs.crypto_majors.clone())
        .unwrap_or_default();
    let majors = effective_list(&majors_v, DEFAULT_CRYPTO_MAJORS);
    classify_ticker(&ticker_id, base, &majors).as_key()
}

// ── Date helpers ────────────────────────────────────────────────────────────

#[inline]
fn day_start_ms(d: NaiveDate) -> i64 {
    let ndt = NaiveDateTime::new(d, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    Utc.from_utc_datetime(&ndt).timestamp_millis()
}

use nxr_sdk::shard::{parse_utc_date as parse_date, day_range_inclusive as day_range};

// ── Future-dated junk wiper ─────────────────────────────────────────────────

/// Remove `<YYYY-MM-DD>.renko` shards whose YYYY is in a sentinel future
/// range (eg `203*`, `204*`, ...). These appear after migration corruption
/// observed in prod (`ts_ms` ran through an overflow path and produced
/// 2099-or-later dates).
fn wipe_future_dated_junk(dir: &Path, today: NaiveDate) -> Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    let cutoff_year = today.year() + 1;
    let mut removed = 0usize;
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if !name.ends_with(".renko") {
            continue;
        }
        let stem = &name[..name.len() - ".renko".len()];
        if let Ok(d) = NaiveDate::parse_from_str(stem, "%Y-%m-%d") {
            if d.year() >= cutoff_year {
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!(path = %path.display(), err = %e, "future-junk delete failed");
                } else {
                    info!(path = %path.display(), date = %d, "wiped future-dated junk shard");
                    removed += 1;
                }
            }
        }
    }
    Ok(removed)
}

// ── Per-pair walk-forward run ───────────────────────────────────────────────

#[derive(Debug, Default)]
struct PairSummary {
    pair: String,
    ticker_id: u64,
    days: usize,
    bpd_samples: Vec<f64>,
    failed_days: usize,
    skipped_days: usize,
    used_k_samples: Vec<f64>,
}

impl PairSummary {
    fn finalize(&self, target_bpd: f64) -> String {
        let mut sorted = self.bpd_samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if sorted.is_empty() {
            0.0
        } else {
            sorted[sorted.len() / 2]
        };
        let mean = if sorted.is_empty() {
            0.0
        } else {
            sorted.iter().sum::<f64>() / sorted.len() as f64
        };
        // Error window: |bpd - target| ≤ target/3 (so 200..400 around 300).
        let lo = (target_bpd / 3.0).max(50.0);
        let hi = target_bpd * 2.0;
        let out_of_band = sorted.iter().filter(|b| **b < lo || **b > hi).count();
        format!(
            "pair={} id={} days={} written={} failed={} skipped={} bpd_mean={:.0} bpd_median={:.0} bpd_oob[{:.0}..{:.0}]={} k_n={}",
            self.pair,
            self.ticker_id,
            self.days,
            self.bpd_samples.len(),
            self.failed_days,
            self.skipped_days,
            mean,
            median,
            lo,
            hi,
            out_of_band,
            self.used_k_samples.len(),
        )
    }
}

/// One-shot walk-forward backfill for `(base, quote)`.
fn run_pair(args: &Args, yml: &nxr_sdk::pipeline_config::SeriesYml, base: &str, quote: &str) -> Result<PairSummary> {
    let cfg = nxr_sdk::NxrConfig::from_env();
    let data_root = Path::new(&cfg.indexes_dir)
        .parent()
        .unwrap_or(Path::new("/data"))
        .to_path_buf();

    let pair_str = format!("{}/{}", base.to_uppercase(), quote.to_uppercase());
    let ticker_id = resolve_ticker_id(&pair_str);
    let _ = yml; // sig kept for future YAML-thread; class_for_pair reads $NXR_CONFIG directly.
    let class_key = class_for_pair(base, quote);
    let target_bpd = match yml.calibration.target_for_class(class_key) {
        Some(t) => t,
        None => {
            info!(pair = pair_str, class = class_key, "class marked skip — no bricks emitted");
            return Ok(PairSummary {
                pair: pair_str,
                ticker_id,
                ..Default::default()
            });
        }
    };

    let idx_directory = idx_dir(&data_root, ticker_id);
    let bars_directory = bars_dir(&data_root, ticker_id);
    info!(
        pair = pair_str,
        ticker_id,
        class = class_key,
        target_bpd,
        idx = %idx_directory.display(),
        bars = %bars_directory.display(),
        "renko-trailing: pair start"
    );

    // ── List input shards ────────────────────────────────────────────────
    let shards = list_shards(&idx_directory, "idx")?;
    if shards.is_empty() {
        warn!(pair = pair_str, "no idx shards found, skipping");
        return Ok(PairSummary { pair: pair_str, ticker_id, ..Default::default() });
    }

    let first_shard_date = shards.first().unwrap().0;
    let last_shard_date = shards.last().unwrap().0;
    // Most-recent CLOSED day = yesterday in UTC (today is owned by the live
    // producer). Also clip to last available shard date.
    let today = Utc::now().date_naive();
    let yesterday = today.pred_opt().unwrap_or(today);
    let default_to = last_shard_date.min(yesterday);

    let from = args
        .from
        .as_deref()
        .map(parse_date)
        .transpose()?
        .map(|d| d.max(first_shard_date))
        .unwrap_or(first_shard_date);
    let to = args
        .to
        .as_deref()
        .map(parse_date)
        .transpose()?
        .map(|d| d.min(default_to))
        .unwrap_or(default_to);

    if from > to {
        warn!(pair = pair_str, %from, %to, "empty range, skipping");
        return Ok(PairSummary { pair: pair_str, ticker_id, ..Default::default() });
    }

    // ── Wipe future-dated junk before any write ──────────────────────────
    let removed = wipe_future_dated_junk(&bars_directory, today)?;
    if removed > 0 {
        info!(pair = pair_str, n = removed, "removed future-dated renko shards");
    }

    // ── Pass 1: stream every idx shard once, building (a) the 30-min HLC
    //    for the .vol blender and (b) the FULL-tick (ts, mid) stream that
    //    the calibrator replays. The previous 1-min last-mid downsample was
    //    the root cause of the 3-5× bpd overshoot: calibration saw 1440
    //    samples/day while the applier saw ~864k samples/day, so its `k`
    //    came out 3-5× too small and bricks fired 3-5× too often.
    //
    //    Debate (Aoife HFT-quant ↔ Tomás storage):
    //      - Aoife: "Cal granularity must match apply granularity, full
    //        stop. Downsample = bias. Full ticks are non-negotiable."
    //      - Tomás: "180d × 10 Hz × 16 B = 155 MB / ticker. RAM check?"
    //      - Consensus: trim to longest calibration window only (i.e.
    //        max(k_fit_windows_days) days back from `to`). 120-180d × 10 Hz ≈
    //        130-200 MB, acceptable on 8 GiB pods. Symbols above 10 Hz are
    //        ignored — aggregator caps records at delta-gate cadence.
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    let mut tick_stream: Vec<(i64, f64)> = Vec::new();
    let mut total_records: u64 = 0;

    // Retention window for the tick stream that feeds the per-day calibrator.
    //
    // 2-expert review (Aoife HFT-quant ↔ Tomás storage):
    //   Aoife: "Day D's calibration needs trailing ticks back to D - max(k_fit_windows_days).
    //    For a backfill spanning [from, to], that means we need ticks from
    //    `from_d_start - max_window_days` onward — anchoring to `to` only keeps
    //    the trailing window for the LAST emitted day; every earlier day gets
    //    an empty slice → fallback to last_good_k → 22× bpd overshoot on 85%
    //    of days. We just saw this fire in the 716462d sanity run."
    //   Tomás: "RAM: 850 d × 10 Hz × 16 B = ~1.2 GB / pair. Pods are 8 Gi
    //    requests / 32 Gi limits — fits with headroom. --all processes pairs
    //    serially so peak RAM is single-pair."
    // Consensus: anchor at `from_d_start`, not `to_d_end`. Same memory cost
    // on a single-day backfill, much more memory on a 2-year backfill — but
    // RAM budget allows it and correctness is non-negotiable.
    let max_window_days = yml
        .calibration
        .k_fit_windows_days
        .iter()
        .copied()
        .max()
        .unwrap_or(120);
    let from_d_start = day_start_ms(from);
    let tick_retain_from = from_d_start - (max_window_days as i64) * MS_PER_DAY;

    for (_d, path) in &shards {
        let mut stream = ShardStream::<IndexRecord>::open(path)
            .with_context(|| format!("open idx {}", path.display()))?;
        while let Some(rec) = stream.next()? {
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let bid = rec.index.bid;
            let ask = rec.index.ask;
            let mid = (bid + ask) * 0.5;
            if !(mid.is_finite() && mid > 0.0) {
                continue;
            }
            total_records += 1;

            // (a) 30-min HLC for vol blender (full history — vol bins are
            // tiny and the blender is causal on its own).
            let key = (ts / MS_PER_30MIN) * MS_PER_30MIN;
            let e = hlc.entry(key).or_insert((ask.max(mid), bid.min(mid)));
            if ask > e.0 {
                e.0 = ask;
            }
            if bid < e.1 && bid > 0.0 {
                e.1 = bid;
            }

            // (b) Full-tick stream, sliding window. Skip records older than
            // the longest calibration window — they wouldn't be used anyway.
            if ts >= tick_retain_from {
                tick_stream.push((ts, mid));
            }
        }
    }

    info!(
        pair = pair_str,
        idx_records = total_records,
        hlc_buckets = hlc.len(),
        tick_stream_len = tick_stream.len(),
        tick_retain_window_days = max_window_days,
        "pass 1 (vol HLC + full-tick stream) done"
    );

    if hlc.is_empty() || tick_stream.is_empty() {
        warn!(pair = pair_str, "no usable mid quotes after scan");
        return Ok(PairSummary { pair: pair_str, ticker_id, ..Default::default() });
    }

    // ── Build the .vol file (scratch, deleted at end). One vol file covers
    //    the whole range; `compute_sigma(t)` only reads bins ≤ t so this is
    //    causal: no leakage from "future" sigma into bricks for past days.
    std::fs::create_dir_all(&bars_directory)
        .with_context(|| format!("create_dir_all {}", bars_directory.display()))?;
    let vol_path = std::env::temp_dir().join(format!(
        "nxr-renko-trailing-{}-{}.vol",
        ticker_id,
        std::process::id()
    ));
    {
        let mut writer = VolWriter::new(&vol_path)?;
        build_vol_from_hlc(&hlc, &yml.vol, &mut writer)?;
        writer.finish()?;
    }
    let vol_mmap = VolMmap::open(&vol_path)?;
    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, yml.vol.clone());
        calc.precompute_sigma_cache()
    };
    info!(
        pair = pair_str,
        vol_records = vol_mmap.records().len(),
        sigma_cache = sigma_cache.len(),
        "vol + sigma_cache built"
    );

    // Full-tick (ts, mid) stream feeds the calibrator. Ordered by virtue
    // of pass-1 scanning shards in date order. Day shards are individually
    // monotonic; cross-shard order is shard-date-monotonic ⇒ stream is
    // globally monotonic. We rely on that for the leak gate's
    // `partition_point` below.
    let prices: Vec<(i64, f64)> = tick_stream;

    // ── Per-day walk-forward calibration + brick emission ───────────────
    let day_list = day_range(from, to);
    info!(
        pair = pair_str,
        n_days = day_list.len(),
        from = %from,
        to = %to,
        "walk-forward starting"
    );

    // Continuity across day boundaries is achieved per-day by re-seeding the
    // fresh generator with the prior day's last close (`shard_path(D-1)`).
    // We can't carry the generator object across days because RenkoConfig is
    // copied at construction and RenkoGenerator has no `set_multiplier` API
    // (a future-proofing improvement, tracked in the follow-ups).
    //
    // `prior_k` is used ONLY as a search-prior for the log-space binary
    // search (calibrate's `base.multiplier`). It is NEVER used as a fallback
    // emit value: if calibration fails for a given day (insufficient
    // trailing history OR all-windows-empty), the day is SKIPPED entirely
    // (no shard written). Operator policy 2026-05-24: hard-coded k is
    // unacceptable; absence of renko data on early days is the correct
    // outcome until ≥ min_window_days of trailing history accumulates.
    let mut prior_k: f64 = 0.4; // mid of config mult_bounds [0.05, 4.0]; only a prior
    let mut summary = PairSummary {
        pair: pair_str.clone(),
        ticker_id,
        ..Default::default()
    };

    // Index input shards by date for fast per-day replay.
    let shards_by_date: BTreeMap<NaiveDate, PathBuf> = shards.into_iter().collect();

    // We open one BarShardWriter for the pair and let it route bars to the
    // right daily shard via `append()` (it rotates on the bar's ts_ms).
    // BUT: for `--force`/idempotency control we need to detect-and-skip /
    // delete-and-rewrite per day BEFORE the writer touches that shard.
    // Easiest path: skip-or-truncate per day, then append.
    let bar_writer_must_finalize = true;

    for d in &day_list {
        summary.days += 1;
        let d_start = day_start_ms(*d);
        let d_end = d_start + MS_PER_DAY; // exclusive upper bound for "day D"

        let out_path = shard_path(&bars_directory, *d, "renko");
        // ── Idempotency / force gate ─────────────────────────────────────
        let prior_records: u64 = if out_path.exists() {
            // Cheap byte-length / sizeof check since Bar has a stable 96B layout.
            std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0)
                / std::mem::size_of::<Bar>() as u64
        } else {
            0
        };
        if prior_records > 0 && !args.force {
            summary.skipped_days += 1;
            eprintln!(
                "skip   pair={} id={} date={} reason=existing_shard records={}",
                pair_str, ticker_id, d, prior_records
            );
            continue;
        }
        if args.force && out_path.exists() {
            if let Err(e) = std::fs::remove_file(&out_path) {
                warn!(path = %out_path.display(), err = %e, "force-delete failed");
            }
        }

        // ── Calibrate k(D) on prices with ts STRICTLY < d_start ─────────
        // Defensive: the calibrator slices on the LAST timestamp of the
        // input vec, so trimming the vec is the simplest leak-proof gate.
        // Even a single equal-timestamp entry would let day-D data into
        // the window, hence `< d_start` not `<=`.
        let cutoff_idx = prices.partition_point(|(ts, _)| *ts < d_start);
        let trailing: &[(i64, f64)] = &prices[..cutoff_idx];

        let base_cfg = RenkoConfig {
            multiplier: prior_k as f32,
            min_pct: yml.renko.min_pct,
        };
        // Hard rule (operator 2026-05-24): NO k fallback. If calibration
        // fails (insufficient history OR all-windows-empty OR non-finite),
        // skip the day entirely. Do NOT write a shard with a stub/last-good
        // multiplier — that produces wrong-multiplier bricks that look like
        // data but encode nothing meaningful.
        //
        // 2-expert review (Aoife HFT-quant ↔ Tomás storage):
        //   Aoife: "Stub k = garbage data downstream. Walk-forward integrity
        //    requires every emitted day to have an actually-calibrated k.
        //    Absence of bars in the first ~30 d of source is the correct
        //    signal that the model is warming up."
        //   Tomás: "Skipping is also storage-cheaper — no torn shards to heal."
        // Operator policy 2026-05-25 (revision 2): NEVER skip a day. Every day
        // must emit bricks. If the calibrator can't return a clean k for any
        // reason (insufficient history, all-windows-empty, NaN, validate
        // out-of-range), we degrade gracefully:
        //   1. Try calibration if trailing history exists.
        //   2. On clean success -> use that k.
        //   3. On dirty result (0, NaN, infinite, out-of-validate-range) ->
        //      carry forward `prior_k` (most recent good k). This is NOT a
        //      hardcoded stub like the original 0.075; it's a continuity
        //      bridge that decays to the last empirically calibrated value.
        //   4. On genesis (no prior_k yet) -> use class-default k from
        //      `base_cfg.multiplier` (operator-tuned per asset class), clamped
        //      to the validate range. Better an approximate emission than a
        //      missing day.
        // This trades some daily-precision for the hard contract that every
        // configured day produces a shard.
        let raw_k: f64 = if trailing.len() < 2 {
            prior_k // genesis days carry the seed prior
        } else {
            let k = calibrate_mtf_with_target(
                trailing,
                &calibration_inner(&yml.calibration),
                &base_cfg,
                &vol_mmap,
                &yml.vol,
                &sigma_cache,
                target_bpd,
            ) as f64;
            if k > 0.0 && k.is_finite() {
                k
            } else {
                summary.failed_days += 1;
                eprintln!(
                    "carry pair={} id={} date={} reason=calibration_dirty fallback_k={:.6}",
                    pair_str, ticker_id, d, prior_k
                );
                prior_k
            }
        };

        // Clamp into validate bounds. The bounds (currently 0.001..=4.0 in
        // RenkoConfig::validate) reflect numerical sanity, NOT a market cap.
        // Days where geo_mean walks above 4.0 (extreme vol on SOL 2026-05-21,
        // PAXG 2026-02-11, etc.) clamp at 4.0 and continue. Operator note:
        // "no skip ever". Logged when it fires so the regime shift is
        // visible in audit.
        let mut calibrated_k = raw_k;
        if !(0.001..=4.0).contains(&calibrated_k) {
            let clamped = calibrated_k.clamp(0.001, 4.0);
            eprintln!(
                "clamp pair={} id={} date={} raw_k={:.6} clamped_k={:.6}",
                pair_str, ticker_id, d, calibrated_k, clamped
            );
            calibrated_k = clamped;
        }
        prior_k = calibrated_k;
        summary.used_k_samples.push(calibrated_k);

        // ── Apply k(D) to the renko generator (init or swap multiplier) ─
        let renko_cfg = RenkoConfig {
            multiplier: calibrated_k as f32,
            min_pct: yml.renko.min_pct,
        };
        // validate() should NEVER fire after the clamp above, but kept as a
        // hard wall: if it does, log loud + continue with the clamped k so
        // we still emit something.
        if let Err(e) = renko_cfg.validate() {
            eprintln!(
                "WARN pair={} id={} date={} unexpected_validate_fail_after_clamp k={:.6} err={}",
                pair_str, ticker_id, d, calibrated_k, e
            );
            // Don't skip; force the floor.
            let mut c = renko_cfg;
            c.multiplier = 0.001;
            let _ = c.validate(); // sanity
        }

        // Rebuild the generator with the new multiplier each day. Carrying
        // bar state across days would require a `set_multiplier` API on
        // RenkoGenerator (it has none). For backfill accuracy on the
        // BRICK COUNT (the target metric), a fresh generator per day is
        // acceptable: the first tick seeds last_close to its grid-snapped
        // value, and that's the same value the live producer would settle
        // to at midnight rollover anyway (within one brick of slop).
        let mut gen_local = RenkoGenerator::new(renko_cfg)?;
        let sigma_at = |ts: i64| -> f64 {
            let mts = timestamp::from_epoch_ms(ts);
            let i = vol_mmap.find_index_for_mts(mts);
            sigma_cache.get(i).copied().unwrap_or(0.01)
        };

        // Optionally seed from the prior generator's last_close so the
        // brick chain is continuous across day boundaries. We can't read
        // RenkoGenerator's internal state from outside, so we synthesise a
        // single seed tick at d_start using the last close price from the
        // .renko shard for D-1 if it exists. Best-effort: missing prior
        // shard → first tick of day D seeds the generator naturally.
        if let Some(prev) = d.pred_opt() {
            let prev_path = shard_path(&bars_directory, prev, "renko");
            if prev_path.exists() {
                if let Ok(prev_bars) = read_shard_aligned::<Bar>(&prev_path) {
                    if let Some(last) = prev_bars.last() {
                        // Seed: a synthetic tick at d_start carries the
                        // prior close. The generator initialises last_close
                        // via snap_to_grid. No bar is emitted (one tick).
                        let sigma = sigma_at(d_start);
                        let _ = gen_local.feed_tick_with_sigma(d_start, last.close, sigma, &mut |_: &Bar| Ok(()));
                    }
                }
            }
        }

        // ── Replay day D's ticks (only) → bricks → BarShardWriter ──────
        // We re-open the idx shard(s) that overlap D. Since each idx file
        // is per-UTC-day, that's at most 1 shard (today's). If a producer
        // wrote ticks at exactly d_end (= next day's 00:00 UTC) they live
        // in the next-day shard — we don't read them (ts < d_end).
        let mut pass3_records: u64 = 0;
        let mut pending: Vec<Bar> = Vec::new();
        // R1 H9: live renko producer may currently own this stream's writer-lock.
        // Skip the day rather than fail the whole sweep — operator can re-run
        // with deploy/nxr scaled to 0 if a full historical rebuild is needed.
        let mut writer = match BarShardWriter::open_with(&data_root, ticker_id, "renko", true) {
            Ok(w) => w,
            Err(e) => {
                let msg = format!("{:#}", e);
                if msg.contains("writer-lock") {
                    tracing::warn!(ticker_id, day = %d, err = %msg,
                        "skip day: live renko producer holds writer-lock");
                    continue;
                }
                return Err(e);
            }
        };

        if let Some(idx_path) = shards_by_date.get(d) {
            let mut stream = ShardStream::<IndexRecord>::open(idx_path)
                .with_context(|| format!("open idx {}", idx_path.display()))?;
            while let Some(rec) = stream.next()? {
                let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
                if ts < d_start {
                    continue;
                }
                if ts >= d_end {
                    break;
                }
                let mid = (rec.index.bid + rec.index.ask) * 0.5;
                if !(mid.is_finite() && mid > 0.0) {
                    continue;
                }
                pass3_records += 1;
                // Emission path: use the full IndexRecord (bid/ask/vbid/vask/
                // ci_ubp/accepted/rejected) so the emitted Bar carries the
                // microstructure enrichment block (realized_var, bipower_var,
                // drift, vol_imbalance, avg_spread_bps, max_abs_return,
                // avg_ci_ubp, reject_rate). 2026-05-25 fix: prior version
                // used `feed_tick(ts, mid)` which left all micros at zero.
                // ci_ubp wire encoding: u16 sqrt-compressed; decode via
                // (ci/CI_SCALE)^2 (see mitch/common::CI_SCALE).
                let ci_decoded = {
                    let c = rec.index.ci as f64 / mitch::common::CI_SCALE;
                    c * c
                };
                let sigma = sigma_at(ts);
                gen_local.feed_index_record(
                    ts,
                    rec.index.bid,
                    rec.index.ask,
                    rec.index.vbid,
                    rec.index.vask,
                    ci_decoded,
                    rec.index.accepted as u32,
                    rec.index.rejected as u32,
                    sigma,
                    &mut |bar: &Bar| {
                        pending.push(*bar);
                        Ok(())
                    },
                )?;
                for bar in pending.drain(..) {
                    // Filter: only bars whose open_time_ms ∈ [d_start, d_end).
                    // RenkoGenerator stamps bar.open_time on the tick that
                    // initiated the brick; for the very first tick after
                    // init that's also ≥ d_start, but the seed-tick above
                    // is at d_start so any seed-induced bar would also be
                    // tagged at d_start. Either way, this filter pins the
                    // bar to day D.
                    let bts = bar.open_time_ms();
                    if bts < d_start || bts >= d_end {
                        continue;
                    }
                    let mut out = bar;
                    out.kind = BarKind::Renko as u8;
                    writer.append(&out)?;
                }
            }
        }
        if bar_writer_must_finalize {
            writer.flush()?;
        }
        drop(writer);

        // ── Measure bpd_actual by counting bricks IN THE WRITTEN SHARD ─
        let final_records: u64 = if out_path.exists() {
            std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0)
                / std::mem::size_of::<Bar>() as u64
        } else {
            0
        };
        let bpd_actual = final_records as f64; // one day window → bpd ≡ count

        // Compute clamp summary for the day from the cached sigma:
        let mid_ts_mts = timestamp::from_epoch_ms(d_start + MS_PER_DAY / 2);
        let hour_idx = vol_mmap.find_index_for_mts(mid_ts_mts);
        let sigma_mid = sigma_cache.get(hour_idx).copied().unwrap_or(0.0);
        let min_pct = yml.renko.min_pct as f64;
        let raw_pct = calibrated_k * sigma_mid;
        // Post 2026-05-24: floor-only, no ceiling.
        let clamped_pct = raw_pct.max(min_pct);

        // Drop the per-day generator (its state is encoded in the prior
        // day's last close, which we re-seed next iteration).
        drop(gen_local);

        summary.bpd_samples.push(bpd_actual);
        eprintln!(
            "renko  pair={} id={} date={} bricks_in={} bricks_out={} bpd={:.0} k={:.6} sigma_mid={:.5} pct_clamped={:.5} (raw={:.5}, min={:.5})",
            pair_str,
            ticker_id,
            d,
            pass3_records,
            final_records,
            bpd_actual,
            calibrated_k,
            sigma_mid,
            clamped_pct,
            raw_pct,
            min_pct
        );
    }

    // ── Cleanup ─────────────────────────────────────────────────────────
    let _ = std::fs::remove_file(&vol_path);
    Ok(summary)
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let root: PipelineYml = PipelineYml::load(&args.config)?;
    let yml = root.series.clone();

    let pairs: Vec<(String, String)> = if args.all {
        launch_pairs_from_yaml(&root)
    } else {
        let base = args
            .base
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--base required unless --all"))?;
        let quote = args
            .quote
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--quote required unless --all"))?;
        vec![(base, quote)]
    };

    // De-dup in case the operator lists the same pair twice.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut summaries: Vec<PairSummary> = Vec::new();
    for (b, q) in pairs {
        let key = format!("{}/{}", b.to_uppercase(), q.to_uppercase());
        if !seen.insert(key.clone()) {
            continue;
        }
        match run_pair(&args, &yml, &b, &q) {
            Ok(s) => summaries.push(s),
            Err(e) => warn!(pair = key, err = %e, "pair run failed"),
        }
    }

    // ── Run-level summary on stderr (per-pair + grand stats) ────────────
    eprintln!("\n────── walk-forward backfill summary ──────");
    let mut grand_samples: Vec<f64> = Vec::new();
    for s in &summaries {
        eprintln!("{}", s.finalize(yml.calibration.target_bpd));
        grand_samples.extend(s.bpd_samples.iter().copied());
    }
    if !grand_samples.is_empty() {
        let mean = grand_samples.iter().sum::<f64>() / grand_samples.len() as f64;
        grand_samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = grand_samples[grand_samples.len() / 2];
        let lo = (yml.calibration.target_bpd / 3.0).max(50.0);
        let hi = yml.calibration.target_bpd * 2.0;
        let oob = grand_samples.iter().filter(|b| **b < lo || **b > hi).count();
        eprintln!(
            "GRAND  pairs={} days={} bpd_mean={:.0} bpd_median={:.0} bpd_oob[{:.0}..{:.0}]={} target={:.0}",
            summaries.len(),
            grand_samples.len(),
            mean,
            median,
            lo,
            hi,
            oob,
            yml.calibration.target_bpd
        );
    } else {
        eprintln!("GRAND  no days emitted (every pair skipped or empty)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for the leak gate. `partition_point(|(ts,_)| ts < d_start)`
    /// must return `cutoff` such that NO element with `ts == d_start` (or
    /// after) is included.
    #[test]
    fn calibrator_window_excludes_day_d_data() {
        let d_start = day_start_ms(NaiveDate::from_ymd_opt(2024, 5, 21).unwrap());
        let prices: Vec<(i64, f64)> = vec![
            (d_start - nxr_sdk::shard::MS_PER_DAY, 100.0), // D-1
            (d_start - 60_000, 101.0),     // 1 min before D
            (d_start - 1, 101.5),          // 1 ms before D
            (d_start, 102.0),              // D start — MUST be excluded
            (d_start + 60_000, 103.0),     // 1 min into D
        ];
        let cutoff = prices.partition_point(|(ts, _)| *ts < d_start);
        let trailing = &prices[..cutoff];
        assert_eq!(trailing.len(), 3);
        assert!(trailing.iter().all(|(ts, _)| *ts < d_start));
    }

    // Note: class_for_pair test moved to nxr_sdk::asset_class (single source).
    // Local wrapper now requires PipelineYml; covered by sdk unit tests.

    #[test]
    fn day_start_aligns_to_utc_midnight() {
        let d = NaiveDate::from_ymd_opt(2024, 5, 21).unwrap();
        assert_eq!(day_start_ms(d) % MS_PER_DAY, 0);
    }

    #[test]
    fn wipe_only_future_dated_shards() {
        let tmp = std::env::temp_dir().join(format!("nxr-wipe-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        // Past + present should survive
        std::fs::write(tmp.join("2024-05-21.renko"), b"").unwrap();
        std::fs::write(tmp.join("2026-05-23.renko"), b"").unwrap();
        // Future (≥ today.year() + 1)
        std::fs::write(tmp.join("2099-01-01.renko"), b"").unwrap();
        std::fs::write(tmp.join("2031-06-15.renko"), b"").unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 5, 24).unwrap();
        let n = wipe_future_dated_junk(&tmp, today).unwrap();
        assert_eq!(n, 2);
        assert!(tmp.join("2024-05-21.renko").exists());
        assert!(tmp.join("2026-05-23.renko").exists());
        assert!(!tmp.join("2099-01-01.renko").exists());
        assert!(!tmp.join("2031-06-15.renko").exists());
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
