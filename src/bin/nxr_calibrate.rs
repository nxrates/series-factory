//! Daily offline calibration of Renko `multiplier` per ticker.
//!
//! Pipeline:
//!   1. Load `config.yml` and the current `ticker-params.json`.
//!   2. For each `(provider, ticker_id)` in `pair_volumes`:
//!        - Infer asset class (fx/crypto · major/alt/stable · cross).
//!        - Look up `target_bpd` for the class; `skip` ⇒ continue.
//!        - Stream the consensus `.idx` file (one per ticker_id) and build 30-min
//!          Parkinson HLC + EMA-smoothed sigma.
//!        - Run MTF binary-search calibration on a 1-min mid downsample.
//!   3. Merge results into `ticker-params.json` (preserving existing fields) and
//!      stamp `calibrated_at`. Atomic write via `nxr_sdk::ipc::write_atomic`.
//!
//! The aggregator picks up the new multipliers on its next mtime check (see
//! `core/src/weights.rs::maybe_reload`).
//!
//! Usage: `nxr-calibrate [--once] [--parallel N]`. `--once` exits after one run
//! (default for k8s CronJob); without it the binary sleeps 24h between runs.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::Parser;
use mitch::timestamp;
use mitch::common::InstrumentType;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{list_shards, ShardStream, MS_PER_30MIN};
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::{resolve_ticker, resolve_ticker_id};
use rayon::prelude::*;
use serde::Deserialize;
use series_factory::bar_construction::{
    build_vol_from_hlc, calibrate_mtf_walkforward, CalibrationConfig, MtfParkinsonCalculator,
    RenkoConfig, VolConfig,
};
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

// ── Synth pair registry (mirrors core/src/synth_registry.rs INITIAL_PAIRS) ──
// Kept inline here because series-factory is a workspace-EXCLUDED crate and
// cannot depend on the core crate; the 5-pair list is small + rarely
// changes. Out-of-sync drift is caught manually until the registry moves to
// nxr-sdk (a future refactor).
struct SynthPairSpec {
    synth_sym: &'static str,
    base_sym: &'static str,
    quote_sym: &'static str,
}

const SYNTH_PAIRS: &[SynthPairSpec] = &[
    SynthPairSpec { synth_sym: "ETH/BTC", base_sym: "ETH/USDT", quote_sym: "BTC/USDT" },
    SynthPairSpec { synth_sym: "SOL/BTC", base_sym: "SOL/USDT", quote_sym: "BTC/USDT" },
    SynthPairSpec { synth_sym: "BNB/BTC", base_sym: "BNB/USDT", quote_sym: "BTC/USDT" },
    SynthPairSpec { synth_sym: "BNB/ETH", base_sym: "BNB/USDT", quote_sym: "ETH/USDT" },
    SynthPairSpec { synth_sym: "SOL/ETH", base_sym: "SOL/USDT", quote_sym: "ETH/USDT" },
];

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(about = "Daily Renko k calibration cron.")]
struct Args {
    /// Run once and exit (default for k8s CronJob).
    #[arg(long)]
    once: bool,
    /// Rayon worker count for the per-ticker loop. Keep low to bound RAM
    /// (each ticker mmaps a .idx and builds a sigma cache).
    #[arg(long, default_value_t = 4)]
    parallel: usize,
}

// ── Config (subset of nxrates.yml) ───────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct NxratesYml {
    #[serde(default)]
    cexs: CexsYml,
    series: SeriesYml,
}

#[derive(Debug, Default, Deserialize)]
struct CexsYml {
    #[serde(default)]
    stablecoins: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SeriesYml {
    renko: RenkoYml,
    vol: VolConfig,
    calibration: CalibrationConfigExt,
}

// max_pct removed (2026-05-24): no ceiling on adaptive renko brick %.
// Debate (Aoife ↔ Tomás): keep field for backward-compat parsing vs strip
// entirely. Consensus: serde tolerates extra keys by default, so dropping
// the field here lets stale config.yml entries (max_pct: 0.10) parse fine.
#[derive(Debug, Deserialize)]
struct RenkoYml {
    min_pct: f32,
}

/// Calibration config with optional per-class `target_bpd` overrides.
/// Defaults to the flat `target_bpd` for the inner `CalibrationConfig`.
#[derive(Debug, Deserialize)]
struct CalibrationConfigExt {
    target_bpd: f64,
    /// Phase 58.L.0 (2026-05-27): renamed from `windows_days` to disambiguate
    /// from the σ-blend MTF (`VolConfig.sigma_blend_windows_days`). Serde
    /// alias keeps existing YAML configs working.
    #[serde(alias = "windows_days")]
    k_fit_windows_days: Vec<usize>,
    min_window_days: usize,
    max_rounds: usize,
    tolerance: f64,
    mult_bounds: [f64; 2],
    #[serde(default)]
    target_bpd_by_class: BTreeMap<String, ClassTarget>,
}

/// "300" → Bpd(300.0); "skip" → Skip.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ClassTarget {
    Bpd(f64),
    Sentinel(String),
}

impl ClassTarget {
    /// `None` ⇒ skip this class; `Some(v)` ⇒ use `v` as target bpd.
    fn resolved(&self) -> Option<f64> {
        match self {
            ClassTarget::Bpd(v) if *v > 0.0 => Some(*v),
            ClassTarget::Bpd(_) => None,
            ClassTarget::Sentinel(s) if s.eq_ignore_ascii_case("skip") => None,
            ClassTarget::Sentinel(s) => {
                warn!(sentinel = s, "unknown target_bpd_by_class sentinel — treating as skip");
                None
            }
        }
    }
}

impl CalibrationConfigExt {
    fn inner(&self) -> CalibrationConfig {
        CalibrationConfig {
            target_bpd: self.target_bpd,
            k_fit_windows_days: self.k_fit_windows_days.clone(),
            min_window_days: self.min_window_days,
            max_rounds: self.max_rounds,
            tolerance: self.tolerance,
            mult_bounds: self.mult_bounds,
        }
    }

    fn target_for(&self, class: AssetClassBucket) -> Option<f64> {
        let key = class.as_str();
        if let Some(t) = self.target_bpd_by_class.get(key) {
            return t.resolved();
        }
        if let Some(t) = self.target_bpd_by_class.get("default") {
            return t.resolved();
        }
        Some(self.target_bpd)
    }
}

// ── Asset-class bucket detection ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum AssetClassBucket {
    CryptoMajor,
    CryptoAlt,
    CryptoCross,
    CryptoStable,
    FxMajor,
    FxCross,
    Unknown,
}

impl AssetClassBucket {
    fn as_str(self) -> &'static str {
        match self {
            Self::CryptoMajor => "crypto_major",
            Self::CryptoAlt => "crypto_alt",
            Self::CryptoCross => "crypto_cross",
            Self::CryptoStable => "crypto_stable",
            Self::FxMajor => "fx_major",
            Self::FxCross => "fx_cross",
            Self::Unknown => "default",
        }
    }
}

/// MITCH AssetClass enum discriminants (mirrored to avoid pulling another dep).
/// 3 = FX, 6 = CR.
const ASSET_CLASS_FX: u8 = 3;
const ASSET_CLASS_CR: u8 = 6;

const FX_MAJORS: &[&str] = &[
    "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD", "USD",
];

const CRYPTO_MAJORS: &[&str] = &[
    "BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "LINK", "DOT",
    "LTC", "BCH", "TRX", "XMR", "ZEC", "SUI", "HYPE", "UNI", "XLM", "HBAR",
    "ETC", "TON",
];

fn classify_pair(pair: &str, ticker_id: u64, stables: &[String]) -> AssetClassBucket {
    let parts: Vec<&str> = pair.split('/').collect();
    let (base, quote) = match parts.as_slice() {
        [b, q] => (b.to_uppercase(), q.to_uppercase()),
        _ => return AssetClassBucket::Unknown,
    };

    // Decode base/quote MITCH class bits from the ticker_id. Bits [59:56] = base
    // class, [39:36] = quote class. We use these as a hint and fall back to
    // symbol-string heuristics; MITCH puts some stables under non-CR classes
    // (e.g. USDS appears as PM=7 in the registry) so name-based detection is
    // authoritative for the stable / non-stable split.
    let base_class = ((ticker_id >> 56) & 0x0F) as u8;
    let quote_class = ((ticker_id >> 36) & 0x0F) as u8;

    let is_base_stable = stables.iter().any(|s| s.eq_ignore_ascii_case(&base));
    let is_quote_stable = stables.iter().any(|s| s.eq_ignore_ascii_case(&quote))
        || quote == "USD" || quote == "USDT" || quote == "USDC";

    // Phase 55 W3.C: name-based stable detection is authoritative. USDS-style
    // pairs were leaking through with k=0.075 because they didn't have both
    // base_class and quote_class == CR, so the MITCH-bit gate above rejected
    // them and they fell into Unknown → default target_bpd. Result: 6360 bpd
    // bricks on stable-quoted stables.  Catch them by name FIRST regardless
    // of MITCH class bits.
    if is_base_stable && is_quote_stable {
        return AssetClassBucket::CryptoStable;
    }

    match (base_class, quote_class) {
        (ASSET_CLASS_FX, ASSET_CLASS_FX) => {
            let majors_only = FX_MAJORS.contains(&base.as_str()) && FX_MAJORS.contains(&quote.as_str());
            if majors_only { AssetClassBucket::FxMajor } else { AssetClassBucket::FxCross }
        }
        (ASSET_CLASS_CR, _) | (_, ASSET_CLASS_CR) => {
            let base_major = CRYPTO_MAJORS.contains(&base.as_str());
            let quote_major = CRYPTO_MAJORS.contains(&quote.as_str());
            // Crypto-cross: both legs non-stable majors (ETH/BTC, SOL/BTC,
            // BNB/ETH, ...). Cross-pair realised vol ≈0.4-0.6× the
            // USD-quoted leg's. Routing to crypto_major target_bpd=300
            // drives the calibrator to compress k toward mult_bounds[0]
            // (observed k=0.01 boundary-clamp 2026-05-26). Bucket
            // separately so target_bpd can be tuned (~100 bpd).
            if base_major && quote_major {
                AssetClassBucket::CryptoCross
            } else if base_major {
                AssetClassBucket::CryptoMajor
            } else {
                AssetClassBucket::CryptoAlt
            }
        }
        _ => {
            // Final fallback: if the base looks like a known crypto major (or
            // unknown alt) by symbol, classify accordingly so we don't lose
            // tickers whose MITCH bits are quirky.
            if CRYPTO_MAJORS.contains(&base.as_str()) {
                AssetClassBucket::CryptoMajor
            } else {
                AssetClassBucket::Unknown
            }
        }
    }
}

// ── Per-ticker calibration ───────────────────────────────────────────────────

#[derive(Debug)]
enum CalOutcome {
    Ok { ticker_id: u64, k: f64 },
    Skipped { ticker_id: u64, reason: String },
    Failed { ticker_id: u64, reason: String },
}

fn calibrate_one(
    ticker_id: u64,
    pair: &str,
    class: AssetClassBucket,
    idx_dir: &Path,
    cal_ext: &CalibrationConfigExt,
    target_bpd: f64,
    vol_cfg: &VolConfig,
    renko_yml: &RenkoYml,
) -> CalOutcome {
    let idx_path = idx_dir.join(format!("{}.idx", ticker_id));
    if !idx_path.exists() {
        return CalOutcome::Skipped {
            ticker_id,
            reason: format!("no .idx at {}", idx_path.display()),
        };
    }

    // Pass 1: stream .idx → 30-min HLC + 1-min mid downsample for calibration.
    let mut stream = match ShardStream::<IndexRecord>::open(&idx_path) {
        Ok(s) => s,
        Err(e) => return CalOutcome::Failed { ticker_id, reason: format!("open .idx: {}", e) },
    };

    // Stream the .idx once, populating both the 30-min HLC map (vol input)
    // and the 1-min last-mid downsample (calibration input). Bucket H = ask,
    // L = bid; matches `renko_from_idx.rs` semantics.
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    let mut price_buckets: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
    loop {
        let rec = match stream.next() {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(e) => return CalOutcome::Failed { ticker_id, reason: format!("read .idx: {}", e) },
        };
        let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
        let bid = rec.index.bid;
        let ask = rec.index.ask;
        let mid = (bid + ask) * 0.5;
        if !(mid.is_finite() && mid > 0.0) { continue; }

        let key = (ts / MS_PER_30MIN) * MS_PER_30MIN;
        let e = hlc.entry(key).or_insert((ask.max(mid), bid.min(mid)));
        if ask > e.0 { e.0 = ask; }
        if bid < e.1 && bid > 0.0 { e.1 = bid; }

        // 1-min last-mid bucket for in-memory calibration.
        let bucket = (ts / 60_000) * 60_000;
        let pe = price_buckets.entry(bucket).or_insert((ts, mid));
        if ts >= pe.0 { *pe = (ts, mid); }
    }

    if hlc.is_empty() {
        return CalOutcome::Skipped { ticker_id, reason: "empty .idx".into() };
    }

    // Build the .vol file (tmp, deleted at end of fn). VolMmap is the de-facto
    // VolSource; reusing the canonical builder keeps calibration bit-for-bit
    // identical to the prod pipeline and the other offline pipelines.
    let vol_path = std::env::temp_dir().join(format!("nxr-calibrate-{}-{}.vol", ticker_id, std::process::id()));
    {
        let mut writer = match VolWriter::new(&vol_path) {
            Ok(w) => w,
            Err(e) => return CalOutcome::Failed { ticker_id, reason: format!("vol writer: {}", e) },
        };
        if let Err(e) = build_vol_from_hlc(&hlc, vol_cfg, &mut writer) {
            return CalOutcome::Failed { ticker_id, reason: format!("vol build: {}", e) };
        }
        if let Err(e) = writer.finish() {
            return CalOutcome::Failed { ticker_id, reason: format!("vol finish: {}", e) };
        }
    }

    let vol_mmap = match VolMmap::open(&vol_path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Failed { ticker_id, reason: format!("vol mmap: {}", e) };
        }
    };

    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let tick_prices: Vec<(i64, f64)> = price_buckets
        .into_iter()
        .map(|(_, (ts, mid))| (ts, mid))
        .collect();

    let base = RenkoConfig {
        multiplier: 0.075,
        min_pct: renko_yml.min_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed { ticker_id, reason: format!("base renko cfg: {}", e) };
    }

    info!(ticker_id, pair, class = class.as_str(), target_bpd, "calibrating (walk-forward)");
    // Phase 58.L.0 (2026-05-27): switched from in-sample
    // `calibrate_mtf_with_target` to `calibrate_mtf_walkforward` per audit
    // point #5(i) 2026-05-26. The 7d holdout is non-overlapping with the
    // training slice, eliminating the regime-leak overfit that produced
    // k≈0.01 boundary-clamps on cross-pairs (live brick-storm root cause).
    const EVAL_HOLDOUT_DAYS: usize = 7;
    let mult = calibrate_mtf_walkforward(
        &tick_prices,
        &cal_ext.inner(),
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
        EVAL_HOLDOUT_DAYS,
    );

    let _ = std::fs::remove_file(&vol_path);

    if !(mult > 0.0 && (mult as f64).is_finite()) {
        return CalOutcome::Failed {
            ticker_id,
            reason: "calibration returned 0 (no window had enough data)".into(),
        };
    }
    CalOutcome::Ok { ticker_id, k: mult as f64 }
}

// ── Synth-pair calibration ───────────────────────────────────────────────────
//
// For each configured synth cross (e.g. ETH/BTC), reconstruct synth ticks
// from the two underlying USDT-quoted leg `.idx` files via event-driven
// min-heap merge, then run the SAME MTF calibrator that the base path uses.
// Output is a single `renko_k` value per synth ticker_id, written to
// `ticker-params.json` alongside base entries; live `bars_renko_synth`
// picks it up via the existing weights hot-reload path.
//
// **Why NOT persist a synth `.idx`:** the kernel design (see audit doc
// `synth-pipeline-design-2026-05-26.md`) keeps synth on the wire only —
// disk has bars + σ, never synth ticks. Calibration is the one place we
// reconstruct ticks transiently in memory.
//
// **Why NOT K_FLOOR fallback on synth:** Method-B σ from event-merged
// ticks is the operator's quality target; if calibrate fails (e.g.
// clamp-detector drops every window), `Failed` is the honest outcome and
// the caller carries the prior value rather than fabricating one.

/// Stream one leg's `.idx` shards into a single ascending-ts iterator of
/// `(ts, bid, ask)` triples. Returns Err if no shards or any read fails.
///
/// `idx_root` must point at the indexes directory itself (e.g. `/data/indexes`,
/// NOT `/data` — `nxr-calibrate`'s NxrConfig::indexes_dir already includes
/// the `indexes/` suffix). Per-ticker shards live at
/// `<idx_root>/<ticker_id>/<YYYY-MM-DD>.idx`.
fn load_leg_ticks_all_shards(idx_root: &Path, ticker_id: u64) -> Result<Vec<(i64, f64, f64)>> {
    let dir = idx_root.join(ticker_id.to_string());
    let shards = list_shards(&dir, "idx")
        .with_context(|| format!("list shards {}", dir.display()))?;
    if shards.is_empty() {
        anyhow::bail!("no .idx shards under {}", dir.display());
    }
    let mut out: Vec<(i64, f64, f64)> = Vec::new();
    for (_d, path) in shards {
        let mut stream = ShardStream::<IndexRecord>::open(&path)
            .with_context(|| format!("open idx {}", path.display()))?;
        while let Some(rec) = stream.next()? {
            let ts = timestamp::to_epoch_ms(rec.header.get_timestamp());
            let bid = rec.index.bid;
            let ask = rec.index.ask;
            if !(bid.is_finite() && ask.is_finite()) { continue; }
            if bid <= 0.0 || ask <= 0.0 { continue; }
            out.push((ts, bid, ask));
        }
    }
    out.sort_by_key(|t| t.0);
    Ok(out)
}

fn calibrate_one_synth(
    synth_id: u64,
    synth_sym: &str,
    leg_a_id: u64,
    leg_b_id: u64,
    idx_root: &Path,
    cal_ext: &CalibrationConfigExt,
    target_bpd: f64,
    vol_cfg: &VolConfig,
    renko_yml: &RenkoYml,
) -> CalOutcome {
    // ── 1. Load both legs ────────────────────────────────────────────────────
    let leg_a = match load_leg_ticks_all_shards(idx_root, leg_a_id) {
        Ok(v) => v,
        Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("leg_a={} {}", leg_a_id, e) },
    };
    let leg_b = match load_leg_ticks_all_shards(idx_root, leg_b_id) {
        Ok(v) => v,
        Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("leg_b={} {}", leg_b_id, e) },
    };
    if leg_a.is_empty() || leg_b.is_empty() {
        return CalOutcome::Skipped { ticker_id: synth_id, reason: "empty leg".into() };
    }

    // ── 2. Event-driven 2-stream merge → synth (ts, bid, ask, mid) ───────────
    // At every leg tick we update last-known of that leg, then if both legs
    // primed emit a synth tick using the worst-case-spread convention:
    //   synth.bid = leg_a.bid / leg_b.ask
    //   synth.ask = leg_a.ask / leg_b.bid
    // (mirrors core/src/synth_kernel.rs:185 + triangulator.rs:17).
    let mut heap: BinaryHeap<Reverse<(i64, u8, usize)>> = BinaryHeap::new();
    heap.push(Reverse((leg_a[0].0, 0u8, 0usize)));
    heap.push(Reverse((leg_b[0].0, 1u8, 0usize)));
    let mut a_last: Option<(f64, f64)> = None;
    let mut b_last: Option<(f64, f64)> = None;
    // Two parallel maps (mirroring `calibrate_one`) — 30-min HLC for the
    // vol input, 1-min last-mid downsample for the calibrator's tick
    // replay. Sized at full leg total as an upper bound; over-allocation
    // is preferable to reallocation on the hot path.
    let mut hlc: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    let mut price_buckets: BTreeMap<i64, (i64, f64)> = BTreeMap::new();
    while let Some(Reverse((ts, which, idx))) = heap.pop() {
        match which {
            0 => {
                let (_, b, a) = leg_a[idx];
                a_last = Some((b, a));
                let next = idx + 1;
                if next < leg_a.len() {
                    heap.push(Reverse((leg_a[next].0, 0, next)));
                }
            }
            _ => {
                let (_, b, a) = leg_b[idx];
                b_last = Some((b, a));
                let next = idx + 1;
                if next < leg_b.len() {
                    heap.push(Reverse((leg_b[next].0, 1, next)));
                }
            }
        }
        // Both legs primed → emit synth tick.
        if let (Some((ab, aa)), Some((bb, ba))) = (a_last, b_last) {
            // Worst-case spread compounding (mirrors live kernel).
            let synth_bid = ab / ba;
            let synth_ask = aa / bb;
            if !(synth_bid.is_finite() && synth_ask.is_finite()
                && synth_bid > 0.0 && synth_ask > 0.0) {
                continue;
            }
            let mid = (synth_bid + synth_ask) * 0.5;
            // 30-min H/L bucket (H = ask, L = bid).
            let key = (ts / MS_PER_30MIN) * MS_PER_30MIN;
            let e = hlc.entry(key).or_insert((synth_ask.max(mid), synth_bid.min(mid)));
            if synth_ask > e.0 { e.0 = synth_ask; }
            if synth_bid < e.1 && synth_bid > 0.0 { e.1 = synth_bid; }
            // 1-min last-mid downsample.
            let bucket = (ts / 60_000) * 60_000;
            let pe = price_buckets.entry(bucket).or_insert((ts, mid));
            if ts >= pe.0 { *pe = (ts, mid); }
        }
    }

    if hlc.is_empty() {
        return CalOutcome::Skipped { ticker_id: synth_id, reason: "empty merged stream".into() };
    }

    // ── 3. VolWriter / VolMmap / sigma cache (identical to base path) ────────
    let vol_path = std::env::temp_dir()
        .join(format!("nxr-calibrate-synth-{}-{}.vol", synth_id, std::process::id()));
    {
        let mut writer = match VolWriter::new(&vol_path) {
            Ok(w) => w,
            Err(e) => return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol writer: {}", e) },
        };
        if let Err(e) = build_vol_from_hlc(&hlc, vol_cfg, &mut writer) {
            return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol build: {}", e) };
        }
        if let Err(e) = writer.finish() {
            return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol finish: {}", e) };
        }
    }
    let vol_mmap = match VolMmap::open(&vol_path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&vol_path);
            return CalOutcome::Failed { ticker_id: synth_id, reason: format!("vol mmap: {}", e) };
        }
    };
    let sigma_cache = {
        let mut calc = MtfParkinsonCalculator::new(&vol_mmap, vol_cfg.clone());
        calc.precompute_sigma_cache()
    };

    let tick_prices: Vec<(i64, f64)> = price_buckets
        .into_iter()
        .map(|(_, (ts, mid))| (ts, mid))
        .collect();

    let base = RenkoConfig {
        multiplier: 0.075,
        min_pct: renko_yml.min_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed { ticker_id: synth_id, reason: format!("base renko cfg: {}", e) };
    }

    info!(synth_id, synth_sym, leg_a_id, leg_b_id, target_bpd, "calibrating synth (walk-forward)");
    // Phase 58.L.0: same walk-forward swap as direct path above.
    const EVAL_HOLDOUT_DAYS: usize = 7;
    let mult = calibrate_mtf_walkforward(
        &tick_prices,
        &cal_ext.inner(),
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
        EVAL_HOLDOUT_DAYS,
    );
    let _ = std::fs::remove_file(&vol_path);

    if !(mult > 0.0 && (mult as f64).is_finite()) {
        return CalOutcome::Failed {
            ticker_id: synth_id,
            reason: "synth calibration returned 0 (clamp-dropped windows or insufficient data)".into(),
        };
    }
    CalOutcome::Ok { ticker_id: synth_id, k: mult as f64 }
}

/// Resolve all `SYNTH_PAIRS` entries to `(synth_id, sym, leg_a_id, leg_b_id)`.
/// Entries that fail to resolve any leg are dropped with a warn.
fn resolve_synth_work() -> Vec<(u64, &'static str, u64, u64)> {
    let mut out = Vec::with_capacity(SYNTH_PAIRS.len());
    for spec in SYNTH_PAIRS {
        let resolve = |sym: &str| -> Option<u64> {
            match resolve_ticker(sym, InstrumentType::SPOT) {
                Ok(m) => Some(m.ticker.id),
                Err(e) => {
                    warn!(sym, err = ?e, "synth pair resolve failed; skipping");
                    None
                }
            }
        };
        let synth_id = match resolve(spec.synth_sym) { Some(v) => v, None => continue };
        let leg_a_id = match resolve(spec.base_sym) { Some(v) => v, None => continue };
        let leg_b_id = match resolve(spec.quote_sym) { Some(v) => v, None => continue };
        out.push((synth_id, spec.synth_sym, leg_a_id, leg_b_id));
    }
    out
}

// ── Main run ─────────────────────────────────────────────────────────────────

fn run_once(args: &Args) -> Result<()> {
    let cfg_path = std::env::var("NXR_CONFIG").unwrap_or_else(|_| "config.yml".to_string());
    let root: NxratesYml = serde_yaml::from_str(
        &std::fs::read_to_string(&cfg_path).with_context(|| format!("read {}", cfg_path))?,
    )
    .with_context(|| format!("parse {}", cfg_path))?;
    let series = &root.series;
    let stables = &root.cexs.stablecoins;

    let nxr_cfg = nxr_sdk::NxrConfig::from_env();
    let params_path = PathBuf::from(&nxr_cfg.ticker_params_path);
    let idx_dir = PathBuf::from(&nxr_cfg.indexes_dir);

    info!(
        cfg = cfg_path,
        params = %params_path.display(),
        idx = %idx_dir.display(),
        parallel = args.parallel,
        "nxr-calibrate starting"
    );

    // Load the existing weights file so we can preserve volumes/exchanges/etc.
    let mut weights_file: WeightsFile = if params_path.exists() {
        let raw = std::fs::read_to_string(&params_path)
            .with_context(|| format!("read {}", params_path.display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", params_path.display()))?
    } else {
        warn!(path = %params_path.display(), "ticker-params.json missing — starting from scratch");
        WeightsFile::default()
    };

    // Build the work list: (pair, ticker_id, class). De-dupe across providers
    // since the same ticker_id appears under multiple exchanges in pair_volumes.
    let mut seen: HashMap<u64, (String, AssetClassBucket)> = HashMap::new();
    for pairs in weights_file.pair_volumes.values() {
        for pair in pairs.keys() {
            let ticker_id = resolve_ticker_id(pair);
            let class = classify_pair(pair, ticker_id, stables);
            seen.entry(ticker_id).or_insert_with(|| (pair.clone(), class));
        }
    }
    let work: Vec<(u64, String, AssetClassBucket)> = seen
        .into_iter()
        .map(|(id, (p, c))| (id, p, c))
        .collect();
    info!(n_tickers = work.len(), "ticker universe assembled");

    // Configure rayon worker count.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.parallel.max(1))
        .build()
        .with_context(|| "build rayon pool")?;

    let cal_ext = &series.calibration;
    let vol_cfg = &series.vol;
    let renko_yml = &series.renko;

    let results: Mutex<Vec<CalOutcome>> = Mutex::new(Vec::with_capacity(work.len()));

    pool.install(|| {
        work.par_iter().for_each(|(ticker_id, pair, class)| {
            let target_bpd = match cal_ext.target_for(*class) {
                Some(t) => t,
                None => {
                    results.lock().unwrap().push(CalOutcome::Skipped {
                        ticker_id: *ticker_id,
                        reason: format!("class {} marked skip", class.as_str()),
                    });
                    return;
                }
            };

            // Panic-safe: one bad ticker (malformed .idx, OOM in sigma cache,
            // ...) must not abort the whole cron. AssertUnwindSafe is sound
            // here because nothing inside is moved across the boundary.
            let pair_clone = pair.clone();
            let class_clone = *class;
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                calibrate_one(
                    *ticker_id,
                    &pair_clone,
                    class_clone,
                    &idx_dir,
                    cal_ext,
                    target_bpd,
                    vol_cfg,
                    renko_yml,
                )
            }))
            .unwrap_or_else(|p| {
                let msg = if let Some(s) = p.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = p.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                CalOutcome::Failed { ticker_id: *ticker_id, reason: format!("panic: {}", msg) }
            });

            results.lock().unwrap().push(outcome);
        });
    });

    // Tally.
    let outcomes = results.into_inner().unwrap();
    let (mut passed, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    let mut renko_k: BTreeMap<String, f64> = weights_file.renko_k_per_ticker.clone();

    for o in &outcomes {
        match o {
            CalOutcome::Ok { ticker_id, k } => {
                passed += 1;
                renko_k.insert(ticker_id.to_string(), *k);
            }
            CalOutcome::Skipped { ticker_id, reason } => {
                skipped += 1;
                info!(ticker_id, %reason, "skipped");
            }
            CalOutcome::Failed { ticker_id, reason } => {
                failed += 1;
                warn!(ticker_id, %reason, "calibration failed");
            }
        }
    }

    info!(
        passed,
        skipped,
        failed,
        total = outcomes.len(),
        "calibration summary (base)"
    );

    // ── Synth-pair pass (5 crosses) ──────────────────────────────────────────
    // Runs unconditionally after the base pass. Cheap (5 pairs, mostly
    // bound by leg .idx I/O which the base pass already warmed in page
    // cache). The clamp-detector inside `calibrate_mtf_with_target` drops
    // degenerate windows; if all windows fail, k is NOT persisted (caller
    // keeps prior). The crypto_cross target_bpd applies.
    let synth_target = cal_ext
        .target_for(AssetClassBucket::CryptoCross)
        .unwrap_or_else(|| {
            warn!("crypto_cross target_bpd missing; falling back to default");
            cal_ext.target_bpd
        });
    let synth_work = resolve_synth_work();
    info!(n_synth = synth_work.len(), synth_target_bpd = synth_target, "synth calibration pass starting");
    let (mut s_passed, mut s_skipped, mut s_failed) = (0usize, 0usize, 0usize);
    for (synth_id, synth_sym, leg_a_id, leg_b_id) in synth_work {
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            calibrate_one_synth(
                synth_id, synth_sym, leg_a_id, leg_b_id,
                &idx_dir, cal_ext, synth_target, vol_cfg, renko_yml,
            )
        }))
        .unwrap_or_else(|p| {
            let msg = if let Some(s) = p.downcast_ref::<&str>() { s.to_string() }
                      else if let Some(s) = p.downcast_ref::<String>() { s.clone() }
                      else { "unknown panic".to_string() };
            CalOutcome::Failed { ticker_id: synth_id, reason: format!("panic: {}", msg) }
        });
        match outcome {
            CalOutcome::Ok { ticker_id, k } => {
                s_passed += 1;
                info!(synth_id = ticker_id, synth_sym, k, "synth calibrated");
                renko_k.insert(ticker_id.to_string(), k);
            }
            CalOutcome::Skipped { ticker_id, reason } => {
                s_skipped += 1;
                info!(synth_id = ticker_id, synth_sym, %reason, "synth skipped");
            }
            CalOutcome::Failed { ticker_id, reason } => {
                s_failed += 1;
                warn!(synth_id = ticker_id, synth_sym, %reason, "synth failed");
            }
        }
    }
    info!(s_passed, s_skipped, s_failed, "calibration summary (synth)");

    weights_file.renko_k_per_ticker = renko_k;
    weights_file.calibrated_at = Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );

    let json = serde_json::to_string_pretty(&weights_file)?;
    // write_atomic requires Pod; we have a String → emit via a tiny shim that
    // mirrors its tmp+rename semantics for the JSON case.
    write_atomic_string(&params_path, &json)?;
    info!(path = %params_path.display(), bytes = json.len(), "ticker-params.json updated");

    Ok(())
}

/// Atomic JSON-string write: `<path>.tmp` + rename. Mirrors
/// `nxr_sdk::ipc::write_atomic` but for non-Pod payloads.
fn write_atomic_string(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {:?}", parent))?;
    }
    let tmp = {
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if ext.is_empty() {
            path.with_extension("tmp")
        } else {
            path.with_extension(format!("{ext}.tmp"))
        }
    };
    std::fs::write(&tmp, contents).with_context(|| format!("write {:?}", tmp))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {:?} -> {:?}", tmp, path))?;
    Ok(())
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();

    loop {
        if let Err(e) = run_once(&args) {
            warn!(err = %e, "calibration run failed");
        }
        if args.once { break; }
        info!("sleeping 24h until next calibration");
        std::thread::sleep(std::time::Duration::from_secs(24 * 3600));
    }
    Ok(())
}
