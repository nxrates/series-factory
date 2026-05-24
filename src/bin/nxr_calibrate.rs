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

use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::Parser;
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{ShardStream, MS_PER_30MIN};
use nxr_sdk::weights_schema::WeightsFile;
use nxr_sdk::resolve_ticker_id;
use rayon::prelude::*;
use serde::Deserialize;
use series_factory::bar_construction::{
    build_vol_from_hlc, calibrate_mtf_with_target, CalibrationConfig, MtfParkinsonCalculator,
    RenkoConfig, VolConfig,
};
use series_factory::vol_bin::{VolMmap, VolWriter};
use tracing::{info, warn};

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

#[derive(Debug, Deserialize)]
struct RenkoYml {
    min_pct: f32,
    max_pct: f32,
}

/// Calibration config with optional per-class `target_bpd` overrides.
/// Defaults to the flat `target_bpd` for the inner `CalibrationConfig`.
#[derive(Debug, Deserialize)]
struct CalibrationConfigExt {
    target_bpd: f64,
    windows_days: Vec<usize>,
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
            windows_days: self.windows_days.clone(),
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
    // class, [39:36] = quote class. We use these as the primary discriminator
    // and fall back to symbol-string heuristics for the major/alt/stable split.
    let base_class = ((ticker_id >> 56) & 0x0F) as u8;
    let quote_class = ((ticker_id >> 36) & 0x0F) as u8;

    let is_base_stable = stables.iter().any(|s| s.eq_ignore_ascii_case(&base));
    let is_quote_stable = stables.iter().any(|s| s.eq_ignore_ascii_case(&quote))
        || quote == "USD" || quote == "USDT" || quote == "USDC";

    match (base_class, quote_class) {
        (ASSET_CLASS_FX, ASSET_CLASS_FX) => {
            let majors_only = FX_MAJORS.contains(&base.as_str()) && FX_MAJORS.contains(&quote.as_str());
            if majors_only { AssetClassBucket::FxMajor } else { AssetClassBucket::FxCross }
        }
        (ASSET_CLASS_CR, _) | (_, ASSET_CLASS_CR) => {
            if is_base_stable && is_quote_stable {
                AssetClassBucket::CryptoStable
            } else if CRYPTO_MAJORS.contains(&base.as_str()) {
                AssetClassBucket::CryptoMajor
            } else {
                AssetClassBucket::CryptoAlt
            }
        }
        _ => AssetClassBucket::Unknown,
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
        max_pct: renko_yml.max_pct,
    };
    if let Err(e) = base.validate() {
        let _ = std::fs::remove_file(&vol_path);
        return CalOutcome::Failed { ticker_id, reason: format!("base renko cfg: {}", e) };
    }

    info!(ticker_id, pair, class = class.as_str(), target_bpd, "calibrating");
    let mult = calibrate_mtf_with_target(
        &tick_prices,
        &cal_ext.inner(),
        &base,
        &vol_mmap,
        vol_cfg,
        &sigma_cache,
        target_bpd,
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
        "calibration summary"
    );

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
