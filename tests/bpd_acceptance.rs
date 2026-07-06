//! BPD acceptance test for renko-trailing-from-idx.
//!
//! Runs `renko-trailing-from-idx --all` against a cluster data root and
//! asserts that median bricks-per-day for each launch symbol lands inside
//! `[target * 0.85, target * 1.15]` (operator: ±15% of 300).
//!
//! Gated with `#[ignore]` because it requires real `.idx` shards under
//! `$NXR_DATA_ROOT`. CI doesn't have those; the operator runs the test
//! manually after a cluster job emits the trailing renko shards. Local
//! invocation:
//!
//! ```bash
//! NXR_DATA_ROOT=/data \
//!   NXR_CONFIG=$PWD/../config.yml \
//!   cargo test --release --test bpd_acceptance -- --ignored --nocapture
//! ```
//!
//! Cluster invocation: build `nxr-tools` image with the latest commit, run
//! a one-shot Job that mounts `/data`, exec `renko-trailing-from-idx --all
//! --config /etc/nxrates/config.yml`, then `cargo test --test
//! bpd_acceptance -- --ignored` on the same node to read the generated
//! `.renko` shards and confirm bpd convergence.
//!
//! Debate (Aoife HFT-quant ↔ Bjørn quant-validation):
//!   - Aoife: "Median is more robust than mean — single weird day (Trump
//!     post, FOMC surprise) won't break the gate."
//!   - Bjørn: "But median hides bimodal failure. We should ALSO log p10/p90
//!     for visibility, even if we only assert on median."
//!   - Consensus: assert median ∈ [255, 345]; print p10/p50/p90 + count
//!     for forensic post-mortem if it fails.
//!
//! Stablecoin classes are skipped (matches `class_for_pair` behaviour).

use std::path::{Path, PathBuf};

use mitch::bar::Bar;
use nxr_sdk::resolve_ticker_id;
use nxr_sdk::shard::{bars_dir, list_shards};

/// Launch pairs that MUST converge to target bpd ±15%.
///
/// Stables (USDC, USDS, USDE, USD1) are excluded because the calibrator
/// skips them (`class_for_pair → crypto_stable → target_for_class → None`).
/// Same list as `renko_trailing_from_idx::LAUNCH_PAIRS` minus stables.
const REQUIRED_PAIRS: &[(&str, &str)] = &[
    ("BTC", "USDT"),
    ("ETH", "USDT"),
    ("BNB", "USDT"),
    ("SOL", "USDT"),
    ("XAUT", "USDT"),
];

const TARGET_BPD: f64 = 300.0;
const TOLERANCE: f64 = 0.15;

fn data_root() -> Option<PathBuf> {
    std::env::var("NXR_DATA_ROOT").ok().map(PathBuf::from)
}

/// Count bars in a single `.renko` shard via byte-length / sizeof(Bar).
fn count_bars(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|m| m.len())
        .unwrap_or(0)
        / std::mem::size_of::<Bar>() as u64
}

#[derive(Debug, Default)]
struct BpdStats {
    samples: Vec<u64>,
}

impl BpdStats {
    fn add(&mut self, n: u64) {
        self.samples.push(n);
    }

    fn percentile(&mut self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples.sort_unstable();
        let ix = ((self.samples.len() as f64 - 1.0) * p).round() as usize;
        self.samples[ix] as f64
    }
}

fn collect_pair_stats(root: &Path, base: &str, quote: &str) -> BpdStats {
    let pair_str = format!("{}/{}", base, quote);
    let ticker_id = resolve_ticker_id(&pair_str);
    let dir = bars_dir(root, ticker_id);
    let mut stats = BpdStats::default();

    let shards = match list_shards(&dir, "renko") {
        Ok(v) => v,
        Err(_) => return stats,
    };

    for (_d, path) in &shards {
        let n = count_bars(path);
        // Drop empty shards: typically migration artifacts, not real days.
        if n > 0 {
            stats.add(n);
        }
    }
    stats
}

/// Core acceptance check. Asserts median bpd is within ±15% of target for
/// every required pair. Skips silently if `NXR_DATA_ROOT` isn't set or no
/// shards exist (CI safe).
#[test]
#[ignore = "requires NXR_DATA_ROOT + renko-trailing-from-idx output"]
fn launch_symbols_median_bpd_within_tolerance() {
    let Some(root) = data_root() else {
        eprintln!("skip: NXR_DATA_ROOT not set");
        return;
    };
    let lo = TARGET_BPD * (1.0 - TOLERANCE);
    let hi = TARGET_BPD * (1.0 + TOLERANCE);

    let mut failures: Vec<String> = Vec::new();
    let mut report: Vec<String> = Vec::new();

    for (b, q) in REQUIRED_PAIRS {
        let mut stats = collect_pair_stats(&root, b, q);
        if stats.samples.is_empty() {
            failures.push(format!("{}/{}: no .renko shards found", b, q));
            continue;
        }
        let p10 = stats.percentile(0.10);
        let p50 = stats.percentile(0.50);
        let p90 = stats.percentile(0.90);
        let n = stats.samples.len();
        let line = format!(
            "{:>4}/{:<4}  n={:>4}  p10={:>5.0}  median={:>5.0}  p90={:>5.0}",
            b, q, n, p10, p50, p90
        );
        report.push(line.clone());
        if p50 < lo || p50 > hi {
            failures.push(format!(
                "{}/{}: median {:.0} outside [{:.0}, {:.0}] — {}",
                b, q, p50, lo, hi, line
            ));
        }
    }

    eprintln!("\n── BPD acceptance report (target={:.0} ±{:.0}%) ──", TARGET_BPD, TOLERANCE * 100.0);
    for line in &report {
        eprintln!("  {}", line);
    }
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("  FAIL: {}", f);
        }
        panic!("{} pair(s) failed BPD tolerance gate", failures.len());
    }
}
