//! Pure statistical diagnostics for Renko bar quality.
//!
//! Objective (no trading metrics anywhere in this module):
//!   Layer 1 (hard gates): DENS (adaptive bars/day), LAG (reversal delay)
//!   Layer 2 (ranking):    J_stats = 0.30*STAT + 0.30*IID + 0.20*HOMO + 0.05*NORM + 0.15*ROBUST
//!
//! Component definitions:
//!   STAT   = 0.6*(1-ADF_p) + 0.4*KPSS_p
//!            ADF:  lower p-value → reject unit root → stationary
//!            KPSS: higher p-value → fail to reject stationarity → stationary
//!   IID    = 1 - mean(|ACF_1..5|) - 0.5*mean(|ACF²_1..5|)
//!            Penalises both returns autocorrelation and volatility clustering.
//!   HOMO   = 1 - CV(fold_variance)
//!            Measures cross-fold variance stability.  MUST be computed at
//!            aggregate level - CV of a single-fold variance is undefined.
//!   NORM   = 1 - 0.1*|skew| - 0.05*|excess_kurtosis|
//!            Light penalty only; fat tails are acceptable for gradient boosting.
//!   ROBUST = 1 - CV(STAT) - 0.5*CV(IID)
//!            Cross-fold regime stability of the diagnostic components themselves.
//!
//! Design invariants:
//!   - No IC, Sharpe, drawdown, or any trading metric anywhere in this file.
//!   - HOMO is aggregate by definition (cross-fold variance dispersion).
//!   - ROBUST is distinct from HOMO: it asks "do STAT/IID themselves vary across
//!     time periods?" not "does the variance level vary?"
//!   - All computation is strictly causal.

use mitch::bar::Bar;
use nxr_sdk::stats::{acf, cv, median};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Hard gate thresholds for filtering degenerate configurations.
///
/// Only the reversal delay gate remains. The density (bars/day) gate was removed -
/// J_stats already captures everything that matters. If 3 bars/day or 1000 bars/day
/// produces better statistical properties, the optimizer should be free to discover that.
#[derive(Debug, Clone)]
pub struct GateSpec {
    /// Reject if reversal delay exceeds this fraction in [0, 1].
    pub max_reversal_delay_pct: f64,
}

impl Default for GateSpec {
    fn default() -> Self {
        Self {
            max_reversal_delay_pct: 0.5,
        }
    }
}

// ── Score types ───────────────────────────────────────────────────────────────

/// Statistical scores for a single temporal fold.
///
/// Does NOT include HOMO - that is computed at aggregate level
/// from the collection of fold variances.
#[derive(Debug, Clone, Default)]
pub struct StatFoldScore {
    /// Stationarity score in [0, 1].
    pub stat: f64,
    /// IID score in [0, 1].
    pub iid: f64,
    /// Normality score in [0, 1].
    pub norm: f64,
    /// Variance of returns in this fold (used to compute aggregate HOMO).
    pub variance: f64,
}

/// Aggregate score across all folds for a single Renko configuration.
#[derive(Debug, Clone)]
pub struct StatAggregateScore {
    pub median_stat: f64,
    pub median_iid: f64,
    /// Cross-fold variance stability (1 - CV of fold variances).
    pub homo: f64,
    pub median_norm: f64,
    /// Cross-fold diagnostic stability (1 - CV(STAT) - 0.5*CV(IID)).
    pub robust: f64,
    /// Primary objective J_stats.
    pub objective: f64,
    /// True if all hard gates were passed.
    pub passed_gates: bool,
    /// Human-readable gate failure reasons.
    pub gate_reasons: Vec<String>,
    /// Observed bars per calendar day.
    pub bars_per_day: f64,
    /// Reversal delay score in [0, 1] (lower = faster).
    pub reversal_delay: f64,
}

impl StatAggregateScore {
    /// J_stats = 0.30*STAT + 0.30*IID + 0.20*HOMO + 0.05*NORM + 0.15*ROBUST
    pub fn compute_objective(&self) -> f64 {
        0.30 * self.median_stat
            + 0.30 * self.median_iid
            + 0.20 * self.homo
            + 0.05 * self.median_norm
            + 0.15 * self.robust
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Compute statistical scores for a single fold from its return series.
///
/// Returns a `StatFoldScore` including the fold variance, which is needed
/// later to compute aggregate HOMO across all folds.
pub fn score_fold(fold_returns: &[f64]) -> StatFoldScore {
    if fold_returns.len() < 20 {
        return StatFoldScore::default();
    }

    let stat = compute_stat(fold_returns);
    let iid = compute_iid(fold_returns);
    let norm = compute_norm(fold_returns);

    let n = fold_returns.len() as f64;
    let mean = fold_returns.iter().sum::<f64>() / n;
    let variance = fold_returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / n;

    StatFoldScore { stat, iid, norm, variance }
}

/// Aggregate per-fold scores into a final config score, applying hard gates.
///
/// `bars` is needed for the reversal-delay gate (requires bar direction sequence).
pub fn aggregate_fold_scores(
    fold_scores: &[StatFoldScore],
    n_bars: usize,
    duration_ms: i64,
    bars: &[Bar],
    gate_spec: &GateSpec,
) -> StatAggregateScore {
    if fold_scores.is_empty() {
        return StatAggregateScore {
            median_stat: 0.0,
            median_iid: 0.0,
            homo: 0.0,
            median_norm: 0.0,
            robust: 0.0,
            objective: 0.0,
            passed_gates: false,
            gate_reasons: vec!["no folds scored".into()],
            bars_per_day: 0.0,
            reversal_delay: 1.0,
        };
    }

    let stats: Vec<f64> = fold_scores.iter().map(|s| s.stat).collect();
    let iids: Vec<f64> = fold_scores.iter().map(|s| s.iid).collect();
    let norms: Vec<f64> = fold_scores.iter().map(|s| s.norm).collect();
    let variances: Vec<f64> = fold_scores.iter().map(|s| s.variance).collect();

    let median_stat = median(&stats);
    let median_iid = median(&iids);
    let median_norm = median(&norms);
    let homo = compute_homo(&variances);
    let robust = compute_robust(&stats, &iids);

    let bars_per_day = compute_density_bars_per_day(n_bars, duration_ms);
    let reversal_delay = compute_reversal_delay_lag(bars, 10);

    let mut reasons = Vec::new();
    if reversal_delay > gate_spec.max_reversal_delay_pct {
        reasons.push(format!(
            "reversal delay {:.3} above max {:.3}",
            reversal_delay, gate_spec.max_reversal_delay_pct
        ));
    }

    let mut score = StatAggregateScore {
        median_stat,
        median_iid,
        homo,
        median_norm,
        robust,
        objective: 0.0,
        passed_gates: reasons.is_empty(),
        gate_reasons: reasons,
        bars_per_day,
        reversal_delay,
    };
    score.objective = score.compute_objective();
    score
}

/// Compute 1-step forward returns from Renko bar closes.
///
/// Returns[i] = (close[i+horizon] - close[i]) / close[i].
/// Causal: only uses information available at bar i.
pub fn compute_returns(bars: &[Bar], horizon: usize) -> Vec<f64> {
    if bars.len() <= horizon {
        return Vec::new();
    }
    (0..bars.len() - horizon)
        .map(|i| {
            let close = bars[i].close;
            let future = bars[i + horizon].close;
            (future - close) / close.max(1e-10)
        })
        .collect()
}

// ── Statistical tests ─────────────────────────────────────────────────────────

/// STAT = 0.6*(1 - ADF_p) + 0.4*KPSS_p
pub fn compute_stat(returns: &[f64]) -> f64 {
    if returns.len() < 50 {
        return 0.5;
    }
    let adf_score = 1.0 - adf_test(returns);
    let kpss_score = kpss_test(returns);
    0.6 * adf_score + 0.4 * kpss_score
}

/// Augmented Dickey-Fuller (simplified OLS on lagged differences).
///
/// H0: unit root.  Returns p-value; lower → more stationary.
fn adf_test(returns: &[f64]) -> f64 {
    let n = returns.len();
    if n < 10 {
        return 0.5;
    }

    let delta: Vec<f64> = (1..n).map(|i| returns[i] - returns[i - 1]).collect();

    let denom: f64 = returns[..n - 1].iter().map(|&r| r * r).sum::<f64>().max(1e-10);
    let beta: f64 = delta.iter().zip(returns.iter()).map(|(&d, &r)| d * r).sum::<f64>() / denom;

    let residuals: Vec<f64> = delta.iter().zip(returns.iter()).map(|(&d, &r)| d - beta * r).collect();
    let df = (residuals.len() as f64 - 2.0).max(1.0);
    let mse = residuals.iter().map(|&e| e * e).sum::<f64>() / df;
    let se = mse.sqrt() / returns[..n - 1].iter().map(|&r| r * r).sum::<f64>().sqrt().max(1e-10);
    let t = beta / se.max(1e-10);

    if t >= 0.0 {
        return 1.0; // Non-stationary direction
    }
    let abs_t = t.abs();
    if abs_t > 4.0 { 0.0 } else if abs_t > 2.5 { 0.01 } else if abs_t > 2.0 { 0.05 } else if abs_t > 1.5 { 0.15 } else { 0.5 }
}

/// KPSS (Kwiatkowski-Phillips-Schmidt-Shin).
///
/// H0: stationary.  Returns p-value; higher → stationary.
fn kpss_test(returns: &[f64]) -> f64 {
    let n = returns.len();
    if n < 10 {
        return 0.5;
    }

    let mean = returns.iter().sum::<f64>() / n as f64;
    let mut partial_sum = 0.0;
    let mut sum_sq = 0.0;
    for &r in returns {
        partial_sum += r - mean;
        sum_sq += partial_sum * partial_sum;
    }

    let variance = returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / n as f64;
    let lm = sum_sq / (variance * (n * n) as f64).max(1e-10);

    if lm < 0.347 { 0.90 } else if lm < 0.463 { 0.70 } else if lm < 0.739 { 0.50 } else if lm < 1.0 { 0.20 } else { 0.05 }
}

/// IID = 1 − mean(|ACF_1..5|) − 0.5·mean(|ACF²_1..5|)
///
/// Penalises autocorrelation in both returns and squared returns.
pub fn compute_iid(returns: &[f64]) -> f64 {
    if returns.len() < 20 {
        return 0.5;
    }

    let max_lag = 5.min(returns.len() / 4);
    if max_lag == 0 {
        return 0.5;
    }

    let squared: Vec<f64> = returns.iter().map(|&r| r * r).collect();
    let mut acf_sum = 0.0;
    let mut acf_sq_sum = 0.0;

    for lag in 1..=max_lag {
        acf_sum += acf(returns, lag).abs();
        acf_sq_sum += acf(&squared, lag).abs();
    }

    let mean_acf = acf_sum / max_lag as f64;
    let mean_acf_sq = acf_sq_sum / max_lag as f64;

    (1.0 - mean_acf - 0.5 * mean_acf_sq).clamp(0.0, 1.0)
}

/// HOMO = 1 − CV(fold_variances)
///
/// Must be called with fold_variances from at least 2 folds.
/// CV of a single-element slice is undefined - this is an aggregate metric.
pub fn compute_homo(fold_variances: &[f64]) -> f64 {
    if fold_variances.len() < 2 {
        return 0.5;
    }
    let c = cv(fold_variances);
    (1.0 - c.min(1.0)).max(0.0)
}

/// NORM = 1 − 0.1·|skew| − 0.05·|excess_kurtosis| (capped)
pub fn compute_norm(returns: &[f64]) -> f64 {
    if returns.len() < 10 {
        return 0.5;
    }

    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / n;
    let std = variance.sqrt().max(1e-10);

    let skew = returns.iter().map(|&r| ((r - mean) / std).powi(3)).sum::<f64>() / n;
    let kurt = returns.iter().map(|&r| ((r - mean) / std).powi(4)).sum::<f64>() / n - 3.0;

    (1.0 - 0.1 * skew.abs() - 0.05 * kurt.abs().min(5.0)).clamp(0.0, 1.0)
}

/// ROBUST = 1 − CV(STAT) − 0.5·CV(IID)
///
/// Measures whether the statistical properties themselves are stable across
/// time periods.  Lower → regime-changing market → bad for ML generalisation.
fn compute_robust(stats: &[f64], iids: &[f64]) -> f64 {
    let cv_stat = cv(stats);
    let cv_iid = cv(iids);
    (1.0 - cv_stat.min(1.0) - 0.5 * cv_iid.min(1.0)).max(0.0)
}

// ── Hard gate helpers ─────────────────────────────────────────────────────────

/// Bars per calendar day.
pub fn compute_density_bars_per_day(n_bars: usize, duration_ms: i64) -> f64 {
    let ms_per_day = 24.0 * 3_600_000.0;
    let days = duration_ms as f64 / ms_per_day;
    if days < 0.1 {
        return 0.0;
    }
    n_bars as f64 / days
}

/// Normalised reversal delay: fraction of a 50-bar maximum.
///
/// 0 = Renko reverses immediately at turning points.
/// 1 = Renko takes 50+ bars to confirm a reversal.
pub fn compute_reversal_delay_lag(bars: &[Bar], window: usize) -> f64 {
    if bars.len() < window * 2 {
        return 0.5;
    }

    let mut turning_points: Vec<(usize, i8)> = Vec::new();
    for i in window..bars.len().saturating_sub(window) {
        let pivot_close = bars[i].close;
        let is_peak = (i - window..i).all(|j| bars[j].close <= pivot_close)
            && (i + 1..=i + window).all(|j| bars[j].close <= pivot_close);
        let is_trough = (i - window..i).all(|j| bars[j].close >= pivot_close)
            && (i + 1..=i + window).all(|j| bars[j].close >= pivot_close);
        if is_peak {
            turning_points.push((i, 1));
        } else if is_trough {
            turning_points.push((i, -1));
        }
    }

    if turning_points.is_empty() {
        return 0.5;
    }

    let mut delays: Vec<usize> = Vec::new();
    for &(tp_idx, tp_dir) in &turning_points {
        for j in tp_idx..bars.len().min(tp_idx + 100) {
            let dir: i8 = if bars[j].is_bullish() { 1 } else { -1 };
            if dir == tp_dir {
                delays.push(j - tp_idx);
                break;
            }
        }
    }

    if delays.is_empty() {
        return 1.0;
    }

    let mean_delay = delays.iter().sum::<usize>() as f64 / delays.len() as f64;
    (mean_delay / 50.0).min(1.0)
}

