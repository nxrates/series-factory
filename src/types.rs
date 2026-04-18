//! Core types for series-factory
//! Now using MITCH protocol Tick format (shared with BTR)

use chrono::{DateTime, Utc};
use std::path::PathBuf;

pub use mitch::TickFrame;
pub use mitch::bar::Bar;

// Constants
pub const STREAMING_BUFFER_SIZE: usize = 100_000;

/// Round a float to 6 significant digits.
#[inline]
pub fn round_to_6_sig_digits(value: f64) -> f64 {
    if value == 0.0 || !value.is_finite() { return value; }
    let magnitude = value.abs().log10().floor();
    let factor = 10.0_f64.powi(5 - magnitude as i32);
    (value * factor).round() / factor
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregationMode {
    Tick,
    Time,
}

impl std::fmt::Display for AggregationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationMode::Tick => write!(f, "tick"),
            AggregationMode::Time => write!(f, "time"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base: String,
    pub quote: String,
    pub sources: Vec<String>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub agg_mode: AggregationMode,
    pub agg_step: f64,
    pub tick_max_deviation: f64,
    pub cache_dir: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Exchange(String),
    Synthetic(GenerativeModel),
}

#[derive(Debug, Clone)]
pub enum GenerativeModel {
    GBM { mu: f64, sigma: f64, base: f64 },
    FBM { mu: f64, sigma: f64, hurst: f64, base: f64 },
    Heston {
        mu: f64,
        sigma: f64,
        kappa: f64,
        theta: f64,
        xi: f64,
        rho: f64,
        base: f64
    },
    NormalJumpDiffusion {
        mu: f64,
        sigma: f64,
        lambda: f64,
        mu_jump: f64,
        sigma_jump: f64,
        base: f64,
    },
    DoubleExpJumpDiffusion {
        mu: f64,
        sigma: f64,
        lambda: f64,
        mu_pos_jump: f64,
        mu_neg_jump: f64,
        p_neg_jump: f64,
        base: f64,
    },
}

