//! Core types for series-factory. Re-exports the MITCH protocol Tick/Bar formats.

use chrono::{DateTime, Utc};
use std::path::PathBuf;

pub use mitch::bar::Bar;
pub use mitch::TickFrame;

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
    /// Per-provider index cycle in ms (default 200 = 5 Hz, matching prod forwarders).
    pub cycle_ms: u64,
    /// Stale-provider threshold in seconds (TDWAP half-life clamp upper bound).
    pub stale_secs: f64,
    /// Z-score threshold for per-provider outlier rejection (matches prod forwarder z-gate).
    pub z_threshold: f64,
    pub ticks_dir: PathBuf,
    pub bars_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Exchange(String),
    Synthetic(GenerativeModel),
}

#[derive(Debug, Clone)]
pub enum GenerativeModel {
    GBM {
        mu: f64,
        sigma: f64,
        base: f64,
    },
    FBM {
        mu: f64,
        sigma: f64,
        hurst: f64,
        base: f64,
    },
    Heston {
        mu: f64,
        sigma: f64,
        kappa: f64,
        theta: f64,
        xi: f64,
        rho: f64,
        base: f64,
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
