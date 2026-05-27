//! Offline bar construction: `.vol` builder (Parkinson sigma from raw HLC)
//! and Renko `multiplier` calibration helpers.
//!
//! Streaming Renko engine, Parkinson MTF blender, and grid utilities live in
//! `nxr_sdk`. This module retains only the offline-only bits (calibrate,
//! `.vol` writer).

pub mod calibrate;
pub mod vol_builder;

pub use calibrate::{
    calibrate_mtf, calibrate_mtf_walkforward, calibrate_mtf_with_target,
    count_bars_from_prices, count_bars_per_day_from_prices, CalibrationConfig, DailyBpdStats,
};
pub use vol_builder::{build_vol_from_hlc, build_vol_from_records, BUCKET_MS as VOL_BUCKET_MS};
