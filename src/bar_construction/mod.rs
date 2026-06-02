//! Offline bar construction: `.vol` builder (Rogers-Satchell sigma over s10
//! OHLC) and Renko `multiplier` calibration helpers.
//!
//! Streaming Renko engine, the MTF σ blender, and grid utilities live in
//! `nxr_sdk`. This module retains only the offline-only bits (calibrate,
//! `.vol` writer).

pub mod calibrate;
pub mod vol_builder;

pub use calibrate::{
    calibrate_mtf, calibrate_mtf_walkforward, calibrate_mtf_with_target,
    count_bars_from_prices, count_bars_per_day_from_prices, CalibrationConfig, DailyBpdStats,
};
pub use vol_builder::{
    build_vol_from_mid_ticks, build_vol_from_s10, write_vol_records_from_ohlc, S10ShardIter,
};
