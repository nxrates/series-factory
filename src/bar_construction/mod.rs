//! Offline bar construction: `.vol` builder (Parkinson sigma from raw HLC)
//! and Renko `multiplier` calibration helpers.
//!
//! Phase 58.L.0 (2026-05-27): the streaming Renko engine + Parkinson MTF
//! blender + grid utilities now live in `nxr_sdk` so live producer and
//! offline pipeline share a single implementation. This module retains only
//! the offline-only bits (calibrate, .vol writer). Re-exports through
//! `nxr_sdk::{renko, parkinson, grid}` keep call-sites short — no shim layer.

pub mod calibrate;
pub mod vol_builder;

pub use calibrate::{
    calibrate_mtf, calibrate_mtf_walkforward, calibrate_mtf_with_target,
    count_bars_from_prices, count_bars_per_day_from_prices, CalibrationConfig, DailyBpdStats,
};
pub use vol_builder::{build_vol_from_hlc, build_vol_from_records, BUCKET_MS as VOL_BUCKET_MS};

// Phase 58.L.0 (2026-05-27): Renko engine + Parkinson MTF + grid helpers
// moved to `nxr_sdk`. Re-export the canonical types here so existing bin/*
// modules continue to compile without churn. These re-exports are
// deliberately the ONLY shim layer — no wrapper types, no shadow methods.
pub use nxr_sdk::{
    grid_step_for_brick, snap_to_25_grid, snap_to_grid,
    MtfParkinsonCalculator, RenkoConfig, RenkoGenerator, TickEmaVolSource, VolConfig, VolSource,
};
