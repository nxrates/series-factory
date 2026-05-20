//! Bar construction primitives: grid snapping, MTF Parkinson volatility, and
//! adaptive Renko bar generation. Emits `mitch::Bar` directly.

pub mod calibrate;
pub mod grid;
pub mod parkinson;
pub mod renko;
pub mod vol_builder;

pub use calibrate::{calibrate_mtf, calibrate_mtf_with_target, count_bars_from_prices, CalibrationConfig};
pub use grid::{grid_step_for_brick, snap_to_25_grid, snap_to_grid};
pub use parkinson::{MtfParkinsonCalculator, VolConfig, VolSource};
pub use renko::{RenkoConfig, RenkoGenerator};
pub use vol_builder::{build_vol_from_hlc, build_vol_from_records, BUCKET_MS as VOL_BUCKET_MS};
