//! Bar construction primitives: grid snapping, MTF Parkinson volatility, and
//! adaptive Renko bar generation. Emits `mitch::Bar` directly.

pub mod calibrate;
pub mod grid;
pub mod parkinson;
pub mod renko;

pub use calibrate::{calibrate_mtf, calibrate_mtf_with_target, count_bars_from_prices, CalibrationConfig};
pub use grid::{grid_step_for_brick, snap_to_25_grid, snap_to_grid};
pub use parkinson::{MtfParkinsonCalculator, VolConfig, VolSource};
pub use renko::{RenkoConfig, RenkoGenerator};
