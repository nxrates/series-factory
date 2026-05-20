//! Price grid snapping helpers for deterministic bar boundaries.
//!
//! Ensures bricks land on consistent price levels regardless of aggregation
//! start time. Max rounding error is ~0.15% of value, well below Parkinson
//! vol (1 to 3% per bar).

/// Snap a positive value to a 4-significant-figure grid with 2/5 multiples.
///
/// Algorithm:
///   1. Compute the unit at the 4th significant figure: u = 10^(floor(log10(x)) - 3)
///   2. Try two candidate grid steps: 2*u and 5*u
///   3. Snap to whichever grid gives less rounding error
///
/// Examples:
///   snap_to_25_grid(174.38)  = 174.4
///   snap_to_25_grid(347.83)  = 347.8
///   snap_to_25_grid(84234.0) = 84240.0
///   snap_to_25_grid(0.00347) = 0.00347
#[inline]
pub fn snap_to_25_grid(value: f64) -> f64 {
    if value <= 0.0 || !value.is_finite() {
        return value;
    }
    let d = value.log10().floor();
    let unit = 10f64.powf(d - 3.0);
    let step2 = unit * 2.0;
    let step5 = unit * 5.0;
    let snapped2 = (value / step2).round() * step2;
    let snapped5 = (value / step5).round() * step5;
    if (value - snapped2).abs() <= (value - snapped5).abs() {
        snapped2
    } else {
        snapped5
    }
}

/// Grid step for a brick size snapped via `snap_to_25_grid`.
///
/// Always returns the finer 2*unit step so brick boundaries land on
/// consistent levels regardless of aggregation start time.
#[inline]
pub fn grid_step_for_brick(brick_size: f64) -> f64 {
    if brick_size <= 0.0 || !brick_size.is_finite() {
        return 1.0;
    }
    let d = brick_size.log10().floor();
    10f64.powf(d - 3.0) * 2.0
}

/// Snap a price to the nearest multiple of `step`.
#[inline]
pub fn snap_to_grid(price: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return price;
    }
    (price / step).round() * step
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snap_25_grid_examples() {
        assert!((snap_to_25_grid(174.38) - 174.4).abs() < 1e-10);
        assert!((snap_to_25_grid(347.83) - 347.8).abs() < 1e-10);
        assert!((snap_to_25_grid(84234.0) - 84240.0).abs() < 1e-10);
        assert!((snap_to_25_grid(0.00347) - 0.00347).abs() < 1e-10);
        assert!((snap_to_25_grid(12.34) - 12.34).abs() < 1e-10);
        assert!((snap_to_25_grid(1000.0) - 1000.0).abs() < 1e-10);
        assert!((snap_to_25_grid(7.53) - 7.530).abs() < 1e-10);
        assert!((snap_to_25_grid(7.531) - 7.530).abs() < 1e-10);
    }

    #[test]
    fn grid_step_examples() {
        let g = grid_step_for_brick(174.4);
        assert!((g - 0.2).abs() < 1e-10);
        assert!((snap_to_grid(79823.45, g) - 79823.4).abs() < 1e-10);

        let g2 = grid_step_for_brick(84240.0);
        assert!((g2 - 20.0).abs() < 1e-10);
        assert!((snap_to_grid(84234.0, g2) - 84240.0).abs() < 1e-10);
    }
}
