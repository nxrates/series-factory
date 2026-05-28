pub mod bar_construction;
pub mod sampler;
pub mod sharding;
pub mod sources;
pub mod stats;
pub mod types;
pub mod vol_bin;

pub use bar_construction::{
    calibrate_mtf, calibrate_mtf_walkforward, calibrate_mtf_with_target, count_bars_from_prices,
    CalibrationConfig,
};
pub use sources::{create_source, TickSource};
pub use sources::common::read_tick_file;
pub use vol_bin::VolMmap;
pub use types::{AggregationMode, Config, DataSource, TickFrame};

/// Split a `BASE-QUOTE` (or `BASE/QUOTE`) pair string into `(base, quote)`
/// slices, validating both halves are non-empty. Returns `None` on malformed
/// input. Single source of truth for the `bin/*.rs` pair-arg parser.
pub fn split_pair(sym: &str) -> Option<(&str, &str)> {
    let sep_ix = sym.find(|c: char| c == '-' || c == '/')?;
    let base = &sym[..sep_ix];
    let quote = &sym[sep_ix + 1..];
    if base.is_empty() || quote.is_empty() {
        return None;
    }
    Some((base, quote))
}
