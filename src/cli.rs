//! Shared CLI flatten-args for offline bins.
//!
//! Replaces the duplicate `data_root: PathBuf` declaration that previously
//! appeared in every offline binary that takes a data-root override. Bins
//! `#[clap(flatten)]` this into their `Args` struct and read it back as
//! `args.common.data_root`.
//!
//! Lives in `series-factory` (rather than `nxr_sdk`) because the SDK does
//! not depend on `clap`; adding it just for a 2-line struct would bloat
//! every consumer's compile time.

use clap::Args;
use std::path::PathBuf;

/// Args shared by every offline bin that reads from `<data_root>`.
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// Root directory holding `indexes/` and `bars/`.
    #[arg(long, default_value = "/data")]
    pub data_root: PathBuf,
}
