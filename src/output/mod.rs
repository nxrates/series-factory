//! MITCH `.bars` binary output.
//!
//! Zero-copy atomic write of a `&[Bar]` slice to disk. Each record is a
//! 128 B `mitch::bar::Bar` (`#[repr(C, packed)]`, `Pod + Zeroable`), matching
//! the on-wire and mmap format consumed by btr-ml / btr-runtime (see
//! `btr/prime/crates/ml/src/barfile.rs`).
//!
//! Uses `nxr_sdk::ipc::write_atomic` (tmp + rename) so that any reader tailing
//! a prior version of the file continues reading the old inode until it
//! re-opens, instead of observing a truncated-then-rewritten buffer.

use crate::types::{Bar, Config};
use anyhow::Result;
use std::path::PathBuf;

#[derive(Default)]
pub struct OutputWriter;

impl OutputWriter {
    #[must_use]
    pub fn new() -> Self { Self }

    pub async fn write_bars(&self, config: &Config, bars: &[Bar]) -> Result<PathBuf> {
        if bars.is_empty() {
            anyhow::bail!("no bars to write");
        }

        let output_path = config.bars_dir.join(self.generate_filename(config));
        // Batch writer: atomic tmp+rename keeps open FDs reading the old inode.
        let path_for_blocking = output_path.clone();
        let bars_owned: Vec<Bar> = bars.to_vec();
        tokio::task::spawn_blocking(move || {
            nxr_sdk::ipc::write_atomic::<Bar>(&path_for_blocking, &bars_owned)
        })
        .await??;

        Ok(output_path)
    }

    fn generate_filename(&self, config: &Config) -> String {
        let from_date = config.from.format("%Y%m%d");
        let to_date = config.to.format("%Y%m%d");
        let sources = config.sources.join("|");
        let mode = config.agg_mode.to_string();
        let step = config.agg_step as u64;
        format!(
            "{}-{}_{}_{}-{}_{}-{}.bars",
            config.base.to_lowercase(),
            config.quote.to_lowercase(),
            sources,
            from_date,
            to_date,
            mode,
            step
        )
    }
}
