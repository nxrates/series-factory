pub mod binance;
pub mod common;
pub mod synthetic;

pub mod bitget;
pub mod bybit;
pub mod okx;

use crate::types::{Config, DataSource, TickFrame};
use async_trait::async_trait;
use anyhow::Result;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::sync::mpsc;

#[async_trait]
pub trait TickSource: Send + Sync {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<TickFrame>>,
    ) -> Result<()>;
}

/// Resolve the canonical MITCH provider id for an exchange name.
///
/// Panics at process start if the name is not in the registry - series-factory
/// is a developer tool so a missing ID is a configuration bug, not a runtime
/// condition to soft-handle.
pub fn provider_id_for(exchange: &str) -> u16 {
    nxr_sdk::get_market_provider_id_by_name(exchange)
        .unwrap_or_else(|| panic!("unknown MITCH exchange name: {}", exchange))
}

/// Synthetic provider IDs live above real MITCH providers (< 2000) and below
/// the core aggregator's triangulation range (60000+). Each SyntheticSource
/// instance gets a distinct id so a run with multiple synthetic sources feeds
/// the two-stage aggregator as independent providers.
static SYNTHETIC_ID: AtomicU16 = AtomicU16::new(4000);

/// Allocate the next synthetic provider id.
pub fn next_synthetic_id() -> u16 {
    SYNTHETIC_ID.fetch_add(1, Ordering::Relaxed)
}

pub async fn create_source(source: &DataSource) -> Result<Box<dyn TickSource>> {
    match source {
        DataSource::Exchange(name) => match name.as_str() {
            "binance" => Ok(Box::new(binance::BinanceSource::new())),
            "bitget" => Ok(Box::new(bitget::BitgetSource::new())),
            "bybit" => Ok(Box::new(bybit::BybitSource::new())),
            "okx" => Ok(Box::new(okx::OKXSource::new())),
            _ => anyhow::bail!("Unsupported exchange: {} (available: binance, bybit, bitget, okx)", name),
        },
        DataSource::Synthetic(model) => Ok(Box::new(synthetic::SyntheticSource::new(model.clone()))),
    }
}
