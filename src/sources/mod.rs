pub mod binance;
pub mod common;
pub mod synthetic;

pub mod bitget;
pub mod bybit;
pub mod okx;

use crate::types::{Config, DataSource, TickFrame};
use async_trait::async_trait;
use anyhow::Result;
use tokio::sync::mpsc;

#[async_trait]
pub trait TickSource: Send + Sync {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<TickFrame>>,
    ) -> Result<()>;
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
