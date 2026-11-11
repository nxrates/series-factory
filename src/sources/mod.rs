pub mod binance;
pub mod common;
pub mod synthetic;

pub mod bitget;
pub mod bybit;
pub mod gateio;
pub mod okx;

use crate::types::{Config, DataSource, TickFrame};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::sync::mpsc;

#[async_trait]
pub trait TickSource: Send + Sync {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()>;
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

/// Split `Config::quote` into `(bare quote, is_perp)`.
///
/// A perp series spells its quote `USDT:PERP`, the SDK's `BASE/QUOTE:PERP`
/// convention (`nxr_sdk::resolve::PERP_SUFFIX`): the MITCH ticker id carries the
/// PERP instrument nibble, and the raw-tick dir (`BTCUSDT:PERP`) and per-provider
/// `.idx` (`BTC-USDT:PERP.idx`) can never alias the spot series.
pub fn split_quote(quote: &str) -> (&str, bool) {
    (
        nxr_sdk::resolve::split_perp(quote).0,
        nxr_sdk::is_perp_symbol(quote),
    )
}

/// `(raw-tick dir name, MITCH ticker id)` of the series a `Config` names.
pub fn series_key(config: &Config) -> (String, u64) {
    let (base, quote) = (config.base.to_uppercase(), config.quote.to_uppercase());
    let tid = nxr_sdk::resolve_ticker_id(&format!("{base}/{quote}"));
    (format!("{base}{quote}"), tid)
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

/// Construct a historical tick source by enum tag.
///
/// **Closed-set by design (2026-05-30):** each historical source has distinct API + parse logic
/// (Binance aggTrades CSV vs Bybit per-trade CSV vs OKX zipped JSON vs
/// Bitget per-symbol path conventions; each also has a perp variant picked by
/// `split_quote`) that can't be YAML-driven without
/// trait-object infrastructure that doesn't currently exist (a registry of
/// `fn(&Config) -> Box<dyn TickSource>` constructors keyed by name). The
/// match arm itself is a thin dispatcher; the per-source URL/template
/// data has already been hoisted to YAML (`archive_url_template`).
/// Deferred to a future refactor if a 5th
/// historical source is added.
pub async fn create_source(source: &DataSource) -> Result<Box<dyn TickSource>> {
    match source {
        DataSource::Exchange(name) => match name.as_str() {
            "binance" => Ok(Box::new(binance::BinanceSource::new())),
            "bitget" => Ok(Box::new(bitget::BitgetSource::new())),
            "bybit" => Ok(Box::new(bybit::BybitSource::new())),
            "gateio" => Ok(Box::new(gateio::GateioSource::new())),
            "okx" => Ok(Box::new(okx::OKXSource::new())),
            _ => anyhow::bail!(
                "Unsupported exchange: {} (available: binance, bybit, bitget, okx, gateio)",
                name
            ),
        },
        DataSource::Synthetic(model) => {
            Ok(Box::new(synthetic::SyntheticSource::new(model.clone())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggregationMode;
    use chrono::Utc;

    fn cfg(quote: &str) -> Config {
        Config {
            base: "BTC".into(),
            quote: quote.into(),
            sources: vec![],
            from: Utc::now(),
            to: Utc::now(),
            agg_mode: AggregationMode::Time,
            agg_step: 60_000.0,
            cycle_ms: 200,
            stale_secs: 30.0,
            z_threshold: 6.0,
            ticks_dir: Default::default(),
            bars_dir: Default::default(),
        }
    }

    /// Same pair, spot vs perp: different MITCH id and different raw-tick dir
    /// (which also names the per-provider `.idx`), so the series never mix.
    #[test]
    fn perp_and_spot_resolve_to_distinct_identities() {
        let (spot_dir, spot_id) = series_key(&cfg("USDT"));
        let (perp_dir, perp_id) = series_key(&cfg("USDT:PERP"));
        assert_ne!(spot_id, perp_id);
        assert_ne!(spot_dir, perp_dir);
        assert_eq!(perp_dir, "BTCUSDT:PERP");
        assert_eq!(split_quote("USDT:PERP"), ("USDT", true));
        assert_eq!(split_quote("USDT"), ("USDT", false));
        assert_eq!(spot_id, nxr_sdk::resolve_ticker_id("BTCUSDT"));
    }
}
