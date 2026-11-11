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
use chrono::NaiveDate;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::sync::mpsc;

#[async_trait]
pub trait TickSource: Send + Sync {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()>;

    /// First and last archive date the source has in `[config.from, config.to]`,
    /// `None` when it has nothing for the series. `Err` is a transport fault
    /// (unknown), never "absent". Default: a source without archives is always available.
    async fn probe(&self, config: &Config) -> Result<Option<(NaiveDate, NaiveDate)>> {
        Ok(Some((config.from.date_naive(), config.to.date_naive())))
    }
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
    let (bare, kind) = nxr_sdk::resolve::split_perp(quote);
    (bare, kind == mitch::common::InstrumentType::PERP)
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
/// Closed set by design: every archive venue is one `ArchiveSource` over its
/// `venue` (layout, symbol spelling, row mapper); spot and perp pick their layout
/// from `split_quote`, and all behaviour beyond that lives in `common`.
pub async fn create_source(source: &DataSource) -> Result<Box<dyn TickSource>> {
    match source {
        DataSource::Exchange(name) => {
            let venue: fn(&Config) -> Result<Option<common::Venue>> = match name.as_str() {
                "binance" => binance::venue,
                "bitget" => bitget::venue,
                "bybit" => bybit::venue,
                "gateio" => gateio::venue,
                "okx" => okx::venue,
                _ => anyhow::bail!(
                    "Unsupported exchange: {} (available: binance, bybit, bitget, okx, gateio)",
                    name
                ),
            };
            Ok(Box::new(common::ArchiveSource::new(venue)))
        }
        DataSource::Synthetic(model) => {
            Ok(Box::new(synthetic::SyntheticSource::new(model.clone())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::common::{csv_ticks, Venue};
    use super::*;
    use crate::types::AggregationMode;
    use chrono::{NaiveDate, Utc};

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

    type VenueFn = fn(&Config) -> Result<Option<Venue>>;

    /// venue x market: (name, constructor, quote, base units per CSV size unit,
    /// the venue's real CSV carrying the same trades).
    ///
    /// The trades are: buy 0.01 BTC @ 50000.0 at 1700000000.5, sell 0.02 BTC
    /// @ 50010.5 at 1700000001.5, and a short/garbage row (plus the header row
    /// where the venue ships one).
    #[rustfmt::skip]
    const CASES: [(&str, VenueFn, &str, f64, &str); 9] = [
        ("binance", binance::venue, "USDT", 1.0,
         "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
          1,50000.0,0.01,1,1,1700000000500,false\n2,50010.5,0.02,2,2,1700000001500,true\n3,500"),
        ("binance", binance::venue, "USDT:PERP", 1.0,
         "1,50000.0,0.01,1,1,1700000000500,false\n2,50010.5,0.02,2,2,1700000001500,true\n3,500"),
        ("bybit", bybit::venue, "USDT", 1.0,
         "id,timestamp,price,volume,side\n1,1700000000500,50000.0,0.01,buy\n\
          2,1700000001500,50010.5,0.02,sell\n3,17000"),
        ("bybit", bybit::venue, "USDT:PERP", 1.0,
         "timestamp,symbol,side,size,price,tickDirection\n\
          1700000000.5,BTCUSDT,Buy,0.01,50000.0,PlusTick\n\
          1700000001.5,BTCUSDT,Sell,0.02,50010.5,MinusTick\n1700000002.5,BTCUSDT,Buy"),
        ("okx", okx::venue, "USDT", 1.0,
         "instrument_name,trade_id,side,price,size,created_time\n\
          BTC-USDT,1,buy,50000.0,0.01,1700000000500\nBTC-USDT,2,sell,50010.5,0.02,1700000001500\n\
          BTC-USDT,3,buy,5"),
        ("okx", okx::venue, "USDT:PERP", 0.01,
         "instrument_name,trade_id,side,price,size,created_time\n\
          BTC-USDT-SWAP,1,buy,50000.0,1,1700000000500\n\
          BTC-USDT-SWAP,2,sell,50010.5,2,1700000001500\nBTC-USDT-SWAP,3,buy,5"),
        ("bitget", bitget::venue, "USDT", 1.0,
         "trade_id,timestamp,price,side,volume(quote),size(base)\n\
          1,1700000000000,50000.0,buy,500.0,0.01\n2,1700000001000,50010.5,sell,1000.21,0.02\n3,17000"),
        ("bitget", bitget::venue, "USDT:PERP", 1.0,
         "trade_id,timestamp,price,side,volume(quote),size(base)\n\
          1,1700000000000,50000.0,buy,500.0,0.01\n2,1700000001000,50010.5,sell,1000.21,0.02\n3,17000"),
        ("gateio", gateio::venue, "USDT:PERP", 0.0001,
         "timestamp,id,price,size\n1700000000.5,1,50000.0,100\n1700000001.5,2,50010.5,-200\n\
          1700000002.5,3"),
    ];

    /// Every adapter, spot and perp, turns the same trades into the same ticks:
    /// timestamp, price, side and quote volume.
    #[test]
    fn every_adapter_yields_identical_ticks() {
        for (name, venue, quote, scale, csv) in CASES {
            let v = venue(&Config {
                quote: quote.into(),
                ..cfg(quote)
            })
            .unwrap()
            .unwrap();
            let t = csv_ticks(csv.as_bytes(), 7, &v, scale).unwrap();
            let id = format!("{name} {quote}");
            assert_eq!(t.len(), 2, "{id}: header and short rows are skipped");
            let want = [
                (1_700_000_000_500, 50000.0, true, 500),
                (1_700_000_001_500, 50010.5, false, 1000),
            ];
            for (tick, (ms, px, buy, vol)) in t.iter().zip(want) {
                let b = tick.body;
                assert_eq!(
                    tick.timestamp(),
                    mitch::timestamp::from_epoch_ms(ms),
                    "{id} ts"
                );
                assert_eq!((b.bid, b.ask), (px, px), "{id} price");
                let (vbid, vask) = (b.vbid, b.vask);
                assert_eq!(
                    (vbid, vask),
                    if buy { (0, vol) } else { (vol, 0) },
                    "{id} side/volume"
                );
            }
        }
    }

    /// Archive URL of every venue x market x granularity, as served (verified live).
    #[test]
    fn archive_urls() {
        let (first, day) = (
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        );
        let b = "https://data.binance.vision/data";
        let o = "https://static.okx.com/cdn/okex/traderecords/trades";
        let g = "https://img.bitgetimg.com/online/trades";
        #[rustfmt::skip]
        let rows: [(&str, VenueFn, &str, Option<String>, Option<String>); 9] = [
            ("binance", binance::venue, "USDT",
             Some(format!("{b}/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01.zip")),
             Some(format!("{b}/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-05.zip"))),
            ("binance", binance::venue, "USDT:PERP",
             Some(format!("{b}/futures/um/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01.zip")),
             Some(format!("{b}/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-05.zip"))),
            ("bybit", bybit::venue, "USDT",
             Some("https://public.bybit.com/spot/BTCUSDT/BTCUSDT-2024-01.csv.gz".into()),
             Some("https://public.bybit.com/spot/BTCUSDT/BTCUSDT_2024-01-05.csv.gz".into())),
            ("bybit", bybit::venue, "USDT:PERP", None,
             Some("https://public.bybit.com/trading/BTCUSDT/BTCUSDT2024-01-05.csv.gz".into())),
            ("okx", okx::venue, "USDT",
             Some(format!("{o}/monthly/202401/BTC-USDT-trades-2024-01.zip")),
             Some(format!("{o}/daily/20240105/BTC-USDT-trades-2024-01-05.zip"))),
            ("okx", okx::venue, "USDT:PERP",
             Some(format!("{o}/monthly/202401/BTC-USDT-SWAP-trades-2024-01.zip")),
             Some(format!("{o}/daily/20240105/BTC-USDT-SWAP-trades-2024-01-05.zip"))),
            ("bitget", bitget::venue, "USDT", None, Some(format!("{g}/SPBL/BTCUSDT/20240105_001.zip"))),
            ("bitget", bitget::venue, "USDT:PERP", None, Some(format!("{g}/UMCBL/BTCUSDT/20240105_001.zip"))),
            ("gateio", gateio::venue, "USDT:PERP",
             Some("https://download.gatedata.org/futures_usdt/trades/202401/BTC_USDT-202401.csv.gz".into()),
             None),
        ];
        for (name, venue, quote, monthly, daily) in rows {
            let v = venue(&cfg(quote)).unwrap().unwrap();
            assert_eq!(
                v.monthly(first).map(|u| u.0),
                monthly,
                "{name} {quote} monthly"
            );
            assert_eq!(v.daily(day, 1).map(|u| u.0), daily, "{name} {quote} daily");
        }
    }

    /// A market the venue has no archive for is `None`, not an error.
    #[test]
    fn unsupported_markets_are_none() {
        for (venue, quote) in [
            (gateio::venue as VenueFn, "USDT"),
            (gateio::venue, "USDC:PERP"),
            (okx::venue, "EUR:PERP"),
            (bitget::venue, "EUR:PERP"),
        ] {
            assert!(venue(&cfg(quote)).unwrap().is_none(), "{quote}");
        }
    }
}
