use crate::sources::common::*;
use crate::sources::{provider_id_for, series_key, split_quote, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

const UM_MONTHLY: &str = "https://data.binance.vision/data/futures/um/monthly/aggTrades/{sym}/";
const UM_DAILY: &str = "https://data.binance.vision/data/futures/um/daily/aggTrades/{sym}/";

pub struct BinanceSource {
    agent: Arc<btr_rpc::Http>,
}

impl BinanceSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    /// Binance aggTrades: id, price, qty, first_id, last_id, timestamp, is_buyer_maker.
    /// Header-tolerant: older files have no header row, recent ones do.
    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        csv_ticks(csv_data, ticker_id, "binance", |r| {
            let (price, qty): (f64, f64) = (field(r, 1)?, field(r, 2)?);
            // is_buyer_maker=true → seller initiated
            let buyer = !r.get(6)?.eq_ignore_ascii_case("true");
            Some((ms_to_us(field(r, 5)?), price, price * qty, buyer))
        })
    }
}

#[async_trait::async_trait]
impl TickSource for BinanceSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        // Uppercase symbol for URL + dir consistency. Binance public archive
        // URLs are uppercase-only (e.g. USDEUSDT not USDeUSDT for Ethena's
        // USDe), and t2i / merge-idx use uppercase BASE+QUOTE conventions on
        // disk.
        let (quote, perp) = split_quote(&config.quote);
        let sym = format!("{}{}", config.base.to_uppercase(), quote.to_uppercase());
        let (dir, tid) = series_key(config);
        info!("Fetching Binance data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        // Archive URL prefixes sourced from YAML
        // `cexs.exchanges.binance.archive_url_template.{monthly,daily}`.
        // USD-M perps (`data/futures/um`) share the spot file naming and columns.
        let urls = crate::sources::common::archive_urls("binance");
        let (monthly_prefix, daily_prefix) = if perp {
            (UM_MONTHLY.to_string(), UM_DAILY.to_string())
        } else {
            (urls.monthly.clone(), urls.daily.clone())
        };
        let files = fetch_monthly_daily(
            &self.agent,
            config,
            "binance",
            &sym,
            &dir,
            tid,
            ".zip",
            Compression::Zip,
            |s, y, m| {
                let f = format!("{}-aggTrades-{:04}-{:02}.zip", s, y, m);
                let url = format!("{}{}", monthly_prefix.replace("{sym}", s), f);
                (url, f)
            },
            |s, d| {
                let f = format!("{}-aggTrades-{}.zip", s, d.format("%Y-%m-%d"));
                let url = format!("{}{}", daily_prefix.replace("{sym}", s), f);
                (url, f)
            },
            &parse,
        )
        .await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("binance"), tx).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// USD-M rows, with and without the header row; price*qty -> quote volume.
    #[test]
    fn perp_csv_header_headerless_and_short_rows() {
        let hdr = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n";
        let rows = "18266802,7240.80,0.010,25090490,25090490,1577750402288,false\n\
                    18266803,7240.43,2.366,25090491,25090492,1577750402710,true\n";
        let a = BinanceSource::parse_csv(rows.as_bytes(), 7).unwrap();
        let b = BinanceSource::parse_csv(format!("{hdr}{rows}").as_bytes(), 7).unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(
            b.len(),
            2,
            "header row must be skipped, not poison the chunk"
        );
        assert_eq!(
            (a[0].body.vask, a[0].body.vbid),
            (72, 0),
            "maker=false => buyer-initiated"
        );
        assert_eq!(
            (a[1].body.vask, a[1].body.vbid),
            (0, 17130),
            "maker=true => seller-initiated"
        );
        assert!((a[0].timestamp_ms() - 1_577_750_402_288).abs() <= 1);
        let cut = format!("{rows}18266804,7240.77,0.035,2509");
        assert_eq!(
            BinanceSource::parse_csv(cut.as_bytes(), 7).unwrap().len(),
            2
        );
    }
}
