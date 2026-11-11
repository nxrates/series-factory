use crate::sources::common::*;
use crate::sources::{provider_id_for, series_key, split_quote, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

const PERP_DAILY: &str = "https://public.bybit.com/trading/{sym}/";

pub struct BybitSource {
    agent: Arc<btr_rpc::Http>,
}

impl BybitSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    /// Spot: id, timestamp (ms), price, volume, side, [rpi]. Header-tolerant.
    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        csv_ticks(csv_data, ticker_id, "bybit", |r| {
            let (price, qty): (f64, f64) = (field(r, 2)?, field(r, 3)?);
            let buyer = r.get(4)?.eq_ignore_ascii_case("buy");
            Some((ms_to_us(field(r, 1)?), price, qty * price, buyer))
        })
    }

    /// Linear perp `public.bybit.com/trading/`: timestamp (epoch seconds,
    /// fractional), symbol, side, size (base), price, ... Header-tolerant.
    fn parse_perp_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        csv_ticks(csv_data, ticker_id, "bybit", |r| {
            let (qty, price): (f64, f64) = (field(r, 3)?, field(r, 4)?);
            let buyer = r.get(2)?.eq_ignore_ascii_case("buy");
            Some((secs_to_us(field(r, 0)?)?, price, qty * price, buyer))
        })
    }
}

#[async_trait::async_trait]
impl TickSource for BybitSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        // Uppercase symbol for URL + dir consistency (see binance.rs).
        let (quote, perp) = split_quote(&config.quote);
        let base = config.base.to_uppercase();
        // USDC-margined perps are `<BASE>PERP`.
        let sym = if perp && quote.eq_ignore_ascii_case("USDC") {
            format!("{base}PERP")
        } else {
            format!("{base}{}", quote.to_uppercase())
        };
        let (dir, tid) = series_key(config);
        info!("Fetching Bybit data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = if perp {
            Self::parse_perp_csv
        } else {
            Self::parse_csv
        };
        // Archive URL prefixes sourced from YAML
        // `cexs.exchanges.bybit.archive_url_template.{monthly,daily}`.
        // Perps: daily-only `public.bybit.com/trading/<SYM>/<SYM><date>.csv.gz`.
        let urls = crate::sources::common::archive_urls("bybit");
        let monthly_prefix = urls.monthly.clone();
        let daily_prefix = urls.daily.clone();
        let files = fetch_monthly_daily(
            &self.agent,
            config,
            "bybit",
            &sym,
            &dir,
            tid,
            ".csv.gz",
            Compression::Gzip,
            |s, y, m| {
                let f = format!("{}-{:04}-{:02}.csv.gz", s, y, m);
                let url = if perp {
                    String::new()
                } else {
                    format!("{}{}", monthly_prefix.replace("{sym}", s), f)
                };
                (url, f)
            },
            |s, d| {
                let (root, f) = if perp {
                    (PERP_DAILY, format!("{}{}.csv.gz", s, d.format("%Y-%m-%d")))
                } else {
                    (
                        daily_prefix.as_str(),
                        format!("{}_{}.csv.gz", s, d.format("%Y-%m-%d")),
                    )
                };
                let url = format!("{}{}", root.replace("{sym}", s), f);
                (url, f)
            },
            &parse,
        )
        .await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("bybit"), tx).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Linear perp rows: fractional epoch seconds, side word, size in base.
    #[test]
    fn perp_csv_seconds_header_and_short_rows() {
        let csv = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI\n\
                   1793836800.221,BTCUSDT,Sell,0.063,80474.10,MinusTick,x,5.0e11,0.063,5069.8683,0\n\
                   1585180700.0647,BTCUSDT,Buy,0.042,6698.5,PlusTick,y,2.8e10,0.042,281.337\n\
                   1585180701.5,BTCUSDT,Buy\n";
        let t = BybitSource::parse_perp_csv(csv.as_bytes(), 7).unwrap();
        assert_eq!(
            t.len(),
            2,
            "5- and 6-column rows parse; header and short row skipped"
        );
        assert_eq!({ t[0].body.vbid }, 5069);
        assert_eq!({ t[1].body.vask }, 281);
        // mts keeps 16 us resolution: floor, never a rounded-up ms.
        assert!((t[0].timestamp_ms() - 1_793_836_800_221).abs() <= 1);
        assert!((t[1].timestamp_ms() - 1_585_180_700_065).abs() <= 1);
    }

    /// Spot rows: id, ms timestamp, price, volume, side; a cut row never panics.
    #[test]
    fn spot_csv_ms_and_short_rows() {
        let csv = "id,timestamp,price,volume,side\n1,1704153600365,44176,0.5,buy\n2,17041536";
        let t = BybitSource::parse_csv(csv.as_bytes(), 7).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!({ t[0].body.vask }, 22088);
        assert!((t[0].timestamp_ms() - 1_704_153_600_365).abs() <= 1);
    }
}
