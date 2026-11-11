use crate::sources::common::*;
use crate::sources::{provider_id_for, series_key, split_quote, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
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

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        // Header-tolerant: `has_headers(false)` + parse-or-skip on the first
        // numeric field, so streaming chunks work whether or not they start
        // on the header row. (The monthly archive's first chunk has the
        // header; every subsequent chunk is pure data.)
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true) // Bybit added 'rpi' column mid-2025; rows mix 5 and 6 fields
            .from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        let pid = provider_id_for("bybit");
        // Reuse one StringRecord across rows (no per-row alloc).
        let mut r = csv::StringRecord::new();
        while rdr.read_record(&mut r)? {
            // Bybit: id, timestamp, price, volume, side, [rpi]
            let ts_raw: i64 = match r[1].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let price: f64 = match r[2].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let qty: f64 = match r[3].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let ts = normalize_timestamp_ms(ts_raw);
            let is_buyer = r[4].eq_ignore_ascii_case("buy");
            ticks.push(TickFrame::new(
                pid,
                mitch::timestamp::from_epoch_ms(ts),
                honest_tick(ticker_id, price, (qty * price) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

impl BybitSource {
    /// Linear perp `public.bybit.com/trading/`: timestamp (epoch seconds,
    /// fractional), symbol, side, size (base), price, ... Header-tolerant.
    fn parse_perp_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true) // trailing `RPI` column appears mid-file-set
            .from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        let pid = provider_id_for("bybit");
        let mut r = csv::StringRecord::new();
        while rdr.read_record(&mut r)? {
            let (Ok(ts), Ok(qty), Ok(price)) = (
                r[0].parse::<f64>(),
                r[3].parse::<f64>(),
                r[4].parse::<f64>(),
            ) else {
                continue;
            };
            ticks.push(TickFrame::new(
                pid,
                mitch::timestamp::from_epoch_us((ts * 1e6).round() as u64),
                honest_tick(
                    ticker_id,
                    price,
                    (qty * price) as u32,
                    r[2].eq_ignore_ascii_case("buy"),
                ),
            ));
        }
        Ok(ticks)
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
    fn perp_csv_seconds_and_header() {
        let csv = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI\n\
                   1793836800.221,BTCUSDT,Sell,0.063,80474.10,MinusTick,x,5.0e11,0.063,5069.8683,0\n\
                   1585180700.0647,BTCUSDT,Buy,0.042,6698.5,PlusTick,y,2.8e10,0.042,281.337\n";
        let t = BybitSource::parse_perp_csv(csv.as_bytes(), 7).unwrap();
        assert_eq!(
            t.len(),
            2,
            "5- and 6-column rows both parse, header skipped"
        );
        assert_eq!({ t[0].body.vbid }, 5069);
        assert_eq!({ t[1].body.vask }, 281);
        // mts keeps 16 us resolution: floor, never a rounded-up ms.
        assert!((t[0].timestamp_ms() - 1_793_836_800_221).abs() <= 1);
    }
}
