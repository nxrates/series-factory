use crate::sources::common::*;
use crate::sources::TickSource;
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct OKXSource { agent: Arc<ureq::Agent> }

impl OKXSource {
    pub fn new() -> Self { Self { agent: http_agent() } }

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        for result in rdr.records() {
            let r = result?;
            // OKX: instrument_name, trade_id, side, price, size, created_time
            let price: f64 = r[3].parse()?;
            let size: f64 = r[4].parse()?;
            let is_buyer = r[2].eq_ignore_ascii_case("buy");
            let ts = normalize_timestamp_ms(r[5].parse()?);
            ticks.push(TickFrame::new(0,
                mitch::timestamp::from_epoch_ms(ts),
                infer_tick(ticker_id, price, (size * price) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for OKXSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        let sym = format!("{}-{}", config.base, config.quote);
        let dir = format!("{}{}", config.base, config.quote);
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching OKX data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        let files = fetch_monthly_daily(
            &self.agent, config, "okx", &sym, &dir, tid, ".zip", Compression::Zip,
            |s, y, m| {
                let f = format!("{}-trades-{:04}-{:02}.zip", s, y, m);
                (format!("https://static.okx.com/cdn/okex/traderecords/trades/monthly/{:04}{:02}/{}", y, m, f), f)
            },
            |s, d| {
                let f = format!("{}-trades-{}.zip", s, d.format("%Y-%m-%d"));
                (format!("https://static.okx.com/cdn/okex/traderecords/trades/daily/{}/{}", d.format("%Y%m%d"), f), f)
            },
            &parse,
        ).await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, tx).await;
        Ok(())
    }
}
