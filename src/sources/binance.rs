use crate::sources::common::*;
use crate::sources::{provider_id_for, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct BinanceSource { agent: Arc<ureq::Agent> }

impl BinanceSource {
    pub fn new() -> Self { Self { agent: http_agent() } }

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        let pid = provider_id_for("binance");
        for result in rdr.records() {
            let r = result?;
            // Binance aggTrades: id, price, qty, first_id, last_id, timestamp, is_buyer_maker
            let price: f64 = r[1].parse()?;
            let qty: f64 = r[2].parse()?;
            let ts = normalize_timestamp_ms(r[5].parse()?);
            let is_buyer = r[6].to_lowercase() != "true"; // is_buyer_maker=true → seller initiated
            ticks.push(TickFrame::new(pid,
                mitch::timestamp::from_epoch_ms(ts),
                infer_tick(ticker_id, price, (price * qty) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for BinanceSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        let sym = format!("{}{}", config.base, config.quote);
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching Binance data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        let files = fetch_monthly_daily(
            &self.agent, config, "binance", &sym, &sym, tid, ".zip", Compression::Zip,
            |s, y, m| {
                let f = format!("{}-aggTrades-{:04}-{:02}.zip", s, y, m);
                (format!("https://data.binance.vision/data/spot/monthly/aggTrades/{}/{}", s, f), f)
            },
            |s, d| {
                let f = format!("{}-aggTrades-{}.zip", s, d.format("%Y-%m-%d"));
                (format!("https://data.binance.vision/data/spot/daily/aggTrades/{}/{}", s, f), f)
            },
            &parse,
        ).await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("binance"), tx).await;
        Ok(())
    }
}
