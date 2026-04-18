use crate::sources::common::*;
use crate::sources::TickSource;
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct BybitSource { agent: Arc<ureq::Agent> }

impl BybitSource {
    pub fn new() -> Self { Self { agent: http_agent() } }

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true) // Bybit added 'rpi' column mid-2025; rows mix 5 and 6 fields
            .from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        for result in rdr.records() {
            let r = result?;
            // Bybit: id, timestamp, price, volume, side, [rpi]
            let ts = normalize_timestamp_ms(r[1].parse()?);
            let price: f64 = r[2].parse()?;
            let qty: f64 = r[3].parse()?;
            let is_buyer = r[4].eq_ignore_ascii_case("buy");
            ticks.push(TickFrame::new(0,
                mitch::timestamp::from_epoch_ms(ts),
                infer_tick(ticker_id, price, (qty * price) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for BybitSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        let sym = format!("{}{}", config.base, config.quote);
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching Bybit data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        let files = fetch_monthly_daily(
            &self.agent, config, "bybit", &sym, &sym, tid, ".csv.gz", Compression::Gzip,
            |s, y, m| {
                let f = format!("{}-{:04}-{:02}.csv.gz", s, y, m);
                (format!("https://public.bybit.com/spot/{}/{}", s, f), f)
            },
            |s, d| {
                let f = format!("{}_{}.csv.gz", s, d.format("%Y-%m-%d"));
                (format!("https://public.bybit.com/spot/{}/{}", s, f), f)
            },
            &parse,
        ).await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, tx).await;
        Ok(())
    }
}
