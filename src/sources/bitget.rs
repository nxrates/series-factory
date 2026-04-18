use crate::sources::common::*;
use crate::sources::TickSource;
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info};

pub struct BitgetSource { agent: Arc<ureq::Agent> }

impl BitgetSource {
    pub fn new() -> Self { Self { agent: http_agent() } }

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        for result in rdr.records() {
            let r = result?;
            // Bitget: trade_id, timestamp, price, side, volume_quote, size_base
            let ts: i64 = r[1].parse()?;
            let price: f64 = r[2].parse()?;
            let is_buyer = r[3].eq_ignore_ascii_case("buy");
            let volume: u32 = r[4].parse::<f64>().unwrap_or(0.0) as u32;
            ticks.push(TickFrame::new(0,
                mitch::timestamp::from_epoch_ms(ts),
                infer_tick(ticker_id, price, volume, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for BitgetSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        let sym = format!("{}{}", config.base, config.quote);
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching Bitget data for {}", sym);

        let cache_dir = config.cache_dir.join("bitget").join(&sym);
        let mut files = Vec::new();
        let mut day = config.from.date_naive();
        let end = config.to.date_naive();

        while day <= end {
            let ds = day.format("%Y%m%d").to_string();
            let mut seq = 1;
            loop {
                let filename = format!("{}_{:03}.zip", ds, seq);
                let cache_path = cache_dir.join(filename.replace(".zip", ".ticks"));
                if cache_path.exists() {
                    files.push(cache_path); seq += 1; continue;
                }
                let url = format!("https://img.bitgetimg.com/online/trades/SPBL/{}/{}", sym, filename);
                match download_and_convert(&self.agent, &url, &cache_path, tid, Compression::Zip, 1, &Self::parse_csv).await {
                    Ok(_) => { files.push(cache_path); seq += 1; }
                    Err(_) => { if seq == 1 { debug!("No Bitget data for {}", ds); } break; }
                }
            }
            day += chrono::Duration::days(1);
        }

        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, tx).await;
        Ok(())
    }
}
