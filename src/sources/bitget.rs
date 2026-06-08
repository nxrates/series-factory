use crate::sources::common::*;
use crate::sources::{provider_id_for, TickSource};
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
        // Header-tolerant: `has_headers(false)` + parse-or-skip on the
        // numeric columns, so streaming chunks work regardless of whether
        // the chunk begins on the header row.
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        let pid = provider_id_for("bitget");
        // Reuse one StringRecord across rows (no per-row alloc).
        let mut r = csv::StringRecord::new();
        while rdr.read_record(&mut r)? {
            // Bitget: trade_id, timestamp, price, side, volume_quote, size_base
            let ts: i64 = match r[1].parse() { Ok(v) => v, Err(_) => continue };
            let price: f64 = match r[2].parse() { Ok(v) => v, Err(_) => continue };
            let is_buyer = r[3].eq_ignore_ascii_case("buy");
            let volume: u32 = r[4].parse::<f64>().unwrap_or(0.0) as u32;
            ticks.push(TickFrame::new(pid,
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
        // Uppercase symbol for URL + dir consistency (see binance.rs).
        let sym = format!("{}{}", config.base.to_uppercase(), config.quote.to_uppercase());
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching Bitget data for {}", sym);

        let ticks_dir = config.ticks_dir.join("bitget").join(&sym);
        let mut files = Vec::new();
        let mut day = config.from.date_naive();
        let end = config.to.date_naive();

        // Daily-only archive URL prefix sourced from YAML
        // `cexs.exchanges.bitget.archive_url_template.daily`
        // (phase 59.R3.C3.O1, 2026-05-30).
        let urls = crate::sources::common::archive_urls("bitget");
        let daily_prefix = urls.daily.replace("{sym}", &sym);

        while day <= end {
            let ds = day.format("%Y%m%d").to_string();
            let mut seq = 1;
            loop {
                let filename = format!("{}_{:03}.zip", ds, seq);
                let cache_path = ticks_dir.join(filename.replace(".zip", ".ticks"));
                if cache_path.exists() {
                    files.push(cache_path); seq += 1; continue;
                }
                let url = format!("{}{}", daily_prefix, filename);
                match download_and_convert(&self.agent, &url, &cache_path, tid, Compression::Zip, 1, &Self::parse_csv).await {
                    Ok(_) => { files.push(cache_path); seq += 1; }
                    Err(_) => { if seq == 1 { debug!("No Bitget data for {}", ds); } break; }
                }
            }
            day += chrono::Duration::days(1);
        }

        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("bitget"), tx).await;
        Ok(())
    }
}
