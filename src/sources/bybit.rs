use crate::sources::common::*;
use crate::sources::{provider_id_for, TickSource};
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
        for result in rdr.records() {
            let r = result?;
            // Bybit: id, timestamp, price, volume, side, [rpi]
            let ts_raw: i64 = match r[1].parse() { Ok(v) => v, Err(_) => continue };
            let price: f64 = match r[2].parse() { Ok(v) => v, Err(_) => continue };
            let qty: f64 = match r[3].parse() { Ok(v) => v, Err(_) => continue };
            let ts = normalize_timestamp_ms(ts_raw);
            let is_buyer = r[4].eq_ignore_ascii_case("buy");
            ticks.push(TickFrame::new(pid,
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
        // Uppercase symbol for URL + dir consistency (see binance.rs).
        let sym = format!("{}{}", config.base.to_uppercase(), config.quote.to_uppercase());
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching Bybit data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        // Archive URL prefixes sourced from YAML
        // `cexs.exchanges.bybit.archive_url_template.{monthly,daily}`
        // (phase 59.R3.C2.O4, 2026-05-30).
        let urls = crate::sources::common::archive_urls("bybit");
        let monthly_prefix = urls.monthly.clone();
        let daily_prefix = urls.daily.clone();
        let files = fetch_monthly_daily(
            &self.agent, config, "bybit", &sym, &sym, tid, ".csv.gz", Compression::Gzip,
            |s, y, m| {
                let f = format!("{}-{:04}-{:02}.csv.gz", s, y, m);
                let url = format!("{}{}", monthly_prefix.replace("{sym}", s), f);
                (url, f)
            },
            |s, d| {
                let f = format!("{}_{}.csv.gz", s, d.format("%Y-%m-%d"));
                let url = format!("{}{}", daily_prefix.replace("{sym}", s), f);
                (url, f)
            },
            &parse,
        ).await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("bybit"), tx).await;
        Ok(())
    }
}
