use crate::sources::common::*;
use crate::sources::{provider_id_for, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct BinanceSource {
    agent: Arc<ureq::Agent>,
}

impl BinanceSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        let pid = provider_id_for("binance");
        // Reuse one StringRecord across rows (read_record reuses its buffer)
        // instead of allocating a fresh record per row via `records()`.
        let mut r = csv::StringRecord::new();
        while rdr.read_record(&mut r)? {
            // Binance aggTrades: id, price, qty, first_id, last_id, timestamp, is_buyer_maker
            let price: f64 = r[1].parse()?;
            let qty: f64 = r[2].parse()?;
            let ts = normalize_timestamp_ms(r[5].parse()?);
            let is_buyer = !r[6].eq_ignore_ascii_case("true"); // is_buyer_maker=true → seller initiated
            ticks.push(TickFrame::new(
                pid,
                mitch::timestamp::from_epoch_ms(ts),
                honest_tick(ticker_id, price, (price * qty) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for BinanceSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        // Uppercase symbol for URL + dir consistency. Binance public archive
        // URLs are uppercase-only (e.g. USDEUSDT not USDeUSDT for Ethena's
        // USDe), and t2i / merge-idx use uppercase BASE+QUOTE conventions on
        // disk.
        let sym = format!(
            "{}{}",
            config.base.to_uppercase(),
            config.quote.to_uppercase()
        );
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching Binance data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        // Archive URL prefixes sourced from YAML
        // `cexs.exchanges.binance.archive_url_template.{monthly,daily}`.
        let urls = crate::sources::common::archive_urls("binance");
        let monthly_prefix = urls.monthly.clone();
        let daily_prefix = urls.daily.clone();
        let files = fetch_monthly_daily(
            &self.agent,
            config,
            "binance",
            &sym,
            &sym,
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
