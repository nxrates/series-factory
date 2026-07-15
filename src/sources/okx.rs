use crate::sources::common::*;
use crate::sources::{provider_id_for, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct OKXSource {
    agent: Arc<ureq::Agent>,
}

impl OKXSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        // Header-tolerant: `has_headers(false)` + parse-or-skip so streaming
        // chunks work whether or not they start on the header row.
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(csv_data));
        let mut ticks = Vec::new();
        let pid = provider_id_for("okx");
        // Reuse one StringRecord across rows (no per-row alloc).
        let mut r = csv::StringRecord::new();
        while rdr.read_record(&mut r)? {
            // OKX: instrument_name, trade_id, side, price, size, created_time
            let price: f64 = match r[3].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let size: f64 = match r[4].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let ts_raw: i64 = match r[5].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let is_buyer = r[2].eq_ignore_ascii_case("buy");
            let ts = normalize_timestamp_ms(ts_raw);
            ticks.push(TickFrame::new(
                pid,
                mitch::timestamp::from_epoch_ms(ts),
                honest_tick(ticker_id, price, (size * price) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for OKXSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        // Uppercase symbol for URL + dir consistency (see binance.rs).
        let sym = format!(
            "{}-{}",
            config.base.to_uppercase(),
            config.quote.to_uppercase()
        );
        let dir = format!(
            "{}{}",
            config.base.to_uppercase(),
            config.quote.to_uppercase()
        );
        let tid = nxr_sdk::resolve_ticker_id(&sym);
        info!("Fetching OKX data for {}", sym);
        let parse: fn(&[u8], u64) -> Result<Vec<TickFrame>> = Self::parse_csv;
        // Archive URL prefixes sourced from YAML
        // `cexs.exchanges.okx.archive_url_template.{monthly,daily}`.
        let urls = crate::sources::common::archive_urls("okx");
        let monthly_prefix = urls.monthly.clone();
        let daily_prefix = urls.daily.clone();
        let files = fetch_monthly_daily(
            &self.agent,
            config,
            "okx",
            &sym,
            &dir,
            tid,
            ".zip",
            Compression::Zip,
            |s, y, m| {
                let f = format!("{}-trades-{:04}-{:02}.zip", s, y, m);
                let url = format!(
                    "{}{}",
                    monthly_prefix
                        .replace("{y:04}", &format!("{:04}", y))
                        .replace("{m:02}", &format!("{:02}", m)),
                    f,
                );
                (url, f)
            },
            |s, d| {
                let f = format!("{}-trades-{}.zip", s, d.format("%Y-%m-%d"));
                let url = format!(
                    "{}{}",
                    daily_prefix.replace("{ds}", &d.format("%Y%m%d").to_string()),
                    f,
                );
                (url, f)
            },
            &parse,
        )
        .await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("okx"), tx).await;
        Ok(())
    }
}
