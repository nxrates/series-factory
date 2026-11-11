use crate::sources::common::*;
use crate::sources::{provider_id_for, series_key, split_quote, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct OKXSource {
    agent: Arc<btr_rpc::Http>,
}

impl OKXSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    /// `ct_val` converts the OKX `size` to base units: 1.0 for spot, the swap's
    /// contract value (`ctVal`, e.g. 0.01 BTC) for perps.
    fn parse_csv(csv_data: &[u8], ticker_id: u64, ct_val: f64) -> Result<Vec<TickFrame>> {
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
                honest_tick(ticker_id, price, (size * ct_val * price) as u32, is_buyer),
            ));
        }
        Ok(ticks)
    }
}

impl OKXSource {
    /// Contract value of a swap (base units per contract) from the public
    /// instruments endpoint. The archive rows carry contracts, not base units.
    async fn swap_ct_val(&self, inst_id: &str) -> Result<f64> {
        let url =
            format!("https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId={inst_id}");
        let reply = self.agent.get(&url, &[]).await?;
        let j: serde_json::Value = serde_json::from_slice(&reply.body)?;
        j["data"][0]["ctVal"]
            .as_str()
            .and_then(|v| v.parse().ok())
            .filter(|v: &f64| *v > 0.0)
            .ok_or_else(|| anyhow::anyhow!("OKX {inst_id}: no ctVal in instruments reply"))
    }
}

#[async_trait::async_trait]
impl TickSource for OKXSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        // Uppercase symbol for URL + dir consistency (see binance.rs).
        let (quote, perp) = split_quote(&config.quote);
        let mut sym = format!("{}-{}", config.base.to_uppercase(), quote.to_uppercase());
        let (dir, tid) = series_key(config);
        let mut ct_val = 1.0;
        if perp {
            sym.push_str("-SWAP");
            ct_val = self.swap_ct_val(&sym).await?;
        }
        info!("Fetching OKX data for {}", sym);
        let parse = |c: &[u8], t: u64| Self::parse_csv(c, t, ct_val);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Swap rows are contracts: notional = size * ctVal * price.
    #[test]
    fn swap_size_scaled_by_ct_val() {
        let csv = "instrument_name,trade_id,side,price,size,created_time\n\
                   BTC-USDT-SWAP,703698429,buy,42800.0,6.0,1704124800014\n";
        let swap = OKXSource::parse_csv(csv.as_bytes(), 7, 0.01).unwrap();
        assert_eq!(swap.len(), 1);
        assert_eq!(
            { swap[0].body.vask },
            2568,
            "6 contracts x 0.01 BTC x 42800"
        );
        let spot = OKXSource::parse_csv(csv.as_bytes(), 7, 1.0).unwrap();
        assert_eq!({ spot[0].body.vask }, 256_800);
    }
}
