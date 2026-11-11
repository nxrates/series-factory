use crate::sources::common::*;
use crate::sources::{provider_id_for, series_key, split_quote, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const PERP_ROOT: &str = "https://img.bitgetimg.com/online/trades/";

pub struct BitgetSource {
    agent: Arc<btr_rpc::Http>,
}

impl BitgetSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    /// Columns: trade_id, timestamp (ms), price, side, volume_quote, size_base.
    /// Spot and perp share them. Header-tolerant.
    fn parse_csv(csv_data: &[u8], ticker_id: u64) -> Result<Vec<TickFrame>> {
        csv_ticks(csv_data, ticker_id, "bitget", |r| {
            let (price, quote): (f64, f64) = (field(r, 2)?, field(r, 4).unwrap_or(0.0));
            let buyer = r.get(3)?.eq_ignore_ascii_case("buy");
            Some((ms_to_us(field(r, 1)?), price, quote, buyer))
        })
    }
}

#[async_trait::async_trait]
impl TickSource for BitgetSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        // Uppercase symbol for URL + dir consistency (see binance.rs).
        let (quote, perp) = split_quote(&config.quote);
        let base = config.base.to_uppercase();
        let (dir, tid) = series_key(config);
        // Perps: same CSV, own product-type path under a fixed root (UMCBL = USDT-margined, CMCBL =
        // USDC-margined, symbol `<BASE>PERP`).
        let (product, sym) = match (perp, quote.to_uppercase().as_str()) {
            (false, q) => ("SPBL", format!("{base}{q}")),
            (true, "USDT") => ("UMCBL", format!("{base}USDT")),
            (true, "USDC") => ("CMCBL", format!("{base}PERP")),
            (true, q) => anyhow::bail!("Bitget perp archive has no {q}-margined product"),
        };
        info!("Fetching Bitget data for {}", sym);

        let ticks_dir = config.ticks_dir.join("bitget").join(&dir);
        let mut files = Vec::new();
        let mut day = config.from.date_naive();
        let end = config.to.date_naive();

        // Daily-only archive URL prefix sourced from YAML
        // `cexs.exchanges.bitget.archive_url_template.daily`.
        let urls = crate::sources::common::archive_urls("bitget");
        let daily_prefix = if perp {
            format!("{PERP_ROOT}{product}/{sym}/")
        } else {
            urls.daily.replace("{sym}", &sym)
        };

        while day <= end {
            let ds = day.format("%Y%m%d").to_string();
            let mut seq = 1;
            loop {
                let filename = format!("{}_{:03}.zip", ds, seq);
                let cache_path = ticks_dir.join(filename.replace(".zip", ".ticks"));
                if cache_path.exists() {
                    files.push(cache_path);
                    seq += 1;
                    continue;
                }
                let url = format!("{}{}", daily_prefix, filename);
                match download_and_convert(
                    &self.agent,
                    &url,
                    &cache_path,
                    tid,
                    Compression::Zip,
                    3,
                    &Self::parse_csv,
                )
                .await
                {
                    // Zero-tick seq file: drop the empty cache and stop scanning
                    // this day's sequence (an empty member means no more trades).
                    Ok(0) => {
                        let _ = std::fs::remove_file(&cache_path);
                        break;
                    }
                    Ok(_) => {
                        files.push(cache_path);
                        seq += 1;
                    }
                    Err(e) => {
                        if !is_missing(&e) {
                            warn!("bitget {sym} {ds}_{seq:03} failed after retries: {e:#}");
                        } else if seq == 1 {
                            debug!("No Bitget data for {}", ds);
                        }
                        break;
                    }
                }
            }
            day += chrono::Duration::days(1);
        }

        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("bitget"), tx).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Perp files use the spot columns; the header row is skipped.
    #[test]
    fn perp_csv_same_columns_as_spot() {
        let csv = "trade_id,timestamp,price,side,volume(quote),size(base)\n\
                   1390652026650378240,1767283200000,87980.5,sell,1161.3426,0.0132\n\
                   139065202999,17672832";
        let t = BitgetSource::parse_csv(csv.as_bytes(), 7).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!({ t[0].body.vbid }, 1161);
        assert!((t[0].timestamp_ms() - 1_767_283_200_000).abs() <= 1);
    }
}
