use crate::sources::common::*;
use crate::sources::{provider_id_for, series_key, split_quote, TickSource};
use crate::types::{Config, TickFrame};
use anyhow::Result;
use chrono::Datelike;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Gate.io USDT-margined perps (`download.gatedata.org/futures_usdt/trades`).
/// Perp-only: the spot archive is not wired here.
const MONTHLY: &str = "https://download.gatedata.org/futures_usdt/trades/";

pub struct GateioSource {
    agent: Arc<btr_rpc::Http>,
}

impl GateioSource {
    pub fn new() -> Self {
        Self {
            agent: http_agent(),
        }
    }

    /// Rows: timestamp (epoch seconds, fractional), id, price, size. `size` is a
    /// signed contract count (negative = taker sell); `quanto` is base units per
    /// contract. Header-tolerant.
    fn parse_csv(csv_data: &[u8], ticker_id: u64, quanto: f64) -> Result<Vec<TickFrame>> {
        csv_ticks(csv_data, ticker_id, "gateio", |r| {
            let (price, size): (f64, f64) = (field(r, 2)?, field(r, 3)?);
            Some((
                secs_to_us(field(r, 0)?)?,
                price,
                size.abs() * quanto * price,
                size > 0.0,
            ))
        })
    }

    /// Base units per contract from the public contracts endpoint (the archive
    /// rows carry contracts).
    async fn quanto_multiplier(&self, contract: &str) -> Result<f64> {
        let url = format!("https://api.gateio.ws/api/v4/futures/usdt/contracts/{contract}");
        let reply = self.agent.get(&url, &[]).await?;
        anyhow::ensure!(
            reply.status == 200,
            "Gate {contract}: contracts HTTP {}",
            reply.status
        );
        let j: serde_json::Value = serde_json::from_slice(&reply.body)?;
        j["quanto_multiplier"]
            .as_str()
            .and_then(|v| v.parse().ok())
            .filter(|v: &f64| *v > 0.0)
            .ok_or_else(|| anyhow::anyhow!("Gate {contract}: no quanto_multiplier in reply"))
    }
}

#[async_trait::async_trait]
impl TickSource for GateioSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<TickFrame>>) -> Result<()> {
        let (quote, perp) = split_quote(&config.quote);
        anyhow::ensure!(
            perp && quote.eq_ignore_ascii_case("USDT"),
            "gateio: only USDT-margined perps (--quote USDT:PERP)"
        );
        let sym = format!("{}_USDT", config.base.to_uppercase());
        let (dir, tid) = series_key(config);
        let quanto = self.quanto_multiplier(&sym).await?;
        info!(
            "Fetching Gate data for {} (quanto_multiplier {})",
            sym, quanto
        );
        let now = chrono::Utc::now().date_naive();
        if config.to.date_naive() >= now.with_day(1).unwrap() {
            warn!(
                "gateio publishes monthly files only: the current month ({}) yields nothing",
                now.format("%Y-%m")
            );
        }
        let parse = |c: &[u8], t: u64| Self::parse_csv(c, t, quanto);
        let files = fetch_monthly_daily(
            &self.agent,
            config,
            "gateio",
            &sym,
            &dir,
            tid,
            ".csv.gz",
            Compression::Gzip,
            |s, y, m| {
                let f = format!("{}-{:04}{:02}.csv.gz", s, y, m);
                (format!("{}{:04}{:02}/{}", MONTHLY, y, m, f), f)
            },
            |_, _| (String::new(), String::new()),
            &parse,
        )
        .await?;
        info!("Processing {} tick files", files.len());
        fetch_cached_ticks(&files, provider_id_for("gateio"), tx).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Signed contract counts: sign is the taker side, quanto scales to base.
    #[test]
    fn signed_contracts_to_side_notional_and_time() {
        let csv = "1704067200.692228,249314541,42317.5,11\n\
                   1704067204.158683,249314566,42323.3,-79\n\
                   1704067205.1,249314567";
        let t = GateioSource::parse_csv(csv.as_bytes(), 7, 0.0001).unwrap();
        assert_eq!(t.len(), 2, "short row skipped");
        assert_eq!(({ t[0].body.vask }, { t[0].body.vbid }), (46, 0));
        assert_eq!(({ t[1].body.vask }, { t[1].body.vbid }), (0, 334));
        assert!((t[0].timestamp_ms() - 1_704_067_200_692).abs() <= 1);
        assert!((t[1].timestamp_ms() - 1_704_067_204_159).abs() <= 1);
        let hdr = format!("timestamp,id,price,size\n{csv}");
        assert_eq!(
            GateioSource::parse_csv(hdr.as_bytes(), 7, 1.0)
                .unwrap()
                .len(),
            2
        );
    }
}
