use crate::types::{AggregationMode, Config, DataSource, GenerativeModel};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about = "Generate aggregated time series from tick data")]
pub struct Args {
    /// Base asset symbol (e.g., BTC)
    #[arg(long, default_value = "BTC")]
    pub base: String,

    /// Quote asset symbol (e.g., USDT)
    #[arg(long, default_value = "USDT")]
    pub quote: String,

    /// Data sources (exchange names or synthetic models, pipe-delimited)
    /// Examples: "binance", "binance|bybit", "gbm(0.0001,0.02,100.0)"
    #[arg(long, default_value = "binance", value_delimiter = '|')]
    pub sources: Vec<String>,

    /// Start date/time (ISO format or relative: now, yesterday, 7-days-ago, 30-days-ago)
    #[arg(long, default_value = "30-days-ago")]
    pub from: String,

    /// End date/time (ISO format or relative)
    #[arg(long, default_value = "yesterday")]
    pub to: String,

    /// Aggregation mode: time (time-based buckets) or tick (price-based buckets)
    #[arg(long, default_value = "time")]
    pub agg_mode: String,

    /// Aggregation step (milliseconds for time mode, ratio for tick mode)
    #[arg(long, default_value = "1000")]
    pub agg_step: f64,

    /// Maximum price deviation for outlier filtering (ratio)
    #[arg(long, default_value = "0.05")]
    pub tick_max_deviation: f64,

    /// Cache directory for downloaded data
    #[arg(long, default_value = "./cache")]
    pub cache_dir: PathBuf,

    /// Output directory for generated files
    #[arg(long, default_value = "./output")]
    pub output_dir: PathBuf,
}

impl Args {
    pub fn into_config(self) -> Result<Config> {
        let from = parse_datetime(&self.from)?;
        let to = parse_datetime(&self.to)?;

        let agg_mode = match self.agg_mode.as_str() {
            "tick" => AggregationMode::Tick,
            "time" => AggregationMode::Time,
            other => return Err(anyhow!("Invalid aggregation mode: '{}'. Use 'time' or 'tick'", other)),
        };

        Ok(Config {
            base: self.base,
            quote: self.quote,
            sources: self.sources,
            from,
            to,
            agg_mode,
            agg_step: self.agg_step,
            tick_max_deviation: self.tick_max_deviation,
            cache_dir: self.cache_dir,
            output_dir: self.output_dir,
        })
    }
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    use chrono::{Duration, NaiveTime};
    
    let now = Utc::now();
    
    match s {
        "now" | "today" => Ok(now.date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "yesterday" => Ok((now - Duration::days(1)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "7-days-ago" => Ok((now - Duration::days(7)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "30-days-ago" => Ok((now - Duration::days(30)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "90-days-ago" => Ok((now - Duration::days(90)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        _ => {
            // Try to parse as regular date
            Ok(DateTime::parse_from_str(&format!("{} 00:00:00 +0000", s), "%Y-%m-%d %H:%M:%S %z")?
                .with_timezone(&Utc))
        }
    }
}

/// Parse a data source string (exchange name or synthetic model)
/// Examples: "binance", "gbm(0.0001,0.02,100.0)", "fbm(0.0001,0.02,0.7,100.0)"
pub fn parse_data_source(source: &str) -> Result<DataSource> {
    // Check if it's a generative model (starts with known model names)
    if let Some(params_str) = source.strip_prefix("gbm(") {
        let params_str = params_str.strip_suffix(')').ok_or_else(|| anyhow!("Missing closing ) in gbm"))?;
        let parts: Vec<&str> = params_str.split(',').collect();
        if parts.len() != 3 {
            return Err(anyhow!("gbm requires 3 parameters: mu,sigma,base"));
        }
        Ok(DataSource::Synthetic(GenerativeModel::GBM {
            mu: parts[0].trim().parse()?,
            sigma: parts[1].trim().parse()?,
            base: parts[2].trim().parse()?,
        }))
    } else if let Some(params_str) = source.strip_prefix("fbm(") {
        let params_str = params_str.strip_suffix(')').ok_or_else(|| anyhow!("Missing closing ) in fbm"))?;
        let parts: Vec<&str> = params_str.split(',').collect();
        if parts.len() != 4 {
            return Err(anyhow!("fbm requires 4 parameters: mu,sigma,hurst,base"));
        }
        Ok(DataSource::Synthetic(GenerativeModel::FBM {
            mu: parts[0].trim().parse()?,
            sigma: parts[1].trim().parse()?,
            hurst: parts[2].trim().parse()?,
            base: parts[3].trim().parse()?,
        }))
    } else if let Some(params_str) = source.strip_prefix("hm(") {
        let params_str = params_str.strip_suffix(')').ok_or_else(|| anyhow!("Missing closing ) in hm"))?;
        let parts: Vec<&str> = params_str.split(',').collect();
        if parts.len() != 7 {
            return Err(anyhow!("hm requires 7 parameters: mu,sigma,kappa,theta,xi,rho,base"));
        }
        Ok(DataSource::Synthetic(GenerativeModel::Heston {
            mu: parts[0].trim().parse()?,
            sigma: parts[1].trim().parse()?,
            kappa: parts[2].trim().parse()?,
            theta: parts[3].trim().parse()?,
            xi: parts[4].trim().parse()?,
            rho: parts[5].trim().parse()?,
            base: parts[6].trim().parse()?,
        }))
    } else if let Some(params_str) = source.strip_prefix("njdm(") {
        let params_str = params_str.strip_suffix(')').ok_or_else(|| anyhow!("Missing closing ) in njdm"))?;
        let parts: Vec<&str> = params_str.split(',').collect();
        if parts.len() != 6 {
            return Err(anyhow!("njdm requires 6 parameters: mu,sigma,lambda,mu_jump,sigma_jump,base"));
        }
        Ok(DataSource::Synthetic(GenerativeModel::NormalJumpDiffusion {
            mu: parts[0].trim().parse()?,
            sigma: parts[1].trim().parse()?,
            lambda: parts[2].trim().parse()?,
            mu_jump: parts[3].trim().parse()?,
            sigma_jump: parts[4].trim().parse()?,
            base: parts[5].trim().parse()?,
        }))
    } else if let Some(params_str) = source.strip_prefix("dejdm(") {
        let params_str = params_str.strip_suffix(')').ok_or_else(|| anyhow!("Missing closing ) in dejdm"))?;
        let parts: Vec<&str> = params_str.split(',').collect();
        if parts.len() != 7 {
            return Err(anyhow!("dejdm requires 7 parameters: mu,sigma,lambda,mu_pos_jump,mu_neg_jump,p_neg_jump,base"));
        }
        Ok(DataSource::Synthetic(GenerativeModel::DoubleExpJumpDiffusion {
            mu: parts[0].trim().parse()?,
            sigma: parts[1].trim().parse()?,
            lambda: parts[2].trim().parse()?,
            mu_pos_jump: parts[3].trim().parse()?,
            mu_neg_jump: parts[4].trim().parse()?,
            p_neg_jump: parts[5].trim().parse()?,
            base: parts[6].trim().parse()?,
        }))
    } else {
        // It's an exchange name
        Ok(DataSource::Exchange(source.to_string()))
    }
}
