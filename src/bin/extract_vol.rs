//! Parkinson volatility extraction from MITCH tick files.
//!
//! Reads .ticks files (TickFrame format) and generates hourly
//! Parkinson volatility files (.vol) for Renko bar generation.
//! σ_park = sqrt(ln(H/L)² / (4·ln2)) per hour, EMA-smoothed.

use anyhow::Result;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::fs;
use tracing::info;

use nxr_sdk::parkinson_sigma;
use series_factory::{
    vol_bin::VolWriter,
    read_tick_file,
};

const EMA_PERIOD: usize = 14;

/// Parkinson volatility extractor for MITCH tick data.
pub struct VolExtractor {
    tick_files: Vec<PathBuf>,
    pair_name: String,
}

impl VolExtractor {
    pub fn new(cache_dir: &Path, base: &str, quote: &str) -> Result<Self> {
        let pair_name = format!("{}{}", base, quote);
        let exchange_dir = cache_dir.join("binance").join(&pair_name);

        info!("Looking for tick files in: {}", exchange_dir.display());

        if !exchange_dir.exists() {
            anyhow::bail!("Directory not found: {}", exchange_dir.display());
        }

        let mut files: Vec<PathBuf> = fs::read_dir(&exchange_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ticks"))
            .collect();

        if files.is_empty() {
            anyhow::bail!("No .ticks files found in {}", exchange_dir.display());
        }

        files.sort();
        info!("Found {} tick files", files.len());

        Ok(Self {
            tick_files: files,
            pair_name,
        })
    }

    /// Extract hourly Parkinson volatility from tick data.
    pub fn extract_hourly_vol(&self, output_path: &Path) -> Result<usize> {
        info!("Starting Parkinson vol extraction for {}", self.pair_name);
        info!("Output: {}", output_path.display());

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut writer = VolWriter::create(output_path, &self.pair_name)?;

        // Aggregate ticks by hour
        let mut hourly_data: std::collections::HashMap<i64, HourlyData> = std::collections::HashMap::new();

        for (idx, tick_file) in self.tick_files.iter().enumerate() {
            if idx % 50 == 0 {
                info!("Processing file {}/{}...", idx + 1, self.tick_files.len());
            }

            let ticks = match read_tick_file(tick_file) {
                Ok(t) => t,
                Err(e) => {
                    info!("Skipping file {:?}: {}", tick_file, e);
                    continue;
                }
            };

            for tick in ticks {
                let hour_key = (tick.timestamp_ms() / 3_600_000) * 3_600_000;

                let entry = hourly_data.entry(hour_key).or_insert_with(|| HourlyData {
                    timestamp: hour_key,
                    high: tick.mid_price(),
                    low: tick.mid_price(),
                });

                let mid = tick.mid_price();
                entry.high = entry.high.max(mid);
                entry.low = entry.low.min(mid);
            }
        }

        info!("Processing complete. {} hours of data", hourly_data.len());

        let mut sorted_hours: Vec<_> = hourly_data.into_values().collect();
        sorted_hours.par_sort_by_key(|h| h.timestamp);

        // Compute Parkinson σ per hour, then EMA-smooth
        let alpha = 2.0 / (EMA_PERIOD as f64 + 1.0);
        let mut ema_sigma: Option<f64> = None;
        let mut record_count = 0;

        for hour in &sorted_hours {
            let sigma = parkinson_sigma(hour.high, hour.low);

            // EMA smoothing
            let smoothed = match ema_sigma {
                Some(prev) => alpha * sigma + (1.0 - alpha) * prev,
                None => sigma,
            };
            ema_sigma = Some(smoothed);

            // Write as percentage
            writer.write_record(hour.timestamp, smoothed * 100.0)?;
            record_count += 1;
        }

        writer.finish()?;
        info!("Parkinson vol extraction complete: {} records", record_count);

        Ok(record_count)
    }
}

struct HourlyData {
    timestamp: i64,
    high: f64,
    low: f64,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        eprintln!("Usage: {} <BASE> <QUOTE> <OUTPUT.vol> [CACHE_DIR]", args[0]);
        eprintln!("");
        eprintln!("Arguments:");
        eprintln!("  BASE        - Base currency (e.g., BTC)");
        eprintln!("  QUOTE       - Quote currency (e.g., USDT)");
        eprintln!("  OUTPUT.vol  - Output Parkinson vol file path");
        eprintln!("  CACHE_DIR   - Cache directory (default: ./cache)");
        std::process::exit(1);
    }

    let base = &args[1];
    let quote = &args[2];
    let output_path = PathBuf::from(&args[3]);

    let cache_dir = if args.len() > 4 {
        PathBuf::from(&args[4])
    } else {
        PathBuf::from("./cache")
    };

    let extractor = VolExtractor::new(&cache_dir, base, quote)?;

    let start = std::time::Instant::now();
    let record_count = extractor.extract_hourly_vol(&output_path)?;
    let elapsed = start.elapsed();

    info!("========================================");
    info!("Parkinson Vol Extraction Complete!");
    info!("========================================");
    info!("  Records: {}", record_count);
    info!("  Output: {}", output_path.display());
    info!("  Time: {:.2}s", elapsed.as_secs_f64());
    info!("========================================");

    Ok(())
}
