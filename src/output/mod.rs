use crate::types::{Bar, Config};
use anyhow::Result;
use std::path::PathBuf;

#[cfg(feature = "parquet-output")]
use arrow::array::{Float64Array, Float32Array, Int64Array, UInt32Array};
#[cfg(feature = "parquet-output")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "parquet-output")]
use std::fs::File;
#[cfg(feature = "parquet-output")]
use std::sync::Arc;

#[derive(Default)]
pub struct OutputWriter;

impl OutputWriter {
    #[must_use]
    pub fn new() -> Self { Self }

    #[cfg(feature = "parquet-output")]
    pub async fn write_bars(&self, config: &Config, bars: &[Bar]) -> Result<PathBuf> {
        if bars.is_empty() {
            anyhow::bail!("No bars to write");
        }

        std::fs::create_dir_all(&config.output_dir)?;
        let output_path = config.output_dir.join(self.generate_filename(config));

        let timestamps: Vec<i64> = bars.iter().map(|b| b.close_time_ms()).collect();
        let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
        let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
        let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
        let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
        let vbids: Vec<u32> = bars.iter().map(|b| b.vbid).collect();
        let vasks: Vec<u32> = bars.iter().map(|b| b.vask).collect();
        let tick_counts: Vec<u32> = bars.iter().map(|b| b.tick_count).collect();
        let dispersions: Vec<f32> = bars.iter().map(|b| b.dispersion).collect();
        let drifts: Vec<f32> = bars.iter().map(|b| b.drift).collect();
        let vol_imbalances: Vec<f32> = bars.iter().map(|b| b.vol_imbalance).collect();
        let tick_efficiencies: Vec<f32> = bars.iter().map(|b| b.tick_efficiency).collect();
        let log_volumes: Vec<f32> = bars.iter().map(|b| b.log_volume).collect();

        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("vbid", DataType::UInt32, false),
            Field::new("vask", DataType::UInt32, false),
            Field::new("tick_count", DataType::UInt32, false),
            Field::new("dispersion", DataType::Float32, false),
            Field::new("drift", DataType::Float32, false),
            Field::new("vol_imbalance", DataType::Float32, false),
            Field::new("tick_efficiency", DataType::Float32, false),
            Field::new("log_volume", DataType::Float32, false),
        ]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(timestamps)) as _,
                Arc::new(Float64Array::from(opens)) as _,
                Arc::new(Float64Array::from(highs)) as _,
                Arc::new(Float64Array::from(lows)) as _,
                Arc::new(Float64Array::from(closes)) as _,
                Arc::new(UInt32Array::from(vbids)) as _,
                Arc::new(UInt32Array::from(vasks)) as _,
                Arc::new(UInt32Array::from(tick_counts)) as _,
                Arc::new(Float32Array::from(dispersions)) as _,
                Arc::new(Float32Array::from(drifts)) as _,
                Arc::new(Float32Array::from(vol_imbalances)) as _,
                Arc::new(Float32Array::from(tick_efficiencies)) as _,
                Arc::new(Float32Array::from(log_volumes)) as _,
            ],
        )?;

        let file = File::create(&output_path)?;
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None)?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(output_path)
    }

    fn generate_filename(&self, config: &Config) -> String {
        let from_date = config.from.format("%Y%m%d");
        let to_date = config.to.format("%Y%m%d");
        let sources = config.sources.join("|");
        let mode = config.agg_mode.to_string();
        let step = config.agg_step as u64;
        format!("{}-{}_{}_{}-{}_{}-{}.parquet",
            config.base.to_lowercase(), config.quote.to_lowercase(),
            sources, from_date, to_date, mode, step)
    }
}
