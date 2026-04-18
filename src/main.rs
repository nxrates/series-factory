mod aggregation;
mod cli;
mod display;
mod output;
mod sources;
mod types;

use aggregation::Aggregator;
use cli::{parse_data_source, Args};
use display::display_data_table;
use output::OutputWriter;
use sources::create_source;
use types::{Bar, Config, DataSource, TickFrame, STREAMING_BUFFER_SIZE};

use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    nxr_sdk::logging::init("info");

    let num_threads = std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(4)
        .max(4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("rayon-worker-{}", i))
        .build_global()
        .expect("Failed to initialize Rayon thread pool");

    info!("Initialized Rayon thread pool with {} threads", num_threads);

    let args = Args::parse();
    let config = args.into_config()?;

    info!("Starting series factory with config: {:?}", config);
    info!("Output directory: {}", config.output_dir.display());

    let data_sources: Result<Vec<DataSource>> = config.sources.iter().map(|s| parse_data_source(s)).collect();
    let data_sources = data_sources?;

    let mut tick_sources = Vec::new();
    for data_source in data_sources.iter() {
        tick_sources.push(create_source(data_source).await?);
    }

    let (tick_tx, mut tick_rx) = mpsc::channel::<Vec<TickFrame>>(100);
    let (bar_tx, mut bar_rx) = mpsc::channel::<Vec<Bar>>(50);

    let config_arc = std::sync::Arc::new(config.clone());
    let fetch_tasks: Vec<_> = tick_sources
        .into_iter()
        .map(|source| {
            let tx = tick_tx.clone();
            let config = config_arc.clone();
            tokio::spawn(async move {
                if let Err(e) = source.fetch_ticks(&config, tx).await {
                    error!("Error fetching ticks: {}", e);
                }
            })
        })
        .collect();

    drop(tick_tx);

    let config_for_agg = config_arc.clone();
    let aggregation_task = tokio::spawn(async move {
        let mut aggregator = Aggregator::new((*config_for_agg).clone());
        let mut total_ticks = 0;
        let mut tick_buffer = Vec::new();

        while let Some(tick_batch) = tick_rx.recv().await {
            let batch_len = tick_batch.len();
            tick_buffer.extend(tick_batch);
            total_ticks += batch_len;

            if tick_buffer.len() >= STREAMING_BUFFER_SIZE {
                tick_buffer.par_sort_unstable_by_key(|t| t.timestamp_ms());
                let batch_bars = aggregator.process_ticks(&tick_buffer);
                if !batch_bars.is_empty() {
                    let _ = bar_tx.send(batch_bars).await;
                }
                tick_buffer.clear();
            }
        }

        if !tick_buffer.is_empty() {
            tick_buffer.par_sort_unstable_by_key(|t| t.timestamp_ms());
            let batch_bars = aggregator.process_ticks(&tick_buffer);
            if !batch_bars.is_empty() {
                let _ = bar_tx.send(batch_bars).await;
            }
        }

        if let Some(final_bar) = aggregator.finalize() {
            let _ = bar_tx.send(vec![final_bar]).await;
        }

        info!("Processed {} total ticks via streaming aggregation", total_ticks);
    });

    let mut all_bars = Vec::new();
    while let Some(bar_batch) = bar_rx.recv().await {
        all_bars.extend(bar_batch);
    }

    for task in fetch_tasks {
        task.await?;
    }
    aggregation_task.await?;

    // Deduplicate by close timestamp
    all_bars.dedup_by_key(|b| b.close_time_ms());

    // Filter out placeholder bars with zero price
    all_bars.retain(|b| b.close > 0.0);

    // Fill time gaps with flat bars
    all_bars = ensure_complete_time_coverage(all_bars, &config);

    if all_bars.is_empty() {
        warn!("No bars generated. Exiting.");
        return Ok(());
    }

    info!("Generated {} bars", all_bars.len());

    let output_writer = OutputWriter::new();
    let output_path = output_writer.write_bars(&config, &all_bars).await?;

    display_data_table(&all_bars);

    info!("Series factory completed successfully!");
    if let Some(filename) = output_path.file_name() {
        info!("Generated: {}", filename.to_string_lossy());
    }
    info!("Total bars: {}", all_bars.len());

    Ok(())
}

/// Fill time gaps with flat (synthetic) bars for continuous coverage.
fn ensure_complete_time_coverage(mut bars: Vec<Bar>, config: &Config) -> Vec<Bar> {
    use crate::types::AggregationMode;

    if config.agg_mode != AggregationMode::Time || bars.is_empty() {
        return bars;
    }

    bars.sort_by_key(|b| b.close_time_ms());

    let step = config.agg_step as i64;
    let start_time = (config.from.timestamp_millis() / step) * step + step;
    let end_time = (config.to.timestamp_millis() / step) * step + step;

    let mut complete = Vec::new();
    let mut last_price = 0.0;
    let mut bar_iter = bars.into_iter();
    let mut next_bar = bar_iter.next();
    let mut current_time = start_time;

    while current_time <= end_time {
        if let Some(ref bar) = next_bar {
            let bar_ms = bar.close_time_ms();
            if bar_ms == current_time {
                last_price = bar.close;
                complete.push(*bar);
                next_bar = bar_iter.next();
            } else if bar_ms > current_time {
                if last_price > 0.0 {
                    complete.push(nxr_sdk::flat_bar(current_time, last_price));
                }
            } else {
                next_bar = bar_iter.next();
                continue;
            }
        } else if last_price > 0.0 {
            complete.push(nxr_sdk::flat_bar(current_time, last_price));
        }
        current_time += step;
    }

    complete
}
