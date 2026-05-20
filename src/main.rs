mod aggregation;
mod cli;
mod display;
mod merge;
mod output;
mod sources;
mod types;

use aggregation::Aggregator;
use cli::{parse_data_source, Args};
use display::display_data_table;
use merge::MergedTickStream;
use output::OutputWriter;
use sources::create_source;
use types::{Bar, Config, DataSource, TickFrame};

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Default capacity of each per-source tick channel. Source tasks produce in
/// `BATCH_SIZE = 10_000` batches, so a capacity of 8 buffers ~80k ticks of
/// headroom per source before the source task blocks. Plenty for ordinary
/// disk-cached replay; bounded enough to avoid pinning gigabytes in memory.
const PER_SOURCE_CHANNEL_CAPACITY: usize = 8;

#[tokio::main]
async fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    // Hard RAM ceiling before any large allocation. A runaway k-way merge
    // over the multi-exchange tick cache once OOM-killed the host; this
    // turns that into a clean allocation failure instead.
    nxr_sdk::memory::apply_safe_cap();

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
    info!("Bars directory: {}", config.bars_dir.display());

    let data_sources: Result<Vec<DataSource>> = config.sources.iter().map(|s| parse_data_source(s)).collect();
    let data_sources = data_sources?;

    let mut tick_sources = Vec::new();
    for data_source in data_sources.iter() {
        tick_sources.push(create_source(data_source).await?);
    }

    // One mpsc per source + fetch task. Each source emits ticks in timestamp
    // order (files are sorted; within-file parsing preserves order). The
    // aggregator consumes from all channels via a min-heap on the head tick
    // of each source, which yields globally sorted ticks regardless of which
    // source's download completes first. No more per-flush `par_sort` and no
    // dependency on a shared buffer size to absorb cross-source arrival
    // skew - both were the root cause of the Jan-1/Jan-2 drift bug.
    let config_arc = std::sync::Arc::new(config.clone());
    let mut source_receivers: Vec<mpsc::Receiver<Vec<TickFrame>>> = Vec::with_capacity(tick_sources.len());
    let mut fetch_tasks = Vec::with_capacity(tick_sources.len());
    for source in tick_sources.into_iter() {
        let (tx, rx) = mpsc::channel::<Vec<TickFrame>>(PER_SOURCE_CHANNEL_CAPACITY);
        source_receivers.push(rx);
        let cfg = config_arc.clone();
        fetch_tasks.push(tokio::spawn(async move {
            if let Err(e) = source.fetch_ticks(&cfg, tx).await {
                error!("Error fetching ticks: {}", e);
            }
        }));
    }

    let (bar_tx, mut bar_rx) = mpsc::channel::<Vec<Bar>>(50);

    let config_for_agg = config_arc.clone();
    let aggregation_task = tokio::spawn(async move {
        let mut aggregator = Aggregator::new((*config_for_agg).clone());
        let mut total_ticks = 0usize;
        let mut merged = MergedTickStream::new(source_receivers).await;
        while let Some(tick) = merged.next().await {
            let batch_bars = aggregator.process_ticks(std::slice::from_ref(&tick));
            total_ticks += 1;
            if !batch_bars.is_empty() {
                let _ = bar_tx.send(batch_bars).await;
            }
        }
        let tail = aggregator.finalize();
        if !tail.is_empty() {
            let _ = bar_tx.send(tail).await;
        }
        info!("Processed {} total ticks via k-way merge", total_ticks);
    });

    let mut all_bars = Vec::new();
    while let Some(bar_batch) = bar_rx.recv().await {
        all_bars.extend(bar_batch);
    }

    for task in fetch_tasks {
        task.await?;
    }
    aggregation_task.await?;

    // Bars may arrive out of order across flushes (tick-mode emits when a
    // band is crossed; Time mode emits on boundary). Sort before dedup so
    // `dedup_by_key` (which only collapses *adjacent* duplicates) sees every
    // pair of equal close timestamps.
    all_bars.sort_by_key(|b| b.close_time_ms());
    all_bars.dedup_by_key(|b| b.close_time_ms());
    all_bars.retain(|b| b.close > 0.0);

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
///
/// Anchors on the actual produced-bar range (`bars.first/last.close_time_ms`)
/// rather than `config.from/to` — the aggregator's outer bucket is anchored
/// on the first *tick*, which may start after `config.from` (e.g. an
/// exchange's history for the pair begins mid-day). Using config-range as
/// the coverage window would either drop leading bars (if `from` is before
/// the first bar's bucket) or synthesize leading flat bars off a zero
/// `last_price` (if the loop advanced past the first real bar).
fn ensure_complete_time_coverage(mut bars: Vec<Bar>, config: &Config) -> Vec<Bar> {
    use crate::types::AggregationMode;

    if config.agg_mode != AggregationMode::Time || bars.is_empty() {
        return bars;
    }

    bars.sort_by_key(|b| b.close_time_ms());

    let step = config.agg_step as i64;
    let first_bar_ms = bars.first().map(|b| b.close_time_ms()).unwrap();
    let last_bar_ms = bars.last().map(|b| b.close_time_ms()).unwrap();

    let mut complete = Vec::new();
    let mut last_price = 0.0;
    let mut bar_iter = bars.into_iter();
    let mut next_bar = bar_iter.next();
    let mut current_time = first_bar_ms;

    while current_time <= last_bar_ms {
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
