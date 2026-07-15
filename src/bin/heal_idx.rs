//! heal-idx — repair sharded `.idx` in place (sort, resample to 200 ms, re-shard).
//!
//! Usage:
//!   heal-idx --ticker-id 435315775907037184 --data-root /data [--dry-run] [--commit]

use anyhow::Result;
use clap::Parser;
use series_factory::idx_heal::heal_ticker_shards;
use tracing::info;

#[derive(Parser, Debug)]
#[command(about = "Repair sharded .idx: sort, resample to 200ms, re-partition by UTC date.")]
struct Args {
    #[arg(long)]
    ticker_id: u64,

    #[clap(flatten)]
    common: series_factory::cli::CommonArgs,

    #[arg(long, default_value = "200")]
    target_ms: i64,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    commit: bool,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();
    if args.commit && args.dry_run {
        anyhow::bail!("--commit and --dry-run are mutually exclusive");
    }
    let rep = heal_ticker_shards(
        &args.common.data_root,
        args.ticker_id,
        args.target_ms,
        args.dry_run,
        args.commit,
    )?;
    info!(
        ticker_id = rep.ticker_id,
        records_in = rep.records_in,
        records_out = rep.records_out,
        shards_in = rep.shards_in,
        shards_out = rep.shards_out,
        misrouted_dropped = rep.misrouted_dropped,
        source_ms = rep.source_ms_detected,
        target_ms = rep.target_ms,
        resampled = rep.resampled,
        "heal-idx complete"
    );
    if !args.dry_run && !args.commit {
        eprintln!("Wrote staging only — re-run with --commit to swap into place");
    }
    Ok(())
}
