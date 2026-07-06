//! Build id-keyed `/data/vol/<MITCH_ID>.vol` from persisted `.s10` shards.
//! Lightweight live-prime helper — no idx scan, no renko rewrite.

use anyhow::Result;
use clap::Parser;
use nxr_sdk::resolve_ticker_id;
use nxr_sdk::shard::{bars_dir, list_shards, vol_path_for_id};
use series_factory::bar_construction::{build_vol_from_s10, S10ShardIter};
use series_factory::vol_bin::VolWriter;
use std::fs;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
#[command(about = "Build id-keyed .vol from persisted .s10 shards (live renko prime).")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    base: String,
    #[arg(long)]
    quote: String,
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let _yml = nxr_sdk::pipeline_config::PipelineYml::load(&args.config)?;
    let cfg = nxr_sdk::NxrConfig::from_env();
    let base = args.base.to_uppercase();
    let quote = args.quote.to_uppercase();
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base, quote));
    let data_root = PathBuf::from(&cfg.bars_dir)
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data"));
    let bars_directory = args
        .out_dir
        .unwrap_or_else(|| bars_dir(&data_root, ticker_id));

    let s10_shards = list_shards(&bars_directory, "s10")?;
    if s10_shards.is_empty() {
        anyhow::bail!(
            "no .s10 shards under {} — run s10-from-idx first",
            bars_directory.display()
        );
    }

    let scratch = std::env::temp_dir().join(format!("nxr-vol-from-s10-{ticker_id}.vol"));
    let mut writer = VolWriter::new(&scratch)?;
    let mut s10_iter = S10ShardIter::new(s10_shards);
    let n_vol = build_vol_from_s10(|| s10_iter.next_bar(), &_yml.series.vol, &mut writer)?;
    writer.finish()?;

    let persist_path = vol_path_for_id(&data_root, ticker_id);
    if let Some(parent) = persist_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = persist_path.with_extension("vol.tmp");
    fs::copy(&scratch, &tmp)?;
    fs::rename(&tmp, &persist_path)?;
    let _ = fs::remove_file(&scratch);

    info!(
        pair = %format!("{}/{}", base, quote),
        ticker_id,
        vol_records = n_vol,
        vol = %persist_path.display(),
        "id-keyed .vol written from .s10 shards"
    );
    Ok(())
}
