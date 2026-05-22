//! Fan four per-provider `.idx` AppendLogs into a single cross-provider
//! `<BASE>-<QUOTE>/<YYYY-MM-DD>.idx` set of daily shards via TDWAP.
//!
//! Mirrors the prod aggregator's inner cycle: at every composite cycle we
//! have one `ProviderEntry` per active provider (built from the last Index
//! each file delivered), feed that slice to `compute_vwap_at`, and emit the
//! resulting composite Index to the daily shard keyed by
//! `boundary_ts.utc_date()`. Per spec (`docs/sharding-spec.md`) all
//! artifacts are sharded by `open_ts.date_utc()` — daily granularity.
//!
//! Default weights (locked in for the BTC/USDT offline replay):
//!   binance=40, okx=20, bybit=30, bitget=10
//! Override at runtime with `--weight <exchange>=<w>` (repeatable) or by
//! pointing `NXR_TICKER_PARAMS_PATH` at a ticker-params.json.
//!
//! Usage:
//!   merge-idx <BASE> <QUOTE>
//!     [--exchange binance --exchange okx --exchange bybit --exchange bitget]
//!     [--cycle-ms 50] [--stale-secs 30]
//!     [--weight binance=40 --weight okx=20 --weight bybit=30 --weight bitget=10]
//!
//! Inputs:  `$NXR_DATA_INDEXES/<exchange>/<BASE>-<QUOTE>.idx` for each exchange
//! Output:  `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>/<YYYY-MM-DD>.idx`
//!         + `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>/manifest.json`

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mitch::common::message_type;
use mitch::header::MitchHeader;
use nxr_sdk::{
    compute_vwap_at,
    ipc::append_log::AppendLog,
    ipc::record::IndexRecord,
    resolve_ticker_id,
    tdwap::ProviderEntry,
};
use series_factory::sharding::{
    list_shards, manifest_path, sha256_file, shard_path, ts_ms_to_utc_date, Manifest, ShardEntry,
    write_manifest,
};
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Canonical default BTC/USDT weights for offline replay. Locked to these
/// four exchanges per product spec; adjust only through the CLI override or
/// a future ticker-params.json bindings.
const DEFAULT_OFFLINE_WEIGHTS: &[(&str, f64)] = &[
    ("binance", 40.0),
    ("okx", 20.0),
    ("bybit", 30.0),
    ("bitget", 10.0),
];

#[derive(Parser, Debug)]
#[command(about = "TDWAP-merge per-provider .idx into a per-day-sharded composite idx dir.")]
struct Args {
    base: String,
    quote: String,
    #[arg(long = "exchange")]
    exchanges: Vec<String>,
    /// Composite cycle in ms. Each cycle reruns the TDWAP over whichever
    /// providers have delivered data in [last_cycle, this_cycle].
    #[arg(long, default_value = "50")]
    cycle_ms: u64,
    /// Half-life clamp feed for the TDWAP decay (prod default = 30 s).
    #[arg(long, default_value = "30")]
    stale_secs: f64,
    /// Override a single provider weight. Repeatable. Example:
    ///   --weight binance=40 --weight okx=20
    #[arg(long = "weight")]
    weight_overrides: Vec<String>,
    /// Override the output directory (per-ticker shard root).
    #[arg(long)]
    out_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let cfg = nxr_sdk::NxrConfig::from_env();

    // resolve providers + weights
    let weight_map = resolve_weights(&args)?;
    let exchanges: Vec<String> = if args.exchanges.is_empty() {
        DEFAULT_OFFLINE_WEIGHTS.iter().map(|(n, _)| n.to_string()).collect()
    } else {
        args.exchanges.clone()
    };

    let base_uc = args.base.to_uppercase();
    let quote_uc = args.quote.to_uppercase();
    let ticker_str = format!("{}-{}", base_uc, quote_uc);
    let ticker_id = resolve_ticker_id(&format!("{}/{}", base_uc, quote_uc));
    let indexes_dir = PathBuf::from(&cfg.indexes_dir);
    // shard root = <indexes_dir>/composite/<BASE>-<QUOTE>/
    let out_dir = args.out_dir.clone().unwrap_or_else(|| {
        indexes_dir
            .join("composite")
            .join(format!("{}-{}", base_uc, quote_uc))
    });
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("create_dir_all {}", out_dir.display()))?;

    // open per-provider .idx as time-sorted streams
    let mut sources = Vec::<SourceStream>::with_capacity(exchanges.len());
    for exch in &exchanges {
        let path = indexes_dir
            .join(exch)
            .join(format!("{}-{}.idx", base_uc, quote_uc));
        let provider_id = nxr_sdk::providers::get_market_provider_id_by_name(exch)
            .with_context(|| format!("unknown exchange {}", exch))?;
        let base_weight = *weight_map
            .get(exch.as_str())
            .with_context(|| format!("no weight configured for {}", exch))?;
        match SourceStream::load(&path, provider_id, base_weight) {
            Ok(s) => {
                info!(
                    exchange = %exch,
                    provider_id,
                    base_weight,
                    path = %path.display(),
                    "opened provider .idx (streaming)"
                );
                sources.push(s);
            }
            Err(e) => {
                warn!(exchange = %exch, path = %path.display(), err = %e, "skip missing .idx");
            }
        }
    }
    if sources.is_empty() {
        anyhow::bail!("no per-provider .idx files loaded; nothing to merge");
    }

    // k-way merge heap keyed on head record timestamp
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(sources.len());
    for i in 0..sources.len() {
        if let Some(ts) = sources[i].peek_ts() {
            heap.push(HeapEntry::new(ts, i));
        }
    }

    // per-provider "last known state" slots
    let mut slots: Vec<Option<ProviderEntry>> = vec![None; sources.len()];

    // simulated clock anchored at earliest record
    let anchor_instant = Instant::now();
    let anchor_ms = heap.peek().map(|e| e.ts_ms).unwrap_or(0);
    let sim_now = |ms: i64| -> Instant {
        anchor_instant + Duration::from_millis((ms - anchor_ms).max(0) as u64)
    };

    // sharded writer: rotates AppendLog on UTC date boundary
    let mut writer = ShardedWriter::new(out_dir.clone());

    let cycle_ms = args.cycle_ms as i64;
    let mut next_cycle_ms: Option<i64> = None;
    let mut dirty = false;
    let mut composites_written: u64 = 0;
    let mut updates: u64 = 0;

    while let Some(HeapEntry { ts_ms, source_idx, .. }) = heap.pop() {
        let rec = sources[source_idx].pop().unwrap();
        updates += 1;
        let now = sim_now(ts_ms);

        // emit composites for every cycle boundary <= record ts
        if next_cycle_ms.is_none() {
            next_cycle_ms = Some(ts_ms + cycle_ms);
        }
        while ts_ms >= next_cycle_ms.unwrap() {
            if dirty {
                let boundary = next_cycle_ms.unwrap();
                let boundary_now = sim_now(boundary);
                if let Some(composite) = compute_vwap_at(
                    ticker_id,
                    slots.iter().filter_map(|s| s.as_ref()),
                    args.stale_secs,
                    boundary_now,
                ) {
                    let mts = mitch::timestamp::from_epoch_ms(boundary);
                    // provider_id=0 ∵ composite ! single provider
                    let header = MitchHeader::new(message_type::INDEX, 0, mts, 1);
                    let rec_out = IndexRecord { header, index: composite };
                    writer.append(boundary, &rec_out)?;
                    composites_written += 1;
                }
                dirty = false;
            }
            next_cycle_ms = Some(next_cycle_ms.unwrap() + cycle_ms);
        }

        // merge new per-provider Index into slot
        let base_weight = sources[source_idx].base_weight;
        match &mut slots[source_idx] {
            Some(entry) => entry.update_at(rec.index, now),
            None => slots[source_idx] = Some(ProviderEntry::new_at(rec.index, base_weight, now)),
        }
        dirty = true;

        if let Some(next_ts) = sources[source_idx].peek_ts() {
            heap.push(HeapEntry::new(next_ts, source_idx));
        }
    }

    // final flush at last observed cycle boundary
    if dirty {
        let boundary = next_cycle_ms.unwrap();
        let boundary_now = sim_now(boundary);
        if let Some(composite) = compute_vwap_at(
            ticker_id,
            slots.iter().filter_map(|s| s.as_ref()),
            args.stale_secs,
            boundary_now,
        ) {
            let mts = mitch::timestamp::from_epoch_ms(boundary);
            let header = MitchHeader::new(message_type::INDEX, 0, mts, 1);
            let rec_out = IndexRecord { header, index: composite };
            writer.append(boundary, &rec_out)?;
            composites_written += 1;
        }
    }
    writer.close()?;

    // build manifest by scanning shards
    let mut manifest = Manifest::new(ticker_str.clone(), ticker_id, "idx");
    for (date, path) in list_shards(&out_dir, "idx")? {
        let entry = shard_entry_for_idx(date, &path)?;
        manifest.upsert(entry);
    }
    let mpath = manifest_path(&out_dir);
    write_manifest(&mpath, &manifest)?;

    info!(
        composites_written,
        provider_updates = updates,
        shards = manifest.shards.len(),
        out_dir = %out_dir.display(),
        manifest = %mpath.display(),
        "merge-idx complete"
    );
    Ok(())
}

/// Per-day AppendLog rotator. Rolls on UTC date boundary.
struct ShardedWriter {
    out_dir: PathBuf,
    current: Option<(NaiveDate, AppendLog<IndexRecord>)>,
}

impl ShardedWriter {
    fn new(out_dir: PathBuf) -> Self {
        Self { out_dir, current: None }
    }

    fn append(&mut self, ts_ms: i64, rec: &IndexRecord) -> Result<()> {
        let date = ts_ms_to_utc_date(ts_ms);
        let need_rotate = match &self.current {
            Some((d, _)) => *d != date,
            None => true,
        };
        if need_rotate {
            // close prior shard ∵ AppendLog has Drop fsync
            self.current = None;
            let path = shard_path(&self.out_dir, date, "idx");
            let log = AppendLog::<IndexRecord>::open(&path)
                .with_context(|| format!("open shard {}", path.display()))?;
            self.current = Some((date, log));
        }
        self.current.as_mut().unwrap().1.append(rec)?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some((_, mut log)) = self.current.take() {
            log.flush()?;
        }
        Ok(())
    }
}

/// Build a manifest entry for a `.idx` shard by streaming the file once.
fn shard_entry_for_idx(date: NaiveDate, path: &Path) -> Result<ShardEntry> {
    use std::io::Read;
    let rec_size = core::mem::size_of::<IndexRecord>();
    let size_bytes = std::fs::metadata(path)?.len();
    let n_records = if rec_size == 0 { 0 } else { size_bytes / rec_size as u64 };
    let mut f = std::fs::File::open(path)?;
    let mut first_ts: i64 = i64::MAX;
    let mut last_ts: i64 = i64::MIN;
    let mut buf = vec![0u8; 4096 * rec_size];
    loop {
        let mut filled = 0usize;
        while filled < buf.len() {
            match f.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        if filled == 0 {
            break;
        }
        if filled % rec_size != 0 {
            anyhow::bail!("shard {} not aligned to IndexRecord", path.display());
        }
        let recs: &[IndexRecord] = bytemuck::cast_slice(&buf[..filled]);
        if let Some(r) = recs.first() {
            let ts = mitch::timestamp::to_epoch_ms(r.header.get_timestamp());
            if ts < first_ts {
                first_ts = ts;
            }
        }
        if let Some(r) = recs.last() {
            let ts = mitch::timestamp::to_epoch_ms(r.header.get_timestamp());
            if ts > last_ts {
                last_ts = ts;
            }
        }
        if filled < buf.len() {
            break;
        }
    }
    if n_records == 0 {
        first_ts = 0;
        last_ts = 0;
    }
    Ok(ShardEntry {
        date: date.format("%Y-%m-%d").to_string(),
        first_ts,
        last_ts,
        n_records,
        size_bytes,
        sha256: sha256_file(path)?,
    })
}

fn resolve_weights(args: &Args) -> Result<std::collections::BTreeMap<String, f64>> {
    let mut m: std::collections::BTreeMap<String, f64> = DEFAULT_OFFLINE_WEIGHTS
        .iter()
        .map(|(n, w)| (n.to_string(), *w))
        .collect();
    for spec in &args.weight_overrides {
        let (k, v) = spec
            .split_once('=')
            .with_context(|| format!("bad --weight spec {:?}; expected name=number", spec))?;
        let w: f64 = v.trim().parse().with_context(|| format!("bad weight {:?}", v))?;
        m.insert(k.trim().to_string(), w);
    }
    Ok(m)
}

/// Chunked streaming reader: each `SourceStream` only holds `WINDOW`
/// records in memory at a time. Four streams therefore keep <1 MiB
/// resident total, independent of file size.
struct SourceStream {
    file: std::fs::File,
    buf: Vec<IndexRecord>,
    cursor: usize,
    base_weight: f64,
    eof: bool,
}

const WINDOW: usize = 4096;

impl SourceStream {
    fn load(path: &std::path::Path, _provider_id: u16, base_weight: f64) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("open {}", path.display()))?;
        let len = file.metadata()?.len() as usize;
        let rec_size = core::mem::size_of::<IndexRecord>();
        if len > 0 && len % rec_size != 0 {
            anyhow::bail!(
                "{} size {} is not a multiple of IndexRecord ({})",
                path.display(),
                len,
                rec_size
            );
        }
        let mut s = Self {
            file,
            buf: Vec::with_capacity(WINDOW),
            cursor: 0,
            base_weight,
            eof: false,
        };
        s.refill()?;
        Ok(s)
    }

    fn refill(&mut self) -> Result<()> {
        use std::io::Read;
        if self.eof {
            return Ok(());
        }
        let rec_size = core::mem::size_of::<IndexRecord>();
        let mut raw = vec![0u8; WINDOW * rec_size];
        let mut filled = 0;
        while filled < raw.len() {
            match self.file.read(&mut raw[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        if filled == 0 {
            self.eof = true;
            self.buf.clear();
            self.cursor = 0;
            return Ok(());
        }
        if filled % rec_size != 0 {
            anyhow::bail!("short read {} not aligned to IndexRecord {}", filled, rec_size);
        }
        let slice: &[IndexRecord] = bytemuck::cast_slice(&raw[..filled]);
        self.buf.clear();
        self.buf.extend_from_slice(slice);
        self.cursor = 0;
        if filled < raw.len() {
            self.eof = true;
        }
        Ok(())
    }

    fn peek_ts(&mut self) -> Option<i64> {
        if self.cursor >= self.buf.len() {
            if self.eof {
                return None;
            }
            self.refill().ok()?;
            if self.buf.is_empty() {
                return None;
            }
        }
        Some(mitch::timestamp::to_epoch_ms(
            self.buf[self.cursor].header.get_timestamp(),
        ))
    }

    fn pop(&mut self) -> Option<IndexRecord> {
        if self.cursor >= self.buf.len() {
            if self.eof {
                return None;
            }
            if self.refill().is_err() {
                return None;
            }
            if self.buf.is_empty() {
                return None;
            }
        }
        let r = self.buf[self.cursor];
        self.cursor += 1;
        Some(r)
    }
}

/// Min-heap entry keyed on timestamp, tie-broken on source index for
/// deterministic ordering.
#[derive(PartialEq, Eq)]
struct HeapEntry {
    // inverted so BinaryHeap (max-heap) gives smallest-first
    neg_ts_ms: i64,
    neg_idx: usize,
    ts_ms: i64,
    source_idx: usize,
}

impl HeapEntry {
    fn new(ts_ms: i64, source_idx: usize) -> Self {
        Self {
            neg_ts_ms: -ts_ms,
            neg_idx: usize::MAX - source_idx,
            ts_ms,
            source_idx,
        }
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.neg_ts_ms
            .cmp(&other.neg_ts_ms)
            .then(self.neg_idx.cmp(&other.neg_idx))
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
