//! Fan four per-provider `.idx` AppendLogs into a single cross-provider
//! `<BASE>-<QUOTE>.idx` via TDWAP.
//!
//! Mirrors the prod aggregator's inner cycle: at every composite cycle we
//! have one `ProviderEntry` per active provider (built from the last Index
//! each file delivered), feed that slice to `compute_vwap_at`, and emit the
//! resulting composite Index to a new AppendLog on disk.
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
//! Output:  `$NXR_DATA_INDEXES/composite/<BASE>-<QUOTE>.idx`

use anyhow::{Context, Result};
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
use std::collections::BinaryHeap;
use std::path::PathBuf;
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
#[command(about = "TDWAP-merge per-provider .idx into a composite .idx.")]
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
    /// Override the output path.
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    nxr_sdk::memory::apply_safe_cap();

    let args = Args::parse();
    let cfg = nxr_sdk::NxrConfig::from_env();

    // --- Resolve providers + weights ---
    let weight_map = resolve_weights(&args)?;
    let exchanges: Vec<String> = if args.exchanges.is_empty() {
        DEFAULT_OFFLINE_WEIGHTS.iter().map(|(n, _)| n.to_string()).collect()
    } else {
        args.exchanges.clone()
    };

    let ticker_id = resolve_ticker_id(&format!("{}/{}", args.base, args.quote));
    let indexes_dir = PathBuf::from(&cfg.indexes_dir);
    let out_path = args.out.unwrap_or_else(|| {
        indexes_dir
            .join("composite")
            .join(format!("{}-{}.idx", args.base.to_uppercase(), args.quote.to_uppercase()))
    });

    // --- Open each provider's .idx and load it into RAM as a time-sorted Vec ---
    // The raw .idx files are small (a few GiB total for 4x2yr at 50ms with
    // sparse windows), so full load is fine and simplifies the k-way merge.
    let mut sources = Vec::<SourceStream>::with_capacity(exchanges.len());
    for exch in &exchanges {
        let path = indexes_dir
            .join(exch)
            .join(format!("{}-{}.idx", args.base.to_uppercase(), args.quote.to_uppercase()));
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

    // --- k-way merge heap keyed on head record timestamp ---
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(sources.len());
    for i in 0..sources.len() {
        if let Some(ts) = sources[i].peek_ts() {
            heap.push(HeapEntry::new(ts, i));
        }
    }

    // Per-provider "last known state" slots. Each slot carries a
    // `ProviderEntry` that updates on every new Index from that source.
    let mut slots: Vec<Option<ProviderEntry>> = vec![None; sources.len()];

    // Simulated clock: anchor at the earliest record's epoch, then advance
    // by (tick_ms - anchor_ms) so `compute_vwap_at`'s time-decay sees data
    // time rather than wall-clock.
    let anchor_instant = Instant::now();
    let anchor_ms = heap.peek().map(|e| e.ts_ms).unwrap_or(0);
    let sim_now = |ms: i64| -> Instant {
        anchor_instant + Duration::from_millis((ms - anchor_ms).max(0) as u64)
    };

    // --- Output log ---
    let mut out: AppendLog<IndexRecord> = AppendLog::open(&out_path)
        .with_context(|| format!("open composite AppendLog {}", out_path.display()))?;

    let cycle_ms = args.cycle_ms as i64;
    let mut next_cycle_ms: Option<i64> = None;
    let mut dirty = false;
    let mut composites_written: u64 = 0;
    let mut updates: u64 = 0;

    while let Some(HeapEntry { ts_ms, source_idx, .. }) = heap.pop() {
        let rec = sources[source_idx].pop().unwrap();
        updates += 1;
        let now = sim_now(ts_ms);

        // Emit composites for every cycle boundary strictly before or equal
        // to this record's timestamp.
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
                    // provider_id=0 for the composite: it isn't a provider,
                    // downstream readers treat it as "aggregate".
                    let header = MitchHeader::new(message_type::INDEX, 0, mts, 1);
                    out.append(&IndexRecord { header, index: composite })
                        .context("composite AppendLog append failed")?;
                    composites_written += 1;
                }
                dirty = false;
            }
            next_cycle_ms = Some(next_cycle_ms.unwrap() + cycle_ms);
        }

        // Merge the new per-provider Index into the slot.
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

    // Final flush at the last observed cycle boundary if there is pending
    // dirty state (e.g. the last record arrived just before a boundary).
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
            out.append(&IndexRecord { header, index: composite })?;
            composites_written += 1;
        }
    }
    out.flush().context("composite AppendLog final flush")?;

    info!(
        composites_written,
        provider_updates = updates,
        out = %out_path.display(),
        "merge-idx complete"
    );
    Ok(())
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
    // Inverted so `BinaryHeap` (max-heap) gives us smallest-first.
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
