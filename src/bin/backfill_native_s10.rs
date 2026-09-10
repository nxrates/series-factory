//! Backfill a native `<ASSET>/USD` `.s10` series from rebased sources.
//!
//! ## Why a new writer
//!
//! `migrate-quote-basis` renames whole trees and never rewrites a record body.
//! A quote rebase rewrites every OHLC corner, so none of its guarantees carry
//! over. This tool is the body-rewriting writer, with its own guarantees.
//!
//! ## Model
//!
//! A plan CSV (`from_ms,to_ms,action,source,mult,mult_max_age_s,tag`) lists
//! NON-OVERLAPPING UTC windows `[from, to)`, one action each:
//!
//! * `fill`: a bucket with no bar on disk (or whose bar the band gate rejects)
//!   gets `source bar x multiplier`.
//! * `replace`: bars on disk in the window are quarantined and dropped; every
//!   bucket gets `source bar x multiplier`.
//! * `hole`: bars on disk in the window are quarantined and dropped; nothing is
//!   written.
//!
//! The multiplier CSV (`ts_ms,mult`, ascending) is looked up at the bar OPEN:
//! the last row with `ts <= open`, valid only while `open - ts < mult_max_age_s`
//! and finite. A missing or stale multiplier leaves a hole, never a stale rebase.
//!
//! ## Band gate (per bar, not per window)
//!
//! `--band` (`minute_ms,lo,hi`) is an independent referee range per minute
//! (venue minute low/high x USD multiplier). EVERY bar of every processed day,
//! kept or written, is judged on close, high and low:
//! `close in [lo(1-c), hi(1+c)]`, `high <= hi(1+w)`, `low >= lo(1-w)`.
//!
//! * A kept bar that fails is quarantined; its bucket becomes fillable.
//! * A written bar that fails is a hole (`band_rejected`), never a backfill.
//! * A minute with no band row is unverifiable: kept bars stay, `fill` bars are
//!   written (counted), `replace` bars are refused (`unverified_rejected`).
//!
//! ## Guarantees
//!
//! * **s10 only.** `.idx`, `.renko` and `.vol` are never read or written.
//! * **One native per asset.** The target id must be USD-quoted (class 3 / id
//!   5001), and every source dir must be named by an id of the same base asset.
//! * **Never a live shard.** `--to` must be before today (UTC); today's file is
//!   never read. A target file written in the last 5 minutes, or with a torn
//!   tail, aborts the run.
//! * **Every refusal before the first write.** Pass 1 plans every day (content
//!   hash `sha_planned`); a refusal (`dup_conflict`, boundary, torn/hot file,
//!   bad input) aborts with nothing written. Pass 2 re-plans each day and
//!   writes only if the hash still matches.
//! * **Untouched days stay untouched.** Not written (`--apply`), or copied
//!   byte-for-byte (`--stage`).
//! * **Kept bars are byte-identical.** Every written file is re-read, and each
//!   kept bar is compared against the bytes read before the write.
//! * **Provenance.** Every written bar has `FLAG_HISTORICAL_BACKFILL` (bit 1)
//!   OR'd in. Source `tick_count` and every other flag bit (incl.
//!   `FLAG_S10_FLAT_FILL`) are copied, so a carried bar stays a carried bar.
//! * **Journal first.** The report is an append+fsync JSONL journal: a day's
//!   `intent` line (action, `sha_before`, `sha_planned`) is durable BEFORE its
//!   quarantine copy or write, and a `done` line (`sha_after`) follows.
//!   `--resume` finishes a crashed `--apply` run to the same bytes;
//!   `--rollback` undoes a complete or partial one from the journal alone.
//! * **Quarantine first.** Before a day file is changed or removed, it is
//!   byte-copied to `<root>/migrations/quarantine/<run>/bars/<id>/<date>.s10`
//!   and the copy is compared.
//! * **Atomic.** Writes go through `write_shard_atomic` (`.tmp`, fsync,
//!   rename). Readers list `*.s10` only, so a crash leaves no readable partial.
//! * **No double midnight bar.** Writers disagree on which file owns the
//!   00:00:00 bucket (live: previous day; repaired/legacy: same day). A bucket
//!   held by an adjacent day file is never written again. A kept bar whose
//!   byte-identical copy the previous day file holds is dropped from the later
//!   file (lossless). A differing copy (`dup_conflict`) refuses the run.
//!   A bar of `--from` held by the file of `--from - 1` that the plan drops
//!   refuses the run: start one day earlier.
//! * **Source is never the target tree.** Copy the source days first.
//! * **Dry run by default.** `--stage <dir>` writes a complete served tree off
//!   the PVC; `--apply` writes the data root.
//!
//! Sealed days are cached by core forever (`s10_cache.rs` `e.sealed`): a
//! changed day is served after the next core restart; a new day at once.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mitch::bar::Bar;
use nxr_sdk::shard::{
    bars_dir, date_stem, day_start_ms, manifest_path, parse_utc_date, read_manifest,
    read_shard_aligned, sha256_file, shard_path, today_utc, ts_ms_to_utc_date, write_manifest,
    write_shard_atomic, Manifest, BAR_MS_S10, FLAG_HISTORICAL_BACKFILL, MS_PER_DAY,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const USD_QUOTE: (u64, u64) = (3, 5001);
const RECENT_WRITE: Duration = Duration::from_secs(300);
const STRIDE: usize = std::mem::size_of::<Bar>();
/// A multiplier outside this band is a broken input file, not a quote rebase.
const MULT_BAND: (f64, f64) = (0.8, 1.25);
/// Instrument type + base class + base id (`mitch ticker.rs TickerId::new`).
const BASE_SHIFT: u32 = 40;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "backfill-native-s10",
    about = "Backfill a native USD .s10 series from rebased sources (dry run by default)"
)]
struct Args {
    /// Data root holding `bars/<target_id>/` (read-only unless --apply/--rollback).
    #[arg(long)]
    data_root: PathBuf,
    /// Native USD-quoted ticker id to write.
    #[arg(long)]
    target_id: u64,
    /// Symbol recorded in a new manifest (e.g. BNB/USD).
    #[arg(long, required_unless_present = "rollback")]
    symbol: Option<String>,
    /// Plan CSV: from_ms,to_ms,action,source,mult,mult_max_age_s,tag
    #[arg(long, required_unless_present = "rollback")]
    plan: Option<PathBuf>,
    /// Referee band CSV: minute_ms,lo,hi (empty lo/hi = unverifiable minute).
    #[arg(long, required_unless_present = "rollback")]
    band: Option<PathBuf>,
    /// Close tolerance outside the band, bp.
    #[arg(long, default_value_t = 25.0)]
    band_close_bp: f64,
    /// High/low tolerance outside the band, bp.
    #[arg(long, default_value_t = 50.0)]
    band_wick_bp: f64,
    /// First target day, inclusive (YYYY-MM-DD).
    #[arg(long, required_unless_present = "rollback")]
    from: Option<String>,
    /// Last target day, inclusive (YYYY-MM-DD). Must be before today (UTC).
    #[arg(long, required_unless_present = "rollback")]
    to: Option<String>,
    /// Write a complete served tree here (untouched days copied verbatim).
    #[arg(long, conflicts_with_all = ["apply", "rollback"])]
    stage: Option<PathBuf>,
    /// Write into --data-root (quarantine copies first).
    #[arg(long)]
    apply: bool,
    /// Finish a crashed --apply run from its journal (same run id and inputs).
    #[arg(long, requires = "apply")]
    resume: bool,
    /// Undo an --apply run (complete or partial) from its journal.
    #[arg(long, conflicts_with_all = ["apply", "stage", "resume"])]
    rollback: bool,
    /// Run id: names the quarantine directory.
    #[arg(long)]
    run_id: String,
    /// Journal / report JSONL (append + fsync per line). Must not exist unless
    /// --resume or --rollback.
    #[arg(long)]
    report: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Action {
    Fill,
    Replace,
    Hole,
}

#[derive(Debug, Clone)]
struct Mult {
    rows: Rc<Vec<(i64, f64)>>,
    max_age_ms: i64,
}

impl Mult {
    fn at(&self, ts: i64) -> Option<f64> {
        let i = self.rows.partition_point(|r| r.0 <= ts);
        let (t, m) = *self.rows.get(i.checked_sub(1)?)?;
        (ts - t < self.max_age_ms && m.is_finite()).then_some(m)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Pass,
    Fail,
    Unverified,
}

/// Referee range per minute; tolerances as fractions.
#[derive(Debug, Clone)]
struct Band {
    rows: Vec<(i64, f64, f64)>,
    close: f64,
    wick: f64,
}

impl Band {
    fn verdict(&self, b: &Bar) -> Verdict {
        let m = b.open_time_ms().div_euclid(60_000) * 60_000;
        let i = self.rows.partition_point(|r| r.0 < m);
        match self.rows.get(i) {
            Some(&(t, lo, hi)) if t == m => {
                let (c, h, l) = (b.close, b.high, b.low);
                let bad = !(c.is_finite() && h.is_finite() && l.is_finite())
                    || c > hi * (1.0 + self.close)
                    || c < lo * (1.0 - self.close)
                    || h > hi * (1.0 + self.wick)
                    || l < lo * (1.0 - self.wick);
                if bad {
                    Verdict::Fail
                } else {
                    Verdict::Pass
                }
            }
            _ => Verdict::Unverified,
        }
    }
}

#[derive(Debug, Clone)]
struct Window {
    from_ms: i64,
    to_ms: i64,
    action: Action,
    source: Option<PathBuf>,
    mult: Option<Mult>,
    tag: String,
}

impl Window {
    fn drops(&self, ts: i64) -> bool {
        self.action != Action::Fill && ts >= self.from_ms && ts < self.to_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum DayAction {
    /// No file before, none after.
    Absent,
    /// No file before, file after.
    New,
    /// File before, unchanged.
    Unchanged,
    /// File before, bars dropped and/or added.
    Merge,
    /// File before, every bar dropped and nothing added: file removed.
    Emptied,
}

#[derive(Debug, Default, Clone, Serialize)]
struct DayReport {
    date: String,
    action: Option<DayAction>,
    existing_bars: usize,
    kept: usize,
    /// Window drops + band drops.
    quarantined: usize,
    /// Kept bars the band gate rejected (subset of `quarantined`).
    band_quarantined: usize,
    /// Kept bars in minutes without a band row.
    kept_unverified: usize,
    backfilled: usize,
    backfilled_tc0: usize,
    /// Written `fill` bars in minutes without a band row.
    backfilled_unverified: usize,
    /// Rebased source bars the band gate rejected: holes.
    band_rejected: usize,
    /// Rebased `replace` bars in minutes without a band row: holes.
    unverified_rejected: usize,
    /// Calendar buckets [00:00:00, 24:00:00) with no bar in this or an adjacent file.
    holes: usize,
    /// Fill buckets skipped because an adjacent day file holds the bar.
    held_by_neighbour: usize,
    /// Kept bars dropped: the previous day file holds a byte-identical bar.
    dedup_identical: usize,
    /// Bars the previous day file also holds with different bytes (refuses writes).
    dup_conflict: usize,
    src_missing: usize,
    src_rejected: usize,
    mult_missing: usize,
    tags: BTreeSet<String>,
    sha_before: Option<String>,
    /// Hash of the file this day will leave on disk (None = no file).
    sha_planned: Option<String>,
    sha_after: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct Summary {
    run_id: String,
    mode: String,
    target_id: u64,
    days: usize,
    resumed_done: usize,
    by_action: BTreeMap<String, usize>,
    bars_after: usize,
    kept: usize,
    quarantined: usize,
    band_quarantined: usize,
    kept_unverified: usize,
    backfilled: usize,
    backfilled_tc0: usize,
    backfilled_unverified: usize,
    band_rejected: usize,
    unverified_rejected: usize,
    holes: usize,
    dedup_identical: usize,
    dup_conflict: usize,
}

impl Summary {
    fn add(&mut self, r: &DayReport, bars_after: usize) {
        self.days += 1;
        *self
            .by_action
            .entry(format!("{:?}", r.action.expect("set")).to_lowercase())
            .or_default() += 1;
        self.bars_after += bars_after;
        self.kept += r.kept;
        self.quarantined += r.quarantined;
        self.band_quarantined += r.band_quarantined;
        self.kept_unverified += r.kept_unverified;
        self.backfilled += r.backfilled;
        self.backfilled_tc0 += r.backfilled_tc0;
        self.backfilled_unverified += r.backfilled_unverified;
        self.band_rejected += r.band_rejected;
        self.unverified_rejected += r.unverified_rejected;
        self.holes += r.holes;
        self.dedup_identical += r.dedup_identical;
        self.dup_conflict += r.dup_conflict;
    }
}

fn quote_of(id: u64) -> (u64, u64) {
    ((id >> 36) & 0xF, (id >> 20) & 0xFFFF)
}

fn bar_bytes(b: &Bar) -> [u8; STRIDE] {
    bytemuck::bytes_of(b).try_into().expect("Bar is 96 bytes")
}

fn sha_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sha_opt(path: &Path) -> Result<Option<String>> {
    path.exists().then(|| sha256_file(path)).transpose()
}

fn sane(b: &Bar) -> bool {
    let (o, h, l, c) = (b.open, b.high, b.low, b.close);
    [o, h, l, c].iter().all(|p| p.is_finite() && *p > 0.0)
        && h >= l
        && h <= l * 2.0
        && b.open_time_ms().rem_euclid(BAR_MS_S10) == 0
}

/// Source bar rebased onto USD. Only the price corners scale; ratios, counts,
/// variances and every flag bit are copied, and bit 1 marks the provenance.
fn rebase(src: &Bar, m: f64) -> Bar {
    let mut b = *src;
    b.open = src.open * m;
    b.high = src.high * m;
    b.low = src.low * m;
    b.close = src.close * m;
    b.flags = src.flags | FLAG_HISTORICAL_BACKFILL;
    b
}

fn load_mult(
    path: &Path,
    max_age_s: i64,
    cache: &mut HashMap<PathBuf, Rc<Vec<(i64, f64)>>>,
) -> Result<Mult> {
    ensure!(
        max_age_s > 0,
        "mult_max_age_s must be > 0 for {}",
        path.display()
    );
    if let Some(rows) = cache.get(path) {
        return Ok(Mult {
            rows: Rc::clone(rows),
            max_age_ms: max_age_s * 1000,
        });
    }
    let raw = fs::read_to_string(path).with_context(|| format!("read mult {}", path.display()))?;
    let mut rows: Vec<(i64, f64)> = Vec::new();
    for (n, line) in raw.lines().enumerate().skip(1) {
        let (t, m) = line
            .split_once(',')
            .with_context(|| format!("{}:{}: want ts_ms,mult", path.display(), n + 1))?;
        let t: i64 = t
            .trim()
            .parse()
            .with_context(|| format!("{}:{} ts", path.display(), n + 1))?;
        let m: f64 = match m.trim() {
            "" | "nan" | "NaN" => f64::NAN,
            s => s
                .parse()
                .with_context(|| format!("{}:{} mult", path.display(), n + 1))?,
        };
        if m.is_finite() {
            ensure!(
                m > MULT_BAND.0 && m < MULT_BAND.1,
                "{}:{} multiplier {m} outside {:?}: broken input",
                path.display(),
                n + 1,
                MULT_BAND
            );
        }
        if let Some(&(prev, _)) = rows.last() {
            ensure!(t > prev, "{}:{} ts not ascending", path.display(), n + 1);
        }
        rows.push((t, m));
    }
    let rows = Rc::new(rows);
    cache.insert(path.to_path_buf(), Rc::clone(&rows));
    Ok(Mult {
        rows,
        max_age_ms: max_age_s * 1000,
    })
}

fn load_band(path: &Path, close_bp: f64, wick_bp: f64) -> Result<Band> {
    for (k, v) in [("--band-close-bp", close_bp), ("--band-wick-bp", wick_bp)] {
        ensure!(v > 0.0 && v < 1000.0, "{k} {v} outside (0, 1000)");
    }
    let raw = fs::read_to_string(path).with_context(|| format!("read band {}", path.display()))?;
    let mut rows: Vec<(i64, f64, f64)> = Vec::new();
    for (n, line) in raw.lines().enumerate().skip(1) {
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        ensure!(
            f.len() == 3,
            "{}:{}: want minute_ms,lo,hi",
            path.display(),
            n + 1
        );
        let t: i64 = f[0]
            .parse()
            .with_context(|| format!("{}:{} minute_ms", path.display(), n + 1))?;
        ensure!(
            t.rem_euclid(60_000) == 0,
            "{}:{}: minute_ms off the minute grid",
            path.display(),
            n + 1
        );
        if let Some(&(prev, _, _)) = rows.last() {
            ensure!(t > prev, "{}:{} minute_ms not ascending", path.display(), n + 1);
        }
        if f[1].is_empty() || f[2].is_empty() {
            continue;
        }
        let (lo, hi): (f64, f64) = (f[1].parse()?, f[2].parse()?);
        ensure!(
            lo.is_finite() && hi.is_finite() && lo > 0.0 && lo <= hi,
            "{}:{}: bad band [{lo}, {hi}]",
            path.display(),
            n + 1
        );
        rows.push((t, lo, hi));
    }
    Ok(Band {
        rows,
        close: close_bp / 1e4,
        wick: wick_bp / 1e4,
    })
}

fn load_plan(path: &Path, target_dir: &Path, target_id: u64) -> Result<Vec<Window>> {
    let raw = fs::read_to_string(path).with_context(|| format!("read plan {}", path.display()))?;
    let target = fs::canonicalize(target_dir).unwrap_or_else(|_| target_dir.to_path_buf());
    let mut cache = HashMap::new();
    let mut out: Vec<Window> = Vec::new();
    for (n, line) in raw.lines().enumerate().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        ensure!(
            f.len() == 7,
            "{}:{}: want 7 fields, got {}",
            path.display(),
            n + 1,
            f.len()
        );
        let from_ms: i64 = f[0].parse().context("from_ms")?;
        let to_ms: i64 = f[1].parse().context("to_ms")?;
        ensure!(
            from_ms < to_ms,
            "{}:{}: empty window",
            path.display(),
            n + 1
        );
        let action = match f[2] {
            "fill" => Action::Fill,
            "replace" => Action::Replace,
            "hole" => Action::Hole,
            a => bail!("{}:{}: unknown action {a}", path.display(), n + 1),
        };
        let (source, mult) = if action == Action::Hole {
            ensure!(
                f[3] == "-" && f[4] == "-",
                "{}:{}: hole takes no source/mult",
                path.display(),
                n + 1
            );
            (None, None)
        } else {
            ensure!(
                f[3] != "-" && f[4] != "-",
                "{}:{}: {} needs source and mult",
                path.display(),
                n + 1,
                f[2]
            );
            let src = PathBuf::from(f[3]);
            let canon =
                fs::canonicalize(&src).with_context(|| format!("source {}", src.display()))?;
            ensure!(
                canon != target,
                "{}:{}: source is the target tree; copy the source days first",
                path.display(),
                n + 1
            );
            // A typo'd parked id (BTC/USDT for BNB/USDT) would pass every
            // price-sanity check; the dir name is the only identity a tree has.
            let sid: u64 = canon
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse().ok())
                .with_context(|| {
                    format!(
                        "{}:{}: source dir {} is not named by a ticker id",
                        path.display(),
                        n + 1,
                        canon.display()
                    )
                })?;
            ensure!(
                sid >> BASE_SHIFT == target_id >> BASE_SHIFT,
                "{}:{}: source id {sid} is another base asset than target {target_id}",
                path.display(),
                n + 1
            );
            let age: i64 = f[5].parse().context("mult_max_age_s")?;
            (
                Some(src),
                Some(load_mult(Path::new(f[4]), age, &mut cache)?),
            )
        };
        if let Some(prev) = out.last() {
            ensure!(
                from_ms >= prev.to_ms,
                "{}:{}: windows overlap or are unsorted ({} < {})",
                path.display(),
                n + 1,
                from_ms,
                prev.to_ms
            );
        }
        out.push(Window {
            from_ms,
            to_ms,
            action,
            source,
            mult,
            tag: f[6].to_string(),
        });
    }
    Ok(out)
}

/// Target day file: exact bytes, refusing torn or hot files.
fn read_target(path: &Path) -> Result<Option<Vec<u8>>> {
    if !path.exists() {
        return Ok(None);
    }
    let meta = fs::metadata(path)?;
    let age = SystemTime::now()
        .duration_since(meta.modified()?)
        .unwrap_or_default();
    ensure!(
        age >= RECENT_WRITE,
        "{} written {:?} ago: a writer may hold it",
        path.display(),
        age
    );
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    ensure!(
        bytes.len() % STRIDE == 0,
        "{} has a torn tail ({} bytes)",
        path.display(),
        bytes.len()
    );
    Ok(Some(bytes))
}

fn bars_of(bytes: &[u8]) -> Vec<Bar> {
    bytemuck::cast_slice::<u8, Bar>(bytes).to_vec()
}

/// Timestamps adjacent day files keep, plus the previous day's bars by timestamp.
type Held = (BTreeSet<i64>, HashMap<i64, [u8; STRIDE]>);

struct Ctx {
    windows: Vec<Window>,
    band: Band,
    target_dir: PathBuf,
    from: NaiveDate,
    to: NaiveDate,
    src_cache: HashMap<(PathBuf, NaiveDate), Rc<Vec<Bar>>>,
}

impl Ctx {
    fn source_day(&mut self, dir: &Path, d: NaiveDate) -> Result<Rc<Vec<Bar>>> {
        let key = (dir.to_path_buf(), d);
        if let Some(v) = self.src_cache.get(&key) {
            return Ok(Rc::clone(v));
        }
        let p = shard_path(dir, d, "s10");
        let v = Rc::new(if p.exists() {
            read_shard_aligned::<Bar>(&p)?
        } else {
            Vec::new()
        });
        if self.src_cache.len() > 64 {
            self.src_cache.clear();
        }
        self.src_cache.insert(key, Rc::clone(&v));
        Ok(v)
    }

    /// Source bar for bucket `ts`, looked up in the files of day(ts) - 1 ..= day(ts),
    /// since either may own the midnight bucket.
    fn source_bar(&mut self, dir: &Path, ts: i64) -> Result<Option<Bar>> {
        let d = ts_ms_to_utc_date(ts);
        for day in [d, d.pred_opt().expect("date")] {
            let bars = self.source_day(dir, day)?;
            let i = bars.partition_point(|b| b.open_time_ms() < ts);
            if let Some(b) = bars.get(i).filter(|b| b.open_time_ms() == ts) {
                return Ok(Some(*b));
            }
        }
        Ok(None)
    }

    fn in_run(&self, d: NaiveDate) -> bool {
        d >= self.from && d <= self.to
    }

    fn dropped(&self, ts: i64) -> bool {
        let i = self.windows.partition_point(|w| w.to_ms <= ts);
        self.windows.get(i).is_some_and(|w| w.drops(ts))
    }

    /// A bar on disk this plan removes: dropped by a window or failed by the band.
    fn drops_bar(&self, b: &Bar) -> bool {
        self.dropped(b.open_time_ms()) || self.band.verdict(b) == Verdict::Fail
    }

    /// The file of `from - 1` may own `from`'s 00:00:00 bucket. If this plan
    /// drops that bar, the run must include that file, or the bar survives.
    fn boundary_check(&self) -> Result<()> {
        let pd = self.from.pred_opt().expect("date");
        let p = shard_path(&self.target_dir, pd, "s10");
        if !p.exists() {
            return Ok(());
        }
        let first = day_start_ms(self.from);
        for b in read_shard_aligned::<Bar>(&p)? {
            ensure!(
                !(b.open_time_ms() >= first && self.drops_bar(&b)),
                "{} holds bucket {} of --from {} and this plan drops it: start the run at --from {}",
                p.display(),
                b.open_time_ms(),
                self.from,
                pd
            );
        }
        Ok(())
    }

    /// Timestamps an adjacent day file will still hold after this run, plus the
    /// previous day's held bars by timestamp. Today's (live) file is never read:
    /// it cannot hold a bucket of a closed day.
    fn neighbour_held(&self, d: NaiveDate) -> Result<Held> {
        let mut held = BTreeSet::new();
        let mut prev = HashMap::new();
        let pd = d.pred_opt().expect("date");
        for nd in [pd, d.succ_opt().expect("date")] {
            let p = shard_path(&self.target_dir, nd, "s10");
            if nd >= today_utc() || !p.exists() {
                continue;
            }
            for b in read_shard_aligned::<Bar>(&p)? {
                let ts = b.open_time_ms();
                if !(self.in_run(nd) && self.drops_bar(&b)) {
                    held.insert(ts);
                    if nd == pd {
                        prev.insert(ts, bar_bytes(&b));
                    }
                }
            }
        }
        Ok((held, prev))
    }

    /// Plan one day against `before` (the original bytes). Pure: writes nothing.
    fn day(&mut self, d: NaiveDate, before: Option<&[u8]>) -> Result<(DayReport, Vec<u8>)> {
        let d0 = day_start_ms(d);
        let d1 = d0 + MS_PER_DAY;
        let mut r = DayReport {
            date: date_stem(d),
            sha_before: before.map(sha_hex),
            ..Default::default()
        };
        let existing = before.map(bars_of).unwrap_or_default();
        r.existing_bars = existing.len();

        let (held, prev) = self.neighbour_held(d)?;
        let mut out: BTreeMap<i64, (Bar, bool)> = BTreeMap::new();
        for b in &existing {
            let ts = b.open_time_ms();
            let v = self.band.verdict(b);
            if self.dropped(ts) {
                r.quarantined += 1;
            } else if v == Verdict::Fail {
                r.quarantined += 1;
                r.band_quarantined += 1;
            } else if prev.get(&ts) == Some(&bar_bytes(b)) {
                r.dedup_identical += 1;
            } else {
                if prev.contains_key(&ts) {
                    r.dup_conflict += 1;
                }
                ensure!(
                    out.insert(ts, (*b, false)).is_none(),
                    "{}: duplicate bar at {ts} on disk",
                    r.date
                );
                if v == Verdict::Unverified {
                    r.kept_unverified += 1;
                }
            }
        }
        r.kept = out.len();
        if r.band_quarantined > 0 {
            r.tags.insert("band-quarantine".into());
        }

        let windows: Vec<Window> = self
            .windows
            .iter()
            .filter(|w| w.action != Action::Hole && w.from_ms < d1 && w.to_ms > d0)
            .cloned()
            .collect();
        for w in &windows {
            let (src, mult) = (
                w.source.as_ref().expect("fill source"),
                w.mult.as_ref().expect("fill mult"),
            );
            let start = w.from_ms.max(d0);
            let mut ts = start + (BAR_MS_S10 - start.rem_euclid(BAR_MS_S10)) % BAR_MS_S10;
            while ts < w.to_ms.min(d1) {
                if out.contains_key(&ts) {
                    ts += BAR_MS_S10;
                    continue;
                }
                if held.contains(&ts) {
                    r.held_by_neighbour += 1;
                    ts += BAR_MS_S10;
                    continue;
                }
                match (self.source_bar(src, ts)?, mult.at(ts)) {
                    (None, _) => r.src_missing += 1,
                    (Some(_), None) => r.mult_missing += 1,
                    (Some(s), Some(m)) => {
                        let b = rebase(&s, m);
                        if !(sane(&b) && b.open_time_ms() == ts) {
                            r.src_rejected += 1;
                        } else {
                            match (self.band.verdict(&b), w.action) {
                                (Verdict::Fail, _) => r.band_rejected += 1,
                                (Verdict::Unverified, Action::Replace) => {
                                    r.unverified_rejected += 1
                                }
                                (v, _) => {
                                    if v == Verdict::Unverified {
                                        r.backfilled_unverified += 1;
                                    }
                                    if b.tick_count == 0 {
                                        r.backfilled_tc0 += 1;
                                    }
                                    out.insert(ts, (b, true));
                                    r.backfilled += 1;
                                    r.tags.insert(w.tag.clone());
                                }
                            }
                        }
                    }
                }
                ts += BAR_MS_S10;
            }
        }
        for w in self
            .windows
            .iter()
            .filter(|w| w.action != Action::Fill && w.from_ms < d1 && w.to_ms > d0)
        {
            if existing.iter().any(|b| w.drops(b.open_time_ms())) {
                r.tags.insert(w.tag.clone());
            }
        }

        let present = out.keys().filter(|t| **t >= d0 && **t < d1).count()
            + held.iter().filter(|t| **t >= d0 && **t < d1).count();
        r.holes =
            (MS_PER_DAY / BAR_MS_S10) as usize - present.min((MS_PER_DAY / BAR_MS_S10) as usize);

        let mut bytes = Vec::with_capacity(out.len() * STRIDE);
        for (b, _) in out.values() {
            bytes.extend_from_slice(&bar_bytes(b));
        }
        let action = match (before.is_some(), bytes.is_empty()) {
            (false, true) => DayAction::Absent,
            (false, false) => DayAction::New,
            (true, _) if r.quarantined == 0 && r.backfilled == 0 && r.dedup_identical == 0 => {
                DayAction::Unchanged
            }
            (true, true) => DayAction::Emptied,
            (true, false) => DayAction::Merge,
        };
        r.action = Some(action);
        r.sha_planned = match action {
            DayAction::Absent | DayAction::Emptied => None,
            DayAction::Unchanged => r.sha_before.clone(),
            DayAction::New | DayAction::Merge => Some(sha_hex(&bytes)),
        };
        if action == DayAction::Unchanged {
            bytes = before.expect("file").to_vec();
        }
        Ok((r, bytes))
    }
}

/// Re-read a written day and check every kept bar is byte-identical and every
/// added bar carries bit 1.
fn verify_written(path: &Path, before: Option<&[u8]>, expect: &[u8]) -> Result<()> {
    let got = fs::read(path).with_context(|| format!("re-read {}", path.display()))?;
    ensure!(
        got == expect,
        "{}: written bytes differ from the plan",
        path.display()
    );
    let old: HashMap<i64, [u8; STRIDE]> = before
        .map(bars_of)
        .unwrap_or_default()
        .iter()
        .map(|b| (b.open_time_ms(), bar_bytes(b)))
        .collect();
    for b in bars_of(&got) {
        match old.get(&b.open_time_ms()) {
            Some(orig) if *orig == bar_bytes(&b) => {}
            _ => ensure!(
                b.flags & FLAG_HISTORICAL_BACKFILL != 0,
                "{}: bar at {} is neither kept byte-identical nor flagged",
                path.display(),
                b.open_time_ms()
            ),
        }
    }
    Ok(())
}

fn quarantine_dir(root: &Path, run_id: &str, id: u64) -> PathBuf {
    root.join("migrations")
        .join("quarantine")
        .join(run_id)
        .join("bars")
        .join(id.to_string())
}

/// Byte copy of `original` in the quarantine dir. An existing copy (a resumed
/// run) must already be identical.
fn ensure_quarantined(qdir: &Path, d: NaiveDate, original: &[u8]) -> Result<()> {
    let q = shard_path(qdir, d, "s10");
    if !q.exists() {
        write_shard_atomic(&q, original)?;
    }
    ensure!(
        fs::read(&q)? == original,
        "quarantine copy {} differs from the original",
        q.display()
    );
    Ok(())
}

/// Append-only, fsync'd JSONL journal.
struct Journal(fs::File);

impl Journal {
    fn append(&mut self, phase: &str, body: &impl Serialize) -> Result<()> {
        let mut v = serde_json::to_value(body)?;
        v.as_object_mut()
            .context("journal line must be an object")?
            .insert("phase".into(), phase.into());
        let mut line = serde_json::to_string(&v)?;
        line.push('\n');
        self.0.write_all(line.as_bytes())?;
        self.0.sync_data()?;
        Ok(())
    }
}

fn read_journal(path: &Path) -> Result<Vec<Value>> {
    let raw = fs::read_to_string(path).with_context(|| format!("read journal {}", path.display()))?;
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(n, l)| {
            serde_json::from_str(l).with_context(|| format!("{}:{}", path.display(), n + 1))
        })
        .collect()
}

#[derive(Debug, Default, Clone)]
struct Prior {
    sha_before: Option<String>,
    sha_planned: Option<String>,
    /// `Some(sha_after)` once the day's `done` line is durable.
    done: Option<Option<String>>,
}

fn opt_str(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

/// Per-date state of the mutating days in a journal.
fn prior_days(lines: &[Value]) -> Result<BTreeMap<String, Prior>> {
    let mut out: BTreeMap<String, Prior> = BTreeMap::new();
    for l in lines {
        let phase = l.get("phase").and_then(Value::as_str).unwrap_or("");
        let date = || opt_str(l, "date").context("journal line without date");
        match phase {
            "intent" => {
                let p = out.entry(date()?).or_default();
                p.sha_before = opt_str(l, "sha_before");
                p.sha_planned = opt_str(l, "sha_planned");
            }
            "done" => {
                let d = date()?;
                let p = out.get_mut(&d).with_context(|| format!("done before intent for {d}"))?;
                p.done = Some(opt_str(l, "sha_after"));
            }
            _ => {}
        }
    }
    Ok(out)
}

#[derive(Debug, Serialize)]
struct Start {
    run_id: String,
    mode: String,
    target_id: u64,
    from: String,
    to: String,
    plan_sha256: String,
    band_sha256: String,
    band_close_bp: f64,
    band_wick_bp: f64,
}

/// Open the journal: a fresh one gets the start line; `--resume` requires the
/// same start line and returns the per-day state of the crashed run.
fn open_journal(
    path: &Path,
    start: &Start,
    resume: bool,
) -> Result<(Journal, BTreeMap<String, Prior>)> {
    let mut want = serde_json::to_value(start)?;
    want.as_object_mut()
        .expect("object")
        .insert("phase".into(), "start".into());
    let prior = if resume {
        let lines = read_journal(path)?;
        ensure!(
            lines.first() == Some(&want),
            "{}: start line differs from this invocation (run id, mode, range, plan or band changed): refuse to resume",
            path.display()
        );
        ensure!(
            !lines.iter().any(|l| l.get("phase").and_then(Value::as_str)
                == Some("rolled_back")),
            "{}: run was rolled back",
            path.display()
        );
        prior_days(&lines)?
    } else {
        ensure!(
            !path.exists(),
            "{} exists: use --resume, or a new run id and report",
            path.display()
        );
        BTreeMap::new()
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open journal {}", path.display()))?;
    let mut j = Journal(f);
    if resume {
        j.append("resume", &json!({}))?;
    } else {
        j.append("start", start)?;
    }
    Ok((j, prior))
}

/// Refresh the manifest from the files on disk, dropping entries for days
/// that no longer have a file (`refresh_kind` alone keeps them).
fn refresh_manifest(out_dir: &Path, target_dir: &Path, symbol: &str, id: u64) -> Result<()> {
    let mut m = read_manifest(&manifest_path(target_dir))?
        .unwrap_or_else(|| Manifest::new(symbol.to_string(), id, "s10"));
    m.refresh_kind::<Bar>(out_dir, "s10")?;
    let on_disk: BTreeSet<String> = nxr_sdk::shard::list_shards(out_dir, "s10")?
        .into_iter()
        .map(|(d, _)| date_stem(d))
        .collect();
    let kept: Vec<_> = m
        .shards
        .iter()
        .filter(|s| on_disk.contains(&s.date))
        .cloned()
        .collect();
    m.set_shards_batch(kept);
    write_manifest(&manifest_path(out_dir), &m)
}

fn days(from: NaiveDate, to: NaiveDate) -> impl Iterator<Item = NaiveDate> {
    std::iter::successors(Some(from), move |d| d.succ_opt().filter(|n| *n <= to))
}

fn run(args: &Args) -> Result<Summary> {
    ensure!(
        quote_of(args.target_id) == USD_QUOTE,
        "target {} is not USD-quoted (quote {:?}): one native per asset, USD only",
        args.target_id,
        quote_of(args.target_id)
    );
    ensure!(
        !args.run_id.is_empty()
            && args
                .run_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "run id must be [A-Za-z0-9_-]+"
    );
    if args.rollback {
        return rollback(args);
    }
    let req = |o: &Option<PathBuf>, k: &str| o.clone().with_context(|| format!("{k} required"));
    let (plan_p, band_p) = (req(&args.plan, "--plan")?, req(&args.band, "--band")?);
    let symbol = args.symbol.clone().context("--symbol required")?;
    let from = parse_utc_date(args.from.as_deref().context("--from required")?)?;
    let to = parse_utc_date(args.to.as_deref().context("--to required")?)?;
    ensure!(from <= to, "--from after --to");
    ensure!(
        to < today_utc(),
        "--to {to} is today or later: never touch a live shard"
    );

    let target_dir = bars_dir(&args.data_root, args.target_id);
    let windows = load_plan(&plan_p, &target_dir, args.target_id)?;
    let band = load_band(&band_p, args.band_close_bp, args.band_wick_bp)?;
    let mode = match (&args.stage, args.apply) {
        (Some(_), _) => "stage",
        (None, true) => "apply",
        (None, false) => "dry-run",
    };
    let out_root = args.stage.clone().unwrap_or_else(|| args.data_root.clone());
    let out_dir = bars_dir(&out_root, args.target_id);
    if let Some(stage) = &args.stage {
        let canon = |p: &Path| fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        ensure!(
            canon(stage) != canon(&args.data_root),
            "--stage must not be the data root"
        );
        fs::create_dir_all(&out_dir)?;
    }
    let qdir = quarantine_dir(&out_root, &args.run_id, args.target_id);

    let mut ctx = Ctx {
        windows,
        band,
        target_dir: target_dir.clone(),
        from,
        to,
        src_cache: HashMap::new(),
    };
    ctx.boundary_check()?;

    let start = Start {
        run_id: args.run_id.clone(),
        mode: mode.into(),
        target_id: args.target_id,
        from: date_stem(from),
        to: date_stem(to),
        plan_sha256: sha256_file(&plan_p)?,
        band_sha256: sha256_file(&band_p)?,
        band_close_bp: args.band_close_bp,
        band_wick_bp: args.band_wick_bp,
    };
    let (mut journal, prior) = open_journal(&args.report, &start, args.resume)?;
    let mut s = Summary {
        run_id: args.run_id.clone(),
        mode: mode.into(),
        target_id: args.target_id,
        ..Default::default()
    };

    // Original bytes of a day: for a day the crashed run already touched, the
    // quarantine copy (its target may hold the planned output by now).
    let original = |d: NaiveDate, p: Option<&Prior>| -> Result<Option<Vec<u8>>> {
        let path = shard_path(&target_dir, d, "s10");
        let Some(p) = p else {
            return read_target(&path);
        };
        let Some(want) = &p.sha_before else {
            return Ok(None);
        };
        let q = shard_path(&qdir, d, "s10");
        let bytes = if q.exists() {
            fs::read(&q)?
        } else {
            read_target(&path)?.with_context(|| format!("{d}: original file is gone"))?
        };
        ensure!(
            sha_hex(&bytes) == *want,
            "{d}: original bytes differ from the journal intent"
        );
        Ok(Some(bytes))
    };

    // Pass 1: plan every day. Every refusal fires here, before any write.
    let mut planned: Vec<(NaiveDate, Option<DayReport>)> = Vec::new();
    for d in days(from, to) {
        let p = prior.get(&date_stem(d));
        if let Some(Prior {
            done: Some(after), ..
        }) = p
        {
            ensure!(
                sha_opt(&shard_path(&out_dir, d, "s10"))? == *after,
                "{d}: journal says done but the file changed since"
            );
            planned.push((d, None));
            continue;
        }
        let before = original(d, p)?;
        let (r, bytes) = ctx.day(d, before.as_deref())?;
        if let Some(p) = p {
            ensure!(
                r.sha_planned == p.sha_planned,
                "{d}: re-plan differs from the journal intent (inputs changed): refuse to resume"
            );
        }
        ensure!(
            mode == "dry-run" || r.dup_conflict == 0,
            "{d}: {} bucket(s) held by the previous file with different bytes: resolve before writing",
            r.dup_conflict
        );
        if mode == "dry-run" {
            s.add(&r, bytes.len() / STRIDE);
            journal.append("plan", &r)?;
        }
        planned.push((d, Some(r)));
    }
    if mode == "dry-run" {
        journal.append("summary", &s)?;
        return Ok(s);
    }

    // Pass 2: re-plan, then journal intent -> quarantine -> write -> done.
    let mut wrote = false;
    for (d, pr) in planned {
        let Some(pr) = pr else {
            s.resumed_done += 1;
            continue;
        };
        let p = prior.get(&date_stem(d));
        let before = original(d, p)?;
        let (mut r, bytes) = ctx.day(d, before.as_deref())?;
        ensure!(
            r.sha_before == pr.sha_before && r.sha_planned == pr.sha_planned,
            "{d}: plan changed between preflight and write (a file changed during the run)"
        );
        let action = r.action.expect("set");
        let dst = shard_path(&out_dir, d, "s10");
        let mutates = !matches!(
            (mode, action),
            (_, DayAction::Absent) | ("apply", DayAction::Unchanged)
        );
        if !mutates {
            s.add(&r, bytes.len() / STRIDE);
            journal.append("plan", &r)?;
            continue;
        }
        if p.is_none() {
            journal.append("intent", &r)?;
        }
        if let Some(orig) = before.as_deref() {
            if action != DayAction::Unchanged {
                ensure_quarantined(&qdir, d, orig)?;
            }
        }
        match action {
            DayAction::Emptied => {
                if mode == "apply" && dst.exists() {
                    ensure!(
                        sha_opt(&dst)? == r.sha_before,
                        "{} changed during the run",
                        dst.display()
                    );
                    fs::remove_file(&dst)?;
                }
            }
            _ => {
                let cur = if mode == "apply" { sha_opt(&dst)? } else { None };
                if mode != "apply" || cur != r.sha_planned {
                    ensure!(
                        mode != "apply" || cur == r.sha_before,
                        "{} changed during the run",
                        dst.display()
                    );
                    write_shard_atomic(&dst, &bytes)?;
                }
                verify_written(&dst, before.as_deref(), &bytes)?;
            }
        }
        wrote = true;
        r.sha_after = sha_opt(&dst)?;
        if mode == "apply" {
            ensure!(
                r.sha_after == r.sha_planned,
                "{d}: sha_after != sha_planned"
            );
        }
        journal.append(
            "done",
            &json!({"date": r.date, "action": action, "sha_after": r.sha_after}),
        )?;
        s.add(&r, bytes.len() / STRIDE);
    }

    if wrote || s.resumed_done > 0 {
        refresh_manifest(&out_dir, &target_dir, &symbol, args.target_id)?;
    }
    journal.append("summary", &s)?;
    Ok(s)
}

/// Undo an `--apply` run from its journal: every day with an `intent` line goes
/// back to its original bytes (quarantine copy) or, if it had no file, is
/// removed. A file that is neither the original nor this run's planned output
/// refuses the rollback (someone else wrote it).
fn rollback(args: &Args) -> Result<Summary> {
    let lines = read_journal(&args.report)?;
    let start = lines
        .first()
        .filter(|l| l.get("phase").and_then(Value::as_str) == Some("start"))
        .context("journal has no start line")?;
    ensure!(
        start.get("mode").and_then(Value::as_str) == Some("apply")
            && start.get("run_id").and_then(Value::as_str) == Some(args.run_id.as_str())
            && start.get("target_id").and_then(Value::as_u64) == Some(args.target_id),
        "journal is not an --apply run of {} / {}",
        args.run_id,
        args.target_id
    );
    let prior = prior_days(&lines)?;
    let target_dir = bars_dir(&args.data_root, args.target_id);
    let qdir = quarantine_dir(&args.data_root, &args.run_id, args.target_id);
    let f = fs::OpenOptions::new().append(true).open(&args.report)?;
    let mut journal = Journal(f);
    let mut s = Summary {
        run_id: args.run_id.clone(),
        mode: "rollback".into(),
        target_id: args.target_id,
        ..Default::default()
    };
    // Plan the whole rollback before touching a file.
    let mut steps = Vec::new();
    for (date, p) in prior.iter().rev() {
        let d = parse_utc_date(date)?;
        let path = shard_path(&target_dir, d, "s10");
        let cur = sha_opt(&path)?;
        let step = match &p.sha_before {
            Some(b) if cur.as_ref() == Some(b) => "untouched",
            Some(b) => {
                ensure!(
                    cur.is_none() || cur == p.sha_planned,
                    "{date}: file is neither the original nor this run's output: refuse"
                );
                let q = shard_path(&qdir, d, "s10");
                ensure!(
                    q.exists() && sha256_file(&q)? == *b,
                    "{date}: quarantine copy missing or differs from the journal"
                );
                "restore"
            }
            None => match &cur {
                None => "untouched",
                Some(c) => {
                    ensure!(
                        Some(c) == p.sha_planned.as_ref(),
                        "{date}: file is not this run's output: refuse"
                    );
                    "remove"
                }
            },
        };
        steps.push((d, path, step, p.sha_before.clone()));
    }
    for (d, path, step, sha_before) in steps {
        let tmp = path.with_extension("s10.tmp");
        if tmp.exists() {
            fs::remove_file(&tmp)?;
        }
        match step {
            "restore" => {
                let orig = fs::read(shard_path(&qdir, d, "s10"))?;
                write_shard_atomic(&path, &orig)?;
                ensure!(sha_opt(&path)? == sha_before, "{d}: restore verify failed");
            }
            "remove" => fs::remove_file(&path)?,
            _ => {}
        }
        journal.append("rolled_back", &json!({"date": date_stem(d), "result": step}))?;
        s.days += 1;
        *s.by_action.entry(step.to_string()).or_default() += 1;
    }
    if target_dir.exists() {
        let sym = read_manifest(&manifest_path(&target_dir))?
            .map(|m| m.ticker)
            .or_else(|| args.symbol.clone())
            .unwrap_or_default();
        refresh_manifest(&target_dir, &target_dir, &sym, args.target_id)?;
    }
    journal.append("summary", &s)?;
    Ok(s)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let args = Args::parse();
    let s = run(&args)?;
    println!("{}", serde_json::to_string_pretty(&s)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nxr_sdk::shard::{list_shards, FLAG_S10_FLAT_FILL};
    use std::sync::atomic::{AtomicU32, Ordering};

    // BNB/USD and BNB/USDT, as on the prod PVC.
    const USD_ID: u64 = 434435947234328576;
    const USDT_ID: u64 = 434436166604816384;
    const BTC_USDT_ID: u64 = 435315775907037184;
    const D: &str = "2026-01-02";

    fn tmp(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "bfns10-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn day0(d: &str) -> i64 {
        day_start_ms(parse_utc_date(d).unwrap())
    }

    fn bar(ts: i64, px: f64, tc: u32, flags: u8) -> Bar {
        let mut b = Bar::new_ohlcv(
            mitch::timestamp::from_epoch_ms(ts),
            mitch::timestamp::from_epoch_ms(ts + BAR_MS_S10 - 1),
            px,
            px * 1.001,
            px * 0.999,
            px,
            7,
            9,
            tc,
        );
        b.flags = flags;
        b.avg_spread_bps = 1.5;
        b
    }

    fn ohlc(ts: i64, o: f64, h: f64, l: f64, c: f64, tc: u32) -> Bar {
        let mut b = bar(ts, c, tc, 0);
        (b.open, b.high, b.low, b.close) = (o, h, l, c);
        b
    }

    /// Write a day file whose mtime is an hour old (clears the hot-file guard).
    fn put(dir: &Path, d: &str, bars: &[Bar]) -> PathBuf {
        let p = shard_path(dir, parse_utc_date(d).unwrap(), "s10");
        let mut bytes = Vec::new();
        for b in bars {
            bytes.extend_from_slice(&bar_bytes(b));
        }
        write_shard_atomic(&p, &bytes).unwrap();
        age(&p);
        p
    }

    fn age(p: &Path) {
        let f = fs::File::options().write(true).open(p).unwrap();
        f.set_modified(SystemTime::now() - Duration::from_secs(3600))
            .unwrap();
    }

    fn series(from: i64, n: i64, px: f64) -> Vec<Bar> {
        (0..n)
            .map(|i| {
                bar(
                    from + i * BAR_MS_S10,
                    px,
                    (i % 3) as u32,
                    if i % 5 == 0 { FLAG_S10_FLAT_FILL } else { 0 },
                )
            })
            .collect()
    }

    fn mult_csv(root: &Path, rows: &[(i64, f64)]) -> PathBuf {
        let p = root.join("mult.csv");
        let mut s = String::from("ts_ms,mult\n");
        for (t, m) in rows {
            s.push_str(&format!("{t},{m}\n"));
        }
        fs::write(&p, s).unwrap();
        p
    }

    fn minute_mult(from: i64, minutes: i64, m: f64) -> Vec<(i64, f64)> {
        (0..minutes).map(|i| (from + i * 60_000, m)).collect()
    }

    /// Band rows; `None` = unverifiable minute.
    fn band_csv(root: &Path, rows: &[(i64, Option<(f64, f64)>)]) -> PathBuf {
        let p = root.join("band.csv");
        let mut s = String::from("minute_ms,lo,hi\n");
        for (t, v) in rows {
            match v {
                Some((lo, hi)) => s.push_str(&format!("{t},{lo},{hi}\n")),
                None => s.push_str(&format!("{t},,\n")),
            }
        }
        fs::write(&p, s).unwrap();
        p
    }

    /// Wide band over 2025-12-30 .. 2026-01-07: every bar in these tests passes.
    fn wide_band(root: &Path) -> PathBuf {
        let from = day0("2025-12-30");
        band_csv(
            root,
            &(0..9 * 1440)
                .map(|i| (from + i * 60_000, Some((1.0, 1e6))))
                .collect::<Vec<_>>(),
        )
    }

    fn plan(root: &Path, rows: &[String]) -> PathBuf {
        let p = root.join("plan.csv");
        fs::write(
            &p,
            format!(
                "from_ms,to_ms,action,source,mult,mult_max_age_s,tag\n{}\n",
                rows.join("\n")
            ),
        )
        .unwrap();
        p
    }

    fn fill(from: i64, to: i64, src: &Path, mult: &Path, age_s: i64) -> String {
        format!(
            "{from},{to},fill,{},{},{age_s},parked",
            src.display(),
            mult.display()
        )
    }

    fn args(root: &Path, plan: PathBuf, from: &str, to: &str) -> Args {
        let band = root.join("band.csv");
        if !band.exists() {
            wide_band(root);
        }
        Args {
            data_root: root.join("data"),
            target_id: USD_ID,
            symbol: Some("BNB/USD".into()),
            plan: Some(plan),
            band: Some(band),
            band_close_bp: 25.0,
            band_wick_bp: 50.0,
            from: Some(from.into()),
            to: Some(to.into()),
            stage: None,
            apply: true,
            resume: false,
            rollback: false,
            run_id: "t1".into(),
            report: root.join(format!(
                "report-{}.jsonl",
                N_REPORT.fetch_add(1, Ordering::Relaxed)
            )),
        }
    }

    static N_REPORT: AtomicU32 = AtomicU32::new(0);

    fn setup(tag: &str) -> (PathBuf, PathBuf, PathBuf) {
        let root = tmp(tag);
        let tgt = bars_dir(&root.join("data"), USD_ID);
        let src = root.join("src").join(USDT_ID.to_string());
        fs::create_dir_all(&tgt).unwrap();
        fs::create_dir_all(&src).unwrap();
        (root, tgt, src)
    }

    fn read(p: &Path) -> Vec<Bar> {
        read_shard_aligned::<Bar>(p).unwrap()
    }

    fn qpath(root: &Path, d: &str) -> PathBuf {
        shard_path(
            &quarantine_dir(&root.join("data"), "t1", USD_ID),
            parse_utc_date(d).unwrap(),
            "s10",
        )
    }

    fn day_lines(report: &Path, phase: &str) -> Vec<Value> {
        read_journal(report)
            .unwrap()
            .into_iter()
            .filter(|l| l["phase"] == phase)
            .collect()
    }

    #[test]
    fn untouched_day_keeps_bytes_and_mtime() {
        let (root, tgt, src) = setup("untouched");
        let d0 = day0(D);
        let p = put(&tgt, D, &series(d0, 100, 700.0));
        let before = (
            fs::read(&p).unwrap(),
            fs::metadata(&p).unwrap().modified().unwrap(),
        );
        put(&src, "2026-01-03", &series(day0("2026-01-03"), 8640, 700.0));
        let m = mult_csv(&root, &minute_mult(day0("2026-01-03"), 1440, 0.999));
        let pl = plan(
            &root,
            &[fill(day0("2026-01-03"), day0("2026-01-04"), &src, &m, 60)],
        );
        let s = run(&args(&root, pl, D, "2026-01-03")).unwrap();
        assert_eq!(s.by_action.get("unchanged"), Some(&1));
        assert_eq!(s.by_action.get("new"), Some(&1));
        assert_eq!(fs::read(&p).unwrap(), before.0);
        assert_eq!(fs::metadata(&p).unwrap().modified().unwrap(), before.1);
    }

    #[test]
    fn partial_merge_keeps_existing_bytes_and_flags_new_bars() {
        let (root, tgt, src) = setup("merge");
        let d0 = day0(D);
        let existing = series(d0, 60, 700.0);
        let p = put(&tgt, D, &existing);
        let orig = fs::read(&p).unwrap();
        let source = series(d0, 8640, 699.0);
        put(&src, D, &source);
        let m = mult_csv(&root, &minute_mult(d0, 1440, 0.999));
        let pl = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &src, &m, 60)]);
        let s = run(&args(&root, pl, D, D)).unwrap();
        assert_eq!(s.by_action.get("merge"), Some(&1));
        let got = read(&p);
        assert_eq!(got.len(), 8640);
        for (i, b) in got.iter().enumerate() {
            if i < 60 {
                assert_eq!(
                    bar_bytes(b),
                    bar_bytes(&existing[i]),
                    "kept bar {i} changed"
                );
            } else {
                let sb = &source[i];
                assert_ne!(b.flags & FLAG_HISTORICAL_BACKFILL, 0, "bit1 missing at {i}");
                assert_eq!(
                    b.flags & FLAG_S10_FLAT_FILL,
                    sb.flags & FLAG_S10_FLAT_FILL,
                    "flat flag lost at {i}"
                );
                assert_eq!({ b.tick_count }, { sb.tick_count });
                assert!((b.close - sb.close * 0.999).abs() < 1e-9);
                assert_eq!({ b.avg_spread_bps }, { sb.avg_spread_bps });
            }
        }
        assert_eq!(fs::read(qpath(&root, D)).unwrap(), orig);
    }

    #[test]
    fn stale_multiplier_leaves_a_hole() {
        let (root, _tgt, src) = setup("mhole");
        let d0 = day0(D);
        put(&src, D, &series(d0, 8640, 700.0));
        // One row at 00:00 and one at 12:00; 60s max age.
        let m = mult_csv(&root, &[(d0, 0.999), (d0 + 43_200_000, f64::NAN)]);
        let pl = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &src, &m, 60)]);
        let s = run(&args(&root, pl, D, D)).unwrap();
        assert_eq!(s.backfilled, 6);
        assert_eq!(s.holes, 8640 - 6);
    }

    #[test]
    fn replace_and_hole_quarantine_what_they_drop() {
        let (root, tgt, src) = setup("replace");
        let d0 = day0(D);
        let h = 3_600_000;
        let existing = series(d0, 8640, 700.0);
        let p = put(&tgt, D, &existing);
        let orig = fs::read(&p).unwrap();
        put(&src, D, &series(d0, 8640, 650.0));
        let m = mult_csv(&root, &minute_mult(d0, 1440, 1.001));
        let pl = plan(
            &root,
            &[
                format!("{},{},hole,-,-,-,poison", d0 + h, d0 + 2 * h),
                format!(
                    "{},{},replace,{},{},60,numfree",
                    d0 + 3 * h,
                    d0 + 4 * h,
                    src.display(),
                    m.display()
                ),
            ],
        );
        let s = run(&args(&root, pl, D, D)).unwrap();
        assert_eq!(s.quarantined, 720);
        assert_eq!(s.backfilled, 360);
        assert_eq!(s.holes, 360);
        let got = read(&p);
        assert_eq!(got.len(), 8640 - 360);
        assert!(got
            .iter()
            .all(|b| !(d0 + h..d0 + 2 * h).contains(&b.open_time_ms())));
        let replaced: Vec<_> = got
            .iter()
            .filter(|b| (d0 + 3 * h..d0 + 4 * h).contains(&b.open_time_ms()))
            .collect();
        assert_eq!(replaced.len(), 360);
        assert!(replaced
            .iter()
            .all(|b| b.flags & FLAG_HISTORICAL_BACKFILL != 0 && (b.close - 650.65).abs() < 1e-9));
        assert_eq!(fs::read(qpath(&root, D)).unwrap(), orig);
    }

    /// Regression, staged BNB/USD 2026-09-07T00:33:20: native O 708.59 / C 746.79
    /// x numfree mult 1.0609 = C 792.30 against a Binance x CB minute range of
    /// [751.6, 752.2] (+533bp). The minute's last close was clean, so a
    /// minute-last check passed it with bit 1. Per bar it is a hole.
    #[test]
    fn bar_poisoned_inside_a_clean_minute_is_a_hole_not_a_backfill() {
        let (root, _tgt, src) = setup("poisonbar");
        let m0 = day0(D) + 33 * 60_000;
        let (t_bad, t_ok) = (m0 + 20_000, m0 + 50_000);
        // Sources: the traced poisoned bar and a clean one at the minute's end.
        put(
            &src,
            D,
            &[
                ohlc(t_bad, 708.59, 746.79, 708.59, 746.79, 50),
                ohlc(t_ok, 708.70, 708.80, 708.60, 708.70, 12),
            ],
        );
        let m = mult_csv(&root, &[(m0, 1.0609)]);
        band_csv(&root, &[(m0, Some((751.6, 752.2)))]);
        let pl = plan(
            &root,
            &[format!(
                "{},{},replace,{},{},60,numfree",
                m0,
                m0 + 60_000,
                src.display(),
                m.display()
            )],
        );
        let s = run(&args(&root, pl, D, D)).unwrap();
        assert_eq!((s.backfilled, s.band_rejected), (1, 1));
        let got = read(&shard_path(
            &bars_dir(&root.join("data"), USD_ID),
            parse_utc_date(D).unwrap(),
            "s10",
        ));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].open_time_ms(), t_ok);
        assert!((got[0].close - 708.70 * 1.0609).abs() < 1e-9);
    }

    /// A kept observed bar with a wick outside the band (+189bp) is quarantined,
    /// and its bucket is refilled only by a source bar that passes the gate.
    /// A minute without a band row keeps its observed bar but refuses a
    /// `replace` bar; a `fill` bar there is written and counted.
    #[test]
    fn kept_wick_is_quarantined_and_unverified_minutes_are_counted() {
        let (root, tgt, src) = setup("wick");
        let d0 = day0(D);
        let (m0, m1, m2) = (d0, d0 + 60_000, d0 + 120_000);
        let p = put(
            &tgt,
            D,
            &[
                ohlc(m0, 752.0, 752.1, 751.9, 752.0, 9),
                ohlc(m0 + 10_000, 752.0, 752.2 * 1.0189, 751.9, 752.0, 9),
                ohlc(m2, 752.0, 752.1, 751.9, 752.0, 9),
            ],
        );
        let orig = fs::read(&p).unwrap();
        put(
            &src,
            D,
            &[
                ohlc(m0 + 10_000, 752.0, 752.1, 751.9, 752.0, 4),
                ohlc(m1, 752.0, 752.1, 751.9, 752.0, 4),
                ohlc(m2 + 10_000, 752.0, 752.1, 751.9, 752.0, 4),
            ],
        );
        let m = mult_csv(&root, &minute_mult(d0, 3, 1.0));
        band_csv(
            &root,
            &[(m0, Some((751.6, 752.2))), (m1, None), (m2, None)],
        );
        let pl = plan(
            &root,
            &[
                fill(m0, m2, &src, &m, 60),
                format!("{},{},replace,{},{},60,numfree", m2 + 10_000, m2 + 20_000, src.display(), m.display()),
            ],
        );
        let s = run(&args(&root, pl, D, D)).unwrap();
        assert_eq!(s.band_quarantined, 1);
        assert_eq!(s.kept_unverified, 1);
        assert_eq!(s.backfilled_unverified, 1);
        assert_eq!(s.unverified_rejected, 1);
        let got: Vec<(i64, bool)> = read(&p)
            .iter()
            .map(|b| (b.open_time_ms(), b.flags & FLAG_HISTORICAL_BACKFILL != 0))
            .collect();
        assert_eq!(
            got,
            vec![(m0, false), (m0 + 10_000, true), (m1, true), (m2, false)]
        );
        assert_eq!(fs::read(qpath(&root, D)).unwrap(), orig);
    }

    #[test]
    fn refuses_non_usd_target_live_day_torn_hot_files_and_foreign_base() {
        let (root, tgt, src) = setup("refuse");
        let d0 = day0(D);
        put(&src, D, &series(d0, 10, 700.0));
        let m = mult_csv(&root, &minute_mult(d0, 10, 1.0));
        let pl = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &src, &m, 60)]);

        let mut a = args(&root, pl.clone(), D, D);
        a.target_id = USDT_ID;
        assert!(run(&a).unwrap_err().to_string().contains("not USD-quoted"));

        let today = date_stem(today_utc());
        assert!(run(&args(&root, pl.clone(), D, &today))
            .unwrap_err()
            .to_string()
            .contains("live shard"));

        let p = shard_path(&tgt, parse_utc_date(D).unwrap(), "s10");
        fs::write(&p, vec![0u8; STRIDE + 5]).unwrap();
        age(&p);
        assert!(run(&args(&root, pl.clone(), D, D))
            .unwrap_err()
            .to_string()
            .contains("torn"));

        fs::write(&p, bar_bytes(&bar(d0, 700.0, 1, 0))).unwrap();
        assert!(run(&args(&root, pl.clone(), D, D))
            .unwrap_err()
            .to_string()
            .contains("writer may hold"));

        // A source that is the target tree is refused at plan load.
        let bad = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &tgt, &m, 60)]);
        age(&p);
        assert!(run(&args(&root, bad, D, D))
            .unwrap_err()
            .to_string()
            .contains("source is the target tree"));

        // A parked tree of another base asset (BTC/USDT for BNB/USD).
        let btc = root.join("src").join(BTC_USDT_ID.to_string());
        put(&btc, D, &series(d0, 10, 70_000.0));
        let foreign = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &btc, &m, 60)]);
        assert!(run(&args(&root, foreign, D, D))
            .unwrap_err()
            .to_string()
            .contains("another base asset"));

        // An existing journal is never clobbered.
        let good = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &src, &m, 60)]);
        let a = args(&root, good, D, D);
        fs::write(&a.report, "{}\n").unwrap();
        let err = run(&a).unwrap_err().to_string();
        assert!(err.contains("--resume"), "{err}");
    }

    #[test]
    fn midnight_bar_held_by_the_previous_file_is_not_written_twice() {
        let (root, tgt, src) = setup("midnight");
        let prev = "2026-01-01";
        let d0 = day0(D);
        // Live convention: the previous file owns (prev 00:00:10 .. D 00:00:00].
        put(&tgt, prev, &series(day0(prev) + BAR_MS_S10, 8640, 700.0));
        put(&src, D, &series(d0, 8640, 700.0));
        let m = mult_csv(&root, &minute_mult(d0, 1440, 1.0));
        let pl = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &src, &m, 60)]);
        let s = run(&args(&root, pl, D, D)).unwrap();
        assert_eq!(s.backfilled, 8639);
        assert_eq!(s.holes, 0);
        let mut all: Vec<i64> = read(&shard_path(&tgt, parse_utc_date(prev).unwrap(), "s10"))
            .iter()
            .chain(read(&shard_path(&tgt, parse_utc_date(D).unwrap(), "s10")).iter())
            .map(|b| b.open_time_ms())
            .collect();
        let n = all.len();
        all.dedup();
        assert_eq!(all.len(), n, "a bucket is on disk twice");
    }

    /// §5.3 class: the file of `--from - 1` owns `--from`'s midnight bar and the
    /// plan holes it. Running from `--from` alone would leave it; the writer
    /// refuses, and the run from the previous day removes it.
    #[test]
    fn dropped_from_midnight_bar_held_by_the_previous_file_needs_the_previous_day() {
        let (root, tgt, _src) = setup("boundary");
        let prev = "2026-01-01";
        let d0 = day0(D);
        let pbars = series(day0(prev) + BAR_MS_S10, 8640, 700.0);
        let pp = put(&tgt, prev, &pbars);
        put(&tgt, D, &series(d0 + BAR_MS_S10, 100, 700.0));
        let pl = plan(&root, &[format!("{},{},hole,-,-,-,p2b", d0, d0 + 3_600_000)]);
        let err = run(&args(&root, pl.clone(), D, D)).unwrap_err().to_string();
        assert!(err.contains("--from 2026-01-01"), "{err}");
        assert_eq!(fs::read(&pp).unwrap().len(), 8640 * STRIDE, "refusal wrote");

        let s = run(&args(&root, pl, prev, D)).unwrap();
        assert_eq!(s.quarantined, 101);
        assert!(read(&pp).iter().all(|b| b.open_time_ms() < d0));
        assert_eq!(fs::read(qpath(&root, prev)).unwrap().len(), 8640 * STRIDE);
    }

    #[test]
    fn identical_midnight_duplicate_is_dropped_from_the_later_file() {
        let (root, tgt, src) = setup("dedup");
        let prev = "2026-01-01";
        let d0 = day0(D);
        // prev (live convention) owns D 00:00:00; D (repaired convention) holds it too.
        let pbars = series(day0(prev) + BAR_MS_S10, 8640, 700.0);
        put(&tgt, prev, &pbars);
        let mut dbars = vec![pbars[8639]];
        dbars.extend(series(d0 + BAR_MS_S10, 100, 701.0));
        let p = put(&tgt, D, &dbars);
        let orig = fs::read(&p).unwrap();
        let m = mult_csv(&root, &minute_mult(d0, 10, 1.0));
        let pl = plan(
            &root,
            &[fill(day0("2026-01-05"), day0("2026-01-06"), &src, &m, 60)],
        );
        let s = run(&args(&root, pl, prev, D)).unwrap();
        assert_eq!((s.dedup_identical, s.dup_conflict), (1, 0));
        let got = read(&p);
        assert_eq!(got.len(), 100);
        assert_eq!(got[0].open_time_ms(), d0 + BAR_MS_S10);
        assert_eq!(fs::read(qpath(&root, D)).unwrap(), orig);
    }

    /// A differing copy of the same bucket in adjacent files would leave two
    /// bars for one bucket: dry-run reports it, stage and apply refuse before
    /// any write.
    #[test]
    fn differing_midnight_duplicate_refuses_to_write() {
        let (root, tgt, src) = setup("dupconflict");
        let prev = "2026-01-01";
        let d0 = day0(D);
        let pbars = series(day0(prev) + BAR_MS_S10, 8640, 700.0);
        let pp = put(&tgt, prev, &pbars);
        let mut other = pbars[8639];
        other.close = 701.0;
        let p = put(&tgt, D, &[other]);
        put(&src, D, &series(d0, 8640, 700.0));
        let m = mult_csv(&root, &minute_mult(d0, 1440, 1.0));
        let pl = plan(&root, &[fill(d0, d0 + MS_PER_DAY, &src, &m, 60)]);
        let before = (fs::read(&pp).unwrap(), fs::read(&p).unwrap());

        let mut dry = args(&root, pl.clone(), prev, D);
        dry.apply = false;
        assert_eq!(run(&dry).unwrap().dup_conflict, 1);

        let err = run(&args(&root, pl.clone(), prev, D)).unwrap_err().to_string();
        assert!(err.contains("different bytes"), "{err}");
        let mut st = args(&root, pl, prev, D);
        st.apply = false;
        st.stage = Some(root.join("stage"));
        assert!(run(&st).is_err());
        assert_eq!((fs::read(&pp).unwrap(), fs::read(&p).unwrap()), before);
        assert!(!qpath(&root, D).exists(), "refusal quarantined");
    }

    #[test]
    fn overlapping_windows_are_rejected() {
        let (root, _tgt, src) = setup("overlap");
        let d0 = day0(D);
        let m = mult_csv(&root, &minute_mult(d0, 10, 1.0));
        let pl = plan(
            &root,
            &[
                fill(d0, d0 + 7_200_000, &src, &m, 60),
                format!("{},{},hole,-,-,-,x", d0 + 3_600_000, d0 + 10_800_000),
            ],
        );
        assert!(run(&args(&root, pl, D, D))
            .unwrap_err()
            .to_string()
            .contains("overlap"));
    }

    #[test]
    fn stage_writes_a_complete_tree_and_leaves_the_data_root_alone() {
        let (root, tgt, src) = setup("stage");
        let d0 = day0(D);
        let p = put(&tgt, D, &series(d0, 100, 700.0));
        let orig = fs::read(&p).unwrap();
        put(&src, "2026-01-03", &series(day0("2026-01-03"), 8640, 700.0));
        let m = mult_csv(&root, &minute_mult(day0("2026-01-03"), 1440, 0.999));
        let pl = plan(
            &root,
            &[fill(day0("2026-01-03"), day0("2026-01-04"), &src, &m, 60)],
        );
        let mut a = args(&root, pl, D, "2026-01-03");
        a.apply = false;
        a.stage = Some(root.join("stage"));
        run(&a).unwrap();
        assert_eq!(fs::read(&p).unwrap(), orig, "stage wrote the data root");
        assert!(!shard_path(&tgt, parse_utc_date("2026-01-03").unwrap(), "s10").exists());
        let sd = bars_dir(&root.join("stage"), USD_ID);
        // A leftover .tmp from a crash is invisible to the shard listing.
        fs::write(sd.join("2026-01-04.s10.tmp"), b"torn").unwrap();
        let listed: Vec<String> = list_shards(&sd, "s10")
            .unwrap()
            .into_iter()
            .map(|(d, _)| date_stem(d))
            .collect();
        assert_eq!(listed, vec![D.to_string(), "2026-01-03".to_string()]);
        assert_eq!(fs::read(sd.join(format!("{D}.s10"))).unwrap(), orig);
        assert!(manifest_path(&sd).exists());
    }

    /// Three days: merge, new, merge. Returns (root, plan, originals).
    fn three_days(tag: &str) -> (PathBuf, PathBuf, Vec<(String, Option<Vec<u8>>)>) {
        let (root, tgt, src) = setup(tag);
        let ds = ["2026-01-02", "2026-01-03", "2026-01-04"];
        put(&tgt, ds[0], &series(day0(ds[0]), 60, 700.0));
        put(&tgt, ds[2], &series(day0(ds[2]), 60, 700.0));
        for d in ds {
            put(&src, d, &series(day0(d), 8640, 699.0));
        }
        let m = mult_csv(&root, &minute_mult(day0(ds[0]), 3 * 1440, 1.001));
        let pl = plan(&root, &[fill(day0(ds[0]), day0("2026-01-05"), &src, &m, 60)]);
        let origs = ds
            .iter()
            .map(|d| {
                let p = shard_path(&tgt, parse_utc_date(d).unwrap(), "s10");
                (d.to_string(), p.exists().then(|| fs::read(&p).unwrap()))
            })
            .collect();
        (root, pl, origs)
    }

    fn tree_shas(dir_root: &Path) -> Vec<Option<String>> {
        ["2026-01-02", "2026-01-03", "2026-01-04"]
            .iter()
            .map(|d| {
                sha_opt(&shard_path(
                    &bars_dir(dir_root, USD_ID),
                    parse_utc_date(d).unwrap(),
                    "s10",
                ))
                .unwrap()
            })
            .collect()
    }

    /// The node dry-run's `sha_planned` is the gate for --apply: it must equal
    /// the staged tree's `sha_after` and the applied tree's, day by day.
    #[test]
    fn dry_run_sha_planned_equals_stage_and_apply_sha_after() {
        let (root, pl, _) = three_days("sha");
        let mut dry = args(&root, pl.clone(), "2026-01-02", "2026-01-04");
        dry.apply = false;
        run(&dry).unwrap();
        let planned: Vec<Option<String>> = day_lines(&dry.report, "plan")
            .iter()
            .map(|l| opt_str(l, "sha_planned"))
            .collect();
        let mut st = args(&root, pl.clone(), "2026-01-02", "2026-01-04");
        st.apply = false;
        st.stage = Some(root.join("stage"));
        run(&st).unwrap();
        assert_eq!(planned, tree_shas(&root.join("stage")));
        let ap = args(&root, pl, "2026-01-02", "2026-01-04");
        run(&ap).unwrap();
        assert_eq!(planned, tree_shas(&root.join("data")));
        let done: Vec<Option<String>> = day_lines(&ap.report, "done")
            .iter()
            .map(|l| opt_str(l, "sha_after"))
            .collect();
        assert_eq!(planned, done);
    }

    /// Crash states rebuilt from a finished run's journal:
    /// A: day 2 written, its `done` line lost; day 3 never started.
    /// B: day 3 intent + quarantine copy durable, target not yet written.
    /// --resume must reach the uninterrupted bytes; --rollback on the partial
    /// state must restore every original and remove the new day.
    #[test]
    fn crashed_apply_resumes_to_the_same_bytes_and_rolls_back() {
        for scenario in ["A", "B"] {
            let (root, pl, origs) = three_days(&format!("crash{scenario}"));
            let a = args(&root, pl.clone(), "2026-01-02", "2026-01-04");
            run(&a).unwrap();
            let full = tree_shas(&root.join("data"));
            let tgt = bars_dir(&root.join("data"), USD_ID);
            let lines = read_journal(&a.report).unwrap();
            // start, i1, d1, i2, d2, i3, d3, summary
            let keep = if scenario == "A" { 4 } else { 6 };
            let mut j = String::new();
            for l in &lines[..keep] {
                j.push_str(&serde_json::to_string(l).unwrap());
                j.push('\n');
            }
            fs::write(&a.report, j).unwrap();
            let (d3, o3) = &origs[2];
            let p3 = shard_path(&tgt, parse_utc_date(d3).unwrap(), "s10");
            fs::write(&p3, o3.as_ref().unwrap()).unwrap();
            age(&p3);
            if scenario == "A" {
                fs::remove_file(qpath(&root, d3)).unwrap();
            }
            let shas_partial = tree_shas(&root.join("data"));

            let mut re = a.clone();
            re.resume = true;
            let s = run(&re).unwrap();
            assert_eq!(s.resumed_done, if scenario == "A" { 1 } else { 2 });
            assert_eq!(tree_shas(&root.join("data")), full, "resume {scenario}");
            // Resuming a finished run is a no-op.
            run(&re).unwrap();
            assert_eq!(tree_shas(&root.join("data")), full);

            // Rollback from the finished journal restores everything.
            let mut rb = a.clone();
            rb.rollback = true;
            rb.apply = false;
            rb.plan = None;
            run(&rb).unwrap();
            for (d, o) in &origs {
                let p = shard_path(&tgt, parse_utc_date(d).unwrap(), "s10");
                assert_eq!(p.exists().then(|| fs::read(&p).unwrap()), *o, "rollback {d}");
            }
            let mut again = a.clone();
            again.resume = true;
            assert!(run(&again).unwrap_err().to_string().contains("rolled back"));

            // Rollback straight from the partial state (no resume first).
            let (root2, pl2, origs2) = three_days(&format!("crashrb{scenario}"));
            let a2 = args(&root2, pl2, "2026-01-02", "2026-01-04");
            run(&a2).unwrap();
            let tgt2 = bars_dir(&root2.join("data"), USD_ID);
            let lines2 = read_journal(&a2.report).unwrap();
            let mut j2 = String::new();
            for l in &lines2[..keep] {
                j2.push_str(&serde_json::to_string(l).unwrap());
                j2.push('\n');
            }
            fs::write(&a2.report, j2).unwrap();
            let p3b = shard_path(&tgt2, parse_utc_date(d3).unwrap(), "s10");
            fs::write(&p3b, origs2[2].1.as_ref().unwrap()).unwrap();
            if scenario == "A" {
                fs::remove_file(qpath(&root2, d3)).unwrap();
            }
            assert_eq!(tree_shas(&root2.join("data")), shas_partial);
            let mut rb2 = a2.clone();
            rb2.rollback = true;
            rb2.apply = false;
            run(&rb2).unwrap();
            for (d, o) in &origs2 {
                let p = shard_path(&tgt2, parse_utc_date(d).unwrap(), "s10");
                assert_eq!(p.exists().then(|| fs::read(&p).unwrap()), *o, "partial rollback {d}");
            }
        }
    }
}
