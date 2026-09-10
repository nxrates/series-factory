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
//! * `fill`: a bucket with no bar on disk gets `source bar x multiplier`.
//! * `replace`: bars on disk in the window are quarantined and dropped; every
//!   bucket gets `source bar x multiplier`.
//! * `hole`: bars on disk in the window are quarantined and dropped; nothing is
//!   written.
//!
//! Buckets outside every window stay exactly as they are on disk.
//!
//! The multiplier CSV (`ts_ms,mult`, ascending) is looked up at the bar OPEN:
//! the last row with `ts <= open`, valid only while `open - ts < mult_max_age_s`
//! and finite. A missing or stale multiplier leaves a hole, never a stale rebase.
//!
//! ## Guarantees
//!
//! * **s10 only.** `.idx`, `.renko` and `.vol` are never read or written.
//! * **One native per asset.** The target id must be USD-quoted (class 3 / id 5001).
//! * **Never a live shard.** `--to` must be before today (UTC). A target file
//!   written in the last 5 minutes, or with a torn tail, aborts the run.
//! * **Untouched days stay untouched.** A day no window changes is not written
//!   (`--apply`), or is copied byte-for-byte (`--stage`).
//! * **Kept bars are byte-identical.** Every written file is re-read, and each
//!   kept bar is compared against the bytes read before the write.
//! * **Provenance.** Every written bar has `FLAG_HISTORICAL_BACKFILL` (bit 1)
//!   OR'd in. Source `tick_count` and every other flag bit (incl.
//!   `FLAG_S10_FLAT_FILL`) are copied, so a carried bar stays a carried bar.
//! * **Quarantine first.** Before a day file is changed or removed, it is
//!   byte-copied to `<root>/migrations/quarantine/<run>/bars/<id>/<date>.s10`
//!   and the copy is compared.
//! * **Atomic.** Writes go through `write_shard_atomic` (`.tmp`, fsync,
//!   rename). Readers list `*.s10` only, so a crash leaves no readable partial.
//! * **No double midnight bar.** Writers disagree on which file owns the
//!   00:00:00 bucket (live: previous day; repaired/legacy: same day). A bucket
//!   already held by an adjacent day file is never written again. A kept bar
//!   whose byte-identical copy the previous day file holds is dropped from the
//!   later file (lossless; the day is quarantined first and the drop counted).
//!   A differing copy is kept and counted as `dup_conflict` (must be 0).
//! * **Source is never the target tree.** A source read from the directory
//!   being rewritten would re-read its own rebased output. Copy it first.
//! * **Dry run by default.** `--stage <dir>` writes a complete served tree off
//!   the PVC; `--apply` writes the data root.
//!
//! Sealed days are cached by core (`rest.rs load_s10_days`): a changed day is
//! served after the next core restart; a new day is served at once.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
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

const USD_QUOTE: (u64, u64) = (3, 5001);
const RECENT_WRITE: Duration = Duration::from_secs(300);
const STRIDE: usize = std::mem::size_of::<Bar>();
/// A multiplier outside this band is a broken input file, not a quote rebase.
const MULT_BAND: (f64, f64) = (0.8, 1.25);

#[derive(Parser, Debug, Clone)]
#[command(
    name = "backfill-native-s10",
    about = "Backfill a native USD .s10 series from rebased sources (dry run by default)"
)]
struct Args {
    /// Data root holding `bars/<target_id>/` (read-only unless --apply).
    #[arg(long)]
    data_root: PathBuf,
    /// Native USD-quoted ticker id to write.
    #[arg(long)]
    target_id: u64,
    /// Symbol recorded in a new manifest (e.g. BNB/USD).
    #[arg(long)]
    symbol: String,
    /// Plan CSV: from_ms,to_ms,action,source,mult,mult_max_age_s,tag
    #[arg(long)]
    plan: PathBuf,
    /// First target day, inclusive (YYYY-MM-DD).
    #[arg(long)]
    from: String,
    /// Last target day, inclusive (YYYY-MM-DD). Must be before today (UTC).
    #[arg(long)]
    to: String,
    /// Write a complete served tree here (untouched days copied verbatim).
    #[arg(long, conflicts_with = "apply")]
    stage: Option<PathBuf>,
    /// Write into --data-root (quarantine copies first).
    #[arg(long)]
    apply: bool,
    /// Run id: names the quarantine directory.
    #[arg(long)]
    run_id: String,
    /// Per-day JSONL report.
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
    quarantined: usize,
    backfilled: usize,
    backfilled_tc0: usize,
    /// Calendar buckets [00:00:00, 24:00:00) with no bar in this or an adjacent file.
    holes: usize,
    /// Fill buckets skipped because an adjacent day file holds the bar.
    held_by_neighbour: usize,
    /// Kept bars dropped: the previous day file holds a byte-identical bar.
    dedup_identical: usize,
    /// Bars the previous day file also holds with different bytes (both kept).
    dup_conflict: usize,
    src_missing: usize,
    src_rejected: usize,
    mult_missing: usize,
    tags: BTreeSet<String>,
    sha_before: Option<String>,
    sha_after: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct Summary {
    run_id: String,
    mode: String,
    target_id: u64,
    days: usize,
    by_action: BTreeMap<String, usize>,
    bars_after: usize,
    kept: usize,
    quarantined: usize,
    backfilled: usize,
    backfilled_tc0: usize,
    holes: usize,
    dedup_identical: usize,
    dup_conflict: usize,
}

fn quote_of(id: u64) -> (u64, u64) {
    ((id >> 36) & 0xF, (id >> 20) & 0xFFFF)
}

fn bar_bytes(b: &Bar) -> [u8; STRIDE] {
    bytemuck::bytes_of(b).try_into().expect("Bar is 96 bytes")
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

fn load_plan(path: &Path, target_dir: &Path) -> Result<Vec<Window>> {
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
        self.windows.iter().any(|w| w.drops(ts))
    }

    /// Timestamps an adjacent day file will still hold after this run, plus the
    /// previous day's held bars by timestamp.
    fn neighbour_held(&self, d: NaiveDate) -> Result<Held> {
        let mut held = BTreeSet::new();
        let mut prev = HashMap::new();
        let pd = d.pred_opt().expect("date");
        for nd in [pd, d.succ_opt().expect("date")] {
            let p = shard_path(&self.target_dir, nd, "s10");
            if !p.exists() {
                continue;
            }
            for b in read_shard_aligned::<Bar>(&p)? {
                let ts = b.open_time_ms();
                if !(self.in_run(nd) && self.dropped(ts)) {
                    held.insert(ts);
                    if nd == pd {
                        prev.insert(ts, bar_bytes(&b));
                    }
                }
            }
        }
        Ok((held, prev))
    }

    fn day(&mut self, d: NaiveDate) -> Result<(DayReport, Option<Vec<u8>>, Vec<u8>)> {
        let d0 = day_start_ms(d);
        let d1 = d0 + MS_PER_DAY;
        let path = shard_path(&self.target_dir, d, "s10");
        let before = read_target(&path)?;
        let mut r = DayReport {
            date: date_stem(d),
            ..Default::default()
        };
        let existing = before.as_deref().map(bars_of).unwrap_or_default();
        r.existing_bars = existing.len();

        let (held, prev) = self.neighbour_held(d)?;
        let mut out: BTreeMap<i64, (Bar, bool)> = BTreeMap::new();
        for b in &existing {
            let ts = b.open_time_ms();
            if self.dropped(ts) {
                r.quarantined += 1;
            } else if prev.get(&ts) == Some(&bar_bytes(b)) {
                r.dedup_identical += 1;
            } else {
                if prev.contains_key(&ts) {
                    r.dup_conflict += 1;
                }
                ensure!(
                    out.insert(ts, (*b, false)).is_none(),
                    "{}: duplicate bar at {ts} on disk",
                    path.display()
                );
            }
        }
        r.kept = out.len();

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
                        if sane(&b) && b.open_time_ms() == ts {
                            if b.tick_count == 0 {
                                r.backfilled_tc0 += 1;
                            }
                            out.insert(ts, (b, true));
                            r.backfilled += 1;
                            r.tags.insert(w.tag.clone());
                        } else {
                            r.src_rejected += 1;
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
        r.action = Some(match (before.is_some(), bytes.is_empty()) {
            (false, true) => DayAction::Absent,
            (false, false) => DayAction::New,
            (true, _) if r.quarantined == 0 && r.backfilled == 0 && r.dedup_identical == 0 => {
                DayAction::Unchanged
            }
            (true, true) => DayAction::Emptied,
            (true, false) => DayAction::Merge,
        });
        Ok((r, before, bytes))
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

fn quarantine(
    root: &Path,
    run_id: &str,
    id: u64,
    d: NaiveDate,
    original: &[u8],
) -> Result<PathBuf> {
    let q = shard_path(
        &root
            .join("migrations")
            .join("quarantine")
            .join(run_id)
            .join("bars")
            .join(id.to_string()),
        d,
        "s10",
    );
    ensure!(
        !q.exists(),
        "quarantine copy {} already exists",
        q.display()
    );
    write_shard_atomic(&q, original)?;
    ensure!(
        fs::read(&q)? == original,
        "quarantine copy {} differs",
        q.display()
    );
    Ok(q)
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
    let from = parse_utc_date(&args.from)?;
    let to = parse_utc_date(&args.to)?;
    ensure!(from <= to, "--from after --to");
    ensure!(
        to < today_utc(),
        "--to {to} is today or later: never touch a live shard"
    );

    let target_dir = bars_dir(&args.data_root, args.target_id);
    let windows = load_plan(&args.plan, &target_dir)?;
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

    let mut ctx = Ctx {
        windows,
        target_dir: target_dir.clone(),
        from,
        to,
        src_cache: HashMap::new(),
    };
    let mut report = String::new();
    let mut s = Summary {
        run_id: args.run_id.clone(),
        mode: mode.into(),
        target_id: args.target_id,
        ..Default::default()
    };
    let mut wrote = false;

    let mut d = from;
    while d <= to {
        let (mut r, before, bytes) = ctx.day(d)?;
        let action = r.action.expect("set");
        let dst = shard_path(&out_dir, d, "s10");
        r.sha_before = shard_path(&target_dir, d, "s10")
            .exists()
            .then(|| sha256_file(&shard_path(&target_dir, d, "s10")))
            .transpose()?;
        match (mode, action) {
            ("dry-run", _) | (_, DayAction::Absent) => {}
            ("stage", DayAction::Unchanged) => {
                write_shard_atomic(&dst, before.as_deref().expect("file"))?;
                verify_written(&dst, before.as_deref(), before.as_deref().expect("file"))?;
                wrote = true;
            }
            ("apply", DayAction::Unchanged) => {}
            (_, DayAction::New | DayAction::Merge) => {
                if let Some(orig) = before.as_deref() {
                    quarantine(&out_root, &args.run_id, args.target_id, d, orig)?;
                    if mode == "apply" {
                        // The file must still be the one planned against.
                        ensure!(
                            fs::read(&dst)? == orig,
                            "{} changed during the run",
                            dst.display()
                        );
                    }
                } else if mode == "apply" {
                    ensure!(!dst.exists(), "{} appeared during the run", dst.display());
                }
                write_shard_atomic(&dst, &bytes)?;
                verify_written(&dst, before.as_deref(), &bytes)?;
                wrote = true;
            }
            (_, DayAction::Emptied) => {
                let orig = before.as_deref().expect("file");
                quarantine(&out_root, &args.run_id, args.target_id, d, orig)?;
                if mode == "apply" {
                    ensure!(
                        fs::read(&dst)? == orig,
                        "{} changed during the run",
                        dst.display()
                    );
                    fs::remove_file(&dst)?;
                    wrote = true;
                }
            }
            (m, a) => bail!("unhandled mode {m} action {a:?}"),
        }
        if mode != "dry-run" && dst.exists() {
            r.sha_after = Some(sha256_file(&dst)?);
        }
        s.days += 1;
        *s.by_action
            .entry(format!("{action:?}").to_lowercase())
            .or_default() += 1;
        s.bars_after += bytes.len() / STRIDE;
        s.kept += r.kept;
        s.quarantined += r.quarantined;
        s.backfilled += r.backfilled;
        s.backfilled_tc0 += r.backfilled_tc0;
        s.holes += r.holes;
        s.dedup_identical += r.dedup_identical;
        s.dup_conflict += r.dup_conflict;
        report.push_str(&serde_json::to_string(&r)?);
        report.push('\n');
        d = d.succ_opt().expect("date");
    }

    if wrote {
        let seed = read_manifest(&manifest_path(&target_dir))?
            .unwrap_or_else(|| Manifest::new(args.symbol.clone(), args.target_id, "s10"));
        let mut m = seed;
        m.refresh_kind::<Bar>(&out_dir, "s10")?;
        write_manifest(&manifest_path(&out_dir), &m)?;
    }
    report.push_str(&serde_json::to_string(&s)?);
    report.push('\n');
    if let Some(parent) = args.report.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.report, report)
        .with_context(|| format!("write report {}", args.report.display()))?;
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
        Args {
            data_root: root.join("data"),
            target_id: USD_ID,
            symbol: "BNB/USD".into(),
            plan,
            from: from.into(),
            to: to.into(),
            stage: None,
            apply: true,
            run_id: "t1".into(),
            report: root.join("report.jsonl"),
        }
    }

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
        let q = shard_path(
            &root
                .join("data/migrations/quarantine/t1/bars")
                .join(USD_ID.to_string()),
            parse_utc_date(D).unwrap(),
            "s10",
        );
        assert_eq!(fs::read(q).unwrap(), orig);
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
        let q = shard_path(
            &root
                .join("data/migrations/quarantine/t1/bars")
                .join(USD_ID.to_string()),
            parse_utc_date(D).unwrap(),
            "s10",
        );
        assert_eq!(fs::read(q).unwrap(), orig);
    }

    #[test]
    fn refuses_non_usd_target_live_day_torn_and_hot_files() {
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
        let q = shard_path(
            &root
                .join("data/migrations/quarantine/t1/bars")
                .join(USD_ID.to_string()),
            parse_utc_date(D).unwrap(),
            "s10",
        );
        assert_eq!(fs::read(q).unwrap(), orig);
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
}
