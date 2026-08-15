//! Rebase stored series off a stablecoin quote (USDC / USDT) onto the USD
//! STORAGE QUOTE, so an asset's on-disk directory matches the denomination the
//! API publishes.
//!
//! ## Why this exists
//!
//! The published denomination is `cexs.pivot.storage_quote` (USD). The pivot an
//! asset aggregates in is inferred per weights run and may move hourly, but the
//! ticker_id that names the directory must not: a dynamic denomination renames
//! an asset's folder whenever its pivot moves. Trees written before that rule
//! landed are filed under the pivot's quote (USDC / USDT) instead of USD, so the
//! serving path addresses `<data>/{indexes,bars}/<usd_id>/` and finds nothing,
//! while the history sits under `<data>/{indexes,bars}/<usdc_or_usdt_id>/`.
//!
//! Only the QUOTE bits change. Instrument type, base class, base id and sub-type
//! are copied through untouched, so this is a pure rename, never a re-derivation.
//!
//! ## Cohorts (measured on the prod PVC: 460 stored ticker dirs)
//!
//!   * **11 USDC-quoted with no USD twin** — clean rename.
//!   * **8 USDC-quoted where the USD twin ALREADY EXISTS** — owner decision:
//!     KEEP USD, DISCARD USDC. The source tree is PARKED whole, never merged,
//!     never deleted. Requires `--on-collision=keep-destination`.
//!   * **99 USDT-quoted** — rename. USDT is treated 1:1 against USD; the owner
//!     accepted the ~3 bps dimensional shift that implies.
//!
//! A dry run prints the authoritative current set; trust it over this comment.
//!
//! ## Guarantees
//!
//! * **Dry run by default.** Nothing moves without `--apply`.
//! * **No half-merge is possible.** Collision is decided PER TICKER, not per
//!   date: if any destination tree exists (`indexes/<new>`, `bars/<new>` or
//!   `vol/<new>.vol`), the whole source ticker is either skipped or parked as a
//!   unit. Parking renames the DIRECTORY, one syscall, so there is no window in
//!   which half the dates live under one basis and half under another. A
//!   per-date collision inside a non-colliding ticker is a contradiction and
//!   aborts that tree rather than silently interleaving two bases.
//! * **Never touches a live-open shard.** Today's (and any future-dated) shard
//!   stays put; the tree is journaled `PartialToday` and finished by the next
//!   run after UTC rotation.
//! * **Never runs against a hot tree.** If any source file was written in the
//!   last 5 minutes the ticker is REFUSED: the aggregator has not retargeted
//!   yet, and a writer holding the old path simply recreates it after the move.
//!   Retarget the aggregator first, wait, then run this.
//! * **Never destroys.** Collided trees are parked under
//!   `<data>/migrations/superseded/<ts>/<old_id>/`. Nothing is deleted.
//! * **Verified.** Per-shard record count and first/last ts are captured before
//!   the move and re-scanned after; a mismatch aborts that tree, which is then
//!   not journaled and is retried by the next run.
//! * **Record BODIES ARE NOT REWRITTEN.** A 56 B `IndexRecord` embeds the old
//!   ticker id. This tool rewrites the MANIFEST only (the phantom-id migration
//!   set that precedent). A full read-modify-write of every body would
//!   reintroduce a torn-write window that a rename does not have, on 4+ GiB of
//!   shards, for a field no reader keys off once the directory is right.
//! * **Journaled.** Every move is recorded to
//!   `<data>/migrations/quote-basis.json` after each unit, so the run is
//!   auditable and resumable after a crash or a `kill`.
//!
//! ## Usage
//!
//! ```text
//! migrate-quote-basis                                              # plan everything
//! migrate-quote-basis --quote usdc --apply                         # cohort 1: clean USDC renames
//! migrate-quote-basis --quote usdc --on-collision keep-destination --apply  # cohort 2: park the 8
//! migrate-quote-basis --quote usdt --apply                         # cohort 3: the 99 USDT trees
//! ```
//!
//! After a run, `renko_k_per_ticker` (keyed by ticker-id STRING) has no entry
//! for the new USD ids and renko falls back to the default multiplier. Run
//! `nxr-calibrate` against the new id set before trusting renko output.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::{Parser, ValueEnum};
use nxr_sdk::shard::{
    date_stem, list_shards, manifest_path, read_manifest, rename_vol, stored_ticker_ids,
    vol_path_for_id, write_manifest, Manifest, ShardEntry, DAILY_TREES,
};
use nxr_sdk::IndexRecord;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Storage quote (`cexs.pivot.storage_quote`): USD, asset class 3, id 5001.
const USD_QUOTE_CLASS: u64 = 3;
const USD_QUOTE_ID: u64 = 5001;
/// Stablecoin quotes eligible for rebasing, as (class, id).
const USDC_QUOTE: (u64, u64) = (6, 18501);
const USDT_QUOTE: (u64, u64) = (6, 17601);

/// A source tree written more recently than this is REFUSED: the aggregator has
/// not retargeted onto the USD id yet, and its open writer would recreate the
/// old path right after the move.
const RECENT_WRITE: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum QuoteFilter {
    All,
    Usdc,
    Usdt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CollisionMode {
    /// Leave a colliding source tree untouched and report it. Default.
    Skip,
    /// Keep the destination; park the ENTIRE colliding source tree.
    KeepDestination,
}

#[derive(Parser, Debug)]
#[command(
    name = "migrate-quote-basis",
    about = "Rebase stored series from a USDC/USDT quote onto the USD storage quote",
    long_about = "Renames <data>/{indexes,bars}/<id> and <data>/vol/<id>.vol from a \
stablecoin-quoted ticker id to the USD-quoted one (quote bits -> class 3 / id 5001).\n\n\
DRY RUN BY DEFAULT: pass --apply to move anything.\n\n\
REFUSES a ticker whose source tree was written in the last 5 minutes: the aggregator must \
already have retargeted onto the USD id, otherwise its open writer recreates the old path \
straight after the move.\n\n\
Collision (the USD twin already exists) is decided per TICKER, never per date, so a \
half-merged series that interleaves two quote bases cannot be produced."
)]
struct Args {
    /// Perform the migration. Without this, nothing is moved.
    #[arg(long)]
    apply: bool,
    /// Which stablecoin-quoted cohort to act on.
    #[arg(long, value_enum, default_value = "all")]
    quote: QuoteFilter,
    /// What to do when the USD destination already exists.
    #[arg(long = "on-collision", value_enum, default_value = "skip")]
    on_collision: CollisionMode,
    /// Override the data root (defaults to the NXR_DATA_* config).
    #[arg(long)]
    data_root: Option<PathBuf>,
}

// ─────────────────────────────────────────────────────────────────────────
// Ticker id quote rebase (pure bit math — see mitch `TickerId`)
// [60-63] itype | [56-59] base_class | [40-55] base_id | [36-39] quote_class
// | [20-35] quote_id | [0-19] sub_type
// ─────────────────────────────────────────────────────────────────────────

/// `(quote_class, quote_id)` of a ticker id.
fn quote_of(id: u64) -> (u64, u64) {
    ((id >> 36) & 0xF, (id >> 20) & 0xFFFF)
}

/// Same id with the quote bits replaced by USD. Every other field is copied.
fn rebase_to_usd(id: u64) -> u64 {
    let keep = !((0xF_u64 << 36) | (0xFFFF_u64 << 20));
    (id & keep) | (USD_QUOTE_CLASS << 36) | (USD_QUOTE_ID << 20)
}

// ─────────────────────────────────────────────────────────────────────────
// Journal (verbatim from migrate_phantom_ids, distinct path)
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum UnitState {
    /// Every shard moved; nothing left under the old id for this unit.
    Done,
    /// Everything except today's live-open shard moved. Re-run after rotation.
    PartialToday,
    /// Destination won the collision; the whole source tree was parked.
    Parked,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Journal {
    /// `"<old_id>|<unit>"` → state. Flat string key so the file stays diffable
    /// and hand-editable during an incident. Record bodies still carry the old
    /// ticker id by design (manifest-only rewrite); this journal is the audit
    /// trail for that decision.
    done: BTreeMap<String, UnitState>,
}

fn journal_path(data_root: &Path) -> PathBuf {
    data_root.join("migrations").join("quote-basis.json")
}

fn load_journal(data_root: &Path) -> Result<Journal> {
    let p = journal_path(data_root);
    if !p.exists() {
        return Ok(Journal::default());
    }
    let raw = std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse {}", p.display()))
}

/// Atomic journal write (`.tmp` → rename) so a crash mid-write cannot leave an
/// unparseable journal and wedge every later run.
fn save_journal(data_root: &Path, j: &Journal) -> Result<()> {
    let p = journal_path(data_root);
    std::fs::create_dir_all(p.parent().unwrap())?;
    let tmp = p.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(j)?)?;
    std::fs::rename(&tmp, &p)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Candidate selection (from DISK, not from config: history exists for ids the
// current config may no longer name)
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Candidate {
    symbol: String,
    old_id: u64,
    new_id: u64,
    /// Which stablecoin the source is quoted in.
    quote: QuoteFilter,
    /// The USD twin already has bytes on disk.
    collides: bool,
    bytes: u64,
}

fn tree_dir(data_root: &Path, subdir: &str, id: u64) -> PathBuf {
    data_root.join(subdir).join(id.to_string())
}

/// (file count, total bytes) directly inside `dir` (shard dirs are flat).
fn dir_files(dir: &Path) -> (u64, u64) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return (0, 0);
    };
    rd.filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .fold((0, 0), |(n, b), m| (n + 1, b + m.len()))
}

/// Every path this ticker owns on disk, source side.
fn source_paths(data_root: &Path, id: u64) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = ["indexes", "bars"]
        .iter()
        .map(|s| tree_dir(data_root, s, id))
        .filter(|p| p.is_dir())
        .collect();
    let vf = vol_path_for_id(data_root, id);
    if vf.is_file() {
        v.push(vf);
    }
    v
}

/// The USD twin has bytes on disk in ANY tree. Decided per ticker so a merge
/// can never happen for part of a series.
fn collides(data_root: &Path, new_id: u64) -> bool {
    source_paths(data_root, new_id)
        .iter()
        .any(|p| if p.is_dir() { dir_files(p).0 > 0 } else { true })
}

/// True if anything under `paths` was modified within `window`. Directory
/// mtimes count: a shard created or removed by a live writer bumps them.
fn written_within(paths: &[PathBuf], window: Duration) -> bool {
    let now = SystemTime::now();
    let mut newest: Option<SystemTime> = None;
    for p in paths {
        let mut stack = vec![p.clone()];
        while let Some(cur) = stack.pop() {
            let Ok(md) = std::fs::metadata(&cur) else {
                continue;
            };
            if let Ok(m) = md.modified() {
                if newest.is_none_or(|n| m > n) {
                    newest = Some(m);
                }
            }
            if md.is_dir() {
                if let Ok(rd) = std::fs::read_dir(&cur) {
                    stack.extend(rd.filter_map(|e| e.ok()).map(|e| e.path()));
                }
            }
        }
    }
    newest.is_some_and(|m| now.duration_since(m).map_or(true, |age| age < window))
}

/// Human symbol for the destination manifest: the source manifest's ticker with
/// its quote leg rebased to USD. Falls back to the bare id when no manifest
/// exists (a bars-only tree written before manifests, for instance).
fn symbol_for(data_root: &Path, old_id: u64, new_id: u64) -> String {
    for (subdir, _) in DAILY_TREES {
        let mp = manifest_path(&tree_dir(data_root, subdir, old_id));
        if let Ok(Some(m)) = read_manifest(&mp) {
            if let Some((base, _)) = m.ticker.split_once('/') {
                return format!("{base}/USD");
            }
        }
    }
    new_id.to_string()
}

fn candidates(data_root: &Path, filter: QuoteFilter) -> Result<Vec<Candidate>> {
    let mut out = Vec::new();
    for old_id in stored_ticker_ids(data_root)? {
        let q = quote_of(old_id);
        let quote = if q == USDC_QUOTE {
            QuoteFilter::Usdc
        } else if q == USDT_QUOTE {
            QuoteFilter::Usdt
        } else {
            continue; // already USD, or a quote this tool has no mandate over
        };
        if filter != QuoteFilter::All && filter != quote {
            continue;
        }
        let new_id = rebase_to_usd(old_id);
        if new_id == old_id {
            continue;
        }
        let (files, bytes) = source_paths(data_root, old_id)
            .iter()
            .fold((0, 0), |(n, b), p| {
                let (dn, db) = if p.is_dir() {
                    dir_files(p)
                } else {
                    (1, p.metadata().map(|m| m.len()).unwrap_or(0))
                };
                (n + dn, b + db)
            });
        // An emptied leftover directory is not a candidate: it has nothing to
        // move, and a finished run must not re-trip the hot-tree refusal on the
        // dirs it just drained.
        if files == 0 {
            continue;
        }
        out.push(Candidate {
            symbol: symbol_for(data_root, old_id, new_id),
            old_id,
            new_id,
            quote,
            collides: collides(data_root, new_id),
            bytes,
        });
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────
// Move / park
// ─────────────────────────────────────────────────────────────────────────

/// Scan `(date, path)` shards into a verifiable fingerprint: date → (n, first, last).
/// Generic over the record type so idx (56 B) and bar (96 B) share one path.
fn fingerprint<T: nxr_sdk::shard::ShardRecord>(
    shards: &[(NaiveDate, PathBuf)],
) -> Result<BTreeMap<String, (u64, i64, i64)>> {
    let mut out = BTreeMap::new();
    for (date, path) in shards {
        let e: ShardEntry = nxr_sdk::shard::shard_entry::<T>(*date, path)?;
        out.insert(e.date.clone(), (e.n_records, e.first_ts, e.last_ts));
    }
    Ok(out)
}

/// Move one tree's shards for one ticker. Returns the resulting state.
fn migrate_tree<T: nxr_sdk::shard::ShardRecord>(
    data_root: &Path,
    c: &Candidate,
    subdir: &str,
    ext: &str,
    apply: bool,
) -> Result<Option<UnitState>> {
    let src = tree_dir(data_root, subdir, c.old_id);
    if !src.is_dir() {
        return Ok(None); // nothing under the old id for this tree
    }
    let all = list_shards(&src, ext)?;
    if all.is_empty() {
        return Ok(None);
    }

    // LIVE-SHARD RULE: never touch today's (or any future-dated) shard. The
    // aggregator holds it open and appends to it; renaming it out from under the
    // writer loses every subsequent record silently.
    let today = chrono::Utc::now().date_naive();
    let (movable, held): (Vec<_>, Vec<_>) = all.into_iter().partition(|(d, _)| *d < today);
    if movable.is_empty() {
        info!(symbol = %c.symbol, tree = %format!("{subdir}.{ext}"),
              "only today's live shard present — deferring whole tree");
        return Ok(Some(UnitState::PartialToday));
    }

    let before = fingerprint::<T>(&movable)?;
    let dst = tree_dir(data_root, subdir, c.new_id);

    info!(
        symbol = %c.symbol, old_id = c.old_id, new_id = c.new_id,
        tree = %format!("{subdir}.{ext}"), shards = movable.len(),
        held_today = held.len(), records = before.values().map(|v| v.0).sum::<u64>(),
        bytes = movable.iter().filter_map(|(_, p)| p.metadata().ok()).map(|m| m.len()).sum::<u64>(),
        src = %src.display(), dst = %dst.display(),
        "{}", if apply { "migrating" } else { "PLAN (dry run)" }
    );
    if !apply {
        return Ok(None);
    }

    std::fs::create_dir_all(&dst).with_context(|| format!("mkdir {}", dst.display()))?;
    let mut moved: Vec<(NaiveDate, PathBuf)> = Vec::with_capacity(movable.len());
    for (date, from) in &movable {
        let to = dst.join(format!("{}.{ext}", date_stem(*date)));
        if to.exists() {
            // Contradiction: this ticker was classified non-colliding, so the
            // destination must not hold this date. Merging here would interleave
            // two quote bases inside one series — the exact corruption this tool
            // exists to prevent. Abort the tree; the journal stays unset and the
            // operator re-classifies with --on-collision.
            bail!(
                "{} {subdir}.{ext} {}: destination shard already exists at {} — \
                 refusing a per-date merge of two quote bases; re-run this ticker with \
                 --on-collision=keep-destination",
                c.symbol,
                date_stem(*date),
                to.display()
            );
        }
        // Same-filesystem rename: atomic, no copy, and the bytes are intact at
        // one path or the other if we are killed mid-loop (resumable).
        std::fs::rename(from, &to)
            .with_context(|| format!("rename {} -> {}", from.display(), to.display()))?;
        moved.push((*date, to));
    }

    // VERIFY: re-scan what landed and compare against the pre-move fingerprint.
    let after = fingerprint::<T>(&moved)?;
    for (date, want) in &before {
        let Some(got) = after.get(date) else { continue };
        if got != want {
            bail!(
                "{} {}.{ext} {date}: verification FAILED — before n/first/last {:?} != after {:?}",
                c.symbol,
                subdir,
                want,
                got
            );
        }
    }

    // Rewrite the destination manifest: identity fields plus a full rescan of
    // whatever now sits in the directory. Record BODIES keep the old ticker id
    // (56 B `IndexRecord`); rewriting 4+ GiB in place would reintroduce a
    // torn-write window a rename does not have.
    let mpath = manifest_path(&dst);
    let mut m =
        read_manifest(&mpath)?.unwrap_or_else(|| Manifest::new(c.symbol.clone(), c.new_id, ext));
    m.ticker = c.symbol.clone();
    m.ticker_id = c.new_id;
    m.refresh_kind::<T>(&dst, ext)?;
    write_manifest(&mpath, &m)?;

    // Source manifest is now a lie (it claims shards that moved). Rescan it; if
    // the tree is empty, retire the file so nothing reads a stale-basis manifest.
    let src_shards = list_shards(&src, ext)?;
    let smpath = manifest_path(&src);
    if src_shards.is_empty() && smpath.exists() {
        let retired = smpath.with_extension("json.migrated");
        std::fs::rename(&smpath, &retired)?;
    } else if let Some(mut sm) = read_manifest(&smpath)? {
        sm.refresh_kind::<T>(&src, ext)?;
        write_manifest(&smpath, &sm)?;
    }

    let state = if held.is_empty() {
        UnitState::Done
    } else {
        UnitState::PartialToday
    };
    info!(symbol = %c.symbol, tree = %format!("{subdir}.{ext}"),
          moved = moved.len(), ?state, "tree migrated + verified");
    Ok(Some(state))
}

/// Move `<data>/vol/<old>.vol` → `<data>/vol/<new>.vol`. NOT covered by the
/// phantom-id migration: without this the sigma prime keeps reading the old id
/// and the live renko producer starts from a cold Parkinson ring.
fn migrate_vol(data_root: &Path, c: &Candidate, apply: bool) -> Result<Option<UnitState>> {
    let src = vol_path_for_id(data_root, c.old_id);
    if !src.is_file() {
        return Ok(None);
    }
    let bytes = src.metadata().map(|m| m.len()).unwrap_or(0);
    info!(symbol = %c.symbol, src = %src.display(),
          dst = %vol_path_for_id(data_root, c.new_id).display(), bytes,
          "{}", if apply { "migrating vol" } else { "PLAN vol (dry run)" });
    if !apply {
        return Ok(None);
    }
    rename_vol(data_root, c.old_id, c.new_id).with_context(|| c.symbol.clone())?;
    Ok(Some(UnitState::Done))
}

/// Park the ENTIRE source ticker under `migrations/superseded/<ts>/<old_id>/`.
/// Directory-granular renames: one syscall per tree, so a half-parked ticker
/// (some dates under the old basis, some under the new) cannot exist.
fn park_tree(data_root: &Path, c: &Candidate, ts: &str, apply: bool) -> Result<Option<UnitState>> {
    let paths = source_paths(data_root, c.old_id);
    if paths.is_empty() {
        return Ok(None);
    }
    let park = data_root
        .join("migrations")
        .join("superseded")
        .join(ts)
        .join(c.old_id.to_string());
    warn!(
        symbol = %c.symbol, old_id = c.old_id, new_id = c.new_id, bytes = c.bytes,
        trees = paths.len(), park = %park.display(),
        "{} destination USD tree exists — KEEPING DESTINATION, parking whole source",
        if apply { "PARKING:" } else { "PLAN (dry run):" }
    );
    if !apply {
        return Ok(None);
    }
    std::fs::create_dir_all(&park)?;
    for p in &paths {
        let name = p.file_name().unwrap();
        let to = if p.is_dir() {
            // `indexes/<id>` and `bars/<id>` share a basename; keep the subdir.
            let sub = park.join(p.parent().and_then(|x| x.file_name()).unwrap());
            std::fs::create_dir_all(&sub)?;
            sub.join(name)
        } else {
            let sub = park.join("vol");
            std::fs::create_dir_all(&sub)?;
            sub.join(name)
        };
        std::fs::rename(p, &to)
            .with_context(|| format!("park {} -> {}", p.display(), to.display()))?;
    }
    Ok(Some(UnitState::Parked))
}

// ─────────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let args = Args::parse();

    let data_root = match &args.data_root {
        Some(p) => p.clone(),
        None => nxr_sdk::config::NxrConfig::from_env_with_hint(
            nxr_sdk::pipeline_config::ConfigHint::Bin,
        )
        .data_root(),
    };
    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();

    let cands = candidates(&data_root, args.quote)?;
    if cands.is_empty() {
        info!(?args.quote, "no stablecoin-quoted shard trees to rebase");
        return Ok(());
    }

    let (colliding, clean): (Vec<_>, Vec<_>) = cands.iter().partition(|c| c.collides);
    info!(
        data_root = %data_root.display(), apply = args.apply, ?args.quote, ?args.on_collision,
        total = cands.len(), clean = clean.len(), colliding = colliding.len(),
        bytes = cands.iter().map(|c| c.bytes).sum::<u64>(),
        "quote-basis rebase -> USD (class {USD_QUOTE_CLASS} / id {USD_QUOTE_ID})"
    );
    for c in &cands {
        info!(
            symbol = %c.symbol, ?c.quote, old_id = c.old_id, new_id = c.new_id,
            bytes = c.bytes,
            action = if c.collides {
                match args.on_collision {
                    CollisionMode::Skip => "SKIP (USD twin exists)",
                    CollisionMode::KeepDestination => "PARK source, keep USD twin",
                }
            } else {
                "rename -> USD"
            },
            "plan"
        );
    }

    let mut journal = load_journal(&data_root)?;
    let mut failures = 0usize;
    let mut migrated: Vec<u64> = Vec::new();

    for c in &cands {
        if journal.done.get(&format!("{}|PARKED", c.old_id)) == Some(&UnitState::Parked) {
            continue;
        }
        // HOT-TREE REFUSAL: the aggregator must have retargeted onto the USD id
        // before this runs, or its open writer recreates the old path.
        if written_within(&source_paths(&data_root, c.old_id), RECENT_WRITE) {
            failures += 1;
            warn!(symbol = %c.symbol, old_id = c.old_id, secs = RECENT_WRITE.as_secs(),
                  "REFUSED: source tree written within the refusal window — retarget the \
                   aggregator onto the USD id, wait, then re-run");
            continue;
        }

        if c.collides {
            match args.on_collision {
                CollisionMode::Skip => {
                    warn!(symbol = %c.symbol, old_id = c.old_id, new_id = c.new_id,
                          "skipped: USD twin exists — re-run with \
                           --on-collision=keep-destination to park this source tree");
                    continue;
                }
                CollisionMode::KeepDestination => match park_tree(&data_root, c, &ts, args.apply) {
                    Ok(Some(state)) if args.apply => {
                        journal.done.insert(format!("{}|PARKED", c.old_id), state);
                        save_journal(&data_root, &journal)?;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        failures += 1;
                        warn!(symbol = %c.symbol, err = %e, "park FAILED — not journaled");
                    }
                },
            }
            continue;
        }

        let mut ok = false;
        for (subdir, ext) in DAILY_TREES {
            let key = format!("{}|{subdir}.{ext}", c.old_id);
            if journal.done.get(&key) == Some(&UnitState::Done) {
                continue; // already finished by an earlier run
            }
            // `.idx` is IndexRecord (56 B); `.s10`/`.renko` are Bar (96 B).
            let res = if ext == "idx" {
                migrate_tree::<IndexRecord>(&data_root, c, subdir, ext, args.apply)
            } else {
                migrate_tree::<nxr_sdk::mitch::bar::Bar>(&data_root, c, subdir, ext, args.apply)
            };
            match res {
                Ok(Some(state)) => {
                    ok = true;
                    if args.apply {
                        // Journal after EVERY unit, not at the end: a crash must
                        // not lose completed work.
                        journal.done.insert(key, state);
                        save_journal(&data_root, &journal)?;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    failures += 1;
                    warn!(symbol = %c.symbol, tree = %format!("{subdir}.{ext}"), err = %e,
                          "tree migration FAILED — not journaled, re-run to retry");
                }
            }
        }
        let vkey = format!("{}|vol", c.old_id);
        if journal.done.get(&vkey) != Some(&UnitState::Done) {
            match migrate_vol(&data_root, c, args.apply) {
                Ok(Some(state)) => {
                    ok = true;
                    if args.apply {
                        journal.done.insert(vkey, state);
                        save_journal(&data_root, &journal)?;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    failures += 1;
                    warn!(symbol = %c.symbol, err = %e, "vol migration FAILED — re-run to retry");
                }
            }
        }
        if ok {
            migrated.push(c.new_id);
        }
    }

    let deferred = journal
        .done
        .values()
        .filter(|s| **s == UnitState::PartialToday)
        .count();
    if deferred > 0 {
        warn!(
            deferred,
            "tree(s) still hold today's live shard — RE-RUN after UTC rotation to finish"
        );
    }
    if !migrated.is_empty() {
        let ids: Vec<String> = migrated.iter().map(|i| i.to_string()).collect();
        warn!(
            count = migrated.len(), new_ids = %ids.join(","),
            "RECALIBRATE: `renko_k_per_ticker` is keyed by ticker-id STRING, so these USD ids \
             have no k and renko silently falls back to the config default multiplier. \
             Run `nxr-calibrate` against the new id set before trusting renko output."
        );
    }
    if failures > 0 {
        bail!("{failures} unit migration(s) failed or were refused — see warnings above");
    }
    if !args.apply {
        info!("dry run complete — re-run with --apply to perform the migration");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn tmp_root(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "mqb-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Build a ticker id from its fields (mirrors mitch `TickerId::new`).
    fn mk(itype: u64, bclass: u64, bid: u64, qclass: u64, qid: u64, sub: u64) -> u64 {
        (itype << 60) | (bclass << 56) | (bid << 40) | (qclass << 36) | (qid << 20) | sub
    }

    #[test]
    fn usdc_and_usdt_rebase_to_usd_preserving_base_bits() {
        for (qclass, qid) in [USDC_QUOTE, USDT_QUOTE] {
            let old = mk(1, 6, 20701, qclass, qid, 0x3_F00F);
            assert_eq!(quote_of(old), (qclass, qid));
            let new = rebase_to_usd(old);
            assert_eq!(quote_of(new), (USD_QUOTE_CLASS, USD_QUOTE_ID));
            // every non-quote field survives
            assert_eq!(new >> 60, old >> 60, "instrument_type");
            assert_eq!((new >> 56) & 0xF, 6, "base_class");
            assert_eq!((new >> 40) & 0xFFFF, 20701, "base_id");
            assert_eq!(new & 0xFFFFF, 0x3_F00F, "sub_type");
            assert_ne!(new, old);
            // idempotent: a USD id is already at its destination
            assert_eq!(rebase_to_usd(new), new);
        }
    }

    #[test]
    fn candidate_scan_classifies_quote_and_collision() {
        let root = tmp_root("cand");
        let usdc = mk(1, 6, 999, USDC_QUOTE.0, USDC_QUOTE.1, 0);
        let usdt = mk(1, 6, 998, USDT_QUOTE.0, USDT_QUOTE.1, 0);
        let usd = mk(1, 6, 997, USD_QUOTE_CLASS, USD_QUOTE_ID, 0);
        for id in [usdc, usdt, usd] {
            std::fs::create_dir_all(root.join("indexes").join(id.to_string())).unwrap();
        }
        for id in [usdc, usdt, usd] {
            std::fs::write(
                root.join("indexes")
                    .join(id.to_string())
                    .join("2020-01-02.idx"),
                [0u8; 56],
            )
            .unwrap();
        }
        // give the USDT ticker an existing USD twin => collision. An EMPTY
        // destination dir is not a collision, only bytes are.
        let twin = root.join("bars").join(rebase_to_usd(usdt).to_string());
        std::fs::create_dir_all(&twin).unwrap();
        std::fs::create_dir_all(root.join("bars").join(rebase_to_usd(usdc).to_string())).unwrap();
        std::fs::write(twin.join("2019-05-05.s10"), [0u8; 96]).unwrap();

        let all = candidates(&root, QuoteFilter::All).unwrap();
        assert_eq!(all.len(), 2, "the already-USD tree is not a candidate");
        let c_usdc = all.iter().find(|c| c.old_id == usdc).unwrap();
        assert_eq!(c_usdc.quote, QuoteFilter::Usdc);
        assert!(!c_usdc.collides);
        let c_usdt = all.iter().find(|c| c.old_id == usdt).unwrap();
        assert_eq!(c_usdt.quote, QuoteFilter::Usdt);
        assert!(c_usdt.collides, "USD twin exists in bars/");
        assert!(
            !collides(&root, rebase_to_usd(usdc)),
            "an empty destination dir is not a collision"
        );

        // the --quote filter narrows the cohort
        let only_usdc = candidates(&root, QuoteFilter::Usdc).unwrap();
        assert_eq!(only_usdc.len(), 1);
        assert_eq!(only_usdc[0].old_id, usdc);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn recent_write_is_refused() {
        let root = tmp_root("hot");
        let dir = root.join("indexes").join("42");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("2026-01-01.idx"), [0u8; 56]).unwrap();
        let paths = vec![dir.clone()];
        assert!(
            written_within(&paths, RECENT_WRITE),
            "a just-written tree must be refused"
        );
        assert!(
            !written_within(&paths, Duration::from_secs(0)),
            "a zero window can never match"
        );
        assert!(
            !written_within(&[root.join("indexes").join("nope")], RECENT_WRITE),
            "an absent tree is not hot"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn dry_run_moves_nothing() {
        let root = tmp_root("dry");
        let old = mk(1, 6, 555, USDC_QUOTE.0, USDC_QUOTE.1, 0);
        let new = rebase_to_usd(old);
        let src = root.join("indexes").join(old.to_string());
        std::fs::create_dir_all(&src).unwrap();
        let shard = src.join("2020-01-02.idx");
        std::fs::write(&shard, vec![0u8; 56 * 3]).unwrap();
        std::fs::create_dir_all(root.join("vol")).unwrap();
        let vol = root.join("vol").join(format!("{old}.vol"));
        std::fs::write(&vol, b"sigma").unwrap();

        let c = Candidate {
            symbol: "T/USD".into(),
            old_id: old,
            new_id: new,
            quote: QuoteFilter::Usdc,
            collides: false,
            bytes: 0,
        };
        assert!(
            migrate_tree::<IndexRecord>(&root, &c, "indexes", "idx", false)
                .unwrap()
                .is_none()
        );
        assert!(migrate_vol(&root, &c, false).unwrap().is_none());
        assert!(park_tree(&root, &c, "TS", false).unwrap().is_none());

        assert!(shard.is_file(), "source shard must be untouched");
        assert!(vol.is_file(), "source vol must be untouched");
        assert!(
            !root.join("indexes").join(new.to_string()).exists(),
            "dry run must not create the destination"
        );
        assert!(
            !root.join("migrations").exists(),
            "dry run must not journal or park"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn park_moves_whole_trees_never_half() {
        let root = tmp_root("park");
        let old = mk(1, 6, 777, USDC_QUOTE.0, USDC_QUOTE.1, 0);
        let new = rebase_to_usd(old);
        for (sub, ext) in [("indexes", "idx"), ("bars", "s10")] {
            let d = root.join(sub).join(old.to_string());
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join(format!("2020-01-02.{ext}")), vec![0u8; 96]).unwrap();
        }
        std::fs::create_dir_all(root.join("vol")).unwrap();
        std::fs::write(root.join("vol").join(format!("{old}.vol")), b"s").unwrap();
        // the USD twin exists => collision
        let twin = root.join("indexes").join(new.to_string());
        std::fs::create_dir_all(&twin).unwrap();
        std::fs::write(twin.join("2019-05-05.idx"), [0u8; 56]).unwrap();
        assert!(collides(&root, new));

        let c = Candidate {
            symbol: "T/USD".into(),
            old_id: old,
            new_id: new,
            quote: QuoteFilter::Usdc,
            collides: true,
            bytes: 0,
        };
        assert_eq!(
            park_tree(&root, &c, "TS", true).unwrap(),
            Some(UnitState::Parked)
        );
        let park = root
            .join("migrations")
            .join("superseded")
            .join("TS")
            .join(old.to_string());
        assert!(park
            .join("indexes")
            .join(old.to_string())
            .join("2020-01-02.idx")
            .is_file());
        assert!(park
            .join("bars")
            .join(old.to_string())
            .join("2020-01-02.s10")
            .is_file());
        assert!(park.join("vol").join(format!("{old}.vol")).is_file());
        assert!(
            !root.join("bars").join(old.to_string()).exists(),
            "source gone, not merged"
        );
        let dst_files: Vec<_> = std::fs::read_dir(&twin)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            dst_files,
            vec!["2019-05-05.idx".to_string()],
            "destination untouched: nothing merged into it"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
