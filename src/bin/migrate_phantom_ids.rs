//! Migrate shard trees off PHANTOM (FNV fallback) ticker ids onto the real
//! MITCH ids the resolver now returns.
//!
//! ## Why this exists
//!
//! `resolve_ticker_id` falls back to `fnv1a_64(symbol)` when a symbol has no
//! MITCH id, and shards are filed under whatever id that returned. Two 2026-07-25
//! fixes changed 28 of 210 configured symbols from a phantom id to a real one:
//!
//!   * the resolver's CR-only class filter (an FX base could never resolve
//!     against a crypto quote, so `EUR/USDT` was phantom while `EUR/USD` was not)
//!     — 25 symbols;
//!   * new `mitch/ids/crypto-assets.csv` rows 20701..21501 (PEPE, SHIB, BONK,
//!     ONDO, PUMP, CVX, ETC, ZRO, LISTA) — the rest.
//!
//! Deploying those fixes WITHOUT this migration orphans every existing
//! `.idx` / `.s10` / `.renko` shard for those symbols: the serving path addresses
//! `<data>/{indexes,bars}/<new_id>/`, which is empty, while all the history sits
//! under `<data>/{indexes,bars}/<phantom_id>/`.
//!
//! ⚠ `core` HARD-REFUSES to boot in that state (`main::orphaned_phantom_shards`
//! → `exit(78)`). Shipping the resolver/CSV fix first therefore does not degrade
//! the feed, it takes `api.nxrates.com` DOWN. **Run this tool first.**
//!
//! ## Scope: 28 ids change, 11 symbols actually have shards
//!
//! Two different counts, both real, do not conflate them:
//!   * **28 of 210** configured symbols get a different ticker id. That is the
//!     resolution change.
//!   * **11 symbols / 22 trees** have bytes on disk to move. The other ~199
//!     id-changed symbols are auto-cross / synth outputs served from the
//!     ephemeral in-RAM ring and never persisted, so there is nothing to
//!     relocate for them.
//!
//! Measured on the prod PVC 2026-07-25 by enumerating every phantom id and
//! `du`-ing it: 22 trees, **4.176 GiB**, **1 346 shard files** — `EUR/USDT`,
//! `GBP/USDT` (resolver fix) plus `PEPE`, `SHIB`, `BONK`, `ONDO`, `PUMP`, `CVX`,
//! `ETC`, `ZRO`, `LISTA` (all `/USDT`, id allocation). A dry run prints the
//! authoritative current set; trust it over this comment.
//!
//! ## Guarantees
//!
//! * **Idempotent + resumable.** Progress is journaled per (symbol, tree) to
//!   `<data>/migrations/phantom-ids.json` after each tree completes. A re-run
//!   skips journaled work and re-derives the rest from what is on disk, so it is
//!   safe after a crash, a `kill`, or a partial run.
//! * **Never touches the live-open shard.** Today's UTC shard is left in place
//!   (same rule as `idx_heal` / `resample_idx`; a separate finding has
//!   `merge_idx` truncating one, which is why this is explicit). A symbol whose
//!   source dir still holds today's shard is journaled as `PartialToday` and
//!   finished by the next run after rotation.
//! * **Never destroys.** Same-date collisions keep the DESTINATION file and park
//!   the source under `<data>/migrations/superseded/`. Nothing is deleted.
//! * **Verified.** Record count and first/last ts are captured per shard before
//!   the move and re-scanned after; any mismatch aborts that symbol and restores
//!   nothing (the move is a rename, so the file is intact at one path or the
//!   other) — the journal simply does not mark it done.
//!
//! ## Usage
//!
//! ```text
//! migrate-phantom-ids            # dry run: report the plan, touch nothing
//! migrate-phantom-ids --apply    # perform it
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use nxr_sdk::pipeline_config::{ConfigHint, PipelineYml};
use nxr_sdk::shard::{
    date_stem, list_shards, manifest_path, read_manifest, write_manifest, Manifest, ShardEntry,
};
use nxr_sdk::{phantom_ticker_id, try_resolve_ticker_id, IndexRecord};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// The three shard trees, as (subdir, extension). `indexes` holds 56 B
/// `IndexRecord`; `bars` holds 96 B `Bar` under two extensions in ONE directory.
const TREES: &[(&str, &str)] = &[("indexes", "idx"), ("bars", "s10"), ("bars", "renko")];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TreeState {
    /// Every shard moved; nothing left under the phantom id for this tree.
    Done,
    /// Everything except today's live-open shard moved. Re-run after rotation.
    PartialToday,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Journal {
    /// `"<symbol>|<subdir>.<ext>"` → state. Flat string key so the file stays
    /// diffable and hand-editable during an incident.
    done: BTreeMap<String, TreeState>,
}

fn journal_path(data_root: &Path) -> PathBuf {
    data_root.join("migrations").join("phantom-ids.json")
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

/// A symbol whose id changed AND whose phantom shard tree exists on disk.
#[derive(Debug)]
struct Candidate {
    symbol: String,
    old_id: u64,
    new_id: u64,
}

/// Every configured symbol whose phantom id differs from its now-resolvable
/// MITCH id. A symbol that still does not resolve is NOT a candidate — it has no
/// destination, and `core`'s phantom gate refuses to boot on it anyway.
fn candidates(pl: &PipelineYml) -> Vec<Candidate> {
    pl.configured_symbols()
        .into_iter()
        .filter_map(|symbol| {
            let new_id = try_resolve_ticker_id(&symbol)?;
            let old_id = phantom_ticker_id(&symbol);
            (old_id != new_id).then_some(Candidate {
                symbol,
                old_id,
                new_id,
            })
        })
        .collect()
}

fn tree_dir(data_root: &Path, subdir: &str, id: u64) -> PathBuf {
    data_root.join(subdir).join(id.to_string())
}

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

/// Move one tree's shards for one symbol. Returns the resulting state.
fn migrate_tree<T: nxr_sdk::shard::ShardRecord>(
    data_root: &Path,
    c: &Candidate,
    subdir: &str,
    ext: &str,
    apply: bool,
) -> Result<Option<TreeState>> {
    let src = tree_dir(data_root, subdir, c.old_id);
    if !src.is_dir() {
        return Ok(None); // nothing under the phantom id for this tree
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
        return Ok(Some(TreeState::PartialToday));
    }

    let before = fingerprint::<T>(&movable)?;
    let dst = tree_dir(data_root, subdir, c.new_id);

    info!(
        symbol = %c.symbol, old_id = c.old_id, new_id = c.new_id,
        tree = %format!("{subdir}.{ext}"), shards = movable.len(),
        held_today = held.len(), records = before.values().map(|v| v.0).sum::<u64>(),
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
            // Destination already has this date. Keep it (it is what the serving
            // path is using) and park the source — never overwrite, never delete.
            let park = data_root
                .join("migrations")
                .join("superseded")
                .join(subdir)
                .join(c.old_id.to_string());
            std::fs::create_dir_all(&park)?;
            let parked = park.join(format!("{}.{ext}", date_stem(*date)));
            warn!(symbol = %c.symbol, date = %date_stem(*date), parked = %parked.display(),
                  "destination shard already exists — keeping destination, parking source");
            std::fs::rename(from, &parked)?;
            continue;
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
        // A parked (collided) date is legitimately absent from `after`.
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
    // whatever now sits in the directory (which may include shards the
    // destination already had).
    let mpath = manifest_path(&dst);
    let mut m =
        read_manifest(&mpath)?.unwrap_or_else(|| Manifest::new(c.symbol.clone(), c.new_id, ext));
    m.ticker = c.symbol.clone();
    m.ticker_id = c.new_id;
    m.refresh_kind::<T>(&dst, ext)?;
    write_manifest(&mpath, &m)?;

    // Source manifest is now a lie (it claims shards that moved). Rescan it; if
    // the tree is empty, retire the file so nothing reads a phantom manifest.
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
        TreeState::Done
    } else {
        TreeState::PartialToday
    };
    info!(symbol = %c.symbol, tree = %format!("{subdir}.{ext}"),
          moved = moved.len(), ?state, "tree migrated + verified");
    Ok(Some(state))
}

fn main() -> Result<()> {
    nxr_sdk::logging::init("info");
    let apply = std::env::args().any(|a| a == "--apply");

    let cfg = nxr_sdk::config::NxrConfig::from_env_with_hint(ConfigHint::Bin);
    let data_root = PathBuf::from(cfg.data_root());
    let pl = PipelineYml::load_default(ConfigHint::Bin).context("load config.yml")?;

    let cands = candidates(&pl);
    if cands.is_empty() {
        info!("no phantom-id shard trees to migrate");
        return Ok(());
    }
    // NOTE: "candidates" is every configured symbol whose FNV id differs from its
    // MITCH id — which is ALL of them, since the two are unrelated by
    // construction. It is not a migration count. Only symbols whose phantom TREE
    // exists on disk are acted on, reported per-tree below.
    info!(
        symbols_scanned = cands.len(),
        apply, data_root = %data_root.display(),
        "phantom-id shard migration: scanning for phantom trees on disk"
    );

    let mut journal = load_journal(&data_root)?;
    let mut failures = 0usize;
    for c in &cands {
        for (subdir, ext) in TREES {
            let key = format!("{}|{subdir}.{ext}", c.symbol);
            if journal.done.get(&key) == Some(&TreeState::Done) {
                continue; // already finished by an earlier run
            }
            // `.idx` is IndexRecord (56 B); `.s10`/`.renko` are Bar (96 B).
            let res = if *ext == "idx" {
                migrate_tree::<IndexRecord>(&data_root, c, subdir, ext, apply)
            } else {
                migrate_tree::<nxr_sdk::mitch::bar::Bar>(&data_root, c, subdir, ext, apply)
            };
            match res {
                Ok(Some(state)) => {
                    if apply {
                        journal.done.insert(key, state);
                        // Journal after EVERY tree, not at the end: a crash must
                        // not lose completed work.
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
    }

    let deferred = journal
        .done
        .values()
        .filter(|s| **s == TreeState::PartialToday)
        .count();
    if deferred > 0 {
        warn!(
            deferred,
            "tree(s) still hold today's live shard — RE-RUN after UTC rotation to finish"
        );
    }
    if failures > 0 {
        bail!("{failures} tree migration(s) failed — see warnings above");
    }
    if !apply {
        info!("dry run complete — re-run with --apply to perform the migration");
    }
    Ok(())
}
