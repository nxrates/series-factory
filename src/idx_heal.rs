//! In-place repair for sharded `.idx` without re-fetching raw ticks.
//!
//! Per-shard pass (bounded memory): read one daily file → sort → optional
//! 50→100 ms bin merge → rewrite staging tree → atomic swap with `.bak`.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp::{from_epoch_ms, to_epoch_ms};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{
    idx_dir, list_shards, read_shard_aligned, shard_path, today_utc, ts_ms_to_utc_date,
    write_shard_atomic, FLAG_IDX_HEALED, FLAG_RENKO_SYNTHETIC_BRICK, ShardRecord,
};
use nxr_sdk::tdwap::{decode_ci_ubp, encode_ci_ubp};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Sentinel sequence written to every healed record's MitchHeader.
///
/// The wire `sequence` field is a `u16` (gap-detection counter, 0..=65535).
/// At 100 ms cadence a single UTC day holds ~864k records — far beyond u16 —
/// so any per-day positional index would wrap and collide. Healed shards are
/// ts-ordered on disk, so the live gap-detection sequence is meaningless
/// post-heal; we set it to 0 to mark "non-monotonic / not a live stream"
/// rather than silently truncating a large index into a wrapped value.
const HEALED_SEQUENCE: u16 = 0;

#[derive(Debug, Clone)]
pub struct HealReport {
    pub ticker_id: u64,
    pub records_in: usize,
    pub records_out: usize,
    pub shards_in: usize,
    pub shards_out: usize,
    pub misrouted_dropped: usize,
    pub source_ms_detected: i64,
    pub target_ms: i64,
    pub resampled: bool,
}

pub fn heal_ticker_shards(
    data_root: &Path,
    ticker_id: u64,
    target_ms: i64,
    dry_run: bool,
    commit: bool,
) -> Result<HealReport> {
    if target_ms <= 0 {
        bail!("target_ms must be > 0");
    }
    let dir = idx_dir(data_root, ticker_id);
    if !dir.is_dir() {
        bail!("missing idx dir {}", dir.display());
    }

    let all_shards = list_shards(&dir, "idx")?;
    if all_shards.is_empty() {
        bail!("no idx shards under {}", dir.display());
    }

    // ⚠ LIVE-SHARD SAFETY: heal rebuilds the whole `<id>/` dir in staging then
    // atomically swaps `<id>` → `<id>.bak-TS` and `staging` → `<id>`. That swap
    // would carry away the CURRENT UTC day's shard — the file the live
    // aggregator (`IdxShardWriter`) holds open — orphaning its fd into the
    // `.bak` inode (live ticks then land in the dead backup while the API reads
    // the healed, frozen `<id>/<today>.idx`). So we (a) EXCLUDE today's shard
    // from the heal/resample pass and (b) copy the live `<today>.idx` verbatim
    // into staging before the swap, so the post-swap dir still contains the
    // untouched live shard for the writer to keep appending to. The commit also
    // takes the writer-lock (below); both guards are intentional. Confirmed
    // prod incident 2026-06-10. UNCONDITIONAL — no flag overrides it.
    let today = today_utc();
    let today_src: Option<PathBuf> = all_shards
        .iter()
        .find(|(d, _)| *d >= today)
        .map(|(_, p)| p.clone());
    let shards: Vec<(NaiveDate, PathBuf)> =
        all_shards.into_iter().filter(|(d, _)| *d < today).collect();
    if let Some(p) = &today_src {
        info!(
            ticker_id,
            shard = %p.display(),
            "skipping current-day shard (live-open, will heal after rotation)"
        );
    }
    if shards.is_empty() {
        bail!(
            "no closed (past-day) idx shards to heal under {} (only today's live shard present)",
            dir.display()
        );
    }

    // Sample source cadence across first/middle/last shards (median of the
    // per-shard medians) rather than the first readable shard alone — a single
    // shard can be unrepresentative (sparse early day, gap day, etc.).
    let source_ms_detected = detect_source_ms(&shards).unwrap_or(target_ms);
    let resampled = source_ms_detected < target_ms - 10;

    let staging = staging_dir(&dir, ticker_id);
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .with_context(|| format!("remove stale staging {}", staging.display()))?;
    }
    if !dry_run {
        fs::create_dir_all(&staging)?;
    }

    let mut records_in = 0usize;
    let mut records_out = 0usize;
    let mut misrouted_dropped = 0usize;
    // Destination day-shards we have written to, so the records_out / shards_out
    // tallies and progress logging reflect *output* days, not source days.
    let mut touched_dates: std::collections::BTreeSet<NaiveDate> = Default::default();

    // Per-shard streaming pass (preserves the OOM fix: only one source shard's
    // records are resident at a time). The merge/resample is applied within the
    // source shard; each resulting record is then routed to *its own* ts-correct
    // UTC day-shard (a single source file can straddle a day boundary), merging
    // into — never overwriting — the destination staging shard.
    for (src_date, path) in &shards {
        let mut recs = read_shard_aligned::<IndexRecord>(path)
            .with_context(|| format!("read {}", path.display()))?;
        records_in += recs.len();
        let before = recs.len();
        recs.retain(|r| r.index.ticker == ticker_id);
        misrouted_dropped += before - recs.len();
        if recs.is_empty() {
            continue;
        }
        recs.sort_by_key(|r| r.shard_ts_ms());
        let out = if resampled {
            resample_bins(&recs, target_ms)?
        } else {
            dedupe_bins(&recs, target_ms)?
        };
        records_out += out.len();

        if dry_run {
            continue;
        }

        // Fan this source shard's output records out by their ts-correct UTC day.
        // Bin-merge keys on the bin start, so a 50→100 ms merge cannot move a
        // record across a day boundary; the fan-out is normally {src_date} and
        // at most two adjacent days when the source file itself straddled a day.
        let mut by_date: BTreeMap<NaiveDate, Vec<IndexRecord>> = BTreeMap::new();
        for mut rec in out {
            let ts = rec.shard_ts_ms();
            let mts = from_epoch_ms(ts);
            let mut header = MitchHeader::new(
                message_type::INDEX,
                rec.header.provider_id(),
                mts,
                rec.header.count.max(1),
            );
            header.set_sequence(HEALED_SEQUENCE);
            rec.header = header;
            by_date.entry(ts_ms_to_utc_date(ts)).or_default().push(rec);
        }

        for (date, new_recs) in by_date {
            merge_into_staging_shard(&staging, date, new_recs)?;
            touched_dates.insert(date);
            if (touched_dates.len() % 50) == 0 {
                info!(date = %date, shards_done = touched_dates.len(), "heal-idx progress");
            }
        }
        let _ = src_date;
    }

    let shards_out = touched_dates.len();

    if misrouted_dropped > 0 {
        warn!(ticker_id, dropped = misrouted_dropped, "removed mis-routed records");
    }

    if dry_run {
        return Ok(HealReport {
            ticker_id,
            records_in,
            records_out,
            shards_in: shards.len(),
            shards_out,
            misrouted_dropped,
            source_ms_detected,
            target_ms,
            resampled,
        });
    }

    // Carry the live current-day shard through the directory swap untouched, so
    // the post-swap `<id>/` still holds it for the live writer (see safety note
    // at the top of this fn). Without this the swap would drop today's shard.
    if let Some(src) = &today_src {
        let date = today;
        let dst = shard_path(&staging, date, "idx");
        fs::copy(src, &dst)
            .with_context(|| format!("passthrough live shard {} → {}", src.display(), dst.display()))?;
        info!(date = %date, "copied live current-day shard into staging verbatim (passthrough)");
    }

    if commit {
        let _lock = nxr_sdk::shard::acquire_idx_writer_lock(&dir)
            .context("cannot commit heal while live idx writer holds lock — scale nxr to 0 first")?;
        let bak = dir.with_extension(format!(
            "bak-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
        ));
        fs::rename(&dir, &bak)
            .with_context(|| format!("backup {} → {}", dir.display(), bak.display()))?;
        fs::rename(&staging, &dir)
            .with_context(|| format!("promote staging → {}", dir.display()))?;
        info!(backup = %bak.display(), shards = shards_out, "heal-idx committed");
    } else {
        info!(staging = %staging.display(), "heal-idx wrote staging (pass --commit to swap)");
    }

    Ok(HealReport {
        ticker_id,
        records_in,
        records_out,
        shards_in: shards.len(),
        shards_out,
        misrouted_dropped,
        source_ms_detected,
        target_ms,
        resampled,
    })
}

fn staging_dir(ticker_dir: &Path, ticker_id: u64) -> PathBuf {
    ticker_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{ticker_id}.heal-staging"))
}

/// Merge `new_recs` into the staging day-shard for `date`, preserving any
/// records already written there by an earlier source shard (a source file that
/// straddled the day boundary, or out-of-order source shards). Reads the
/// existing staging shard (bounded: one day ≤ ~30MB), appends, re-sorts by ts,
/// and rewrites atomically. KEEP-not-overwrite is the whole point of the
/// per-record routing restore.
fn merge_into_staging_shard(
    staging: &Path,
    date: NaiveDate,
    mut new_recs: Vec<IndexRecord>,
) -> Result<()> {
    let dst = shard_path(staging, date, "idx");
    if dst.exists() {
        let mut existing = read_shard_aligned::<IndexRecord>(&dst)
            .with_context(|| format!("read staging {}", dst.display()))?;
        existing.append(&mut new_recs);
        new_recs = existing;
    }
    new_recs.sort_by_key(|r| r.shard_ts_ms());
    let bytes: Vec<u8> = new_recs
        .iter()
        .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
        .collect();
    write_shard_atomic(&dst, &bytes)
}

/// Detect the source cadence by sampling the first, middle, and last shards and
/// taking the median of their per-shard median deltas. More robust than reading
/// only the first readable shard (which can be a sparse/gap day).
fn detect_source_ms(shards: &[(NaiveDate, PathBuf)]) -> Option<i64> {
    if shards.is_empty() {
        return None;
    }
    let idxs = [0, shards.len() / 2, shards.len() - 1];
    let mut samples: Vec<i64> = idxs
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter_map(|i| {
            read_shard_aligned::<IndexRecord>(&shards[i].1)
                .ok()
                .and_then(|r| detect_median_delta_ms(&r))
        })
        .collect();
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    Some(samples[samples.len() / 2])
}

fn detect_median_delta_ms(recs: &[IndexRecord]) -> Option<i64> {
    if recs.len() < 2 {
        return None;
    }
    let mut deltas: Vec<i64> = recs
        .windows(2)
        .map(|w| w[1].shard_ts_ms() - w[0].shard_ts_ms())
        .filter(|d| *d > 0 && *d < 5000)
        .collect();
    if deltas.is_empty() {
        return None;
    }
    deltas.sort_unstable();
    Some(deltas[deltas.len() / 2])
}

fn bin_start(ts_ms: i64, target_ms: i64) -> i64 {
    (ts_ms / target_ms) * target_ms
}

fn resample_bins(recs: &[IndexRecord], target_ms: i64) -> Result<Vec<IndexRecord>> {
    if recs.is_empty() {
        return Ok(Vec::new());
    }
    let mut bins: BTreeMap<i64, Vec<IndexRecord>> = BTreeMap::new();
    for rec in recs {
        bins.entry(bin_start(rec.shard_ts_ms(), target_ms))
            .or_default()
            .push(*rec);
    }
    let mut out = Vec::with_capacity(bins.len());
    for (_bin, chunk) in bins.into_iter() {
        out.push(merge_chunk(&chunk, target_ms)?);
    }
    Ok(out)
}

fn dedupe_bins(recs: &[IndexRecord], target_ms: i64) -> Result<Vec<IndexRecord>> {
    if recs.is_empty() {
        return Ok(Vec::new());
    }
    let mut bins: BTreeMap<i64, Vec<IndexRecord>> = BTreeMap::new();
    for rec in recs {
        bins.entry(bin_start(rec.shard_ts_ms(), target_ms))
            .or_default()
            .push(*rec);
    }
    let mut out = Vec::with_capacity(bins.len());
    for (_bin, chunk) in bins.into_iter() {
        if chunk.len() == 1 {
            out.push(chunk[0]);
        } else {
            out.push(merge_chunk(&chunk, target_ms)?);
        }
    }
    Ok(out)
}

fn merge_chunk(chunk: &[IndexRecord], target_ms: i64) -> Result<IndexRecord> {
    if chunk.is_empty() {
        bail!("empty chunk");
    }
    let first = &chunk[0];
    let bin_ms = bin_start(first.shard_ts_ms(), target_ms);
    let bin_mts = from_epoch_ms(bin_ms);

    let ticker = first.index.ticker;
    let provider_id = first.header.provider_id();

    let mut bid_num = 0.0_f64;
    let mut bid_den = 0.0_f64;
    let mut ask_num = 0.0_f64;
    let mut ask_den = 0.0_f64;
    let mut vbid_sum: u32 = 0;
    let mut vask_sum: u32 = 0;
    let mut ci_num = 0.0_f64;
    let mut ci_den = 0.0_f64;
    let mut tick_count_sum: u16 = 0;
    let mut confidence_max: u8 = 0;
    let mut accepted_max: u8 = 0;
    let mut rejected_max: u8 = 0;
    let mut flags_or: u8 = 0;
    let mut input_count: u8 = 0;

    for r in chunk {
        let bid = r.index.bid;
        let ask = r.index.ask;
        let vbid = r.index.vbid;
        let vask = r.index.vask;
        let ci_u16 = r.index.ci;
        if r.index.ticker != ticker {
            bail!("ticker mismatch within bin");
        }
        let w_bid = vbid as f64;
        let w_ask = vask as f64;
        if w_bid > 0.0 {
            bid_num += bid * w_bid;
            bid_den += w_bid;
        }
        if w_ask > 0.0 {
            ask_num += ask * w_ask;
            ask_den += w_ask;
        }
        vbid_sum = vbid_sum.saturating_add(vbid);
        vask_sum = vask_sum.saturating_add(vask);
        let ci_dec = decode_ci_ubp(ci_u16);
        let w_ci = (vbid + vask) as f64;
        let w = if w_ci > 0.0 {
            w_ci
        } else {
            r.index.tick_count as f64
        };
        if w > 0.0 {
            ci_num += ci_dec * ci_dec * w;
            ci_den += w;
        }
        tick_count_sum = tick_count_sum.saturating_add(r.index.tick_count);
        confidence_max = confidence_max.max(r.index.confidence);
        accepted_max = accepted_max.max(r.index.accepted);
        rejected_max = rejected_max.max(r.index.rejected);
        flags_or |= r.index.flags;
        input_count = input_count.saturating_add(1);
    }

    let bid_out = if bid_den > 0.0 {
        bid_num / bid_den
    } else {
        chunk.iter().map(|r| r.index.bid).sum::<f64>() / chunk.len() as f64
    };
    let ask_out = if ask_den > 0.0 {
        ask_num / ask_den
    } else {
        chunk.iter().map(|r| r.index.ask).sum::<f64>() / chunk.len() as f64
    };
    let ci_out = if ci_den > 0.0 {
        encode_ci_ubp((ci_num / ci_den).sqrt())
    } else {
        chunk.iter().map(|r| r.index.ci).max().unwrap_or(0)
    };

    let mut header = MitchHeader::new(message_type::INDEX, provider_id, bin_mts, input_count.max(1));
    // Sequence is re-stamped to HEALED_SEQUENCE (0) by the caller after ts-routing;
    // value here is irrelevant. See HEALED_SEQUENCE for why healed seq is sentinel.
    header.set_sequence(HEALED_SEQUENCE);
    let body = Index {
        ticker,
        bid: bid_out,
        ask: ask_out,
        vbid: vbid_sum,
        vask: vask_sum,
        ci: ci_out,
        tick_count: tick_count_sum,
        confidence: confidence_max,
        accepted: accepted_max,
        rejected: rejected_max,
        // FLAG_IDX_HEALED (0x10) — distinct from FLAG_RENKO_SYNTHETIC_BRICK
        // (0x04) which the old local RESAMPLE_FLAG collided with.
        flags: flags_or | FLAG_IDX_HEALED,
    };
    Ok(IndexRecord::new(header, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitch::header::MitchHeader;
    use nxr_sdk::ipc::record::IndexRecord;
    use nxr_sdk::shard::{list_shards, read_shard_aligned, shard_path, write_shard_atomic, ShardRecord};

    fn rec(ts: i64, bid: f64) -> IndexRecord {
        let id = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let mts = from_epoch_ms(ts);
        let header = MitchHeader::new(message_type::INDEX, 0, mts, 1);
        let index = Index {
            ticker: id,
            bid,
            ask: bid * 1.0001,
            vbid: 1,
            vask: 1,
            ci: 100,
            tick_count: 1,
            confidence: 50,
            accepted: 1,
            rejected: 0,
            flags: 0,
        };
        IndexRecord::new(header, index)
    }

    #[test]
    fn heal_sorts_out_of_order_shards() {
        let root = std::env::temp_dir().join("heal_idx_test");
        let _ = fs::remove_dir_all(&root);
        let id = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let dir = idx_dir(&root, id);
        fs::create_dir_all(&dir).unwrap();
        // Anchor on a CLOSED past day (2 days ago) so the live-shard skip guard
        // does not exclude it — heal only operates on past-day shards now.
        let base = nxr_sdk::now_ms() as i64 - 2 * 86_400_000;
        let recs = vec![rec(base + 200, 100.0), rec(base, 99.0)];
        let bytes: Vec<u8> = recs
            .iter()
            .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
            .collect();
        let date = nxr_sdk::shard::ts_ms_to_utc_date(base);
        write_shard_atomic(&shard_path(&dir, date, "idx"), &bytes).unwrap();

        let rep = heal_ticker_shards(&root, id, 100, false, true).unwrap();
        assert_eq!(rep.records_in, 2);
        // base and base+200 fall in different 100 ms bins (dedupe path, not
        // resampled since 200 >= target-10) → exactly 2 records out.
        assert_eq!(rep.records_out, 2);
        let shards = list_shards(&idx_dir(&root, id), "idx").unwrap();
        let healed = read_shard_aligned::<IndexRecord>(&shards[0].1).unwrap();
        assert!(healed[0].shard_ts_ms() <= healed[1].shard_ts_ms());
        // Healed records carry FLAG_IDX_HEALED, NOT FLAG_RENKO_SYNTHETIC_BRICK.
        for h in &healed {
            assert_eq!(h.index.flags & FLAG_RENKO_SYNTHETIC_BRICK, 0);
        }
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn heal_routes_records_across_day_boundary() {
        // A source shard straddling a UTC day boundary must fan its records out
        // to the correct day-shards (the per-record routing restore, BUG 3).
        let root = std::env::temp_dir().join("heal_idx_route_test");
        let _ = fs::remove_dir_all(&root);
        let id = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let dir = idx_dir(&root, id);
        fs::create_dir_all(&dir).unwrap();
        // Pick a UTC midnight well after the MITCH 2010 epoch (from_epoch_ms
        // clamps anything <= 2010 to 0). Records at boundary-100 and +100 belong
        // to two different UTC days but are both written under the earlier day's
        // file — heal must re-route the second to the next day's shard.
        let boundary = ts_ms_to_utc_date(1_700_000_000_000) // some 2023 day
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis()
            + MS_PER_DAY_LOCAL; // next UTC midnight
        let day0 = ts_ms_to_utc_date(boundary - 100);
        let r0 = rec(boundary - 100, 100.0);
        let r1 = rec(boundary + 100, 101.0);
        let bytes: Vec<u8> = [r0, r1]
            .iter()
            .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
            .collect();
        write_shard_atomic(&shard_path(&dir, day0, "idx"), &bytes).unwrap();

        heal_ticker_shards(&root, id, 100, false, true).unwrap();
        let shards = list_shards(&idx_dir(&root, id), "idx").unwrap();
        // Two distinct day-shards expected.
        assert_eq!(shards.len(), 2, "record past midnight must land in next day's shard");
        let _ = fs::remove_dir_all(&root);
    }

    const MS_PER_DAY_LOCAL: i64 = 86_400_000;

    /// LIVE-SHARD SAFETY: heal excludes the current UTC day's shard (live-open)
    /// and carries it through the directory swap verbatim, while still healing a
    /// closed past-day shard. Renaming today's shard would orphan the live
    /// writer's fd into the `.bak` inode (prod incident 2026-06-10).
    #[test]
    fn heal_skips_today_passes_through_live_shard() {
        let root = std::env::temp_dir().join("heal_idx_skip_today");
        let _ = fs::remove_dir_all(&root);
        let id = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let dir = idx_dir(&root, id);
        fs::create_dir_all(&dir).unwrap();

        let now = nxr_sdk::now_ms() as i64;
        let today = today_utc();
        let past_ts = now - 3 * MS_PER_DAY_LOCAL; // closed past day
        let past = ts_ms_to_utc_date(past_ts);

        // Past-day shard (will be healed).
        let past_bytes: Vec<u8> = [rec(past_ts, 100.0), rec(past_ts + 200, 101.0)]
            .iter()
            .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
            .collect();
        write_shard_atomic(&shard_path(&dir, past, "idx"), &past_bytes).unwrap();

        // Today's live shard with a distinctive bid → must survive untouched.
        let live_bid = 424_242.0_f64;
        let live_bytes: Vec<u8> = [rec(now - 1000, live_bid)]
            .iter()
            .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
            .collect();
        let live_path = shard_path(&dir, today, "idx");
        write_shard_atomic(&live_path, &live_bytes).unwrap();

        let rep = heal_ticker_shards(&root, id, 100, false, true).unwrap();
        // Only the past shard's records were healed (today's excluded).
        assert_eq!(rep.records_in, 2, "today's shard not read into the heal pass");

        let shards = list_shards(&idx_dir(&root, id), "idx").unwrap();
        let dates: Vec<NaiveDate> = shards.iter().map(|(d, _)| *d).collect();
        assert!(dates.contains(&past), "past-day shard healed + present");
        assert!(dates.contains(&today), "today's live shard carried through swap");

        // Today's shard is byte-identical to the original live data (verbatim
        // passthrough — NOT healed, NOT flagged).
        let today_path = shard_path(&idx_dir(&root, id), today, "idx");
        let live = read_shard_aligned::<IndexRecord>(&today_path).unwrap();
        assert_eq!(live.len(), 1, "live shard untouched (1 record)");
        let live_idx = live[0].index; // copy out of packed struct
        let (live_out_bid, live_out_flags) = (live_idx.bid, live_idx.flags); // scalar copies
        assert_eq!(live_out_bid, live_bid, "live bid preserved verbatim");
        assert_eq!(
            live_out_flags & FLAG_IDX_HEALED,
            0,
            "live shard NOT marked healed (passthrough only)"
        );

        let _ = fs::remove_dir_all(&root);
    }
}
