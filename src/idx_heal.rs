//! In-place repair for sharded `.idx` without re-fetching raw ticks.
//!
//! Pass: collect all shards → sort by ts → optional 50→100 ms bin merge →
//! re-partition by UTC date → atomic rewrite with `.bak` retention.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp::{from_epoch_ms, to_epoch_ms};
use nxr_sdk::ipc::record::IndexRecord;
use nxr_sdk::shard::{
    idx_dir, list_shards, read_shard_aligned, shard_path, ts_ms_to_utc_date, write_shard_atomic,
    ShardRecord,
};
use nxr_sdk::tdwap::{decode_ci_ubp, encode_ci_ubp};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

const RESAMPLE_FLAG: u8 = 0x04;
const REC_BYTES: usize = std::mem::size_of::<IndexRecord>();

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

    let shards = list_shards(&dir, "idx")?;
    if shards.is_empty() {
        bail!("no idx shards under {}", dir.display());
    }

    let mut all: Vec<IndexRecord> = Vec::new();
    for (_date, path) in &shards {
        let recs = read_shard_aligned::<IndexRecord>(path)
            .with_context(|| format!("read {}", path.display()))?;
        all.extend(recs);
    }
    let records_in = all.len();
    let misrouted_dropped = {
        let before = all.len();
        all.retain(|r| r.index.ticker == ticker_id);
        before - all.len()
    };
    if misrouted_dropped > 0 {
        warn!(
            ticker_id,
            dropped = misrouted_dropped,
            "removed mis-routed records"
        );
    }

    all.sort_by_key(|r| r.shard_ts_ms());

    let source_ms_detected = detect_median_delta_ms(&all).unwrap_or(target_ms);
    let resampled = source_ms_detected < target_ms - 10;
    let merged = if resampled {
        resample_bins(&all, target_ms)?
    } else {
        dedupe_bins(&all, target_ms)?
    };
    let records_out = merged.len();

    let mut by_date: BTreeMap<NaiveDate, Vec<IndexRecord>> = BTreeMap::new();
    for (seq, mut rec) in merged.into_iter().enumerate() {
        let ts = rec.shard_ts_ms();
        let date = ts_ms_to_utc_date(ts);
        let mts = from_epoch_ms(ts);
        rec.header = MitchHeader::new(
            message_type::INDEX,
            rec.header.provider_id(),
            mts,
            rec.header.count.max(1),
        );
        rec.header.set_sequence(seq as u16);
        by_date.entry(date).or_default().push(rec);
    }

    for recs in by_date.values_mut() {
        recs.sort_by_key(|r| r.shard_ts_ms());
        for (i, rec) in recs.iter_mut().enumerate() {
            rec.header.set_sequence(i as u16);
        }
    }

    if dry_run {
        return Ok(HealReport {
            ticker_id,
            records_in,
            records_out,
            shards_in: shards.len(),
            shards_out: by_date.len(),
            misrouted_dropped,
            source_ms_detected,
            target_ms,
            resampled,
        });
    }

    let staging = dir.with_extension("heal-staging");
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .with_context(|| format!("remove stale staging {}", staging.display()))?;
    }
    fs::create_dir_all(&staging)?;

    for (date, recs) in &by_date {
        let path = shard_path(&staging, *date, "idx");
        let bytes: Vec<u8> = recs
            .iter()
            .flat_map(|r| bytemuck::bytes_of(r).iter().copied())
            .collect();
        write_shard_atomic(&path, &bytes)?;
    }

    if commit {
        let _lock = nxr_sdk::shard::acquire_idx_writer_lock(&dir)
            .context("cannot commit heal while live idx writer holds lock — scale nxr to 0 first")?;
        let bak = dir.with_extension(format!(
            "bak-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
        ));
        fs::rename(&dir, &bak).with_context(|| format!("backup {} → {}", dir.display(), bak.display()))?;
        fs::rename(&staging, &dir).with_context(|| format!("promote staging → {}", dir.display()))?;
        info!(backup = %bak.display(), "heal-idx committed");
    } else {
        info!(
            staging = %staging.display(),
            "heal-idx wrote staging (pass --commit to swap)"
        );
    }

    Ok(HealReport {
        ticker_id,
        records_in,
        records_out,
        shards_in: shards.len(),
        shards_out: by_date.len(),
        misrouted_dropped,
        source_ms_detected,
        target_ms,
        resampled,
    })
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
    for (i, (_bin, chunk)) in bins.into_iter().enumerate() {
        out.push(merge_chunk(&chunk, target_ms, i as u16)?);
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
    for (i, (_bin, chunk)) in bins.into_iter().enumerate() {
        if chunk.len() == 1 {
            out.push(chunk[0]);
        } else {
            out.push(merge_chunk(&chunk, target_ms, i as u16)?);
        }
    }
    Ok(out)
}

fn merge_chunk(chunk: &[IndexRecord], target_ms: i64, out_seq: u16) -> Result<IndexRecord> {
    if chunk.is_empty() {
        bail!("empty chunk");
    }
    let first = &chunk[0];
    let bin_ms = bin_start(first.shard_ts_ms(), target_ms);
    let bin_mts = from_epoch_ms(bin_ms);

    let ticker = first.index.ticker;
    let msg_type = first.header.message_type();
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
    header.set_sequence(out_seq);
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
        flags: flags_or | RESAMPLE_FLAG,
    };
    Ok(IndexRecord::new(header, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitch::header::MitchHeader;
    use nxr_sdk::ipc::record::IndexRecord;
    use nxr_sdk::shard::{IdxShardWriter, ShardRecord};

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
        let mut w = IdxShardWriter::open(&root, id, false).unwrap();
        let base = nxr_sdk::now_ms() as i64 - 3600_000;
        w.append(&rec(base + 100, 100.0)).unwrap();
        w.append(&rec(base, 99.0)).unwrap();
        drop(w);

        let rep = heal_ticker_shards(&root, id, 100, false, true).unwrap();
        assert_eq!(rep.records_in, 2);
        assert_eq!(rep.records_out, 2);
        let shards = list_shards(&idx_dir(&root, id), "idx").unwrap();
        let healed = read_shard_aligned::<IndexRecord>(&shards[0].1).unwrap();
        assert!(healed[0].shard_ts_ms() <= healed[1].shard_ts_ms());
        let _ = fs::remove_dir_all(&root);
    }
}
