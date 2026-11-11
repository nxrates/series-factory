//! `ticks-to-idx` and `merge-idx` honour their UTC window: nothing outside it is
//! folded, stamped or written, and a shard of a day outside it is never touched.

use mitch::common::message_type;
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp::from_epoch_ms;
use nxr_sdk::ipc::record::IndexRecord;
use series_factory::TickFrame;
use std::path::{Path, PathBuf};
use std::process::Command;

const DAY_MS: i64 = 86_400_000;
/// 2026-08-10 00:00:00 UTC.
const D10: i64 = 1_786_320_000_000;

fn root(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("sf-window-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run(bin: &str, root: &Path, args: &[&str]) {
    let out = Command::new(bin)
        .args(args)
        .env("NXR_DATA_ROOT", root)
        .env(
            "NXR_CONFIG",
            concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"),
        )
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn idx_ts(path: &Path) -> Vec<i64> {
    let b = std::fs::read(path).unwrap();
    bytemuck::cast_slice::<u8, IndexRecord>(&b)
        .iter()
        .map(|r| mitch::timestamp::to_epoch_ms(r.header.get_timestamp()))
        .collect()
}

fn idx_record(ms: i64) -> IndexRecord {
    let header = MitchHeader::new(message_type::INDEX, 101, from_epoch_ms(ms), 1);
    IndexRecord::new(header, Index::new(1, 100.0, 100.0, 0, 5, 5, 1, 1, 1, 0))
}

/// The closing flush must not rotate into the day after the window and replace
/// that day's finished shard.
#[test]
fn merge_final_flush_stays_inside_the_window() {
    let r = root("merge");
    let dir = r.join("indexes/binance");
    std::fs::create_dir_all(&dir).unwrap();
    let recs: Vec<IndexRecord> = [D10 + 1_000, D10 + DAY_MS - 900, D10 + DAY_MS - 100]
        .iter()
        .map(|&t| idx_record(t))
        .collect();
    std::fs::write(dir.join("BTC-USDT.idx"), bytemuck::cast_slice(&recs)).unwrap();
    let out = r.join("indexes/out");
    std::fs::create_dir_all(&out).unwrap();
    let next = out.join("2026-08-11.idx");
    let finished = idx_record(D10 + DAY_MS + 5_000);
    std::fs::write(&next, bytemuck::bytes_of(&finished)).unwrap();
    let before = std::fs::read(&next).unwrap();
    run(
        env!("CARGO_BIN_EXE_merge-idx"),
        &r,
        &[
            "BTC",
            "USDT",
            "--exchange",
            "binance",
            "--out-dir",
            out.to_str().unwrap(),
            "--from-date",
            "2026-08-10",
            "--to-date",
            "2026-08-10",
        ],
    );
    assert_eq!(
        std::fs::read(&next).unwrap(),
        before,
        "next day's shard was touched"
    );
    let ts = idx_ts(&out.join("2026-08-10.idx"));
    assert!(!ts.is_empty() && ts.iter().all(|t| *t < D10 + DAY_MS));
}

/// Ticks outside the window are not folded, and no record is stamped past it.
#[test]
fn t2i_window_drops_outside_ticks_and_final_stamp() {
    let r = root("t2i");
    let dir = r.join("ticks/binance/BTCUSDT");
    std::fs::create_dir_all(&dir).unwrap();
    let tick = |ms: i64| {
        TickFrame::new(
            101,
            from_epoch_ms(ms),
            mitch::Tick {
                ticker: 1,
                bid: 100.0,
                ask: 100.0,
                vbid: 0,
                vask: 5,
            },
        )
    };
    let ticks: Vec<TickFrame> = [
        D10 - 500,
        D10 + 1_000,
        D10 + 2_000,
        D10 + DAY_MS - 100,
        D10 + DAY_MS + 500,
    ]
    .iter()
    .map(|&t| tick(t))
    .collect();
    std::fs::write(dir.join("a.ticks"), bytemuck::cast_slice(&ticks)).unwrap();
    let out = r.join("out.idx");
    run(
        env!("CARGO_BIN_EXE_ticks-to-idx"),
        &r,
        &[
            "binance",
            "BTC",
            "USDT",
            "--out",
            out.to_str().unwrap(),
            "--from-date",
            "2026-08-10",
            "--to-date",
            "2026-08-10",
        ],
    );
    let ts = idx_ts(&out);
    assert!(!ts.is_empty());
    assert!(ts.iter().all(|t| *t >= D10 && *t < D10 + DAY_MS), "{ts:?}");
}

/// A same-second run of a whole-second venue (Bitget) split over two files is
/// spread once across the second: one record per slot, time never going back.
#[test]
fn t2i_spreads_a_coarse_stamp_run_across_files() {
    let r = root("spread");
    let dir = r.join("ticks/bitget/BTCUSDT");
    std::fs::create_dir_all(&dir).unwrap();
    let s = D10 + 10_000;
    let tick = |ms: i64| {
        TickFrame::new(
            102,
            from_epoch_ms(ms),
            mitch::Tick {
                ticker: 1,
                bid: 100.0,
                ask: 100.0,
                vbid: 0,
                vask: 5,
            },
        )
    };
    let a = [tick(s), tick(s)];
    let b = [tick(s), tick(s), tick(s + 1000)];
    std::fs::write(dir.join("20260810_001.ticks"), bytemuck::cast_slice(&a)).unwrap();
    std::fs::write(dir.join("20260810_002.ticks"), bytemuck::cast_slice(&b)).unwrap();
    let out = r.join("out.idx");
    run(
        env!("CARGO_BIN_EXE_ticks-to-idx"),
        &r,
        &["bitget", "BTC", "USDT", "--out", out.to_str().unwrap()],
    );
    let ts = idx_ts(&out);
    assert!(ts.windows(2).all(|w| w[0] <= w[1]), "{ts:?}");
    let in_second = ts.iter().filter(|t| **t > s && **t <= s + 1000).count();
    assert_eq!(in_second, 4, "four trades, four slots: {ts:?}");
}
