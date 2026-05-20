//! Smoke tests for the `integrity-check` binary.
//!
//! These build minimal binary fixtures (truncated, monotone-violation, crossed
//! quote, NaN sigma, renko discontinuity, good baseline) and invoke the
//! `integrity-check` binary against each, asserting exit code + stderr
//! substrings.
//!
//! Note: this test relies on the `integrity-check` binary being wired in
//! `Cargo.toml` (`[[bin]] name = "integrity-check"`). Until the Phase 55
//! orchestrator adds that entry the test is dormant — Cargo will simply skip
//! it. Once wired, `cargo test -p series-factory --test integrity_smoke`
//! exercises every fixture.

#![cfg(test)]

use std::path::PathBuf;
use std::process::Command;

use bytemuck::{bytes_of, Pod, Zeroable};
use mitch::common::{message_sizes, message_type};
use mitch::header::MitchHeader;
use mitch::index::Index;
use mitch::timestamp;
use nxr_sdk::ipc::record::IndexRecord;

// ── Fixture builders ────────────────────────────────────────────────────────

/// Build a syntactically valid `IndexRecord` for tests.
fn good_record(epoch_ms: i64, bid: f64, ask: f64) -> IndexRecord {
    let mts = timestamp::from_epoch_ms(epoch_ms);
    let header = MitchHeader::new(message_type::INDEX, 1, mts, 1);
    let index = Index::new(
        0xDEAD_BEEF_u64,
        bid,
        ask,
        100,   // ci
        1_000, // vbid
        1_000, // vask
        10,    // tick_count
        1,     // confidence
        1,     // accepted
        0,     // rejected
    );
    IndexRecord::new(header, index)
}

/// 14-byte vol record, mirroring `series_factory::vol_bin::VolRecord` so the
/// test does not need a runtime dependency on that struct's privacy boundary.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C, packed)]
struct VolRow {
    mts: [u8; 6],
    sigma_pct: f64,
}

fn vol_row(epoch_ms: i64, sigma_pct: f64) -> VolRow {
    VolRow {
        mts: timestamp::encode_u48(timestamp::from_epoch_ms(epoch_ms)),
        sigma_pct,
    }
}

/// Bar fixture (96 B). Mirrors the layout of `mitch::Bar`.
fn make_bar(
    open_ms: i64,
    close_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    kind: u8,
) -> mitch::bar::Bar {
    let open_mts = timestamp::from_epoch_ms(open_ms);
    let close_mts = timestamp::from_epoch_ms(close_ms);
    let mut b = mitch::bar::Bar::new_ohlcv(open_mts, close_mts, open, high, low, close, 0, 0, 1);
    b.kind = kind;
    b
}

fn write_file(name: &str, bytes: &[u8]) -> PathBuf {
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "nxr-integrity-smoke-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        name
    ));
    std::fs::write(&path, bytes).expect("write fixture");
    path
}

// ── Binary invocation ───────────────────────────────────────────────────────

/// Locate the binary built by Cargo for the current test target.
///
/// `CARGO_BIN_EXE_integrity-check` is set by Cargo iff the bin is declared in
/// `Cargo.toml`. If it isn't (pre-orchestrator wiring), the test is skipped.
fn bin_path() -> Option<PathBuf> {
    option_env!("CARGO_BIN_EXE_integrity-check").map(PathBuf::from)
}

fn run(args: &[&str]) -> (i32, String, String) {
    let bin = bin_path().expect("integrity-check bin not wired into Cargo.toml — skipping");
    let out = Command::new(bin)
        .args(args)
        .output()
        .expect("spawn integrity-check");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let code = out.status.code().unwrap_or(-1);
    (code, stdout, stderr)
}

fn skip_if_no_bin() -> bool {
    if bin_path().is_none() {
        eprintln!("SKIP: CARGO_BIN_EXE_integrity-check not set (bin not wired)");
        return true;
    }
    false
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn idx_good_file_clean() {
    if skip_if_no_bin() { return; }
    let mut recs: Vec<IndexRecord> = Vec::new();
    let t0 = 1_700_000_000_000;
    for i in 0..32 {
        recs.push(good_record(t0 + i * 100, 100.0, 100.1));
    }
    let bytes: &[u8] = bytemuck::cast_slice(&recs);
    let path = write_file("good.idx", bytes);
    let (code, _stdout, stderr) = run(&["idx", path.to_str().unwrap()]);
    assert_eq!(code, 0, "expected clean exit, got {}; stderr={}", code, stderr);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn idx_truncated_file_errors() {
    if skip_if_no_bin() { return; }
    // 56 + 30 bytes → not a multiple of 56.
    let mut recs: Vec<IndexRecord> = Vec::new();
    recs.push(good_record(1_700_000_000_000, 100.0, 100.1));
    let full: &[u8] = bytemuck::cast_slice(&recs);
    let mut bytes = full.to_vec();
    bytes.extend_from_slice(&[0u8; 30]);
    let path = write_file("truncated.idx", &bytes);
    let (code, _stdout, stderr) = run(&["idx", path.to_str().unwrap()]);
    assert_eq!(code, 2, "expected error exit, got {}; stderr={}", code, stderr);
    assert!(
        stderr.contains("truncated"),
        "expected 'truncated' in stderr, got: {}",
        stderr
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn idx_non_monotone_ts_errors() {
    if skip_if_no_bin() { return; }
    let mut recs: Vec<IndexRecord> = Vec::new();
    recs.push(good_record(1_700_000_000_000, 100.0, 100.1));
    recs.push(good_record(1_700_000_000_100, 100.0, 100.1));
    recs.push(good_record(1_700_000_000_050, 100.0, 100.1)); // backwards
    let bytes: &[u8] = bytemuck::cast_slice(&recs);
    let path = write_file("nonmono.idx", bytes);
    let (code, _stdout, stderr) = run(&["idx", path.to_str().unwrap()]);
    assert_eq!(code, 2);
    assert!(
        stderr.contains("non-monotone"),
        "expected 'non-monotone' in stderr, got: {}",
        stderr
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn idx_crossed_quote_errors() {
    if skip_if_no_bin() { return; }
    // ask < bid is rejected by Index::validate inside check_idx.
    let mut recs: Vec<IndexRecord> = Vec::new();
    recs.push(good_record(1_700_000_000_000, 100.5, 100.0));
    let bytes: &[u8] = bytemuck::cast_slice(&recs);
    let path = write_file("crossed.idx", bytes);
    let (code, _stdout, stderr) = run(&["idx", path.to_str().unwrap()]);
    assert_eq!(code, 2);
    assert!(
        stderr.to_lowercase().contains("ask")
            || stderr.contains("Index::validate"),
        "expected crossed-quote error, got: {}",
        stderr
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn vol_nan_sigma_errors() {
    if skip_if_no_bin() { return; }
    let rows = [
        vol_row(1_700_000_000_000, 0.01),
        vol_row(1_700_000_000_100, f64::NAN),
    ];
    let bytes: &[u8] = bytemuck::cast_slice(&rows);
    // Sanity: 14 B per row.
    assert_eq!(bytes.len(), rows.len() * 14);
    let path = write_file("nan.vol", bytes);
    let (code, _stdout, stderr) = run(&["vol", path.to_str().unwrap()]);
    assert_eq!(code, 2);
    assert!(
        stderr.to_lowercase().contains("non-finite") || stderr.to_lowercase().contains("nan"),
        "expected NaN/finite error, got: {}",
        stderr
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn bars_renko_discontinuity_errors() {
    if skip_if_no_bin() { return; }
    // Two renko bars where bar2.open != bar1.close.
    let b1 = make_bar(
        1_700_000_000_000,
        1_700_000_000_500,
        100.0,
        101.0,
        100.0,
        101.0,
        1, // renko
    );
    let b2 = make_bar(
        1_700_000_000_500,
        1_700_000_001_000,
        102.0, // ≠ b1.close (101.0)
        103.0,
        102.0,
        103.0,
        1,
    );
    let bars = [b1, b2];
    let bytes: &[u8] = bytemuck::cast_slice(&bars);
    assert_eq!(bytes.len(), 2 * message_sizes::BAR);
    let path = write_file("renko_bad.bars", bytes);
    let (code, _stdout, stderr) = run(&["bars", path.to_str().unwrap()]);
    assert_eq!(code, 2, "expected discontinuity error; stderr={}", stderr);
    assert!(
        stderr.contains("renko discontinuity"),
        "expected 'renko discontinuity', got: {}",
        stderr
    );
    let _ = std::fs::remove_file(&path);
}

// Touch the VolRow type & bytes_of to silence "unused" lints on toolchains
// that don't otherwise reach the helpers (e.g. when CARGO_BIN_EXE_* is unset
// and every test early-returns).
#[allow(dead_code)]
fn _keep_used() {
    let r = vol_row(0, 0.0);
    let _ = bytes_of(&r);
}
