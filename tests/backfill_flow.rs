//! `backfill-all` orchestration with fake stage binaries on PATH: when a month's
//! sources may be deleted, which windows are asked for, and what a fetch fault does.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

struct Rig {
    root: PathBuf,
    bin: PathBuf,
    log: PathBuf,
}

impl Rig {
    /// `fetch_exit`: exit code of the fake fetch.
    fn new(tag: &str, fetch_exit: i32) -> Self {
        let root = std::env::temp_dir().join(format!("sf-flow-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let log = root.join("calls.log");
        let data = root.join("data");
        let script = |name: &str, body: &str| {
            let p = bin.join(name);
            fs::write(
                &p,
                format!(
                    "#!/bin/sh\necho \"{name} $@\" >> {}\n{body}\n",
                    log.display()
                ),
            )
            .unwrap();
            fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
        };
        // fetch writes a raw file; t2i writes its per-provider idx; merge writes a composite shard
        script(
            "fetch-crypto-history",
            &format!("mkdir -p {d}/ticks/binance/BTCUSDT && : > {d}/ticks/binance/BTCUSDT/a.ticks\nexit {fetch_exit}", d = data.display()),
        );
        script(
            "ticks-to-idx",
            &format!(
                "mkdir -p {d}/indexes/binance && : > {d}/indexes/binance/BTC-USDT.idx",
                d = data.display()
            ),
        );
        script("merge-idx", "exit 0");
        Self { root, bin, log }
    }

    fn run(&self, extra: &[&str]) {
        let data = self.root.join("data");
        let path = format!("{}:{}", self.bin.display(), std::env::var("PATH").unwrap());
        let _ = Command::new(env!("CARGO_BIN_EXE_backfill-all"))
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"))
            .args([
                "--tickers",
                "BTC-USDT",
                "--exchanges",
                "binance",
                "--skip-probe",
                "--min-free-gib",
                "0",
            ])
            .args([
                "--from",
                "2026-08-10",
                "--to",
                "2026-08-11",
                "--out-dir",
                data.to_str().unwrap(),
            ])
            .args([
                "--log-file",
                self.root.join("report.json").to_str().unwrap(),
            ])
            .args(extra)
            .env("PATH", path)
            .env("NXR_DATA_ROOT", &data)
            .env(
                "NXR_CONFIG",
                concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"),
            )
            .output()
            .unwrap();
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn data(&self, rel: &str) -> PathBuf {
        self.root.join("data").join(rel)
    }
}

fn exists(p: &Path) -> bool {
    p.exists()
}

/// A venue's fetch fault: its month is neither merged nor disposed, so its days stay todo.
#[test]
fn fetch_fault_skips_merge_and_keeps_sources() {
    let r = Rig::new("fault", 1);
    r.run(&["--steps", "fetch,t2i,merge"]);
    let calls = r.calls();
    assert!(calls.contains("fetch-crypto-history"), "{calls}");
    assert!(
        !calls.contains("merge-idx") && !calls.contains("ticks-to-idx"),
        "{calls}"
    );
    assert!(
        exists(&r.data("ticks/binance/BTCUSDT/a.ticks")),
        "raw was deleted"
    );
}

/// Sources go only after a merge ran and succeeded in this run.
#[test]
fn sources_are_disposed_only_after_a_successful_merge() {
    let r = Rig::new("dispose", 0);
    r.run(&["--steps", "fetch,t2i"]);
    assert!(
        exists(&r.data("ticks/binance/BTCUSDT/a.ticks")),
        "no merge ran, raw must stay"
    );
    assert!(
        exists(&r.data("indexes/binance/BTC-USDT.idx")),
        "no merge ran, idx must stay"
    );
    // merge-only: existing per-provider idx are not deleted at the start
    r.run(&["--steps", "merge"]);
    assert!(r.calls().contains("merge-idx"), "{}", r.calls());
}

/// The window never reaches the newest two days: their UTC+8 tail files may not exist yet.
#[test]
fn window_stops_two_days_before_today() {
    let r = Rig::new("lag", 0);
    let today = chrono::Utc::now().date_naive();
    let from = (today - chrono::Duration::days(5)).to_string();
    let data = r.root.join("data");
    let path = format!("{}:{}", r.bin.display(), std::env::var("PATH").unwrap());
    let _ = Command::new(env!("CARGO_BIN_EXE_backfill-all"))
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"))
        .args([
            "--tickers",
            "BTC-USDT",
            "--exchanges",
            "binance",
            "--skip-probe",
            "--min-free-gib",
            "0",
        ])
        .args([
            "--from",
            &from,
            "--to",
            "today",
            "--out-dir",
            data.to_str().unwrap(),
        ])
        .args([
            "--steps",
            "fetch",
            "--log-file",
            r.root.join("report.json").to_str().unwrap(),
        ])
        .env("PATH", path)
        .env("NXR_DATA_ROOT", &data)
        .env(
            "NXR_CONFIG",
            concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"),
        )
        .output()
        .unwrap();
    let want = format!("--to-date {}", today - chrono::Duration::days(2));
    assert!(r.calls().contains(&want), "want {want} in {}", r.calls());
}
