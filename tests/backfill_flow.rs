//! `backfill-all` orchestration with fake stage binaries on PATH: when sources are
//! deleted, which days are held or finalised, what is recorded, and what a lock
//! or a fetch error does.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

struct Rig {
    root: PathBuf,
    bin: PathBuf,
    log: PathBuf,
}

/// Fake stages. `states`: the JSON line fake `fetch-crypto-history` prints;
/// `fetch_exit` / `merge_exit`: exit codes. Like the real stages: fetch writes a raw
/// file, t2i (run without `--delete-after-convert`) writes a per-provider idx and
/// keeps the raw file, merge writes one shard per day of its window and the manifest.
impl Rig {
    fn new(tag: &str, states: &str, fetch_exit: i32, merge_exit: i32) -> Self {
        let root = std::env::temp_dir().join(format!("sf-flow-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let log = root.join("calls.log");
        let data = root.join("data");
        let tid = nxr_sdk::resolve_ticker_id("BTC/USDT");
        let comp = data.join(format!("indexes/{tid}"));
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
        fs::write(root.join("states.json"), states).unwrap();
        script(
            "fetch-crypto-history",
            &format!(
                "mkdir -p {d}/ticks/binance/BTCUSDT && : > {d}/ticks/binance/BTCUSDT/a.ticks\ncat {s}\nexit {fetch_exit}",
                d = data.display(),
                s = root.join("states.json").display()
            ),
        );
        script(
            "ticks-to-idx",
            &format!(
                "mkdir -p {d}/indexes/binance && : > {d}/indexes/binance/BTC-USDT.idx",
                d = data.display()
            ),
        );
        // merge: `--from-date A --to-date B` -> one 56-byte shard per day, plus the manifest
        script(
            "merge-idx",
            &format!(
                r#"[ {merge_exit} -ne 0 ] && exit {merge_exit}
while [ $# -gt 0 ]; do case $1 in --from-date) A=$2;; --to-date) B=$2;; esac; shift; done
mkdir -p {c}; d=$A; E=""
while :; do head -c 56 /dev/zero > {c}/$d.idx; E="$E{{\"date\":\"$d\",\"first_ts\":0,\"last_ts\":0,\"n_records\":1,\"size_bytes\":56,\"sha256\":\"\"}},"; [ "$d" = "$B" ] && break; d=$(date -j -v+1d -f %Y-%m-%d $d +%Y-%m-%d); done
echo "{{\"ticker\":\"T\",\"ticker_id\":1,\"kind\":\"idx\",\"shards\":[${{E%,}}],\"first_ts\":0,\"last_ts\":0,\"last_updated\":0,\"schema_version\":1}}" > {c}/manifest.json"#,
                c = comp.display()
            ),
        );
        script("integrity-check", "exit 0");
        Self { root, bin, log }
    }

    /// Replace the fake validator's behaviour (a shell snippet).
    fn validator(&self, body: &str) {
        let p = self.bin.join("integrity-check");
        fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
    }

    fn run(&self, window: &[&str], steps: &str) -> std::process::Output {
        let data = self.root.join("data");
        let path = format!("{}:{}", self.bin.display(), std::env::var("PATH").unwrap());
        Command::new(env!("CARGO_BIN_EXE_backfill-all"))
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"))
            .args([
                "--tickers",
                "BTC-USDT",
                "--exchanges",
                "binance,bitget",
                "--skip-probe",
                "--min-free-gib",
                "0",
            ])
            .args(["--steps", steps])
            .args(window)
            .args(["--out-dir", data.to_str().unwrap()])
            .args([
                "--log-file",
                self.root.join("report.json").to_str().unwrap(),
            ])
            .env("PATH", path)
            .env("NXR_DATA_ROOT", &data)
            .env(
                "NXR_CONFIG",
                concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"),
            )
            .output()
            .unwrap()
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn data(&self, rel: &str) -> PathBuf {
        self.root.join("data").join(rel)
    }

    fn report(&self) -> String {
        fs::read_to_string(self.root.join("report.json")).unwrap_or_default()
    }
}

const STEPS: &str = "fetch,t2i,merge";
const OLD: [&str; 4] = ["--from", "2026-08-10", "--to", "2026-08-11"];

fn states(binance: &str, bitget: &str, days: &[&str]) -> String {
    let row = |ex: &str, st: &str| {
        let d: Vec<String> = days.iter().map(|d| format!("\"{d}\":{st}")).collect();
        format!(
            "{{\"ticker\":\"BTC\",\"exchange\":\"{ex}\",\"days\":{{{}}}}}",
            d.join(",")
        )
    };
    format!("[{},{}]", row("binance", binance), row("bitget", bitget))
}

/// Sources go only after every merge of the run succeeded: raw files and
/// per-provider idx are gone after success and survive a failed merge.
#[test]
fn sources_are_disposed_after_a_successful_merge_and_kept_after_a_failed_one() {
    let st = states("\"got\"", "\"got\"", &["2026-08-10", "2026-08-11"]);
    let ok = Rig::new("dispose-ok", &st, 0, 0);
    ok.run(&OLD, STEPS);
    assert!(
        !ok.data("ticks/binance/BTCUSDT/a.ticks").exists(),
        "raw kept after success"
    );
    assert!(
        !ok.data("indexes/binance/BTC-USDT.idx").exists(),
        "idx kept after success"
    );

    let bad = Rig::new("dispose-bad", &st, 0, 1);
    bad.run(&OLD, STEPS);
    assert!(bad.calls().contains("merge-idx"));
    assert!(
        bad.data("ticks/binance/BTCUSDT/a.ticks").exists(),
        "raw deleted after failed merge"
    );
    assert!(
        bad.data("indexes/binance/BTC-USDT.idx").exists(),
        "idx deleted after failed merge"
    );

    // not asked to merge: nothing may be deleted
    let none = Rig::new("dispose-none", &st, 0, 0);
    none.run(&OLD, "fetch,t2i");
    assert!(none.data("ticks/binance/BTCUSDT/a.ticks").exists());
    assert!(none.data("indexes/binance/BTC-USDT.idx").exists());
}

/// An old day a venue lacks is finalised without it, and the venues that built
/// it are recorded.
#[test]
fn old_days_are_finalised_without_a_missing_venue_and_recorded() {
    let st = states("\"got\"", "\"missing\"", &["2026-08-10", "2026-08-11"]);
    let r = Rig::new("old", &st, 0, 0);
    let out = r.run(&OLD, STEPS);
    assert!(out.status.success());
    assert!(r.calls().contains("merge-idx"), "{}", r.calls());
    let tid = nxr_sdk::resolve_ticker_id("BTC/USDT");
    let venues = fs::read_to_string(r.data(&format!("indexes/{tid}/venues.json"))).unwrap();
    assert!(
        venues.contains("2026-08-10") && venues.contains("binance") && !venues.contains("bitget"),
        "{venues}"
    );
    assert!(r.report().contains("venue-coverage"), "{}", r.report());
}

/// A day inside the publish-lag window is held, not merged, and holding is not a failure.
#[test]
fn recent_days_are_held_while_a_venue_is_missing() {
    let today = chrono::Utc::now().date_naive();
    let (a, b) = (
        today - chrono::Duration::days(3),
        today - chrono::Duration::days(2),
    );
    let st = states("\"got\"", "\"missing\"", &[&a.to_string(), &b.to_string()]);
    let r = Rig::new("held", &st, 0, 0);
    let out = r.run(&["--from", &a.to_string(), "--to", &b.to_string()], STEPS);
    assert!(out.status.success(), "held days must not fail the run");
    assert!(!r.calls().contains("merge-idx"), "{}", r.calls());
    assert!(
        r.report().contains("held-days") && r.report().contains("not published yet"),
        "{}",
        r.report()
    );
}

/// A transport failure holds only its day: the days around it are still built.
#[test]
fn a_failed_day_is_split_out_of_the_run() {
    let st = format!(
        "[{}]",
        "{\"ticker\":\"BTC\",\"exchange\":\"binance\",\"days\":{\"2026-08-10\":\"got\",\"2026-08-11\":{\"failed\":\"boom\"},\"2026-08-12\":\"got\"}}"
    );
    let r = Rig::new("split", &st, 0, 0);
    r.run(&["--from", "2026-08-10", "--to", "2026-08-12"], STEPS);
    let calls = r.calls();
    assert!(
        calls.contains("--from-date 2026-08-10 --to-date 2026-08-10"),
        "{calls}"
    );
    assert!(
        calls.contains("--from-date 2026-08-12 --to-date 2026-08-12"),
        "{calls}"
    );
    assert!(!calls.contains("--to-date 2026-08-11"), "{calls}");
}

/// A fetch that errors (a venue could not be asked) skips the run's merge and keeps its sources.
#[test]
fn fetch_error_skips_merge_and_keeps_sources() {
    let r = Rig::new("fault", "[]", 1, 0);
    r.run(&OLD, STEPS);
    let calls = r.calls();
    assert!(
        !calls.contains("merge-idx") && !calls.contains("ticks-to-idx"),
        "{calls}"
    );
    assert!(
        r.data("ticks/binance/BTCUSDT/a.ticks").exists(),
        "raw was deleted"
    );
}

/// A ticker another process is running is skipped (exit 0) and its marker untouched.
#[test]
fn a_locked_ticker_is_skipped_and_its_marker_left_alone() {
    use std::os::unix::io::AsRawFd;
    let r = Rig::new("lock", "[]", 0, 0);
    let dir = r.data("backfill");
    fs::create_dir_all(dir.join("progress")).unwrap();
    let lock = fs::File::create(dir.join("BTC-USDT.lock")).unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let marker = dir.join("progress/BTC-USDT.start");
    fs::write(&marker, "other process").unwrap();
    let out = r.run(&OLD, STEPS);
    assert!(out.status.success());
    assert!(!r.calls().contains("fetch-crypto-history"), "{}", r.calls());
    assert_eq!(fs::read_to_string(&marker).unwrap(), "other process");
    assert!(!dir.join("progress/BTC-USDT.failed").exists());
    assert!(
        r.report().contains("locked by another backfill"),
        "{}",
        r.report()
    );
}

/// The window never reaches today: its shard belongs to the live writer.
#[test]
fn window_stops_at_yesterday() {
    let r = Rig::new("last", "[]", 0, 0);
    let today = chrono::Utc::now().date_naive();
    let from = (today - chrono::Duration::days(5)).to_string();
    r.run(&["--from", &from, "--to", "today"], "fetch");
    let want = format!("--to-date {}", today - chrono::Duration::days(1));
    assert!(r.calls().contains(&want), "want {want} in {}", r.calls());
}

/// Only the validator's error verdict (exit 2) deletes and unlists a composite
/// shard; a validator that is killed leaves good data alone (and fails the step).
#[test]
fn validation_deletes_only_on_the_validators_error_verdict() {
    let st = states("\"got\"", "\"got\"", &["2026-08-10", "2026-08-11"]);
    let tid = nxr_sdk::resolve_ticker_id("BTC/USDT");
    let shard = |r: &Rig| r.data(&format!("indexes/{tid}/2026-08-10.idx"));

    let bad = Rig::new("val-bad", &st, 0, 0);
    bad.validator("exit 2");
    bad.run(&OLD, STEPS);
    assert!(
        !shard(&bad).exists(),
        "a shard the validator condemned was kept"
    );

    let killed = Rig::new("val-killed", &st, 0, 0);
    killed.validator("kill -9 $$");
    killed.run(&OLD, STEPS);
    assert!(shard(&killed).exists(), "a tool failure deleted good data");
    assert!(killed.report().contains("\"failed\""), "the step must fail");
}
