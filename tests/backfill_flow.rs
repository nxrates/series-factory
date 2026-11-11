//! `backfill-all` orchestration with fake stage binaries on PATH: when sources are
//! deleted, which days are held or finalised, what is recorded, and what a lock
//! or a fetch error does.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

struct Rig {
    exchanges: std::cell::RefCell<String>,
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
        fs::write(root.join("probe.json"), "[]").unwrap();
        fs::write(root.join("probe_recent.json"), "[]").unwrap();
        script(
            "fetch-crypto-history",
            &format!(
                "case \"$*\" in *--probe*) F=$(echo \"$*\" | sed 's/.*--from-date \\([0-9-]*\\).*/\\1/'); if [ -f {e} ] && [ $(expr \"$F\" \\> \"$(cat {e})\") = 1 ]; then cat {pr}; else cat {p}; fi; exit 0;; esac\nmkdir -p {d}/ticks/binance/BTCUSDT && : > {d}/ticks/binance/BTCUSDT/a.ticks\ncat {s}\nexit {fetch_exit}",
                d = data.display(),
                s = root.join("states.json").display(),
                p = root.join("probe.json").display(),
                pr = root.join("probe_recent.json").display(),
                e = root.join("edge.txt").display()
            ),
        );
        script(
            "ticks-to-idx",
            &format!(
                "mkdir -p {d}/indexes/binance && : > {d}/indexes/binance/BTC-USDT.idx",
                d = data.display()
            ),
        );
        // merge: `--from-date A --to-date B` -> one 56-byte shard per day, plus the
        // manifest. The day list comes from a file (no date arithmetic in the shell).
        let days: Vec<String> = chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .iter_days()
            .take(4000)
            .map(|d| d.to_string())
            .collect();
        fs::write(root.join("days.txt"), days.join("\n")).unwrap();
        script(
            "merge-idx",
            &format!(
                r#"[ {merge_exit} -ne 0 ] && exit {merge_exit}
while [ $# -gt 0 ]; do case $1 in --from-date) A=$2;; --to-date) B=$2;; esac; shift; done
mkdir -p {c}; E=""
for d in $(awk -v a=$A -v b=$B '$0>=a && $0<=b' {days}); do head -c 56 /dev/zero > {c}/$d.idx; E="$E{{\"date\":\"$d\",\"first_ts\":0,\"last_ts\":0,\"n_records\":1,\"size_bytes\":56,\"sha256\":\"\"}},"; done
echo "{{\"ticker\":\"T\",\"ticker_id\":1,\"kind\":\"idx\",\"shards\":[${{E%,}}],\"first_ts\":0,\"last_ts\":0,\"last_updated\":0,\"schema_version\":1}}" > {c}/manifest.json"#,
                c = comp.display(),
                days = root.join("days.txt").display()
            ),
        );
        script("integrity-check", "exit 0");
        Self {
            exchanges: std::cell::RefCell::new("binance,bitget".into()),
            root,
            bin,
            log,
        }
    }

    /// Replace the fake validator's behaviour (a shell snippet).
    fn validator(&self, body: &str) {
        let p = self.bin.join("integrity-check");
        fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
    }

    fn run(&self, window: &[&str], steps: &str) -> std::process::Output {
        self.run_with(window, steps, &["--skip-probe"])
    }

    fn run_with_probe_rebuild(&self, from: &str, to: &str, probe: &str) -> std::process::Output {
        fs::write(self.root.join("probe.json"), probe).unwrap();
        self.run_with(&["--from", from, "--to", to], STEPS, &["--rebuild"])
    }

    fn run_with(&self, window: &[&str], steps: &str, extra: &[&str]) -> std::process::Output {
        let data = self.root.join("data");
        let path = format!("{}:{}", self.bin.display(), std::env::var("PATH").unwrap());
        Command::new(env!("CARGO_BIN_EXE_backfill-all"))
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yml"))
            .args([
                "--tickers",
                "BTC-USDT",
                "--exchanges",
                &self.exchanges.borrow(),
                "--min-free-gib",
                "0",
            ])
            .args(extra)
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
    let out = r.run_with(
        &["--from", &a.to_string(), "--to", &b.to_string()],
        STEPS,
        &["--skip-probe", "--rebuild"],
    );
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
    let row = |ex: &str, d11: &str| {
        format!("{{\"ticker\":\"BTC\",\"exchange\":\"{ex}\",\"days\":{{\"2026-08-10\":\"got\",\"2026-08-11\":{d11},\"2026-08-12\":\"got\"}}}}")
    };
    let st = format!(
        "[{},{}]",
        row("binance", "{\"failed\":\"boom\"}"),
        row("bitget", "\"got\"")
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
    bad.validator("echo '2026-11-11T00:00:00Z  INFO a log line before the report'; echo '{\"errors\":[\"bad record\"],\"warnings\":[]}'; exit 2");
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

/// States JSON for binance and bitget over `[from, to]`: binance has every day,
/// bitget only those at or before `bitget_until`.
fn ranged_states(
    from: chrono::NaiveDate,
    to: chrono::NaiveDate,
    bitget_until: chrono::NaiveDate,
) -> String {
    let days: Vec<chrono::NaiveDate> = from.iter_days().take_while(|d| *d <= to).collect();
    let row = |ex: &str, f: &dyn Fn(&chrono::NaiveDate) -> &'static str| {
        let d: Vec<String> = days.iter().map(|d| format!("\"{d}\":{}", f(d))).collect();
        format!(
            "{{\"ticker\":\"BTC\",\"exchange\":\"{ex}\",\"days\":{{{}}}}}",
            d.join(",")
        )
    };
    format!(
        "[{},{}]",
        row("binance", &|_| "\"got\""),
        row("bitget", &|d| if *d <= bitget_until {
            "\"got\""
        } else {
            "\"missing\""
        })
    )
}

const PROBE_NO_BITGET: &str = r#"[{"ticker":"BTC","exchange":"binance","has_data":true,"first_date":null,"last_date":null},{"ticker":"BTC","exchange":"bitget","has_data":false,"first_date":null,"last_date":null}]"#;

/// With the probe on, a run of only recent days never drops a venue: it has not
/// published them yet. The days are held, the probe is not even asked, and nothing
/// is re-sourced from Pyth.
#[test]
fn probe_cannot_drop_a_venue_on_recent_days() {
    let today = chrono::Utc::now().date_naive();
    let (a, b) = (
        today - chrono::Duration::days(3),
        today - chrono::Duration::days(2),
    );
    let st = states("\"got\"", "\"missing\"", &[&a.to_string(), &b.to_string()]);
    let r = Rig::new("probe-recent", &st, 0, 0);
    // --rebuild: exactly the two recent days, no widening
    let out = r.run_with_probe_rebuild(&a.to_string(), &b.to_string(), PROBE_NO_BITGET);
    assert!(out.status.success());
    let calls = r.calls();
    assert!(
        !calls.contains("--probe"),
        "probe judged recent days: {calls}"
    );
    assert!(!calls.contains("pyth"), "{calls}");
    assert!(
        !calls.contains("merge-idx"),
        "finalised without bitget: {calls}"
    );
    assert!(r.report().contains("held-days"), "{}", r.report());
}

/// A mixed run with a venue the probe finds nothing on: only the published days
/// are probed, the venue is dropped on them, so the recent days are NOT held on
/// its account; everything finalises and `venues.json` records it without the venue.
#[test]
fn mixed_run_drops_a_dead_venue_and_holds_nothing_for_it() {
    let today = chrono::Utc::now().date_naive();
    let edge = today - chrono::Duration::days(5);
    let (from, to) = (
        edge - chrono::Duration::days(3),
        today - chrono::Duration::days(2),
    );
    let st = ranged_states(from, to, from - chrono::Duration::days(1)); // bitget has nothing
    let r = Rig::new("probe-mixed", &st, 0, 0);
    r.run_with_probe_rebuild(&from.to_string(), &to.to_string(), PROBE_NO_BITGET);
    let calls = r.calls();
    assert!(
        calls.contains(&format!("--to-date {edge} --probe")),
        "probe window: {calls}"
    );
    let merge = calls
        .lines()
        .find(|l| l.starts_with("merge-idx"))
        .expect("merged");
    assert!(
        merge.contains(&format!("--from-date {from} --to-date {to}")),
        "{merge}"
    );
    assert!(
        !r.report().contains("held-days"),
        "held on a dropped venue's account: {}",
        r.report()
    );
    let tid = nxr_sdk::resolve_ticker_id("BTC/USDT");
    let venues = fs::read_to_string(r.data(&format!("indexes/{tid}/venues.json"))).unwrap();
    for d in [from, edge, to] {
        assert!(
            venues.contains(&d.to_string()),
            "{d} not recorded: {venues}"
        );
    }
    assert!(
        venues.contains("binance") && !venues.contains("bitget"),
        "{venues}"
    );
}

/// A listing a few days old looks, on the published days, like a symbol nobody has.
/// The recent days tell them apart: the venues that have them stay, no Pyth, no skip.
#[test]
fn a_new_listing_is_not_routed_to_pyth() {
    let today = chrono::Utc::now().date_naive();
    let edge = today - chrono::Duration::days(5);
    let (from, to) = (
        edge - chrono::Duration::days(3),
        today - chrono::Duration::days(2),
    );
    let st = ranged_states(from, to, from - chrono::Duration::days(1));
    let r = Rig::new("new-listing", &st, 0, 0);
    fs::write(r.root.join("edge.txt"), edge.to_string()).unwrap();
    fs::write(r.root.join("probe_recent.json"), PROBE_NO_BITGET).unwrap();
    r.run_with_probe_rebuild(&from.to_string(), &to.to_string(), "[{\"ticker\":\"BTC\",\"exchange\":\"binance\",\"has_data\":false,\"first_date\":null,\"last_date\":null},{\"ticker\":\"BTC\",\"exchange\":\"bitget\",\"has_data\":false,\"first_date\":null,\"last_date\":null}]");
    let calls = r.calls();
    assert!(!calls.contains("pyth"), "routed to pyth: {calls}");
    assert!(calls.contains("merge-idx"), "skipped: {calls}");
    assert!(r.report().contains("\"status\": \"ok\""), "{}", r.report());
    // the recent probe only says the series exists: every requested venue stays, so
    // bitget (Missing on the recent days) holds them
    let fetch = calls
        .lines()
        .find(|l| l.starts_with("fetch-crypto-history") && !l.contains("--probe"))
        .unwrap();
    assert!(fetch.contains("--exchanges binance,bitget "), "{fetch}");
    assert!(r.report().contains("held-days"), "{}", r.report());
}

/// A venue with no archive for the market is dropped first and reported once, not a fault.
#[test]
fn an_unsupported_venue_is_reported_once_and_does_not_fail_the_run() {
    let today = chrono::Utc::now().date_naive();
    let (a, b) = (
        today - chrono::Duration::days(3),
        today - chrono::Duration::days(2),
    );
    let st = states("\"got\"", "\"got\"", &[&a.to_string(), &b.to_string()]);
    let r = Rig::new("unsupported", &st, 0, 0);
    *r.exchanges.borrow_mut() = "binance,bitget,gateio".into();
    let out = r.run_with(
        &["--from", &a.to_string(), "--to", &b.to_string()],
        STEPS,
        &["--rebuild"],
    );
    assert!(out.status.success());
    let fetch = r
        .calls()
        .lines()
        .find(|l| l.starts_with("fetch-crypto-history"))
        .unwrap()
        .to_string();
    assert!(
        fetch.contains("--exchanges binance,bitget ")
            || fetch.contains("--exchanges binance,bitget\n"),
        "{fetch}"
    );
    let rep = r.report();
    assert_eq!(
        rep.matches("gateio: no archive for BTC/USDT").count(),
        1,
        "{rep}"
    );
    assert!(!rep.contains("venue-faults"), "{rep}");
}

/// A report that covers one venue fully and another partially: the missing
/// venue-day is failed, not fine.
#[test]
fn a_partial_fetch_report_fails_the_missing_venue_day() {
    let full = "{\"ticker\":\"BTC\",\"exchange\":\"binance\",\"days\":{\"2026-08-10\":\"got\",\"2026-08-11\":\"got\"}}";
    let part = "{\"ticker\":\"BTC\",\"exchange\":\"bitget\",\"days\":{\"2026-08-10\":\"got\"}}";
    let r = Rig::new("partial", &format!("[{full},{part}]"), 0, 0);
    r.run(&OLD, STEPS);
    let merge = r
        .calls()
        .lines()
        .find(|l| l.starts_with("merge-idx"))
        .unwrap()
        .to_string();
    assert!(
        merge.contains("--from-date 2026-08-10 --to-date 2026-08-10"),
        "{merge}"
    );
    assert!(
        r.report().contains("2026-08-11") && r.report().contains("venue-faults"),
        "{}",
        r.report()
    );
}

/// A fetch that prints no usable state report must not pass every venue.
#[test]
fn an_empty_or_cut_off_fetch_report_fails_open_no_more() {
    for (tag, report) in [
        ("empty", ""),
        (
            "cut",
            "[{\"ticker\":\"BTC\",\"exchange\":\"binance\",\"days\":{\"2026-08-1",
        ),
    ] {
        let r = Rig::new(&format!("report-{tag}"), report, 0, 0);
        r.run(&OLD, STEPS);
        assert!(
            !r.calls().contains("merge-idx"),
            "{tag}: merged without a report: {}",
            r.calls()
        );
        assert!(r.report().contains("venue-faults"), "{tag}: {}", r.report());
    }
}

/// A window ending inside the lag is widened; `--rebuild` is exact.
#[test]
fn short_window_is_widened_unless_rebuilding() {
    let today = chrono::Utc::now().date_naive();
    let from = (today - chrono::Duration::days(7)).to_string();
    let r = Rig::new("widen", "[]", 0, 0);
    r.run(&["--from", &from, "--to", "today"], "fetch");
    let want = format!("--from-date {}", today - chrono::Duration::days(19));
    assert!(r.calls().contains(&want), "{want} in {}", r.calls());
    let r = Rig::new("exact", "[]", 0, 0);
    r.run_with(
        &["--from", &from, "--to", "today"],
        "fetch",
        &["--skip-probe", "--rebuild"],
    );
    assert!(
        r.calls().contains(&format!("--from-date {from}")),
        "{}",
        r.calls()
    );
}

/// No requested venue can serve the series: skipped with a reason, fetch never called.
#[test]
fn zero_supported_venues_skips_the_ticker() {
    let today = chrono::Utc::now().date_naive();
    let (a, b) = (
        today - chrono::Duration::days(3),
        today - chrono::Duration::days(2),
    );
    let r = Rig::new("zero-venues", "[]", 0, 0);
    *r.exchanges.borrow_mut() = "gateio".into();
    let out = r.run_with(
        &["--from", &a.to_string(), "--to", &b.to_string()],
        STEPS,
        &["--rebuild"],
    );
    assert!(out.status.success());
    assert!(!r.calls().contains("fetch-crypto-history"), "{}", r.calls());
    assert!(
        r.report().contains("\"status\": \"skipped\"") && r.report().contains("no requested venue"),
        "{}",
        r.report()
    );
}

/// A shard the merge wrote that does not match its manifest entry is removed and
/// reported, so a short shard cannot feed s10; the day stays todo.
#[test]
fn a_shard_not_matching_its_manifest_is_removed_and_reported() {
    let st = states("\"got\"", "\"got\"", &["2026-08-10", "2026-08-11"]);
    let r = Rig::new("short-shard", &st, 0, 0);
    let merge = r.bin.join("merge-idx");
    let tid = nxr_sdk::resolve_ticker_id("BTC/USDT");
    let comp = r.data(&format!("indexes/{tid}"));
    let mut script = fs::read_to_string(&merge).unwrap();
    script.push_str(&format!(
        "\nfor f in {}/*.idx; do head -c 30 $f > $f.t; mv $f.t $f; done\n",
        comp.display()
    ));
    fs::write(&merge, script).unwrap();
    r.run(&OLD, STEPS);
    assert!(
        !comp.join("2026-08-10.idx").exists(),
        "short shard left for s10 to read"
    );
    assert!(r.report().contains("days-not-built"), "{}", r.report());
}
