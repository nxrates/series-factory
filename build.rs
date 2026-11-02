fn main() {
    let git = |a: &[&str]| {
        std::process::Command::new("git")
            .args(a)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };
    let sha = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(&["status", "--porcelain"])
        .map_or("unknown".to_string(), |s| (!s.is_empty()).to_string());
    println!("cargo:rustc-env=SF_GIT_SHA={sha}");
    println!("cargo:rustc-env=SF_GIT_DIRTY={dirty}");
    for p in ["build.rs", "Cargo.toml", "src"] {
        println!("cargo:rerun-if-changed={p}");
    }
    if let Some(d) = git(&["rev-parse", "--git-dir"]) {
        for f in ["HEAD", "index"] {
            println!("cargo:rerun-if-changed={d}/{f}");
        }
    }
}
