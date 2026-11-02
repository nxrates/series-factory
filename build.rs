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
    let dirty = git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty());
    println!("cargo:rustc-env=SF_GIT_SHA={sha}");
    println!("cargo:rustc-env=SF_GIT_DIRTY={dirty}");
}
