//! Every direct HTTP client in this repo is allowlisted (`egress-allowlist.txt`); a public source
//! goes through btr-rpc (Http/RpcPool/identity) instead.

#[test]
fn every_direct_client_is_allowlisted() {
    let bad = btr_rpc::guard::check(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")),
        include_str!("../egress-allowlist.txt"),
    );
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}
