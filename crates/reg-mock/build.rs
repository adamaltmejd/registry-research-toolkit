//! `reg-mock` ships in the `reg_meta` release, whose version lives only in
//! `crates/reg-meta/Cargo.toml` (the release skill bumps that line alone). Read it from
//! there so `reg-mock --version` names the release it was built in.

fn main() {
    let manifest = "../reg-meta/Cargo.toml";
    println!("cargo:rerun-if-changed={manifest}");
    let text = std::fs::read_to_string(manifest).expect("crates/reg-meta/Cargo.toml is readable");
    let version = text
        .lines()
        .find_map(|l| l.strip_prefix("version = \""))
        .and_then(|l| l.strip_suffix('"'))
        .expect("crates/reg-meta/Cargo.toml has a `version = \"…\"` line");
    println!("cargo:rustc-env=REG_META_VERSION={version}");
}
