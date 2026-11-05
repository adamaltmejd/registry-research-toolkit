//! Determinism is a tested property (AGENTS.md, "Testing policy"): the same spec, seed
//! and size give byte-identical files in every format. A regression that iterates a
//! hash map, stamps a time (the `.dta` header, Parquet's `created_by`) or shares a
//! random stream between files fails here.

use std::path::Path;

use reg_core::project::ExtractFormat;
use reg_mock::spec::Spec;
use reg_mock::{generate, write};

fn run(spec: &Spec, dir: &Path) -> Vec<(String, Vec<u8>)> {
    let _ = std::fs::remove_dir_all(dir);
    let tables = generate::generate(spec, 7, 500, false).expect("generates");
    write::write_all(spec, &tables, dir, 7, 500).expect("writes");
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("written")
        .map(|e| {
            let path = e.expect("entry").path();
            let name = path
                .file_name()
                .expect("a file")
                .to_string_lossy()
                .into_owned();
            (name, std::fs::read(&path).expect("readable"))
        })
        .collect();
    files.sort();
    files
}

#[test]
fn same_spec_and_seed_give_identical_bytes() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/every_type.spec.json");
    let mut spec: Spec =
        serde_json::from_slice(&std::fs::read(fixture).expect("fixture")).expect("a spec");
    let tmp = Path::new(env!("CARGO_TARGET_TMPDIR")).join("reg-mock-determinism");
    for format in [
        ExtractFormat::Dta,
        ExtractFormat::Parquet,
        ExtractFormat::Csv,
    ] {
        spec.format = format;
        let first = run(&spec, &tmp.join(format!("{format:?}-1")));
        let second = run(&spec, &tmp.join(format!("{format:?}-2")));
        assert_eq!(first.len(), 3, "{format:?}: two files and the manifest");
        assert!(first == second, "{format:?}: a rerun changed the bytes");
    }
}
