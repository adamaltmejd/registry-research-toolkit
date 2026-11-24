//! The size guard (#2651): a run estimated above 2 GiB is refused before any row is
//! generated unless `--allow-large`, and `compile` warns of a source whose rows per
//! person across its years pass 50. A regression that drops the estimate lets a
//! 10 000-person run write hundreds of MB unannounced.

use std::path::Path;

use reg_mock::generate;
use reg_mock::size::row_warnings;
use reg_mock::spec::Spec;

fn spec(rows_per_unit: f64) -> Spec {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/every_type.spec.json");
    let mut spec: Spec =
        serde_json::from_slice(&std::fs::read(fixture).expect("fixture")).expect("a spec");
    // Thirty years of the persons file, as a table with no year column delivers.
    spec.format = reg_core::project::ExtractFormat::Csv;
    spec.files[0]
        .columns
        .retain(|c| c.attribute != Some(reg_mock::spec::Attribute::Year));
    spec.files[0].years = (1995..2025).collect();
    spec.files[0].rows_per_unit = rows_per_unit;
    spec
}

#[test]
fn an_estimate_above_the_cap_is_refused_without_allow_large() {
    let spec = spec(300.0);
    let refusal = generate::plan(&spec, 1, 3000, false)
        .err()
        .expect("refused");
    assert_eq!(refusal.code, "size_too_large");
    assert!(generate::plan(&spec, 1, 3000, true).is_ok());
}

#[test]
fn rows_per_person_above_fifty_warn_with_the_cause() {
    let warnings = row_warnings(&spec(3.0));
    assert_eq!(warnings.len(), 1, "only the persons file: {warnings:?}");
    assert_eq!(warnings[0].code, "rows_per_person");
    assert!(warnings[0].message.contains("no year column"));
    assert_eq!(row_warnings(&spec(1.0)), vec![]);
}
