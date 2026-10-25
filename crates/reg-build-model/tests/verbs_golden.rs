//! The neutral verb schema against the golden corpus (`tests/verbs/README.md`): one real
//! entry of today's tree per old verb (`before.toml`) and its neutral form
//! (`after.toml`), at the verb's hardest case.
//!
//! It fails if an `after.toml` names a field the schema lacks or misspells one
//! (`deny_unknown_fields`), if an old verb has no pair, or if a converted entry's legacy
//! case id is not the case id its `before.toml` names, which would break the case ids the
//! artifact stores.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use reg_build_model::verbs::Verbs;

/// Every verb table of today's tree that the port converts.
const OLD_VERBS: [&str; 33] = [
    "identity.partition",
    "identity.unassigned",
    "identity.column_owner",
    "identity.split",
    "identity.rename",
    "identity.edition_split",
    "identity.route",
    "variant.name",
    "errata.field",
    "errata.data_type",
    "errata.classification_reference",
    "errata.flags",
    "errata.occurrence_period.scb",
    "errata.occurrence_period.sos",
    "errata.occurrence_carry",
    "errata.delivered",
    "errata.version",
    "errata.column.scb",
    "errata.column.sos",
    "errata.support",
    "representation.parallel",
    "representation.alias_window",
    "representation.period_family",
    "representation.delivery_metadata",
    "representation.matrix",
    "coding.warning",
    "coding.choice",
    "coding.uncoded",
    "coding.omit",
    "coding.extend",
    "coding.documented",
    "coding.sentinel",
    "coding.support",
];

/// The case id a `before.toml` names on its first line, `# case: <id>`.
fn case_id(before: &str) -> &str {
    before
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("# case: "))
        .expect("before.toml starts with `# case: <id>`")
        .trim()
}

/// Every legacy case id in a parsed `after.toml`.
fn legacy_ids(verbs: &Verbs) -> Vec<&str> {
    let mut ids = Vec::new();
    ids.extend(
        verbs
            .identity
            .assign
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        verbs
            .table
            .assign
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    let errata = &verbs.errata;
    ids.extend(
        errata
            .field
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        errata
            .period
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        errata
            .delivered
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        errata
            .delivery
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        errata
            .column
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        errata
            .support
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    let representation = &verbs.representation;
    ids.extend(
        representation
            .parallel
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        representation
            .period_family
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        representation
            .delivery_metadata
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    ids.extend(
        representation
            .matrix
            .iter()
            .map(|entry| entry.legacy.case_id.as_str()),
    );
    let coding = &verbs.coding;
    for entries in [
        &coding.warning,
        &coding.choice,
        &coding.uncoded,
        &coding.omit,
        &coding.extend,
        &coding.documented,
        &coding.sentinel,
        &coding.support,
    ] {
        ids.extend(entries.iter().map(|entry| entry.legacy.case_id.as_str()));
    }
    ids
}

#[test]
fn every_old_verb_has_a_pair_the_schema_reads() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/verbs");
    let pairs: BTreeSet<String> = fs::read_dir(&root)
        .expect("corpus")
        .map(|entry| entry.expect("corpus entry"))
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().into_string().expect("utf-8 name"))
        .collect();
    let expected: BTreeSet<String> = OLD_VERBS.iter().map(|verb| (*verb).to_owned()).collect();
    assert_eq!(pairs, expected, "one pair per old verb");
    for pair in &pairs {
        let dir = root.join(pair);
        let before = fs::read_to_string(dir.join("before.toml")).expect("before.toml");
        let after = fs::read_to_string(dir.join("after.toml")).expect("after.toml");
        toml::from_str::<toml::Table>(&before).expect("before.toml is TOML");
        let verbs: Verbs =
            toml::from_str(&after).unwrap_or_else(|err| panic!("{pair}/after.toml: {err}"));
        // `variant.name` becomes `errata.table`, which carries no legacy case id.
        if pair == "variant.name" {
            assert_eq!(
                verbs.errata.table.len(),
                1,
                "{pair}: one errata.table entry"
            );
            continue;
        }
        assert_eq!(
            legacy_ids(&verbs),
            vec![case_id(&before)],
            "{pair}: one converted entry, carrying the old case id"
        );
    }
}
