//! Published ids across catalog releases (`cases/release-ids`): `reg-build verify
//! --previous` and `reg-build diff` on an overlapping natural target.

use std::fs;
use std::path::Path;

use reg_conformance::{Scratch, cases, copy_tree};
use serde_json::Value;

use crate::common::cli::{Ran, curated, reg_build};
use crate::common::read_json;
use crate::common::steward::{edit, read, write};

use super::lock::membership_case;

/// `reg-build build` of `inputs` and `curation` into `out` as `version`.
fn build(inputs: &Path, curation: &Path, out: &Path, version: &str) {
    curated(
        "build",
        inputs,
        curation,
        &[
            "--out".as_ref(),
            out.as_os_str(),
            "--catalog-version".as_ref(),
            version.as_ref(),
        ],
    )
    .ok();
}

fn verify_previous(current: &Path, previous: &Path) -> Ran {
    reg_build([
        "verify".as_ref(),
        current.as_os_str(),
        "--previous".as_ref(),
        previous.as_os_str(),
    ])
}

#[test]
fn published_id_targets() {
    // Fails if surviving pids conceal disjoint tables, spellings or periods, if exact
    // interval equality replaces overlap, or if register/implicit-table renames count
    // as table moves. Growth adds targets beside partial overlap. The golden follows
    // the source/ledgers; periods are half-open Unix days derived from source dates.
    // Previous 1.0.0 with a dev candidate pins strictness: keying on the candidate or
    // treating every previous release as pre-v1 would allow these moved ids.
    let fixture = cases().join("release-ids");
    for change in ["variable", "table", "grown"] {
        let expected = &read_json(&fixture.join("expected.json")).unwrap()[change];
        let expected_text = read_json(&fixture.join("expected-text.json")).unwrap()[change]
            .as_array()
            .unwrap()
            .iter()
            .map(|line| line.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let dir = Scratch::new("release-ids").unwrap();
        let curation = dir.path().join("curation");
        copy_tree(&fixture.join("curation"), &curation).unwrap();
        if change != "grown" {
            fs::copy(
                fixture.join(format!("{change}-ids.toml")),
                curation.join("registers/demo/r/ids.toml"),
            )
            .unwrap();
        }
        if change == "variable" {
            fs::copy(
                fixture.join("variable-implicit-ids.toml"),
                curation.join("registers/demo/implicit/ids.toml"),
            )
            .unwrap();
        }
        let previous = dir.path().join("previous.db");
        let build_previous = |version| {
            build(
                &fixture.join("inputs"),
                &fixture.join("curation"),
                &previous,
                version,
            );
        };
        build_previous("1.0.0");
        let current = dir.path().join("current.db");
        let inputs = fixture.join(if change == "grown" { "grown" } else { "inputs" });
        build(&inputs, &curation, &current, "dev");
        let moved = expected.as_array().is_some_and(|ids| !ids.is_empty());
        let verified = verify_previous(&current, &previous);
        if moved {
            verified.refused(&[
                &format!("previous release: moved id {};", expected_text[0]),
                "retire",
                "mint",
            ]);
        } else {
            verified.ok();
        }
        let args = |json: bool| {
            let mut args = vec!["diff".as_ref(), previous.as_os_str(), current.as_os_str()];
            if json {
                args.push("--json".as_ref());
            }
            reg_build(args)
        };
        // Exit 1: the two catalogs differ.
        let diff = args(true);
        assert_eq!(diff.code, Some(1), "{}", diff.stderr);
        let document: Value = serde_json::from_str(&diff.stdout).unwrap();
        assert_eq!(&document["ids"]["moved"], expected, "{change}");
        if change == "variable" {
            // Previous 0.x pins leniency even for a dev candidate: applying the v1
            // moved refusal here, or reading the candidate version, breaks this
            // report golden.
            build_previous("0.68.0");
            let reported = verify_previous(&current, &previous).ok();
            assert_eq!(
                reported.stdout,
                read(&fixture.join("pre-v1-moved-expected.txt")).unwrap()
            );
        }
    }
}

#[test]
fn removed_membership_id_requires_retirement() {
    // Regression: verify compares ident only, allowing a removed var_set pid to
    // disappear without retirement. Both retired and unretired twins use real builds.
    // Previous 1.0.0 with a dev candidate pins refusal for ids published after the
    // freeze; previous 0.x pins leniency for the same removal.
    let dir = Scratch::new("membership-retirement").unwrap();
    let (fixture, _, curation, _) = membership_case(dir.path());
    let inputs = fixture.join("inputs");
    let locked_build = |out: &Path, version: &str| {
        curated("lock", &inputs, &curation, &[]).ok();
        build(&inputs, &curation, out, version);
    };
    let previous = dir.path().join("previous.db");
    let current = dir.path().join("current.db");
    locked_build(&previous, "1.0.0");
    let statements = curation.join("registers/demo/r/curation.toml");
    let original = read(&statements).unwrap();
    edit(&statements, "sets = [\"concept\"]", "sets = []", true).unwrap();
    let removed = read(&statements).unwrap();
    locked_build(&current, "dev");
    let pid = "set/demo/r/concept";
    let refused = verify_previous(&current, &previous).refused(&[]);
    assert_eq!(
        refused.stderr,
        format!("reg-build: previous release: removed id {pid}\n")
    );
    let retired = curation.join("retired.toml");
    write(
        &retired,
        &format!("\"{pid}\" = {{ kind = \"set\", in = \"0.68.0\", reason = \"withdrawn\" }}\n"),
    )
    .unwrap();
    locked_build(&current, "dev");
    verify_previous(&current, &previous).ok();

    fs::remove_file(&retired).unwrap();
    write(&statements, &original).unwrap();
    locked_build(&previous, "0.68.0");
    write(&statements, &removed).unwrap();
    locked_build(&current, "dev");
    let reported = verify_previous(&current, &previous).ok();
    assert_eq!(
        reported.stdout,
        read(&fixture.join("pre-v1-expected.txt")).unwrap()
    );
}
