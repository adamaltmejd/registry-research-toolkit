//! Raw inputs and curation produce one complete refusal report before publication
//! (`cases/build-report`).

use std::collections::BTreeMap;
use std::fs;

use reg_conformance::{Scratch, cases, copy_tree};
use serde_json::{Value, json};

use crate::common::cli::curated;
use crate::common::read_json;
use crate::common::steward::edit;

#[test]
fn release_collects_all_refusals_before_publication() {
    // Fails if release curation or any register's first ledger refusal returns before
    // the report, loses later ids/registers, or publishes despite a refusal. Expected
    // ids/counts come from this fixture's source and ledger, including two split and
    // two unplaced ids per register.
    let fixture = cases().join("build-report");
    let expected = read_json(&fixture.join("expected.json")).unwrap();
    let dir = Scratch::new("build-report").unwrap();
    let (inputs, curation) = (fixture.join("inputs"), fixture.join("curation"));
    let dev_report = dir.path().join("dev.json");
    let dev_db = dir.path().join("dev.db");
    curated(
        "build",
        &inputs,
        &curation,
        &[
            "--out".as_ref(),
            dev_db.as_os_str(),
            "--report".as_ref(),
            dev_report.as_os_str(),
        ],
    )
    .ok();
    let previous = b"previous catalog must survive a refused build\n";
    let catalog = dir.path().join("catalog.db");
    fs::write(&catalog, previous).unwrap();
    let release_report = dir.path().join("release.json");
    curated(
        "build",
        &inputs,
        &curation,
        &[
            "--release".as_ref(),
            "--out".as_ref(),
            catalog.as_os_str(),
            "--report".as_ref(),
            release_report.as_os_str(),
        ],
    )
    .refused(&[]);
    assert_eq!(fs::read(&catalog).unwrap(), previous);
    let mut report = read_json(&release_report).unwrap();
    let mut normal = read_json(&dev_report).unwrap();
    assert_eq!(report["registers"], expected["registers"]);
    let refusals = report.as_object_mut().unwrap().remove("refusals").unwrap();
    assert_eq!(
        normal.as_object_mut().unwrap().remove("refusals"),
        Some(json!([]))
    );
    assert_eq!(
        report, normal,
        "release and dev reports differ beyond refusals"
    );

    let mut counts: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    for refusal in refusals.as_array().unwrap() {
        let register = refusal["register"].as_str().unwrap_or("curation");
        let kind = refusal["kind"].as_str().unwrap();
        let details = refusal["details"].as_array().unwrap();
        counts
            .entry(register.to_owned())
            .or_default()
            .insert(kind.to_owned(), details.len());
        let want = &expected["registers"][register];
        if kind == "unminted-variable" {
            assert_eq!(&Value::from(details.clone()), &want["unminted"]);
        } else if let Some(split) = kind.strip_prefix("ledger-id-") {
            let key = if split == "split" {
                "split_ids"
            } else {
                "unplaced_ids"
            };
            let ids = want[key].as_array().unwrap();
            assert_eq!(ids.len(), details.len(), "{kind}: {details:?}");
            for (ident, detail) in ids.iter().zip(details) {
                let detail = detail.as_str().unwrap();
                for needle in [
                    format!("ledger id {ident}"),
                    format!("/registers/{register}/ids.toml:"),
                    kind.to_owned(),
                ] {
                    assert!(detail.contains(&needle), "no {needle:?} in {detail}");
                }
            }
        } else if kind == "curation" {
            let located = expected["curation_refusals"][register].as_str().unwrap();
            assert!(
                details
                    .iter()
                    .any(|d| d.as_str().unwrap().contains(located)),
                "no {located:?} in {details:?}"
            );
        }
    }
    assert_eq!(
        serde_json::to_value(counts).unwrap(),
        expected["refusal_counts"]
    );
}

#[test]
fn stale_set_presentation_reports_and_withdraws_facets() {
    // Regression: a stale set label skips its declaration but accepted facets cause a
    // hard error before the requested dev/release reports. The locked fixture is the
    // allowed twin; changing only its label must warn (dev) or refuse (release) at
    // every now-undeclared facet. Locations/codes come from the authored fixture.
    let fixture = cases().join("fixtures/webapp-dev");
    let expected = &read_json(&cases().join("lock/set-facets/expected.json")).unwrap()["stale"];
    let dir = Scratch::new("stale-set").unwrap();
    let curation = dir.path().join("curation");
    copy_tree(&fixture.join("curation"), &curation).unwrap();
    let inputs = fixture.join("inputs");
    let catalog = dir.path().join("catalog.db");
    let build = |extra: &[&std::ffi::OsStr]| {
        let mut args = vec!["--out".as_ref(), catalog.as_os_str()];
        args.extend_from_slice(extra);
        curated("build", &inputs, &curation, &args)
    };
    curated("lock", &inputs, &curation, &[]).ok();
    build(&[]).ok();
    let text = |key: &str| expected[key].as_str().unwrap();
    edit(
        &curation.join("registers/demo/bas/curation.toml"),
        text("label"),
        text("replacement"),
        false,
    )
    .unwrap();
    let located: Vec<String> = expected["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            format!(
                "{}: {}:",
                r["location"].as_str().unwrap(),
                r["code"].as_str().unwrap()
            )
        })
        .collect();
    let has_all = |lines: &[String]| {
        for want in &located {
            assert!(
                lines.iter().any(|line| line.contains(want.as_str())),
                "no {want:?} in {lines:?}"
            );
        }
    };
    let dev_report = dir.path().join("dev.json");
    build(&["--report".as_ref(), dev_report.as_os_str()]).ok();
    let strings = |value: &Value| -> Vec<String> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect()
    };
    has_all(&strings(&read_json(&dev_report).unwrap()["warnings"]));
    let previous = fs::read(&catalog).unwrap();
    let release_report = dir.path().join("release.json");
    build(&[
        "--release".as_ref(),
        "--report".as_ref(),
        release_report.as_os_str(),
    ])
    .refused(&[]);
    assert_eq!(fs::read(&catalog).unwrap(), previous);
    let report = read_json(&release_report).unwrap();
    let details: Vec<String> = report["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["kind"] == "curation" && r["register"] == "demo/bas")
        .flat_map(|r| strings(&r["details"]))
        .collect();
    assert_eq!(details.len(), located.len(), "{details:?}");
    has_all(&details);
}
