//! Where the report may go, which the corpus cannot express: its output folder
//! is always `output`. The report is written beside the output folder, so an
//! output named `.diagnostics` would put it in the export, and a planted link
//! or hard link named like the report would redirect it onto a staging or
//! exported file. Each case starts from the corpus's passing trial candidate.

#[path = "../corpus/materialize.rs"]
mod materialize;

use std::path::{Path, PathBuf};

use reg_export_gate::{Code, Finding, Refusal, export_gate};
use serde_json::Value;

const TRIAL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../reg_export_gate/tests/cases/pass-trial"
);

/// A fresh case root holding the trial candidate in `staging`.
fn trial(name: &str) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("paths")
        .join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("a root");
    let case: Value =
        serde_json::from_slice(&std::fs::read(Path::new(TRIAL).join("case.json")).expect("case"))
            .expect("JSON");
    let staging = root.join("staging");
    materialize::materialize(
        &staging,
        case["candidate"].as_object().expect("a candidate"),
    );
    let at = |key: &str| Path::new(TRIAL).join(case[key].as_str().expect("a path"));
    (root, staging, at("schema"), at("policy"))
}

#[test]
fn output_named_like_the_diagnostics_folder_refuses() {
    // Regression: the gate passed and then wrote its report into the export;
    // `.DIAGNOSTICS` is the same folder on a case-insensitive volume.
    for name in [".diagnostics", ".DIAGNOSTICS"] {
        let (root, staging, schema, policy) = trial(&format!("diagnostics-output{name}"));
        let output = root.join(name);
        let result = export_gate(&staging, &output, Some(&schema), Some(&policy));
        assert_eq!(
            result,
            Err(Refusal {
                code: Code::Setup,
                findings: vec![Finding {
                    file: None,
                    column: None,
                    position: None,
                    rule: "paths_overlap",
                }],
            }),
            "{name}"
        );
        assert!(!output.exists(), "{name}");
    }
}

#[cfg(unix)]
#[test]
fn a_planted_report_link_is_never_written_through() {
    // Regression: the report write followed a hard link (or symlink) named
    // like the report and replaced the staging file it pointed at.
    let (root, staging, schema, policy) = trial("planted-report");
    let target = staging.join("summary_by_year.parquet");
    let before = std::fs::read(&target).expect("a staging file");
    let dir = root.join(".diagnostics");
    std::fs::create_dir(&dir).expect("a diagnostics folder");
    std::fs::hard_link(&target, dir.join("export_gate_report.json")).expect("a hard link");
    let output = root.join("output");
    assert_eq!(
        export_gate(&staging, &output, Some(&schema), Some(&policy)),
        Ok(2)
    );
    assert_eq!(std::fs::read(&target).expect("still there"), before);
    let report = std::fs::read_to_string(dir.join("export_gate_report.json")).expect("a report");
    assert!(report.contains("\"pass\""));
}

#[cfg(unix)]
#[test]
fn an_output_that_is_a_link_is_not_cleared() {
    // Regression: clearing followed the link and deleted the gate export it
    // pointed at, outside the output path the pipeline named; with a trailing
    // separator (`output/`) the OS follows the link even for a no-follow check.
    for spelling in ["output", "output/"] {
        let (root, staging, schema, policy) = trial(&format!("linked-{}", spelling.len()));
        let elsewhere = root.join("elsewhere");
        std::fs::create_dir(&elsewhere).expect("a folder");
        let first = export_gate(
            &staging,
            &elsewhere.join("export"),
            Some(&schema),
            Some(&policy),
        );
        assert_eq!(first, Ok(2));
        std::os::unix::fs::symlink(elsewhere.join("export"), root.join("output")).expect("a link");
        let result = export_gate(&staging, &root.join(spelling), Some(&schema), None);
        assert_eq!(
            result.map_err(|r| (r.code, r.findings[0].rule)),
            Err((Code::Setup, "output_not_gate_export")),
            "{spelling}"
        );
        assert_eq!(
            std::fs::read_dir(elsewhere.join("export"))
                .expect("kept")
                .count(),
            3,
            "{spelling}"
        );
    }
}
