//! `export_project`: the reviewer's export of a checked server-mode output to
//! the destination (no R counterpart). Temporary folders stand in for the data
//! VM and the agent VM's read-only view. Each case starts from the trial
//! candidate checked with `check_project`; the reviewer names the SHA-256 of
//! the `receipt.json` they inspected. Unix only, as server mode is.
#![cfg(unix)]

#[path = "../project/deployment.rs"]
mod deployment;
#[path = "../corpus/materialize.rs"]
mod materialize;

use std::path::{Path, PathBuf};

use deployment::{Deployment, chmod};
use reg_export_gate::{Code, RECEIPT, export_gate, export_project};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

/// A deployment whose staging passed `check`, with an empty destination only
/// its owner can write; gives the receipt's SHA-256.
fn checked(name: &str) -> (Deployment, String) {
    let d = Deployment::new(&format!("deliver-{name}"));
    let destination = d.root.join("destination");
    std::fs::create_dir(&destination).expect("a destination");
    chmod(&destination, 0o755);
    assert_eq!(d.check("trial"), Ok(2));
    let receipt = sha256(&std::fs::read(d.root.join("output").join(RECEIPT)).expect("a receipt"));
    (d, receipt)
}

fn export(d: &Deployment, receipt: &str) -> Result<PathBuf, (Code, &'static str)> {
    export_project(&d.config, "trial", receipt).map_err(|r| (r.code, r.findings[0].rule))
}

fn entries(dir: &Path) -> usize {
    std::fs::read_dir(dir).expect("a folder").count()
}

#[test]
fn an_approved_export_lands_with_its_hashes_and_repeats_as_a_check() {
    // Regression: delivery copied without re-hashing, wrote a second copy on a
    // repeat, or let a changed byte at the destination stand.
    let (d, receipt) = checked("pass");
    let output = d.root.join("output");
    let target = export(&d, &receipt).expect("delivered");
    assert_eq!(target, d.root.join("destination").join(&receipt[..16]));
    // The receipt as landed lists every file as landed, and is the one named.
    let landed = std::fs::read(target.join(RECEIPT)).expect("a receipt");
    assert_eq!(sha256(&landed), receipt);
    let listed: Value = serde_json::from_slice(&landed).expect("JSON");
    for file in listed["files"].as_array().expect("files") {
        let name = file["name"].as_str().expect("a name");
        let bytes = std::fs::read(target.join(name)).expect("a landed file");
        assert_eq!(file["sha256"], sha256(&bytes), "{name}");
        assert_eq!(bytes, std::fs::read(output.join(name)).expect("checked"));
    }
    assert_eq!(entries(&target), entries(&output));
    // A second export re-verifies and writes nothing new.
    assert_eq!(export(&d, &receipt), Ok(target.clone()));
    assert_eq!(entries(&d.root.join("destination")), 1);
    // A byte changed at the destination fails that re-verification.
    let file = target.join("summary_by_year.parquet");
    let mut bytes = std::fs::read(&file).expect("a file");
    *bytes.last_mut().expect("bytes") ^= 1;
    std::fs::write(&file, bytes).expect("a write");
    assert_eq!(export(&d, &receipt), Err((Code::Delivery, "hash_mismatch")));
}

#[test]
fn a_receipt_other_than_the_one_named_refuses() {
    // Regression: export delivered whatever the output held, not what the
    // reviewer inspected.
    let (d, receipt) = checked("other-receipt");
    let other = sha256(receipt.as_bytes());
    assert_eq!(
        export(&d, &other),
        Err((Code::Delivery, "receipt_mismatch"))
    );
    assert_eq!(entries(&d.root.join("destination")), 0);
}

#[test]
fn the_authors_files_under_the_genuine_receipt_refuse() {
    // Regression: export compared only the receipt it re-derived with the one
    // read, so the author's own Parquet (metadata and all), which decodes to
    // the same values and so re-derives the same receipt, passed as the gate's.
    let (d, receipt) = checked("forged");
    let output = d.root.join("output");
    for name in ["persons_by_year_group.parquet", "summary_by_year.parquet"] {
        std::fs::copy(d.staging.join(name), output.join(name)).expect("a copy");
    }
    assert_eq!(
        export(&d, &receipt),
        Err((Code::Delivery, "receipt_mismatch"))
    );
    assert_eq!(entries(&d.root.join("destination")), 0);
}

#[test]
fn a_file_replaced_after_the_check_refuses() {
    // Regression: export re-checked the files but compared them with nothing,
    // so a different passing table swapped in after review was delivered.
    let (d, receipt) = checked("replaced");
    let mut candidate = d.case["candidate"].clone();
    candidate["persons_by_year_group.parquet"]["columns"][1]["values"][0] = json!("Uppsala");
    let staging = d.root.join("staging-other");
    materialize::materialize(&staging, candidate.as_object().expect("a candidate"));
    let other = d.root.join("other");
    let schemas = d.config.join("schemas");
    assert_eq!(
        export_gate(
            &staging,
            &other,
            Some(&schemas.join("trial.schema.json")),
            Some(&schemas.join("trial.policy.json")),
        ),
        Ok(2)
    );
    let name = "persons_by_year_group.parquet";
    std::fs::copy(other.join(name), d.root.join("output").join(name)).expect("a copy");
    assert_eq!(
        export(&d, &receipt),
        Err((Code::Delivery, "receipt_mismatch"))
    );
}

#[test]
fn a_destination_others_can_write_refuses() {
    // Regression: export delivered into a folder the pipeline's user could
    // write, so the agent VM's view was not only the reviewer's bytes.
    // Its parent too: whoever can write that can move the destination aside.
    let (d, receipt) = checked("writable-destination");
    let destination = d.root.join("destination");
    for (folder, mode) in [(&destination, 0o777), (&d.root, 0o775)] {
        chmod(folder, mode);
        assert_eq!(
            export(&d, &receipt),
            Err((Code::Delivery, "destination_writable")),
            "{}",
            folder.display()
        );
        assert_eq!(entries(&destination), 0);
        chmod(folder, 0o755);
    }
}

#[test]
fn an_output_configured_inside_the_destination_overlaps() {
    // Regression: export read an output already in the agent VM's view, so a
    // checked but unreviewed export had reached the agent.
    let (d, _) = checked("overlap");
    let output = d.root.join("destination/output");
    let schemas = d.config.join("schemas");
    assert_eq!(
        export_gate(
            &d.staging,
            &output,
            Some(&schemas.join("trial.schema.json")),
            Some(&schemas.join("trial.policy.json")),
        ),
        Ok(2)
    );
    let receipt = sha256(&std::fs::read(output.join(RECEIPT)).expect("a receipt"));
    let mut project = d.project();
    project["output"] = json!(output);
    d.write_project(&project);
    assert_eq!(export(&d, &receipt), Err((Code::Setup, "paths_overlap")));
}
