//! Curation source fixtures → located lock refusal, before catalog publication
//! (`cases/lock`), and the lock's statement identity and stale-premise rules.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use reg_conformance::{Scratch, cases, copy_tree};
use serde_json::Value;

use crate::common::cli::{column, curated, exec};
use crate::common::read_json;
use crate::common::steward::{edit, read, write};

fn scratch(label: &str) -> Scratch {
    Scratch::new(label).unwrap()
}

/// A copy of the fixture's curation in `dir/curation`.
fn curation_copy(fixture: &Path, dir: &Path) -> PathBuf {
    let curation = dir.join("curation");
    copy_tree(&fixture.join("curation"), &curation).unwrap();
    curation
}

#[test]
fn set_facets_refuse_invalid_membership() {
    // Regression: publish facets on an undeclared axis or on a nonmember. The HTTP
    // show-set case covers the allowed twin with two ordered axes; lock must locate
    // the offending variable statement.
    let fixture = cases().join("fixtures/webapp-dev");
    let expected = read_json(&cases().join("lock/set-facets/expected.json")).unwrap();
    for (scenario, from, to) in [
        (
            "undeclared",
            "fields.facets = { measure =",
            "fields.facets = { undeclared =",
        ),
        (
            "nonmember",
            "fields.sets = [\"inkomst\"]",
            "fields.label = \"Inkomst före skatt\"",
        ),
    ] {
        let dir = scratch("set-facets");
        let curation = curation_copy(&fixture, dir.path());
        let inputs = fixture.join("inputs");
        curated("lock", &inputs, &curation, &[]).ok();
        let statements = curation.join("registers/demo/bas/curation.toml");
        edit(&statements, from, to, false).unwrap();
        let want = &expected[scenario];
        curated("lock", &inputs, &curation, &[]).refused(&[&format!(
            "registers/demo/bas/curation.toml:line={}: {}:",
            want["line"],
            want["code"].as_str().unwrap()
        )]);
    }
}

#[test]
fn assignments_refuse_concurrent_folds_at_lock() {
    // Regression: assignment resolution accepts concurrent different folds in one
    // table, leaving only artifact verify to reject the resulting segment. The
    // one-day overlap must refuse beside nonconcurrent and cross-table allowed twins.
    let case = cases().join("lock/concurrent-fold-assignment");
    let expected = read_json(&case.join("expected.json")).unwrap();
    let dir = scratch("concurrent-fold");
    let curation = curation_copy(&case, dir.path());
    let statements = curation.join("registers/demo/assignment/curation.toml");
    let inputs = case.join("inputs");
    fs::copy(case.join("allowed.toml"), &statements).unwrap();
    curated("lock", &inputs, &curation, &[]).ok();
    fs::copy(case.join("refused.toml"), &statements).unwrap();
    let refused = curated("lock", &inputs, &curation, &[]);
    assert_eq!(
        refused.code.map(i64::from),
        expected["exit_code"].as_i64(),
        "{}",
        refused.stderr
    );
    // Bystander assignments at lines 1 and 7 must not receive this refusal.
    let located: Vec<String> = refused
        .stderr
        .lines()
        .filter(|line| line.contains("D-11:"))
        .filter_map(|line| line.split_once("registers/"))
        .map(|(_, rest)| format!("registers/{rest}"))
        .collect();
    assert_eq!(Value::from(located), expected["located_errors"]);
}

#[test]
fn merged_source_keys_take_variable_statements() {
    // Regression: either a `var:` statement on a variable that curation merged from two
    // overlapping source keys is refused `ambiguous`, so the merge cannot be finished,
    // or the exemption widens and an unmerged two-key column takes an unguarded statement.
    let case = cases().join("lock/merged-key-statements");
    let expected = read_json(&case.join("expected.json")).unwrap();
    let dir = scratch("merged-keys");
    let curation = curation_copy(&case, dir.path());
    let statements = curation.join("registers/demo/merge/curation.toml");
    let inputs = case.join("inputs");
    curated("lock", &inputs, &curation, &[]).ok();
    let mut text = fs::read_to_string(&statements).unwrap();
    text.push_str(&fs::read_to_string(case.join("refused.toml")).unwrap());
    fs::write(&statements, text).unwrap();
    curated("lock", &inputs, &curation, &[]).refused(&[expected["refused"].as_str().unwrap()]);
}

#[test]
fn release_moves_whole_and_attached() {
    // Regression: `set table` on a release subject moves its occurrences but leaves
    // the release (or its gap marker) behind, or detaches them into dated rows, so the
    // destination has no release for editions, cadence and gaps. Twin: moving one
    // column of that release beside the whole move is a partial move and must refuse
    // at both statements, not apply in statement order.
    let case = cases().join("lock/release-move");
    let expected = read_json(&case.join("expected.json")).unwrap();
    let dir = scratch("release-move");
    let curation = curation_copy(&case, dir.path());
    let inputs = case.join("inputs");
    let catalog = dir.path().join("catalog.db");
    curated("lock", &inputs, &curation, &[]).ok();
    curated(
        "build",
        &inputs,
        &curation,
        &["--release".as_ref(), "--out".as_ref(), catalog.as_os_str()],
    )
    .ok();
    let rows: Value = column(
        &catalog,
        "SELECT json_array(t.pid, r.source_key, o.source_key, o.status) FROM occurrence o \
         JOIN tbl t ON t.id=o.table_id JOIN release r ON r.id=o.release_id \
         AND r.table_id=o.table_id ORDER BY t.pid, r.source_key, o.source_key",
    )
    .iter()
    .map(|row| serde_json::from_str::<Value>(row).unwrap())
    .collect();
    assert_eq!(rows, expected["occurrences"]);
    let statements = curation.join("registers/demo/mv/curation.toml");
    let mut text = fs::read_to_string(&statements).unwrap();
    text.push_str(&fs::read_to_string(case.join("refused.toml")).unwrap());
    fs::write(&statements, text).unwrap();
    let refused = expected["refused"].as_array().unwrap();
    curated("lock", &inputs, &curation, &[]).refused(
        &refused
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>(),
    );
}

/// Each occurrence's `[variable pid, exact spelling, <column>]` in `db`, in `order`.
fn occurrences(db: &Path, value: &str, order: &str) -> Value {
    let sql = format!(
        "SELECT json_array(v.pid, s.exact, {value}) FROM occurrence o \
         JOIN variable v ON v.id=o.variable_id JOIN spelling s ON s.id=o.spelling_id \
         ORDER BY {order}"
    );
    column(db, &sql)
        .iter()
        .map(|row| serde_json::from_str::<Value>(row).unwrap())
        .collect()
}

#[test]
fn inherited_identity_uses_remaining_ledger_anchors() {
    // Regression: the alphabetically first ledger id owns the whole default source
    // key, so assigning its earlier spelling silently absorbs the later spelling. The
    // dated anchor must also identify the year after its day. Untouched X/Y keep their
    // default ids despite a spanning anchor and later single-group anchors: an
    // unrelated Z assignment must not rename X from a to b.
    let case = cases().join("lock/ledger-inheritance");
    let dir = scratch("ledger-inheritance");
    let curation = curation_copy(&case, dir.path());
    let catalog = dir.path().join("catalog.db");
    let inputs = case.join("inputs");
    curated("lock", &inputs, &curation, &[]).ok();
    curated(
        "build",
        &inputs,
        &curation,
        &["--release".as_ref(), "--out".as_ref(), catalog.as_os_str()],
    )
    .ok();
    assert_eq!(
        occurrences(
            &catalog,
            "date(o.start * 86400, 'unixepoch')",
            "o.start, s.exact"
        ),
        read_json(&case.join("expected.json")).unwrap()
    );
}

#[test]
fn evidence_edit_moves_no_identity() {
    // Regression: statement identity hashes `evidence`, so rewording it moves the lock
    // key, the asserted occurrence's `stmt:` source key and the ledger anchor that pins
    // its id (maintainer ruling, 2026-11-04). The committed lock and the ledger's
    // `# stmt:` anchor are the oracle. Twin: a caveat is content, so adding one must
    // still move the lock key; it fails if identity starts dropping more than evidence.
    let case = cases().join("lock/evidence-identity");
    let inputs = case.join("inputs");
    let lock = "registers/demo/r/curation.lock";
    let committed = fs::read(case.join("curation").join(lock)).unwrap();
    let marker = "evidence = \"Delivery lists";
    for (edit_kind, to) in [
        (
            "evidence",
            "evidence = \"Reworded: delivery lists".to_owned(),
        ),
        (
            "caveat",
            format!("caveat = \"Single value in 2010.\"\n{marker}"),
        ),
    ] {
        let dir = scratch("evidence-identity");
        let curation = curation_copy(&case, dir.path());
        edit(
            &curation.join("registers/demo/r/curation.toml"),
            marker,
            &to,
            false,
        )
        .unwrap();
        curated("lock", &inputs, &curation, &[]).ok();
        let relocked = fs::read(curation.join(lock)).unwrap();
        if edit_kind == "caveat" {
            assert_ne!(relocked, committed, "a caveat must move the lock key");
            continue;
        }
        assert_eq!(relocked, committed, "an evidence edit moved the lock");
        let catalog = dir.path().join("catalog.db");
        curated(
            "build",
            &inputs,
            &curation,
            &["--release".as_ref(), "--out".as_ref(), catalog.as_os_str()],
        )
        .ok();
        assert_eq!(
            occurrences(&catalog, "o.source_key", "v.pid"),
            read_json(&case.join("expected.json")).unwrap()
        );
    }
}

/// The memberships overlay on the published-id source and ledgers
/// (`cases/release-ids`): its fixture, the overlay directory, the curation copy in
/// `dir` with `allowed.toml` and the tag vocabulary applied, and the overlay's
/// `expected.json`.
pub fn membership_case(dir: &Path) -> (PathBuf, PathBuf, PathBuf, Value) {
    let fixture = cases().join("release-ids");
    let overlay = cases().join("lock/memberships");
    let curation = curation_copy(&fixture, dir);
    fs::copy(
        overlay.join("allowed.toml"),
        curation.join("registers/demo/r/curation.toml"),
    )
    .unwrap();
    fs::copy(
        overlay.join("tag-vocabulary.toml"),
        curation.join("tag-vocabulary.toml"),
    )
    .unwrap();
    let expected = read_json(&overlay.join("expected.json")).unwrap();
    (fixture, overlay, curation, expected)
}

#[test]
fn membership_refusals() {
    // Regression: source occurrences are counted as set members instead of final
    // variables, an unknown tag is accepted, or a retired var_set pid is accepted. The
    // singleton uses three nonconcurrent source occurrences resolving to one final
    // variable. The allowed twin has two distinct variables in different same-register
    // tables. Multi-group: a variable selector and an occurrence selector put the same
    // final variable in different groups; neither group's origin may be hidden, and
    // its faceted member pins refusal before an unqualified facet map could choose
    // the alphabetically first set.
    for change in ["singleton", "unknown-tag", "retired-set", "multi-group"] {
        let dir = scratch("membership");
        let (fixture, overlay, curation, expected) = membership_case(dir.path());
        let inputs = fixture.join(if change == "singleton" {
            "grown"
        } else {
            "inputs"
        });
        let out = dir.path().join("catalog.db");
        let run = |command: &str| {
            let extra: &[&OsStr] = if command == "build" {
                &["--out".as_ref(), out.as_os_str()]
            } else {
                &[]
            };
            curated(command, &inputs, &curation, extra)
        };
        run("lock").ok();
        if change == "retired-set" {
            fs::copy(overlay.join("retired.toml"), curation.join("retired.toml")).unwrap();
        } else {
            fs::copy(
                overlay.join(format!("{change}.toml")),
                curation.join("registers/demo/r/curation.toml"),
            )
            .unwrap();
            if change == "singleton" {
                run("lock").ok();
            }
        }
        let want = &expected[change];
        let text = |key: &str| want[key].as_str().unwrap().to_owned();
        let refused = run(&text("command")).refused(&[
            &format!("{}: {}:", text("location"), text("code")),
            &text("pid"),
        ]);
        if change == "multi-group" {
            for (group, origin) in want["groups"].as_object().unwrap() {
                let origin = curation.join(origin.as_str().unwrap());
                let named = format!("{group} at {}", origin.display());
                assert!(refused.stderr.contains(&named), "{}", refused.stderr);
            }
        }
    }
}

#[test]
fn vocabulary_wording_needs_no_relock() {
    // Regression: vocabulary text becomes a premise dependency (a reworded definition
    // then needs a relock), or is omitted from the build's curation digest.
    let dir = scratch("vocabulary");
    let (fixture, _, curation, expected) = membership_case(dir.path());
    let inputs = fixture.join("inputs");
    let catalog = dir.path().join("catalog.db");
    let build = || {
        curated(
            "build",
            &inputs,
            &curation,
            &["--out".as_ref(), catalog.as_os_str()],
        )
        .ok();
        column(
            &catalog,
            "SELECT value FROM meta WHERE key='curation_digest'",
        )
    };
    curated("lock", &inputs, &curation, &[]).ok();
    let lock = curation.join("registers/demo/r/curation.lock");
    let original_lock = fs::read(&lock).unwrap();
    let original_digest = build();
    edit(
        &curation.join("tag-vocabulary.toml"),
        expected["tag_text"][1].as_str().unwrap(),
        expected["reworded_definition"].as_str().unwrap(),
        true,
    )
    .unwrap();
    assert_ne!(
        build(),
        original_digest,
        "the digest ignores vocabulary text"
    );
    curated("lock", &inputs, &curation, &[]).ok();
    assert_eq!(fs::read(&lock).unwrap(), original_lock);
}

/// Every refusal detail of the build report at `path`, one per line.
fn report_details(path: &Path) -> String {
    let report = read_json(path).unwrap();
    report["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|refusal| refusal["details"].as_array().unwrap())
        .map(|detail| detail.as_str().unwrap().to_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
// One scenario, read top to bottom: its steps share the artifact they mutate.
#[allow(clippy::too_many_lines)]
fn variable_premise_tracks_matches_not_occurrence_fields() {
    // Regression: variable metadata locks occurrence/container field values, or keeps
    // only a match count and misses a same-count locator replacement. Source overlays
    // and expected JSON supply the allowed field drift, matched-row counts and the
    // located refusal.
    let dir = scratch("variable-premise");
    let (fixture, overlay, curation, expectations) = membership_case(dir.path());
    let expected = &expectations["variable_premise"];
    let statements = curation.join("registers/demo/r/curation.toml");
    fs::copy(overlay.join("variable-level.toml"), &statements).unwrap();
    let inputs = dir.path().join("inputs");
    copy_tree(&fixture.join("inputs"), &inputs).unwrap();
    let source = inputs.join("frame/registers.jsonl");
    let report = dir.path().join("report.json");
    let catalog = dir.path().join("catalog.db");
    let set_source = |from: &Path| {
        fs::copy(from, &source).unwrap();
    };
    let lock = || curated("lock", &inputs, &curation, &[]);
    let build = || {
        let _ = fs::remove_file(&catalog);
        curated(
            "build",
            &inputs,
            &curation,
            &[
                "--release".as_ref(),
                "--out".as_ref(),
                catalog.as_os_str(),
                "--report".as_ref(),
                report.as_os_str(),
            ],
        )
    };
    let text = |value: &Value| value.as_str().unwrap().to_owned();
    let premise = format!(
        "{}: {}:",
        text(&expected["location"]),
        text(&expected["code"])
    );
    set_source(&overlay.join("container-baseline.jsonl"));
    lock().ok();
    set_source(&overlay.join("containers-changed.jsonl"));
    build().ok();
    set_source(&overlay.join("fields-changed.jsonl"));
    build().ok();
    let count = &expected["original_count"];
    for (from, new_count) in [
        (overlay.join("match-replaced.jsonl"), count),
        (
            fixture.join("grown/frame/registers.jsonl"),
            &expected["grown_count"],
        ),
    ] {
        set_source(&from);
        build().refused(&[]);
        let details = report_details(&report);
        for needle in [
            premise.clone(),
            text(&expected["subject"]),
            format!("old count {count}, new count {new_count}"),
        ] {
            assert!(details.contains(&needle), "no {needle:?} in: {details}");
        }
    }

    // Regression: shared dependencies cannot be read back, or a changed shared
    // container is not a located stale premise of each statement that reads it. The two occurrence setters share the register, table and
    // release described by the source overlays.
    let expected = &expectations["shared_objects"];
    set_source(&overlay.join("container-baseline.jsonl"));
    fs::copy(overlay.join("shared-objects.toml"), &statements).unwrap();
    lock().ok();
    let lock_path = curation.join("registers/demo/r/curation.lock");
    let original_lock = fs::read(&lock_path).unwrap();
    lock().ok();
    assert_eq!(fs::read(&lock_path).unwrap(), original_lock, "relock moved");
    build().ok();

    // Regression: corrupt content-addressed maps are accepted as reviewed facts.
    // Renaming a key and removing an object exercise separate integrity failures.
    let invalid = format!("{}: invalid premise lock:", lock_path.display());
    for (change, code) in expected["object_refusals"].as_object().unwrap() {
        let mut tampered: Value = serde_json::from_slice(&original_lock).unwrap();
        let objects = tampered["objects"].as_object_mut().unwrap();
        let key = objects.keys().next().unwrap().clone();
        let value = objects.remove(&key).unwrap();
        if change == "digest" {
            objects.insert("0".repeat(64), value);
        }
        write(&lock_path, &tampered.to_string()).unwrap();
        build().refused(&[&invalid, code.as_str().unwrap(), "run reg-build lock"]);
    }
    fs::write(&lock_path, &original_lock).unwrap();

    set_source(&overlay.join("containers-changed.jsonl"));
    build().refused(&[]);
    let details = report_details(&report);
    let code = text(&expected["code"]);
    for location in expected["locations"].as_array().unwrap() {
        let located = format!("{}: {code}:", text(location));
        assert!(details.contains(&located), "no {located:?} in: {details}");
    }
    // Regression: a v1 or v2 lock is silently accepted rather than requiring a
    // re-lock. These deliberately obsolete inputs exercise the version gate.
    let mut obsolete = read_json(&overlay.join("v1.lock")).unwrap();
    for version in [1, 2] {
        obsolete["version"] = version.into();
        write(&lock_path, &obsolete.to_string()).unwrap();
        build().refused(&[
            &format!("unsupported premise lock version {version}"),
            "run reg-build lock",
        ]);
    }
}

#[test]
fn description_has_one_variable_owner() {
    // Regression: accept occurrence-owned descriptions or overwrite conflicting var:
    // descriptions. The valid HTTP/MCP descriptions fixture is the allowed twin.
    let case = cases().join("lock/variable-description");
    let fixture = cases().join("fixtures/descriptions");
    let expected = read_json(&case.join("expected.json")).unwrap();
    for scenario in ["occurrence", "conflict"] {
        let dir = scratch("description-owner");
        let curation = curation_copy(&fixture, dir.path());
        fs::copy(
            case.join(format!("{scenario}.toml")),
            curation.join("registers/demo/descriptions/curation.toml"),
        )
        .unwrap();
        let want = &expected[scenario];
        assert_eq!(want["exit_code"], 1);
        let located: Vec<String> = want["located_lines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|line| format!("registers/demo/descriptions/curation.toml:{line}:"))
            .collect();
        let mut needles: Vec<&str> = located.iter().map(String::as_str).collect();
        needles.push(want["code"].as_str().unwrap());
        curated("lock", &fixture.join("inputs"), &curation, &[]).refused(&needles);
    }
}

#[test]
fn reserved_provider_prefixes_refuse_at_lock() {
    // Regression: a manifest or source frame uses a reserved discriminator as its
    // provider, making all emitted public ids unreachable. The base fixture is the
    // allowed twin; expected.json names every reserved prefix and its source location.
    let fixture = cases().join("fixtures/base");
    let expected = read_json(&cases().join("lock/reserved-provider/expected.json")).unwrap();
    let dir = scratch("reserved-provider");
    let curation = curation_copy(&fixture, dir.path());
    let inputs = dir.path().join("inputs");
    copy_tree(&fixture.join("inputs"), &inputs).unwrap();
    let manifest = curation.join("inputs.toml");
    let frame = inputs.join("frame/base.jsonl");
    let (original_manifest, original_frame) = (read(&manifest).unwrap(), read(&frame).unwrap());
    let lock = || exec(&mut crate::common::cli::curate("lock", &inputs, &curation));
    lock().ok();
    let code = expected["code"].as_str().unwrap();
    for provider in expected["providers"].as_array().unwrap() {
        let provider = provider.as_str().unwrap();
        edit(
            &manifest,
            "pid = \"demo\"",
            &format!("pid = \"{provider}\""),
            true,
        )
        .unwrap();
        lock().refused(&[&format!(
            "{}: {code}:",
            expected["manifest_location"].as_str().unwrap()
        )]);
        // A frame's provider must not bypass a valid manifest's namespace check.
        write(&manifest, &original_manifest).unwrap();
        edit(
            &frame,
            "\"provider\":\"demo\"",
            &format!("\"provider\":\"{provider}\""),
            true,
        )
        .unwrap();
        lock().refused(&[&format!(
            "{}: {code}:",
            expected["frame_location"].as_str().unwrap()
        )]);
        write(&frame, &original_frame).unwrap();
    }
}

#[test]
fn steward_names_stay_out_of_the_global_catalog() {
    // Regression: the global build publishes curated text, a line of a curation file
    // without source names (inputs.toml) or an id that names a data steward, or accepts a private:ev-NNNN reference the pinned private evidence file
    // does not hold (maintainer ruling, 2026-11-05). Each refusal is located with its
    // code. Twin: a resolving reference builds, and the catalog carries the reference
    // only, never the private text.
    let fixture = cases().join("lock/evidence-identity");
    let case = cases().join("lock/steward-names");
    let expected = read_json(&case.join("expected.json")).unwrap();
    let marker = expected["marker"].as_str().unwrap();
    let setup = |label: &str| {
        let dir = scratch(label);
        let curation = curation_copy(&fixture, dir.path());
        let inputs = dir.path().join("inputs");
        copy_tree(&fixture.join("inputs"), &inputs).unwrap();
        fs::copy(
            case.join("private-evidence.toml"),
            inputs.join("private-evidence.toml"),
        )
        .unwrap();
        let mut manifest = read(&curation.join("inputs.toml")).unwrap();
        manifest.push_str(
            "\n[[input]]\nname = \"private-evidence\"\nreader = \"private-evidence\"\npath = \"private-evidence.toml\"\n",
        );
        write(&curation.join("inputs.toml"), &manifest).unwrap();
        (dir, curation, inputs)
    };
    let build = |dir: &Scratch, curation: &Path, inputs: &Path| {
        curated("lock", inputs, curation, &[]).ok();
        let catalog = dir.path().join("catalog.db");
        let ran = curated(
            "build",
            inputs,
            curation,
            &["--release".as_ref(), "--out".as_ref(), catalog.as_os_str()],
        );
        (ran, catalog)
    };
    for (scenario, want) in expected["refused"].as_object().unwrap() {
        let (dir, curation, inputs) = setup(scenario);
        let from = want.get("from").and_then(Value::as_str).unwrap_or(marker);
        edit(
            &curation.join(want["file"].as_str().unwrap()),
            from,
            want["to"].as_str().unwrap(),
            false,
        )
        .unwrap();
        build(&dir, &curation, &inputs)
            .0
            .refused(&[want["located"].as_str().unwrap()]);
    }
    let (dir, curation, inputs) = setup("steward-names-allowed");
    edit(
        &curation.join("registers/demo/r/curation.toml"),
        marker,
        expected["allowed"].as_str().unwrap(),
        false,
    )
    .unwrap();
    let (ran, catalog) = build(&dir, &curation, &inputs);
    ran.ok();
    let published = column(
        &catalog,
        "SELECT t.body FROM statement s JOIN text t ON t.id = s.evidence_text ORDER BY 1",
    );
    assert_eq!(serde_json::json!(published), expected["published_evidence"]);
    let private = read(&case.join("private-evidence.toml")).unwrap();
    let text = private.split('"').nth(1).unwrap();
    assert!(
        column(&catalog, "SELECT body FROM text")
            .iter()
            .all(|body| !body.contains(text)),
        "the private evidence text reached the catalog"
    );
}
