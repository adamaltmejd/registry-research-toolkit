//! Runs the shared case corpus (`reg_export_gate/tests/cases/*/case.json`)
//! against the gate as a pipeline's last step calls it: a candidate staging
//! folder in; the output folder, the report and the refusal code out. The
//! corpus is the oracle shared with the R gate; everything a case asserts is in
//! its JSON (`reg_export_gate/DESIGN.md`, "Case corpus"), and this runner
//! asserts what the R runner does. Each case runs through the binary too.

mod materialize;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use reg_export_gate::{Finding, RECEIPT, Refusal, VERSION, export_gate};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const CASES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../reg_export_gate/tests/cases"
);

fn sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

/// A finding as the corpus and the report write it: an absent key is null.
fn json_finding(f: &Value) -> Finding {
    let text = |key: &str| f.get(key).and_then(Value::as_str).map(str::to_owned);
    Finding {
        file: text("file"),
        column: text("column"),
        position: f
            .get("position")
            .and_then(Value::as_u64)
            .map(|p| usize::try_from(p).expect("a position")),
        // Leaked once per distinct rule string read: a test process.
        rule: Box::leak(text("rule").unwrap_or_default().into_boxed_str()),
    }
}

/// `major.minor.patch` as a comparable triple.
fn semver(v: &str) -> (u64, u64, u64) {
    let mut parts = v
        .split('.')
        .map(|p| p.parse::<u64>().expect("a version part"));
    let mut next = || parts.next().expect("three version parts");
    (next(), next(), next())
}

/// Each schema file's name and its exported columns: all but the check-only ones.
fn exported_columns(schema: &Value) -> Vec<(String, Vec<String>)> {
    schema["files"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|file| {
            let exported = file["columns"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|col| col["role"] != "check_only")
                .map(|col| col["name"].as_str().expect("a column name").to_owned())
                .collect();
            (file["name"].as_str().expect("a name").to_owned(), exported)
        })
        .collect()
}

/// The case's sentinel truths: distinctive values its check-only columns
/// hold, as decimal text and as little-endian `int64` bytes (the rewrite
/// writes integers as uncompressed `int64`).
fn sentinel_needles(case: &Value) -> Vec<(i64, Vec<Vec<u8>>)> {
    case["sentinels"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|v| {
            let v = v.as_i64().expect("an integer sentinel");
            (
                v,
                vec![v.to_string().into_bytes(), v.to_le_bytes().to_vec()],
            )
        })
        .collect()
}

fn holds(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Finding order is not part of the contract (the R runner sorts too).
fn sorted(mut findings: Vec<Finding>) -> Vec<Finding> {
    findings.sort_by_key(|f| (f.file.clone(), f.column.clone(), f.position, f.rule));
    findings
}

/// The entry names of `dir`, sorted.
fn listing(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("a folder")
        .map(|e| {
            e.expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// Every entry of `dir` with its bytes (`None` for a folder).
fn snapshot(dir: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    listing(dir)
        .into_iter()
        .map(|name| {
            let path = dir.join(&name);
            let bytes = path.is_file().then(|| std::fs::read(&path).expect("bytes"));
            (name, bytes)
        })
        .collect()
}

/// Text a candidate carries outside its column values (file and column
/// metadata keys and values, spliced bytes): never in the report, never
/// in an export.
fn hidden_strings(files: &Map<String, Value>) -> Vec<String> {
    let mut out = Vec::new();
    for spec in files.values() {
        for (k, v) in spec
            .get("metadata")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            out.push(k.clone());
            out.extend(v.as_str().map(str::to_owned));
        }
        for col in spec
            .get("columns")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            // Keys too: a key can carry a value as well as a value can.
            let meta = col.get("metadata").and_then(Value::as_object);
            for (k, v) in meta.into_iter().flatten() {
                out.push(k.clone());
                out.extend(v.as_str().map(str::to_owned));
            }
        }
        out.extend(
            spec.get("inner_bytes")
                .and_then(Value::as_str)
                .map(str::to_owned),
        );
    }
    out.retain(|s| !s.is_empty());
    out
}

/// Every string value, and every file or column name the schema does not
/// declare: the author's, so never in the report.
fn author_strings(files: &Map<String, Value>, schema: Option<&Path>) -> Vec<String> {
    let declared: Vec<Value> = schema
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .and_then(|s| s.get("files").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    let mut out = Vec::new();
    for (name, spec) in files {
        let file = declared.iter().find(|f| f["name"] == name.as_str());
        if file.is_none() && name != RECEIPT {
            out.push(name.clone());
        }
        let want: Vec<&str> = file
            .and_then(|f| f["columns"].as_array())
            .into_iter()
            .flatten()
            .filter_map(|c| c["name"].as_str())
            .collect();
        for col in spec
            .get("columns")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let col_name = col["name"].as_str().expect("a column name");
            if !want.contains(&col_name) {
                out.push(col_name.to_owned());
            }
            if col["type"] == "string" {
                let values = col["values"].as_array().into_iter().flatten();
                out.extend(values.filter_map(|v| v.as_str().map(str::to_owned)));
            }
        }
    }
    out
}

struct Run {
    root: PathBuf,
    staging: PathBuf,
    output: PathBuf,
    schema: Option<PathBuf>,
    policy: Option<PathBuf>,
    result: Result<usize, Refusal>,
    staged: Vec<(String, Option<Vec<u8>>)>,
}

/// A fresh case folder `<name>-<attempt>` with the candidate in `staging` and
/// any prior output in `output`.
fn prepare(name: &str, case_dir: &Path, case: &Value, attempt: u32) -> Run {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("corpus")
        .join(format!("{name}-{attempt}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("a case root");
    let staging = root.join("staging");
    let output = root.join("output");
    materialize::materialize(
        &staging,
        case["candidate"].as_object().expect("a candidate"),
    );
    if let Some(before) = case.get("output_before").and_then(Value::as_object) {
        materialize::materialize(&output, before);
    }
    let path = |key: &str| case[key].as_str().map(|p| case_dir.join(p));
    let (schema, policy) = (path("schema"), path("policy"));
    let staged = snapshot(&staging);
    Run {
        root,
        staging,
        output,
        schema,
        policy,
        result: Ok(0),
        staged,
    }
}

fn run_case(name: &str, case_dir: &Path, case: &Value, attempt: u32) -> Run {
    let mut run = prepare(name, case_dir, case, attempt);
    run.result = export_gate(
        &run.staging,
        &run.output,
        run.schema.as_deref(),
        run.policy.as_deref(),
    );
    run
}

/// The case run through the binary's explicit form: its stdout, exit status
/// and stderr.
fn run_binary(name: &str, case_dir: &Path, case: &Value) -> (Run, Output) {
    let run = prepare(name, case_dir, case, 3);
    let mut command = Command::new(env!("CARGO_BIN_EXE_reg-export-gate"));
    command
        .arg("check")
        .arg("--staging")
        .arg(&run.staging)
        .arg("--output")
        .arg(&run.output);
    if let Some(schema) = &run.schema {
        command.arg("--schema").arg(schema);
    }
    if let Some(policy) = &run.policy {
        command.arg("--policy").arg(policy);
    }
    let out = command.output().expect("the binary runs");
    (run, out)
}

/// The binary's stdout line and exit status for the case, from the CLI
/// contract (this crate's DESIGN.md, "The binary").
fn cli_expectation(case: &Value) -> (String, i32) {
    let Some(code) = case["expect"]["code"].as_str() else {
        return ("pass\n".to_owned(), 0);
    };
    let status = match code {
        "setup" => 10,
        "inventory" => 11,
        "structure" => 12,
        "disclosure" => 13,
        "backstop" => 14,
        other => panic!("unknown code {other}"),
    };
    (format!("refused: {code}\n"), status)
}

/// A folder's entries and bytes, or `None` if it is not there.
fn output_snapshot(dir: &Path) -> Option<Vec<(String, Option<Vec<u8>>)>> {
    dir.exists().then(|| snapshot(dir))
}

/// The binary does what the library does: the contract's one stdout line and
/// exit status, nothing on stderr, the same report and the same output, and
/// staging left as it was.
fn expect_binary(f: &mut Failures, name: &str, case_dir: &Path, case: &Value, library: &Run) {
    let (run, out) = run_binary(name, case_dir, case);
    let (line, status) = cli_expectation(case);
    f.check(out.stdout == line.as_bytes(), || {
        format!("binary stdout {:?}", String::from_utf8_lossy(&out.stdout))
    });
    f.check(out.status.code() == Some(status), || {
        format!("binary exit {:?} not {status}", out.status.code())
    });
    f.check(out.stderr.is_empty(), || "binary wrote to stderr".into());
    let report =
        |root: &Path| std::fs::read(root.join(".diagnostics/export_gate_report.json")).ok();
    f.check(report(&run.root) == report(&library.root), || {
        "binary report differs".into()
    });
    f.check(
        output_snapshot(&run.output) == output_snapshot(&library.output),
        || "binary output differs".into(),
    );
    f.check(snapshot(&run.staging) == run.staged, || {
        "binary changed staging".into()
    });
}

/// An exported file's column names, decoded.
fn column_names(bytes: Vec<u8>) -> Vec<String> {
    let reader = SerializedFileReader::new(Bytes::from(bytes)).expect("an exported Parquet file");
    reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_owned())
        .collect()
}

fn kv_metadata_free(bytes: Vec<u8>) -> bool {
    let reader = SerializedFileReader::new(Bytes::from(bytes)).expect("an exported Parquet file");
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .is_none()
}

/// Collects a case's failures.
struct Failures(Vec<String>);

impl Failures {
    fn check(&mut self, ok: bool, what: impl FnOnce() -> String) {
        if !ok {
            self.0.push(what());
        }
    }
}

fn expect_refused(f: &mut Failures, case: &Value, run: &Run, report: &Value) {
    let expect = &case["expect"];
    let code = expect["code"].as_str().expect("a code");
    let want = sorted(
        expect["findings"]
            .as_array()
            .expect("findings")
            .iter()
            .map(json_finding)
            .collect(),
    );
    match &run.result {
        Err(refusal) => {
            f.check(refusal.code.as_str() == code, || {
                format!("code {} not {code}", refusal.code.as_str())
            });
            f.check(sorted(refusal.findings.clone()) == want, || {
                format!("findings {:?} not {want:?}", refusal.findings)
            });
        }
        Ok(_) => f.0.push(format!("passed; expected {code}")),
    }
    f.check(report["code"] == code, || {
        format!("report code {}", report["code"])
    });
    let reported: Vec<Finding> = report["findings"]
        .as_array()
        .into_iter()
        .flatten()
        .map(json_finding)
        .collect();
    f.check(sorted(reported) == want, || "report findings differ".into());
    if expect["output"] == "unchanged" {
        let before: BTreeSet<String> = case["output_before"]
            .as_object()
            .expect("output_before")
            .keys()
            .cloned()
            .collect();
        let after: BTreeSet<String> = listing(&run.output).into_iter().collect();
        f.check(after == before, || format!("output changed: {after:?}"));
    } else {
        f.check(!run.output.exists(), || "output exists".into());
    }
}

fn expect_pass(
    f: &mut Failures,
    name: &str,
    case_dir: &Path,
    case: &Value,
    run: &Run,
    report: &Value,
) {
    if let Err(refusal) = &run.result {
        f.0.push(format!("refused {refusal:?}"));
        return;
    }
    f.check(report["result"] == "pass", || "report not pass".into());
    f.check(
        report["findings"].as_array().is_some_and(Vec::is_empty),
        || "report findings on a pass".into(),
    );
    let schema_path = run.schema.as_deref().expect("a pass has a schema");
    let schema: Value =
        serde_json::from_slice(&std::fs::read(schema_path).expect("schema")).expect("schema JSON");
    let declared: Vec<String> = schema["files"]
        .as_array()
        .expect("files")
        .iter()
        .map(|file| file["name"].as_str().expect("a name").to_owned())
        .collect();
    let mut want = declared.clone();
    want.push(RECEIPT.to_owned());
    want.sort();
    f.check(listing(&run.output) == want, || {
        format!("output holds {:?}", listing(&run.output))
    });
    // The export holds the checked data only: no key-value metadata and none
    // of the bytes the candidate carried beside its values.
    let hidden = hidden_strings(case["candidate"].as_object().expect("a candidate"));
    for file in &declared {
        let bytes = std::fs::read(run.output.join(file)).expect("an exported file");
        for value in &hidden {
            let found = bytes.windows(value.len()).any(|w| w == value.as_bytes());
            f.check(!found, || format!("{file} holds {value:?}"));
        }
        f.check(kv_metadata_free(bytes.clone()), || {
            format!("{file} has key-value metadata")
        });
        // The decoded columns are the schema's, less its check-only ones.
        let exported: Vec<String> = exported_columns(&schema)
            .into_iter()
            .find(|(name, _)| name == file)
            .map_or_else(Vec::new, |(_, exported)| exported);
        f.check(column_names(bytes) == exported, || {
            format!("{file} does not hold exactly the exported columns")
        });
    }
    let receipt_bytes = std::fs::read(run.output.join(RECEIPT)).expect("a receipt");
    let receipt: Value = serde_json::from_slice(&receipt_bytes).expect("receipt JSON");
    // Top-level keys in the format's order, read from the text: a parsed map
    // sorts its keys.
    let text = String::from_utf8_lossy(&receipt_bytes);
    let at = |key: &str| text.find(&format!("\n  \"{key}\":"));
    let order: Vec<_> = [
        "format",
        "format_version",
        "gate",
        "schema",
        "policy",
        "files",
    ]
    .iter()
    .map(|k| at(k))
    .collect();
    f.check(
        order.iter().all(Option::is_some)
            && order.is_sorted()
            && receipt.as_object().is_some_and(|m| m.len() == 6),
        || "receipt keys not the format's".into(),
    );
    // The receipt is exactly its golden: these fields and no others, so no
    // value can ride in it, and its hashes are those of the files as they
    // landed.
    let file_hash = |p: &Path| sha256(&std::fs::read(p).expect("bytes"));
    let policy_path = run.policy.as_deref().expect("a pass has a policy");
    let policy: Value =
        serde_json::from_slice(&std::fs::read(policy_path).expect("policy")).expect("policy JSON");
    let golden = serde_json::json!({
        "format": "reg-export-gate/receipt",
        "format_version": 1,
        "gate": { "implementation": "rust", "version": VERSION },
        "schema": {
            "name": schema["name"], "version": schema["version"],
            "sha256": file_hash(schema_path),
        },
        "policy": {
            "name": policy["name"], "version": policy["version"],
            "sha256": file_hash(policy_path),
        },
        "files": declared.iter().map(|name| serde_json::json!({
            "name": name, "sha256": file_hash(&run.output.join(name)),
        })).collect::<Vec<_>>(),
    });
    f.check(receipt == golden, || "receipt not its golden".into());
    // Deterministic: a second run over the same candidate writes the same bytes.
    let again = run_case(name, case_dir, case, 2);
    f.check(
        std::fs::read(again.output.join(RECEIPT)).ok() == Some(receipt_bytes),
        || "a second run wrote another receipt".into(),
    );
}

fn expect_case(name: &str, case_dir: &Path, case: &Value) -> Vec<String> {
    let mut f = Failures(Vec::new());
    let candidate = case["candidate"].as_object().expect("a candidate");
    let run = run_case(name, case_dir, case, 1);
    let report_path = run
        .root
        .join(".diagnostics")
        .join("export_gate_report.json");
    let report_text = std::fs::read_to_string(report_path).unwrap_or_default();
    let report: Value = serde_json::from_str(&report_text).unwrap_or(Value::Null);
    for value in author_strings(candidate, run.schema.as_deref())
        .into_iter()
        .chain(hidden_strings(candidate))
    {
        f.check(!report_text.contains(&value), || {
            format!("report holds {value:?}")
        });
    }
    // The report is exactly its golden: the gate version, the result, the
    // code and the findings the case expects (compared below), nothing more.
    let refused = case["expect"]["result"] == "refused";
    f.check(
        report.as_object().is_some_and(|r| {
            r.keys().map(String::as_str).collect::<BTreeSet<_>>()
                == BTreeSet::from(["code", "findings", "gate_version", "result"])
        }) && report["gate_version"] == VERSION
            && report["result"] == if refused { "refused" } else { "pass" }
            && (refused || report["code"].is_null())
            && report["findings"].as_array().is_some_and(|fs| {
                fs.iter().all(|f| {
                    f.as_object().is_some_and(|f| {
                        f.keys().map(String::as_str).collect::<BTreeSet<_>>()
                            == BTreeSet::from(["column", "file", "position", "rule"])
                    })
                })
            }),
        || format!("report not its golden: {report_text}"),
    );
    // Sentinel truths reach neither the report nor any file the gate wrote.
    let written: Vec<(String, Vec<u8>)> =
        std::iter::once(("report".to_owned(), report_text.clone().into_bytes()))
            .chain(
                output_snapshot(&run.output)
                    .into_iter()
                    .flatten()
                    .filter_map(|(name, bytes)| bytes.map(|b| (name, b))),
            )
            .collect();
    for (value, needles) in sentinel_needles(case) {
        for (name, bytes) in &written {
            f.check(!needles.iter().any(|n| holds(bytes, n)), || {
                format!("{name} holds the sentinel {value}")
            });
        }
    }
    // Pass or refusal, staging is left as it was.
    f.check(snapshot(&run.staging) == run.staged, || {
        "staging changed".into()
    });
    if refused {
        expect_refused(&mut f, case, &run, &report);
    } else {
        expect_pass(&mut f, name, case_dir, case, &run, &report);
    }
    expect_binary(&mut f, name, case_dir, case, &run);
    f.0
}

#[test]
fn corpus() {
    let mut cases: Vec<PathBuf> = std::fs::read_dir(CASES)
        .expect("the corpus")
        .map(|e| e.expect("a case").path())
        .collect();
    cases.sort();
    assert!(!cases.is_empty(), "no corpus case");
    let mut failures = Vec::new();
    for case_dir in &cases {
        let name = case_dir.file_name().unwrap().to_string_lossy().into_owned();
        let case: Value =
            serde_json::from_slice(&std::fs::read(case_dir.join("case.json")).expect("case.json"))
                .expect("JSON");
        // The reference implements every case: one written for a later gate
        // than this one fails rather than being skipped (the R runner skips).
        if let Some(min) = case["gate_min"].as_str()
            && semver(min) > semver(VERSION)
        {
            failures.push(format!("{name}: needs gate {min}, this is {VERSION}"));
            continue;
        }
        for failure in expect_case(&name, case_dir, &case) {
            failures.push(format!("{name}: {failure}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
