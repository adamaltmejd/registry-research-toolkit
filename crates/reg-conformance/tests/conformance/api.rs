//! The `api` corpus: readable request/expected cases against the Rust server, and the
//! startup refusals (conformance/README.md, "API cases").

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::common::http_case::{
    StepResponse, case_ports, fixture_source, json_eq, run_http_requests, select_json,
};
use crate::common::{case_dirs, read_json, read_toml, run_cases, server};

#[test]
fn api_cases() {
    let surface = reg_conformance::cases().join("api");
    let cases: Vec<_> = case_dirs(&surface)
        .into_iter()
        .filter(|case| case.join("request.json").is_file())
        .collect();
    run_cases("api", &cases, check_case);
}

fn check_case(case: &Path) -> Result<(), String> {
    let request = read_json(&case.join("request.json"))?;
    let expected = read_json(&case.join("expected.json"))?;
    if let Some(refusal) = expected.get("startup_error") {
        return check_refusal(&request, refusal);
    }
    let steps = request["requests"].as_array().ok_or("requests is a list")?;
    let responses = run_http_requests(steps, &case_ports(&request)?)?;
    let oracles = expected.as_array().ok_or("expected is a list")?;
    if oracles.len() != responses.len() {
        return Err(format!(
            "{} oracles for {} steps",
            oracles.len(),
            responses.len()
        ));
    }
    for (index, oracle) in oracles.iter().enumerate() {
        check_step(case, &responses, index, oracle)
            .map_err(|why| format!("step {index}: {why}"))?;
    }
    Ok(())
}

fn check_step(
    case: &Path,
    responses: &[StepResponse],
    index: usize,
    oracle: &Value,
) -> Result<(), String> {
    let response = &responses[index];
    let differ = |what: &str, got: &dyn std::fmt::Display, want: &dyn std::fmt::Display| {
        Err(format!("{what}: got {got}, expected {want}"))
    };
    if oracle["status"].as_u64() != Some(u64::from(response.status)) {
        return differ("status", &response.status, &oracle["status"]);
    }
    if let Some(want) = oracle.get("location") {
        let got = response
            .headers
            .get("location")
            .map_or(Value::Null, |l| Value::from(l.as_str()));
        if got != *want {
            return differ("location", &got, want);
        }
    }
    if let Some(want) = oracle.get("media_type")
        && want.as_str() != Some(response.media_type())
    {
        return differ("media_type", &response.media_type(), want);
    }
    for (name, want) in oracle
        .get("headers")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let got = response
            .headers
            .get(name)
            .map_or(Value::Null, |v| Value::from(v.as_str()));
        if got != *want {
            return differ(&format!("header {name}"), &got, want);
        }
    }
    if let Some(file) = oracle.get("bytes").and_then(Value::as_str) {
        let want = fs::read(case.join(file)).map_err(|e| format!("{file}: {e}"))?;
        if response.content != want {
            return Err(format!("bytes differ from {file}"));
        }
    }
    for (pointer, want) in oracle
        .get("json")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let got = select_json(&response.body, pointer)?;
        if !json_eq(&got, want) {
            return differ(pointer, &got, want);
        }
    }
    // An object's exact key set, for values no oracle can spell (a generation).
    for (pointer, want) in oracle
        .get("keys")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let mut got: Vec<String> = match select_json(&response.body, pointer)? {
            Value::Object(map) => map.keys().cloned().collect(),
            Value::Array(items) => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map_or_else(|| item.to_string(), str::to_owned)
                })
                .collect(),
            other => return Err(format!("keys {pointer}: {other} has no keys")),
        };
        got.sort();
        let got = Value::from(got);
        if got != *want {
            return differ(&format!("keys {pointer}"), &got, want);
        }
    }
    if let Some(members) = oracle.get("members") {
        check_members(responses, members)?;
    }
    Ok(())
}

/// A tie-order-free oracle across steps: `pointer` projected from each of `steps` in
/// turn and concatenated holds no repeats and exactly the `equals` set.
fn check_members(responses: &[StepResponse], members: &Value) -> Result<(), String> {
    let pointer = members["pointer"].as_str().ok_or("members pointer")?;
    let mut rows = Vec::new();
    for step in members["steps"].as_array().ok_or("members steps")? {
        let response = step
            .as_u64()
            .and_then(|s| responses.get(usize::try_from(s).ok()?))
            .ok_or(format!("members: no step {step}"))?;
        match select_json(&response.body, pointer)? {
            Value::Array(items) => rows.extend(items.iter().map(Value::to_string)),
            other => return Err(format!("members {pointer}: {other} is not a list")),
        }
    }
    let distinct: HashSet<&String> = rows.iter().collect();
    let equals: HashSet<String> = members["equals"]
        .as_array()
        .ok_or("members equals")?
        .iter()
        .map(Value::to_string)
        .collect();
    if distinct.len() != rows.len() || distinct != equals.iter().collect() {
        return Err(format!(
            "members {pointer}: got {rows:?}, expected {equals:?}"
        ));
    }
    Ok(())
}

/// The server refuses the case's artifact: it prints the error document as the last
/// line of stderr and exits with the code's `exit` status in `api/errors.toml`, with
/// the oracle's `fields` when it gives them. The `manifest` overrides are written to
/// the private copy's `meta` table, a `null` deleting the key; `serve.db: "absent"`
/// serves an empty directory instead of an artifact.
fn check_refusal(request: &Value, refusal: &Value) -> Result<(), String> {
    let scratch = reg_conformance::Scratch::new("refusal")?;
    let db = if request.pointer("/serve/db").and_then(Value::as_str) == Some("absent") {
        let absent = scratch.path().join("absent");
        fs::create_dir(&absent).map_err(|e| e.to_string())?;
        absent
    } else {
        let catalog = scratch.path().join("catalog");
        fs::create_dir(&catalog).map_err(|e| e.to_string())?;
        let path = catalog.join("reg_meta.db");
        fs::copy(
            reg_conformance::cached_reader_artifact(&fixture_source(request))?,
            &path,
        )
        .map_err(|e| e.to_string())?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).map_err(|e| e.to_string())?;
        let conn = rusqlite::Connection::open(&path).map_err(|e| e.to_string())?;
        for (key, value) in request
            .get("manifest")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            match value.as_str() {
                Some(value) => {
                    conn.execute("INSERT OR REPLACE INTO meta VALUES (?1, ?2)", (key, value))
                }
                None => conn.execute("DELETE FROM meta WHERE key = ?1", [key]),
            }
            .map_err(|e| format!("manifest {key}: {e}"))?;
        }
        catalog
    };
    let stderr_path = scratch.path().join("stderr");
    let stderr = fs::File::create(&stderr_path).map_err(|e| e.to_string())?;
    let mut child = Command::new(server::reg_meta())
        .args(server::serve_argv(&db))
        .current_dir(reg_conformance::repo())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .map_err(|e| format!("{}: {e}", server::reg_meta().display()))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("the server started instead of refusing".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let stderr = fs::read_to_string(&stderr_path).map_err(|e| e.to_string())?;
    let last = stderr.trim().lines().last().unwrap_or_default();
    let document: Value = serde_json::from_str(last)
        .map_err(|e| format!("stderr's last line is not JSON ({e}): {stderr}"))?;
    if document["code"] != refusal["code"] {
        return Err(format!(
            "code: got {}, expected {}: {stderr}",
            document["code"], refusal["code"]
        ));
    }
    if let Some(fields) = refusal.get("fields")
        && !json_eq(&document["fields"], fields)
    {
        return Err(format!(
            "fields: got {}, expected {fields}",
            document["fields"]
        ));
    }
    let code = refusal["code"].as_str().ok_or("startup_error code")?;
    let want = exit_status(code)?;
    if status.code().map(i64::from) != want {
        return Err(format!("exit: got {status}, expected {want:?}"));
    }
    Ok(())
}

/// The `exit` status `api/errors.toml` gives `code`, if any.
fn exit_status(code: &str) -> Result<Option<i64>, String> {
    let errors = read_toml(&reg_conformance::repo().join("conformance/api/errors.toml"))?;
    let entry = errors
        .get("code")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .find(|entry| entry.get("code").and_then(toml::Value::as_str) == Some(code))
        .ok_or(format!("{code} is not in errors.toml"))?;
    Ok(entry.get("exit").and_then(toml::Value::as_integer))
}
