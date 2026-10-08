//! The Rust server's contract equals the approved API spec in `conformance/api/`.

use std::collections::{BTreeMap, BTreeSet};

use reg_catalog::{CONTRACT_VERSION, Code, ops};
use serde_json::{Value, json};

fn spec(name: &str) -> toml::Table {
    let path = format!(
        "{}/../../conformance/api/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap().parse().unwrap()
}

/// Fails when a code is added to, dropped from or changed in one side only.
#[test]
fn error_enum_equals_errors_toml() {
    let table = spec("errors.toml");
    let expected: BTreeMap<String, Value> = table["code"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let row = row.as_table().unwrap();
            let get = |key: &str| row.get(key).map(|v| json!(v));
            (
                row["code"].as_str().unwrap().to_owned(),
                json!({
                    "class": get("class"),
                    "status": get("status"),
                    "exit": get("exit"),
                    "fields": get("fields").unwrap_or(json!([])),
                }),
            )
        })
        .collect();
    let actual: BTreeMap<String, Value> = Code::ALL
        .iter()
        .map(|code| {
            let s = code.spec();
            let some = |n: i64| (n != 0).then_some(n);
            (
                s.name.to_owned(),
                json!({
                    "class": s.class,
                    "status": some(s.status.into()),
                    "exit": some(s.exit.into()),
                    "fields": s.fields,
                }),
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

/// The type of a parameter in `operations.toml` (`scope?`) as the `OpenAPI` schema
/// the server must generate for it.
fn param_schema(ty: &str) -> Value {
    if let Some(members) = ty.strip_prefix("enum(").and_then(|t| t.strip_suffix(')')) {
        return json!({"type": "string", "enum": members.split('|').collect::<Vec<_>>()});
    }
    match ty {
        "scope" => json!({"$ref": "#/components/schemas/Scope"}),
        "string" | "ref" | "period" | "cursor" => json!({"type": "string"}),
        "limit" => json!({"type": "integer", "minimum": 1, "maximum": 200}),
        _ => panic!("map parameter type {ty:?}"),
    }
}

/// Every served operation is a row of `operations.toml` with the same route and
/// parameters, and `meta.contract_version` is the table's. Fails when a route,
/// parameter or optionality drifts on either side. (Completeness per slice joins
/// when slice 3a's last operation ships.)
#[test]
fn openapi_matches_operations_toml() {
    let table = spec("operations.toml");
    assert_eq!(table["contract_version"].as_str(), Some(CONTRACT_VERSION));
    let rows: BTreeMap<&str, &toml::Table> = table["operation"]
        .as_array()
        .unwrap()
        .iter()
        .map(|op| {
            let op = op.as_table().unwrap();
            (op["name"].as_str().unwrap(), op)
        })
        .collect();
    let openapi = serde_json::to_value(ops::openapi("0")).unwrap();
    let mut served = BTreeSet::new();
    for (path, item) in openapi["paths"].as_object().unwrap() {
        for (method, operation) in item.as_object().unwrap() {
            let name = operation["operationId"].as_str().unwrap();
            let row = rows
                .get(name)
                .unwrap_or_else(|| panic!("{name} is not in the table"));
            let route = format!("{} {path}", method.to_uppercase());
            assert!(
                row["http"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|r| r.as_str() == Some(&route)),
                "{route}"
            );
            let expected: BTreeMap<String, Value> = row["params"]
                .as_table()
                .unwrap()
                .iter()
                .map(|(param, ty)| {
                    let ty = ty.as_str().unwrap();
                    let optional = ty.ends_with('?');
                    let schema = param_schema(ty.trim_end_matches('?'));
                    (
                        param.clone(),
                        json!({"required": !optional, "schema": schema}),
                    )
                })
                .collect();
            let actual: BTreeMap<String, Value> = operation["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    assert_eq!(p["in"], "query");
                    let schema = p["schema"].clone();
                    (
                        p["name"].as_str().unwrap().to_owned(),
                        json!({"required": p["required"], "schema": schema}),
                    )
                })
                .collect();
            assert_eq!(actual, expected, "{name}");
            served.insert(name.to_owned());
        }
    }
    assert!(served.contains("context") && served.contains("search"));
}
