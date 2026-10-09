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
        "boolean" => json!({"type": "boolean"}),
        "storage_id" => json!({"type": "string", "pattern": "^-?[0-9]+$"}),
        _ => panic!("map parameter type {ty:?}"),
    }
}

/// Every served operation is a row of `operations.toml` with the same route and
/// parameters, every served download a `[[download]]` row with the same route and
/// media type, taking its operation's parameters plus its route's other
/// placeholders, and `meta.contract_version` is the table's. A parameter the route
/// names is a path parameter. Fails when a route, parameter, its location or
/// optionality, or a media type drifts on either side. (Completeness per slice joins
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
    let downloads: BTreeMap<&str, &toml::Table> = table["download"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let row = row.as_table().unwrap();
            (row["route"].as_str().unwrap(), row)
        })
        .collect();
    let openapi = serde_json::to_value(ops::openapi("0")).unwrap();
    let mut served = BTreeSet::new();
    for (path, item) in openapi["paths"].as_object().unwrap() {
        for (method, operation) in item.as_object().unwrap() {
            let route = format!("{} {path}", method.to_uppercase());
            let (name, mut params) = if let Some(id) = operation["operationId"].as_str() {
                // A route without some of its operation's path parameters carries
                // the name with `_without_` and them (`show_without_ref`).
                let name = id.split("_without_").next().unwrap();
                let row = rows
                    .get(name)
                    .unwrap_or_else(|| panic!("{name} is not in the table"));
                assert!(
                    row["http"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|r| r.as_str() == Some(&route)),
                    "{route}"
                );
                // A parameter another of the row's routes names in its path is a
                // path parameter only (`show`'s `ref` on `GET /api/catalog`), and
                // required on the route that names it, as OpenAPI requires of any
                // path parameter.
                let mut params = expected_params(row);
                params.retain(|param, _| {
                    let placeholder = format!("{{{param}}}");
                    path.contains(&placeholder)
                        || !row["http"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|r| r.as_str().unwrap().contains(&placeholder))
                });
                (name, params)
            } else {
                let download = downloads
                    .get(route.as_str())
                    .unwrap_or_else(|| panic!("{route} is not a download"));
                let media_type = download["media_type"].as_str().unwrap();
                assert!(
                    operation["responses"]["200"]["content"]
                        .get(media_type)
                        .is_some(),
                    "{route}"
                );
                let name = download["operation"].as_str().unwrap();
                (name, expected_params(rows[name]))
            };
            for placeholder in path.split('/').filter_map(|s| s.strip_prefix('{')) {
                let placeholder = placeholder.trim_end_matches('}');
                params
                    .entry(placeholder.to_owned())
                    .or_insert_with(|| json!({"required": true, "schema": param_schema("string")}));
            }
            let expected: BTreeMap<String, Value> = params
                .into_iter()
                .map(|(param, mut value)| {
                    let located = if path.contains(&format!("{{{param}}}")) {
                        "path"
                    } else {
                        "query"
                    };
                    value["in"] = located.into();
                    if located == "path" {
                        value["required"] = true.into();
                    }
                    (param, value)
                })
                .collect();
            let actual: BTreeMap<String, Value> = operation["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    (
                        p["name"].as_str().unwrap().to_owned(),
                        json!({"in": p["in"], "required": p["required"], "schema": p["schema"]}),
                    )
                })
                .collect();
            assert_eq!(actual, expected, "{route}");
            served.insert(name.to_owned());
        }
    }
    assert!(served.contains("context") && served.contains("search"));
}

/// An operation row's `params` as `{required, schema}` by name.
fn expected_params(row: &toml::Table) -> BTreeMap<String, Value> {
    row["params"]
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
        .collect()
}
