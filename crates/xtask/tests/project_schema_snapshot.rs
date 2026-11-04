//! The plugin's `project_data.json` JSON Schema equals the one cut from the committed
//! `openapi.json`: `ProjectData` plus every component it reaches, as `$defs`.
//!
//! Regenerate with `REG_META_BLESS=1 cargo test -p xtask --test project_schema_snapshot`.

use std::collections::BTreeMap;
use std::path::Path;

use xtask::json::Json;

const PREFIX: &str = "#/components/schemas/";

/// Collects the component each `$ref` under `node` names, transitively, into `defs`.
fn collect(node: &Json, schemas: &Json, defs: &mut BTreeMap<String, Json>) {
    match node {
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => {}
        Json::Array(items) => items.iter().for_each(|item| collect(item, schemas, defs)),
        Json::Object(members) => {
            for (key, value) in members {
                let target = match value {
                    Json::String(s) if key == "$ref" => s.strip_prefix(PREFIX),
                    _ => None,
                };
                let Some(name) = target else {
                    collect(value, schemas, defs);
                    continue;
                };
                if !defs.contains_key(name) {
                    let schema = schemas
                        .get(name)
                        .unwrap_or_else(|| panic!("openapi.json: no component {name}"));
                    defs.insert(name.to_owned(), schema.clone());
                    collect(schema, schemas, defs);
                }
            }
        }
    }
}

/// Fails when a type under `ProjectData` changes without the plugin's schema file.
/// Key order is the contract's own; `$defs` is sorted by name.
#[test]
fn project_schema_is_current() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let openapi: Json = serde_json::from_str(
        &std::fs::read_to_string(root.join("crates/reg-meta/openapi.json")).unwrap(),
    )
    .unwrap();
    let schemas = openapi
        .get("components")
        .and_then(|c| c.get("schemas"))
        .unwrap();
    let project_data = schemas.get("ProjectData").unwrap();
    let mut defs = BTreeMap::new();
    collect(project_data, schemas, &mut defs);
    defs.remove("ProjectData");

    let Json::Object(project_members) = project_data else {
        panic!("ProjectData is not an object");
    };
    let leaf = |s: &str| Json::String(s.to_owned());
    let mut members = vec![
        (
            "$schema".to_owned(),
            leaf("https://json-schema.org/draft/2020-12/schema"),
        ),
        ("title".to_owned(), leaf("project_data.json")),
    ];
    members.extend(project_members.iter().cloned());
    members.push(("$defs".to_owned(), Json::Object(defs.into_iter().collect())));
    let rendered = serde_json::to_string_pretty(&Json::Object(members))
        .unwrap()
        .replace(&format!("\"{PREFIX}"), "\"#/$defs/")
        + "\n";

    let path = root.join("plugins/microdata-tools-se/skills/register-project/project_data.schema.json");
    if std::env::var_os("REG_META_BLESS").is_some() {
        std::fs::write(&path, &rendered).unwrap();
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == rendered,
        "{} is stale; regenerate it with REG_META_BLESS=1 cargo test -p xtask --test project_schema_snapshot",
        path.display()
    );
}
