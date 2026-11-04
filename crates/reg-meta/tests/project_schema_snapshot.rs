//! The plugin's `project_data.json` JSON Schema equals the one cut from the committed
//! `openapi.json`: `ProjectData` plus every component it reaches, as `$defs`.
//!
//! Regenerate with `REG_META_BLESS=1 cargo test -p reg-meta --test project_schema_snapshot`.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::Serialize;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq, Serializer};

const PREFIX: &str = "#/components/schemas/";

/// A JSON value that keeps object members in document order. `serde_json::Value` sorts
/// them unless its `preserve_order` feature is on, and that feature would unify into
/// every crate built alongside this one; the schema keeps the contract's own order.
#[derive(Clone)]
enum Json {
    Leaf(serde_json::Value),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
}

impl Serialize for Json {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Json::Leaf(value) => value.serialize(serializer),
            Json::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Json::Object(members) => {
                let mut map = serializer.serialize_map(Some(members.len()))?;
                for (key, value) in members {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = Json;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_unit<E>(self) -> Result<Json, E> {
                Ok(Json::Leaf(serde_json::Value::Null))
            }
            fn visit_bool<E>(self, b: bool) -> Result<Json, E> {
                Ok(Json::Leaf(b.into()))
            }
            fn visit_i64<E>(self, n: i64) -> Result<Json, E> {
                Ok(Json::Leaf(n.into()))
            }
            fn visit_u64<E>(self, n: u64) -> Result<Json, E> {
                Ok(Json::Leaf(n.into()))
            }
            fn visit_f64<E>(self, n: f64) -> Result<Json, E> {
                Ok(Json::Leaf(n.into()))
            }
            fn visit_str<E>(self, s: &str) -> Result<Json, E> {
                Ok(Json::Leaf(s.into()))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Json::Array(items))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
                let mut members = Vec::new();
                while let Some(member) = map.next_entry()? {
                    members.push(member);
                }
                Ok(Json::Object(members))
            }
        }
        deserializer.deserialize_any(JsonVisitor)
    }
}

/// Collects the component each `$ref` under `node` names, transitively, into `defs`.
fn collect(node: &Json, schemas: &Json, defs: &mut BTreeMap<String, Json>) {
    match node {
        Json::Leaf(_) => {}
        Json::Array(items) => items.iter().for_each(|item| collect(item, schemas, defs)),
        Json::Object(members) => {
            for (key, value) in members {
                let target = match value {
                    Json::Leaf(serde_json::Value::String(s)) if key == "$ref" => {
                        s.strip_prefix(PREFIX)
                    }
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
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let openapi: Json =
        serde_json::from_str(&std::fs::read_to_string(crate_dir.join("openapi.json")).unwrap())
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
    let leaf = |s: &str| Json::Leaf(s.into());
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

    let path = crate_dir
        .join("../../plugins/microdata-tools-se/skills/register-project/project_data.schema.json");
    if std::env::var_os("REG_META_BLESS").is_some() {
        std::fs::write(&path, &rendered).unwrap();
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == rendered,
        "{} is stale; regenerate it with REG_META_BLESS=1 cargo test -p reg-meta --test project_schema_snapshot",
        path.display()
    );
}
