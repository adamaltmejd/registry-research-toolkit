//! Frame JSONL: the frame itself as an input, written by hand (conformance fixtures).
//! One record per line, as `reg-frame` names its fields, with three differences: the
//! reader assigns every `locator` (the file and line), a `value_set` line states `items`
//! and `names` and the reader computes its `hash`, and an occurrence names its value
//! sets (`valueset`, `population_valuesets[].valueset`) instead of hashing them. Blank
//! lines and lines starting with `#` are skipped. The reader has no config.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_frame::{Frame, Item, Locator, Record, ValueSet};
use serde_json::{Map, Value};

use crate::{ReadError, ReadOutput, error};

/// Read every `*.jsonl` directly in `rel` (relative to the inputs root), in name order.
///
/// # Errors
/// An unreadable or empty directory, a line that is not a record, a `locator`, `also`
/// or `hash` the reader assigns, or a value-set name that matches none or several.
pub fn read(root: &Path, rel: &str) -> Result<ReadOutput, ReadError> {
    let mut names: Vec<String> = std::fs::read_dir(root.join(rel))
        .map_err(|e| error(rel, e.to_string()))?
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| Path::new(name).extension().is_some_and(|e| e == "jsonl"))
        .collect();
    names.sort();
    if names.is_empty() {
        return Err(error(rel, "no .jsonl file"));
    }
    let mut out = ReadOutput::default();
    let mut lines = Vec::new();
    for name in names {
        let path = format!("{rel}/{name}");
        let text =
            std::fs::read_to_string(root.join(&path)).map_err(|e| error(&path, e.to_string()))?;
        for (i, line) in text.lines().enumerate() {
            let n = i + 1;
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let located = |message: &str| error(&path, format!("line {n}: {message}"));
            let Value::Object(mut object) =
                serde_json::from_str(line).map_err(|e| located(&e.to_string()))?
            else {
                return Err(located("not a JSON object"));
            };
            if object.contains_key("locator") || object.contains_key("also") {
                return Err(located("locator is assigned by the reader"));
            }
            let locator = Locator {
                input: path.clone(),
                at: format!("line={n}"),
            };
            object.insert("locator".to_owned(), locator_value(&locator));
            lines.push((path.clone(), n, locator, object));
        }
        out.files.push(path);
    }

    // First pass: value sets, so the second pass can resolve names to their hashes.
    let mut records = Vec::new();
    let mut by_name: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (path, n, locator, object) in &lines {
        if object.get("kind").and_then(Value::as_str) != Some("value_set") {
            continue;
        }
        let set = value_set(object, locator.clone())
            .map_err(|m| error(path, format!("line {n}: {m}")))?;
        for name in &set.names {
            by_name
                .entry(name.clone())
                .or_default()
                .insert(set.hash.clone());
        }
        records.push(Record::ValueSet(set));
    }

    // Second pass: every other record, its value-set names replaced by hashes.
    for (path, n, _, mut object) in lines {
        let located = |message: String| error(&path, format!("line {n}: {message}"));
        if object.get("kind").and_then(Value::as_str) == Some("value_set") {
            continue;
        }
        if let Some(Value::String(name)) = object.get("valueset") {
            let hash = resolve(&by_name, name).map_err(located)?;
            object.insert("valueset".to_owned(), Value::String(hash));
        }
        if let Some(Value::Array(populations)) = object.get_mut("population_valuesets") {
            for population in populations {
                if let Some(Value::String(name)) = population.get("valueset") {
                    let hash = resolve(&by_name, name).map_err(located)?;
                    population["valueset"] = Value::String(hash);
                }
            }
        }
        let record =
            serde_json::from_value(Value::Object(object)).map_err(|e| located(e.to_string()))?;
        records.push(record);
    }
    out.frame = Frame::new(records);
    Ok(out)
}

fn locator_value(locator: &Locator) -> Value {
    serde_json::to_value(locator).expect("a locator serializes")
}

/// The value set a `value_set` line states. It cannot go through `Record`, which
/// requires the `hash` this computes.
fn value_set(object: &Map<String, Value>, locator: Locator) -> Result<ValueSet, String> {
    if object.contains_key("hash") {
        return Err("hash is assigned by the reader".to_owned());
    }
    if let Some(key) = object
        .keys()
        .find(|k| !["kind", "items", "names", "locator"].contains(&k.as_str()))
    {
        return Err(format!("unknown field `{key}`"));
    }
    let field = |key: &str| object.get(key).cloned().unwrap_or(Value::Null);
    let items: Vec<Item> =
        serde_json::from_value(field("items")).map_err(|e| format!("items: {e}"))?;
    let names: BTreeSet<String> = match object.get("names") {
        None => BTreeSet::new(),
        Some(names) => serde_json::from_value(names.clone()).map_err(|e| format!("names: {e}"))?,
    };
    let mut set = ValueSet::new(items, None, locator);
    set.names = names;
    Ok(set)
}

/// The hash of the one value set named `name`.
fn resolve(by_name: &BTreeMap<String, BTreeSet<String>>, name: &str) -> Result<String, String> {
    let mut hashes = by_name.get(name).into_iter().flatten();
    match (hashes.next(), hashes.next()) {
        (None, _) => Err(format!("no value set named {name:?}")),
        (Some(hash), None) => Ok(hash.clone()),
        _ => Err(format!("value set name {name:?} is ambiguous")),
    }
}
