//! Steward provenance stays out of the global catalog (`crates/DESIGN.md`, "Steward
//! provenance"). Public curation text and published ids must not name a data steward;
//! evidence that does lives in the private inputs repository and is cited as
//! `private:ev-NNNN`, which must resolve in the manifest's `private-evidence` input.
//! Steward holdings builds are not checked: their curation is the steward's own.

use std::collections::BTreeSet;
use std::path::Path;

use reg_readers::curation::{LocatedClassification, LocatedStatement};
use serde_json::Value;

use crate::error::Result;

/// Field paths that hold source names, not curated text: selectors, delivered spellings
/// and a classification's reader locator stay as the source delivers them. Exact paths
/// from the top of an entry, so a nested key of the same name (a facet axis called
/// `spelling`) is still curated text.
const SOURCE_NAMES: [&str; 5] = [
    "subject",
    "object",
    "fields.spelling",
    "fact.fields.spelling",
    "value_set",
];

/// Every curated string of `value` with its dotted field path, source names excepted.
fn texts<'a>(value: &'a Value, path: &str, out: &mut Vec<(String, &'a str)>) {
    match value {
        Value::String(text) => out.push((path.to_owned(), text)),
        Value::Array(items) => items.iter().for_each(|item| texts(item, path, out)),
        Value::Object(fields) => {
            for (key, item) in fields {
                let path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if !SOURCE_NAMES.contains(&path.as_str()) {
                    texts(item, &path, out);
                }
            }
        }
        _ => {}
    }
}

/// Every key and string value of `value`.
fn decoded<'a>(value: &'a Value, out: &mut Vec<&'a str>) {
    match value {
        Value::String(text) => out.push(text),
        Value::Array(items) => items.iter().for_each(|item| decoded(item, out)),
        Value::Object(fields) => {
            for (key, item) in fields {
                out.push(key);
                decoded(item, out);
            }
        }
        _ => {}
    }
}

/// Refusals for one curated entry at `at`: steward tokens in its text and unresolved
/// private evidence references.
fn entry(at: &str, value: &Value, keys: Option<&BTreeSet<String>>, out: &mut Vec<String>) {
    let mut fields = Vec::new();
    texts(value, "", &mut fields);
    for (field, text) in fields {
        if field == "evidence"
            && let Some(key) = text.strip_prefix("private:")
        {
            match keys {
                _ if !reg_readers::private_evidence::is_key(key) => out.push(format!(
                    "{at}: private-evidence-ref: {text:?} is not private:ev-NNNN"
                )),
                None => out.push(format!(
                    "{at}: private-evidence-undeclared: {text:?} needs a private-evidence input in inputs.toml"
                )),
                Some(keys) if !keys.contains(key) => out.push(format!(
                    "{at}: private-evidence-unresolved: {key} is not in the private evidence file"
                )),
                Some(_) => {}
            }
            continue;
        }
        if let Some(token) = reg_readers::private_evidence::steward_name(text) {
            let fix = if field == "evidence" {
                "move it to the private evidence file and cite it as private:ev-NNNN"
            } else {
                "reword it without the steward"
            };
            out.push(format!(
                "{at}: steward-name: {field} names a steward ({token}); {fix}"
            ));
        }
    }
}

/// The global build's curated text, checked before any input is read; every refusal
/// at once, located.
///
/// # Errors
/// Each steward token in curated text, and each private evidence reference that is
/// malformed, undeclared or unresolved.
pub(crate) fn curation(
    curation: &Path,
    inputs: &Path,
    manifest: &[reg_readers::Input],
    statements: &[LocatedStatement],
    classifications: &[LocatedClassification],
) -> Result<()> {
    let declared: Vec<_> = manifest
        .iter()
        .filter(|input| input.reader == "private-evidence")
        .collect();
    let keys = match declared[..] {
        [] => None,
        [input] => Some(reg_readers::private_evidence::keys(inputs, &input.path)?),
        _ => return Err("private-evidence-input: inputs.toml declares more than one".into()),
    };
    let mut out = Vec::new();
    for s in statements {
        let at = format!("{}:{}", s.path.display(), s.line);
        entry(
            &at,
            &serde_json::to_value(&s.statement)?,
            keys.as_ref(),
            &mut out,
        );
    }
    let inventory = curation.join("classifications.toml");
    for c in classifications {
        let at = format!("{}:{}", inventory.display(), c.line);
        entry(
            &at,
            &serde_json::to_value(&c.classification)?,
            keys.as_ref(),
            &mut out,
        );
    }
    // Files with no source names: every decoded key and value is public text, and so is
    // every raw line, for the comments. The raw scan alone misses escapes (`\nSWECOV`).
    for file in ["inputs.toml", "tag-vocabulary.toml", "retired.toml"] {
        let path = curation.join(file);
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        // The loaders that read these files refuse invalid TOML first.
        if let Ok(table) = toml::from_str::<toml::Table>(&text) {
            let value = serde_json::to_value(&table)?;
            let mut strings = Vec::new();
            decoded(&value, &mut strings);
            for text in strings {
                if let Some(name) = reg_readers::private_evidence::steward_name(text) {
                    out.push(format!(
                        "{}: steward-name: {text:?} names a steward ({name}); reword it without the steward",
                        path.display()
                    ));
                }
            }
        }
        for (n, line) in text.lines().enumerate() {
            if let Some(name) = reg_readers::private_evidence::steward_name(line) {
                out.push(format!(
                    "{}:{}: steward-name: the line names a steward ({name}); reword it without the steward",
                    path.display(),
                    n + 1
                ));
            }
        }
    }
    refuse(out)
}

/// The global build's published ids (register public names, table and variable
/// slugs), checked; ledger ids are located at their `ids.toml` line.
///
/// # Errors
/// Each id with a steward token.
pub(crate) fn ids(curation: &Path, resolved: &[reg_resolve::Resolved]) -> Result<()> {
    let mut out = Vec::new();
    for r in resolved {
        let (provider, register) = (&r.register.provider, &r.register.register);
        let ledger = crate::ledger_path(curation, provider, register);
        let ids = std::iter::once(&r.public)
            .chain(r.tables.iter().filter_map(|t| t.slug.as_ref()))
            .chain(r.variables.iter().map(|v| &v.slug));
        for id in ids {
            if let Some(token) = reg_readers::private_evidence::steward_name(id) {
                let at = match ledger_line(&ledger, id) {
                    Some(line) => format!("{}:{line}", ledger.display()),
                    None => ledger.display().to_string(),
                };
                out.push(format!(
                    "{at}: steward-name: id {id:?} of {provider}/{register} names a steward ({token}); switch it, with no retirement"
                ));
            }
        }
    }
    refuse(out)
}

/// The `ids.toml` line that states `id`: the `public` name, a `[tables]` value or a
/// `[variables]` key. `None` for an id the ledger does not state (a slugified handle).
fn ledger_line(path: &Path, id: &str) -> Option<usize> {
    #[derive(serde::Deserialize)]
    struct Spans {
        public: Option<toml::Spanned<String>>,
        #[serde(default)]
        tables: std::collections::BTreeMap<String, toml::Spanned<String>>,
        #[serde(default)]
        variables: std::collections::BTreeMap<toml::Spanned<String>, toml::Value>,
    }
    let text = std::fs::read_to_string(path).ok()?;
    let spans: Spans = toml::from_str(&text).ok()?;
    let span = spans
        .public
        .iter()
        .chain(spans.tables.values())
        .chain(spans.variables.keys())
        .find(|stated| stated.get_ref() == id)?
        .span();
    Some(reg_readers::curation::Lines::new(&text).at(span.start))
}

fn refuse(mut out: Vec<String>) -> Result<()> {
    if out.is_empty() {
        return Ok(());
    }
    out.sort();
    out.dedup();
    Err(out.join("\n").into())
}
