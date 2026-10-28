//! Classification CSVs: one code list per file, read as a value set named by the
//! file's stem. The header names of the code, label and parent columns come from
//! `curation/readers/classification-csv.toml`; other columns are not read in A
//! (item validity waits for classification versions, milestone B).

use std::path::Path;

use reg_frame::{Frame, Item, Locator, Record, ValueSet};
use serde::Deserialize;

use crate::{ReadError, ReadOutput, config, error, nonblank};

/// `curation/readers/classification-csv.toml`: accepted header names per column.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub code: Vec<String>,
    pub label: Vec<String>,
    pub parent: Vec<String>,
    /// Exact input locators whose declared hierarchy is flat.
    #[serde(default)]
    pub flat: Vec<String>,
}

impl Config {
    /// # Errors
    /// An unreadable or invalid config file.
    pub fn load(path: &Path) -> Result<Self, ReadError> {
        config(path)
    }
}

/// Read every `*.csv` directly in `rel` (relative to the inputs root), in name order.
///
/// # Errors
/// An unreadable directory or file, or a file with no recognised code column.
pub fn read(root: &Path, rel: &str, config: &Config) -> Result<ReadOutput, ReadError> {
    let dir = root.join(rel);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .map_err(|e| error(rel, e.to_string()))?
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| Path::new(name).extension().is_some_and(|e| e == "csv"))
        .collect();
    names.sort();
    let mut out = ReadOutput::default();
    let mut records = Vec::new();
    let mut orphaned = 0;
    for name in names {
        let path = format!("{rel}/{name}");
        let mut reader =
            csv::Reader::from_path(root.join(&path)).map_err(|e| error(&path, e.to_string()))?;
        let headers = reader.headers().map_err(|e| error(&path, e.to_string()))?;
        let find = |names: &[String]| headers.iter().position(|h| names.iter().any(|n| n == h));
        let code = find(&config.code)
            .ok_or_else(|| error(&path, "no code column (see classification-csv.toml)"))?;
        let label = find(&config.label);
        let parent = if config.flat.contains(&path) {
            None
        } else {
            find(&config.parent)
        };
        let mut items = Vec::new();
        for record in reader.records() {
            let record = record.map_err(|e| error(&path, e.to_string()))?;
            let cell = |i: Option<usize>| i.and_then(|i| record.get(i)).and_then(nonblank);
            let Some(code) = cell(Some(code)) else {
                continue;
            };
            items.push(Item {
                code,
                label: cell(label),
                parent: cell(parent),
            });
        }
        let stem = name.trim_end_matches(".csv").to_owned();
        let locator = Locator {
            input: path.clone(),
            at: String::new(),
        };
        let (set, dropped) = ValueSet::counted(items, Some(stem), locator);
        orphaned += dropped;
        records.push(Record::ValueSet(set));
        out.files.push(path);
    }
    if orphaned > 0 {
        out.warnings.push(format!(
            "{rel}: {orphaned} items name a parent the list does not hold (dropped)"
        ));
    }
    out.frame = Frame::new(records);
    Ok(out)
}
