//! `reg-frame`: the source frame, the one record shape every reader produces
//! (`crates/DESIGN.md`, "The catalog builder").
//!
//! A frame states registers, tables, documented columns (occurrences) and value sets,
//! each with a locator back to its input. Nothing here names a provider or a file
//! format: provider coordinates survive only as opaque strings (source keys, handles,
//! locators). Dates are inclusive ISO days, the convention of `reg-core`'s interval
//! algebra; an absent bound is unstated, never invented.
//!
//! Frames serialize as canonical JSONL: one record per line, keys sorted, no floats,
//! records sorted. Two pins of one input give two diffable frames.

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Where a fact was stated: the input (a path relative to the inputs root) and a
/// position inside it, both opaque to everything but the reader that wrote them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Locator {
    pub input: String,
    pub at: String,
}

/// A register as its source states it. `register` is the source handle; with no
/// `bind` statement it is also the catalog slug.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Register {
    pub provider: String,
    pub register: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub locator: Locator,
}

/// A table (a row shape) as its source states it. An empty `table` handle is the
/// register's one implicit table, used when the source states no tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Table {
    pub provider: String,
    pub register: String,
    pub table: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    /// `year`, `half`, `quarter`, `month`, `day` or `irregular`, from a stated
    /// frequency the reader's config recognises.
    pub cadence: Option<String>,
    pub locator: Locator,
}

/// One column as documented over one stated period. `column` is the exact spelling.
/// `source_key` is the reader's opaque identity evidence: the provider's variable id,
/// else the folded name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Occurrence {
    pub provider: String,
    pub register: String,
    pub table: String,
    pub column: String,
    pub source_key: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub label: Option<String>,
    pub definition: Option<String>,
    pub unit: Option<String>,
    /// Normalized by the reader's type table: `int`, `decimal`, `char`, `date`,
    /// `datetime`; the literal when the table has no entry.
    pub dtype: Option<String>,
    pub length: Option<i64>,
    /// The content hash of a value set stated in the same frame.
    pub valueset: Option<String>,
    /// A code system the source names but the frame cannot resolve (a link, a
    /// classification name). Evidence only: classification references are curated.
    pub code_system: Option<String>,
    pub sensitive: Option<bool>,
    pub identifier: Option<bool>,
    pub locator: Locator,
}

/// One code of a value set; `parent` is another item's code.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Item {
    pub code: String,
    pub label: Option<String>,
    pub parent: Option<String>,
}

/// A code list as stated, with the name the source gives it. Equal content is one
/// value set: [`ValueSet::hash`] is its address.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ValueSet {
    pub hash: String,
    pub items: Vec<Item>,
    pub name: Option<String>,
    pub locator: Locator,
}

impl ValueSet {
    /// The value set of `items` (sorted by code, the canonical order), addressed by
    /// the SHA-256 of their canonical JSON. Duplicate codes keep the first label.
    #[must_use]
    pub fn new(mut items: Vec<Item>, name: Option<String>, locator: Locator) -> Self {
        items.sort_by(|a, b| a.code.cmp(&b.code));
        items.dedup_by(|b, a| a.code == b.code);
        let hash = hex(&Sha256::digest(canonical_line(&items)));
        Self {
            hash,
            items,
            name,
            locator,
        }
    }
}

/// One frame record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Register(Register),
    Table(Table),
    ValueSet(ValueSet),
    Occurrence(Occurrence),
}

/// A reader's whole output for one input: its records, sorted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Frame {
    pub records: Vec<Record>,
}

impl Frame {
    /// Records in canonical order, duplicates removed.
    #[must_use]
    pub fn new(mut records: Vec<Record>) -> Self {
        records.sort();
        records.dedup();
        Self { records }
    }

    /// The canonical JSONL text: one record per line, keys sorted.
    #[must_use]
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        for record in &self.records {
            out.push_str(&canonical_line(record));
            out.push('\n');
        }
        out
    }
}

/// `value` as one line of canonical JSON: object keys sorted, no insignificant
/// whitespace. Frame types hold no floats (`crates/DESIGN.md`).
///
/// # Panics
/// Never: frame types serialize to JSON.
#[must_use]
pub fn canonical_line<T: Serialize>(value: &T) -> String {
    let mut tree = serde_json::to_value(value).expect("frame records serialize");
    tree.sort_all_objects();
    tree.to_string()
}

/// Lowercase hex of `bytes`.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}
