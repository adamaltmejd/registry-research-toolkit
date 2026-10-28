//! `reg-frame`: the source frame, the one record shape every reader produces
//! (`crates/DESIGN.md`, "The catalog builder").
//!
//! A frame states registers, tables, stated deliveries (releases), documented columns
//! (occurrences), value sets, and the links and notices the source states between and
//! about them, each with a locator back to its input. Nothing here names a provider or a
//! file format: provider coordinates survive only as opaque strings (source keys, handles,
//! locators). Dates are inclusive ISO days, the convention of `reg-core`'s interval
//! algebra; an absent bound is unstated, never invented.
//!
//! Frames serialize as canonical JSONL: one record per line, keys sorted, no floats,
//! records sorted. Two pins of one input give two diffable frames.

pub mod address;
pub mod statement;

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The first and last day of open time: an unstated bound, once a stage must name one.
pub const OPEN_START: &str = "0001-01-01";
pub const OPEN_END: &str = "9999-12-31";

/// Where a fact was stated: the input (a path relative to the inputs root) and a
/// position inside it, both opaque to everything but the reader that wrote them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Locator {
    pub input: String,
    pub at: String,
}

/// The closed vocabulary of dtype families carried by the source frame and curation.
/// Width is stated separately as length; aliases retain their original spelling.
#[must_use]
pub fn type_family(dtype: &str) -> Option<&'static str> {
    match dtype {
        "int" | "integer" => Some("integer"),
        "decimal" => Some("decimal"),
        "char" | "text" => Some("text"),
        "date" => Some("date"),
        "datetime" => Some("datetime"),
        "boolean" => Some("boolean"),
        "geometry" => Some("geometry"),
        "uuid" => Some("uuid"),
        "binary" => Some("binary"),
        "unspecified" => Some("unspecified"),
        "identifier" => Some("identifier"),
        _ => None,
    }
}

/// A register as its source states it. `register` is the source handle; with no
/// `bind` statement it is also the catalog slug.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// A population a release states, in the source's order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Population {
    pub name: String,
    pub definition: Option<String>,
    /// The stated reference day, when the source gives one day.
    pub day: Option<String>,
}

/// A delivery of one table as its source states it. Never synthesized. `release` is
/// the source handle. The reader's name grammar places `name` in time: `from`, `to`
/// and `grain` are all stated or, for a name it cannot place, all absent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub provider: String,
    pub register: String,
    pub table: String,
    pub release: String,
    pub name: String,
    pub description: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    /// `year`, `half`, `quarter`, `month`, `day` or `span`.
    pub grain: Option<String>,
    /// `final`, `preliminary` or `unstated`.
    pub edition: String,
    /// Words of the name beside its period, which tell same-period deliveries apart.
    pub qualifier: Option<String>,
    #[serde(default)]
    pub populations: Vec<Population>,
    pub locator: Locator,
}

/// A value set an occurrence states for one of its release's populations, when its
/// rows disagree (`population` indexes [`Release::populations`]). Rows of one
/// population that name different sets give that population several, as alternatives.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PopulationValueSet {
    pub population: usize,
    pub valueset: String,
}

/// One column as documented over one stated period. `column` is the exact spelling;
/// empty, the source documents the variable in this period without a column (a gap
/// marker). `source_key` is the reader's opaque identity evidence: the provider's
/// variable id, else the folded name.
///
/// An occurrence of a release takes the release's period and states no bounds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Occurrence {
    pub provider: String,
    pub register: String,
    pub table: String,
    pub release: Option<String>,
    pub column: String,
    pub source_key: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub label: Option<String>,
    pub definition: Option<String>,
    pub operational_definition: Option<String>,
    pub unit: Option<String>,
    /// Normalized by the reader's type table: `int`, `decimal`, `char`, `date`,
    /// `datetime`; the literal when the table has no entry.
    pub dtype: Option<String>,
    pub length: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// The content hash of a value set stated in the same frame.
    pub valueset: Option<String>,
    /// Value sets that differ by population, in place of `valueset`.
    #[serde(default)]
    pub population_valuesets: Vec<PopulationValueSet>,
    /// `false` when the source states the column carries no codes (a number or free
    /// text); unstated otherwise.
    pub coded: Option<bool>,
    /// A code system the source names but the frame cannot resolve (a link, a
    /// classification name). Evidence only: classification references are curated.
    pub code_system: Option<String>,
    pub sensitive: Option<bool>,
    pub identifier: Option<bool>,
    pub locator: Locator,
    /// Other input rows stating the same occurrence, sorted.
    #[serde(default)]
    pub also: Vec<Locator>,
    /// Fields on which those rows disagree, sorted; the first row's value stands as
    /// provisional, and resolve reports a source conflict (F6).
    #[serde(default)]
    pub conflicts: Vec<String>,
}

/// One code of a value set; `parent` is another item's code.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Item {
    pub code: String,
    pub label: Option<String>,
    pub parent: Option<String>,
}

/// A code list as stated, with the names the source gives it. Equal content is one
/// value set: [`ValueSet::hash`] is its address.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueSet {
    pub hash: String,
    pub items: Vec<Item>,
    #[serde(default)]
    pub names: BTreeSet<String>,
    pub locator: Locator,
}

impl ValueSet {
    /// The value set of `items` (sorted by code, the canonical order), addressed by
    /// the SHA-256 of their canonical JSON. Duplicate codes keep the first label. A
    /// parent that is not an item's code is dropped: the catalog holds a parent only as
    /// an item's ordinal, so the address must be of what the file can hold (a source
    /// naming a chapter code without listing it, e.g. ks87-p, loses that link).
    #[must_use]
    pub fn new(items: Vec<Item>, name: Option<String>, locator: Locator) -> Self {
        Self::counted(items, name, locator).0
    }

    /// [`ValueSet::new`], and how many items named a parent that was dropped, for a
    /// reader to report.
    #[must_use]
    pub fn counted(mut items: Vec<Item>, name: Option<String>, locator: Locator) -> (Self, usize) {
        items.sort_by(|a, b| a.code.cmp(&b.code));
        items.dedup_by(|b, a| a.code == b.code);
        let codes: BTreeSet<String> = items.iter().map(|i| i.code.clone()).collect();
        let mut dropped = 0;
        for item in &mut items {
            if item.parent.as_ref().is_some_and(|p| !codes.contains(p)) {
                item.parent = None;
                dropped += 1;
            }
        }
        let hash = hex(&Sha256::digest(canonical_line(&items)));
        let set = Self {
            hash,
            items,
            names: name.into_iter().collect(),
            locator,
        };
        (set, dropped)
    }
}

/// One end of a source-stated link, in source coordinates: the link stage resolves it to a
/// public id. `kind` is `register`, `table`, `release` or `variable`. Exactly one of `register`
/// (a register handle) and `register_name` (a register *name* as the source writes it, matched
/// by fold) is set. `table` is a table handle (table and release ends); `key` a release handle
/// (release) or a source key (variable).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub kind: String,
    pub provider: String,
    pub register: Option<String>,
    pub register_name: Option<String>,
    pub table: Option<String>,
    pub key: Option<String>,
}

/// A relation the source states: `continues` (the subject takes over from the object, from
/// `at`) or `derived_from` (the subject is derived from the object).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    /// Serialized as `relation`: the record tag owns `kind`.
    #[serde(rename = "relation")]
    pub kind: String,
    pub subject: Endpoint,
    pub object: Endpoint,
    /// The first day the relation holds (inclusive ISO day), when stated.
    pub at: Option<String>,
    /// The source's own words about it.
    pub note: Option<String>,
    pub locator: Locator,
}

/// A notice the source states about one entity (a series break, an end): shown to users.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notice {
    pub subject: Endpoint,
    pub at: Option<String>,
    pub text: String,
    pub locator: Locator,
}

/// One section of a parsed document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocSection {
    pub heading: Option<String>,
    pub page: Option<u32>,
    pub body: String,
}

/// What a document is about, as the source names it; `page` 0 is the whole document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocSubject {
    pub endpoint: Endpoint,
    pub page: u32,
}

/// A document the source publishes about its registers: a parsed text or a PDF.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Doc {
    /// SHA-256 (lowercase hex) of the original file's bytes.
    pub hash: String,
    /// `parsed` or `pdf`. Serialized as `doc_kind`: the record tag owns `kind`.
    #[serde(rename = "doc_kind")]
    pub kind: String,
    pub title: String,
    pub licence: String,
    /// The original file's size.
    pub bytes: u64,
    /// Where the original is published, when it has a URL of its own.
    pub url: Option<String>,
    /// `parsed` only, in order. Empty for `pdf`.
    #[serde(default)]
    pub sections: Vec<DocSection>,
    /// `pdf` only: the bytes as lowercase hex ([`hex`]), so the frame stays JSONL.
    pub content: Option<String>,
    #[serde(default)]
    pub subjects: Vec<DocSubject>,
    pub locator: Locator,
}

/// One frame record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Register(Register),
    Table(Table),
    Release(Release),
    ValueSet(ValueSet),
    Occurrence(Occurrence),
    Link(Link),
    Notice(Notice),
    Doc(Doc),
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

/// The bytes of lowercase hex `text`, or `None` when it is not hex (odd length, a
/// non-hex character, uppercase).
#[must_use]
pub fn unhex(text: &str) -> Option<Vec<u8>> {
    let digit = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    };
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks(2)
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
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
