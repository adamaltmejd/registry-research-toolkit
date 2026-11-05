//! `reg-core`: contracts shared by the Rust runtime, the build (`reg-build`) and,
//! through `reg-core-wasm`, the SPA.
//!
//! This crate holds the FQID and period grammars ([`Fqid`], [`Period`]), the text
//! folds, and the `project_data.json` types ([`project`]) with their structural
//! validator ([`validate_structural`]), and the interval algebra over ISO date
//! intervals ([`merge`], [`render`]): the shared contracts of `crates/DESIGN.md`, "One
//! implementation per contract".
//!
//! All Unicode data is on one version, [`UNICODE_VERSION`] (17.0.0): Rust std's
//! `to_lowercase` and `White_Space`, `unicode-properties` 0.1.4 (general category) and
//! the generated case-folding table.
//!
//! The oracles are `conformance/cases/folds/`, `conformance/cases/grammar/` and
//! `crates/reg-core/tests/project/corpus/`.

/// The catalog file's schema version: `PRAGMA user_version` and
/// `meta.schema_version` (as decimal text). Readers admit exactly this version.
pub const CATALOG_SCHEMA_VERSION: u32 = 10;

/// A holdings file's physical schema. Its semantic tables retain the catalog schema.
pub const HOLDINGS_SCHEMA_VERSION: u32 = 6;

mod case_folding;
mod digest;
mod grammar;
mod interval;
pub mod issue;
pub mod manifest;
pub mod project;
mod structural;
mod validation;

use case_folding::CASE_FOLDING;
pub use case_folding::UNICODE_VERSION;
pub use digest::{hex, is_sha256_hex, sha256_hex, unhex};
pub use grammar::{
    Fqid, GrammarError, Period, PeriodToken, Term, add_iso_months, is_slug, iso_day_number,
    next_iso_day, period_token_for_bounds, prev_iso_day,
};
pub use interval::{Interval, days, gaps, hull, intersect, merge, overlap, render};
pub use structural::{q as quote, validate_structural};
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};
pub use validation::{IssueLevel, ValidationIssue, ValidationResult};

/// Python `str.isspace()`: `White_Space` plus the four information separators
/// U+001C..U+001F, which Python treats as whitespace and Unicode does not.
fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `str.split()` with no arguments.
fn py_split(s: &str) -> impl Iterator<Item = &str> {
    s.split(py_isspace).filter(|t| !t.is_empty())
}

/// PCRE `\p{L}`: general category L*. Not `char::is_alphabetic`, which also admits
/// `Other_Alphabetic` marks and symbols (U+0345, circled letters U+24B6..).
#[must_use]
pub fn is_letter(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Letter
}

/// Whether a character belongs to a Unicode punctuation category.
#[must_use]
pub fn is_punctuation(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Punctuation
}

/// Python `str.casefold()`: full case folding (`CaseFolding.txt` statuses C and F).
fn case_fold(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match CASE_FOLDING.binary_search_by_key(&c, |&(from, _)| from) {
            Ok(i) => out.push_str(CASE_FOLDING[i].1),
            Err(_) => out.push(c),
        }
    }
    out
}

/// `fold_identity`: Unicode lowercase with the `Final_Sigma` rule, no normalization.
///
/// The column identity rule, today's `py_lower` (Python `str.lower()`).
#[must_use]
pub fn fold_identity(s: &str) -> String {
    s.to_lowercase()
}

/// A label's comparison key: case- and spacing-insensitive ([`fold_identity`] over the
/// whitespace-normalised label).
#[must_use]
pub fn label_key(label: &str) -> String {
    fold_identity(&label.split_whitespace().collect::<Vec<_>>().join(" "))
}

/// What two overlapping occurrences of one variable must agree on (crates/DESIGN.md
/// "Segments hold whole occurrences"). Resolve exempts family-only disagreement under
/// one source key and folded spelling. The spelling is not part of the form; the label
/// compares by [`label_key`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Form {
    pub label: Option<String>,
    pub dtype: Option<String>,
    pub valueset: Option<String>,
}

impl Form {
    /// The form of an occurrence's label, type family and value set.
    #[must_use]
    pub fn new(label: Option<&str>, dtype: Option<&str>, valueset: Option<&str>) -> Self {
        Self {
            label: label.map(label_key),
            dtype: dtype.map(str::to_owned),
            valueset: valueset.map(str::to_owned),
        }
    }
}

/// The search query normalizer: split on whitespace, join with one space, case fold.
#[must_use]
pub fn normalized_search_query(s: &str) -> String {
    case_fold(&py_split(s).collect::<Vec<_>>().join(" "))
}
