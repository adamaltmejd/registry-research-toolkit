//! `reg-core`: contracts shared by the Rust runtime and, through bindings, the build.
//!
//! This crate holds the FQID and period grammars ([`Fqid`], [`Period`]), the text
//! folds, and the `project_data.json` types ([`project`]) with their structural
//! validator ([`validate_structural`]), and the interval algebra over ISO date
//! intervals ([`merge`], [`render`]): the shared contracts of `crates/DESIGN.md`, "One
//! implementation per contract".
//!
//! All Unicode data is on one version, [`UNICODE_VERSION`] (17.0.0): Rust std's
//! `to_lowercase` and `White_Space`, `unicode-normalization` 0.1.25 (NFKD, canonical
//! combining class), `unicode-properties` 0.1.4 (general category) and the generated
//! case-folding table.
//!
//! The oracles are `conformance/cases/folds/`, `conformance/cases/grammar/` and
//! `crates/reg-core/tests/project/corpus/`.

/// The catalog file's schema version (schema v4 §3.2): `PRAGMA user_version` and
/// `meta.schema_version` (as decimal text). Readers admit exactly this version.
pub const CATALOG_SCHEMA_VERSION: u32 = 4;

mod case_folding;
mod grammar;
mod interval;
pub mod project;
mod structural;
mod validation;

use case_folding::CASE_FOLDING;
pub use case_folding::UNICODE_VERSION;
pub use grammar::{
    Fqid, GrammarError, Period, PeriodToken, Term, add_iso_months, is_slug, iso_day_number,
    next_iso_day, period_token_for_bounds, prev_iso_day,
};
pub use interval::{Interval, gaps, intersect, merge, overlap, render};
pub use structural::{q as quote, quoted_list, validate_structural};
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;
use unicode_properties::{GeneralCategory, GeneralCategoryGroup, UnicodeGeneralCategory};
pub use validation::{IssueLevel, ValidationIssue, ValidationResult};

/// Python `str.isspace()`: `White_Space` plus the four information separators
/// U+001C..U+001F, which Python treats as whitespace and Unicode does not.
fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `str.strip()` with no arguments.
#[must_use]
pub fn py_strip(s: &str) -> &str {
    s.trim_matches(py_isspace)
}

/// The class `\d` in Python's `re`: general category Nd (decimal digits), not the
/// superscripts, fractions and other numbers `char::is_numeric` also admits.
#[must_use]
pub fn py_isdecimal(c: char) -> bool {
    c.general_category() == GeneralCategory::DecimalNumber
}

/// Python `str.split()` with no arguments.
fn py_split(s: &str) -> impl Iterator<Item = &str> {
    s.split(py_isspace).filter(|t| !t.is_empty())
}

/// The class `[^\W_]` in Python's `re`: general category L* or N*. Not
/// `char::is_alphanumeric`, whose `Alphabetic` property also admits `Other_Alphabetic`
/// marks and symbols (U+0345, circled letters U+24B6..).
fn py_isalnum(c: char) -> bool {
    matches!(
        c.general_category_group(),
        GeneralCategoryGroup::Letter | GeneralCategoryGroup::Number
    )
}

/// PCRE `\p{L}`: general category L*. Not `char::is_alphabetic`, which also admits
/// `Other_Alphabetic` marks and symbols (U+0345, circled letters U+24B6..).
#[must_use]
pub fn is_letter(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Letter
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

/// Passes of [`fold_search`]: two change any string at most (NFKD can emit a capital
/// the first case fold could not see: U+1D2C MODIFIER LETTER CAPITAL A -> "A" -> "a");
/// the third confirms. Each pass maps characters independently, so the bound the
/// every-scalar sweep checks holds for every string.
const FOLD_SEARCH_MAX_PASSES: usize = 3;

fn fold_search_pass(s: &str) -> String {
    case_fold(s)
        .nfkd()
        .filter(|&c| canonical_combining_class(c) == 0)
        .collect()
}

/// `fold_search`: full case folding, NFKD, drop characters with a nonzero canonical
/// combining class, repeated until the text stops changing; then split on whitespace
/// and join with single spaces. Idempotent.
///
/// # Panics
///
/// If the passes reach no fixed point within their cap, which is a bug in this crate's
/// Unicode data, not an input error; the every-scalar test rules it out.
#[must_use]
pub fn fold_search(s: &str) -> String {
    let mut text = fold_search_pass(s);
    for _ in 1..FOLD_SEARCH_MAX_PASSES {
        let next = fold_search_pass(&text);
        if next == text {
            return py_split(&text).collect::<Vec<_>>().join(" ");
        }
        text = next;
    }
    panic!("fold_search found no fixed point in {FOLD_SEARCH_MAX_PASSES} passes: {s:?}");
}

/// The search query normalizer: split on whitespace, join with one space, case fold.
#[must_use]
pub fn normalized_search_query(s: &str) -> String {
    case_fold(&py_split(s).collect::<Vec<_>>().join(" "))
}

/// The FTS5 query builder: each whitespace token with an alphanumeric character becomes
/// a quoted prefix term (`"tok"*`, embedded quotes doubled), space-joined. `None` when
/// no token qualifies.
#[must_use]
pub fn fts_match_query(raw: &str) -> Option<String> {
    let terms: Vec<String> = py_split(raw)
        .filter(|t| t.chars().any(py_isalnum))
        .map(|t| format!("\"{}\"*", t.replace('"', "\"\"")))
        .collect();
    (!terms.is_empty()).then(|| terms.join(" "))
}

/// The search terms of a text: the alphanumeric runs (`[^\W_]+`) of its
/// [`fold_search`] fold, in order.
#[must_use]
pub fn fts_terms(s: &str) -> Vec<String> {
    fold_search(s)
        .split(|c: char| !py_isalnum(c))
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{py_isdecimal, py_strip};

    /// Fails if `py_isdecimal` widens to every number (`²`, `½`, `Ⅻ`) or misses a
    /// non-ASCII decimal digit, or `py_strip` misses Python's U+001C..U+001F.
    #[test]
    fn python_decimal_and_strip() {
        assert!(['3', '\u{0663}', '\u{FF13}'].into_iter().all(py_isdecimal));
        assert!(
            !['\u{00B2}', '\u{00BD}', '\u{216B}']
                .into_iter()
                .any(py_isdecimal)
        );
        assert_eq!(py_strip("\u{1c} \u{3000}F32\u{1f}\n"), "F32");
    }
}
