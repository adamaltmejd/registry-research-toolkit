//! `reg-core`: contracts shared by the Rust runtime and, through bindings, the build.
//!
//! This crate holds the text folds of `RUST_RUNTIME_SPEC.md` section 5. Each fold
//! matches today's Python reader (`reg_meta`) byte for byte, except on characters
//! whose Unicode data changed after Python's UCD version.
//!
//! All Unicode data is on one version, [`UNICODE_VERSION`] (17.0.0): Rust std's
//! `to_lowercase` and `White_Space`, `unicode-normalization` 0.1.25 (NFKD, canonical
//! combining class), `unicode-properties` 0.1.4 (general category) and the generated
//! case-folding table.
//!
//! The oracle is `conformance/cases/folds/`.

mod case_folding;

use case_folding::CASE_FOLDING;
pub use case_folding::UNICODE_VERSION;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

/// Python `str.isspace()`: `White_Space` plus the four information separators
/// U+001C..U+001F, which Python treats as whitespace and Unicode does not.
fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
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
/// `reg_meta.queries.fold_search`.
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
///
/// Today's `reg_meta.queries._normalized_search_query`.
#[must_use]
pub fn normalized_search_query(s: &str) -> String {
    case_fold(&py_split(s).collect::<Vec<_>>().join(" "))
}

/// The FTS5 query builder: each whitespace token with an alphanumeric character becomes
/// a quoted prefix term (`"tok"*`, embedded quotes doubled), space-joined. `None` when
/// no token qualifies.
///
/// Today's `reg_meta.queries._fts_match_query`.
#[must_use]
pub fn fts_match_query(raw: &str) -> Option<String> {
    let terms: Vec<String> = py_split(raw)
        .filter(|t| t.chars().any(py_isalnum))
        .map(|t| format!("\"{}\"*", t.replace('"', "\"\"")))
        .collect();
    (!terms.is_empty()).then(|| terms.join(" "))
}
