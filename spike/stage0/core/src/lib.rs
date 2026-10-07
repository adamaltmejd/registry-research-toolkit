//! Stage-0 spike of `reg-core`: the text folds and the FTS query builder.
//!
//! Each function mirrors a named Python function in `reg_meta` and must agree with it
//! byte for byte; `scripts/parity_folds.py` checks that over every code point and over
//! the real catalog's strings.

use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

#[cfg(feature = "python")]
mod py;

/// Python `str.isspace()`: Rust's `White_Space` plus the four information separators
/// U+001C..U+001F, which Python treats as whitespace and Unicode does not.
pub fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `str.split()` with no arguments.
pub fn py_split(s: &str) -> impl Iterator<Item = &str> {
    s.split(py_isspace).filter(|t| !t.is_empty())
}

/// Python `str.isalnum()` for one character, i.e. the class `[^\W_]` in `re`: general
/// category L* or N*. Not `char::is_alphanumeric`, whose `Alphabetic` property also
/// admits `Other_Alphabetic` marks and symbols (U+0345, circled letters U+24B6..).
pub fn py_isalnum(c: char) -> bool {
    matches!(
        c.general_category_group(),
        GeneralCategoryGroup::Letter | GeneralCategoryGroup::Number
    )
}

/// `fold_identity` — Python `str.lower()` (`py_lower`, column identity).
pub fn fold_identity(s: &str) -> String {
    s.to_lowercase()
}

/// `fold_search` — `queries._fold_search_text`: strip, casefold, NFKD, drop characters
/// with a nonzero canonical combining class, collapse whitespace runs to one space.
pub fn fold_search(s: &str) -> String {
    let folded = caseless::default_case_fold_str(s.trim_matches(py_isspace));
    let mut out = String::with_capacity(folded.len());
    let mut in_space = false;
    for c in folded.nfkd() {
        if canonical_combining_class(c) != 0 {
            continue;
        }
        if py_isspace(c) {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

/// `queries._normalized_search_query`: `" ".join(query.split()).casefold()`.
pub fn normalized_search_query(s: &str) -> String {
    caseless::default_case_fold_str(&py_split(s).collect::<Vec<_>>().join(" "))
}

/// `queries._fts_match_query`: each whitespace token that has an alphanumeric character
/// becomes a quoted prefix term, quotes doubled; terms are space-joined.
pub fn fts_match_query(raw: &str) -> Option<String> {
    let terms: Vec<String> = py_split(raw)
        .filter(|t| t.chars().any(py_isalnum))
        .map(|t| format!("\"{}\"*", t.replace('"', "\"\"")))
        .collect();
    (!terms.is_empty()).then(|| terms.join(" "))
}
