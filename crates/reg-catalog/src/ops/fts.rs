//! The one FTS5 query builder `search` and `docs_search` share.

use crate::Error;

/// The longest `q` accepted, in characters.
const MAX_QUERY: usize = 200;

/// The FTS5 expression of a user query `q`: its [`terms`] joined by spaces (AND). `None`
/// when `q` has no letter or digit.
///
/// # Errors
///
/// [`terms`]'s.
pub fn query(q: &str) -> Result<Option<String>, Error> {
    let terms = terms(q)?;
    Ok((!terms.is_empty()).then(|| terms.join(" ")))
}

/// The terms of a user query `q`: each maximal run of Unicode letters and digits becomes
/// a quoted prefix term (`"tok"*`), so FTS operators in `q` are inert. Each term is a
/// whole FTS5 expression of its own.
///
/// # Errors
///
/// `invalid_parameter` (`q`) when `q` is longer than 200 characters or holds a NUL.
pub fn terms(q: &str) -> Result<Vec<String>, Error> {
    if q.chars().count() > MAX_QUERY || q.contains('\0') {
        return Err(Error::invalid_parameter("q"));
    }
    // A token is letters and digits only, so it cannot hold `"` and needs no escaping.
    Ok(q.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| format!("\"{token}\"*"))
        .collect())
}
