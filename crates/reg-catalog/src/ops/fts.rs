//! The one FTS5 query builder `search`, `search_facets` and `docs_search` share.

use crate::Error;

/// The longest `q` accepted, in characters.
const MAX_QUERY: usize = 200;

/// The FTS5 expression of a user query `q`: each maximal run of Unicode letters and
/// digits becomes a quoted prefix term (`"tok"*`), joined by spaces (AND), so FTS
/// operators in `q` are inert. `None` when `q` has no letter or digit.
///
/// # Errors
///
/// `invalid_parameter` (`q`) when `q` is longer than 200 characters or holds a NUL.
pub fn query(q: &str) -> Result<Option<String>, Error> {
    if q.chars().count() > MAX_QUERY || q.contains('\0') {
        return Err(Error::invalid_parameter("q"));
    }
    // A token is letters and digits only, so it cannot hold `"` and needs no escaping.
    let terms: Vec<String> = q
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| format!("\"{token}\"*"))
        .collect();
    Ok((!terms.is_empty()).then(|| terms.join(" ")))
}
