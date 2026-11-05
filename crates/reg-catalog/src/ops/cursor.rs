//! Cursors: opaque, bound to the catalog generation and to the request they continue
//! (`operations.toml`: a cursor from another generation is `stale_cursor`).

use super::Params;
use reg_core::{hex, sha256_hex};

use crate::{Code, Error};

/// The cursor context of a request: every parameter but `cursor` and `limit`, sorted
/// by name, as `name=<hex of the value>` joined by `&`. Hex, because a free-text
/// value may hold `&` or `=`, which would let two requests share a context.
pub(super) fn context(params: &Params) -> String {
    params
        .values
        .iter()
        .filter(|(name, _)| !matches!(**name, "cursor" | "steward_cursor" | "limit"))
        .flat_map(|(name, values)| {
            values
                .iter()
                .map(move |value| format!("{name}={}", hex(value.as_bytes())))
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// The page tail: a fetch of `take` rows plus one that came back longer than `take`
/// has a next page. `rows` is cut to `take`, and `after` makes the cursor from the last
/// row kept (`None` there, or when `rows` was not longer, is no next page).
pub(super) fn next<T>(
    rows: &mut Vec<T>,
    take: usize,
    after: impl FnOnce(&T) -> Option<String>,
) -> Option<String> {
    if rows.len() <= take {
        return None;
    }
    rows.truncate(take);
    rows.last().and_then(after)
}

#[must_use]
pub fn invalid(message: &str) -> Error {
    Error::new(Code::InvalidCursor, message, vec![])
}

/// A cursor is the hex of `generation.context.offset.position`: the generation it
/// was issued on, the request it continues (the SHA-256 of `context`: fixed-size, so
/// a request naming 200 variables keeps a short cursor, and free of `.`), where the
/// next page starts and the position of the row before it (last, so it may hold a
/// `.`).
#[must_use]
pub fn encode(generation: &str, context: &str, offset: usize, position: &str) -> String {
    hex(format!("{generation}.{}.{offset}.{position}", sha256_hex(context)).as_bytes())
}

/// The cursor's offset (1 to `depth`) and position.
///
/// # Errors
///
/// `stale_cursor` for another generation's cursor; `invalid_cursor` for one that
/// does not decode, continues another request or passes the depth.
pub fn decode(
    cursor: &str,
    generation: &str,
    context: &str,
    depth: usize,
) -> Result<(usize, String), Error> {
    let malformed = || invalid("Cursor is malformed.");
    let bytes = (0..cursor.len())
        .step_by(2)
        .map(|i| {
            cursor
                .get(i..i + 2)
                .and_then(|b| u8::from_str_radix(b, 16).ok())
        })
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(malformed)?;
    let text = String::from_utf8(bytes).map_err(|_| malformed())?;
    let mut parts = text.splitn(4, '.');
    let (Some(issued), Some(issued_for), Some(offset), Some(after)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed());
    };
    if issued != generation {
        return Err(Error::new(
            Code::StaleCursor,
            "The catalog changed since this cursor was issued.",
            vec![generation.into()],
        ));
    }
    if issued_for != sha256_hex(context) {
        return Err(invalid("Cursor was issued for other parameters."));
    }
    let offset = offset
        .parse()
        .ok()
        .filter(|o| (1..=depth).contains(o))
        .ok_or_else(malformed)?;
    Ok((offset, after.to_owned()))
}
