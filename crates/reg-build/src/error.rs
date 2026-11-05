//! The builder's error: a message, printed as `reg-build: <message>` with exit 1.
//!
//! `?` turns a library error into its own text, as `.map_err(|e| e.to_string())`
//! did; a refusal that needs context still formats it at the call site. It converts to
//! and from `String`, so functions that only build messages keep `Result<T, String>`.

use std::fmt;

#[derive(Debug)]
pub struct Error(String);

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<Error> for String {
    fn from(e: Error) -> Self {
        e.0
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self(message.to_owned())
    }
}

macro_rules! by_text {
    ($($t:ty),*) => {$(
        impl From<$t> for Error {
            fn from(e: $t) -> Self {
                Self(e.to_string())
            }
        }
    )*};
}

by_text!(
    rusqlite::Error,
    rusqlite::types::FromSqlError,
    std::io::Error,
    serde_json::Error,
    std::num::TryFromIntError,
    reg_readers::ReadError
);

/// Every row of `sql` under `params`, mapped by `map`.
pub fn rows<T>(
    db: &rusqlite::Connection,
    sql: &str,
    params: impl rusqlite::Params,
    map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    Ok(db
        .prepare(sql)?
        .query_map(params, map)?
        .collect::<rusqlite::Result<_>>()?)
}
