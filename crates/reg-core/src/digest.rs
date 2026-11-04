//! SHA-256 and lowercase hex: the one spelling of a content hash and a cursor.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

/// Lowercase hex of `bytes`.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        write!(out, "{byte:02x}").expect("write to String");
    }
    out
}

/// SHA-256 of `bytes` as 64 lowercase hex digits.
#[must_use]
pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    hex(&Sha256::digest(bytes))
}
