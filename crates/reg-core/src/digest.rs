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

/// Whether `text` is a SHA-256 digest as [`sha256_hex`] spells it: 64 lowercase hex
/// digits.
#[must_use]
pub fn is_sha256_hex(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}
