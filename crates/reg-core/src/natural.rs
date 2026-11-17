//! Number-aware string order (`crates/DESIGN.md`, "Sets and tags"): set members by
//! facet value and name, so "Diagnoskod 2" comes before "Diagnoskod 10".

use std::cmp::Ordering;

/// `left` against `right` with each run of ASCII digits compared as a number; the rest
/// compares byte for byte as `str` does. Runs are compared without parsing, so any
/// length works. Strings equal by that rule ("01" and "1") fall back to `str` order,
/// so the order is total.
#[must_use]
pub fn natural_cmp(left: &str, right: &str) -> Ordering {
    let (mut lhs, mut rhs) = (left.as_bytes(), right.as_bytes());
    loop {
        match (lhs.first(), rhs.first()) {
            (None, None) => return left.cmp(right),
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(l), Some(r)) if l.is_ascii_digit() && r.is_ascii_digit() => {
                let (l_run, l_rest) = split_digits(lhs);
                let (r_run, r_rest) = split_digits(rhs);
                let (l_run, r_run) = (trim_zeros(l_run), trim_zeros(r_run));
                let order = l_run.len().cmp(&r_run.len()).then_with(|| l_run.cmp(r_run));
                if order.is_ne() {
                    return order;
                }
                (lhs, rhs) = (l_rest, r_rest);
            }
            (Some(l), Some(r)) => {
                if l != r {
                    return l.cmp(r);
                }
                (lhs, rhs) = (&lhs[1..], &rhs[1..]);
            }
        }
    }
}

fn split_digits(bytes: &[u8]) -> (&[u8], &[u8]) {
    bytes.split_at(
        bytes
            .iter()
            .position(|c| !c.is_ascii_digit())
            .unwrap_or(bytes.len()),
    )
}

fn trim_zeros(digits: &[u8]) -> &[u8] {
    &digits[digits
        .iter()
        .position(|&c| c != b'0')
        .unwrap_or(digits.len())..]
}
