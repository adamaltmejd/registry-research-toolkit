//! The identifier check on columns the steward marks as löpnummer (`[[lopnr]]`,
//! crates/DESIGN.md, the löpnummer columns section): a marked column's mapped catalog
//! variables must not be explicitly non-identifiers. It reads the claims `verify_mappings`
//! derives for the column, so the segments are the ones the compiler selects (spelling or
//! declared mapping, and the release rule), not a second selection.
use std::collections::BTreeSet;

use reg_core::days;

use super::verify_mappings::{Claim, Mappings};
use crate::error::Result;

/// Refuse a marked column mapped to a variable whose identifier flag is false in a
/// segment its table is served; collect `(entry, variable)` for a flag that is unknown.
/// `claims` count only on the days the mapping was accepted: not blocked ambiguous ones,
/// and only where a `mapped` interval of their (table, variable) overlaps their spans. A
/// marked column with no mapping, and an identifier column without the mark, pass. Names the
/// `[[lopnr]]` entry by position and the variable by public id, never a physical name.
pub(super) fn check(
    n: usize,
    claims: &[Claim],
    mapped: &Mappings,
    unknown: &mut BTreeSet<(usize, String)>,
) -> Result<()> {
    for claim in claims.iter().filter(|c| {
        !c.ambiguous
            && mapped.iter().any(|(a, spans)| {
                a.table == c.attributes.table
                    && a.variable == c.attributes.variable
                    && !days::overlap(&c.spans, spans).is_empty()
            })
    }) {
        let variable = &claim.attributes.variable;
        match claim.identifier {
            Some(false) => {
                return Err(format!(
                    "config:lopnr={n}: holdings_lopnr_not_identifier: {variable} is mapped to a column marked as a löpnummer but the catalog says it is not an identifier"
                )
                .into());
            }
            None => {
                unknown.insert((n, variable.clone()));
            }
            Some(true) => {}
        }
    }
    Ok(())
}

/// One warning per entry and variable whose identifier flag the catalog does not state.
pub(super) fn warn(unknown: &BTreeSet<(usize, String)>) {
    for (n, variable) in unknown {
        eprintln!(
            "warning: config:lopnr={n}: holdings_lopnr_identifier_unknown: the catalog states no identifier flag for {variable}, mapped to a column marked as a löpnummer"
        );
    }
}
