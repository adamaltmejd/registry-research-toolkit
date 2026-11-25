//! The identifier check on columns the steward marks as löpnummer (`[[lopnr]]`,
//! crates/DESIGN.md, the löpnummer columns section): a marked column's mapped catalog
//! variables must not be explicitly non-identifiers. `verify_mappings::expected` records
//! the flag of each claim it selects for an accepted interval, so the check reads only
//! what the compiler's selection rule chose.
use std::collections::BTreeSet;

use super::verify_mappings::Accepted;
use crate::error::Result;

/// Refuse a marked column whose selected segment has identifier flag false; collect
/// `(entry, variable)` for a flag that is unknown. A marked column with no mapping, and an
/// identifier column without the mark, pass. Names the `[[lopnr]]` entry by position and
/// the variable by public id, never a physical name.
pub(super) fn check(
    n: usize,
    accepted: &Accepted,
    unknown: &mut BTreeSet<(usize, String)>,
) -> Result<()> {
    for (variable, identifier) in accepted {
        match identifier {
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
