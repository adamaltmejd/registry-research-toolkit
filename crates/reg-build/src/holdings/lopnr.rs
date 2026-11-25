//! The identifier check on columns the steward marks as SCB löpnummer (`[[lopnr]]`,
//! crates/DESIGN.md "SCB löpnummer columns"): a marked column's mapped catalog variables
//! must not be explicitly non-identifiers. Re-derived by `verify` from the source reader
//! config, so a build and a verify agree.
use std::collections::{BTreeMap, BTreeSet};

use reg_readers::holdings as physical;
use rusqlite::Connection;

use super::compile;
use crate::error::{Result, rows};

/// Refuse a marked column mapped to a variable whose identifier flag is false in a
/// segment its mapping covers; print a warning for a flag that is unknown. A marked
/// column with no mapping, and an identifier column without the mark, pass. Names the
/// `[[lopnr]]` entry by position and the variable by public id, never a physical name.
pub(super) fn check(db: &Connection, base: &Connection, held: &[physical::Table]) -> Result<()> {
    let marked: BTreeMap<(&str, &str, &str), usize> = held
        .iter()
        .flat_map(|t| {
            t.columns.iter().filter_map(move |c| {
                c.lopnr.map(|n| {
                    (
                        (t.storage_schema.as_str(), t.name.as_str(), c.name.as_str()),
                        n,
                    )
                })
            })
        })
        .collect();
    let index = compile::segment_index(base, db)?;
    let mut unknown = BTreeSet::new();
    let mappings = rows(
        db,
        "SELECT h.schema_name,h.physical_name,c.name,m.table_pid,m.variable_pid,m.start,m.end
         FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
         JOIN holding_table h ON h.id=c.holding_id WHERE c.lopnr=1
         ORDER BY h.id,c.ordinal,m.id",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
            ))
        },
    )?;
    for (schema, table, column, table_pid, variable, start, end) in mappings {
        let n = marked[&(schema.as_str(), table.as_str(), column.as_str())];
        for segment in index
            .get(&table_pid)
            .into_iter()
            .flatten()
            .filter(|s| s.variable == variable && s.start < end && s.end > start)
        {
            match segment.identifier {
                Some(false) => {
                    return Err(format!(
                        "config:lopnr={n}: holdings_lopnr_not_identifier: {variable} is mapped to a column marked as an SCB löpnummer but the catalog says it is not an identifier"
                    )
                    .into());
                }
                None => {
                    unknown.insert((n, variable.clone()));
                }
                Some(true) => {}
            }
        }
    }
    for (n, variable) in unknown {
        eprintln!(
            "warning: config:lopnr={n}: holdings_lopnr_identifier_unknown: the catalog states no identifier flag for {variable}, mapped to a column marked as an SCB löpnummer"
        );
    }
    Ok(())
}
