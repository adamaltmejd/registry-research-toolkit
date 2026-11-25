//! The identifier check on columns the steward marks as löpnummer (`[[lopnr]]`,
//! crates/DESIGN.md, the löpnummer columns section): a marked column's mapped catalog variables
//! must not be explicitly non-identifiers. Run by `verify`, which the build also runs,
//! from the source reader config, so a build and a verify agree.
use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;

use super::{compile, input};
use crate::error::Result;

/// Refuse a marked column mapped to a variable whose identifier flag is false in a
/// segment its table is served; print a warning for a flag that is unknown. A marked
/// column with no mapping, and an identifier column without the mark, pass. Names the
/// `[[lopnr]]` entry by position and the variable by public id, never a physical name.
/// A segment counts only on the days the mapping's table reads it (the release rule that
/// compiles the mapping), not a concurrent release's overlapping segment.
pub(super) fn check(
    db: &Connection,
    variables: &compile::Variables<'_>,
    topology: &input::Topology,
    held: &reg_readers::holdings::ReadOutput,
    primaries: &compile::Primaries,
) -> Result<()> {
    if !held
        .tables
        .iter()
        .flat_map(|t| &t.columns)
        .any(|c| c.lopnr.is_some())
    {
        return Ok(());
    }
    let keyed = compile::schema_keyed(&held.tables);
    let authored: BTreeMap<_, _> = topology
        .table
        .iter()
        .map(|t| (compile::authored_identity(t, keyed), t))
        .collect();
    let mut query = db.prepare(
        "SELECT m.table_pid,m.variable_pid,m.start,m.end
         FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
         JOIN holding_table h ON h.id=c.holding_id
         WHERE h.physical_name=?1 AND c.name=?2 AND (?3 IS NULL OR h.schema_name=?3)
         ORDER BY m.id",
    )?;
    let mut unknown = BTreeSet::new();
    for table in &held.tables {
        let reach = compile::Reach {
            release: compile::stated_release(
                primaries,
                authored[&compile::physical_identity(table)],
            )?,
            variables,
        };
        for (column, n) in table.columns.iter().filter_map(|c| c.lopnr.map(|n| (c, n))) {
            let mappings = query
                .query_map(
                    rusqlite::params![table.name, column.name, table.schema],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, i64>(3)?,
                        ))
                    },
                )?
                .collect::<Result<Vec<_>, _>>()?;
            for (table_pid, variable, start, end) in mappings {
                for segment in variables
                    .get(variable.as_str())
                    .into_iter()
                    .flatten()
                    .filter(|s| s.table == table_pid)
                    .filter(|s| !reach.spans(s, &[(start, end)]).is_empty())
                {
                    match segment.identifier {
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
            }
        }
    }
    for (n, variable) in unknown {
        eprintln!(
            "warning: config:lopnr={n}: holdings_lopnr_identifier_unknown: the catalog states no identifier flag for {variable}, mapped to a column marked as a löpnummer"
        );
    }
    Ok(())
}
