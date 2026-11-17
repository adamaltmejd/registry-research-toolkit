//! Which of a steward's accepted placements an order draws on (crates/DESIGN.md, "From a
//! logical order to physical files" and "Orderable columns"). `materialize_order`
//! (`reg-catalog`) selects with these; the holdings build (`reg-build`) counts what they
//! select for an order of everything. The two share only `reg-core`, so the rule lives
//! here, as SQL over the holdings schema and a pure fold.

use crate::days::{Span, merge, minus, overlap};

/// The files an entry draws on, for every query over holding table `h` with `:release`
/// bound to the entry's: an entry of one release (a shared day of a concurrent table)
/// draws only on the files that state that release.
pub const LANE: &str = "(:release IS NULL OR h.release = :release)";

/// The accepted placements of (`:table`, `:variable`) over `[:lo, :hi)` in database
/// `:space` (every database when NULL) and lane `:release`, as rows of (holding,
/// `projection_of`, column, start, end, caveat), for [`select`].
#[must_use]
pub fn placements_sql() -> String {
    format!(
        "SELECT h.id, h.projection_of, c.id, m.start, m.end, m.caveat
         FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
           JOIN holding_table h ON h.id = c.holding_id
         WHERE m.table_pid = :table AND m.variable_pid = :variable
           AND m.start < :hi AND m.end > :lo
           AND (:space IS NULL OR h.space = :space) AND {LANE}
         ORDER BY h.id, c.ordinal, m.start"
    )
}

/// One accepted placement: a mapping row of a holding table's column.
#[derive(Clone, Debug)]
pub struct Placement {
    pub holding: i64,
    pub projection_of: Option<i64>,
    pub column: i64,
    pub days: Span,
    pub caveat: Option<String>,
}

/// The placements an order over `days` draws on, from the [`placements_sql`] rows of one
/// (table, variable): a projection keeps only the days its parent does not also place,
/// and only those inside the request. A projection the parent covers wholly yields
/// nothing; its column is delivered through the parent.
#[must_use]
pub fn select(found: Vec<Placement>, days: Span) -> Vec<Placement> {
    let mut covered: std::collections::BTreeMap<i64, Vec<Span>> = std::collections::BTreeMap::new();
    for p in &found {
        covered.entry(p.holding).or_default().push(p.days);
    }
    let mut out = Vec::new();
    for p in found {
        let Some(parent) = p.projection_of.and_then(|parent| covered.get(&parent)) else {
            out.push(p);
            continue;
        };
        // A remainder can lie wholly outside the request (a parent-covered request).
        for days in overlap(&minus(&[p.days], &merge(parent.clone())), &[days]) {
            out.push(Placement { days, ..p.clone() });
        }
    }
    out
}

/// Whether a declared row key (one entry of a holding table's `key_requirements`) takes
/// its time from the table's physical period column. That column then joins an order's
/// file as a row key with no mapping of its own, wherever the key's scope meets the
/// file's days; a held key supplies nothing.
#[must_use]
pub fn key_reads_period_column(key: &serde_json::Value) -> bool {
    key["hold"].is_null() && key["period_key"].as_str() == Some("column")
}

/// The variables a declared row key (one entry of `key_requirements`) needs in an order's
/// file: its entity components, then its time components. Each column of the file's table
/// mapped to one of them joins the file.
#[must_use]
pub fn key_variables(key: &serde_json::Value) -> Vec<&str> {
    ["entity", "time"]
        .iter()
        .flat_map(|k| key[*k].as_array().into_iter().flatten())
        .filter_map(serde_json::Value::as_str)
        .collect()
}
