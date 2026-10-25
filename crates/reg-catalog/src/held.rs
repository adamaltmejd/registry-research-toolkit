//! The scope predicate: SQL that admits a provider,
//! register or variable to the read scope. Reference admits everything; holdings
//! admits what an authored mapping of a known-scope table holds. Only trusted SQL
//! (aliases and integers) is spliced in.

use std::fmt::Write as _;

use reg_core::merge;
use rusqlite::{Connection, params};

use crate::{Error, Scope};

/// Narrows a variable's holdings: to tables of `years` (intervals tables whose
/// periods overlap them), to one variant, and to one delivery column's
/// representation.
#[derive(Clone, Copy, Default)]
pub(crate) struct Narrow<'a> {
    pub years: Option<(u16, u16)>,
    pub variant: Option<&'a str>,
    pub column: Option<&'a str>,
}

/// A held mapping's bounds over its table's `holding_period` row `hp`: a year or
/// month pivot member, or a column window (#1645), holds only its own period, which
/// lies inside one segment of the edition.
pub(crate) const HELD_LO: &str = "COALESCE(hm.member_lo, hm.window_lo, hp.lo)";
pub(crate) const HELD_HI: &str = "COALESCE(hm.member_hi, hm.window_hi, hp.hi)";

/// Whether mapping `m` (of column `c`) is its table's one representation of its
/// `(variable, variant)`. Possession is physical: such a table holds the variable
/// under every spelling SCB gave its column where SCB did not deliver the mapped
/// spelling beside it and the table has no column of that spelling ([`serves`]), so
/// a per-year rename never subtracts held years (`Doda_20241231`'s `DodDatum` over
/// SCB's 1998–2023 `DodDat`). The builder refuses an inventory where this would
/// serve a cell another location also serves (`holdings_compile.canonical_inventory`,
/// `inventory_cell_conflict`).
pub(crate) fn one_representation(m: &str, c: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM holding_column sc \
         JOIN holding_mapping sm USING(column_id) WHERE sc.table_id = {c}.table_id \
         AND sm.variable_id = {m}.variable_id AND sm.variant_id = {m}.variant_id \
         AND sm.representation_canonical != {m}.representation_canonical)"
    )
}

/// Whether column `c`'s table has no other column whose name folds to the spelling
/// `rep`: where it has one (mapped elsewhere or unmapped), that column holds `rep`'s
/// data, so `c` does not. The builder's copies (`derive/browse.py`, `inventory.py`)
/// fold with `py_lower`/`fold_identity`, the same function.
pub(crate) fn no_column_of(c: &str, rep: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM holding_column oc WHERE oc.table_id = {c}.table_id \
         AND oc.column_id != {c}.column_id AND fold_identity(oc.name) = fold_identity({rep}))"
    )
}

/// Whether `variable` delivers the representation `rep` at `variant` over the bounds
/// `lo..hi` (SQL expressions; both NULL for a year-independent representation).
pub(crate) fn delivered(variable: &str, variant: &str, rep: &str, lo: &str, hi: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM expanded_state sr WHERE sr.variable_id = {variable} \
         AND sr.register_variant_id = {variant} AND sr.canonical_column = {rep} \
         AND ((sr.valid_from IS NULL AND {lo} IS NULL) \
         OR (sr.valid_from <= {hi} AND sr.valid_to >= {lo})))"
    )
}

/// Whether the variable delivers mapping `m`'s own representation over `lo..hi`.
fn delivered_beside(m: &str, lo: &str, hi: &str) -> String {
    delivered(
        &format!("{m}.variable_id"),
        &format!("{m}.variant_id"),
        &format!("{m}.representation_canonical"),
        lo,
        hi,
    )
}

/// Whether mapping `m` (of column `c`) serves the representation `rep` (an SQL
/// expression naming one of its variable's canonical spellings at its variant)
/// over that representation's bounds `lo..hi`: its own, or, as its table's
/// [`one_representation`], one SCB did not deliver its own spelling beside and no
/// other column of its table is named for ([`no_column_of`]). Where both are delivered,
/// sibling columns side by side, the spelling chooses the column.
pub(crate) fn serves_at_by(m: &str, c: &str, rep: &str, lo: &str, hi: &str) -> String {
    format!(
        "({m}.representation_canonical = {rep} OR ({rep} IS NOT NULL \
         AND {} AND NOT {} AND {}))",
        one_representation(m, c),
        delivered_beside(m, lo, hi),
        no_column_of(c, rep),
    )
}

/// [`serves_at_by`] for mapping `hm` of column `hc`.
pub(crate) fn serves_at(rep: &str, lo: &str, hi: &str) -> String {
    serves_at_by("hm", "hc", rep, lo, hi)
}

/// [`serves_at`] at any time: `rep` has a state or window the mapping holds (a dated
/// one over its held periods, a year-independent one in a year-independent table)
/// that it serves. A read that clips to held periods uses [`periods`] per
/// representation instead.
pub(crate) fn serves(rep: &str) -> String {
    format!(
        "(hm.representation_canonical = {rep} OR ({rep} IS NOT NULL AND {} \
         AND EXISTS (SELECT 1 FROM expanded_state se WHERE se.variable_id = hm.variable_id \
         AND se.register_variant_id = hm.variant_id AND se.canonical_column = {rep} \
         AND NOT {} \
         AND CASE WHEN se.valid_from IS NULL \
         THEN (SELECT st.scope FROM holding_table st WHERE st.table_id = hc.table_id) \
         = 'year_independent' \
         ELSE EXISTS (SELECT 1 FROM holding_period sp WHERE sp.table_id = hc.table_id \
         AND se.valid_from <= COALESCE(hm.member_hi, hm.window_hi, sp.hi) \
         AND se.valid_to >= COALESCE(hm.member_lo, hm.window_lo, sp.lo)) END) \
         AND {}))",
        one_representation("hm", "hc"),
        delivered_beside("hm", "se.valid_from", "se.valid_to"),
        no_column_of("hc", rep),
    )
}

fn held(source: &str, anchor: &str, narrow: Narrow) -> String {
    let mut sql = format!(
        "EXISTS (SELECT 1 FROM {source} JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) WHERE {anchor} AND ht.scope != 'unknown'"
    );
    if let Some((lo, hi)) = narrow.years {
        write!(
            sql,
            " AND ht.scope = 'intervals' AND EXISTS (SELECT 1 FROM holding_period hp \
             WHERE hp.table_id = ht.table_id AND {HELD_LO} <= '{hi:04}-12-31' \
             AND {HELD_HI} >= '{lo:04}-01-01')"
        )
        .expect("write to String");
    }
    if let Some(variant) = narrow.variant {
        write!(sql, " AND hm.variant_id = {variant}").expect("write to String");
    }
    if let Some(column) = narrow.column {
        // A mapping's canonical representation is its resolver-emitted spelling
        // (`holdings_compile`), so the column's identity fold names the
        // representation it must serve (looked up once, not per arm of `serves`).
        write!(
            sql,
            " AND EXISTS (SELECT 1 FROM resolver_column rc WHERE rc.variable_id = hm.variable_id \
             AND rc.register_variant_id = hm.variant_id \
             AND rc.delivery_column_lower = fold_identity({column}) AND {})",
            serves("rc.delivery_column_name")
        )
        .expect("write to String");
    }
    sql + ")"
}

/// The variable `id` (an SQL expression) is in scope.
pub(crate) fn variable(scope: Scope, id: &str, narrow: Narrow) -> String {
    match scope {
        Scope::Reference => "1".to_owned(),
        Scope::Holdings => held(
            "holding_mapping hm",
            &format!("hm.variable_id = {id}"),
            narrow,
        ),
    }
}

/// The register variant `id` holds some variable in scope.
pub(crate) fn variant(scope: Scope, id: &str) -> String {
    match scope {
        Scope::Reference => "1".to_owned(),
        Scope::Holdings => held(
            "holding_mapping hm",
            &format!("hm.variant_id = {id}"),
            Narrow::default(),
        ),
    }
}

/// The register `id` holds some variable in scope.
pub(crate) fn register(scope: Scope, id: &str) -> String {
    register_in(scope, id, None)
}

/// The register `id` holds some variable in scope, in tables of `years` when given.
pub(crate) fn register_in(scope: Scope, id: &str, years: Option<(u16, u16)>) -> String {
    match scope {
        Scope::Reference => "1".to_owned(),
        Scope::Holdings => held(
            "variable hv JOIN holding_mapping hm ON hm.variable_id = hv.variable_id",
            &format!("hv.register_id = {id}"),
            Narrow {
                years,
                ..Narrow::default()
            },
        ),
    }
}

/// The provider `id` has a register in scope.
pub(crate) fn provider(scope: Scope, id: &str) -> String {
    match scope {
        Scope::Reference => "1".to_owned(),
        Scope::Holdings => format!(
            "EXISTS (SELECT 1 FROM register hr WHERE hr.provider_id = {id} AND {})",
            register(scope, "hr.register_id")
        ),
    }
}

/// The SQL `delivery_column_name` a reader shows for the alias `va`: its canonical
/// spelling in holdings (today's `py_catalog_column`), as delivered in reference.
pub(crate) fn shown_column(scope: Scope, alias: &str) -> String {
    match scope {
        Scope::Reference => format!("{alias}.delivery_column_name"),
        // Under the holdings predicate the alias is a held representation, so its
        // resolver row exists; the fallback keeps the expression total.
        Scope::Holdings => format!(
            "COALESCE((SELECT rc.delivery_column_name FROM resolver_column rc \
             WHERE rc.variable_id = {alias}.variable_id \
             AND rc.register_variant_id = {alias}.register_variant_id \
             AND rc.delivery_column_lower = fold_identity({alias}.delivery_column_name)), \
             {alias}.delivery_column_name)"
        ),
    }
}

/// A held representation's periods, as `holding_period` rows; `(None, None)` is a
/// table without periods (a year-independent one).
pub(crate) type Periods = Vec<(Option<String>, Option<String>)>;

/// The periods over which the steward holds `variable_id`'s representation `rep` at
/// `variant`, in tables of `period_scope`, read from the mappings that serve it
/// ([`serves_at`]) over its own bounds `own` (both None for a year-independent one):
/// the one statement of "held periods of (variable, variant, representation)" that
/// `states` clips to and `warnings` clips a dated warning to. Unclipped and
/// unmerged; [`clip`] narrows them.
pub(crate) fn periods(
    conn: &Connection,
    variable_id: i64,
    variant: i64,
    rep: &str,
    period_scope: &str,
    own: (Option<&str>, Option<&str>),
) -> Result<Periods, Error> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {HELD_LO}, {HELD_HI} FROM holding_mapping hm \
         JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id) \
         WHERE hm.variable_id = ?1 AND hm.variant_id = ?2 AND {} \
         AND ht.scope = ?4 ORDER BY 1, 2",
        serves_at("?3", "?5", "?6"),
    ))?;
    let found = stmt
        .query_map(
            params![variable_id, variant, rep, period_scope, own.0, own.1],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    Ok(found)
}

/// The dated periods over which the steward holds any variable of `register_id`, at
/// `variant` when given: the register's (or that variant's) held span, which a
/// register-wide dated warning clips to.
pub(crate) fn register_periods(
    conn: &Connection,
    register_id: i64,
    variant: Option<i64>,
) -> Result<Periods, Error> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {HELD_LO}, {HELD_HI} FROM variable v \
         JOIN holding_mapping hm ON hm.variable_id = v.variable_id \
         JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) \
         JOIN holding_period hp USING(table_id) \
         WHERE v.register_id = ?1 AND (?2 IS NULL OR hm.variant_id = ?2) \
         AND ht.scope = 'intervals' ORDER BY 1, 2"
    ))?;
    let found = stmt
        .query_map(params![register_id, variant], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(found)
}

/// Today's `_scope_states` clip: `held`'s dated periods clipped to a dated
/// representation's `own` bounds and the `request`'s (ISO dates), sorted, with
/// overlapping and day-adjacent periods merged (`inventory._merge`).
pub(crate) fn clip(
    held: &[(Option<String>, Option<String>)],
    own: (&str, &str),
    request: Option<(&str, &str)>,
) -> Vec<(String, String)> {
    let (mut lo_bound, mut hi_bound) = own;
    if let Some((lo, hi)) = request {
        lo_bound = lo_bound.max(lo);
        hi_bound = hi_bound.min(hi);
    }
    let clipped: Vec<(String, String)> = held
        .iter()
        .filter_map(|(lo, hi)| {
            let lo = lo.as_deref()?.max(lo_bound);
            let hi = hi.as_deref()?.min(hi_bound);
            (lo <= hi).then(|| (lo.to_owned(), hi.to_owned()))
        })
        .collect();
    merge(clipped)
}
