//! The scope predicate: SQL that admits a provider,
//! register or variable to the read scope. Reference admits everything; holdings
//! admits what an authored mapping of a known-scope table holds. Only trusted SQL
//! (aliases and integers) is spliced in.

use std::fmt::Write as _;

use reg_core::merge;

use crate::Scope;

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

/// Whether mapping `hm` (of column `hc`) is its table's one representation of its
/// `(variable, variant)`. Possession is physical: such a table holds the variable
/// under every spelling SCB gave its column where SCB did not deliver the mapped
/// spelling beside it ([`serves`]), so a per-year rename never subtracts held years
/// (`Doda_20241231`'s `DodDatum` over SCB's 1998–2023 `DodDat`).
// simplify: the builder's one-to-one rule still compares spellings, so a second
// table mapping another spelling over a year this one serves by its fallback
// would serve that cell twice. None does on SWECOV (2026-10-24); check it in
// `holdings_compile` against `expanded_state` once a steward inventory has one.
pub(crate) const ONE_REPRESENTATION: &str = "NOT EXISTS (SELECT 1 FROM holding_column sc \
     JOIN holding_mapping sm USING(column_id) WHERE sc.table_id = hc.table_id \
     AND sm.variable_id = hm.variable_id AND sm.variant_id = hm.variant_id \
     AND sm.representation_canonical != hm.representation_canonical)";

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

/// Whether the variable delivers mapping `hm`'s own representation over `lo..hi`.
fn delivered_beside(lo: &str, hi: &str) -> String {
    delivered(
        "hm.variable_id",
        "hm.variant_id",
        "hm.representation_canonical",
        lo,
        hi,
    )
}

/// Whether mapping `hm` (of column `hc`) serves the representation `rep` (an SQL
/// expression naming one of its variable's canonical spellings at its variant)
/// over that representation's bounds `lo..hi`: its own, or, as its table's
/// [`ONE_REPRESENTATION`], one SCB did not deliver its own spelling beside. Where
/// both are delivered, sibling columns side by side, the spelling chooses the column.
pub(crate) fn serves_at(rep: &str, lo: &str, hi: &str) -> String {
    format!(
        "(hm.representation_canonical = {rep} OR ({rep} IS NOT NULL \
         AND {ONE_REPRESENTATION} AND NOT {}))",
        delivered_beside(lo, hi)
    )
}

/// [`serves_at`] at any time: `rep` has a state or window the mapping holds (a dated
/// one over its held periods, a year-independent one in a year-independent table)
/// that it serves. A read that clips to held periods uses [`serves_at`] per
/// representation instead.
pub(crate) fn serves(rep: &str) -> String {
    format!(
        "(hm.representation_canonical = {rep} OR ({rep} IS NOT NULL AND {ONE_REPRESENTATION} \
         AND EXISTS (SELECT 1 FROM expanded_state se WHERE se.variable_id = hm.variable_id \
         AND se.register_variant_id = hm.variant_id AND se.canonical_column = {rep} \
         AND NOT {} \
         AND CASE WHEN se.valid_from IS NULL \
         THEN (SELECT st.scope FROM holding_table st WHERE st.table_id = hc.table_id) \
         = 'year_independent' \
         ELSE EXISTS (SELECT 1 FROM holding_period sp WHERE sp.table_id = hc.table_id \
         AND se.valid_from <= COALESCE(hm.member_hi, hm.window_hi, sp.hi) \
         AND se.valid_to >= COALESCE(hm.member_lo, hm.window_lo, sp.lo)) END)))",
        delivered_beside("se.valid_from", "se.valid_to")
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
