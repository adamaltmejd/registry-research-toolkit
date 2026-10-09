//! Today's `scope_predicate` (`reg_meta.holdings`): SQL that admits a provider,
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

fn held(source: &str, anchor: &str, narrow: Narrow) -> String {
    let mut sql = format!(
        "EXISTS (SELECT 1 FROM {source} JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) WHERE {anchor} AND ht.scope != 'unknown'"
    );
    if let Some((lo, hi)) = narrow.years {
        write!(
            sql,
            " AND ht.scope = 'intervals' AND EXISTS (SELECT 1 FROM holding_period hp \
             WHERE hp.table_id = ht.table_id AND hp.lo <= '{hi:04}-12-31' \
             AND hp.hi >= '{lo:04}-01-01')"
        )
        .expect("write to String");
    }
    if let Some(variant) = narrow.variant {
        write!(sql, " AND hm.variant_id = {variant}").expect("write to String");
    }
    if let Some(column) = narrow.column {
        // Today's `py_catalog_column` equality: a mapping's canonical
        // representation is its resolver-emitted spelling (`holdings_compile`), so
        // the column holds it exactly when the column's identity fold names it.
        write!(
            sql,
            " AND hm.representation_canonical = (SELECT rc.delivery_column_name \
             FROM resolver_column rc WHERE rc.variable_id = hm.variable_id \
             AND rc.register_variant_id = hm.variant_id \
             AND rc.delivery_column_lower = fold_identity({column}))"
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
