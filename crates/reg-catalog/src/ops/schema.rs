//! `schema` and `diff`: a register's or a variable's delivered columns, and a
//! register's columns between two periods (today's `get schema`, `get datacolumns` and
//! `get diff`), read from the compiled `expanded_state`.
//!
//! A column row is one representation the resolver emits over the whole history:
//! every `expanded_state` row but `base_fallback`, with the content of its state, or of
//! its alias window where that window's metadata or coding is per column. Holdings
//! keeps a representation the steward holds (its canonical column, mapped in a table
//! of its period scope) and clips it to the held periods, merging day-adjacent ones.
//! Rows show the delivered spelling in both scopes. A period filters by the calendar
//! years it touches and drops year-independent rows, as `--years` did.

use std::collections::BTreeMap;

use reg_core::Period;
use rusqlite::Connection;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::refs::{self, Target, VAR_ID, fqid, invalid_kind};
use super::{Params, Server, cursor, states};
use crate::held;
use crate::{Code, Error, Scope, hex};

/// `Page<SchemaColumn>`.
#[derive(Serialize, ToSchema)]
pub struct SchemaPage {
    items: Vec<SchemaColumn>,
    next_cursor: Option<String>,
}

/// One delivered column of a register variant over one window: the CLI's `get schema`
/// column row with its variant and window. `column` is null for a state SCB named no
/// column for; `valid_from` and `valid_to` are null when year-independent.
#[derive(Clone, Serialize, ToSchema)]
pub struct SchemaColumn {
    variant: Option<String>,
    variant_name: Option<String>,
    variant_description: Option<String>,
    /// `intervals` or `year_independent`.
    period_scope: String,
    valid_from: Option<String>,
    valid_to: Option<String>,
    fqid: Option<String>,
    variable_name: Option<String>,
    /// SCB's numeric variable id; null for other providers.
    var_id: Option<i64>,
    source: Option<String>,
    column: Option<String>,
    data_type: Option<String>,
    data_length: Option<String>,
    value_set_version_label: String,
    operational_definition: Option<String>,
    source_register_text: Option<String>,
    definition: Option<String>,
    measurement_unit: Option<String>,
    /// The variable's concept group, as a group ref.
    group: Option<String>,
    group_label: Option<String>,
}

/// `diff`'s result: the variants whose columns differ between `from` and `to`.
#[derive(Serialize, ToSchema)]
pub struct Diff {
    register: String,
    register_name: String,
    from: String,
    to: String,
    variants: Vec<VariantDiff>,
}

/// A variant's columns added at `to`, removed since `from` and changed, by `fqid`.
#[derive(Serialize, ToSchema)]
pub struct VariantDiff {
    variant: Option<String>,
    variant_name: Option<String>,
    summary: Summary,
    added: Vec<DiffColumn>,
    removed: Vec<DiffColumn>,
    changed: Vec<Changed>,
}

#[derive(Serialize, ToSchema)]
pub struct Summary {
    added: usize,
    removed: usize,
    changed: usize,
    unchanged: usize,
}

/// A variable's column in one period: its first representation there.
#[derive(Clone, Serialize, ToSchema)]
pub struct DiffColumn {
    fqid: Option<String>,
    var_id: Option<i64>,
    variable_name: Option<String>,
    data_type: Option<String>,
    data_length: Option<String>,
    column: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct Changed {
    fqid: Option<String>,
    var_id: Option<i64>,
    variable_name: Option<String>,
    changes: Vec<Change>,
}

/// One differing field: `data_type`, `data_length` or `column`.
#[derive(Serialize, ToSchema)]
pub struct Change {
    field: &'static str,
    from: Option<String>,
    to: Option<String>,
}

/// A representation with what holdings, ordering and `diff` read beside its row.
#[derive(Clone)]
struct Rep {
    variable_id: i64,
    variant_id: i64,
    state_id: i64,
    /// The state's own column: `diff` takes a variable's first representation in
    /// the state order `get diff` read them in.
    state_column: Option<String>,
    canonical: Option<String>,
    variable_slug: Option<String>,
    row: SchemaColumn,
}

type Key<'a> = Option<&'a str>;

impl Rep {
    /// A page's order: variant (unslugged last), window, variable, column, label.
    fn page_order(&self) -> (bool, Key<'_>, Key<'_>, Key<'_>, Key<'_>, Key<'_>, &str) {
        let r = &self.row;
        (
            r.variant.is_none(),
            r.variant.as_deref(),
            r.valid_from.as_deref(),
            r.valid_to.as_deref(),
            self.variable_slug.as_deref(),
            r.column.as_deref(),
            &r.value_set_version_label,
        )
    }

    /// `get diff`'s order within a variable: the state's column, the state, then the
    /// representation's window and column.
    fn state_order(&self) -> (Key<'_>, i64, Key<'_>, Key<'_>, Key<'_>) {
        let r = &self.row;
        (
            self.state_column.as_deref(),
            self.state_id,
            r.valid_from.as_deref(),
            r.valid_to.as_deref(),
            r.column.as_deref(),
        )
    }
}

pub fn schema(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let limit = super::limit(params)?;
    let period = super::period(params, "period")?;
    let conn = catalog.connect()?;
    let value = params.get("ref").copied().unwrap_or_default();
    let (register_id, variable_id) = match refs::resolve(&conn, scope, Some(value))? {
        Target::Register { id, .. } => (id, None),
        Target::Variable { id } => (
            conn.query_row(
                "SELECT register_id FROM variable WHERE variable_id = ?",
                [id],
                |row| row.get(0),
            )?,
            Some(id),
        ),
        _ => return Err(invalid_kind(value)),
    };
    let variant = super::variant(params)?;
    let years = period.map(Period::years);
    // simplify: loads and sorts the register's whole history per page (frida 47k
    // rows, ~0.3 s of SQL); move `period` and the order into SQL with keyset paging
    // if a page passes ~1 s or G1's served schema walk exceeds its share of 5 min.
    let mut reps = representations(&conn, scope, register_id, variable_id, variant, years)?;
    if let Some(years) = years {
        reps.retain(|rep| overlaps(&rep.row, years));
    }
    reps.sort_by(|a, b| a.page_order().cmp(&b.page_order()));
    // The cursor binds what selects and orders the rows, never `limit`.
    let context = json!([value, period.map(|p| p.to_string()), variant, scope]);
    let context = hex(&Sha256::digest(context.to_string().as_bytes()));
    let offset = params
        .get("cursor")
        .map(|c| cursor::decode(c, catalog.generation(), &context, reps.len().max(1)))
        .transpose()?
        .map_or(0, |(offset, _)| offset);
    let end = (offset + limit).min(reps.len());
    let page = SchemaPage {
        items: reps[offset.min(end)..end]
            .iter()
            .map(|r| r.row.clone())
            .collect(),
        next_cursor: (end < reps.len())
            .then(|| cursor::encode(catalog.generation(), &context, end, "")),
    };
    Ok(serde_json::to_value(page).expect("SchemaPage serializes"))
}

pub fn diff(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    // The transport refuses a request without a required parameter.
    let [from, to] = ["from", "to"]
        .map(|name| super::period(params, name)?.ok_or_else(|| Error::invalid_parameter(name)));
    let (from, to) = (from?, to?);
    let conn = server.catalog.connect()?;
    let value = params.get("ref").copied().unwrap_or_default();
    let Target::Register { id, provider, slug } = refs::resolve(&conn, scope, Some(value))? else {
        return Err(invalid_kind(value));
    };
    let register_name: String = conn.query_row(
        "SELECT name FROM register WHERE register_id = ?",
        [id],
        |row| row.get(0),
    )?;
    let mut by_variant: BTreeMap<(bool, Option<String>), Vec<Rep>> = BTreeMap::new();
    let variant = super::variant(params)?;
    for rep in representations(&conn, scope, id, None, variant, None)? {
        by_variant
            .entry((rep.row.variant.is_none(), rep.row.variant.clone()))
            .or_default()
            .push(rep);
    }
    let mut compared = false;
    let mut variants = Vec::new();
    for reps in by_variant.into_values() {
        let (before, after) = (columns_at(&reps, from), columns_at(&reps, to));
        if before.is_empty() || after.is_empty() {
            continue;
        }
        compared = true;
        let mut diff = VariantDiff {
            variant: reps[0].row.variant.clone(),
            variant_name: reps[0].row.variant_name.clone(),
            summary: Summary {
                added: 0,
                removed: 0,
                changed: 0,
                unchanged: 0,
            },
            added: by_fqid(after.iter().filter(|(k, _)| !before.contains_key(*k))),
            removed: by_fqid(before.iter().filter(|(k, _)| !after.contains_key(*k))),
            changed: Vec::new(),
        };
        for (variable, old) in &before {
            let Some(new) = after.get(variable) else {
                continue;
            };
            let changes: Vec<Change> = [
                ("data_type", &old.data_type, &new.data_type),
                ("data_length", &old.data_length, &new.data_length),
                ("column", &old.column, &new.column),
            ]
            .into_iter()
            .filter(|(_, a, b)| a != b)
            .map(|(field, a, b)| Change {
                field,
                from: a.clone(),
                to: b.clone(),
            })
            .collect();
            if changes.is_empty() {
                diff.summary.unchanged += 1;
            } else {
                diff.changed.push(Changed {
                    fqid: new.fqid.clone(),
                    var_id: new.var_id,
                    variable_name: new.variable_name.clone(),
                    changes,
                });
            }
        }
        diff.changed.sort_by(|a, b| a.fqid.cmp(&b.fqid));
        diff.summary.added = diff.added.len();
        diff.summary.removed = diff.removed.len();
        diff.summary.changed = diff.changed.len();
        if diff.summary.added + diff.summary.removed + diff.summary.changed > 0 {
            variants.push(diff);
        }
    }
    // A variant filter that keeps nothing answers no variants (as `schema` answers
    // no rows), not a missing register.
    if !compared && variant.is_none() {
        return Err(Error::new(
            Code::NotFound,
            format!("{value:?} delivers no columns in both {from} and {to} in this scope."),
            vec![value.into()],
        ));
    }
    let diff = Diff {
        register: format!("{provider}/{slug}"),
        register_name,
        from: from.to_string(),
        to: to.to_string(),
        variants,
    };
    Ok(serde_json::to_value(diff).expect("Diff serializes"))
}

/// A dated row overlaps the calendar years `(lo, hi)`.
fn overlaps(row: &SchemaColumn, (lo, hi): (u16, u16)) -> bool {
    match (&row.valid_from, &row.valid_to) {
        (Some(from), Some(to)) => {
            from.as_str() <= format!("{hi:04}-12-31").as_str()
                && to.as_str() >= format!("{lo:04}-01-01").as_str()
        }
        _ => false,
    }
}

/// Each variable's first representation in `period` (`Rep::state_order`), by
/// variable id.
fn columns_at(reps: &[Rep], period: Period) -> BTreeMap<i64, DiffColumn> {
    let mut found: Vec<&Rep> = reps
        .iter()
        .filter(|r| overlaps(&r.row, period.years()))
        .collect();
    found.sort_by(|a, b| a.state_order().cmp(&b.state_order()));
    let mut out = BTreeMap::new();
    for rep in found {
        out.entry(rep.variable_id).or_insert_with(|| DiffColumn {
            fqid: rep.row.fqid.clone(),
            var_id: rep.row.var_id,
            variable_name: rep.row.variable_name.clone(),
            data_type: rep.row.data_type.clone(),
            data_length: rep.row.data_length.clone(),
            column: rep.row.column.clone(),
        });
    }
    out
}

fn by_fqid<'a>(columns: impl Iterator<Item = (&'a i64, &'a DiffColumn)>) -> Vec<DiffColumn> {
    let mut out: Vec<DiffColumn> = columns.map(|(_, c)| c.clone()).collect();
    out.sort_by(|a, b| a.fqid.cmp(&b.fqid));
    out
}

/// The register's representations in scope, or one variable's, of the variant
/// `variant` when given (a slug, or `_default` for the unslugged one). Holdings
/// clips them to the calendar `years` as well as to their held periods.
fn representations(
    conn: &Connection,
    scope: Scope,
    register_id: i64,
    variable_id: Option<i64>,
    variant: Option<&str>,
    years: Option<(u16, u16)>,
) -> Result<Vec<Rep>, Error> {
    // A window's own content replaces its state's only where the window is per
    // column, and a window never inherits its state's operational definition
    // (today's `_expand_state_windows`).
    // Holdings needs no variant predicate here: `held_clip` keeps only mapped
    // representations, whose variant is therefore held.
    let per_column = |field: &str| {
        format!("CASE WHEN w.column_metadata = 'per_column' THEN w.{field} ELSE vs.{field} END")
    };
    let sql = format!(
        "SELECT es.variable_id, es.register_variant_id, es.state_id, vs.delivery_column_name, \
         es.canonical_column, v.slug, rv.slug, rv.name, rv.description, vs.period_scope, \
         es.valid_from, es.valid_to, p.slug, r.slug, v.name, \
         {VAR_ID}, v.source_label, es.delivery_column_name, {}, {}, \
         CASE WHEN w.coding_metadata = 'per_column' THEN w.value_set_version_label \
         ELSE vs.value_set_version_label END, \
         CASE WHEN es.window_valid_from IS NULL THEN vs.operational_definition \
         WHEN w.column_metadata = 'per_column' THEN w.operational_definition END, \
         {}, {}, {}, \
         (SELECT g.group_key || char(0) || g.label FROM concept_group_variable m \
          JOIN concept_group g ON g.group_id = m.group_id \
          WHERE m.variable_id = v.variable_id ORDER BY g.group_key LIMIT 1) \
         FROM variable v JOIN expanded_state es ON es.variable_id = v.variable_id \
         JOIN variable_state vs ON vs.state_id = es.state_id \
         JOIN register_variant rv ON rv.register_variant_id = es.register_variant_id \
         JOIN register r ON r.register_id = v.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         LEFT JOIN variable_alias_window w ON w.variable_id = es.variable_id \
         AND w.register_variant_id = es.register_variant_id \
         AND w.delivery_column_name = es.delivery_column_name \
         AND w.valid_from = es.window_valid_from \
         WHERE v.register_id = ?1 AND (?2 IS NULL OR v.variable_id = ?2) \
         AND (?3 IS NULL OR rv.slug = ?3 OR (?3 = '_default' AND rv.slug IS NULL)) AND es.kind != 'base_fallback' \
         ORDER BY v.slug, rv.slug, vs.valid_from, vs.valid_to, \
         vs.value_set_version_label, {}",
        per_column("data_type"),
        per_column("data_length"),
        per_column("source_register_text"),
        per_column("definition"),
        per_column("measurement_unit"),
        states::within_state("es"),
    );
    let mut stmt = conn.prepare(&sql)?;
    let reps = stmt
        .query_map((register_id, variable_id, variant), |row| {
            let [provider, register, variable]: [Option<String>; 3] =
                [row.get(12)?, row.get(13)?, row.get(5)?];
            let group: Option<String> = row.get(25)?;
            let (group, group_label) = match group.as_deref().and_then(|g| g.split_once('\0')) {
                Some((key, label)) => (
                    Some(format!(
                        "group/{}/{}/{key}",
                        provider.as_deref().unwrap_or_default(),
                        register.as_deref().unwrap_or_default()
                    )),
                    Some(label.to_owned()),
                ),
                None => (None, None),
            };
            Ok(Rep {
                variable_id: row.get(0)?,
                variant_id: row.get(1)?,
                state_id: row.get(2)?,
                state_column: row.get(3)?,
                canonical: row.get(4)?,
                row: SchemaColumn {
                    variant: row.get(6)?,
                    variant_name: row.get(7)?,
                    variant_description: row.get(8)?,
                    period_scope: row.get(9)?,
                    valid_from: row.get(10)?,
                    valid_to: row.get(11)?,
                    fqid: fqid(&[provider, register, variable.clone()]),
                    variable_name: row.get(14)?,
                    var_id: row.get(15)?,
                    source: row.get(16)?,
                    column: row.get(17)?,
                    data_type: row.get(18)?,
                    data_length: row.get(19)?,
                    value_set_version_label: row.get(20)?,
                    operational_definition: row.get(21)?,
                    source_register_text: row.get(22)?,
                    definition: row.get(23)?,
                    measurement_unit: row.get(24)?,
                    group,
                    group_label,
                },
                variable_slug: variable,
            })
        })?
        .collect::<rusqlite::Result<Vec<Rep>>>()?;
    match scope {
        Scope::Reference => Ok(reps),
        Scope::Holdings => held_clip(conn, register_id, reps, years),
    }
}

/// Today's `_scope_states`: a representation the steward holds, clipped to its held
/// periods and the request's `years` (merged, day-adjacent ones joined); one per
/// merged interval.
fn held_clip(
    conn: &Connection,
    register_id: i64,
    reps: Vec<Rep>,
    years: Option<(u16, u16)>,
) -> Result<Vec<Rep>, Error> {
    type Key = (i64, i64, String, String);
    let mut held: BTreeMap<Key, held::Periods> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT hm.variable_id, hm.variant_id, hm.representation_canonical, ht.scope, \
         hp.lo, hp.hi FROM holding_mapping hm JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id) \
         WHERE ht.scope != 'unknown' \
         AND hm.variable_id IN (SELECT variable_id FROM variable WHERE register_id = ?)",
    )?;
    let mut rows = stmt.query([register_id])?;
    while let Some(row) = rows.next()? {
        held.entry((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            .or_default()
            .push((row.get(4)?, row.get(5)?));
    }
    let request = years.map(|(lo, hi)| (format!("{lo:04}-01-01"), format!("{hi:04}-12-31")));
    let request = request.as_ref().map(|(lo, hi)| (lo.as_str(), hi.as_str()));
    let mut out = Vec::new();
    for rep in reps {
        let Some(canonical) = rep.canonical.clone() else {
            continue;
        };
        let key = (
            rep.variable_id,
            rep.variant_id,
            canonical,
            rep.row.period_scope.clone(),
        );
        let Some(periods) = held.get(&key) else {
            continue;
        };
        let (Some(from), Some(to)) = (rep.row.valid_from.as_deref(), rep.row.valid_to.as_deref())
        else {
            // Year-independent: held when any table of its scope maps it.
            out.push(rep);
            continue;
        };
        for (lo, hi) in held::clip(periods, (from, to), request) {
            let mut piece = rep.clone();
            piece.row.valid_from = Some(lo);
            piece.row.valid_to = Some(hi);
            out.push(piece);
        }
    }
    Ok(out)
}
