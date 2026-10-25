//! `warnings`: a register's or variable's data warnings (today's
//! `Catalog.data_warnings` behind `/data_warnings`), filtered by period, variant and
//! delivery column, and in holdings scope by what is held, a dated warning clipped
//! to the held periods.

use std::collections::BTreeSet;

use reg_core::Period;
use rusqlite::{Connection, OptionalExtension, ToSql, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::{ArrayBuilder, RefOr, Schema};

use super::refs::{self, Target};
use super::{Components, Params, Server, component};
use crate::held::{HELD_HI, HELD_LO, serves};
use crate::{Code, Error, Scope};

/// A retained source limitation or interpretation assumption: the builder's
/// `DataWarning`, reconstructed from its `data_warning` row.
#[derive(Serialize, ToSchema)]
pub struct DataWarning {
    /// SHA-256 of the rest of the warning with `acknowledged_by` and `case_id`
    /// set to null, so moving a curation entry never changes it.
    warning_id: String,
    register_fqid: String,
    variable_fqid: Option<String>,
    variant: Option<String>,
    delivery_column_name: Option<String>,
    valid_from: Option<String>,
    valid_to: Option<String>,
    code: String,
    /// `warning` or `error`.
    severity: String,
    summary: String,
    detail: String,
    diagnostic_detail_sha256: String,
    fields: Vec<String>,
    refs: Vec<SourceRecordRef>,
    withheld_output: Vec<String>,
    /// The curation entry that accepts the issue, by file and position. When
    /// several entries yield the same warning, the first by file, then position.
    acknowledged_by: Option<String>,
    /// The curation entry the warning annotates, by file and position; the first
    /// by file, then position, when several yield the same warning.
    case_id: Option<String>,
}

/// The `evidence_json` of a `data_warning` row: the fields without a column.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    diagnostic_detail_sha256: String,
    fields: Vec<String>,
    refs: Vec<SourceRecordRef>,
    withheld_output: Vec<String>,
    acknowledged_by: Option<String>,
    case_id: Option<String>,
}

/// One semantic source member a warning cites.
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceRecordRef {
    source: String,
    semantic_record_key: Vec<String>,
}

pub fn schema(components: &mut Components) -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(component::<DataWarning>(components))
        .into()
}

/// The representative spelling of the warning's column at its variant (today's
/// `py_catalog_column`): a state's spelling of its fold, else the lowest alias
/// spelling, else the column as written.
const CANONICAL: &str = "COALESCE(\
    (SELECT MIN(s.delivery_column_name) FROM variable_state s \
     WHERE s.variable_id = w.variable_id AND s.register_variant_id = w.register_variant_id \
     AND fold_identity(s.delivery_column_name) = fold_identity(w.delivery_column_name)), \
    (SELECT MIN(a.delivery_column_name) FROM variable_alias_window a \
     WHERE a.variable_id = w.variable_id AND a.register_variant_id = w.register_variant_id \
     AND fold_identity(a.delivery_column_name) = fold_identity(w.delivery_column_name)), \
    w.delivery_column_name)";

pub fn warnings(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let period = super::period(params, "period")?;
    let unassigned_only = super::flag(params, "unassigned_only")?;
    let variant = super::variant(params)?;
    let representation = params.get("representation").copied();
    if representation.is_some_and(|r| {
        reg_core::py_strip(r).is_empty()
            || r.chars().count() > 255
            || r.chars().any(|c| (c as u32) < 32)
    }) {
        return Err(Error::invalid_parameter("representation"));
    }
    let conn = server.catalog.connect()?;
    let reference = params["ref"];
    let (register_id, variable_id) = match refs::resolve(&conn, scope, Some(reference))? {
        Target::Register { id, .. } => (id, None),
        Target::Variable { id } => (
            conn.query_row(
                "SELECT register_id FROM variable WHERE variable_id = ?",
                [id],
                |row| row.get(0),
            )?,
            Some(id),
        ),
        _ => {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{reference:?} names neither a register nor a variable."),
                vec![reference.into()],
            ));
        }
    };
    let bounds = period.map(Period::iso_bounds);
    let mut clauses = vec!["w.register_id = :register".to_owned()];
    let mut args: Vec<(&str, &dyn ToSql)> = vec![(":register", &register_id)];
    if unassigned_only {
        clauses.push("w.variable_id IS NULL".into());
    }
    if let Some(id) = &variable_id {
        clauses.push("(w.variable_id IS NULL OR w.variable_id = :variable)".into());
        args.push((":variable", id));
    }
    if let Some(variant) = &variant {
        clauses.push(
            "(w.register_variant_id IS NULL OR (SELECT rv.slug FROM register_variant rv \
             WHERE rv.register_variant_id = w.register_variant_id) = :variant)"
                .into(),
        );
        args.push((":variant", variant));
    }
    if let Some(representation) = &representation {
        clauses.push(format!(
            "(w.delivery_column_name IS NULL OR {CANONICAL} = :representation)"
        ));
        args.push((":representation", representation));
    }
    if let Some((lo, hi)) = &bounds {
        clauses.push("(w.valid_from IS NULL OR w.valid_from <= :hi)".into());
        clauses.push("(w.valid_to IS NULL OR w.valid_to >= :lo)".into());
        args.push((":lo", lo));
        args.push((":hi", hi));
    }
    if scope == Scope::Holdings {
        clauses.push(held(
            bounds.is_some(),
            variant.is_some(),
            representation.is_some(),
        ));
    }
    let sql = format!(
        "{SELECT}, w.variable_id, w.register_variant_id, {CANONICAL} FROM {FROM} \
         WHERE {} ORDER BY w.warning_id",
        clauses.join(" AND ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let found = stmt
        .query_map(args.as_slice(), |row| {
            let owner = Owner {
                variable: row.get(12)?,
                variant: row.get(13)?,
                column: row.get(14)?,
            };
            Ok((stored(row)?, owner))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let found: Vec<DataWarning> = match scope {
        Scope::Reference => found.into_iter().map(|(w, _)| w).collect(),
        Scope::Holdings => {
            let request = bounds.as_ref().map(|(lo, hi)| (lo.as_str(), hi.as_str()));
            held_windows(&conn, register_id, found, variant, representation, request)?
        }
    };
    Ok(serde_json::to_value(found).expect("warnings serialize"))
}

/// [`held_window`] over `found`, under the request's variant slug, representation
/// and period.
fn held_windows(
    conn: &Connection,
    register_id: i64,
    found: Vec<(DataWarning, Owner)>,
    variant: Option<&str>,
    representation: Option<&str>,
    request: Option<(&str, &str)>,
) -> Result<Vec<DataWarning>, Error> {
    let selected = match variant {
        None => None,
        Some(slug) => conn
            .prepare_cached(
                "SELECT register_variant_id FROM register_variant \
                 WHERE register_id = ? AND slug = ?",
            )?
            .query_row(params![register_id, slug], |row| row.get(0))
            .optional()?,
    };
    let narrow = Narrow {
        variant: selected,
        representation,
        request,
    };
    let mut kept = Vec::new();
    for (w, owner) in found {
        if let Some(w) = held_window(conn, register_id, w, &owner, narrow)? {
            kept.push(w);
        }
    }
    Ok(kept)
}

/// What a warning row is attached to, beyond its public fields: its variable and
/// variant ids and its column's canonical spelling ([`CANONICAL`]).
struct Owner {
    variable: Option<i64>,
    variant: Option<i64>,
    column: Option<String>,
}

/// The request's narrowing, which stands in for a warning's unset variant or column
/// as the SQL [`held`] predicate's does.
#[derive(Clone, Copy)]
struct Narrow<'a> {
    variant: Option<i64>,
    representation: Option<&'a str>,
    request: Option<(&'a str, &'a str)>,
}

/// The bounds an unset end of a half-open warning window stands for.
const FIRST_DAY: &str = "0001-01-01";
const LAST_DAY: &str = "9999-12-31";

/// A holdings-scope warning's window clipped to what the steward holds (#1900): a
/// variable's warning to the held periods of its variable at its variant and
/// representation (else the requested ones, else every held one, as [`held`]
/// admits it), a register-wide warning to the register's held span. The window
/// becomes the hull of the clipped periods, so the warning keeps one row and its
/// id; a warning wholly outside them, or whose clipped window misses the requested
/// period, is dropped. A warning without a window, or one whose cell the steward
/// holds in no dated table (a year-independent holding), has nothing to clip
/// against and is kept as stored.
fn held_window(
    conn: &Connection,
    register_id: i64,
    mut w: DataWarning,
    owner: &Owner,
    narrow: Narrow,
) -> Result<Option<DataWarning>, Error> {
    if w.valid_from.is_none() && w.valid_to.is_none() {
        return Ok(Some(w));
    }
    let own = (
        w.valid_from.as_deref().unwrap_or(FIRST_DAY),
        w.valid_to.as_deref().unwrap_or(LAST_DAY),
    );
    let held = match owner.variable {
        None => crate::held::register_periods(conn, register_id)?,
        Some(variable) => variable_periods(
            conn,
            variable,
            owner.variant.or(narrow.variant),
            owner.column.as_deref().or(narrow.representation),
            own,
        )?,
    };
    if held.is_empty() {
        return Ok(Some(w));
    }
    let clipped = crate::held::clip(&held, own, None);
    let (Some((lo, _)), Some((_, hi))) = (clipped.first(), clipped.last()) else {
        return Ok(None);
    };
    if narrow
        .request
        .is_some_and(|request| crate::held::clip(&held, own, Some(request)).is_empty())
    {
        return Ok(None);
    }
    w.valid_from = Some(lo.clone());
    w.valid_to = Some(hi.clone());
    Ok(Some(w))
}

/// The dated held periods of `variable` at `variant` (else at every variant a dated
/// table maps it at) under `rep` (else under each such mapping's own
/// representation), as [`crate::held::periods`] reads them over the window `own`.
fn variable_periods(
    conn: &Connection,
    variable: i64,
    variant: Option<i64>,
    rep: Option<&str>,
    own: (&str, &str),
) -> Result<crate::held::Periods, Error> {
    let cells: BTreeSet<(i64, String)> = conn
        .prepare_cached(
            "SELECT DISTINCT hm.variant_id, hm.representation_canonical FROM holding_mapping hm \
             JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) \
             WHERE hm.variable_id = ?1 AND (?2 IS NULL OR hm.variant_id = ?2) \
             AND ht.scope = 'intervals'",
        )?
        .query_map(params![variable, variant], |row| {
            let mapped: String = row.get(1)?;
            Ok((row.get(0)?, rep.map_or(mapped, str::to_owned)))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut periods = Vec::new();
    for (variant, rep) in cells {
        periods.extend(crate::held::periods(
            conn,
            variable,
            variant,
            &rep,
            "intervals",
            (Some(own.0), Some(own.1)),
        )?);
    }
    Ok(periods)
}

/// A `data_warning` row joined to what rebuilds its warning (`SELECT … FROM FROM`):
/// the owner FQIDs and variant slug from their IDs, and the shared summary and
/// detail text.
const SELECT: &str = "SELECT lower(hex(w.warning_id)), p.slug || '/' || r.slug, v.slug, \
     rv.slug, w.delivery_column_name, w.valid_from, w.valid_to, w.code, w.severity, \
     st.text, dt.text, w.evidence_json";
const FROM: &str = "data_warning w \
     JOIN register r USING(register_id) JOIN provider p USING(provider_id) \
     LEFT JOIN variable v ON v.variable_id = w.variable_id \
     LEFT JOIN register_variant rv ON rv.register_variant_id = w.register_variant_id \
     JOIN data_warning_text st ON st.text_id = w.summary_id \
     JOIN data_warning_text dt ON dt.text_id = w.detail_id";

/// The warning a `SELECT` row stores.
fn stored(row: &rusqlite::Row) -> rusqlite::Result<DataWarning> {
    let register_fqid: String = row.get(1)?;
    let variable: Option<String> = row.get(2)?;
    let evidence: String = row.get(11)?;
    let evidence: Evidence = serde_json::from_str(&evidence).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, err.into())
    })?;
    Ok(DataWarning {
        warning_id: row.get(0)?,
        variable_fqid: variable.map(|v| format!("{register_fqid}/{v}")),
        register_fqid,
        variant: row.get(3)?,
        delivery_column_name: row.get(4)?,
        valid_from: row.get(5)?,
        valid_to: row.get(6)?,
        code: row.get(7)?,
        severity: row.get(8)?,
        summary: row.get(9)?,
        detail: row.get(10)?,
        diagnostic_detail_sha256: evidence.diagnostic_detail_sha256,
        fields: evidence.fields,
        refs: evidence.refs,
        withheld_output: evidence.withheld_output,
        acknowledged_by: evidence.acknowledged_by,
        case_id: evidence.case_id,
    })
}

/// Today's held-mapping predicate: a variable's warning is held at its variant and
/// column, else at the requested ones, else at any; in tables of the period when one
/// is requested.
fn held(period: bool, variant: bool, representation: bool) -> String {
    let held_period = if period {
        format!(
            " AND ht.scope = 'intervals' AND EXISTS (SELECT 1 FROM holding_period hp \
             WHERE hp.table_id = ht.table_id AND {HELD_LO} <= :hi AND {HELD_HI} >= :lo)"
        )
    } else {
        String::new()
    };
    let held_variant = if variant {
        "(SELECT selected.register_variant_id FROM register_variant selected \
         WHERE selected.register_id = w.register_id AND selected.slug = :variant)"
    } else {
        "hm.variant_id"
    };
    let held_column = if representation {
        ":representation"
    } else {
        "hm.representation_canonical"
    };
    format!(
        "(w.variable_id IS NULL OR EXISTS (SELECT 1 FROM holding_mapping hm \
         JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) \
         WHERE hm.variable_id = w.variable_id AND ht.scope != 'unknown'{held_period} \
         AND hm.variant_id = COALESCE(w.register_variant_id, {held_variant}) \
         AND {}))",
        serves(&format!("COALESCE({CANONICAL}, {held_column})"))
    )
}
