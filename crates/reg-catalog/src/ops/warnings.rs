//! `warnings`: a register's or variable's data warnings (today's
//! `Catalog.data_warnings` behind `/data_warnings`), filtered by period, variant and
//! delivery column, and in holdings scope by what is held.

use reg_core::Period;
use rusqlite::ToSql;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::{ArrayBuilder, RefOr, Schema};

use super::refs::{self, Target};
use super::{Components, Params, Server, component};
use crate::{Code, Error, Scope};

/// A retained source limitation or interpretation assumption: the builder's
/// `DataWarning`, reconstructed from its `data_warning` row.
#[derive(Serialize, ToSchema)]
pub struct DataWarning {
    /// SHA-256 of the rest of the warning.
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
    acknowledged_by: Option<String>,
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
        "{SELECT} WHERE {} ORDER BY w.warning_id",
        clauses.join(" AND ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let found = stmt
        .query_map(args.as_slice(), stored)?
        .collect::<rusqlite::Result<Vec<DataWarning>>>()?;
    Ok(serde_json::to_value(found).expect("warnings serialize"))
}

/// A `data_warning` row joined to what rebuilds its warning: the owner FQIDs and
/// variant slug from their IDs, and the shared summary and detail text.
const SELECT: &str = "SELECT lower(hex(w.warning_id)), p.slug || '/' || r.slug, v.slug, \
     rv.slug, w.delivery_column_name, w.valid_from, w.valid_to, w.code, w.severity, \
     st.text, dt.text, w.evidence_json FROM data_warning w \
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
        " AND ht.scope = 'intervals' AND EXISTS (SELECT 1 FROM holding_period hp \
         WHERE hp.table_id = ht.table_id AND hp.lo <= :hi AND hp.hi >= :lo)"
    } else {
        ""
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
         AND hm.representation_canonical = COALESCE({CANONICAL}, {held_column})))"
    )
}
