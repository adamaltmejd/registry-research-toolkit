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

/// A retained source limitation or interpretation assumption: today's
/// `DataWarning`, as the build stored it.
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
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
    source_subject: String,
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
    let period = super::period(params)?;
    let unassigned_only = super::flag(params, "unassigned_only")?;
    let variant = params.get("variant").copied();
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
        "SELECT w.warning_json FROM data_warning w WHERE {} ORDER BY w.warning_id",
        clauses.join(" AND ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let found = stmt
        .query_map(args.as_slice(), |row| row.get::<_, String>(0))?
        .map(|json| {
            let json = json?;
            serde_json::from_str(&json).map_err(|err| {
                Error::new(
                    Code::InternalError,
                    format!("Unreadable data warning: {err}"),
                    vec![],
                )
            })
        })
        .collect::<Result<Vec<DataWarning>, Error>>()?;
    Ok(serde_json::to_value(found).expect("warnings serialize"))
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
