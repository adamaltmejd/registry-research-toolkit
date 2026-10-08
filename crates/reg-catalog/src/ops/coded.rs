//! `coded_variables`: the variables with coded value sets, by common name, read from
//! the compiled `coded_variable_stats` of the scope and ordered by distinct codes
//! (ties by name), cursor-paged.

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::{Params, Server, cursor};
use crate::{Error, Scope, hex};

/// `Page<CodedVariable>`.
#[derive(Serialize, ToSchema)]
pub struct CodedPage {
    items: Vec<CodedVariable>,
    next_cursor: Option<String>,
}

/// The CLI's `get coded-variables` row: a common name's distinct codes over every
/// coded state under it, its registers and its coded states.
#[derive(Serialize, ToSchema)]
pub struct CodedVariable {
    variable_name: String,
    n_distinct_codes: i64,
    n_registers: i64,
    n_instances: i64,
}

pub fn coded_variables(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let limit = super::limit(params)?;
    let conn = catalog.connect()?;
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM coded_variable_stats WHERE scope = ?",
        [scope.as_str()],
        |row| row.get(0),
    )?;
    let total = usize::try_from(total).expect("a row count");
    // The cursor binds the scope, which selects the rows, never `limit`.
    let context = hex(&Sha256::digest(json!([scope]).to_string().as_bytes()));
    let offset = params
        .get("cursor")
        .map(|c| cursor::decode(c, catalog.generation(), &context, total.max(1)))
        .transpose()?
        .map_or(0, |(offset, _)| offset);
    // `limit` and `offset` are integers the request bounds.
    let items = conn
        .prepare(&format!(
            "SELECT variable_name, n_distinct_codes, n_registers, n_instances \
             FROM coded_variable_stats WHERE scope = ? \
             ORDER BY n_distinct_codes DESC, variable_name LIMIT {limit} OFFSET {offset}"
        ))?
        .query_map([scope.as_str()], |row| {
            Ok(CodedVariable {
                variable_name: row.get(0)?,
                n_distinct_codes: row.get(1)?,
                n_registers: row.get(2)?,
                n_instances: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let end = offset + items.len();
    let page = CodedPage {
        items,
        next_cursor: (end < total).then(|| cursor::encode(catalog.generation(), &context, end, "")),
    };
    Ok(serde_json::to_value(page).expect("CodedPage serializes"))
}
