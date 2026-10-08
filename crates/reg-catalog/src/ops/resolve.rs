//! `resolve`: delivered column names to the variables delivering them (today's
//! `resolve`), by exact `fold_identity` match over `variable_alias`. Holdings matches
//! only an alias whose representation a known-scope table maps.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Value, json};
use utoipa::ToSchema;

use super::refs::{self, VAR_ID, fqid};
use super::{Params, Server};
use crate::held::{self, Narrow};
use crate::{Error, Scope};

#[derive(Serialize, ToSchema)]
pub struct Resolved {
    /// One row per requested name, in request order.
    columns: Vec<Column>,
}

/// A requested name: `matched` with its variables, or `no_match`.
#[derive(Serialize, ToSchema)]
pub struct Column {
    column_name: String,
    /// `matched` or `no_match`.
    status: &'static str,
    /// Ordered by register, provider key and variable; split siblings share a
    /// `var_id` and differ by `fqid`.
    matches: Vec<Match>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct Match {
    fqid: Option<String>,
    /// SCB's numeric variable id; null for other providers.
    var_id: Option<i64>,
    variable_name: Option<String>,
    /// The column's one spelling for the variable: a state's own, else the lowest
    /// alias spelling.
    matched_column: String,
    register: Option<String>,
}

pub fn resolve(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let register = params
        .get("register")
        .map(|value| refs::register(&conn, scope, value))
        .transpose()?
        .map(|register| register.id);
    let columns = params.list("columns");
    let wanted: Vec<String> = columns.iter().map(|c| reg_core::fold_identity(c)).collect();
    // One pass over the aliases for every requested name (a join on the names
    // instead folded every alias once per name: 4 s for 200). The state's spelling
    // takes the fold over the aliases' (today's `representative_columns`); the
    // alias's own row decides holdings, never the request's string.
    // simplify: the state spelling reads each matched variable's wide state rows
    // (1.3 s for the pin's 200 most delivered columns, 3.9k variables; one common
    // column 40 ms); read the narrow base rows of `expanded_state` instead if
    // resolve's latency matters.
    let sql = format!(
        "WITH alias AS MATERIALIZED (SELECT * FROM (SELECT variable_id, \
         register_variant_id, delivery_column_name, \
         fold_identity(delivery_column_name) AS lower FROM variable_alias) \
         WHERE lower IN (SELECT value FROM json_each(?1))) \
         SELECT a.lower, MIN(a.delivery_column_name), \
         (SELECT MIN(vs.delivery_column_name) FROM variable_state vs \
          WHERE vs.variable_id = v.variable_id \
          AND fold_identity(vs.delivery_column_name) = a.lower), \
         p.slug, r.slug, v.slug, {VAR_ID}, v.name \
         FROM alias a \
         JOIN variable v ON v.variable_id = a.variable_id \
         JOIN register r ON r.register_id = v.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         WHERE (?2 IS NULL OR v.register_id = ?2) AND {} \
         GROUP BY a.lower, v.variable_id \
         ORDER BY a.lower, v.register_id, v.provider_key, v.variable_id",
        held::variable(
            scope,
            "a.variable_id",
            Narrow {
                variant: Some("a.register_variant_id"),
                column: Some("a.delivery_column_name"),
                ..Narrow::default()
            }
        ),
    );
    let mut found: BTreeMap<String, Vec<Match>> = BTreeMap::new();
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query((json!(wanted).to_string(), register))?;
    while let Some(row) = rows.next()? {
        let [provider, register, variable]: [Option<String>; 3] =
            [row.get(3)?, row.get(4)?, row.get(5)?];
        let alias: String = row.get(1)?;
        let state: Option<String> = row.get(2)?;
        found.entry(row.get(0)?).or_default().push(Match {
            fqid: fqid(&[provider.clone(), register.clone(), variable]),
            var_id: row.get(6)?,
            variable_name: row.get(7)?,
            matched_column: state.unwrap_or(alias),
            register: fqid(&[provider, register]),
        });
    }
    let columns = columns
        .iter()
        .zip(&wanted)
        .map(|(name, lower)| {
            let matches = found.get(lower).cloned().unwrap_or_default();
            Column {
                column_name: (*name).to_owned(),
                status: if matches.is_empty() {
                    "no_match"
                } else {
                    "matched"
                },
                matches,
            }
        })
        .collect();
    Ok(serde_json::to_value(Resolved { columns }).expect("Resolved serializes"))
}
