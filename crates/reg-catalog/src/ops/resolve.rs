//! `resolve`: delivered column names to variables, through `name_fold` → `alias` →
//! `variable`.

use rusqlite::OptionalExtension;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs;
use super::{Cache, Operation, Param, Params, Server, Type, component};
use crate::Error;

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "resolve",
    paths: &["/api/resolve"],
    tool: Some("resolve"),
    description: "Find the variables behind column names (up to 200), optionally in one \
        register. Names that match nothing are reported as unmatched.",
    params: &[
        Param::required("columns", Type::Strings),
        Param::optional("register", Type::Ref),
    ],
    cache: Cache::Minute,
    run: resolve,
    result: component::<Resolved>,
}];

/// `{columns: [ResolvedColumn]}`.
#[derive(Serialize, ToSchema)]
pub struct Resolved {
    columns: Vec<ResolvedColumn>,
}

/// A requested column and the variables it names.
#[derive(Serialize, ToSchema)]
pub struct ResolvedColumn {
    /// As asked.
    column: String,
    /// `matched` for one variable, `ambiguous` for several, `unmatched` for none.
    status: &'static str,
    matches: Vec<ResolvedMatch>,
}

#[derive(Serialize, ToSchema)]
pub struct ResolvedMatch {
    fqid: String,
    /// The variable's label.
    name: String,
    /// The register's fqid.
    register: String,
    /// `delivered` or `curated`.
    alias: String,
}

fn resolve(server: &Server, params: &Params) -> Result<Value, Error> {
    let register = params
        .get("register")
        .map(|value| {
            let origin = server.catalog.connect_for_ref(value)?;
            let entity = refs::resolve(&origin, value)?;
            if entity.kind == "register" {
                Ok(entity.pid)
            } else {
                Err(Error::invalid_ref(
                    value,
                    format!("{value:?} is a {}, not a register.", entity.kind),
                ))
            }
        })
        .transpose()?;
    let conn = server.catalog.connect()?;
    let extra = server.catalog.connect_holdings()?;
    let mut lookups = Vec::new();
    for origin in std::iter::once(&conn).chain(extra.as_ref()) {
        let local_register = if let Some(pid) = &register {
            let Some(id) = origin
                .query_row("SELECT id FROM register WHERE pid = ?", [pid], |row| {
                    row.get::<_, i64>(0)
                })
                .optional()?
            else {
                continue;
            };
            Some(id)
        } else {
            None
        };
        lookups.push((
            origin.prepare(
                "SELECT v.pid, l.body, r.pid, a.kind
         FROM name_fold f JOIN alias a ON a.fold_id = f.id
           JOIN variable v ON v.id = a.variable_id
           JOIN text l ON l.id = v.label_text
           JOIN register r ON r.id = v.register_id
         WHERE f.folded = ?1 AND (?2 IS NULL OR v.register_id = ?2)
         ORDER BY v.pid",
            )?,
            local_register,
        ));
    }
    let mut columns = Vec::new();
    for column in params.list("columns") {
        let mut matches = Vec::new();
        for (lookup, local_register) in &mut lookups {
            matches.extend(
                lookup
                    .query_map((reg_core::fold_identity(column), *local_register), |row| {
                        Ok(ResolvedMatch {
                            fqid: row.get(0)?,
                            name: row.get(1)?,
                            register: row.get(2)?,
                            alias: row.get(3)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        // Steward mode: a variable the steward does not hold is not served.
        matches.retain(|m| server.catalog.serves("variable", &m.fqid));
        matches.sort_by(|a, b| a.fqid.cmp(&b.fqid));
        matches.dedup_by(|a, b| a.fqid == b.fqid);
        columns.push(ResolvedColumn {
            column: (*column).to_owned(),
            status: match matches.len() {
                0 => "unmatched",
                1 => "matched",
                _ => "ambiguous",
            },
            matches,
        });
    }
    Ok(serde_json::to_value(Resolved { columns }).expect("Resolved serializes"))
}
