//! `resolve`: delivered column names to variables, through `name_fold` → `alias` →
//! `variable`.

use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs;
use super::{Cache, Operation, Param, Params, Server, Type, component};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "resolve",
    paths: &["/api/resolve"],
    tool: Some("resolve"),
    description: "Match delivered column names (at most 200) to variables, optionally within \
                  one register. A name that matches nothing is reported as unmatched.",
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

/// `shape.ResolvedColumn`: a requested column and the variables it names.
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
    let conn = server.catalog.connect()?;
    let register = params
        .get("register")
        .map(|value| {
            let entity = refs::resolve(&conn, value)?;
            if entity.kind == "register" {
                Ok(entity.id)
            } else {
                Err(Error::new(
                    Code::InvalidRef,
                    format!("{value:?} is a {}, not a register.", entity.kind),
                    vec![(*value).into()],
                ))
            }
        })
        .transpose()?;
    let mut lookup = conn.prepare(
        "SELECT v.pid, l.body, r.pid, a.kind
         FROM name_fold f JOIN alias a ON a.fold_id = f.id
           JOIN variable v ON v.id = a.variable_id
           JOIN text l ON l.id = v.label_text
           JOIN register r ON r.id = v.register_id
         WHERE f.folded = ?1 AND (?2 IS NULL OR v.register_id = ?2)
         ORDER BY v.pid",
    )?;
    let mut columns = Vec::new();
    for column in params.list("columns") {
        let matches: Vec<ResolvedMatch> = lookup
            .query_map((reg_core::fold_identity(column), register), |row| {
                Ok(ResolvedMatch {
                    fqid: row.get(0)?,
                    name: row.get(1)?,
                    register: row.get(2)?,
                    alias: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
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
