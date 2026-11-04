//! `findings`: the review items and notices on a register, a table or a variable.

use rusqlite::named_params;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Named};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, cursor, limit};
use crate::{Error, rows};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "findings",
    paths: &["/api/findings/{ref}"],
    tool: None,
    description: "The review items and notices on a register, table or variable (including its \
        column histories), optionally for one period, table or rule code, as one group per \
        severity and code: its count and its first `limit` items. A group's `next_cursor`, \
        sent with `code` set to the group's code, pages its items.",
    params: &[
        Param::required("ref", Type::Ref),
        Param::optional("period", Type::Period),
        Param::optional("table", Type::Ref),
        Param::optional("code", Type::String),
        Param::optional("limit", Type::Limit),
        Param::optional("cursor", Type::Cursor),
    ],
    cache: Cache::Minute,
    run: findings,
    result: component::<FindingGroups>,
}];

#[derive(Serialize, ToSchema)]
pub struct FindingGroups {
    /// Review groups first, then notices; by code within each.
    groups: Vec<FindingGroup>,
}

/// The findings of one severity and code.
#[derive(Serialize, ToSchema)]
pub struct FindingGroup {
    code: String,
    /// `review` or `notice`.
    severity: String,
    /// How many findings the group holds under the request's filters.
    count: usize,
    /// One page of them, in subject order.
    items: Vec<Finding>,
    /// The next page, for a request with `code` set to this group's code.
    #[schema(required = true)]
    next_cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct Finding {
    /// A rule code and its sub-kind, `F7:edition-disagreement`.
    code: String,
    /// `review` or `notice`.
    severity: String,
    subject: Subject,
    #[schema(required = true)]
    period: Option<Period>,
    message: String,
    /// A curator's caveat.
    #[schema(required = true)]
    caveat: Option<String>,
}

/// What a finding is about.
#[derive(Serialize, ToSchema)]
pub struct Subject {
    /// `register`, `table`, `release`, `variable`, `segment`, `occurrence` or `valueset`.
    kind: String,
    /// The register, table or variable it is about.
    #[schema(required = true)]
    fqid: Option<String>,
    /// The name of `fqid`, as `show` gives it.
    #[schema(required = true)]
    name: Option<String>,
    /// Its table.
    #[schema(required = true)]
    table: Option<String>,
    /// The name of `table`.
    #[schema(required = true)]
    table_name: Option<String>,
    /// Its period; `null` when not known.
    #[schema(required = true)]
    period: Option<Period>,
    /// A release's literal delivery name.
    #[schema(required = true)]
    release: Option<String>,
    /// The exact column name in that delivery.
    #[schema(required = true)]
    spelling: Option<String>,
    /// A value set's hash.
    #[schema(required = true)]
    value_set: Option<String>,
}

// The findings of the entity a ref names, by subject: the register's own, or those of
// the table or variable and their segments and occurrences (a table's releases too).
const SCOPE_REGISTER: &str = "f.register_id = :id";
const SCOPE_TABLE: &str = "(f.subject_kind = 'table' AND f.subject_id = :id)
    OR (f.subject_kind = 'release'
        AND f.subject_id IN (SELECT id FROM release WHERE table_id = :id))
    OR (f.subject_kind = 'segment'
        AND f.subject_id IN (SELECT id FROM segment WHERE table_id = :id))
    OR (f.subject_kind = 'occurrence'
        AND f.subject_id IN (SELECT id FROM occurrence WHERE table_id = :id))";
const SCOPE_VARIABLE: &str = "(f.subject_kind = 'variable' AND f.subject_id = :id)
    OR (f.subject_kind = 'segment'
        AND f.subject_id IN (SELECT id FROM segment WHERE variable_id = :id))
    OR (f.subject_kind = 'occurrence'
        AND f.subject_id IN (SELECT id FROM occurrence WHERE variable_id = :id))";

#[allow(clippy::too_many_lines)]
fn findings(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect_for_ref(params["ref"])?;
    let value = params["ref"];
    let entity = refs::resolve(&conn, value)?;
    let scope = match entity.kind.as_str() {
        "register" => SCOPE_REGISTER,
        "table" => SCOPE_TABLE,
        "variable" => SCOPE_VARIABLE,
        kind => {
            return Err(Error::invalid_ref(
                value,
                format!("{value:?} is a {kind}, not a register, a table or a variable."),
            ));
        }
    };
    let table = params
        .get("table")
        .map(|t| refs::table(&conn, t))
        .transpose()?;
    let window = super::days(params, "period")?;
    let code = params.get("code").copied();
    let take = limit(params)?;
    // A cursor pages one group, so it comes with that group's code (every cursor is
    // issued for one). A code holds one severity; were it to hold two, the offset
    // would page both groups alike.
    let offset = match params.get("cursor") {
        Some(_) if code.is_none() => return Err(Error::invalid_parameter("cursor")),
        Some(&token) => {
            cursor::decode(
                token,
                server.catalog.generation(),
                &cursor::context(params),
                // Cursors are plain hex, so a forged one may carry any offset; keep it
                // within what SQLite binds.
                usize::try_from(i64::MAX).unwrap_or(usize::MAX),
            )?
            .0
        }
        None => 0,
    };
    let end = offset.saturating_add(take);
    let bind = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);

    let found = rows(
        &conn,
        &sql(scope),
        named_params! {
            ":id": entity.id,
            ":table": table,
            ":lo": window.map(|w| w.0),
            ":hi": window.map(|w| w.1),
            ":code": code,
            ":offset": bind(offset),
            ":end": bind(end),
        },
        |row| {
            let placed = |start: Option<i64>, end: Option<i64>| {
                start.zip(end).map(|(s, e)| server.period(s, e))
            };
            let finding = Finding {
                code: row.get(0)?,
                severity: row.get(1)?,
                subject: Subject {
                    kind: row.get(2)?,
                    fqid: row.get(3)?,
                    name: None,
                    table: row.get(4)?,
                    table_name: None,
                    period: placed(row.get(5)?, row.get(6)?),
                    release: row.get(7)?,
                    spelling: row.get(8)?,
                    value_set: row.get(9)?,
                },
                period: placed(row.get(10)?, row.get(11)?),
                message: row.get(12)?,
                caveat: row.get(13)?,
            };
            let count = usize::try_from(row.get::<_, i64>(14)?).unwrap_or(0);
            Ok((finding, count))
        },
    )?;
    let names = refs::names(
        server,
        found
            .iter()
            .flat_map(|(f, _)| [&f.subject.fqid, &f.subject.table])
            .flatten()
            .map(String::as_str),
    )?;
    let mut groups: Vec<FindingGroup> = Vec::new();
    for (mut finding, count) in found {
        let subject = &mut finding.subject;
        let kind = match subject.kind.as_str() {
            "register" => Named::Register,
            "table" => Named::Table,
            _ => Named::Variable,
        };
        subject.name = subject.fqid.as_ref().and_then(|f| names.get(kind, f));
        subject.table_name = subject
            .table
            .as_ref()
            .and_then(|t| names.get(Named::Table, t));
        match groups.last_mut() {
            Some(group) if group.severity == finding.severity && group.code == finding.code => {
                group.items.push(finding);
            }
            _ => {
                let next_cursor = (end < count).then(|| {
                    let mut next = Params {
                        values: params.values.clone(),
                    };
                    next.values.insert("code", vec![finding.code.as_str()]);
                    cursor::encode(
                        server.catalog.generation(),
                        &cursor::context(&next),
                        end,
                        "",
                    )
                });
                groups.push(FindingGroup {
                    code: finding.code.clone(),
                    severity: finding.severity.clone(),
                    count,
                    items: vec![finding],
                    next_cursor,
                });
            }
        }
    }
    Ok(serde_json::to_value(FindingGroups { groups }).expect("findings serialize"))
}

/// The query for one `scope`: each (severity, code) group's total and its items
/// `:offset` < position <= `:end`, groups review first, then by code. Each subject kind
/// joins only its own table, so `fqid`, `table` and the period come out per kind.
/// Within a group, items run by subject kind, then subject, start and message; NULLs
/// sort first, which is the order the contract asks for. A group with no item in the
/// window is left out (only a cursor past its end does that).
fn sql(scope: &str) -> String {
    format!(
        "WITH hit AS (
         SELECT f.code AS code, f.severity AS severity, f.subject_kind AS kind,
                CASE f.subject_kind WHEN 'register' THEN reg.pid WHEN 'table' THEN tb.pid
                     ELSE vr.pid END AS fqid,
                st.pid AS tbl, COALESCE(rl.start, sg.start, oc.start) AS s_start,
                COALESCE(rl.end, sg.end, oc.end) AS s_end,
                rn.body AS rel, sp.exact AS spelling, vs.hash AS value_set,
                f.start AS f_start, f.end AS f_end, m.body AS message, c.body AS caveat,
                CASE f.subject_kind WHEN 'register' THEN 0 WHEN 'table' THEN 1
                     WHEN 'release' THEN 2 WHEN 'variable' THEN 3 WHEN 'segment' THEN 4
                     WHEN 'occurrence' THEN 5 ELSE 6 END AS kind_rank
         FROM finding f
           JOIN text m ON m.id = f.message_text
           LEFT JOIN text c ON c.id = f.caveat_text
           LEFT JOIN register reg ON f.subject_kind = 'register' AND reg.id = f.subject_id
           LEFT JOIN tbl tb ON f.subject_kind = 'table' AND tb.id = f.subject_id
           LEFT JOIN release rl ON f.subject_kind = 'release' AND rl.id = f.subject_id
           LEFT JOIN segment sg ON f.subject_kind = 'segment' AND sg.id = f.subject_id
           LEFT JOIN occurrence oc ON f.subject_kind = 'occurrence' AND oc.id = f.subject_id
           LEFT JOIN variable vr ON vr.id = CASE f.subject_kind
                WHEN 'variable' THEN f.subject_id
                WHEN 'segment' THEN sg.variable_id
                WHEN 'occurrence' THEN oc.variable_id END
           LEFT JOIN tbl st ON st.id = COALESCE(rl.table_id, sg.table_id, oc.table_id)
           LEFT JOIN valueset vs ON f.subject_kind = 'valueset' AND vs.id = f.subject_id
           LEFT JOIN spelling sp ON sp.id = oc.spelling_id
           LEFT JOIN text rn ON rn.id = rl.name_text
         WHERE ({scope})
           AND (:table IS NULL OR COALESCE(st.id, tb.id) = :table)
           AND (:lo IS NULL OR f.start IS NULL OR (f.start < :hi AND :lo < f.end))
           AND (:code IS NULL OR f.code = :code
                OR substr(f.code, 1, length(:code) + 1) = :code || ':')),
         ranked AS (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY severity, code
                    ORDER BY kind_rank, fqid, tbl, s_start, rel, spelling, value_set, f_start,
                             message) AS position,
                COUNT(*) OVER (PARTITION BY severity, code) AS total
         FROM hit)
         SELECT code, severity, kind, fqid, tbl, s_start, s_end, rel, spelling, value_set,
                f_start, f_end, message, caveat, total
         FROM ranked
         WHERE position > :offset AND position <= :end
         ORDER BY severity <> 'review', code, position"
    )
}
