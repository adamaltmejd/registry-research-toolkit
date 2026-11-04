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
                usize::MAX,
            )?
            .0
        }
        None => 0,
    };
    let end = offset.saturating_add(take);

    // The query streams every finding in order; each group's count is its rows, and
    // only the rows in the page are read past their code. Ranking in SQL instead
    // (ROW_NUMBER and COUNT over each group) sorted every wide row twice more: about
    // 550 ms against 200 ms for a register with about 37,000 findings.
    let mut last: Option<(String, String)> = None;
    let mut position = 0usize;
    let found = rows(
        &conn,
        &sql(scope),
        named_params! {
            ":id": entity.id,
            ":table": table,
            ":lo": window.map(|w| w.0),
            ":hi": window.map(|w| w.1),
            ":code": code,
        },
        |row| {
            let key: (String, String) = (row.get(0)?, row.get(1)?);
            if last.as_ref() == Some(&key) {
                position += 1;
            } else {
                position = 1;
                last = Some(key.clone());
            }
            if position <= offset || position > end {
                return Ok((key, None));
            }
            let placed = |start: Option<i64>, end: Option<i64>| {
                start.zip(end).map(|(s, e)| server.period(s, e))
            };
            let finding = Finding {
                code: key.0.clone(),
                severity: key.1.clone(),
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
            Ok((key, Some(finding)))
        },
    )?;
    let names = refs::names(
        server,
        found
            .iter()
            .filter_map(|(_, f)| f.as_ref())
            .flat_map(|f| [&f.subject.fqid, &f.subject.table])
            .flatten()
            .map(String::as_str),
    )?;
    let mut groups: Vec<FindingGroup> = Vec::new();
    for ((code, severity), finding) in found {
        let group = match groups.last_mut() {
            Some(group) if group.code == code && group.severity == severity => group,
            _ => {
                groups.push(FindingGroup {
                    code,
                    severity,
                    count: 0,
                    items: Vec::new(),
                    next_cursor: None,
                });
                groups.last_mut().expect("just pushed")
            }
        };
        group.count += 1;
        let Some(mut finding) = finding else { continue };
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
        group.items.push(finding);
    }
    // A group with no item in the page is left out (only a cursor past its end does
    // that).
    groups.retain(|group| !group.items.is_empty());
    for group in &mut groups {
        group.next_cursor = (end < group.count).then(|| {
            let mut next = Params {
                values: params.values.clone(),
            };
            next.values.insert("code", vec![group.code.as_str()]);
            cursor::encode(
                server.catalog.generation(),
                &cursor::context(&next),
                end,
                "",
            )
        });
    }
    Ok(serde_json::to_value(FindingGroups { groups }).expect("findings serialize"))
}

/// The query for one `scope`: its findings by group, review groups first, then by code.
/// Each subject kind joins only its own table, so `fqid`, `table` and the period come
/// out per kind. Within a group, items run by subject kind, then subject, start and
/// message; NULLs sort first, which is the order the contract asks for.
fn sql(scope: &str) -> String {
    format!(
        "SELECT f.code, f.severity, f.subject_kind,
                CASE f.subject_kind WHEN 'register' THEN reg.pid WHEN 'table' THEN tb.pid
                     ELSE vr.pid END,
                st.pid, COALESCE(rl.start, sg.start, oc.start), COALESCE(rl.end, sg.end, oc.end),
                rn.body, sp.exact, vs.hash, f.start, f.end, m.body, c.body
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
                OR substr(f.code, 1, length(:code) + 1) = :code || ':')
         ORDER BY f.severity <> 'review', f.code,
                  CASE f.subject_kind WHEN 'register' THEN 0 WHEN 'table' THEN 1
                      WHEN 'release' THEN 2 WHEN 'variable' THEN 3 WHEN 'segment' THEN 4
                      WHEN 'occurrence' THEN 5 ELSE 6 END,
                  4, 5, 6, 8, 9, 10, 11, 13"
    )
}
