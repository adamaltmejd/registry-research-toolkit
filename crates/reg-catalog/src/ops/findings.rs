//! `findings`: the review items and notices on a register, a table or a variable.

use rusqlite::named_params;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::{ArrayBuilder, RefOr, Schema};

use super::{Cache, Components, Operation, Param, Params, Period, Server, Type, component, refs};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "findings",
    paths: &["/api/findings/{ref}"],
    tool: None,
    description: "The review items and notices on a register, a table or a variable \
        (its segments and occurrences included), optionally narrowed to a period, a \
        table or a rule code.",
    params: &[
        Param::required("ref", Type::Ref),
        Param::optional("period", Type::Period),
        Param::optional("table", Type::Ref),
        Param::optional("code", Type::String),
    ],
    cache: Cache::Minute,
    run: findings,
    result,
}];

fn result(components: &mut Components) -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(component::<Finding>(components))
        .into()
}

/// `shape.Finding`.
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
    /// An acked statement's caveat.
    #[schema(required = true)]
    caveat: Option<String>,
}

/// `shape.Subject`: what a finding is about, by natural key.
#[derive(Serialize, ToSchema)]
pub struct Subject {
    /// `register`, `table`, `release`, `variable`, `segment`, `occurrence` or
    /// `valueset`.
    kind: String,
    /// The register, table or variable; for a segment or an occurrence, its variable.
    #[schema(required = true)]
    fqid: Option<String>,
    /// The table of a release, segment or occurrence.
    #[schema(required = true)]
    table: Option<String>,
    /// The release's, segment's or occurrence's period; `null` when unplaced.
    #[schema(required = true)]
    period: Option<Period>,
    /// A release's literal delivery name.
    #[schema(required = true)]
    release: Option<String>,
    /// An occurrence's exact spelling.
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

fn findings(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let value = params["ref"];
    let entity = refs::resolve(&conn, value)?;
    let scope = match entity.kind.as_str() {
        "register" => SCOPE_REGISTER,
        "table" => SCOPE_TABLE,
        "variable" => SCOPE_VARIABLE,
        kind => {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{value:?} is a {kind}, not a register, a table or a variable."),
                vec![value.into()],
            ));
        }
    };
    let table = params
        .get("table")
        .map(|t| refs::table(&conn, t))
        .transpose()?;
    let day = |iso: &str| {
        reg_core::iso_day_number(iso).ok_or_else(|| {
            Error::new(
                Code::InternalError,
                format!("The period bound {iso:?} is not a day."),
                vec![],
            )
        })
    };
    // The period as half-open day numbers: the last day is inclusive.
    let window = super::period(params, "period")?
        .map(|p| {
            let (first, last) = p.iso_bounds();
            Ok::<_, Error>((day(&first)?, day(&last)? + 1))
        })
        .transpose()?;
    let code = params.get("code").copied();

    let items = conn
        .prepare(&sql(scope))?
        .query_map(
            named_params! {
                ":id": entity.id,
                ":table": table,
                ":lo": window.map(|w| w.0),
                ":hi": window.map(|w| w.1),
                ":code": code,
            },
            |row| {
                let placed = |start: Option<i64>, end: Option<i64>| {
                    start.zip(end).map(|(s, e)| server.period(s, e))
                };
                Ok(Finding {
                    code: row.get(0)?,
                    severity: row.get(1)?,
                    subject: Subject {
                        kind: row.get(2)?,
                        fqid: row.get(3)?,
                        table: row.get(4)?,
                        period: placed(row.get(5)?, row.get(6)?),
                        release: row.get(7)?,
                        spelling: row.get(8)?,
                        value_set: row.get(9)?,
                    },
                    period: placed(row.get(10)?, row.get(11)?),
                    message: row.get(12)?,
                    caveat: row.get(13)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(serde_json::to_value(items).expect("findings serialize"))
}

/// The query for one `scope`. Each subject kind joins only its own table, so `fqid`,
/// `table` and the period come out per kind. NULLs sort first, which is the order the
/// contract asks for; the `ORDER BY` numbers are select-list positions.
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
           AND (:table IS NULL OR st.id = :table)
           AND (:lo IS NULL OR f.start IS NULL OR (f.start < :hi AND :lo < f.end))
           AND (:code IS NULL OR f.code = :code
                OR substr(f.code, 1, length(:code) + 1) = :code || ':')
         ORDER BY CASE f.subject_kind WHEN 'register' THEN 0 WHEN 'table' THEN 1
                      WHEN 'release' THEN 2 WHEN 'variable' THEN 3 WHEN 'segment' THEN 4
                      WHEN 'occurrence' THEN 5 ELSE 6 END,
                  4, 5, 6, 8, 9, 10, 11, f.code, 13"
    )
}
