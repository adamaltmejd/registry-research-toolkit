//! `validate` and `order`: the order check of design v4 §9 over the v4 catalog
//! (crates/DESIGN.md, "The logical order"). One function, [`check`], answers both
//! operations (through [`checked`], which adds the holdings stage on a steward
//! server), which is what makes their results identical.
//!
//! The check, per source: resolve the table (`refs::table`) and the bindings' ids; check
//! the period against the table (`_default` exactly on a timeless table); follow the
//! table's dated `continues` successors, each day going to the table whose `at_start`
//! window holds it, and report an undated one (`table_succession_undated`, S5) instead
//! of following it; then per binding clip the request to the variable's segments
//! (`window.rs`), split it by form and collect the window findings.
//!
//! Findings are ordered per source, then per binding (the source's own findings first),
//! then by code and period start. A finding's `path` is the
//! field it is about (`/sources/0/table`, `/sources/0/period`,
//! `/sources/0/bindings/1/variable`, `/sources/0/bindings/1/classification`), the
//! binding for a window finding (`/sources/0/bindings/1`), the source for an undated
//! succession.

mod materialize;
mod rows;
mod successor;
mod window;
mod words;

pub(crate) use rows::StewardKeys;

use std::collections::BTreeSet;

use reg_core::project::{ProjectData, Source, StudyWindow};
use reg_core::{Interval, IssueLevel, merge, overlap};
use rusqlite::Connection;
use serde::Serialize;
use utoipa::ToSchema;
use utoipa::openapi::{RefOr, Schema};

use serde_json::Value;

use super::refs::{Named, Names};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, iso_day, refs};
use crate::{Code, Error, rows};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "validate",
        paths: &["/api/project/validate"],
        tool: Some("order"),
        description: "Check a project_data.json: every problem with its level. Always 200; `ok` \
            is true when `order` would succeed.",
        params: &[Param::required("project", Type::Project)],
        cache: Cache::Revalidate,
        run: validate,
        result: component::<Validation>,
    },
    Operation {
        name: "order",
        paths: &["/api/project/order"],
        tool: Some("order"),
        description: "What a project_data.json would order: per requested variable, the table, \
            period and column name, the periods that cannot be delivered, the row keys and any \
            warnings; or `order_blocked` listing every problem.",
        params: &[Param::required("project", Type::Project)],
        cache: Cache::Revalidate,
        run: order,
        result: component::<LogicalOrder>,
    },
    Operation {
        name: "materialize_order",
        paths: &["/api/project/materialize"],
        tool: Some("order"),
        description: "The MONA order file for a project_data.json on this data holder's tables: \
            one file per table and delivery, with its columns, row keys, row filter, \
            undeliverable periods and warnings; or `order_blocked` listing every problem.",
        params: &[Param::required("project", Type::Project)],
        cache: Cache::Revalidate,
        run: materialize::materialize,
        result: component::<reg_core::manifest::Manifest>,
    },
];

/// `validate`'s result: `ok` exactly when no issue is an `error`.
#[derive(Serialize, ToSchema)]
pub struct Validation {
    ok: bool,
    issues: Vec<NamedIssue>,
}

/// The `project` body as a JSON object.
fn project(params: &Params) -> Result<Value, Error> {
    serde_json::from_str::<Value>(params["project"])
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| Error::invalid_parameter("project"))
}

/// The structural issues, then (when none is an error) the order check's findings.
fn validate(server: &Server, params: &Params) -> Result<Value, Error> {
    let issues = match reg_core::project::check(&project(params)?) {
        Err(result) => result.issues.into_iter().map(NamedIssue::unnamed).collect(),
        Ok(project) => match checked(server, &project)? {
            Ok(order) => order.findings,
            Err(findings) => named_issues(server, findings)?,
        },
    };
    let ok = !issues.iter().any(|named| named.issue.blocks());
    Ok(serde_json::to_value(Validation { ok, issues }).expect("Validation serializes"))
}

/// The logical order, `project_invalid` with the structural issues, or `order_blocked`
/// with every finding.
fn order(server: &Server, params: &Params) -> Result<Value, Error> {
    let project = structural(&project(params)?)?;
    match checked(server, &project)? {
        Ok(order) => Ok(serde_json::to_value(order).expect("LogicalOrder serializes")),
        Err(findings) => Err(blocked(server, findings)?),
    }
}

/// The logical order with every non-blocking finding, or every finding that blocks it.
/// A steward server also runs the holdings stage of `materialize_order`, so `validate`
/// and `order` refuse exactly what it would block (an unheld variable is
/// `mapping_missing`) and carry its warnings on success (#2194).
fn checked(
    server: &Server,
    project: &ProjectData,
) -> Result<Result<LogicalOrder, Vec<Issue>>, Error> {
    let mut order = match check(server, project)? {
        Ok(order) => order,
        Err(findings) => return Ok(Err(findings)),
    };
    if let (Some(held), Some(steward)) =
        (server.catalog.connect_holdings()?, server.catalog.steward())
    {
        match materialize::manifest(server, &held, steward, project, &order)? {
            Ok(manifest) => delivered(server, &mut order, manifest)?,
            Err(findings) => return Ok(Err(findings)),
        }
    }
    Ok(Ok(order))
}

/// The logical order cut to what the steward's files deliver: the order file's clips
/// replace the logical ones (they start from the same requests), each entry keeps only
/// its binding's delivered days under the file column's own name ([`held_names`]), and
/// each row key the days its table's entries keep, so a `coverage_gap` is not reported
/// as delivered.
fn delivered(
    server: &Server,
    order: &mut LogicalOrder,
    manifest: reg_core::manifest::Manifest,
) -> Result<(), Error> {
    let open = server.open;
    // By binding, not variable: a followed successor's entry names the successor.
    let ordered = |entry: &OrderEntry| {
        manifest
            .clips
            .iter()
            .find(|c| c.source == entry.source && c.binding == entry.binding)
            .map(|c| reg_core::days::merge(c.ordered.iter().map(|p| p.days(open)).collect()))
    };
    // The file column names each binding's days are delivered under.
    let columns = |entry: &OrderEntry| -> Vec<((i64, i64), &str)> {
        manifest
            .files
            .iter()
            .flat_map(|file| &file.columns)
            .flat_map(|column| column.refs.iter().map(move |r| (r, column.name.as_str())))
            .filter(|(r, _)| {
                r.source == entry.source
                    && r.binding == Some(entry.binding)
                    && r.table == entry.table
                    && r.variable == entry.variable
            })
            .map(|(r, name)| (r.period.days(open), name))
            .collect()
    };
    order.entries = std::mem::take(&mut order.entries)
        .into_iter()
        .flat_map(|entry| {
            let whole = [entry.period.days(open)];
            let mut days = match ordered(&entry) {
                None => whole.to_vec(),
                Some(days) => reg_core::days::overlap(&whole, &days),
            };
            let columns = columns(&entry);
            // Only the days a file column delivers: a gap no clip reports (one inside
            // a delivered requested year, #2409) is still not listed as delivered, and
            // an entry no column delivers (an unheld successor) drops.
            let delivered = reg_core::days::merge(columns.iter().map(|c| c.0).collect());
            days = reg_core::days::overlap(&days, &delivered);
            days.into_iter()
                .flat_map(|piece| held_names(piece, &columns, &entry.spelling))
                .map(|((lo, hi), spelling)| OrderEntry {
                    period: server.period(lo, hi),
                    spelling,
                    ..entry.clone()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    // A row identifier is delivered only over days some entry of its table is.
    order.row_keys = std::mem::take(&mut order.row_keys)
        .into_iter()
        .flat_map(|keys| {
            let days = reg_core::days::merge(
                order
                    .entries
                    .iter()
                    .filter(|e| e.source == keys.source && e.table == keys.table)
                    .map(|e| e.period.days(open))
                    .collect(),
            );
            reg_core::days::overlap(&[keys.period.days(open)], &days)
                .into_iter()
                .map(|(lo, hi)| OrderRowKeys {
                    period: server.period(lo, hi),
                    ..keys.clone()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let names = refs::names(
        server,
        manifest
            .clips
            .iter()
            .map(|c| c.variable.as_str())
            .chain(manifest.findings.iter().flat_map(issue_refs)),
    )?;
    order.clips = manifest
        .clips
        .into_iter()
        .map(|c| {
            Ok(OrderClip {
                variable_name: names.of(Named::Variable, &c.variable)?,
                source: c.source,
                variable: c.variable,
                ordered: clip_ordered(
                    server,
                    &c.requested
                        .iter()
                        .map(|p| p.days(server.open))
                        .collect::<Vec<_>>(),
                    &reg_core::days::merge(c.ordered.iter().map(|p| p.days(server.open)).collect()),
                ),
                requested: c.requested,
            })
        })
        .collect::<Result<_, Error>>()?;
    order.findings = manifest
        .findings
        .into_iter()
        .map(|issue| NamedIssue::name(issue, &names))
        .collect();
    Ok(())
}

/// The stretches of `piece` by the file column name delivering them (#2409): a held
/// file can name the column otherwise than the catalog (`AvgAr` for `avgar`), and the
/// name can change between files. A stretch several differently named columns deliver
/// at once (a slot family's members) keeps the catalog `spelling`. Adjacent stretches
/// under one name join.
fn held_names(
    (lo, hi): (i64, i64),
    columns: &[((i64, i64), &str)],
    spelling: &str,
) -> Vec<((i64, i64), String)> {
    let mut cuts: Vec<i64> = columns
        .iter()
        .flat_map(|&((a, b), _)| [a, b])
        .filter(|&d| lo < d && d < hi)
        .chain([lo, hi])
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    let mut out: Vec<((i64, i64), String)> = Vec::new();
    for window in cuts.windows(2) {
        let (a, b) = (window[0], window[1]);
        let names: BTreeSet<&str> = columns
            .iter()
            .filter(|&&((c, d), _)| c <= a && b <= d)
            .map(|&(_, name)| name)
            .collect();
        let name = match names.len() {
            1 => names.into_iter().next().expect("one name"),
            _ => spelling,
        };
        match out.last_mut() {
            Some(((_, end), last)) if *end == a && last == name => *end = b,
            _ => out.push(((a, b), name.to_owned())),
        }
    }
    out
}

/// The structurally valid project, or `project_invalid` with its issues.
fn structural(value: &Value) -> Result<ProjectData, Error> {
    reg_core::project::check(value).map_err(|result| {
        let issues: Vec<Issue> = result.issues.into_iter().map(Issue::from).collect();
        Error::new(
            Code::ProjectInvalid,
            format!(
                "The project is structurally invalid ({} issues).",
                issues.len()
            ),
            vec![serde_json::to_value(issues).expect("issues serialize")],
        )
    })
}

/// `order_blocked` with every finding, named.
///
/// # Errors
///
/// [`refs::names`]'s.
fn blocked(server: &Server, findings: Vec<Issue>) -> Result<Error, Error> {
    let findings = named_issues(server, findings)?;
    Ok(Error::new(
        Code::OrderBlocked,
        format!("The order is blocked by {} findings.", findings.len()),
        vec![serde_json::to_value(findings).expect("findings serialize")],
    ))
}

pub use reg_core::issue::Issue;

/// A finding with the names of the catalog entities it is about, next to their ids.
/// The MONA order file carries the bare `Issue`: names are for readers.
#[derive(Serialize)]
pub struct NamedIssue {
    #[serde(flatten)]
    issue: Issue,
    variable_name: Option<String>,
    table_name: Option<String>,
    successor_name: Option<String>,
}

/// `NamedIssue`'s fields and their descriptions, after `Issue`'s.
const ISSUE_NAMES: [(&str, &str); 3] = [
    (
        "variable_name",
        "The name of `variable`, as `show` gives it; `null` when `variable` is null or \
         names no catalog variable.",
    ),
    (
        "table_name",
        "The name of `table`; `null` when `table` is null or names no catalog table.",
    ),
    (
        "successor_name",
        "The name of `successor_fqid`; `null` when that is null.",
    ),
];

/// `Issue`'s object with the name fields added. Not `allOf` over `Issue`: its
/// `additionalProperties: false` (`deny_unknown_fields`) would refuse the names.
impl utoipa::PartialSchema for NamedIssue {
    fn schema() -> RefOr<Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, SchemaType, Type};
        let RefOr::T(Schema::Object(mut object)) = Issue::schema() else {
            unreachable!("Issue's schema is an inline object");
        };
        object.description = Some(
            "A located finding with the names of the catalog entities it is about. The MONA \
             order file carries the bare `Issue`."
                .into(),
        );
        for (name, description) in ISSUE_NAMES {
            object.properties.insert(
                name.into(),
                ObjectBuilder::new()
                    .schema_type(SchemaType::from_iter([Type::String, Type::Null]))
                    .description(Some(description))
                    .into(),
            );
            object.required.push(name.into());
        }
        RefOr::T(Schema::Object(object))
    }
}

impl ToSchema for NamedIssue {
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        <Issue as ToSchema>::schemas(schemas);
    }
}

impl NamedIssue {
    /// A structural issue, which names nothing in the catalog.
    fn unnamed(issue: reg_core::ValidationIssue) -> Self {
        Self {
            issue: Issue::from(issue),
            variable_name: None,
            table_name: None,
            successor_name: None,
        }
    }

    fn name(issue: Issue, names: &Names) -> Self {
        let of = |kind, fqid: &Option<String>| fqid.as_ref().and_then(|f| names.get(kind, f));
        Self {
            variable_name: of(Named::Variable, &issue.variable),
            table_name: of(Named::Table, &issue.table),
            // A replaced variable's successor, or an undated succession's table.
            successor_name: of(Named::Variable, &issue.successor_fqid)
                .or_else(|| of(Named::Table, &issue.successor_fqid)),
            issue,
        }
    }
}

/// The ids a finding names.
fn issue_refs(issue: &Issue) -> impl Iterator<Item = &str> {
    [&issue.variable, &issue.table, &issue.successor_fqid]
        .into_iter()
        .flatten()
        .map(String::as_str)
}

/// `findings` named with one [`refs::names`] read.
fn named_issues(server: &Server, findings: Vec<Issue>) -> Result<Vec<NamedIssue>, Error> {
    let names = refs::names(server, findings.iter().flat_map(issue_refs))?;
    Ok(findings
        .into_iter()
        .map(|issue| NamedIssue::name(issue, &names))
        .collect())
}

#[derive(Serialize, ToSchema)]
pub struct LogicalOrder {
    catalog: OrderCatalog,
    project: OrderProject,
    /// Per source, per requested variable in request order, then by period start.
    entries: Vec<OrderEntry>,
    /// Per source, then by period start.
    row_keys: Vec<OrderRowKeys>,
    /// Every period that cannot be delivered, also when the order succeeds.
    clips: Vec<OrderClip>,
    /// The non-blocking findings (level `warning` or `info`).
    findings: Vec<NamedIssue>,
}

/// The catalog generation an order was checked against (`meta`).
#[derive(Serialize, ToSchema)]
pub struct OrderCatalog {
    generation_id: String,
    catalog_version: String,
    schema_version: String,
}

/// The project an order was checked for.
#[derive(Serialize, ToSchema)]
pub struct OrderProject {
    name: String,
    schema_version: String,
    reg_meta_version: String,
    /// A hash of the project document.
    project_hash: String,
}

/// One stretch of one requested variable in one table.
#[derive(Clone, Serialize, ToSchema)]
pub struct OrderEntry {
    /// The project source's name.
    source: String,
    /// The variable's position in the source.
    binding: usize,
    provider: String,
    register: String,
    /// The table that serves the period.
    table: String,
    /// The table's name, as `show` gives it.
    table_name: String,
    variable: String,
    /// The variable's name, as `show` gives it.
    variable_name: String,
    /// As the project states it.
    #[schema(required = true)]
    display_name: Option<String>,
    /// The requested period cut to what the table has; `{start: null, end: null}` for a timeless table.
    period: Period,
    /// The exact column name: on a steward server the name the steward's files use
    /// over this period, else the catalog's.
    spelling: String,
    #[serde(rename = "type")]
    #[schema(required = true)]
    dtype: Option<String>,
    #[schema(required = true)]
    length: Option<i64>,
    #[schema(required = true)]
    value_set: Option<OrderValueSet>,
    /// A class version's public id.
    #[schema(required = true)]
    classification: Option<String>,
}

/// An entry's value set by its content hash.
#[derive(Clone, Serialize, ToSchema)]
pub struct OrderValueSet {
    hash: String,
    n_items: i64,
}

/// A serving table's row-key columns over a period.
#[derive(Clone, Serialize, ToSchema)]
pub struct OrderRowKeys {
    source: String,
    table: String,
    /// The table's name.
    table_name: String,
    period: Period,
    columns: Vec<RowKeyColumn>,
}

/// A row-key column: its variable and the spelling it is delivered under.
#[derive(Clone, Serialize, ToSchema)]
pub struct RowKeyColumn {
    variable: String,
    /// The variable's name.
    variable_name: String,
    spelling: String,
}

/// A requested variable that covers less than the requested period.
#[derive(Serialize, ToSchema)]
pub struct OrderClip {
    source: String,
    variable: String,
    /// The variable's name.
    variable_name: String,
    requested: Vec<Period>,
    /// The requested days less the years with no delivery: a requested year delivered on
    /// some day reads as ordered whole (#2392).
    ordered: Vec<Period>,
}

/// A resolved table with what the check reads of it.
#[derive(Clone)]
struct Table {
    id: i64,
    pid: String,
    register: String,
    provider: String,
    timeless: bool,
    /// The end of its coverage hull (a day number).
    end: i64,
}

/// A table that serves part of a source: its window (inclusive ISO days) and the
/// requested days in it.
struct Serving {
    table: Table,
    requested: Vec<Interval>,
}

/// An undated table succession the request would need: (successor pid, continued
/// table pid, the requested days past the continued table's coverage end).
type Undated = (String, String, Interval);

/// [`serving`]'s answer: the serving tables, the undated successions the request would
/// need and the ids of the whole succession chain.
type Routed = (Vec<Serving>, Vec<Undated>, Vec<i64>);

/// What one source contributes.
#[derive(Default)]
struct Checked {
    findings: Vec<Issue>,
    entries: Vec<OrderEntry>,
    row_keys: Vec<OrderRowKeys>,
    clips: Vec<OrderClip>,
}

/// The order check of a structurally valid project: the logical order with its
/// non-blocking findings, or every finding (the blocking ones at level `error`) when
/// something blocks.
///
/// # Errors
///
/// `internal_error` when a catalog query fails; never for a finding.
pub fn check(
    server: &Server,
    project: &ProjectData,
) -> Result<Result<LogicalOrder, Vec<Issue>>, Error> {
    let conn = server.catalog.connect()?;
    // Steward mode reads the composed view (crates/DESIGN.md, "Three questions, three
    // scopes"): a source whose table only the holdings DB defines is checked on that
    // origin, so its row ids never meet base ones; row keys stay pid-keyed on `conn`.
    let held = server.catalog.connect_holdings()?;
    if held.is_some() {
        server.catalog.attach_holdings(&conn)?;
    }
    // Row keys come from the chosen database's tables, as the files do; its findings
    // are the holdings stage's to report.
    let space = materialize::database(server, project).0;
    let mut findings = Vec::new();
    let mut entries = Vec::new();
    let mut row_keys = Vec::new();
    let mut clips = Vec::new();
    for (index, source) in project.sources.iter().enumerate() {
        let (origin, composed) = match &held {
            Some(held)
                if refs::table(&conn, &source.table).is_err()
                    && refs::table(held, &source.table).is_ok() =>
            {
                (held, false)
            }
            _ => (&conn, held.is_some()),
        };
        let checked = check_source(
            server,
            origin,
            &conn,
            space.as_deref(),
            composed,
            index,
            source,
        )?;
        findings.extend(checked.findings);
        entries.extend(checked.entries);
        row_keys.extend(checked.row_keys);
        clips.extend(checked.clips);
    }
    // Editing the study window never rewrites a source's period; a source left wholly
    // outside it blocks (reg-core DESIGN.md, "Logical selection").
    if let Some(window) = project.window.as_ref().and_then(StudyWindow::interval) {
        for (index, source) in project.sources.iter().enumerate() {
            let Ok(Some(items)) = source.period.items() else {
                continue;
            };
            if overlap(&items, std::slice::from_ref(&window)).is_empty() {
                findings.push(Issue {
                    source: Some(source.name.clone()),
                    ..Issue::new(
                        IssueLevel::Error,
                        "source_outside_window",
                        format!("/sources/{index}/period"),
                        format!(
                            "{}'s period lies wholly outside the study window {}..{}; change the \
                         period or the window.",
                            source.name,
                            &window.0[..4],
                            &window.1[..4]
                        ),
                    )
                });
            }
        }
    }
    if project.sources.is_empty() {
        // A valid draft, but nothing to order (reg-core DESIGN.md, "An empty project").
        findings.push(Issue::new(
            IssueLevel::Error,
            "project_empty",
            "/sources",
            "The project has no sources, so there is nothing to order.",
        ));
    }
    if findings.iter().any(Issue::blocks) {
        return Ok(Err(findings));
    }
    let findings = name_order(server, &mut entries, &mut row_keys, &mut clips, findings)?;
    Ok(Ok(LogicalOrder {
        catalog: OrderCatalog {
            generation_id: server.catalog.generation().to_owned(),
            catalog_version: server.catalog.meta("catalog_version").to_owned(),
            schema_version: server.catalog.meta("schema_version").to_owned(),
        },
        project: OrderProject {
            name: project.name.clone(),
            schema_version: project.schema_version.clone(),
            reg_meta_version: project.reg_meta_version.clone(),
            project_hash: reg_core::project::project_hash(project),
        },
        entries,
        row_keys,
        clips,
        findings,
    }))
}

/// Name an order's rows and findings with one [`refs::names`] read: the rows are built
/// with empty names until here.
fn name_order(
    server: &Server,
    entries: &mut [OrderEntry],
    row_keys: &mut [OrderRowKeys],
    clips: &mut [OrderClip],
    findings: Vec<Issue>,
) -> Result<Vec<NamedIssue>, Error> {
    let names = refs::names(
        server,
        entries
            .iter()
            .flat_map(|e| [e.table.as_str(), e.variable.as_str()])
            .chain(row_keys.iter().flat_map(|k| {
                std::iter::once(k.table.as_str())
                    .chain(k.columns.iter().map(|c| c.variable.as_str()))
            }))
            .chain(clips.iter().map(|c| c.variable.as_str()))
            .chain(findings.iter().flat_map(issue_refs)),
    )?;
    for entry in entries.iter_mut() {
        entry.table_name = names.of(Named::Table, &entry.table)?;
        entry.variable_name = names.of(Named::Variable, &entry.variable)?;
    }
    for keys in row_keys.iter_mut() {
        keys.table_name = names.of(Named::Table, &keys.table)?;
        for column in &mut keys.columns {
            column.variable_name = names.of(Named::Variable, &column.variable)?;
        }
    }
    for clip in clips.iter_mut() {
        clip.variable_name = names.of(Named::Variable, &clip.variable)?;
    }
    Ok(findings
        .into_iter()
        .map(|issue| NamedIssue::name(issue, &names))
        .collect())
}

/// One source on its table's origin `conn`: its own findings, then each binding's.
/// Row keys read `keys`, the base connection.
#[allow(clippy::too_many_lines)]
fn check_source(
    server: &Server,
    conn: &Connection,
    keys: &Connection,
    space: Option<&str>,
    composed: bool,
    index: usize,
    source: &Source,
) -> Result<Checked, Error> {
    let at = |field: &str| format!("/sources/{index}/{field}");
    let issue = |level, code: &str, path: String, message: String| Issue {
        source: Some(source.name.clone()),
        ..Issue::new(level, code, path, message)
    };
    let mut own = Vec::new();
    let table = match table_ref(conn, &source.table)? {
        Ok(table) => Some(table),
        Err((code, message)) => {
            own.push(issue(IssueLevel::Error, code, at("table"), message));
            None
        }
    };
    let table_pid = table.as_ref().map(|t| t.pid.clone());
    let located = |mut issue: Issue| {
        issue.table.clone_from(&table_pid);
        issue
    };
    let items = match requested(server, source, table.as_ref()) {
        Ok(items) => items,
        Err((code, message)) => {
            own.push(located(issue(
                IssueLevel::Error,
                code,
                at("period"),
                message,
            )));
            None
        }
    };
    let mut checked = Checked::default();
    let (Some(table), Some(items)) = (table, items) else {
        // Every finding at once: the bindings' ids still resolve.
        own.sort_by_key(Issue::key);
        checked.findings = own;
        for (b, binding) in source.bindings.iter().enumerate() {
            let mut found = window::refs(
                conn,
                composed,
                index,
                b,
                source,
                binding,
                table_pid.as_deref(),
            )?
            .1;
            found.sort_by_key(Issue::key);
            checked.findings.extend(found);
        }
        return Ok(checked);
    };
    let requested = merge(items.clone());
    let (mut serving, undated, chain) = serving(server, conn, table, &requested)?;
    for (successor, continued, past) in undated {
        let mut finding = issue(
            IssueLevel::Info,
            "table_succession_undated",
            format!("/sources/{index}"),
            format!(
                "{continued} is continued by {successor} on no stated day, so its data for {} is not \
                 followed into {successor}",
                span(server, &past)
            ),
        );
        finding.table = Some(continued);
        finding.period = Some(period(server, &past));
        finding.successor_fqid = Some(successor);
        own.push(finding);
    }
    own.sort_by_key(Issue::key);
    checked.findings = own;
    let table_pid = table_pid.unwrap_or_default();
    let mut followed = Vec::new();
    let scope = window::Scope {
        server,
        conn,
        index,
        source,
        table: &table_pid,
        serving: &serving,
        chain: &chain,
        requested: &requested,
        composed,
    };
    for (b, binding) in source.bindings.iter().enumerate() {
        let outcome = window::binding(&scope, b, binding)?;
        let mut found = outcome.findings;
        found.sort_by_key(Issue::key);
        checked.findings.extend(found);
        checked.entries.extend(outcome.entries);
        checked.clips.extend(outcome.clip);
        followed.extend(outcome.serving);
    }
    if !followed.is_empty() {
        serving = combine_serving(serving, followed);
    }
    let (row_keys, mut keyed) = rows::row_keys(server, keys, space, index, source, &serving)?;
    rows::unbound(&mut keyed, &checked.findings);
    keyed.sort_by_key(Issue::key);
    checked.findings.extend(keyed);
    checked.row_keys = row_keys;
    Ok(checked)
}

/// Coalesce followed table windows once for row keys.
fn combine_serving(mut serving: Vec<Serving>, followed: Vec<Serving>) -> Vec<Serving> {
    serving.extend(followed);
    // Several bindings can follow into the same table; row keys see its
    // requested days once.
    serving.sort_by_key(|s| s.table.id);
    let mut combined: Vec<Serving> = Vec::new();
    for s in serving {
        match combined.last_mut() {
            Some(last) if last.table.id == s.table.id => {
                last.requested.extend(s.requested);
                last.requested = merge(std::mem::take(&mut last.requested));
            }
            _ => combined.push(s),
        }
    }
    let mut serving = combined;
    serving.sort_by_key(|s| (s.requested[0].0.clone(), s.table.pid.clone()));
    serving
}

/// The days of each period item (`None` when the table did not resolve; one item, the
/// open sentinel, for `_default`), or the finding that refuses the period:
/// `period_not_orderable`, or `_default` anywhere but on a timeless table and a dated
/// period on one.
fn requested(
    server: &Server,
    source: &Source,
    table: Option<&Table>,
) -> Result<Option<Vec<Interval>>, (&'static str, String)> {
    let intervals = source.period.items().map_err(|reason| {
        (
            "period_not_orderable",
            format!("The period is not orderable: {reason}."),
        )
    })?;
    match (table, intervals) {
        (None, _) => Ok(None),
        (Some(t), None) if !t.timeless => Err((
            "period_default_on_dated_table",
            format!(
                "\"_default\" is a timeless table's period; {} is dated.",
                t.pid
            ),
        )),
        (Some(t), Some(_)) if t.timeless => Err((
            "period_on_timeless_table",
            format!("{} is timeless; its period is \"_default\".", t.pid),
        )),
        (Some(_), None) => Ok(Some(vec![(
            iso_day(server.open.0),
            iso_day(server.open.1 - 1),
        )])),
        (Some(_), Some(intervals)) => Ok(Some(intervals)),
    }
}

/// The table a source names, or the finding that refuses it: `fqid_unresolved`,
/// `retired_ref` ("retired in <version>") or `table_is_register` listing the tables.
fn table_ref(
    conn: &Connection,
    value: &str,
) -> Result<Result<Table, (&'static str, String)>, Error> {
    let id = match refs::table(conn, value) {
        Ok(id) => id,
        Err(err) => return refused(conn, value, "table", &err).map(Err),
    };
    let table = conn.query_row(
        "SELECT t.pid, r.pid, p.pid, t.timeless, t.end
         FROM tbl t JOIN register r ON r.id = t.register_id JOIN provider p ON p.id = r.provider_id
         WHERE t.id = ?",
        [id],
        |row| {
            Ok(Table {
                id,
                pid: row.get(0)?,
                register: row.get(1)?,
                provider: row.get(2)?,
                timeless: row.get(3)?,
                end: row.get(4)?,
            })
        },
    )?;
    Ok(Ok(table))
}

/// The finding for a ref `refs` refused, naming what was expected (`what`); a catalog
/// failure stays an error.
fn refused(
    conn: &Connection,
    value: &str,
    what: &str,
    err: &Error,
) -> Result<(&'static str, String), Error> {
    match err.kind() {
        Code::TableIsRegister => {
            let tables: Vec<String> = rows(
                conn,
                "SELECT t.pid FROM tbl t JOIN register r ON r.id = t.register_id
                     WHERE r.pid = ? ORDER BY t.pid",
                [value],
                |row| row.get(0),
            )?;
            Ok((
                "table_is_register",
                format!(
                    "{value} is a register; name one of its tables: {}.",
                    tables.join(", ")
                ),
            ))
        }
        Code::RetiredRef => {
            let retired_in: String = conn.query_row(
                "SELECT retired_in FROM ident_retired WHERE pid = ?",
                [value],
                |row| row.get(0),
            )?;
            Ok((
                "retired_ref",
                format!("{value} was retired in catalog {retired_in}."),
            ))
        }
        Code::NotFound | Code::InvalidRef | Code::AmbiguousRef => Ok((
            "fqid_unresolved",
            format!("{value} names no {what} in the catalog."),
        )),
        _ => Err(Error::internal(format!("Resolving {value:?} failed."))),
    }
}

/// The tables that serve `requested` from `table`: the table itself, then its dated
/// `continues` successors, followed transitively; each day goes to the table whose
/// `at_start` window holds it. Also each undated succession whose successor the request
/// would need (it reaches past the table's coverage end inside the table's window), as
/// [`Undated`]; it is not followed (S5): no day is inferred from coverage (v4 §2.1).
/// Last, the ids of the whole chain (the table and every dated successor), whether or
/// not it serves a requested day.
fn serving(
    server: &Server,
    conn: &Connection,
    table: Table,
    requested: &[Interval],
) -> Result<Routed, Error> {
    let mut chain = vec![(server.open.0, table)];
    let mut seen = BTreeSet::from([chain[0].1.id]);
    let mut undated = Vec::new();
    let mut next = 0;
    let mut successors = conn.prepare(
        "SELECT r.subject_id, r.at_start, t.pid FROM relation r JOIN tbl t ON t.id = r.subject_id
         WHERE r.kind = 'continues' AND r.subject_kind = 'table' AND r.object_kind = 'table'
           AND r.object_id = ?
         ORDER BY r.at_start IS NULL, r.at_start, t.pid",
    )?;
    while next < chain.len() {
        let rows: Vec<(i64, Option<i64>, String)> = successors
            .query_map([chain[next].1.id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        for (subject, at_start, pid) in rows {
            match at_start {
                None => undated.push((chain[next].1.id, pid)),
                Some(at) if seen.insert(subject) => {
                    if let Ok(Ok(successor)) = table_ref(conn, &pid) {
                        chain.push((at, successor));
                    }
                }
                Some(_) => {}
            }
        }
        next += 1;
    }
    chain.sort_by_key(|(from, _)| *from);
    let ids = chain.iter().map(|(_, t)| t.id).collect();
    let ends: Vec<i64> = chain
        .iter()
        .skip(1)
        .map(|(from, _)| *from)
        .chain([server.open.1])
        .collect();
    let mut serving = Vec::new();
    let mut gone = Vec::new();
    for ((from, table), to) in chain.into_iter().zip(ends) {
        if from >= to {
            continue;
        }
        let window = (iso_day(from), iso_day(to - 1));
        for (object, successor) in &undated {
            if *object == table.id && table.end < to {
                let past = overlap(
                    requested,
                    &[(iso_day(table.end.max(from)), window.1.clone())],
                );
                if let (Some(first), Some(last)) = (past.first(), past.last()) {
                    let past = (first.0.clone(), last.1.clone());
                    gone.push((successor.clone(), table.pid.clone(), past));
                }
            }
        }
        let part = overlap(requested, &[window]);
        if !part.is_empty() {
            serving.push(Serving {
                table,
                requested: part,
            });
        }
    }
    Ok((serving, gone, ids))
}

/// The parts of `gaps` (half-open day numbers, ascending) an order reports: all but the
/// calendar years the request covers whole and `delivered` reaches on some day. Such a
/// year reads as delivered, and no whole year fills its gap, so a finding or clip would
/// name a year that comes (`reg_webapp/frontend/DESIGN.md` → Time, #2389; #2409).
pub(super) fn reported_gaps(
    gaps: &[(i64, i64)],
    requested: &[(i64, i64)],
    delivered: &[(i64, i64)],
) -> Vec<(i64, i64)> {
    use reg_core::days::{covered, merge, minus, overlap};
    let delivered = merge(delivered.to_vec());
    let (Some(g0), Some(g1), Some(d0), Some(d1)) = (
        gaps.first(),
        gaps.last(),
        delivered.first(),
        delivered.last(),
    ) else {
        return gaps.to_vec();
    };
    let year = |d: i64| -> i32 { iso_day(d)[..4].parse().expect("an ISO year") };
    let start = |y: i32| reg_core::iso_day_number(&format!("{y:04}-01-01"));
    let first = year(g0.0).max(year(d0.0));
    let last = year(g1.1 - 1).min(year(d1.1 - 1));
    let quiet: Vec<(i64, i64)> = (first..=last)
        .filter_map(|y| Some((start(y)?, start(y + 1)?)))
        .filter(|&span| covered(&[span], requested) && !overlap(&[span], &delivered).is_empty())
        .collect();
    minus(gaps, &quiet)
}

/// An [`OrderClip`]'s `ordered`: the requested days (half-open day numbers) less the gaps an
/// order reports ([`reported_gaps`]). A requested year `delivered` reaches on some day,
/// such as a 31 December snapshot, reads as ordered whole, so `requested` less `ordered`
/// names only the years with no delivery (#2392).
pub(super) fn clip_ordered(
    server: &Server,
    requested: &[(i64, i64)],
    delivered: &[(i64, i64)],
) -> Vec<Period> {
    use reg_core::days::{merge, minus};
    let requested = merge(requested.to_vec());
    let missing = minus(&requested, delivered);
    minus(&requested, &reported_gaps(&missing, &requested, delivered))
        .into_iter()
        .map(|(lo, hi)| server.period(lo, hi))
        .collect()
}

/// [`reported_gaps`] over inclusive ISO intervals.
pub(super) fn reported_intervals(
    gaps: &[Interval],
    requested: &[Interval],
    delivered: &[Interval],
) -> Vec<Interval> {
    reported_gaps(&days(gaps), &days(requested), &days(delivered))
        .into_iter()
        .map(|(lo, hi)| interval(lo, hi))
        .collect()
}

/// Inclusive ISO intervals as half-open day numbers.
pub(super) fn days(v: &[Interval]) -> Vec<(i64, i64)> {
    v.iter().map(|(lo, hi)| (day(lo), day(hi) + 1)).collect()
}

/// The day number of an ISO day.
fn day(iso: &str) -> i64 {
    reg_core::iso_day_number(iso).expect("an ISO day")
}

/// The inclusive ISO interval of the day numbers `[start, end)`.
fn interval(start: i64, end: i64) -> Interval {
    (iso_day(start), iso_day(end - 1))
}

/// A result period of an inclusive ISO interval.
fn period(server: &Server, (lo, hi): &Interval) -> Period {
    server.period(day(lo), day(hi) + 1)
}

/// An inclusive ISO interval in plain words ([`words::plain`]).
fn span(server: &Server, interval: &Interval) -> String {
    words::plain(&[period(server, interval)])
}
