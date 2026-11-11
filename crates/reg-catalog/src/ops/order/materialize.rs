//! `materialize_order`: the logical order bound to the serving steward's physical
//! holdings, written as the MONA order file (crates/DESIGN.md, "From a logical order to
//! physical files" and "MONA order file").
//!
//! After the logical check, each entry meets `holding_mapping` by public ids. Every
//! placement that holds part of it is selected (all partition members), except a
//! projection's copy of what its parent also holds. Placements group into one file
//! per holding table; ordered columns come in logical request order (slot members by
//! index), then the table's declared keys over the file's days. The file's row filter
//! is the union of its requested days, at the encoding's grain. Every refusal is a
//! located finding; nothing partial is returned.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::IssueLevel;
use reg_core::days::{merge, minus, overlap};
use reg_core::manifest::{
    CatalogIdentity, Clip, Column, Encoding, File, HoldingsIdentity, MANIFEST_VERSION, Manifest,
    Mode, ProjectIdentity, Provenance, Range, Ref, Role, RowFilter, Slot, Storage, StorageFamily,
    safe_file_name, stata_name,
};
use reg_core::project::{ColumnType, ExtractFormat, ProjectData};
use rusqlite::Connection;
use serde_json::Value;

use super::{Issue, LogicalOrder, OrderEntry, check, project, reported_gaps, words};
use crate::holdings::Steward;
use crate::ops::{Params, Server, iso_day};
use crate::{Code, Error, rows};

/// Half-open day numbers `[start, end)`.
type Days = (i64, i64);

/// `POST /api/project/materialize`.
pub(super) fn materialize(server: &Server, params: &Params) -> Result<Value, Error> {
    let (Some(held), Some(steward)) =
        (server.catalog.connect_holdings()?, server.catalog.steward())
    else {
        return Err(Error::new(
            Code::HoldingsUnavailable,
            "This server has no tables to order from, so it cannot produce an order file.",
            vec![],
        ));
    };
    let project = super::structural(&project(params)?)?;
    if project.name.trim().is_empty() {
        return Err(Error::new(
            Code::ProjectUnnamed,
            "The project has no name; the order file's provenance needs one.",
            vec![],
        ));
    }
    let result = match check(server, &project)? {
        Ok(order) => manifest(server, &held, steward, &project, &order)?,
        Err(findings) => Err(findings),
    };
    match result {
        Ok(manifest) => Ok(serde_json::to_value(manifest).expect("Manifest serializes")),
        Err(findings) => Err(super::blocked(server, findings)?),
    }
}

/// The order file of a project's logical order, or every finding that blocks it: the
/// logical check's, then the holdings stage's. `validate` and `order` run this on a
/// steward server too (`super::checked`), so they refuse exactly what `materialize`
/// would.
pub(super) fn manifest(
    server: &Server,
    held: &Connection,
    steward: Steward,
    project: &ProjectData,
    order: &LogicalOrder,
) -> Result<Result<Manifest, Vec<Issue>>, Error> {
    let provenance = Provenance {
        mode: Mode::StewardHoldings,
        steward: steward.owner.clone(),
        catalog: CatalogIdentity {
            release: server.catalog.meta("catalog_version").to_owned(),
            schema: server.catalog.meta("schema_version").to_owned(),
            generation: server.catalog.generation().to_owned(),
        },
        holdings: HoldingsIdentity {
            owner: steward.owner,
            generation: steward.holdings_generation,
        },
        base_generation: steward.catalog_generation,
        project: ProjectIdentity {
            schema_version: project.schema_version.clone(),
            project_hash: reg_core::project::project_hash(project),
            name: project.name.clone(),
        },
    };
    build(server, held, project, order, provenance)
}

/// One accepted placement of a logical (table, variable) in a physical column.
#[derive(Clone)]
struct Placement {
    holding: i64,
    projection_of: Option<i64>,
    column: i64,
    days: Days,
    caveat: Option<String>,
}

/// A physical table with what a file needs of it.
struct Holding {
    physical_name: String,
    /// The name findings use (`holdings::TABLE_LABEL`).
    label: String,
    schema_name: Option<String>,
    file_name: String,
    delivery: Option<String>,
    partition: Option<String>,
    projection: Option<String>,
    /// (column id, name, encoding).
    period_column: Option<(i64, String, String)>,
    keys: String,
    held: Vec<Days>,
}

/// A physical column's stored facts.
struct Stored {
    name: String,
    ordinal: i64,
    storage: Storage,
    slot: Option<Slot>,
}

/// A file being assembled: its columns by id, with the ordering key of each.
#[derive(Default)]
struct Draft {
    /// The first logical appearance: (source, binding, entry).
    first: (usize, usize, usize),
    /// Per source index, the requested days the file serves.
    days: BTreeMap<usize, Vec<Days>>,
    /// Column id -> (ordering key, role, refs).
    columns: BTreeMap<i64, (ColumnKey, Role, Vec<Ref>)>,
    /// The first binding path a file refusal is located at.
    path: String,
    source: String,
}

/// Ordered columns first (by source, binding, entry, slot index, ordinal), then keys
/// in declaration order.
type ColumnKey = (u8, usize, usize, usize, i64, i64);

/// Per (source, binding): its variable, requested days and held days.
type Bindings = BTreeMap<(usize, usize), (String, Vec<Days>, Vec<Days>)>;

#[allow(clippy::too_many_lines)]
fn build(
    server: &Server,
    held: &Connection,
    project: &ProjectData,
    order: &LogicalOrder,
    provenance: Provenance,
) -> Result<Result<Manifest, Vec<Issue>>, Error> {
    let open = server.open;
    let index: BTreeMap<&str, usize> = project
        .sources
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();
    let (space, mut findings) = database(server, project);
    let space = space.as_deref();
    let mut drafts: BTreeMap<i64, Draft> = BTreeMap::new();
    let mut bindings = Bindings::new();
    for (e, entry) in order.entries.iter().enumerate() {
        let s = index[entry.source.as_str()];
        let days = entry.period.days(open);
        let placements = placements(held, &entry.table, &entry.variable, days, space)?;
        // Keyed by the project binding's own variable: a followed successor's entry
        // names the successor, but its logical clip is recorded under the binding.
        let binding = bindings.entry((s, entry.binding)).or_insert_with(|| {
            (
                project.sources[s].bindings[entry.binding].variable.clone(),
                vec![],
                vec![],
            )
        });
        binding.1.push(days);
        for p in &placements {
            let piece = (days.0.max(p.days.0), days.1.min(p.days.1));
            binding.2.push(piece);
            let draft = drafts.entry(p.holding).or_insert_with(|| Draft {
                first: (s, entry.binding, e),
                path: format!("/sources/{s}/bindings/{}", entry.binding),
                source: entry.source.clone(),
                ..Draft::default()
            });
            draft.days.entry(s).or_default().push(piece);
            let stored = stored(held, p.column)?;
            let slot_index = stored
                .slot
                .as_ref()
                .and_then(|slot| slot.index.parse::<i64>().ok())
                .unwrap_or(0);
            let key = (0, s, entry.binding, e, slot_index, stored.ordinal);
            let column = draft
                .columns
                .entry(p.column)
                .or_insert_with(|| (key, Role::Ordered, vec![]));
            column.0 = column.0.min(key);
            column.2.push(reference(server, entry, piece));
            if let Some(caveat) = &p.caveat {
                findings.push(Issue {
                    source: Some(entry.source.clone()),
                    variable: Some(entry.variable.clone()),
                    table: Some(entry.table.clone()),
                    period: Some(server.period(piece.0, piece.1)),
                    ..Issue::new(
                        IssueLevel::Warning,
                        "mapping_caveat",
                        format!("/sources/{s}/bindings/{}", entry.binding),
                        format!(
                            "{} in {} for {}: {caveat}",
                            entry.variable,
                            entry.table,
                            span(server, piece)
                        ),
                    )
                });
            }
        }
        gaps(
            held,
            server,
            s,
            entry,
            days,
            space,
            &placements,
            &mut findings,
        )?;
    }
    // Per source, every day it requested of any table: a file's rows outside these are
    // extra, whichever file serves the rest.
    let mut asked: BTreeMap<usize, Vec<Days>> = BTreeMap::new();
    for (&(s, _), (_, requested, _)) in &bindings {
        asked.entry(s).or_default().extend(requested);
    }
    for days in asked.values_mut() {
        *days = merge(std::mem::take(days));
    }
    not_in_database(
        held,
        server,
        project,
        order,
        &bindings,
        space,
        &mut findings,
    )?;
    let clips = clip(
        server,
        order,
        &project.sources,
        &mut bindings,
        &mut findings,
    );
    let format = project.extract_format.unwrap_or(ExtractFormat::Dta);
    let mut files = Vec::new();
    let mut names: BTreeMap<String, String> = BTreeMap::new();
    for (holding_id, mut draft) in drafts {
        let holding = holding(held, holding_id)?;
        let at = |code: &str, message: String| Issue {
            source: Some(draft.source.clone()),
            ..Issue::new(IssueLevel::Error, code, draft.path.clone(), message)
        };
        if let Err(reason) = safe_file_name(&holding.file_name) {
            findings.push(at(
                "file_name_unsafe",
                format!(
                    "The stored file name {:?} of table {:?} is {reason}.",
                    holding.file_name, holding.label
                ),
            ));
        }
        if let Some(other) = names.insert(holding.file_name.to_lowercase(), holding.label.clone()) {
            findings.push(at(
                "file_name_unsafe",
                format!(
                    "Tables {other:?} and {:?} name files that differ only in case.",
                    holding.label
                ),
            ));
        }
        if holding.schema_name.is_none() {
            findings.push(at(
                "physical_schema_missing",
                format!("Table {:?} has no single stored schema.", holding.label),
            ));
        }
        for days in draft.days.values_mut() {
            *days = merge(std::mem::take(days));
        }
        keys(
            server,
            held,
            holding_id,
            &holding,
            &project.sources,
            &mut draft,
            &mut findings,
        )?;
        let row_filter = row_filter(
            server,
            &holding,
            &draft,
            &asked,
            &project.sources,
            &mut findings,
        );
        let mut columns: Vec<(ColumnKey, Column)> = Vec::new();
        for (column_id, (key, role, mut refs)) in std::mem::take(&mut draft.columns) {
            let stored = stored(held, column_id)?;
            refs.sort_by(|a, b| {
                (
                    index[a.source.as_str()],
                    a.binding.is_none(),
                    a.binding,
                    &a.period.start,
                )
                    .cmp(&(
                        index[b.source.as_str()],
                        b.binding.is_none(),
                        b.binding,
                        &b.period.start,
                    ))
            });
            columns.push((
                key,
                Column {
                    stata_name: (format == ExtractFormat::Dta)
                        .then(|| stata_name(&stored.name))
                        .flatten(),
                    name: stored.name,
                    storage: stored.storage,
                    role,
                    refs,
                    slot: stored.slot,
                },
            ));
        }
        columns.sort_by_key(|(key, _)| *key);
        files.push((
            draft.first,
            File {
                name: holding.file_name,
                schema: holding.schema_name.unwrap_or_default(),
                table: holding.physical_name,
                delivery: holding.delivery,
                partition: holding.partition,
                projection: holding.projection,
                row_filter,
                columns: columns.into_iter().map(|(_, c)| c).collect(),
            },
        ));
    }
    files.sort_by(|a, b| (a.0, &a.1.name).cmp(&(b.0, &b.1.name)));
    let mut physical: Vec<Issue> = findings;
    physical.sort_by(|a, b| (&a.path, a.key()).cmp(&(&b.path, b.key())));
    let mut all: Vec<Issue> = Vec::new();
    for finding in &order.findings {
        if !held_as_stated(server, held, project, space, &finding.issue)? {
            all.push(finding.issue.clone());
        }
    }
    all.extend(physical);
    if all.iter().any(Issue::blocks) {
        return Ok(Err(all));
    }
    Ok(Ok(Manifest {
        version: MANIFEST_VERSION,
        extract_format: format,
        provenance,
        files: files.into_iter().map(|(_, f)| f).collect(),
        clips,
        findings: all,
    }))
}

/// Whether `issue` is a `binding_type_differs` whose binding's type override is of the
/// storage family of every steward column that holds its days (maintainer G14): the
/// project then matches the file it gets, and the catalog type is not what it reads.
fn held_as_stated(
    server: &Server,
    held: &Connection,
    project: &ProjectData,
    space: Option<&str>,
    issue: &Issue,
) -> Result<bool, Error> {
    let (true, Some(table), Some(variable), Some(period)) = (
        issue.code == "binding_type_differs",
        &issue.table,
        &issue.variable,
        &issue.period,
    ) else {
        return Ok(false);
    };
    let stated = project
        .sources
        .iter()
        .enumerate()
        .flat_map(|(s, source)| {
            let at = move |b| format!("/sources/{s}/bindings/{b}");
            source
                .bindings
                .iter()
                .enumerate()
                .map(move |(b, binding)| (at(b), binding))
        })
        .find(|(path, _)| *path == issue.path)
        .and_then(|(_, binding)| binding.column_type)
        .and_then(ColumnType::storage_family);
    let Some(stated) = stated else {
        return Ok(false);
    };
    let placements = placements(held, table, variable, period.days(server.open), space)?;
    if placements.is_empty() {
        return Ok(false);
    }
    for p in &placements {
        let storage = stored(held, p.column)?.storage;
        if storage.dtype.as_deref().and_then(StorageFamily::of) != Some(stated) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A binding's reference to the days of `entry` a column holds.
fn reference(server: &Server, entry: &OrderEntry, (lo, hi): Days) -> Ref {
    Ref {
        source: entry.source.clone(),
        binding: Some(entry.binding),
        table: entry.table.clone(),
        variable: entry.variable.clone(),
        period: server.period(lo, hi),
        spelling: entry.spelling.clone(),
    }
}

/// The database the order draws from, or the finding that leaves it unchosen: `None`
/// with a `database_required` or `database_unknown` error, and then placements are
/// not filtered by database. A single-database steward needs no `linkage`; one that
/// names its implicit database is accepted.
pub(super) fn database(server: &Server, project: &ProjectData) -> (Option<String>, Vec<Issue>) {
    let spaces = server.catalog.spaces().unwrap_or_default();
    // The id first, as `linkage.database` takes it: a name may carry its own
    // parenthesis ("Name (Gloss)"), so "Name (id)" read as "Name (Gloss) (id)".
    let names = || {
        spaces
            .iter()
            .map(|s| format!("{:?} for {}", s.id, s.name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match &project.linkage {
        None if spaces.len() > 1 => (
            None,
            vec![Issue::new(
                IssueLevel::Error,
                "database_required",
                "/linkage",
                format!(
                    "The data is kept in {} databases, each with its own person key: {}. Choose \
                     the one this project draws from in linkage.database, and make a \
                     separate project for each database.",
                    spaces.len(),
                    names()
                ),
            )],
        ),
        None => (spaces.first().map(|s| s.id.clone()), vec![]),
        Some(linkage) if spaces.iter().any(|s| s.id == linkage.database) => {
            (Some(linkage.database.clone()), vec![])
        }
        Some(linkage) => (
            None,
            vec![Issue::new(
                IssueLevel::Error,
                "database_unknown",
                "/linkage/database",
                format!(
                    "There is no database {:?}. The databases are: {}.",
                    linkage.database,
                    names()
                ),
            )],
        ),
    }
}

/// Each binding no placement in the chosen database holds on any day, though another
/// database holds one of its entries over the requested days, is `not_in_database`,
/// naming those databases; it replaces the binding's gap warnings, which [`clip`] would
/// otherwise turn into `mapping_missing`. A binding the chosen database holds in part
/// keeps its `coverage_gap`. An order never draws from two databases, so a project
/// merged from two databases' projects refuses here (crates/DESIGN.md, "Linkage
/// spaces").
fn not_in_database(
    held: &Connection,
    server: &Server,
    project: &ProjectData,
    order: &LogicalOrder,
    bindings: &Bindings,
    space: Option<&str>,
    findings: &mut Vec<Issue>,
) -> Result<(), Error> {
    let Some(space) = space else {
        return Ok(());
    };
    let index: BTreeMap<&str, usize> = project
        .sources
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();
    for (&(s, b), (variable, _, holds)) in bindings {
        if !holds.is_empty() {
            continue;
        }
        let mut elsewhere: BTreeSet<(i64, String)> = BTreeSet::new();
        for entry in order
            .entries
            .iter()
            .filter(|e| index[e.source.as_str()] == s && e.binding == b)
        {
            let days = entry.period.days(server.open);
            elsewhere.extend(rows(
                held,
                "SELECT s.ord, s.name FROM holding_space s WHERE s.id <> ?5 AND EXISTS(
                     SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
                       JOIN holding_table h ON h.id = c.holding_id
                     WHERE h.space = s.id AND m.table_pid = ?1 AND m.variable_pid = ?2
                       AND m.start < ?4 AND m.end > ?3)",
                rusqlite::params![entry.table, entry.variable, days.0, days.1, space],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?);
        }
        if elsewhere.is_empty() {
            continue;
        }
        let path = format!("/sources/{s}/bindings/{b}");
        findings.retain(|f| {
            f.path != path
                || !["coverage_gap", "column_window_unavailable"].contains(&f.code.as_str())
        });
        let chosen: String = held.query_row(
            "SELECT name FROM holding_space WHERE id = ?",
            [space],
            |r| r.get(0),
        )?;
        let source = &project.sources[s];
        // "for the requested period" only when the chosen database holds the column
        // in some other years.
        let held_other_years = order
            .entries
            .iter()
            .filter(|e| index[e.source.as_str()] == s && e.binding == b)
            .map(|e| {
                held.query_row(
                    "SELECT EXISTS(SELECT 1 FROM holding_mapping m
                       JOIN holding_column c ON c.id = m.column_id
                       JOIN holding_table h ON h.id = c.holding_id
                     WHERE h.space = ?3 AND m.table_pid = ?1 AND m.variable_pid = ?2)",
                    rusqlite::params![e.table, e.variable, space],
                    |r| r.get::<_, bool>(0),
                )
            })
            .collect::<Result<Vec<_>, _>>()?
            .contains(&true);
        let when = if held_other_years {
            " for the requested period"
        } else {
            ""
        };
        let names: Vec<&str> = elsewhere.iter().map(|(_, name)| name.as_str()).collect();
        findings.push(Issue {
            source: Some(source.name.clone()),
            variable: Some(variable.clone()),
            table: Some(source.table.clone()),
            ..Issue::new(
                IssueLevel::Error,
                "not_in_database",
                path,
                format!(
                    "{variable} in {} is not in the database {chosen}{when}. It is in: {}. \
                     Make a separate project for each database.",
                    source.table,
                    names.join(", ")
                ),
            )
        });
    }
    Ok(())
}

/// The accepted placements of (table, variable) over `days` in database `space`
/// (every database when `None`); a projection keeps only the days its parent does not
/// also place, and only those inside the request.
fn placements(
    held: &Connection,
    table: &str,
    variable: &str,
    days: Days,
    space: Option<&str>,
) -> Result<Vec<Placement>, Error> {
    let found: Vec<Placement> = rows(
        held,
        "SELECT h.id, h.projection_of, c.id, m.start, m.end, m.caveat
             FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
               JOIN holding_table h ON h.id = c.holding_id
             WHERE m.table_pid = ?1 AND m.variable_pid = ?2 AND m.start < ?4 AND m.end > ?3
               AND (?5 IS NULL OR h.space = ?5)
             ORDER BY h.id, c.ordinal, m.start",
        rusqlite::params![table, variable, days.0, days.1, space],
        |r| {
            Ok(Placement {
                holding: r.get(0)?,
                projection_of: r.get(1)?,
                column: r.get(2)?,
                days: (r.get(3)?, r.get(4)?),
                caveat: r.get(5)?,
            })
        },
    )?;
    let mut covered: BTreeMap<i64, Vec<Days>> = BTreeMap::new();
    for p in &found {
        covered.entry(p.holding).or_default().push(p.days);
    }
    let mut out = Vec::new();
    for p in found {
        let Some(parent) = p.projection_of.and_then(|parent| covered.get(&parent)) else {
            out.push(p);
            continue;
        };
        // A remainder can lie wholly outside the request (a parent-covered request).
        for days in overlap(&minus(&[p.days], &merge(parent.clone())), &[days]) {
            out.push(Placement { days, ..p.clone() });
        }
    }
    Ok(out)
}

fn holding(held: &Connection, id: i64) -> Result<Holding, Error> {
    let mut holding = held.query_row(
        &format!(
            "SELECT h.physical_name, h.schema_name, h.file_name, h.delivery, h.partition,
                h.projection_token, p.id, p.name, h.period_encoding, h.key_requirements,
                {}
             FROM holding_table h LEFT JOIN holding_column p ON p.id = h.period_column_id
             WHERE h.id = ?",
            crate::holdings::TABLE_LABEL
        ),
        [id],
        |r| {
            Ok(Holding {
                physical_name: r.get(0)?,
                schema_name: r.get(1)?,
                file_name: r.get(2)?,
                delivery: r.get(3)?,
                partition: r.get(4)?,
                projection: r.get(5)?,
                period_column: match (r.get(6)?, r.get(7)?, r.get(8)?) {
                    (Some(id), Some(column), Some(encoding)) => Some((id, column, encoding)),
                    _ => None,
                },
                keys: r.get(9)?,
                label: r.get(10)?,
                held: vec![],
            })
        },
    )?;
    holding.held = rows(
        held,
        "SELECT start, end FROM holding_period WHERE holding_id = ? ORDER BY start",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    holding.held = merge(std::mem::take(&mut holding.held));
    Ok(holding)
}

fn stored(held: &Connection, column: i64) -> Result<Stored, Error> {
    Ok(held.query_row(
        "SELECT name, ordinal, storage_type, length, precision, scale, slot_family, slot_kind,
                slot_index
         FROM holding_column WHERE id = ?",
        [column],
        |r| {
            Ok(Stored {
                name: r.get(0)?,
                ordinal: r.get(1)?,
                storage: Storage {
                    dtype: r.get(2)?,
                    length: r.get(3)?,
                    precision: r.get(4)?,
                    scale: r.get(5)?,
                },
                slot: match (r.get::<_, Option<String>>(7)?, r.get(8)?) {
                    (Some(kind), Some(index)) => Some(Slot {
                        family: r.get(6)?,
                        kind,
                        index,
                    }),
                    _ => None,
                },
            })
        },
    )?)
}

/// One span of days in plain words ([`words::plain`]).
fn span(server: &Server, (lo, hi): Days) -> String {
    words::plain(&[server.period(lo, hi)])
}

/// Spans of days in plain words ([`words::plain`]).
fn plain(server: &Server, days: &[Days]) -> String {
    let periods: Vec<_> = days.iter().map(|&(lo, hi)| server.period(lo, hi)).collect();
    words::plain(&periods)
}

/// The holdings findings for the days of `entry` no placement holds. A contested cell
/// (a publish-time `ambiguous_delivered_owners` notice naming this variable among its
/// rivals) is `mapping_missing` and blocks; a day inside a routing table's delivery
/// that only a column window leaves out is `column_window_unavailable`; any other is a
/// `coverage_gap`. Whether the last two block is decided per binding in [`clip`].
#[allow(clippy::too_many_arguments)]
fn gaps(
    held: &Connection,
    server: &Server,
    source: usize,
    entry: &OrderEntry,
    days: Days,
    space: Option<&str>,
    placements: &[Placement],
    findings: &mut Vec<Issue>,
) -> Result<(), Error> {
    let covered: Vec<Days> = merge(
        placements
            .iter()
            .map(|p| (days.0.max(p.days.0), days.1.min(p.days.1)))
            .collect(),
    );
    let missing = minus(&[days], &covered);
    if missing.is_empty() {
        return Ok(());
    }
    let contested: Vec<Days> = merge(rows(
        held,
        "SELECT n.start, n.end FROM holding_notice n
             JOIN holding_notice_candidate c ON c.notice_id = n.id
             JOIN holding_table h ON h.id = n.holding_id
             WHERE n.code = 'ambiguous_delivered_owners'
               AND c.table_pid = ?1 AND c.variable_pid = ?2 AND (?3 IS NULL OR h.space = ?3)",
        rusqlite::params![entry.table, entry.variable, space],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?);
    let windowed: Vec<Days> = merge(
        rows(
            held,
            "SELECT DISTINCT p.start, p.end FROM holding_period p
               JOIN holding_table h ON h.id = p.holding_id
             WHERE (?3 IS NULL OR h.space = ?3)
               AND EXISTS(SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
               WHERE c.holding_id = p.holding_id AND m.table_pid = ?1 AND m.variable_pid = ?2
                 AND m.window IS NOT NULL)",
            rusqlite::params![entry.table, entry.variable, space],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?,
    );
    let mut issue = |level, code: &str, piece: Days, message: String| {
        findings.push(Issue {
            source: Some(entry.source.clone()),
            variable: Some(entry.variable.clone()),
            table: Some(entry.table.clone()),
            period: Some(server.period(piece.0, piece.1)),
            ..Issue::new(
                level,
                code,
                format!("/sources/{source}/bindings/{}", entry.binding),
                message,
            )
        });
    };
    for piece in overlap(&missing, &contested) {
        issue(
            IssueLevel::Error,
            "mapping_missing",
            piece,
            format!(
                "{} in {} cannot be ordered for {}: it is not clear whether its column \
                 there holds this variable or another one. Leave out those years.",
                entry.variable,
                entry.table,
                span(server, piece)
            ),
        );
    }
    let rest = minus(&missing, &contested);
    for piece in overlap(&rest, &windowed) {
        issue(
            IssueLevel::Warning,
            "column_window_unavailable",
            piece,
            gap_message(
                "column_window_unavailable",
                &entry.variable,
                &entry.table,
                server,
                piece,
            ),
        );
    }
    for piece in minus(&rest, &windowed) {
        issue(
            IssueLevel::Warning,
            "coverage_gap",
            piece,
            gap_message("coverage_gap", &entry.variable, &entry.table, server, piece),
        );
    }
    Ok(())
}

/// A `coverage_gap` or `column_window_unavailable` message over `days` in words; [`clip`]
/// rewrites it when it reports only part of the gap.
fn gap_message(code: &str, variable: &str, table: &str, server: &Server, (lo, hi): Days) -> String {
    let days = words::plain_none(&[server.period(lo, hi)]);
    if code == "column_window_unavailable" {
        format!("The column for {variable} in {table} does not cover {days}.")
    } else {
        format!("There is no data on {variable} in {table} for {days}.")
    }
}

/// Per binding: a binding no placement holds at all blocks (its gaps become errors,
/// `coverage_gap` as `mapping_missing`); each binding delivered over less than its
/// request (logical availability and physical possession together) is a clip.
fn clip(
    server: &Server,
    order: &LogicalOrder,
    sources: &[reg_core::project::Source],
    bindings: &mut Bindings,
    findings: &mut Vec<Issue>,
) -> Vec<Clip> {
    let open = server.open;
    let mut clips = Vec::new();
    for (&(s, b), (variable, requested, holds)) in bindings.iter_mut() {
        let path = format!("/sources/{s}/bindings/{b}");
        let held = merge(std::mem::take(holds));
        let source = &sources[s].name;
        // The source's own request, not the logical entries: those are already cut to
        // catalog coverage, and the logical check may have left that cut unreported.
        let stated = match sources[s].period.items() {
            Ok(Some(items)) => Some(merge(
                items
                    .iter()
                    .map(|(lo, hi)| (super::day(lo), super::day(hi) + 1))
                    .collect(),
            )),
            Ok(None) => Some(vec![open]),
            Err(_) => None,
        };
        let asked = stated.unwrap_or_else(|| {
            order
                .clips
                .iter()
                .find(|c| &c.source == source && &c.variable == variable)
                .map_or_else(
                    || merge(std::mem::take(requested)),
                    |c| merge(c.requested.iter().map(|p| p.days(open)).collect()),
                )
        });
        // A gap inside a requested year the binding delivers in part is not reported
        // (#2409), judged over the whole binding: its entries can split mid-year.
        let gap =
            |f: &Issue| ["coverage_gap", "column_window_unavailable"].contains(&f.code.as_str());
        let mut kept = Vec::with_capacity(findings.len());
        for f in std::mem::take(findings) {
            let Some(piece) = (f.path == path && gap(&f))
                .then(|| f.period.as_ref().map(|p| p.days(open)))
                .flatten()
            else {
                kept.push(f);
                continue;
            };
            for sub in reported_gaps(&[piece], &asked, &held) {
                let mut part = f.clone();
                if sub != piece {
                    part.message = gap_message(
                        &part.code,
                        part.variable.as_deref().unwrap_or_default(),
                        part.table.as_deref().unwrap_or_default(),
                        server,
                        sub,
                    );
                    part.period = Some(server.period(sub.0, sub.1));
                }
                kept.push(part);
            }
        }
        *findings = kept;
        for f in findings.iter_mut().filter(|f| f.path == path) {
            // A gap says what the researcher can do only here, where its level is
            // decided: nothing when other years are delivered, else change the request.
            let gap = ["coverage_gap", "column_window_unavailable"].contains(&f.code.as_str());
            if held.is_empty() {
                f.level = IssueLevel::Error;
                if f.code == "coverage_gap" {
                    "mapping_missing".clone_into(&mut f.code);
                }
            }
            if gap {
                f.message.push_str(if held.is_empty() {
                    " Choose other years, or remove the variable from the project."
                } else {
                    " Those years will not be delivered."
                });
            }
        }
        if !reported_gaps(&minus(&asked, &held), &asked, &held).is_empty() {
            let periods = |days: &[Days]| days.iter().map(|&(a, z)| server.period(a, z)).collect();
            clips.push(Clip {
                source: source.clone(),
                binding: b,
                variable: variable.clone(),
                requested: periods(&asked),
                ordered: periods(&held),
            });
        }
    }
    clips
}

/// Add each declared key of the holding over the days the file extracts: the key column joins
/// once, as `carried` when the table carries its parent's copy, else `row_key`; a
/// column a binding already orders stays `ordered`. A key on hold, uncovered or placed
/// in two columns blocks the order. A declaration with `period_key = "column"` adds
/// the physical period column as a `row_key` without references.
#[allow(clippy::too_many_lines)]
fn keys(
    server: &Server,
    held: &Connection,
    holding_id: i64,
    holding: &Holding,
    sources: &[reg_core::project::Source],
    draft: &mut Draft,
    findings: &mut Vec<Issue>,
) -> Result<(), Error> {
    let requirements: Vec<Value> = serde_json::from_str(&holding.keys)
        .map_err(|_| Error::internal("The row identifier data is invalid."))?;
    let mut columns = held.prepare(
        "SELECT c.id, m.table_pid, m.start, m.end, m.carried, c.name, m.caveat
         FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
         WHERE c.holding_id = ? AND m.variable_pid = ? ORDER BY c.ordinal, m.start",
    )?;
    // Keys are resolved over the rows the file extracts, not only the requested days:
    // the row filter widens to the encoding's grain, and an unfiltered file extracts
    // its whole delivery.
    let extracted = extracted(holding, draft);
    let per_source: Vec<usize> = draft.days.keys().copied().collect();
    for (r, requirement) in requirements.iter().enumerate() {
        let scope: Vec<Days> = match requirement["period"].as_str() {
            None => vec![(i64::MIN, i64::MAX)],
            Some(period) => period
                .split(',')
                .map(|token| {
                    let (lo, hi) = token
                        .trim()
                        .parse::<reg_core::Period>()
                        .map_err(|_| Error::internal("The row identifier data is invalid."))?
                        .iso_bounds();
                    Ok((super::day(&lo), super::day(&hi) + 1))
                })
                .collect::<Result<_, Error>>()?,
        };
        let variables: Vec<&str> = ["entity", "time"]
            .iter()
            .flat_map(|k| requirement[*k].as_array().into_iter().flatten())
            .filter_map(Value::as_str)
            .collect();
        for s in &per_source {
            let days = overlap(&extracted, &scope);
            if days.is_empty() {
                continue;
            }
            let block = |code: &str, message: String| Issue {
                source: Some(sources[*s].name.clone()),
                ..Issue::new(IssueLevel::Error, code, format!("/sources/{s}"), message)
            };
            if !requirement["hold"].is_null() {
                findings.push(block(
                    "held_key_missing",
                    format!(
                        "Table {:?} cannot be ordered: its row identifier is not available. \
                         Leave this table out.",
                        holding.label
                    ),
                ));
                continue;
            }
            for (v, variable) in variables.iter().enumerate() {
                let found: Vec<KeyMapping> = columns
                    .query_map(rusqlite::params![holding_id, variable], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            (row.get(2)?, row.get(3)?),
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                let over: Vec<_> = found
                    .iter()
                    .filter(|(_, _, d, ..)| !overlap(&days, &[*d]).is_empty())
                    .collect();
                // Two columns are ambiguous only over a day both place the key; a key
                // in one column for some years and another for the rest is not.
                let distinct: BTreeSet<i64> = over
                    .iter()
                    .enumerate()
                    .flat_map(|(i, a)| over[i + 1..].iter().map(move |b| (a, b)))
                    .filter(|(a, b)| {
                        a.0 != b.0 && !overlap(&overlap(&days, &[a.2]), &[b.2]).is_empty()
                    })
                    .flat_map(|(a, b)| [a.0, b.0])
                    .collect();
                let covered = merge(over.iter().map(|(_, _, d, ..)| *d).collect());
                let uncovered = minus(&days, &covered);
                if !distinct.is_empty() {
                    let mut issue = block(
                        "held_key_ambiguous",
                        format!(
                            "Table {:?} has {} columns that could be its row identifier \
                             {variable}, so it cannot be ordered for the years requested. \
                             Leave out those years.",
                            holding.label,
                            distinct.len()
                        ),
                    );
                    issue.variable = Some((*variable).to_owned());
                    findings.push(issue);
                    continue;
                }
                if let Some(&piece) = uncovered.first() {
                    let mut issue = block(
                        "held_key_missing",
                        format!(
                            "Table {:?} has no column for its row identifier {variable} \
                             for {}, so those years cannot be ordered. Leave them out.",
                            holding.label,
                            words::plain_none(&[server.period(piece.0, piece.1)])
                        ),
                    );
                    issue.variable = Some((*variable).to_owned());
                    issue.period = Some(server.period(piece.0, piece.1));
                    findings.push(issue);
                    continue;
                }
                for (column, table, d, carried, _, caveat) in over {
                    let entry = draft.columns.entry(*column).or_insert_with(|| {
                        let role = if *carried {
                            Role::Carried
                        } else {
                            Role::RowKey
                        };
                        ((1, r, v, 0, 0, 0), role, vec![])
                    });
                    // A source whose binding orders this key column already references
                    // it; every other source sharing the file still needs the key.
                    if entry.1 == Role::Ordered
                        && entry
                            .2
                            .iter()
                            .any(|r| r.source == sources[*s].name && r.binding.is_some())
                    {
                        continue;
                    }
                    let name = found
                        .iter()
                        .find(|f| f.0 == *column)
                        .map(|f| f.4.clone())
                        .unwrap_or_default();
                    for piece in overlap(&days, &[*d]) {
                        // A bound key reported its caveat at the binding (it stays
                        // `ordered` above); an added key reports it at the source.
                        if let Some(caveat) = caveat {
                            findings.push(Issue {
                                source: Some(sources[*s].name.clone()),
                                variable: Some((*variable).to_owned()),
                                table: Some(table.clone()),
                                period: Some(server.period(piece.0, piece.1)),
                                ..Issue::new(
                                    IssueLevel::Warning,
                                    "mapping_caveat",
                                    format!("/sources/{s}"),
                                    format!(
                                        "{variable} in {table} for {}: {caveat}",
                                        span(server, piece)
                                    ),
                                )
                            });
                        }
                        entry.2.push(Ref {
                            source: sources[*s].name.clone(),
                            binding: None,
                            table: table.clone(),
                            variable: (*variable).to_owned(),
                            period: server.period(piece.0, piece.1),
                            spelling: name.clone(),
                        });
                    }
                }
            }
            // The physical period column is a held row key wherever an active
            // declaration supplies its time from it; it places no logical variable.
            if requirement["period_key"].as_str() == Some("column")
                && let Some((column, ..)) = &holding.period_column
            {
                draft
                    .columns
                    .entry(*column)
                    .or_insert_with(|| ((1, r, variables.len(), 0, 0, 0), Role::RowKey, vec![]));
            }
        }
    }
    Ok(())
}

/// A key's mapping: (column id, table, days, carried, column name, caveat).
type KeyMapping = (i64, String, Days, bool, String, Option<String>);

/// The held days a file extracts: its requested days widened to the period column's
/// grain, or the whole delivery when it has no period column (the row filter's rule).
fn extracted(holding: &Holding, draft: &Draft) -> Vec<Days> {
    let union = merge(draft.days.values().flatten().copied().collect());
    match &holding.period_column {
        _ if holding.held.is_empty() => union,
        None => holding.held.clone(),
        Some((_, _, encoding)) => {
            let encoding = Encoding::parse(encoding).expect("an admitted encoding");
            overlap(
                &union
                    .iter()
                    .map(|&d| widen(encoding, d))
                    .collect::<Vec<_>>(),
                &holding.held,
            )
        }
    }
}

/// The first and last held day numbers rounded to an encoding's grain.
fn widen(encoding: Encoding, (lo, hi): Days) -> Days {
    let start = |iso: &str| -> String {
        match encoding {
            Encoding::Year => format!("{}-01-01", &iso[..4]),
            Encoding::Yyyymm => format!("{}-01", &iso[..7]),
            Encoding::Date | Encoding::Yyyymmdd => iso.to_owned(),
        }
    };
    let next = |iso: &str| -> String {
        // The first day of the next grain unit after the inclusive last day.
        match encoding {
            Encoding::Year => {
                let y: i64 = iso[..4].parse().expect("an ISO year");
                format!("{:04}-01-01", y + 1)
            }
            Encoding::Yyyymm => {
                reg_core::add_iso_months(&format!("{}-01", &iso[..7]), 1).expect("an ISO month")
            }
            Encoding::Date | Encoding::Yyyymmdd => reg_core::next_iso_day(iso),
        }
    };
    let first = iso_day(lo);
    let last = iso_day(hi - 1);
    (super::day(&start(&first)), super::day(&next(&last)))
}

/// The file's row filter: the union of its sources' requested days, widened to the
/// encoding's grain, with a bound omitted where it cuts no held day; none when that
/// covers every held day. A table without a period column is extracted whole, which
/// is reported when the request is narrower; sources sharing a filtered file are told
/// that each of its columns is extracted over the union. The findings name the extra
/// days, never the physical file or its delivery code (#2409).
fn row_filter(
    server: &Server,
    holding: &Holding,
    draft: &Draft,
    asked: &BTreeMap<usize, Vec<Days>>,
    sources: &[reg_core::project::Source],
    findings: &mut Vec<Issue>,
) -> Option<RowFilter> {
    let union = merge(draft.days.values().flatten().copied().collect());
    let spans = |days: &[Days]| plain(server, days);
    let mut info = |s: usize, code: &str, message: String| {
        let issue = Issue {
            source: Some(sources[s].name.clone()),
            ..Issue::new(IssueLevel::Info, code, format!("/sources/{s}"), message)
        };
        // Two files of one source can add the same days.
        if !findings.contains(&issue) {
            findings.push(issue);
        }
    };
    let Some((_, column, encoding)) = &holding.period_column else {
        for s in draft.days.keys() {
            let extra = minus(&holding.held, &asked[s]);
            if !extra.is_empty() {
                info(
                    *s,
                    "rows_unfiltered",
                    format!(
                        "Rows from {} are included because this table has no year column.",
                        spans(&extra)
                    ),
                );
            }
        }
        return None;
    };
    let encoding = Encoding::parse(encoding).expect("an admitted encoding");
    let widened = merge(union.iter().map(|&d| widen(encoding, d)).collect());
    // A source is told when the grain adds held days around its own request that no
    // source asked for; days another source asked for are `row_filter_union`'s.
    for (s, days) in &draft.days {
        let around = merge(days.iter().map(|&d| widen(encoding, d)).collect());
        let extra = overlap(&minus(&minus(&around, &union), &asked[s]), &holding.held);
        if !extra.is_empty() {
            let unit = match encoding {
                Encoding::Year => "year",
                Encoding::Yyyymm => "month",
                Encoding::Date | Encoding::Yyyymmdd => "day",
            };
            info(
                *s,
                "row_filter_widened",
                format!(
                    "Rows from {} are included because this table's rows are selected \
                     by whole {unit}s.",
                    spans(&extra)
                ),
            );
        }
    }
    let ranges = overlap(&widened, &holding.held);
    // The union finding comes before the whole-delivery early return: sources that
    // jointly cover the delivery still each get every column over the union.
    if draft.days.len() > 1 {
        for (s, days) in &draft.days {
            if *days != union {
                info(
                    *s,
                    "row_filter_union",
                    format!(
                        "This table shares its file with another table of the project, so \
                         its columns are delivered for {}.",
                        spans(&union)
                    ),
                );
            }
        }
    }
    if minus(&holding.held, &ranges).is_empty() {
        return None;
    }
    let first = holding.held.first().map_or(i64::MIN, |d| d.0);
    let last = holding.held.last().map_or(i64::MAX, |d| d.1);
    Some(RowFilter {
        column: column.clone(),
        encoding,
        ranges: ranges
            .iter()
            .map(|&(lo, hi)| Range {
                from: (lo > first).then(|| iso_day(lo)),
                to: (hi < last).then(|| iso_day(hi - 1)),
            })
            .collect(),
    })
}
