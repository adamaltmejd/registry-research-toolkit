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

use super::{Issue, LogicalOrder, OrderEntry, check, project};
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
            "This server has no data holder's tables, so it cannot produce an order file.",
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
    let mut findings: Vec<Issue> = Vec::new();
    let mut drafts: BTreeMap<i64, Draft> = BTreeMap::new();
    let mut bindings = Bindings::new();
    for (e, entry) in order.entries.iter().enumerate() {
        let s = index[entry.source.as_str()];
        let days = entry.period.days(open);
        let placements = placements(held, &entry.table, &entry.variable, days)?;
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
                            "{} in {} over {}: {caveat}",
                            entry.variable,
                            entry.table,
                            span(server, piece)
                        ),
                    )
                });
            }
        }
        gaps(held, server, s, entry, days, &placements, &mut findings)?;
    }
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
                    holding.file_name, holding.physical_name
                ),
            ));
        }
        if let Some(other) = names.insert(
            holding.file_name.to_lowercase(),
            holding.physical_name.clone(),
        ) {
            findings.push(at(
                "file_name_unsafe",
                format!(
                    "Tables {other:?} and {:?} name files that differ only in case.",
                    holding.physical_name
                ),
            ));
        }
        if holding.schema_name.is_none() {
            findings.push(at(
                "physical_schema_missing",
                format!(
                    "Table {:?} has no single stored schema.",
                    holding.physical_name
                ),
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
        let row_filter = row_filter(server, &holding, &draft, &project.sources, &mut findings);
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
        if !held_as_stated(server, held, project, &finding.issue)? {
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
    let placements = placements(held, table, variable, period.days(server.open))?;
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

/// The accepted placements of (table, variable) over `days`; a projection keeps only
/// the days its parent does not also place, and only those inside the request.
fn placements(
    held: &Connection,
    table: &str,
    variable: &str,
    days: Days,
) -> Result<Vec<Placement>, Error> {
    let found: Vec<Placement> = rows(
        held,
        "SELECT h.id, h.projection_of, c.id, m.start, m.end, m.caveat
             FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
               JOIN holding_table h ON h.id = c.holding_id
             WHERE m.table_pid = ?1 AND m.variable_pid = ?2 AND m.start < ?4 AND m.end > ?3
             ORDER BY h.id, c.ordinal, m.start",
        rusqlite::params![table, variable, days.0, days.1],
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
        "SELECT h.physical_name, h.schema_name, h.file_name, h.delivery, h.partition,
                h.projection_token, p.id, p.name, h.period_encoding, h.key_requirements
         FROM holding_table h LEFT JOIN holding_column p ON p.id = h.period_column_id
         WHERE h.id = ?",
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

fn span(server: &Server, (lo, hi): Days) -> String {
    let p = server.period(lo, hi);
    format!(
        "[{}, {})",
        p.start.as_deref().unwrap_or("open"),
        p.end.as_deref().unwrap_or("open")
    )
}

/// The holdings findings for the days of `entry` no placement holds. A contested cell
/// (a publish-time `ambiguous_delivered_owners` notice naming this variable among its
/// rivals) is `mapping_missing` and blocks; a day inside a routing table's delivery
/// that only a column window leaves out is `column_window_unavailable`; any other is a
/// `coverage_gap`. Whether the last two block is decided per binding in [`clip`].
fn gaps(
    held: &Connection,
    server: &Server,
    source: usize,
    entry: &OrderEntry,
    days: Days,
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
             WHERE n.code = 'ambiguous_delivered_owners'
               AND c.table_pid = ? AND c.variable_pid = ?",
        [&entry.table, &entry.variable],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?);
    let windowed: Vec<Days> = merge(
        rows(
            held,
            "SELECT DISTINCT p.start, p.end FROM holding_period p
             WHERE EXISTS(SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
               WHERE c.holding_id = p.holding_id AND m.table_pid = ? AND m.variable_pid = ?
                 AND m.window IS NOT NULL)",
            [&entry.table, &entry.variable],
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
                "{} in {} cannot be ordered for {}: the data holder's column for it is \
                 also claimed by another variable. Leave out those years, or ask the data \
                 holder",
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
            format!(
                "The data holder's column for {} in {} does not cover {}, so those years \
                 cannot be delivered",
                entry.variable,
                entry.table,
                span(server, piece)
            ),
        );
    }
    for piece in minus(&rest, &windowed) {
        issue(
            IssueLevel::Warning,
            "coverage_gap",
            piece,
            format!(
                "The data holder has no data on {} in {} for {}, so those years cannot be \
                 delivered",
                entry.variable,
                entry.table,
                span(server, piece)
            ),
        );
    }
    Ok(())
}

/// Per binding: a binding no placement holds at all blocks (its gaps become errors,
/// `coverage_gap` as `mapping_missing`); each binding delivered over less than its
/// request (logical availability and physical possession together) is a clip.
fn clip(
    server: &Server,
    order: &LogicalOrder,
    sources: &[reg_core::project::Source],
    bindings: &mut Bindings,
    findings: &mut [Issue],
) -> Vec<Clip> {
    let open = server.open;
    let mut clips = Vec::new();
    for (&(s, b), (variable, requested, holds)) in bindings.iter_mut() {
        let path = format!("/sources/{s}/bindings/{b}");
        let held = merge(std::mem::take(holds));
        if held.is_empty() {
            for f in findings.iter_mut().filter(|f| f.path == path) {
                f.level = IssueLevel::Error;
                if f.code == "coverage_gap" {
                    "mapping_missing".clone_into(&mut f.code);
                }
            }
        }
        let source = &sources[s].name;
        let asked = order
            .clips
            .iter()
            .find(|c| &c.source == source && &c.variable == variable)
            .map_or_else(
                || merge(std::mem::take(requested)),
                |c| merge(c.requested.iter().map(|p| p.days(open)).collect()),
            );
        if asked != held {
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
        .map_err(|_| Error::internal("The holder's row-key data is invalid."))?;
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
                        .map_err(|_| Error::internal("The holder's row-key data is invalid."))?
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
                        "Table {:?} cannot be ordered yet: the data holder does not \
                         supply its row identifier. Leave this table out, or ask the data \
                         holder",
                        holding.physical_name
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
                    findings.push(block(
                        "held_key_ambiguous",
                        format!(
                            "Table {:?} has {} columns that could be its row identifier \
                             {variable}, so it cannot be ordered for the years requested. Ask \
                             the data holder",
                            holding.physical_name,
                            distinct.len()
                        ),
                    ));
                    continue;
                }
                if let Some(&piece) = uncovered.first() {
                    let mut issue = block(
                        "held_key_missing",
                        format!(
                            "Table {:?} has no column for its row identifier {variable} \
                             for {}, so those years cannot be ordered. Leave them out",
                            holding.physical_name,
                            span(server, piece)
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
                                        "{variable} in {table} over {}: {caveat}",
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
/// that each of its columns is extracted over the union.
fn row_filter(
    server: &Server,
    holding: &Holding,
    draft: &Draft,
    sources: &[reg_core::project::Source],
    findings: &mut Vec<Issue>,
) -> Option<RowFilter> {
    let union = merge(draft.days.values().flatten().copied().collect());
    let info = |s: usize, code: &str, message: String| Issue {
        source: Some(sources[s].name.clone()),
        ..Issue::new(IssueLevel::Info, code, format!("/sources/{s}"), message)
    };
    let Some((_, column, encoding)) = &holding.period_column else {
        for (s, days) in &draft.days {
            if !minus(&holding.held, days).is_empty() {
                findings.push(info(
                    *s,
                    "rows_unfiltered",
                    format!(
                        "Table {:?} has no period column, so every row of its \
                         delivery {} is extracted",
                        holding.physical_name,
                        holding.delivery.as_deref().unwrap_or("(timeless)")
                    ),
                ));
            }
        }
        return None;
    };
    let encoding = Encoding::parse(encoding).expect("an admitted encoding");
    let widened = merge(union.iter().map(|&d| widen(encoding, d)).collect());
    // A source is told when the grain adds days around its own request that no source
    // asked for; days another source asked for are `row_filter_union`'s.
    for (s, days) in &draft.days {
        let around = merge(days.iter().map(|&d| widen(encoding, d)).collect());
        if !minus(&around, &union).is_empty() {
            findings.push(info(
                *s,
                "row_filter_widened",
                format!(
                    "Rows of table {:?} are filtered on {column:?} at its {} precision, \
                     so the filter covers whole units around the request",
                    holding.physical_name,
                    serde_json::to_value(encoding)
                        .expect("serializes")
                        .as_str()
                        .unwrap_or("")
                ),
            ));
        }
    }
    let ranges = overlap(&widened, &holding.held);
    // The union finding comes before the whole-delivery early return: sources that
    // jointly cover the delivery still each get every column over the union.
    if draft.days.len() > 1 {
        for (s, days) in &draft.days {
            if *days != union {
                findings.push(info(
                    *s,
                    "row_filter_union",
                    format!(
                        "Several sources share table {:?}; every column of its file \
                         is extracted over the union of their periods, {}",
                        holding.physical_name,
                        union
                            .iter()
                            .map(|&d| span(server, d))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
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
