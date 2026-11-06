//! Source-derived mapping equality, independent of compiler candidate/placement logic.
use std::collections::BTreeMap;

use reg_core::days::{self, Span};
use rusqlite::Connection;

use super::{compile, input};
use crate::error::Result;

type Mappings = BTreeMap<Attributes, Vec<Span>>;
/// Blocked spans by (notice code, rival table, rival variable).
type Contested = BTreeMap<(String, String, String), Vec<Span>>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Attributes {
    table: String,
    variable: String,
    role: String,
    carried: bool,
    caveat: Option<String>,
    window: Option<String>,
}

struct Claim {
    attributes: Attributes,
    spans: Vec<Span>,
    ambiguous: bool,
}

/// Projection key presence/uniqueness has already been checked by `keys::verify`.
/// Here only the source declarations determine the required carried flag.
fn projection_copy(
    table: &input::Table,
    tables: &BTreeMap<compile::Identity, &input::Table>,
    keyed: bool,
    variable: &str,
    span: Span,
) -> Result<bool> {
    // A projection's parent is in the projection's own schema.
    let Some(parent) = table
        .projection_of
        .as_ref()
        .and_then(|p| tables.get(&(compile::authored_identity(table, keyed).0, p.clone())))
    else {
        return Ok(false);
    };
    let mut shared = Vec::new();
    for child in &table.key {
        for parent in &parent.key {
            if child.hold.is_some() || parent.hold.is_some() {
                continue;
            }
            if child
                .entity
                .iter()
                .any(|v| v == variable && parent.entity.contains(v))
                || child
                    .time
                    .iter()
                    .any(|v| v == variable && parent.time.contains(v))
            {
                let scope = |key: &input::RowKey| {
                    key.period
                        .as_deref()
                        .map(compile::periods)
                        .transpose()
                        .map(|s| s.unwrap_or_else(|| vec![span]))
                };
                shared.extend(days::overlap(&scope(child)?, &scope(parent)?));
            }
        }
    }
    Ok(days::covered(&[span], &shared))
}

fn claims(
    table: &input::Table,
    column: &reg_readers::holdings::Column,
    index: &BTreeMap<String, Vec<compile::Segment>>,
) -> Result<Vec<Claim>> {
    let decision = table.column.iter().find(|c| c.name == column.name);
    let spans = compile::column_periods(decision, &compile::table_periods(table)?)?;
    if spans.is_empty()
        || decision.is_some_and(|c| c.hold.is_some() || c.unmap.is_some())
        || table.key.iter().any(|k| k.hold.is_some())
    {
        return Ok(Vec::new());
    }
    let mut claims = Vec::new();
    // Choose explicit versus automatic from the full authored list, before holds.
    if let Some(decision) = decision.filter(|d| !d.mapping.is_empty()) {
        for mapping in &decision.mapping {
            if mapping.hold.is_some() {
                continue;
            }
            if mapping.locator.trim().is_empty() {
                return Err("stale_declared_mapping_target: evidence locator required".into());
            }
            let wanted = mapping
                .delivery
                .as_deref()
                .map(compile::periods)
                .transpose()?
                .unwrap_or_else(|| spans.clone());
            let segments = index
                .get(&mapping.table_pid)
                .ok_or("holdings_table_route_unresolved")?;
            let variable: Vec<_> = segments
                .iter()
                .filter(|s| s.variable == mapping.variable_pid)
                .collect();
            if variable.is_empty() {
                return Err(
                    "stale_declared_mapping_target: variable absent from routed table".into(),
                );
            }
            let documented: Vec<_> = variable
                .iter()
                .filter(|s| {
                    s.state != "ambiguous" && s.timeless == (table.disposition == "timeless")
                })
                .map(|s| (s.start, s.end))
                .collect();
            if mapping.delivery.is_some()
                && (!days::covered(&wanted, &spans) || !days::covered(&wanted, &documented))
            {
                return Err(
                    "inventory_window_invalid: authored window escapes file or documented coverage"
                        .into(),
                );
            }
            for segment in segments.iter().filter(|s| {
                s.variable == mapping.variable_pid
                    && s.timeless == (table.disposition == "timeless")
            }) {
                claims.push(claim(
                    segment,
                    &wanted,
                    mapping.carried,
                    mapping.caveat.clone(),
                    mapping.delivery.clone(),
                ));
            }
        }
    } else {
        let spelling = reg_core::fold_identity(&column.match_name);
        for route in &table.routes {
            let segments = index.get(route).ok_or("holdings_table_route_unresolved")?;
            for segment in segments.iter().filter(|s| {
                reg_core::fold_identity(&s.spelling) == spelling
                    && s.timeless == (table.disposition == "timeless")
            }) {
                claims.push(claim(segment, &spans, false, None, None));
            }
        }
    }
    claims.retain(|c| !c.spans.is_empty());
    Ok(claims)
}

fn claim(
    segment: &compile::Segment,
    wanted: &[Span],
    carried: bool,
    caveat: Option<String>,
    window: Option<String>,
) -> Claim {
    Claim {
        attributes: Attributes {
            table: segment.table.clone(),
            variable: segment.variable.clone(),
            role: segment.role.clone(),
            carried,
            caveat,
            window,
        },
        spans: days::overlap(wanted, &[(segment.start, segment.end)]),
        ambiguous: segment.state == "ambiguous",
    }
}

fn expected(
    claims: &[Claim],
    table: &input::Table,
    tables: &BTreeMap<compile::Identity, &input::Table>,
    keyed: bool,
) -> Result<(Mappings, Contested)> {
    let mut routes = BTreeMap::<&str, Vec<&Claim>>::new();
    for claim in claims {
        routes
            .entry(&claim.attributes.table)
            .or_default()
            .push(claim);
    }
    let mut expected = Mappings::new();
    let mut contested = Contested::new();
    for claims in routes.values() {
        for span in days::slices(claims.iter().flat_map(|c| c.spans.iter().copied())) {
            let active: Vec<_> = claims
                .iter()
                .filter(|c| days::covered(&[span], &c.spans))
                .collect();
            let Some(first) = active.first() else {
                continue;
            };
            // Preserve every rival, including ambiguous semantic segments, until
            // ownership is decided for this logical table and interval.
            let rivals = active
                .iter()
                .any(|c| c.attributes.variable != first.attributes.variable);
            if rivals || active.iter().any(|c| c.ambiguous) {
                // Each blocked interval records every rival, which orders refuse.
                let code = if rivals {
                    "ambiguous_delivered_owners"
                } else {
                    "ambiguous_catalog_segment"
                };
                for c in &active {
                    contested
                        .entry((
                            code.to_owned(),
                            c.attributes.table.clone(),
                            c.attributes.variable.clone(),
                        ))
                        .or_default()
                        .push(span);
                }
                continue;
            }
            if active.iter().any(|c| {
                c.attributes.carried != first.attributes.carried
                    || c.attributes.caveat != first.attributes.caveat
                    || c.attributes.window != first.attributes.window
            }) {
                return Err(
                    "inventory_cell_conflict: incompatible admitted mapping attributes".into(),
                );
            }
            let mut attributes = first.attributes.clone();
            attributes.carried |=
                projection_copy(table, tables, keyed, &attributes.variable, span)?;
            expected.entry(attributes).or_default().push(span);
        }
    }
    Ok((expected, contested))
}

pub(super) fn verify(
    db: &Connection,
    base: &Connection,
    topology: &input::Topology,
    census: &reg_readers::holdings::ReadOutput,
) -> Result<()> {
    // segment_index seeds from every tbl row, including segmentless tables.
    let index = compile::segment_index(base, db)?;
    compile::renames_resolve(&index, &census.rename_targets)?;
    let keyed = compile::schema_keyed(&census.tables);
    let tables: BTreeMap<_, _> = topology
        .table
        .iter()
        .map(|t| (compile::authored_identity(t, keyed), t))
        .collect();
    let mut query = db.prepare(
        "SELECT m.table_pid,m.variable_pid,m.role,m.carried,m.caveat,m.window,m.start,m.end
         FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
         JOIN holding_table h ON h.id=c.holding_id WHERE h.physical_name=?1 AND c.name=?2 AND (?3 IS NULL OR h.schema_name=?3)",
    )?;
    let mut rivals = db.prepare(
        "SELECT n.code,r.table_pid,r.variable_pid,n.start,n.end
         FROM holding_notice_candidate r JOIN holding_notice n ON n.id=r.notice_id
         JOIN holding_column c ON c.id=n.column_id JOIN holding_table h ON h.id=c.holding_id
         WHERE h.physical_name=?1 AND c.name=?2 AND (?3 IS NULL OR h.schema_name=?3)",
    )?;
    for table in &census.tables {
        let authored = tables[&compile::census_identity(table)];
        for column in &table.columns {
            let location = format!("census:row={}:column={}", table.source_row, column.ordinal);
            let mut compare = || -> Result<Option<&str>> {
                let (mut expected, mut contested) =
                    expected(&claims(authored, column, &index)?, authored, &tables, keyed)?;
                let mut stored = Mappings::new();
                let rows = query.query_map(
                    rusqlite::params![table.name, column.name, table.schema],
                    |r| {
                        Ok((
                            Attributes {
                                table: r.get(0)?,
                                variable: r.get(1)?,
                                role: r.get(2)?,
                                carried: r.get(3)?,
                                caveat: r.get(4)?,
                                window: r.get(5)?,
                            },
                            (r.get(6)?, r.get(7)?),
                        ))
                    },
                )?;
                for row in rows {
                    let (attributes, span) = row?;
                    stored.entry(attributes).or_default().push(span);
                }
                for mappings in [&mut stored, &mut expected] {
                    for spans in mappings.values_mut() {
                        *spans = days::merge(std::mem::take(spans));
                    }
                }
                if stored != expected {
                    return Ok(Some(
                        "holdings_mapping_source_mismatch: complete intervals or admitted attributes differ",
                    ));
                }
                let mut recorded = Contested::new();
                let rows = rivals.query_map(
                    rusqlite::params![table.name, column.name, table.schema],
                    |r| Ok(((r.get(0)?, r.get(1)?, r.get(2)?), (r.get(3)?, r.get(4)?))),
                )?;
                for row in rows {
                    let (key, span) = row?;
                    recorded.entry(key).or_default().push(span);
                }
                for spans in recorded.values_mut().chain(contested.values_mut()) {
                    *spans = days::merge(std::mem::take(spans));
                }
                Ok((recorded != contested).then_some(
                    "holdings_contested_source_mismatch: blocked intervals or their recorded rivals differ",
                ))
            };
            if let Some(code) = compare().map_err(|e| format!("{location}: {e}"))? {
                return Err(format!("{location}: {code}").into());
            }
        }
    }
    Ok(())
}
