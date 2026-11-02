//! Interval-local ownership and metadata-only physical emission.
use std::collections::{BTreeMap, BTreeSet};

use reg_readers::holdings as census;
use rusqlite::{Connection, params};

use super::input;

type Span = (i64, i64);

#[derive(Clone)]
pub(super) struct Segment {
    pub table: String,
    pub variable: String,
    pub start: i64,
    pub end: i64,
    pub spelling: String,
    pub role: String,
    pub state: String,
    pub dtype: Option<String>,
    pub length: Option<i64>,
    pub timeless: bool,
}

pub(super) fn segments(db: &Connection) -> Result<Vec<Segment>, String> {
    let mut query = db
        .prepare(
            "SELECT t.pid,v.pid,s.start,s.end,sp.exact,s.role,s.state,s.dtype,s.length,t.timeless
      FROM segment s JOIN tbl t ON t.id=s.table_id JOIN variable v ON v.id=s.variable_id
      JOIN spelling sp ON sp.id=s.spelling_id ORDER BY t.pid,v.pid,s.start",
        )
        .map_err(|e| e.to_string())?;
    query
        .query_map([], |r| {
            Ok(Segment {
                table: r.get(0)?,
                variable: r.get(1)?,
                start: r.get(2)?,
                end: r.get(3)?,
                spelling: r.get(4)?,
                role: r.get(5)?,
                state: r.get(6)?,
                dtype: r.get(7)?,
                length: r.get(8)?,
                timeless: r.get(9)?,
            })
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

pub(super) fn merge(mut spans: Vec<Span>) -> Vec<Span> {
    spans.sort_unstable();
    let mut out: Vec<Span> = Vec::new();
    for (start, end) in spans {
        if let Some(last) = out.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
        } else {
            out.push((start, end));
        }
    }
    out
}

pub(super) fn covered(wanted: &[Span], by: &[Span]) -> bool {
    let by = merge(by.to_vec());
    wanted
        .iter()
        .all(|(a, b)| by.iter().any(|(c, d)| c <= a && b <= d))
}

pub(super) fn overlap(left: &[Span], right: &[Span]) -> Vec<Span> {
    merge(
        left.iter()
            .flat_map(|(a, b)| {
                right.iter().filter_map(move |(c, d)| {
                    let span = ((*a).max(*c), (*b).min(*d));
                    (span.0 < span.1).then_some(span)
                })
            })
            .collect(),
    )
}

pub(super) fn periods(value: &str) -> Result<Vec<Span>, String> {
    let mut out = Vec::new();
    for token in value.split(',') {
        let period: reg_core::Period = token
            .trim()
            .parse()
            .map_err(|_| "inventory_window_invalid: invalid period spelling")?;
        let (lo, hi) = period.iso_bounds();
        out.push((crate::emit::start(&lo), crate::emit::end(&hi)));
    }
    Ok(merge(out))
}

fn canonical_period(value: &str) -> Result<String, String> {
    let spans = value
        .split(',')
        .map(|token| {
            token
                .trim()
                .parse::<reg_core::Period>()
                .map(reg_core::Period::iso_bounds)
                .map_err(|_| "inventory_window_invalid: invalid period spelling".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(reg_core::render(&reg_core::merge(spans)))
}

fn table_periods(table: &input::Table) -> Result<Vec<Span>, String> {
    match (table.disposition.as_str(), table.delivery.as_deref()) {
        ("dated", Some(delivery)) => periods(delivery),
        ("timeless", None) => Ok(vec![(
            crate::emit::start(reg_frame::OPEN_START),
            crate::emit::end(reg_frame::OPEN_END),
        )]),
        ("unknown" | "lookup" | "excluded", None)
            if table.reason.as_ref().is_some_and(|r| !r.trim().is_empty()) =>
        {
            Ok(vec![])
        }
        _ => Err(
            "holdings_disposition_invalid: disposition needs explicit coverage or reason".into(),
        ),
    }
}

fn column_periods(column: Option<&input::Column>, spans: &[Span]) -> Result<Vec<Span>, String> {
    let Some(column) = column else {
        return Ok(spans.to_vec());
    };
    match (column.slot_kind.as_deref(), column.slot_index.as_ref()) {
        (None, None) => Ok(spans.to_vec()),
        (Some("position"), Some(index)) if index.to_string().parse::<u32>().is_ok() => {
            Ok(spans.to_vec())
        }
        (Some(kind @ ("year" | "month")), Some(index))
            if index.to_string().len() == if kind == "year" { 4 } else { 7 } =>
        {
            periods(&index.to_string())?;
            // The index is topology, not a validity override: CIS2006.emp04 owes
            // the documented 2006 delivery, despite its 2004 reference-year index.
            Ok(spans.to_vec())
        }
        _ => Err("inventory_window_invalid: invalid temporal slot".into()),
    }
}

pub(super) fn held_intervals(
    db: &Connection,
    holding: i64,
    variable: &str,
) -> Result<Vec<Span>, String> {
    let mut query = db
        .prepare(
            "SELECT m.start,m.end FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
      WHERE c.holding_id=? AND m.variable_pid=?",
        )
        .map_err(|e| e.to_string())?;
    query
        .query_map(params![holding, variable], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
fn notice(
    db: &Connection,
    holding: i64,
    column: Option<i64>,
    code: &str,
    spans: Option<Span>,
    detail: (&[Span], &[Span], &[Span]),
    message: &str,
    locator: &str,
) -> Result<(), String> {
    db.execute("INSERT INTO holding_notice(holding_id,column_id,code,start,end,requested,documented,admitted,message,locator)
      VALUES(?,?,?,?,?,?,?,?,?,?)",params![holding,column,code,spans.map(|s|s.0),spans.map(|s|s.1),
      serde_json::to_string(detail.0).map_err(|e|e.to_string())?,serde_json::to_string(detail.1).map_err(|e|e.to_string())?,
      serde_json::to_string(detail.2).map_err(|e|e.to_string())?,message,locator])
      .map_err(|e|e.to_string())?;
    Ok(())
}

#[derive(Clone)]
struct Candidate {
    segment: Segment,
    spans: Vec<Span>,
    carried: bool,
    caveat: Option<String>,
    window: Option<String>,
    locator: String,
}

fn candidates(
    table: &input::Table,
    column: &census::Column,
    decision: Option<&input::Column>,
    owed: &[Span],
    index: &BTreeMap<String, Vec<Segment>>,
) -> Result<Vec<Candidate>, String> {
    let mut out = Vec::new();
    if let Some(decision) = decision
        && !decision.mapping.is_empty()
    {
        for mapping in &decision.mapping {
            if mapping.hold.is_some() {
                continue;
            }
            if mapping.locator.trim().is_empty() {
                return Err("stale_declared_mapping_target: evidence locator required".into());
            }
            let segments = index
                .get(&mapping.table_pid)
                .ok_or("holdings_table_route_unresolved: explicit route absent")?;
            let variable: Vec<_> = segments
                .iter()
                .filter(|s| s.variable == mapping.variable_pid)
                .collect();
            if variable.is_empty() {
                return Err(
                    "stale_declared_mapping_target: variable absent from routed table".into(),
                );
            }
            let wanted = match &mapping.delivery {
                Some(window) => {
                    let wanted = periods(window)?;
                    if !covered(&wanted, owed) {
                        return Err(
                            "inventory_window_invalid: authored window escapes file/slot reach"
                                .into(),
                        );
                    }
                    wanted
                }
                None => owed.to_vec(),
            };
            let documented = merge(
                variable
                    .iter()
                    .filter(|s| {
                        s.state != "ambiguous" && s.timeless == (table.disposition == "timeless")
                    })
                    .map(|s| (s.start, s.end))
                    .collect(),
            );
            if mapping.delivery.is_some() && !covered(&wanted, &documented) {
                return Err("inventory_window_invalid: authored window lacks unambiguous documented coverage".into());
            }
            for segment in variable {
                if segment.timeless != (table.disposition == "timeless") {
                    continue;
                }
                let spans = overlap(&wanted, &[(segment.start, segment.end)]);
                if !spans.is_empty() {
                    out.push(Candidate {
                        segment: segment.clone(),
                        spans,
                        carried: mapping.carried,
                        caveat: mapping.caveat.clone(),
                        window: mapping.delivery.clone(),
                        locator: mapping.locator.clone(),
                    });
                }
            }
        }
    } else {
        let folded = reg_core::fold_identity(&column.match_name);
        for route in &table.routes {
            let segments = index
                .get(route)
                .ok_or("holdings_table_route_unresolved: default route absent")?;
            for segment in segments {
                if segment.timeless != (table.disposition == "timeless")
                    || reg_core::fold_identity(&segment.spelling) != folded
                {
                    continue;
                }
                let spans = overlap(owed, &[(segment.start, segment.end)]);
                if !spans.is_empty() {
                    out.push(Candidate {
                        segment: segment.clone(),
                        spans,
                        carried: false,
                        caveat: None,
                        window: None,
                        locator: table.locator.clone(),
                    });
                }
            }
        }
    }
    Ok(out)
}

/// Emit only uncontested interval slices. Distinct variables remain rival owners;
/// several routes to the same variable are separate valid placements.
#[allow(clippy::too_many_lines)]
fn placements(
    db: &Connection,
    holding: i64,
    column: i64,
    candidates: &[Candidate],
    owed: &[Span],
    locator: &str,
    keys: &[input::Key],
) -> Result<Vec<Segment>, String> {
    let mut by_table = BTreeMap::<&str, Vec<&Candidate>>::new();
    let mut withheld = BTreeSet::new();
    for c in candidates {
        if let Some(key) = keys.iter().find(|k| k.hold.is_some()) {
            withheld.insert(key.locator.as_str());
            for span in &c.spans {
                notice(
                    db,
                    holding,
                    Some(column),
                    "row_key_held",
                    Some(*span),
                    (&[], &[], &[]),
                    key.hold.as_deref().expect("held key"),
                    &key.locator,
                )?;
            }
        } else {
            by_table.entry(&c.segment.table).or_default().push(c);
        }
    }
    if by_table.is_empty()
        && (!withheld.is_empty() || !keys.is_empty() && keys.iter().all(|k| k.hold.is_some()))
    {
        let held: Vec<_> = keys.iter().filter(|k| k.hold.is_some()).collect();
        let reason = held
            .iter()
            .map(|k| k.hold.as_deref().expect("held key"))
            .collect::<Vec<_>>()
            .join("; ");
        let follow_up = held
            .iter()
            .filter_map(|k| k.follow_up.as_deref())
            .collect::<Vec<_>>()
            .join("; ");
        db.execute(
            "UPDATE holding_column SET disposition='hold',reason=?,follow_up=? WHERE id=?",
            params![reason, follow_up, column],
        )
        .map_err(|_| "holdings_row_key_invalid: hold needs a reason and follow-up")?;
    }
    let mut accepted = Vec::new();
    let mut admitted = Vec::new();
    let mut documented = Vec::new();
    for cs in by_table.values() {
        let cuts: BTreeSet<_> = cs
            .iter()
            .flat_map(|c| c.spans.iter().flat_map(|(a, b)| [*a, *b]))
            .collect();
        let cuts: Vec<_> = cuts.into_iter().collect();
        for pair in cuts.windows(2) {
            let span = (pair[0], pair[1]);
            let active: Vec<_> = cs
                .iter()
                .filter(|c| covered(&[span], &c.spans))
                .copied()
                .collect();
            if active.is_empty() {
                continue;
            }
            documented.push(span);
            let owners: BTreeSet<_> = active.iter().map(|c| &c.segment.variable).collect();
            if owners.len() > 1 {
                notice(
                    db,
                    holding,
                    Some(column),
                    "ambiguous_delivered_owners",
                    Some(span),
                    (&[], &[], &[]),
                    "Distinct documented owners claim this physical column over the contested interval",
                    locator,
                )?;
                continue;
            }
            if active.iter().any(|c| c.segment.state == "ambiguous") {
                notice(
                    db,
                    holding,
                    Some(column),
                    "ambiguous_catalog_segment",
                    Some(span),
                    (&[], &[], &[]),
                    "Catalog ambiguity cannot be cleared by a physical owner choice",
                    locator,
                )?;
                continue;
            }
            // Final preference has already been resolved into catalog segments.
            let c = active[0];
            if active.iter().any(|other| {
                other.carried != c.carried || other.caveat != c.caveat || other.window != c.window
            }) {
                return Err(
                    "inventory_cell_conflict: incompatible declarations of the same placement"
                        .into(),
                );
            }
            db.execute("INSERT INTO holding_mapping(column_id,table_pid,variable_pid,start,end,role,carried,caveat,window,locator)
              VALUES(?,?,?,?,?,?,?,?,?,?)",params![column,c.segment.table,c.segment.variable,span.0,span.1,c.segment.role,
              c.carried,c.caveat,c.window,c.locator]).map_err(|_|"inventory_cell_conflict: duplicate placement".to_owned())?;
            if c.segment.state == "preliminary" {
                notice(
                    db,
                    holding,
                    Some(column),
                    "preliminary_data",
                    Some(span),
                    (&[], &[], &[]),
                    "Documented coverage is preliminary",
                    &c.locator,
                )?;
            }
            if let Some(caveat) = &c.caveat {
                notice(
                    db,
                    holding,
                    Some(column),
                    "mapping_caveat",
                    Some(span),
                    (&[], &[], &[]),
                    caveat,
                    &c.locator,
                )?;
            }
            admitted.push(span);
            accepted.push(c.segment.clone());
        }
    }
    let admitted = merge(admitted);
    if admitted.is_empty() {
        notice(
            db,
            holding,
            Some(column),
            "mapping_missing",
            None,
            (owed, &merge(documented), &[]),
            "No accepted semantic placement; retained in the coverage denominator",
            locator,
        )?;
    } else if !covered(owed, &admitted) {
        notice(
            db,
            holding,
            Some(column),
            "period_clipped",
            None,
            (owed, &merge(documented), &admitted),
            "Physical reach intersects documented orderable coverage",
            locator,
        )?;
    }
    Ok(accepted)
}

fn storage_findings(
    db: &Connection,
    holding: i64,
    column: i64,
    storage: &census::Storage,
    segments: &[Segment],
    locator: &str,
) -> Result<(), String> {
    let physical = storage.data_type.to_ascii_lowercase();
    let storage_family = match physical.as_str() {
        "tinyint" | "smallint" | "int" | "bigint" | "bit" => Some("integer"),
        "decimal" | "numeric" | "real" | "float" | "money" | "smallmoney" => Some("real"),
        "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" => Some("text"),
        "date" | "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset" | "time" => {
            Some("date")
        }
        _ => None,
    };
    if storage_family.is_none() {
        return Err("holdings_storage_unsafe: unsupported orderable storage type".into());
    }
    let mut findings = BTreeSet::new();
    for segment in segments {
        let dtype = segment.dtype.as_deref().and_then(reg_frame::dtype_info);
        let family = dtype.and_then(|d| d.family);
        let logical_family = match family {
            Some("integer" | "boolean") => Some("integer"),
            Some("decimal") => Some("real"),
            Some("text" | "identifier") => Some("text"),
            Some("temporal") => Some("date"),
            _ => None,
        };
        if logical_family.is_none() {
            findings.insert("uncurated");
        } else if (logical_family == Some("text") && storage_family != Some("text"))
            || (logical_family == Some("real") && storage_family == Some("integer"))
        {
            findings.insert("narrower");
        } else if logical_family != storage_family {
            findings.insert("disagrees");
        }
        if logical_family == Some("text")
            && storage_family == Some("text")
            && storage
                .length
                .zip(segment.length)
                .is_some_and(|(physical, logical)| physical >= 0 && physical < logical)
        {
            findings.insert("narrower");
        }
    }
    for code in findings {
        notice(
            db,
            holding,
            Some(column),
            code,
            None,
            (&[], &[], &[]),
            "Storage facts need review against adopted catalog typing",
            locator,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn write(
    db: &mut Connection,
    base: &Connection,
    topology: input::Topology,
    mut census: Vec<census::Table>,
) -> Result<(usize, usize, usize, usize), String> {
    let mut index = BTreeMap::<String, Vec<Segment>>::new();
    for semantic in [base, &*db] {
        let mut tables = semantic
            .prepare("SELECT pid FROM tbl")
            .map_err(|e| e.to_string())?;
        for table in tables
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?
        {
            index.entry(table.map_err(|e| e.to_string())?).or_default();
        }
    }
    for segment in segments(base)?.into_iter().chain(segments(db)?) {
        index
            .entry(segment.table.clone())
            .or_default()
            .push(segment);
    }
    let mut authored = BTreeMap::new();
    for table in topology.table {
        if table.locator.trim().is_empty()
            || authored
                .insert(table.physical_name.clone(), table)
                .is_some()
        {
            return Err("holdings_accounting_invalid: duplicate table or missing locator".into());
        }
    }
    census.sort_by(|a, b| a.name.cmp(&b.name));
    let ids: BTreeMap<_, _> = census
        .iter()
        .enumerate()
        .map(|(i, t)| {
            (
                t.name.clone(),
                i64::try_from(i + 1).expect("table count fits i64"),
            )
        })
        .collect();
    if ids.len() != authored.len() || ids.keys().any(|name| !authored.contains_key(name)) {
        return Err("holdings_accounting_invalid: authored tables differ from census".into());
    }
    let count_tables = census.len();
    let tx = db.transaction().map_err(|e| e.to_string())?;
    let mut count_columns = 0usize;
    let mut mapped = 0usize;
    let mut unmatched = 0usize;
    let mut next_column = 1i64;
    // Parents and row-period columns reference rows filled later in this transaction.
    tx.execute_batch("PRAGMA defer_foreign_keys=ON;")
        .map_err(|e| e.to_string())?;
    for table in census {
        let authored = &authored[&table.name];
        let id = ids[&table.name];
        let result = (|| -> Result<(), String> {
            let spans = table_periods(authored)?;
            if authored.partition.is_some() && authored.projection_of.is_some() {
                return Err(
                    "inventory_cell_conflict: partition and projection are exclusive".into(),
                );
            }
            let projection = authored
                .projection_of
                .as_ref()
                .map(|name| {
                    ids.get(name)
                        .copied()
                        .ok_or("holdings_projection_invalid: missing parent")
                })
                .transpose()?;
            if authored.period_column.is_some() != authored.period_encoding.is_some() {
                return Err(
                    "holdings_period_column_invalid: column and encoding must be paired".into(),
                );
            }
            let storages: Vec<_> = table
                .columns
                .iter()
                .flat_map(|c| c.storage.iter())
                .filter(|s| {
                    authored
                        .schema_name
                        .as_ref()
                        .is_none_or(|name| *name == s.schema_name)
                })
                .collect();
            let schema: BTreeSet<_> = storages.iter().map(|s| s.schema_name.as_str()).collect();
            let kinds: BTreeSet<_> = storages.iter().map(|s| s.table_type.as_str()).collect();
            let schema_name = authored
                .schema_name
                .as_deref()
                .or_else(|| (schema.len() == 1).then(|| *schema.first().expect("one schema")));
            let table_type = (kinds.len() == 1).then(|| *kinds.first().expect("one kind"));
            let file_name = reg_resolve::slugify(&table.name).replace('-', "_");
            if file_name.is_empty() {
                return Err("holdings_filename_invalid: empty slug".into());
            }
            tx.execute("INSERT INTO holding_table(id,physical_name,schema_name,table_type,file_name,disposition,delivery,partition,projection_of,
          projection_token,period_encoding,reason,locator) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",params![id,table.name,schema_name,table_type,
          file_name,authored.disposition,authored.delivery.as_deref().map(canonical_period).transpose()?,authored.partition,projection,authored.projection_token,
          Option::<String>::None,authored.reason,authored.locator]).map_err(|_|"holdings_table_invalid: duplicate name or invalid topology".to_owned())?;
            for (start, end) in &spans {
                tx.execute(
                    "INSERT INTO holding_period VALUES(?,?,?)",
                    params![id, start, end],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.execute(
                "UPDATE holding_table SET key_requirements=? WHERE id=?",
                params![
                    serde_json::to_string(&authored.key).map_err(|e| e.to_string())?,
                    id
                ],
            )
            .map_err(|e| e.to_string())?;
            for route in &authored.routes {
                if !index.contains_key(route) {
                    return Err("holdings_table_route_unresolved: default table absent".into());
                }
                tx.execute("INSERT INTO holding_route VALUES(?,?)", params![id, route])
                    .map_err(|_| "holdings_table_route_unresolved: duplicate route".to_owned())?;
            }
            let mut decisions = BTreeMap::new();
            for (position, c) in authored.column.iter().enumerate() {
                if c.locator.trim().is_empty()
                    || decisions.insert(c.name.as_str(), c).is_some()
                    || !table.columns.iter().any(|physical| physical.name == c.name)
                {
                    let routes: Vec<_> = c.mapping.iter().map(|m| m.locator.as_str()).collect();
                    return Err(format!(
                        "stale_declared_mapping_target: topology:column={}: locator={} routes={routes:?}: column decision is stale, duplicate or unlocated",
                        position + 1,
                        c.locator
                    ));
                }
            }
            let mut period_column = None;
            for column in &table.columns {
                let result = (|| -> Result<(), String> {
                    count_columns += 1;
                    let cid = next_column;
                    next_column += 1;
                    let decision = decisions.get(column.name.as_str()).copied();
                    let hold = decision.and_then(|c| c.hold.as_deref());
                    let unmap = decision.and_then(|c| c.unmap.as_deref());
                    if hold
                        .into_iter()
                        .chain(unmap)
                        .any(|reason| reason.trim().is_empty())
                        || hold.is_some()
                            && decision
                                .and_then(|c| c.follow_up.as_deref())
                                .is_none_or(|issue| issue.trim().is_empty())
                    {
                        return Err("holdings_column_disposition_invalid: hold/unmap needs a reason and hold needs a follow-up".into());
                    }
                    if hold.is_some() && unmap.is_some()
                        || ((hold.is_some() || unmap.is_some())
                            && decision.is_some_and(|c| !c.mapping.is_empty()))
                    {
                        return Err(
                            "inventory_cell_conflict: hold/unmap cannot carry mappings".into()
                        );
                    }
                    let locator =
                        decision.map_or(authored.locator.as_str(), |c| c.locator.as_str());
                    let eligible: Vec<_> = column
                        .storage
                        .iter()
                        .filter(|s| schema_name.is_none_or(|schema| schema == s.schema_name))
                        .collect();
                    let storage = (eligible.len() == 1).then(|| eligible[0]);
                    let disposition = if hold.is_some() {
                        "hold"
                    } else if unmap.is_some() {
                        "unmap"
                    } else {
                        "candidate"
                    };
                    tx.execute(
                        "INSERT INTO holding_column VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        params![
                            cid,
                            id,
                            column.name,
                            column.ordinal,
                            storage.map(|s| s.data_type.as_str()),
                            storage.and_then(|s| s.length),
                            storage.and_then(|s| s.precision),
                            storage.and_then(|s| s.scale),
                            decision.and_then(|c| c.slot_kind.as_deref()),
                            decision.and_then(|c| c.slot_index.as_ref().map(ToString::to_string)),
                            disposition,
                            hold.or(unmap),
                            decision.and_then(|c| c.follow_up.as_deref()),
                            locator
                        ],
                    )
                    .map_err(|_| {
                        "holdings_column_invalid: invalid role, slot or hold/unmap metadata"
                            .to_owned()
                    })?;
                    if authored
                        .period_column
                        .as_ref()
                        .is_some_and(|name| *name == column.name)
                    {
                        period_column = Some(cid);
                    }
                    let owed = column_periods(decision, &spans)?;
                    for mapping in decision.into_iter().flat_map(|c| &c.mapping) {
                        if let Some(reason) = &mapping.hold {
                            if reason.trim().is_empty()
                                || mapping
                                    .follow_up
                                    .as_deref()
                                    .is_none_or(|s| s.trim().is_empty())
                                || mapping.locator.trim().is_empty()
                            {
                                return Err("holdings_mapping_hold_invalid: reason, follow-up and locator required".into());
                            }
                            let requested = mapping
                                .delivery
                                .as_deref()
                                .map(periods)
                                .transpose()?
                                .unwrap_or_else(|| owed.clone());
                            notice(
                                &tx,
                                id,
                                Some(cid),
                                "mapping_held",
                                None,
                                (&requested, &[], &[]),
                                &format!(
                                    "{reason} Follow-up: {}",
                                    mapping.follow_up.as_deref().expect("validated follow-up")
                                ),
                                &mapping.locator,
                            )?;
                        } else if mapping.follow_up.is_some() {
                            return Err(
                                "holdings_mapping_hold_invalid: follow-up without hold".into()
                            );
                        }
                    }
                    if hold.is_some() || unmap.is_some() || spans.is_empty() {
                        return Ok(());
                    }
                    let candidates = candidates(authored, column, decision, &owed, &index)?;
                    let accepted =
                        placements(&tx, id, cid, &candidates, &owed, locator, &authored.key)?;
                    if accepted.is_empty() {
                        unmatched += 1;
                        notice(
                            &tx,
                            id,
                            Some(cid),
                            "unmatched",
                            None,
                            (&[], &[], &[]),
                            "Storage column has no accepted semantic owner",
                            locator,
                        )?;
                    } else {
                        if schema_name.is_none() || table_type.is_none() {
                            return Err("holdings_storage_ambiguous: orderable table needs one schema and table type".into());
                        }
                        mapped += 1;
                        let storage = storage.ok_or(if eligible.is_empty() {
                    "holdings_storage_missing: orderable column needs a storage fact"
                } else {
                    "holdings_storage_ambiguous: orderable column needs exactly one storage fact"
                })?;
                        storage_findings(&tx, id, cid, storage, &accepted, locator)?;
                    }
                    Ok(())
                })();
                result.map_err(|error| format!("column={}: {error}", column.ordinal))?;
            }
            if authored.period_column.is_some() && period_column.is_none() {
                return Err(
                    "holdings_period_column_invalid: declared row-period column is absent".into(),
                );
            }
            tx.execute(
                "UPDATE holding_table SET period_column_id=?,period_encoding=? WHERE id=?",
                params![period_column, authored.period_encoding, id],
            )
            .map_err(|e| e.to_string())?;
            Ok(())
        })();
        result.map_err(|error| format!("holdings:table={id}: {error}"))?;
    }
    let copied_keys = {
        let mut query = tx
            .prepare(
                "SELECT m.id,h.projection_of,m.variable_pid,m.start,m.end FROM holding_mapping m
          JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id
          WHERE h.projection_of IS NOT NULL AND EXISTS (SELECT 1 FROM json_each(h.key_requirements) k WHERE EXISTS (SELECT 1 FROM json_each(k.value,'$.entity') e WHERE e.value=m.variable_pid) OR EXISTS (SELECT 1 FROM json_each(k.value,'$.time') e WHERE e.value=m.variable_pid)) ORDER BY m.id",
            )
            .map_err(|e| e.to_string())?;
        query
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?
    };
    for (mapping, parent, variable, start, end) in copied_keys {
        if covered(&[(start, end)], &held_intervals(&tx, parent, &variable)?) {
            tx.execute("UPDATE holding_mapping SET carried=1 WHERE id=?", [mapping])
                .map_err(|e| e.to_string())?;
        }
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok((count_tables, count_columns, mapped, unmatched))
}
