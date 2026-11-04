//! Interval-local ownership and metadata-only physical emission.
use std::collections::{BTreeMap, BTreeSet};

use reg_readers::holdings as census;
use rusqlite::{Connection, params};

use reg_core::days::{self, Span};

use super::input;
use crate::error::{Result, rows};

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

pub(super) fn segments(db: &Connection) -> Result<Vec<Segment>> {
    rows(
        db,
        "SELECT t.pid,v.pid,s.start,s.end,sp.exact,s.role,s.state,s.dtype,s.length,t.timeless
      FROM segment s JOIN tbl t ON t.id=s.table_id JOIN variable v ON v.id=s.variable_id
      JOIN spelling sp ON sp.id=s.spelling_id ORDER BY t.pid,v.pid,s.start",
        [],
        |r| {
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
        },
    )
}

pub(super) fn periods(value: &str) -> Result<Vec<Span>> {
    let mut out = Vec::new();
    for token in value.split(',') {
        let period: reg_core::Period = token
            .trim()
            .parse()
            .map_err(|_| "inventory_window_invalid: invalid period spelling")?;
        let (lo, hi) = period.iso_bounds();
        out.push((crate::emit::start(&lo), crate::emit::end(&hi)));
    }
    Ok(days::merge(out))
}

pub(super) fn canonical_period(value: &str) -> Result<String> {
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

pub(super) fn table_periods(table: &input::Table) -> Result<Vec<Span>> {
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

pub(super) fn column_periods(column: Option<&input::Column>, spans: &[Span]) -> Result<Vec<Span>> {
    let Some(column) = column else {
        return Ok(spans.to_vec());
    };
    if column.slot_family.as_ref().is_some_and(|family| {
        family.trim().is_empty() || column.slot_kind.as_deref() != Some("position")
    }) {
        return Err("holdings_slot_invalid: a documented family requires position slots".into());
    }
    match (column.slot_kind.as_deref(), column.slot_index.as_ref()) {
        (None, None) => Ok(spans.to_vec()),
        (Some("position"), Some(index))
            if index.to_string().parse::<u32>().is_ok_and(|n| n > 0) =>
        {
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

#[allow(clippy::too_many_arguments)]
fn notice(
    db: &Connection,
    holding: i64,
    column: Option<i64>,
    code: &str,
    spans: Option<Span>,
    detail: (&[Span], &[Span], &[Span]),
    message: &str,
) -> Result<i64> {
    db.execute("INSERT INTO holding_notice(holding_id,column_id,code,start,end,requested,documented,admitted,message)
      VALUES(?,?,?,?,?,?,?,?,?)",params![holding,column,code,spans.map(|s|s.0),spans.map(|s|s.1),
      serde_json::to_string(detail.0).map_err(|e|e.to_string())?,serde_json::to_string(detail.1).map_err(|e|e.to_string())?,
      serde_json::to_string(detail.2).map_err(|e|e.to_string())?,message])
      .map_err(|e|e.to_string())?;
    Ok(db.last_insert_rowid())
}

#[derive(Clone)]
pub(super) struct Candidate {
    pub segment: Segment,
    spans: Vec<Span>,
    carried: bool,
    caveat: Option<String>,
    window: Option<String>,
}

pub(super) fn candidates(
    table: &input::Table,
    column: &census::Column,
    decision: Option<&input::Column>,
    owed: &[Span],
    index: &BTreeMap<String, Vec<Segment>>,
) -> Result<Vec<Candidate>> {
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
                    if !days::covered(&wanted, owed) {
                        return Err(
                            "inventory_window_invalid: authored window escapes file/slot reach"
                                .into(),
                        );
                    }
                    wanted
                }
                None => owed.to_vec(),
            };
            let documented = days::merge(
                variable
                    .iter()
                    .filter(|s| {
                        s.state != "ambiguous" && s.timeless == (table.disposition == "timeless")
                    })
                    .map(|s| (s.start, s.end))
                    .collect(),
            );
            if mapping.delivery.is_some() && !days::covered(&wanted, &documented) {
                return Err("inventory_window_invalid: authored window lacks unambiguous documented coverage".into());
            }
            for segment in variable {
                if segment.timeless != (table.disposition == "timeless") {
                    continue;
                }
                let spans = days::overlap(&wanted, &[(segment.start, segment.end)]);
                if !spans.is_empty() {
                    out.push(Candidate {
                        segment: segment.clone(),
                        spans,
                        carried: mapping.carried,
                        caveat: mapping.caveat.clone(),
                        window: mapping.delivery.clone(),
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
                let spans = days::overlap(owed, &[(segment.start, segment.end)]);
                if !spans.is_empty() {
                    out.push(Candidate {
                        segment: segment.clone(),
                        spans,
                        carried: false,
                        caveat: None,
                        window: None,
                    });
                }
            }
        }
    }
    Ok(out)
}

pub(super) enum Placement<'a> {
    Accepted(Span, &'a Candidate),
    /// The rivals left unplaced, as distinct (table, variable) pairs.
    Blocked(
        Span,
        &'static str,
        &'static str,
        BTreeSet<(&'a str, &'a str)>,
    ),
}

/// Apply interval-local ownership for emission.
pub(super) fn plan(candidates: &[Candidate]) -> Result<Vec<Placement<'_>>> {
    let mut by_table = BTreeMap::<&str, Vec<&Candidate>>::new();
    for candidate in candidates {
        by_table
            .entry(&candidate.segment.table)
            .or_default()
            .push(candidate);
    }
    let mut placements = Vec::new();
    for cs in by_table.values() {
        for span in days::slices(cs.iter().flat_map(|c| c.spans.iter().copied())) {
            let active: Vec<_> = cs
                .iter()
                .filter(|c| days::covered(&[span], &c.spans))
                .copied()
                .collect();
            if active.is_empty() {
                continue;
            }
            let rivals: BTreeSet<_> = active
                .iter()
                .map(|c| (c.segment.table.as_str(), c.segment.variable.as_str()))
                .collect();
            if rivals.len() > 1 {
                placements.push(Placement::Blocked(span, "ambiguous_delivered_owners",
                    "Distinct documented owners claim this physical column over the contested interval", rivals));
                continue;
            }
            if active.iter().any(|c| c.segment.state == "ambiguous") {
                placements.push(Placement::Blocked(
                    span,
                    "ambiguous_catalog_segment",
                    "Catalog ambiguity cannot be cleared by a physical owner choice",
                    rivals,
                ));
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
            placements.push(Placement::Accepted(span, c));
        }
    }
    Ok(placements)
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
    keys: &[input::RowKey],
) -> Result<Vec<Segment>> {
    if let Some(key) = keys.iter().find(|key| key.hold.is_some()) {
        db.execute(
            "UPDATE holding_column SET disposition='hold',reason=?,follow_up=? WHERE id=?",
            params![key.hold, key.follow_up, column],
        )
        .map_err(|_| "holdings_row_key_invalid: hold needs a reason and follow-up")?;
        notice(
            db,
            holding,
            Some(column),
            "row_key_held",
            None,
            (owed, &[], &[]),
            key.hold.as_deref().expect("held key"),
        )?;
        return Ok(Vec::new());
    }
    let mut accepted = Vec::new();
    let mut admitted = Vec::new();
    let mut documented = Vec::new();
    for placement in plan(candidates)? {
        let (span, c) = match placement {
            Placement::Accepted(span, c) => (span, c),
            Placement::Blocked(span, code, message, rivals) => {
                documented.push(span);
                let id = notice(
                    db,
                    holding,
                    Some(column),
                    code,
                    Some(span),
                    (&[], &[], &[]),
                    message,
                )?;
                for (table, variable) in rivals {
                    db.execute(
                        "INSERT INTO holding_notice_candidate VALUES(?,?,?)",
                        params![id, table, variable],
                    )?;
                }
                continue;
            }
        };
        documented.push(span);
        db.execute("INSERT INTO holding_mapping(column_id,table_pid,variable_pid,start,end,role,carried,caveat,window)
              VALUES(?,?,?,?,?,?,?,?,?)",params![column,c.segment.table,c.segment.variable,span.0,span.1,c.segment.role,
              c.carried,c.caveat,c.window]).map_err(|_|"inventory_cell_conflict: duplicate placement".to_owned())?;
        if c.segment.state == "preliminary" {
            notice(
                db,
                holding,
                Some(column),
                "preliminary_data",
                Some(span),
                (&[], &[], &[]),
                "Documented coverage is preliminary",
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
            )?;
        }
        admitted.push(span);
        accepted.push(c.segment.clone());
    }
    let admitted = days::merge(admitted);
    if admitted.is_empty() {
        notice(
            db,
            holding,
            Some(column),
            "mapping_missing",
            None,
            (owed, &days::merge(documented), &[]),
            "No accepted semantic placement; retained in the coverage denominator",
        )?;
    } else if !days::covered(owed, &admitted) {
        notice(
            db,
            holding,
            Some(column),
            "period_clipped",
            None,
            (owed, &days::merge(documented), &admitted),
            "Physical reach intersects documented orderable coverage",
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
) -> Result<()> {
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
        )?;
    }
    Ok(())
}

/// Declared routes exist even when their semantic tables have no segments.
pub(super) fn segment_index(
    base: &Connection,
    db: &Connection,
) -> Result<BTreeMap<String, Vec<Segment>>> {
    let mut index = BTreeMap::<String, Vec<Segment>>::new();
    for semantic in [base, db] {
        let mut tables = semantic.prepare("SELECT pid FROM tbl")?;
        for table in tables.query_map([], |row| row.get::<_, String>(0))? {
            index.entry(table?).or_default();
        }
    }
    for segment in segments(base)?.into_iter().chain(segments(db)?) {
        index
            .entry(segment.table.clone())
            .or_default()
            .push(segment);
    }
    Ok(index)
}

#[allow(clippy::too_many_lines)]
pub(super) fn write(
    db: &mut Connection,
    base: &Connection,
    topology: input::Topology,
    mut census: Vec<census::Table>,
) -> Result<(usize, usize, usize, usize)> {
    let index = segment_index(base, db)?;
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
    let tx = db.transaction()?;
    let mut count_columns = 0usize;
    let mut mapped = 0usize;
    let mut unmatched = 0usize;
    let mut next_column = 1i64;
    let mut file_names = BTreeMap::<String, String>::new();
    // Parents and row-period columns reference rows filled later in this transaction.
    tx.execute_batch("PRAGMA defer_foreign_keys=ON;")?;
    for table in census {
        let authored = &authored[&table.name];
        let id = ids[&table.name];
        let result = (|| -> Result<()> {
            super::keys::validate_shapes(id, &authored.key)?;
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
            // The runner's rules (reg_core::manifest), applied at publication so a
            // name it would refuse never reaches an order. Slugs are lowercase, so a
            // case-insensitive collision is an exact one here.
            reg_core::manifest::safe_file_name(&file_name).map_err(|reason| {
                format!(
                    "holdings_filename_invalid: locator={}: file name {file_name:?} is {reason}",
                    authored.locator
                )
            })?;
            if let Some(other) = file_names.insert(file_name.clone(), table.name.clone()) {
                return Err(format!(
                    "holdings_filename_invalid: locator={}: file name {file_name:?} is also physical table {other:?}'s",
                    authored.locator
                ).into());
            }
            tx.execute("INSERT INTO holding_table(id,physical_name,schema_name,table_type,file_name,disposition,delivery,partition,projection_of,
          projection_token,period_encoding,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",params![id,table.name,schema_name,table_type,
          file_name,authored.disposition,authored.delivery.as_deref().map(canonical_period).transpose()?,authored.partition,projection,authored.projection_token,
          Option::<String>::None,authored.reason]).map_err(|_|"holdings_table_invalid: duplicate name or invalid topology".to_owned())?;
            for (start, end) in &spans {
                tx.execute(
                    "INSERT INTO holding_period VALUES(?,?,?)",
                    params![id, start, end],
                )?;
            }
            tx.execute(
                "UPDATE holding_table SET key_requirements=? WHERE id=?",
                params![serde_json::to_string(&authored.key)?, id],
            )?;
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
                    ).into());
                }
            }
            let mut period_column = None;
            for column in &table.columns {
                let result = (|| -> Result<()> {
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
                            decision.and_then(|c| c.slot_family.as_deref()),
                            decision.and_then(|c| c.slot_kind.as_deref()),
                            decision.and_then(|c| c.slot_index.as_ref().map(ToString::to_string)),
                            disposition,
                            hold.or(unmap),
                            decision.and_then(|c| c.follow_up.as_deref()),
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
                    let accepted = placements(&tx, id, cid, &candidates, &owed, &authored.key)?;
                    if accepted.is_empty() && authored.key.iter().any(|key| key.hold.is_some()) {
                        return Ok(());
                    }
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
                        storage_findings(&tx, id, cid, storage, &accepted)?;
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
            )?;
            Ok(())
        })();
        result.map_err(|error| format!("holdings:table={id}: {error}"))?;
    }
    super::keys::carry_projections(&tx)?;
    tx.commit()?;
    Ok((count_tables, count_columns, mapped, unmatched))
}
