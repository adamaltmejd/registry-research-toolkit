//! Interval-local ownership and metadata-only physical emission.
use std::collections::{BTreeMap, BTreeSet};

use reg_readers::holdings as census;
use rusqlite::{Connection, params};

use reg_core::days::{self, Span};
use reg_core::manifest::StorageFamily;
use reg_core::project::ColumnType;

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
    /// The primary release it belongs to in a table with concurrent releases, by name.
    pub release: Option<String>,
}

pub(super) fn segments(db: &Connection) -> Result<Vec<Segment>> {
    rows(
        db,
        "SELECT t.pid,v.pid,s.start,s.end,sp.exact,s.role,s.state,s.dtype,s.length,t.timeless,n.body
      FROM segment s JOIN tbl t ON t.id=s.table_id JOIN variable v ON v.id=s.variable_id
      JOIN spelling sp ON sp.id=s.spelling_id LEFT JOIN release r ON r.id=s.release_id
      LEFT JOIN text n ON n.id=r.name_text ORDER BY t.pid,v.pid,s.start,n.body",
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
                release: r.get(10)?,
            })
        },
    )
}

/// The ISO bounds of each period of a comma-separated list.
fn bounds(value: &str) -> Result<Vec<(String, String)>> {
    value
        .split(',')
        .map(|token| {
            token
                .trim()
                .parse::<reg_core::Period>()
                .map(reg_core::Period::iso_bounds)
                .map_err(|_| "inventory_window_invalid: invalid period spelling".into())
        })
        .collect()
}

pub(super) fn periods(value: &str) -> Result<Vec<Span>> {
    let spans = bounds(value)?
        .iter()
        .map(|(lo, hi)| (crate::emit::start(lo), crate::emit::end(hi)))
        .collect();
    Ok(days::merge(spans))
}

pub(super) fn canonical_period(value: &str) -> Result<String> {
    Ok(reg_core::render(&reg_core::merge(bounds(value)?)))
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

/// Per table, its placed primary releases (catalog name and days), each table's read
/// on its origin as the order check reads them: the base catalog's, or the holdings
/// database's for a table only it defines.
pub(super) type Primaries = BTreeMap<String, Vec<(String, Span)>>;

pub(super) fn primaries(base: &Connection, db: &Connection) -> Result<Primaries> {
    let sql = "SELECT t.pid,n.body,r.start,r.end FROM release r JOIN tbl t ON t.id=r.table_id
      JOIN text n ON n.id=r.name_text WHERE r.is_primary=1 AND r.start IS NOT NULL";
    let read = |conn: &Connection| {
        rows(conn, sql, [], |r| {
            Ok((r.get::<_, String>(0)?, (r.get(1)?, (r.get(2)?, r.get(3)?))))
        })
    };
    let based: BTreeSet<String> = rows(base, "SELECT pid FROM tbl", [], |r| r.get(0))?
        .into_iter()
        .collect();
    let mut out = Primaries::new();
    for (table, release) in read(base)?.into_iter().chain(
        read(db)?
            .into_iter()
            .filter(|(table, _)| !based.contains(table)),
    ) {
        out.entry(table).or_default().push(release);
    }
    Ok(out)
}

/// Per table, the days two or more of its placed primary releases share
/// (crates/DESIGN.md, "Concurrent releases"): an order serves them per named release.
pub(super) fn shared_days(primaries: &Primaries) -> BTreeMap<String, Vec<Span>> {
    primaries
        .iter()
        .filter_map(|(table, releases)| {
            let spans: Vec<Span> = releases.iter().map(|(_, span)| *span).collect();
            let shared: Vec<Span> = days::slices(spans.iter().copied())
                .into_iter()
                .filter(|slice| {
                    spans
                        .iter()
                        .filter(|s| days::covered(&[*slice], &[**s]))
                        .count()
                        > 1
                })
                .collect();
            (!shared.is_empty()).then(|| (table.clone(), days::merge(shared)))
        })
        .collect()
}

/// The catalog name of the release a physical table states (`release`), resolved
/// among the placed primary releases of its routed tables as an order resolves a
/// source's `releases`: exactly, else under the identity fold. Only a dated table
/// states one, and its delivery lies inside that release's period.
pub(super) fn stated_release(
    primaries: &Primaries,
    table: &input::Table,
) -> Result<Option<String>> {
    let Some(name) = &table.release else {
        return Ok(None);
    };
    let refuse = |why: &str| -> crate::error::Error {
        format!("holdings_release_invalid: release {name:?} {why}").into()
    };
    if table.disposition != "dated" {
        return Err(refuse("is stated by a table that is not dated"));
    }
    let routed: Vec<&(String, Span)> = table
        .routes
        .iter()
        .filter_map(|route| primaries.get(route))
        .flatten()
        .collect();
    let folded = reg_core::fold_identity(name);
    let exact: Vec<_> = routed.iter().filter(|(body, _)| body == name).collect();
    let matched = if exact.is_empty() {
        routed
            .iter()
            .filter(|(body, _)| reg_core::fold_identity(body) == folded)
            .collect()
    } else {
        exact
    };
    let names: BTreeSet<&str> = matched.iter().map(|(body, _)| body.as_str()).collect();
    let [body] = names.into_iter().collect::<Vec<_>>()[..] else {
        return Err(refuse("names no one primary release of a routed table"));
    };
    let delivery = table_periods(table)?;
    if !matched
        .iter()
        .any(|(_, span)| days::covered(&delivery, &[*span]))
    {
        return Err(refuse("does not cover the table's delivery"));
    }
    Ok(Some(body.to_owned()))
}

/// The days a physical table may place of the segments it meets (crates/DESIGN.md,
/// "Concurrent releases"): the days each serves an order under the shared release
/// shadowing rule (`reg_core::days::shadow`), with the table's release as the lane. A
/// table that states a release is that release's file, so it places that release's
/// segments and releaseless ones where they do not reach, never another release's. One
/// that states none places as on an unshared day; an order's entry of a release is not
/// served from it ([`stated_release`]).
pub(super) struct Reach<'a> {
    /// The release's catalog name ([`stated_release`]).
    pub release: Option<String>,
    pub variables: &'a Variables<'a>,
}

impl Reach<'_> {
    /// `wanted`'s days of `segment` the table may place.
    pub(super) fn spans(&self, segment: &Segment, wanted: &[Span]) -> Vec<Span> {
        let siblings: Vec<&Segment> = self
            .variables
            .get(segment.variable.as_str())
            .into_iter()
            .flatten()
            .filter(|s| s.table == segment.table)
            .copied()
            .collect();
        let at = siblings
            .iter()
            .position(|s| std::ptr::eq(*s, segment))
            .expect("a segment is among its variable's segments");
        let lines: Vec<(Span, Option<&str>)> = siblings
            .iter()
            .map(|s| ((s.start, s.end), s.release.as_deref()))
            .collect();
        let served = days::shadow(&lines, self.release.as_deref()).swap_remove(at);
        days::overlap(wanted, &served)
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
    variables: &Variables<'_>,
    reach: &Reach<'_>,
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
            let variable = mapping_segments(index, variables, mapping)?;
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
            // Documented as the file's release reads it: another release's segment on a
            // shared day documents nothing for this file.
            let documented = days::merge(
                variable
                    .iter()
                    .filter(|s| {
                        s.state != "ambiguous" && s.timeless == (table.disposition == "timeless")
                    })
                    .flat_map(|s| reach.spans(s, &wanted))
                    .collect(),
            );
            if mapping.delivery.is_some() && !days::covered(&wanted, &documented) {
                return Err("inventory_window_invalid: authored window lacks unambiguous documented coverage".into());
            }
            for segment in variable {
                if segment.timeless != (table.disposition == "timeless") {
                    continue;
                }
                let spans = reach.spans(segment, &wanted);
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
        let target = Target::of(&column.match_name);
        for route in &table.routes {
            let segments = index
                .get(route)
                .ok_or("holdings_table_route_unresolved: default route absent")?;
            for segment in segments {
                if segment.timeless != (table.disposition == "timeless") || !target.matches(segment)
                {
                    continue;
                }
                let spans = reach.spans(segment, owed);
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
    let Some(storage_family) = StorageFamily::of(&storage.data_type) else {
        return Err("holdings_storage_unsafe: unsupported orderable storage type".into());
    };
    let mut findings = BTreeSet::new();
    for segment in segments {
        // The published name: the family of the catalog type the order override uses too.
        let logical_family = segment
            .dtype
            .as_deref()
            .and_then(reg_frame::dtype_info)
            .and_then(|d| d.published)
            .and_then(ColumnType::parse)
            .and_then(ColumnType::storage_family);
        let Some(logical_family) = logical_family else {
            findings.insert("uncurated");
            continue;
        };
        if (logical_family == StorageFamily::Text && storage_family != StorageFamily::Text)
            || (logical_family == StorageFamily::Real && storage_family == StorageFamily::Integer)
        {
            findings.insert("narrower");
        } else if logical_family != storage_family {
            findings.insert("disagrees");
        }
        if logical_family == StorageFamily::Text
            && storage_family == StorageFamily::Text
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

/// Every table's segments of each variable, in table order.
pub(super) type Variables<'a> = BTreeMap<&'a str, Vec<&'a Segment>>;

pub(super) fn by_variable(index: &BTreeMap<String, Vec<Segment>>) -> Variables<'_> {
    let mut variables = Variables::new();
    for segment in index.values().flatten() {
        variables
            .entry(segment.variable.as_str())
            .or_default()
            .push(segment);
    }
    variables
}

/// The segments an explicit column mapping places from: its variable's in the table it
/// names or, when it names none (a steward-derived table), in every table that
/// documents the variable. Compile and verify both read placements through this.
pub(super) fn mapping_segments<'a>(
    index: &'a BTreeMap<String, Vec<Segment>>,
    variables: &Variables<'a>,
    mapping: &input::Mapping,
) -> Result<Vec<&'a Segment>> {
    let Some(table) = &mapping.table_pid else {
        return variables
            .get(mapping.variable_pid.as_str())
            .cloned()
            .ok_or_else(|| {
                "stale_declared_mapping_target: variable is documented in no table".into()
            });
    };
    let segments: Vec<_> = index
        .get(table)
        .ok_or("holdings_table_route_unresolved: explicit route absent")?
        .iter()
        .filter(|s| s.variable == mapping.variable_pid)
        .collect();
    if segments.is_empty() {
        return Err("stale_declared_mapping_target: variable absent from routed table".into());
    }
    Ok(segments)
}

/// A census column's match target, folded once: a spelling or a variable FQID.
pub(super) enum Target<'a> {
    Spelling(String),
    Variable(&'a str),
}

impl<'a> Target<'a> {
    pub(super) fn of(target: &'a census::Match) -> Self {
        match target {
            census::Match::Spelling(name) => Self::Spelling(reg_core::fold_identity(name)),
            census::Match::Variable(fqid) => Self::Variable(fqid),
        }
    }

    pub(super) fn matches(&self, segment: &Segment) -> bool {
        match self {
            Self::Spelling(folded) => reg_core::fold_identity(&segment.spelling) == *folded,
            Self::Variable(fqid) => segment.variable == *fqid,
        }
    }
}

/// Refuse a `[[rename]]` whose target matches no segment anywhere in the catalog or
/// semantic view: it could never match, so it is a typo or a stale entry.
pub(super) fn renames_resolve(
    index: &BTreeMap<String, Vec<Segment>>,
    targets: &[census::Match],
) -> Result<()> {
    let segments = || index.values().flatten();
    let spellings: BTreeSet<_> = segments()
        .map(|s| reg_core::fold_identity(&s.spelling))
        .collect();
    let variables: BTreeSet<_> = segments().map(|s| s.variable.as_str()).collect();
    match (1..).zip(targets).find(|(_, t)| match t {
        census::Match::Spelling(name) => !spellings.contains(&reg_core::fold_identity(name)),
        census::Match::Variable(fqid) => !variables.contains(fqid.as_str()),
    }) {
        Some((n, _)) => Err(format!(
            "census_rename_unresolved: rename {n}: target matches no documented spelling or variable"
        )
        .into()),
        None => Ok(()),
    }
}

/// A physical table's identity: (schema, name) when the census reader is schema-keyed,
/// the name alone (schema `None`) otherwise.
pub(super) type Identity = (Option<String>, String);

/// Whether the census reader keyed its tables by schema.
pub(super) fn schema_keyed(census: &[census::Table]) -> bool {
    census.iter().any(|t| t.schema.is_some())
}

pub(super) fn census_identity(table: &census::Table) -> Identity {
    (table.schema.clone(), table.name.clone())
}

/// An authored table's identity. Without a schema-keyed census `schema_name` only selects
/// storage facts and is no part of the identity.
pub(super) fn authored_identity(table: &input::Table, keyed: bool) -> Identity {
    (
        table.schema_name.clone().filter(|_| keyed),
        table.physical_name.clone(),
    )
}

#[allow(clippy::too_many_lines)]
pub(super) fn write(
    db: &mut Connection,
    base: &Connection,
    topology: input::Topology,
    mut census: Vec<census::Table>,
    rename_targets: &[census::Match],
    spaces: &super::spaces::Spaces,
) -> Result<(usize, usize, usize, usize)> {
    let index = segment_index(base, db)?;
    let variables = by_variable(&index);
    let primaries = primaries(base, db)?;
    renames_resolve(&index, rename_targets)?;
    let keyed = schema_keyed(&census);
    let mut authored = BTreeMap::new();
    for table in topology.table {
        if table.locator.trim().is_empty()
            || authored
                .insert(authored_identity(&table, keyed), table)
                .is_some()
        {
            return Err("holdings_accounting_invalid: duplicate table or missing locator".into());
        }
    }
    census.sort_by_key(census_identity);
    let ids: BTreeMap<_, _> = census
        .iter()
        .enumerate()
        .map(|(i, t)| {
            (
                census_identity(t),
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
    for (ord, (space, name)) in (1i64..).zip(&spaces.list) {
        tx.execute(
            "INSERT INTO holding_space VALUES(?,?,?)",
            params![space, name, ord],
        )?;
    }
    for table in census {
        let identity = census_identity(&table);
        let authored = &authored[&identity];
        let id = ids[&identity];
        let result = (|| -> Result<()> {
            super::keys::validate_shapes(id, &authored.key)?;
            let spans = table_periods(authored)?;
            let release = stated_release(&primaries, authored)?;
            let reach = Reach {
                release: release.clone(),
                variables: &variables,
            };
            if authored.partition.is_some() && authored.projection_of.is_some() {
                return Err(
                    "inventory_cell_conflict: partition and projection are exclusive".into(),
                );
            }
            let projection = authored
                .projection_of
                .as_ref()
                .map(|name| {
                    // A projection's parent is in the projection's own schema.
                    ids.get(&(table.schema.clone(), name.clone()))
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
            let file_name = reg_resolve::slugify(&match &table.schema {
                Some(schema) => format!("{schema}_{}", table.name),
                None => table.name.clone(),
            })
            .replace('-', "_");
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
            // The period column and its encoding are set together once the columns exist.
            tx.execute("INSERT INTO holding_table(id,physical_name,space,key_requirements,schema_name,table_type,file_name,disposition,delivery,release,partition,projection_of,
          projection_token,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",params![id,table.name,spaces.of(&table),serde_json::to_string(&authored.key)?,schema_name,table_type,
          file_name,authored.disposition,authored.delivery.as_deref().map(canonical_period).transpose()?,release,authored.partition,projection,authored.projection_token,
          authored.reason]).map_err(|_|"holdings_table_invalid: duplicate name or invalid topology".to_owned())?;
            for (start, end) in &spans {
                tx.execute(
                    "INSERT INTO holding_period VALUES(?,?,?)",
                    params![id, start, end],
                )?;
            }
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
                    let candidates = candidates(
                        authored, column, decision, &owed, &index, &variables, &reach,
                    )?;
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
