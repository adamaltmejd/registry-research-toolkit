//! Sole publication validator for the catalog/holdings pair.
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rusqlite::Connection;

use super::{compile, meta, open};

pub(super) fn distinct(holdings: &Connection, base: &Connection) -> Result<(), String> {
    {
        let sql = "SELECT pid FROM ident WHERE kind!='provider' UNION SELECT pid FROM var_set UNION SELECT pid FROM ident_retired";
        let mut query = base.prepare(sql).map_err(|e| e.to_string())?;
        let ids: BTreeSet<String> = query
            .query_map([], |r| r.get(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let mut query = holdings.prepare(sql).map_err(|e| e.to_string())?;
        for id in query
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?
        {
            if ids.contains(&id.map_err(|e| e.to_string())?) {
                return Err("holdings_semantic_collision: supplemental public id overwrites reference entity".into());
            }
        }
    }
    Ok(())
}

fn no_rows(db: &Connection, sql: &str, code: &str) -> Result<(), String> {
    let mut stmt = db.prepare(sql).map_err(|e| e.to_string())?;
    if stmt
        .query([])
        .map_err(|e| e.to_string())?
        .next()
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err(code.into());
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn pair(path: &Path, catalog: &Path) -> Result<(), String> {
    crate::verify::catalog(catalog, None)?;
    let base = open(catalog)?;
    let db = open(path)?;
    let user: u32 = db
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if user != reg_core::HOLDINGS_SCHEMA_VERSION
        || meta(&db, "schema_version")? != reg_core::HOLDINGS_SCHEMA_VERSION.to_string()
        || meta(&db, "semantic_schema_version")? != reg_core::CATALOG_SCHEMA_VERSION.to_string()
    {
        return Err(
            "holdings_schema_mismatch: exact physical and semantic schemas required".into(),
        );
    }
    if meta(&db, "catalog_generation")? != meta(&base, "generation_id")?
        || meta(&db, "catalog_sha256")? != crate::sha256_file(catalog)?.0
    {
        return Err("holdings_catalog_mismatch: catalog generation or file digest differs".into());
    }
    if meta(&db, "owner_kind")? != "steward" || !reg_core::is_slug(&meta(&db, "owner_id")?) {
        return Err("holdings_owner_invalid".into());
    }
    for key in ["steward", "scope"] {
        if !serde_json::from_str::<serde_json::Value>(&meta(&db, key)?)
            .map_err(|_| "holdings_presentation_invalid")?
            .is_object()
        {
            return Err("holdings_presentation_invalid: expected a JSON object".into());
        }
    }
    if meta(&db, "logo")?.trim().is_empty() {
        return Err("holdings_presentation_invalid: missing logo".into());
    }
    for key in [
        "generation_id",
        "semantic_generation",
        "semantic_sha256",
        "compiler_digest",
    ] {
        let value = meta(&db, key)?;
        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("holdings_meta_invalid: invalid {key}"));
        }
    }
    crate::verify::semantic(&db)?;
    distinct(&db, &base)?;
    external_relations(&db, &base)?;
    no_rows(
        &db,
        "PRAGMA foreign_key_check",
        "holdings_reference_invalid: foreign key",
    )?;
    for (sql, code) in [
        (
            "SELECT 1 FROM holding_table h WHERE (h.disposition IN ('dated','timeless')) != EXISTS(SELECT 1 FROM holding_period p WHERE p.holding_id=h.id)",
            "holdings_accounting_invalid: coverage disposition",
        ),
        (
            "SELECT 1 FROM holding_period a JOIN holding_period b ON a.holding_id=b.holding_id AND a.start<b.start AND a.end>b.start",
            "holdings_period_invalid: overlapping intervals",
        ),
        (
            "SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id WHERE h.disposition NOT IN ('dated','timeless') OR c.disposition!='candidate'",
            "holdings_mapping_invalid: unavailable disposition",
        ),
        (
            "SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id WHERE c.storage_type IS NULL",
            "holdings_storage_missing: orderable column lacks storage",
        ),
        (
            "SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id WHERE h.schema_name IS NULL OR trim(h.schema_name)='' OR h.table_type NOT IN ('BASE TABLE','VIEW') OR h.table_type IS NULL",
            "holdings_storage_ambiguous: orderable table needs one schema and table type",
        ),
        (
            "SELECT 1 FROM holding_column WHERE (disposition IN ('hold','unmap') AND trim(reason)='') OR (disposition='hold' AND trim(follow_up)='')",
            "holdings_column_disposition_invalid: blank reason or follow-up",
        ),
        (
            "SELECT 1 FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id WHERE lower(c.storage_type) NOT IN ('tinyint','smallint','int','bigint','bit','decimal','numeric','real','float','money','smallmoney','char','nchar','varchar','nvarchar','text','ntext','date','datetime','datetime2','smalldatetime','datetimeoffset','time')",
            "holdings_storage_unsafe: unsupported orderable storage type",
        ),
        (
            "SELECT 1 FROM holding_table h JOIN holding_column c ON c.id=h.period_column_id WHERE c.holding_id!=h.id",
            "holdings_period_column_invalid: foreign column",
        ),
        (
            "SELECT 1 FROM holding_table h JOIN holding_column c ON c.id=h.period_column_id WHERE c.storage_type IS NULL AND EXISTS(SELECT 1 FROM holding_mapping m JOIN holding_column mc ON mc.id=m.column_id WHERE mc.holding_id=h.id)",
            "holdings_storage_missing: row-period column needs unambiguous storage facts",
        ),
        (
            "SELECT 1 FROM holding_table h JOIN holding_table p ON p.id=h.projection_of WHERE h.id=p.id OR p.projection_of IS NOT NULL",
            "holdings_projection_invalid: parent must not be a projection",
        ),
        (
            "SELECT 1 FROM holding_column c WHERE c.disposition='candidate' AND EXISTS(SELECT 1 FROM holding_period p WHERE p.holding_id=c.holding_id) AND NOT EXISTS(SELECT 1 FROM holding_mapping m WHERE m.column_id=c.id) AND NOT EXISTS(SELECT 1 FROM holding_notice n WHERE n.column_id=c.id AND n.code='mapping_missing')",
            "holdings_accounting_invalid: unaccounted unmatched column",
        ),
        (
            "SELECT 1 FROM holding_mapping a JOIN holding_mapping b ON a.column_id=b.column_id AND a.table_pid=b.table_pid AND a.id<b.id AND a.start<b.end AND b.start<a.end",
            "inventory_cell_conflict: competing owners or duplicate placements",
        ),
    ] {
        no_rows(&db, sql, code)?;
    }
    for (meta_key, table) in [
        ("census_tables", "holding_table"),
        ("census_columns", "holding_column"),
    ] {
        let count: i64 = db
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        if meta(&db, meta_key)? != count.to_string() {
            return Err("holdings_accounting_invalid: census counts differ".into());
        }
    }
    let mut index = BTreeMap::<(String, String), Vec<compile::Segment>>::new();
    for segment in compile::segments(&base)?
        .into_iter()
        .chain(compile::segments(&db)?)
    {
        index
            .entry((segment.table.clone(), segment.variable.clone()))
            .or_default()
            .push(segment);
    }
    let mut row_keys = BTreeMap::<&str, Vec<&compile::Segment>>::new();
    for segments in index.values() {
        for key in segments.iter().filter(|s| s.role == "row_key") {
            row_keys.entry(&key.table).or_default().push(key);
        }
    }
    let mut held_keys = BTreeMap::<(i64, String, String), Vec<(i64, i64)>>::new();
    let mut keys = db
        .prepare(
            "SELECT c.holding_id,m.table_pid,m.variable_pid,m.start,m.end
      FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id WHERE m.role='row_key'",
        )
        .map_err(|e| e.to_string())?;
    let mut keys = keys.query([]).map_err(|e| e.to_string())?;
    while let Some(key) = keys.next().map_err(|e| e.to_string())? {
        held_keys
            .entry((
                key.get(0).map_err(|e| e.to_string())?,
                key.get(1).map_err(|e| e.to_string())?,
                key.get(2).map_err(|e| e.to_string())?,
            ))
            .or_default()
            .push((
                key.get(3).map_err(|e| e.to_string())?,
                key.get(4).map_err(|e| e.to_string())?,
            ));
    }
    let mut table_ids = BTreeSet::new();
    for conn in [&base, &db] {
        let mut query = conn
            .prepare("SELECT pid FROM tbl")
            .map_err(|e| e.to_string())?;
        table_ids.extend(
            query
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?,
        );
    }
    let mut query = db
        .prepare("SELECT table_pid FROM holding_route")
        .map_err(|e| e.to_string())?;
    for route in query
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
    {
        if !table_ids.contains(&route.map_err(|e| e.to_string())?) {
            return Err("holdings_table_route_unresolved".into());
        }
    }
    let mut query=db.prepare("SELECT m.table_pid,m.variable_pid,m.start,m.end,m.role,m.carried,h.id,h.disposition,m.window,m.caveat,m.locator
      FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id ORDER BY m.id")
      .map_err(|e|e.to_string())?;
    let mut rows = query.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let table: String = row.get(0).map_err(|e| e.to_string())?;
        let variable: String = row.get(1).map_err(|e| e.to_string())?;
        let start: i64 = row.get(2).map_err(|e| e.to_string())?;
        let end: i64 = row.get(3).map_err(|e| e.to_string())?;
        let role: String = row.get(4).map_err(|e| e.to_string())?;
        let carried: bool = row.get(5).map_err(|e| e.to_string())?;
        let holding: i64 = row.get(6).map_err(|e| e.to_string())?;
        let disposition: String = row.get(7).map_err(|e| e.to_string())?;
        let window: Option<String> = row.get(8).map_err(|e| e.to_string())?;
        let caveat: Option<String> = row.get(9).map_err(|e| e.to_string())?;
        let locator: String = row.get(10).map_err(|e| e.to_string())?;
        let candidates = index
            .get(&(table.clone(), variable))
            .ok_or("stale_declared_mapping_target")?;
        let admitted: Vec<_> = candidates
            .iter()
            .filter(|s| {
                s.state != "ambiguous"
                    && s.role == role
                    && s.timeless == (disposition == "timeless")
            })
            .map(|s| (s.start, s.end))
            .collect();
        if !compile::covered(&[(start, end)], &admitted) || carried && role != "row_key" {
            return Err("inventory_window_invalid: mapping escapes unambiguous semantic coverage or key role".into());
        }
        let mut periods = db
            .prepare("SELECT start,end FROM holding_period WHERE holding_id=?")
            .map_err(|e| e.to_string())?;
        let periods = periods
            .query_map([holding], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        if !compile::covered(&[(start, end)], &periods)
            || locator.trim().is_empty()
            || caveat.is_some_and(|c| c.trim().is_empty())
            || window
                .map(|w| compile::periods(&w).map(|w| !compile::covered(&[(start, end)], &w)))
                .transpose()?
                .unwrap_or(false)
        {
            return Err(
                "inventory_window_invalid: mapping escapes file/window or lacks evidence".into(),
            );
        }
        for key in row_keys.get(table.as_str()).into_iter().flatten() {
            let required = (start.max(key.start), end.min(key.end));
            if required.0 >= required.1 {
                continue;
            }
            let held = held_keys.get(&(holding, table.clone(), key.variable.clone()));
            if held.is_none_or(|held| !compile::covered(&[required], held)) {
                return Err(
                    "holdings_key_missing: orderable placement lacks its required held row key"
                        .into(),
                );
            }
        }
    }
    // Same logical cell in two columns needs an explicit partition, a projection,
    // or a carried key. Shared spelling and labels alone never authorize possession.
    no_rows(&db,"SELECT 1 FROM holding_mapping a JOIN holding_mapping b ON a.id<b.id AND a.table_pid=b.table_pid AND a.variable_pid=b.variable_pid
      AND a.start<b.end AND b.start<a.end JOIN holding_column ca ON ca.id=a.column_id JOIN holding_column cb ON cb.id=b.column_id
      JOIN holding_table ha ON ha.id=ca.holding_id JOIN holding_table hb ON hb.id=cb.holding_id
      WHERE a.carried=0 AND b.carried=0 AND NOT(ha.id!=hb.id AND ha.partition IS NOT NULL AND hb.partition IS NOT NULL AND ha.partition!=hb.partition)
      AND ha.projection_of IS NOT hb.id AND hb.projection_of IS NOT ha.id",
      "inventory_cell_conflict: undeclared duplicate physical placement")?;
    let mut children=db.prepare("SELECT h.projection_of,m.variable_pid,m.start,m.end FROM holding_mapping m
      JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id WHERE h.projection_of IS NOT NULL")
        .map_err(|e|e.to_string())?;
    let mut children = children.query([]).map_err(|e| e.to_string())?;
    while let Some(child) = children.next().map_err(|e| e.to_string())? {
        let parent: i64 = child.get(0).map_err(|e| e.to_string())?;
        let variable: String = child.get(1).map_err(|e| e.to_string())?;
        let required = (
            child.get(2).map_err(|e| e.to_string())?,
            child.get(3).map_err(|e| e.to_string())?,
        );
        let admitted = compile::held_intervals(&db, parent, &variable)?;
        if !compile::covered(&[required], &admitted) {
            return Err(
                "holdings_projection_invalid: parent lacks projected variable/window".into(),
            );
        }
    }
    Ok(())
}

fn external_relations(db: &Connection, base: &Connection) -> Result<(), String> {
    let mut query = db
        .prepare(
            "SELECT r.subject_kind,r.subject_pid,r.object_kind,r.object_pid FROM holding_relation r
      JOIN statement s ON s.id=r.statement_id WHERE s.kind='link' AND s.evidence_text IS NOT NULL",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = query.query([]).map_err(|e| e.to_string())?;
    let mut count = 0i64;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        for (origin, kind, pid) in [
            (db, row.get::<_, String>(0), row.get::<_, String>(1)),
            (base, row.get::<_, String>(2), row.get::<_, String>(3)),
        ] {
            let (kind, pid) = (
                kind.map_err(|e| e.to_string())?,
                pid.map_err(|e| e.to_string())?,
            );
            let table = match kind.as_str() {
                "register" => "register",
                "table" => "tbl",
                "variable" => "variable",
                _ => return Err("holdings_relation_invalid: endpoint kind".into()),
            };
            let exists: bool = origin
                .query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE pid=?)"),
                    [pid],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())?;
            if !exists {
                return Err("holdings_relation_invalid: unresolved qualified endpoint".into());
            }
        }
        count += 1;
    }
    let total: i64 = db
        .query_row("SELECT count(*) FROM holding_relation", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if count != total {
        return Err("holdings_relation_invalid: ordinary link evidence missing".into());
    }
    no_rows(db,"SELECT 1 FROM holding_relation a JOIN holding_relation b ON a.id<b.id AND a.subject_kind=b.subject_kind
      AND a.subject_pid=b.subject_pid AND a.object_kind=b.object_kind AND a.object_pid=b.object_pid
      WHERE a.at_start IS NULL OR b.at_start IS NULL OR (a.at_start<b.at_end AND b.at_start<a.at_end)",
      "holdings_relation_invalid: overlapping curated derivations")
}
