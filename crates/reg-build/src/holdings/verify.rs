//! Sole publication validator for the catalog/holdings pair.
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_core::days;
use rusqlite::Connection;

use super::{compile, input, keys, meta, open, references};
use crate::error::{Result, rows};

struct StoredColumn {
    ordinal: i64,
    dtype: Option<String>,
    length: Option<i64>,
    precision: Option<i64>,
    scale: Option<i64>,
    offered: bool,
    slot_family: Option<String>,
    slot_kind: Option<String>,
    slot_index: Option<String>,
}

/// Compare artifact coordinates with the independently re-read, admitted sources.
/// Artifact counters are summaries, never the census oracle.
#[allow(clippy::too_many_lines)]
fn sources(
    db: &Connection,
    root: &Path,
) -> Result<(
    input::Topology,
    reg_readers::holdings::ReadOutput,
    BTreeSet<references::Occurrence>,
)> {
    let pin = input::Pin {
        repo: meta(db, "steward_repo")?,
        commit: meta(db, "steward_commit")?,
        config: meta(db, "steward_config")?,
    };
    let config = input::admit(root, &pin, &meta(db, "owner_id")?)?;
    let declarations = references::read(root, &config)?;
    if references::keys(&declarations) != references::stored(db)? {
        return Err("holdings_reference_occurrence_source_mismatch".into());
    }
    if meta(db, "input_manifest_sha256")? != input::manifest_digest(&config.pins) {
        return Err(
            "holdings_source_pin_mismatch: input manifest differs from pinned config".into(),
        );
    }
    let census = reg_readers::holdings::read(
        &input::path(root, &config.inputs.census)?,
        &input::path(root, &config.inputs.storage_export)?,
        &input::path(root, &config.inputs.census_reader)?,
    )?;
    let source_tables = census.tables.len();
    let source_columns: usize = census.tables.iter().map(|t| t.columns.len()).sum();
    let keyed = compile::schema_keyed(&census.tables);
    let topology: input::Topology = crate::toml_file(&input::path(root, &config.inputs.holdings)?)
        .map_err(|_| "holdings_topology_invalid: malformed authored topology")?;
    // Each authored table with its delivery and coverage, parsed for every table before
    // any is compared.
    let authored: BTreeMap<compile::Identity, _> = topology
        .table
        .iter()
        .map(|table| {
            let delivery = table
                .delivery
                .as_deref()
                .map(compile::canonical_period)
                .transpose()?;
            Ok((
                compile::authored_identity(table, keyed),
                (table, delivery, compile::table_periods(table)?),
            ))
        })
        .collect::<Result<_, String>>()?;
    let authored_columns: BTreeMap<_, _> = topology
        .table
        .iter()
        .flat_map(|t| {
            t.column
                .iter()
                .map(move |c| ((compile::authored_identity(t, keyed), c.name.as_str()), c))
        })
        .collect();
    let sql = "SELECT physical_name,id,schema_name,table_type FROM holding_table";
    let mut tables = BTreeMap::<compile::Identity, (i64, Option<String>, Option<String>)>::new();
    for (identity, stored) in rows(db, sql, [], |r| {
        let schema: Option<String> = r.get(2)?;
        Ok((
            (schema.clone().filter(|_| keyed), r.get(0)?),
            (r.get(1)?, schema, r.get(3)?),
        ))
    })? {
        // The DDL's unique (schema_name, physical_name) admits repeated NULL schemas, and
        // a name-keyed identity ignores the schema: a second row would escape the census.
        let id = stored.0;
        if tables.insert(identity, stored).is_some() {
            return Err(format!("holdings:table={id}: holdings_census_table_extra").into());
        }
    }
    for table in &census.tables {
        let location = format!("census:row={}", table.source_row);
        let (id, schema, kind) = tables
            .remove(&compile::census_identity(table))
            .ok_or_else(|| format!("{location}: holdings_census_table_missing"))?;
        #[allow(clippy::type_complexity)]
        let (placement, disposition, delivery, recorded, binding, parent_in_schema): (
            (Option<String>, Option<String>, Option<String>),
            String,
            Option<String>,
            String,
            (Option<String>, Option<String>),
            bool,
        ) = db.query_row(
            // A schema-keyed projection's parent is in the projection's own schema.
            "SELECT p.physical_name,h.projection_token,h.partition,h.disposition,h.delivery,
                h.key_requirements,c.name,h.period_encoding,
                p.id IS NULL OR NOT ?2 OR p.schema_name IS h.schema_name
             FROM holding_table h LEFT JOIN holding_table p ON p.id=h.projection_of
             LEFT JOIN holding_column c ON c.id=h.period_column_id WHERE h.id=?1",
            rusqlite::params![id, keyed],
            |r| {
                Ok((
                    (r.get(0)?, r.get(1)?, r.get(2)?),
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    (r.get(6)?, r.get(7)?),
                    r.get(8)?,
                ))
            },
        )?;
        let Some((spec, spec_delivery, spec_periods)) =
            authored.get(&compile::census_identity(table))
        else {
            return Err(format!("{location}: holdings_topology_source_mismatch").into());
        };
        if !parent_in_schema
            || (&spec.projection_of, &spec.projection_token, &spec.partition)
                != (&placement.0, &placement.1, &placement.2)
        {
            return Err(format!("{location}: holdings_topology_source_mismatch").into());
        }
        if (&spec.disposition, spec_delivery, spec_periods)
            != (&disposition, &delivery, &keys::periods(db, id)?)
        {
            return Err(format!(
                "{location}: holdings_period_source_mismatch: authored table disposition or delivery differs"
            ).into());
        }
        let recorded: Vec<input::RowKey> = serde_json::from_str(&recorded)
            .map_err(|_| format!("{location}: holdings_row_key_invalid"))?;
        if spec.key != recorded {
            return Err(format!("{location}: holdings_row_key_source_mismatch").into());
        }
        if (&spec.period_column, &spec.period_encoding) != (&binding.0, &binding.1) {
            return Err(format!("{location}: holdings_period_source_mismatch").into());
        }
        let period_keys: Vec<_> = recorded
            .iter()
            .filter(|key| key.hold.is_none() && key.period_key.as_deref() == Some("column"))
            .collect();
        let mut period_offered = false;
        if !period_keys.is_empty() {
            let spans: Vec<(i64, i64)> = rows(
                db,
                "SELECT m.start,m.end FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id WHERE c.holding_id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            for key in period_keys {
                let scope = key.period.as_deref().map(compile::periods).transpose()?;
                if scope.as_ref().map_or(!spans.is_empty(), |scope| {
                    !days::overlap(&spans, scope).is_empty()
                }) {
                    period_offered = true;
                    break;
                }
            }
        }
        let mut query = db
            .prepare(
                "SELECT c.name,c.ordinal,c.storage_type,c.length,c.precision,c.scale,
            EXISTS(SELECT 1 FROM holding_mapping m WHERE m.column_id=c.id)
              OR (COALESCE(c.id=(SELECT period_column_id FROM holding_table WHERE id=c.holding_id),0) AND ?2)
            ,c.slot_family,c.slot_kind,c.slot_index
            FROM holding_column c WHERE c.holding_id=?1",
            )?;
        let mut columns: BTreeMap<String, StoredColumn> = query
            .query_map(rusqlite::params![id, period_offered], |r| {
                Ok((
                    r.get(0)?,
                    StoredColumn {
                        ordinal: r.get(1)?,
                        dtype: r.get(2)?,
                        length: r.get(3)?,
                        precision: r.get(4)?,
                        scale: r.get(5)?,
                        offered: r.get(6)?,
                        slot_family: r.get(7)?,
                        slot_kind: r.get(8)?,
                        slot_index: r.get(9)?,
                    },
                ))
            })?
            .collect::<Result<_, _>>()?;
        for column in &table.columns {
            let location = format!("{location}:column={}", column.ordinal);
            let stored = columns
                .remove(&column.name)
                .ok_or_else(|| format!("{location}: holdings_census_column_missing"))?;
            if stored.ordinal != column.ordinal {
                return Err(format!("{location}: holdings_census_ordinal_mismatch").into());
            }
            let authored = authored_columns
                .get(&(compile::census_identity(table), column.name.as_str()))
                .copied();
            compile::column_periods(authored, &[])
                .map_err(|error| format!("{location}: {error}"))?;
            if stored.slot_family.as_deref() != authored.and_then(|c| c.slot_family.as_deref())
                || stored.slot_kind.as_deref() != authored.and_then(|c| c.slot_kind.as_deref())
                || stored.slot_index
                    != authored.and_then(|c| c.slot_index.as_ref().map(ToString::to_string))
            {
                return Err(format!("{location}: holdings_slot_source_mismatch").into());
            }
            if stored.offered {
                let declared_schema = spec.schema_name.as_deref();
                let facts: Vec<_> = column
                    .storage
                    .iter()
                    .filter(|s| declared_schema.is_none_or(|name| name == s.schema_name))
                    .collect();
                let [fact] = facts.as_slice() else {
                    return Err(format!(
                        "{location}: holdings_source_storage_ambiguous: offered column needs exactly one source fact"
                    ).into());
                };
                if schema.as_deref() != Some(fact.schema_name.as_str())
                    || kind.as_deref() != Some(fact.table_type.as_str())
                    || stored.dtype.as_deref() != Some(fact.data_type.as_str())
                    || (stored.length, stored.precision, stored.scale)
                        != (fact.length, fact.precision, fact.scale)
                {
                    return Err(format!("{location}: holdings_source_storage_mismatch").into());
                }
            }
        }
        if let Some((_, stored)) = columns.first_key_value() {
            let ordinal = stored.ordinal;
            return Err(format!(
                "holdings:table={id}:column={ordinal}: holdings_census_column_extra"
            )
            .into());
        }
    }
    if let Some((_, (id, ..))) = tables.first_key_value() {
        return Err(format!("holdings:table={id}: holdings_census_table_extra").into());
    }
    for (key, count) in [
        ("census_tables", source_tables),
        ("census_columns", source_columns),
    ] {
        if meta(db, key)? != count.to_string() {
            return Err(format!("census:{key}: holdings_source_accounting_mismatch").into());
        }
    }
    Ok((topology, census, declarations))
}

pub(super) fn distinct(
    holdings: &Connection,
    base: &Connection,
    declarations: &BTreeSet<references::Occurrence>,
) -> Result<()> {
    let mut allowed = references::admitted(holdings, base, declarations)?;
    allowed.extend(references::overlays(holdings, base)?);
    {
        let sql = "SELECT pid FROM ident WHERE kind!='provider' UNION SELECT pid FROM var_set UNION SELECT pid FROM ident_retired";
        let ids: BTreeSet<String> = rows(base, sql, [], |r| r.get(0))?.into_iter().collect();
        let mut query = holdings.prepare(sql)?;
        for id in query.query_map([], |r| r.get::<_, String>(0))? {
            let id = id?;
            if ids.contains(&id) && !allowed.contains(&id) {
                return Err("holdings_semantic_collision: supplemental public id overwrites reference entity".into());
            }
        }
    }
    Ok(())
}

fn no_rows(db: &Connection, sql: &str, code: &str) -> Result<()> {
    let mut stmt = db.prepare(sql)?;
    if stmt.query([])?.next()?.is_some() {
        return Err(code.into());
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn pair(path: &Path, catalog: &Path, root: &Path) -> Result<()> {
    crate::verify::catalog(catalog, None)?;
    let base = open(catalog)?;
    let db = open(path)?;
    let user: u32 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
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
    // Before the source re-read: a leak refuses even when the steward checkout is gone.
    super::scope::check(&db)?;
    let (topology, census, declarations) = sources(&db, root)?;
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
        "input_manifest_sha256",
    ] {
        let value = meta(&db, key)?;
        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("holdings_meta_invalid: invalid {key}").into());
        }
    }
    crate::verify::semantic(&db)?;
    distinct(&db, &base, &declarations)?;
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
    let mut table_ids = BTreeSet::new();
    for conn in [&base, &db] {
        table_ids.extend(rows(conn, "SELECT pid FROM tbl", [], |r| {
            r.get::<_, String>(0)
        })?);
    }
    let mut query = db.prepare("SELECT table_pid FROM holding_route")?;
    for route in query.query_map([], |r| r.get::<_, String>(0))? {
        if !table_ids.contains(&route?) {
            return Err("holdings_table_route_unresolved".into());
        }
    }
    let mut query=db.prepare("SELECT m.table_pid,m.variable_pid,m.start,m.end,m.role,m.carried,h.id,h.disposition,m.window,m.caveat
      FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id ORDER BY m.id")
      .map_err(|e|e.to_string())?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let table: String = row.get(0)?;
        let variable: String = row.get(1)?;
        let start: i64 = row.get(2)?;
        let end: i64 = row.get(3)?;
        let role: String = row.get(4)?;
        let holding: i64 = row.get(6)?;
        let disposition: String = row.get(7)?;
        let window: Option<String> = row.get(8)?;
        let caveat: Option<String> = row.get(9)?;
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
        if !days::covered(&[(start, end)], &admitted) {
            return Err("inventory_window_invalid: mapping escapes unambiguous semantic coverage or declared role".into());
        }
        let periods = keys::periods(&db, holding)?;
        if !days::covered(&[(start, end)], &periods)
            || caveat.is_some_and(|c| c.trim().is_empty())
            || window
                .map(|w| compile::periods(&w).map(|w| !days::covered(&[(start, end)], &w)))
                .transpose()?
                .unwrap_or(false)
        {
            return Err(
                "inventory_window_invalid: mapping escapes file/window or has a blank caveat"
                    .into(),
            );
        }
    }
    super::keys::verify(&db, &base, &topology)?;
    super::verify_mappings::verify(&db, &base, &topology, &census)?;
    // A documented position family only authorizes distinct positions in one file.
    // Other duplicates still need a partition, projection, or carried key.
    let mut duplicates = db.prepare(
        "SELECT ha.id,hb.id,a.table_pid,a.variable_pid,max(a.start,b.start),min(a.end,b.end) FROM holding_mapping a JOIN holding_mapping b ON a.id<b.id AND a.table_pid=b.table_pid AND a.variable_pid=b.variable_pid
      AND a.start<b.end AND b.start<a.end JOIN holding_column ca ON ca.id=a.column_id JOIN holding_column cb ON cb.id=b.column_id
      JOIN holding_table ha ON ha.id=ca.holding_id JOIN holding_table hb ON hb.id=cb.holding_id
      WHERE a.carried=0 AND b.carried=0 AND NOT(ha.id!=hb.id AND ha.partition IS NOT NULL AND hb.partition IS NOT NULL AND ha.partition!=hb.partition)
      AND ha.projection_of IS NOT hb.id AND hb.projection_of IS NOT ha.id
      AND NOT(ha.id=hb.id AND ca.slot_family IS NOT NULL AND cb.slot_family IS NOT NULL
        AND ca.slot_family=cb.slot_family AND ca.slot_kind='position' AND cb.slot_kind='position'
        AND CAST(ca.slot_index AS INTEGER)!=CAST(cb.slot_index AS INTEGER))"
    )?;
    let mut conflicts = BTreeSet::new();
    for row in duplicates.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, i64>(5)?,
        ))
    })? {
        let (left, right, table, variable, start, end) = row?;
        conflicts.insert(format!("holdings:tables={left},{right}:route={table}:variable={variable}:period={start}..{end}: inventory_cell_conflict: undeclared duplicate physical placement"));
    }
    if !conflicts.is_empty() {
        return Err(conflicts.into_iter().collect::<Vec<_>>().join("\n").into());
    }
    Ok(())
}

fn external_relations(db: &Connection, base: &Connection) -> Result<()> {
    let mut query = db.prepare(
        "SELECT r.subject_kind,r.subject_pid,r.object_kind,r.object_pid FROM holding_relation r
      JOIN statement s ON s.id=r.statement_id WHERE s.kind='link' AND s.evidence_text IS NOT NULL",
    )?;
    let mut rows = query.query([])?;
    let mut count = 0i64;
    while let Some(row) = rows.next()? {
        for (origin, kind, pid) in [
            (db, row.get::<_, String>(0), row.get::<_, String>(1)),
            (base, row.get::<_, String>(2), row.get::<_, String>(3)),
        ] {
            let (kind, pid) = (kind?, pid?);
            let table = match kind.as_str() {
                "register" => "register",
                "table" => "tbl",
                "variable" => "variable",
                _ => return Err("holdings_relation_invalid: endpoint kind".into()),
            };
            let exists: bool = origin.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE pid=?)"),
                [pid],
                |r| r.get(0),
            )?;
            if !exists {
                return Err("holdings_relation_invalid: unresolved qualified endpoint".into());
            }
        }
        count += 1;
    }
    let total: i64 = db.query_row("SELECT count(*) FROM holding_relation", [], |r| r.get(0))?;
    if count != total {
        return Err("holdings_relation_invalid: ordinary link evidence missing".into());
    }
    no_rows(db,"SELECT 1 FROM holding_relation a JOIN holding_relation b ON a.id<b.id AND a.subject_kind=b.subject_kind
      AND a.subject_pid=b.subject_pid AND a.object_kind=b.object_kind AND a.object_pid=b.object_pid
      WHERE a.at_start IS NULL OR b.at_start IS NULL OR (a.at_start<b.at_end AND b.at_start<a.at_end)",
      "holdings_relation_invalid: overlapping curated derivations")
}
