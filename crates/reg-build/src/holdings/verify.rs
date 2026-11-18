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
    slot_family: Option<String>,
    slot_kind: Option<String>,
    slot_index: Option<String>,
}

/// Compare artifact coordinates with the independently re-read, admitted sources.
/// Artifact counters are summaries, never the export oracle.
#[allow(clippy::too_many_lines)]
fn sources(
    db: &Connection,
    primaries: &compile::Primaries,
    starts: &super::cumulative::Starts,
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
    let held = reg_readers::holdings::read(
        &input::path(root, &config.inputs.storage_export)?,
        &input::path(root, &config.inputs.census_reader)?,
    )?;
    let spaces = super::spaces::resolve(root, &config, &held.tables)?;
    let stored: Vec<(i64, String, String)> = rows(
        db,
        "SELECT ord,id,name FROM holding_space ORDER BY ord",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if !stored
        .iter()
        .map(|(ord, id, name)| (*ord, id, name))
        .eq((1i64..)
            .zip(&spaces.list)
            .map(|(ord, (id, name))| (ord, id, name)))
    {
        return Err(
            "config:space: holdings_space_source_mismatch: stored spaces differ from the config"
                .into(),
        );
    }
    let source_tables = held.tables.len();
    let source_columns: usize = held.tables.iter().map(|t| t.columns.len()).sum();
    let keyed = compile::schema_keyed(&held.tables);
    let mut topology: input::Topology =
        crate::toml_file(&input::path(root, &config.inputs.holdings)?)
            .map_err(|_| "holdings_topology_invalid: malformed authored topology")?;
    // The effective topology, as compile resolves the extracts of cumulative tables.
    let mut effective: BTreeMap<compile::Identity, input::Table> = topology
        .table
        .drain(..)
        .map(|t| (compile::authored_identity(&t, keyed), t))
        .collect();
    super::cumulative::resolve(&mut effective, &held.tables, &spaces, starts, primaries)?;
    topology.table = effective.into_values().collect();
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
    let sql = "SELECT physical_name,id,schema_name,table_type,space FROM holding_table";
    #[allow(clippy::type_complexity)]
    let mut tables =
        BTreeMap::<compile::Identity, (i64, Option<String>, Option<String>, String)>::new();
    for (identity, stored) in rows(db, sql, [], |r| {
        let schema: Option<String> = r.get(2)?;
        Ok((
            (schema.clone().filter(|_| keyed), r.get(0)?),
            (r.get(1)?, schema, r.get(3)?, r.get(4)?),
        ))
    })? {
        // The DDL's unique (schema_name, physical_name) admits repeated NULL schemas, and
        // a name-keyed identity ignores the schema: a second row would escape the export.
        let id = stored.0;
        if tables.insert(identity, stored).is_some() {
            return Err(format!("holdings:table={id}: holdings_census_table_extra").into());
        }
    }
    for table in &held.tables {
        let location = format!("storage_export:row={}", table.source_row);
        let (id, schema, kind, space) = tables
            .remove(&compile::physical_identity(table))
            .ok_or_else(|| format!("{location}: holdings_census_table_missing"))?;
        if space != spaces.of(table) {
            return Err(format!("{location}: holdings_space_source_mismatch").into());
        }
        if schema.as_deref() != Some(table.storage_schema.as_str())
            || kind.as_deref() != Some(table.table_type.as_str())
        {
            return Err(format!("{location}: holdings_source_storage_mismatch").into());
        }
        #[allow(clippy::type_complexity)]
        let (
            placement,
            disposition,
            delivery,
            recorded,
            binding,
            parent_in_schema,
            release,
            superseded_by,
        ): (
            (Option<String>, Option<String>, Option<String>),
            String,
            Option<String>,
            String,
            (Option<String>, Option<String>),
            bool,
            Option<String>,
            Option<compile::Identity>,
        ) = db.query_row(
            // A schema-keyed projection's parent is in the projection's own schema.
            "SELECT p.physical_name,h.projection_token,h.partition,h.disposition,h.delivery,
                h.key_requirements,c.name,h.period_encoding,
                p.id IS NULL OR NOT ?2 OR p.schema_name IS h.schema_name,h.release,
                s.schema_name,s.physical_name
             FROM holding_table h LEFT JOIN holding_table p ON p.id=h.projection_of
             LEFT JOIN holding_table s ON s.id=h.superseded_by
             LEFT JOIN holding_column c ON c.id=h.period_column_id WHERE h.id=?1",
            rusqlite::params![id, keyed],
            |r| {
                let schema: Option<String> = r.get(10)?;
                Ok((
                    (r.get(0)?, r.get(1)?, r.get(2)?),
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    (r.get(6)?, r.get(7)?),
                    r.get(8)?,
                    r.get(9)?,
                    r.get::<_, Option<String>>(11)?
                        .map(|name| (schema.filter(|_| keyed), name)),
                ))
            },
        )?;
        let Some((spec, spec_delivery, spec_periods)) =
            authored.get(&compile::physical_identity(table))
        else {
            return Err(format!("{location}: holdings_topology_source_mismatch").into());
        };
        if !parent_in_schema
            || (&spec.projection_of, &spec.projection_token, &spec.partition)
                != (&placement.0, &placement.1, &placement.2)
        {
            return Err(format!("{location}: holdings_topology_source_mismatch").into());
        }
        if (
            &spec.disposition,
            spec_delivery,
            spec_periods,
            &spec.superseded_by,
        ) != (
            &disposition,
            &delivery,
            &keys::periods(db, id)?,
            &superseded_by,
        ) {
            return Err(format!(
                "{location}: holdings_period_source_mismatch: authored table disposition or delivery differs"
            ).into());
        }
        if compile::stated_release(primaries, spec).map_err(|e| format!("{location}: {e}"))?
            != release
        {
            return Err(format!(
                "{location}: holdings_release_source_mismatch: stored release differs from the authored one"
            )
            .into());
        }
        let recorded: Vec<input::RowKey> = serde_json::from_str(&recorded)
            .map_err(|_| format!("{location}: holdings_row_key_invalid"))?;
        if spec.key != recorded {
            return Err(format!("{location}: holdings_row_key_source_mismatch").into());
        }
        if (&spec.period_column, &spec.period_encoding) != (&binding.0, &binding.1) {
            return Err(format!("{location}: holdings_period_source_mismatch").into());
        }
        let mut query = db.prepare(
            "SELECT name,ordinal,storage_type,length,precision,scale,slot_family,slot_kind,slot_index
            FROM holding_column WHERE holding_id=?1",
        )?;
        let mut columns: BTreeMap<String, StoredColumn> = query
            .query_map([id], |r| {
                Ok((
                    r.get(0)?,
                    StoredColumn {
                        ordinal: r.get(1)?,
                        dtype: r.get(2)?,
                        length: r.get(3)?,
                        precision: r.get(4)?,
                        scale: r.get(5)?,
                        slot_family: r.get(6)?,
                        slot_kind: r.get(7)?,
                        slot_index: r.get(8)?,
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
                .get(&(compile::physical_identity(table), column.name.as_str()))
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
            let fact = &column.storage;
            if stored.dtype.as_deref() != Some(fact.data_type.as_str())
                || (stored.length, stored.precision, stored.scale)
                    != (fact.length, fact.precision, fact.scale)
            {
                return Err(format!("{location}: holdings_source_storage_mismatch").into());
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
    Ok((topology, held, declarations))
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
    // Before the source re-read, and independent of the resolver it checks.
    let starts = super::cumulative::starts(&base, &db)?;
    super::cumulative::check(&db, &starts)?;
    let primaries = compile::primaries(&base, &db)?;
    let shared = compile::shared_days(&primaries);
    let (topology, held, declarations) = sources(&db, &primaries, &starts, root)?;
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
    // segment_index seeds from every tbl row, including segmentless tables.
    let tables = compile::segment_index(&base, &db)?;
    let variables = compile::by_variable(&tables);
    super::keys::verify(&db, &base, &variables, &topology)?;
    super::verify_mappings::verify(&db, &tables, &variables, &topology, &held, &primaries)?;
    // A documented position family only authorizes distinct positions in one file.
    // Other duplicates within one space still need a partition, projection, or carried
    // key; the same placement in two spaces is two databases' copies, and on a shared
    // day of its table, in two releases' files, each release's own delivery.
    let mut duplicates = db.prepare(
        "SELECT ha.id,hb.id,a.table_pid,a.variable_pid,max(a.start,b.start),min(a.end,b.end),
        ha.release IS NOT NULL AND hb.release IS NOT NULL AND ha.release!=hb.release FROM holding_mapping a JOIN holding_mapping b ON a.id<b.id AND a.table_pid=b.table_pid AND a.variable_pid=b.variable_pid
      AND a.start<b.end AND b.start<a.end JOIN holding_column ca ON ca.id=a.column_id JOIN holding_column cb ON cb.id=b.column_id
      JOIN holding_table ha ON ha.id=ca.holding_id JOIN holding_table hb ON hb.id=cb.holding_id AND hb.space=ha.space
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
            r.get::<_, bool>(6)?,
        ))
    })? {
        let (left, right, table, variable, start, end, releases) = row?;
        if releases
            && shared
                .get(&table)
                .is_some_and(|shared| days::covered(&[(start, end)], shared))
        {
            continue;
        }
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
