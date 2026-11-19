//! Pair admission and database-origin selection for steward metadata. Row ids never
//! cross the two database boundaries; physical placements use public ids.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use reg_core::days::Span;
use reg_core::hex;
use reg_core::issue::Period;

use crate::{Catalog, Code, Error, connect, read_meta};

/// SQL for how readers name the physical table `h`: its name, qualified as
/// `<schema>.<name>` only when another held table has the same name (holdings keyed by
/// schema), so single-schema holders read their names unchanged.
pub(crate) const TABLE_LABEL: &str = "CASE WHEN EXISTS(SELECT 1 FROM holding_table o \
     WHERE o.physical_name = h.physical_name AND o.id <> h.id) \
     THEN ifnull(h.schema_name || '.', '') || h.physical_name ELSE h.physical_name END";

pub(crate) struct Holdings {
    path: PathBuf,
    meta: BTreeMap<String, String>,
    possession: Possession,
    /// The linkage spaces, in the steward's order.
    spaces: Vec<Space>,
}

/// What a steward deployment serves: the public ids an accepted placement names, and
/// the registers and providers above them, each with the years held (placements
/// merged, in order) (maintainer 2026-11-04: everything shown is held, everything held
/// is shown). Read once at admission; the pair is immutable while served.
#[derive(Default)]
pub(crate) struct Possession {
    variables: HashMap<String, Vec<Span>>,
    tables: HashMap<String, Vec<Span>>,
    registers: HashMap<String, Vec<Span>>,
    providers: HashMap<String, Vec<Span>>,
    /// (variable, table) pairs a placement names: a variable held in one table is
    /// not held in another table the steward holds through other columns.
    pairs: HashSet<(String, String)>,
    /// The sets with two or more held members. A set of one held member is not a
    /// group to the steward's reader, so that member reads as a plain variable
    /// (maintainer 2026-11-11, #2410).
    sets: HashSet<String>,
    /// Shown though nothing placed holds them: the tables only blocked files route to,
    /// and the registers and providers above them, with the years those files hold.
    blocked: Blocked,
    /// Each catalog table's blocked files, whether or not a placement also holds it.
    not_orderable: HashMap<String, Vec<NotOrderable>>,
    /// (variable, table) pairs the catalog documents in a table only blocked files route
    /// to: the variables a request may name there although no placement holds them.
    blocked_pairs: HashSet<(String, String)>,
}

#[derive(Default)]
struct Blocked {
    tables: HashMap<String, Vec<Span>>,
    registers: HashMap<String, Vec<Span>>,
    providers: HashMap<String, Vec<Span>>,
}

/// A file the data holder holds that no order can draw on yet, and why.
#[derive(Clone, Serialize, ToSchema)]
pub struct NotOrderable {
    /// The held file, as `segments` names files.
    pub file: String,
    /// `row_key_held`, `table_unknown` or `no_orderable_column`.
    pub reason: String,
    /// The years the file holds; absent when the data holder states none
    /// (`table_unknown`).
    // Filled from `days` by `Server::not_orderable`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub spans: Vec<Period>,
    #[serde(skip)]
    days: Vec<Span>,
    /// What the reason means.
    pub message: &'static str,
    /// The data holder's own notes on each held row key, as written.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub holds: Vec<Hold>,
}

/// A data holder's note on a held row key, and where it follows the hold up.
#[derive(Clone, Serialize, ToSchema)]
pub struct Hold {
    pub hold: String,
    #[schema(required = true)]
    pub follow_up: Option<String>,
}

impl NotOrderable {
    fn new(file: String, reason: &str, keys: &str, days: Vec<Span>) -> Self {
        let message = match reason {
            "row_key_held" => {
                "Held by the data holder, but not orderable yet: the columns that identify \
                 its rows are on hold. Ask the data holder."
            }
            "table_unknown" => {
                "Held by the data holder, but not orderable yet: the data holder has not \
                 stated which years the file covers. Ask the data holder."
            }
            _ => {
                "Held by the data holder, but not orderable yet: none of its columns can be \
                 ordered. Ask the data holder."
            }
        };
        let holds = serde_json::from_str::<Vec<serde_json::Value>>(keys)
            .unwrap_or_default()
            .iter()
            .filter_map(|key| {
                Some(Hold {
                    hold: key["hold"].as_str()?.to_owned(),
                    follow_up: key["follow_up"].as_str().map(str::to_owned),
                })
            })
            .collect();
        Self {
            file,
            reason: reason.to_owned(),
            spans: Vec::new(),
            days,
            message,
            holds,
        }
    }

    /// This file with its years as result periods.
    pub(crate) fn dated(&self, period: impl Fn(i64, i64) -> Period) -> Self {
        Self {
            spans: self
                .days
                .iter()
                .map(|&(start, end)| period(start, end))
                .collect(),
            ..self.clone()
        }
    }
}

impl Possession {
    fn read(conn: &Connection) -> Result<Self, Error> {
        // A placement or a route names its table by public id, in either origin.
        let mut above: HashMap<String, (String, String)> = HashMap::new();
        for origin in ["main", "held"] {
            let mut query = conn.prepare(&format!(
                "SELECT DISTINCT t.pid, r.pid, p.pid FROM
                   (SELECT table_pid FROM held.holding_mapping
                    UNION SELECT table_pid FROM held.holding_route) m
                   JOIN {origin}.tbl t ON t.pid = m.table_pid
                   JOIN {origin}.register r ON r.id = t.register_id
                   JOIN {origin}.provider p ON p.id = r.provider_id"
            ))?;
            for row in query.query_map([], |r| Ok((r.get(0)?, (r.get(1)?, r.get(2)?))))? {
                let (table, parents) = row?;
                above.insert(table, parents);
            }
        }
        let mut possession = Self::default();
        let mut query = conn.prepare(
            "SELECT variable_pid, table_pid, start, end FROM held.holding_mapping
             ORDER BY start, end",
        )?;
        let placements = query.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                (r.get(2)?, r.get(3)?),
            ))
        })?;
        for placement in placements {
            let (variable, table, span) = placement?;
            if let Some((register, provider)) = above.get(&table) {
                possession
                    .registers
                    .entry(register.clone())
                    .or_default()
                    .push(span);
                possession
                    .providers
                    .entry(provider.clone())
                    .or_default()
                    .push(span);
            }
            possession.pairs.insert((variable.clone(), table.clone()));
            possession.variables.entry(variable).or_default().push(span);
            possession.tables.entry(table).or_default().push(span);
        }
        for map in [
            &mut possession.variables,
            &mut possession.tables,
            &mut possession.registers,
            &mut possession.providers,
        ] {
            for spans in map.values_mut() {
                *spans = reg_core::days::merge(std::mem::take(spans));
            }
        }
        // A set's members, by public id, from either origin: a pid both define keeps
        // the union of their members.
        let mut members: HashMap<String, HashSet<String>> = HashMap::new();
        for origin in ["main", "held"] {
            let mut query = conn.prepare(&format!(
                "SELECT s.pid, v.pid FROM {origin}.var_set s
                   JOIN {origin}.var_set_member m ON m.set_id = s.id
                   JOIN {origin}.variable v ON v.id = m.variable_id
                 WHERE s.kind = 'set'"
            ))?;
            for row in query.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))? {
                let (set, variable): (String, String) = row?;
                if possession.variables.contains_key(&variable) {
                    members.entry(set).or_default().insert(variable);
                }
            }
        }
        possession.sets = members
            .into_iter()
            .filter(|(_, held)| held.len() >= 2)
            .map(|(set, _)| set)
            .collect();
        possession.read_blocked(conn, &above)?;
        Ok(possession)
    }

    /// Every catalog table a blocked file routes to, with its blocked files and the years
    /// each holds (crates/DESIGN.md, "Held tables that cannot be ordered"); and the tables
    /// only blocked files route to, with those years rolled up to the registers and
    /// providers nothing placed holds.
    fn read_blocked(
        &mut self,
        conn: &Connection,
        above: &HashMap<String, (String, String)>,
    ) -> Result<(), Error> {
        let mut query = conn.prepare(&format!(
            "SELECT r.table_pid, {TABLE_LABEL}, b.reason, h.key_requirements, h.id
             FROM held.holding_blocked b
               JOIN held.holding_table h ON h.id = b.holding_id
               JOIN held.holding_route r ON r.holding_id = h.id
             ORDER BY r.table_pid, 2"
        ))?;
        let mut periods = conn.prepare(
            "SELECT start, end FROM held.holding_period WHERE holding_id = ? ORDER BY start, end",
        )?;
        let found = query
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (table, file, reason, keys, holding) in found {
            let spans = reg_core::days::merge(
                periods
                    .query_map([holding], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<rusqlite::Result<Vec<Span>>>()?,
            );
            // A table a placement holds is served over its placements; its blocked
            // files are listed, not rolled up.
            if !self.tables.contains_key(&table) {
                let (register, provider) = above.get(&table).ok_or_else(|| {
                    Error::internal(format!("blocked route {table} names no table"))
                })?;
                for (map, pid) in [
                    (&mut self.blocked.tables, &table),
                    (&mut self.blocked.registers, register),
                    (&mut self.blocked.providers, provider),
                ] {
                    map.entry(pid.clone()).or_default().extend(&spans);
                }
            }
            self.not_orderable
                .entry(table)
                .or_default()
                .push(NotOrderable::new(file, &reason, &keys, spans));
        }
        for map in [
            &mut self.blocked.tables,
            &mut self.blocked.registers,
            &mut self.blocked.providers,
        ] {
            // A register or provider some placement holds is served, not blocked.
            for spans in map.values_mut() {
                *spans = reg_core::days::merge(std::mem::take(spans));
            }
        }
        let (registers, providers) = (&self.registers, &self.providers);
        self.blocked
            .registers
            .retain(|pid, _| !registers.contains_key(pid));
        self.blocked
            .providers
            .retain(|pid, _| !providers.contains_key(pid));
        let tables = serde_json::to_string(&self.blocked.tables.keys().collect::<Vec<_>>())
            .expect("pids serialize");
        for origin in ["main", "held"] {
            let mut query = conn.prepare(&format!(
                "SELECT DISTINCT v.pid, t.pid FROM {origin}.segment s
                   JOIN {origin}.variable v ON v.id = s.variable_id
                   JOIN {origin}.tbl t ON t.id = s.table_id
                 WHERE t.pid IN (SELECT value FROM json_each(?))"
            ))?;
            for pair in query.query_map([&tables], |r| Ok((r.get(0)?, r.get(1)?)))? {
                self.blocked_pairs.insert(pair?);
            }
        }
        Ok(())
    }

    fn of(&self, kind: &str) -> Option<&HashMap<String, Vec<Span>>> {
        match kind {
            "provider" => Some(&self.providers),
            "register" => Some(&self.registers),
            "table" => Some(&self.tables),
            "variable" => Some(&self.variables),
            _ => None,
        }
    }
}

impl Blocked {
    fn of(&self, kind: &str) -> Option<&HashMap<String, Vec<Span>>> {
        match kind {
            "provider" => Some(&self.providers),
            "register" => Some(&self.registers),
            "table" => Some(&self.tables),
            _ => None,
        }
    }
}

/// The data holder and the versions of its data.
#[derive(Clone, Serialize, ToSchema)]
pub struct Steward {
    pub owner: String,
    pub catalog_generation: String,
    pub holdings_generation: String,
    pub schema_version: String,
}

/// One of the data holder's databases: its tables share a person key.
#[derive(Clone, Serialize, ToSchema)]
pub struct Space {
    /// Stable slug.
    pub id: String,
    /// The name a researcher picks it by.
    pub name: String,
}

/// The data holder's name, logo and description.
#[derive(ToSchema)]
#[expect(
    dead_code,
    reason = "schema only; the served value is the admitted JSON"
)]
pub struct StewardConfig {
    presentation: StewardPresentation,
    /// The logo's SVG markup.
    logo: String,
    /// The databases whose tables share a person key, in the steward's order: one
    /// for a steward that declares none. An order draws from one.
    spaces: Vec<Space>,
}

#[derive(Deserialize, ToSchema)]
#[expect(dead_code, reason = "deserialized only to check the admitted JSON")]
pub struct StewardPresentation {
    id: String,
    name: String,
    long_name: Option<String>,
    /// Inline markdown: emphasis and `[text](https://…)` links.
    description: Option<String>,
    /// How a researcher hands in an order.
    order_instructions: Option<String>,
}

fn file_digest(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 65_536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex(&hash.finalize()))
}

impl Catalog {
    /// Admit the reference catalog and, for a non-global selection, its pinned
    /// `reg_meta_<steward>.db` partner. The owner comes from startup configuration.
    /// `catalog_sha256`, when given, is the base catalog's SHA-256 computed when the
    /// image was built; the partner's pin is checked against it instead of hashing the
    /// file at boot (`reg_webapp/DESIGN.md`, "Deployment").
    ///
    /// # Errors
    /// `invalid_parameter`, `db_not_found` or `schema_incompatible` before serving.
    pub fn open_selected(
        dir: &Path,
        selected: &str,
        mmap_size: i64,
        catalog_sha256: Option<&str>,
    ) -> Result<Self, Error> {
        let mut catalog = Self::open(dir, mmap_size)?;
        if selected == "global" {
            return Ok(catalog);
        }
        if !reg_core::is_slug(selected) {
            return Err(Error::invalid_parameter("--catalog"));
        }
        let path = dir.join(format!("reg_meta_{selected}.db"));
        if !path.is_file() {
            return Err(Error::new(
                Code::DbNotFound,
                "Selected holdings database is missing.",
                vec![],
            ));
        }
        let refuse = |reason: &str| {
            Error::new(
                Code::SchemaIncompatible,
                format!("Holdings pair is inadmissible: {reason}."),
                vec![
                    serde_json::Value::Null,
                    reg_core::HOLDINGS_SCHEMA_VERSION.to_string().into(),
                ],
            )
        };
        let meta =
            read_meta(&path, mmap_size).map_err(|_| refuse("unreadable holdings manifest"))?;
        let physical_schema: u32 = connect(&path, mmap_size)
            .and_then(|conn| conn.query_row("PRAGMA user_version", [], |r| r.get(0)))
            .map_err(|_| refuse("unreadable holdings schema"))?;
        if physical_schema != reg_core::HOLDINGS_SCHEMA_VERSION {
            return Err(refuse("physical schema version does not match"));
        }
        let expected_schema = reg_core::HOLDINGS_SCHEMA_VERSION.to_string();
        let semantic_schema = reg_core::CATALOG_SCHEMA_VERSION.to_string();
        for (key, expected) in [
            ("schema_version", expected_schema.as_str()),
            ("semantic_schema_version", semantic_schema.as_str()),
            ("owner_kind", "steward"),
            ("owner_id", selected),
            ("catalog_generation", catalog.meta("generation_id")),
        ] {
            if meta.get(key).map(String::as_str) != Some(expected) {
                return Err(refuse(&format!("{key} does not match")));
            }
        }
        let generation = meta
            .get("generation_id")
            .ok_or_else(|| refuse("missing generation_id"))?;
        if !reg_core::is_sha256_hex(generation) {
            return Err(refuse("malformed generation_id"));
        }
        if meta
            .get("steward")
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            // Serde also reads a struct from a positional array; the served JSON must be an object.
            .filter(serde_json::Value::is_object)
            .and_then(|value| serde_json::from_value::<StewardPresentation>(value).ok())
            .is_none()
        {
            return Err(refuse(
                "steward must be a JSON object with string id and name",
            ));
        }
        if meta
            .get("scope")
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .is_none_or(|value| !value.is_object())
        {
            return Err(refuse("scope must be a JSON object"));
        }
        if meta.get("logo").is_none_or(|value| value.trim().is_empty()) {
            return Err(refuse("missing logo"));
        }
        let digest = match catalog_sha256 {
            Some(digest) => digest.to_owned(),
            None => file_digest(&catalog.path).map_err(|_| refuse("catalog cannot be hashed"))?,
        };
        if meta.get("catalog_sha256") != Some(&digest) {
            return Err(refuse("catalog file digest does not match"));
        }
        let mut pair = Sha256::new();
        pair.update(catalog.meta("generation_id").as_bytes());
        pair.update(generation.as_bytes());
        catalog.generation = hex(&pair.finalize());
        catalog.holdings = Some(Holdings {
            path,
            meta,
            possession: Possession::default(),
            spaces: Vec::new(),
        });
        catalog.admit_identities()?;
        catalog
            .admit_possession()
            .map_err(|_| refuse("holdings placements or spaces are unreadable"))?;
        Ok(catalog)
    }

    fn admit_possession(&mut self) -> Result<(), Error> {
        let conn = self.connect()?;
        self.attach_holdings(&conn)?;
        let possession = Possession::read(&conn)?;
        let mut query = conn.prepare("SELECT id, name FROM held.holding_space ORDER BY ord")?;
        let spaces = query
            .query_map([], |r| {
                Ok(Space {
                    id: r.get(0)?,
                    name: r.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if spaces.is_empty() {
            return Err(Error::internal("Holdings declare no linkage space."));
        }
        if let Some(holdings) = self.holdings.as_mut() {
            holdings.possession = possession;
            holdings.spaces = spaces;
        }
        Ok(())
    }

    fn admit_identities(&self) -> Result<(), Error> {
        let conn = self.connect()?;
        self.attach_holdings(&conn)?;
        let collision: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM main.ident b JOIN held.ident s ON s.pid = b.pid
             WHERE (b.kind != 'provider' OR s.kind != 'provider') AND NOT (
               b.kind=s.kind AND (
               (b.kind='variable' AND EXISTS(SELECT 1 FROM held.holding_reference_occurrence x WHERE x.variable_pid=b.pid))
               OR (b.kind='table' AND EXISTS(SELECT 1 FROM held.holding_reference_occurrence x WHERE x.table_pid=b.pid))
               OR (b.kind='register' AND EXISTS(SELECT 1 FROM held.holding_reference_occurrence x
                 JOIN held.variable v ON v.pid=x.variable_pid JOIN held.register r ON r.id=v.register_id WHERE r.pid=b.pid))
               -- An overlay: the steward adds its own variables to the same register or table.
               OR (b.kind='register' AND EXISTS(SELECT 1 FROM main.register br JOIN main.provider bp ON bp.id=br.provider_id
                 JOIN held.register hr ON hr.pid=br.pid JOIN held.provider hp ON hp.id=hr.provider_id
                 WHERE br.pid=b.pid AND hp.pid=bp.pid)
                 -- Its implicit table shares the pid and must match too.
                 AND NOT EXISTS(SELECT 1 FROM held.tbl ht JOIN held.register hr ON hr.id=ht.register_id
                   WHERE ht.pid=b.pid AND NOT EXISTS(SELECT 1 FROM main.tbl bt JOIN main.register br ON br.id=bt.register_id
                     WHERE bt.pid=ht.pid AND br.pid=hr.pid AND bt.timeless=ht.timeless)))
               OR (b.kind='table' AND EXISTS(SELECT 1 FROM main.tbl bt JOIN main.register br ON br.id=bt.register_id
                 JOIN held.tbl ht ON ht.pid=bt.pid JOIN held.register hr ON hr.id=ht.register_id
                 WHERE bt.pid=b.pid AND hr.pid=br.pid AND ht.timeless=bt.timeless)))))
             OR EXISTS(SELECT 1 FROM main.ident_retired b JOIN held.ident s ON s.pid = b.pid)
             OR EXISTS(SELECT 1 FROM main.ident b JOIN held.ident_retired s ON s.pid = b.pid)",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error::new(Code::SchemaIncompatible, "Supplemental identity index is unreadable.", vec![]))?;
        if collision {
            return Err(Error::new(
                Code::SchemaIncompatible,
                "Supplemental public id collides with the catalog.",
                vec![],
            ));
        }
        Ok(())
    }

    /// Whether this deployment serves the entity: always outside steward mode; in
    /// steward mode only what the steward holds. A set is served when two or more of
    /// its members are held (#2410). Value sets, classifications and documents are not
    /// possession claims (crates/DESIGN.md, "Three questions, three scopes") and are
    /// always served.
    #[must_use]
    pub fn serves(&self, kind: &str, pid: &str) -> bool {
        let Some(held) = self.holdings.as_ref().map(|h| &h.possession) else {
            return true;
        };
        match kind {
            "set" => held.sets.contains(pid),
            kind => held.of(kind).is_none_or(|map| map.contains_key(pid)),
        }
    }

    /// Whether `show`, `search` and `segments` show the entity: what `serves` serves,
    /// and in steward mode also a table only blocked files route to, with its register
    /// and provider (crates/DESIGN.md, "Held tables that cannot be ordered").
    #[must_use]
    pub fn shows(&self, kind: &str, pid: &str) -> bool {
        self.serves(kind, pid)
            || self
                .holdings
                .as_ref()
                .and_then(|h| h.possession.blocked.of(kind))
                .is_some_and(|map| map.contains_key(pid))
    }

    /// Whether the shown provider, register or table is held only by files no order can
    /// draw on yet.
    #[must_use]
    pub fn blocked(&self, kind: &str, pid: &str) -> bool {
        !self.serves(kind, pid) && self.shows(kind, pid)
    }

    /// Each table or register shown only through blocked files, with each span those
    /// files hold (`None` for one whose files state no years), in pid order: what
    /// `search` matches them over, as `held_spans` gives their hits' periods.
    pub(crate) fn blocked_spans(&self, kind: &str) -> Vec<(&str, Option<Span>)> {
        let mut out: Vec<(&str, Option<Span>)> = self
            .holdings
            .as_ref()
            .and_then(|h| h.possession.blocked.of(kind))
            .into_iter()
            .flatten()
            .flat_map(|(pid, spans)| {
                let spans: Vec<Option<Span>> = if spans.is_empty() {
                    vec![None]
                } else {
                    spans.iter().copied().map(Some).collect()
                };
                spans.into_iter().map(move |span| (pid.as_str(), span))
            })
            .collect();
        out.sort_unstable();
        out
    }

    /// A shown table's files no order can draw on yet, whether or not a placement also
    /// holds it; empty when it has none or outside steward mode. Their `spans` are
    /// unfilled: `Server::not_orderable` dates them.
    pub(crate) fn not_orderable(&self, table: &str) -> &[NotOrderable] {
        self.holdings
            .as_ref()
            .and_then(|h| h.possession.not_orderable.get(table))
            .map_or(&[], Vec::as_slice)
    }

    /// Whether a request about `table` may name `variable`: always outside steward
    /// mode; in steward mode when the steward holds the variable, or when `table` is
    /// shown only through blocked files and the catalog documents the variable there.
    #[must_use]
    pub fn admits_in(&self, variable: &str, table: &str) -> bool {
        self.serves("variable", variable)
            || self.holdings.as_ref().is_some_and(|h| {
                h.possession
                    .blocked_pairs
                    .contains(&(variable.to_owned(), table.to_owned()))
            })
    }

    /// Whether this deployment serves `variable` in `table`: always outside steward
    /// mode; in steward mode only when a placement names that pair.
    #[must_use]
    pub fn serves_in(&self, variable: &str, table: &str) -> bool {
        self.holdings.as_ref().is_none_or(|h| {
            h.possession
                .pairs
                .contains(&(variable.to_owned(), table.to_owned()))
        })
    }

    /// In steward mode, the years a shown provider, register, table or variable is
    /// held, merged and in order: its placements', or for one only blocked files hold,
    /// those files' periods. `None` outside steward mode or for another kind.
    #[must_use]
    pub fn held_spans(&self, kind: &str, pid: &str) -> Option<&[Span]> {
        let held = &self.holdings.as_ref()?.possession;
        held.of(kind)?
            .get(pid)
            .or_else(|| held.blocked.of(kind)?.get(pid))
            .map(Vec::as_slice)
    }

    /// The admitted steward's identity; absent in reference-only mode.
    #[must_use]
    pub fn steward(&self) -> Option<Steward> {
        self.holdings.as_ref().map(|h| Steward {
            owner: h.meta["owner_id"].clone(),
            catalog_generation: h.meta["catalog_generation"].clone(),
            holdings_generation: h.meta["generation_id"].clone(),
            schema_version: h.meta["schema_version"].clone(),
        })
    }

    /// The id a project's `catalog` names this catalog by: the admitted steward's, or
    /// `global` in reference-only mode.
    #[must_use]
    pub fn id(&self) -> String {
        self.steward()
            .map_or_else(|| "global".to_owned(), |steward| steward.owner)
    }

    /// The admitted steward's linkage spaces in config order: one for a steward that
    /// declares none. Absent in reference-only mode.
    pub(crate) fn spaces(&self) -> Option<&[Space]> {
        self.holdings.as_ref().map(|h| h.spaces.as_slice())
    }

    /// The steward's presentation and logo, served from the admitted artifact;
    /// runtime never reads the steward repository.
    pub(crate) fn steward_config(&self) -> Option<serde_json::Value> {
        self.holdings.as_ref().map(|h| serde_json::json!({
            "presentation": serde_json::from_str::<serde_json::Value>(&h.meta["steward"]).expect("admitted JSON"),
            "logo": h.meta["logo"],
            "spaces": h.spaces,
        }))
    }

    /// A read-only connection to the supplemental semantics and physical holdings.
    ///
    /// # Errors
    /// `catalog_unavailable` when the admitted partner is no longer readable.
    pub fn connect_holdings(&self) -> Result<Option<Connection>, Error> {
        self.holdings
            .as_ref()
            .map(|h| {
                connect(&h.path, self.mmap_size).map_err(|err| crate::unavailable(&h.path, &err))
            })
            .transpose()
    }

    /// Resolve the database origin before using any local row id. Exact public ids
    /// take precedence over names; a bare name in both origins stays ambiguous. In
    /// steward mode an entity the steward does not hold is `not_found` (`serves`).
    ///
    /// # Errors
    /// The usual reference errors, or `catalog_unavailable` for an unreadable file.
    pub fn connect_for_ref(&self, value: &str) -> Result<Connection, Error> {
        self.connect_admitted(value, Self::serves)
    }

    /// `connect_for_ref` for `show`, `search` and `segments`, which also admit what
    /// `shows` shows.
    ///
    /// # Errors
    /// As `connect_for_ref`.
    pub fn connect_for_shown_ref(&self, value: &str) -> Result<Connection, Error> {
        self.connect_admitted(value, Self::shows)
    }

    /// `connect_for_ref` for a ref named in a request about `table`: a variable is
    /// admitted by `admits_in`, any other entity by `serves`.
    ///
    /// # Errors
    /// As `connect_for_ref`.
    pub(crate) fn connect_for_ref_in(&self, value: &str, table: &str) -> Result<Connection, Error> {
        let conn = self.origin_for_ref(value)?;
        if self.holdings.is_some() {
            let entity = crate::ops::refs::resolve(&conn, value)?;
            let admitted = if entity.kind == "variable" {
                self.admits_in(&entity.pid, table)
            } else {
                self.serves(&entity.kind, &entity.pid)
            };
            if !admitted {
                return Err(Error::not_found(value));
            }
        }
        Ok(conn)
    }

    fn connect_admitted(
        &self,
        value: &str,
        admits: fn(&Self, &str, &str) -> bool,
    ) -> Result<Connection, Error> {
        let conn = self.origin_for_ref(value)?;
        if self.holdings.is_some() {
            let entity = crate::ops::refs::resolve(&conn, value)?;
            if !admits(self, &entity.kind, &entity.pid) {
                return Err(Error::not_found(value));
            }
        }
        Ok(conn)
    }

    fn origin_for_ref(&self, value: &str) -> Result<Connection, Error> {
        let base = self.connect()?;
        let Some(extra) = self.connect_holdings()? else {
            return Ok(base);
        };
        let exact = |conn: &Connection| -> Result<bool, Error> {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM ident WHERE pid = ?)",
                [value],
                |r| r.get(0),
            )?)
        };
        if exact(&base)? {
            return Ok(base);
        }
        if exact(&extra)? {
            return Ok(extra);
        }
        let first = crate::ops::refs::resolve(&base, value);
        let second = crate::ops::refs::resolve(&extra, value);
        match (first, second) {
            (Ok(a), Ok(b)) => Err(Error::new(
                Code::AmbiguousRef,
                "The name is used in both the catalog and the holder's data; use an id.",
                vec![
                    value.into(),
                    serde_json::json!([
                        {"fqid": a.pid, "kind": a.kind}, {"fqid": b.pid, "kind": b.kind}
                    ]),
                ],
            )),
            (Err(first), Err(second)) => Err(
                if (second.kind() == Code::AmbiguousRef && first.kind() != Code::AmbiguousRef)
                    || (first.kind() == Code::NotFound && second.kind() == Code::RetiredRef)
                {
                    second
                } else {
                    first
                },
            ),
            (Err(err), _) | (_, Err(err)) if err.kind() == Code::AmbiguousRef => Err(err),
            (Ok(_), _) => Ok(base),
            (_, Ok(_)) => Ok(extra),
        }
    }

    /// Physical facts attached under an explicit database origin for public-id joins.
    /// Steward mode only: every caller checks for the holdings pair first.
    pub(crate) fn attach_holdings(&self, conn: &Connection) -> Result<(), Error> {
        let holdings = self
            .holdings
            .as_ref()
            .ok_or_else(|| Error::internal("Holdings attached without a holdings database."))?;
        conn.execute(
            "ATTACH DATABASE ? AS held",
            [crate::read_uri(&holdings.path)],
        )?;
        Ok(())
    }

    /// Select a content-addressed shared value set or document without exposing ids
    /// from the other database origin.
    pub(crate) fn connect_for_hash(&self, table: &str, hash: &str) -> Result<Connection, Error> {
        let base = self.connect()?;
        let exists = |conn: &Connection| -> Result<bool, Error> {
            Ok(conn
                .query_row(
                    &format!("SELECT 1 FROM {table} WHERE hash = ?"),
                    [hash],
                    |_| Ok(()),
                )
                .optional()?
                .is_some())
        };
        if exists(&base)? {
            return Ok(base);
        }
        if let Some(extra) = self.connect_holdings()?
            && exists(&extra)?
        {
            return Ok(extra);
        }
        Ok(base)
    }
}
