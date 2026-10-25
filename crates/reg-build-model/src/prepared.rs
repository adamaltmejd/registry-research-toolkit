//! The v16 prepared source records, read as the Python builder writes them
//! (`reg_meta_build/src/reg_meta_build/prepared_sources.py`, `_DDL` and `_write_record`).
//!
//! These are provider-shaped records: SCB's integer ids, `SoS`'s Deldatamängd tokens and
//! the thin providers' authored keys all appear here as delivered. Only the normalize
//! stage reads them; everything after it sees the neutral [`crate::model`] types, which
//! hold a record only as opaque [`crate::Evidence`].
//!
//! A payload is stored once per `(kind, digest)` and written with Pydantic's
//! `exclude_defaults=True`, so a missing key is that model's default. The serde
//! defaults below restate those defaults; the digest golden
//! (`tests/digest_golden.rs`) fails if one drifts.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, OptionalExtension, Row};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::Error;
use crate::model::StatedField;

/// The record schema this reader understands (`prepared_sources.py` `_SCHEMA_VERSION`).
pub const RECORDS_SCHEMA_VERSION: u32 = 16;
/// The prepared catalog layout this reader understands (`prepared_catalog.py`).
pub const CATALOG_SCHEMA_VERSION: u32 = 5;

/// Every `SourceFields` name, in declaration order (`source_records.py`). A stated
/// field outside this set is a contract violation.
pub const FIELD_NAMES: [&str; 40] = [
    "availability",
    "column_name",
    "name",
    "definition",
    "description",
    "operational_definition",
    "data_type",
    "data_length",
    "representation",
    "source_attribution",
    "measurement_unit",
    "reference_period",
    "sensitivity",
    "conditional_sensitivity",
    "identifier",
    "purpose",
    "classification_declared",
    "value_set_declared",
    "base_register",
    "population_definition",
    "population_comment",
    "population_date",
    "coverage",
    "coverage_from",
    "coverage_to",
    "geographic_coverage",
    "update_frequency",
    "aggregation_level",
    "documentation_url",
    "landing_page",
    "access_url",
    "access_rights",
    "legislation",
    "source_version",
    "source_date",
    "dataset_label",
    "measurement_information",
    "documentation_status",
    "first_approved_at",
    "last_approved_at",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinateStatus {
    Value,
    Unknown,
    NotApplicable,
}

/// A provider-native id: SCB's integers, or a string id.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum NativeId {
    Int(i64),
    Str(String),
}

/// `SourceCoordinate`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coordinate {
    pub status: CoordinateStatus,
    #[serde(default)]
    pub native_id: Option<NativeId>,
    #[serde(default)]
    pub name: Option<String>,
}

/// `NativeCoordinates`: SCB's five ids. The neutral model has no equivalent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Native {
    pub register_id: Option<i64>,
    pub register_variant_id: Option<i64>,
    pub edition_id: Option<i64>,
    pub variable_id: Option<i64>,
    pub member_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    NotApplicable,
    Unknown,
    YearIndependent,
    Intervals,
    Pooled,
}

/// `ScopeInterval`: a null `end` is open-ended, never unknown.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeInterval {
    pub start: String,
    pub end: Option<String>,
}

/// `TemporalScope`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub kind: ScopeKind,
    #[serde(default)]
    pub intervals: Vec<ScopeInterval>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub pooled_start: Option<String>,
    #[serde(default)]
    pub pooled_end: Option<String>,
}

/// The stated `SourceFields`, keyed by a [`FIELD_NAMES`] name; an absent name is unstated.
pub type Fields = BTreeMap<String, StatedField>;

/// `DeliveredCell`: one delivered cell as read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub name: String,
    pub present: bool,
    #[serde(default)]
    pub raw_value: Option<String>,
    pub interpreted_value: String,
    #[serde(default)]
    pub raw_type: Option<String>,
    #[serde(default)]
    pub storage_type: Option<String>,
    #[serde(default)]
    pub number_format: Option<String>,
    #[serde(default)]
    pub hyperlink_target: Option<String>,
    #[serde(default)]
    pub hyperlink_location: Option<String>,
    #[serde(default)]
    pub cached_raw_value: Option<String>,
    #[serde(default)]
    pub cached_raw_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentKind {
    Register,
    Variant,
    Edition,
    Population,
    ObjectType,
}

impl ParentKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Variant => "variant",
            Self::Edition => "edition",
            Self::Population => "population",
            Self::ObjectType => "object_type",
        }
    }
}

/// `SourceFieldCells`: the delivered-cell positions that support one parent field.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldCells {
    pub field: String,
    pub positions: Vec<u32>,
}

/// `SourceParentObservation`: a register, variant, edition, population or object-type
/// claim that the record's delivered row also states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parent {
    pub kind: ParentKind,
    pub coordinate: Coordinate,
    pub register: Coordinate,
    pub variant: Option<Coordinate>,
    pub edition: Option<Coordinate>,
    pub fields: Fields,
    pub field_cells: Vec<FieldCells>,
}

/// `RecordLocator`: where one physical row of the record was delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Locator {
    pub semantic_record_key: Vec<String>,
    pub physical_file: String,
    pub physical_table: String,
    pub physical_record: String,
    pub physical_cells: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Sv,
    En,
}

impl Language {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sv => "sv",
            Self::En => "en",
        }
    }
}

/// `SourceRecord` with its `SourceSubject` flattened: one prepared occurrence.
///
/// The occurrence's `family_payload` (`native_variable_key`) is not read: grouping is
/// recomputed from links by normalize, never taken from the Python grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The occurrence's position in the prepared corpus, from 1.
    pub ordinal: i64,
    pub record_id: String,
    pub source: String,
    pub source_revision_id: String,
    pub locators: Vec<Locator>,
    pub provider: String,
    pub register: Coordinate,
    pub variant: Coordinate,
    pub variant_references: Vec<Coordinate>,
    pub population: Coordinate,
    pub variable: Coordinate,
    pub member: Coordinate,
    pub native: Native,
    /// `edition_scope`.
    pub edition_scope: Scope,
    /// `edition_period_scope`.
    pub edition_period_scope: Scope,
    pub fields: Fields,
    pub parent_facts: Vec<Parent>,
    pub language: Option<Language>,
    pub original_period_text: Option<String>,
    pub context: Vec<String>,
    pub delivered_cells: Vec<Cell>,
}

/// `files/records/manifest.json`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordsManifest {
    pub format: String,
    pub schema_version: u32,
    pub scope: String,
    pub partial: bool,
    pub record_count: u64,
    pub table_count: u64,
    pub table_row_count: u64,
    pub revisions: Vec<Revision>,
    pub database_path: String,
    pub database_size: u64,
    pub database_sha256: String,
    pub database_git_blob: String,
}

/// `SourceRevision`: one selected delivery of one source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub revision_id: String,
    pub dataset: String,
    pub publisher: String,
    pub purpose: String,
    pub upstream_revision: String,
    pub artifact_path: String,
    pub artifact_size: u64,
    pub artifact_sha256: String,
}

/// One input's accounting row in the catalog manifest.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputAccounting {
    pub origin: String,
    pub path: String,
    pub role: String,
    pub record_usage: String,
    pub present: bool,
    pub disposition: String,
    pub revision: Option<Revision>,
    #[serde(default)]
    pub counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub exclusion_reason: Option<String>,
}

/// A value-set child of the prepared root.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueChild {
    pub path: String,
    pub revision_id: String,
    pub manifest_sha256: String,
}

/// One member of the prepared root with its preparation proofs.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProof {
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub git_blob: String,
}

/// The prepared root's `manifest.json` (`prepared_catalog.py` `_ManifestDocument`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogManifest {
    pub format: String,
    pub schema_version: u32,
    pub prepare_code_sha256: String,
    pub input_commit: String,
    pub bundle_path: String,
    pub bundle_manifest_sha256: String,
    pub snapshot_path: String,
    pub snapshot_manifest_sha256: String,
    pub inputs: Vec<InputAccounting>,
    pub records_manifest_sha256: String,
    pub values: Vec<ValueChild>,
    /// `SourceSupportJoin`s, read by the slice that ports support joins.
    pub support_joins: Vec<serde_json::Value>,
    pub evidence_count: u64,
    pub files: Vec<FileProof>,
}

/// What a prepared root's own tables hold, to compare with its manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub records: u64,
    pub tables: u64,
    pub table_rows: u64,
    pub evidence: u64,
    pub values: u64,
}

/// An opened prepared root: both manifests checked against each other and the
/// records database opened read-only.
///
/// Acceptance (the Git blob proofs against the inputs repository) stays with the
/// Python build's input selection until preparation ports (phase 5); this reader
/// checks the manifest chain, the layout and the database size.
pub struct PreparedRoot {
    root: PathBuf,
    catalog: CatalogManifest,
    catalog_sha256: String,
    manifest: RecordsManifest,
    records: Records,
}

/// A v16 records database (`files/records/files/records.sqlite`), opened read-only.
pub struct Records {
    conn: Connection,
}

const RECORDS_DIR: &str = "files/records";
const EVIDENCE: &str = "files/evidence.jsonl";

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<(T, String), Error> {
    let bytes = fs::read(path).map_err(|err| Error::io(path, &err))?;
    let value = serde_json::from_slice(&bytes)
        .map_err(|err| Error::contract(format!("{}: {err}", path.display())))?;
    Ok((value, sha256_hex(&bytes)))
}

impl PreparedRoot {
    /// Open the prepared root at `root` (the directory holding `manifest.json`).
    ///
    /// # Errors
    ///
    /// An unreadable member, an unknown format or schema, a broken manifest chain or a
    /// database whose size differs from its manifest.
    pub fn open(root: &Path) -> Result<Self, Error> {
        let (catalog, catalog_sha256): (CatalogManifest, _) =
            read_json(&root.join("manifest.json"))?;
        if catalog.format != "reg-meta-prepared-catalog-sources"
            || catalog.schema_version != CATALOG_SCHEMA_VERSION
        {
            return Err(Error::contract(format!(
                "prepared root {} is {} schema {}; this reader reads \
                 reg-meta-prepared-catalog-sources schema {CATALOG_SCHEMA_VERSION}",
                root.display(),
                catalog.format,
                catalog.schema_version
            )));
        }
        let records_dir = root.join(RECORDS_DIR);
        let (records, records_sha256): (RecordsManifest, _) =
            read_json(&records_dir.join("manifest.json"))?;
        if records_sha256 != catalog.records_manifest_sha256 {
            return Err(Error::contract(
                "records manifest differs from the prepared root's records_manifest_sha256"
                    .to_owned(),
            ));
        }
        if records.format != "reg-meta-prepared-source-records"
            || records.schema_version != RECORDS_SCHEMA_VERSION
            || records.database_path != "files/records.sqlite"
        {
            return Err(Error::contract(format!(
                "prepared records are {} schema {}; this reader reads \
                 reg-meta-prepared-source-records schema {RECORDS_SCHEMA_VERSION}",
                records.format, records.schema_version
            )));
        }
        let database = records_dir.join(&records.database_path);
        let size = fs::metadata(&database)
            .map_err(|err| Error::io(&database, &err))?
            .len();
        if size != records.database_size {
            return Err(Error::contract(format!(
                "{} is {size} bytes; its manifest says {}",
                database.display(),
                records.database_size
            )));
        }
        Ok(Self {
            root: root.to_owned(),
            catalog,
            catalog_sha256,
            records: Records::open(&database)?,
            manifest: records,
        })
    }

    #[must_use]
    pub fn catalog(&self) -> &CatalogManifest {
        &self.catalog
    }

    /// The SHA-256 of the root's `manifest.json` bytes: the build's
    /// `prepared_manifest_sha256` pin.
    #[must_use]
    pub fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }

    #[must_use]
    pub fn records_manifest(&self) -> &RecordsManifest {
        &self.manifest
    }

    #[must_use]
    pub fn records(&self) -> &Records {
        &self.records
    }

    /// Count what the root holds: occurrences, evidence tables and rows, evidence
    /// lines and value-set directories.
    ///
    /// # Errors
    ///
    /// A database or evidence file that cannot be read.
    pub fn counts(&self) -> Result<Counts, Error> {
        let count = |sql: &str| -> Result<u64, Error> {
            let count: i64 = self.records.conn.query_row(sql, [], |row| row.get(0))?;
            u64::try_from(count).map_err(|_| Error::contract(format!("{sql}: {count}")))
        };
        let evidence_path = self.root.join(EVIDENCE);
        let evidence = fs::read(&evidence_path).map_err(|err| Error::io(&evidence_path, &err))?;
        Ok(Counts {
            records: count("SELECT COUNT(*) FROM occurrence")?,
            tables: count("SELECT COUNT(*) FROM evidence_table")?,
            table_rows: count("SELECT COUNT(*) FROM evidence_row")?,
            evidence: evidence.split(|&byte| byte == b'\n').count() as u64 - 1,
            values: fs::read_dir(self.root.join("files/values"))
                .map_err(|err| Error::io(&self.root.join("files/values"), &err))?
                .count() as u64,
        })
    }
}

impl Records {
    /// Open a records database read-only and check its schema version.
    ///
    /// # Errors
    ///
    /// A database that cannot be opened or is not schema 16.
    pub fn open(database: &Path) -> Result<Self, Error> {
        let uri = format!("file:{}?mode=ro&immutable=1", database.display());
        let conn = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )?;
        let version: u32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != RECORDS_SCHEMA_VERSION {
            return Err(Error::contract(format!(
                "records database user_version is {version}, not {RECORDS_SCHEMA_VERSION}"
            )));
        }
        Ok(Self { conn })
    }

    /// The distinct `source` datasets that hold occurrences, in order.
    ///
    /// # Errors
    ///
    /// A database that cannot be read.
    pub fn sources(&self) -> Result<Vec<String>, Error> {
        let mut statement = self
            .conn
            .prepare("SELECT DISTINCT source FROM occurrence ORDER BY source")?;
        let sources = statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(sources)
    }

    /// Decode every occurrence of `source` (all sources when `None`) in ordinal order
    /// and pass it to `visit`. Records stream; memory is bounded by the payload cache.
    ///
    /// # Errors
    ///
    /// A database that cannot be read, a payload that violates its contract, or an
    /// error from `visit`.
    pub fn for_each_record(
        &self,
        source: Option<&str>,
        mut visit: impl FnMut(Record) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut payloads = Payloads::new(&self.conn)?;
        let mut locators = self.conn.prepare_cached(
            "SELECT physical_file, physical_table, physical_record, cells_prefix, \
             cells_payload FROM locator WHERE occurrence = ? ORDER BY position",
        )?;
        let mut cells = self.conn.prepare_cached(
            "SELECT payload FROM delivered_cell WHERE occurrence = ? ORDER BY position",
        )?;
        let sql = format!(
            "SELECT ordinal, source, semantic_key, record_id, revision_id, provider, \
             register_payload, variant_payload, variant_references_payload, \
             population_payload, variable_payload, member_payload, native_payload, \
             scope_payload, period_payload, fields_payload, language, \
             original_period_text, context_payload, parents_payload FROM occurrence {} \
             ORDER BY ordinal",
            if source.is_some() {
                "WHERE source = ?1"
            } else {
                ""
            }
        );
        let mut statement = self.conn.prepare(&sql)?;
        let mut rows = match source {
            Some(source) => statement.query([source])?,
            None => statement.query([])?,
        };
        while let Some(row) = rows.next()? {
            let ordinal: i64 = row.get(0)?;
            let semantic_key: Vec<String> = serde_json::from_str(&row.get::<_, String>(2)?)
                .map_err(|err| Error::contract(format!("occurrence {ordinal} key: {err}")))?;
            let mut record_locators = Vec::new();
            let mut located = locators.query([ordinal])?;
            while let Some(locator) = located.next()? {
                let prefix: String = locator.get(3)?;
                let suffixes: Vec<String> = payloads.decode(locator.get(4)?, "strings")?;
                record_locators.push(Locator {
                    semantic_record_key: semantic_key.clone(),
                    physical_file: locator.get(0)?,
                    physical_table: locator.get(1)?,
                    physical_record: locator.get(2)?,
                    physical_cells: suffixes
                        .iter()
                        .map(|cell| format!("{prefix}{cell}"))
                        .collect(),
                });
            }
            let mut record_cells = Vec::new();
            let mut delivered = cells.query([ordinal])?;
            while let Some(cell) = delivered.next()? {
                record_cells.push(payloads.decode(cell.get(0)?, "cell")?);
            }
            visit(read_record(
                &mut payloads,
                row,
                record_locators,
                record_cells,
            )?)?;
        }
        Ok(())
    }
}

fn read_record(
    payloads: &mut Payloads<'_>,
    row: &Row<'_>,
    locators: Vec<Locator>,
    delivered_cells: Vec<Cell>,
) -> Result<Record, Error> {
    let ordinal: i64 = row.get(0)?;
    let language = match row.get::<_, Option<String>>(16)?.as_deref() {
        None => None,
        Some("sv") => Some(Language::Sv),
        Some("en") => Some(Language::En),
        Some(other) => {
            return Err(Error::contract(format!(
                "occurrence {ordinal} language {other:?}"
            )));
        }
    };
    Ok(Record {
        ordinal,
        source: row.get(1)?,
        record_id: row.get(3)?,
        source_revision_id: row.get(4)?,
        locators,
        provider: row.get(5)?,
        register: payloads.decode(row.get(6)?, "coordinate")?,
        variant: payloads.decode(row.get(7)?, "coordinate")?,
        variant_references: payloads.decode(row.get(8)?, "coordinates")?,
        population: payloads.decode(row.get(9)?, "coordinate")?,
        variable: payloads.decode(row.get(10)?, "coordinate")?,
        member: payloads.decode(row.get(11)?, "coordinate")?,
        native: payloads.decode(row.get(12)?, "native")?,
        edition_scope: payloads.decode(row.get(13)?, "scope")?,
        edition_period_scope: payloads.decode(row.get(14)?, "scope")?,
        fields: payloads.fields(row.get(15)?)?,
        parent_facts: payloads.parents(row.get(19)?)?,
        language,
        original_period_text: row.get(17)?,
        context: payloads.decode(row.get(18)?, "strings")?,
        delivered_cells,
    })
}

/// The interned `payload` table, with a clear-all cache of bodies by id.
struct Payloads<'conn> {
    statement: rusqlite::CachedStatement<'conn>,
    cache: HashMap<i64, (String, String)>,
}

// simplify: clear-all at the limit, as `_DECODED_PAYLOAD_CACHE_LIMIT` in
// prepared_sources.py; measure hit rates if a full-corpus read is slow.
const PAYLOAD_CACHE_LIMIT: usize = 65_536;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParentRefs {
    kind: ParentKind,
    coordinate: i64,
    register: i64,
    variant: Option<i64>,
    edition: Option<i64>,
    fields: i64,
    field_cells: Vec<FieldCells>,
}

impl<'conn> Payloads<'conn> {
    fn new(conn: &'conn Connection) -> Result<Self, Error> {
        Ok(Self {
            statement: conn.prepare_cached("SELECT kind, body FROM payload WHERE id = ?")?,
            cache: HashMap::new(),
        })
    }

    fn body(&mut self, id: i64, kind: &str) -> Result<&str, Error> {
        if !self.cache.contains_key(&id) {
            let found: Option<(String, String)> = self
                .statement
                .query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))
                .optional()?;
            let found = found.ok_or_else(|| Error::contract(format!("missing payload {id}")))?;
            if self.cache.len() >= PAYLOAD_CACHE_LIMIT {
                self.cache.clear();
            }
            self.cache.insert(id, found);
        }
        let (stored, body) = &self.cache[&id];
        if stored != kind {
            return Err(Error::contract(format!(
                "payload {id} is a {stored}, expected a {kind}"
            )));
        }
        Ok(body)
    }

    fn decode<T: serde::de::DeserializeOwned>(&mut self, id: i64, kind: &str) -> Result<T, Error> {
        let body = self.body(id, kind)?;
        serde_json::from_str(body)
            .map_err(|err| Error::contract(format!("payload {id} ({kind}): {err}")))
    }

    fn fields(&mut self, id: i64) -> Result<Fields, Error> {
        let refs: BTreeMap<String, i64> = self.decode(id, "fields")?;
        let mut fields = Fields::new();
        for (name, field) in refs {
            if !FIELD_NAMES.contains(&name.as_str()) {
                return Err(Error::contract(format!(
                    "payload {id}: unknown field {name}"
                )));
            }
            let value = self.decode(field, "field")?;
            fields.insert(name, value);
        }
        Ok(fields)
    }

    fn parents(&mut self, id: i64) -> Result<Vec<Parent>, Error> {
        let refs: Vec<i64> = self.decode(id, "parents")?;
        refs.into_iter()
            .map(|parent| {
                let refs: ParentRefs = self.decode(parent, "parent")?;
                Ok(Parent {
                    kind: refs.kind,
                    coordinate: self.decode(refs.coordinate, "coordinate")?,
                    register: self.decode(refs.register, "coordinate")?,
                    variant: refs
                        .variant
                        .map(|variant| self.decode(variant, "coordinate"))
                        .transpose()?,
                    edition: refs
                        .edition
                        .map(|edition| self.decode(edition, "coordinate"))
                        .transpose()?,
                    fields: self.fields(refs.fields)?,
                    field_cells: refs.field_cells,
                })
            })
            .collect()
    }
}
