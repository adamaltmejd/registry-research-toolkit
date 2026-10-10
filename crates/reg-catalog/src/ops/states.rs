//! `states`: a variable's representations over the compiled `expanded_state`, with
//! the request-dependent rules of section 3 applied at read time: the window
//! fallback, held and requested clipping and warning attribution
//! (`crates/DESIGN.md`, "Compiled states and request-time rules").

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{fold_identity, period_token_for_bounds};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::refs::{self, Target};
use super::show::variant_families;
use super::values::ScopedSentinel;
use super::{Params, Server, cursor};
use crate::{Code, Error, Scope, hex};

/// The open-ended `valid_to` sentinel, which has no finite period token.
const OPEN_ENDED: &str = "9999-12-31";
/// Paging stops at this depth, as `search`'s.
const DEPTH: usize = 1000;
/// `value_set_version`'s spelling of the empty label, which a query string cannot
/// carry (today's webapp sentinel).
const NO_VERSION: &str = "_none";

/// One representation a request emits: an `expanded_state` row, with its bounds
/// clipped to held periods in holdings scope.
#[derive(Clone)]
pub(crate) struct Emitted {
    pub expanded_state_id: i64,
    pub state_id: i64,
    pub register_variant_id: i64,
    /// `base`, `base_fallback`, `source_window`, `curated_window` or `coded_window`.
    pub kind: String,
    pub delivery_column_name: Option<String>,
    pub canonical_column: Option<String>,
    /// `intervals` or `year_independent`.
    pub period_scope: String,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    /// The `variable_alias_window` key start; None on a base row.
    pub window_valid_from: Option<String>,
}

impl Emitted {
    fn overlaps(&self, (lo, hi): (&str, &str)) -> bool {
        matches!((&self.valid_from, &self.valid_to), (Some(from), Some(to)) if from.as_str() <= hi && to.as_str() >= lo)
    }
}

/// SQL order keys for one state's `expanded_state` rows (alias `e`): its base row,
/// then its windows in the resolver's order, source and coded before curated, each
/// by key start and column (`derive/states.py`). A window's key is unique within its
/// state, so the keys are total there; `expanded_state_id` is never an order key.
pub(crate) fn within_state(e: &str) -> String {
    format!(
        "{e}.kind NOT IN ('base', 'base_fallback'), {e}.kind = 'curated_window', \
         {e}.window_valid_from, {e}.delivery_column_name"
    )
}

/// The representations of `variable_id` a request emits, in today's reader order
/// (`Catalog.states`, `Catalog.resolve_at`), held-clipped in holdings scope.
///
/// Without `bounds`, the whole history: every row but `base_fallback`. With
/// `bounds` (ISO dates), the dated states overlapping them, each by the window
/// fallback: a `base_fallback` state emits its overlapping source and coded windows,
/// or its base when none overlaps; every state adds its overlapping curated windows.
/// `variant` narrows to one register variant.
pub(crate) fn emitted(
    conn: &Connection,
    scope: Scope,
    variable_id: i64,
    variant: Option<i64>,
    bounds: Option<(&str, &str)>,
) -> Result<Vec<Emitted>, Error> {
    // Per state in today's chronological order; its base row first, then its
    // windows in the resolver's order (source before curated).
    // A variable's states are unique by (variant, valid_from, version label).
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT e.expanded_state_id, e.state_id, e.register_variant_id, e.kind, \
         e.delivery_column_name, e.canonical_column, s.period_scope, e.valid_from, \
         e.valid_to, e.window_valid_from \
         FROM expanded_state e JOIN variable_state s ON s.state_id = e.state_id \
         JOIN register_variant rv ON rv.register_variant_id = e.register_variant_id \
         WHERE e.variable_id = ?1 AND (?2 IS NULL OR e.register_variant_id = ?2) \
         ORDER BY s.valid_from, s.valid_to, s.value_set_version_label, rv.slug, {}",
        within_state("e")
    ))?;
    let rows = stmt
        .query_map(params![variable_id, variant], |row| {
            Ok(Emitted {
                expanded_state_id: row.get(0)?,
                state_id: row.get(1)?,
                register_variant_id: row.get(2)?,
                kind: row.get(3)?,
                delivery_column_name: row.get(4)?,
                canonical_column: row.get(5)?,
                period_scope: row.get(6)?,
                valid_from: row.get(7)?,
                valid_to: row.get(8)?,
                window_valid_from: row.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for state in rows.chunk_by(|a, b| a.state_id == b.state_id) {
        let (base, windows) = state.split_first().expect("a chunk has a row");
        let Some(bounds) = bounds else {
            out.extend(state.iter().filter(|e| e.kind != "base_fallback").cloned());
            continue;
        };
        if base.period_scope != "intervals" || !base.overlaps(bounds) {
            continue;
        }
        let overlapping = windows.iter().filter(|w| w.overlaps(bounds));
        let (curated, replacing): (Vec<&Emitted>, Vec<&Emitted>) =
            overlapping.partition(|w| w.kind == "curated_window");
        if base.kind == "base" || replacing.is_empty() {
            out.push(base.clone());
        }
        out.extend(replacing.into_iter().chain(curated).cloned());
    }
    // Today's reader sorts the expansion of a variable that has alias windows (any
    // variant) and keeps the chronological state order otherwise; the stable sort
    // keeps the state order on ties.
    let windowed: bool = conn
        .prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM variable_alias_window WHERE variable_id = ?)",
        )?
        .query_row([variable_id], |row| row.get(0))?;
    if windowed {
        out.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
    }
    match scope {
        Scope::Reference => Ok(out),
        Scope::Holdings => held(conn, variable_id, out, bounds),
    }
}

/// The value set and version label `e` emits: a coded window's own, else its state's.
pub(crate) fn value_set(
    conn: &Connection,
    variable_id: i64,
    e: &Emitted,
) -> Result<(Option<i64>, String), Error> {
    let pair = |row: &Row| Ok((row.get(0)?, row.get(1)?));
    if let Some(start) = &e.window_valid_from {
        let coded = conn
            .prepare_cached(
                "SELECT value_set_id, value_set_version_label FROM variable_alias_window \
                 WHERE variable_id = ? AND register_variant_id = ? \
                 AND delivery_column_name = ? AND valid_from = ? \
                 AND coding_metadata = 'per_column'",
            )?
            .query_row(
                params![
                    variable_id,
                    e.register_variant_id,
                    e.delivery_column_name,
                    start
                ],
                pair,
            )
            .optional()?;
        if let Some(coded) = coded {
            return Ok(coded);
        }
    }
    Ok(conn
        .prepare_cached(
            "SELECT value_set_id, value_set_version_label FROM variable_state WHERE state_id = ?",
        )?
        .query_row([e.state_id], pair)?)
}

/// The register variant `slug` of `variable_id`'s register; none when it has no such
/// variant (today's `_resolve_variant_id`).
pub(crate) fn variant_id(
    conn: &Connection,
    variable_id: i64,
    slug: &str,
) -> Result<Option<i64>, Error> {
    Ok(conn
        .prepare_cached(
            "SELECT rv.register_variant_id FROM register_variant rv \
             JOIN variable v USING(register_id) WHERE v.variable_id = ? AND rv.slug = ?",
        )?
        .query_row(params![variable_id, slug], |row| row.get(0))
        .optional()?)
}

/// Today's sort key of a windowed variable's expansion; a missing value sorts as "".
fn sort_key(e: &Emitted) -> (&str, &str, &str, &str) {
    (
        e.period_scope.as_str(),
        e.valid_from.as_deref().unwrap_or_default(),
        e.valid_to.as_deref().unwrap_or_default(),
        e.delivery_column_name.as_deref().unwrap_or_default(),
    )
}

/// A coded window's classification: slug, short name, name, provenance, evidence.
type Link = (String, String, String, Option<String>, Option<String>);

/// Today's `_scope_states`: a representation without a column goes; a
/// year-independent one stays when a year-independent table holds it; a dated one
/// becomes its held periods clipped to its own bounds and the request, day-adjacent
/// periods merged.
fn held(
    conn: &Connection,
    variable_id: i64,
    emitted: Vec<Emitted>,
    bounds: Option<(&str, &str)>,
) -> Result<Vec<Emitted>, Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT hp.lo, hp.hi FROM holding_mapping hm JOIN holding_column hc USING(column_id) \
         JOIN holding_table ht USING(table_id) LEFT JOIN holding_period hp USING(table_id) \
         WHERE hm.variable_id = ? AND hm.variant_id = ? AND hm.representation_canonical = ? \
         AND ht.scope = ? ORDER BY hp.lo, hp.hi",
    )?;
    let mut periods: BTreeMap<(i64, String, String), crate::held::Periods> = BTreeMap::new();
    let mut out = Vec::new();
    for e in emitted {
        let Some(column) = e.canonical_column.clone() else {
            continue;
        };
        let key = (e.register_variant_id, column, e.period_scope.clone());
        if !periods.contains_key(&key) {
            let found = stmt
                .query_map(params![variable_id, key.0, key.1, key.2], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?
                .collect::<rusqlite::Result<_>>()?;
            periods.insert(key.clone(), found);
        }
        let held = &periods[&key];
        let (Some(from), Some(to)) = (e.valid_from.clone(), e.valid_to.clone()) else {
            if !held.is_empty() {
                out.push(e);
            }
            continue;
        };
        out.extend(
            crate::held::clip(held, (&from, &to), bounds)
                .into_iter()
                .map(|(lo, hi)| Emitted {
                    valid_from: Some(lo),
                    valid_to: Some(hi),
                    ..e.clone()
                }),
        );
    }
    Ok(out)
}

/// A variable's warning, as the attribution predicate reads it.
struct Warning {
    id: String,
    variant: Option<i64>,
    column_fold: Option<String>,
    valid_from: Option<String>,
    valid_to: Option<String>,
}

/// The ids of `warnings` that apply to `e` at its emitted bounds, by the
/// attribution predicate of `crates/DESIGN.md`; `warnings` are the variable's,
/// ordered by id.
fn warning_ids(warnings: &[Warning], e: &Emitted) -> Vec<String> {
    let column = e.canonical_column.as_deref().map(fold_identity);
    warnings
        .iter()
        .filter(|w| {
            w.variant.is_none_or(|v| v == e.register_variant_id)
                && (w.column_fold.is_none() || w.column_fold == column)
                && w.valid_from
                    .as_ref()
                    .is_none_or(|f| e.valid_to.as_ref().is_some_and(|to| f <= to))
                && w.valid_to
                    .as_ref()
                    .is_none_or(|t| e.valid_from.as_ref().is_some_and(|from| t >= from))
        })
        .map(|w| w.id.clone())
        .collect()
}

/// `value_set_summary`: a value set's code count and, when its codes are a dense
/// integer run, its span.
#[derive(Clone, Serialize, ToSchema)]
pub struct ValueSetSummary {
    code_count: usize,
    integer_range: Option<IntegerRange>,
}

#[derive(Clone, Copy, Serialize, ToSchema)]
pub struct IntegerRange {
    min: i64,
    max: i64,
}

/// A classification linked to a representation, with its stored conformance verdict.
#[derive(Serialize, ToSchema)]
pub struct StateClassification {
    slug: String,
    short_name: String,
    name: String,
    provenance: Option<String>,
    conformance: Option<Conformance>,
}

/// The delivered domain against its declared classification; the mismatching codes
/// are `values`' `nonstandard` and `sentinels` partitions.
#[derive(Serialize, ToSchema)]
pub struct Conformance {
    declared_classification_slug: String,
    declared_classification_short_name: String,
    declared_classification_name: String,
    status: String,
    checked_code_count: i64,
    matched_code_count: i64,
    nonconforming_code_count: i64,
    overlap: f64,
    nonstandard_code_count: i64,
    sentinel_code_count: i64,
    /// Always empty here (today's light hydration).
    nonconforming_codes: Vec<Value>,
}

/// A state: today's `VariableState` under the catalog page's light hydration (its
/// codes are the `values` facet).
#[derive(Serialize, ToSchema)]
#[allow(clippy::struct_field_names)] // `state_id` is the wire name.
pub struct State {
    #[serde(serialize_with = "storage_id")]
    #[schema(value_type = String)]
    state_id: i64,
    variant: String,
    variant_label: Option<String>,
    variant_family: Option<String>,
    variant_family_label: Option<String>,
    #[serde(serialize_with = "storage_id")]
    #[schema(value_type = String)]
    register_variant_id: i64,
    /// `intervals` or `year_independent`.
    period_scope: String,
    valid_from: Option<String>,
    valid_to: Option<String>,
    data_type: Option<String>,
    data_length: Option<String>,
    delivery_column_name: Option<String>,
    source_register_text: Option<String>,
    operational_definition: Option<String>,
    definition: Option<String>,
    measurement_unit: Option<String>,
    name: Option<String>,
    description: Option<String>,
    /// The data warnings that apply to this representation at its bounds.
    warning_ids: Vec<String>,
    provenance: Option<String>,
    pooled: bool,
    value_set_version_label: String,
    /// The coding window's start, for a column coded on its own.
    coding_window_from: Option<String>,
    #[serde(serialize_with = "optional_storage_id")]
    #[schema(value_type = Option<String>)]
    value_set_id: Option<i64>,
    /// Always null: a state's codes are the `values` facet.
    #[schema(value_type = Option<Vec<Value>>)]
    value_set: Option<Vec<Value>>,
    value_set_summary: Option<ValueSetSummary>,
    is_identifier: bool,
    classifications: Vec<StateClassification>,
    /// The coarsest period token of the bounds; `_default` when year-independent,
    /// null when open-ended.
    period_token: Option<String>,
}

/// A storage id as a decimal string, which JSON clients keep exactly (today's
/// `CatalogStorageId`).
#[allow(clippy::trivially_copy_pass_by_ref)] // serde's `serialize_with` signature
pub(super) fn storage_id<S: serde::Serializer>(id: &i64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(id)
}

#[allow(clippy::ref_option)] // serde's `serialize_with` signature
pub(super) fn optional_storage_id<S: serde::Serializer>(
    id: &Option<i64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match id {
        Some(id) => serializer.collect_str(id),
        None => serializer.serialize_none(),
    }
}

#[derive(Serialize, ToSchema)]
pub struct StatesPage {
    items: Vec<State>,
    next_cursor: Option<String>,
}

pub fn states(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let limit = super::limit(params)?;
    let period = super::period(params, "period")?;
    let variant = super::variant(params)?;
    let version = super::value_set_version(params)?;
    let conn = catalog.connect()?;
    let reference = params["ref"];
    let Target::Variable { id } = refs::resolve(&conn, scope, Some(reference))? else {
        return Err(Error::new(
            Code::InvalidRef,
            format!("{reference:?} does not name a variable."),
            vec![reference.into()],
        ));
    };
    let context = json!([id, period.map(|p| p.to_string()), variant, version, scope]);
    let context = hex(&Sha256::digest(context.to_string().as_bytes()));
    let after = params
        .get("cursor")
        .map(|c| cursor::decode(c, catalog.generation(), &context, DEPTH))
        .transpose()?;
    let variant = match variant {
        None => None,
        Some(slug) => {
            // A slug no variant of the register has emits nothing (today's
            // `resolve_at`).
            let Some(found) = variant_id(&conn, id, slug)? else {
                let empty = StatesPage {
                    items: Vec::new(),
                    next_cursor: None,
                };
                return Ok(serde_json::to_value(empty).expect("StatesPage serializes"));
            };
            Some(found)
        }
    };
    let bounds = period.map(reg_core::Period::iso_bounds);
    let mut found = emitted(
        &conn,
        scope,
        id,
        variant,
        bounds.as_ref().map(|(lo, hi)| (lo.as_str(), hi.as_str())),
    )?;
    let mut hydrate = Hydrate::new(&conn, id)?;
    if let Some(version) = version {
        let version = if version == NO_VERSION { "" } else { version };
        let mut kept = Vec::new();
        for e in found {
            if hydrate.version_label(&e)? == version {
                kept.push(e);
            }
        }
        found = kept;
    }
    let position = |e: &Emitted| {
        format!(
            "{}:{}",
            e.expanded_state_id,
            e.valid_from.as_deref().unwrap_or_default()
        )
    };
    let offset = match &after {
        None => 0,
        Some((offset, after)) => {
            if *offset > found.len() || position(&found[offset - 1]) != *after {
                return Err(cursor::invalid(
                    "Cursor no longer matches the result ordering.",
                ));
            }
            *offset
        }
    };
    let end = (offset + limit).min(DEPTH).min(found.len());
    let next_cursor = (end < found.len() && end < DEPTH).then(|| {
        cursor::encode(
            catalog.generation(),
            &context,
            end,
            &position(&found[end - 1]),
        )
    });
    let items = found[offset..end]
        .iter()
        .map(|e| hydrate.state(e))
        .collect::<Result<_, _>>()?;
    Ok(serde_json::to_value(StatesPage { items, next_cursor }).expect("StatesPage serializes"))
}

/// A `variable_alias_window` row's content.
struct Window {
    provenance: Option<String>,
    per_column: bool,
    coded: bool,
    data_type: Option<String>,
    data_length: Option<String>,
    operational_definition: Option<String>,
    source_register_text: Option<String>,
    definition: Option<String>,
    measurement_unit: Option<String>,
    name: Option<String>,
    description: Option<String>,
    value_set_id: Option<i64>,
    value_set_version_label: String,
}

/// Reads a variable's state and window content, memoizing what states share.
struct Hydrate<'a> {
    conn: &'a Connection,
    variable_id: i64,
    warnings: Vec<Warning>,
    families: BTreeMap<String, (String, String)>,
    summaries: BTreeMap<i64, ValueSetSummary>,
}

impl<'a> Hydrate<'a> {
    fn new(conn: &'a Connection, variable_id: i64) -> Result<Self, Error> {
        let (register_id, provider, register): (i64, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT r.register_id, p.slug, r.slug FROM variable v \
                 JOIN register r USING(register_id) JOIN provider p USING(provider_id) \
                 WHERE v.variable_id = ?",
                [variable_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        let warnings = conn
            .prepare(
                "SELECT lower(hex(warning_id)), register_variant_id, delivery_column_name, \
                 valid_from, valid_to FROM data_warning \
                 WHERE register_id = ? AND variable_id = ? ORDER BY warning_id",
            )?
            .query_map([register_id, variable_id], |row| {
                Ok(Warning {
                    id: row.get(0)?,
                    variant: row.get(1)?,
                    column_fold: row.get::<_, Option<String>>(2)?.map(|c| fold_identity(&c)),
                    valid_from: row.get(3)?,
                    valid_to: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        let families = match (provider, register) {
            (Some(provider), Some(register)) => {
                variant_families(conn, register_id, &provider, &register)?
            }
            _ => BTreeMap::new(),
        };
        Ok(Self {
            conn,
            variable_id,
            warnings,
            families,
            summaries: BTreeMap::new(),
        })
    }

    fn window(&self, e: &Emitted) -> Result<Option<Window>, Error> {
        let Some(start) = &e.window_valid_from else {
            return Ok(None);
        };
        Ok(Some(self.conn.prepare_cached(
            "SELECT provenance, column_metadata = 'per_column', coding_metadata = 'per_column', \
             data_type, data_length, operational_definition, source_register_text, definition, \
             measurement_unit, name, description, value_set_id, value_set_version_label \
             FROM variable_alias_window WHERE variable_id = ? AND register_variant_id = ? \
             AND delivery_column_name = ? AND valid_from = ?",
        )?
        .query_row(
            params![self.variable_id, e.register_variant_id, e.delivery_column_name, start],
            |row| {
                Ok(Window {
                    provenance: row.get(0)?,
                    per_column: row.get(1)?,
                    coded: row.get(2)?,
                    data_type: row.get(3)?,
                    data_length: row.get(4)?,
                    operational_definition: row.get(5)?,
                    source_register_text: row.get(6)?,
                    definition: row.get(7)?,
                    measurement_unit: row.get(8)?,
                    name: row.get(9)?,
                    description: row.get(10)?,
                    value_set_id: row.get(11)?,
                    value_set_version_label: row.get(12)?,
                })
            },
        )?))
    }

    /// The representation's value-set version label: a coded window's own, else its
    /// state's.
    fn version_label(&self, e: &Emitted) -> Result<String, Error> {
        value_set(self.conn, self.variable_id, e).map(|(_, label)| label)
    }

    fn state(&mut self, e: &Emitted) -> Result<State, Error> {
        let mut state = self
            .conn
            .prepare_cached(
                "SELECT rv.slug, rv.name, s.data_type, s.data_length, s.source_register_text, \
             s.operational_definition, s.definition, s.measurement_unit, s.name, s.description, \
             s.provenance, s.pooled, s.value_set_version_label, s.value_set_id, v.is_identifier \
             FROM variable_state s JOIN variable v USING(variable_id) \
             JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id \
             WHERE s.state_id = ?",
            )?
            .query_row([e.state_id], |row| self.base(row, e))?;
        let classifications = match self.window(e)? {
            Some(window) => {
                state.operational_definition = None;
                if window.provenance.is_some() {
                    state.provenance = window.provenance;
                }
                if window.per_column {
                    state.data_type = window.data_type;
                    state.data_length = window.data_length;
                    state.operational_definition = window.operational_definition;
                    state.source_register_text = window.source_register_text;
                    state.definition = window.definition;
                    state.measurement_unit = window.measurement_unit;
                    state.name = window.name;
                    state.description = window.description;
                }
                if window.coded {
                    state.value_set_id = window.value_set_id;
                    state.value_set_version_label = window.value_set_version_label;
                    state.coding_window_from.clone_from(&e.window_valid_from);
                    self.window_classifications(e)?
                } else {
                    self.state_classifications(e.state_id)?
                }
            }
            None => self.state_classifications(e.state_id)?,
        };
        state.classifications = classifications;
        state.value_set_summary = state.value_set_id.map(|id| self.summary(id)).transpose()?;
        state.warning_ids = warning_ids(&self.warnings, e);
        Ok(state)
    }

    fn base(&self, row: &Row, e: &Emitted) -> rusqlite::Result<State> {
        let variant: Option<String> = row.get(0)?;
        let variant = variant.ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(
                0,
                "register_variant.slug".into(),
                rusqlite::types::Type::Null,
            )
        })?;
        let (family, family_label) = self
            .families
            .get(&variant)
            .cloned()
            .map_or((None, None), |(key, label)| (Some(key), Some(label)));
        let period_token = match (&e.valid_from, &e.valid_to) {
            (Some(from), Some(to)) if to != OPEN_ENDED => Some(period_token_for_bounds(from, to)),
            (Some(_), Some(_)) => None,
            _ => Some("_default".to_owned()),
        };
        Ok(State {
            state_id: e.state_id,
            variant,
            variant_label: row.get(1)?,
            variant_family: family,
            variant_family_label: family_label,
            register_variant_id: e.register_variant_id,
            period_scope: e.period_scope.clone(),
            valid_from: e.valid_from.clone(),
            valid_to: e.valid_to.clone(),
            data_type: row.get(2)?,
            data_length: row.get(3)?,
            delivery_column_name: e.delivery_column_name.clone(),
            source_register_text: row.get(4)?,
            operational_definition: row.get(5)?,
            definition: row.get(6)?,
            measurement_unit: row.get(7)?,
            name: row.get(8)?,
            description: row.get(9)?,
            warning_ids: Vec::new(),
            provenance: row.get(10)?,
            pooled: row.get(11)?,
            value_set_version_label: row.get(12)?,
            coding_window_from: None,
            value_set_id: row.get(13)?,
            value_set: None,
            value_set_summary: None,
            is_identifier: row.get(14)?,
            classifications: Vec::new(),
            period_token,
        })
    }

    fn state_classifications(&self, state_id: i64) -> Result<Vec<StateClassification>, Error> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT c.id, c.slug, c.short_name, c.name, sc.provenance, cf.status, \
             cf.checked_code_count, cf.matched_code_count, cf.nonconforming_code_count, cf.overlap, \
             (SELECT COUNT(DISTINCT vc.code) FROM classification_conformance_code ccc \
              JOIN value_code vc USING(code_id) WHERE ccc.state_id = sc.state_id \
              AND ccc.declared_classification_id = c.id AND ccc.member_kind = 'nonstandard'), \
             (SELECT COUNT(DISTINCT vc.code) FROM classification_conformance_code ccc \
              JOIN value_code vc USING(code_id) WHERE ccc.state_id = sc.state_id \
              AND ccc.declared_classification_id = c.id AND ccc.member_kind = 'sentinel') \
             FROM state_classification sc JOIN classification c ON c.id = sc.classification_id \
             LEFT JOIN classification_conformance cf ON cf.state_id = sc.state_id \
             AND cf.declared_classification_id = sc.classification_id \
             WHERE sc.state_id = ? ORDER BY c.slug",
        )?;
        let found = stmt
            .query_map([state_id], |row| {
                let (slug, short_name, name): (String, String, String) =
                    (row.get(1)?, row.get(2)?, row.get(3)?);
                let conformance = row
                    .get::<_, Option<String>>(5)?
                    .map(|status| {
                        Ok::<_, rusqlite::Error>(Conformance {
                            declared_classification_slug: slug.clone(),
                            declared_classification_short_name: short_name.clone(),
                            declared_classification_name: name.clone(),
                            status,
                            checked_code_count: row.get(6)?,
                            matched_code_count: row.get(7)?,
                            nonconforming_code_count: row.get(8)?,
                            overlap: row.get(9)?,
                            nonstandard_code_count: row.get(10)?,
                            sentinel_code_count: row.get(11)?,
                            nonconforming_codes: Vec::new(),
                        })
                    })
                    .transpose()?;
                Ok(StateClassification {
                    slug,
                    short_name,
                    name,
                    provenance: row.get(4)?,
                    conformance,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(found)
    }

    /// A coded window's classifications, the conformance counted from its stored
    /// evidence (today's `_alias_classifications`).
    fn window_classifications(&self, e: &Emitted) -> Result<Vec<StateClassification>, Error> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT c.slug, c.short_name, c.name, ac.provenance, ac.conformance \
             FROM alias_window_classification ac JOIN classification c ON c.id = ac.classification_id \
             WHERE ac.variable_id = ? AND ac.register_variant_id = ? \
             AND ac.delivery_column_name = ? AND ac.valid_from = ? ORDER BY c.slug",
        )?;
        let rows: Vec<Link> = stmt
            .query_map(
                params![
                    self.variable_id,
                    e.register_variant_id,
                    e.delivery_column_name,
                    e.window_valid_from
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<_>>()?;
        rows.into_iter()
            .map(|(slug, short_name, name, provenance, evidence)| {
                let conformance = evidence
                    .map(|evidence| alias_conformance(&slug, &short_name, &name, &evidence))
                    .transpose()?;
                Ok(StateClassification {
                    slug,
                    short_name,
                    name,
                    provenance,
                    conformance,
                })
            })
            .collect()
    }

    /// `value_set_summary`, once per distinct value set.
    fn summary(&mut self, value_set_id: i64) -> Result<ValueSetSummary, Error> {
        if let Some(summary) = self.summaries.get(&value_set_id) {
            return Ok(summary.clone());
        }
        let members: Vec<(String, String)> = self
            .conn
            .prepare_cached(
                "SELECT vc.code, vc.label FROM value_set_member vsm \
                 JOIN value_code vc ON vsm.code_id = vc.code_id WHERE vsm.value_set_id = ?",
            )?
            .query_map([value_set_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let summary = ValueSetSummary {
            code_count: members.len(),
            integer_range: dense_integer_range(&members),
        };
        self.summaries.insert(value_set_id, summary.clone());
        Ok(summary)
    }
}

/// A coded window's stored conformance evidence (today's `_AliasConformanceEvidence`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AliasEvidence {
    declared_classification: String,
    status: AliasStatus,
    checked_codes: Vec<String>,
    #[serde(default)]
    pub nonconforming_members: Vec<(String, String)>,
    #[serde(default)]
    pub sentinel_members: Vec<(String, String)>,
    /// Per-code evidence, read by `values`' extension members.
    #[serde(default)]
    pub scoped_sentinels: Vec<ScopedSentinel>,
}

/// A coded window's evidence, which must declare `slug`; corrupt evidence, or
/// evidence declaring another book, is an `internal_error`.
pub(super) fn alias_evidence(slug: &str, evidence: &str) -> Result<AliasEvidence, Error> {
    let corrupt = |detail: String| {
        Error::new(
            Code::InternalError,
            format!("Unreadable alias conformance for {slug}: {detail}"),
            vec![],
        )
    };
    let evidence: AliasEvidence =
        serde_json::from_str(evidence).map_err(|err| corrupt(err.to_string()))?;
    if evidence.declared_classification != slug {
        return Err(corrupt(format!(
            "it declares {:?}",
            evidence.declared_classification
        )));
    }
    Ok(evidence)
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum AliasStatus {
    Conforming,
    Extended,
}

/// The conformance counts of a coded window from its evidence ([`alias_evidence`]).
fn alias_conformance(
    slug: &str,
    short_name: &str,
    name: &str,
    evidence: &str,
) -> Result<Conformance, Error> {
    let evidence = alias_evidence(slug, evidence)?;
    let codes = |members: &[(String, String)]| -> BTreeSet<String> {
        members.iter().map(|(code, _)| code.clone()).collect()
    };
    let checked: BTreeSet<&String> = evidence.checked_codes.iter().collect();
    let checked = i64::try_from(checked.len()).expect("a count fits");
    let nonstandard = codes(&evidence.nonconforming_members);
    let sentinel = codes(&evidence.sentinel_members);
    let unmatched = i64::try_from(nonstandard.union(&sentinel).count()).expect("a count fits");
    #[allow(clippy::cast_precision_loss)]
    let overlap = if checked == 0 {
        1.0
    } else {
        (checked - unmatched) as f64 / checked as f64
    };
    Ok(Conformance {
        declared_classification_slug: slug.to_owned(),
        declared_classification_short_name: short_name.to_owned(),
        declared_classification_name: name.to_owned(),
        status: match evidence.status {
            AliasStatus::Conforming => "conforming",
            AliasStatus::Extended => "extended",
        }
        .to_owned(),
        checked_code_count: checked,
        matched_code_count: checked - unmatched,
        nonconforming_code_count: unmatched,
        overlap,
        nonstandard_code_count: i64::try_from(nonstandard.len()).expect("a count fits"),
        sentinel_code_count: i64::try_from(sentinel.len()).expect("a count fits"),
        nonconforming_codes: Vec::new(),
    })
}

/// Today's `dense_integer_range`: the span of a value set of at least 10 distinct
/// canonical integer codes (within JavaScript's safe integers), each labelled by
/// nothing but its number, covering at least 90% of their span.
fn dense_integer_range(members: &[(String, String)]) -> Option<IntegerRange> {
    const MIN_COUNT: usize = 10;
    const MAX_SAFE: i64 = (1 << 53) - 1;
    if members.len() < MIN_COUNT {
        return None;
    }
    let mut seen = BTreeSet::new();
    for (code, label) in members {
        let code = reg_core::py_strip(code);
        let digits = code.strip_prefix('-').unwrap_or(code);
        let canonical = !digits.is_empty()
            && digits.bytes().all(|b| b.is_ascii_digit())
            && (digits == "0" || !digits.starts_with('0'));
        let value = canonical
            .then(|| code.parse::<i64>().ok())
            .flatten()
            .filter(|v| v.abs() <= MAX_SAFE)?;
        let label = fold_identity(reg_core::py_strip(label));
        let restates = label.is_empty()
            || [
                format!("{value}"),
                format!("{value} år"),
                format!("{value} ar"),
                format!("{value} year"),
                format!("{value} years"),
                format!("{value} yr"),
                format!("{value} yrs"),
                format!("age {value}"),
                format!("ålder {value}"),
            ]
            .contains(&label);
        if !restates || !seen.insert(value) {
            return None;
        }
    }
    let (min, max) = (*seen.first()?, *seen.last()?);
    #[allow(clippy::cast_precision_loss)]
    let dense = seen.len() as f64 / (max - min + 1) as f64 >= 0.9;
    dense.then_some(IntegerRange { min, max })
}
