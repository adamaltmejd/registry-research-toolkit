//! Opaque evidence and the guard digest basis.
//!
//! Curation guards (`expected_*_sha256`) pin the records they rely on by
//! `evidence_sha256` (`reg_meta_build/src/reg_meta_build/source_evidence.py`): the
//! SHA-256 of Python canonical JSON (`json.dumps(sort_keys=True, ensure_ascii=False,
//! separators=(",", ":"))`) of the record's full Pydantic JSON dump, with two per-type
//! rules. Fields a type lists as `evidence_position` are dropped (`record_id` and
//! `source_revision_id` on a record; `physical_record` and `physical_cells` on a
//! locator), and lists it lists as `evidence_multiset` (a record's `locators`) are
//! sorted by their encoded canonical JSON.
//!
//! The Rust builder reproduces this basis exactly rather than re-pinning every guard:
//! the guards stay curation data, unchanged across the port, and a converted entry that
//! selects the same records carries the same digest. `tests/digest_golden.rs` pins it
//! against digests the Python builder computed.

use std::sync::Arc;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::model::{FieldStatus, Scalar, StatedField};
use crate::prepared::{
    Cell, Coordinate, CoordinateStatus, FIELD_NAMES, Fields, Locator, NativeId, Parent, Record,
    Scope, ScopeKind,
};

/// A prepared record behind an opaque handle: provider ids and delivered cells live
/// here and nowhere else in the model. Rules and curation can compare and digest it,
/// never read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence(Arc<Record>);

impl Evidence {
    #[must_use]
    pub fn new(record: Record) -> Self {
        Self(Arc::new(record))
    }

    /// The record's `evidence_sha256`, the basis of every record guard.
    #[must_use]
    pub fn sha256(&self) -> String {
        record_evidence_sha256(&self.0)
    }

    /// The record id, which names the record in located errors and the build report.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.0.record_id
    }

    /// Normalize reads the record it wraps; nothing after normalize may.
    #[expect(dead_code, reason = "normalize reads evidence (phase 3, slice 1)")]
    pub(crate) fn record(&self) -> &Record {
        &self.0
    }
}

/// Python `canonical_json`. `serde_json` without `preserve_order` keeps object keys in
/// byte order, which for UTF-8 is code-point order, as `sort_keys` sorts; both escape
/// only `"`, `\` and C0 controls (short forms for `\b\f\n\r\t`, else lowercase
/// `\u00xx`) when Python's `ensure_ascii` is off.
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    value.to_string()
}

/// Python `canonical_sha256`: lowercase hex SHA-256 of [`canonical_json`].
#[must_use]
pub fn canonical_sha256(value: &Value) -> String {
    let digest = Sha256::digest(canonical_json(value).as_bytes());
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// `evidence_sha256(record)` for a prepared record.
#[must_use]
pub fn record_evidence_sha256(record: &Record) -> String {
    canonical_sha256(&record_evidence(record))
}

/// The evidence encoding of a `SourceRecord`: its JSON dump by alias, every default
/// present, minus `record_id` and `source_revision_id`, locators as a sorted multiset.
fn record_evidence(record: &Record) -> Value {
    let mut locators: Vec<Value> = record.locators.iter().map(locator).collect();
    locators.sort_by_cached_key(canonical_json);
    json!({
        "source": record.source,
        "locators": locators,
        "subject": {
            "provider": record.provider,
            "register": coordinate(&record.register),
            "variant": coordinate(&record.variant),
            "variant_references": record.variant_references.iter().map(coordinate).collect::<Vec<_>>(),
            "population": coordinate(&record.population),
            "variable": coordinate(&record.variable),
            "member": coordinate(&record.member),
            "native": {
                "register_id": record.native.register_id,
                "register_variant_id": record.native.register_variant_id,
                "edition_id": record.native.edition_id,
                "variable_id": record.native.variable_id,
                "member_id": record.native.member_id,
            },
        },
        "edition_scope": scope(&record.edition_scope),
        "edition_period_scope": scope(&record.edition_period_scope),
        "fields": fields(&record.fields),
        "parent_facts": record.parent_facts.iter().map(parent).collect::<Vec<_>>(),
        "language": record.language.map(crate::prepared::Language::as_str),
        "original_period_text": record.original_period_text,
        "context": record.context,
        "delivered_cells": record.delivered_cells.iter().map(cell).collect::<Vec<_>>(),
    })
}

/// A locator without its `evidence_position` fields.
fn locator(locator: &Locator) -> Value {
    json!({
        "semantic_record_key": locator.semantic_record_key,
        "physical_file": locator.physical_file,
        "physical_table": locator.physical_table,
    })
}

fn coordinate(coordinate: &Coordinate) -> Value {
    json!({
        "status": match coordinate.status {
            CoordinateStatus::Value => "value",
            CoordinateStatus::Unknown => "unknown",
            CoordinateStatus::NotApplicable => "not_applicable",
        },
        "native_id": coordinate.native_id.as_ref().map(|id| match id {
            NativeId::Int(value) => Value::from(*value),
            NativeId::Str(value) => Value::from(value.as_str()),
        }),
        "name": coordinate.name,
    })
}

fn scope(scope: &Scope) -> Value {
    json!({
        "kind": match scope.kind {
            ScopeKind::NotApplicable => "not_applicable",
            ScopeKind::Unknown => "unknown",
            ScopeKind::YearIndependent => "year_independent",
            ScopeKind::Intervals => "intervals",
            ScopeKind::Pooled => "pooled",
        },
        "intervals": scope.intervals.iter().map(|interval| json!({
            "start": interval.start,
            "end": interval.end,
        })).collect::<Vec<_>>(),
        "label": scope.label,
        "pooled_start": scope.pooled_start,
        "pooled_end": scope.pooled_end,
    })
}

fn scalar(value: &Scalar) -> Value {
    match value {
        Scalar::Bool(value) => Value::from(*value),
        Scalar::Int(value) => Value::from(*value),
        Scalar::Str(value) => Value::from(value.as_str()),
    }
}

fn field(field: &StatedField) -> Value {
    json!({
        "status": match field.status {
            FieldStatus::Value => "value",
            FieldStatus::Unknown => "unknown",
            FieldStatus::Negative => "negative",
        },
        "value": field.value.as_ref().map(scalar),
        "raw_value": field.raw_value.as_ref().map(scalar),
    })
}

/// Every `SourceFields` name, null where unstated.
fn fields(fields: &Fields) -> Value {
    Value::Object(
        FIELD_NAMES
            .iter()
            .map(|&name| {
                (
                    (*name).to_owned(),
                    fields.get(name).map_or(Value::Null, field),
                )
            })
            .collect::<Map<_, _>>(),
    )
}

fn parent(parent: &Parent) -> Value {
    json!({
        "kind": parent.kind.as_str(),
        "coordinate": coordinate(&parent.coordinate),
        "register": coordinate(&parent.register),
        "variant": parent.variant.as_ref().map(coordinate),
        "edition": parent.edition.as_ref().map(coordinate),
        "fields": fields(&parent.fields),
        "field_cells": parent.field_cells.iter().map(|cells| json!({
            "field": cells.field,
            "positions": cells.positions,
        })).collect::<Vec<_>>(),
    })
}

/// A delivered cell; the cached source value and type are dumped only when present
/// (`exclude_if` on `DeliveredCell`).
fn cell(cell: &Cell) -> Value {
    let mut value = json!({
        "name": cell.name,
        "present": cell.present,
        "raw_value": cell.raw_value,
        "interpreted_value": cell.interpreted_value,
        "raw_type": cell.raw_type,
        "storage_type": cell.storage_type,
        "number_format": cell.number_format,
        "hyperlink_target": cell.hyperlink_target,
        "hyperlink_location": cell.hyperlink_location,
    });
    if let Value::Object(map) = &mut value {
        if let Some(cached) = &cell.cached_raw_value {
            map.insert("cached_raw_value".to_owned(), Value::from(cached.as_str()));
        }
        if let Some(cached) = &cell.cached_raw_type {
            map.insert("cached_raw_type".to_owned(), Value::from(cached.as_str()));
        }
    }
    value
}
