//! The saved catalog answers `compile` reads: HTTP API response documents (`{data,
//! meta}`) saved verbatim by `curl`. Only the fields compile uses are read, and unknown
//! fields are ignored, since these are another contract's documents
//! (conformance/api/operations.toml), not this crate's.

use reg_core::issue::Period;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Envelope<T> {
    pub data: T,
    pub meta: Meta,
}

#[derive(Deserialize)]
pub struct Meta {
    pub generation: String,
}

/// `POST /api/project/order`.
#[derive(Deserialize)]
pub struct LogicalOrder {
    pub entries: Vec<Entry>,
    pub row_keys: Vec<RowKeys>,
}

#[derive(Deserialize)]
pub struct Entry {
    pub source: String,
    pub binding: usize,
    pub table: String,
    pub variable: String,
    pub period: Period,
    pub spelling: String,
}

#[derive(Deserialize)]
pub struct RowKeys {
    pub source: String,
    pub table: String,
    pub period: Period,
    pub columns: Vec<RowKeyColumn>,
}

#[derive(Deserialize)]
pub struct RowKeyColumn {
    pub variable: String,
    pub spelling: String,
}

/// A page of `GET /api/segments/{table}?variable=…`.
#[derive(Deserialize)]
pub struct SegmentPage {
    pub items: Vec<Segment>,
}

#[derive(Deserialize)]
pub struct Segment {
    pub variable: String,
    pub table: String,
    pub period: Period,
    #[serde(rename = "type", default)]
    pub dtype: Option<String>,
    #[serde(default)]
    pub length: Option<i64>,
    #[serde(default)]
    pub value_set: Option<ValueSetRef>,
    #[serde(default)]
    pub identifier: Option<bool>,
}

#[derive(Deserialize)]
pub struct ValueSetRef {
    pub hash: String,
}

/// A page of `GET /api/values?value_set=…`.
#[derive(Deserialize)]
pub struct ValuesPage {
    pub items: Vec<ValueItem>,
    pub value_set: ValueSetRef,
}

#[derive(Deserialize)]
pub struct ValueItem {
    pub code: String,
    pub label: Option<String>,
    pub sentinel: bool,
}
