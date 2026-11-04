//! The case corpus's folder format (`reg_export_gate/DESIGN.md`, "Case corpus")
//! written to disk: per name, a Parquet file from typed columns (with optional
//! file and column key-value metadata, bytes spliced before the footer and
//! repeated values), raw text (optionally repeated), or a folder.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::{Map, Value};

/// int64's NA in R; the corpus format makes this bit pattern a null.
const INT64_NA_BITS: u64 = 0x8000_0000_0000_0000;

/// A null cell, or the value `get` reads; any other JSON is a corpus error.
fn cell<'a, T>(v: &'a Value, get: impl Fn(&'a Value) -> Option<T>) -> Option<T> {
    if v.is_null() {
        None
    } else {
        Some(get(v).expect("a cell of the column's type"))
    }
}

fn int(v: &Value) -> Option<i64> {
    if v.is_null() {
        None
    } else {
        Some(v.as_i64().expect("an integer cell"))
    }
}

/// A double cell: null, `"NaN"`, `"0x"` + 16 hex digits (exact IEEE 754 bits,
/// high byte first; `0x8000000000000000` is a null), or a number.
fn double(v: &Value) -> Option<f64> {
    match v {
        Value::Null => None,
        Value::String(s) if s == "NaN" => Some(f64::NAN),
        Value::String(s) => {
            let hex = s.strip_prefix("0x").expect("a 0x bit pattern");
            assert_eq!(hex.len(), 16, "16 hex digits");
            let bits = u64::from_str_radix(hex, 16).expect("hex digits");
            (bits != INT64_NA_BITS).then(|| f64::from_bits(bits))
        }
        v => Some(v.as_f64().expect("a number cell")),
    }
}

fn array(ty: &str, values: &[Value]) -> (DataType, ArrayRef) {
    match ty {
        "int32" => (
            DataType::Int32,
            Arc::new(
                values
                    .iter()
                    .map(|v| int(v).map(|x| i32::try_from(x).expect("an int32 cell")))
                    .collect::<Int32Array>(),
            ),
        ),
        "int64" => (
            DataType::Int64,
            Arc::new(values.iter().map(int).collect::<Int64Array>()),
        ),
        "double" => (
            DataType::Float64,
            Arc::new(values.iter().map(double).collect::<Float64Array>()),
        ),
        "string" => (
            DataType::Utf8,
            Arc::new(
                values
                    .iter()
                    .map(|v| cell(v, Value::as_str))
                    .collect::<StringArray>(),
            ),
        ),
        "bool" => (
            DataType::Boolean,
            Arc::new(
                values
                    .iter()
                    .map(|v| cell(v, Value::as_bool))
                    .collect::<BooleanArray>(),
            ),
        ),
        other => panic!("unknown corpus type {other}"),
    }
}

/// `values`, or `repeat` copies of them in a row.
fn repeated<T: Clone>(values: &[T], repeat: Option<&Value>) -> Vec<T> {
    let n = repeat.map_or(1, |r| {
        usize::try_from(r.as_u64().expect("a repeat count")).unwrap()
    });
    (0..n).flat_map(|_| values.iter().cloned()).collect()
}

fn string_map(v: Option<&Value>) -> HashMap<String, String> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().expect("string metadata").to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn parquet(spec: &Map<String, Value>) -> Vec<u8> {
    let columns = spec["columns"].as_array().expect("columns");
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for col in columns {
        let name = col["name"].as_str().expect("a column name");
        let values = repeated(col["values"].as_array().expect("values"), col.get("repeat"));
        let (data_type, array) = array(col["type"].as_str().expect("a column type"), &values);
        fields
            .push(Field::new(name, data_type, true).with_metadata(string_map(col.get("metadata"))));
        arrays.push(array);
    }
    let file_metadata = string_map(spec.get("metadata"));
    let schema = Arc::new(Schema::new(fields).with_metadata(file_metadata));
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("a record batch");
    // Snappy, as R's arrow writes by default.
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).expect("a writer");
    writer.write(&batch).expect("written");
    writer.close().expect("closed");
    if let Some(text) = spec.get("inner_bytes").and_then(Value::as_str) {
        splice_before_footer(&mut bytes, text.as_bytes());
    }
    bytes
}

/// Splices `text` in just before the footer. The footer's offsets are absolute
/// and its length is unchanged, so readers still read the file; the bytes ride
/// along unread.
fn splice_before_footer(bytes: &mut Vec<u8>, text: &[u8]) {
    let n = bytes.len();
    let footer = u32::from_le_bytes(bytes[n - 8..n - 4].try_into().expect("4 bytes")) as usize;
    let at = n - 8 - footer;
    bytes.splice(at..at, text.iter().copied());
}

/// Writes a case folder description into `dir`, which must not exist.
pub fn materialize(dir: &Path, files: &Map<String, Value>) {
    std::fs::create_dir_all(dir).expect("a case folder");
    for (name, spec) in files {
        let path = dir.join(name);
        let spec = spec.as_object().expect("an entry");
        if spec.get("folder").and_then(Value::as_bool) == Some(true) {
            std::fs::create_dir(&path).expect("a folder");
        } else if let Some(text) = spec.get("text").and_then(Value::as_str) {
            let bytes = repeated(text.as_bytes(), spec.get("repeat"));
            std::fs::write(&path, bytes).expect("a text file");
        } else {
            std::fs::write(&path, parquet(spec)).expect("a Parquet file");
        }
    }
}
