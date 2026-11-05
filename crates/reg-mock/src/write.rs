//! Write a generated [`Table`] the way `reg_mona_runner` writes the real file
//! (`reg_mona_runner/DESIGN.md`, "CSV" and "Types per format"), so analysis code reads
//! mock and real files alike. Every file says it is synthetic inside (a `.dta` dataset
//! label, Parquet key-value metadata); CSV has no place for it, so `mock_manifest.json`
//! beside the files says it for all.

use std::fmt::Write as _;
use std::io::{Seek, Write};
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use dta::stata::dta::byte_order::ByteOrder;
use dta::stata::dta::dta_writer::DtaWriter;
use dta::stata::dta::header::Header;
use dta::stata::dta::release::Release;
use dta::stata::dta::schema::Schema as DtaSchema;
use dta::stata::dta::value::Value;
use dta::stata::dta::variable::Variable;
use dta::stata::dta::variable_type::VariableType;
use dta::stata::missing_value::MissingValue;
use dta::stata::stata_double::StataDouble;
use dta::stata::stata_long::StataLong;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use std::path::Path;

use reg_core::project::ExtractFormat;

use crate::Refusal;
use crate::generate::{Cell, Kind, Table};
use crate::spec::{Spec, SpecColumn};

/// The marker every mock file carries.
pub const MARKER: &str = "Synthetic mock data from reg-meta mock: not real records";

/// Stata dates count days from 1960-01-01; this crate's from 1970-01-01.
const STATA_EPOCH_DAYS: i64 = 3653;

/// The longest `str` a `.dta` file holds; longer text would be a `strL`, which mocks
/// never draw.
const MAX_STR: usize = 2045;

/// Write every table into `dir` in the spec's format, then `mock_manifest.json`.
///
/// # Panics
///
/// Never: the manifest is plain JSON.
///
/// # Errors
///
/// A located refusal for an I/O error or a value the format cannot hold.
pub fn write_all(
    spec: &Spec,
    tables: &[(String, Table)],
    dir: &Path,
    seed: u64,
    persons: u64,
) -> Result<(), Refusal> {
    std::fs::create_dir_all(dir)
        .map_err(|e| Refusal::new("io", dir.display().to_string(), e.to_string()))?;
    let extension = spec.format.as_str();
    let mut written = Vec::new();
    for (file, (name, table)) in spec.files.iter().zip(tables) {
        let path = dir.join(format!("{name}.{extension}"));
        let at = path.display().to_string();
        let mut out = std::io::BufWriter::new(
            std::fs::File::create(&path)
                .map_err(|e| Refusal::new("io", at.clone(), e.to_string()))?,
        );
        match spec.format {
            ExtractFormat::Csv => csv(&mut out, &file.columns, table),
            ExtractFormat::Parquet => parquet(&mut out, &file.columns, table),
            ExtractFormat::Dta => dta(&mut out, &file.columns, table),
        }
        .and_then(|()| out.flush().map_err(|e| e.to_string()))
        .map_err(|message| Refusal::new("write_failed", at, message))?;
        written.push(
            serde_json::json!({"file": format!("{name}.{extension}"), "rows": table.rows.len()}),
        );
    }
    let manifest = serde_json::json!({
        "note": MARKER,
        "project": spec.project.name,
        "seed": seed,
        "persons": persons,
        "files": written,
    });
    let mut text = serde_json::to_string_pretty(&manifest).expect("serializes");
    text.push('\n');
    let path = dir.join("mock_manifest.json");
    std::fs::write(&path, text)
        .map_err(|e| Refusal::new("io", path.display().to_string(), e.to_string()))
}

/// # Errors
///
/// An I/O error, or a value the format cannot hold (as the runner refuses it).
pub fn csv(out: &mut impl Write, columns: &[SpecColumn], table: &Table) -> Result<(), String> {
    let mut text = String::new();
    let header: Vec<String> = columns.iter().map(|c| csv_field(&c.name)).collect();
    text.push_str(&header.join(","));
    text.push('\n');
    for row in &table.rows {
        for (i, (cell, kind)) in row.iter().zip(&table.kinds).enumerate() {
            if i > 0 {
                text.push(',');
            }
            match cell {
                Cell::Null => {}
                Cell::Int(n) => write!(text, "{n}").expect("a String"),
                Cell::Real(x) => text.push_str(&real(*x, *kind)),
                Cell::Text(s) => text.push_str(&csv_field(s)),
                Cell::Date(day) => text.push_str(&reg_core::issue::iso_day(*day)),
                Cell::DateTime(ms) => text.push_str(&date_time(*ms)),
                Cell::Bool(b) => text.push(if *b { '1' } else { '0' }),
            }
        }
        text.push('\n');
    }
    out.write_all(text.as_bytes()).map_err(|e| e.to_string())
}

/// Quoted only when it must be: empty (unlike NULL), or holding a quote, comma, line
/// break or edge whitespace.
fn csv_field(s: &str) -> String {
    let must = s.is_empty()
        || s.contains(['"', ',', '\n', '\r'])
        || s.starts_with(char::is_whitespace)
        || s.ends_with(char::is_whitespace);
    if must {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

/// A decimal at its scale; a float to 15 significant digits, never in exponent form.
fn real(x: f64, kind: Kind) -> String {
    if let Kind::Decimal(_, scale) = kind {
        format!("{x:.*}", usize::from(scale))
    } else {
        let digits = if x == 0.0 {
            0
        } else {
            #[allow(clippy::cast_possible_truncation)]
            let magnitude = x.abs().log10().floor() as i32;
            usize::try_from((14 - magnitude).max(0)).unwrap_or(0)
        };
        let fixed = format!("{x:.digits$}");
        if fixed.contains('.') {
            fixed.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            fixed
        }
    }
}

fn date_time(ms: i64) -> String {
    let (day, rest) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000) / 1000);
    format!(
        "{} {:02}:{:02}:{:02}",
        reg_core::issue::iso_day(day),
        rest / 3600,
        rest / 60 % 60,
        rest % 60
    )
}

/// # Errors
///
/// An Arrow or Parquet error.
pub fn parquet(
    out: &mut (impl Write + Send),
    columns: &[SpecColumn],
    table: &Table,
) -> Result<(), String> {
    let err = |e: &dyn std::fmt::Display| e.to_string();
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (i, (column, kind)) in columns.iter().zip(&table.kinds).enumerate() {
        let cells = table.rows.iter().map(|r| &r[i]);
        let (data_type, array): (DataType, ArrayRef) = match *kind {
            Kind::Int32 => {
                let mut b = Int32Builder::new();
                for c in cells {
                    b.append_option(int(c).map(|n| i32::try_from(n).unwrap_or(i32::MAX)));
                }
                (DataType::Int32, Arc::new(b.finish()))
            }
            Kind::Int64 => {
                let mut b = Int64Builder::new();
                cells.for_each(|c| b.append_option(int(c)));
                (DataType::Int64, Arc::new(b.finish()))
            }
            Kind::Decimal(p, s) => {
                let mut b = Decimal128Builder::new();
                for c in cells {
                    #[allow(clippy::cast_possible_truncation)]
                    b.append_option(
                        float(c).map(|x| (x * 10f64.powi(i32::from(s))).round() as i128),
                    );
                }
                let array = b
                    .finish()
                    .with_precision_and_scale(p, i8::try_from(s).map_err(|e| err(&e))?)
                    .map_err(|e| err(&e))?;
                (
                    DataType::Decimal128(p, i8::try_from(s).map_err(|e| err(&e))?),
                    Arc::new(array),
                )
            }
            Kind::Float64 => {
                let mut b = Float64Builder::new();
                cells.for_each(|c| b.append_option(float(c)));
                (DataType::Float64, Arc::new(b.finish()))
            }
            Kind::Float32 => {
                let mut b = Float32Builder::new();
                #[allow(clippy::cast_possible_truncation)]
                cells.for_each(|c| b.append_option(float(c).map(|x| x as f32)));
                (DataType::Float32, Arc::new(b.finish()))
            }
            Kind::Bool => {
                let mut b = BooleanBuilder::new();
                cells.for_each(|c| {
                    b.append_option(if let Cell::Bool(v) = c {
                        Some(*v)
                    } else {
                        None
                    });
                });
                (DataType::Boolean, Arc::new(b.finish()))
            }
            Kind::Date => {
                let mut b = Date32Builder::new();
                for c in cells {
                    b.append_option(day(c).map(|d| i32::try_from(d).unwrap_or(i32::MAX)));
                }
                (DataType::Date32, Arc::new(b.finish()))
            }
            Kind::DateTime => {
                let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
                cells.for_each(|c| b.append_option(ms(c).map(|m| m * 1000)));
                (
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    Arc::new(b.finish()),
                )
            }
            Kind::Text => {
                let mut b = StringBuilder::new();
                cells.for_each(|c| {
                    b.append_option(if let Cell::Text(s) = c { Some(s) } else { None });
                });
                (DataType::Utf8, Arc::new(b.finish()))
            }
        };
        fields.push(Field::new(&column.name, data_type, true));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| err(&e))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("reg-meta mock".to_owned())
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "reg_mock".to_owned(),
            MARKER.to_owned(),
        )]))
        .build();
    let options = ArrowWriterOptions::new().with_properties(properties);
    let mut writer =
        ArrowWriter::try_new_with_options(out, schema, options).map_err(|e| err(&e))?;
    writer.write(&batch).map_err(|e| err(&e))?;
    writer.close().map_err(|e| err(&e))?;
    Ok(())
}

/// Stata 14+ (format 118), as haven writes it: per the runner's type table, `int` a
/// long, `bigint` and decimals of up to 15 digits a double, wider decimals text, days
/// `%td`, times `%tc`, text `str` with NULL as `""`. A column is named its `stata_name`
/// when it has one, labelled with the full name.
///
/// # Errors
///
/// An I/O or format error, or a value Stata cannot hold exactly (as the runner refuses).
pub fn dta(out: impl Write + Seek, columns: &[SpecColumn], table: &Table) -> Result<(), String> {
    let err = |e: &dyn std::fmt::Display| e.to_string();
    let mut schema = DtaSchema::builder();
    for (i, (column, kind)) in columns.iter().zip(&table.kinds).enumerate() {
        let (variable_type, format) = match *kind {
            Kind::Int32 | Kind::Bool => (VariableType::Long, "%12.0g".to_owned()),
            Kind::Decimal(p, _) if p > 15 => (
                VariableType::FixedString(str_width(table, i, *kind)?),
                String::new(),
            ),
            Kind::Int64 | Kind::Decimal(..) => (VariableType::Double, "%20.0g".to_owned()),
            Kind::Float64 | Kind::Float32 => (VariableType::Double, "%10.0g".to_owned()),
            Kind::Date => (VariableType::Long, "%td".to_owned()),
            Kind::DateTime => (VariableType::Double, "%tc".to_owned()),
            Kind::Text => (
                VariableType::FixedString(str_width(table, i, *kind)?),
                String::new(),
            ),
        };
        let format = if format.is_empty() {
            let VariableType::FixedString(w) = variable_type else {
                unreachable!()
            };
            format!("%-{w}s")
        } else {
            format
        };
        let name = column.stata_name.as_deref().unwrap_or(&column.name);
        let mut variable = Variable::builder(variable_type, name).format(format);
        if let Some(label) = &column.label {
            variable = variable.label(label.clone());
        }
        schema = schema.add_variable(variable);
    }
    let header = Header::builder(Release::V118, ByteOrder::LittleEndian)
        .dataset_label(MARKER)
        .build();
    let mut records = DtaWriter::new()
        .from_writer(out)
        .write_header(header)
        .map_err(|e| err(&e))?
        .write_schema(schema.build().map_err(|e| err(&e))?)
        .map_err(|e| err(&e))?
        .into_record_writer()
        .map_err(|e| err(&e))?;
    let missing_long = Value::Long(StataLong::Missing(MissingValue::System));
    let missing_double = Value::Double(StataDouble::Missing(MissingValue::System));
    for row in &table.rows {
        let mut values = Vec::with_capacity(row.len());
        for (cell, kind) in row.iter().zip(&table.kinds) {
            values.push(match (*kind, cell) {
                (Kind::Decimal(p, _), Cell::Real(x)) if p > 15 => {
                    Value::String(real(*x, *kind).into())
                }
                (Kind::Text, Cell::Null) => Value::string(""),
                (Kind::Decimal(p, _), Cell::Null) if p > 15 => Value::string(""),
                (Kind::Text, Cell::Text(s)) => Value::String(s.as_str().into()),
                (Kind::Int32 | Kind::Date | Kind::Bool, Cell::Null) => missing_long.clone(),
                (_, Cell::Null) => missing_double.clone(),
                (Kind::Int32, Cell::Int(n)) => {
                    // Stata reserves the longs above 2147483620 for missing values.
                    let n = i32::try_from(*n)
                        .ok()
                        .filter(|n| *n <= 2_147_483_620)
                        .ok_or("a long past Stata's range: order it as Parquet or CSV")?;
                    Value::Long(StataLong::Present(n))
                }
                (Kind::Bool, Cell::Bool(b)) => Value::Long(StataLong::Present(i32::from(*b))),
                (Kind::Date, Cell::Date(d)) => Value::Long(StataLong::Present(
                    i32::try_from(d + STATA_EPOCH_DAYS).map_err(|e| err(&e))?,
                )),
                (Kind::DateTime, Cell::DateTime(m)) => {
                    #[allow(clippy::cast_precision_loss)]
                    let stata_ms = (m + STATA_EPOCH_DAYS * 86_400_000) as f64;
                    Value::Double(StataDouble::Present(stata_ms))
                }
                (_, Cell::Int(n)) => {
                    if n.unsigned_abs() >= 1 << 53 {
                        return Err("an integer at or past 2^53: order it as Parquet or CSV".into());
                    }
                    #[allow(clippy::cast_precision_loss)]
                    Value::Double(StataDouble::Present(*n as f64))
                }
                (_, Cell::Real(x)) => Value::Double(StataDouble::Present(*x)),
                (kind, cell) => return Err(format!("a {cell:?} in a {kind:?} column")),
            });
        }
        records.write_record(&values).map_err(|e| err(&e))?;
    }
    records
        .into_long_string_writer()
        .map_err(|e| err(&e))?
        .into_value_label_writer()
        .map_err(|e| err(&e))?
        .finish()
        .map_err(|e| err(&e))?;
    Ok(())
}

/// The widest value in bytes, at least 1, as haven sizes a `str`.
fn str_width(table: &Table, i: usize, kind: Kind) -> Result<u16, String> {
    let width = table
        .rows
        .iter()
        .map(|r| match &r[i] {
            Cell::Text(s) => s.len(),
            Cell::Real(x) => real(*x, kind).len(),
            _ => 0,
        })
        .max()
        .unwrap_or(0)
        .max(1);
    if width > MAX_STR {
        return Err(format!("a text longer than {MAX_STR} bytes"));
    }
    u16::try_from(width).map_err(|e| e.to_string())
}

fn int(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Int(n) => Some(*n),
        _ => None,
    }
}

fn float(cell: &Cell) -> Option<f64> {
    match cell {
        Cell::Real(x) => Some(*x),
        #[allow(clippy::cast_precision_loss)]
        Cell::Int(n) => Some(*n as f64),
        _ => None,
    }
}

fn day(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Date(d) => Some(*d),
        _ => None,
    }
}

fn ms(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::DateTime(m) => Some(*m),
        _ => None,
    }
}
