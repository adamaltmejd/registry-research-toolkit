//! The paths, clearing a prior export, the rewrite, the receipt and the report
//! (`reg_export_gate/DESIGN.md`, "The export is a rewrite" and "Receipt").

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use serde::Serialize;
use serde_json::Value;

use crate::structure::Candidate;
use crate::{ColumnType, Finding, Policy, RECEIPT, Refusal, Schema, VERSION, sha256_hex};

const RECEIPT_FORMAT: &str = "reg-export-gate/receipt";
const REPORT: &str = "export_gate_report.json";
const DIAGNOSTICS: &str = ".diagnostics";
/// The rows per row group the gate writes, as the R gate does.
const ROW_GROUP_ROWS: usize = 1_048_576;

fn setup(rule: &'static str) -> Refusal {
    Refusal::setup(Finding::rule(rule))
}

/// The folder `output` sits in; a bare name sits in the current folder.
pub(crate) fn parent(output: &Path) -> PathBuf {
    match output.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// A path's identity on disk: device and inode, so two spellings of one folder
/// (case or Unicode form on a case-insensitive volume, a link) compare equal.
#[cfg(unix)]
fn identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
}

#[cfg(not(unix))]
fn identity(path: &Path) -> Option<PathBuf> {
    path.canonicalize().ok()
}

/// Whether `inner` (a resolved path) or a folder it is in is `outer`.
fn inside(inner: &Path, outer: &Path) -> bool {
    let Some(outer) = identity(outer) else {
        return false;
    };
    inner
        .ancestors()
        .any(|a| identity(a).as_ref() == Some(&outer))
}

/// Whether staging (resolved, if it exists), the output and the diagnostics
/// folder beside the output are apart: none is another of them or inside one.
/// Compared by identity on disk, and the output's name also case-blind against
/// `.diagnostics`, which may not exist yet. `None` if the output's folder
/// cannot be resolved or the output has no name.
fn apart(staging: Option<&Path>, output: &Path) -> Option<bool> {
    let parent = parent(output).canonicalize().ok()?;
    let name = output.file_name()?;
    let o = parent.join(name);
    let o = if o.exists() {
        o.canonicalize().ok()?
    } else {
        o
    };
    let d = parent.join(DIAGNOSTICS);
    let same = identity(&o).is_some_and(|o| identity(&d) == Some(o));
    let overlap = staging.is_some_and(|s| inside(&o, s) || inside(s, &o) || inside(s, &d));
    Some(!(name.eq_ignore_ascii_case(DIAGNOSTICS) || same || overlap))
}

/// Staging exists, the output's folder exists, and staging, the output and the
/// diagnostics folder beside it are apart, so nothing the gate clears or the
/// report it writes can land in staging, and the report never in the export.
pub(crate) fn check_paths(staging: &Path, output: &Path) -> Result<(), Refusal> {
    if !staging.is_dir() {
        return Err(setup("staging_missing"));
    }
    if !parent(output).is_dir() || output.file_name().is_none() {
        return Err(setup("output_parent_missing"));
    }
    let s = staging
        .canonicalize()
        .map_err(|_| setup("internal_error"))?;
    match apart(Some(&s), output) {
        Some(true) => Ok(()),
        Some(false) => Err(setup("paths_overlap")),
        None => Err(setup("internal_error")),
    }
}

/// Writes a new file, never through a link and never over an existing one: a
/// name planted in a folder the pipeline can write cannot redirect the write.
fn write_new(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)?.write_all(bytes)
}

/// The data file names a gate receipt lists, or `None` if `path` is not one.
fn receipt_files(path: &Path) -> Option<Vec<String>> {
    // Opened as staging files are: never through a link, never blocking.
    let crate::structure::Outcome::Ok(bytes) = crate::structure::read(path) else {
        return None;
    };
    let doc: Value = serde_json::from_slice(&bytes).ok()?;
    if doc.get("format").and_then(Value::as_str) != Some(RECEIPT_FORMAT) {
        return None;
    }
    let files = doc.get("files")?.as_array()?;
    if files.is_empty() {
        return None;
    }
    files
        .iter()
        .map(|f| {
            let name = f.get("name")?.as_str()?;
            crate::schema::is_file_name(name).then(|| name.to_owned())
        })
        .collect()
}

/// Removes the prior export in `output`, or an empty folder there, before
/// anything is checked, so a refused run never leaves an old export looking
/// current. A prior export is a gate receipt and only files it lists, all
/// regular files; the gate deletes exactly those. Anything else is not the
/// gate's to delete: `output_not_gate_export`, with nothing deleted.
pub(crate) fn clear_output(output: &Path) -> Result<(), Refusal> {
    let Ok(meta) = std::fs::symlink_metadata(output) else {
        return Ok(());
    };
    if !meta.is_dir() {
        return Err(setup("output_not_gate_export"));
    }
    let listing = || -> Result<Vec<(String, bool)>, Refusal> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(output).map_err(|_| setup("internal_error"))? {
            let entry = entry.map_err(|_| setup("internal_error"))?;
            let regular = entry.file_type().is_ok_and(|t| t.is_file());
            // A name that is not UTF-8 is never one a receipt lists.
            let name = entry.file_name().into_string().unwrap_or_default();
            entries.push((name, regular));
        }
        Ok(entries)
    };
    let present = listing()?;
    if !present.is_empty() {
        let receipt_regular = present.iter().any(|(n, r)| n == RECEIPT && *r);
        let listed = receipt_regular
            .then(|| receipt_files(&output.join(RECEIPT)))
            .flatten();
        let ours = listed.is_some_and(|listed| {
            present
                .iter()
                .all(|(n, r)| *r && (n == RECEIPT || listed.contains(n)))
        });
        if !ours {
            return Err(setup("output_not_gate_export"));
        }
        for (name, _) in &present {
            // A file that will not go is caught by the listing below.
            let _ = std::fs::remove_file(output.join(name));
        }
        if !listing()?.is_empty() {
            return Err(setup("output_not_removable"));
        }
    }
    std::fs::remove_dir(output).map_err(|_| setup("output_not_removable"))
}

/// The one Arrow type the gate writes each schema type as.
fn export_type(ty: ColumnType) -> DataType {
    match ty {
        ColumnType::Integer => DataType::Int64,
        ColumnType::Double => DataType::Float64,
        ColumnType::String => DataType::Utf8,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Date => DataType::Date32,
    }
}

/// Every option that affects the bytes, pinned, so the same data writes the
/// same bytes under one gate version whatever file it came from.
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_created_by(format!("reg-export-gate {VERSION}"))
        .build()
}

/// A checked table written afresh: a new Arrow schema of the export types in
/// the spec's order, with no key-value metadata (not even `ARROW:schema`), so
/// nothing of the author's file but its values reaches the export.
fn rewrite(cand: &Candidate) -> Option<Vec<u8>> {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (col, array) in cand.spec.columns.iter().zip(&cand.columns) {
        let ty = export_type(col.ty);
        let array: ArrayRef = if array.data_type() == &DataType::Int32 {
            Arc::new(
                array
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|v| v.map(i64::from))
                    .collect::<Int64Array>(),
            )
        } else {
            array.clone()
        };
        fields.push(Field::new(&col.name, ty, true));
        arrays.push(array);
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).ok()?;
    let options = ArrowWriterOptions::new()
        .with_properties(writer_properties())
        .with_skip_arrow_metadata(true);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut bytes, schema, options).ok()?;
    writer.write(&batch).ok()?;
    writer.close().ok()?;
    Some(bytes)
}

#[derive(Serialize)]
struct Gate {
    implementation: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct Pinned<'a> {
    name: &'a str,
    version: u64,
    sha256: &'a str,
}

#[derive(Serialize)]
struct Written {
    name: String,
    sha256: String,
}

/// Fields in the format's order; no timestamp, so a rerun over the same data
/// writes the same receipt.
#[derive(Serialize)]
struct Receipt<'a> {
    format: &'static str,
    format_version: u64,
    gate: Gate,
    schema: Pinned<'a>,
    policy: Pinned<'a>,
    files: Vec<Written>,
}

fn json_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).expect("serializable");
    bytes.push(b'\n');
    bytes
}

/// Writes the checked tables and the receipt into a new folder beside
/// `output`, then renames it to `output`: the files and the receipt appear
/// together or not at all. On any failure the new folder is removed whole.
pub(crate) fn export(
    output: &Path,
    schema: &Schema,
    policy: &Policy,
    candidates: &[Candidate],
) -> Result<(), Refusal> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let fresh = parent(output).join(format!(".export-gate-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&fresh).map_err(|_| setup("export_not_writable"))?;
    let written = (|| {
        let mut files = Vec::new();
        for cand in candidates {
            let bytes = rewrite(cand).ok_or_else(|| setup("internal_error"))?;
            write_new(&fresh.join(&cand.spec.name), &bytes)
                .map_err(|_| setup("export_not_writable"))?;
            files.push(Written {
                name: cand.spec.name.clone(),
                sha256: sha256_hex(&bytes),
            });
        }
        let receipt = Receipt {
            format: RECEIPT_FORMAT,
            format_version: 1,
            gate: Gate {
                implementation: "rust",
                version: VERSION,
            },
            schema: Pinned {
                name: &schema.name,
                version: schema.version,
                sha256: &schema.sha256,
            },
            policy: Pinned {
                name: &policy.name,
                version: policy.version,
                sha256: &policy.sha256,
            },
            files,
        };
        write_new(&fresh.join(RECEIPT), &json_bytes(&receipt))
            .map_err(|_| setup("export_not_writable"))?;
        std::fs::rename(&fresh, output).map_err(|_| setup("move_failed"))
    })();
    if written.is_err() {
        let _ = std::fs::remove_dir_all(&fresh);
    }
    written
}

#[derive(Serialize)]
struct ReportFinding<'a> {
    file: Option<&'a str>,
    column: Option<&'a str>,
    position: Option<usize>,
    rule: &'a str,
}

#[derive(Serialize)]
struct Report<'a> {
    gate_version: &'static str,
    result: &'static str,
    code: Option<&'static str>,
    findings: Vec<ReportFinding<'a>>,
}

/// Writes `.diagnostics/export_gate_report.json` beside `output`, never into
/// it. Best effort: a report that cannot be written does not change the
/// result. It is written only where it cannot touch staging or the export (the
/// paths check's rule, applied again since that check may be what refused),
/// into a real folder, as a new file: a planted link or hard link named like
/// the report is unlinked, never written through.
pub(crate) fn write_report(staging: &Path, output: &Path, refusal: Option<&Refusal>) {
    // A staging path that exists but does not resolve (a dangling link) could
    // be anywhere: no report.
    let staging = match staging.canonicalize() {
        Ok(s) => Some(s),
        Err(_) if std::fs::symlink_metadata(staging).is_err() => None,
        Err(_) => return,
    };
    let Ok(dir) = parent(output).canonicalize().map(|p| p.join(DIAGNOSTICS)) else {
        return;
    };
    if apart(staging.as_deref(), output) != Some(true) {
        return;
    }
    let report = Report {
        gate_version: VERSION,
        result: if refusal.is_some() { "refused" } else { "pass" },
        code: refusal.map(|r| r.code.as_str()),
        findings: refusal
            .map(|r| {
                r.findings
                    .iter()
                    .map(|f| ReportFinding {
                        file: f.file.as_deref(),
                        column: f.column.as_deref(),
                        position: f.position,
                        rule: f.rule,
                    })
                    .collect()
            })
            .unwrap_or_default(),
    };
    let _ = std::fs::create_dir(&dir);
    // Checked again now the folder exists, by identity: an alias of it the
    // first check could not see (another case or form) is caught here.
    if !std::fs::symlink_metadata(&dir).is_ok_and(|m| m.is_dir())
        || apart(staging.as_deref(), output) != Some(true)
    {
        return;
    }
    let path = dir.join(REPORT);
    let _ = std::fs::remove_file(&path);
    let _ = write_new(&path, &json_bytes(&report));
}
