//! Structure: each schema file is readable Parquet within the size caps, whose
//! columns are the schema's, in its order, each of an accepted type.
//!
//! Each file is opened once, never through a link, and read into memory; it is
//! decoded from those bytes, so what is checked is what the rewrite writes. The
//! author's embedded Arrow schema (`ARROW:schema`) is ignored: column types come
//! from the Parquet types alone.

use std::cell::Cell;
use std::fs::OpenOptions;
use std::io::Read as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Once;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema as ArrowSchema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};

use crate::{Code, ColumnType, File, Finding, Refusal, Schema};

/// The largest staging file read, in bytes (maintainer, 2026-11-04).
pub const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
/// The most rows a staging file may hold (maintainer, 2026-11-04).
pub const MAX_ROWS: usize = 100_000;
/// The most decoded Arrow memory a staging file may take. Not a disclosure
/// rule: it stops a few compressed values expanding without bound, where the
/// row cap does not (one huge string repeated). simplify: it counts finished
/// batches only; the decoder's page buffers (sized by the page header, up to
/// 2 GiB a page) are not counted, and bounding them needs the decoder's help.
pub const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

/// A schema file as decoded: its spec and its record batches, whose columns are
/// exactly the spec's, in its order, once the check passes. The batches' Arrow
/// schema is the decoder's and can carry the author's file metadata: the
/// rewrite builds its own.
#[derive(Debug)]
pub struct Candidate {
    pub spec: File,
    pub batches: Vec<RecordBatch>,
}

/// What reading a staging file gave.
enum Outcome<T> {
    Ok(T),
    TooLarge,
    Unreadable,
}

fn accepts(ty: ColumnType, data_type: &DataType) -> bool {
    match ty {
        // Width is not a disclosure concern, and R writes int32.
        ColumnType::Integer => matches!(data_type, DataType::Int32 | DataType::Int64),
        ColumnType::Double => *data_type == DataType::Float64,
        ColumnType::String => *data_type == DataType::Utf8,
        ColumnType::Boolean => *data_type == DataType::Boolean,
        ColumnType::Date => *data_type == DataType::Date32,
    }
}

#[cfg(unix)]
fn open(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    // A link swapped in after the inventory fails to open; a FIFO opens
    // without waiting for a writer and is then refused as not a regular file.
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
fn open(path: &Path) -> std::io::Result<std::fs::File> {
    OpenOptions::new().read(true).open(path)
}

/// The file's bytes, read once from one handle checked to be a regular file,
/// and never more than the cap plus one byte.
fn read(path: &Path) -> Outcome<Vec<u8>> {
    let Ok(file) = open(path) else {
        return Outcome::Unreadable;
    };
    if !file.metadata().is_ok_and(|m| m.is_file()) {
        return Outcome::Unreadable;
    }
    let mut bytes = Vec::new();
    if file
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Outcome::Unreadable;
    }
    if bytes.len() as u64 > MAX_FILE_BYTES {
        Outcome::TooLarge
    } else {
        Outcome::Ok(bytes)
    }
}

thread_local! {
    static DECODING: Cell<bool> = const { Cell::new(false) };
}

/// The default panic hook prints the message before `catch_unwind` sees it,
/// and a decoder message can quote an author's name or value. This hook stays
/// silent for panics while this thread decodes and defers to the previous hook
/// for every other panic.
fn quiet_decoder_panics() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !DECODING.with(Cell::get) {
                previous(info);
            }
        }));
    });
}

/// Decodes one schema file, or gives its findings. In order: the footer's row
/// counts against the row cap, the columns against the schema (so a column
/// that would be refused, a nested one say, is never decoded), then the rows,
/// counted again and with their decoded size summed as they come. Anything that
/// is not readable Parquet, a decoder panic included, is `unreadable`, with no
/// error text kept.
fn decode(spec: &File, bytes: Bytes) -> Result<Vec<RecordBatch>, Vec<Finding>> {
    let file = spec.name.as_str();
    let refuse = |rule| Err(vec![Finding::file(rule, file)]);
    quiet_decoder_panics();
    DECODING.with(|d| d.set(true));
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)
        else {
            return refuse("unreadable");
        };
        let metadata = builder.metadata();
        let within = |n: i64| usize::try_from(n).is_ok_and(|n| n <= MAX_ROWS);
        // Each group checked and the sum without wrapping, so declared counts
        // cannot overflow back under the cap.
        let grouped = metadata.row_groups().iter().try_fold(0_i64, |sum, group| {
            let n = group.num_rows();
            within(n).then(|| sum.checked_add(n)).flatten()
        });
        if !within(metadata.file_metadata().num_rows()) || !grouped.is_some_and(within) {
            return refuse("too_large");
        }
        let mut findings = Vec::new();
        check_file(spec, builder.schema(), &mut findings);
        if !findings.is_empty() {
            return Err(findings);
        }
        let Ok(reader) = builder.build() else {
            return refuse("unreadable");
        };
        let mut batches = Vec::new();
        let (mut rows, mut size) = (0, 0);
        for batch in reader {
            let Ok(batch) = batch else {
                return refuse("unreadable");
            };
            rows += batch.num_rows();
            size += batch.get_array_memory_size();
            if rows > MAX_ROWS || size > MAX_DECODED_BYTES {
                return refuse("too_large");
            }
            batches.push(batch);
        }
        Ok(batches)
    }));
    DECODING.with(|d| d.set(false));
    decoded.unwrap_or_else(|_| refuse("unreadable"))
}

fn check_file(spec: &File, schema: &ArrowSchema, findings: &mut Vec<Finding>) {
    let file = spec.name.as_str();
    let want: Vec<&str> = spec.columns.iter().map(|c| c.name.as_str()).collect();
    let have: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for col in &want {
        if !have.contains(col) {
            findings.push(Finding::column("missing_column", file, col));
        }
    }
    for (i, col) in have.iter().enumerate() {
        if !want.contains(col) {
            findings.push(Finding::position("extra_column", Some(file), i + 1));
        }
    }
    let same_set = want.iter().all(|c| have.contains(c)) && have.iter().all(|c| want.contains(c));
    if same_set && want != have {
        findings.push(Finding::file("column_order", file));
    }
    for col in &spec.columns {
        // The first column of that name; a duplicate already refused above.
        if let Some(field) = schema.fields().iter().find(|f| f.name() == &col.name)
            && !accepts(col.ty, field.data_type())
        {
            findings.push(Finding::column("column_type", file, &col.name));
        }
    }
}

pub(crate) fn check(staging: &Path, schema: &Schema) -> Result<Vec<Candidate>, Refusal> {
    let mut findings = Vec::new();
    let mut candidates = Vec::new();
    for spec in &schema.files {
        let decoded = match read(&staging.join(&spec.name)) {
            Outcome::Ok(bytes) => decode(spec, Bytes::from(bytes)),
            Outcome::TooLarge => Err(vec![Finding::file("too_large", &spec.name)]),
            Outcome::Unreadable => Err(vec![Finding::file("unreadable", &spec.name)]),
        };
        match decoded {
            Ok(batches) => candidates.push(Candidate {
                spec: spec.clone(),
                batches,
            }),
            Err(file_findings) => findings.extend(file_findings),
        }
    }
    if findings.is_empty() {
        Ok(candidates)
    } else {
        Err(Refusal {
            code: Code::Structure,
            findings,
        })
    }
}
