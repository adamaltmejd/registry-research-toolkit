//! `reg-readers`: one reader per input format, each turning one pinned input into a
//! source frame (`crates/DESIGN.md`, "The catalog builder"). Readers are the only
//! builder code that names a provider or a file format; their vocabulary (header
//! aliases, type tables, sheet names) is data in `curation/readers/`, and no register
//! name, row number or cell literal appears in reader code.
//!
//! - [`transcription`]: the transcription CSV, the interchange format thin providers
//!   are entered in;
//! - [`workbook`]: the Socialstyrelsen metadata template (XLSX, through calamine);
//! - [`classification`]: classification CSVs, read as value sets.

pub mod classification;
pub mod transcription;
pub mod workbook;

use std::path::Path;

use reg_frame::Frame;
use serde::Deserialize;

/// One pinned input of the build manifest (`curation/inputs.toml`). `reader` names
/// the reader and its config, `curation/readers/<reader>.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    /// The input's name in the catalog's input table.
    pub name: String,
    pub reader: String,
    /// A file or directory, relative to the inputs root.
    pub path: String,
    pub provider: Option<String>,
    pub register: Option<String>,
}

/// Read `input` with its reader, configured from the `readers` directory.
///
/// # Errors
/// An unknown reader, a missing provider or register the reader needs, or the
/// reader's own refusal.
pub fn read(root: &Path, readers: &Path, input: &Input) -> Result<ReadOutput, ReadError> {
    let config = readers.join(format!("{}.toml", input.reader));
    let need = |value: &Option<String>, what: &str| {
        value.clone().ok_or_else(|| {
            error(
                &input.name,
                format!("reader {} needs a {what}", input.reader),
            )
        })
    };
    match input.reader.as_str() {
        "transcription" => transcription::read(
            root,
            &input.path,
            &need(&input.provider, "provider")?,
            &transcription::Config::load(&config)?,
        ),
        "sos-workbook" => workbook::read(
            root,
            &input.path,
            &need(&input.provider, "provider")?,
            &need(&input.register, "register")?,
            &workbook::Config::load(&config)?,
        ),
        "classification-csv" => {
            classification::read(root, &input.path, &classification::Config::load(&config)?)
        }
        other => Err(error(&input.name, format!("unknown reader {other:?}"))),
    }
}

/// What a reader returns for one input.
#[derive(Debug, Default)]
pub struct ReadOutput {
    pub frame: Frame,
    /// Every file the reader read, relative to the inputs root, for the input table.
    pub files: Vec<String>,
    /// Source facts the reader could not interpret and left unstated, one line each,
    /// for the build report.
    pub warnings: Vec<String>,
}

/// A reader refusal: the input it concerns and what to fix.
#[derive(Debug)]
pub struct ReadError {
    pub input: String,
    pub message: String,
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.input, self.message)
    }
}

impl std::error::Error for ReadError {}

pub(crate) fn error(input: &str, message: impl Into<String>) -> ReadError {
    ReadError {
        input: input.to_owned(),
        message: message.into(),
    }
}

/// The source key of a source without variable ids: the folded column name. The
/// frame scopes keys to their register.
#[must_use]
pub fn name_key(column: &str) -> String {
    format!("n:{}", reg_core::fold_identity(column))
}

/// A TOML file (a reader config, the manifest, a ledger), parsed; the target type
/// decides whether unknown keys are refused.
///
/// # Errors
/// An unreadable file or invalid TOML, located at the file.
pub fn config<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ReadError> {
    let name = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|e| error(&name, e.to_string()))?;
    toml::from_str(&text).map_err(|e| error(&name, e.to_string()))
}

/// `text` trimmed, or `None` when nothing is left.
pub(crate) fn nonblank(text: &str) -> Option<String> {
    let t = text.trim();
    (!t.is_empty()).then(|| t.to_owned())
}

/// The last day of `month` (1-12) in `year`, as an ISO day.
pub(crate) fn month_end(year: u32, month: u32) -> Option<String> {
    if !(1..=12).contains(&month) {
        return None;
    }
    let (y, m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    Some(reg_core::prev_iso_day(&format!("{y:04}-{m:02}-01")))
}
