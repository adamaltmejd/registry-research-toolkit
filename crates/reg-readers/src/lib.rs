//! `reg-readers`: one reader per input format, each turning one pinned input into a
//! source frame (`crates/DESIGN.md`, "The catalog builder"). Readers are the only
//! builder code that names a provider or a file format; their vocabulary (header
//! aliases, type tables, sheet names) is data in `curation/readers/`, and no register
//! name, row number or cell literal appears in reader code.
//!
//! - [`transcription`]: the transcription CSV, the interchange format thin providers
//!   are entered in;
//! - [`workbook`]: the Socialstyrelsen metadata template (XLSX, through calamine);
//! - [`scb`]: the SCB metadata export (pipe-delimited CP1252 CSVs);
//! - [`classification`]: classification CSVs, read as value sets.

pub mod classification;
pub mod lisa;
pub mod names;
pub mod scb;
pub mod transcription;
pub mod workbook;

use std::path::{Path, PathBuf};

use reg_frame::Frame;
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// One pinned input of the build manifest (`curation/inputs.toml`). `reader` names
/// the reader and its config, `curation/readers/<reader>.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    /// The input's name in the catalog's input table.
    pub name: String,
    pub reader: String,
    /// A file or directory, relative to the inputs root; for a release asset, the
    /// asset's file name.
    pub path: String,
    pub provider: Option<String>,
    pub register: Option<String>,
    /// The inputs repository's release tag the file is an asset of, fetched outside
    /// the build into `<assets>/<release>/<path>`.
    pub release: Option<String>,
    /// The asset's pinned SHA-256; required with `release`.
    pub sha256: Option<String>,
}

impl Input {
    /// Where the input's bytes are: under the assets directory for a release asset,
    /// else under the inputs root.
    ///
    /// # Errors
    /// A release asset with no assets directory or no pinned hash.
    pub fn locate(&self, root: &Path, assets: Option<&Path>) -> Result<PathBuf, ReadError> {
        match &self.release {
            None => Ok(root.join(&self.path)),
            Some(release) => {
                let assets = assets.ok_or_else(|| {
                    error(
                        &self.name,
                        "a release asset; pass --assets with its directory",
                    )
                })?;
                if self.sha256.is_none() {
                    return Err(error(&self.name, "a release asset needs a sha256 pin"));
                }
                Ok(assets.join(release).join(&self.path))
            }
        }
    }
}

/// Read `input` with its reader, configured from the `readers` directory. A release
/// asset is verified against its pin first.
///
/// # Errors
/// An unknown reader, a missing provider or register the reader needs, an asset
/// that is missing or does not match its pin, or the reader's own refusal.
pub fn read(
    root: &Path,
    assets: Option<&Path>,
    readers: &Path,
    input: &Input,
) -> Result<ReadOutput, ReadError> {
    let config = readers.join(format!("{}.toml", input.reader));
    let need = |value: &Option<String>, what: &str| {
        value.clone().ok_or_else(|| {
            error(
                &input.name,
                format!("reader {} needs a {what}", input.reader),
            )
        })
    };
    // A release asset is verified before any reader sees it; a reader of the inputs
    // root takes none, and the scb reader takes nothing else.
    let asset = match (&input.release, &input.sha256) {
        (Some(_), Some(pin)) => Some(verified(input, pin, &input.locate(root, assets)?)?),
        (None, None) => None,
        _ => return Err(error(&input.name, "release and sha256 are stated together")),
    };
    let in_root = || match asset {
        Some(_) => Err(error(
            &input.name,
            format!(
                "reader {} reads the inputs root, not a release asset",
                input.reader
            ),
        )),
        None => Ok(()),
    };
    match input.reader.as_str() {
        "transcription" => in_root().and_then(|()| {
            transcription::read(
                root,
                &input.path,
                &need(&input.provider, "provider")?,
                &transcription::Config::load(&config)?,
            )
        }),
        "sos-workbook" => in_root().and_then(|()| {
            workbook::read(
                root,
                &input.path,
                &need(&input.provider, "provider")?,
                &need(&input.register, "register")?,
                &workbook::Config::load(&config)?,
            )
        }),
        "lisa" => in_root().and_then(|()| {
            lisa::read(
                root,
                &input.path,
                &need(&input.provider, "provider")?,
                &need(&input.register, "register")?,
                &lisa::Config::load(&config)?,
            )
        }),
        "scb" => scb::read(
            &asset.clone().ok_or_else(|| {
                error(
                    &input.name,
                    "reader scb reads a release asset with a sha256 pin",
                )
            })?,
            &input.path,
            &need(&input.provider, "provider")?,
            &scb::Config::load(&config)?,
        ),
        "classification-csv" => in_root().and_then(|()| {
            classification::read(root, &input.path, &classification::Config::load(&config)?)
        }),
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

/// `path`, once its bytes match the input's pin.
fn verified(input: &Input, pin: &str, path: &Path) -> Result<PathBuf, ReadError> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| error(&input.name, format!("{}: {e}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    loop {
        let n = std::io::Read::read(&mut file, &mut buffer)
            .map_err(|e| error(&input.name, e.to_string()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    let actual = reg_frame::hex(&hasher.finalize());
    if actual != pin {
        return Err(error(
            &input.name,
            format!("{} has sha256 {actual}, pinned {pin}", path.display()),
        ));
    }
    Ok(path.to_owned())
}

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
