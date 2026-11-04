//! The MONA order file (crates/DESIGN.md, "MONA order file"): the closed, versioned
//! manifest `materialize_order` writes and `reg_mona_runner/swecov_extract.R` reads.
//! Every object refuses an unknown field; an absent optional field means none.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::is_letter;
use crate::issue::{Issue, Period};
use crate::project::ExtractFormat;

/// The manifest `version` this crate writes; the runner refuses every other.
pub const MANIFEST_VERSION: u32 = 4;

/// The order file: one file per table and delivery, with its columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Always 4.
    pub version: u32,
    /// The project's format, resolved to `dta` when it states none.
    pub extract_format: ExtractFormat,
    pub provenance: Provenance,
    /// By first appearance in the order, then name.
    pub files: Vec<File>,
    /// Every requested variable the order delivers less of than requested.
    pub clips: Vec<Clip>,
    /// The non-blocking findings (level `warning` or `info`).
    pub findings: Vec<Issue>,
}

/// Which artifacts and which project the order binds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub mode: Mode,
    /// The data holder whose tables serve the order.
    pub steward: String,
    pub catalog: CatalogIdentity,
    pub holdings: HoldingsIdentity,
    /// The reference catalog's own `generation_id`.
    pub base_generation: String,
    pub project: ProjectIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ManifestMode))]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    StewardHoldings,
}

/// The catalog version the order was checked against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CatalogIdentity {
    pub release: String,
    pub schema: String,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct HoldingsIdentity {
    pub owner: String,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ProjectIdentity {
    pub schema_version: String,
    /// A hash of the project document.
    pub project_hash: String,
    /// Never blank: an unnamed project is `project_unnamed`.
    pub name: String,
}

/// One output file: one of the holder's tables in one delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ManifestFile))]
#[serde(deny_unknown_fields)]
pub struct File {
    /// The file's name without the extension; the runner adds the format's extension.
    pub name: String,
    /// The exact schema and table in the database.
    pub schema: String,
    pub table: String,
    /// The authored delivery period; null for a timeless table.
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub delivery: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// The projection token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<String>,
    /// Absent: every row of the table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<RowFilter>,
    /// Requested columns in request order, then the row keys.
    pub columns: Vec<Column>,
}

/// Keep a row whose period value lies in any range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct RowFilter {
    pub column: String,
    pub encoding: Encoding,
    /// Sorted and disjoint.
    pub ranges: Vec<Range>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    Date,
    Year,
    Yyyymm,
    Yyyymmdd,
}

impl Encoding {
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "date" => Self::Date,
            "year" => Self::Year,
            "yyyymm" => Self::Yyyymm,
            "yyyymmdd" => Self::Yyyymmdd,
            _ => return None,
        })
    }
}

/// Inclusive ISO days; an absent bound means no limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = RowFilterRange))]
#[serde(deny_unknown_fields)]
pub struct Range {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
}

/// One column of the file, with every requested variable it delivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ManifestColumn))]
#[serde(deny_unknown_fields)]
pub struct Column {
    /// The exact column name in the file.
    pub name: String,
    /// Only on a `dta` order, when `name` is not a valid Stata variable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stata_name: Option<String>,
    pub storage: Storage,
    pub role: Role,
    pub refs: Vec<Ref>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<Slot>,
}

/// The expected SQL storage, checked before extraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Storage {
    #[serde(rename = "type")]
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub dtype: Option<String>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub length: Option<i64>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub precision: Option<i64>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub scale: Option<i64>,
}

/// `ordered`: requested; `row_key`: a key the file needs; `carried`: the parent table's key copied into this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ColumnRole))]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Ordered,
    RowKey,
    Carried,
}

/// A requested variable this column delivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ColumnRef))]
#[serde(deny_unknown_fields)]
pub struct Ref {
    pub source: String,
    /// The variable's position in the source; null for a row key nobody requested.
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub binding: Option<usize>,
    pub table: String,
    pub variable: String,
    /// The days of this reference the file holds.
    pub period: Period,
    /// The catalog's column name; the file uses `name`.
    pub spelling: String,
}

/// How the column is laid out in the holder's files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Slot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    pub kind: String,
    pub index: String,
}

/// A requested variable that covers less than the requested period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ManifestClip))]
#[serde(deny_unknown_fields)]
pub struct Clip {
    pub source: String,
    pub binding: usize,
    pub variable: String,
    pub requested: Vec<Period>,
    pub ordered: Vec<Period>,
}

/// Why a stored file stem cannot name an output file.
///
/// # Errors
///
/// The reason, when `name` is outside the runner's slug spelling, longer than 128
/// UTF-16 units, the stem of a Windows device name or of the runner's own logs (case
/// ignored).
pub fn safe_file_name(name: &str) -> Result<(), &'static str> {
    const DEVICES: [&str; 4] = ["con", "prn", "aux", "nul"];
    let mut chars = name.chars();
    let slug = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ',' | '-'));
    if !slug {
        return Err("not slug spelling");
    }
    if name.encode_utf16().count() > 128 {
        return Err("longer than 128 UTF-16 units");
    }
    let stem = name
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let numbered = ["com", "lpt"].iter().any(|p| {
        stem.strip_prefix(p)
            .is_some_and(|n| n.len() == 1 && matches!(n.as_bytes()[0], b'1'..=b'9'))
    });
    if DEVICES.contains(&stem.as_str()) || numbered {
        return Err("a Windows device name");
    }
    if matches!(stem.as_str(), "run_log" | "error_log") {
        return Err("the runner's log name");
    }
    Ok(())
}

/// The Stata variable name a `.dta` file holds `name` under, when `name` is not one
/// (the v3 rule, kept byte-identical): its first 25 characters, each that is not a
/// letter, ASCII digit or `_` made `_` (and `_` put first when it would start with a
/// digit or `str` and a digit), then `_` and the first 6 hex digits of the SHA-256 of
/// its UTF-8 bytes. A function of the name alone, so a column takes one alias in every
/// table, order and catalog generation. `None` for a valid Stata name.
#[must_use]
pub fn stata_name(name: &str) -> Option<String> {
    if is_stata_name(name) {
        return None;
    }
    let mut alias: String = name
        .chars()
        .map(|c| {
            if is_letter(c) || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let str_digit = alias
        .strip_prefix("str")
        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()));
    if str_digit || alias.starts_with(|c: char| c.is_ascii_digit()) {
        alias.insert(0, '_');
    }
    let mut alias: String = alias.chars().take(25).collect();
    alias.push('_');
    alias.push_str(&crate::hex(&Sha256::digest(name.as_bytes())[..3]));
    debug_assert!(is_stata_name(&alias), "{alias} is a Stata name");
    Some(alias)
}

/// Whether `name` is a variable name a `.dta` file (format 118) can hold: Stata's rule
/// (stricter than `ReadStat`), which `stata_name_ok` in `reg_mona_runner` repeats. 1 to
/// 32 characters; a letter (general category L*) or `_` first, then letters, ASCII
/// digits and `_`; not a reserved word, nor `str` followed by a digit.
fn is_stata_name(name: &str) -> bool {
    const RESERVED: [&str; 20] = [
        "_all", "_b", "byte", "_coef", "_cons", "double", "float", "if", "in", "int", "long", "_n",
        "_N", "_pi", "_pred", "_rc", "_skip", "strL", "using", "with",
    ];
    let mut chars = name.chars();
    chars.next().is_some_and(|c| is_letter(c) || c == '_')
        && chars.all(|c| is_letter(c) || c.is_ascii_digit() || c == '_')
        && name.chars().count() <= 32
        && !RESERVED.contains(&name)
        && !name
            .strip_prefix("str")
            .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::safe_file_name;

    // Fails if a stem the runner refuses (a device name in any case, its own log
    // name, a name past SQL Server's 128 units) passes, or a plain stem is refused.
    #[test]
    fn safe_file_name_refuses_what_the_runner_refuses() {
        assert!(safe_file_name("swecov_sos_sv").is_ok());
        assert!(safe_file_name(&"a".repeat(128)).is_ok());
        assert!(safe_file_name(&"a".repeat(129)).is_err());
        assert!(safe_file_name("con").is_err());
        assert!(safe_file_name("CON").is_err());
        assert!(safe_file_name("Lpt9.x").is_err());
        assert!(safe_file_name("com10").is_ok());
        assert!(safe_file_name("run_log").is_err());
        assert!(safe_file_name("Error_Log").is_err());
        assert!(safe_file_name("../x").is_err());
        assert!(safe_file_name("").is_err());
    }

    // Fails if the alias drifts from the frozen v3 manifest's bytes (the v3
    // `stata-names` order golden, `git show f5993d7d8^:conformance/cases/order/`),
    // which researchers' do-files already name.
    #[test]
    fn stata_name_keeps_the_v3_aliases() {
        assert_eq!(
            super::stata_name("LopNrForSammanstalldaForetagsgrupper").as_deref(),
            Some("LopNrForSammanstalldaFore_bdf70d")
        );
        assert_eq!(
            super::stata_name("\u{24b6}Value").as_deref(),
            Some("_Value_e5ce0c")
        );
    }
}
