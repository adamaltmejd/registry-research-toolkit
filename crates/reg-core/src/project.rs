//! The `project_data.json` types and their two JSON encodings (`RUST_RUNTIME_SPEC.md`
//! section 5, package 3e.1).
//!
//! The types are the retired `reg_schema.project_data` models. They keep the raw spelling
//! of every value (an int year stays an int, `"2018"` stays a string), and serialize
//! as Pydantic's `model_dump(mode="json")` does: every optional field present, absent
//! as `null`, `panels` as `[]`, a bare-string panel member as `{"source": ...}`. That
//! shape is what [`project_hash`] hashes.
//!
//! [`ProjectData::from_value`] is the only way to build a project from JSON: it runs
//! [`crate::validate_structural`] first, which reports every problem and rejects
//! every shape the types would misread (serde reads a struct from a JSON array by
//! position), and deserializes only an accepted document.

use std::fmt::Write as _;

use serde::de::{Deserializer, Error as _};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    Interval, IssueLevel, Period, PeriodToken, ValidationIssue, ValidationResult, merge, quote,
    validate_structural,
};

/// The `project_data.json` contract this runtime reads, exactly. The SPA seeds a new
/// draft at this version (`reg_webapp/frontend/src/lib/project_data.test.ts` reads it
/// from here).
pub const SCHEMA_VERSION: &str = "3.0.0";

/// The project in `raw`, or the issues that reject it without a catalog: the single
/// entry point the server's validate and order operations and the SPA (through
/// `reg-core-wasm`) share.
///
/// The supported-version decision runs first and alone ([`version_issue`]); then the
/// structural validator, through [`ProjectData::from_value`]. Any JSON value is read:
/// a non-object root is the structural `invalid_root` issue.
///
/// # Errors
///
/// The rejecting result: the one `unsupported_schema_version` issue, or every
/// structural issue.
pub fn check(raw: &Value) -> Result<ProjectData, ValidationResult> {
    if let Some(issue) = version_issue(raw) {
        // Alone: every other check reads the document as the contract it rejects.
        return Err(ValidationResult {
            issues: vec![issue],
        });
    }
    ProjectData::from_value(raw)
}

/// The supported-version decision, taken before any other check reads the document
/// as this contract: a string `schema_version` other than [`SCHEMA_VERSION`] is one
/// `unsupported_schema_version` issue. An absent or non-string one is the structural
/// validator's to report.
fn version_issue(raw: &Value) -> Option<ValidationIssue> {
    let version = raw.get("schema_version")?.as_str()?;
    (version != SCHEMA_VERSION).then(|| ValidationIssue {
        level: IssueLevel::Error,
        code: "unsupported_schema_version",
        path: "/schema_version".into(),
        message: format!(
            "project schema_version {} is not supported: this build reads \
             project_data.json schema {SCHEMA_VERSION} exactly. Re-author the project \
             against the current schema; there is no migration path.",
            quote(version)
        ),
        successor_fqid: None,
    })
}

/// A closed string enum: one list of `(variant, wire name)` pairs gives the serde
/// encoding, the allowed values the validator reports and, with the `openapi`
/// feature, the schema enum.
macro_rules! str_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $wire:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }

        impl $name {
            /// Every value, in declaration order.
            pub(crate) const ALL: &[Self] = &[$(Self::$variant),+];

            /// The wire spelling.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }

            /// The value spelled `s`, if any.
            #[must_use]
            pub(crate) fn parse(s: &str) -> Option<Self> {
                Self::ALL.iter().copied().find(|v| v.as_str() == s)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        /// The wire spellings, as the schema `Project<name>`.
        #[cfg(feature = "openapi")]
        impl utoipa::PartialSchema for $name {
            fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::Type::String)
                    .enum_values(Some([$($wire),+]))
                    .into()
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::ToSchema for $name {
            fn name() -> std::borrow::Cow<'static, str> {
                concat!("Project", stringify!($name)).into()
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = String::deserialize(deserializer)?;
                Self::parse(&s).ok_or_else(|| D::Error::custom(format!("unknown {}: {s}", stringify!($name))))
            }
        }
    };
}

str_enum!(
    /// The deployment a project is ordered from.
    Steward { Global = "global", Ifau = "ifau", Swecov = "swecov" }
);
str_enum!(
    /// A binding's column type.
    ColumnType {
        Id = "id",
        Categorical = "categorical",
        Numeric = "numeric",
        Date = "date",
        Datetime = "datetime",
        Opaque = "opaque",
    }
);
str_enum!(
    /// The storage of an `id` column.
    IdSubtype { Integer = "integer", String = "string" }
);
str_enum!(
    /// The storage of a `numeric` column.
    NumericSubtype { Integer = "integer", Double = "double" }
);

/// The top-level `project_data.json` document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectData))]
#[serde(deny_unknown_fields)]
pub struct ProjectData {
    pub schema_version: String,
    pub steward: Steward,
    pub reg_meta_version: String,
    pub name: String,
    pub sources: Vec<Source>,
    #[serde(default)]
    pub panels: Vec<Panel>,
    // An explicit `null` is `invalid_field_type`: absent means no window.
    #[serde(default)]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub window: Option<StudyWindow>,
}

/// [`ProjectData`]'s fields as deserialized, so that `ProjectData` has no public
/// `Deserialize` that skips validation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fields {
    schema_version: String,
    steward: Steward,
    reg_meta_version: String,
    name: String,
    sources: Vec<Source>,
    #[serde(default)]
    panels: Vec<Panel>,
    #[serde(default)]
    window: Option<StudyWindow>,
}

impl ProjectData {
    /// The project in `value`, or every structural issue that rejects it.
    ///
    /// # Errors
    ///
    /// The structural validation result, when it has an error.
    ///
    /// # Panics
    ///
    /// If an accepted document does not deserialize: a validator bug, which the
    /// corpora rule out for every accepted case.
    pub fn from_value(value: &Value) -> Result<Self, ValidationResult> {
        let result = validate_structural(value);
        if !result.ok() {
            return Err(result);
        }
        let f = Fields::deserialize(value).expect("an accepted project deserializes");
        Ok(Self {
            schema_version: f.schema_version,
            steward: f.steward,
            reg_meta_version: f.reg_meta_version,
            name: f.name,
            sources: f.sources,
            panels: f.panels,
            window: f.window,
        })
    }
}

/// One logical extraction: a register variant, a requested period and its bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectSource))]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub name: String,
    /// The 3-part coordinate `<provider>/<register>/<variant>`.
    pub register_variant: String,
    pub period: SourcePeriod,
    pub bindings: Vec<Binding>,
}

/// One variable to extract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectBinding))]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// The binding FQID `<provider>/<register>/<slug>`.
    pub variable: String,
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub id_subtype: Option<IdSubtype>,
    #[serde(default)]
    pub numeric_subtype: Option<NumericSubtype>,
    #[serde(default)]
    pub date_format: Option<String>,
    #[serde(default)]
    pub datetime_format: Option<String>,
    /// A classification FQID `class/<slug>`.
    #[serde(default)]
    pub value_set: Option<String>,
    #[serde(default)]
    pub representation: Option<String>,
}

/// A period endpoint as written: an int year or a period-token string (`"_default"`
/// included, which only a scalar source period may hold).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectPeriodValue))]
#[serde(untagged)]
pub enum PeriodValue {
    Year(i64),
    Token(String),
}

/// The `{"from": ..., "to": ...}` range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectPeriodRange))]
#[serde(deny_unknown_fields)]
pub struct PeriodRange {
    pub r#from: PeriodValue,
    pub to: PeriodValue,
}

/// One contiguous piece of a source period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectPeriodSegment))]
#[serde(untagged)]
pub enum PeriodSegment {
    Value(PeriodValue),
    Range(PeriodRange),
}

/// `Source.period`: one segment, or a sorted, disjoint list of them (an interrupted
/// series).
//
// A list variant comes first in each untagged enum: serde also reads a struct from a
// JSON array, by position, so `["2018-01", "2018-04"]` would otherwise become a
// `PeriodRange`. (A plain comment: a doc comment here is the schema description.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectSourcePeriod))]
#[serde(untagged)]
pub enum SourcePeriod {
    List(Vec<PeriodSegment>),
    Segment(PeriodSegment),
}

impl SourcePeriod {
    /// The requested days, merged ([`merge`]); `None` for `"_default"`, a
    /// year-independent selection.
    ///
    /// # Errors
    ///
    /// A scalar `{from, to}` whose start is after its end, which the structural
    /// validator accepts (it checks inversion in lists only): the reason the period
    /// is not orderable.
    ///
    /// # Panics
    ///
    /// On an endpoint outside the period grammar, which the structural validator
    /// refuses, so a [`ProjectData`] never holds one.
    pub fn intervals(&self) -> Result<Option<Vec<Interval>>, String> {
        let segments = match self {
            Self::Segment(PeriodSegment::Value(PeriodValue::Token(t))) if t == "_default" => {
                return Ok(None);
            }
            Self::Segment(segment) => std::slice::from_ref(segment),
            Self::List(list) => list.as_slice(),
        };
        let mut days = Vec::new();
        for segment in segments {
            days.push(match segment {
                PeriodSegment::Value(v) => Period::Token(v.token()).iso_bounds(),
                PeriodSegment::Range(PeriodRange { from, to }) => {
                    let (lo, hi) = Period::Range {
                        from: from.token(),
                        to: to.token(),
                    }
                    .iso_bounds();
                    if lo > hi {
                        return Err(format!(
                            "edition range 'from' is after 'to': {}..{}",
                            quote(&from.spelling()),
                            quote(&to.spelling())
                        ));
                    }
                    (lo, hi)
                }
            });
        }
        Ok(Some(merge(days)))
    }
}

impl PeriodValue {
    /// The value as a token string: an int year zero-padded to four digits.
    fn spelling(&self) -> String {
        match self {
            Self::Year(year) => format!("{year:04}"),
            Self::Token(token) => token.clone(),
        }
    }

    fn token(&self) -> PeriodToken {
        PeriodToken::parse(&self.spelling()).expect("an accepted project's periods parse")
    }
}

/// A panel over sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectPanel))]
#[serde(deny_unknown_fields)]
pub struct Panel {
    pub panel_id: String,
    #[serde(deserialize_with = "members")]
    #[cfg_attr(feature = "openapi", schema(value_type = Vec<Member>, inline))]
    pub members: Vec<PanelMember>,
    #[serde(default)]
    pub entity_key: Option<EntityKey>,
    #[serde(default)]
    pub time_key: Option<TimeKey>,
    #[serde(default)]
    pub comment: Option<String>,
}

/// A panel member; the bare-string shorthand deserializes to `{"source": <name>}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectPanelMember))]
#[serde(deny_unknown_fields)]
pub struct PanelMember {
    pub source: String,
    // An explicit `null` override is `invalid_field_type`: absent inherits.
    #[serde(default)]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub entity_key: Option<EntityKey>,
    #[serde(default)]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub time_key: Option<TimeKey>,
}

/// A panel member as written: a source name, or a member object.
#[derive(Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
enum Member {
    Source(String),
    Member(PanelMember),
}

fn members<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<PanelMember>, D::Error> {
    Ok(Vec::<Member>::deserialize(deserializer)?
        .into_iter()
        .map(|m| match m {
            Member::Source(source) => PanelMember {
                source,
                entity_key: None,
                time_key: None,
            },
            Member::Member(m) => m,
        })
        .collect())
}

/// A panel's entity key: one column, or a composite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectEntityKey))]
#[serde(untagged)]
pub enum EntityKey {
    Column(String),
    Composite(Vec<String>),
}

/// A panel's time key: one time point, or a composite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectTimeKey))]
#[serde(untagged)]
pub enum TimeKey {
    Composite(Vec<TimePoint>),
    Point(TimePoint),
}

/// A literal year, a column ref, `{"period": ...}` or `{"range": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectTimePoint))]
#[serde(untagged)]
pub enum TimePoint {
    Year(i64),
    Column(String),
    Literal(LiteralPeriod),
    Range(TimeRange),
}

/// `{"period": int | string}`: a literal period, as opposed to a column ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectLiteralPeriod))]
#[serde(deny_unknown_fields)]
pub struct LiteralPeriod {
    pub period: PeriodValue,
}

/// `{"range": {"from": ..., "to": ...}}`: a literal period range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectTimeRange))]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    pub range: PeriodRange,
}

/// The optional study window, in plain int years.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = ProjectStudyWindow))]
#[serde(deny_unknown_fields)]
pub struct StudyWindow {
    pub r#from: i64,
    pub to: i64,
}

/// `value` as a JSON tree with every object's keys sorted by code point, as Python's
/// `sort_keys=True` sorts them; struct field order never leaks.
fn tree<T: Serialize>(value: &T) -> Value {
    let mut tree = serde_json::to_value(value).expect("project and result types serialize to JSON");
    tree.sort_all_objects();
    tree
}

/// The manifest and validation-result encoding: sorted keys, two-space indent,
/// non-ASCII as is, trailing newline. Python's
/// `json.dumps(..., sort_keys=True, indent=2, ensure_ascii=False) + "\n"`.
///
/// # Panics
///
/// If `value` has a map with non-string keys, which no type of this crate has.
#[must_use]
pub fn to_json_pretty<T: Serialize>(value: &T) -> String {
    let mut text = serde_json::to_string_pretty(&tree(value)).expect("a JSON tree encodes");
    text.push('\n');
    text
}

/// The project identity of an order manifest: SHA-256, as lowercase hex, of the
/// project's compact canonical JSON (sorted keys, no whitespace, non-ASCII as is).
#[must_use]
pub fn project_hash(project: &ProjectData) -> String {
    let canonical = tree(project).to_string();
    Sha256::digest(canonical.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
}
