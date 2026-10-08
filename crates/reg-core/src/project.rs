//! The `project_data.json` types and their two JSON encodings (`RUST_RUNTIME_SPEC.md`
//! section 5, package 3e.1).
//!
//! The types are today's `reg_schema.project_data` models. They keep the raw spelling
//! of every value (an int year stays an int, `"2018"` stays a string), and serialize
//! as Pydantic's `model_dump(mode="json")` does: every optional field present, absent
//! as `null`, `panels` as `[]`, a bare-string panel member as `{"source": ...}`. That
//! shape is what [`project_hash`] hashes.
//!
//! Deserialize one only after [`crate::validate_structural`] has accepted the
//! document: the validator reports every problem, while deserialization stops at the
//! first. A document the validator accepts always deserializes.

use std::fmt::Write as _;

use serde::de::{Deserializer, Error as _};
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// A closed string enum: one list of `(variant, wire name)` pairs gives the serde
/// encoding and the allowed values the validator reports.
macro_rules! str_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $wire:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &[Self] = &[$(Self::$variant),+];

            /// The wire spelling.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }

            /// The value spelled `s`, if any.
            #[must_use]
            pub fn parse(s: &str) -> Option<Self> {
                Self::ALL.iter().copied().find(|v| v.as_str() == s)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectData {
    pub schema_version: String,
    pub steward: Steward,
    pub reg_meta_version: String,
    pub name: String,
    pub sources: Vec<Source>,
    #[serde(default)]
    pub panels: Vec<Panel>,
    #[serde(default)]
    pub window: Option<StudyWindow>,
}

/// One logical extraction: a register variant, a requested period and its bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
/// included, which only a scalar [`SourcePeriod`] may hold).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PeriodValue {
    Year(i64),
    Token(String),
}

/// The `{"from": ..., "to": ...}` range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeriodRange {
    pub r#from: PeriodValue,
    pub to: PeriodValue,
}

/// One contiguous piece of a source period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PeriodSegment {
    Value(PeriodValue),
    Range(PeriodRange),
}

/// `Source.period`: one segment, or a sorted, disjoint list of them (an interrupted
/// series).
///
/// A list variant comes first in each untagged enum: serde also reads a struct from
/// a JSON array, by position, so `["2018-01", "2018-04"]` would otherwise become a
/// [`PeriodRange`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SourcePeriod {
    List(Vec<PeriodSegment>),
    Segment(PeriodSegment),
}

/// A panel over sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Panel {
    pub panel_id: String,
    #[serde(deserialize_with = "members")]
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
#[serde(deny_unknown_fields)]
pub struct PanelMember {
    pub source: String,
    #[serde(default)]
    pub entity_key: Option<EntityKey>,
    #[serde(default)]
    pub time_key: Option<TimeKey>,
}

fn members<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<PanelMember>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Member {
        Source(String),
        Member(PanelMember),
    }
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
#[serde(untagged)]
pub enum EntityKey {
    Column(String),
    Composite(Vec<String>),
}

/// A panel's time key: one time point, or a composite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TimeKey {
    Composite(Vec<TimePoint>),
    Point(TimePoint),
}

/// A literal year, a column ref, `{"period": ...}` or `{"range": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TimePoint {
    Year(i64),
    Column(String),
    Literal(LiteralPeriod),
    Range(TimeRange),
}

/// `{"period": int | string}`: a literal period, as opposed to a column ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiteralPeriod {
    pub period: PeriodValue,
}

/// `{"range": {"from": ..., "to": ...}}`: a literal period range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    pub range: PeriodRange,
}

/// The optional study window, in plain int years.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StudyWindow {
    pub r#from: i64,
    pub to: i64,
}

/// `value` as a JSON tree. Its objects are `serde_json::Map`, a `BTreeMap` (the
/// workspace never enables `preserve_order`), so keys come out sorted by code point
/// as Python's `sort_keys=True` sorts them; struct field order never leaks.
fn tree<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("project and result types serialize to JSON")
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
/// Today's `reg_meta.order._project_hash`.
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
