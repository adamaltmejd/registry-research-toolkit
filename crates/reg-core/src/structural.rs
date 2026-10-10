//! The structural validator of `project_data.json`: every rule checkable from the
//! document alone, ported from `reg_schema/structural.py` (`RUST_RUNTIME_SPEC.md`
//! package 3e.1).
//!
//! It reads the raw JSON tree, not the [`crate::project`] types, because it must
//! report on documents those types cannot hold, and it accumulates every issue rather
//! than stopping at the first. Issues come out in the Python validator's order.
//!
//! Each message is a fixed template filled with the offending values; a quoted value
//! is a string in single quotes, each character but `"` escaped with Rust's
//! `char::escape_debug` (not Python `repr`). The oracle is
//! `crates/reg-core/tests/project/corpus/`, whose cases pin the quoting and these
//! other intentional differences from the retired Python validator:
//!
//! - Periods go through the period grammar, so an int year outside 1900..=2099 is an
//!   `invalid_period`, and list bounds are calendar-exact (February ends on the 28th
//!   in a common year).
//! - FQID segments are exactly `[A-Za-z0-9_-]+`; Python's `$` also let one trailing
//!   newline through.
//! - `"_default"` without a usable `register_variant` is an `invalid_period`; Python
//!   raised `UnboundLocalError`.
//! - A non-string `representation` is an `invalid_field_type`; Python let it through
//!   to fail model construction.
//! - Composite literal time keys render as their values (`[2018, '2019-H1']`), not as
//!   Python's internal canonical tuples.
//! - An int is a JSON integer that fits `i64`.

use serde_json::{Map, Value};

use crate::grammar::{Date, PeriodToken};
use crate::project::{ColumnType, IdSubtype, NumericSubtype, Steward};
use crate::validation::{IssueLevel, ValidationIssue, ValidationResult};

const PROJECT_REQUIRED: [&str; 5] = [
    "schema_version",
    "steward",
    "reg_meta_version",
    "name",
    "sources",
];
const PROJECT_OPTIONAL: [&str; 2] = ["panels", "window"];
const SOURCE_KEYS: [&str; 4] = ["name", "register_variant", "period", "bindings"];
const BINDING_KEYS: [&str; 9] = [
    "variable",
    "type",
    "display_name",
    "id_subtype",
    "numeric_subtype",
    "date_format",
    "datetime_format",
    "value_set",
    "representation",
];
const PANEL_KEYS: [&str; 5] = ["panel_id", "members", "entity_key", "time_key", "comment"];
const PANEL_MEMBER_KEYS: [&str; 3] = ["source", "entity_key", "time_key"];
const WINDOW_KEYS: [&str; 2] = ["from", "to"];

/// Fields valid only on one column type, with that type.
const SUBTYPE_FIELDS: [(&str, ColumnType); 4] = [
    ("id_subtype", ColumnType::Id),
    ("numeric_subtype", ColumnType::Numeric),
    ("date_format", ColumnType::Date),
    ("datetime_format", ColumnType::Datetime),
];

const PERIOD_FORMS: &str =
    "(YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY, VTYYYY, LA<YYYY>, YYYY-Q[1-4], YYYY-H[12])";

/// Run the structural rules over a parsed `project_data.json`. A non-object root is an
/// `invalid_root` issue, never a panic.
#[must_use]
pub fn validate_structural(data: &Value) -> ValidationResult {
    let mut issues = Issues::default();
    match data.as_object() {
        None => issues.error(
            "invalid_root",
            String::new(),
            "project_data.json root must be an object".into(),
        ),
        Some(root) => {
            check_top_level(root, &mut issues);
            check_window(get(root, "window"), &mut issues);
            check_sources(get(root, "sources"), &mut issues);
            check_panels(get(root, "panels"), get(root, "sources"), &mut issues);
        }
    }
    ValidationResult { issues: issues.0 }
}

#[derive(Default)]
struct Issues(Vec<ValidationIssue>);

impl Issues {
    fn error(&mut self, code: &'static str, path: String, message: String) {
        self.0.push(ValidationIssue {
            level: IssueLevel::Error,
            code,
            path,
            message,
            successor_fqid: None,
        });
    }
}

/// `key`'s value unless absent or `null` (Python's `dict.get(key) is None`).
fn get<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    map.get(key).filter(|v| !v.is_null())
}

/// A string quoted for a message (see the module documentation); exported as
/// [`crate::quote`] for the semantic layer's messages.
#[must_use]
pub fn q(s: &str) -> String {
    let mut quoted = String::from("'");
    for c in s.chars() {
        if c == '"' {
            quoted.push(c);
        } else {
            quoted.extend(c.escape_debug());
        }
    }
    quoted.push('\'');
    quoted
}

/// Strings quoted for a message as a list: `['a', 'b']`.
pub fn quoted_list<'a>(items: impl IntoIterator<Item = &'a str>) -> String {
    let items: Vec<String> = items.into_iter().map(q).collect();
    format!("[{}]", items.join(", "))
}

/// The allowed values of a closed enum, sorted, for a message.
fn allowed(values: impl IntoIterator<Item = &'static str>) -> String {
    let mut values: Vec<&str> = values.into_iter().collect();
    values.sort_unstable();
    quoted_list(values)
}

/// An RFC 6901 reference token.
fn jp_escape(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// An int: a JSON integer that fits `i64` (never a bool or a float).
fn int(v: &Value) -> Option<i64> {
    v.as_i64()
}

fn iso((y, m, d): Date) -> String {
    format!("{y:04}-{m:02}-{d:02}")
}

fn check_unexpected_keys(
    map: &Map<String, Value>,
    allowed: &[&str],
    base: &str,
    label: &str,
    issues: &mut Issues,
) {
    // `serde_json::Map` iterates in key order, as Python's `sorted(...)` does.
    for key in map.keys().filter(|k| !allowed.contains(&k.as_str())) {
        issues.error(
            "unexpected_field",
            format!("{base}/{}", jp_escape(key)),
            format!("unexpected key {} on {label}", q(key)),
        );
    }
}

/// `field`'s value if present and not `null`; otherwise reports
/// `missing_required_field` or `invalid_field_type`.
fn required<'a>(
    map: &'a Map<String, Value>,
    field: &str,
    base: &str,
    label: &str,
    issues: &mut Issues,
) -> Option<&'a Value> {
    match map.get(field) {
        None => {
            issues.error(
                "missing_required_field",
                format!("{base}/{field}"),
                format!("{label} is required"),
            );
            None
        }
        Some(Value::Null) => {
            issues.error(
                "invalid_field_type",
                format!("{base}/{field}"),
                format!("{label} must not be null"),
            );
            None
        }
        Some(v) => Some(v),
    }
}

/// A required field that must be a string.
fn required_string<'a>(
    map: &'a Map<String, Value>,
    field: &str,
    base: &str,
    label: &str,
    issues: &mut Issues,
) -> Option<&'a str> {
    let value = required(map, field, base, label, issues)?;
    let s = value.as_str();
    if s.is_none() {
        issues.error(
            "invalid_field_type",
            format!("{base}/{field}"),
            format!("{label} must be a string"),
        );
    }
    s
}

/// An optional field that, when present and not `null`, must be a string.
fn optional_string(
    map: &Map<String, Value>,
    field: &str,
    base: &str,
    label: &str,
    issues: &mut Issues,
) {
    if get(map, field).is_some_and(|v| !v.is_string()) {
        issues.error(
            "invalid_field_type",
            format!("{base}/{field}"),
            format!("{label} must be a string"),
        );
    }
}

// --- FQIDs --------------------------------------------------------------

// Not `grammar::Fqid`: its slugs are lowercase kebab-case, while the frozen corpus
// accepts underscores and the reserved `_default` (`hush_type`, `.../_default`).

fn fqid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// `<provider>/<register>/<variant or slug>`, the provider not `class`.
fn three_part(s: &str) -> Option<Vec<&str>> {
    let segs: Vec<&str> = s.split('/').collect();
    (segs.len() == 3 && segs[0] != "class" && segs.iter().all(|s| fqid_segment(s))).then_some(segs)
}

fn classification_fqid(s: &str) -> bool {
    let segs: Vec<&str> = s.split('/').collect();
    segs.len() == 2 && segs[0] == "class" && segs.iter().all(|s| fqid_segment(s))
}

// --- Periods ------------------------------------------------------------

/// A period endpoint: an int year or a period-token string, through the grammar.
fn endpoint(v: &Value) -> Option<PeriodToken> {
    match v {
        Value::Number(_) => PeriodToken::parse(&int(v)?.to_string()),
        Value::String(s) => PeriodToken::parse(s),
        _ => None,
    }
}

/// A `{"from", "to"}` object with valid endpoints, as its first and last day.
fn period_range(v: &Value) -> Option<(Date, Date)> {
    let map = v.as_object()?;
    if map.len() != 2 {
        return None;
    }
    let from = endpoint(map.get("from")?)?;
    let to = endpoint(map.get("to")?)?;
    Some((from.bounds().0, to.bounds().1))
}

/// One segment of a source period (an endpoint or a range; not a nested list), as its
/// first and last day.
fn segment(v: &Value) -> Option<(Date, Date)> {
    endpoint(v)
        .map(PeriodToken::bounds)
        .or_else(|| period_range(v))
}

fn check_period(period: &Value, base: &str, issues: &mut Issues) {
    let path = format!("{base}/period");
    if period == "_default" || endpoint(period).is_some() {
        return;
    }
    let message = match period {
        Value::Number(_) if int(period).is_some() => {
            format!("period year {period} must be from 1900 to 2099")
        }
        Value::String(s) => format!(
            "period string {} must match a period grammar form {PERIOD_FORMS}",
            q(s)
        ),
        Value::Object(_) if period_range(period).is_some() => return,
        Value::Object(_) => "period object must be {'from': ..., 'to': ...} with int or \
                             period-token endpoints"
            .into(),
        Value::Array(list) => return check_period_list(list, &path, issues),
        _ => "period must be an int, a period-token string, a {'from','to'} range object, \
              or a list of those segment forms"
            .into(),
    };
    issues.error("invalid_period", path, message);
}

/// The list form (an interrupted series): non-empty, every member a segment, each
/// member not inverted, members sorted ascending and not overlapping (adjacent is
/// fine).
fn check_period_list(list: &[Value], path: &str, issues: &mut Issues) {
    if list.is_empty() {
        issues.error(
            "invalid_period",
            path.to_owned(),
            "period list must be non-empty (a list period is an interrupted series of segments)"
                .into(),
        );
        return;
    }
    let bounds: Vec<Option<(Date, Date)>> = list.iter().map(segment).collect();
    for (i, b) in bounds.iter().enumerate() {
        if b.is_none() {
            issues.error(
                "invalid_period",
                format!("{path}/{i}"),
                "period list member must be an int year, a period-token string, or a \
                 {'from','to'} range object (a nested list is not a segment)"
                    .into(),
            );
        }
    }
    // Ordering needs every member valid; the issues above already fail the document.
    let Some(bounds) = bounds.into_iter().collect::<Option<Vec<_>>>() else {
        return;
    };
    let mut inverted = false;
    for (i, &(lo, hi)) in bounds.iter().enumerate() {
        if lo > hi {
            inverted = true;
            issues.error(
                "invalid_period",
                format!("{path}/{i}"),
                format!(
                    "period list member {i} is an inverted range (starts {}, ends {})",
                    iso(lo),
                    iso(hi)
                ),
            );
        }
    }
    if inverted {
        return;
    }
    for (i, pair) in bounds.windows(2).enumerate() {
        let ((prev_lo, prev_hi), (lo, _)) = (pair[0], pair[1]);
        let (prev, i) = (i, i + 1);
        let message = if lo < prev_lo {
            format!(
                "period list members must be sorted ascending: member {i} starts {}, before \
                 member {prev} ({})",
                iso(lo),
                iso(prev_lo)
            )
        } else if lo <= prev_hi {
            format!(
                "period list members must not overlap: member {i} starts {}, inside member \
                 {prev} (ends {})",
                iso(lo),
                iso(prev_hi)
            )
        } else {
            continue;
        };
        issues.error("invalid_period", format!("{path}/{i}"), message);
    }
}

// --- Top level ----------------------------------------------------------

fn check_top_level(root: &Map<String, Value>, issues: &mut Issues) {
    let keys: Vec<&str> = PROJECT_REQUIRED
        .into_iter()
        .chain(PROJECT_OPTIONAL)
        .collect();
    check_unexpected_keys(root, &keys, "", "project root", issues);
    for field in PROJECT_REQUIRED {
        match root.get(field) {
            None => issues.error(
                "missing_required_field",
                format!("/{field}"),
                format!("required top-level field {} is missing", q(field)),
            ),
            Some(Value::Null) => issues.error(
                "invalid_field_type",
                format!("/{field}"),
                format!("{field} must not be null"),
            ),
            Some(_) => {}
        }
    }
    for field in PROJECT_OPTIONAL {
        if root.get(field) == Some(&Value::Null) {
            issues.error(
                "invalid_field_type",
                format!("/{field}"),
                format!("{field} must not be null when present"),
            );
        }
    }
    for field in ["schema_version", "reg_meta_version", "name"] {
        if get(root, field).is_some_and(|v| !v.is_string()) {
            issues.error(
                "invalid_field_type",
                format!("/{field}"),
                format!("{field} must be a string"),
            );
        }
    }
    match get(root, "steward") {
        None => {}
        Some(Value::String(s)) if Steward::parse(s).is_some() => {}
        Some(Value::String(s)) => issues.error(
            "invalid_enum_value",
            "/steward".into(),
            format!(
                "steward must be one of {}; got {}",
                allowed(Steward::ALL.iter().map(|v| v.as_str())),
                q(s)
            ),
        ),
        Some(_) => issues.error(
            "invalid_field_type",
            "/steward".into(),
            "steward must be a string".into(),
        ),
    }
}

/// The optional study window: a closed `{"from", "to"}` of int years, `to >= from`.
fn check_window(window: Option<&Value>, issues: &mut Issues) {
    let Some(window) = window else { return };
    let Some(map) = window.as_object() else {
        issues.error(
            "invalid_field_type",
            "/window".into(),
            "window must be an object {'from': <year>, 'to': <year>}".into(),
        );
        return;
    };
    check_unexpected_keys(map, &WINDOW_KEYS, "/window", "window", issues);
    let mut years = [None, None];
    for (year, field) in years.iter_mut().zip(WINDOW_KEYS) {
        let label = format!("window {}", q(field));
        let Some(value) = required(map, field, "/window", &label, issues) else {
            continue;
        };
        *year = int(value);
        if year.is_none() {
            issues.error(
                "invalid_field_type",
                format!("/window/{field}"),
                format!("{label} must be an integer year"),
            );
        }
    }
    if let [Some(from), Some(to)] = years
        && to < from
    {
        issues.error(
            "invalid_window",
            "/window".into(),
            format!("window 'to' ({to}) must be >= 'from' ({from})"),
        );
    }
}

// --- Sources ------------------------------------------------------------

fn check_sources(sources: Option<&Value>, issues: &mut Issues) {
    let Some(sources) = sources else { return };
    let Some(sources) = sources.as_array() else {
        issues.error(
            "invalid_field_type",
            "/sources".into(),
            "sources must be an array".into(),
        );
        return;
    };
    let mut names: Vec<(&str, usize)> = Vec::new();
    for (i, source) in sources.iter().enumerate() {
        let base = format!("/sources/{i}");
        match source.as_object() {
            Some(source) => check_source(source, &base, i, &mut names, issues),
            None => issues.error(
                "invalid_field_type",
                base,
                "source must be an object".into(),
            ),
        }
    }
}

fn check_source<'a>(
    source: &'a Map<String, Value>,
    base: &str,
    index: usize,
    names: &mut Vec<(&'a str, usize)>,
    issues: &mut Issues,
) {
    check_unexpected_keys(source, &SOURCE_KEYS, base, "source", issues);
    if let Some(name) = required_string(source, "name", base, "source 'name'", issues) {
        match names.iter().find(|(n, _)| *n == name) {
            Some((_, first)) => issues.error(
                "duplicate_source_name",
                format!("{base}/name"),
                format!("source name {} duplicates /sources/{first}", q(name)),
            ),
            None => names.push((name, index)),
        }
    }

    // The coordinate's provider/register prefix scopes every binding FQID.
    let rv = required_string(
        source,
        "register_variant",
        base,
        "source 'register_variant'",
        issues,
    );
    let variant = rv.and_then(three_part);
    if let (Some(rv), None) = (rv, &variant) {
        issues.error(
            "invalid_fqid",
            format!("{base}/register_variant"),
            format!(
                "register_variant must be a 3-part variant coordinate \
                 <provider>/<register>/<variant>; got {}",
                q(rv)
            ),
        );
    }

    if let Some(period) = required(source, "period", base, "source 'period'", issues) {
        check_period(period, base, issues);
        if period == "_default" && variant.as_ref().is_none_or(|v| v[2] == "_default") {
            issues.error(
                "invalid_period",
                format!("{base}/period"),
                "year-independent selection requires a concrete register_variant".into(),
            );
        }
    }

    let Some(bindings) = required(source, "bindings", base, "source 'bindings'", issues) else {
        return;
    };
    let Some(bindings) = bindings.as_array() else {
        issues.error(
            "invalid_field_type",
            format!("{base}/bindings"),
            "source 'bindings' must be an array".into(),
        );
        return;
    };
    if bindings.is_empty() {
        issues.error(
            "empty_bindings",
            format!("{base}/bindings"),
            "source must have at least one column".into(),
        );
        return;
    }
    let prefix = variant.as_ref().map(|v| &v[..2]);
    let mut display_names: Vec<(&str, String)> = Vec::new();
    for (j, binding) in bindings.iter().enumerate() {
        let binding_base = format!("{base}/bindings/{j}");
        let Some(binding) = binding.as_object() else {
            issues.error(
                "invalid_field_type",
                binding_base,
                "binding must be an object".into(),
            );
            continue;
        };
        check_binding(binding, &binding_base, prefix, issues);
        let Some(dn) = binding.get("display_name").and_then(Value::as_str) else {
            continue;
        };
        let path = format!("{binding_base}/display_name");
        match display_names.iter().find(|(n, _)| *n == dn) {
            Some((_, prior)) => issues.error(
                "display_name_collision",
                path,
                format!("display_name {} duplicates the one at {prior}", q(dn)),
            ),
            None => display_names.push((dn, path)),
        }
    }
}

fn check_binding(
    binding: &Map<String, Value>,
    base: &str,
    prefix: Option<&[&str]>,
    issues: &mut Issues,
) {
    check_unexpected_keys(binding, &BINDING_KEYS, base, "binding", issues);
    if let Some(variable) = required_string(binding, "variable", base, "binding 'variable'", issues)
    {
        match three_part(variable) {
            None => issues.error(
                "invalid_fqid",
                format!("{base}/variable"),
                format!(
                    "binding 'variable' must be a 3-segment binding FQID \
                     <provider>/<register>/<slug>; got {}",
                    q(variable)
                ),
            ),
            Some(segs) => {
                if let Some(prefix) = prefix
                    && segs[..2] != *prefix
                {
                    issues.error(
                        "fqid_register_variant_mismatch",
                        format!("{base}/variable"),
                        format!(
                            "binding FQID prefix {} must equal the source register_variant \
                             prefix {}",
                            quoted_list(segs[..2].iter().copied()),
                            quoted_list(prefix.iter().copied())
                        ),
                    );
                }
            }
        }
    }

    let typ = required_string(binding, "type", base, "binding 'type'", issues);
    let column_type = typ.and_then(ColumnType::parse);
    if let (Some(typ), None) = (typ, column_type) {
        issues.error(
            "invalid_enum_value",
            format!("{base}/type"),
            format!(
                "binding type must be one of {}; got {}",
                allowed(ColumnType::ALL.iter().map(|v| v.as_str())),
                q(typ)
            ),
        );
    }

    optional_string(
        binding,
        "display_name",
        base,
        "binding 'display_name'",
        issues,
    );
    optional_string(
        binding,
        "representation",
        base,
        "binding 'representation'",
        issues,
    );
    check_subtypes(binding, base, column_type, issues);

    match get(binding, "value_set") {
        None => {}
        Some(Value::String(vs)) if classification_fqid(vs) => {}
        Some(Value::String(vs)) => issues.error(
            "invalid_fqid",
            format!("{base}/value_set"),
            format!(
                "value_set must be a 2-segment classification FQID class/<slug>; got {}",
                q(vs)
            ),
        ),
        Some(_) => issues.error(
            "invalid_field_type",
            format!("{base}/value_set"),
            "value_set must be a string".into(),
        ),
    }
}

/// The subtype and format fields: each only on its column type, a string, and the
/// subtypes from their enums.
fn check_subtypes(
    binding: &Map<String, Value>,
    base: &str,
    column_type: Option<ColumnType>,
    issues: &mut Issues,
) {
    for (field, owner) in SUBTYPE_FIELDS {
        let Some(value) = get(binding, field) else {
            continue;
        };
        if let Some(typ) = column_type
            && typ != owner
        {
            issues.error(
                "subtype_on_wrong_type",
                format!("{base}/{field}"),
                format!(
                    "{} is only valid on type={}; binding type is {}",
                    q(field),
                    q(owner.as_str()),
                    q(typ.as_str())
                ),
            );
            continue;
        }
        let Some(value) = value.as_str() else {
            issues.error(
                "invalid_field_type",
                format!("{base}/{field}"),
                format!("{field} must be a string"),
            );
            continue;
        };
        let values: Vec<&str> = match field {
            "id_subtype" if IdSubtype::parse(value).is_none() => {
                IdSubtype::ALL.iter().map(|v| v.as_str()).collect()
            }
            "numeric_subtype" if NumericSubtype::parse(value).is_none() => {
                NumericSubtype::ALL.iter().map(|v| v.as_str()).collect()
            }
            _ => continue,
        };
        issues.error(
            "invalid_enum_value",
            format!("{base}/{field}"),
            format!(
                "{field} must be one of {}; got {}",
                allowed(values),
                q(value)
            ),
        );
    }
}

// --- Panel keys ---------------------------------------------------------

/// `{"period": int | string}`; the string is not grammar-checked here (a semantic
/// concern).
fn literal_period(v: &Value) -> Option<&Value> {
    let map = v.as_object()?;
    let period = map.get("period").filter(|_| map.len() == 1)?;
    (int(period).is_some() || period.is_string()).then_some(period)
}

/// `{"range": {"from", "to"}}` with valid endpoints, as its raw endpoints.
fn time_range(v: &Value) -> Option<(&Value, &Value)> {
    let map = v.as_object()?;
    let range = map.get("range").filter(|_| map.len() == 1)?;
    period_range(range)?;
    Some((&range["from"], &range["to"]))
}

/// A literal time point as written, so `2018` and `{"period": 2018}` compare equal.
#[derive(Debug, Clone, PartialEq)]
enum Literal<'a> {
    Period(&'a Value),
    Range(&'a Value, &'a Value),
}

impl Literal<'_> {
    fn of(v: &Value) -> Option<Literal<'_>> {
        if int(v).is_some() {
            return Some(Literal::Period(v));
        }
        if let Some(period) = literal_period(v) {
            return Some(Literal::Period(period));
        }
        time_range(v).map(|(from, to)| Literal::Range(from, to))
    }

    fn render(&self) -> String {
        let value = |v: &Value| v.as_str().map_or_else(|| v.to_string(), q);
        match self {
            Literal::Period(v) => value(v),
            Literal::Range(from, to) => format!("{}..{}", value(from), value(to)),
        }
    }
}

/// A literal time key, scalar or composite, for the uniqueness rule.
#[derive(Debug, Clone, PartialEq)]
enum LiteralKey<'a> {
    Scalar(Literal<'a>),
    Composite(Vec<Literal<'a>>),
}

fn literal_key(v: &Value) -> Option<LiteralKey<'_>> {
    match v {
        Value::Array(items) => items
            .iter()
            .map(Literal::of)
            .collect::<Option<_>>()
            .map(LiteralKey::Composite),
        _ => Literal::of(v).map(LiteralKey::Scalar),
    }
}

/// What a valid time key holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeKind {
    LiteralScalar,
    RefScalar,
    LiteralComposite,
    RefComposite,
}

impl TimeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LiteralScalar => "literal_scalar",
            Self::RefScalar => "ref_scalar",
            Self::LiteralComposite => "literal_composite",
            Self::RefComposite => "ref_composite",
        }
    }

    fn composite(self) -> bool {
        matches!(self, Self::LiteralComposite | Self::RefComposite)
    }
}

/// A composite key as compared across members: column refs, or literal values.
#[derive(Debug, Clone, PartialEq)]
enum Composite<'a> {
    Refs(Vec<&'a str>),
    Literals(Vec<Literal<'a>>),
}

impl Composite<'_> {
    fn render(&self) -> String {
        match self {
            Composite::Refs(refs) => quoted_list(refs.iter().copied()),
            Composite::Literals(literals) => {
                let items: Vec<String> = literals.iter().map(Literal::render).collect();
                format!("[{}]", items.join(", "))
            }
        }
    }
}

/// Whether an entity key is a string or a non-empty array of strings.
fn check_entity_key(value: &Value, path: &str, issues: &mut Issues) -> bool {
    match value {
        Value::String(_) => true,
        Value::Array(items) if items.is_empty() => {
            issues.error(
                "invalid_field_type",
                path.to_owned(),
                "entity_key array must be non-empty".into(),
            );
            false
        }
        Value::Array(items) => {
            let mut ok = true;
            for (i, item) in items.iter().enumerate() {
                if !item.is_string() {
                    ok = false;
                    issues.error(
                        "invalid_field_type",
                        format!("{path}/{i}"),
                        "entity_key array elements must be strings".into(),
                    );
                }
            }
            ok
        }
        _ => {
            issues.error(
                "invalid_field_type",
                path.to_owned(),
                "entity_key must be a string or array of strings".into(),
            );
            false
        }
    }
}

/// A time key's kind, or `None` (with an issue) when its shape is invalid. A
/// composite must be all refs or all literals.
fn check_time_key(value: &Value, path: &str, issues: &mut Issues) -> Option<TimeKind> {
    match value {
        Value::String(_) => Some(TimeKind::RefScalar),
        Value::Array(items) if items.is_empty() => {
            issues.error(
                "invalid_field_type",
                path.to_owned(),
                "time_key array must be non-empty".into(),
            );
            None
        }
        Value::Array(items) => {
            let mut kinds = Vec::new();
            for (i, item) in items.iter().enumerate() {
                if item.is_string() {
                    kinds.push(TimeKind::RefComposite);
                } else if Literal::of(item).is_some() {
                    kinds.push(TimeKind::LiteralComposite);
                } else {
                    issues.error(
                        "invalid_field_type",
                        format!("{path}/{i}"),
                        "time_key element must be int, string, {'period': int|str}, or \
                         {'range': {'from','to'}}"
                            .into(),
                    );
                }
            }
            if kinds.len() < items.len() {
                return None;
            }
            if kinds.iter().any(|&k| k != kinds[0]) {
                issues.error(
                    "composite_time_key_mixed_kinds",
                    path.to_owned(),
                    "composite time_key array must be homogeneous: all column refs or all \
                     literals"
                        .into(),
                );
                return None;
            }
            Some(kinds[0])
        }
        _ if Literal::of(value).is_some() => Some(TimeKind::LiteralScalar),
        Value::Object(_) => {
            issues.error(
                "literal_period_invalid",
                path.to_owned(),
                "time_key object form must be {'period': int|str} or {'range': {'from','to'}} \
                 with period-token endpoints"
                    .into(),
            );
            None
        }
        _ => {
            issues.error(
                "invalid_field_type",
                path.to_owned(),
                "time_key must be int, string, {'period': ...}, or an array".into(),
            );
            None
        }
    }
}

/// The column refs of a key: the string, or an array's strings.
fn refs(value: Option<&Value>) -> Vec<&str> {
    match value {
        Some(Value::String(s)) => vec![s],
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

// --- Panels -------------------------------------------------------------

/// A source's display names, for the panel key-ref rule. `complete` is false when any
/// binding may lack an explicit `display_name` (its default needs the catalog), and
/// the rule is then skipped.
struct SourceColumns<'a> {
    display_names: Vec<&'a str>,
    complete: bool,
}

/// Sources by name (the first of a duplicated name), when `/sources` is an array.
fn source_index(sources: Option<&Value>) -> Option<Vec<(&str, SourceColumns<'_>)>> {
    let sources = sources?.as_array()?;
    let mut index: Vec<(&str, SourceColumns<'_>)> = Vec::new();
    for source in sources.iter().filter_map(Value::as_object) {
        let Some(name) = source.get("name").and_then(Value::as_str) else {
            continue;
        };
        if index.iter().any(|(n, _)| *n == name) {
            continue;
        }
        let bindings = source.get("bindings").and_then(Value::as_array);
        let names: Vec<Option<&str>> = bindings
            .into_iter()
            .flatten()
            .map(|b| b.get("display_name").and_then(Value::as_str))
            .collect();
        index.push((
            name,
            SourceColumns {
                complete: bindings.is_some() && names.iter().all(Option::is_some),
                display_names: names.into_iter().flatten().collect(),
            },
        ));
    }
    Some(index)
}

/// A member's effective keys: its overrides, or the panel defaults.
#[derive(Clone)]
struct Keys<'a> {
    entity: Option<&'a Value>,
    entity_path: String,
    entity_ok: bool,
    time: Option<&'a Value>,
    time_path: String,
    time_kind: Option<TimeKind>,
    time_overridden: bool,
}

/// What one panel's members share: the panel defaults and the cross-member
/// accumulators.
struct PanelScope<'a> {
    base: String,
    defaults: Keys<'a>,
    composite_entities: Vec<(String, Composite<'a>)>,
    composite_times: Vec<(String, Composite<'a>)>,
    // simplify: linear scan; a panel has a handful of members.
    literal_times: Vec<(LiteralKey<'a>, String)>,
}

/// Panel-spanning state.
struct Panels<'a> {
    /// `None` when `/sources` is not an array, so names cannot resolve.
    sources: Option<Vec<(&'a str, SourceColumns<'a>)>>,
    /// Source name to the first panel that references it.
    source_panel: Vec<(&'a str, String)>,
    panel_ids: Vec<(&'a str, usize)>,
}

fn check_panels(panels: Option<&Value>, sources: Option<&Value>, issues: &mut Issues) {
    let Some(panels) = panels else { return };
    let Some(panels) = panels.as_array() else {
        issues.error(
            "invalid_field_type",
            "/panels".into(),
            "panels must be an array".into(),
        );
        return;
    };
    let mut state = Panels {
        sources: source_index(sources),
        source_panel: Vec::new(),
        panel_ids: Vec::new(),
    };
    for (pi, panel) in panels.iter().enumerate() {
        let base = format!("/panels/{pi}");
        match panel.as_object() {
            Some(panel) => check_panel(panel, base, pi, &mut state, issues),
            None => issues.error("invalid_field_type", base, "panel must be an object".into()),
        }
    }
}

fn check_panel<'a>(
    panel: &'a Map<String, Value>,
    base: String,
    index: usize,
    state: &mut Panels<'a>,
    issues: &mut Issues,
) {
    check_unexpected_keys(panel, &PANEL_KEYS, &base, "panel", issues);
    if let Some(id) = required_string(panel, "panel_id", &base, "panel 'panel_id'", issues) {
        match state.panel_ids.iter().find(|(p, _)| *p == id) {
            Some((_, first)) => issues.error(
                "duplicate_panel_id",
                format!("{base}/panel_id"),
                format!("panel_id {} duplicates /panels/{first}", q(id)),
            ),
            None => state.panel_ids.push((id, index)),
        }
    }
    optional_string(panel, "comment", &base, "panel 'comment'", issues);
    let entity = get(panel, "entity_key");
    let entity_path = format!("{base}/entity_key");
    let entity_ok = entity.is_none_or(|e| check_entity_key(e, &entity_path, issues));
    let time = get(panel, "time_key");
    let time_path = format!("{base}/time_key");
    let time_kind = time.and_then(|t| check_time_key(t, &time_path, issues));

    let Some(members) = required(panel, "members", &base, "panel 'members'", issues) else {
        return;
    };
    let Some(members) = members.as_array() else {
        issues.error(
            "invalid_field_type",
            format!("{base}/members"),
            "panel 'members' must be an array".into(),
        );
        return;
    };
    if members.is_empty() {
        issues.error(
            "empty_members",
            format!("{base}/members"),
            "panel must have at least one member".into(),
        );
        return;
    }
    let mut scope = PanelScope {
        base,
        defaults: Keys {
            entity,
            entity_path,
            entity_ok,
            time,
            time_path,
            time_kind,
            time_overridden: false,
        },
        composite_entities: Vec::new(),
        composite_times: Vec::new(),
        literal_times: Vec::new(),
    };
    for (mi, member) in members.iter().enumerate() {
        let mbase = format!("{}/members/{mi}", scope.base);
        let Some((source, keys)) = member_keys(member, &mbase, &scope.defaults, issues) else {
            continue;
        };
        collect_keys(&keys, &mbase, &mut scope, issues);
        if let Some(source) = source {
            check_member_source(source, member, &mbase, &keys, &scope.base, state, issues);
        }
    }
    // Scalar keys may differ across members; only composites must agree, in order.
    check_composites(&scope.composite_entities, "entity_key", issues);
    check_composites(&scope.composite_times, "time_key", issues);
}

fn check_composites(composites: &[(String, Composite<'_>)], label: &str, issues: &mut Issues) {
    let Some(((first_path, first), rest)) = composites.split_first() else {
        return;
    };
    for (path, composite) in rest {
        if composite != first {
            issues.error(
                "composite_key_inconsistent",
                path.clone(),
                format!(
                    "composite {label} {} differs from {} at {first_path}",
                    composite.render(),
                    first.render()
                ),
            );
        }
    }
}

/// A member override: absent inherits the panel default; explicit `null` is an
/// `invalid_field_type` and also inherits, so the later rules still have a key.
fn member_override<'a>(
    member: &'a Map<String, Value>,
    field: &str,
    mbase: &str,
    issues: &mut Issues,
) -> Option<&'a Value> {
    if member.get(field) == Some(&Value::Null) {
        issues.error(
            "invalid_field_type",
            format!("{mbase}/{field}"),
            format!("panel member {} must not be null when present", q(field)),
        );
    }
    get(member, field)
}

/// A member's source name (if valid) and effective keys, or `None` for a member
/// that is neither a source name nor an object.
fn member_keys<'a>(
    member: &'a Value,
    mbase: &str,
    defaults: &Keys<'a>,
    issues: &mut Issues,
) -> Option<(Option<&'a str>, Keys<'a>)> {
    let map = match member {
        Value::String(source) => return Some((Some(source), defaults.clone())),
        Value::Object(map) => map,
        _ => {
            issues.error(
                "invalid_field_type",
                mbase.to_owned(),
                "panel member must be a string (source name) or an object".into(),
            );
            return None;
        }
    };
    check_unexpected_keys(map, &PANEL_MEMBER_KEYS, mbase, "panel member", issues);
    let source = required_string(map, "source", mbase, "panel member 'source'", issues);
    let mut keys = defaults.clone();
    if let Some(entity) = member_override(map, "entity_key", mbase, issues) {
        keys.entity = Some(entity);
        keys.entity_path = format!("{mbase}/entity_key");
        keys.entity_ok = check_entity_key(entity, &keys.entity_path, issues);
    }
    if let Some(time) = member_override(map, "time_key", mbase, issues) {
        keys.time = Some(time);
        keys.time_path = format!("{mbase}/time_key");
        keys.time_kind = check_time_key(time, &keys.time_path, issues);
        keys.time_overridden = true;
    }
    Some((source, keys))
}

/// The panel-wide key rules: a member's composite time key of the panel's composite
/// kind, composites gathered for the ordering rule, literal time keys unique.
///
/// An absent effective key is no structural error: it is inherited from the
/// variant's panel template, which needs the catalog.
fn collect_keys<'a>(keys: &Keys<'a>, mbase: &str, scope: &mut PanelScope<'a>, issues: &mut Issues) {
    let panel_kind = scope.defaults.time_kind;
    let kind_mismatch = keys.time_overridden
        && matches!((panel_kind, keys.time_kind), (Some(p), Some(m)) if p.composite() && m.composite() && p != m);
    if kind_mismatch {
        issues.error(
            "time_key_member_kind_mismatch",
            format!("{mbase}/time_key"),
            format!(
                "member composite time_key kind {} must match panel-level kind {}",
                q(keys.time_kind.map_or("", TimeKind::as_str)),
                q(panel_kind.map_or("", TimeKind::as_str))
            ),
        );
    }

    if keys.entity_ok && keys.entity.is_some_and(Value::is_array) {
        scope
            .composite_entities
            .push((keys.entity_path.clone(), Composite::Refs(refs(keys.entity))));
    }
    // A kind mismatch is already reported; comparing its refs with literals would
    // repeat it as an ordering error.
    if !kind_mismatch && keys.time_kind.is_some_and(TimeKind::composite) {
        let composite = match keys.time.and_then(literal_key) {
            Some(LiteralKey::Composite(items))
                if keys.time_kind == Some(TimeKind::LiteralComposite) =>
            {
                Composite::Literals(items)
            }
            _ => Composite::Refs(refs(keys.time)),
        };
        scope
            .composite_times
            .push((keys.time_path.clone(), composite));
    }

    if let (Some(TimeKind::LiteralScalar | TimeKind::LiteralComposite), Some(key)) =
        (keys.time_kind, keys.time.and_then(literal_key))
    {
        match scope.literal_times.iter().find(|(k, _)| *k == key) {
            Some((_, first)) => issues.error(
                "literal_time_key_duplicate",
                keys.time_path.clone(),
                format!("literal time_key duplicates the one at {first}"),
            ),
            None => scope.literal_times.push((key, keys.time_path.clone())),
        }
    }
}

/// The member's source: defined in `/sources`, in at most one panel, and holding
/// the key columns.
fn check_member_source<'a>(
    source: &'a str,
    member: &Value,
    mbase: &str,
    keys: &Keys<'_>,
    pbase: &str,
    state: &mut Panels<'a>,
    issues: &mut Issues,
) {
    // Resolved before the cross-panel rule, so an undefined source reused across
    // panels is only `panel_member_unknown_source`. With `/sources` malformed, names
    // cannot resolve and that error is already reported.
    let Some(sources) = &state.sources else {
        return;
    };
    let Some((_, columns)) = sources.iter().find(|(n, _)| *n == source) else {
        issues.error(
            "panel_member_unknown_source",
            if member.is_object() {
                format!("{mbase}/source")
            } else {
                mbase.to_owned()
            },
            format!(
                "panel member references source {} which is not defined in /sources",
                q(source)
            ),
        );
        return;
    };

    // At most one panel per source; two members of one panel sharing a source is a
    // different condition.
    match state.source_panel.iter().find(|(n, _)| *n == source) {
        None => state.source_panel.push((source, pbase.to_owned())),
        Some((_, prior)) if prior != pbase => issues.error(
            "source_referenced_by_multiple_panels",
            mbase.to_owned(),
            format!(
                "source {} already referenced by panel at {prior}",
                q(source)
            ),
        ),
        Some(_) => {}
    }

    // Skipped when a column's display name is a catalog default.
    if !columns.complete {
        return;
    }
    let time_refs = match keys.time_kind {
        Some(TimeKind::RefScalar | TimeKind::RefComposite) => refs(keys.time),
        _ => Vec::new(),
    };
    for (label, code, path, key_refs) in [
        (
            "entity_key",
            "entity_key_unknown_column",
            &keys.entity_path,
            refs(keys.entity),
        ),
        (
            "time_key",
            "time_key_unknown_column",
            &keys.time_path,
            time_refs,
        ),
    ] {
        for r in key_refs
            .into_iter()
            .filter(|r| !columns.display_names.contains(r))
        {
            issues.error(
                code,
                path.clone(),
                format!(
                    "{label} {} does not match any display_name on source {}",
                    q(r),
                    q(source)
                ),
            );
        }
    }
}
