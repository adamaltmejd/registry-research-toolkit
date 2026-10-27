//! The structural validator of `project_data.json`: every rule checkable from the
//! document alone (`crates/reg-core/DESIGN.md`).
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
//! - An int is a JSON integer that fits `i64`.
//!
//! `"_default"` is a period like any other here: whether it fits the table is a catalog
//! rule (`period_default_on_dated_table`).

use serde_json::{Map, Value};

use crate::grammar::{Date, PeriodToken};
use crate::project::{ColumnType, IdSubtype, NumericSubtype};
use crate::validation::{IssueLevel, ValidationIssue, ValidationResult};

const PROJECT_REQUIRED: [&str; 4] = ["schema_version", "reg_meta_version", "name", "sources"];
const PROJECT_OPTIONAL: [&str; 2] = ["panels", "window"];
const SOURCE_KEYS: [&str; 4] = ["name", "table", "period", "bindings"];
const BINDING_KEYS: [&str; 8] = [
    "variable",
    "type",
    "display_name",
    "id_subtype",
    "numeric_subtype",
    "date_format",
    "datetime_format",
    "classification",
];
const PANEL_KEYS: [&str; 3] = ["panel_id", "members", "comment"];
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

/// `<provider>/<register>/<name>`, the provider not `class`.
fn three_part(s: &str) -> Option<Vec<&str>> {
    let segs: Vec<&str> = s.split('/').collect();
    (segs.len() == 3 && segs[0] != "class" && segs.iter().all(|s| fqid_segment(s))).then_some(segs)
}

/// A table's public id: `<provider>/<register>` or `<provider>/<register>/<table>`,
/// the provider not `class`.
fn table_parts(s: &str) -> Option<Vec<&str>> {
    let segs: Vec<&str> = s.split('/').collect();
    ((2..=3).contains(&segs.len()) && segs[0] != "class" && segs.iter().all(|s| fqid_segment(s)))
        .then_some(segs)
}

fn classification_fqid(s: &str) -> bool {
    let segs: Vec<&str> = s.split('/').collect();
    segs.len() == 3 && segs[0] == "class" && segs.iter().all(|s| fqid_segment(s))
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

/// The structural rules of one `Source.period` on its own, with paths relative to
/// the period (`""`, `/0`): what the project check applies at
/// `/sources/<i>/period`. [`crate::project::SourcePeriod::from_value`] runs it.
#[must_use]
pub(crate) fn validate_source_period(period: &Value) -> ValidationResult {
    let mut issues = Issues::default();
    check_period(period, String::new(), &mut issues);
    ValidationResult { issues: issues.0 }
}

fn check_period(period: &Value, path: String, issues: &mut Issues) {
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

    // The table's provider/register prefix scopes every binding FQID.
    let table = required_string(source, "table", base, "source 'table'", issues);
    let parts = table.and_then(table_parts);
    if let (Some(table), None) = (table, &parts) {
        issues.error(
            "invalid_fqid",
            format!("{base}/table"),
            format!(
                "table must be a 2- or 3-part public id <provider>/<register>[/<table>]; got {}",
                q(table)
            ),
        );
    }

    if let Some(period) = required(source, "period", base, "source 'period'", issues) {
        check_period(period, format!("{base}/period"), issues);
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
    let prefix = parts.as_ref().map(|p| &p[..2]);
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
                        "fqid_table_mismatch",
                        format!("{base}/variable"),
                        format!(
                            "binding FQID prefix {} must equal the source table prefix {}",
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
    check_subtypes(binding, base, column_type, issues);

    match get(binding, "classification") {
        None => {}
        Some(Value::String(pin)) if classification_fqid(pin) => {}
        Some(Value::String(pin)) => issues.error(
            "invalid_fqid",
            format!("{base}/classification"),
            format!(
                "classification must be a 3-segment classification id \
                 class/<family>/<version>; got {}",
                q(pin)
            ),
        ),
        Some(_) => issues.error(
            "invalid_field_type",
            format!("{base}/classification"),
            "classification must be a string".into(),
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

// --- Panels -------------------------------------------------------------

/// Panel-spanning state.
struct Panels<'a> {
    /// The source names, `None` when `/sources` is not an array, so names cannot resolve.
    sources: Option<Vec<&'a str>>,
    /// Source name to the first panel that references it.
    source_panel: Vec<(&'a str, String)>,
    panel_ids: Vec<(&'a str, usize)>,
}

/// The source names (a duplicated name once), when `/sources` is an array.
fn source_names(sources: Option<&Value>) -> Option<Vec<&str>> {
    let mut names: Vec<&str> = Vec::new();
    for name in sources?
        .as_array()?
        .iter()
        .filter_map(|source| source.get("name").and_then(Value::as_str))
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    Some(names)
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
        sources: source_names(sources),
        source_panel: Vec::new(),
        panel_ids: Vec::new(),
    };
    for (pi, panel) in panels.iter().enumerate() {
        let base = format!("/panels/{pi}");
        match panel.as_object() {
            Some(panel) => check_panel(panel, &base, pi, &mut state, issues),
            None => issues.error("invalid_field_type", base, "panel must be an object".into()),
        }
    }
}

fn check_panel<'a>(
    panel: &'a Map<String, Value>,
    base: &str,
    index: usize,
    state: &mut Panels<'a>,
    issues: &mut Issues,
) {
    check_unexpected_keys(panel, &PANEL_KEYS, base, "panel", issues);
    if let Some(id) = required_string(panel, "panel_id", base, "panel 'panel_id'", issues) {
        match state.panel_ids.iter().find(|(p, _)| *p == id) {
            Some((_, first)) => issues.error(
                "duplicate_panel_id",
                format!("{base}/panel_id"),
                format!("panel_id {} duplicates /panels/{first}", q(id)),
            ),
            None => state.panel_ids.push((id, index)),
        }
    }
    optional_string(panel, "comment", base, "panel 'comment'", issues);

    let Some(members) = required(panel, "members", base, "panel 'members'", issues) else {
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
    for (mi, member) in members.iter().enumerate() {
        let mbase = format!("{base}/members/{mi}");
        match member.as_str() {
            Some(source) => check_member_source(source, &mbase, base, state, issues),
            None => issues.error(
                "invalid_field_type",
                mbase,
                "panel member must be a string (source name)".into(),
            ),
        }
    }
}

/// The member's source: defined in `/sources`, and in at most one panel.
fn check_member_source<'a>(
    source: &'a str,
    mbase: &str,
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
    if !sources.contains(&source) {
        issues.error(
            "panel_member_unknown_source",
            mbase.to_owned(),
            format!(
                "panel member references source {} which is not defined in /sources",
                q(source)
            ),
        );
        return;
    }

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
}
