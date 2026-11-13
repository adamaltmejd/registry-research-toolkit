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

use crate::grammar::{Date, PeriodToken, iso};
use crate::project::{ColumnType, ExtractFormat};
use crate::validation::{IssueLevel, ValidationIssue, ValidationResult};

const PROJECT_REQUIRED: [&str; 4] = ["schema_version", "reg_meta_version", "name", "sources"];
const PROJECT_OPTIONAL: [&str; 4] = ["panels", "window", "extract_format", "linkage"];
const SOURCE_KEYS: [&str; 5] = ["name", "table", "manual", "period", "bindings"];
const MANUAL_ORIGIN_KEYS: [&str; 2] = ["provider", "register"];
const MANUAL_BINDING_KEYS: [&str; 2] = ["manual", "display_name"];
const MANUAL_COLUMN_KEYS: [&str; 3] = ["column", "type", "codes"];
const MANUAL_CODE_KEYS: [&str; 3] = ["code", "label", "sentinel"];
const BINDING_KEYS: [&str; 5] = [
    "variable",
    "follow_successor",
    "type",
    "display_name",
    "classification",
];
const PANEL_KEYS: [&str; 3] = ["panel_id", "members", "comment"];
const WINDOW_KEYS: [&str; 2] = ["from", "to"];
const LINKAGE_KEYS: [&str; 1] = ["database"];

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
            check_linkage(get(root, "linkage"), &mut issues);
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

/// `<provider>/<register>/<name>`, the provider neither `class` nor `set`.
fn three_part(s: &str) -> Option<Vec<&str>> {
    let segs: Vec<&str> = s.split('/').collect();
    (segs.len() == 3 && !matches!(segs[0], "class" | "set") && segs.iter().all(|s| fqid_segment(s)))
        .then_some(segs)
}

/// A table's public id: `<provider>/<register>` or `<provider>/<register>/<table>`,
/// the provider neither `class` nor `set`.
fn table_parts(s: &str) -> Option<Vec<&str>> {
    let segs: Vec<&str> = s.split('/').collect();
    ((2..=3).contains(&segs.len())
        && !matches!(segs[0], "class" | "set")
        && segs.iter().all(|s| fqid_segment(s)))
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
    optional_string(root, "extract_format", "", "extract_format", issues);
    if let Some(format) = get(root, "extract_format").and_then(Value::as_str)
        && ExtractFormat::parse(format).is_none()
    {
        issues.error(
            "invalid_enum_value",
            "/extract_format".into(),
            format!(
                "extract_format must be one of {}; got {}",
                allowed(ExtractFormat::ALL.iter().map(|v| v.as_str())),
                q(format)
            ),
        );
    }
}

/// The optional `linkage`: a closed `{"database": <string>}`. Whether the database is
/// one of the steward's is a holdings rule (`database_unknown`).
fn check_linkage(linkage: Option<&Value>, issues: &mut Issues) {
    let Some(linkage) = linkage else { return };
    let Some(map) = linkage.as_object() else {
        issues.error(
            "invalid_field_type",
            "/linkage".into(),
            "linkage must be an object {'database': <id>}".into(),
        );
        return;
    };
    check_unexpected_keys(map, &LINKAGE_KEYS, "/linkage", "linkage", issues);
    required_string(map, "database", "/linkage", "linkage 'database'", issues);
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

#[allow(clippy::too_many_lines)]
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

    // A catalog source names a `table`, a manual one carries `manual`: by key, so an
    // explicit `null` counts as present and can never reach a type neither variant reads.
    let kind = match (source.contains_key("table"), source.contains_key("manual")) {
        (true, false) => Some(Kind::Catalog),
        (false, true) => Some(Kind::Manual),
        (both, _) => {
            issues.error(
                "source_kind",
                base.to_owned(),
                if both {
                    "source has both 'table' and 'manual': a source is a catalog table or \
                     entered by hand, not both"
                } else {
                    "source needs 'table' (a catalog table) or 'manual' (data the catalog \
                     does not document, entered by hand)"
                }
                .into(),
            );
            None
        }
    };

    // The table's provider/register prefix scopes every binding FQID.
    let mut parts = None;
    match kind {
        Some(Kind::Catalog) => {
            let table = required_string(source, "table", base, "source 'table'", issues);
            parts = table.and_then(table_parts);
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
        }
        Some(Kind::Manual) => check_manual_origin(source, base, issues),
        None => {}
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
    let mut columns: Vec<(&str, String)> = Vec::new();
    let mut catalog_bindings = 0;
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
        match (
            binding.contains_key("variable"),
            binding.contains_key("manual"),
        ) {
            (true, true) => issues.error(
                "binding_kind",
                binding_base.clone(),
                "binding has both 'variable' and 'manual': a column is a catalog variable \
                 or entered by hand, not both"
                    .into(),
            ),
            (false, false) => issues.error(
                "binding_kind",
                binding_base.clone(),
                "binding needs 'variable' (a catalog variable) or 'manual' (a column the \
                 catalog does not document, entered by hand)"
                    .into(),
            ),
            (true, false) if kind == Some(Kind::Manual) => issues.error(
                "binding_kind",
                format!("{binding_base}/variable"),
                "a source entered by hand holds only columns entered by hand: a catalog \
                 variable needs a source with its 'table'"
                    .into(),
            ),
            (true, false) => {
                catalog_bindings += 1;
                check_binding(binding, &binding_base, prefix, issues);
            }
            (false, true) => {
                if let Some(column) = check_manual_binding(binding, &binding_base, issues) {
                    let path = format!("{binding_base}/manual/column");
                    match columns
                        .iter()
                        .find(|(c, _)| crate::fold_identity(c) == crate::fold_identity(column))
                    {
                        Some((_, prior)) => issues.error(
                            "duplicate_manual_column",
                            path,
                            format!("column {} duplicates the one at {prior}", q(column)),
                        ),
                        None => columns.push((column, path)),
                    }
                }
            }
        }
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
    // Every binding an object of one kind, all of them by hand: the source has no catalog
    // column, so nothing would check its period or give it row keys and a file.
    let all_manual = bindings.iter().all(|b| {
        b.as_object()
            .is_some_and(|b| b.contains_key("manual") && !b.contains_key("variable"))
    });
    if kind == Some(Kind::Catalog) && catalog_bindings == 0 && all_manual {
        issues.error(
            "catalog_source_unbound",
            format!("{base}/bindings"),
            "a catalog source needs at least one catalog variable; a source whose columns \
             are all entered by hand is a manual source (replace 'table' with 'manual')"
                .into(),
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Catalog,
    Manual,
}

/// A required string field that must also be non-blank.
fn non_blank<'a>(
    map: &'a Map<String, Value>,
    field: &str,
    base: &str,
    label: &str,
    issues: &mut Issues,
) -> Option<&'a str> {
    let value = required_string(map, field, base, label, issues)?;
    if value.trim().is_empty() {
        issues.error(
            "invalid_field_type",
            format!("{base}/{field}"),
            format!("{label} must be a non-blank string"),
        );
        return None;
    }
    Some(value)
}

/// A manual source's closed `{"provider", "register"}`: free-text names, not ids.
fn check_manual_origin(source: &Map<String, Value>, base: &str, issues: &mut Issues) {
    let Some(manual) = required(source, "manual", base, "source 'manual'", issues) else {
        return;
    };
    let mbase = format!("{base}/manual");
    let Some(manual) = manual.as_object() else {
        issues.error(
            "invalid_field_type",
            mbase,
            "source 'manual' must be an object {'provider': <name>, 'register': <name>}".into(),
        );
        return;
    };
    check_unexpected_keys(
        manual,
        &MANUAL_ORIGIN_KEYS,
        &mbase,
        "source 'manual'",
        issues,
    );
    for field in MANUAL_ORIGIN_KEYS {
        non_blank(
            manual,
            field,
            &mbase,
            &format!("source manual {}", q(field)),
            issues,
        );
    }
}

/// A manual binding: a closed `{manual, display_name}` whose `manual` is a closed
/// `{column, type, codes}`. The column name, when it is a non-blank string.
fn check_manual_binding<'a>(
    binding: &'a Map<String, Value>,
    base: &str,
    issues: &mut Issues,
) -> Option<&'a str> {
    // `follow_successor`, `classification` and the binding `type` name catalog facts,
    // so they are unknown keys here, `null` included.
    check_unexpected_keys(
        binding,
        &MANUAL_BINDING_KEYS,
        base,
        "manual binding",
        issues,
    );
    optional_string(
        binding,
        "display_name",
        base,
        "binding 'display_name'",
        issues,
    );
    let manual = required(binding, "manual", base, "binding 'manual'", issues)?;
    let mbase = format!("{base}/manual");
    let Some(manual) = manual.as_object() else {
        issues.error(
            "invalid_field_type",
            mbase,
            "binding 'manual' must be an object {'column': <name>, 'type': <type>, \
             'codes': [...]}"
                .into(),
        );
        return None;
    };
    check_unexpected_keys(
        manual,
        &MANUAL_COLUMN_KEYS,
        &mbase,
        "binding 'manual'",
        issues,
    );
    let column = non_blank(manual, "column", &mbase, "manual 'column'", issues);
    let typ = required_string(manual, "type", &mbase, "manual 'type'", issues);
    if let Some(typ) = typ
        && ColumnType::parse(typ).is_none()
    {
        issues.error(
            "invalid_enum_value",
            format!("{mbase}/type"),
            format!(
                "manual type must be one of {}; got {}",
                allowed(ColumnType::ALL.iter().map(|v| v.as_str())),
                q(typ)
            ),
        );
    }
    if let Some(codes) = manual.get("codes") {
        let identifier = typ == Some(ColumnType::Identifier.as_str());
        check_manual_codes(codes, &format!("{mbase}/codes"), identifier, issues);
    }
    column
}

/// A manual column's `codes`: a non-empty array of closed `{code, label, sentinel}`
/// objects, each code once, and none on an identifier column.
fn check_manual_codes(codes: &Value, cbase: &str, identifier: bool, issues: &mut Issues) {
    let Some(codes) = codes.as_array().filter(|c| !c.is_empty()) else {
        issues.error(
            "invalid_field_type",
            cbase.into(),
            "manual 'codes' must be a non-empty array of {'code', 'label', 'sentinel'} objects"
                .into(),
        );
        return;
    };
    // Codes listed for an identifier would be real ids, and the project leaves MONA.
    if identifier {
        issues.error(
            "identifier_codes",
            cbase.into(),
            "an identifier column lists no codes: they would be real ids, and the project \
             is sent outside MONA"
                .into(),
        );
    }
    let mut seen: Vec<(&str, usize)> = Vec::new();
    for (k, code) in codes.iter().enumerate() {
        let kbase = format!("{cbase}/{k}");
        let Some(code) = code.as_object() else {
            issues.error(
                "invalid_field_type",
                kbase,
                "a manual code must be an object {'code', 'label', 'sentinel'}".into(),
            );
            continue;
        };
        check_unexpected_keys(code, &MANUAL_CODE_KEYS, &kbase, "manual code", issues);
        optional_string(code, "label", &kbase, "code 'label'", issues);
        if get(code, "sentinel").is_some_and(|v| !v.is_boolean()) {
            issues.error(
                "invalid_field_type",
                format!("{kbase}/sentinel"),
                "code 'sentinel' must be a boolean".into(),
            );
        }
        let Some(value) = non_blank(code, "code", &kbase, "code 'code'", issues) else {
            continue;
        };
        match seen.iter().find(|(c, _)| *c == value) {
            Some((_, first)) => issues.error(
                "duplicate_code",
                format!("{kbase}/code"),
                format!("code {} duplicates {cbase}/{first}", q(value)),
            ),
            None => seen.push((value, k)),
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
    if binding
        .get("follow_successor")
        .is_some_and(|value| !value.is_boolean())
    {
        issues.error(
            "invalid_field_type",
            format!("{base}/follow_successor"),
            "follow_successor must be a boolean".into(),
        );
    }
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

    // The optional catalog type override: absent is the catalog type, `null` is refused
    // like the root's optional fields.
    match binding.get("type") {
        Some(Value::Null) => issues.error(
            "invalid_field_type",
            format!("{base}/type"),
            "binding 'type' must not be null when present".into(),
        ),
        Some(Value::String(typ)) if ColumnType::parse(typ).is_none() => issues.error(
            "invalid_enum_value",
            format!("{base}/type"),
            format!(
                "binding type must be one of {}; got {}",
                allowed(ColumnType::ALL.iter().map(|v| v.as_str())),
                q(typ)
            ),
        ),
        None | Some(Value::String(_)) => {}
        Some(_) => issues.error(
            "invalid_field_type",
            format!("{base}/type"),
            "binding 'type' must be a string".into(),
        ),
    }

    optional_string(
        binding,
        "display_name",
        base,
        "binding 'display_name'",
        issues,
    );

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
