//! `compile`: project + saved catalog answers + rules → [`Spec`]. Pure; the directory
//! read is [`load_answers`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_core::issue::Period;
use reg_core::manifest::{self, Manifest, Storage, StorageFamily};
use reg_core::project::{ColumnType, ExtractFormat, ProjectData};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::Refusal;
use crate::input::{Envelope, LogicalOrder, Segment, SegmentPage, ValuesPage};
use crate::rules::{ColumnRules, DEFAULT_NULL_RATE, RULES_VERSION, Rules};
use crate::spec::{
    Attribute, Code, Layout, Population, SPEC_VERSION, Spec, SpecColumn, SpecFile, SpecProject,
    Unit,
};

/// The order the saved answers describe: the logical order (any catalog), or the
/// steward's order file, which fixes file and column names.
pub enum Order {
    Logical(LogicalOrder),
    Delivery(Box<Manifest>),
}

/// The saved catalog answers, all from one catalog generation.
pub struct Answers {
    pub generation: String,
    pub order: Order,
    pub segments: Vec<Segment>,
    /// Value set hash → its codes, in the catalog's order.
    pub values: BTreeMap<String, Vec<Code>>,
}

/// Read a directory of saved answers: `order.json` (`POST /api/project/order`) or
/// `manifest.json` (`POST /api/project/materialize`), and every page under
/// `segments/` and `values/`.
///
/// # Errors
///
/// A located refusal for an unreadable file, a document that is not the answer it
/// should be, both or neither order file, or answers from different generations.
pub fn load_answers(dir: &Path) -> Result<Answers, Refusal> {
    let mut generations = BTreeSet::new();
    let (logical, delivery) = (dir.join("order.json"), dir.join("manifest.json"));
    let order = match (logical.exists(), delivery.exists()) {
        (true, false) => Order::Logical(read_answer(&logical, &mut generations)?),
        (false, true) => Order::Delivery(Box::new(read_answer(&delivery, &mut generations)?)),
        _ => {
            return Err(Refusal::new(
                "order_answer_missing",
                dir.display().to_string(),
                "save exactly one of order.json (the order answer) and manifest.json (the \
                 materialize answer)",
            ));
        }
    };
    let order_periods: Vec<&Period> = match &order {
        Order::Logical(o) => o
            .entries
            .iter()
            .map(|e| &e.period)
            .chain(o.row_keys.iter().map(|k| &k.period))
            .collect(),
        Order::Delivery(m) => m
            .files
            .iter()
            .flat_map(|f| &f.columns)
            .flat_map(|c| &c.refs)
            .map(|r| &r.period)
            .collect(),
    };
    let order_file = if logical.exists() {
        &logical
    } else {
        &delivery
    };
    check_periods(order_periods, order_file)?;
    let mut segments = Vec::new();
    for path in json_files(&dir.join("segments"))? {
        let page: SegmentPage = read_answer(&path, &mut generations)?;
        check_periods(page.items.iter().map(|s| &s.period), &path)?;
        segments.extend(page.items);
    }
    let mut values: BTreeMap<String, Vec<Code>> = BTreeMap::new();
    let mut sizes: BTreeMap<String, Option<usize>> = BTreeMap::new();
    // Pages in file-name order: name them so a set's pages sort in page order.
    for path in json_files(&dir.join("values"))? {
        let page: ValuesPage = read_answer(&path, &mut generations)?;
        sizes.insert(page.value_set.hash.clone(), page.value_set.n_items);
        values
            .entry(page.value_set.hash)
            .or_default()
            .extend(page.items.into_iter().map(|item| Code {
                code: item.code,
                label: item.label,
                sentinel: item.sentinel,
            }));
    }
    // A page missing or saved twice (or filtered with `q`, `level`, `parent`) would
    // give the mock a wrong code list.
    for (hash, codes) in &values {
        let distinct: BTreeSet<&str> = codes.iter().map(|c| c.code.as_str()).collect();
        if distinct.len() != codes.len() || sizes[hash].is_some_and(|n| n != codes.len()) {
            return Err(Refusal::new(
                "values_incomplete",
                format!("values/{hash}"),
                "the saved pages of this value set do not hold each code once: save every \
                 page of the unfiltered values answer, once each",
            ));
        }
    }
    let mut generations = generations.into_iter();
    let generation = generations.next().unwrap_or_default();
    if generations.next().is_some() {
        return Err(Refusal::new(
            "mixed_generations",
            dir.display().to_string(),
            "the saved answers come from different catalog generations: save them all again",
        ));
    }
    Ok(Answers {
        generation,
        order,
        segments,
        values,
    })
}

/// Every bound an ISO day, so later reads of the periods cannot fail.
fn check_periods<'a>(
    periods: impl IntoIterator<Item = &'a Period>,
    path: &Path,
) -> Result<(), Refusal> {
    let iso = |d: &Option<String>| {
        d.as_deref()
            .is_none_or(|d| reg_core::iso_day_number(d).is_some())
    };
    if periods.into_iter().all(|p| iso(&p.start) && iso(&p.end)) {
        return Ok(());
    }
    Err(Refusal::new(
        "answer_invalid",
        path.display().to_string(),
        "a period bound is not an ISO day; save the response document unchanged",
    ))
}

fn json_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, Refusal> {
    let refuse = |e: std::io::Error| Refusal::new("io", dir.display().to_string(), e.to_string());
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(refuse)? {
        let path = entry.map_err(refuse)?.path();
        if path.extension().is_some_and(|e| e == "json") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn read_answer<T: DeserializeOwned>(
    path: &Path,
    generations: &mut BTreeSet<String>,
) -> Result<T, Refusal> {
    let at = path.display().to_string();
    let bytes = std::fs::read(path).map_err(|e| Refusal::new("io", at.clone(), e.to_string()))?;
    let envelope: Envelope<T> = serde_json::from_slice(&bytes).map_err(|e| {
        Refusal::new(
            "answer_invalid",
            at,
            format!("not a saved catalog answer of this kind ({e}); save the response document unchanged"),
        )
    })?;
    generations.insert(envelope.meta.generation);
    Ok(envelope.data)
}

/// The type a mock column is drawn and written in (#2071, G8/G10): the project's
/// binding override, else the held storage's type, else the catalog's. The `bool` is
/// whether the held storage still decides the written type (no override).
///
/// # Errors
///
/// `type_unknown` when no source states a type; `type_unsupported` for a type mocks
/// do not generate (geometry, uuid, binary, or a storage the runner cannot write).
pub fn resolve_type(
    binding_override: Option<ColumnType>,
    storage: Option<&Storage>,
    catalog: Option<&str>,
) -> Result<(ColumnType, bool), &'static str> {
    let (dtype, held) = if let Some(dtype) = binding_override {
        (dtype, false)
    } else if let Some(sql) = storage.and_then(|s| s.dtype.as_deref()) {
        (storage_type(sql).ok_or("type_unsupported")?, true)
    } else {
        let catalog = catalog.ok_or("type_unknown")?;
        (ColumnType::parse(catalog).ok_or("type_unknown")?, false)
    };
    match dtype {
        ColumnType::Geometry | ColumnType::Uuid | ColumnType::Binary => Err("type_unsupported"),
        _ => Ok((dtype, held)),
    }
}

/// The catalog type a SQL storage type holds: its [`StorageFamily`], split where the
/// family covers two catalog types.
fn storage_type(sql: &str) -> Option<ColumnType> {
    match sql.to_ascii_lowercase().as_str() {
        "bit" => Some(ColumnType::Boolean),
        "date" => Some(ColumnType::Date),
        "datetime" | "datetime2" | "smalldatetime" => Some(ColumnType::Datetime),
        // `time` and `datetimeoffset` are Date-family storage no format writes alike.
        "time" | "datetimeoffset" => None,
        _ => match StorageFamily::of(sql)? {
            StorageFamily::Integer => Some(ColumnType::Integer),
            StorageFamily::Real => Some(ColumnType::Decimal),
            StorageFamily::Text => Some(ColumnType::Text),
            StorageFamily::Date => None,
        },
    }
}

/// A file name when no delivery layout is known: the table's public id with `_` for
/// `/` and `-`, then the year (`scb/lsum/individer-sociala-agi-ku`, 2019 →
/// `scb_lsum_individer_sociala_agi_ku_2019`). Public ids are already lowercase ASCII
/// slugs, so no further normalization is needed.
#[must_use]
pub fn standard_file_name(table: &str, year: Option<i32>) -> String {
    let stem = table.replace(['/', '-'], "_");
    match year {
        Some(year) => format!("{stem}_{year}"),
        None => stem,
    }
}

/// The years a period touches, or `None` for a timeless (all-open) period.
///
/// # Errors
///
/// A period open at one end only, which a served order never has.
fn years(period: &Period) -> Result<Option<Vec<i32>>, ()> {
    let (start, end) = match (&period.start, &period.end) {
        (None, None) => return Ok(None),
        (Some(start), Some(end)) => (start, end),
        _ => return Err(()),
    };
    let year = |iso: &str| iso.get(..4).and_then(|y| y.parse::<i32>().ok()).ok_or(());
    let first = year(start)?;
    // `end` is exclusive: a period ending on 1 January ends in the year before.
    let last = year(end)? - i32::from(end.ends_with("-01-01"));
    Ok(Some((first..=last).collect()))
}

/// Compile the spec.
///
/// # Errors
///
/// Every located refusal at once: an invalid project or rules file, a source or column
/// the rules name that the order lacks (or the reverse for sources), a column whose type
/// is unknown or unsupported, a missing segment or value set page, a file name the
/// runner would refuse or that two files share.
pub fn compile(project: &Value, rules: &Rules, answers: &Answers) -> Result<Spec, Vec<Refusal>> {
    let mut refusals = Vec::new();
    let project = match reg_core::project::check(project) {
        Ok(project) => project,
        Err(result) => {
            return Err(result
                .issues
                .into_iter()
                .map(|i| Refusal::new("project_invalid", i.path, i.message))
                .collect());
        }
    };
    if rules.version != RULES_VERSION {
        return Err(vec![Refusal::new(
            "rules_version",
            "mock_rules.json",
            format!("rules version must be {RULES_VERSION}"),
        )]);
    }
    check_rule_sources(&project, rules, &mut refusals);
    let hash = reg_core::project::project_hash(&project);
    let answered = match &answers.order {
        Order::Logical(order) => &order.project.project_hash,
        Order::Delivery(manifest) => &manifest.provenance.project.project_hash,
    };
    if *answered != hash {
        return Err(vec![Refusal::new(
            "answers_stale",
            "order",
            "the saved order answer is for another version of the project: save the \
             answers again for this project_data.json",
        )]);
    }
    let format = project.extract_format.unwrap_or(ExtractFormat::Dta);
    let ctx = Context {
        project: &project,
        answers,
        format,
    };
    let (layout, built) = match &answers.order {
        Order::Logical(order) => (Layout::Standard, ctx.standard_files(order, &mut refusals)),
        Order::Delivery(manifest) => (
            Layout::Delivery,
            ctx.delivery_files(manifest, &mut refusals),
        ),
    };
    let mut files = apply_rules(built, rules, &mut refusals);
    // Each rename applies once, to a compiled name; check_files then refuses a final
    // name two files share.
    for from in rules.rename.keys() {
        if !files.iter().any(|f| &f.name == from) {
            refusals.push(Refusal::new(
                "rename_unknown_file",
                format!("/rename/{from}"),
                "no compiled file has this name",
            ));
        }
    }
    for file in &mut files {
        if let Some(to) = rules.rename.get(&file.name) {
            file.name.clone_from(to);
        }
    }
    for table in &rules.tables {
        for column in &table.columns {
            if let Err(code) = resolve_type(Some(column.dtype), None, None) {
                refusals.push(Refusal::new(
                    code,
                    format!("/tables/{}/{}", table.name, column.name),
                    "mocks generate integer, decimal, text, date, datetime, boolean and identifier columns",
                ));
            }
        }
        files.push(table.clone());
    }
    let null_rate = rules.null_rate.unwrap_or(DEFAULT_NULL_RATE);
    check_files(&files, null_rate, &mut refusals);
    if !refusals.is_empty() {
        return Err(refusals);
    }
    Ok(Spec {
        version: SPEC_VERSION,
        format,
        layout,
        project: SpecProject {
            name: project.name.clone(),
            hash,
        },
        catalog_generation: answers.generation.clone(),
        null_rate,
        files,
    })
}

/// A rules entry for each project source, and none for another.
fn check_rule_sources(project: &ProjectData, rules: &Rules, refusals: &mut Vec<Refusal>) {
    for source in &project.sources {
        if !rules.sources.contains_key(&source.name) {
            refusals.push(Refusal::new(
                "rules_source_missing",
                format!("/sources/{}", source.name),
                "state the source's unit, population and rows_per_unit in the rules",
            ));
        }
    }
    for name in rules.sources.keys() {
        if !project.sources.iter().any(|s| &s.name == name) {
            refusals.push(Refusal::new(
                "rules_source_unknown",
                format!("/sources/{name}"),
                "the project has no source of this name",
            ));
        }
    }
}

struct Context<'a> {
    project: &'a ProjectData,
    answers: &'a Answers,
    format: ExtractFormat,
}

/// One column of a variable in a table over a period, before the rules.
struct Column<'a> {
    name: &'a str,
    source: &'a str,
    binding: Option<usize>,
    table: &'a str,
    variable: &'a str,
    period: &'a Period,
    /// The held file and column, in the delivery layout.
    placement: Option<(&'a str, &'a str)>,
}

impl Context<'_> {
    /// One file per table and year (or one for a timeless table): the source's
    /// requested columns, then the table's row keys.
    fn standard_files(&self, order: &LogicalOrder, refusals: &mut Vec<Refusal>) -> Vec<Built> {
        let mut files: BTreeMap<(&str, Option<i32>), Built> = BTreeMap::new();
        let ordered = order.entries.iter().map(|e| Column {
            name: &e.spelling,
            source: &e.source,
            binding: Some(e.binding),
            table: &e.table,
            variable: &e.variable,
            period: &e.period,
            placement: None,
        });
        let keys = order.row_keys.iter().flat_map(|k| {
            k.columns.iter().map(move |c| Column {
                name: &c.spelling,
                source: &k.source,
                binding: None,
                table: &k.table,
                variable: &c.variable,
                period: &k.period,
                placement: None,
            })
        });
        for column in ordered.chain(keys) {
            let Ok(years) = years(column.period) else {
                refusals.push(open_period(column.source, column.variable));
                continue;
            };
            let years: Vec<Option<i32>> = match years {
                Some(years) => years.into_iter().map(Some).collect(),
                None => vec![None],
            };
            for year in years {
                // Keyed by table, not by file name: two tables whose names collide stay
                // two files, and check_files refuses the shared name.
                let built = files.entry((column.table, year)).or_insert_with(|| {
                    Built::new(
                        standard_file_name(column.table, year),
                        year.into_iter().collect(),
                    )
                });
                // A row key the source also requested is that column, of its type: the
                // binding's override, which the row key does not carry.
                if column.binding.is_none()
                    && let Some(i) = built
                        .file
                        .columns
                        .iter()
                        .position(|c| c.name == column.name)
                {
                    built.add_source(i, column.source);
                    continue;
                }
                // Each year's own segment, within the ordered period: a value set or type
                // can change between and within years.
                let period = year.map(|y| within(year_period(y), column.period));
                let draft = Column {
                    period: period.as_ref().unwrap_or(column.period),
                    ..column
                };
                let Some(spec_column) = self.column(&draft, None, refusals) else {
                    continue;
                };
                // Two sources' columns of one name are one column, of one type.
                match built
                    .file
                    .columns
                    .iter()
                    .position(|c| c.name == column.name)
                {
                    Some(i) => {
                        same_type(
                            &built.file.columns[i],
                            &spec_column,
                            &built.file.name,
                            refusals,
                        );
                        built.add_source(i, column.source);
                    }
                    None => built.push(column.source, spec_column),
                }
            }
        }
        files.into_values().collect()
    }

    /// The order file's files and columns, as named. A file's years are its columns'
    /// requested years (simplify: the runner extracts a file without a row filter over
    /// its whole delivery; the mock holds only the requested years, upgrade if a
    /// pipeline reads years it did not order).
    fn delivery_files(&self, manifest: &Manifest, refusals: &mut Vec<Refusal>) -> Vec<Built> {
        let mut files = Vec::new();
        for file in &manifest.files {
            let mut built = Built::new(file.name.clone(), Vec::new());
            let mut years = BTreeSet::new();
            for column in &file.columns {
                let row_filter = file.row_filter.as_ref().filter(|f| f.column == column.name);
                // The runner's period column: a row key placing no variable.
                // It has no row filter when the order covers the whole delivery.
                let Some(r) = column.refs.first() else {
                    if column.role == manifest::Role::RowKey {
                        built.push_unsourced(year_column(column, row_filter.map(|f| f.encoding)));
                    } else {
                        refusals.push(Refusal::new(
                            "column_unplaced",
                            format!("{}/{}", file.name, column.name),
                            "the order file has a column that places no variable and is not \
                             a row key: save the materialize answer again",
                        ));
                    }
                    continue;
                };
                let mut column_years = BTreeSet::new();
                for r in &column.refs {
                    match self::years(&r.period) {
                        Ok(Some(ys)) => column_years.extend(ys),
                        Ok(None) => {}
                        Err(()) => refusals.push(open_period(&r.source, &r.variable)),
                    }
                }
                years.extend(column_years.iter().copied());
                // A column several requested variables share is drawn as the first.
                let draft = Column {
                    name: &column.name,
                    source: &r.source,
                    binding: r.binding,
                    table: &r.table,
                    variable: &r.variable,
                    period: &r.period,
                    placement: Some((&file.table, &column.name)),
                };
                if let Some(mut spec_column) = self.column(&draft, Some(&column.storage), refusals)
                {
                    spec_column.stata_name.clone_from(&column.stata_name);
                    spec_column.label = column.stata_name.as_ref().map(|_| column.name.clone());
                    if let Some(filter) = row_filter {
                        spec_column.attribute = Some(Attribute::Year);
                        spec_column.encoding = Some(filter.encoding);
                    }
                    spec_column.years = column_years.into_iter().collect();
                    // Every variable the column delivers must agree on its type.
                    for other in &column.refs[1..] {
                        let draft = Column {
                            source: &other.source,
                            binding: other.binding,
                            table: &other.table,
                            variable: &other.variable,
                            period: &other.period,
                            ..draft
                        };
                        if let Some(other_column) =
                            self.column(&draft, Some(&column.storage), refusals)
                        {
                            same_type(&spec_column, &other_column, &file.name, refusals);
                        }
                    }
                    built.push(&r.source, spec_column);
                    let i = built.file.columns.len() - 1;
                    for other in &column.refs[1..] {
                        built.add_source(i, &other.source);
                    }
                }
            }
            built.file.years = years.into_iter().collect();
            // A column held in every year of its file needs no years of its own.
            for column in &mut built.file.columns {
                if column.years == built.file.years {
                    column.years.clear();
                }
            }
            files.push(built);
        }
        files
    }

    /// The column's type, codes and flags from its segment, the project override and
    /// the held storage.
    fn column(
        &self,
        column: &Column<'_>,
        storage: Option<&Storage>,
        refusals: &mut Vec<Refusal>,
    ) -> Option<SpecColumn> {
        let at = format!("{}/{}/{}", column.source, column.table, column.name);
        let Some(segment) = self.segment(column) else {
            refusals.push(Refusal::new(
                "segment_missing",
                at,
                format!(
                    "save the segments answer for {} with variable={} into segments/, or for \
                     a variable of another register its own answer (segments/{})",
                    column.table, column.variable, column.variable
                ),
            ));
            return None;
        };
        let binding_override = column.binding.and_then(|b| {
            let source = self
                .project
                .sources
                .iter()
                .find(|s| s.name == column.source)?;
            source.bindings.get(b)?.column_type
        });
        let (dtype, held) = match resolve_type(binding_override, storage, segment.dtype.as_deref())
        {
            Ok(resolved) => resolved,
            Err(code) => {
                refusals.push(Refusal::new(
                    code,
                    at,
                    "state the column's type with the binding's `type` in project_data.json; \
                     mocks generate integer, decimal, text, date, datetime, boolean and \
                     identifier columns",
                ));
                return None;
            }
        };
        let codes = match &segment.value_set {
            Some(vs) => {
                let Some(codes) = self.answers.values.get(&vs.hash) else {
                    refusals.push(Refusal::new(
                        "values_missing",
                        at,
                        format!(
                            "save every page of the values answer for value_set={} into values/",
                            vs.hash
                        ),
                    ));
                    return None;
                };
                codes.clone()
            }
            None => Vec::new(),
        };
        let stata_name = (self.format == ExtractFormat::Dta && storage.is_none())
            .then(|| manifest::stata_name(column.name))
            .flatten();
        Some(SpecColumn {
            name: column.name.to_owned(),
            label: stata_name.as_ref().map(|_| column.name.to_owned()),
            stata_name,
            dtype,
            storage: held.then(|| storage.cloned()).flatten(),
            length: segment.length,
            codes,
            attribute: None,
            map: BTreeMap::new(),
            encoding: None,
            years: Vec::new(),
            identifier: segment.identifier.unwrap_or(false),
            null_rate: None,
        })
    }

    /// The variable's segment that overlaps the period, earliest first: the held
    /// placement's row (delivery layout), else the table's, else for a row key any
    /// table's, since a row key can be another register's variable (a person id
    /// carried into a steward table) whose segments are its own table's.
    fn segment(&self, column: &Column<'_>) -> Option<&Segment> {
        let open = (i64::MIN, i64::MAX);
        let (start, end) = column.period.days(open);
        let overlapping = || {
            self.answers
                .segments
                .iter()
                .filter(|s| s.variable == column.variable)
                .filter(move |s| {
                    let (s_start, s_end) = s.period.days(open);
                    s_start < end && start < s_end
                })
        };
        let earliest = |a: &&Segment, b: &&Segment| a.period.days(open).cmp(&b.period.days(open));
        let in_table = || overlapping().filter(|s| s.table == column.table);
        column
            .placement
            .and_then(|(file, name)| {
                in_table()
                    .filter(|s| {
                        s.file.as_deref() == Some(file) && s.column.as_deref() == Some(name)
                    })
                    .min_by(earliest)
            })
            .or_else(|| in_table().min_by(earliest))
            .or_else(|| {
                column
                    .binding
                    .is_none()
                    .then(|| overlapping().min_by(earliest))
                    .flatten()
            })
    }
}

/// The one-year period `[year-01-01, year+1-01-01)`.
fn year_period(year: i32) -> Period {
    Period {
        start: Some(format!("{year:04}-01-01")),
        end: Some(format!("{:04}-01-01", year + 1)),
    }
}

/// `period` cut to `bounds`; ISO days compare as text.
fn within(period: Period, bounds: &Period) -> Period {
    let later = |a: Option<String>, b: &Option<String>| match (a, b) {
        (Some(a), Some(b)) => Some(if *b > a { b.clone() } else { a }),
        (a, b) => a.or_else(|| b.clone()),
    };
    let earlier = |a: Option<String>, b: &Option<String>| match (a, b) {
        (Some(a), Some(b)) => Some(if *b < a { b.clone() } else { a }),
        (a, b) => a.or_else(|| b.clone()),
    };
    Period {
        start: later(period.start, &bounds.start),
        end: earlier(period.end, &bounds.end),
    }
}

/// Columns merged into one must agree on the type they are written in.
fn same_type(kept: &SpecColumn, other: &SpecColumn, file: &str, refusals: &mut Vec<Refusal>) {
    if kept.dtype != other.dtype || kept.storage.is_some() != other.storage.is_some() {
        refusals.push(Refusal::new(
            "column_types_disagree",
            format!("{file}/{}", kept.name),
            "the variables delivered in this column resolve to different types: give their \
             bindings the same `type` in project_data.json",
        ));
    }
}

/// The runner's period column, typed by its storage; without a row filter its
/// encoding is unknown and the year is written whole.
fn year_column(column: &manifest::Column, encoding: Option<manifest::Encoding>) -> SpecColumn {
    SpecColumn {
        name: column.name.clone(),
        stata_name: column.stata_name.clone(),
        label: column.stata_name.as_ref().map(|_| column.name.clone()),
        dtype: storage_type(column.storage.dtype.as_deref().unwrap_or("int"))
            .unwrap_or(ColumnType::Integer),
        storage: Some(column.storage.clone()),
        length: column.storage.length,
        codes: Vec::new(),
        attribute: Some(Attribute::Year),
        map: BTreeMap::new(),
        encoding,
        years: Vec::new(),
        identifier: false,
        null_rate: Some(0.0),
    }
}

fn open_period(source: &str, variable: &str) -> Refusal {
    Refusal::new(
        "period_open",
        format!("{source}/{variable}"),
        "the order answer holds a period open at one end; order a bounded period",
    )
}

/// A file under construction, with each column's project source.
struct Built {
    file: SpecFile,
    /// Each column's sources: several where sources share a column.
    column_sources: Vec<Vec<String>>,
}

impl Built {
    fn new(name: String, years: Vec<i32>) -> Self {
        Self {
            file: SpecFile {
                name,
                source: None,
                unit: Unit::Person,
                population: Population::default(),
                rows_per_unit: 1.0,
                years,
                columns: Vec::new(),
            },
            column_sources: Vec::new(),
        }
    }

    fn push(&mut self, source: &str, column: SpecColumn) {
        self.file.source.get_or_insert_with(|| source.to_owned());
        self.column_sources.push(vec![source.to_owned()]);
        self.file.columns.push(column);
    }

    /// A column no source names (the period column): no rules reach it.
    fn push_unsourced(&mut self, column: SpecColumn) {
        self.column_sources.push(Vec::new());
        self.file.columns.push(column);
    }

    fn add_source(&mut self, column: usize, source: &str) {
        let sources = &mut self.column_sources[column];
        if !sources.iter().any(|s| s == source) {
            sources.push(source.to_owned());
        }
    }
}

/// Each file takes its unit, population and rows from its sources' rules, which must
/// agree when a file delivers several sources; each column its own source's column
/// rules. A column rule that matches no column is refused: a misspelt name would
/// otherwise silently do nothing.
fn apply_rules(built: Vec<Built>, rules: &Rules, refusals: &mut Vec<Refusal>) -> Vec<SpecFile> {
    let mut used = BTreeSet::new();
    let mut files = Vec::new();
    for Built {
        mut file,
        column_sources,
    } in built
    {
        let sources: BTreeSet<&String> = column_sources.iter().flatten().collect();
        let mut stated = sources.iter().filter_map(|s| rules.sources.get(*s));
        if let Some(first) = stated.next() {
            let key = |r: &crate::rules::SourceRules| {
                (r.unit, r.population.clone(), r.rows_per_unit.mean.to_bits())
            };
            if stated.any(|other| key(other) != key(first)) {
                refusals.push(Refusal::new(
                    "file_sources_disagree",
                    file.name.clone(),
                    "the sources this file delivers state different units, populations or \
                     rows_per_unit: state the same for each",
                ));
            }
            file.unit = first.unit;
            file.population = first.population.clone();
            file.rows_per_unit = first.rows_per_unit.mean;
        }
        for (column, sources) in file.columns.iter_mut().zip(&column_sources) {
            let stated: Vec<&ColumnRules> = sources
                .iter()
                .filter_map(|source| {
                    let column_rules = rules.sources.get(source)?.columns.get(&column.name)?;
                    used.insert((source.clone(), column.name.clone()));
                    Some(column_rules)
                })
                .collect();
            let Some(column_rules) = stated.first() else {
                continue;
            };
            if stated.iter().any(|other| other != column_rules) {
                refusals.push(Refusal::new(
                    "column_rules_disagree",
                    format!("{}/{}", file.name, column.name),
                    "the sources sharing this column state different rules for it: state the \
                     same for each",
                ));
            }
            if column_rules.attribute.is_some() {
                column.attribute = column_rules.attribute;
            }
            column.map.clone_from(&column_rules.map);
            column.null_rate = column_rules.null_rate;
            if let Some(codes) = &column_rules.codes {
                column.codes.clone_from(codes);
            }
        }
        files.push(file);
    }
    for (source, source_rules) in &rules.sources {
        for name in source_rules.columns.keys() {
            if !used.contains(&(source.clone(), name.clone())) {
                refusals.push(Refusal::new(
                    "rules_column_unknown",
                    format!("/sources/{source}/columns/{name}"),
                    "no file of this source has a column of this name",
                ));
            }
        }
    }
    files
}

/// File names the runner would accept, unique ignoring case; rates in `[0, 1]`, rows
/// per unit above 0, an age range not reversed, a value map only on an attribute.
fn check_files(files: &[SpecFile], null_rate: f64, refusals: &mut Vec<Refusal>) {
    let rate_ok = |rate: f64| (0.0..=1.0).contains(&rate);
    if !rate_ok(null_rate) {
        refusals.push(Refusal::new(
            "rate_invalid",
            "/null_rate",
            "a rate is between 0 and 1",
        ));
    }
    let mut seen = BTreeSet::new();
    for file in files {
        if let Err(reason) = manifest::safe_file_name(&file.name) {
            refusals.push(Refusal::new("file_name_invalid", file.name.clone(), reason));
        }
        if !seen.insert(file.name.to_ascii_lowercase()) {
            refusals.push(Refusal::new(
                "file_name_collision",
                file.name.clone(),
                "two files would share this name (case ignored)",
            ));
        }
        // JSON holds no NaN.
        if file.rows_per_unit <= 0.0 {
            refusals.push(Refusal::new(
                "rate_invalid",
                file.name.clone(),
                "rows_per_unit must be above 0",
            ));
        }
        if let (Some(min), Some(max)) = (file.population.age_min, file.population.age_max)
            && min > max
        {
            refusals.push(Refusal::new(
                "population_invalid",
                file.name.clone(),
                "age_min is above age_max",
            ));
        }
        for column in &file.columns {
            if column.attribute.is_none() && !column.map.is_empty() {
                refusals.push(Refusal::new(
                    "map_without_attribute",
                    format!("{}/{}", file.name, column.name),
                    "a value map applies to a unit attribute: state the column's attribute",
                ));
            }
            if column.null_rate.is_some_and(|r| !rate_ok(r)) {
                refusals.push(Refusal::new(
                    "rate_invalid",
                    format!("{}/{}", file.name, column.name),
                    "a rate is between 0 and 1",
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use reg_core::manifest::Storage;
    use reg_core::project::ColumnType;

    use super::resolve_type;

    // The #2071 precedence (G8/G10): a regression that let the held storage or the
    // catalog beat the project's override, or the catalog beat the storage, or that
    // generated a type mocks refuse, fails here.
    #[test]
    fn override_beats_storage_beats_catalog() {
        let int = Storage {
            dtype: Some("int".into()),
            length: None,
            precision: Some(10),
            scale: Some(0),
        };
        assert_eq!(
            resolve_type(Some(ColumnType::Text), Some(&int), Some("decimal")),
            Ok((ColumnType::Text, false))
        );
        assert_eq!(
            resolve_type(None, Some(&int), Some("text")),
            Ok((ColumnType::Integer, true))
        );
        assert_eq!(
            resolve_type(None, None, Some("date")),
            Ok((ColumnType::Date, false))
        );
        assert_eq!(resolve_type(None, None, None), Err("type_unknown"));
        assert_eq!(
            resolve_type(None, None, Some("geometry")),
            Err("type_unsupported")
        );
    }
}
