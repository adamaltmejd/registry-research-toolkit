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
    for (path, page) in pages::<SegmentPage>(&dir.join("segments"), &mut generations, |p| {
        p.next_cursor.as_deref()
    })? {
        check_periods(page.items.iter().map(|s| &s.period), &path)?;
        segments.extend(page.items);
    }
    let mut values: BTreeMap<String, Vec<Code>> = BTreeMap::new();
    let mut sizes: BTreeMap<String, Option<usize>> = BTreeMap::new();
    for (_, page) in pages::<ValuesPage>(&dir.join("values"), &mut generations, |p| {
        p.next_cursor.as_deref()
    })? {
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

/// Every saved page under `dir`, in page order. Each answer's pages are named
/// `<name>-0001.json`, `<name>-0002.json`, ... (as `save-answers.sh` writes them), so a
/// page left out or a last page that still points to a next one is refused rather
/// than compiled from part of an answer.
fn pages<T: DeserializeOwned>(
    dir: &Path,
    generations: &mut BTreeSet<String>,
    next: impl Fn(&T) -> Option<&str>,
) -> Result<Vec<(std::path::PathBuf, T)>, Refusal> {
    let mut answers: BTreeMap<String, Vec<(u32, std::path::PathBuf)>> = BTreeMap::new();
    for path in json_files(dir)? {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        let numbered = stem
            .rsplit_once('-')
            .filter(|(_, n)| n.len() == 4)
            .and_then(|(name, n)| Some((name.to_owned(), n.parse::<u32>().ok()?)));
        let Some((name, number)) = numbered else {
            return Err(Refusal::new(
                "answer_invalid",
                path.display().to_string(),
                "name each saved page <answer>-NNNN.json, numbered from 0001",
            ));
        };
        answers.entry(name).or_default().push((number, path));
    }
    let mut out = Vec::new();
    for (name, mut numbered) in answers {
        numbered.sort();
        let count = numbered.len();
        for (i, (number, path)) in numbered.into_iter().enumerate() {
            let page: T = read_answer(&path, generations)?;
            let last = i + 1 == count;
            if usize::try_from(number).ok() != Some(i + 1) || next(&page).is_some() == last {
                return Err(Refusal::new(
                    "answers_incomplete",
                    format!("{}/{name}", dir.display()),
                    "a page of this answer is missing: save every page, following next_cursor \
                     until it is null",
                ));
            }
            out.push((path, page));
        }
    }
    Ok(out)
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

/// A physical column's name and its references.
type NamedRefs<'a> = (&'a str, Vec<Column<'a>>);

/// One reference of a physical column: a variable in a table over a period.
#[derive(Clone)]
struct Column<'a> {
    name: &'a str,
    source: &'a str,
    binding: Option<usize>,
    table: &'a str,
    variable: &'a str,
    period: &'a Period,
    /// The held file and column, in the delivery layout.
    placement: Option<(&'a str, &'a str)>,
    /// The periods the column covers, when not the reference's own: a standard file's
    /// year, each of a delivery file's years.
    year_periods: Vec<Period>,
}

impl Context<'_> {
    /// One file per table and year (or one for a timeless table): the source's
    /// requested columns, then the table's row keys, each name resolved once.
    fn standard_files(&self, order: &LogicalOrder, refusals: &mut Vec<Refusal>) -> Vec<Built> {
        // Keyed by table, not by file name: two tables whose names collide stay two
        // files, and check_files refuses the shared name.
        let mut files: BTreeMap<(&str, Option<i32>), Vec<NamedRefs<'_>>> = BTreeMap::new();
        let ordered = order.entries.iter().map(|e| Column {
            name: &e.spelling,
            source: &e.source,
            binding: Some(e.binding),
            table: &e.table,
            variable: &e.variable,
            period: &e.period,
            placement: None,
            year_periods: Vec::new(),
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
                year_periods: Vec::new(),
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
                // The year within the ordered period: a value set or type can change
                // between and within years.
                let mut r = column.clone();
                r.year_periods = year
                    .map(|y| within(year_period(y), column.period))
                    .into_iter()
                    .collect();
                let refs = files.entry((column.table, year)).or_default();
                let at = refs.iter().position(|(name, _)| *name == column.name);
                let i = at.unwrap_or_else(|| {
                    refs.push((column.name, Vec::new()));
                    refs.len() - 1
                });
                refs[i].1.push(r);
            }
        }
        let mut built_files = Vec::new();
        for ((table, year), columns) in files {
            let mut built = Built::new(standard_file_name(table, year), year.into_iter().collect());
            for (name, refs) in columns {
                if let Some(column) = self.resolve(&built.file.name, name, &refs, None, refusals) {
                    built.push_with(column, &refs);
                }
            }
            built_files.push(built);
        }
        built_files
    }

    /// The order file's files and columns, as named; each column resolved once over
    /// the file's years, which the runner reads every column over.
    fn delivery_files(&self, manifest: &Manifest, refusals: &mut Vec<Refusal>) -> Vec<Built> {
        let mut files = Vec::new();
        for file in &manifest.files {
            let mut built = Built::new(file.name.clone(), Vec::new());
            // simplify: the runner extracts a file without a row filter over its whole
            // delivery; the mock holds only the requested years, upgrade if a pipeline
            // reads years it did not order.
            let mut years = BTreeSet::new();
            for r in file.columns.iter().flat_map(|c| &c.refs) {
                match self::years(&r.period) {
                    Ok(Some(ys)) => years.extend(ys),
                    Ok(None) => {}
                    Err(()) => refusals.push(open_period(&r.source, &r.variable)),
                }
            }
            built.file.years = years.into_iter().collect();
            // Each file year, not their span: a gap year's value sets are not extracted.
            let file_years: Vec<Period> =
                built.file.years.iter().map(|&y| year_period(y)).collect();
            for column in &file.columns {
                let row_filter = file.row_filter.as_ref().filter(|f| f.column == column.name);
                // The runner's period column: a row key placing no variable. It has no
                // row filter when the order covers the whole delivery.
                if column.refs.is_empty() {
                    if column.role == manifest::Role::RowKey {
                        match year_column(column, row_filter.map(|f| f.encoding)) {
                            Ok(year) => built.push_unsourced(year),
                            Err(code) => refusals.push(Refusal::new(
                                code,
                                format!("{}/{}", file.name, column.name),
                                "the order file's period column has no storage type mocks \
                                 write: save the materialize answer again",
                            )),
                        }
                    } else {
                        refusals.push(Refusal::new(
                            "column_unplaced",
                            format!("{}/{}", file.name, column.name),
                            "the order file has a column that places no variable and is not \
                             a row key: save the materialize answer again",
                        ));
                    }
                    continue;
                }
                let refs: Vec<Column<'_>> = column
                    .refs
                    .iter()
                    .map(|r| Column {
                        name: &column.name,
                        source: &r.source,
                        binding: r.binding,
                        table: &r.table,
                        variable: &r.variable,
                        period: &r.period,
                        placement: Some((&file.table, &column.name)),
                        year_periods: file_years.clone(),
                    })
                    .collect();
                let Some(mut spec_column) = self.resolve(
                    &file.name,
                    &column.name,
                    &refs,
                    Some(&column.storage),
                    refusals,
                ) else {
                    continue;
                };
                spec_column.stata_name.clone_from(&column.stata_name);
                spec_column.label = column.stata_name.as_ref().map(|_| column.name.clone());
                if let Some(filter) = row_filter {
                    spec_column.attribute = Some(Attribute::Year);
                    spec_column.encoding = Some(filter.encoding);
                }
                // Missing only in the file's years its placement holds no segment.
                let held = self.held_years(&file.table, &column.name);
                if !held.is_empty() {
                    spec_column.years = built
                        .file
                        .years
                        .iter()
                        .copied()
                        .filter(|y| held.contains(y))
                        .collect();
                    if spec_column.years == built.file.years {
                        spec_column.years.clear();
                    }
                }
                built.push_with(spec_column, &refs);
            }
            files.push(built);
        }
        files
    }

    /// The years a held placement holds, from its segment rows; empty when the saved
    /// answers have none (no mask).
    fn held_years(&self, file: &str, column: &str) -> Vec<i32> {
        let mut held = BTreeSet::new();
        for segment in &self.answers.segments {
            if segment.file.as_deref() == Some(file) && segment.column.as_deref() == Some(column) {
                held.extend(years(&segment.period).ok().flatten().unwrap_or_default());
            }
        }
        held.into_iter().collect()
    }

    /// One physical column from all its references (DESIGN.md, "Invariants", 2).
    fn resolve(
        &self,
        file: &str,
        name: &str,
        refs: &[Column<'_>],
        storage: Option<&Storage>,
        refusals: &mut Vec<Refusal>,
    ) -> Option<SpecColumn> {
        let at = format!("{file}/{name}");
        // The project's type for any variable the column delivers.
        let mut overrides: Vec<ColumnType> = Vec::new();
        for t in refs.iter().filter_map(|r| self.binding_type(r)) {
            if !overrides.contains(&t) {
                overrides.push(t);
            }
        }
        if overrides.len() > 1 {
            refusals.push(Refusal::new(
                "column_types_disagree",
                at,
                "the variables delivered in this column have different `type`s in \
                 project_data.json: give them the same",
            ));
            return None;
        }
        let mut segments: Vec<&Segment> = Vec::new();
        for r in refs {
            let found = self.segments(r);
            if found.is_empty() {
                refusals.push(Refusal::new(
                    "segment_missing",
                    format!("{}/{}/{}", r.source, r.table, r.name),
                    format!(
                        "save the segments answer for {} with variable={} into segments/, \
                         or for a variable of another register its own answer \
                         (segments/{})",
                        r.table, r.variable, r.variable
                    ),
                ));
                return None;
            }
            segments.extend(found);
        }
        segments.sort_by_key(|s| s.period.days((i64::MIN, i64::MAX)));
        let primary = segments[0];
        // The catalog's type decides only without an override or held storage; then
        // every segment's must resolve, and to the same type.
        let resolved = segments
            .iter()
            .map(|s| resolve_type(overrides.first().copied(), storage, s.dtype.as_deref()))
            .try_fold(None, |seen: Option<(ColumnType, bool)>, r| {
                let r = r?;
                match seen {
                    Some(seen) if seen != r => Err("column_types_disagree"),
                    _ => Ok(Some(r)),
                }
            });
        let (dtype, held) = match resolved {
            Ok(resolved) => resolved.expect("a segment per reference"),
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
        let mut codes: Vec<Code> = Vec::new();
        let hashes: BTreeSet<&str> = segments
            .iter()
            .filter_map(|s| s.value_set.as_ref().map(|v| v.hash.as_str()))
            .collect();
        for hash in hashes {
            let Some(set) = self.answers.values.get(hash) else {
                refusals.push(Refusal::new(
                    "values_missing",
                    at,
                    format!(
                        "save every page of the values answer for value_set={hash} into values/"
                    ),
                ));
                return None;
            };
            for code in set {
                if !codes.iter().any(|c| c.code == code.code) {
                    codes.push(code.clone());
                }
            }
        }
        let stata_name = (self.format == ExtractFormat::Dta && storage.is_none())
            .then(|| manifest::stata_name(name))
            .flatten();
        Some(SpecColumn {
            name: name.to_owned(),
            label: stata_name.as_ref().map(|_| name.to_owned()),
            stata_name,
            dtype,
            storage: held.then(|| storage.cloned()).flatten(),
            length: primary.length,
            codes,
            attribute: None,
            map: BTreeMap::new(),
            encoding: None,
            years: Vec::new(),
            identifier: primary.identifier.unwrap_or(false),
            null_rate: None,
        })
    }

    /// The project's `type` for the reference's variable: its own binding's, or for a
    /// reference without one (a row key, a carried copy) its source's binding of that
    /// variable, else any source's.
    fn binding_type(&self, r: &Column<'_>) -> Option<ColumnType> {
        let source = self.project.sources.iter().find(|s| s.name == r.source);
        if let Some(b) = r.binding {
            return source?.bindings.get(b)?.column_type;
        }
        let of = |bindings: &[reg_core::project::Binding]| {
            bindings
                .iter()
                .filter(|b| b.variable == r.variable)
                .find_map(|b| b.column_type)
        };
        source
            .and_then(|s| of(&s.bindings))
            .or_else(|| self.project.sources.iter().find_map(|s| of(&s.bindings)))
    }

    /// The reference's segments over its period, earliest first: the held placement's
    /// rows (delivery layout), else its table's, else for a reference without a binding
    /// any table's, since a row key can be another register's variable (a person id
    /// carried into a steward table) whose segments are its own table's.
    fn segments(&self, r: &Column<'_>) -> Vec<&Segment> {
        let open = (i64::MIN, i64::MAX);
        let periods: Vec<(i64, i64)> = if r.year_periods.is_empty() {
            vec![r.period.days(open)]
        } else {
            r.year_periods.iter().map(|p| p.days(open)).collect()
        };
        let overlapping = || {
            let periods = &periods;
            self.answers
                .segments
                .iter()
                .filter(|s| s.variable == r.variable)
                .filter(move |s| {
                    let (s_start, s_end) = s.period.days(open);
                    periods
                        .iter()
                        .any(|&(start, end)| s_start < end && start < s_end)
                })
        };
        let in_table = || overlapping().filter(|s| s.table == r.table);
        let mut found: Vec<&Segment> = match r.placement {
            Some((file, name)) => in_table()
                .filter(|s| s.file.as_deref() == Some(file) && s.column.as_deref() == Some(name))
                .collect(),
            None => Vec::new(),
        };
        if found.is_empty() {
            found = in_table().collect();
        }
        if found.is_empty() && r.binding.is_none() {
            found = overlapping().collect();
        }
        found.sort_by_key(|s| s.period.days(open));
        found
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

/// The runner's period column, typed by its storage; without a row filter its
/// encoding is unknown and the year is written whole. A storage type that is missing or
/// one mocks do not write is refused, as for any column.
fn year_column(
    column: &manifest::Column,
    encoding: Option<manifest::Encoding>,
) -> Result<SpecColumn, &'static str> {
    let (dtype, _) = resolve_type(None, Some(&column.storage), None)?;
    Ok(SpecColumn {
        name: column.name.clone(),
        stata_name: column.stata_name.clone(),
        label: column.stata_name.as_ref().map(|_| column.name.clone()),
        dtype,
        storage: Some(column.storage.clone()),
        length: column.storage.length,
        codes: Vec::new(),
        attribute: Some(Attribute::Year),
        map: BTreeMap::new(),
        encoding,
        years: Vec::new(),
        identifier: false,
        null_rate: Some(0.0),
    })
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

    /// A column with the sources of its references.
    fn push_with(&mut self, column: SpecColumn, refs: &[Column<'_>]) {
        let mut sources: Vec<String> = Vec::new();
        for r in refs {
            if !sources.iter().any(|s| s == r.source) {
                sources.push(r.source.to_owned());
            }
        }
        if let Some(first) = sources.first() {
            self.file.source.get_or_insert_with(|| first.clone());
        }
        self.column_sources.push(sources);
        self.file.columns.push(column);
    }

    /// A column no source names (the period column): no rules reach it.
    fn push_unsourced(&mut self, column: SpecColumn) {
        self.column_sources.push(Vec::new());
        self.file.columns.push(column);
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
