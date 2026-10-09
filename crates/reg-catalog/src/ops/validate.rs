//! `validate`: every issue of a `project_data.json` document (today's
//! `reg_meta.semantic.validate_project`). The supported-version decision and the
//! structural validator (`reg-core`) run first, without the catalog; the semantic
//! layer then resolves each source's variant and each binding in reference scope over
//! the compiled states ([`emitted`]), and on a steward artifact checks each binding
//! against the steward's holdings. An invalid project is a result (`ok: false`),
//! never an error.
//!
//! Availability is decided once, by [`resolve_binding`] (today's
//! `reg_meta.order.resolve_binding`, which the order materializer shares): a binding
//! is requested wherever it is available inside the source's period, so availability
//! narrower than the request is an informational clip and only availability empty
//! everywhere in it blocks.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::project::{Binding, ProjectData, Source, version_issue};
use reg_core::{
    Fqid, Interval, IssueLevel, ValidationIssue, ValidationResult, intersect, merge, overlap,
    quote, quoted_list, render, snap_month_end,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs;
use super::states::{Emitted, emitted, value_set, variant_id};
use super::{Params, Server};
use crate::{Error, Scope};

/// The validation result: `ok` when no issue is an error.
#[derive(Serialize, ToSchema)]
pub struct Validation {
    ok: bool,
    /// In emission order: per source, its variant and period, then per binding.
    issues: Vec<Issue>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[schema(as = IssueLevel)]
enum Level {
    Error,
    Warning,
    Info,
}

/// One finding about the document.
#[derive(Serialize, ToSchema)]
#[schema(as = ValidationIssue)]
struct Issue {
    level: Level,
    /// A stable identifier, such as `period_outside_state_validity`.
    code: &'static str,
    /// An RFC 6901 JSON pointer into the document; empty for the whole document.
    path: String,
    message: String,
    /// The successor a `variable_replaced` finding names; null otherwise.
    #[schema(required = true)]
    successor_fqid: Option<String>,
}

impl From<ValidationResult> for Validation {
    fn from(result: ValidationResult) -> Self {
        let ok = result.ok();
        let issues = result
            .issues
            .into_iter()
            .map(|issue| {
                let ValidationIssue {
                    level,
                    code,
                    path,
                    message,
                    successor_fqid,
                } = issue;
                let level = match level {
                    IssueLevel::Error => Level::Error,
                    IssueLevel::Warning => Level::Warning,
                    IssueLevel::Info => Level::Info,
                };
                Issue {
                    level,
                    code,
                    path,
                    message,
                    successor_fqid,
                }
            })
            .collect();
        Self { ok, issues }
    }
}

pub fn validate(server: &Server, _scope: Scope, params: &Params) -> Result<Value, Error> {
    let result = match project(params)? {
        Err(rejected) => rejected,
        Ok(project) => {
            let conn = server.catalog.connect()?;
            let mut semantic = Semantic {
                conn: &conn,
                steward: server.catalog.is_steward(),
                issues: Vec::new(),
            };
            for (i, source) in project.sources.iter().enumerate() {
                semantic.source(&format!("/sources/{i}"), source)?;
            }
            ValidationResult {
                issues: semantic.issues,
            }
        }
    };
    Ok(serde_json::to_value(Validation::from(result)).expect("Validation serializes"))
}

/// The `project` parameter as a project, or the issues that reject it without the
/// catalog: the supported-version decision alone, else the structural validator's.
/// `order` shares this door (today's `project_from_raw`).
pub(super) fn project(params: &Params) -> Result<Result<ProjectData, ValidationResult>, Error> {
    // The transports hand the body over as JSON text: HTTP after refusing malformed
    // bytes, MCP from its object argument.
    let raw: Value = serde_json::from_str(params["project"])
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| Error::invalid_parameter("project"))?;
    if let Some(issue) = version_issue(&raw) {
        // Alone: every other check reads the document as the contract it rejects.
        return Ok(Err(ValidationResult {
            issues: vec![issue],
        }));
    }
    Ok(ProjectData::from_value(&raw))
}

/// The representations the steward holds variable `id` in under `variant`, in
/// tables of a known scope (today's `Holdings.columns`).
pub(super) fn held_representations(
    conn: &Connection,
    id: i64,
    variant: i64,
) -> Result<BTreeSet<String>, Error> {
    Ok(conn
        .prepare_cached(
            "SELECT DISTINCT hm.representation_canonical FROM holding_mapping hm \
             JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) \
             WHERE ht.scope != 'unknown' AND hm.variable_id = ? AND hm.variant_id = ?",
        )?
        .query_map([id, variant], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

struct Semantic<'a> {
    conn: &'a Connection,
    steward: bool,
    issues: Vec<ValidationIssue>,
}

impl Semantic<'_> {
    fn push(&mut self, level: IssueLevel, code: &'static str, path: String, message: String) {
        self.issues.push(ValidationIssue {
            level,
            code,
            path,
            message,
            successor_fqid: None,
        });
    }

    fn source(&mut self, base: &str, source: &Source) -> Result<(), Error> {
        let variant_ok = self.register_variant(base, &source.register_variant)?;
        let requested = match source.period.intervals() {
            Ok(requested) => requested,
            Err(reason) => {
                self.push(
                    IssueLevel::Error,
                    "period_not_orderable",
                    format!("{base}/period"),
                    format!(
                        "source {} has no orderable period: {reason}",
                        quote(&source.name)
                    ),
                );
                return Ok(());
            }
        };
        for (i, binding) in source.bindings.iter().enumerate() {
            let base = format!("{base}/bindings/{i}");
            self.binding(&base, source, binding, variant_ok, requested.as_deref())?;
        }
        Ok(())
    }

    /// Whether `<provider>/<register>/<variant>` names a variant; reports it when not.
    fn register_variant(&mut self, base: &str, coordinate: &str) -> Result<bool, Error> {
        let parts: Vec<&str> = coordinate.split('/').collect();
        let [provider, register, variant] = parts[..] else {
            unreachable!("the structural validator admits only 3-part register variants")
        };
        let known: Vec<String> = self
            .conn
            .prepare_cached(
                "SELECT rv.slug FROM register_variant rv JOIN register r USING(register_id) \
                 JOIN provider p USING(provider_id) \
                 WHERE p.slug = ? AND r.slug = ? AND rv.slug IS NOT NULL ORDER BY rv.slug",
            )?
            .query_map([provider, register], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let message = if known.is_empty() {
            format!(
                "register_variant {} resolves to no register or no variants in reg_meta",
                quote(coordinate)
            )
        } else if known.iter().any(|slug| slug == variant) {
            return Ok(true);
        } else {
            format!(
                "variant {} is not a known variant of {provider}/{register} (known: {})",
                quote(variant),
                quoted_list(known.iter().map(String::as_str))
            )
        };
        let path = format!("{base}/register_variant");
        self.push(IssueLevel::Error, "fqid_unresolved", path, message);
        Ok(false)
    }

    fn binding(
        &mut self,
        base: &str,
        source: &Source,
        binding: &Binding,
        variant_ok: bool,
        requested: Option<&[Interval]>,
    ) -> Result<(), Error> {
        let path = format!("{base}/variable");
        let Ok(Fqid::Variable {
            provider,
            register,
            variable,
        }) = binding.variable.parse()
        else {
            let message = format!(
                "binding variable {} is not a parseable FQID",
                quote(&binding.variable)
            );
            self.push(IssueLevel::Error, "fqid_unresolved", path, message);
            return Ok(());
        };
        let slugs = [provider, register, variable];
        // A direct hit only: a retired FQID is not followed to its successor, which
        // `variable_replaced` names instead.
        let Some(id) = refs::variable_id(self.conn, Scope::Reference, &slugs)? else {
            let message = format!(
                "column {} resolves to no variable in reg_meta",
                quote(&binding.variable)
            );
            self.push(IssueLevel::Error, "fqid_unresolved", path, message);
            return self.value_set(base, binding);
        };
        self.hints(&path, binding, id, &slugs, requested)?;
        // The structural validator matches the binding's register to the source's.
        let variant = if variant_ok {
            let slug = source.register_variant.rsplit('/').next().expect("a slug");
            variant_id(self.conn, id, slug)?
        } else {
            None
        };
        if let Some(variant) = variant {
            let columns = self.period(&path, source, binding, id, variant, requested)?;
            if self.steward {
                self.admission(&path, source, binding, id, variant, columns.as_ref())?;
            }
        }
        self.value_set(base, binding)
    }

    /// The non-blocking hints: a deprecated variable, and each successor whose
    /// succession is effective by the requested period's last year.
    fn hints(
        &mut self,
        path: &str,
        binding: &Binding,
        id: i64,
        slugs: &[String; 3],
        requested: Option<&[Interval]>,
    ) -> Result<(), Error> {
        let deprecated: bool = self
            .conn
            .prepare_cached("SELECT deprecated FROM variable WHERE variable_id = ?")?
            .query_row([id], |row| row.get(0))?;
        if deprecated {
            self.push(
                IssueLevel::Info,
                "deprecated_traversal",
                path.to_owned(),
                format!(
                    "column {} resolves to a deprecated catalog variable; prefer a current \
                     successor when one is available",
                    quote(&binding.variable)
                ),
            );
        }
        // A year-independent request has no last year, so no succession is effective
        // by it (today's reader raises IndexError on a dated one: a Rust-only fix).
        let Some(requested) = requested.filter(|r| !r.is_empty()) else {
            return Ok(());
        };
        let last_year: i64 = requested[requested.len() - 1].1[..4]
            .parse()
            .expect("an ISO year");
        let successors: Vec<(String, String, String, Option<i64>)> = self
            .conn
            .prepare_cached(
                "SELECT successor_provider, successor_register, successor_variable, \
                 effective_year FROM variable_replaced_by WHERE predecessor_provider = ? \
                 AND predecessor_register = ? AND predecessor_variable = ? ORDER BY 1, 2, 3",
            )?
            .query_map(params![slugs[0], slugs[1], slugs[2]], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        for (provider, register, variable, effective) in successors {
            // An undated succession is tied to no requested year.
            let Some(year) = effective.filter(|&year| year <= last_year) else {
                continue;
            };
            let fqid = refs::fqid(&[
                Some(provider.clone()),
                Some(register.clone()),
                Some(variable.clone()),
            ]);
            let target = fqid
                .clone()
                .unwrap_or_else(|| format!("{provider}/{register}/{variable}"));
            self.issues.push(ValidationIssue {
                level: IssueLevel::Info,
                code: "variable_replaced",
                path: path.to_owned(),
                message: format!(
                    "column {} has replacement {} effective {year} by requested period {}",
                    quote(&binding.variable),
                    quote(&target),
                    render(requested)
                ),
                successor_fqid: fqid,
            });
        }
        Ok(())
    }

    /// The availability findings of [`resolve_binding`], under the validation codes;
    /// the binding's columns (delivered spelling to canonical) unless a finding
    /// blocks it.
    fn period(
        &mut self,
        path: &str,
        source: &Source,
        binding: &Binding,
        id: i64,
        variant: i64,
        requested: Option<&[Interval]>,
    ) -> Result<Option<BTreeMap<String, String>>, Error> {
        let pin = binding.representation.as_deref();
        let resolution = resolve_binding(
            self.conn,
            source,
            binding,
            pin,
            id,
            Some(variant),
            requested,
        )?;
        let variable = quote(&binding.variable);
        let at = &source.register_variant;
        let period = &resolution.requested_period;
        if let Some(ordered) = &resolution.clip {
            self.push(
                IssueLevel::Info,
                "range_period_partially_covered",
                path.to_owned(),
                format!(
                    "column {variable} is available for only part of requested period \
                     {period} at {at}; it is ordered for {ordered}"
                ),
            );
        }
        if let Some(Finding { code, message, .. }) = resolution.finding {
            let code = match code {
                "binding_unavailable" => "period_outside_state_validity",
                "representation_unknown" => "binding_representation_unknown",
                "representation_ambiguous" => "binding_value_set_version_ambiguous",
                code => code,
            };
            self.push(IssueLevel::Error, code, path.to_owned(), message);
            return Ok(None);
        }
        let states = &resolution.states;
        let columns = states
            .iter()
            .filter_map(|s| {
                let column = s.emitted.delivery_column_name.clone()?;
                let canonical = s.emitted.canonical_column.clone();
                Some((column.clone(), canonical.unwrap_or(column)))
            })
            .collect();
        // A backstop the build's co-delivery curation should make unreachable: two
        // value sets delivered at one requested instant.
        let codelivered = states.iter().enumerate().any(|(i, a)| {
            states[i + 1..].iter().any(|b| {
                a.value_set.0 != b.value_set.0 && !overlap(&a.intervals, &b.intervals).is_empty()
            })
        });
        if codelivered {
            let mut labels: BTreeMap<Option<i64>, &str> = BTreeMap::new();
            for s in states {
                labels.entry(s.value_set.0).or_insert(&s.value_set.1);
            }
            let mut labels: Vec<&str> = labels.into_values().collect();
            labels.sort_unstable();
            self.push(
                IssueLevel::Error,
                "binding_value_set_version_ambiguous",
                path.to_owned(),
                format!(
                    "column {variable} resolves to several co-delivered value sets {} on one \
                     column at {at} period {period} — this reg_meta build needs co-delivery \
                     curation",
                    quoted_list(labels)
                ),
            );
        } else if states.len() > 1 {
            self.push(
                IssueLevel::Info,
                "binding_state_drifts_within_period",
                path.to_owned(),
                format!(
                    "column {variable} spans {} states across a transition within period \
                     {period}",
                    states.len()
                ),
            );
        }
        Ok(Some(columns))
    }

    /// On a steward artifact, warn when the steward holds the binding in no
    /// representation under the source's variant, or not in one it resolves to.
    /// Unresolved `columns` (a blocked binding) skip the representation check.
    fn admission(
        &mut self,
        path: &str,
        source: &Source,
        binding: &Binding,
        id: i64,
        variant: i64,
        columns: Option<&BTreeMap<String, String>>,
    ) -> Result<(), Error> {
        let held = held_representations(self.conn, id, variant)?;
        let (variable, at) = (quote(&binding.variable), &source.register_variant);
        if held.is_empty() {
            let message = format!(
                "column {variable} resolves in reg_meta but is outside this deployment's \
                 steward catalog under {at} — the steward does not supply it there"
            );
            let code = "fqid_outside_steward_catalog";
            self.push(IssueLevel::Warning, code, path.to_owned(), message);
            return Ok(());
        }
        let missing: Vec<&String> = columns
            .into_iter()
            .flatten()
            .filter(|(_, canonical)| !held.contains(*canonical))
            .map(|(column, _)| column)
            .collect();
        if !missing.is_empty() {
            let listed = |columns: Vec<&String>| {
                columns
                    .into_iter()
                    .map(|c| quote(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let message = format!(
                "column {variable} resolves to representation {}, which this steward does \
                 not supply under {at} — available there as {} only",
                listed(missing),
                listed(held.iter().collect())
            );
            let code = "representation_outside_steward_catalog";
            self.push(IssueLevel::Warning, code, path.to_owned(), message);
        }
        Ok(())
    }

    /// A binding's `value_set` must name a classification.
    fn value_set(&mut self, base: &str, binding: &Binding) -> Result<(), Error> {
        let Some(value_set) = &binding.value_set else {
            return Ok(());
        };
        let reason = match value_set.parse() {
            Ok(Fqid::Classification { classification }) => {
                let found = self
                    .conn
                    .prepare_cached("SELECT 1 FROM classification WHERE slug = ?")?
                    .query_row([classification], |_| Ok(()))
                    .optional()?;
                if found.is_some() {
                    return Ok(());
                }
                "resolves to no classification in reg_meta"
            }
            _ => "is not a parseable FQID",
        };
        self.push(
            IssueLevel::Error,
            "value_set_missing",
            format!("{base}/value_set"),
            format!("value_set {} {reason}", quote(value_set)),
        );
        Ok(())
    }
}

/// A kept state: its representation, the requested days it is available for, and
/// its value set and version label.
pub(super) struct Kept {
    pub(super) emitted: Emitted,
    intervals: Vec<Interval>,
    value_set: (Option<i64>, String),
}

/// A blocking finding of [`resolve_binding`]: today's order finding code, its
/// message and the period it names.
pub(super) struct Finding {
    pub(super) code: &'static str,
    pub(super) message: String,
    pub(super) period: Option<String>,
}

/// What one binding resolves to inside its source's period (today's
/// `BindingResolution`).
pub(super) struct Resolution {
    /// The request as rendered for messages; `_default` when year-independent.
    pub(super) requested_period: String,
    /// The states the request reaches, narrowed to a pinned representation, in the
    /// resolver's order.
    pub(super) states: Vec<Kept>,
    /// The available request partitioned into `(lo, hi, delivery column)` windows of
    /// constant representation, sorted; none when year-independent or blocked.
    pub(super) slices: Vec<(String, String, String)>,
    /// Where the binding is available, rendered, when that is less than the request.
    pub(super) clip: Option<String>,
    pub(super) finding: Option<Finding>,
}

/// Resolve `binding` (the variable `id`, at the source's register variant
/// `variant`, none when the source names no variant of the register) inside
/// `requested` (`None`: year-independent), narrowed to `pin`, the representation
/// the binding pins.
pub(super) fn resolve_binding(
    conn: &Connection,
    source: &Source,
    binding: &Binding,
    pin: Option<&str>,
    id: i64,
    variant: Option<i64>,
    requested: Option<&[Interval]>,
) -> Result<Resolution, Error> {
    let requested_period = requested.map_or_else(|| "_default".to_owned(), render);
    let blocked = |code, message, period: Option<String>| Resolution {
        requested_period: requested_period.clone(),
        states: Vec::new(),
        slices: Vec::new(),
        clip: None,
        finding: Some(Finding {
            code,
            message,
            period,
        }),
    };
    let (variable, at) = (quote(&binding.variable), &source.register_variant);
    let Some(requested) = requested else {
        return year_independent(conn, id, variant, pin, |code, message| {
            blocked(code, message, Some("_default".into()))
        });
    };
    let mut states = Vec::new();
    let mut by_column: BTreeMap<String, Vec<Interval>> = BTreeMap::new();
    // What the binding is delivered as somewhere the request reaches.
    let mut offered = BTreeSet::new();
    for (e, overlaps) in reach(conn, id, variant, requested)? {
        let Some(column) = e.delivery_column_name.clone() else {
            return Ok(blocked(
                "representation_unresolved",
                format!(
                    "column {variable} resolves to a state with no delivery column at {at}; \
                     an unresolved representation cannot be ordered"
                ),
                Some(render(&merge(overlaps))),
            ));
        };
        offered.insert(column.clone());
        if pin.is_some_and(|pin| pin != column) {
            continue;
        }
        by_column
            .entry(column)
            .or_default()
            .extend(overlaps.iter().cloned());
        let value_set = value_set(conn, id, &e)?;
        states.push(Kept {
            emitted: e,
            intervals: merge(overlaps),
            value_set,
        });
    }
    let period = &requested_period;
    if by_column.is_empty() {
        return Ok(match pin {
            Some(pin) if !offered.is_empty() => blocked(
                "representation_unknown",
                format!(
                    "column {variable} pins representation {}, which is not a delivery \
                     column at {at} in {period} (available: {})",
                    quote(pin),
                    quoted_list(offered.iter().map(String::as_str))
                ),
                Some(period.clone()),
            ),
            _ => blocked(
                "binding_unavailable",
                format!("column {variable} has no state covering {at} anywhere in {period}"),
                Some(period.clone()),
            ),
        });
    }
    let mut slices: Vec<(String, String, String)> = by_column
        .iter()
        .flat_map(|(column, intervals)| {
            merge(intervals.clone())
                .into_iter()
                .map(move |(lo, hi)| (lo, hi, column.clone()))
        })
        .collect();
    slices.sort_unstable();
    let availability = merge(
        slices
            .iter()
            .map(|(lo, hi, _)| (lo.clone(), hi.clone()))
            .collect(),
    );
    let coexisting = coexisting(&slices);
    let finding = (!coexisting.is_empty()).then(|| Finding {
        code: "representation_ambiguous",
        message: format!(
            "column {variable} resolves to co-existing representations {} at {at}; pin one \
             with `representation` — a manifest never guesses",
            quoted_list(coexisting)
        ),
        period: Some(period.clone()),
    });
    Ok(Resolution {
        // The clip is reported even beside a finding, which is stated against it.
        clip: (availability != requested).then(|| render(&availability)),
        requested_period,
        states,
        slices,
        finding,
    })
}

/// The columns whose slices overlap in time: parallel representations the binding
/// must choose between. Distinct columns in disjoint windows are a sequential rename.
fn coexisting(slices: &[(String, String, String)]) -> BTreeSet<&str> {
    let mut coexisting = BTreeSet::new();
    for (i, (a_lo, a_hi, a)) in slices.iter().enumerate() {
        for (b_lo, b_hi, b) in &slices[i + 1..] {
            if a != b && a_lo <= b_hi && b_lo <= a_hi {
                coexisting.extend([a.as_str(), b.as_str()]);
            }
        }
    }
    coexisting
}

/// The dated states `requested` reaches, each with the requested days it covers, in
/// the resolver's order. Resolved per requested segment: one resolution over the
/// request's outer span would let a window in one segment hide the fallback another
/// segment needs. A state reaching several segments stays one state.
fn reach(
    conn: &Connection,
    id: i64,
    variant: Option<i64>,
    requested: &[Interval],
) -> Result<Vec<(Emitted, Vec<Interval>)>, Error> {
    let mut reached: Vec<(Emitted, Vec<Interval>)> = Vec::new();
    let Some(variant) = variant else {
        return Ok(reached);
    };
    for segment in requested {
        let bounds = (segment.0.as_str(), segment.1.as_str());
        for e in emitted(conn, Scope::Reference, id, Some(variant), Some(bounds))? {
            let window = (
                e.valid_from.clone().expect("a dated state has bounds"),
                snap_month_end(e.valid_to.as_deref().expect("a dated state has bounds")),
            );
            let Some(overlap) = intersect(&window, segment) else {
                continue;
            };
            // Keyed as today's resolver keys a window: by state, column and start, so
            // a month window and the annual fallback another segment reaches (same
            // state, column and start) are one state.
            let key = |r: &Emitted| {
                (
                    r.state_id,
                    r.delivery_column_name.clone(),
                    r.valid_from.clone(),
                )
            };
            match reached.iter_mut().find(|(r, _)| key(r) == key(&e)) {
                Some((_, overlaps)) => overlaps.push(overlap),
                None => reached.push((e, vec![overlap])),
            }
        }
    }
    Ok(reached)
}

/// A `_default` request: the variant's year-independent states. The structural
/// validator refuses `_default` on a `_default` variant, so the variant is concrete.
fn year_independent(
    conn: &Connection,
    id: i64,
    variant: Option<i64>,
    pin: Option<&str>,
    blocked: impl Fn(&'static str, String) -> Resolution,
) -> Result<Resolution, Error> {
    let independent: Vec<Emitted> = match variant {
        Some(variant) => emitted(conn, Scope::Reference, id, Some(variant), None)?
            .into_iter()
            .filter(|e| e.period_scope == "year_independent")
            .collect(),
        None => Vec::new(),
    };
    if independent.iter().any(|e| e.delivery_column_name.is_none()) {
        let message = "year-independent state has no delivery column";
        return Ok(blocked("representation_unresolved", message.into()));
    }
    let offered = !independent.is_empty();
    let mut states = Vec::new();
    for e in independent {
        if pin.is_none_or(|pin| e.delivery_column_name.as_deref() == Some(pin)) {
            let value_set = value_set(conn, id, &e)?;
            states.push(Kept {
                emitted: e,
                intervals: Vec::new(),
                value_set,
            });
        }
    }
    let columns: BTreeSet<_> = states
        .iter()
        .map(|s| &s.emitted.delivery_column_name)
        .collect();
    let value_sets: BTreeSet<_> = states.iter().map(|s| s.value_set.0).collect();
    let refusal = if states.is_empty() {
        // Today's truthiness: an empty pin pins nothing.
        let code = if offered && pin.is_some_and(|pin| !pin.is_empty()) {
            "representation_unknown"
        } else {
            "binding_unavailable"
        };
        Some((code, "no matching year-independent delivery state"))
    } else if columns.len() > 1 {
        let message = "pin one year-independent delivery column";
        Some(("representation_ambiguous", message))
    } else if value_sets.len() > 1 {
        let message = "year-independent column has conflicting value sets";
        Some(("representation_ambiguous", message))
    } else {
        None
    };
    if let Some((code, message)) = refusal {
        return Ok(blocked(code, message.into()));
    }
    Ok(Resolution {
        requested_period: "_default".into(),
        states,
        slices: Vec::new(),
        clip: None,
        finding: None,
    })
}
