//! `validate`: every issue of a `project_data.json` document. The supported-version decision and the
//! structural validator (`reg-core`) run first, without the catalog; the semantic
//! layer then resolves each source's variant (and its succession family, for a dated
//! period) and each binding in reference scope over
//! the compiled states ([`emitted`]), and on a steward artifact checks each binding
//! against the steward's holdings. An invalid project is a result (`ok: false`),
//! never an error.
//!
//! Availability is decided once, by [`resolve_binding`] (which the order materializer
//! shares): a binding
//! is requested wherever it is available inside the source's period, so availability
//! narrower than the request is no finding (the order's clip states it) and only
//! availability empty everywhere in it blocks. The one period finding is the reverse:
//! a source period narrower than what its columns deliver in the study window.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::project::{Binding, ProjectData, Source, StudyWindow, check};
use reg_core::{
    Fqid, Interval, IssueLevel, ValidationIssue, ValidationResult, fold_identity, gaps, intersect,
    merge, overlap, quote, quoted_list, render,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use super::family::{Part, VariantFamily, variant_family};
use super::order::held_days;
use super::refs;
use super::states::{Emitted, emitted, value_set};
use super::{Params, Server};
use crate::held::serves;
use crate::{Error, Scope};

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
            // Each catalog answers from its own holdings, so a document of another
            // steward is refused alone, as `order` refuses it: its source issues
            // would come from the wrong holdings.
            if project.steward == server.catalog.name() {
                for (i, source) in project.sources.iter().enumerate() {
                    semantic.source(&format!("/sources/{i}"), source, project.window.as_ref())?;
                }
            } else {
                semantic.push(
                    IssueLevel::Error,
                    "steward_mismatch",
                    "/steward".into(),
                    steward_mismatch(&project.steward, server.catalog.name()),
                );
            }
            ValidationResult {
                issues: semantic.issues,
            }
        }
    };
    Ok(serde_json::to_value(result).expect("ValidationResult serializes"))
}

/// The `project` parameter as a project, or the issues that reject it without the
/// catalog ([`check`]). `order` shares this door (today's `project_from_raw`).
pub(super) fn project(params: &Params) -> Result<Result<ProjectData, ValidationResult>, Error> {
    // The transports hand the body over as JSON text: HTTP after refusing malformed
    // bytes, MCP from its object argument.
    let raw: Value = serde_json::from_str(params["project"])
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| Error::invalid_parameter("project"))?;
    Ok(check(&raw))
}

/// The `steward_mismatch` message, `validate`'s issue and `order`'s refusal.
pub(super) fn steward_mismatch(wanted: &str, served: &str) -> String {
    let wanted = quote(wanted);
    format!(
        "this project's steward is {wanted} and this deployment serves the {} steward; \
         order the project from the deployment that serves {wanted}. Its steward is \
         provenance — no deployment rewrites it to match its own",
        quote(served)
    )
}

/// The representations the steward holds variable `id` in under `variant`, in
/// tables of a known scope: each spelling a mapping serves (`held::serves`).
pub(super) fn held_representations(
    conn: &Connection,
    id: i64,
    variant: i64,
) -> Result<BTreeSet<String>, Error> {
    Ok(conn
        .prepare_cached(&format!(
            "SELECT DISTINCT rc.delivery_column_name FROM resolver_column rc \
             JOIN holding_mapping hm ON hm.variable_id = rc.variable_id \
             AND hm.variant_id = rc.register_variant_id \
             JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) \
             WHERE ht.scope != 'unknown' AND hm.variable_id = ?1 AND hm.variant_id = ?2 \
             AND {}",
            serves("rc.delivery_column_name")
        ))?
        .query_map([id, variant], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

/// A resolved column of a binding: its family member, delivered spelling and
/// canonical spelling.
type Column = (i64, String, String);

/// What a binding is resolved against: the source's period (`None`:
/// year-independent) and the study window's days, when the project has a window.
#[derive(Clone, Copy)]
struct Availability<'a> {
    requested: Option<&'a [Interval]>,
    window: Option<&'a Interval>,
}

/// What a binding does inside the study window: the days a period widened into
/// them would order, and the days it would block on.
#[derive(Default)]
struct WindowReach {
    served: Vec<Interval>,
    blocking: Vec<Interval>,
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

    fn source(
        &mut self,
        base: &str,
        source: &Source,
        window: Option<&StudyWindow>,
    ) -> Result<(), Error> {
        let family = if self.register_variant(base, &source.register_variant)? {
            variant_family(self.conn, &source.register_variant)?
        } else {
            None
        };
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
        // A year-independent source has no days to compare with the window.
        let window = window
            .and_then(StudyWindow::interval)
            .filter(|_| requested.is_some());
        let (mut served, mut blocking) = (Vec::new(), Vec::new());
        for (i, binding) in source.bindings.iter().enumerate() {
            let base = format!("{base}/bindings/{i}");
            let at = Availability {
                requested: requested.as_deref(),
                window: window.as_ref(),
            };
            let reach = self.binding(&base, source, binding, family.as_ref(), at)?;
            served.extend(reach.served);
            blocking.extend(reach.blocking);
        }
        let (Some(window), Some(requested)) = (window, requested) else {
            return Ok(());
        };
        // A day counts only where widening would order it: some binding serves it
        // and none would block on it.
        let delivered = gaps(&merge(served), blocking);
        let missed = gaps(&delivered, requested.clone());
        if !missed.is_empty() {
            self.push(
                IssueLevel::Warning,
                "source_period_narrower_than_available",
                format!("{base}/period"),
                format!(
                    "source {} orders {}, narrower than its columns deliver in the study \
                     window {}: {} also delivered",
                    quote(&source.name),
                    render(&requested),
                    render(&[window]),
                    render(&missed)
                ),
            );
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

    /// Resolve one binding over its source's family and report its issues; what it
    /// does inside the study window, nothing without a window or a resolved variant.
    fn binding(
        &mut self,
        base: &str,
        source: &Source,
        binding: &Binding,
        family: Option<&VariantFamily>,
        at: Availability<'_>,
    ) -> Result<WindowReach, Error> {
        let Availability { requested, window } = at;
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
            return Ok(WindowReach::default());
        };
        let slugs = [provider, register, variable];
        // A direct hit only: a replaced variable's successor is the
        // `variable_replaced` hint, never a substitute.
        let Some(id) = refs::variable_id(self.conn, Scope::Reference, &slugs)? else {
            let message = format!(
                "column {} resolves to no variable in reg_meta",
                quote(&binding.variable)
            );
            self.push(IssueLevel::Error, "fqid_unresolved", path, message);
            self.value_set(base, binding)?;
            return Ok(WindowReach::default());
        };
        self.hints(&path, binding, id, &slugs, requested)?;
        // The structural validator matches the binding's register to the source's,
        // so the source's family is a family of the variable's register.
        let mut reach = WindowReach::default();
        if let Some(family) = family {
            let columns = self.period(&path, source, binding, id, family, requested)?;
            if self.steward {
                let members = match requested {
                    Some(requested) => family.split(requested).into_iter().map(|p| p.0).collect(),
                    None => vec![family.named],
                };
                self.admission(&path, source, binding, id, &members, columns.as_deref())?;
            }
            if let Some(window) = window {
                // simplify: a second resolution per binding on every validate with a
                // window; fold it into the period's resolution if validate latency on
                // the live catalog grows noticeably.
                reach = self.window_reach(source, binding, id, family, window)?;
            }
        }
        self.value_set(base, binding)?;
        Ok(reach)
    }

    /// The order's pass asked for the whole window: the days a period widened into
    /// them would serve (on a steward artifact, only the held ones) and the days it
    /// would block on (available but not held, or a finding such as co-existing
    /// representations).
    fn window_reach(
        &self,
        source: &Source,
        binding: &Binding,
        id: i64,
        family: &VariantFamily,
        window: &Interval,
    ) -> Result<WindowReach, Error> {
        let pin = binding.representation.as_deref();
        let window = std::slice::from_ref(window);
        let resolution = resolve_binding(
            self.conn,
            source,
            binding,
            pin,
            id,
            Some(family),
            Some(window),
        )?;
        let available: Vec<Interval> = resolution.slices.iter().map(Slice::days).collect();
        if let Some(finding) = &resolution.finding {
            // A finding that names no slices (an unresolved or unknown
            // representation) does not say which days it blocks: all of them may.
            // No availability at all blocks nothing; the order clips it away.
            let blocking = match finding.code {
                "binding_unavailable" => Vec::new(),
                _ if available.is_empty() => window.to_vec(),
                _ => available,
            };
            return Ok(WindowReach {
                served: Vec::new(),
                blocking,
            });
        }
        if !self.steward {
            return Ok(WindowReach {
                served: available,
                blocking: Vec::new(),
            });
        }
        let held = held_days(self.conn, id, &resolution.slices)?;
        Ok(WindowReach {
            blocking: gaps(&merge(available), held.clone()),
            served: held,
        })
    }

    /// The non-blocking hints: a deprecated variable, a sensitive one, and each
    /// successor whose succession is effective by the requested period's last year.
    fn hints(
        &mut self,
        path: &str,
        binding: &Binding,
        id: i64,
        slugs: &[String; 3],
        requested: Option<&[Interval]>,
    ) -> Result<(), Error> {
        let (deprecated, sensitive): (bool, bool) = self
            .conn
            .prepare_cached("SELECT deprecated, is_sensitive FROM variable WHERE variable_id = ?")?
            .query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))?;
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
        // Flagged, never blocked (#1547): the steward decides whether to share it.
        if sensitive {
            self.push(
                IssueLevel::Warning,
                "variable_sensitive",
                path.to_owned(),
                format!(
                    "column {} is a sensitive variable; it stays in the order, but the \
                     steward shares it with caution and may decline it",
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
    /// the binding's columns (member, delivered spelling and canonical) unless a
    /// finding blocks it.
    fn period(
        &mut self,
        path: &str,
        source: &Source,
        binding: &Binding,
        id: i64,
        family: &VariantFamily,
        requested: Option<&[Interval]>,
    ) -> Result<Option<Vec<Column>>, Error> {
        let pin = binding.representation.as_deref();
        let resolution =
            resolve_binding(self.conn, source, binding, pin, id, Some(family), requested)?;
        let variable = quote(&binding.variable);
        let at = &source.register_variant;
        let period = &resolution.requested_period;
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
        let mut columns: Vec<Column> = states
            .iter()
            .filter_map(|s| {
                let column = s.emitted.delivery_column_name.clone()?;
                let canonical = s.emitted.canonical_column.clone();
                let member = s.emitted.register_variant_id;
                Some((member, column.clone(), canonical.unwrap_or(column)))
            })
            .collect();
        columns.sort_unstable();
        columns.dedup();
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
        } else if drifts(states) > 1 {
            self.push(
                IssueLevel::Info,
                "binding_state_drifts_within_period",
                path.to_owned(),
                format!(
                    "column {variable} spans {} states across a transition within period \
                     {period}",
                    drifts(states)
                ),
            );
        }
        Ok(Some(columns))
    }

    /// On a steward artifact, warn when the steward holds the binding in no
    /// representation under any family member the period reaches (`members`), or a
    /// column it resolves to is not held under its own member. Unresolved `columns`
    /// (a blocked binding) skip the representation check.
    fn admission(
        &mut self,
        path: &str,
        source: &Source,
        binding: &Binding,
        id: i64,
        members: &[i64],
        columns: Option<&[Column]>,
    ) -> Result<(), Error> {
        let mut by_member = BTreeMap::new();
        for &member in members {
            by_member.insert(member, held_representations(self.conn, id, member)?);
        }
        let held: BTreeSet<&String> = by_member.values().flatten().collect();
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
        let mut missing: Vec<&String> = columns
            .into_iter()
            .flatten()
            .filter(|(member, _, canonical)| {
                by_member
                    .get(member)
                    .is_none_or(|held| !held.contains(canonical))
            })
            .map(|(_, column, _)| column)
            .collect();
        missing.sort_unstable();
        missing.dedup();
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
                listed(held.into_iter().collect())
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

/// How many states a binding spans for `binding_state_drifts_within_period`. `states`
/// is member-contiguous ([`reach`] walks the family split member by member), and each
/// state is compared with the one before it: within one member it always counts; the
/// first state of the next member counts only where its canonical column (by
/// `fold_identity`, as the order matches it) or value set differs from the previous
/// member's last state. A member boundary is a transition only where the
/// representation or coding changes there; a difference in any other fact (the data
/// type) across it is not counted.
fn drifts(states: &[Kept]) -> usize {
    let carried = |before: &Kept, s: &Kept| {
        before.emitted.register_variant_id != s.emitted.register_variant_id
            && before
                .emitted
                .canonical_column
                .as_deref()
                .map(fold_identity)
                == s.emitted.canonical_column.as_deref().map(fold_identity)
            && before.value_set == s.value_set
    };
    states
        .iter()
        .enumerate()
        .filter(|(i, s)| *i == 0 || !carried(&states[i - 1], s))
        .count()
}

/// A window of constant representation: its days, its delivery column and the
/// family member delivering it.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Slice {
    pub(super) lo: String,
    pub(super) hi: String,
    pub(super) column: String,
    pub(super) variant: i64,
}

impl Slice {
    pub(super) fn days(&self) -> Interval {
        (self.lo.clone(), self.hi.clone())
    }
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
    /// The available request partitioned into windows of constant representation
    /// and member, sorted; none when year-independent or blocked.
    pub(super) slices: Vec<Slice>,
    /// Where the binding is available, rendered, when that is less than the request.
    pub(super) clip: Option<String>,
    pub(super) finding: Option<Finding>,
}

/// Resolve `binding` (the variable `id`, over the source's variant `family`, none
/// when the source names no variant of the register) inside `requested` (`None`:
/// year-independent, at the named variant exactly), narrowed to `pin`, the
/// representation the binding pins. A dated request is resolved once over every
/// member's share ([`VariantFamily::split`]): one availability clip against the
/// whole request, and only a request no member serves blocks.
pub(super) fn resolve_binding(
    conn: &Connection,
    source: &Source,
    binding: &Binding,
    pin: Option<&str>,
    id: i64,
    family: Option<&VariantFamily>,
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
        return year_independent(conn, id, family.map(|f| f.named), pin, |code, message| {
            blocked(code, message, Some("_default".into()))
        });
    };
    let mut states = Vec::new();
    let mut by_column: BTreeMap<(String, i64), Vec<Interval>> = BTreeMap::new();
    // What the binding is delivered as somewhere the request reaches.
    let mut offered = BTreeSet::new();
    let parts = family.map(|f| f.split(requested)).unwrap_or_default();
    for (e, overlaps) in reach(conn, id, &parts)? {
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
            .entry((column, e.register_variant_id))
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
    let mut slices: Vec<Slice> = by_column
        .iter()
        .flat_map(|((column, variant), intervals)| {
            merge(intervals.clone())
                .into_iter()
                .map(move |(lo, hi)| Slice {
                    lo,
                    hi,
                    column: column.clone(),
                    variant: *variant,
                })
        })
        .collect();
    slices.sort_unstable();
    let availability = merge(slices.iter().map(Slice::days).collect());
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
fn coexisting(slices: &[Slice]) -> BTreeSet<&str> {
    let mut coexisting = BTreeSet::new();
    for (i, a) in slices.iter().enumerate() {
        for b in &slices[i + 1..] {
            if a.column != b.column && a.lo <= b.hi && b.lo <= a.hi {
                coexisting.extend([a.column.as_str(), b.column.as_str()]);
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
    parts: &[Part],
) -> Result<Vec<(Emitted, Vec<Interval>)>, Error> {
    let mut reached: Vec<(Emitted, Vec<Interval>)> = Vec::new();
    for (variant, segment) in parts
        .iter()
        .flat_map(|(variant, days)| days.iter().map(move |d| (*variant, d)))
    {
        let bounds = (segment.0.as_str(), segment.1.as_str());
        for e in emitted(conn, Scope::Reference, id, Some(variant), Some(bounds))? {
            let window = (
                e.valid_from.clone().expect("a dated state has bounds"),
                e.valid_to.clone().expect("a dated state has bounds"),
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
