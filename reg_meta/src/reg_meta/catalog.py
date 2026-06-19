"""Catalog: FQID-to-row resolution against the reg_meta SQLite DB.

Implements the FQID API (see DESIGN.md → Catalog API surface): ``Catalog.resolve(fqid)``
turns any FQID kind into a typed entity row (the 3-segment binding arm resolves
to the longitudinal ``ResolvedVariable``), with ``resolve_at`` + the per-edge
accessors for period/relationship traversal.

A2.6: the binding FQID is 3-segment (`provider/register/slug`). Variant and
period are delivery coordinates passed to ``resolve_at`` (not FQID segments),
and the variant / register_version FQID kinds — plus the ``editions`` discovery
path that enumerated per-edition bindings — are gone (see DESIGN.md → FQID grammar).
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypeVar

from .db import db_path_from_args, open_db
from .errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from .fqid import (
    DEFAULT_VARIANT_SLUG,
    Fqid,
    FqidError,
    FqidKind,
    parse,
    period_token_to_bounds,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable
    from pathlib import Path

# Succession-chain tuple grain (variable triple, register pair, or classification
# 1-tuple); shared so `_walk_terminal` preserves arity across grains (see
# `resolve_terminal_successor`).
_SuccTuple = TypeVar("_SuccTuple", tuple[str, str, str], tuple[str, str], tuple[str])


@dataclass(frozen=True)
class ResolvedProvider:
    fqid: Fqid
    provider_id: int
    name: str


@dataclass(frozen=True)
class ResolvedRegister:
    fqid: Fqid
    register_id: int
    provider_id: int
    # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): `name` was `registernamn`, `purpose` was `registersyfte`.
    # `registerrubrik` is dropped (redundant with name).
    name: str
    purpose: str | None


@dataclass(frozen=True)
class ResolvedClassification:
    fqid: Fqid
    classification_id: int
    short_name: str
    name: str
    # OUTBOUND succession (#571): the editions that replaced this one
    # (`classification_replaced_by`, keyed on this edition's slug). Empty for a
    # terminal (current) edition.
    replaced_by: tuple[ClassificationRef, ...] = ()
    via_same_as: tuple[Fqid, ...] | None = None


# Catalog-tree children shapes (A5.1b-i; see DESIGN.md → Catalog API surface): thin child nodes for the webapp's
# catalog browse (`/api/catalog` → providers → registers → bindings). Each carries
# the addressable `Fqid` (webapp serializes `str(fqid)`) + a display `name`,
# mirroring the `Resolved*` style but without the per-entity detail those carry.
@dataclass(frozen=True)
class ProviderSummary:
    fqid: Fqid
    name: str


@dataclass(frozen=True)
class RegisterSummary:
    fqid: Fqid
    name: str
    purpose: str | None


@dataclass(frozen=True)
class BindingSummary:
    fqid: Fqid
    name: str | None


# The open-ended `variable_state.valid_to` sentinel (the reg_meta_build DDL
# default). A window ending here is "ongoing" — it has no finite upper bound.
OPEN_ENDED_VALID_TO = "9999-12-31"


@dataclass(frozen=True)
class VariableCoverage:
    """Coverage aggregate for one variable over its `variable_state` windows
    (#351): the study-window signal a browse row needs without resolving every
    state. `coverage_from` is the earliest `valid_from`; `coverage_to` the latest
    FINITE `valid_to` (None when the latest window is open-ended — see
    `open_ended` — or when the variable has no states); `state_count` > 1 inside a
    window signals a break worth surfacing. A variable with no states has
    `state_count == 0` and both bounds None (distinct from open-ended)."""

    coverage_from: str | None
    coverage_to: str | None
    open_ended: bool
    state_count: int


@dataclass(frozen=True)
class RegisterCoverage:
    """Coverage aggregate for one register (#351): `variable_count` is its
    slugged (browsable) variables; the span is over ALL their states.
    `coverage_to`/`open_ended` follow `VariableCoverage`."""

    variable_count: int
    coverage_from: str | None
    coverage_to: str | None
    open_ended: bool


def _coverage_bounds(
    cov_from: str | None, cov_to: str | None
) -> tuple[str | None, str | None, bool]:
    """Map a `(MIN(valid_from), MAX(valid_to))` SQL aggregate to
    `(coverage_from, coverage_to, open_ended)`. The open-ended sentinel as the
    max means "ongoing": `coverage_to` is None and `open_ended` True. NULLs (a
    stateless variable / empty register) yield both bounds None and `open_ended`
    False."""
    open_ended = cov_to == OPEN_ENDED_VALID_TO
    return cov_from, (None if open_ended else cov_to), open_ended


# Concept-group browse shapes (#303; see DESIGN.md → Concept groups): a derived
# PRESENTATION-ONLY grouping of near-identical browse rows (split-sibling edge
# components, month-suffixed families, curated facet families). A group is NOT an
# FQID-addressable entity — members carry the real leaf FQIDs; the group is a
# fold-and-pick affordance for browse surfaces. Classification VINTAGE editions
# (lkf1980…lkf2026, ssyk1996→ssyk2012) are NOT folded here — they surface as
# succession edges in `classification_replaced_by` (#571).
@dataclass(frozen=True)
class GroupFacet:
    """One facet assignment on a group member: `axis` names the dimension
    ('month' / 'rank' / 'vintage'), `value` sorts (zero-padded where needed),
    `label` displays."""

    axis: str
    value: str
    label: str


@dataclass(frozen=True)
class ConceptGroupMember:
    """A group member: the leaf's FQID (binding or classification), its
    display name, and its facet assignments (empty on edge-group members)."""

    fqid: Fqid
    name: str | None
    facets: tuple[GroupFacet, ...]


@dataclass(frozen=True)
class ConceptGroupSummary:
    """One derived concept group. `key` is the scope-unique derivation key
    (slug stem / min member slug / curated key) — a stable anchor for UI
    state, not an FQID. `axes` are the sorted distinct facet axes the members
    carry (empty for edge groups); members are ordered by their facet values
    along `axes`, then slug."""

    key: str
    label: str
    source: str  # 'edge' | 'token' | 'curated'
    axes: tuple[str, ...]
    members: tuple[ConceptGroupMember, ...]


@dataclass(frozen=True)
class TagSummary:
    """One curated thematic tag (#311) in the global vocabulary. `slug` is the
    globally-unique tag id; `member_count` / `starred_count` are this tag's total
    members and the subset flagged golden/recommended (across both grains)."""

    slug: str
    label: str
    description: str | None
    member_count: int
    starred_count: int


@dataclass(frozen=True)
class TagMembership:
    """A tag a register/variable belongs to (#311), from its side: the tag's
    `slug`/`label`, plus THIS membership's `rank` (curated order within the tag),
    `starred` (golden/recommended flag) and one-line `note` rationale."""

    slug: str
    label: str
    rank: int
    starred: bool
    note: str | None


def _tag_membership(row: sqlite3.Row) -> TagMembership:
    """Build a `TagMembership` from a `(slug, label, rank, starred, note)` row.
    `starred` is stored as INTEGER 0/1 — coerce to bool."""
    return TagMembership(
        slug=row["slug"],
        label=row["label"],
        rank=row["rank"],
        starred=bool(row["starred"]),
        note=row["note"],
    )


# A2.5b variant-browser shape (see reg_webapp/DESIGN.md → Catalog router structure): a variant is a register sub-resource, NOT
# an FQID-addressable node (the variant left the binding FQID; see DESIGN.md → Two-level variable model), so this
# carries the variant `slug` (the `?variant=` browse coordinate) + display fields,
# not an `Fqid`. A4.4c adds the panel-shape columns (read-only; see reg_webapp/DESIGN.md → Catalog router structure): a
# `panel_entity_key` that is a bare variable-slug str or a tuple of slugs
# (composite), the `panel_time_key` ("period", a variable-slug, or a tuple of
# slugs (composite)), and the `panel_time_grain` ('delivery'/'row'). Most
# variants carry no panel data → all three are None.
@dataclass(frozen=True)
class VariantSummary:
    slug: str
    name: str | None
    description: str | None
    display_group: str | None
    panel_entity_key: str | tuple[str, ...] | None
    panel_time_key: str | tuple[str, ...] | None
    panel_time_grain: str | None


def _decode_panel_entity_key(raw: str | None) -> str | tuple[str, ...] | None:
    """Decode a stored panel key (A4.4c): a JSON-array string → tuple
    (composite key), any other string → itself (simple bare slug / "period"),
    NULL → None. Mirrors the `populate_slugs` writer (json.dumps for the tuple
    case). Generic — reused for both `panel_entity_key` and `panel_time_key`."""
    if raw is None:
        return None
    if raw.startswith("["):
        return tuple(json.loads(raw))
    return raw


# The polymorphic period a caller passes to `resolve_at` (see DESIGN.md → Catalog API surface; see reg_schema/DESIGN.md → Two layers: models vs. validator). Mirrors
# `Source.period`: a bare year (int), a period token ("HT2020"/"2020-Q3"/
# "2020-08"/"2018-12-31"), an explicit range dict {"from", "to"} (endpoints are
# int or token), or the "_default" snapshot sentinel (no period filter). It is a
# delivery coordinate, NOT an FQID segment (the binding FQID is 3-seg; see DESIGN.md → FQID grammar).
Period = int | str | dict


@dataclass(frozen=True)
class VariableState:
    """One `variable_state` row (see DESIGN.md → Two-level variable model) — a variable's per-delivery shape, tagged
    with the **variant coordinate** it was delivered in. The longitudinal
    `ResolvedVariable.states` is a tuple of these; `resolve_at` returns the
    subset whose validity range intersects the queried period."""

    state_id: int
    # `register_variant.slug` for `register_variant_id`. The two-level model (see DESIGN.md → Two-level variable model) makes the column
    # NOT NULL, so a state always carries a real variant — `variant` is always a
    # resolved slug, never the synth `_default` placeholder (that fiction exists
    # only at variant-slot resolve time, not on a stored state).
    variant: str
    register_variant_id: int
    valid_from: str  # ISO 8601 'YYYY-MM-DD', inclusive
    valid_to: str  # ISO 8601 'YYYY-MM-DD', inclusive ('9999-12-31' open-ended)
    data_type: str | None
    data_length: str | None
    # Denormalized latest alias for the state (see DESIGN.md → Two-level variable model); full alias history lives in
    # `variable_alias`.
    delivery_column_name: str | None
    # Overlap discriminator (see reg_meta_build/DESIGN.md → Build-time triage (SCB); multi-vintage / grain / coding). NOT NULL
    # DEFAULT '' in the DDL, so '' means "no discriminator", not absent.
    value_set_version_label: str
    value_set_id: int | None
    # (code, label) pairs for `value_set_id`, hydrated eagerly when non-NULL.
    # None when the state carries no value set. Eager (frozen dataclass favors
    # it); typical per-state code fan-out is small.
    value_set: tuple[tuple[str, str], ...] | None
    # Variable-grain `variable.is_identifier` denormalized onto every state via a
    # JOIN (constant across a variable's states), so consumers with no
    # ResolvedVariable in scope (the `resolve_at` / `/states` paths) can still
    # read the authoritative identifier flag.
    is_identifier: bool
    # Classification family slug (see DESIGN.md → Classifications) for this state's value set (e.g. 'lkf2007'),
    # resolved per-state from `variable_state.classification_id` — it varies
    # across a variable's states. None for code-less / unclassified states.
    classification_slug: str | None


@dataclass(frozen=True)
class VariableRef:
    """A variable-grain edge endpoint (see DESIGN.md → Composite registers and source tracking): the 3-part `(provider, register,
    variable)` identity of a `same_as` / `replaced_by` neighbor. Carried by
    `predecessors`/`successors` and `ResolvedVariable.same_as`/`.replaced_by`.

    A2.6: `fqid` is the neighbor's 3-segment binding FQID — the edge triple IS
    the binding FQID now that the variant/period left the grammar (see DESIGN.md → FQID grammar and Composite registers and source tracking).
    Build-time slug validation guarantees the triple round-trips, so this is
    never None in practice.
    """

    fqid: Fqid | None
    provider: str
    register: str
    variable: str
    # #142: succession refs (predecessors/successors) carry the human transition
    # reason (`timeseries_event.beskrivning`) and the AktuellVariabel-grain
    # `effective_year` (the successor edition's year). Both None on `same_as`
    # refs and on bare-Variabel/Register/RegisterVariant-grain succession (no
    # edition → no year; documented asymmetry, reg_meta_build/db.py).
    reason: str | None = None
    effective_year: int | None = None


@dataclass(frozen=True)
class ClassificationRef:
    """A classification-grain succession edge endpoint (#571): one
    `classification_replaced_by` neighbor of a classification edition. Carried by
    `classification_predecessors`/`classification_successors` and
    `ResolvedClassification.replaced_by`.

    The classification FQID is 2-segment (`class/<slug>`), so the edge endpoint is
    a single slug — no provider/register triple (unlike `VariableRef`). There is
    no `reason`/`beskrivning` column on `classification_replaced_by`: `note`
    carries the build provenance instead (e.g. `derived:vintage_chain`).
    Succession references the EXACT edition slug, so `slug` is the load-bearing
    identity; `fqid` is best-effort (None on a malformed slug, mirroring
    `_ref_fqid`) — succession tolerates dead predecessor editions by design."""

    fqid: Fqid | None
    slug: str
    effective_year: int | None = None
    note: str | None = None


@dataclass(frozen=True)
class ClassificationEdition:
    """One edition in a classification succession chain (#571), as returned by
    `Catalog.classification_chain`. Unlike `ClassificationRef` (a single edge
    endpoint), this is a fully-hydrated node in the WHOLE chain — the webapp
    browse panel renders the complete edition timeline from a list of these.

    `slug` is the load-bearing identity (succession references the exact edition
    slug). `fqid`/`name` are None for a DEAD edition — a slug that appears in
    `classification_replaced_by` but has no live `classification` row (a renamed
    or retired vintage); the webapp renders a dead edition as plain text, not a
    link. `effective_year` is the year on the `classification_replaced_by` edge
    that names this edition as `successor_slug` (None for the terminal, which is
    no edge's successor). `is_current` marks the terminal (head) edition — the one
    with no outbound successor; `is_self` marks the edition the caller queried
    (resolved to its canonical live slug when the query was a `same_as` alias)."""

    slug: str
    fqid: Fqid | None
    name: str | None
    effective_year: int | None
    is_current: bool
    is_self: bool


@dataclass(frozen=True)
class RelatedRef:
    """A variable-grain sibling edge (split; see reg_meta_build/DESIGN.md → Build-time triage (SCB)): `variable_related_to`. Same
    3-part identity as `VariableRef` plus the `relation_kind` (auto-derived split
    reason: `code_vs_label_pair`, `import_bug_suspect`, or the generic
    `same_definition_different_column`; full taxonomy in reg_meta_build/DESIGN.md).
    `fqid` is the sibling's 3-segment binding FQID (A2.6)."""

    fqid: Fqid | None
    provider: str
    register: str
    variable: str
    relation_kind: str


@dataclass(frozen=True)
class LineageEdge:
    """Consumer-side lineage at STATE grain (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)): one `variable_state_lineage`
    row tying a consumer state to a source state over their validity
    intersection. `source_fqid` is the source state's 3-part binding FQID,
    best-effort (None when the source's slugs aren't populated)."""

    consumer_state_id: int
    source_state_id: int
    valid_from: str  # intersection start (ISO 8601 'YYYY-MM-DD')
    valid_to: str  # intersection end (ISO 8601 'YYYY-MM-DD')
    source_fqid: Fqid | None = None


@dataclass(frozen=True)
class LineageWarning:
    """Build-time lineage warning for a consumer state (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)):
    `variable_state_lineage_warning`. `warning_kind` is 'no_source_state' or
    'ambiguous_source_variant'."""

    consumer_state_id: int
    warning_kind: str
    message: str


@dataclass(frozen=True)
class ResolvedVariable:
    """Longitudinal resolution of a binding FQID (see DESIGN.md → Catalog API surface): the addressable
    variable's shared metadata + its full `variable_state` history (each state
    tagged with its variant) + variable-grain edges."""

    # The caller's 3-segment binding FQID, preserved through a `same_as`
    # traversal (so a result reports the FQID the caller asked for).
    fqid: Fqid
    variable_id: int
    register_id: int
    provider_key: str
    name: str | None
    definition: str | None
    description: str | None
    measurement_unit: str | None
    is_sensitive: bool
    is_identifier: bool
    source_register_id: int | None
    source_register_text: str | None
    # Full state history, chronological ascending (oldest first). Each state
    # carries its variant coordinate + period range.
    states: tuple[VariableState, ...]
    same_as: tuple[VariableRef, ...]  # variable_same_as (equivalence)
    replaced_by: tuple[
        VariableRef, ...
    ]  # OUTBOUND successors (see DESIGN.md → Catalog API surface)
    related_to: tuple[RelatedRef, ...]  # variable_related_to (split siblings)
    lineage: tuple[LineageEdge, ...]  # variable_state_lineage (consumer-side)
    # Traversal path (3-segment binding FQIDs) when resolved via `same_as`; None
    # on a direct hit.
    via_same_as: tuple[Fqid, ...] | None = None


ResolvedEntity = (
    ResolvedProvider | ResolvedRegister | ResolvedVariable | ResolvedClassification
)


def _not_found(fqid: Fqid) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="fqid_not_found",
        error_class="query",
        message=f"FQID does not resolve to any row: {fqid!s}",
        remediation="Use `reg-meta search` to locate entities by name or ID.",
    )


# Distinguishes "variant slug names no variant under this register" (→ empty
# resolve_at result) from the `_default` slug (→ None register_variant_id, which
# matches no real state — also correct). `None` alone can't carry both meanings.
class _Missing:
    """Sentinel type for `_resolve_variant_id`. A distinct class rather than a
    bare `object()` so `ty` narrows `int | _Missing` to `int` past the guard
    (no `# type: ignore` needed at the call site)."""


_MISSING = _Missing()


def _bad_period(period: Period, detail: str) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_USAGE,
        code="invalid_period",
        error_class="query",
        message=f"invalid period {period!r}: {detail}",
        remediation=(
            "Use an int year, a period token (HT2020 / 2020-Q3 / 2020-08 / "
            "2018-12-31), a range {'from': ..., 'to': ...}, or '_default'."
        ),
    )


def _endpoint_bounds(value: int | str) -> tuple[str, str]:
    """ISO `(lo, hi)` for a single range endpoint (int year or period token)."""
    if isinstance(value, bool):  # bool is an int subclass — reject explicitly
        raise _bad_period(value, "range endpoint must be a year or period token")
    if isinstance(value, int):
        return f"{value:04d}-01-01", f"{value:04d}-12-31"
    if isinstance(value, str):
        try:
            return period_token_to_bounds(value)
        except FqidError as exc:
            raise _bad_period(value, str(exc)) from exc
    raise _bad_period(value, "range endpoint must be a year or period token")


def _period_bounds(period: Period) -> tuple[str, str] | None:
    """Expand a `Period` (see reg_schema/DESIGN.md → Two layers: models vs. validator) to an inclusive ISO `(lo, hi)` interval, or None
    for the `_default` sentinel (no period filter). Fail-fast on malformed
    input (`invalid_period`, EXIT_USAGE)."""
    if isinstance(period, bool):  # bool is an int subclass — reject before int
        raise _bad_period(period, "expected int year, token, range, or '_default'")
    if isinstance(period, int):
        return f"{period:04d}-01-01", f"{period:04d}-12-31"
    if isinstance(period, str):
        if period == DEFAULT_VARIANT_SLUG:  # "_default" snapshot sentinel
            return None
        try:
            return period_token_to_bounds(period)
        except FqidError as exc:
            raise _bad_period(period, str(exc)) from exc
    if isinstance(period, dict):
        if set(period) != {"from", "to"}:
            raise _bad_period(period, "range must have exactly 'from' and 'to' keys")
        lo, _ = _endpoint_bounds(period["from"])
        _, hi = _endpoint_bounds(period["to"])
        if lo > hi:
            raise _bad_period(period, "range 'from' is after 'to'")
        return lo, hi
    raise _bad_period(period, "expected int year, token, range, or '_default'")


class Catalog:
    """FQID resolution against an open reg_meta SQLite connection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        # Source-side keys present in `variable_same_as` / `classification_same_as`.
        # Loaded lazily on first miss; same_as graphs are curator-curated and
        # tiny (tens of entries), and the common case is "no edge for this
        # tuple", so a cached frozenset short-circuits BFS without an SQL round
        # trip every miss. Catalog treats the DB as immutable for its lifetime.
        self._var_same_as_sources: frozenset[tuple[str, str, str]] | None = None
        self._class_same_as_sources: frozenset[tuple[str, str]] | None = None

    @classmethod
    def open(cls, db_arg: str | Path | None = None) -> Catalog:
        path = db_path_from_args(str(db_arg) if db_arg is not None else None)
        return cls(open_db(path))

    def close(self) -> None:
        self._conn.close()

    def resolve(self, fqid: str | Fqid) -> ResolvedEntity:
        if isinstance(fqid, str):
            parsed = parse(fqid)
        else:
            # Round-trip-validate so a hand-constructed Fqid with missing
            # required fields fails fast with FqidError instead of TypeError
            # inside a resolver.
            parsed = parse(str(fqid))
            if parsed.kind is not fqid.kind:
                raise FqidError(
                    f"Fqid(kind={fqid.kind.value}) is incomplete; "
                    f"emit-then-parse yields kind={parsed.kind.value}"
                )
        return _DISPATCH[parsed.kind](self, parsed)

    # ── A5.1b-i catalog-tree children enumeration (see DESIGN.md → Catalog API surface) ──
    # The webapp browse consumes these for the `/api/catalog` node tree. Each
    # returns a thin Summary list, slug-ordered (deterministic, matches the
    # FQID-leaf the webapp links on). An unknown parent slug returns an empty
    # list — not an error: a genuinely-absent node maps to 404 via `resolve()`,
    # and a present parent with no children maps to an empty children list.

    def list_providers(self) -> list[ProviderSummary]:
        """Every provider in the catalog (e.g. scb, sos), ordered by slug."""
        rows = self._conn.execute(
            "SELECT slug, name FROM provider ORDER BY slug"
        ).fetchall()
        return [
            ProviderSummary(fqid=Fqid.provider_fqid(r["slug"]), name=r["name"])
            for r in rows
        ]

    def list_registers(self, provider_slug: str) -> list[RegisterSummary]:
        """Registers under a provider, ordered by slug. A register with a NULL
        slug isn't addressable by a register FQID, so it's excluded (symmetric
        with `list_bindings`'s variable-slug filter; `register.slug` is nullable
        like `variable.slug`, both filled by the build's slug derivation). Empty
        when the provider slug names no provider OR the provider has no slugged
        registers (both are an empty children list to the webapp)."""
        rows = self._conn.execute(
            "SELECT r.slug, r.name, r.purpose "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug IS NOT NULL ORDER BY r.slug",
            (provider_slug,),
        ).fetchall()
        return [
            RegisterSummary(
                fqid=Fqid.register_fqid(provider_slug, r["slug"]),
                name=r["name"],
                purpose=r["purpose"],
            )
            for r in rows
        ]

    def list_bindings(
        self, provider_slug: str, register_slug: str
    ) -> list[BindingSummary]:
        """A register's bindings — its register-unique variable slugs — ordered
        by slug. A variable with a NULL slug isn't addressable by a binding FQID,
        so it's excluded (the FQID leaf is the browse key). Empty when the
        (provider, register) pair names no register OR it has no slugged
        variables."""
        rows = self._conn.execute(
            "SELECT v.slug, v.name "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL "
            "ORDER BY v.slug",
            (provider_slug, register_slug),
        ).fetchall()
        return [
            BindingSummary(
                fqid=Fqid.binding_fqid(provider_slug, register_slug, r["slug"]),
                name=r["name"],
            )
            for r in rows
        ]

    def register_variable_coverage(
        self, provider_slug: str, register_slug: str
    ) -> dict[str, VariableCoverage]:
        """Per-variable coverage for a register's bindings (#351), keyed by
        variable slug (the binding-FQID leaf, so the webapp zips it onto each
        `list_bindings` child). One GROUP BY over `variable_state`; a LEFT JOIN
        keeps stateless variables (coverage None, count 0). Measured ~9 ms on the
        worst real register (scb/ulf, 7.3k variables) — query-time, no
        materialized columns (see reg_webapp/DESIGN.md → Coverage aggregates)."""
        rows = self._conn.execute(
            "SELECT v.slug AS slug, MIN(vs.valid_from) AS cov_from, "
            "MAX(vs.valid_to) AS cov_to, COUNT(vs.state_id) AS nstates "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL "
            "GROUP BY v.variable_id",
            (provider_slug, register_slug),
        ).fetchall()
        out: dict[str, VariableCoverage] = {}
        for r in rows:
            cov_from, cov_to, open_ended = _coverage_bounds(r["cov_from"], r["cov_to"])
            out[r["slug"]] = VariableCoverage(
                coverage_from=cov_from,
                coverage_to=cov_to,
                open_ended=open_ended,
                state_count=r["nstates"],
            )
        return out

    def provider_register_coverage(
        self, provider_slug: str
    ) -> dict[str, RegisterCoverage]:
        """Per-register coverage for a provider's registers (#351), keyed by
        register slug. `variable_count` counts slugged variables (matching
        `list_bindings`); the span is over all their states. One GROUP BY;
        ~40 ms across scb's 238 registers (query-time — see DESIGN.md)."""
        rows = self._conn.execute(
            "SELECT r.slug AS slug, COUNT(DISTINCT v.variable_id) AS nvar, "
            "MIN(vs.valid_from) AS cov_from, MAX(vs.valid_to) AS cov_to "
            "FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "LEFT JOIN variable v "
            "ON v.register_id = r.register_id AND v.slug IS NOT NULL "
            "LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE p.slug = ? AND r.slug IS NOT NULL "
            "GROUP BY r.register_id",
            (provider_slug,),
        ).fetchall()
        out: dict[str, RegisterCoverage] = {}
        for r in rows:
            cov_from, cov_to, open_ended = _coverage_bounds(r["cov_from"], r["cov_to"])
            out[r["slug"]] = RegisterCoverage(
                variable_count=r["nvar"],
                coverage_from=cov_from,
                coverage_to=cov_to,
                open_ended=open_ended,
            )
        return out

    def list_variants(
        self, provider_slug: str, register_slug: str
    ) -> list[VariantSummary]:
        """A register's variants — the `register_variant` sub-resource (the
        `?variant=` browse axis; see reg_webapp/DESIGN.md → Catalog router structure) — ordered by slug. A variant with a NULL
        slug isn't browse-addressable, so it's excluded (symmetric with
        `list_bindings`). Empty when the (provider, register) pair names no
        register OR it has no slugged variants. (`_default` is a real variant
        slug — the synthesized variant for LSS/BU/SOL etc.; see DESIGN.md → Two-level variable model — so it is
        returned, not filtered.)"""
        rows = self._conn.execute(
            "SELECT rv.slug, rv.name, rv.description, rv.display_group, "
            "rv.panel_entity_key, rv.panel_time_key, rv.panel_time_grain "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug IS NOT NULL "
            "ORDER BY rv.slug",
            (provider_slug, register_slug),
        ).fetchall()
        return [
            VariantSummary(
                slug=r["slug"],
                name=r["name"],
                description=r["description"],
                display_group=r["display_group"],
                panel_entity_key=_decode_panel_entity_key(r["panel_entity_key"]),
                # `_decode_panel_entity_key` is the generic stored-key decoder —
                # reused for the (now-composite-capable) time key too.
                panel_time_key=_decode_panel_entity_key(r["panel_time_key"]),
                panel_time_grain=r["panel_time_grain"],
            )
            for r in rows
        ]

    def list_concept_groups(
        self, provider_slug: str, register_slug: str
    ) -> list[ConceptGroupSummary]:
        """Derived concept groups for a register (#303; see DESIGN.md →
        Concept groups), ordered by group key. Presentation-only: members
        carry the real binding FQIDs; browse surfaces collapse member rows
        under the group and expand to a facet picker. Empty when the
        (provider, register) pair names no register OR it has no groups."""
        rows = self._conn.execute(
            "SELECT g.group_id, g.group_key, g.label AS group_label, g.source, "
            "v.slug AS variable_slug, v.name AS variable_name, "
            "f.axis, f.value, f.label AS facet_label "
            "FROM concept_group g "
            "JOIN register r ON g.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN concept_group_variable m ON m.group_id = g.group_id "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "LEFT JOIN concept_group_variable_facet f "
            "  ON f.variable_id = m.variable_id "
            "WHERE p.slug = ? AND r.slug = ? AND g.kind = 'variable' "
            "  AND v.slug IS NOT NULL "
            "ORDER BY g.group_key, v.slug, f.axis",
            (provider_slug, register_slug),
        ).fetchall()
        # group_id → (key, label, source, {slug: (name, [facet, ...])})
        acc: dict[int, tuple[str, str, str, dict[str, tuple[str | None, list]]]] = {}
        for r in rows:
            _, _, _, members = acc.setdefault(
                r["group_id"],
                (r["group_key"], r["group_label"], r["source"], {}),
            )
            _, facets = members.setdefault(r["variable_slug"], (r["variable_name"], []))
            if r["axis"] is not None:
                facets.append(GroupFacet(r["axis"], r["value"], r["facet_label"]))
        out: list[ConceptGroupSummary] = []
        for key, label, source, members in acc.values():
            axes = tuple(sorted({f.axis for _, (_, fs) in members.items() for f in fs}))

            def member_sort(item: tuple[str, tuple[str | None, list]]) -> tuple:
                slug, (_, facets) = item
                by_axis = {f.axis: f.value for f in facets}
                return (*(by_axis.get(a, "") for a in axes), slug)  # noqa: B023

            out.append(
                ConceptGroupSummary(
                    key=key,
                    label=label,
                    source=source,
                    axes=axes,
                    members=tuple(
                        ConceptGroupMember(
                            fqid=Fqid.binding_fqid(provider_slug, register_slug, slug),
                            name=name,
                            facets=tuple(facets),
                        )
                        for slug, (name, facets) in sorted(
                            members.items(), key=member_sort
                        )
                    ),
                )
            )
        out.sort(key=lambda g: g.key)
        return out

    def list_classification_groups(self) -> list[ConceptGroupSummary]:
        """Curated classification umbrella groups (see DESIGN.md → Concept
        groups), ordered by group key. `concept_group_classification` holds
        only CURATED umbrella entries — e.g. `group:sun`, which groups the
        three genuinely-distinct SUN dimensions (`sun-niva2020`,
        `sun-inriktning2020`, `sun-grupp2020`) on a `dimension` axis (#516).
        Derived classification VINTAGE editions (lkf1980…lkf2026,
        ssyk1996→ssyk2012, sun-niva2000→sun-niva2020) are NOT here; they
        appear as succession edges in `classification_replaced_by` (#571).
        Members carry the real `class/<slug>` FQIDs. The group's single facet
        axis is read from `concept_group.facet_axis` — every member shares it."""
        rows = self._conn.execute(
            "SELECT g.group_id, g.group_key, g.label AS group_label, g.source, "
            "g.facet_axis AS axis, "
            "c.slug AS cls_slug, c.name AS cls_name, m.facet_value, m.facet_label "
            "FROM concept_group g "
            "JOIN concept_group_classification m ON m.group_id = g.group_id "
            "JOIN classification c ON c.id = m.classification_id "
            "WHERE g.kind = 'classification' AND c.slug IS NOT NULL "
            "ORDER BY g.group_key, m.facet_value, c.slug"
        ).fetchall()
        acc2: dict[int, tuple[str, str, str, str, list[ConceptGroupMember]]] = {}
        for r in rows:
            _, _, _, axis, members = acc2.setdefault(
                r["group_id"],
                (r["group_key"], r["group_label"], r["source"], r["axis"], []),
            )
            members.append(
                ConceptGroupMember(
                    fqid=Fqid.classification_fqid(r["cls_slug"]),
                    name=r["cls_name"],
                    facets=(GroupFacet(axis, r["facet_value"], r["facet_label"]),),
                )
            )
        return [
            ConceptGroupSummary(
                key=key,
                label=label,
                source=source,
                axes=(axis,),
                members=tuple(members),
            )
            for key, label, source, axis, members in sorted(
                acc2.values(), key=lambda g: g[0]
            )
        ]

    def list_tags(self) -> list[TagSummary]:
        """The curated thematic tag vocabulary (#311) with per-tag member counts,
        ordered by slug. `member_count` spans both grains; `starred_count` is the
        golden/recommended subset. Empty when no tags are curated (the machinery-
        only ship state)."""
        rows = self._conn.execute(
            "SELECT t.slug, t.label, t.description, "
            "COUNT(tm.tag_id) AS member_count, "
            "COALESCE(SUM(tm.starred), 0) AS starred_count "
            "FROM tag t "
            "LEFT JOIN tag_member tm ON tm.tag_id = t.tag_id "
            "GROUP BY t.tag_id "
            "ORDER BY t.slug"
        ).fetchall()
        return [
            TagSummary(
                slug=r["slug"],
                label=r["label"],
                description=r["description"],
                member_count=r["member_count"],
                starred_count=r["starred_count"],
            )
            for r in rows
        ]

    def tags_for_variable(self, fqid: Fqid) -> list[TagMembership]:
        """Tags the variable at `fqid` (a 3-seg binding FQID) belongs to (#311),
        ordered by tag rank then slug. Each carries this membership's
        rank/starred/note. Empty when the variable resolves to no tags (or the
        FQID names no variable)."""
        rows = self._conn.execute(
            "SELECT t.slug, t.label, tm.rank, tm.starred, tm.note "
            "FROM tag_member tm "
            "JOIN tag t ON t.tag_id = tm.tag_id "
            "JOIN variable v ON v.variable_id = tm.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ? "
            "ORDER BY tm.rank, t.slug",
            (fqid.provider, fqid.register, fqid.variable),
        ).fetchall()
        return [_tag_membership(r) for r in rows]

    def tags_for_register(self, fqid: Fqid) -> list[TagMembership]:
        """Tags the register at `fqid` (a 2-seg `provider/register` FQID) belongs
        to (#311), ordered by tag rank then slug. Empty when none (or the FQID
        names no register)."""
        rows = self._conn.execute(
            "SELECT t.slug, t.label, tm.rank, tm.starred, tm.note "
            "FROM tag_member tm "
            "JOIN tag t ON t.tag_id = tm.tag_id "
            "JOIN register r ON r.register_id = tm.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? "
            "ORDER BY tm.rank, t.slug",
            (fqid.provider, fqid.register),
        ).fetchall()
        return [_tag_membership(r) for r in rows]

    def _resolve_provider(self, fqid: Fqid) -> ResolvedProvider:
        row = self._conn.execute(
            "SELECT provider_id, name FROM provider WHERE slug = ?",
            (fqid.provider,),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedProvider(
            fqid=fqid, provider_id=row["provider_id"], name=row["name"]
        )

    def _resolve_register(self, fqid: Fqid) -> ResolvedRegister:
        # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): `name` / `purpose` (was `registernamn` / `registersyfte`).
        # `registerrubrik` is dropped per the same glossary rename.
        row = self._conn.execute(
            "SELECT r.register_id, r.provider_id, r.name, r.purpose "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ?",
            (fqid.provider, fqid.register),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedRegister(
            fqid=fqid,
            register_id=row["register_id"],
            provider_id=row["provider_id"],
            name=row["name"],
            purpose=row["purpose"],
        )

    def _resolve_binding(self, fqid: Fqid) -> ResolvedVariable:
        """Longitudinal resolution (see DESIGN.md → Catalog API surface). The 3-segment binding FQID selects
        ONE `variable` row by register-unique slug (exact match, no
        derive-at-resolve); from it we gather the shared metadata, the full
        `variable_state` history (each tagged with its variant), and the
        variable-grain edges. Period-independent — period narrowing lives in
        `resolve_at`.
        """
        resolved = self._resolve_variable_identity(fqid)
        if resolved is None:
            raise _not_found(fqid)
        var, via_same_as = resolved
        return self._build_resolved_variable(fqid, var, via_same_as)

    def _var_same_as_source_keys(self) -> frozenset[tuple[str, str, str]]:
        if self._var_same_as_sources is None:
            rows = self._conn.execute(
                "SELECT DISTINCT a_provider, a_register, a_variable "
                "FROM variable_same_as"
            ).fetchall()
            self._var_same_as_sources = frozenset((r[0], r[1], r[2]) for r in rows)
        return self._var_same_as_sources

    def _resolve_variable_identity(
        self, fqid: Fqid
    ) -> tuple[sqlite3.Row, tuple[Fqid, ...] | None] | None:
        """Resolve a binding FQID to its `variable` row, following curated
        `variable_same_as` edges when the direct slug lookup misses. Returns
        `(variable_row, via_same_as_path)` — `via_same_as` is None on a direct
        hit, else the traversal path (3-segment binding FQIDs). None when neither
        the direct lookup nor any same_as edge resolves.

        Variable identity is period- and variant-independent (the FQID's slug is
        the register-unique natural key; see DESIGN.md → Two-level variable model) — there is no edition/instance to
        thread."""
        assert fqid.provider is not None and fqid.register is not None
        assert fqid.variable is not None
        direct = self._lookup_variable(fqid.provider, fqid.register, fqid.variable)
        if direct is not None:
            return direct, None
        return self._resolve_variable_via_same_as(fqid)

    def _resolve_variable_via_same_as(
        self, fqid: Fqid
    ) -> tuple[sqlite3.Row, tuple[Fqid, ...]] | None:
        """BFS through `variable_same_as` (variable grain; see DESIGN.md → Composite registers and source tracking) until a target
        variable EXISTS (by register-unique slug). The edge is variable grain
        (cross-register / cross-provider equivalence), so the target register
        may use other variant slugs; the path records each hop as its 3-segment
        binding FQID."""
        assert fqid.provider and fqid.register and fqid.variable
        if (
            fqid.provider,
            fqid.register,
            fqid.variable,
        ) not in self._var_same_as_source_keys():
            return None
        start_key = (fqid.provider, fqid.register, fqid.variable)
        visited: set[tuple[str, str, str]] = {start_key}
        queue: deque[tuple[str, str, str, tuple[Fqid, ...]]] = deque()
        queue.append((*start_key, ()))
        while queue:
            prov, reg, variable, path = queue.popleft()
            rows = self._conn.execute(
                "SELECT b_provider, b_register, b_variable "
                "FROM variable_same_as "
                "WHERE a_provider = ? AND a_register = ? AND a_variable = ?",
                (prov, reg, variable),
            ).fetchall()
            for row in rows:
                n_prov = row["b_provider"]
                n_reg = row["b_register"]
                n_variable = row["b_variable"]
                key = (n_prov, n_reg, n_variable)
                if key in visited:
                    continue
                visited.add(key)
                try:
                    step_fqid = Fqid.binding_fqid(n_prov, n_reg, n_variable)
                except FqidError:
                    # Malformed slug — populate_slugs validates on write, so
                    # this is a build-invariant break; skip the candidate.
                    continue
                new_path = (*path, step_fqid)
                target = self._lookup_variable(n_prov, n_reg, n_variable)
                if target is not None:
                    return target, new_path
                queue.append((n_prov, n_reg, n_variable, new_path))
        return None

    def _build_resolved_variable(
        self, fqid: Fqid, var: sqlite3.Row, via_same_as: tuple[Fqid, ...] | None
    ) -> ResolvedVariable:
        """Assemble a `ResolvedVariable` from a resolved `variable` row: shared
        metadata + full chronological state history + variable-grain edges. The
        edge accessors all key off the resolved variable's own
        (provider, register, slug) triple, so a same_as-resolved binding reports
        the TARGET's edges (the binding resolved *to* that variable)."""
        meta = self._lookup_variable_meta(var["variable_id"])
        triple = (meta["provider_slug"], meta["register_slug"], meta["slug"])
        edges = self._edges_for_variable(*triple, var["variable_id"])
        return ResolvedVariable(
            fqid=fqid,
            variable_id=var["variable_id"],
            register_id=meta["register_id"],
            provider_key=meta["provider_key"],
            name=meta["name"],
            definition=meta["definition"],
            description=meta["description"],
            measurement_unit=meta["measurement_unit"],
            is_sensitive=bool(meta["is_sensitive"]),
            is_identifier=bool(meta["is_identifier"]),
            source_register_id=meta["source_register_id"],
            source_register_text=meta["source_register_text"],
            states=self._states_for_variable(var["variable_id"]),
            same_as=edges["same_as"],
            replaced_by=edges["replaced_by"],
            related_to=edges["related_to"],
            lineage=edges["lineage"],
            via_same_as=via_same_as,
        )

    # ── A2.5 state-anchored resolution helpers ─────────────────────────────

    def _lookup_variable(
        self, provider: str, register: str, variable_slug: str
    ) -> sqlite3.Row | None:
        """THE variable row for a register-unique slug (see DESIGN.md → Two-level variable model — natural key)."""
        return self._conn.execute(
            "SELECT v.variable_id, v.register_id, v.provider_key, "
            "v.name AS variable_name "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
            (provider, register, variable_slug),
        ).fetchone()

    def _lookup_variable_meta(self, variable_id: int) -> sqlite3.Row:
        """The variable's shared metadata + its own (provider, register, slug)
        triple — the canonical identity for the edge accessors. The variable_id
        is already resolved, so this always finds a row."""
        row = self._conn.execute(
            "SELECT v.register_id, v.provider_key, v.slug, v.name, v.definition, "
            "v.description, v.measurement_unit, v.is_sensitive, v.is_identifier, "
            "v.source_register_id, v.source_register_text, "
            "r.slug AS register_slug, p.slug AS provider_slug "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.variable_id = ?",
            (variable_id,),
        ).fetchone()
        assert row is not None  # variable_id resolved upstream
        return row

    def _states_in_bounds(
        self,
        variable_id: int,
        register_variant_id: int | None,
        bounds: tuple[str, str] | None,
    ) -> list[sqlite3.Row]:
        """`variable_state` rows for the variable whose validity range intersects
        `bounds` (an inclusive ISO `(lo, hi)` date interval), **chronological
        ascending** (oldest first) for the public surface. `register_variant_id`
        None spans every variant; `bounds` None returns every state (the
        `_default` / no-period-filter case).

        A2.5 generalizes the interim year-granular overlap test: bounds
        are full ISO dates, so sub-annual queries (`HT2020`, `2020-08`, a range)
        intersect precisely against the stored full-date validity ranges (see DESIGN.md → Two-level variable model) —
        the year-only INTERIM limit is lifted. The interval test is the standard
        `valid_from <= hi AND valid_to >= lo` (string compare is chronologically
        correct because every stored value is a full date)."""
        # JOIN `variable` to denormalize the variable-grain `is_identifier` flag
        # onto each state (see DESIGN.md → Two-level variable model — the column is variable-grain); LEFT JOIN
        # `classification` for the per-state `classification_slug` (NULL for
        # code-less states). Columns are qualified so the ORDER BY stays
        # unambiguous.
        if register_variant_id is not None:
            rows = self._conn.execute(
                "SELECT vs.state_id, vs.register_variant_id, vs.data_type, "
                "vs.data_length, vs.delivery_column_name, vs.value_set_id, "
                "vs.value_set_version_label, vs.valid_from, vs.valid_to, "
                "v.is_identifier, c.slug AS classification_slug "
                "FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "LEFT JOIN classification c ON vs.classification_id = c.id "
                "WHERE vs.variable_id = ? AND vs.register_variant_id = ? "
                "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
                "vs.state_id",
                (variable_id, register_variant_id),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT vs.state_id, vs.register_variant_id, vs.data_type, "
                "vs.data_length, vs.delivery_column_name, vs.value_set_id, "
                "vs.value_set_version_label, vs.valid_from, vs.valid_to, "
                "v.is_identifier, c.slug AS classification_slug "
                "FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "LEFT JOIN classification c ON vs.classification_id = c.id "
                "WHERE vs.variable_id = ? "
                "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
                "vs.register_variant_id, vs.state_id",
                (variable_id,),
            ).fetchall()
        if bounds is None:
            return rows
        lo, hi = bounds
        return [r for r in rows if r["valid_from"] <= hi and r["valid_to"] >= lo]

    def _value_set_codes(
        self, value_set_id: int | None
    ) -> tuple[tuple[str, str], ...] | None:
        """(code, label) pairs for a `value_set_id`, deterministically ordered.
        None when the state carries no value set."""
        if value_set_id is None:
            return None
        rows = self._conn.execute(
            "SELECT vc.code, vc.label FROM value_set_member vsm "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vsm.value_set_id = ? ORDER BY vc.code, vc.label",
            (value_set_id,),
        ).fetchall()
        return tuple((r["code"], r["label"]) for r in rows)

    def _row_to_state(self, row: sqlite3.Row) -> VariableState:
        """Build a `VariableState` from a `variable_state` row, tagging it with
        its variant slug and hydrating its value set."""
        rvid = row["register_variant_id"]
        variant = self._variant_slug(rvid)
        if variant is None:
            # see DESIGN.md → Two-level variable model: register_variant_id is NOT NULL on variable_state and FK'd to
            # register_variant, so a missing slug is a build-invariant break, not
            # a normal case — surface it loudly rather than emit a bad coordinate.
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="state_variant_unresolved",
                error_class="query",
                message=(
                    f"variable_state {row['state_id']} references "
                    f"register_variant {rvid} with no slug"
                ),
                remediation="Rebuild the reg_meta DB (slug population is incomplete).",
            )
        return VariableState(
            state_id=row["state_id"],
            variant=variant,
            register_variant_id=rvid,
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            data_type=row["data_type"],
            data_length=row["data_length"],
            delivery_column_name=row["delivery_column_name"],
            value_set_version_label=row["value_set_version_label"],
            value_set_id=row["value_set_id"],
            value_set=self._value_set_codes(row["value_set_id"]),
            is_identifier=bool(row["is_identifier"]),
            classification_slug=row["classification_slug"],
        )

    def _states_for_variable(self, variable_id: int) -> tuple[VariableState, ...]:
        """Full chronological state history for a variable (all variants)."""
        return tuple(
            self._expand_state_windows(
                variable_id, self._states_in_bounds(variable_id, None, None), None
            )
        )

    def _variable_windows(
        self, variable_id: int
    ) -> dict[int, list[tuple[str, str, str]]]:
        """`variable_alias_window` rows (#319) for a merged monthly-family
        variable, grouped by `register_variant_id` → [(delivery_column_name,
        valid_from, valid_to), …] sorted by window start. EMPTY for every
        non-merged variable (no window rows), so the per-month expansion is a
        no-op there. One indexed point-lookup on `idx_variable_alias_window_lookup`."""
        out: dict[int, list[tuple[str, str, str]]] = {}
        for rvid, col, wfrom, wto in self._conn.execute(
            "SELECT register_variant_id, delivery_column_name, valid_from, valid_to "
            "FROM variable_alias_window WHERE variable_id = ? "
            "ORDER BY register_variant_id, valid_from, delivery_column_name",
            (variable_id,),
        ):
            out.setdefault(rvid, []).append((col, wfrom, wto))
        return out

    def _expand_state_windows(
        self,
        variable_id: int,
        rows: list[sqlite3.Row],
        bounds: tuple[str, str] | None,
    ) -> list[VariableState]:
        """Map period-filtered `variable_state` rows to `VariableState`s, expanding
        a MERGED monthly-family variable's ANNUAL state into one `VariableState`
        per month-column window that OVERLAPS `bounds` (#319). The stored state is
        ONE annual single-claim row per year; the per-month dimension is READ-TIME
        from `variable_alias_window` — `resolve_at("2024-03")` → the mar column,
        `resolve_at("2024")` → 12 windows. Non-merged variables have no window
        rows → each state maps 1:1 via `_row_to_state` (byte-identical behaviour).

        D2: a year's 12 windows SHARE the annual state's `state_id` +
        `value_set_version_label` (one claim, 12 representations); only
        `delivery_column_name` + `valid_from`/`valid_to` are overridden per window.
        The per-window identity is the compound (state_id, delivery_column_name,
        valid_from). A window is attributed to the state whose validity range
        contains it (windows were emitted per the state's delivery year)."""
        windows_by_variant = self._variable_windows(variable_id)
        if not windows_by_variant:
            return [self._row_to_state(r) for r in rows]
        lo, hi = bounds if bounds is not None else ("0001-01-01", "9999-12-31")
        out: list[VariableState] = []
        for row in rows:
            base = self._row_to_state(row)
            windows = windows_by_variant.get(row["register_variant_id"], [])
            # Windows belonging to THIS annual state (within its validity range)
            # that also overlap the queried bounds.
            matched = [
                (col, wfrom, wto)
                for (col, wfrom, wto) in windows
                if base.valid_from <= wfrom
                and wto <= base.valid_to
                and wfrom <= hi
                and wto >= lo
            ]
            if not matched:
                # A merged variable's state with no window in range (e.g. a year
                # the family didn't deliver this column) — keep the annual row so
                # the variable never silently drops a claim.
                out.append(base)
                continue
            for col, wfrom, wto in matched:
                out.append(
                    replace(
                        base,
                        delivery_column_name=col,
                        valid_from=wfrom,
                        valid_to=wto,
                    )
                )
        out.sort(key=lambda s: (s.valid_from, s.valid_to, s.delivery_column_name or ""))
        return out

    def _variant_slug(self, register_variant_id: int) -> str | None:
        row = self._conn.execute(
            "SELECT slug FROM register_variant WHERE register_variant_id = ?",
            (register_variant_id,),
        ).fetchone()
        return row["slug"] if row else None

    def _resolve_variant_id(
        self, register_id: int, variant_slug: str
    ) -> int | _Missing:
        """register_variant_id for a (register, variant slug) pair, or the
        `_MISSING` sentinel when the slug names no variant under the register (a
        genuine miss → empty `resolve_at` result). `_default` is looked up like
        any other slug: a curated `_default` row matches its id; a variant-less
        register has no `register_variant` row (and no states), so `_default`
        misses → empty (states always carry a real `register_variant_id`; see DESIGN.md → Two-level variable model)."""
        row = self._conn.execute(
            "SELECT register_variant_id FROM register_variant "
            "WHERE register_id = ? AND slug = ?",
            (register_id, variant_slug),
        ).fetchone()
        return row["register_variant_id"] if row is not None else _MISSING

    # ── A2.5 variable-grain edge accessors (see DESIGN.md → Catalog API surface) ──

    @staticmethod
    def _ref_fqid(provider: str, register: str, variable: str) -> Fqid | None:
        """Best-effort 3-segment binding FQID for an edge endpoint (see DESIGN.md → Composite registers and source tracking and FQID grammar).
        The stored triple is exactly the binding FQID now; build-time slug
        validation guarantees it round-trips, but a malformed/NULL slug surfaces
        as None rather than raising (the triple stays the load-bearing identity)."""
        try:
            return Fqid.binding_fqid(provider, register, variable)
        except FqidError:
            return None

    def _edges_for_variable(
        self, provider: str, register: str, variable: str, variable_id: int
    ) -> dict[str, tuple]:
        """All variable-grain edges for one resolved variable, keyed by the
        canonical (provider, register, variable) triple. Factored so `resolve`
        and the standalone accessors share one query set."""
        return {
            "same_as": self._same_as_edges(provider, register, variable),
            "replaced_by": self._successor_edges(provider, register, variable),
            "related_to": self._related_edges(provider, register, variable),
            "lineage": self._lineage_edges(variable_id),
        }

    def _same_as_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[VariableRef, ...]:
        """`variable_same_as` neighbors (a-side keyed; stored both directions)."""
        rows = self._conn.execute(
            "SELECT b_provider, b_register, b_variable FROM variable_same_as "
            "WHERE a_provider = ? AND a_register = ? AND a_variable = ? "
            "ORDER BY b_provider, b_register, b_variable",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            VariableRef(
                fqid=self._ref_fqid(r["b_provider"], r["b_register"], r["b_variable"]),
                provider=r["b_provider"],
                register=r["b_register"],
                variable=r["b_variable"],
            )
            for r in rows
        )

    def _successor_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[VariableRef, ...]:
        """OUTBOUND succession (`variable_replaced_by`, predecessor side = this
        variable). #142: carries `beskrivning` (reason) + `effective_year`."""
        rows = self._conn.execute(
            "SELECT successor_provider, successor_register, successor_variable, "
            "effective_year, beskrivning FROM variable_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "AND predecessor_variable = ? "
            "ORDER BY successor_provider, successor_register, successor_variable",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            VariableRef(
                fqid=self._ref_fqid(
                    r["successor_provider"],
                    r["successor_register"],
                    r["successor_variable"],
                ),
                provider=r["successor_provider"],
                register=r["successor_register"],
                variable=r["successor_variable"],
                reason=r["beskrivning"],
                effective_year=r["effective_year"],
            )
            for r in rows
        )

    def _predecessor_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[VariableRef, ...]:
        """INBOUND succession (`variable_replaced_by`, successor side = this
        variable). Uses the A2.5 successor-side index. #142 fields carried."""
        rows = self._conn.execute(
            "SELECT predecessor_provider, predecessor_register, predecessor_variable, "
            "effective_year, beskrivning FROM variable_replaced_by "
            "WHERE successor_provider = ? AND successor_register = ? "
            "AND successor_variable = ? "
            "ORDER BY predecessor_provider, predecessor_register, predecessor_variable",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            VariableRef(
                fqid=self._ref_fqid(
                    r["predecessor_provider"],
                    r["predecessor_register"],
                    r["predecessor_variable"],
                ),
                provider=r["predecessor_provider"],
                register=r["predecessor_register"],
                variable=r["predecessor_variable"],
                reason=r["beskrivning"],
                effective_year=r["effective_year"],
            )
            for r in rows
        )

    @staticmethod
    def _class_ref_fqid(slug: str) -> Fqid | None:
        """Best-effort 2-segment classification FQID for a succession endpoint
        (#571). The stored slug IS the identity; a malformed slug surfaces as None
        rather than raising (mirrors `_ref_fqid` for the variable grain)."""
        try:
            return Fqid.classification_fqid(slug)
        except FqidError:
            return None

    def _classification_successor_edges(
        self, slug: str
    ) -> tuple[ClassificationRef, ...]:
        """OUTBOUND classification succession (`classification_replaced_by`,
        predecessor side = this edition). Keyed on the LITERAL slug — succession
        references the exact edition, so this does NOT same_as-canonicalize."""
        rows = self._conn.execute(
            "SELECT successor_slug, effective_year, note FROM classification_replaced_by "
            "WHERE predecessor_slug = ? ORDER BY successor_slug",
            (slug,),
        ).fetchall()
        return tuple(
            ClassificationRef(
                fqid=self._class_ref_fqid(r["successor_slug"]),
                slug=r["successor_slug"],
                effective_year=r["effective_year"],
                note=r["note"],
            )
            for r in rows
        )

    def _classification_predecessor_edges(
        self, slug: str
    ) -> tuple[ClassificationRef, ...]:
        """INBOUND classification succession (`classification_replaced_by`,
        successor side = this edition). Uses the successor-side index. Keyed on the
        LITERAL slug (no same_as canonicalization — succession is per-edition)."""
        rows = self._conn.execute(
            "SELECT predecessor_slug, effective_year, note FROM classification_replaced_by "
            "WHERE successor_slug = ? ORDER BY predecessor_slug",
            (slug,),
        ).fetchall()
        return tuple(
            ClassificationRef(
                fqid=self._class_ref_fqid(r["predecessor_slug"]),
                slug=r["predecessor_slug"],
                effective_year=r["effective_year"],
                note=r["note"],
            )
            for r in rows
        )

    def _related_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[RelatedRef, ...]:
        """`variable_related_to` siblings (a-side keyed; stored both directions,
        see reg_meta_build/DESIGN.md → Build-time triage (SCB)). Carries `relation_kind`."""
        rows = self._conn.execute(
            "SELECT b_provider, b_register, b_variable, relation_kind "
            "FROM variable_related_to "
            "WHERE a_provider = ? AND a_register = ? AND a_variable = ? "
            "ORDER BY b_provider, b_register, b_variable, relation_kind",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            RelatedRef(
                fqid=self._ref_fqid(r["b_provider"], r["b_register"], r["b_variable"]),
                provider=r["b_provider"],
                register=r["b_register"],
                variable=r["b_variable"],
                relation_kind=r["relation_kind"],
            )
            for r in rows
        )

    def _lineage_edges(self, variable_id: int) -> tuple[LineageEdge, ...]:
        """Consumer-side lineage (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)) for this variable's states (the consumer
        side). A2.6: `source_fqid` is the source state's 3-segment binding FQID,
        joined from the source state's variable → register → provider. Best-effort
        — a NULL/malformed source slug surfaces as None (`_ref_fqid`)."""
        rows = self._conn.execute(
            "SELECT l.consumer_state_id, l.source_state_id, l.valid_from, l.valid_to, "
            "sp.slug AS src_provider, sr.slug AS src_register, sv.slug AS src_variable "
            "FROM variable_state_lineage l "
            "JOIN variable_state cs ON l.consumer_state_id = cs.state_id "
            "JOIN variable_state ss ON l.source_state_id = ss.state_id "
            "JOIN variable sv ON ss.variable_id = sv.variable_id "
            "JOIN register sr ON sv.register_id = sr.register_id "
            "JOIN provider sp ON sr.provider_id = sp.provider_id "
            "WHERE cs.variable_id = ? "
            "ORDER BY l.consumer_state_id, l.source_state_id",
            (variable_id,),
        ).fetchall()
        return tuple(
            LineageEdge(
                consumer_state_id=r["consumer_state_id"],
                source_state_id=r["source_state_id"],
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                source_fqid=(
                    self._ref_fqid(
                        r["src_provider"], r["src_register"], r["src_variable"]
                    )
                    if r["src_provider"] and r["src_register"] and r["src_variable"]
                    else None
                ),
            )
            for r in rows
        )

    def _lineage_warning_rows(self, variable_id: int) -> tuple[LineageWarning, ...]:
        """Build-time lineage warnings for this variable's states (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage))."""
        rows = self._conn.execute(
            "SELECT w.consumer_state_id, w.warning_kind, w.message "
            "FROM variable_state_lineage_warning w "
            "JOIN variable_state cs ON w.consumer_state_id = cs.state_id "
            "WHERE cs.variable_id = ? "
            "ORDER BY w.consumer_state_id, w.warning_kind",
            (variable_id,),
        ).fetchall()
        return tuple(
            LineageWarning(
                consumer_state_id=r["consumer_state_id"],
                warning_kind=r["warning_kind"],
                message=r["message"],
            )
            for r in rows
        )

    # ── A2.5 public period-resolution + edge-traversal API (see DESIGN.md → Catalog API surface) ──

    def resolve_at(
        self,
        fqid: str | Fqid,
        period: Period,
        *,
        variant: str | None = None,
        value_set_version: str | None = None,
    ) -> list[VariableState]:
        """Point/range resolution (see DESIGN.md → Catalog API surface): the `VariableState`s whose validity
        intersects `period`, chronological ascending. Length 1 for the common
        single-state-in-one-variant-and-version point query; length N across
        variants (omitting `variant`), range periods crossing transitions, or
        co-delivered classification vintages. Empty list when no state covers the
        period (no exception) — only the binding FQID not resolving raises.

        `period` is polymorphic (see reg_schema/DESIGN.md → Two layers: models vs. validator): int year, period token, range dict
        {"from","to"}, or "_default" (no period filter). `variant` narrows to one
        variant (the Source's `register_variant`); `value_set_version` narrows
        multi-vintage results to a single state by `value_set_version_label`.
        """
        parsed = self._parse_binding(fqid)
        resolved = self._resolve_variable_identity(parsed)
        if resolved is None:
            raise _not_found(parsed)
        var, _ = resolved
        variable_id = var["variable_id"]
        meta = self._lookup_variable_meta(variable_id)

        register_variant_id: int | None = None
        if variant is not None:
            rvid = self._resolve_variant_id(meta["register_id"], variant)
            if isinstance(rvid, _Missing):
                return []  # variant slug names no variant under this register
            register_variant_id = rvid

        bounds = _period_bounds(period)
        states = self._expand_state_windows(
            variable_id,
            self._states_in_bounds(variable_id, register_variant_id, bounds),
            bounds,
        )
        if value_set_version is not None:
            states = [
                s for s in states if s.value_set_version_label == value_set_version
            ]
        return states

    def states(self, fqid: str | Fqid) -> list[VariableState]:
        """see DESIGN.md → Catalog API surface: the variable's full state history (≡ `resolve(fqid).states`)."""
        # Route through _parse_binding (like the edge accessors) so a non-binding
        # FQID fails with the structured `not_a_binding_fqid` error instead of a
        # raw AttributeError off a ResolvedRegister/etc. (reg_webapp wants a 4xx, not 500; see reg_webapp/DESIGN.md → Catalog router structure).
        parsed = self._parse_binding(fqid)
        return list(self._resolve_binding(parsed).states)

    def predecessors(self, fqid: str | Fqid) -> list[VariableRef]:
        """see DESIGN.md → Catalog API surface: variables this binding's variable replaced (inbound succession)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._predecessor_edges(provider, register, variable))

    def successors(self, fqid: str | Fqid) -> list[VariableRef]:
        """see DESIGN.md → Catalog API surface: variables that replaced this binding's variable (outbound)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._successor_edges(provider, register, variable))

    def related(self, fqid: str | Fqid) -> list[RelatedRef]:
        """see DESIGN.md → Catalog API surface: split-sibling variables (variable grain; see reg_meta_build/DESIGN.md → Build-time triage (SCB))."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._related_edges(provider, register, variable))

    def classification_successors(self, fqid: str | Fqid) -> list[ClassificationRef]:
        """The editions that replaced this classification edition (outbound
        succession, #571). Keyed on the literal slug — succession tolerates a DEAD
        predecessor edition (a renamed/retired slug still carries edges), so unlike
        `successors` this does NOT require the slug to resolve to a live row."""
        return list(
            self._classification_successor_edges(self._parse_classification(fqid))
        )

    def classification_predecessors(self, fqid: str | Fqid) -> list[ClassificationRef]:
        """The editions this classification edition replaced (inbound succession,
        #571). Keyed on the literal slug; tolerates a dead edition like
        `classification_successors`."""
        return list(
            self._classification_predecessor_edges(self._parse_classification(fqid))
        )

    def classification_chain(self, fqid: str | Fqid) -> list[ClassificationEdition]:
        """The FULL edition timeline of a classification's succession chain (#571),
        oldest first, terminal (current) last — what the webapp browse panel
        renders to show "this classification has N editions" instead of only the
        immediate neighbor.

        Three things the immediate-neighbor accessors don't do:

          - same_as canonicalization: the queried slug may be a curated
            `classification_same_as` alias (see DESIGN.md → Classifications). We
            resolve it to the canonical LIVE edition's real slug, so the chain (and
            `is_self`) is anchored on the canonical edition, not the alias. A slug
            that resolves to no live row falls back to itself — succession tolerates
            dead slugs.
          - full walk: from the canonical slug we find the terminal (the chain end
            with no outbound successor), then collect ALL predecessors transitively
            from the terminal, assembling every edition in the chain.
          - dead-edition marking: a slug that has a `classification_replaced_by`
            edge but no live `classification` row is a DEAD edition — `fqid`/`name`
            None, rendered as plain text.

        A standalone classification with no succession edges returns a single
        edition (`is_current` and `is_self` both True). The predecessor walk lives
        here rather than reusing `queries._classification_editions` because
        `queries` imports `catalog` (catalog is the lower layer — importing back
        would be circular); the terminal walk reuses
        `_first_classification_successor_slug` + `_walk_terminal`."""
        queried = self._parse_classification(fqid)
        canonical = self._canonical_classification_slug(queried)
        # Terminal = the chain head. `_walk_terminal` returns None when the start
        # has no outbound edge (it IS the terminal), so fall back to canonical.
        walked = self._walk_terminal(
            (canonical,), self._first_classification_successor_slug
        )
        terminal = walked[0] if walked is not None else canonical
        year_by_slug = self._classification_chain_years(terminal)
        name_by_slug = self._classification_chain_names(year_by_slug.keys())
        editions = [
            ClassificationEdition(
                slug=slug,
                fqid=(self._class_ref_fqid(slug) if slug in name_by_slug else None),
                name=name_by_slug.get(slug),
                effective_year=year,
                is_current=(slug == terminal),
                is_self=(slug == canonical),
            )
            for slug, year in year_by_slug.items()
        ]
        # Oldest first; the terminal (current) sorts LAST regardless of year (it
        # carries no successor-side year). Undated predecessors sort after dated
        # ones but before the terminal; slug is the stable tiebreak.
        editions.sort(
            key=lambda e: (
                e.is_current,
                e.effective_year is None,
                e.effective_year or 0,
                e.slug,
            )
        )
        return editions

    def _canonical_classification_slug(self, slug: str) -> str:
        """Resolve a (possibly `same_as`-aliased) classification slug to the
        canonical LIVE edition's real slug. `_resolve_classification` follows the
        same_as graph to a live `classification` row whose id we re-read the slug
        from; an unresolvable slug falls back to itself (succession tolerates dead
        slugs)."""
        try:
            resolved = self._resolve_classification(Fqid.classification_fqid(slug))
        except RegMetaError:
            return slug
        row = self._conn.execute(
            "SELECT slug FROM classification WHERE id = ?",
            (resolved.classification_id,),
        ).fetchone()
        return row["slug"] if row and row["slug"] is not None else slug

    def _classification_chain_years(self, terminal_slug: str) -> dict[str, int | None]:
        """Every edition slug in the chain ending at `terminal_slug` → its
        `effective_year` (the year on the `classification_replaced_by` edge that
        names it as `successor_slug`). The terminal is no edge's successor, so it
        gets None. Predecessor-BFS up the successor side from the terminal, with a
        `seen` cycle guard."""
        year_by_slug: dict[str, int | None] = {terminal_slug: None}
        seen = {terminal_slug}
        frontier = [terminal_slug]
        while frontier:
            placeholders = ",".join("?" * len(frontier))
            rows = self._conn.execute(
                "SELECT predecessor_slug, effective_year "
                "FROM classification_replaced_by "
                f"WHERE successor_slug IN ({placeholders})",
                frontier,
            ).fetchall()
            nxt: list[str] = []
            for row in rows:
                pred = row["predecessor_slug"]
                if pred in seen:
                    continue
                seen.add(pred)
                year_by_slug[pred] = row["effective_year"]
                nxt.append(pred)
            frontier = nxt
        return year_by_slug

    def _classification_chain_names(
        self, slugs: Iterable[str]
    ) -> dict[str, str | None]:
        """Map each chain slug to its live `classification.name`. A slug ABSENT
        from the result is a DEAD edition (an edge slug with no live row) — the
        caller reads "absent" as dead, so a present-but-NULL name stays in the
        map."""
        slug_list = list(slugs)
        if not slug_list:
            return {}
        placeholders = ",".join("?" * len(slug_list))
        return {
            row["slug"]: row["name"]
            for row in self._conn.execute(
                f"SELECT slug, name FROM classification WHERE slug IN ({placeholders})",
                slug_list,
            ).fetchall()
        }

    def dimensions(self, fqid: str | Fqid) -> list[ConceptGroupSummary]:
        """see DESIGN.md → Catalog API surface: the concept-group dimension
        memberships (the variant facet groups — level / population / rank / …)
        that contain this binding's variable. Resolves `same_as` like the other
        edge accessors, so an alias cites its resolved target's groups (#489)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        target = str(Fqid.binding_fqid(provider, register, variable))
        return [
            g
            for g in self.list_concept_groups(provider, register)
            if any(str(m.fqid) == target for m in g.members)
        ]

    def lineage(self, fqid: str | Fqid) -> list[LineageEdge]:
        """see DESIGN.md → Catalog API surface: consumer-side composite lineage edges (state grain; see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage))."""
        _, _, _, variable_id = self._resolve_edge_triple(fqid)
        return list(self._lineage_edges(variable_id))

    def lineage_warnings(self, fqid: str | Fqid) -> list[LineageWarning]:
        """see DESIGN.md → Catalog API surface: build-time lineage warnings for the binding (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage))."""
        _, _, _, variable_id = self._resolve_edge_triple(fqid)
        return list(self._lineage_warning_rows(variable_id))

    def resolve_terminal_successor(self, fqid: str | Fqid) -> Fqid | None:
        """Walk the succession chain from a (possibly DEAD/renamed) FQID to its
        TERMINAL successor — the chain end with NO further outbound edge. Returns
        that terminal as an `Fqid` of the same kind, or None when the start has no
        outbound succession at all (genuinely unknown — the caller should 404).
        Used by the webapp to 301-redirect a citation of a renamed slug to where it
        lives now (#355 PART 2; register grain added in #412, classification in
        #571).

        Dispatches on FQID kind:
          - VARIABLE_BINDING → walks `variable_replaced_by` on the stored
            (provider, register, variable) triple, returns a binding `Fqid`.
          - REGISTER → walks `register_replaced_by` on the stored
            (provider, register) pair, returns a register `Fqid`.
          - CLASSIFICATION → walks `classification_replaced_by` on the stored
            edition slug, returns a classification `Fqid` (e.g. an old vintage
            edition redirects to the current one).
          - PROVIDER → None: there is no succession table for providers, so a
            rename there has nowhere to redirect.

        Unlike `successors` / `_resolve_edge_triple`, this does NOT require the
        FQID to resolve to a live row — that is the whole point: a renamed slug
        404s (its `variable` / `register` / `classification` row is gone), and we
        walk purely on the stored string tuple in the succession table to find
        where the citation should redirect.

        Always walks to the ABSOLUTE chain end, never hop-by-hop: a webapp 301 can
        be cached, and double-rename churn (A→B then B→C) would leave a cached
        A→B redirect pointing at a now-dead intermediate. Resolving to the terminal
        every time keeps the redirect correct under churn.

        Split simplification: the schema does NOT enforce 1:1 succession — a
        predecessor may have several successors (a split). Renames are 1:1, so this
        is rare; when it happens we pick deterministically (ORDER BY successor
        tuple, first row) so a 301 stays stable and never 404s, rather than
        refusing. Cycle guard: a `seen` set defends against a malformed
        double-rename loop (A→B→A) in the DB so the walk always terminates.
        """
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        if parsed.kind is FqidKind.VARIABLE_BINDING:
            assert parsed.provider and parsed.register and parsed.variable
            start = (parsed.provider, parsed.register, parsed.variable)
            terminal = self._walk_terminal(start, self._first_successor_triple)
            return None if terminal is None else Fqid.binding_fqid(*terminal)
        if parsed.kind is FqidKind.REGISTER:
            assert parsed.provider and parsed.register
            pair = (parsed.provider, parsed.register)
            terminal = self._walk_terminal(pair, self._first_register_successor_pair)
            return None if terminal is None else Fqid.register_fqid(*terminal)
        if parsed.kind is FqidKind.CLASSIFICATION:
            assert parsed.classification
            # 1-tuple start: `_first_classification_successor_slug(*current)`
            # unpacks `(slug,)` to the single slug arg.
            cstart = (parsed.classification,)
            terminal = self._walk_terminal(
                cstart, self._first_classification_successor_slug
            )
            return None if terminal is None else Fqid.classification_fqid(*terminal)
        # PROVIDER grain has no succession table — a rename there has nowhere to
        # redirect. Bail before any SQL.
        return None

    @staticmethod
    def _walk_terminal(
        start: _SuccTuple, first_successor: Callable[..., _SuccTuple | None]
    ) -> _SuccTuple | None:
        """Shared chain walk for `resolve_terminal_successor`. Follows
        `first_successor` from `start` to the ABSOLUTE chain end and returns the
        terminal tuple, or None when `start` has no outbound edge at all. The
        per-grain difference is only the `first_successor` accessor (and so the
        tuple arity); the walk semantics — terminal = no outbound edge, `seen`
        cycle guard, None when the start is itself terminal — are identical."""
        seen: set[_SuccTuple] = {start}
        current = start
        while True:
            nxt = first_successor(*current)
            if nxt is None or nxt in seen:
                # Terminal (no outbound edge) or a defensive cycle break.
                break
            seen.add(nxt)
            current = nxt
        if current == start:
            return None  # start had no outbound edge — genuinely unknown
        return current

    def _first_successor_triple(
        self, provider: str, register: str, variable: str
    ) -> tuple[str, str, str] | None:
        """The deterministically-first outbound `variable_replaced_by` successor
        triple for a predicate triple, or None when there is none. Reuses the
        `_successor_edges` SELECT shape (predecessor-side keyed, ORDER BY successor
        triple) but reads only the successor triple — the walk needs no `#142`
        reason/effective_year. ORDER BY + LIMIT 1 makes the split pick (see
        `resolve_terminal_successor`) deterministic in SQL."""
        row = self._conn.execute(
            "SELECT successor_provider, successor_register, successor_variable "
            "FROM variable_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "AND predecessor_variable = ? "
            "ORDER BY successor_provider, successor_register, successor_variable "
            "LIMIT 1",
            (provider, register, variable),
        ).fetchone()
        if row is None:
            return None
        return (
            row["successor_provider"],
            row["successor_register"],
            row["successor_variable"],
        )

    def _first_register_successor_pair(
        self, provider: str, register: str
    ) -> tuple[str, str] | None:
        """The deterministically-first outbound `register_replaced_by` successor
        pair for a predecessor (provider, register), or None when there is none.
        Register-grain analogue of `_first_successor_triple`: predecessor-side
        keyed, ORDER BY + LIMIT 1 makes the split pick (see
        `resolve_terminal_successor`) deterministic in SQL. Reads only the
        successor pair — the walk needs no effective_year/note."""
        row = self._conn.execute(
            "SELECT successor_provider, successor_register "
            "FROM register_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "ORDER BY successor_provider, successor_register "
            "LIMIT 1",
            (provider, register),
        ).fetchone()
        if row is None:
            return None
        return (row["successor_provider"], row["successor_register"])

    def _first_classification_successor_slug(self, slug: str) -> tuple[str] | None:
        """The deterministically-first outbound `classification_replaced_by`
        successor slug for a predecessor edition, or None when there is none.
        Classification-grain analogue of `_first_successor_triple` (#571):
        predecessor-keyed, ORDER BY + LIMIT 1 makes the split pick (see
        `resolve_terminal_successor`) deterministic in SQL. Returns a 1-tuple so
        `_walk_terminal` preserves arity across grains; reads only the successor
        slug — the walk needs no effective_year/note."""
        row = self._conn.execute(
            "SELECT successor_slug FROM classification_replaced_by "
            "WHERE predecessor_slug = ? ORDER BY successor_slug LIMIT 1",
            (slug,),
        ).fetchone()
        if row is None:
            return None
        return (row["successor_slug"],)

    def _resolve_edge_triple(self, fqid: str | Fqid) -> tuple[str, str, str, int]:
        """Resolve a binding FQID to the canonical (provider, register, variable,
        variable_id) the edge tables key on. Raises `_not_found` when the binding
        doesn't resolve (consistent with `resolve`/`states`)."""
        parsed = self._parse_binding(fqid)
        resolved = self._resolve_variable_identity(parsed)
        if resolved is None:
            raise _not_found(parsed)
        var, _ = resolved
        meta = self._lookup_variable_meta(var["variable_id"])
        return (
            meta["provider_slug"],
            meta["register_slug"],
            meta["slug"],
            var["variable_id"],
        )

    @staticmethod
    def _parse_binding(fqid: str | Fqid) -> Fqid:
        """Parse a binding FQID and assert it is one. The catalog API accessors (see DESIGN.md → Catalog API surface) only
        accept binding FQIDs; a non-binding kind is a usage error."""
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        if parsed.kind is not FqidKind.VARIABLE_BINDING:
            raise RegMetaError(
                exit_code=EXIT_USAGE,
                code="not_a_binding_fqid",
                error_class="query",
                message=(
                    f"expected a variable-binding FQID, got "
                    f"kind={parsed.kind.value}: {parsed!s}"
                ),
                remediation="Pass a 3-segment binding FQID (provider/register/slug).",
            )
        return parsed

    @staticmethod
    def _parse_classification(fqid: str | Fqid) -> str:
        """Parse a classification FQID and return its literal slug. The
        classification succession accessors only accept classification FQIDs; a
        non-classification kind is a usage error (mirrors `_parse_binding`)."""
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        if parsed.kind is not FqidKind.CLASSIFICATION:
            raise RegMetaError(
                exit_code=EXIT_USAGE,
                code="not_a_classification_fqid",
                error_class="query",
                message=(
                    f"expected a classification FQID, got "
                    f"kind={parsed.kind.value}: {parsed!s}"
                ),
                remediation="Pass a 2-segment classification FQID (class/<slug>).",
            )
        assert parsed.classification
        return parsed.classification

    def _resolve_classification(self, fqid: Fqid) -> ResolvedClassification:
        direct = self._resolve_classification_direct(fqid)
        if direct is not None:
            return direct
        via = self._resolve_classification_via_same_as(fqid)
        if via is not None:
            return via
        raise _not_found(fqid)

    def _resolve_classification_direct(
        self, fqid: Fqid
    ) -> ResolvedClassification | None:
        # A2.6.1: the slug bakes in the vintage and is globally UNIQUE, so a
        # single-bind slug lookup hits at most one row. The old (slug, version)
        # two-bind and the publisher-disambiguation branch are gone — there is
        # no cross-publisher (slug, version) collision to narrow.
        assert fqid.classification
        row = self._conn.execute(
            "SELECT id, short_name, name FROM classification WHERE slug = ?",
            (fqid.classification,),
        ).fetchone()
        if not row:
            return None
        # Outbound succession edges (#571) key on the resolved row's OWN slug
        # (`fqid.classification` == the row's slug on a direct hit). The same_as
        # path `replace(hit, fqid=fqid, …)`s off this direct hit, so it inherits
        # the resolved edition's edges (only `fqid`/`via_same_as` are overridden).
        return ResolvedClassification(
            fqid=fqid,
            classification_id=row["id"],
            short_name=row["short_name"],
            name=row["name"],
            replaced_by=self._classification_successor_edges(fqid.classification),
        )

    def _class_same_as_source_keys(self) -> frozenset[tuple[str, str]]:
        if self._class_same_as_sources is None:
            rows = self._conn.execute(
                "SELECT DISTINCT a_provider, a_classification_slug "
                "FROM classification_same_as"
            ).fetchall()
            self._class_same_as_sources = frozenset((r[0], r[1]) for r in rows)
        return self._class_same_as_sources

    def _resolve_classification_via_same_as(
        self, fqid: Fqid
    ) -> ResolvedClassification | None:
        """BFS through `classification_same_as`.

        A2.6.1: the slug bakes in the vintage and is globally UNIQUE, so each
        slug maps to exactly one row and the FQID needs no version. This path is
        only reached when the direct (single-bind) lookup missed — i.e. the
        queried slug has no row — which is exactly the same_as use case: a
        caller's old/equivalent slug that's been retired in favor of a present
        one. The BFS seeds from the edge sources naming the queried slug
        (edges carry a provider; the FQID grammar has none), walks edges keyed
        (provider, classification_slug), and resolves each neighbor with a
        single-bind slug lookup.
        """
        assert fqid.classification
        # Seed the provider from the edge table, NOT a classification-row lookup:
        # we're here precisely because the queried slug has no row, so the seed
        # provider can only come from the edges that name this slug as a source.
        sources = self._class_same_as_source_keys()
        seeds = [p for (p, slug) in sources if slug == fqid.classification]
        if not seeds:
            return None
        visited: set[tuple[str, str]] = {(p, fqid.classification) for p in seeds}
        queue: deque[tuple[str, str, tuple[Fqid, ...]]] = deque()
        for p in seeds:
            queue.append((p, fqid.classification, ()))
        while queue:
            prov, slug, path = queue.popleft()
            rows = self._conn.execute(
                "SELECT b_provider, b_classification_slug FROM classification_same_as "
                "WHERE a_provider = ? AND a_classification_slug = ?",
                (prov, slug),
            ).fetchall()
            for row in rows:
                n_prov = row["b_provider"]
                n_slug = row["b_classification_slug"]
                key = (n_prov, n_slug)
                if key in visited:
                    continue
                visited.add(key)
                try:
                    n_fqid = Fqid.classification_fqid(n_slug)
                except FqidError:
                    continue
                # Slug is globally UNIQUE: at most one row, no version loop,
                # no publisher disambiguation. A direct miss means the slug
                # isn't in the DB (deleted post-build) — keep walking the edge
                # graph in case a further hop lands on a present row.
                hit = self._resolve_classification_direct(n_fqid)
                if hit is not None:
                    return replace(hit, fqid=fqid, via_same_as=(*path, n_fqid))
                queue.append((n_prov, n_slug, (*path, n_fqid)))
        return None


_DISPATCH = {
    FqidKind.PROVIDER: Catalog._resolve_provider,
    FqidKind.REGISTER: Catalog._resolve_register,
    FqidKind.VARIABLE_BINDING: Catalog._resolve_binding,
    FqidKind.CLASSIFICATION: Catalog._resolve_classification,
}
