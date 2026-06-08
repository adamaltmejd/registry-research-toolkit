"""Catalog: FQID-to-row resolution against the reg_meta SQLite DB.

Implements the FQID API in REFACTOR_SPEC.md §5.10: ``Catalog.resolve(fqid)``
turns any FQID kind into a typed entity row (the 3-segment binding arm resolves
to the longitudinal ``ResolvedVariable``), with ``resolve_at`` + the per-edge
accessors for period/relationship traversal.

A2.6: the binding FQID is 3-segment (`provider/register/slug`). Variant and
period are delivery coordinates passed to ``resolve_at`` (not FQID segments),
and the variant / register_version FQID kinds — plus the ``editions`` discovery
path that enumerated per-edition bindings — are gone (§5.2).
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

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
    from pathlib import Path


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
    # §5.11: `name` was `registernamn`, `purpose` was `registersyfte`.
    # `registerrubrik` is dropped (redundant with name).
    name: str
    purpose: str | None


@dataclass(frozen=True)
class ResolvedClassification:
    fqid: Fqid
    classification_id: int
    short_name: str
    name: str
    via_same_as: tuple[Fqid, ...] | None = None


# §5.10 catalog-tree children shapes (A5.1b-i): thin child nodes for the webapp's
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


# A2.5b variant-browser shape (§9.5): a variant is a register sub-resource, NOT
# an FQID-addressable node (the variant left the binding FQID, §5.0.1), so this
# carries the variant `slug` (the `?variant=` browse coordinate) + display fields,
# not an `Fqid`. A4.4c adds the §9.5 panel-shape columns (read-only): a
# `panel_entity_key` that is a bare variable-slug str or a tuple of slugs
# (composite), the `panel_time_key` ("period" or a variable-slug), and the
# `panel_time_grain` ('delivery'/'row'). Most variants carry no panel data → all
# three are None.
@dataclass(frozen=True)
class VariantSummary:
    slug: str
    name: str | None
    description: str | None
    display_group: str | None
    panel_entity_key: str | tuple[str, ...] | None
    panel_time_key: str | None
    panel_time_grain: str | None


def _decode_panel_entity_key(raw: str | None) -> str | tuple[str, ...] | None:
    """Decode the stored `panel_entity_key` (A4.4c): a JSON-array string →
    tuple (composite key), any other string → itself (simple bare slug), NULL →
    None. Mirrors the `populate_slugs` writer (json.dumps for the tuple case)."""
    if raw is None:
        return None
    if raw.startswith("["):
        return tuple(json.loads(raw))
    return raw


# §5.10 / §6.2: the polymorphic period a caller passes to `resolve_at`. Mirrors
# `Source.period`: a bare year (int), a period token ("HT2020"/"2020-Q3"/
# "2020-08"/"2018-12-31"), an explicit range dict {"from", "to"} (endpoints are
# int or token), or the "_default" snapshot sentinel (no period filter). It is a
# delivery coordinate, NOT an FQID segment (the binding FQID is 3-seg, §5.2).
Period = int | str | dict


@dataclass(frozen=True)
class VariableState:
    """§5.1: one `variable_state` row — a variable's per-delivery shape, tagged
    with the **variant coordinate** it was delivered in. The longitudinal
    `ResolvedVariable.states` is a tuple of these; `resolve_at` returns the
    subset whose validity range intersects the queried period."""

    state_id: int
    # `register_variant.slug` for `register_variant_id`. §5.1 makes the column
    # NOT NULL, so a state always carries a real variant — `variant` is always a
    # resolved slug, never the synth `_default` placeholder (that fiction exists
    # only at variant-slot resolve time, not on a stored state).
    variant: str
    register_variant_id: int
    valid_from: str  # ISO 8601 'YYYY-MM-DD', inclusive
    valid_to: str  # ISO 8601 'YYYY-MM-DD', inclusive ('9999-12-31' open-ended)
    data_type: str | None
    data_length: str | None
    # Denormalized latest alias for the state (§5.1); full alias history lives in
    # `variable_alias`.
    delivery_column_name: str | None
    # §5.7 overlap discriminator (multi-vintage / grain / coding). NOT NULL
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
    # §5.7 classification family slug for this state's value set (e.g. 'lkf2007'),
    # resolved per-state from `variable_state.classification_id` — it varies
    # across a variable's states. None for code-less / unclassified states.
    classification_slug: str | None


@dataclass(frozen=True)
class VariableRef:
    """A variable-grain edge endpoint (§5.5): the 3-part `(provider, register,
    variable)` identity of a `same_as` / `replaced_by` neighbor. Carried by
    `predecessors`/`successors` and `ResolvedVariable.same_as`/`.replaced_by`.

    A2.6: `fqid` is the neighbor's 3-segment binding FQID — the edge triple IS
    the binding FQID now that the variant/period left the grammar (§5.2/§5.5).
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
class RelatedRef:
    """A variable-grain sibling edge (§5.7 split): `variable_related_to`. Same
    3-part identity as `VariableRef` plus the `relation_kind` (split reason,
    e.g. `same_definition_different_column`). `fqid` is the sibling's 3-segment
    binding FQID (A2.6)."""

    fqid: Fqid | None
    provider: str
    register: str
    variable: str
    relation_kind: str


@dataclass(frozen=True)
class LineageEdge:
    """§5.6 consumer-side lineage at STATE grain: one `variable_state_lineage`
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
    """§5.6 build-time lineage warning for a consumer state:
    `variable_state_lineage_warning`. `warning_kind` is 'no_source_state' or
    'ambiguous_source_variant'."""

    consumer_state_id: int
    warning_kind: str
    message: str


@dataclass(frozen=True)
class ResolvedVariable:
    """§5.10 longitudinal resolution of a binding FQID: the addressable
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
    replaced_by: tuple[VariableRef, ...]  # OUTBOUND successors (§5.10)
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
    """Expand a §6.2 `Period` to an inclusive ISO `(lo, hi)` interval, or None
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

    # ── A5.1b-i catalog-tree children enumeration (§5.10) ──────────────────
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

    def list_variants(
        self, provider_slug: str, register_slug: str
    ) -> list[VariantSummary]:
        """A register's variants — the `register_variant` sub-resource (the
        `?variant=` browse axis, §9.5) — ordered by slug. A variant with a NULL
        slug isn't browse-addressable, so it's excluded (symmetric with
        `list_bindings`). Empty when the (provider, register) pair names no
        register OR it has no slugged variants. (`_default` is a real variant
        slug — the synthesized variant for LSS/BU/SOL etc., §5.1 — so it is
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
                panel_time_key=r["panel_time_key"],
                panel_time_grain=r["panel_time_grain"],
            )
            for r in rows
        ]

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
        # §5.11: `name` / `purpose` (was `registernamn` / `registersyfte`).
        # `registerrubrik` is dropped per §5.11.
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
        """Longitudinal resolution (§5.10). The 3-segment binding FQID selects
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
        the register-unique natural key, §5.1) — there is no edition/instance to
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
        """BFS through `variable_same_as` (variable grain, §5.5) until a target
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
        """THE variable row for a register-unique slug (§5.1 natural key)."""
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
        intersect precisely against the stored full-date validity ranges (§5.1) —
        the year-only INTERIM limit is lifted. The interval test is the standard
        `valid_from <= hi AND valid_to >= lo` (string compare is chronologically
        correct because every stored value is a full date)."""
        # JOIN `variable` to denormalize the variable-grain `is_identifier` flag
        # onto each state (§5.1 column is variable-grain); LEFT JOIN
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
            # §5.1: register_variant_id is NOT NULL on variable_state and FK'd to
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
            self._row_to_state(r)
            for r in self._states_in_bounds(variable_id, None, None)
        )

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
        misses → empty (states always carry a real `register_variant_id`, §5.1)."""
        row = self._conn.execute(
            "SELECT register_variant_id FROM register_variant "
            "WHERE register_id = ? AND slug = ?",
            (register_id, variant_slug),
        ).fetchone()
        return row["register_variant_id"] if row is not None else _MISSING

    # ── A2.5 variable-grain edge accessors (§5.10) ─────────────────────────

    @staticmethod
    def _ref_fqid(provider: str, register: str, variable: str) -> Fqid | None:
        """Best-effort 3-segment binding FQID for an edge endpoint (§5.5/§5.2).
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

    def _related_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[RelatedRef, ...]:
        """`variable_related_to` siblings (a-side keyed; stored both directions,
        §5.7). Carries `relation_kind`."""
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
        """§5.6 consumer-side lineage for this variable's states (the consumer
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
        """§5.6 build-time lineage warnings for this variable's states."""
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

    # ── A2.5 public period-resolution + edge-traversal API (§5.10) ─────────

    def resolve_at(
        self,
        fqid: str | Fqid,
        period: Period,
        *,
        variant: str | None = None,
        value_set_version: str | None = None,
    ) -> list[VariableState]:
        """§5.10 point/range resolution: the `VariableState`s whose validity
        intersects `period`, chronological ascending. Length 1 for the common
        single-state-in-one-variant-and-version point query; length N across
        variants (omitting `variant`), range periods crossing transitions, or
        co-delivered classification vintages. Empty list when no state covers the
        period (no exception) — only the binding FQID not resolving raises.

        `period` is polymorphic (§6.2): int year, period token, range dict
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
        states = [
            self._row_to_state(r)
            for r in self._states_in_bounds(variable_id, register_variant_id, bounds)
        ]
        if value_set_version is not None:
            states = [
                s for s in states if s.value_set_version_label == value_set_version
            ]
        return states

    def states(self, fqid: str | Fqid) -> list[VariableState]:
        """§5.10: the variable's full state history (≡ `resolve(fqid).states`)."""
        # Route through _parse_binding (like the edge accessors) so a non-binding
        # FQID fails with the structured `not_a_binding_fqid` error instead of a
        # raw AttributeError off a ResolvedRegister/etc. (§9.5 wants a 4xx, not 500).
        parsed = self._parse_binding(fqid)
        return list(self._resolve_binding(parsed).states)

    def predecessors(self, fqid: str | Fqid) -> list[VariableRef]:
        """§5.10: variables this binding's variable replaced (inbound succession)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._predecessor_edges(provider, register, variable))

    def successors(self, fqid: str | Fqid) -> list[VariableRef]:
        """§5.10: variables that replaced this binding's variable (outbound)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._successor_edges(provider, register, variable))

    def related(self, fqid: str | Fqid) -> list[RelatedRef]:
        """§5.10: split-sibling variables (variable grain, §5.7)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._related_edges(provider, register, variable))

    def lineage(self, fqid: str | Fqid) -> list[LineageEdge]:
        """§5.10: consumer-side composite lineage edges (state grain, §5.6)."""
        _, _, _, variable_id = self._resolve_edge_triple(fqid)
        return list(self._lineage_edges(variable_id))

    def lineage_warnings(self, fqid: str | Fqid) -> list[LineageWarning]:
        """§5.10: build-time lineage warnings for the binding (§5.6)."""
        _, _, _, variable_id = self._resolve_edge_triple(fqid)
        return list(self._lineage_warning_rows(variable_id))

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
        """Parse a binding FQID and assert it is one. The §5.10 accessors only
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
        row = self._conn.execute(
            "SELECT id, short_name, name FROM classification WHERE slug = ?",
            (fqid.classification,),
        ).fetchone()
        if not row:
            return None
        return ResolvedClassification(
            fqid=fqid,
            classification_id=row["id"],
            short_name=row["short_name"],
            name=row["name"],
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
