"""Catalog: FQID-to-row resolution against the reg_meta SQLite DB.

Implements the FQID API in REFACTOR_SPEC.md §5.10: ``Catalog.resolve(fqid)``
turns any FQID kind into a typed entity row (the binding arm resolves to the
longitudinal ``ResolvedVariable``), with ``resolve_at`` + the per-edge
accessors for period/relationship traversal. ``Catalog.editions(...)`` is the
separate discovery path enumerating a variable's bindings under a register.
"""

from __future__ import annotations

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
    validate_slug,
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
class ResolvedRegisterVariant:
    fqid: Fqid
    # `register_variant_id is None` marks the §5.1 synthesized `_default` placeholder
    # for variant-less registers — the slot is transparent at resolve time,
    # not backed by a register_variant row.
    register_variant_id: int | None
    register_id: int
    # §5.11 rename. `description` replaces the dropped `registervariantrubrik`
    # carrier; SCB's `registervariantbeskrivning` is the value.
    name: str | None
    description: str | None
    display_group: str | None


@dataclass(frozen=True)
class ResolvedRegisterVersion:
    fqid: Fqid
    regver_id: int
    register_variant_id: int
    register_id: int
    registerversionnamn: str | None


@dataclass(frozen=True)
class ResolvedVariableBinding:
    fqid: Fqid
    cvid: int
    register_id: int
    register_variant_id: int
    regver_id: int
    var_id: int
    # §5.11: SCB `variabelnamn` → `variable_name`; `kolumnnamn` →
    # `delivery_column_name` (the un-prefixed SCB delivery column
    # header — `Kon`, `PersonNr`, etc.).
    variable_name: str | None
    delivery_column_name: str | None
    # §5.6 lineage: source cvid + source binding FQID. NULL on canonical bindings.
    # Both populated by the surviving discovery path (`editions` / `_row_to_binding`).
    via_source_id: int | None = None
    lineage: Fqid | None = None


@dataclass(frozen=True)
class ResolvedClassification:
    fqid: Fqid
    classification_id: int
    short_name: str
    name: str
    via_same_as: tuple[Fqid, ...] | None = None


# §5.10 / §6.2: the polymorphic period a caller passes to `resolve_at`. Mirrors
# `Source.period`: a bare year (int), a period token ("HT2020"/"2020-Q3"/
# "2020-08"/"2018-12-31"), an explicit range dict {"from", "to"} (endpoints are
# int or token), or the "_default" snapshot sentinel (no period filter). It is
# NOT an FQID segment — the binding FQID parser is untouched here (A2.6 owns the
# 3-seg flip).
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


@dataclass(frozen=True)
class VariableRef:
    """A variable-grain edge endpoint (§5.5): the 3-part `(provider, register,
    variable)` identity of a `same_as` / `replaced_by` neighbor. Carried by
    `predecessors`/`successors` and `ResolvedVariable.same_as`/`.replaced_by`.

    `fqid` is None: the edge tables store only the 3-part variable identity, and
    the binding FQID grammar is still 5-seg (variant + period) until A2.6, so no
    addressable FQID can be built from an edge row. The `provider`/`register`/
    `variable` triple is the load-bearing identity; `fqid` becomes the 3-seg
    binding FQID once A2.6 flips the grammar.
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
    e.g. `same_definition_different_column`). `fqid` is None for the same reason
    as `VariableRef`."""

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
    tagged with its variant) + variable-grain edges. Replaces the interim
    per-state `ResolvedVariableBinding` on the `resolve()` path (the discovery
    `editions()` path still returns `ResolvedVariableBinding`)."""

    # The caller's binding FQID, preserved through a `same_as` traversal (like
    # the old ResolvedVariableBinding.fqid). Still the interim 5-seg form.
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
    # Traversal path (interim 5-seg FQIDs) when resolved via `same_as`; None on
    # a direct hit. Preserved from the interim resolver's semantics.
    via_same_as: tuple[Fqid, ...] | None = None


ResolvedEntity = (
    ResolvedProvider
    | ResolvedRegister
    | ResolvedRegisterVariant
    | ResolvedRegisterVersion
    | ResolvedVariable
    | ResolvedClassification
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


# Shared SELECT + FROM + JOINs for the binding **discovery** path (`editions`).
# Includes the consumer-side join chain (`*_src`) so the lineage FQID for
# rows with `via_source_id IS NOT NULL` is built directly from the result
# row — no per-row roundtrip. Callers append their WHERE/ORDER BY.
# A2.1.5: the variable slug is read from the stored `variable.slug` column
# (joined via `vi.var_id` → `v.provider_key`), not derived from
# `delivery_column_name` at query time.
#
# ⚠️ Interim fan-out (discovery only). `provider_key` is a NON-unique join hint:
# an A2.2 triage split puts several `variable` rows under one
# `(register_id, provider_key)` but relinks `variable_state`, not
# `variable_instance`, so a single instance fans across all sibling `variable`
# rows here. Variable resolution (`_resolve_variable_identity` /
# `_resolve_variable_via_same_as`) was flipped in A2.2 onto `variable_state`
# (keyed by `variable_id`), which is fan-out-free; only `editions` (discovery —
# lists a variable's bindings) still reads this join. The
# `variable_slug == variable` filter keeps
# the right slug, but the cvid/metadata are the shared instance's for split
# siblings — a known discovery quirk that clears when `editions` moves onto
# `variable_state` (A2.5 `states`/`resolve_at`) and `variable_instance` is
# dropped (A2.7). See MIGRATION_PLAN A2.2/A2.5.
_BINDING_QUERY = (
    "SELECT vi.cvid, vi.register_id, vi.register_variant_id, vi.regver_id, vi.var_id, "
    "vi.via_source_id, "
    "v.name AS variable_name, v.slug AS variable_slug, va.delivery_column_name, "
    "rv.slug AS variant_slug, rver.slug AS version_slug, "
    "p_src.slug AS src_provider_slug, r_src.slug AS src_register_slug, "
    "rv_src.slug AS src_variant_slug, rver_src.slug AS src_version_slug "
    "FROM variable_instance vi "
    "JOIN register_version rver ON vi.regver_id = rver.regver_id "
    "JOIN register_variant rv ON vi.register_variant_id = rv.register_variant_id "
    "JOIN register r ON vi.register_id = r.register_id "
    "JOIN provider p ON r.provider_id = p.provider_id "
    "JOIN variable v ON vi.register_id = v.register_id "
    "    AND CAST(vi.var_id AS TEXT) = v.provider_key "
    "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
    "LEFT JOIN variable_instance vi_src ON vi.via_source_id = vi_src.cvid "
    "LEFT JOIN register_version rver_src ON vi_src.regver_id = rver_src.regver_id "
    "LEFT JOIN register_variant rv_src ON vi_src.register_variant_id = rv_src.register_variant_id "
    "LEFT JOIN register r_src ON vi_src.register_id = r_src.register_id "
    "LEFT JOIN provider p_src ON r_src.provider_id = p_src.provider_id "
)


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

    def _resolve_variant(self, fqid: Fqid) -> ResolvedRegisterVariant:
        # §5.11: `name` was `registervariantnamn`, `description` was
        # `registervariantbeskrivning`. `registervariantrubrik` and
        # `registervariantsekretess` are dropped.
        row = self._conn.execute(
            "SELECT rv.register_variant_id, rv.register_id, rv.name, "
            "rv.description, rv.display_group "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
            (fqid.provider, fqid.register, fqid.variant),
        ).fetchone()
        if row:
            return ResolvedRegisterVariant(
                fqid=fqid,
                register_variant_id=row["register_variant_id"],
                register_id=row["register_id"],
                name=row["name"],
                description=row["description"],
                display_group=row["display_group"],
            )
        if fqid.variant == DEFAULT_VARIANT_SLUG:
            synth = self._synthesize_default_variant(fqid)
            if synth is not None:
                return synth
        raise _not_found(fqid)

    def _synthesize_default_variant(self, fqid: Fqid) -> ResolvedRegisterVariant | None:
        """§5.1: variant-less registers expose a transparent `_default` slot
        resolved on the fly. Returns None when the register has real variants
        or doesn't exist — caller falls through to not-found."""
        row = self._conn.execute(
            "SELECT r.register_id FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM register_variant rv WHERE rv.register_id = r.register_id"
            ")",
            (fqid.provider, fqid.register),
        ).fetchone()
        if not row:
            return None
        return ResolvedRegisterVariant(
            fqid=fqid,
            register_variant_id=None,
            register_id=row["register_id"],
            name=None,
            description=None,
            display_group=None,
        )

    def _resolve_version(self, fqid: Fqid) -> ResolvedRegisterVersion:
        # §5.2: `register_version.slug` is the canonical version-slot token —
        # either a derived period (`2018`, `HT2020`) or a curated slug for
        # rows the period regex can't disambiguate. populate_slugs writes both
        # kinds; the resolver just matches the slug column. §5.3 uniqueness
        # is enforced by `UNIQUE(register_variant_id, slug)` (db.py), so fetchone is safe.
        #
        # §5.1 follow-up: `_default` versions against a variant-less register
        # aren't reachable today — `register_version.register_variant_id` is NOT NULL,
        # so no version row can exist without a real variant row. When SOS
        # ingestion lands, either make the column nullable or extend this
        # resolver to synthesize the variant slot the way `_resolve_variant`
        # does.
        row = self._conn.execute(
            "SELECT rver.regver_id, rver.register_variant_id, rv.register_id, "
            "rver.registerversionnamn "
            "FROM register_version rver "
            "JOIN register_variant rv ON rver.register_variant_id = rv.register_variant_id "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ? AND rver.slug = ?",
            (fqid.provider, fqid.register, fqid.variant, fqid.period),
        ).fetchone()
        if row is None:
            raise _not_found(fqid)
        return ResolvedRegisterVersion(
            fqid=fqid,
            regver_id=row["regver_id"],
            register_variant_id=row["register_variant_id"],
            register_id=row["register_id"],
            registerversionnamn=row["registerversionnamn"],
        )

    def _resolve_binding(self, fqid: Fqid) -> ResolvedVariable:
        """A2.5 longitudinal resolution (§5.10). The binding FQID selects ONE
        `variable` row (register-unique slug); from it we gather the shared
        metadata, the full `variable_state` history (each tagged with its
        variant), and the variable-grain edges. Period-independent — period
        narrowing lives in `resolve_at`. Still parses the interim 5-seg binding
        grammar (the 3-seg flip is A2.6); `fqid.variant`/`fqid.period` are
        validated by the parser but not used for selection here.
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
        hit, else the traversal path (interim 5-seg FQIDs). None when neither the
        direct lookup nor any same_as edge resolves.

        Variable identity is period- and variant-independent (the FQID's slug is
        the register-unique natural key, §5.1), so this is simpler than the
        interim per-state resolver — there is no edition/instance to thread."""
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
        variable EXISTS (by register-unique slug). Variant-independent: the edge
        is variable grain, so the target register may use other variant slugs;
        the path FQIDs record the interim 5-seg form under the query's variant
        purely as a breadcrumb (resolution doesn't depend on it)."""
        assert fqid.provider and fqid.register and fqid.variable
        assert fqid.variant is not None and fqid.period is not None
        if (
            fqid.provider,
            fqid.register,
            fqid.variable,
        ) not in self._var_same_as_source_keys():
            return None
        variant = fqid.variant
        period = fqid.period
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
                    step_fqid = Fqid.binding_fqid(
                        n_prov, n_reg, variant, period, n_variable
                    )
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
        if register_variant_id is not None:
            rows = self._conn.execute(
                "SELECT state_id, register_variant_id, data_type, data_length, "
                "delivery_column_name, value_set_id, value_set_version_label, "
                "valid_from, valid_to FROM variable_state "
                "WHERE variable_id = ? AND register_variant_id = ? "
                "ORDER BY valid_from, valid_to, value_set_version_label, state_id",
                (variable_id, register_variant_id),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT state_id, register_variant_id, data_type, data_length, "
                "delivery_column_name, value_set_id, value_set_version_label, "
                "valid_from, valid_to FROM variable_state WHERE variable_id = ? "
                "ORDER BY valid_from, valid_to, value_set_version_label, "
                "register_variant_id, state_id",
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
                fqid=None,
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
                fqid=None,
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
                fqid=None,
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
                fqid=None,
                provider=r["b_provider"],
                register=r["b_register"],
                variable=r["b_variable"],
                relation_kind=r["relation_kind"],
            )
            for r in rows
        )

    def _lineage_edges(self, variable_id: int) -> tuple[LineageEdge, ...]:
        """§5.6 consumer-side lineage for this variable's states (the consumer
        side). `source_fqid` stays None: the source-side 3-part binding FQID
        isn't addressable until the A2.6 grammar flip (same reason as the refs'
        `fqid`), so there's nothing to build from the source slugs yet."""
        rows = self._conn.execute(
            "SELECT l.consumer_state_id, l.source_state_id, l.valid_from, l.valid_to "
            "FROM variable_state_lineage l "
            "JOIN variable_state cs ON l.consumer_state_id = cs.state_id "
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
                source_fqid=None,
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
                remediation="Pass a 5-segment binding FQID (provider/register/variant/period/variable).",
            )
        return parsed

    def _row_to_binding(
        self, row: sqlite3.Row, fqid: Fqid, variable_slug: str
    ) -> ResolvedVariableBinding:
        """Build a ResolvedVariableBinding from a `_BINDING_QUERY` row.

        Lineage is built from the joined source-side slug columns. Slugs may
        be transiently NULL between INSERT and `populate_slugs` (db.py); when
        any source slug is missing we leave `lineage` as None. A populated
        but malformed slug surfaces as `FqidError` — populate_slugs validates
        on write, so reaching this path means a build-time invariant broke
        and we want it loud, not silently dropped.
        """
        via = row["via_source_id"]
        lineage: Fqid | None = None
        if via is not None:
            src = (
                row["src_provider_slug"],
                row["src_register_slug"],
                row["src_variant_slug"],
                row["src_version_slug"],
                variable_slug,
            )
            if all(s for s in src):
                lineage = Fqid.binding_fqid(*src)
        return ResolvedVariableBinding(
            fqid=fqid,
            cvid=row["cvid"],
            register_id=row["register_id"],
            register_variant_id=row["register_variant_id"],
            regver_id=row["regver_id"],
            var_id=row["var_id"],
            # `variable_name` aliases `v.name`; `delivery_column_name` is
            # `va.delivery_column_name` (was `va.kolumnnamn`).
            variable_name=row["variable_name"],
            delivery_column_name=row["delivery_column_name"],
            via_source_id=via,
            lineage=lineage,
        )

    def editions(
        self, *, provider: str, register: str, variable: str
    ) -> list[ResolvedVariableBinding]:
        """All variable bindings of ``variable`` under ``provider/register``.

        Returns every ``(variant, period)`` combination where a
        variable_instance row exists whose `delivery_column_name` folds to
        ``variable``, including consumer-side bindings from §5.6 (their
        ``lineage`` field carries the source-side FQID). Results are ordered
        by ``(variant_slug, version_slug, cvid, delivery_column_name)`` for
        deterministic iteration.

        Slug inputs are validated; non-existent provider/register/variable
        yields an empty list (discovery, not resolution).
        """
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variable, "variable")

        rows = self._conn.execute(
            _BINDING_QUERY + "WHERE p.slug = ? AND r.slug = ? "
            "ORDER BY rv.slug, rver.slug, vi.cvid, va.delivery_column_name",
            (provider, register),
        ).fetchall()

        out: list[ResolvedVariableBinding] = []
        # variable_alias is keyed by (cvid, delivery_column_name) — a single
        # instance can have multiple aliases (e.g. `Kon` + `Kön`), so the LEFT
        # JOIN can yield one row per alias. A2.1.5: match the stored
        # `variable_slug` (not the derived delivery column); dedupe by cvid for
        # one binding per instance.
        seen: set[int] = set()
        for row in rows:
            if row["variable_slug"] != variable:
                continue
            cvid = row["cvid"]
            if cvid in seen:
                continue
            variant_slug = row["variant_slug"]
            version_slug = row["version_slug"]
            # Skip rows whose slug columns aren't populated (NULL on pre-1c
            # DBs or partial fixtures) — they can't be addressed by FQID.
            if not variant_slug or not version_slug:
                continue
            fqid = Fqid.binding_fqid(
                provider, register, variant_slug, version_slug, variable
            )
            out.append(self._row_to_binding(row, fqid, variable))
            seen.add(cvid)
        return out

    def _resolve_classification(self, fqid: Fqid) -> ResolvedClassification:
        direct = self._resolve_classification_direct(fqid)
        if direct is not None:
            return direct
        via = self._resolve_classification_via_same_as(fqid)
        if via is not None:
            return via
        raise _not_found(fqid)

    def _resolve_classification_direct(
        self, fqid: Fqid, *, publisher: str | None = None
    ) -> ResolvedClassification | None:
        # `publisher` is set by the same_as BFS when it narrowed the neighbor
        # by publisher; without it (the initial top-level lookup) the FQID
        # grammar carries no publisher slot, so cross-publisher (slug, version)
        # collisions can't be disambiguated here either way. When set, we
        # constrain the row by publisher to keep BFS from silently crossing
        # publisher namespaces under a colliding slug/version pair.
        if publisher is None:
            row = self._conn.execute(
                "SELECT id, short_name, name FROM classification "
                "WHERE slug = ? AND version = ?",
                (fqid.classification, fqid.version),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT id, short_name, name FROM classification "
                "WHERE slug = ? AND version = ? "
                "AND LOWER(COALESCE(publisher, 'scb')) = ?",
                (fqid.classification, fqid.version, publisher),
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

        Edges are keyed on (provider, classification_slug) only — the version
        is not part of the edge (§5.3 field reference). Same_as targets are
        version-unambiguous at build time, so the target's version is whatever
        the DB row carries; we pick it up from `classification.version` after
        the slug match.
        """
        assert fqid.classification and fqid.version
        # The classification FQID grammar has no provider slot — the publisher
        # is implicit. The (slug, version) lookup may have missed precisely
        # because the version drifted (the primary same_as use case), so we
        # seed the BFS from every publisher that owns *any* row for this slug.
        # Defaulting to 'scb' would silently break non-SCB classifications.
        publishers = {
            (r[0] or "scb").lower()
            for r in self._conn.execute(
                "SELECT DISTINCT publisher FROM classification WHERE slug = ?",
                (fqid.classification,),
            ).fetchall()
        }
        if not publishers:
            # No row carries this slug; nothing to traverse from.
            return None
        sources = self._class_same_as_source_keys()
        seeds = [p for p in publishers if (p, fqid.classification) in sources]
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
                # Same_as is version-agnostic; the classification FQID needs
                # a version. Forward-edge build validation rejects slugs with
                # multiple versions, but the auto-inserted *reverse* edge can
                # land us on a multi-version slug stem (e.g. `sun` v2000 +
                # v2020), so we try every matching version. NULL publisher is
                # normalized to 'scb' (matches build-side
                # `COALESCE(publisher,'scb')`) so a SCB-published row can't
                # accidentally match a query under another publisher.
                vrows = self._conn.execute(
                    "SELECT version FROM classification "
                    "WHERE slug = ? AND LOWER(COALESCE(publisher, 'scb')) = ? "
                    "ORDER BY version",
                    (n_slug, n_prov),
                ).fetchall()
                if not vrows:
                    # Target slug missing from DB despite passing build-time
                    # validation — slugged classifications must have been
                    # deleted post-build. Skip silently.
                    continue
                candidate_fqids: list[Fqid] = []
                for vrow in vrows:
                    try:
                        candidate_fqids.append(
                            Fqid.classification_fqid(n_slug, vrow["version"])
                        )
                    except FqidError:
                        continue
                for n_fqid in candidate_fqids:
                    new_path = (*path, n_fqid)
                    # Pass publisher so cross-publisher (slug, version)
                    # collisions can't silently land us on the wrong row.
                    hit = self._resolve_classification_direct(n_fqid, publisher=n_prov)
                    if hit is not None:
                        return replace(hit, fqid=fqid, via_same_as=new_path)
                # Continue BFS regardless of version count — further hops
                # don't depend on which version we picked, just on the
                # same_as edge graph. Use the first valid candidate for
                # the path-trace breadcrumb.
                if candidate_fqids:
                    queue.append((n_prov, n_slug, (*path, candidate_fqids[0])))
        return None


_DISPATCH = {
    FqidKind.PROVIDER: Catalog._resolve_provider,
    FqidKind.REGISTER: Catalog._resolve_register,
    FqidKind.REGISTER_VARIANT: Catalog._resolve_variant,
    FqidKind.REGISTER_VERSION: Catalog._resolve_version,
    FqidKind.VARIABLE_BINDING: Catalog._resolve_binding,
    FqidKind.CLASSIFICATION: Catalog._resolve_classification,
}
