"""Catalog: FQID-to-row resolution against the reg_meta SQLite DB.

Implements the FQID API in REFACTOR_SPEC.md §5.8: ``Catalog.resolve(fqid)``
turns any FQID kind into a typed entity row, ``Catalog.editions(...)``
enumerates all variable bindings of a given variable slug under a register.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from .db import db_path_from_args, open_db
from .errors import EXIT_NOT_FOUND, RegMetaError
from .fqid import (
    DEFAULT_VARIANT_SLUG,
    Fqid,
    FqidError,
    FqidKind,
    parse,
    validate_slug,
)
from .queries import extract_year

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
    # A2.2 interim resolver flip: the `variable_state` row this binding resolved
    # through (keyed by `variable_id`), so split siblings sharing one
    # `provider_key` resolve to their OWN state, not a shared instance's. The
    # discriminating metadata (delivery_column_name, value_set_version_label)
    # comes from this state; `cvid`/`via_source_id` stay sourced from the shared
    # `variable_instance` for §5.6 lineage until A2.4/A2.7. None on the discovery
    # path (`editions`), which still reads the interim instance join.
    state_id: int | None = None
    value_set_version_label: str | None = None
    # §5.6 lineage: source cvid + source binding FQID. NULL on canonical bindings.
    via_source_id: int | None = None
    lineage: Fqid | None = None
    # §5.5: path of intermediate FQIDs walked when the direct lookup missed
    # and a curated same_as edge resolved instead. None on direct hits.
    # §6.7 calls this "info, not warning"; the validator decides severity.
    via_same_as: tuple[Fqid, ...] | None = None


@dataclass(frozen=True)
class ResolvedClassification:
    fqid: Fqid
    classification_id: int
    short_name: str
    name: str
    via_same_as: tuple[Fqid, ...] | None = None


ResolvedEntity = (
    ResolvedProvider
    | ResolvedRegister
    | ResolvedRegisterVariant
    | ResolvedRegisterVersion
    | ResolvedVariableBinding
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
# rows here. Point resolution (`_resolve_binding_direct` / `_resolve_binding_via_
# same_as`) was flipped in A2.2 onto `variable_state` (keyed by `variable_id`),
# which is fan-out-free; only `editions` (discovery — lists a variable's
# bindings) still reads this join. The `variable_slug == variable` filter keeps
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

    def _resolve_binding(self, fqid: Fqid) -> ResolvedVariableBinding:
        direct = self._resolve_binding_direct(fqid)
        if direct is not None:
            return direct
        via = self._resolve_binding_via_same_as(fqid)
        if via is not None:
            return via
        raise _not_found(fqid)

    def _var_same_as_source_keys(self) -> frozenset[tuple[str, str, str]]:
        if self._var_same_as_sources is None:
            rows = self._conn.execute(
                "SELECT DISTINCT a_provider, a_register, a_variable "
                "FROM variable_same_as"
            ).fetchall()
            self._var_same_as_sources = frozenset((r[0], r[1], r[2]) for r in rows)
        return self._var_same_as_sources

    def _resolve_binding_direct(self, fqid: Fqid) -> ResolvedVariableBinding | None:
        """A2.2 interim resolver flip (§5.7/§5.10). Resolve through
        `variable_state` keyed by `variable_id`, NOT the
        `variable_instance`→`provider_key` join (which fanned one instance
        across split siblings sharing a `provider_key`). The FQID's variable
        slug selects THE variable row (register-unique slug); its
        `variable_state` for the queried (variant, period) supplies the
        discriminating metadata, so each sibling resolves to its own state.
        `cvid`/`via_source_id` stay sourced from the (shared, interim)
        `variable_instance` for §5.6 lineage until A2.4/A2.7. Still parses the
        interim 5-seg grammar — the 3-seg flip is A2.6.
        """
        # A binding FQID has all five segments populated (parse-validated); the
        # asserts narrow `str | None` → `str` for the typed helpers below.
        assert fqid.provider is not None and fqid.register is not None
        assert fqid.variable is not None
        var = self._lookup_variable(fqid.provider, fqid.register, fqid.variable)
        if var is None:
            return None
        slot = self._lookup_version_slot(fqid)
        if slot is None:
            return None
        state = self._lookup_state(
            var["variable_id"], slot["register_variant_id"], fqid.period
        )
        if state is None:
            return None
        inst = self._lookup_instance(
            var["register_id"],
            slot["register_variant_id"],
            slot["regver_id"],
            var["provider_key"],
        )
        if inst is None:
            return None
        return self._build_state_binding(fqid, var, slot["regver_id"], state, inst)

    def _resolve_binding_via_same_as(
        self, fqid: Fqid
    ) -> ResolvedVariableBinding | None:
        """BFS through `variable_same_as` (variable grain, §5.5) until a target
        variable resolves.

        A2.2 flip: the target resolves **variant-independently** — by
        `variable_id` + period over `variable_state`, across any variant — so a
        cross-register edge whose target register uses *different* variant slugs
        still resolves. The interim direct path inherited the query's variant
        slug into the target register (which it may lack); dropping that
        dependency is what `test_cross_register_same_as_variant_mismatch` pins.
        """
        assert fqid.provider and fqid.register and fqid.variable
        assert fqid.variant is not None and fqid.period is not None
        if (
            fqid.provider,
            fqid.register,
            fqid.variable,
        ) not in self._var_same_as_source_keys():
            return None
        # variant/period are constant across the traversal (the variable-grain
        # edge can't change them). The path FQIDs record the interim 5-seg form
        # under the query's variant; resolution itself is variant-independent
        # (the target register may use other variant slugs).
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
                hit = self._resolve_binding_target_any_variant(
                    n_prov, n_reg, n_variable, period
                )
                if hit is not None:
                    # Restore the caller's FQID; the traversal path (every node
                    # walked, interim 5-seg) goes in via_same_as.
                    return replace(hit, fqid=fqid, via_same_as=new_path)
                queue.append((n_prov, n_reg, n_variable, new_path))
        return None

    # ── A2.2 state-anchored binding-resolution helpers ────────────────────

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

    def _lookup_version_slot(self, fqid: Fqid) -> sqlite3.Row | None:
        """register_variant_id + regver_id for the FQID's (variant, period)."""
        return self._conn.execute(
            "SELECT rver.regver_id, rv.register_variant_id "
            "FROM register_version rver "
            "JOIN register_variant rv "
            "    ON rver.register_variant_id = rv.register_variant_id "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ? AND rver.slug = ?",
            (fqid.provider, fqid.register, fqid.variant, fqid.period),
        ).fetchone()

    def _overlapping_states(
        self,
        variable_id: int,
        register_variant_id: int | None,
        period: str | None,
    ) -> list[sqlite3.Row]:
        """All `variable_state` rows for the variable whose validity range covers
        `period`, **latest-era first** (most recent `valid_from`/`valid_to`, then
        deterministic). `register_variant_id` None spans every variant (the
        variable-grain `same_as` path). A yearless period (`_default`) returns
        every state in order.

        INTERIM precision limit (Codex #139, deferred): `variable_state` validity
        is **year-granular** (the coalescer year-expands), so for a sub-annual
        variant with two editions in one calendar year (`HT2018` / `VT2018`) both
        states cover all of 2018 and can't be told apart here — resolving one term
        may return the other term's `delivery_column_name`/`value_set_version_label`
        (the `cvid` is still edition-exact via `_lookup_instance`). The fix needs
        sub-annual state bounds or carrying the regver into selection — part of
        the A2.5/A2.6 resolver+state-model rework, not this interim flip."""
        if register_variant_id is not None:
            rows = self._conn.execute(
                "SELECT state_id, register_variant_id, data_type, data_length, "
                "delivery_column_name, value_set_id, value_set_version_label, "
                "valid_from, valid_to FROM variable_state "
                "WHERE variable_id = ? AND register_variant_id = ? "
                "ORDER BY valid_from DESC, valid_to DESC, value_set_version_label, "
                "state_id",
                (variable_id, register_variant_id),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT state_id, register_variant_id, data_type, data_length, "
                "delivery_column_name, value_set_id, value_set_version_label, "
                "valid_from, valid_to FROM variable_state WHERE variable_id = ? "
                "ORDER BY valid_from DESC, valid_to DESC, value_set_version_label, "
                "register_variant_id, state_id",
                (variable_id,),
            ).fetchall()
        year = extract_year(period or "")
        if year is None:
            return rows
        lo, hi = f"{year:04d}-01-01", f"{year:04d}-12-31"
        return [r for r in rows if r["valid_from"] <= hi and r["valid_to"] >= lo]

    def _lookup_state(
        self, variable_id: int, register_variant_id: int, period: str | None
    ) -> sqlite3.Row | None:
        """The variable's latest-era `variable_state` covering `period` within
        the pinned `register_variant_id` (the direct path). When several states
        co-deliver the period (a §5.7 multi-vintage fold), the interim point
        resolver returns the latest-era one rather than an arbitrary lexical
        pick; A2.5 `resolve_at` lists all and a `@value_set_version` FQID
        selector (A2.6) narrows to one."""
        states = self._overlapping_states(variable_id, register_variant_id, period)
        return states[0] if states else None

    def _lookup_instance(
        self,
        register_id: int,
        register_variant_id: int,
        regver_id: int | None,
        provider_key: str,
    ) -> sqlite3.Row | None:
        """cvid + via_source_id for §5.6 lineage. Shared across split siblings
        in the interim (they share `provider_key`/`var_id`); A2.4 moves lineage
        onto `variable_state_lineage`. `regver_id` None matches any era (the
        variant-independent `same_as` path)."""
        try:
            var_id = int(provider_key)
        except (TypeError, ValueError):
            return None  # SOS non-numeric provider_key — no interim instance join
        if regver_id is not None:
            return self._conn.execute(
                "SELECT cvid, regver_id, via_source_id FROM variable_instance "
                "WHERE register_id = ? AND register_variant_id = ? "
                "AND regver_id = ? AND var_id = ? ORDER BY cvid LIMIT 1",
                (register_id, register_variant_id, regver_id, var_id),
            ).fetchone()
        return self._conn.execute(
            "SELECT cvid, regver_id, via_source_id FROM variable_instance "
            "WHERE register_id = ? AND register_variant_id = ? AND var_id = ? "
            "ORDER BY cvid LIMIT 1",
            (register_id, register_variant_id, var_id),
        ).fetchone()

    def _variant_slug(self, register_variant_id: int) -> str | None:
        row = self._conn.execute(
            "SELECT slug FROM register_variant WHERE register_variant_id = ?",
            (register_variant_id,),
        ).fetchone()
        return row["slug"] if row else None

    def _resolve_binding_target_any_variant(
        self, provider: str, register: str, variable_slug: str, period: str
    ) -> ResolvedVariableBinding | None:
        """Resolve a `same_as` target by `variable_id` + period across any
        variant (the edge is variable-grain). **Iterates** the period-overlapping
        states (latest-era first) and returns the first whose variant actually
        delivers `period` — i.e. has a matching `register_version` slot AND an
        instance — so a low-id sub-annual variant overlapping the year (e.g. a
        `HT2018`-only variant) can't shadow an annual variant (`2018`) that
        genuinely resolves (Codex P2 #139). The result `fqid` is the target's own
        5-seg binding under the resolved variant; missing the edition (no variant
        delivers `period`) returns None rather than the wrong edition."""
        var = self._lookup_variable(provider, register, variable_slug)
        if var is None:
            return None
        for state in self._overlapping_states(var["variable_id"], None, period):
            rvid = state["register_variant_id"]
            variant_slug = self._variant_slug(rvid)
            if variant_slug is None:
                continue
            try:
                target_fqid = Fqid.binding_fqid(
                    provider, register, variant_slug, period, variable_slug
                )
            except FqidError:
                # Malformed slug — populate_slugs validates on write, so this is
                # a build-invariant break; skip this candidate.
                continue
            slot = self._lookup_version_slot(target_fqid)
            if slot is None:
                continue  # this variant doesn't deliver the period — try the next
            inst = self._lookup_instance(
                var["register_id"], rvid, slot["regver_id"], var["provider_key"]
            )
            if inst is None:
                continue
            return self._build_state_binding(
                target_fqid, var, slot["regver_id"], state, inst
            )
        return None

    def _lineage_fqid(self, source_cvid: int, variable_slug: str) -> Fqid | None:
        """Source-side binding FQID for a consumer instance's `via_source_id`
        (§5.6). The source shares the consumer's variable slug (lineage matches
        on slug). None when any source slug is unpopulated."""
        row = self._conn.execute(
            "SELECT p.slug AS prov, r.slug AS reg, rv.slug AS variant, "
            "rver.slug AS period FROM variable_instance vi "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "JOIN register_variant rv "
            "    ON vi.register_variant_id = rv.register_variant_id "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE vi.cvid = ?",
            (source_cvid,),
        ).fetchone()
        if row is None or not all(
            (row["prov"], row["reg"], row["variant"], row["period"])
        ):
            return None
        try:
            return Fqid.binding_fqid(
                row["prov"], row["reg"], row["variant"], row["period"], variable_slug
            )
        except FqidError:
            return None

    def _build_state_binding(
        self,
        fqid: Fqid,
        var: sqlite3.Row,
        regver_id: int,
        state: sqlite3.Row,
        inst: sqlite3.Row,
    ) -> ResolvedVariableBinding:
        # fqid is always a binding here (direct or same_as target) → variable set.
        assert fqid.variable is not None
        via = inst["via_source_id"]
        lineage = self._lineage_fqid(via, fqid.variable) if via is not None else None
        return ResolvedVariableBinding(
            fqid=fqid,
            cvid=inst["cvid"],
            register_id=var["register_id"],
            register_variant_id=state["register_variant_id"],
            regver_id=regver_id,
            var_id=int(var["provider_key"]),
            variable_name=var["variable_name"],
            delivery_column_name=state["delivery_column_name"],
            state_id=state["state_id"],
            value_set_version_label=state["value_set_version_label"],
            via_source_id=via,
            lineage=lineage,
        )

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
