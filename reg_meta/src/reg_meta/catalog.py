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
    # `regvar_id is None` marks the §5.1 synthesized `_default` placeholder
    # for variant-less registers — the slot is transparent at resolve time,
    # not backed by a register_variant row.
    regvar_id: int | None
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
    regvar_id: int
    register_id: int
    registerversionnamn: str | None


@dataclass(frozen=True)
class ResolvedVariableBinding:
    fqid: Fqid
    cvid: int
    register_id: int
    regvar_id: int
    regver_id: int
    var_id: int
    # §5.11: SCB `variabelnamn` → `variable_name`; `kolumnnamn` →
    # `delivery_column_name` (the un-prefixed SCB delivery column
    # header — `Kon`, `PersonNr`, etc.).
    variable_name: str | None
    delivery_column_name: str | None
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


# Shared SELECT + FROM + JOINs for queries that emit ResolvedVariableBinding.
# Includes the consumer-side join chain (`*_src`) so the lineage FQID for
# rows with `via_source_id IS NOT NULL` is built directly from the result
# row — no per-row roundtrip. Callers append their WHERE/ORDER BY.
# A2.1.5: the variable slug is read from the stored `variable_state.slug`
# column, not derived from `delivery_column_name` at query time. The slug is
# denormalized per `(register_id, regvar_id, var_id)` triple and invariant
# across eras (the build enforces this), so a correlated subquery returning the
# slug from any one state row is the canonical slug — no era arithmetic, and it
# preserves the one-row-per-(cvid, alias) shape that `_resolve_binding_direct`
# and `editions` iterate (a JOIN would fan the result out across eras).
#
# Bridge note: this still resolves the *binding* off `variable_instance`; A2.5
# moves binding resolution to `variable_state` wholesale. Until then, a
# build-time triage split (A2.2) that mints a NEW var_id for a niva-sibling
# leaves that sibling without a `variable_instance` row, so its stored slug is
# storable+readable but not yet end-to-end resolvable through this query — that
# second sibling waits for A2.5.
_BINDING_QUERY = (
    "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.regver_id, vi.var_id, "
    "vi.via_source_id, "
    "v.name AS variable_name, va.delivery_column_name, "
    "(SELECT vs.slug FROM variable_state vs "
    " WHERE vs.register_id = vi.register_id AND vs.regvar_id = vi.regvar_id "
    " AND vs.var_id = vi.var_id AND vs.slug IS NOT NULL LIMIT 1) "
    "AS variable_slug, "
    "rv.slug AS variant_slug, rver.slug AS version_slug, "
    "p_src.slug AS src_provider_slug, r_src.slug AS src_register_slug, "
    "rv_src.slug AS src_variant_slug, rver_src.slug AS src_version_slug "
    "FROM variable_instance vi "
    "JOIN register_version rver ON vi.regver_id = rver.regver_id "
    "JOIN register_variant rv ON vi.regvar_id = rv.regvar_id "
    "JOIN register r ON vi.register_id = r.register_id "
    "JOIN provider p ON r.provider_id = p.provider_id "
    "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
    "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
    "LEFT JOIN variable_instance vi_src ON vi.via_source_id = vi_src.cvid "
    "LEFT JOIN register_version rver_src ON vi_src.regver_id = rver_src.regver_id "
    "LEFT JOIN register_variant rv_src ON vi_src.regvar_id = rv_src.regvar_id "
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
            "SELECT rv.regvar_id, rv.register_id, rv.name, "
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
                regvar_id=row["regvar_id"],
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
            regvar_id=None,
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
        # is enforced by `UNIQUE(regvar_id, slug)` (db.py), so fetchone is safe.
        #
        # §5.1 follow-up: `_default` versions against a variant-less register
        # aren't reachable today — `register_version.regvar_id` is NOT NULL,
        # so no version row can exist without a real variant row. When SOS
        # ingestion lands, either make the column nullable or extend this
        # resolver to synthesize the variant slot the way `_resolve_variant`
        # does.
        row = self._conn.execute(
            "SELECT rver.regver_id, rver.regvar_id, rv.register_id, "
            "rver.registerversionnamn "
            "FROM register_version rver "
            "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
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
            regvar_id=row["regvar_id"],
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
        # A2.1.5: scan instances under the (variant, version) pair and match the
        # **stored** `variable_state.slug` (carried as `variable_slug` in
        # `_BINDING_QUERY`) instead of deriving from
        # `variable_alias.delivery_column_name` at query time (§5.3). The
        # version match is an exact slug comparison — `register_version.slug`
        # carries the period or curated token. `ORDER BY vi.cvid,
        # va.delivery_column_name` makes first-match deterministic; uniqueness
        # of (variant, version, variable_slug) is a §5.3 invariant.
        assert fqid.variable is not None
        variable_slug = fqid.variable
        rows = self._conn.execute(
            _BINDING_QUERY + "WHERE p.slug = ? AND r.slug = ? "
            "AND rv.slug = ? AND rver.slug = ? "
            "ORDER BY vi.cvid, va.delivery_column_name",
            (fqid.provider, fqid.register, fqid.variant, fqid.period),
        ).fetchall()
        for row in rows:
            if row["variable_slug"] == variable_slug:
                return self._row_to_binding(row, fqid, variable_slug)
        return None

    def _resolve_binding_via_same_as(
        self, fqid: Fqid
    ) -> ResolvedVariableBinding | None:
        """BFS through `variable_same_as` until a candidate resolves directly.

        Empty-string sentinels in the edge's `a_variant`/`a_period` mark
        wildcard scope; on the `b_` side empty means "inherit from current
        node", non-empty means "narrow to this slot". Equivalence traversal
        is the only place where the query's variant/period can change
        mid-resolve. The visited set keys on the **full 5-tuple** (incl.
        inherited variant/period) so two narrowing edges from the same
        source to the same target under different variants/periods both
        get explored.
        """
        assert fqid.provider and fqid.register and fqid.variable
        assert fqid.variant is not None and fqid.period is not None
        if (
            fqid.provider,
            fqid.register,
            fqid.variable,
        ) not in self._var_same_as_source_keys():
            return None
        start_key = (
            fqid.provider,
            fqid.register,
            fqid.variant,
            fqid.period,
            fqid.variable,
        )
        visited: set[tuple[str, str, str, str, str]] = {start_key}
        queue: deque[tuple[str, str, str, str, str, tuple[Fqid, ...]]] = deque()
        queue.append((*start_key, ()))
        while queue:
            prov, reg, variant, period, variable, path = queue.popleft()
            rows = self._conn.execute(
                "SELECT b_provider, b_register, b_variant, b_period, b_variable "
                "FROM variable_same_as "
                "WHERE a_provider = ? AND a_register = ? AND a_variable = ? "
                "AND (a_variant = '' OR a_variant = ?) "
                "AND (a_period = '' OR a_period = ?)",
                (prov, reg, variable, variant, period),
            ).fetchall()
            for row in rows:
                n_prov = row["b_provider"]
                n_reg = row["b_register"]
                n_variant = row["b_variant"] or variant
                n_period = row["b_period"] or period
                n_variable = row["b_variable"]
                key = (n_prov, n_reg, n_variant, n_period, n_variable)
                if key in visited:
                    continue
                visited.add(key)
                try:
                    n_fqid = Fqid.binding_fqid(
                        n_prov, n_reg, n_variant, n_period, n_variable
                    )
                except FqidError:
                    # Malformed slug in DB — populate_slugs validates on write,
                    # so this means a build-time invariant broke. Skip the
                    # candidate rather than wrap a "not found" in a cryptic
                    # FqidError trace.
                    continue
                new_path = (*path, n_fqid)
                hit = self._resolve_binding_direct(n_fqid)
                if hit is not None:
                    # Preserve the caller's FQID on the result; the
                    # traversal path goes in via_same_as.
                    return replace(hit, fqid=fqid, via_same_as=new_path)
                queue.append((n_prov, n_reg, n_variant, n_period, n_variable, new_path))
        return None

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
            regvar_id=row["regvar_id"],
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
