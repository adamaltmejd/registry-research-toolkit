"""Catalog: FQID-to-row resolution against the regmeta SQLite DB.

Implements the FQID API in REFACTOR_SPEC.md §5.8: ``Catalog.resolve(fqid)``
turns any FQID kind into a typed entity row, ``Catalog.editions(...)``
enumerates all variable bindings of a given variable slug under a register.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .db import db_path_from_args, open_db
from .errors import EXIT_NOT_FOUND, RegmetaError
from .fqid import (
    DEFAULT_VARIANT_SLUG,
    Fqid,
    FqidError,
    FqidKind,
    derive_variable_slug,
    parse,
    validate_slug,
)


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
    registernamn: str
    registerrubrik: str | None
    registersyfte: str | None


@dataclass(frozen=True)
class ResolvedRegisterVariant:
    fqid: Fqid
    # `regvar_id is None` marks the §5.1 synthesized `_default` placeholder
    # for variant-less registers — the slot is transparent at resolve time,
    # not backed by a register_variant row.
    regvar_id: int | None
    register_id: int
    registervariantnamn: str | None
    registervariantrubrik: str | None
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
    variabelnamn: str | None
    kolumnnamn: str | None
    # §5.6 lineage: source cvid + source binding FQID. NULL on canonical bindings.
    via_source_id: int | None = None
    lineage: Fqid | None = None


@dataclass(frozen=True)
class ResolvedClassification:
    fqid: Fqid
    classification_id: int
    short_name: str
    name: str


ResolvedEntity = (
    ResolvedProvider
    | ResolvedRegister
    | ResolvedRegisterVariant
    | ResolvedRegisterVersion
    | ResolvedVariableBinding
    | ResolvedClassification
)


def _not_found(fqid: Fqid) -> RegmetaError:
    return RegmetaError(
        exit_code=EXIT_NOT_FOUND,
        code="fqid_not_found",
        error_class="query",
        message=f"FQID does not resolve to any row: {fqid!s}",
        remediation="Use `regmeta search` to locate entities by name or ID.",
    )


class Catalog:
    """FQID resolution against an open regmeta SQLite connection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

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
        row = self._conn.execute(
            "SELECT r.register_id, r.provider_id, r.registernamn, "
            "r.registerrubrik, r.registersyfte "
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
            registernamn=row["registernamn"],
            registerrubrik=row["registerrubrik"],
            registersyfte=row["registersyfte"],
        )

    def _resolve_variant(self, fqid: Fqid) -> ResolvedRegisterVariant:
        row = self._conn.execute(
            "SELECT rv.regvar_id, rv.register_id, rv.registervariantnamn, "
            "rv.registervariantrubrik, rv.display_group "
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
                registervariantnamn=row["registervariantnamn"],
                registervariantrubrik=row["registervariantrubrik"],
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
            registervariantnamn=None,
            registervariantrubrik=None,
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
        # No materialized binding rows yet: scan instances under the
        # (variant, version) pair and derive variable slug from kolumnnamn
        # (§5.3). The version match is now an exact slug comparison
        # (`register_version.slug` carries the period or curated token);
        # `_resolve_version`'s same column query mirrors this.
        # `ORDER BY vi.cvid, va.kolumnnamn` makes first-match deterministic;
        # uniqueness of (variant, version, variable_slug) is a §5.3 invariant.
        rows = self._conn.execute(
            "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.regver_id, "
            "vi.var_id, vi.via_source_id, "
            "v.variabelnamn, va.kolumnnamn, rver.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "JOIN register_variant rv ON vi.regvar_id = rv.regvar_id "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
            "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ? AND rver.slug = ? "
            "ORDER BY vi.cvid, va.kolumnnamn",
            (fqid.provider, fqid.register, fqid.variant, fqid.period),
        ).fetchall()
        for row in rows:
            if derive_variable_slug(row["kolumnnamn"]) == fqid.variable:
                via = row["via_source_id"]
                return ResolvedVariableBinding(
                    fqid=fqid,
                    cvid=row["cvid"],
                    register_id=row["register_id"],
                    regvar_id=row["regvar_id"],
                    regver_id=row["regver_id"],
                    var_id=row["var_id"],
                    variabelnamn=row["variabelnamn"],
                    kolumnnamn=row["kolumnnamn"],
                    via_source_id=via,
                    lineage=(
                        self._lineage_fqid(via, fqid.variable)
                        if via is not None
                        else None
                    ),
                )
        raise _not_found(fqid)

    def _lineage_fqid(self, via_cvid: int, variable_slug: str) -> Fqid | None:
        """Source-side binding FQID for a consumer instance, or None if any
        slug component is missing."""
        row = self._conn.execute(
            "SELECT p.slug AS provider_slug, r.slug AS register_slug, "
            "rv.slug AS variant_slug, rver.slug AS version_slug "
            "FROM variable_instance vi "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "JOIN register_variant rv ON vi.regvar_id = rv.regvar_id "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE vi.cvid = ?",
            (via_cvid,),
        ).fetchone()
        if not row:
            return None
        parts = (
            row["provider_slug"],
            row["register_slug"],
            row["variant_slug"],
            row["version_slug"],
            variable_slug,
        )
        if any(p is None or p == "" for p in parts):
            return None
        try:
            return Fqid.binding_fqid(*parts)
        except FqidError:
            return None

    def editions(
        self, *, provider: str, register: str, variable: str
    ) -> list[ResolvedVariableBinding]:
        """All variable bindings of ``variable`` under ``provider/register``.

        Returns every ``(variant, period)`` combination where a
        variable_instance row exists whose kolumnnamn folds to ``variable``,
        including consumer-side bindings from §5.6 (their ``lineage`` field
        carries the source-side FQID). Results are ordered by
        ``(variant_slug, version_slug, cvid, kolumnnamn)`` for deterministic
        iteration.

        Slug inputs are validated; non-existent provider/register/variable
        yields an empty list (discovery, not resolution).
        """
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variable, "variable")

        rows = self._conn.execute(
            "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.regver_id, "
            "vi.var_id, vi.via_source_id, "
            "v.variabelnamn, va.kolumnnamn, "
            "rv.slug AS variant_slug, rver.slug AS version_slug "
            "FROM variable_instance vi "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "JOIN register_variant rv ON vi.regvar_id = rv.regvar_id "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
            "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
            "WHERE p.slug = ? AND r.slug = ? "
            "ORDER BY rv.slug, rver.slug, vi.cvid, va.kolumnnamn",
            (provider, register),
        ).fetchall()

        out: list[ResolvedVariableBinding] = []
        for row in rows:
            if derive_variable_slug(row["kolumnnamn"]) != variable:
                continue
            variant_slug = row["variant_slug"]
            version_slug = row["version_slug"]
            # Skip rows whose slug columns aren't populated (NULL on pre-1c
            # DBs or partial fixtures) — they can't be addressed by FQID.
            if not variant_slug or not version_slug:
                continue
            try:
                fqid = Fqid.binding_fqid(
                    provider, register, variant_slug, version_slug, variable
                )
            except FqidError:
                continue
            via = row["via_source_id"]
            out.append(
                ResolvedVariableBinding(
                    fqid=fqid,
                    cvid=row["cvid"],
                    register_id=row["register_id"],
                    regvar_id=row["regvar_id"],
                    regver_id=row["regver_id"],
                    var_id=row["var_id"],
                    variabelnamn=row["variabelnamn"],
                    kolumnnamn=row["kolumnnamn"],
                    via_source_id=via,
                    lineage=(
                        self._lineage_fqid(via, variable) if via is not None else None
                    ),
                )
            )
        return out

    def _resolve_classification(self, fqid: Fqid) -> ResolvedClassification:
        row = self._conn.execute(
            "SELECT id, short_name, name FROM classification "
            "WHERE slug = ? AND version = ?",
            (fqid.classification, fqid.version),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedClassification(
            fqid=fqid,
            classification_id=row["id"],
            short_name=row["short_name"],
            name=row["name"],
        )


_DISPATCH = {
    FqidKind.PROVIDER: Catalog._resolve_provider,
    FqidKind.REGISTER: Catalog._resolve_register,
    FqidKind.REGISTER_VARIANT: Catalog._resolve_variant,
    FqidKind.REGISTER_VERSION: Catalog._resolve_version,
    FqidKind.VARIABLE_BINDING: Catalog._resolve_binding,
    FqidKind.CLASSIFICATION: Catalog._resolve_classification,
}
