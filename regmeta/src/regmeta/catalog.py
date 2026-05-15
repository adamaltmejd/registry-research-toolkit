"""Catalog: FQID-to-row resolution against the regmeta SQLite DB.

Implements ``Catalog.resolve(fqid)`` per REFACTOR_SPEC.md §5.8 — the single
entry point that turns any FQID kind into a typed entity row. Slug columns
are populated incrementally (1c onwards); until then most resolves return
``fqid_not_found``, which the caller surfaces as a regular RegmetaError.

Bindings are resolved by deriving the variable slug from the cvid's alias
(§5.3 auto-slugged-from-kolumnnamn rule). Build-time binding rows land in
step 1e; this resolver is the seam they will fill.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .db import db_path_from_args, open_db
from .errors import EXIT_NOT_FOUND, RegmetaError
from .fqid import Fqid, FqidError, FqidKind, derive_variable_slug, parse


@dataclass(frozen=True)
class ResolvedProvider:
    fqid: Fqid
    provider_id: int
    slug: str
    name: str


@dataclass(frozen=True)
class ResolvedRegister:
    fqid: Fqid
    register_id: int
    provider_id: int
    provider_slug: str
    slug: str
    registernamn: str
    registerrubrik: str | None
    registersyfte: str | None


@dataclass(frozen=True)
class ResolvedRegisterVariant:
    fqid: Fqid
    regvar_id: int
    register_id: int
    provider_slug: str
    register_slug: str
    slug: str
    registervariantnamn: str | None
    registervariantrubrik: str | None
    display_group: str | None


@dataclass(frozen=True)
class ResolvedRegisterVersion:
    fqid: Fqid
    regver_id: int
    regvar_id: int
    register_id: int
    provider_slug: str
    register_slug: str
    variant_slug: str
    period: str
    registerversionnamn: str | None


@dataclass(frozen=True)
class ResolvedVariableBinding:
    fqid: Fqid
    cvid: int
    register_id: int
    regvar_id: int
    regver_id: int
    var_id: int
    provider_slug: str
    register_slug: str
    variant_slug: str
    period: str
    variable_slug: str
    variabelnamn: str | None
    kolumnnamn: str | None


@dataclass(frozen=True)
class ResolvedClassification:
    fqid: Fqid
    classification_id: int
    slug: str
    version: str
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
        message=f"FQID does not resolve to any row: {fqid.emit()!r}",
        remediation=(
            "Slug columns are populated incrementally — register/variant "
            "slugs land with step 1c, variable bindings with step 1e. "
            "Use `regmeta search` to locate entities by name."
        ),
    )


class Catalog:
    """FQID resolution against an open regmeta SQLite connection.

    Read-only by construction (the connection opened via ``open()`` is RO).
    Callers that want to manage the connection lifecycle directly can pass
    one to the constructor.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @classmethod
    def open(cls, db_arg: str | Path | None = None) -> Catalog:
        """Open a read-only catalog at the resolved DB path.

        Resolution follows the regular ``--db`` argument shape: explicit
        directory > ``$REGMETA_DB`` > ``$XDG_DATA_HOME/regmeta`` > platform
        default.
        """
        db_arg_str = str(db_arg) if db_arg is not None else None
        path = db_path_from_args(db_arg_str)
        return cls(open_db(path))

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ resolve

    def resolve(self, fqid: str | Fqid) -> ResolvedEntity:
        """Resolve any FQID to its typed entity. Raises on miss."""
        parsed = parse(fqid) if isinstance(fqid, str) else fqid
        if not isinstance(parsed, Fqid):
            raise FqidError(
                f"resolve() expected str or Fqid; got {type(parsed).__name__}"
            )

        kind_dispatch = {
            FqidKind.PROVIDER: self._resolve_provider,
            FqidKind.REGISTER: self._resolve_register,
            FqidKind.REGISTER_VARIANT: self._resolve_variant,
            FqidKind.REGISTER_VERSION: self._resolve_version,
            FqidKind.VARIABLE_BINDING: self._resolve_binding,
            FqidKind.CLASSIFICATION: self._resolve_classification,
        }
        return kind_dispatch[parsed.kind](parsed)

    def _resolve_provider(self, fqid: Fqid) -> ResolvedProvider:
        row = self._conn.execute(
            "SELECT provider_id, slug, name FROM provider WHERE slug = ?",
            (fqid.provider,),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedProvider(
            fqid=fqid,
            provider_id=row["provider_id"],
            slug=row["slug"],
            name=row["name"],
        )

    def _resolve_register(self, fqid: Fqid) -> ResolvedRegister:
        row = self._conn.execute(
            "SELECT r.register_id, r.provider_id, p.slug AS provider_slug, "
            "r.slug, r.registernamn, r.registerrubrik, r.registersyfte "
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
            provider_slug=row["provider_slug"],
            slug=row["slug"],
            registernamn=row["registernamn"],
            registerrubrik=row["registerrubrik"],
            registersyfte=row["registersyfte"],
        )

    def _resolve_variant(self, fqid: Fqid) -> ResolvedRegisterVariant:
        row = self._conn.execute(
            "SELECT rv.regvar_id, rv.register_id, p.slug AS provider_slug, "
            "r.slug AS register_slug, rv.slug, rv.registervariantnamn, "
            "rv.registervariantrubrik, rv.display_group "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
            (fqid.provider, fqid.register, fqid.variant),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedRegisterVariant(
            fqid=fqid,
            regvar_id=row["regvar_id"],
            register_id=row["register_id"],
            provider_slug=row["provider_slug"],
            register_slug=row["register_slug"],
            slug=row["slug"],
            registervariantnamn=row["registervariantnamn"],
            registervariantrubrik=row["registervariantrubrik"],
            display_group=row["display_group"],
        )

    def _resolve_version(self, fqid: Fqid) -> ResolvedRegisterVersion:
        # Period match: extract year via the same helper queries.extract_year
        # uses, scoped to the variant. Today's register_version stores the
        # version name (e.g. "LISA 2018"); we filter Python-side because
        # mass-querying for a known period stays fast at fixture scale and
        # avoids replicating extract_year() in SQL.
        from .queries import extract_year

        rows = self._conn.execute(
            "SELECT rver.regver_id, rver.regvar_id, rv.register_id, "
            "p.slug AS provider_slug, r.slug AS register_slug, "
            "rv.slug AS variant_slug, rver.registerversionnamn "
            "FROM register_version rver "
            "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
            (fqid.provider, fqid.register, fqid.variant),
        ).fetchall()
        target = fqid.period
        # Numeric-year periods match extract_year output; non-year period
        # forms (HT2020, 2020-Q1) match by substring against the version name.
        try:
            target_year = int(target) if target and target.isdigit() else None
        except ValueError:
            target_year = None
        for row in rows:
            name = row["registerversionnamn"] or ""
            if target_year is not None and extract_year(name) == target_year:
                matched = row
                break
            if target_year is None and target in name:
                matched = row
                break
        else:
            raise _not_found(fqid)
        return ResolvedRegisterVersion(
            fqid=fqid,
            regver_id=matched["regver_id"],
            regvar_id=matched["regvar_id"],
            register_id=matched["register_id"],
            provider_slug=matched["provider_slug"],
            register_slug=matched["register_slug"],
            variant_slug=matched["variant_slug"],
            period=target,
            registerversionnamn=matched["registerversionnamn"],
        )

    def _resolve_binding(self, fqid: Fqid) -> ResolvedVariableBinding:
        # Bindings are materialized as DB rows in step 1e. Until then,
        # resolve on-the-fly: locate the cvid via register_version + variable
        # alias, deriving variable slug from kolumnnamn (§5.3).
        version = self._resolve_version(
            Fqid.register_version_fqid(
                fqid.provider, fqid.register, fqid.variant, fqid.period
            )
        )
        rows = self._conn.execute(
            "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.regver_id, "
            "vi.var_id, v.variabelnamn, va.kolumnnamn "
            "FROM variable_instance vi "
            "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
            "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
            "WHERE vi.regver_id = ? "
            "ORDER BY vi.cvid, va.kolumnnamn",
            (version.regver_id,),
        ).fetchall()
        for row in rows:
            slug = derive_variable_slug(row["kolumnnamn"])
            if slug == fqid.variable:
                return ResolvedVariableBinding(
                    fqid=fqid,
                    cvid=row["cvid"],
                    register_id=row["register_id"],
                    regvar_id=row["regvar_id"],
                    regver_id=row["regver_id"],
                    var_id=row["var_id"],
                    provider_slug=version.provider_slug,
                    register_slug=version.register_slug,
                    variant_slug=version.variant_slug,
                    period=version.period,
                    variable_slug=slug,
                    variabelnamn=row["variabelnamn"],
                    kolumnnamn=row["kolumnnamn"],
                )
        raise _not_found(fqid)

    def _resolve_classification(self, fqid: Fqid) -> ResolvedClassification:
        row = self._conn.execute(
            "SELECT id, slug, version, short_name, name FROM classification "
            "WHERE slug = ? AND version = ?",
            (fqid.classification, fqid.version),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedClassification(
            fqid=fqid,
            classification_id=row["id"],
            slug=row["slug"],
            version=row["version"],
            short_name=row["short_name"],
            name=row["name"],
        )
