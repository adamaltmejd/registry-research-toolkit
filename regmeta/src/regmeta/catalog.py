"""Catalog: FQID-to-row resolution against the regmeta SQLite DB.

Implements ``Catalog.resolve(fqid)`` per REFACTOR_SPEC.md §5.8 — the single
entry point that turns any FQID kind into a typed entity row.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .db import db_path_from_args, open_db
from .errors import EXIT_NOT_FOUND, RegmetaError
from .fqid import Fqid, FqidKind, derive_variable_slug, parse
from .queries import extract_year


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
    regvar_id: int
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
        parsed = parse(fqid) if isinstance(fqid, str) else fqid
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
        if not row:
            raise _not_found(fqid)
        return ResolvedRegisterVariant(
            fqid=fqid,
            regvar_id=row["regvar_id"],
            register_id=row["register_id"],
            registervariantnamn=row["registervariantnamn"],
            registervariantrubrik=row["registervariantrubrik"],
            display_group=row["display_group"],
        )

    def _resolve_version(self, fqid: Fqid) -> ResolvedRegisterVersion:
        # Period filtering happens Python-side because `extract_year` is
        # regex-anchored ("v19999" matches no year) — a SQL substring filter
        # would false-positive. Replaced by a direct `period` column once 1c
        # materializes it.
        rows = self._conn.execute(
            "SELECT rver.regver_id, rver.regvar_id, rv.register_id, "
            "rver.registerversionnamn "
            "FROM register_version rver "
            "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
            (fqid.provider, fqid.register, fqid.variant),
        ).fetchall()
        target = fqid.period
        target_year = int(target) if target and target.isdigit() else None
        for row in rows:
            name = row["registerversionnamn"] or ""
            if target_year is not None and extract_year(name) == target_year:
                break
            if target_year is None and target in name:
                break
        else:
            raise _not_found(fqid)
        return ResolvedRegisterVersion(
            fqid=fqid,
            regver_id=row["regver_id"],
            regvar_id=row["regvar_id"],
            register_id=row["register_id"],
            registerversionnamn=row["registerversionnamn"],
        )

    def _resolve_binding(self, fqid: Fqid) -> ResolvedVariableBinding:
        # Bindings are materialized as DB rows in step 1e; until then resolve
        # by scanning instances under the variant and deriving variable slug
        # from kolumnnamn (§5.3). One JOIN'd query so the version match and
        # the alias lookup share a single roundtrip.
        rows = self._conn.execute(
            "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.regver_id, "
            "vi.var_id, v.variabelnamn, va.kolumnnamn, rver.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "JOIN register_variant rv ON vi.regvar_id = rv.regvar_id "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
            "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ? "
            "ORDER BY vi.cvid, va.kolumnnamn",
            (fqid.provider, fqid.register, fqid.variant),
        ).fetchall()
        target = fqid.period
        target_year = int(target) if target and target.isdigit() else None
        for row in rows:
            name = row["registerversionnamn"] or ""
            if target_year is not None and extract_year(name) != target_year:
                continue
            if target_year is None and target not in name:
                continue
            if derive_variable_slug(row["kolumnnamn"]) == fqid.variable:
                return ResolvedVariableBinding(
                    fqid=fqid,
                    cvid=row["cvid"],
                    register_id=row["register_id"],
                    regvar_id=row["regvar_id"],
                    regver_id=row["regver_id"],
                    var_id=row["var_id"],
                    variabelnamn=row["variabelnamn"],
                    kolumnnamn=row["kolumnnamn"],
                )
        raise _not_found(fqid)

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
