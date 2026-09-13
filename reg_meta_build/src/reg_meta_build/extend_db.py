"""Insert-only steward catalog overlay built from curated-provider TOMLs.

``extend-db`` copies a released global database, loads every ``*.toml`` in a
steward provider directory through :class:`sources.curated.CuratedAdapter`, and
inserts that IR into the copy. The released base is never mutated. The separate
delivery-inventory TOML remains the steward holdings statement used by the
post-overlay Section 12 coverage gate.
"""

from __future__ import annotations

import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta.db import DB_FILENAME
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidError, FqidKind, validate_slug

from .id import mint
from .ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)
from .sources.curated import CuratedAdapter

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class _ProviderGraph:
    providers: tuple[tuple[str, str], ...]
    registers: tuple[IRRegister, ...]
    variants: tuple[IRVariant, ...]
    variables: tuple[IRVariable, ...]
    states: tuple[IRVariableState, ...]
    aliases: tuple[IRVariableAlias, ...]
    alias_windows: tuple[IRVariableAliasWindow, ...]


def _cfg_error(message: str, remediation: str) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code="extend_providers_invalid",
        error_class="configuration",
        message=message,
        remediation=remediation,
    )


def resolve_steward_providers_dir(providers_dir: Path | None, steward: str) -> Path:
    """Resolve an explicit provider directory or the checkout's steward input."""
    resolved = (
        providers_dir.expanduser().resolve()
        if providers_dir is not None
        else (
            Path(__file__).resolve().parents[2] / "input_data" / steward / "providers"
        ).resolve()
    )
    if not resolved.is_dir():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="extend_providers_dir_not_found",
            error_class="configuration",
            message=f"Steward providers directory not found: {resolved}",
            remediation=(
                "Pass --providers-dir or author one curated-provider TOML per "
                f"provider under input_data/{steward}/providers/."
            ),
        )
    return resolved


def _load_provider_ir(providers_dir: Path, steward: str) -> _ProviderGraph:
    """Load all steward provider TOMLs through the shared curated adapter."""
    paths = sorted(providers_dir.glob("*.toml"), key=lambda path: path.name)
    if not paths:
        raise _cfg_error(
            f"Steward providers directory has no provider TOMLs: {providers_dir}",
            "Author one <provider>.toml file or point --providers-dir at the delivery.",
        )

    providers: list[tuple[str, str]] = []
    registers: list[IRRegister] = []
    variants: list[IRVariant] = []
    variables: list[IRVariable] = []
    states: list[IRVariableState] = []
    aliases: list[IRVariableAlias] = []
    alias_windows: list[IRVariableAliasWindow] = []
    for path in paths:
        provider = path.stem
        try:
            validate_slug(provider, FqidKind.PROVIDER)
        except FqidError as exc:
            raise _cfg_error(
                f"Provider TOML basename {provider!r} is not a valid provider slug: {exc}",
                "Rename the file to a valid, non-reserved <provider>.toml basename.",
            ) from exc

        adapter = CuratedAdapter(provider, steward=steward)
        for obj in adapter.emit(providers_dir):
            if isinstance(obj, IRRegister):
                registers.append(obj)
            elif isinstance(obj, IRVariant):
                variants.append(obj)
            elif isinstance(obj, IRVariable):
                variables.append(obj)
            elif isinstance(obj, IRVariableState):
                states.append(obj)
            elif isinstance(obj, IRVariableAlias):
                aliases.append(obj)
            elif isinstance(obj, IRVariableAliasWindow):
                alias_windows.append(obj)
            else:
                raise _cfg_error(
                    f"Provider {provider!r} emitted unsupported IR {type(obj).__name__}.",
                    "Remove unsupported classification/value-set content from the flavor.",
                )
        if adapter.classification_candidates:
            raise _cfg_error(
                f"Provider {provider!r} declares classification linkage, which the "
                "steward overlay does not materialize.",
                "Remove `classification`; steward classification linkage is not part "
                "of this delivery contract.",
            )
        assert adapter.provider_name is not None
        providers.append((provider, adapter.provider_name))

    return _ProviderGraph(
        providers=tuple(providers),
        registers=tuple(registers),
        variants=tuple(variants),
        variables=tuple(variables),
        states=tuple(states),
        aliases=tuple(aliases),
        alias_windows=tuple(alias_windows),
    )


def _provider_id_by_slug(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        slug: provider_id
        for provider_id, slug in conn.execute("SELECT provider_id, slug FROM provider")
    }


def _insert_providers(
    conn: sqlite3.Connection, providers: tuple[tuple[str, str], ...]
) -> int:
    """Mint steward providers; matching existing rows are idempotent."""
    existing = dict(conn.execute("SELECT slug, name FROM provider"))
    inserted = 0
    for slug, name in providers:
        if slug in existing:
            if existing[slug] != name:
                raise _cfg_error(
                    f"extend-db provider {slug!r} already has name "
                    f"{existing[slug]!r}; its TOML gives {name!r}.",
                    "Reconcile the provider name with the released base DB.",
                )
            continue
        conn.execute(
            "INSERT INTO provider (provider_id, slug, name) VALUES (?, ?, ?)",
            (mint("provider", slug), slug, name),
        )
        inserted += 1
    return inserted


def _assert_steward_rows_slugged(conn: sqlite3.Connection) -> None:
    """Fail when a steward register or variant remains unaddressable."""
    missing: list[str] = []
    for row in conn.execute(
        "SELECT p.slug, r.register_id, r.name FROM register r "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE p.slug NOT IN ('scb', 'sos') AND r.slug IS NULL "
        "ORDER BY r.register_id"
    ):
        missing.append(f"register {row[0]}#{row[1]} ({row[2]!r})")
    for row in conn.execute(
        "SELECT p.slug, rv.register_variant_id, rv.name FROM register_variant rv "
        "JOIN register r ON r.register_id = rv.register_id "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE p.slug NOT IN ('scb', 'sos') AND rv.slug IS NULL "
        "ORDER BY rv.register_variant_id"
    ):
        missing.append(f"register_variant {row[0]}#{row[1]} ({row[2]!r})")
    if missing:
        raise _cfg_error(
            f"{len(missing)} steward register/variant row(s) have no slug after "
            f"slug population. Sample: {'; '.join(missing[:5])}.",
            "Add the missing slug pins to fqid_slugs/<steward>/ or pass --skip-slugs.",
        )


def resolve_steward_slug_dir(
    slug_dir: Path | None, steward: str, *, skip_slugs: bool
) -> Path | None:
    """Resolve the steward slug dir, or ``None`` when slugging is skipped."""
    if skip_slugs:
        return None
    if slug_dir is not None:
        resolved = slug_dir.expanduser().resolve()
    else:
        from .fqid_slugs import repo_slug_dir

        global_dir = repo_slug_dir()
        if global_dir is None:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="extend_slug_dir_not_found",
                error_class="configuration",
                message=(
                    "No --slug-dir given and no repo checkout found for the "
                    f"steward slug dir (fqid_slugs/{steward}/)."
                ),
                remediation=(
                    "Pass --slug-dir, run from a repo checkout, or use --skip-slugs."
                ),
            )
        resolved = global_dir / steward
    if not resolved.is_dir():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="extend_slug_dir_not_found",
            error_class="configuration",
            message=f"Steward slug dir not found: {resolved}",
            remediation=(
                f"Create fqid_slugs/{steward}/ (defaults to churning) or "
                "pass --skip-slugs."
            ),
        )
    return resolved


def resolve_delivery_inventory(
    inventory_path: Path | None, steward: str, *, skip_holdings_gate: bool
) -> Path | None:
    """Resolve the distinct Section 12 steward holdings statement."""
    if skip_holdings_gate:
        return None
    if inventory_path is not None:
        return inventory_path.expanduser().resolve()
    candidate = (
        Path(__file__).resolve().parents[3]
        / "reg_webapp"
        / "stewards"
        / steward
        / "inventory.toml"
    )
    if not candidate.is_file():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="extend_delivery_inventory_not_found",
            error_class="configuration",
            message=(
                "No --delivery-inventory given and no committed holdings "
                f"statement for steward {steward!r} at {candidate}."
            ),
            remediation=(
                "Pass --delivery-inventory, run from a repo checkout carrying "
                f"reg_webapp/stewards/{steward}/inventory.toml, or pass "
                "--skip-holdings-gate (the flavor then ships unchecked against "
                "the steward's holdings)."
            ),
        )
    return candidate


def extend_db(
    base_db: Path,
    providers_dir: Path | None,
    db_dir: Path,
    *,
    steward: str,
    slug_dir: Path | None = None,
    skip_slugs: bool = False,
    pre_rename_hook: Callable[[Path], None] | None = None,
) -> dict[str, Any]:
    """Apply steward curated-provider IR to a copy of ``base_db``."""
    from .db import (
        _insert_core_graph_from_ir,
        _populate_fts,
        _progress,
        _unlink_wal_sidecars,
        publish_db,
    )
    from .fqid_slugs import populate_slugs, populate_variable_slugs

    base_db = base_db.expanduser().resolve()
    db_dir = db_dir.expanduser().resolve()

    if not base_db.is_file():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="extend_base_db_not_found",
            error_class="configuration",
            message=f"Base global DB not found: {base_db}",
            remediation="Pass --base-db pointing at a released reg_meta.db.",
        )
    if base_db == (db_dir / DB_FILENAME):
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="extend_base_db_is_output",
            error_class="configuration",
            message=(
                f"--base-db {base_db} resolves to the output DB path; the base is "
                "read-only and must differ from the output."
            ),
            remediation="Point --db at a different output directory.",
        )

    resolved_providers_dir = resolve_steward_providers_dir(providers_dir, steward)
    graph = _load_provider_ir(resolved_providers_dir, steward)
    steward_slug_dir = resolve_steward_slug_dir(
        slug_dir, steward, skip_slugs=skip_slugs
    )

    db_dir.mkdir(parents=True, exist_ok=True)
    final_path = db_dir / DB_FILENAME
    tmp_path = final_path.with_suffix(".db.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    shutil.copy2(base_db, tmp_path)

    conn = sqlite3.connect(tmp_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=OFF")
    write_failed = True
    counts = {
        "providers": 0,
        "registers": len(graph.registers),
        "variants": len(graph.variants),
        "variables": len(graph.variables),
        "states": len(graph.states),
    }
    try:
        _progress(f"extend-db: overlaying steward {steward!r} onto {base_db.name}")
        counts["providers"] = _insert_providers(conn, graph.providers)
        _insert_core_graph_from_ir(
            conn,
            registers=list(graph.registers),
            variants=list(graph.variants),
            variables=list(graph.variables),
            states=list(graph.states),
            aliases=list(graph.aliases),
            alias_windows=list(graph.alias_windows),
            provider_ids=_provider_id_by_slug(conn),
        )

        if not skip_slugs:
            assert steward_slug_dir is not None
            populate_slugs(conn, steward_slug_dir, strict=False)
            populate_variable_slugs(conn, steward_slug_dir, incremental=True)
            _assert_steward_rows_slugged(conn)

        for fts in ("register_fts", "variable_fts"):
            conn.execute(f"INSERT INTO {fts}({fts}) VALUES('delete-all')")
        _populate_fts(conn, include_value_code=False)

        violations = list(conn.execute("PRAGMA foreign_key_check"))
        if violations:
            sample = ", ".join(f"{v[0]}#{v[1]}" for v in violations[:5])
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="foreign_key_violation",
                error_class="configuration",
                message=(
                    f"PRAGMA foreign_key_check returned {len(violations)} "
                    f"violation(s) before commit. Sample: {sample}."
                ),
                remediation="Inspect the curated provider's register/variant references.",
            )
        conn.execute("PRAGMA foreign_keys=ON")
        conn.commit()
        write_failed = False
    finally:
        conn.close()
        if write_failed:
            tmp_path.unlink(missing_ok=True)

    if pre_rename_hook is not None:
        try:
            pre_rename_hook(tmp_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            _unlink_wal_sidecars(tmp_path)
            raise

    publish_db(tmp_path, final_path)
    _progress(f"Flavored database written to {final_path}")
    return {**counts, "db_path": str(final_path)}
