"""Insert-only steward catalog overlay built from curated-provider TOMLs.

``extend-db`` copies a released global database, loads every ``*.toml`` in a
steward provider directory through :class:`sources.curated.CuratedAdapter`, and
inserts that IR into the copy. The released base is never mutated. The separate
delivery-inventory TOML remains the steward holdings statement used by the
post-overlay Section 12 coverage gate.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import shutil
import sqlite3
import tomllib
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from reg_meta.catalog import DataWarning
from reg_meta.db import DB_FILENAME
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidError, FqidKind, validate_slug
from reg_meta.source_evidence import canonical_sha256

from .data_warnings import write_data_warnings
from .db import open_built_db
from .id import mint
from .ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)
from .source_records import TemporalScope
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


def _load_provider_ir(
    providers_dir: Path,
    steward: str,
    classification_short_names: frozenset[str] = frozenset(),
) -> _ProviderGraph:
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

        adapter = CuratedAdapter(
            provider,
            steward=steward,
            classification_short_names=classification_short_names,
        )
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
        resolved = global_dir.parent / "fqid_slugs" / steward
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
    data_warnings: tuple[DataWarning, ...] = (),
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

    data_warnings = TypeAdapter(tuple[DataWarning, ...]).validate_python(
        data_warnings, strict=True
    )
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
    with closing(open_built_db(base_db)) as source_db:
        classification_short_names = frozenset(
            name
            for (name,) in source_db.execute("SELECT short_name FROM classification")
        )
    graph = _load_provider_ir(
        resolved_providers_dir, steward, classification_short_names
    )
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
        "data_warnings": len(data_warnings),
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

        write_data_warnings(conn, data_warnings)

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


class _UndatedHolding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    table: str = Field(min_length=1)
    register_fqid: str = Field(alias="register")
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str = Field(min_length=1)


def load_undated_holdings(
    policy_path: Path,
    source_path: Path,
    *,
    input_commit: str,
    input_manifest_sha256: str,
) -> tuple[DataWarning, ...]:
    """Validate exact undated holdings evidence; retain no annual admission links."""
    if not re.fullmatch(r"[0-9a-f]{40}", input_commit) or not re.fullmatch(
        r"[0-9a-f]{64}", input_manifest_sha256
    ):
        raise ValueError("Undated holdings require full accepted input pins")
    entries, rows_by_table, source_sha256 = load_holdings_retention_policy(
        policy_path, source_path
    )
    warnings = []
    for entry in sorted(entries, key=lambda item: item.table):
        rows = rows_by_table.get(entry.table)
        evidence = {
            "table": entry.table,
            "rows": rows,
            "source_sha256": source_sha256,
            "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest(),
            "input_commit": input_commit,
            "input_manifest_sha256": input_manifest_sha256,
            "temporal_scope": TemporalScope(
                kind="unknown", label=entry.reason
            ).model_dump(mode="json"),
        }
        detail = json.dumps(
            evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        payload = {
            "register_fqid": entry.register_fqid,
            "variable_fqid": None,
            "variant": None,
            "delivery_column_name": None,
            "valid_from": None,
            "valid_to": None,
            "code": "unknown_holding_edition",
            "severity": "warning",
            "summary": "A delivered table has no established calendar coverage",
            "detail": detail,
            "diagnostic_detail_sha256": canonical_sha256(evidence),
            "source_subject": entry.table,
            "fields": ["inventory.edition"],
            "refs": [],
            "withheld_output": ["annual_availability", "catalog_ordering"],
            "acknowledged_by": "source_policy",
            "case_id": entry.table,
        }
        # Normalize optional defaults before deriving the content identity.
        warning = DataWarning.model_validate_json(
            json.dumps({"warning_id": canonical_sha256(payload), **payload})
        )
        warnings.append(warning)
    return tuple(warnings)


def load_private_holdings_warnings(
    root: Path,
    *,
    input_commit: str,
    input_manifest_sha256: str,
) -> tuple[DataWarning, ...]:
    """Read only exact committed private input bytes before extension copies its base."""
    from ._accepted_prepared import read_accepted_manifest
    from .input_snapshot import _git, _git_bytes, _index_tags

    selection = read_accepted_manifest(
        root, expected_sha256=input_manifest_sha256, input_commit=input_commit
    )
    manifest = json.loads(selection.manifest_bytes)
    if manifest.get("artifact_kind") != "swecov-private-extension-input-candidate":
        raise ValueError("Not a private SWECOV extension input candidate")
    proofs = {proof["path"]: proof for proof in manifest["files"]}
    if len(proofs) != len(manifest["files"]):
        raise ValueError("Private input manifest has duplicate files")
    committed_names = {
        name.removeprefix(selection.relative + "/")
        if selection.relative != "."
        else name
        for name in _git(selection.repository, "ls-files").splitlines()
    }
    actual_names = {
        path.relative_to(selection.root).as_posix()
        for path in selection.root.rglob("*")
        if path.is_file() and ".git" not in path.relative_to(selection.root).parts
    }
    if committed_names != {*proofs, "manifest.json"} or actual_names != committed_names:
        raise ValueError("Private input manifest does not cover the complete candidate")
    tags = _index_tags(selection.repository)
    for name, proof in proofs.items():
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != name:
            raise ValueError(f"Unsafe private input manifest path: {name}")
        target = selection.root / path
        if any(part.is_symlink() for part in (target, *target.parents)):
            raise ValueError(f"Private input manifest member is a symlink: {name}")
        member = (Path(selection.relative) / path).as_posix()
        if tags.get(member) != "H":
            raise ValueError(
                f"Private input member is not ordinarily materialized: {name}"
            )
        committed = _git_bytes(
            selection.repository, "cat-file", "blob", f"{input_commit}:{member}"
        )
        if (
            len(committed) != proof["size"]
            or hashlib.sha256(committed).hexdigest() != proof["sha256"]
            or target.read_bytes() != committed
        ):
            raise ValueError(
                f"Private input manifest member differs from accepted bytes: {name}"
            )
    policy_name = "policy/holdings_policy.toml"
    source_names = [
        name
        for name in proofs
        if name.startswith("swecov/SWECOV_variables_full_") and name.endswith(".csv")
    ]
    if policy_name not in proofs or len(source_names) != 1:
        raise ValueError(
            "Private input manifest requires one holdings CSV and holdings policy"
        )
    undated = load_undated_holdings(
        selection.root / policy_name,
        selection.root / source_names[0],
        input_commit=input_commit,
        input_manifest_sha256=input_manifest_sha256,
    )
    return undated + _private_unknown_validity_warnings(
        selection.root,
        input_commit=input_commit,
        input_manifest_sha256=input_manifest_sha256,
    )


def load_holdings_retention_policy(
    policy_path: Path,
    source_path: Path,
) -> tuple[tuple[_UndatedHolding, ...], dict[str, list[dict[str, Any]]], str]:
    """Check exact source and ordered-row guards for inventory and warning emission."""
    policy = tomllib.loads(policy_path.read_text(encoding="utf-8"))
    source = source_path.read_bytes()
    source_sha256 = hashlib.sha256(source).hexdigest()
    if policy.get("source_sha256") != source_sha256:
        raise ValueError("Undated holdings source SHA-256 does not match policy")
    entries = TypeAdapter(tuple[_UndatedHolding, ...]).validate_python(
        policy.get("retain_unknown", [])
    )
    if len({entry.table for entry in entries}) != len(entries):
        raise ValueError("Duplicate undated holdings table")
    rows_by_table: dict[str, list[dict[str, Any]]] = {}
    for line, cells in enumerate(csv.reader(io.StringIO(source.decode("utf-8"))), 1):
        if len(cells) >= 3:
            rows_by_table.setdefault(cells[2].strip(), []).append(
                {"line": line, "cells": cells}
            )
    excluded = {entry["table"] for entry in policy.get("exclude", [])}
    for entry in entries:
        rows = rows_by_table.get(entry.table)
        if not rows or canonical_sha256(rows) != entry.rows_sha256:
            raise ValueError(f"Undated holdings row guard failed: {entry.table}")
        if entry.table in excluded:
            raise ValueError(f"Retained holding cannot also be excluded: {entry.table}")
    return entries, rows_by_table, source_sha256


def _private_unknown_validity_warnings(
    root: Path,
    *,
    input_commit: str,
    input_manifest_sha256: str,
) -> tuple[DataWarning, ...]:
    """Keep source validity absence separate from witnessed holdings editions."""
    from reg_meta.inventory import load_inventory

    from .fqid_slugs import load_slug_dir

    graph = _load_provider_ir(root / "providers", "swecov")
    names = {
        (entry.provider, entry.source_id): entry.slug
        for entry in load_slug_dir(root / "slugs")
        if entry.kind == "register"
    }
    inventory = load_inventory(root / "policy" / "inventory.toml")
    variables = {value.variable_id: value for value in graph.variables}
    warnings = []
    for register in graph.registers:
        states = tuple(
            state
            for state in graph.states
            if variables[state.variable_id].register_id == register.register_id
            and (state.valid_from is None or state.valid_to is None)
        )
        if not states:
            continue
        slug = names.get((register.provider, str(register.register_id)))
        if slug is None:
            raise ValueError(
                f"Private warning register lacks accepted naming: {register.register_id}"
            )
        register_fqid = f"{register.provider}/{slug}"
        variable_ids = {state.variable_id for state in states}
        table_witnesses = [
            table.model_dump(mode="json")
            for table in inventory.tables
            if any(
                mapping.register_variant.startswith(register_fqid + "/")
                for column in table.columns
                for mapping in column.mappings
            )
        ]
        evidence = {
            "input_commit": input_commit,
            "input_manifest_sha256": input_manifest_sha256,
            "states": [state.model_dump(mode="json") for state in states],
            "aliases": [
                alias.model_dump(mode="json")
                for alias in graph.aliases
                if alias.variable_id in variable_ids
            ],
            "alias_windows": [
                window.model_dump(mode="json")
                for window in graph.alias_windows
                if window.variable_id in variable_ids
            ],
            "holding_table_witnesses": table_witnesses,
            "source_validity": "Unknown where the authored state bound is absent.",
            "storage_sentinels": {
                "missing_start": "0001-01-01",
                "missing_end": "9999-12-31",
            },
            "interpretation": "Storage sentinels do not establish observed calendar coverage or year independence. Holdings editions witness delivered availability only; no additional annual alias equivalence is asserted.",
        }
        payload = {
            "register_fqid": register_fqid,
            "variable_fqid": None,
            "variant": None,
            "delivery_column_name": None,
            "valid_from": None,
            "valid_to": None,
            "code": "unknown_source_validity",
            "severity": "warning",
            "summary": "Private source state validity bounds are undocumented",
            "detail": json.dumps(
                evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ),
            "diagnostic_detail_sha256": canonical_sha256(evidence),
            "source_subject": register_fqid,
            "fields": ["source.validity"],
            "refs": [],
            "withheld_output": ["source_validity_bounds"],
            "acknowledged_by": "source_policy",
            "case_id": f"unknown-validity:{register_fqid}",
        }
        warnings.append(
            DataWarning.model_validate_json(
                json.dumps({"warning_id": canonical_sha256(payload), **payload})
            )
        )
    return tuple(warnings)
