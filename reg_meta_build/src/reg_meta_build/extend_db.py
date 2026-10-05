"""Compile accepted steward metadata and physical holdings onto an exact base.

Strict extension consumes pinned provider, inventory and policy inputs with
committed steward naming. Metadata-only diagnostics are create-only and cannot
be activated as runtime catalogs. The validated global base is never mutated.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from contextlib import ExitStack, closing
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter
from reg_meta.catalog import DataWarning
from reg_meta.db import DB_FILENAME
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidError, FqidKind, validate_slug
from reg_meta.source_evidence import canonical_json

from .data_warnings import write_data_warnings
from .db import get_manifest, open_built_db
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
                f"Author one curated-provider TOML per provider under {resolved}."
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
            "Author one <provider>.toml file in the accepted providers/ directory.",
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
            "Commit the missing slug pins to fqid_slugs/<steward>/ and rebuild.",
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
                    "No repo checkout found for the "
                    f"steward slug dir (fqid_slugs/{steward}/)."
                ),
                remediation=(
                    "Run from a source checkout with committed steward slug pins."
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
                f"Create and commit the required pins in fqid_slugs/{steward}/."
            ),
        )
    return resolved


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
    holdings_input: Path | None = None,
    input_commit: str | None = None,
    input_manifest_sha256: str | None = None,
    diagnostic: bool = False,
) -> dict[str, Any]:
    """Build strict accepted holdings or an explicit metadata-only diagnostic."""
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
    if not diagnostic and (data_warnings or pre_rename_hook is not None):
        raise ValueError(
            "Supplemental warnings and publish hooks require diagnostic=True"
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

    from reg_meta.inventory import load_inventory

    from .artifact_identity import (
        builder_commit,
        committed_steward_slugs,
        generation_id,
    )
    from .holdings_accounting import account_holdings
    from .holdings_compile import compile_holdings, write_holdings_assessment_warnings

    with ExitStack() as inputs:
        revision = builder_commit() if not diagnostic else None
        if holdings_input is None and not diagnostic:
            raise ValueError(
                "Publishable extension requires accepted holdings and exact pins"
            )
        if holdings_input is not None and diagnostic:
            raise ValueError(
                "Accepted holdings require the full strict compilation path"
            )
        if holdings_input is not None:
            holdings_input = holdings_input.expanduser().resolve()
            if not input_commit or not input_manifest_sha256:
                raise ValueError("Private holdings require both exact input pins")
            if slug_dir is not None or skip_slugs:
                raise ValueError(
                    "Publishable extension requires committed steward naming without diagnostic overrides"
                )
            providers_dir = providers_dir or holdings_input / "providers"
            if providers_dir.expanduser().resolve() != holdings_input / "providers":
                raise ValueError(
                    "Private providers must come from the accepted holdings candidate"
                )
            assert revision is not None
            pinned_slug_dir = committed_steward_slugs(steward, revision=revision)
            snapshot = Path(
                inputs.enter_context(TemporaryDirectory(prefix="reg-meta-holdings-"))
            )
            read_private_holdings_input(
                holdings_input,
                input_commit=input_commit,
                input_manifest_sha256=input_manifest_sha256,
                materialize_to=snapshot,
            )
            holdings_input = snapshot
            providers_dir = snapshot / "providers"
        else:
            pinned_slug_dir = None

        resolved_providers_dir = resolve_steward_providers_dir(providers_dir, steward)
        with base_db.open("rb") as base_file:
            base_digest = hashlib.file_digest(base_file, "sha256").hexdigest()
        with closing(open_built_db(base_db)) as source_db:
            if holdings_input is not None:
                from .holdings_validation import validate_compiled_holdings

                validate_compiled_holdings(source_db)
                base_manifest = get_manifest(source_db)
                if (
                    base_manifest.get("catalog_artifact_kind"),
                    base_manifest.get("catalog_publishable"),
                    base_manifest.get("catalog_completeness"),
                ) != ("catalog", "true", "complete"):
                    raise ValueError(
                        "Steward compilation requires a complete publishable global base"
                    )
            classification_short_names = frozenset(
                name
                for (name,) in source_db.execute(
                    "SELECT short_name FROM classification"
                )
            )
        graph = _load_provider_ir(
            resolved_providers_dir, steward, classification_short_names
        )
        steward_slug_dir = pinned_slug_dir or resolve_steward_slug_dir(
            slug_dir, steward, skip_slugs=skip_slugs
        )

        accounting = None
        if holdings_input is not None:
            inventory = load_inventory(holdings_input / "policy/inventory.toml")
            if inventory.steward != steward:
                raise ValueError(
                    "Accepted inventory steward does not match selected steward"
                )
            accounting = account_holdings(holdings_input, inventory)

        db_dir.mkdir(parents=True, exist_ok=True)
        final_path = db_dir / DB_FILENAME
        tmp_path = final_path.with_suffix(".db.tmp")
        if tmp_path.exists():
            tmp_path.unlink()
        shutil.copy2(base_db, tmp_path)
        with tmp_path.open("rb") as copied:
            if hashlib.file_digest(copied, "sha256").hexdigest() != base_digest:
                tmp_path.unlink(missing_ok=True)
                raise ValueError("Base database changed during selection")

        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA foreign_keys=ON")
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
                populate_variable_slugs(
                    conn, steward_slug_dir, incremental=True, persist_auto=diagnostic
                )
                _assert_steward_rows_slugged(conn)

            write_data_warnings(conn, data_warnings)
            manifest = get_manifest(conn)
            if diagnostic:
                if base_generation := manifest.pop("generation_id", None):
                    manifest["base_generation_id"] = base_generation
                manifest.pop("builder_commit", None)
                conn.execute(
                    "DELETE FROM import_manifest WHERE key IN ('generation_id', 'builder_commit')"
                )
                manifest["base_db_sha256"] = base_digest
                manifest.update(
                    catalog_artifact_kind="diagnostic",
                    catalog_publishable="false",
                    catalog_completeness="incomplete",
                )
                conn.executemany(
                    "INSERT OR REPLACE INTO import_manifest(key, value) VALUES (?, ?)",
                    sorted(manifest.items()),
                )
            if holdings_input is not None:
                if not manifest.get("generation_id"):
                    raise ValueError(
                        "Steward compilation requires a complete publishable schema-9 catalog with generation identity"
                    )
                assert input_commit is not None and input_manifest_sha256 is not None
                assert revision is not None
                compiled = compile_holdings(
                    conn, holdings_input, steward=steward, accounting=accounting
                )
                counts["data_warnings"] += write_holdings_assessment_warnings(conn)
                manifest.update(
                    {
                        "catalog_artifact_kind": "steward",
                        "builder_commit": revision,
                        "steward": steward,
                        "base_db_sha256": base_digest,
                        "base_generation_id": manifest["generation_id"],
                        "holdings_input_commit": input_commit,
                        "holdings_manifest_sha256": input_manifest_sha256,
                        "holdings_policy_sha256": compiled.accounting.policy_sha256,
                        "holdings_accounting_counts": canonical_json(
                            compiled.accounting.counts
                        ),
                        "holdings_accounting_sha256": compiled.accounting.sha256,
                    }
                )
                manifest["generation_id"] = generation_id(manifest)
                conn.executemany(
                    "INSERT OR REPLACE INTO import_manifest(key, value) VALUES (?, ?)",
                    sorted(manifest.items()),
                )
                counts.update(
                    holding_tables=compiled.tables,
                    holding_columns=compiled.columns,
                    holding_mappings=compiled.mappings,
                )

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
            conn.commit()
            write_failed = False
        finally:
            conn.close()
            if write_failed:
                tmp_path.unlink(missing_ok=True)

        try:
            if not diagnostic:
                assert revision is not None
                from .validate import validate_built_db

                validation = validate_built_db(
                    tmp_path, flavored=True, slug_dir=pinned_slug_dir
                )
                _progress(validation.format_report())
                if not validation.passed:
                    raise RegMetaError(
                        exit_code=EXIT_CONFIG,
                        code="validation_failed",
                        error_class="configuration",
                        message="Compiled steward validation failed: "
                        + "; ".join(validation.failures),
                        remediation="Review the located build failure and regenerate from accepted inputs.",
                    )
                committed_steward_slugs(steward, revision=revision)
            if pre_rename_hook is not None:
                pre_rename_hook(tmp_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            _unlink_wal_sidecars(tmp_path)
            raise

        if diagnostic:
            try:
                final_path.hardlink_to(tmp_path)
            finally:
                tmp_path.unlink(missing_ok=True)
        else:
            publish_db(tmp_path, final_path)
        _progress(f"Flavored database written to {final_path}")
        return {**counts, "db_path": str(final_path)}


def read_private_holdings_input(
    root: Path,
    *,
    input_commit: str,
    input_manifest_sha256: str,
    materialize_to: Path | None = None,
) -> dict[str, Any]:
    """Verify accepted input and optionally materialize its committed bytes for a build."""
    from ._accepted_prepared import read_accepted_manifest
    from .input_snapshot import _git, _git_bytes, _index_tags

    if materialize_to is not None and (
        not materialize_to.is_dir() or any(materialize_to.iterdir())
    ):
        raise ValueError(
            "Accepted input snapshot requires an empty build-owned directory"
        )
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
        if materialize_to is not None:
            snapshot_member = materialize_to / path
            snapshot_member.parent.mkdir(parents=True, exist_ok=True)
            snapshot_member.write_bytes(committed)
    if materialize_to is not None:
        (materialize_to / "manifest.json").write_bytes(selection.manifest_bytes)
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
    required = {
        "policy/inventory.toml",
        "policy/source_policy.toml",
        "policy/inventory_overlay.toml",
        "policy/holdings_policy.toml",
    }
    if not required <= proofs.keys() or not any(
        name.startswith("providers/") and name.endswith(".toml") for name in proofs
    ):
        raise ValueError(
            "Private input manifest lacks accepted inventory, policies or providers"
        )
    return manifest
