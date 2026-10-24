"""Compile accepted steward metadata and physical holdings onto an exact base.

Strict extension consumes a pinned candidate (providers, census, inventory) with
the committed steward slugs and spec, and refuses a candidate generated from
another spec or global. Metadata-only diagnostics are create-only and cannot
be activated as runtime catalogs. The validated global base is never mutated.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
from contextlib import ExitStack, closing
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from reg_core_py import GrammarError

from .db import DB_FILENAME, get_manifest, open_built_db
from .errors import EXIT_CONFIG, RegMetaError
from .holdings_accounting import CANDIDATE_ROLES, INVENTORY_ROLE_PATH
from .id import mint
from .ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)
from .slug_grammar import validate_slug
from .source_evidence import canonical_json
from .sources.curated import CuratedAdapter

if TYPE_CHECKING:
    from .relations import CuratedRelations

# A private candidate's `manifest.json` `artifact_kind`, for any steward.
CANDIDATE_KIND = "steward-private-extension-input-candidate"


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


def resolve_steward_providers_dir(providers_dir: Path) -> Path:
    """Resolve the caller's explicit steward provider directory."""
    resolved = providers_dir.expanduser().resolve()
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
            validate_slug(provider, "provider")
        except GrammarError as exc:
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


def _insert_steward_same_as(
    conn: sqlite3.Connection,
    relations: CuratedRelations,
    pins: frozenset[tuple[str, str]],
) -> int:
    """Write the curated same_as edges of the steward whose register `pins` are
    given (#1549); another steward's edges are not this flavor's and are ignored.

    Each endpoint resolves by its slug FQID against the overlaid catalog, or the
    build refuses it. The base's edges plus these pass the global graph checks,
    each base pair taken in the direction `relations` declares it, so the cycle
    check matches the global build's when `relations` built the base. Returns the
    number of edges written (each stored both ways)."""
    from .catalog_dependencies import CatalogDependencyError, MissingCatalogDependency
    from .relations import check_same_as_graph, steward_same_as

    variable_edges = [edge for edge in relations.same_as if edge.grain == "variable"]
    edges = [
        (edge.a_fqid(), edge.b_fqid())
        for edge in variable_edges
        if steward_same_as(edge, pins)
    ]
    if not edges:
        return 0
    live = {
        "/".join(row)
        for row in conn.execute(
            "SELECT p.slug, r.slug, v.slug FROM variable v "
            "JOIN register r ON r.register_id = v.register_id "
            "JOIN provider p ON p.provider_id = r.provider_id "
            "WHERE r.slug IS NOT NULL AND v.slug IS NOT NULL"
        )
    }
    if missing := tuple(
        MissingCatalogDependency(("variable", fqid), f"variable_same_as:{index}")
        for index, edge in enumerate(edges, 1)
        for fqid in edge
        if fqid not in live
    ):
        raise CatalogDependencyError(missing)
    # The base stores each pair both ways, without its declared direction; a pair
    # `relations` no longer declares falls back to sorted order.
    direction = {
        frozenset(pair): pair
        for pair in ((edge.a_fqid(), edge.b_fqid()) for edge in variable_edges)
    }
    base = {
        frozenset(("/".join(row[:3]), "/".join(row[3:])))
        for row in conn.execute(
            "SELECT a_provider, a_register, a_variable, "
            "b_provider, b_register, b_variable FROM variable_same_as"
        )
    }
    check_same_as_graph(
        [direction.get(pair, tuple(sorted(pair))) for pair in sorted(base, key=sorted)]
        + edges,
        label="steward variable same_as",
    )
    conn.executemany(
        "INSERT INTO variable_same_as VALUES (?, ?, ?, ?, ?, ?)",
        [(*x.split("/"), *y.split("/")) for a, b in edges for x, y in ((a, b), (b, a))],
    )
    return len(edges)


def resolve_steward_slug_dir(slug_dir: Path | None, steward: str) -> Path:
    """Resolve the steward slug dir."""
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
    providers_dir: Path,
    db_dir: Path,
    *,
    steward: str,
    slug_dir: Path | None = None,
    holdings_input: Path | None = None,
    input_commit: str | None = None,
    input_manifest_sha256: str | None = None,
    diagnostic: bool = False,
    base_sha256: str | None = None,
    relations: Path | None = None,
) -> dict[str, Any]:
    """Build strict accepted holdings or an explicit metadata-only diagnostic.

    `base_sha256` pins the base bytes a diagnostic caller hashed (the steward
    generator prints it as the candidate's global pin); an accepted candidate
    pins them itself. `relations` is a diagnostic's `relations.toml`, whose
    steward same_as edges it applies; strict extension reads the checkout's."""
    from .db import (
        _insert_core_graph_from_ir,
        _unlink_wal_sidecars,
        publish_db,
    )
    from .derive import derive, derive_holdings
    from .fqid_slugs import (
        populate_slugs,
        populate_variable_slugs,
        steward_register_pins,
    )
    from .source_files import _progress

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

    from ._curation import repo_curation_path
    from .artifact_identity import (
        builder_commit,
        committed_steward_slugs,
        committed_steward_spec,
        generation_id,
    )
    from .holdings_accounting import account_holdings
    from .holdings_compile import compile_holdings
    from .inventory import load_inventory
    from .relations import load_relations
    from .steward_spec import STEWARDS_DIR, load_steward_spec

    candidate_manifest: dict[str, Any] = {}
    spec = spec_digest = None
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
            if slug_dir is not None or relations is not None:
                raise ValueError(
                    "Publishable extension requires committed steward naming without diagnostic overrides"
                )
            if providers_dir.expanduser().resolve() != holdings_input / "providers":
                raise ValueError(
                    "Private providers must come from the accepted holdings candidate"
                )
            assert revision is not None
            pinned_slug_dir = committed_steward_slugs(steward, revision=revision)
            snapshot = Path(
                inputs.enter_context(TemporaryDirectory(prefix="reg-meta-holdings-"))
            )
            candidate_manifest = read_private_holdings_input(
                holdings_input,
                input_commit=input_commit,
                input_manifest_sha256=input_manifest_sha256,
                materialize_to=snapshot,
            )
            holdings_input = snapshot
            providers_dir = snapshot / "providers"
            # The spec is read at the builder revision, never from the candidate;
            # the candidate pins the spec it was generated from.
            spec = load_steward_spec(committed_steward_spec(steward, revision=revision))
            spec_digest = spec.sha256
            if candidate_manifest["steward_spec_sha256"] != spec_digest:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="extend_steward_spec_mismatch",
                    error_class="configuration",
                    message=(
                        f"The candidate was generated from steward spec "
                        f"{candidate_manifest['steward_spec_sha256']}; the builder "
                        f"revision's {STEWARDS_DIR}/{steward} is {spec_digest}."
                    ),
                    remediation=(
                        "Regenerate and accept the candidate from the builder "
                        "revision's spec, or build from the revision it pins."
                    ),
                )
            # The checkout is clean at `revision` (`committed_steward_spec`), so
            # this is the revision's committed relations.toml.
            relations = repo_curation_path("relations.toml")
        else:
            pinned_slug_dir = None

        resolved_providers_dir = resolve_steward_providers_dir(providers_dir)
        with base_db.open("rb") as base_file:
            base_digest = hashlib.file_digest(base_file, "sha256").hexdigest()
        expected_base = (
            candidate_manifest["global_db_sha256"]
            if candidate_manifest
            else base_sha256
        )
        if expected_base is not None and expected_base != base_digest:
            # The generator auto-mapped the inventory against this global; another
            # global can change which variable is the single candidate.
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="extend_base_db_mismatch",
                error_class="configuration",
                message=(
                    f"--base-db is {base_digest}; the inventory is pinned to "
                    f"global {expected_base}."
                ),
                remediation=(
                    "Regenerate and accept the candidate against this global DB "
                    "before extending it."
                ),
            )
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
            slug_dir, steward
        )

        accounting = None
        if holdings_input is not None:
            inventory = load_inventory(holdings_input / INVENTORY_ROLE_PATH)
            if inventory.steward != steward:
                raise ValueError(
                    "Accepted inventory steward does not match selected steward"
                )
            assert spec is not None
            accounting = account_holdings(
                holdings_input, inventory, spec=spec, steward=steward
            )

        db_dir.mkdir(parents=True, exist_ok=True)
        final_path = db_dir / DB_FILENAME
        tmp_path = final_path.with_suffix(".db.tmp")
        # A killed run can leave the tmp's WAL sidecars, with or without the tmp
        # itself; SQLite would replay that WAL onto the fresh, verified copy.
        tmp_path.unlink(missing_ok=True)
        _unlink_wal_sidecars(tmp_path)
        # A released base is often read-only (0444). `copyfile` gives the staging
        # copy a fresh writable mode; `copy2` would carry 0444 over and SQLite
        # could not open it for writing.
        shutil.copyfile(base_db, tmp_path)
        with tmp_path.open("rb") as copied:
            if hashlib.file_digest(copied, "sha256").hexdigest() != base_digest:
                tmp_path.unlink(missing_ok=True)
                raise ValueError("Base database changed during selection")

        conn: sqlite3.Connection | None = None
        write_failed = True
        counts = {
            "providers": 0,
            "registers": len(graph.registers),
            "variants": len(graph.variants),
            "variables": len(graph.variables),
            "states": len(graph.states),
        }
        try:
            conn = sqlite3.connect(tmp_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA foreign_keys=ON")
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

            populate_slugs(conn, steward_slug_dir, strict=False)
            populate_variable_slugs(
                conn, steward_slug_dir, incremental=True, persist_auto=diagnostic
            )
            _assert_steward_rows_slugged(conn)
            counts["same_as_edges"] = _insert_steward_same_as(
                conn, load_relations(relations), steward_register_pins(steward_slug_dir)
            )

            # Holdings must see the steward's own metadata, so derive runs over
            # base plus overlay.
            conn.commit()
            derive(conn)
            manifest = get_manifest(conn)
            # Derivation provenance belongs to the base; base_generation_id
            # already covers it, so an extension does not inherit the key.
            if manifest.pop("derived_from_generation_id", None):
                conn.execute(
                    "DELETE FROM import_manifest WHERE key = 'derived_from_generation_id'"
                )
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
                assert spec is not None and spec_digest is not None
                compiled = compile_holdings(
                    conn,
                    holdings_input,
                    steward=steward,
                    spec=spec,
                    accounting=accounting,
                )
                manifest.update(
                    {
                        "catalog_artifact_kind": "steward",
                        "builder_commit": revision,
                        "steward": steward,
                        "base_db_sha256": base_digest,
                        "base_generation_id": manifest["generation_id"],
                        "holdings_input_commit": input_commit,
                        "holdings_manifest_sha256": input_manifest_sha256,
                        "steward_spec_sha256": spec_digest,
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
                derive_holdings(conn)
                counts.update(
                    holding_tables=compiled.tables,
                    holding_columns=compiled.columns,
                    holding_mappings=compiled.mappings,
                )

            # The base artifact's statistics predate the overlay rows.
            conn.execute("ANALYZE")

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
            if conn is not None:
                conn.close()
            if write_failed:
                tmp_path.unlink(missing_ok=True)
                _unlink_wal_sidecars(tmp_path)

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
    from .steward_storage import StorageSchemaError, parse_storage_types

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
    if manifest.get("artifact_kind") != CANDIDATE_KIND:
        raise ValueError(f"Not a {CANDIDATE_KIND}")
    proofs = {proof["path"]: proof for proof in manifest["files"]}
    if len(proofs) != len(manifest["files"]):
        raise ValueError("Private input manifest has duplicate files")
    committed_names = {
        name.removeprefix(selection.relative + "/")
        if selection.relative != "."
        else name
        # Only the candidate's folder: the acceptance repository may hold others.
        for name in _git(
            selection.repository, "ls-files", "--", f":(literal){selection.relative}"
        ).splitlines()
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
    roles = manifest.get("roles")
    if (
        not isinstance(roles, dict)
        or roles.keys() != set(CANDIDATE_ROLES)
        or roles["inventory"] != INVENTORY_ROLE_PATH
        or not all(isinstance(path, str) and path in proofs for path in roles.values())
    ):
        raise ValueError(
            "Private input manifest requires roles "
            f"{', '.join(CANDIDATE_ROLES)} naming its members "
            f"(inventory = {INVENTORY_ROLE_PATH})"
        )
    if len(set(roles.values())) != len(CANDIDATE_ROLES):
        raise ValueError(
            "Private input manifest roles must name distinct members: "
            + ", ".join(f"{role} = {roles[role]}" for role in CANDIDATE_ROLES)
        )
    # Parse the accepted blob the loop above verified, never the working tree again.
    member = (Path(selection.relative) / roles["storage_export"]).as_posix()
    committed = _git_bytes(
        selection.repository, "cat-file", "blob", f"{input_commit}:{member}"
    )
    try:
        parse_storage_types(committed.decode("utf-8"), roles["storage_export"])
    except (UnicodeDecodeError, StorageSchemaError) as exc:
        raise ValueError(
            f"Private input storage_export role is not a storage-schema export: {exc}"
        ) from None
    if not any(
        name.startswith("providers/") and name.endswith(".toml") for name in proofs
    ):
        raise ValueError("Private input manifest lacks accepted providers")
    if copies := sorted(
        name
        for name in proofs
        if name.startswith("policy/") and name != INVENTORY_ROLE_PATH
    ):
        raise ValueError(
            "Private input candidate carries policy copies extend-db never reads "
            f"(the steward spec is read at the builder revision): {', '.join(copies)}"
        )
    for key in ("steward_spec_sha256", "global_db_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get(key, ""))):
            raise ValueError(f"Private input manifest requires a {key} pin")
    return manifest
