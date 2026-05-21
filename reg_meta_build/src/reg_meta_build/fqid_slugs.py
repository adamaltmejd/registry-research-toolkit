"""Slug TOML loading, validation, population, seeding, and snapshot.

Curated slugs live in per-provider TOML files at ``reg_meta_build/fqid_slugs/``.
This module parses them against the FQID grammar (REFACTOR_SPEC.md §5.2-5.3),
writes the slug columns during build, and ships the seed/precheck/snapshot
machinery the CLI exposes.

Variables auto-slug from the latest kolumnnamn at build time. Explicit
``[variable]`` TOML rows are exceptions — overrides, deprecations, or
``same_as`` curation.
"""

from __future__ import annotations

import json
import re
import sqlite3
import tomllib
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import (
    FqidError,
    FqidKind,
    derive_period,
    derive_period_with_span,
    derive_variable_slug,
    validate_slug,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

EntityKind = Literal[
    "register",
    "register_variant",
    "register_version",
    "variable",
    "classification",
]
ENTITY_KINDS: tuple[EntityKind, ...] = (
    "register",
    "register_variant",
    "register_version",
    "variable",
    "classification",
)
PROVIDER_FILE_SUFFIX = ".toml"
CLASSIFICATIONS_FILE = "classifications.toml"
SNAPSHOT_FILENAME = ".snapshot.json"

_KINDS_WITH_SAME_AS: frozenset[EntityKind] = frozenset({"variable", "classification"})

# Top-level keys accepted in a provider TOML; anything else is a typo
# (e.g. `[registers."34"]` vs the singular form) that today would otherwise
# silently no-op.
_PROVIDER_TOPLEVEL_KEYS: frozenset[str] = frozenset(
    {"register", "register_variant", "register_version", "variable"}
)
_CLASSIFICATIONS_TOPLEVEL_KEYS: frozenset[str] = frozenset({"classification"})

# Allowed keys for inline `same_as` tables (§5.3 worked examples).
_SAME_AS_KEYS_VARIABLE: frozenset[str] = frozenset(
    {"provider", "register", "register_variant", "period", "variable_slug"}
)
_SAME_AS_KEYS_CLASSIFICATION: frozenset[str] = frozenset(
    {"provider", "classification_slug"}
)
_SAME_AS_REQUIRED_VARIABLE: frozenset[str] = frozenset(
    {"provider", "register", "variable_slug"}
)
_SAME_AS_REQUIRED_CLASSIFICATION: frozenset[str] = frozenset(
    {"provider", "classification_slug"}
)


@dataclass(frozen=True)
class SlugEntry:
    """One row of a slug TOML.

    ``source_id`` is the literal TOML key (always a quoted string per §5.3).
    ``provider`` is the filename stem for provider-scoped files; ``None`` for
    classifications.
    """

    kind: EntityKind
    source_id: str
    slug: str | None
    provider: str | None = None
    version: str | None = None
    display_group: str | None = None
    # Whether the source ID is retired from current deliveries. populate_slugs
    # only short-circuits the missing-row branch on this — a still-live row
    # with `deprecated = true` is still slugged so resolution keeps working.
    deprecated: bool = False
    # Validated for shape and cycle-freedom (§5.4) but not yet applied to the
    # DB; the resolver-side typo-correction lands with consumer-side binding
    # materialization in step 1e.
    replaced_by: str | None = None
    # §5.5 cross-rename equivalence; materialized into `variable_same_as` /
    # `classification_same_as` edge tables by `materialize_same_as_edges`.
    # `Catalog.resolve` follows the edges transitively when direct lookup misses.
    same_as: tuple[dict[str, str], ...] = field(default_factory=tuple)


def repo_slug_dir() -> Path | None:
    """Return ``reg_meta_build/fqid_slugs/`` from a repo checkout, or ``None``.

    Wheel installs do not ship the slug TOMLs — they are maintainer artifacts
    consumed by the build, alongside ``classifications.toml`` and ``docs/``.
    """
    pkg_dir = Path(__file__).resolve().parent
    candidate = pkg_dir.parent.parent / "fqid_slugs"
    return candidate if candidate.is_dir() else None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _err(code: str, message: str, remediation: str) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code=code,
        error_class="configuration",
        message=message,
        remediation=remediation,
    )


def _toml_str(value: str) -> str:
    """Quote ``value`` as a TOML basic string with the required escapes.

    Used by the seed-emit path. Hand-rolled because the dependency footprint
    doesn't yet pull in ``tomli_w`` and the escape set is small.
    """
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\b", "\\b")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\f", "\\f")
        .replace("\r", "\\r")
    )
    return f'"{escaped}"'


def _toml_comment(value: str) -> str:
    """Collapse newlines so ``value`` is safe as a single-line TOML comment.

    A literal ``\\n`` would terminate the comment and let the rest of the
    string parse as TOML — defensive normalization, even though current SCB
    data is single-line.
    """
    return value.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")


# A "residual" carries scope info when it has a 3+ alphabetic run NOT in
# `_RESIDUAL_CONNECTOR_TOKENS`. Shorter runs (`kv`, `fr`) are abbreviations,
# and the 3-char Swedish function words below are conjunctions/prepositions
# without scope content. 3 is the smallest length threshold that admits real
# content like `Öar` or `maj`.
_RESIDUAL_ALPHA_RE = re.compile(r"[^\W\d_]+", flags=re.UNICODE)
_RESIDUAL_CONNECTOR_TOKENS = frozenset({"och", "för", "med", "men"})


def _period_residual(version_name: str | None) -> str | None:
    """Return the source-name text outside the period match, or ``None`` when
    the row is "clean" (no period match at all, or residual is connectors-only).

    Surfaces the §5.3 audit case: ``derive_period`` extracts a period token but
    the source name carried extra descriptors (`Strandlinje, 2019` → matched
    `2019`, residual `Strandlinje,`) that auto-derive would silently drop.
    Seed-slugs uses this to refuse the round-trip skip for residual-bearing
    rows so a curator sees a stub and can either rename the slug or accept
    the auto-derive by committing the explicit entry.

    Connector-only residuals (`,`, ` - `, `och`, `och med`) return ``None`` —
    they don't encode scope info, so flagging them would be noise.
    """
    match = derive_period_with_span(version_name)
    if match is None:
        return None
    assert version_name is not None  # narrowed by match-not-None
    _, start, end = match
    residual = (version_name[:start] + " " + version_name[end:]).strip()
    if not residual:
        return None
    for token in _RESIDUAL_ALPHA_RE.findall(residual):
        if len(token) >= 3 and token.lower() not in _RESIDUAL_CONNECTOR_TOKENS:
            return residual
    return None


def _parse_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise _err(
            "slug_toml_unreadable",
            f"Could not parse slug TOML {path}: {exc}",
            "Ensure the file is valid TOML.",
        ) from exc


def _live_providers(conn: sqlite3.Connection) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT p.slug FROM provider p "
            "JOIN register r ON r.provider_id = p.provider_id "
            "ORDER BY p.slug"
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# Parsing and validation
# ---------------------------------------------------------------------------


def _allowed_fields(kind: EntityKind) -> frozenset[str]:
    base = {"slug", "deprecated", "replaced_by"}
    if kind == "register_variant":
        return frozenset(base | {"display_group"})
    if kind == "classification":
        return frozenset(base | {"version", "same_as"})
    if kind == "variable":
        return frozenset(base | {"same_as"})
    # register_version: just base. registerversionnamn already serves as the
    # human-readable label; no display_group needed.
    return frozenset(base)


def _validate_same_as(
    kind: EntityKind, source_id: str, raw: Any
) -> tuple[dict[str, str], ...]:
    if raw is None:
        return ()
    if kind not in _KINDS_WITH_SAME_AS:
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `same_as` only allowed on variable / classification.",
            "Remove the field or move the entry to the correct kind.",
        )
    if not isinstance(raw, list) or not all(isinstance(r, dict) for r in raw):
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `same_as` must be an array of inline tables.",
            'Use TOML syntax: same_as = [{ provider = "scb", ... }].',
        )
    allowed_keys = (
        _SAME_AS_KEYS_VARIABLE if kind == "variable" else _SAME_AS_KEYS_CLASSIFICATION
    )
    required_keys = (
        _SAME_AS_REQUIRED_VARIABLE
        if kind == "variable"
        else _SAME_AS_REQUIRED_CLASSIFICATION
    )
    for ref in raw:
        unknown_keys = set(ref) - allowed_keys
        if unknown_keys:
            raise _err(
                "slug_toml_invalid",
                f"{kind}.{source_id!r}: `same_as` has unknown key(s): "
                f"{sorted(unknown_keys)}.",
                f"Allowed keys for {kind}: {sorted(allowed_keys)}.",
            )
        missing_keys = required_keys - ref.keys()
        if missing_keys:
            raise _err(
                "slug_toml_invalid",
                f"{kind}.{source_id!r}: `same_as` missing required key(s): "
                f"{sorted(missing_keys)}.",
                f"Required for {kind}: {sorted(required_keys)}.",
            )
        if not all(isinstance(v, str) and v for v in ref.values()):
            raise _err(
                "slug_toml_invalid",
                f"{kind}.{source_id!r}: `same_as` entries must be non-empty strings.",
                "Each link names provider + slugs as quoted strings.",
            )
    return tuple(dict(ref) for ref in raw)


def _validate_entry_slug(
    kind: EntityKind, source_id: str, slug: str | None, *, required: bool
) -> None:
    if slug is None:
        if required:
            raise _err(
                "slug_toml_invalid",
                f"{kind}.{source_id!r}: missing required `slug` field.",
                'Add `slug = "..."` to the entry.',
            )
        return
    if not isinstance(slug, str):
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `slug` must be a string, got {type(slug).__name__}.",
            "Quote the value as a TOML string.",
        )
    if kind == "register_variant":
        slot: FqidKind | str = FqidKind.REGISTER_VARIANT
    elif kind == "register_version":
        slot = FqidKind.REGISTER_VERSION
    else:
        slot = kind
    try:
        validate_slug(
            slug,
            slot,
            allow_default=(kind in ("register_variant", "register_version")),
            allow_period=(kind == "register_version"),
        )
    except ValueError as exc:
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: {exc}",
            "Adjust the slug to match the §5.2 grammar.",
        ) from exc


def _validate_entry(
    kind: EntityKind,
    source_id: str,
    entry: dict[str, Any],
    *,
    provider: str | None,
) -> SlugEntry:
    allowed = _allowed_fields(kind)
    unknown = set(entry) - allowed
    if unknown:
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: unknown field(s): {sorted(unknown)}.",
            f"Allowed fields for {kind}: {sorted(allowed)}.",
        )
    # Source-ID shape: parse here so `populate_slugs` and precheck never see
    # malformed keys. `[register."01"]` (leading zero) or `[variable."1.2.3"]`
    # (extra segment) fail at TOML load with a clear field-shape error rather
    # than blowing up later as an `slug_unknown_source_id` lookup miss.
    if kind == "register":
        _parse_register_id(source_id)
    elif kind in ("register_variant", "variable"):
        _parse_variant_id(source_id)
    elif kind == "register_version":
        _parse_version_id(source_id)
    slug = entry.get("slug")
    _validate_entry_slug(kind, source_id, slug, required=kind != "variable")
    # `bool(value)` would silently flip `deprecated = "false"` to True and
    # `deprecated = "no"` to True — both common maintainer typos that would
    # mask real source-ID drift downstream (deprecated rows skip the
    # missing-row check in populate_slugs).
    deprecated_raw = entry.get("deprecated", False)
    if not isinstance(deprecated_raw, bool):
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `deprecated` must be a TOML boolean "
            f"(true/false), got {type(deprecated_raw).__name__}.",
            "Use bare `deprecated = true` or `deprecated = false`.",
        )
    replaced_by = entry.get("replaced_by")
    if replaced_by is not None and (
        not isinstance(replaced_by, str) or not replaced_by
    ):
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `replaced_by` must be a non-empty string.",
            "Point it at the TOML key of the replacement row.",
        )
    version = entry.get("version")
    if kind == "classification":
        if not version or not isinstance(version, str):
            raise _err(
                "slug_toml_invalid",
                f"{kind}.{source_id!r}: classifications require a string `version`.",
                'Add `version = "<stem>"` (e.g. "2020").',
            )
        # `version` becomes the third segment of `class/<slug>/<version>`, so
        # it must round-trip through the FQID grammar. Catching this here
        # surfaces the error at TOML load rather than at FQID emission.
        try:
            validate_slug(version, "classification version", allow_period=True)
        except FqidError as exc:
            raise _err(
                "slug_toml_invalid",
                f"{kind}.{source_id!r}: `version` {version!r} fails the FQID "
                f"grammar ({exc}).",
                "Use a kebab-case slug or a period token (e.g. `2020`, `2020-Q1`).",
            ) from exc
    elif version is not None:
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `version` is only valid on classifications.",
            "Remove the field or move the entry to [[classification]].",
        )
    display_group = entry.get("display_group")
    if display_group is not None and not isinstance(display_group, str):
        raise _err(
            "slug_toml_invalid",
            f"{kind}.{source_id!r}: `display_group` must be a string.",
            "Quote the value or remove it.",
        )
    return SlugEntry(
        kind=kind,
        source_id=source_id,
        slug=slug,
        provider=provider,
        version=version,
        display_group=display_group,
        deprecated=deprecated_raw,
        replaced_by=replaced_by,
        same_as=_validate_same_as(kind, source_id, entry.get("same_as")),
    )


def _resolve_replaced_by(entries: list[SlugEntry], *, scope: str) -> None:
    by_key: dict[tuple[str, str], SlugEntry] = {
        (e.kind, e.source_id): e for e in entries
    }
    for entry in entries:
        if entry.replaced_by is None:
            continue
        seen: set[tuple[str, str]] = {(entry.kind, entry.source_id)}
        cur = entry
        while cur.replaced_by is not None:
            nxt_key = (cur.kind, cur.replaced_by)
            if nxt_key not in by_key:
                raise _err(
                    "slug_toml_invalid",
                    f"{scope}: {cur.kind}.{cur.source_id!r} replaced_by "
                    f"{cur.replaced_by!r} which is not declared.",
                    "Add the replacement row, or remove the replaced_by link.",
                )
            if nxt_key in seen:
                raise _err(
                    "slug_toml_invalid",
                    f"{scope}: replaced_by cycle through {entry.kind}.{entry.source_id!r}.",
                    "Break the cycle so resolution terminates.",
                )
            seen.add(nxt_key)
            cur = by_key[nxt_key]


def load_provider_toml(path: Path) -> list[SlugEntry]:
    """Parse a per-provider slug TOML (``scb.toml``, ``sos.toml``, …)."""
    provider = path.stem
    try:
        validate_slug(provider, FqidKind.PROVIDER)
    except ValueError as exc:
        raise _err(
            "slug_toml_invalid",
            f"{path.name}: provider slug {provider!r} fails the FQID grammar ({exc}).",
            "Rename the file to a lowercase-kebab provider slug (e.g. `scb.toml`).",
        ) from exc
    data = _parse_toml(path)
    unknown_top = set(data) - _PROVIDER_TOPLEVEL_KEYS
    if unknown_top:
        raise _err(
            "slug_toml_invalid",
            f"{path.name}: unknown top-level table(s): {sorted(unknown_top)}.",
            f"Allowed: {sorted(_PROVIDER_TOPLEVEL_KEYS)}. Check for typos "
            '(e.g. `[registers."..."]` -> `[register."..."]`).',
        )
    entries: list[SlugEntry] = []
    # Slug uniqueness scope follows the FQID grammar (§5.2):
    # - register: provider-wide (`<provider>/<register>`)
    # - register_variant, variable: per parent register (the register slot in
    #   `<provider>/<register>/<variant>...` already disambiguates them).
    # - register_version: per parent variant (the variant slot disambiguates
    #   `scb/r/v1/2020` from `scb/r/v2/2020`).
    # Source IDs are dotted: variant/variable `<reg>.<sub>`, version
    # `<reg>.<var>.<ver>`.
    seen_slugs: dict[tuple[str, ...], str] = {}
    for kind in ("register", "register_variant", "register_version", "variable"):
        if kind not in data:
            continue
        table = data[kind]
        if not isinstance(table, dict):
            raise _err(
                "slug_toml_invalid",
                f"{path.name}: `{kind}` must be a table-of-tables.",
                f'Use [{kind}."<id>"] entries, not a flat array.',
            )
        for source_id, raw in table.items():
            if not isinstance(raw, dict):
                raise _err(
                    "slug_toml_invalid",
                    f"{path.name}: {kind}.{source_id!r} must be a TOML table.",
                    f'Use the dotted-key form: [{kind}."<id>"].',
                )
            entry = _validate_entry(kind, source_id, raw, provider=provider)
            if entry.slug is not None:
                if kind in ("register_variant", "variable"):
                    reg_id, _, _ = source_id.partition(".")
                    slug_key: tuple[str, ...] = (kind, reg_id, entry.slug)
                    scope_desc = f"within register {reg_id!r}"
                elif kind == "register_version":
                    reg_id, _, rest = source_id.partition(".")
                    regvar_id, _, _ = rest.partition(".")
                    slug_key = (kind, reg_id, regvar_id, entry.slug)
                    scope_desc = f"within variant {reg_id!r}.{regvar_id!r}"
                else:
                    slug_key = (kind, entry.slug)
                    scope_desc = f"within provider {provider!r}"
                prev = seen_slugs.get(slug_key)
                if prev is not None:
                    raise _err(
                        "slug_toml_invalid",
                        f"{path.name}: slug {entry.slug!r} reused by "
                        f"{kind}.{prev!r} and {kind}.{source_id!r} "
                        f"({scope_desc}).",
                        f"Slugs must be unique per kind {scope_desc}.",
                    )
                seen_slugs[slug_key] = source_id
            entries.append(entry)
    _resolve_replaced_by(entries, scope=path.name)
    return entries


def load_classifications_toml(path: Path) -> list[SlugEntry]:
    """Parse the provider-independent classification slug TOML."""
    data = _parse_toml(path)
    unknown_top = set(data) - _CLASSIFICATIONS_TOPLEVEL_KEYS
    if unknown_top:
        raise _err(
            "slug_toml_invalid",
            f"{path.name}: unknown top-level table(s): {sorted(unknown_top)}.",
            f"Allowed: {sorted(_CLASSIFICATIONS_TOPLEVEL_KEYS)}. Check for "
            'typos like `[classifications."..."]`.',
        )
    table = data.get("classification") or {}
    if not isinstance(table, dict):
        raise _err(
            "slug_toml_invalid",
            f"{path.name}: `classification` must be a table-of-tables.",
            'Use [classification."<short_name>"] entries.',
        )
    entries: list[SlugEntry] = []
    seen_pairs: dict[tuple[str, str], str] = {}
    for source_id, raw in table.items():
        if not isinstance(raw, dict):
            raise _err(
                "slug_toml_invalid",
                f"{path.name}: classification.{source_id!r} must be a TOML table.",
                'Use [classification."<short_name>"] = { slug = ..., version = ... }.',
            )
        entry = _validate_entry("classification", source_id, raw, provider=None)
        pair_key = (entry.slug or "", entry.version or "")
        prev = seen_pairs.get(pair_key)
        if prev is not None:
            raise _err(
                "slug_toml_invalid",
                f"{path.name}: (slug={entry.slug!r}, version={entry.version!r}) "
                f"reused by {prev!r} and {source_id!r}.",
                "Classification FQIDs are (slug, version) pairs — keep them unique.",
            )
        seen_pairs[pair_key] = source_id
        entries.append(entry)
    _resolve_replaced_by(entries, scope=path.name)
    return entries


UNFROZEN_MARKER = "UNFROZEN"


def is_unfrozen(slug_dir: Path) -> bool:
    """§5.4 pre-v1 escape hatch: returns True iff ``UNFROZEN`` sentinel file
    exists in ``slug_dir``.

    While the file is present, ``precheck-slugs --update-snapshot`` writes
    rename/removal diffs through to ``.snapshot.json`` instead of refusing,
    and the snapshot-immutability CI test skips its rename guard. Removing
    the file at v1 release restores the grow-only guarantee.

    Renames are still *reported* in CLI output and JSON envelopes so a
    pre-v1 maintainer sees what's drifting; the sentinel only lifts the
    refusal, not the visibility.
    """
    return (slug_dir / UNFROZEN_MARKER).is_file()


def load_slug_dir(slug_dir: Path) -> list[SlugEntry]:
    """Load every TOML under ``slug_dir`` into a flat ``SlugEntry`` list."""
    if not slug_dir.is_dir():
        raise _err(
            "slug_dir_not_found",
            f"Slug directory not found: {slug_dir}",
            "Create the directory or pass --slug-dir.",
        )
    entries: list[SlugEntry] = []
    for path in sorted(slug_dir.glob(f"*{PROVIDER_FILE_SUFFIX}")):
        if path.name == CLASSIFICATIONS_FILE:
            entries.extend(load_classifications_toml(path))
        else:
            entries.extend(load_provider_toml(path))
    return entries


# ---------------------------------------------------------------------------
# Build-time population
# ---------------------------------------------------------------------------


def _parse_canonical_int(value: str) -> int | None:
    """Reject leading zeros so `"1.10"` and `"1.010"` cannot alias the same
    DB row while still appearing distinct as TOML keys."""
    if not value or not value.isdigit():
        return None
    if len(value) > 1 and value[0] == "0":
        return None
    return int(value)


def _parse_register_id(source_id: str) -> int:
    parsed = _parse_canonical_int(source_id)
    if parsed is None:
        raise _err(
            "slug_toml_invalid",
            f"register.{source_id!r}: TOML key must be the numeric RegisterId "
            f"in canonical form (no leading zeros).",
            "Quote SCB's RegisterId integer as a string key.",
        )
    return parsed


def _parse_variant_id(source_id: str) -> tuple[int, int]:
    parts = source_id.split(".")
    if len(parts) != 2:
        raise _err(
            "slug_toml_invalid",
            f"register_variant.{source_id!r}: expected `<RegisterId>.<RegVarID>`.",
            "Compose the key from SCB's two integer IDs.",
        )
    reg = _parse_canonical_int(parts[0])
    var = _parse_canonical_int(parts[1])
    if reg is None or var is None:
        raise _err(
            "slug_toml_invalid",
            f"register_variant.{source_id!r}: both halves must be integers "
            f"in canonical form (no leading zeros).",
            "Use the literal SCB IDs.",
        )
    return reg, var


def _parse_version_id(source_id: str) -> tuple[int, int, int]:
    parts = source_id.split(".")
    if len(parts) != 3:
        raise _err(
            "slug_toml_invalid",
            f"register_version.{source_id!r}: expected "
            f"`<RegisterId>.<RegVarID>.<RegVerID>`.",
            "Compose the key from SCB's three integer IDs.",
        )
    reg = _parse_canonical_int(parts[0])
    var = _parse_canonical_int(parts[1])
    ver = _parse_canonical_int(parts[2])
    if reg is None or var is None or ver is None:
        raise _err(
            "slug_toml_invalid",
            f"register_version.{source_id!r}: all three halves must be integers "
            f"in canonical form (no leading zeros).",
            "Use the literal SCB IDs.",
        )
    return reg, var, ver


def _live_register_ids(conn: sqlite3.Connection, provider_slug: str) -> set[int]:
    rows = conn.execute(
        "SELECT r.register_id FROM register r "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ?",
        (provider_slug,),
    ).fetchall()
    return {r[0] for r in rows}


def _live_variant_keys(
    conn: sqlite3.Connection, provider_slug: str
) -> set[tuple[int, int]]:
    rows = conn.execute(
        "SELECT rv.register_id, rv.regvar_id FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND rv.slug IS NULL",
        (provider_slug,),
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def _live_version_keys(
    conn: sqlite3.Connection, provider_slug: str
) -> set[tuple[int, int, int]]:
    rows = conn.execute(
        "SELECT rv.register_id, rver.regvar_id, rver.regver_id "
        "FROM register_version rver "
        "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ?",
        (provider_slug,),
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def _autoderive_version_slugs(conn: sqlite3.Connection, provider_slug: str) -> int:
    """Populate `register_version.slug` from `derive_period(registerversionnamn)`
    for every version row under this provider whose slug column is still NULL.

    Periodized versions (8,599 / 8,608 in current SCB) land their derived period
    as the slug here; the 9 unperiodized aux tables stay NULL and must be
    curated via `[register_version."<reg>.<var>.<ver>"]` TOML entries. The
    strict-mode unslugged check fires for any leftover NULL.
    """
    rows = conn.execute(
        "SELECT rver.regver_id, rver.registerversionnamn "
        "FROM register_version rver "
        "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND rver.slug IS NULL",
        (provider_slug,),
    ).fetchall()
    updates = [
        (period, regver_id)
        for regver_id, name in rows
        if (period := derive_period(name)) is not None
    ]
    if updates:
        try:
            conn.executemany(
                "UPDATE register_version SET slug = ? WHERE regver_id = ?", updates
            )
        except sqlite3.IntegrityError as exc:
            # UNIQUE(regvar_id, slug) trips when two siblings derive to the
            # same period, or when a TOML override already wrote the slug
            # this row would auto-derive to. `precheck_slugs` is the
            # canonical guard for this case; surface a pointer there
            # instead of the raw sqlite message.
            raise _err(
                "slug_periodized_collision",
                f"Auto-derived register_version slug collides with a sibling "
                f"under provider {provider_slug!r}: {exc}.",
                "Run `reg-meta-build precheck-slugs` to list the colliding "
                "rows, then add a curated "
                '`[register_version."<RegisterId>.<RegVarID>.<RegVerID>"]` '
                "entry on one sibling to disambiguate.",
            ) from exc
    return len(updates)


def populate_slugs(
    conn: sqlite3.Connection,
    slug_dir: Path,
    *,
    strict: bool = True,
) -> dict[str, int]:
    """Read ``slug_dir`` and write slug columns on register / register_variant /
    register_version / classification.

    Register_version is mixed-source: most rows auto-derive their slug from
    ``registerversionnamn`` (period regex), and TOML entries override or
    fill in unperiodized rows.

    ``strict=True`` (the default for real builds) refuses if any live source
    ID has no slug entry. Tests pass ``strict=False`` to populate whatever's
    available without enforcing coverage.

    Returns ``{"register": n, "register_variant": n, "register_version": n,
    "register_version_auto": n, "classification": n}``.
    """
    # Function-level import — `db` imports `populate_slugs` at module top, so
    # importing `db` at our module top would close a cycle. Lazy resolution
    # is safe because by call time both modules are fully loaded.
    from .db import _progress

    entries = load_slug_dir(slug_dir)
    counts = {
        "register": 0,
        "register_variant": 0,
        "register_version": 0,
        "register_version_auto": 0,
        "classification": 0,
    }

    # `[variable]` slug overrides are forward-compat metadata only — the
    # binding-resolver derives slugs from kolumnnamn at query time (catalog.py
    # _resolve_binding) until the §5.6 binding rows land in step 1e. Raise
    # loudly so a maintainer doesn't commit an override that does nothing.
    inert_variable_overrides = [
        e for e in entries if e.kind == "variable" and e.slug is not None
    ]
    if inert_variable_overrides:
        sample = ", ".join(
            f"{e.provider}/{e.source_id} -> {e.slug!r}"
            for e in inert_variable_overrides[:5]
        )
        raise _err(
            "slug_variable_override_unsupported",
            f"{len(inert_variable_overrides)} [variable] entries declare a "
            f"`slug` field, but variable slug overrides are not yet wired in "
            f"(§5.6 binding materialization lands in step 1e). First: {sample}.",
            'Drop the `slug = "..."` field; auto-slug from kolumnnamn applies. '
            "Other metadata (deprecated, replaced_by, same_as) is parsed and "
            "preserved for the upcoming step.",
        )

    by_provider: dict[str, list[SlugEntry]] = {}
    classification_entries: list[SlugEntry] = []
    for entry in entries:
        if entry.kind == "classification":
            classification_entries.append(entry)
        elif entry.provider is not None:
            by_provider.setdefault(entry.provider, []).append(entry)

    # Enumerate live providers so strict mode catches absent TOMLs too.
    for provider_slug in _live_providers(conn):
        prov_entries = by_provider.get(provider_slug, [])
        live_regs = _live_register_ids(conn, provider_slug)
        live_variants = _live_variant_keys(conn, provider_slug)
        live_versions = _live_version_keys(conn, provider_slug)

        for entry in prov_entries:
            if entry.slug is None:
                continue
            if entry.kind == "register":
                register_id = _parse_register_id(entry.source_id)
                if register_id not in live_regs:
                    if entry.deprecated:
                        continue
                    raise _err(
                        "slug_unknown_source_id",
                        f"{provider_slug}.toml: register.{entry.source_id!r} "
                        f"has no live row in this build.",
                        "Mark the entry deprecated=true or drop it.",
                    )
                conn.execute(
                    "UPDATE register SET slug = ? WHERE register_id = ?",
                    (entry.slug, register_id),
                )
                counts["register"] += 1
            elif entry.kind == "register_variant":
                key = _parse_variant_id(entry.source_id)
                if key not in live_variants:
                    if entry.deprecated:
                        continue
                    raise _err(
                        "slug_unknown_source_id",
                        f"{provider_slug}.toml: register_variant.{entry.source_id!r} "
                        f"has no live row in this build.",
                        "Mark the entry deprecated=true or drop it.",
                    )
                conn.execute(
                    "UPDATE register_variant SET slug = ?, display_group = ? "
                    "WHERE register_id = ? AND regvar_id = ?",
                    (entry.slug, entry.display_group, key[0], key[1]),
                )
                counts["register_variant"] += 1
            elif entry.kind == "register_version":
                vkey = _parse_version_id(entry.source_id)
                if vkey not in live_versions:
                    if entry.deprecated:
                        continue
                    raise _err(
                        "slug_unknown_source_id",
                        f"{provider_slug}.toml: register_version.{entry.source_id!r} "
                        f"has no live row in this build.",
                        "Mark the entry deprecated=true or drop it.",
                    )
                conn.execute(
                    "UPDATE register_version SET slug = ? WHERE regver_id = ?",
                    (entry.slug, vkey[2]),
                )
                counts["register_version"] += 1

        # Auto-derive runs *after* TOML overrides so that curated rows occupy
        # their slug slot before the period regex fires. Without that order,
        # two periodized siblings would race for the same auto-derived slug
        # and trip UNIQUE(regvar_id, slug) on the second INSERT — even if the
        # collision is fully resolved once TOML overrides land.
        counts["register_version_auto"] += _autoderive_version_slugs(
            conn, provider_slug
        )

        if strict:
            _assert_no_unslugged(
                conn,
                sql=(
                    "SELECT r.register_id, r.registernamn FROM register r "
                    "JOIN provider p ON r.provider_id = p.provider_id "
                    "WHERE p.slug = ? AND r.slug IS NULL "
                    "ORDER BY r.register_id"
                ),
                params=(provider_slug,),
                label="register",
                sample_fmt=_format_register_sample,
                provider_slug=provider_slug,
                add_hint='[register."<id>"] slug = "..."',
            )
            _assert_no_unslugged(
                conn,
                sql=(
                    "SELECT rv.register_id, rv.regvar_id, rv.registervariantnamn "
                    "FROM register_variant rv "
                    "JOIN register r ON rv.register_id = r.register_id "
                    "JOIN provider p ON r.provider_id = p.provider_id "
                    "WHERE p.slug = ? AND rv.slug IS NULL "
                    "ORDER BY rv.register_id, rv.regvar_id"
                ),
                params=(provider_slug,),
                label="register_variant",
                sample_fmt=_format_variant_sample,
                provider_slug=provider_slug,
                add_hint='[register_variant."<RegisterId>.<RegVarID>"]',
            )
            _assert_no_unslugged(
                conn,
                sql=(
                    "SELECT rv.register_id, rver.regvar_id, rver.regver_id, "
                    "rver.registerversionnamn "
                    "FROM register_version rver "
                    "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
                    "JOIN register r ON rv.register_id = r.register_id "
                    "JOIN provider p ON r.provider_id = p.provider_id "
                    "WHERE p.slug = ? AND rver.slug IS NULL "
                    "ORDER BY rver.regver_id"
                ),
                params=(provider_slug,),
                label="register_version",
                sample_fmt=_format_version_sample,
                provider_slug=provider_slug,
                add_hint='[register_version."<RegisterId>.<RegVarID>.<RegVerID>"]',
            )

    for entry in classification_entries:
        if entry.slug is None:
            continue
        # Cross-check: a slug TOML version mismatch against the DB row would
        # snapshot a different FQID (`class/<slug>/<toml_version>`) than the
        # one the catalog actually emits at query time.
        row = conn.execute(
            "SELECT version FROM classification WHERE short_name = ?",
            (entry.source_id,),
        ).fetchone()
        if row is None:
            if entry.deprecated:
                continue
            raise _err(
                "slug_unknown_source_id",
                f"{CLASSIFICATIONS_FILE}: classification.{entry.source_id!r} "
                f"has no row in this build.",
                "Add the short_name to classifications.toml (the seed) or drop "
                "the slug entry.",
            )
        db_version = row[0] if isinstance(row, tuple) else row["version"]
        if entry.version != db_version:
            raise _err(
                "slug_classification_version_mismatch",
                f"{CLASSIFICATIONS_FILE}: classification.{entry.source_id!r} "
                f"version {entry.version!r} does not match the DB row "
                f"({db_version!r}).",
                "Reconcile the TOML version with classifications.toml (the seed) "
                "so the snapshotted FQID matches what the catalog emits.",
            )
        conn.execute(
            "UPDATE classification SET slug = ? WHERE short_name = ?",
            (entry.slug, entry.source_id),
        )
        counts["classification"] += 1

    if strict:
        missing = conn.execute(
            "SELECT short_name FROM classification WHERE slug IS NULL "
            "ORDER BY short_name"
        ).fetchall()
        if missing:
            sample = ", ".join(m[0] for m in missing[:5])
            raise _err(
                "slug_missing_for_source_id",
                f"{len(missing)} classification(s) have no slug in "
                f"{CLASSIFICATIONS_FILE}. First: {sample}.",
                'Add a `[classification."<short_name>"]` entry with slug and version.',
            )

    _progress(
        f"  Slugged {counts['register']:,} registers, "
        f"{counts['register_variant']:,} variants, "
        f"{counts['register_version_auto']:,}+{counts['register_version']:,} "
        f"versions (derived+curated), "
        f"{counts['classification']:,} classifications"
    )
    return counts


def _format_register_sample(row: Any) -> str:
    return f"{row[0]} ({row[1]!r})"


def _format_variant_sample(row: Any) -> str:
    return f"{row[0]}.{row[1]} ({row[2]!r})"


def _format_version_sample(row: Any) -> str:
    return f"{row[0]}.{row[1]}.{row[2]} ({row[3]!r})"


def _assert_no_unslugged(
    conn: sqlite3.Connection,
    *,
    sql: str,
    params: tuple[Any, ...],
    label: str,
    sample_fmt: Callable[[Any], str],
    provider_slug: str,
    add_hint: str,
) -> None:
    missing = conn.execute(sql, params).fetchall()
    if not missing:
        return
    sample = ", ".join(sample_fmt(r) for r in missing[:5])
    raise _err(
        "slug_missing_for_source_id",
        f"{len(missing)} {label}(s) under provider {provider_slug!r} have no "
        f"slug in {provider_slug}.toml. First: {sample}.",
        f"Run `reg-meta-build precheck-slugs` to list every missing ID, "
        f"then add `{add_hint}` entries.",
    )


# ---------------------------------------------------------------------------
# same_as edge materialization (§5.5)
# ---------------------------------------------------------------------------


# 5-tuple (provider, register, variant, period, variable) / 2-tuple
# (provider, classification_slug). Variant/period use empty-string sentinels
# for wildcard scope — see the db.py table comment for the SQLite-NULL
# rationale.
_VarKey = tuple[str, str, str, str, str]
_ClassKey = tuple[str, str]


def _variable_source_slug(
    conn: sqlite3.Connection, register_id: int, var_id: int, entry: SlugEntry
) -> str:
    """Auto-derive the variable_slug for a `[variable]` TOML entry.

    The TOML key (`34.137`) identifies a `(register_id, var_id)` pair. The
    catalog derives the variable_slug from the latest kolumnnamn at resolve
    time; for same_as we must agree on a stable canonical form **at build
    time** without writing a slug column to `variable_instance` (variable
    overrides are still blocked in populate_slugs). Strategy: derive a slug
    from every alias under the pair, require a unique result. Multiple
    distinct slugs would force the maintainer into a curated override
    (§5.5 narrow scoping, or an explicit slug field once those land).
    """
    rows = conn.execute(
        "SELECT DISTINCT va.kolumnnamn FROM variable_alias va "
        "JOIN variable_instance vi ON vi.cvid = va.cvid "
        "WHERE vi.register_id = ? AND vi.var_id = ? "
        "AND va.kolumnnamn IS NOT NULL",
        (register_id, var_id),
    ).fetchall()
    if not rows:
        raise _err(
            "slug_same_as_unresolved_source",
            f"{entry.provider}.toml: variable.{entry.source_id!r} has no "
            f"kolumnnamn alias in the DB, so its variable_slug can't be "
            f"derived for `same_as` materialization.",
            "Check that the (register_id, var_id) appears in "
            "Variabelinformation.csv; mark the entry deprecated=true if the "
            "variable is retired.",
        )
    slugs = {s for r in rows if (s := derive_variable_slug(r[0])) is not None}
    if not slugs:
        raise _err(
            "slug_same_as_unresolved_source",
            f"{entry.provider}.toml: variable.{entry.source_id!r} has "
            f"kolumnnamn alias(es) but none derive to a valid slug; cannot "
            f"anchor `same_as`.",
            "Inspect the kolumnnamn aliases in `variable_alias`; they may be "
            "empty after NFKD/grammar folding. Add an explicit slug override "
            "(when §5.6 follow-up lands) or correct the upstream alias.",
        )
    if len(slugs) > 1:
        raise _err(
            "slug_same_as_ambiguous_source",
            f"{entry.provider}.toml: variable.{entry.source_id!r} aliases "
            f"derive to multiple slugs ({sorted(slugs)!r}); cannot pick a "
            f"canonical form for `same_as`.",
            "Resolve the ambiguity in `variable_alias` upstream, or wait for "
            "variable slug overrides (§5.6 follow-up) before adding same_as.",
        )
    return next(iter(slugs))


def _validate_variable_target(
    conn: sqlite3.Connection, entry: SlugEntry, ref: dict[str, str]
) -> _VarKey:
    """Validate a same_as target's provider+register+variant+period exist.

    `variable_slug` itself is **not** validated against the DB — that's the
    point of slug-anchored linking: the link survives forward-of-data
    renames. Provider and register slugs are checked because if those are
    typos the link is permanently dead. Key shape (required/allowed) is
    enforced upstream by `_validate_same_as` at TOML load time.
    """
    provider_slug = ref["provider"]
    register_slug = ref["register"]
    variant_slug = ref.get("register_variant", "")
    period_slug = ref.get("period", "")
    variable_slug = ref["variable_slug"]

    # Provider + register existence: hard error per design call (matches the
    # rest of slug validation).
    row = conn.execute(
        "SELECT r.register_id FROM register r "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND r.slug = ?",
        (provider_slug, register_slug),
    ).fetchone()
    if row is None:
        raise _err(
            "slug_same_as_unknown_register",
            f"{entry.provider}.toml: variable.{entry.source_id!r} same_as "
            f"target {provider_slug!r}/{register_slug!r} does not exist in "
            f"this build.",
            "Fix the slug typo, or mark the originating entry deprecated=true "
            "if the target register is retired.",
        )
    register_id = row[0]

    # Optional narrowing slots: when present they must resolve too.
    if variant_slug:
        row = conn.execute(
            "SELECT 1 FROM register_variant WHERE register_id = ? AND slug = ?",
            (register_id, variant_slug),
        ).fetchone()
        if row is None:
            raise _err(
                "slug_same_as_unknown_variant",
                f"{entry.provider}.toml: variable.{entry.source_id!r} same_as "
                f"target variant {variant_slug!r} not found under "
                f"{provider_slug}/{register_slug}.",
                "Drop the `register_variant` narrowing key or fix the slug.",
            )
    if period_slug:
        # The resolver inherits the query's variant when traversing an edge
        # (`n_variant = b_variant or current_variant`), so a period-only
        # target can only resolve when the inherited variant happens to
        # carry that period. Accepting period without variant at build
        # time lets edges pass that can never resolve at query time —
        # require the pair together so the build catches that mismatch.
        if not variant_slug:
            raise _err(
                "slug_same_as_period_without_variant",
                f"{entry.provider}.toml: variable.{entry.source_id!r} same_as "
                f"target carries `period` without `register_variant`. "
                f"The resolver inherits the query's variant and cannot fan "
                f"out across variants, so a period-only narrowing is "
                f"ambiguous and not supported.",
                'Add `register_variant = "..."` alongside `period`, or '
                "drop both narrowing keys to apply across all variants.",
            )
        row = conn.execute(
            "SELECT 1 FROM register_version rver "
            "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
            "WHERE rv.register_id = ? AND rv.slug = ? AND rver.slug = ?",
            (register_id, variant_slug, period_slug),
        ).fetchone()
        if row is None:
            raise _err(
                "slug_same_as_unknown_period",
                f"{entry.provider}.toml: variable.{entry.source_id!r} same_as "
                f"target period {period_slug!r} not found under "
                f"{provider_slug}/{register_slug}/{variant_slug}.",
                "Drop the `period` narrowing key or fix the slug.",
            )

    return (provider_slug, register_slug, variant_slug, period_slug, variable_slug)


def _validate_classification_target(
    entry: SlugEntry,
    ref: dict[str, str],
    by_slug: dict[tuple[str, str], list[str]],
) -> _ClassKey:
    """Validate a classification same_as target — provider + classification_slug.

    The version is intentionally not part of the key (§5.3 field reference);
    same_as for classifications links *families*, with version drift handled
    by `replaced_by` or `supersedes`. If the target slug names multiple
    versions in the DB, the link is ambiguous and we error. Key shape
    (required/allowed) is enforced upstream by `_validate_same_as`.
    """
    provider_slug = ref["provider"]
    classification_slug = ref["classification_slug"]
    versions = by_slug.get((provider_slug, classification_slug), [])
    if not versions:
        raise _err(
            "slug_same_as_unknown_classification",
            f"classifications.toml: classification.{entry.source_id!r} "
            f"same_as target {provider_slug}/{classification_slug!r} does "
            f"not exist in this build.",
            "Fix the slug typo, or mark the originating entry "
            "deprecated=true if the target classification is retired.",
        )
    if len(versions) > 1:
        raise _err(
            "slug_same_as_ambiguous_classification",
            f"classifications.toml: classification.{entry.source_id!r} "
            f"same_as target slug {classification_slug!r} matches multiple "
            f"versions ({sorted(versions)!r}); the link is ambiguous.",
            "Use distinct classification slugs per family (cross-version "
            "drift belongs in `supersedes` / `replaced_by`, not same_as).",
        )
    return (provider_slug, classification_slug)


def _reject_same_as_cycles(edges: list[tuple[Any, Any]], *, label: str) -> None:
    """Reject directed cycles in the as-declared same_as graph.

    Even though same_as forms an equivalence (we store both directions in
    the DB at insert time), the **TOML-level** directed graph must be
    acyclic — that catches self-loops (`A → A`) and reciprocal-declaration
    typos (`A → B` + `B → A`) which are redundant and let a maintainer
    miss-edit one side.
    """
    if not edges:
        return
    adj: dict[Any, list[Any]] = {}
    for a, b in edges:
        if a == b:
            raise _err(
                "slug_same_as_self_loop",
                f"{label}: same_as entry references itself ({a!r}).",
                "Remove the self-reference.",
            )
        adj.setdefault(a, []).append(b)
        adj.setdefault(b, [])

    # WHITE = 0 unvisited, GRAY = 1 on current DFS stack, BLACK = 2 done.
    color: dict[Any, int] = dict.fromkeys(adj, 0)
    parent: dict[Any, Any] = {}

    def visit(node: Any) -> None:
        color[node] = 1
        for nxt in adj[node]:
            if color[nxt] == 1:
                # Reconstruct the cycle for a useful error.
                cycle = [nxt, node]
                cur = node
                while parent.get(cur) is not None and parent[cur] != nxt:
                    cur = parent[cur]
                    cycle.append(cur)
                cycle.append(nxt)
                raise _err(
                    "slug_same_as_cycle",
                    f"{label}: same_as forms a cycle: "
                    f"{' -> '.join(repr(n) for n in reversed(cycle))}.",
                    "Declare each equivalence from one side only; the "
                    "build stores the reverse edge automatically.",
                )
            if color[nxt] == 0:
                parent[nxt] = node
                visit(nxt)
        color[node] = 2

    for start in list(adj):
        if color[start] == 0:
            visit(start)


def materialize_same_as_edges(
    conn: sqlite3.Connection, slug_dir: Path
) -> dict[str, int]:
    """Translate `SlugEntry.same_as` references into edge rows.

    Variables and classifications each get their own edge table. Each TOML
    edge becomes two rows (A→B and B→A) so the resolver can BFS in one
    direction without a UNION lookup. Cycles in the as-declared directed
    graph are rejected (§5.5).

    Runs after `populate_slugs` — register/variant/version slugs must be
    written before we can validate same_as targets against them.
    """
    entries = load_slug_dir(slug_dir)

    # Pre-index classifications by (provider, slug) so we can detect ambiguous
    # target lookups without a row-per-target query.
    class_by_slug: dict[tuple[str, str], list[str]] = {}
    # Classifications carry no provider in classifications.toml; they belong
    # to the SCB-wide registry. Treat the publisher field as the provider.
    cls_rows = conn.execute(
        "SELECT short_name, slug, version, publisher FROM classification "
        "WHERE slug IS NOT NULL"
    ).fetchall()
    for row in cls_rows:
        publisher_slug = (row[3] or "scb").lower()
        class_by_slug.setdefault((publisher_slug, row[1]), []).append(row[2])

    var_edges: list[tuple[_VarKey, _VarKey]] = []
    class_edges: list[tuple[_ClassKey, _ClassKey]] = []

    for entry in entries:
        if not entry.same_as:
            continue
        if entry.deprecated:
            # The whole point of deprecated is "slug retained, no new links" —
            # don't materialize from a retired side. Resolver still walks
            # through if the *other* side points back via its own same_as.
            continue

        if entry.kind == "variable":
            if entry.provider is None:
                raise _err(
                    "slug_same_as_internal",
                    f"variable.{entry.source_id!r}: missing provider context "
                    f"(SlugEntry.provider is None).",
                    "Report this as a bug — provider should be set by the TOML loader.",
                )
            register_id, var_id = _parse_variant_id(entry.source_id)
            row = conn.execute(
                "SELECT r.slug FROM register r "
                "JOIN provider p ON r.provider_id = p.provider_id "
                "WHERE p.slug = ? AND r.register_id = ?",
                (entry.provider, register_id),
            ).fetchone()
            if row is None or row[0] is None:
                raise _err(
                    "slug_same_as_unresolved_source",
                    f"{entry.provider}.toml: variable.{entry.source_id!r} "
                    f"source register has no slug; cannot anchor same_as.",
                    "Ensure the register has a slug entry (populate_slugs "
                    "should have written it).",
                )
            src_variable_slug = _variable_source_slug(conn, register_id, var_id, entry)
            src: _VarKey = (
                entry.provider,
                row[0],
                "",
                "",
                src_variable_slug,
            )
            for ref in entry.same_as:
                tgt = _validate_variable_target(conn, entry, ref)
                var_edges.append((src, tgt))

        elif entry.kind == "classification":
            row = conn.execute(
                "SELECT slug, version, publisher FROM classification "
                "WHERE short_name = ?",
                (entry.source_id,),
            ).fetchone()
            if row is None or row[0] is None:
                raise _err(
                    "slug_same_as_unresolved_source",
                    f"classifications.toml: classification.{entry.source_id!r} "
                    f"has no DB slug; cannot anchor same_as.",
                    "Ensure populate_slugs wrote the slug column first.",
                )
            src_provider = (row[2] or "scb").lower()
            src_key: _ClassKey = (src_provider, row[0])
            for ref in entry.same_as:
                tgt_key = _validate_classification_target(entry, ref, class_by_slug)
                class_edges.append((src_key, tgt_key))

    _reject_same_as_cycles(list(var_edges), label="variable same_as")
    _reject_same_as_cycles(list(class_edges), label="classification same_as")

    # Insert both directions. Plain INSERT — cycle detection above already
    # rejects reciprocal declarations, so any PK collision here is a build
    # invariant violation that we want to surface, not silence.
    for a, b in var_edges:
        for src_t, tgt_t in ((a, b), (b, a)):
            conn.execute(
                "INSERT INTO variable_same_as ("
                "a_provider, a_register, a_variant, a_period, a_variable, "
                "b_provider, b_register, b_variant, b_period, b_variable) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (*src_t, *tgt_t),
            )
    for a_k, b_k in class_edges:
        for src_c, tgt_c in ((a_k, b_k), (b_k, a_k)):
            conn.execute(
                "INSERT INTO classification_same_as ("
                "a_provider, a_classification_slug, "
                "b_provider, b_classification_slug) VALUES (?, ?, ?, ?)",
                (*src_c, *tgt_c),
            )

    return {"variable": len(var_edges), "classification": len(class_edges)}


# ---------------------------------------------------------------------------
# seed-slugs
# ---------------------------------------------------------------------------


_PAREN_RE = re.compile(r"\s*\(([^)]*)\)")
# SCB ships some registers under English Survey/Statistics sibling names
# (e.g. "Continuing Vocational Training Statistics" vs "... Survey").
_SURVEY_STATS_RE = re.compile(r"\b(survey|statistics)\b", re.IGNORECASE)
DefaultCandidateClass = Literal["exact", "near", "kept"]


def _fold(s: str) -> str:
    """Case/diacritic/whitespace-insensitive fold used by the _default heuristic."""
    if not s:
        return ""
    folded = unicodedata.normalize("NFKD", s.strip().lower())
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", folded)


def _strip_parens(s: str) -> str:
    return _PAREN_RE.sub("", s).strip()


def _parenthetical_contents(s: str) -> list[str]:
    return [m.group(1) for m in _PAREN_RE.finditer(s)]


def _canonical(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", _fold(s))


def classify_default_candidate(
    register_name: str, variant_name: str
) -> tuple[DefaultCandidateClass, str]:
    """Classify a single-variant register as an ``_default`` candidate.

    Returns ``(class, reason)`` where class is one of:

    - ``exact``: variant name mirrors register name (PR #90's 73 exact rows).
    - ``near``: mirror after parenthetical-abbrev strip, parenthetical-only
      variant name, Survey/Statistics sibling swap, or one canonical form is
      a substring of the other (PR #90's 34 near rows).
    - ``kept``: variant name carries unique disambiguating info.

    The function only judges name-mirror similarity. Callers decide whether
    `_default` is appropriate (e.g. by also checking single-variant cardinality).
    """
    fr, fv = _fold(register_name), _fold(variant_name)
    if not fr or not fv:
        return "kept", "missing name"
    if fr == fv:
        return "exact", "Registernamn == Registervariantnamn"
    spr, spv = _strip_parens(register_name), _strip_parens(variant_name)
    if _fold(spr) == _fold(spv):
        return "near", "match after stripping parenthetical abbrev"
    for abbr in _parenthetical_contents(register_name):
        if _fold(abbr) == fv:
            return "near", f"variant name matches register parenthetical ({abbr!r})"
    for abbr in _parenthetical_contents(variant_name):
        if _fold(abbr) == fr:
            return "near", f"register name matches variant parenthetical ({abbr!r})"
    swapped = _SURVEY_STATS_RE.sub(
        lambda m: "statistics" if m.group(1).lower() == "survey" else "survey",
        spr,
    )
    if _fold(swapped) == _fold(spv):
        return "near", "Survey/Statistics sibling"
    cr, cv = _canonical(spr), _canonical(spv)
    if cr and cv and min(len(cr), len(cv)) >= 5:
        shorter, longer = (cr, cv) if len(cr) <= len(cv) else (cv, cr)
        if shorter in longer:
            return "near", "shorter canonical form is a substring of the longer"
    return "kept", "descriptive variant name carries unique information"


@dataclass(frozen=True)
class DefaultSlugCandidate:
    """One single-variant register that's a candidate for ``slug = "_default"``."""

    provider: str
    source_id: str  # `<RegisterId>.<RegVarID>`
    register_name: str
    variant_name: str
    classification: DefaultCandidateClass
    reason: str
    current_slug: str | None  # None if the variant has no slug curated yet


def iter_default_slug_candidates(
    conn: sqlite3.Connection,
) -> Iterator[DefaultSlugCandidate]:
    """Walk every single-variant register across all providers.

    Yields one ``DefaultSlugCandidate`` per row (including ``kept`` ones) so
    callers can present the full picture. The seed-slug hint filters to
    exact + near; the bootstrap script shows all three classes.
    """
    rows = conn.execute(
        "SELECT p.slug, r.register_id, r.registernamn, "
        "rv.regvar_id, rv.registervariantnamn, rv.slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE rv.register_id IN ("
        "  SELECT register_id FROM register_variant "
        "  GROUP BY register_id HAVING COUNT(*) = 1"
        ") "
        "ORDER BY p.slug, r.register_id, rv.regvar_id"
    ).fetchall()
    for provider, rid, rname, vid, vname, current_slug in rows:
        cls, reason = classify_default_candidate(rname or "", vname or "")
        yield DefaultSlugCandidate(
            provider=provider,
            source_id=f"{rid}.{vid}",
            register_name=rname or "",
            variant_name=vname or "",
            classification=cls,
            reason=reason,
            current_slug=current_slug,
        )


_DEFAULT_SLUG = "_default"
_HINT_PREVIEW_LIMIT = 5


def format_default_slug_hints(
    candidates: list[DefaultSlugCandidate], *, all_hints: bool
) -> str | None:
    """Format the stderr hint block for ``seed-slugs``.

    Only flags candidates whose current slug differs from ``_default`` — once
    a maintainer has applied the suggestion the hint goes silent on the next
    run. Returns ``None`` when there's nothing to suggest.
    """
    actionable = [
        c
        for c in candidates
        if c.classification in ("exact", "near") and c.current_slug != _DEFAULT_SLUG
    ]
    if not actionable:
        return None
    total = len(actionable)
    shown = actionable if all_hints else actionable[:_HINT_PREVIEW_LIMIT]
    lines = [
        f"Hint: {total} single-variant register(s) have name-mirror variants — "
        f'consider `slug = "_default"`.',
    ]
    for cand in shown:
        lines.append(f"  {cand.provider}/{cand.source_id}   {cand.register_name!r}")
    if not all_hints and total > len(shown):
        remaining = total - len(shown)
        lines.append(
            f"  ... ({remaining} more — pass --all-hints to see the full list)"
        )
    return "\n".join(lines) + "\n"


def seed_provider_toml(conn: sqlite3.Connection, provider_slug: str) -> str:
    """Emit a starter TOML for ``provider_slug`` from the live build.

    Auto-derives a slug for each register/register_variant from
    ``registernamn`` / ``registervariantnamn``; the maintainer edits the
    result by hand before committing. Variables are auto-slugged from
    kolumnnamn at build time, so they're omitted from the seed.
    """
    lines: list[str] = [
        f"# Starter slug TOML for provider {provider_slug!r}.",
        "# Generated by `reg-meta-build seed-slugs`. Hand-review every slug,",
        "# then commit to reg_meta_build/fqid_slugs/.",
        "",
    ]
    regs = conn.execute(
        "SELECT r.register_id, r.registernamn, r.slug FROM register r "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? ORDER BY r.register_id",
        (provider_slug,),
    ).fetchall()
    if not regs:
        lines.append(f"# (no registers found for provider {provider_slug!r})\n")
        return "\n".join(lines)
    # Pre-fetch variants and versions, grouped by register_id for hierarchical
    # emission (register → its variants → its version overrides).
    variants_by_reg: dict[int, list[tuple[int, str | None, str | None]]] = {}
    for row in conn.execute(
        "SELECT rv.register_id, rv.regvar_id, rv.registervariantnamn, rv.slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? "
        "ORDER BY rv.register_id, rv.regvar_id",
        (provider_slug,),
    ).fetchall():
        register_id, regvar_id, name, existing_slug = row
        variants_by_reg.setdefault(register_id, []).append(
            (regvar_id, name, existing_slug)
        )

    versions_by_reg: dict[int, list[tuple[int, int, str | None, str | None]]] = {}
    # First claimant of each effective slug under a given regvar_id. Used to
    # auto-emit the §5.3 rule 5 collision annotation `(vs <regver>:<slug>)`:
    # if the row we're about to emit has a `derive_period(name)` that another
    # regver already holds, that other regver is named in the comment so a
    # future curator immediately sees *why* the curated slug isn't the bare
    # period. SQL `ORDER BY ... regver_id` gives deterministic claimants.
    sibling_slug_claimants: dict[int, dict[str, int]] = {}
    for row in conn.execute(
        "SELECT rv.register_id, rver.regvar_id, rver.regver_id, "
        "rver.registerversionnamn, rver.slug "
        "FROM register_version rver "
        "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? "
        "ORDER BY rv.register_id, rver.regvar_id, rver.regver_id",
        (provider_slug,),
    ).fetchall():
        register_id, regvar_id, regver_id, name, existing_slug = row
        versions_by_reg.setdefault(register_id, []).append(
            (regvar_id, regver_id, name, existing_slug)
        )
        effective = existing_slug or derive_period(name)
        if effective is not None:
            sibling_slug_claimants.setdefault(regvar_id, {}).setdefault(
                effective, regver_id
            )

    for register_id, name, existing_slug in regs:
        candidate = existing_slug or derive_variable_slug(name) or "TODO"
        # Register-level audit comment: the `registernamn` is the
        # authoritative source of what this register is. Makes the file
        # scannable when the slug is an opaque acronym (e.g. `fou`, `kkv`).
        if name:
            lines.append(f"# {_toml_comment(name)}")
        lines.append(f"[register.{_toml_str(str(register_id))}]")
        lines.append(f"slug = {_toml_str(candidate)}")
        lines.append("")

        for regvar_id, vname, existing_slug in variants_by_reg.get(register_id, []):
            v_candidate = existing_slug or (
                derive_variable_slug(vname) if vname else None
            )
            v_candidate = v_candidate or "TODO"
            lines.append(
                f"[register_variant.{_toml_str(f'{register_id}.{regvar_id}')}]"
            )
            lines.append(f"slug = {_toml_str(v_candidate)}")
            if vname:
                lines.append(f"display_group = {_toml_str(vname)}")
            lines.append("")

        # Emit only version overrides that need TOML curation. Skip rows where
        # the next build's auto-derive will reproduce the current state AND
        # the source name has no scope info beyond the period token — i.e.
        # periodized name, slug matches derived (or is NULL waiting for fill),
        # and no `_period_residual`. Curated overrides whose slug differs from
        # `derive_period(name)` (collision-resolution slugs like
        # `ankor-anklingar-1968-1997`) and residual-bearing rows like
        # `Strandlinje, 2019` (period match but `Strandlinje,` dropped on
        # auto-derive) must round-trip through a reseed so the curator sees
        # the stub and decides: rename, or accept by committing `slug = derived`.
        for regvar_id, regver_id, vername, existing_slug in versions_by_reg.get(
            register_id, []
        ):
            derived = derive_period(vername)
            residual = _period_residual(vername)
            if (
                derived is not None
                and existing_slug in (None, derived)
                and residual is None
            ):
                continue
            key = f"{register_id}.{regvar_id}.{regver_id}"
            # Audit comment: preserves the source `registerversionnamn` verbatim
            # (wrapped in single quotes) so the next curator can verify any
            # typo/abbreviation normalization (§5.3). Two terse parentheticals
            # may attach OUTSIDE the quotes:
            #   `(vs <claimant>:<slug>)` — derive_period(name) collides with a
            #       sibling's effective slug (§5.3 rule 5).
            #   `(residual: "<text>")` — auto-derive would drop scope info from
            #       the source name (§5.3 rule 6).
            # Both can co-occur on the same row; order is collision then residual.
            if vername:
                quoted_name = f"'{_toml_comment(vername)}'"
                annotations = ""
                if derived is not None:
                    claimant = sibling_slug_claimants.get(regvar_id, {}).get(derived)
                    if claimant is not None and claimant != regver_id:
                        annotations += f" (vs {claimant}:{derived})"
                if residual is not None:
                    annotations += f" (residual: {_toml_str(residual)})"
                lines.append(f"# {quoted_name}{annotations}")
            lines.append(f"[register_version.{_toml_str(key)}]")
            # Residual-bearing rows pre-fill with the auto-derive value so doing
            # nothing accepts auto-derive (curator commits the explicit entry as
            # an acknowledgement); renaming the slug is the active curation path.
            lines.append(f"slug = {_toml_str(existing_slug or derived or 'TODO')}")
            lines.append("")
    return "\n".join(lines)


def seed_classifications_toml(conn: sqlite3.Connection) -> str:
    """Emit a starter TOML for classifications. The maintainer edits the
    auto-derived slug — classification short_names like ``SUN2020-NIVA``
    don't fold to a great default."""
    lines: list[str] = [
        "# Starter classification slug TOML.",
        "# Generated by `reg-meta-build seed-slugs`. Hand-review the slug",
        "# (auto-derived from short_name, often needs shortening) and version.",
        "",
    ]
    rows = conn.execute(
        "SELECT short_name, version FROM classification ORDER BY short_name"
    ).fetchall()
    if not rows:
        lines.append("# (no classifications populated yet)\n")
        return "\n".join(lines)
    for short, version in rows:
        candidate = derive_variable_slug(short) or "TODO"
        lines.append(f"[classification.{_toml_str(short)}]")
        lines.append(f"slug = {_toml_str(candidate)}")
        lines.append(f"version = {_toml_str(version or 'TODO')}")
        lines.append("")
    return "\n".join(lines)


def seed_all(conn: sqlite3.Connection, out_dir: Path) -> dict[str, Path]:
    """Write a starter TOML for every distinct provider plus classifications."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for provider_slug in _live_providers(conn):
        path = out_dir / f"{provider_slug}.toml"
        path.write_text(seed_provider_toml(conn, provider_slug), encoding="utf-8")
        written[path.name] = path
    cls_path = out_dir / CLASSIFICATIONS_FILE
    cls_path.write_text(seed_classifications_toml(conn), encoding="utf-8")
    written[cls_path.name] = cls_path
    return written


# ---------------------------------------------------------------------------
# precheck-slugs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PrecheckResult:
    missing_registers: tuple[tuple[str, str, str], ...]  # (provider, id, name)
    missing_variants: tuple[tuple[str, str, str], ...]
    missing_versions: tuple[tuple[str, str, str], ...]  # unperiodized + un-curated
    missing_classifications: tuple[str, ...]
    parse_errors: tuple[str, ...]
    # Reverse direction: TOML source IDs that don't (or no longer) exist in
    # the DB and would fail `populate_slugs(strict=True)` at build time.
    # Deprecated entries are excluded — they're allowed to outlive their DB
    # row. Non-fatal: precheck surfaces them so maintainers can drop or mark
    # them before a build attempt.
    stale_registers: tuple[tuple[str, str], ...] = ()  # (provider, source_id)
    stale_variants: tuple[tuple[str, str], ...] = ()
    stale_versions: tuple[tuple[str, str], ...] = ()
    stale_classifications: tuple[str, ...] = ()
    # Sibling register_version rows that would share a slug under the
    # populate_slugs (TOML-override-else-derived) algorithm, violating
    # `UNIQUE(regvar_id, slug)` mid-build. One entry per colliding row,
    # carrying the slug they'd all land on. Surfaces the failure at
    # precheck time instead of as a raw sqlite IntegrityError later.
    colliding_versions: tuple[tuple[str, str, str, str], ...] = ()
    entries: tuple[SlugEntry, ...] = ()

    @property
    def ok(self) -> bool:
        return not (
            self.missing_registers
            or self.missing_variants
            or self.missing_versions
            or self.missing_classifications
            or self.parse_errors
            or self.stale_registers
            or self.stale_variants
            or self.stale_versions
            or self.stale_classifications
            or self.colliding_versions
        )


def precheck_slugs(conn: sqlite3.Connection, slug_dir: Path) -> PrecheckResult:
    """Enumerate live source IDs that lack a slug entry, plus any TOML
    parse / validation errors. Does not raise on missing slugs — callers
    decide whether to exit on the result.
    """
    parse_errors: list[str] = []
    entries: list[SlugEntry] = []
    try:
        entries = load_slug_dir(slug_dir)
    except RegMetaError as exc:
        parse_errors.append(exc.message)

    by_provider_kind: dict[tuple[str, str], set[str]] = {}
    classification_ids: set[str] = set()
    # Curated version-slot overrides keyed (provider, source_id) -> slug.
    # The collision pass needs the actual slug value, not just whether an
    # entry exists; otherwise it can't tell a sibling-disambiguating override
    # apart from one that re-collides with an auto-derived sibling.
    toml_version_slugs: dict[tuple[str, str], str] = {}
    for entry in entries:
        if entry.kind == "classification":
            classification_ids.add(entry.source_id)
        elif entry.provider is not None and entry.slug is not None:
            by_provider_kind.setdefault((entry.provider, entry.kind), set()).add(
                entry.source_id
            )
            if entry.kind == "register_version":
                toml_version_slugs[(entry.provider, entry.source_id)] = entry.slug

    # One pass per kind. The same row sets feed both the missing-slug check
    # (live row, no TOML entry) and the stale-entry check (TOML entry, no live
    # row), so we materialize each once.
    live_regs_by_provider: dict[str, set[int]] = {}
    live_vars_by_provider: dict[str, set[tuple[int, int]]] = {}
    live_versions_by_provider: dict[str, set[tuple[int, int, int]]] = {}
    missing_regs: list[tuple[str, str, str]] = []
    missing_variants: list[tuple[str, str, str]] = []
    missing_versions: list[tuple[str, str, str]] = []
    colliding_versions: list[tuple[str, str, str, str]] = []
    for provider_slug in _live_providers(conn):
        slugged_regs = by_provider_kind.get((provider_slug, "register"), set())
        reg_rows = conn.execute(
            "SELECT r.register_id, r.registernamn FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? ORDER BY r.register_id",
            (provider_slug,),
        ).fetchall()
        live_regs_by_provider[provider_slug] = {rid for (rid, _) in reg_rows}
        for register_id, registernamn in reg_rows:
            if str(register_id) not in slugged_regs:
                missing_regs.append(
                    (provider_slug, str(register_id), registernamn or "")
                )

        slugged_variants = by_provider_kind.get(
            (provider_slug, "register_variant"), set()
        )
        var_rows = conn.execute(
            "SELECT rv.register_id, rv.regvar_id, rv.registervariantnamn, rv.slug "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? ORDER BY rv.register_id, rv.regvar_id",
            (provider_slug,),
        ).fetchall()
        live_vars_by_provider[provider_slug] = {
            (rid, vid) for (rid, vid, _, _) in var_rows
        }
        for register_id, regvar_id, name, _slug in var_rows:
            key = f"{register_id}.{regvar_id}"
            if key not in slugged_variants:
                missing_variants.append((provider_slug, key, name or ""))

        # register_version: only the rows whose registerversionnamn doesn't
        # auto-derive a period need a TOML entry. Periodized versions get
        # their slug filled by populate_slugs without any curation.
        slugged_versions = by_provider_kind.get(
            (provider_slug, "register_version"), set()
        )
        ver_rows = conn.execute(
            "SELECT rv.register_id, rver.regvar_id, rver.regver_id, "
            "rver.registerversionnamn "
            "FROM register_version rver "
            "JOIN register_variant rv ON rver.regvar_id = rv.regvar_id "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? "
            "ORDER BY rver.regver_id",
            (provider_slug,),
        ).fetchall()
        live_versions_by_provider[provider_slug] = {
            (rid, vid, verid) for (rid, vid, verid, _) in ver_rows
        }
        # Walk version rows once: (1) flag unperiodized rows with no TOML
        # entry, (2) group by (regvar_id, would-be-slug) for the collision
        # pass. The would-be slug mirrors populate_slugs's resolution order:
        # TOML override first, else derive_period(name). Rows where both are
        # None fall out of the collision pass — they're already caught by (1).
        siblings_by_slug: dict[tuple[int, str], list[tuple[str, str]]] = {}
        for register_id, regvar_id, regver_id, name in ver_rows:
            key = f"{register_id}.{regvar_id}.{regver_id}"
            override = toml_version_slugs.get((provider_slug, key))
            would_be_slug = override or derive_period(name)
            if would_be_slug is None:
                if key not in slugged_versions:
                    missing_versions.append((provider_slug, key, name or ""))
                continue
            siblings_by_slug.setdefault((regvar_id, would_be_slug), []).append(
                (key, name or "")
            )

        for (_regvar_id, slug), rows in siblings_by_slug.items():
            if len(rows) > 1:
                for key, name in rows:
                    colliding_versions.append((provider_slug, key, name, slug))

    db_classifications: set[str] = set()
    missing_classifications: list[str] = []
    for (short,) in conn.execute(
        "SELECT short_name FROM classification ORDER BY short_name"
    ).fetchall():
        db_classifications.add(short)
        if short not in classification_ids:
            missing_classifications.append(short)

    stale_regs, stale_vars, stale_vers, stale_cls = _stale_toml_entries(
        entries,
        live_regs_by_provider=live_regs_by_provider,
        live_vars_by_provider=live_vars_by_provider,
        live_versions_by_provider=live_versions_by_provider,
        db_classifications=db_classifications,
    )

    return PrecheckResult(
        missing_registers=tuple(missing_regs),
        missing_variants=tuple(missing_variants),
        missing_versions=tuple(missing_versions),
        missing_classifications=tuple(missing_classifications),
        parse_errors=tuple(parse_errors),
        stale_registers=tuple(stale_regs),
        stale_variants=tuple(stale_vars),
        stale_versions=tuple(stale_vers),
        stale_classifications=tuple(stale_cls),
        colliding_versions=tuple(colliding_versions),
        entries=tuple(entries),
    )


def _stale_toml_entries(
    entries: list[SlugEntry],
    *,
    live_regs_by_provider: dict[str, set[int]],
    live_vars_by_provider: dict[str, set[tuple[int, int]]],
    live_versions_by_provider: dict[str, set[tuple[int, int, int]]],
    db_classifications: set[str],
) -> tuple[
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[str],
]:
    """Find TOML entries whose source IDs don't exist in the DB.

    Mirrors the `slug_unknown_source_id` check inside `populate_slugs`, but
    surfaces every stale entry at once instead of failing on the first.
    Deprecated entries are excluded — they're allowed to outlive their DB row.
    Variable entries are excluded — slug overrides are not yet supported and
    `populate_slugs` rejects them with a clearer error.

    Source IDs are guaranteed parseable here: `_validate_entry` calls
    `_parse_register_id` / `_parse_variant_id` / `_parse_version_id` at TOML
    load.
    """
    stale_regs: list[tuple[str, str]] = []
    stale_vars: list[tuple[str, str]] = []
    stale_vers: list[tuple[str, str]] = []
    stale_cls: list[str] = []

    for entry in entries:
        if entry.deprecated:
            continue
        if entry.kind == "register" and entry.provider is not None:
            live = live_regs_by_provider.get(entry.provider, set())
            if _parse_register_id(entry.source_id) not in live:
                stale_regs.append((entry.provider, entry.source_id))
        elif entry.kind == "register_variant" and entry.provider is not None:
            live = live_vars_by_provider.get(entry.provider, set())
            if _parse_variant_id(entry.source_id) not in live:
                stale_vars.append((entry.provider, entry.source_id))
        elif entry.kind == "register_version" and entry.provider is not None:
            live_v = live_versions_by_provider.get(entry.provider, set())
            if _parse_version_id(entry.source_id) not in live_v:
                stale_vers.append((entry.provider, entry.source_id))
        elif entry.kind == "classification":
            if entry.source_id not in db_classifications:
                stale_cls.append(entry.source_id)

    return stale_regs, stale_vars, stale_vers, stale_cls


# ---------------------------------------------------------------------------
# Snapshot (§5.4 grow-only immutability)
# ---------------------------------------------------------------------------


def snapshot_payload(entries: list[SlugEntry]) -> dict[str, dict[str, str]]:
    """Build the canonical ``{key: slug}`` snapshot. Keys are stable per
    entry; renaming or removing a key fails the snapshot test.

    Variables without a TOML-declared slug are skipped — they auto-derive at
    build time and aren't part of the curated, hand-frozen set.
    """
    payload: dict[str, dict[str, str]] = {kind: {} for kind in ENTITY_KINDS}
    for entry in entries:
        if entry.slug is None:
            continue
        if entry.kind == "classification":
            key = f"{entry.source_id}|{entry.version}"
        elif entry.provider is not None:
            key = f"{entry.provider}/{entry.source_id}"
        else:
            key = entry.source_id
        payload[entry.kind][key] = entry.slug
    return payload


def read_snapshot(snapshot_path: Path) -> dict[str, dict[str, str]]:
    if not snapshot_path.is_file():
        return {kind: {} for kind in ENTITY_KINDS}
    try:
        data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _err(
            "slug_snapshot_unreadable",
            f"Could not parse slug snapshot {snapshot_path}: {exc}",
            "Regenerate it with `reg-meta-build precheck-slugs --update-snapshot`.",
        ) from exc
    return {kind: dict(data.get(kind) or {}) for kind in ENTITY_KINDS}


def write_snapshot(snapshot_path: Path, payload: dict[str, dict[str, str]]) -> None:
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def diff_snapshot(
    previous: dict[str, dict[str, str]],
    current: dict[str, dict[str, str]],
) -> dict[str, list[str]]:
    """Compare two snapshot payloads. Adds are allowed; removes and slug
    renames are reported. Returned dict has three keys: ``removed``,
    ``renamed``, ``added``."""
    removed: list[str] = []
    renamed: list[str] = []
    added: list[str] = []
    for kind in ENTITY_KINDS:
        prev = previous.get(kind, {})
        cur = current.get(kind, {})
        for key, slug in prev.items():
            if key not in cur:
                removed.append(f"{kind}/{key} (was {slug!r})")
                continue
            if cur[key] != slug:
                renamed.append(f"{kind}/{key}: {slug!r} -> {cur[key]!r}")
        for key, slug in cur.items():
            if key not in prev:
                added.append(f"{kind}/{key} = {slug!r}")
    return {"removed": removed, "renamed": renamed, "added": added}


__all__ = (
    "CLASSIFICATIONS_FILE",
    "ENTITY_KINDS",
    "DefaultCandidateClass",
    "DefaultSlugCandidate",
    "EntityKind",
    "PrecheckResult",
    "SNAPSHOT_FILENAME",
    "SlugEntry",
    "UNFROZEN_MARKER",
    "classify_default_candidate",
    "diff_snapshot",
    "format_default_slug_hints",
    "is_unfrozen",
    "iter_default_slug_candidates",
    "load_classifications_toml",
    "load_provider_toml",
    "load_slug_dir",
    "populate_slugs",
    "precheck_slugs",
    "read_snapshot",
    "repo_slug_dir",
    "seed_all",
    "seed_classifications_toml",
    "seed_provider_toml",
    "snapshot_payload",
    "write_snapshot",
)
