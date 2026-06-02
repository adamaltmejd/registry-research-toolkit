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
import tomllib
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import (
    FqidKind,
    derive_variable_slug,
    validate_slug,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterator

# A2.6: `register_version` is gone — the FQID grammar has no version segment
# (§5.2), version slugs are no longer curated or persisted, and the build-time
# `register_version` table is dropped before ship.
EntityKind = Literal[
    "register",
    "register_variant",
    "variable",
    "classification",
]
ENTITY_KINDS: tuple[EntityKind, ...] = (
    "register",
    "register_variant",
    "variable",
    "classification",
)
PROVIDER_FILE_SUFFIX = ".toml"
# Auto-derived variable slugs live alongside the hand-curated `<provider>.toml`
# in `<provider>.auto.toml` (§5.3). Both files feed one in-memory index; the
# hand-curated file wins on key clash. The auto file is build-generated
# (`write_auto_toml`) and committed; it must never be hand-edited.
AUTO_FILE_SUFFIX = ".auto.toml"
CLASSIFICATIONS_FILE = "classifications.toml"
SNAPSHOT_FILENAME = ".snapshot.json"

_KINDS_WITH_SAME_AS: frozenset[EntityKind] = frozenset({"variable", "classification"})

# Top-level keys accepted in a provider TOML; anything else is a typo
# (e.g. `[registers."34"]` vs the singular form) that today would otherwise
# silently no-op. `lineage_defaults` / `lineage` (§5.6) are NOT SlugEntry rows
# — `load_lineage_config` parses them separately — but they're legal top-level
# tables, so the strict typo check must accept them.
_PROVIDER_TOPLEVEL_KEYS: frozenset[str] = frozenset(
    {
        "register",
        "register_variant",
        "variable",
        "lineage_defaults",
        "lineage",
    }
)
_CLASSIFICATIONS_TOPLEVEL_KEYS: frozenset[str] = frozenset({"classification"})

# Allowed keys for inline `same_as` tables (§5.3 worked examples).
# A2.1.5: variable same_as is variable-grain — no `register_variant`/`period`
# narrowing keys (the variant/period slots were dropped, §5.5). One edge covers
# every variant/period that delivers either variable.
_SAME_AS_KEYS_VARIABLE: frozenset[str] = frozenset(
    {"provider", "register", "variable_slug"}
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
        return frozenset(base | {"same_as"})
    if kind == "variable":
        return frozenset(base | {"same_as"})
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
    # The register_variant slug is a delivery coordinate (§5.1) that still
    # persists a real `_default` row for variant-less registers, so it keeps
    # `allow_default`. No other slot allows `_default` or period-shaped slugs.
    # `kind` is the slot label here — `validate_slug` only uses it for the error
    # message (REGISTER_VARIANT left FqidKind in A2.6, so pass the string).
    try:
        validate_slug(slug, kind, allow_default=(kind == "register_variant"))
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
    # malformed keys. `[register."01"]` (leading zero) fails at TOML load with a
    # clear field-shape error rather than blowing up later as a
    # `slug_unknown_source_id` lookup miss. A `variable` key takes an optional
    # third segment — the §5.7 split-sibling discriminator (A2.2).
    if kind == "register":
        _parse_register_id(source_id)
    elif kind == "register_variant":
        _parse_variant_id(source_id)
    elif kind == "variable":
        _parse_variable_id(source_id)
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
    # A2.6.1: classifications no longer carry a `version` field — the vintage
    # is baked into the slug (`class/<slug>`, e.g. `sun2020`). A stray `version`
    # on any entry is rejected by the unknown-field guard above (it's no longer
    # in `_allowed_fields`); the baked slug is validated by `_validate_entry`'s
    # normal slug-grammar check, same as every other slug.
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


def _provider_from_path(path: Path) -> str:
    """Provider slug from a provider TOML filename.

    Both ``scb.toml`` and the build-generated ``scb.auto.toml`` belong to
    provider ``scb``. ``path.stem`` would yield ``scb.auto`` for the latter
    (the dot fails the provider slug grammar), so strip the ``.auto`` suffix
    explicitly.
    """
    if path.name.endswith(AUTO_FILE_SUFFIX):
        return path.name[: -len(AUTO_FILE_SUFFIX)]
    return path.stem


def load_provider_toml(path: Path) -> list[SlugEntry]:
    """Parse a per-provider slug TOML (``scb.toml``, ``sos.toml``, …).

    Also handles the build-generated ``<provider>.auto.toml`` (§5.3): same
    grammar and shape, only auto-derived variable rows in practice. The
    ``.auto`` companion maps to the same provider as ``<provider>.toml``.
    """
    provider = _provider_from_path(path)
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
    # Source IDs are dotted: variant/variable `<reg>.<sub>`.
    seen_slugs: dict[tuple[str, ...], str] = {}
    for kind in ("register", "register_variant", "variable"):
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
    seen_slugs: dict[str, str] = {}
    for source_id, raw in table.items():
        if not isinstance(raw, dict):
            raise _err(
                "slug_toml_invalid",
                f"{path.name}: classification.{source_id!r} must be a TOML table.",
                'Use [classification."<short_name>"] = { slug = ... }.',
            )
        entry = _validate_entry("classification", source_id, raw, provider=None)
        # A2.6.1: the classification FQID is the 2-segment `class/<slug>`, so
        # the slug alone must be unique (the vintage is baked in, §5.2).
        slug_key = entry.slug or ""
        prev = seen_slugs.get(slug_key)
        if prev is not None:
            raise _err(
                "slug_toml_invalid",
                f"{path.name}: slug {entry.slug!r} reused by {prev!r} and "
                f"{source_id!r}.",
                "Classification FQIDs are slugs — keep them unique.",
            )
        seen_slugs[slug_key] = source_id
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


def _parse_variable_id(source_id: str) -> tuple[int, str]:
    """Variable source-ID key: `<RegisterId>.<VarId>` for a 1:1 variable, or
    `<RegisterId>.<VarId>.<discriminator>` for a §5.7 split sibling (A2.2 —
    siblings share `VarId`; the trailing delivery-column slug disambiguates
    their auto-slug cache entries). Returns `(register_id, var_key)`.

    `RegisterId` is always a canonical integer. `VarId` is the variable's
    `provider_key`: a canonical integer for SCB (the SCB VarId), but a TEXT
    variable name for a non-SCB provider (SOS `provider_key`, e.g. `ALDER` —
    A4.4b). It is returned as a string and matched against
    `variable.provider_key` (CAST AS TEXT downstream, see `_variable_source_slug`);
    a purely-numeric VarId must still be in canonical form (no leading zeros) so
    two SCB keys can't alias one DB row."""
    parts = source_id.split(".")
    if len(parts) not in (2, 3):
        raise _err(
            "slug_toml_invalid",
            f"variable.{source_id!r}: expected `<RegisterId>.<VarId>` or "
            f"`<RegisterId>.<VarId>.<discriminator>` (split sibling).",
            "Compose the key from the register id + the variable's provider_key "
            "(plus a column discriminator for splits).",
        )
    reg = _parse_canonical_int(parts[0])
    if reg is None:
        raise _err(
            "slug_toml_invalid",
            f"variable.{source_id!r}: RegisterId must be an integer in canonical "
            f"form (no leading zeros).",
            "Use the literal register id.",
        )
    var_key = parts[1]
    if not var_key:
        raise _err(
            "slug_toml_invalid",
            f"variable.{source_id!r}: the VarId segment is empty.",
            "Use the SCB VarId or the non-SCB provider_key.",
        )
    # A purely-numeric VarId is an SCB id — enforce canonical form so `1.10` and
    # `1.010` can't alias one DB row. A non-numeric VarId is a non-SCB
    # provider_key (e.g. a SOS variable name `ALDER`), accepted as text.
    if var_key.isdigit() and _parse_canonical_int(var_key) is None:
        raise _err(
            "slug_toml_invalid",
            f"variable.{source_id!r}: a numeric VarId must be in canonical form "
            f"(no leading zeros).",
            "Use the literal SCB VarId.",
        )
    if len(parts) == 3 and not parts[2]:
        raise _err(
            "slug_toml_invalid",
            f"variable.{source_id!r}: split-sibling discriminator is empty.",
            "Provide a non-empty delivery-column discriminator.",
        )
    return reg, var_key


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
        "SELECT rv.register_id, rv.register_variant_id FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND rv.slug IS NULL",
        (provider_slug,),
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def populate_slugs(
    conn: sqlite3.Connection,
    slug_dir: Path,
    *,
    strict: bool = True,
) -> dict[str, int]:
    """Read ``slug_dir`` and write slug columns on register / register_variant /
    classification. (A2.6: register_version has no slug — version left the FQID
    grammar, §5.2.)

    ``strict=True`` (the default for real builds) refuses if any live source
    ID has no slug entry. Tests pass ``strict=False`` to populate whatever's
    available without enforcing coverage.

    Returns ``{"register": n, "register_variant": n, "classification": n}``.
    """
    # Function-level import — `db` imports `populate_slugs` at module top, so
    # importing `db` at our module top would close a cycle. Lazy resolution
    # is safe because by call time both modules are fully loaded.
    from .db import _progress

    entries = load_slug_dir(slug_dir)
    counts = {
        "register": 0,
        "register_variant": 0,
        "classification": 0,
    }

    # A2.1.5: `[variable]` slug overrides are now wired — they write the stored
    # `variable.slug` column via `populate_variable_slugs` (called separately
    # from `build_db`, after this function). `populate_slugs` itself only
    # handles register / variant / classification and intentionally
    # skips variable rows here; the override gate that previously rejected
    # `[variable] slug = ...` is lifted. A2.6: register_version has no slug
    # column or curation anymore — version is not an FQID segment (§5.2).

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
                    "WHERE register_id = ? AND register_variant_id = ?",
                    (entry.slug, entry.display_group, key[0], key[1]),
                )
                counts["register_variant"] += 1

        if strict:
            _assert_no_unslugged(
                conn,
                sql=(
                    "SELECT r.register_id, r.name FROM register r "
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
                    "SELECT rv.register_id, rv.register_variant_id, rv.name "
                    "FROM register_variant rv "
                    "JOIN register r ON rv.register_id = r.register_id "
                    "JOIN provider p ON r.provider_id = p.provider_id "
                    "WHERE p.slug = ? AND rv.slug IS NULL "
                    "ORDER BY rv.register_id, rv.register_variant_id"
                ),
                params=(provider_slug,),
                label="register_variant",
                sample_fmt=_format_variant_sample,
                provider_slug=provider_slug,
                add_hint='[register_variant."<RegisterId>.<RegVarID>"]',
            )

    for entry in classification_entries:
        if entry.slug is None:
            continue
        # A2.6.1: the slug is matched to its DB row by short_name (the TOML
        # key). There's no `version` column left to cross-check — the vintage
        # lives in the slug, which `class.slug UNIQUE` keeps globally distinct.
        row = conn.execute(
            "SELECT 1 FROM classification WHERE short_name = ?",
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
                'Add a `[classification."<short_name>"]` entry with a slug.',
            )

    _progress(
        f"  Slugged {counts['register']:,} registers, "
        f"{counts['register_variant']:,} variants, "
        f"{counts['classification']:,} classifications"
    )
    return counts


# ---------------------------------------------------------------------------
# Variable slugs (§5.3 stored `variable.slug`)
# ---------------------------------------------------------------------------


def _curated_variable_slugs(
    entries: list[SlugEntry],
) -> dict[tuple[str, str], str]:
    """Hand-curated `[variable]` slug overrides, keyed (provider, source_id).

    Only entries from the hand-curated ``<provider>.toml`` carry an explicit
    slug; auto entries from ``<provider>.auto.toml`` are returned by
    :func:`_auto_variable_slugs`. The caller passes only the curated file's
    entries here, so a curated slug always wins on a key clash regardless of
    file-load order.
    """
    out: dict[tuple[str, str], str] = {}
    for e in entries:
        if e.kind == "variable" and e.slug is not None and e.provider is not None:
            out[(e.provider, e.source_id)] = e.slug
    return out


def _auto_variable_slugs(auto_entries: list[SlugEntry]) -> dict[str, str]:
    """Auto-derived slugs from one ``<provider>.auto.toml``, keyed by source_id.

    The auto file is provider-specific (one per provider), so a bare source_id
    key is unambiguous — this is the in-memory `{source_id: slug}` index the
    population loop reads and `write_auto_toml` writes back.
    """
    out: dict[str, str] = {}
    for e in auto_entries:
        if e.kind == "variable" and e.slug is not None:
            out[e.source_id] = e.slug
    return out


def write_auto_toml(
    path: Path,
    provider: str,
    slugs: dict[str, str],
    derivation: dict[str, str] | None = None,
) -> None:
    """Emit ``<provider>.auto.toml`` with one `[variable."<id>"]` row per slug.

    Deterministic (sorted by source ID, numeric-aware) so rebuilds produce
    byte-identical output given identical inputs. Generated artifact — the
    header warns against hand-editing; curator overrides go in the
    hand-curated ``<provider>.toml`` instead (§5.3).

    ``derivation`` (A4.4a) maps ``source_id`` → derivation class (the
    ``_DERIVATION_*`` constants). When present, each ``slug = "…"`` line carries
    an inline ``# source: <class>`` comment recording HOW the slug was derived — a
    provenance aid for the later name-fallback curation seam. It is a **TOML
    comment, not a field**: ``tomllib`` ignores it, so it never reaches
    ``SlugEntry`` / ``_allowed_fields`` / ``snapshot_payload`` and a
    ``--providers=scb`` build stays byte-identical in its slug VALUES.

    An entry absent from ``derivation`` emits no comment. The caller
    (``populate_variable_slugs``) seeds ``derivation`` from BOTH the existing
    file's markers (carried forward via ``read_auto_derivations`` so an
    incremental rewrite keeps prior provenance) and this build's first-sight
    classes, so absence is limited to legacy rows written before A4.4a.
    """
    lines = [
        f"# AUTO-GENERATED by reg-meta-build — do not hand-edit ({provider}).",
        "# Auto-derived variable slugs (§5.3): one entry per (register, var)",
        "# pair, slug folded from the latest kolumnnamn on first sight and",
        "# never recomputed. Curator overrides belong in the hand-curated",
        f"# {provider}.toml; an override there shadows the entry here.",
        "# The `# source:` comment after each slug records its derivation class",
        "# (A4.4a) for the name-fallback curation seam; tomllib ignores it.",
        "",
    ]

    def _sort_key(source_id: str) -> tuple[int, int, int, str]:
        # SCB variable IDs are `<int>.<int>`; sort numerically for a stable,
        # human-scannable file. A non-integer key (a future non-SCB provider)
        # falls back to a string sort so the writer never crashes — the leading
        # flag keeps the two regimes from interleaving.
        reg, _, var = source_id.partition(".")
        if reg.isdigit() and var.isdigit():
            return (0, int(reg), int(var), "")
        return (1, 0, 0, source_id)

    derivation = derivation or {}
    for source_id in sorted(slugs, key=_sort_key):
        lines.append(f"[variable.{_toml_str(source_id)}]")
        slug_line = f"slug = {_toml_str(slugs[source_id])}"
        kind = derivation.get(source_id)
        if kind is not None:
            slug_line += f"  # source: {kind}"
        lines.append(slug_line)
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


# `[variable."<source_id>"]` header + the `# source:` derivation marker, read
# back from a committed `<provider>.auto.toml`. tomllib drops the comment, so the
# provenance round-trips through a small line scanner instead — the marker is the
# single source of truth the precheck worklist reads (no re-derivation).
_AUTO_VAR_HEADER_RE = re.compile(r'^\[variable\."(?P<source_id>[^"]+)"\]\s*$')
_AUTO_SOURCE_COMMENT_RE = re.compile(r"#\s*source:\s*(?P<kind>\S+)\s*$")


def read_auto_derivations(path: Path) -> dict[str, str]:
    """Parse the `# source:` derivation markers from a `<provider>.auto.toml`.

    Returns ``{source_id: derivation_kind}`` for every variable entry that
    carries an A4.4a marker; entries without one (e.g. legacy rows written
    before A4.4a) are simply absent. A line scanner — `tomllib` strips comments,
    so it can't surface them. Tolerant by design: a malformed file just yields a
    partial/empty map (this feeds an advisory worklist that must never gate).
    """
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return out  # tolerant by design — feeds an advisory worklist, never gates
    current: str | None = None
    for raw_line in text.splitlines():
        header = _AUTO_VAR_HEADER_RE.match(raw_line.strip())
        if header is not None:
            current = header.group("source_id")
            continue
        # `"slug "` (with the trailing space the writer always emits) — not bare
        # `"slug"` — so a hypothetical `slugfoo = …` key can't consume the slot.
        if current is not None and raw_line.lstrip().startswith("slug "):
            marker = _AUTO_SOURCE_COMMENT_RE.search(raw_line)
            if marker is not None:
                out[current] = marker.group("kind")
            current = None
    return out


# Length cap for name-fallback slugs (§5.3). kolumnnamn-derived slugs are short;
# name-derived fallbacks are truncated to a readable FQID leaf on a hyphen
# boundary. Residual truncation collisions get a numeric suffix via _uniquify.
_NAME_SLUG_MAX_LEN = 60


def _name_slug(name: str | None, *, cap: int = _NAME_SLUG_MAX_LEN) -> str | None:
    """Derive a length-capped variable slug from a variable NAME (§5.3 fallback).

    Used when the kolumnnamn-derived slug is unavailable or not register-unique.
    Real SCB registers reuse generic delivery columns (`Kolumn1`/`RadNr`/
    `OBS_VALUE`) across many distinct variables, and ~2k variables carry a
    numeric/absent kolumnnamn; in those cases the identity lives in the name.
    Reuses the kolumnnamn fold (NFKD→ASCII→lowercase→hyphenate) then truncates
    to ``cap`` on a hyphen boundary so the leaf stays readable. Returns ``None``
    when the name yields nothing slug-shaped (empty / leading-digit / all
    non-ASCII after fold).
    """
    base = derive_variable_slug(name)
    if base is None or len(base) <= cap:
        return base
    head = base[:cap]
    cut = head.rfind("-")
    if cut > 0:
        head = head[:cut]
    head = head.strip("-")
    # Truncation can land on a token the full slug escaped but the validator
    # rejects on its own — a RESERVED word (`class`) or a period (`2018`). Don't
    # store an unaddressable slug: re-validate the truncated head (derive is
    # idempotent on a valid slug, returns None on reserved/period/invalid) and
    # keep the full, known-valid base when truncation isn't usable.
    return derive_variable_slug(head) or base


def _uniquify(base: str, used: set[str]) -> str:
    """Return ``base`` if free in ``used``, else ``base-2`` / ``base-3`` / … .

    Deterministic numeric suffix so first-sight derivation is reproducible.
    Once a slug is frozen into ``<provider>.auto.toml`` (§5.3 immutability) the
    suffix is stable — a later-added variable just takes the next free index;
    existing slugs are read back from the auto file and never recomputed.
    """
    if base not in used:
        return base
    i = 2
    while f"{base}-{i}" in used:
        i += 1
    return f"{base}-{i}"


def _fallback_slug(provider_key: str) -> str:
    """Last-resort register-unique slug seed when neither kolumnnamn nor name
    yields one. Prefixes the provider key with ``v`` to satisfy the
    leading-letter grammar (``v881``); stable because ``provider_key`` is. The
    caller runs it through :func:`_uniquify` for the rare shared-key case.
    """
    return derive_variable_slug(f"v{provider_key}") or "v"


# A4.4a: derivation classes for an auto-emitted variable slug — which arm of the
# Pass-3 fallback chain (`populate_variable_slugs`) produced its base. Recorded
# as a `# source:` comment in `<provider>.auto.toml` (provenance only, never a
# field) and used to assemble the name-fallback curation worklist. The classes
# mirror the docstring numbering (steps 3-6); the `-N` disambiguator from
# `_uniquify` is recorded orthogonally as a `+disambiguated` suffix on the base
# class (`_DISAMBIGUATED_MARKER`), not a separate class.
_DERIVATION_FOLD = "fold"  # step (3) §5.7 shared column stem (triage)
_DERIVATION_DRIFT_NAME = "drift-name"  # step (3a) register-unique name basis
_DERIVATION_DRIFT_COLUMN = "drift-earliest-column"  # step (3b) earliest column
_DERIVATION_DRIFT_NAME_RESIDUAL = "drift-name-residual"  # step (3b) → name
_DERIVATION_DRIFT_KOL_RESIDUAL = "drift-kolumnnamn-residual"  # (3b) → latest col
_DERIVATION_DRIFT_FALLBACK = "drift-v-provider-key"  # step (3b) → last resort
_DERIVATION_KOLUMNNAMN = "kolumnnamn"  # step (4) register-unique latest column
_DERIVATION_NAME = "name-fallback"  # step (5) name-derived
_DERIVATION_KOL_RESIDUAL = "kolumnnamn-residual"  # step (5) → latest column
_DERIVATION_FALLBACK = "v-provider-key"  # step (6) last resort

# The curation backlog (A4.4a name-fallback worklist): classes whose base came
# from the variable NAME or the `v<provider_key>` last resort — the auto-pick a
# curator most often wants to prettify. The fold / kolumnnamn / drift-earliest-
# column classes have a column-derived basis and are NOT in the worklist. A `-N`
# disambiguated slug joins the worklist regardless of its base class (the suffix
# is itself a non-canonical, collision-driven artifact).
_NAME_FALLBACK_DERIVATIONS: frozenset[str] = frozenset(
    {
        _DERIVATION_DRIFT_NAME,
        _DERIVATION_DRIFT_NAME_RESIDUAL,
        _DERIVATION_DRIFT_KOL_RESIDUAL,
        _DERIVATION_DRIFT_FALLBACK,
        _DERIVATION_NAME,
        _DERIVATION_KOL_RESIDUAL,
        _DERIVATION_FALLBACK,
    }
)

# A `-N` disambiguator suffix (`_uniquify`) is recorded as a `+disambiguated`
# marker on the base class. Strip it to compare against the base taxonomy.
_DISAMBIGUATED_MARKER = "+disambiguated"


def _is_name_fallback_derivation(kind: str) -> bool:
    """Whether an A4.4a derivation class belongs in the name-fallback worklist.

    True for any name-derived / last-resort base class, OR for ANY class that
    carried a `-N` disambiguator (the suffix is itself a non-canonical artifact
    a curator may want to replace with a stable slug).
    """
    if kind.endswith(_DISAMBIGUATED_MARKER):
        return True
    return kind in _NAME_FALLBACK_DERIVATIONS


def _split_sibling_disc(
    conn: sqlite3.Connection, register_id: int, provider_key: str | int
) -> dict[int, str]:
    """variable_id → §5.7 split-sibling discriminator for the siblings sharing
    ``(register_id, provider_key)``: the sibling's EARLIEST delivery-column
    slug, uniquified in column-sorted order (§5.4 rename stability, #139).

    Single source of truth for BOTH the auto.toml cache key
    (:func:`populate_variable_slugs`) and the curated ``same_as`` / slug-override
    anchor (:func:`_variable_source_slug`), so the two never diverge — a curator
    anchoring on ``<reg>.<var>.<disc>`` selects the same sibling the auto key
    named. Returns ``{}`` / a single entry for an unsplit key (harmless).

    **Immutability scope — DEFERRED to #141, NOT solved here.** This gives split
    siblings *distinct, rebuild-stable* slugs, which is all the pre-v1
    regenerate-every-build model needs: ``UNFROZEN`` regenerates ``scb.auto.toml``
    each build, so no published FQID exists to break yet. Full §5.4 immutability
    across triage transitions — a sibling's delivery column being renamed, or a
    1:1 variable *becoming* a split (migrating the old 2-part auto slug onto the
    right sibling) — needs the slug-freeze format / rename-tracking and is tracked
    in #141. Those facets are intentionally out of this PR's scope; do not
    "fix" them with a column-based heuristic, which can attach a published slug
    to the wrong sibling."""
    rows = conn.execute(
        "SELECT v.variable_id, (SELECT vs.delivery_column_name FROM variable_state vs "
        " WHERE vs.variable_id = v.variable_id AND vs.delivery_column_name IS NOT NULL "
        " ORDER BY vs.valid_from ASC, vs.delivery_column_name ASC LIMIT 1) "
        "FROM variable v WHERE v.register_id = ? AND v.provider_key = CAST(? AS TEXT)",
        (register_id, provider_key),
    ).fetchall()
    disc: dict[int, str] = {}
    used: set[str] = set()
    for kol, vid in sorted((r[1] or "", r[0]) for r in rows):
        base = derive_variable_slug(kol) or "x"
        cand, i = base, 0
        while cand in used:
            i += 1
            cand = f"{base}-{i}"
        used.add(cand)
        disc[vid] = cand
    return disc


def populate_variable_slugs(
    conn: sqlite3.Connection,
    slug_dir: Path,
    fold_slugs: dict[int, str] | None = None,
) -> dict[str, int]:
    """Populate register-unique `variable.slug` (§5.3) — always succeeds.

    `fold_slugs` (§5.7) maps a *folded* variable's `variable_id` to its
    shared-column-stem slug base. A fold keeps one variable whose states span
    several representation columns (`Ssyk3` / `Ssyk5`), so the latest-column
    auto-derive would pick one representation (`ssyk5`) instead of the stem
    (`ssyk`); the triage-supplied stem overrides that for first-sight slugs.

    `variable` is register-scoped, so each row gets exactly one slug and the
    natural key `(register_id, slug)` is register-unique (`idx_variable_slug`).
    Each first-sight variable's slug comes from a fallback chain, every
    candidate run through a per-register :func:`_uniquify`, so the build always
    yields a unique slug without a "curate every collision" gate — real SCB data
    has generic delivery columns (`Kolumn1`×148, `RadNr`×137, `OBS_VALUE`×121)
    and ~2k variables with a numeric/absent kolumnnamn, so neither the
    kolumnnamn alone nor strict manual curation scales:

    1. **Curated** `[variable."<reg>.<var>"]` slug in ``<provider>.toml`` — wins.
    2. **Existing auto** slug in ``<provider>.auto.toml`` — kept verbatim
       (§5.3 immutability: a kolumnnamn/name change can't rot a published slug).
    3. **Drift-stable basis** (§5.3/#143) — when the variable's
       ``delivery_column_name`` is *not* constant across its states (the column
       was renamed/revised across editions), the latest column is a misleading,
       version-coupled basis (``sun2020inr1`` for a var that was
       SUN96→SUN2000→SUN2020). Slug from the **name** when register-unique among
       drifters (``utbildningsinriktning``), else the **earliest** delivery
       column (#139's split-sibling discriminator basis — siblings share a name,
       so name collides and they route here).
    4. **kolumnnamn-derived**, when register-unique among first-sight variables
       (the short, common case: ``kon``).
    5. **name-derived** (length-capped, :func:`_name_slug`) — when the
       kolumnnamn slug collides, is generic, or is absent.
    6. **``v<provider_key>``** last resort.

    The hand-curated override (1) stays the curator's hook to prettify any auto
    slug. New auto slugs are persisted to ``<provider>.auto.toml``.

    Returns ``{"curated": n, "auto_existing": n, "auto_new": n}``.
    """
    from .db import _progress

    counts = {"curated": 0, "auto_existing": 0, "auto_new": 0}
    # First-sight variables whose delivery column drifts (§5.3/#143) — slugged
    # from a stable basis (name / earliest column) rather than the latest
    # column. Build-signal only; not a slug-source category, so it's reported on
    # the progress line, not returned in `counts`.
    drift_new = 0

    # Curated overrides come from the hand-curated <provider>.toml only.
    curated_entries = [
        e
        for path in sorted(slug_dir.glob(f"*{PROVIDER_FILE_SUFFIX}"))
        if path.name != CLASSIFICATIONS_FILE
        and not path.name.endswith(AUTO_FILE_SUFFIX)
        for e in load_provider_toml(path)
    ]
    curated = _curated_variable_slugs(curated_entries)
    # A hand-curated [variable] slug override must match a live variable — a
    # non-deprecated override for a missing (register, var) is a typo that would
    # otherwise be silently ignored (the variable auto-slugs under a different
    # FQID). §5.4 retired variables use deprecated=true; auto-file entries are
    # exempt (they legitimately outlive deliveries, so they never reach here —
    # `curated_entries` excludes `.auto.toml`). Verified after the provider loop.
    curated_required = {
        (e.provider, e.source_id)
        for e in curated_entries
        if e.kind == "variable"
        and e.slug is not None
        and not e.deprecated
        and e.provider is not None
    }
    applied_curated: set[tuple[str, str]] = set()

    for provider_slug in _live_providers(conn):
        auto_path = slug_dir / f"{provider_slug}{AUTO_FILE_SUFFIX}"
        auto: dict[str, str] = {}
        # A4.4a: source_id → derivation class — the `# source:` comment basis for
        # the rewritten auto.toml + the name-fallback worklist. Seeded from the
        # EXISTING file's markers so an incremental rewrite (the prior file + new
        # appended slugs, the §5.4-freeze workflow) preserves the provenance of
        # rows it isn't re-deriving; Pass 3 below overwrites/adds for first-sight
        # slugs. Pre-v1 the file regenerates from scratch (UNFROZEN, no prior
        # file), so this is a no-op then and every slug is freshly classed.
        auto_derivation: dict[str, str] = {}
        if auto_path.is_file():
            auto = _auto_variable_slugs(load_provider_toml(auto_path))
            auto_derivation.update(read_auto_derivations(auto_path))
        # variable_state.delivery_column_name is the coalesced per-era column
        # (not raw variable_alias) — stays correct after A2.7 drops
        # variable_instance. "Latest" = highest valid_to, lexically smallest on
        # ties (matches the coalescer tie-break, §5.3). `early_kol` is the
        # mirror (lowest valid_from) — the #139 split-sibling discriminator
        # basis — and `n_cols` is the §5.3/#143 drift signal: a variable whose
        # delivery column is NOT constant across its states (n_cols > 1) must
        # NOT slug from its latest column (a version-specific name like
        # `sun2020inr1` for a var that was SUN96→SUN2000→SUN2020). Ordered by
        # register so the per-register uniqueness scope is one groupby pass.
        variables = conn.execute(
            "SELECT v.variable_id, v.register_id, v.provider_key, v.name, "
            "(SELECT vs.delivery_column_name FROM variable_state vs "
            " WHERE vs.variable_id = v.variable_id "
            " AND vs.delivery_column_name IS NOT NULL "
            " ORDER BY vs.valid_to DESC, vs.delivery_column_name ASC LIMIT 1) AS kol, "
            "(SELECT vs.delivery_column_name FROM variable_state vs "
            " WHERE vs.variable_id = v.variable_id "
            " AND vs.delivery_column_name IS NOT NULL "
            " ORDER BY vs.valid_from ASC, vs.delivery_column_name ASC LIMIT 1) AS early_kol, "
            "(SELECT COUNT(DISTINCT vs.delivery_column_name) FROM variable_state vs "
            " WHERE vs.variable_id = v.variable_id "
            " AND vs.delivery_column_name IS NOT NULL) AS n_cols "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? "
            "ORDER BY v.register_id, v.variable_id",
            (provider_slug,),
        ).fetchall()

        # §5.7 split siblings share one `provider_key`, so `(register_id,
        # provider_key)` is NOT a unique auto-slug cache key. Without a
        # discriminator the last sibling overwrites the shared `auto.toml`
        # entry, and the next build replays that one slug onto every sibling →
        # `UNIQUE(register_id, slug)` fails (Codex P2 on #139). Disambiguate a
        # *split* provider_key's siblings by their delivery-column slug. Unsplit
        # provider_keys (96%) keep the plain 2-part key. The discriminator is
        # uniquified in **column-sorted** order (NOT variable_id order, which is
        # an AUTOINCREMENT that changes across rebuilds) so two siblings whose
        # columns slug-collide still get distinct, rebuild-stable keys.
        # §5.7 split siblings share one `provider_key`, so `(register_id,
        # provider_key)` is NOT a unique auto-slug cache key — without a
        # discriminator the last sibling overwrites the shared `auto.toml` entry
        # and the next build replays that one slug onto every sibling →
        # `UNIQUE(register_id, slug)` fails (Codex #139). Disambiguate a *split*
        # provider_key's siblings by `_split_sibling_disc` (earliest-column slug,
        # uniquified) — the same helper `_variable_source_slug` uses, so a
        # curated `same_as`/override anchored on `<reg>.<var>.<disc>` picks the
        # same sibling. Unsplit provider_keys (96%) keep the plain 2-part key.
        split_pk = {
            key for key, n in Counter((r[1], r[2]) for r in variables).items() if n > 1
        }
        disc: dict[int, str] = {}  # variable_id → discriminator (split siblings)
        for _rid, _pk in split_pk:
            disc.update(_split_sibling_disc(conn, _rid, _pk))

        def _source_id(rid: int, pk: str, vid: int) -> str:
            # The variable source-ID uses '.' as the segment separator, so a
            # provider_key containing '.' would be mis-parsed downstream as a
            # split-sibling 3-part key (`_parse_variable_id` → phantom
            # discriminator, silent slug mis-attribution). SCB keys are integers
            # (dot-free); a non-SCB provider_key (a SOS variable name, A4.4b) must
            # be too. Fail fast rather than corrupt the key.
            if "." in pk:
                raise _err(
                    "slug_toml_invalid",
                    f"variable provider_key {pk!r} (register {rid}) contains '.', "
                    "which collides with the source-ID segment separator.",
                    "Rename the source column / provider_key so it has no dot.",
                )
            if (rid, pk) in split_pk:
                return f"{rid}.{pk}.{disc[vid]}"
            return f"{rid}.{pk}"

        # §5.4 immutability: reserve every PUBLISHED slug per register so a new
        # variable can't reuse one (which would recreate a published FQID). Two
        # sources, both of which flow into the grow-only snapshot:
        #   - frozen auto.toml slugs (incl. entries pruned from this delivery);
        #   - hand-curated slugs for variables NOT live in this delivery — a
        #     deprecated/§5.4-retired entry kept in scb.toml (a non-deprecated
        #     stale one is also flagged by the override-staleness check below).
        # Curated slugs for LIVE variables are intentionally NOT pre-reserved:
        # they're reserved when applied in Pass 1, and pre-reserving would trip
        # the own-slug branch of the curated-conflict check. source_id is
        # "<register_id>.<provider_key>"; register_id is the leading int segment.
        live_sids = {
            _source_id(rid, pk, _vid) for _vid, rid, pk, _n, _k, _ek, _nc in variables
        }
        reserved_by_register: dict[int, set[str]] = defaultdict(set)
        for sid, prev_slug in auto.items():
            reserved_by_register[int(sid.partition(".")[0])].add(prev_slug)
        for (cprov, csid), cslug in curated.items():
            if cprov == provider_slug and csid not in live_sids:
                reserved_by_register[int(csid.partition(".")[0])].add(cslug)

        auto_dirty = False
        # The build connection yields plain tuples (not sqlite3.Row), so unpack
        # positionally: (variable_id, register_id, provider_key, name, kol,
        # early_kol, n_cols). `pending` carries the early column + a `drift`
        # flag (n_cols > 1) into the §5.3/#143 stable-basis fallback in Pass 3.
        for register_id, group in groupby(variables, key=lambda row: row[1]):
            used: set[str] = set(reserved_by_register.get(register_id, ()))
            pending: list[
                tuple[int, str, str | None, str | None, str | None, bool]
            ] = []

            # Pass 1: curated + existing-auto slugs are fixed — assign and
            # reserve them so first-sight derivation can't collide with them.
            for variable_id, _reg, provider_key, name, kol, early_kol, n_cols in group:
                source_id = _source_id(register_id, provider_key, variable_id)
                curated_slug = curated.get((provider_slug, source_id))
                if curated_slug is not None:
                    fixed, kind = curated_slug, "curated"
                    applied_curated.add((provider_slug, source_id))
                    # A curated override must not reuse a slug already reserved
                    # for a DIFFERENT source in this register — a frozen
                    # auto.toml slug (§5.4 immutability) or another row assigned
                    # above. Re-taking the variable's own prior auto slug is
                    # fine. Without this it falls through to a raw
                    # UNIQUE(register_id, slug) failure (live other source) or
                    # silently duplicates a retired published FQID (pruned one).
                    if fixed in used and fixed != auto.get(source_id):
                        raise _err(
                            "slug_variable_override_conflict",
                            f'{provider_slug}.toml: [variable."{source_id}"] slug '
                            f"{fixed!r} is already reserved by another variable "
                            f"in register {register_id} (a frozen auto slug or "
                            f"another curated row).",
                            "Choose a register-unique slug for the override.",
                        )
                else:
                    fixed, kind = auto.get(source_id), "auto_existing"
                if fixed is not None:
                    conn.execute(
                        "UPDATE variable SET slug = ? WHERE variable_id = ?",
                        (fixed, variable_id),
                    )
                    used.add(fixed)
                    counts[kind] += 1
                else:
                    pending.append(
                        (variable_id, provider_key, name, kol, early_kol, n_cols > 1)
                    )

            # Pass 2: kolumnnamn-slug frequency among first-sight variables.
            # A kol slug is usable directly only if exactly one pending variable
            # derives it and it isn't already taken by a curated/auto slug.
            # Drifters (§5.3/#143) are excluded from `kol_freq`: they won't claim
            # a latest-column slug, so they mustn't block a stable-column sibling
            # that legitimately wants it (a drifter's last column can equal an
            # unsplit variable's only column — the same-column-different-var_id
            # reuse #143 calls out). `name_freq` mirrors `kol_freq` for the
            # drifters that prefer their (stable) name basis.
            kol_slug = {
                vid: derive_variable_slug(k) for vid, _pk, _nm, k, _ek, _dr in pending
            }
            kol_freq = Counter(
                kol_slug[vid]
                for vid, _pk, _nm, _k, _ek, drift in pending
                if not drift and kol_slug[vid] is not None
            )
            name_slug_of = {
                vid: _name_slug(nm) for vid, _pk, nm, _k, _ek, drift in pending if drift
            }
            name_freq = Counter(s for s in name_slug_of.values() if s is not None)

            # Pass 3: assign first-sight slugs via the fallback chain. `kind`
            # records WHICH arm produced the base (A4.4a provenance); it tracks
            # the existing selection 1:1 and never alters a slug VALUE.
            for variable_id, provider_key, name, _kol, early_kol, drift in pending:
                ks = kol_slug[variable_id]
                fold = fold_slugs.get(variable_id) if fold_slugs else None
                if fold:
                    # §5.7 fold: slug from the shared column stem (triage-
                    # supplied), not a single representation column.
                    base = fold
                    kind = _DERIVATION_FOLD
                elif drift:
                    # §5.3/#143: the delivery column isn't constant across this
                    # variable's states, so the latest-column slug is misleading
                    # and version-coupled (`sun2020inr1` for a var that was
                    # SUN96→SUN2000→SUN2020). Slug from a stable basis: the NAME
                    # when register-unique among drifters (the unsplit 1:1 case →
                    # a version-neutral `utbildningsinriktning`), else the
                    # EARLIEST delivery column (#139's split-sibling
                    # discriminator basis — split siblings share a name, so name
                    # collides and routes them here, staying rebuild-stable).
                    # #141's frozen-build rename-immutability is a separate,
                    # post-slug-freeze concern.
                    ns = name_slug_of[variable_id]
                    if ns is not None and name_freq[ns] == 1 and ns not in used:
                        base = ns
                        kind = _DERIVATION_DRIFT_NAME
                    else:
                        # Mirror the `or` chain's short-circuit to name the arm
                        # that actually won (the value is unchanged — this only
                        # reads which operand is truthy).
                        early = derive_variable_slug(early_kol)
                        if early is not None:
                            base, kind = early, _DERIVATION_DRIFT_COLUMN
                        elif ns is not None:
                            base, kind = ns, _DERIVATION_DRIFT_NAME_RESIDUAL
                        elif ks is not None:
                            base, kind = ks, _DERIVATION_DRIFT_KOL_RESIDUAL
                        else:
                            base = _fallback_slug(provider_key)
                            kind = _DERIVATION_DRIFT_FALLBACK
                    drift_new += 1
                elif ks is not None and kol_freq[ks] == 1 and ks not in used:
                    base = ks
                    kind = _DERIVATION_KOLUMNNAMN
                else:
                    name_base = _name_slug(name)
                    if name_base is not None:
                        base, kind = name_base, _DERIVATION_NAME
                    elif ks is not None:
                        base, kind = ks, _DERIVATION_KOL_RESIDUAL
                    else:
                        base = _fallback_slug(provider_key)
                        kind = _DERIVATION_FALLBACK
                slug = _uniquify(base, used)
                # A `-N` suffix is a collision-driven, non-canonical artifact —
                # tag it so the worklist surfaces it regardless of base class.
                if slug != base:
                    kind = f"{kind}+disambiguated"
                conn.execute(
                    "UPDATE variable SET slug = ? WHERE variable_id = ?",
                    (slug, variable_id),
                )
                used.add(slug)
                source_id = _source_id(register_id, provider_key, variable_id)
                auto[source_id] = slug
                auto_derivation[source_id] = kind
                auto_dirty = True
                counts["auto_new"] += 1

        if auto_dirty:
            write_auto_toml(auto_path, provider_slug, auto, auto_derivation)

    # Typo guard: every non-deprecated hand-curated [variable] override must have
    # matched a live variable above; an unmatched one is a stale/typo'd source
    # key that would silently no-op (the variable auto-slugs instead).
    stale_overrides = sorted(curated_required - applied_curated)
    if stale_overrides:
        sample = ", ".join(f"{prov}/{sid}" for prov, sid in stale_overrides[:10])
        raise _err(
            "slug_variable_override_stale",
            f"{len(stale_overrides)} hand-curated [variable] slug override(s) "
            f"reference a (register, var) with no live variable — likely a typo: "
            f"{sample}.",
            "Fix the source key, or mark the entry deprecated=true if the "
            "variable is retired.",
        )

    _progress(
        f"  Variable slugs: {counts['curated']:,} curated, "
        f"{counts['auto_existing']:,} auto (existing), "
        f"{counts['auto_new']:,} auto (new); "
        f"{drift_new:,} new from a drift-stable basis (§5.3/#143)"
    )
    return counts


def _format_register_sample(row: Any) -> str:
    return f"{row[0]} ({row[1]!r})"


def _format_variant_sample(row: Any) -> str:
    return f"{row[0]}.{row[1]} ({row[2]!r})"


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


# A2.1.5: variable same_as keys are variable-grain 3-tuples
# (provider, register, variable). The variant/period slots were dropped (§5.5).
# Classification keys are 2-tuples (provider, classification_slug).
_VarKey = tuple[str, str, str]
_ClassKey = tuple[str, str]


def _variable_source_slug(
    conn: sqlite3.Connection, register_id: int, var_id: str, entry: SlugEntry
) -> str:
    """Read the stored variable slug for a `[variable]` TOML entry's source.

    The TOML key identifies the variable whose `variable.slug` anchors the
    `a_variable` side of the same_as edge. `populate_variable_slugs` runs before
    `materialize_same_as_edges`, so the slug column is populated here.

    A bare `<register_id>.<var_id>` key is 1:1 with a variable unless A2.2 triage
    split that `var_id` into siblings — then it is **ambiguous** and rejected.
    A curator anchors a specific sibling with the 3-part
    `<register_id>.<var_id>.<discriminator>` key (the same discriminator the
    auto-slug cache uses, via `_split_sibling_disc`); we resolve it to the one
    matching sibling. The "no slug" error covers the genuine curation case
    (variable absent / underivable at build).
    """
    rows = conn.execute(
        "SELECT variable_id, slug FROM variable "
        "WHERE register_id = ? AND provider_key = CAST(? AS TEXT) "
        "AND slug IS NOT NULL",
        (register_id, var_id),
    ).fetchall()
    if not rows:
        raise _err(
            "slug_same_as_unresolved_source",
            f"{entry.provider}.toml: variable.{entry.source_id!r} has no "
            f"stored slug on `variable`, so its variable_slug can't be "
            f"anchored for `same_as` materialization.",
            "Check that the (register_id, var_id) appears in the build (it may "
            "fold to an underivable slug — curate "
            f'`[variable."{entry.source_id}"]` in {entry.provider}.toml); mark '
            "the entry deprecated=true if the variable is retired.",
        )
    parts = entry.source_id.split(".")
    disc_key = parts[2] if len(parts) == 3 else None
    if disc_key is not None:
        sib_disc = _split_sibling_disc(conn, register_id, var_id)
        matches = [slug for vid, slug in rows if sib_disc.get(vid) == disc_key]
        if len(matches) == 1:
            return matches[0]
        raise _err(
            "slug_same_as_unresolved_source",
            f"{entry.provider}.toml: variable.{entry.source_id!r} split-sibling "
            f"discriminator {disc_key!r} matches {len(matches)} of {len(rows)} "
            f"siblings under (register_id, provider_key).",
            "Use the exact §5.7 sibling discriminator (the earliest "
            "delivery-column slug); `reg-meta-build precheck-slugs` lists them.",
        )
    if len(rows) > 1:
        raise _err(
            "slug_same_as_ambiguous_source",
            f"{entry.provider}.toml: variable.{entry.source_id!r} maps to "
            f"{len(rows)} variables sharing (register_id, provider_key) "
            f"(slugs {sorted(r[1] for r in rows)!r}) — an A2.2 triage split. A "
            f"same_as anchored on the bare source key is ambiguous; the edge "
            f"would attach to an arbitrary sibling.",
            "Anchor the same_as on the specific sibling via the 3-part "
            '`[variable."<reg>.<var>.<discriminator>"]` key.',
        )
    return rows[0][1]


def _validate_variable_target(
    conn: sqlite3.Connection, entry: SlugEntry, ref: dict[str, str]
) -> _VarKey:
    """Validate a same_as target's provider + register exist (variable grain).

    `variable_slug` itself is **not** validated against the DB — that's the
    point of slug-anchored linking: the link survives forward-of-data renames.
    Provider and register slugs are checked because if those are typos the link
    is permanently dead. A2.1.5: the edge is variable-grain — there are no
    `register_variant`/`period` narrowing slots (§5.5), so the only DB check is
    provider+register existence. Key shape is enforced upstream by
    `_validate_same_as` (which now rejects variant/period keys) at TOML load.
    """
    provider_slug = ref["provider"]
    register_slug = ref["register"]
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
    return (provider_slug, register_slug, variable_slug)


def _validate_classification_target(
    entry: SlugEntry,
    ref: dict[str, str],
    by_slug: dict[tuple[str, str], str],
) -> _ClassKey:
    """Validate a classification same_as target — provider + classification_slug.

    A2.6.1: the slug bakes in the vintage and is globally UNIQUE, so a
    `(provider, slug)` key maps to exactly one row — the former multi-version
    ambiguity check is structurally impossible and gone. This is now a pure
    presence check. Cross-version drift still belongs in `supersedes` /
    `replaced_by`, not same_as. Key shape is enforced upstream by
    `_validate_same_as`.
    """
    provider_slug = ref["provider"]
    classification_slug = ref["classification_slug"]
    if (provider_slug, classification_slug) not in by_slug:
        raise _err(
            "slug_same_as_unknown_classification",
            f"classifications.toml: classification.{entry.source_id!r} "
            f"same_as target {provider_slug}/{classification_slug!r} does "
            f"not exist in this build.",
            "Fix the slug typo, or mark the originating entry "
            "deprecated=true if the target classification is retired.",
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

    # Pre-index classification (provider, slug) presence so we can validate
    # same_as targets without a row-per-target query. A2.6.1: the slug is
    # globally UNIQUE (vintage baked in), so each key maps to exactly one row —
    # the value is just the short_name for diagnostics.
    class_by_slug: dict[tuple[str, str], str] = {}
    # Classifications carry no provider in classifications.toml; they belong
    # to the SCB-wide registry. Treat the publisher field as the provider.
    cls_rows = conn.execute(
        "SELECT short_name, slug, publisher FROM classification WHERE slug IS NOT NULL"
    ).fetchall()
    for row in cls_rows:
        publisher_slug = (row[2] or "scb").lower()
        class_by_slug[(publisher_slug, row[1])] = row[0]

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
            # `_parse_variable_id` accepts the optional split-sibling 3rd
            # segment; `_variable_source_slug` resolves it to the sibling.
            register_id, var_id = _parse_variable_id(entry.source_id)
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
            src: _VarKey = (entry.provider, row[0], src_variable_slug)
            for ref in entry.same_as:
                tgt = _validate_variable_target(conn, entry, ref)
                var_edges.append((src, tgt))

        elif entry.kind == "classification":
            row = conn.execute(
                "SELECT slug, publisher FROM classification WHERE short_name = ?",
                (entry.source_id,),
            ).fetchone()
            if row is None or row[0] is None:
                raise _err(
                    "slug_same_as_unresolved_source",
                    f"classifications.toml: classification.{entry.source_id!r} "
                    f"has no DB slug; cannot anchor same_as.",
                    "Ensure populate_slugs wrote the slug column first.",
                )
            src_provider = (row[1] or "scb").lower()
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
                "a_provider, a_register, a_variable, "
                "b_provider, b_register, b_variable) "
                "VALUES (?, ?, ?, ?, ?, ?)",
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
# §5.6 consumer-side binding lineage config (TOML-only, no SQL table)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LineageConfig:
    """Source-variant pinning for §5.6 lineage, parsed from slug TOMLs.

    Keyed by PROVIDER because register.slug is not globally unique — two
    providers can reuse a register slug, so an `scb/rtb` default must not bleed
    onto an `sos/rtb` source (Codex P2 on #145). `defaults` maps
    `(provider_slug, source_register_slug)` → the heuristic default source
    variant slug (the `[lineage_defaults]` block). `overrides` maps
    `(provider_slug, consumer_register_slug, variable_slug)` →
    `(source_register_slug, source_variant_slug)` (the
    `[lineage."<consumer>.<slug>"]` blocks). The provider is the one owning the
    `<provider>.toml` the block lives in: the source register's provider for
    defaults, the consumer's provider for overrides.

    Both carry pure shape (string-typed values); existence of the named
    registers / variants is validated by `link_variable_state_lineage` against
    the DB — this loader stays DB-free so it's testable in isolation.
    """

    defaults: dict[tuple[str, str], str]
    overrides: dict[tuple[str, str, str], tuple[str, str]]


def load_lineage_config(slug_dir: Path) -> LineageConfig:
    """Parse `[lineage_defaults]` and `[lineage."<consumer>.<slug>"]` from every
    provider TOML under ``slug_dir`` (excluding ``classifications.toml``).

    Keys carry the provider (from the `<provider>.toml` filename via
    `_provider_from_path`) because register.slug is not globally unique. A
    duplicate key *within one provider* (e.g. across `scb.toml` and its `.auto`
    companion) is a fail-fast error; the SAME register slug under DIFFERENT
    providers is fine — distinct keys. The TOML dotted-key form
    `[lineage."lisa.kon"]` parses as a single quoted key under `lineage`;
    register/variable slugs are `[a-z0-9-]` (no `.`), so splitting the key on
    the first `.` cleanly recovers `(consumer_register, variable_slug)`.
    """
    defaults: dict[tuple[str, str], str] = {}
    overrides: dict[tuple[str, str, str], tuple[str, str]] = {}

    for path in sorted(slug_dir.glob("*.toml")):
        if path.name == "classifications.toml":
            continue
        provider = _provider_from_path(path)
        data = _parse_toml(path)

        raw_defaults = data.get("lineage_defaults", {})
        if not isinstance(raw_defaults, dict):
            raise _err(
                "lineage_defaults_malformed",
                f"{path.name}: [lineage_defaults] must be a table of "
                f'source_register = "variant_slug" entries.',
                "Use a [lineage_defaults] table with string values.",
            )
        for src_register, variant in raw_defaults.items():
            if not isinstance(variant, str):
                raise _err(
                    "lineage_default_not_string",
                    f"{path.name}: [lineage_defaults] {src_register!r} must be a "
                    f"string variant slug, got {type(variant).__name__}.",
                    "Set the value to the source variant slug, e.g. "
                    'rtb = "folkbokforda-personer".',
                )
            default_key = (provider, src_register)
            if default_key in defaults:
                raise _err(
                    "lineage_default_duplicate",
                    f"{path.name}: duplicate [lineage_defaults] entry for "
                    f"source register {src_register!r} under provider "
                    f"{provider!r} (already set to {defaults[default_key]!r}).",
                    "Declare each (provider, source-register) default once.",
                )
            defaults[default_key] = variant

        raw_overrides = data.get("lineage", {})
        if not isinstance(raw_overrides, dict):
            raise _err(
                "lineage_override_malformed",
                f"{path.name}: [lineage] must contain "
                '[lineage."<consumer_register>.<variable_slug>"] tables.',
                "Use quoted dotted-key tables under [lineage].",
            )
        for key, block in raw_overrides.items():
            if not isinstance(block, dict):
                raise _err(
                    "lineage_override_malformed",
                    f"{path.name}: [lineage.{key!r}] must be a table with "
                    "source_register and source_variant keys.",
                    "Use a table, e.g. "
                    '[lineage."lisa.inkomst_pension"] '
                    'source_register = "rams" source_variant = "individregister".',
                )
            # Split on the FIRST '.' — register slugs are dot-free, so the
            # remainder is the (also dot-free) variable slug.
            consumer_register, sep, variable_slug = key.partition(".")
            if not sep or not consumer_register or not variable_slug:
                raise _err(
                    "lineage_override_key_malformed",
                    f"{path.name}: [lineage.{key!r}] key must be "
                    '"<consumer_register>.<variable_slug>".',
                    'Use the dotted form, e.g. [lineage."lisa.kon"].',
                )
            source_register = block.get("source_register")
            source_variant = block.get("source_variant")
            if not isinstance(source_register, str) or not isinstance(
                source_variant, str
            ):
                raise _err(
                    "lineage_override_incomplete",
                    f"{path.name}: [lineage.{key!r}] requires string "
                    "source_register and source_variant keys.",
                    "Set both keys, e.g. "
                    'source_register = "rams" source_variant = "individregister".',
                )
            override_key = (provider, consumer_register, variable_slug)
            if override_key in overrides:
                raise _err(
                    "lineage_override_duplicate",
                    f"{path.name}: duplicate [lineage] override for "
                    f"{key!r} under provider {provider!r}.",
                    "Declare each (provider, consumer-variable) override once.",
                )
            overrides[override_key] = (source_register, source_variant)

    return LineageConfig(defaults=defaults, overrides=overrides)


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
        "SELECT p.slug, r.register_id, r.name, "
        "rv.register_variant_id, rv.name, rv.slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE rv.register_id IN ("
        "  SELECT register_id FROM register_variant "
        "  GROUP BY register_id HAVING COUNT(*) = 1"
        ") "
        "ORDER BY p.slug, r.register_id, rv.register_variant_id"
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
        "SELECT r.register_id, r.name, r.slug FROM register r "
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
        "SELECT rv.register_id, rv.register_variant_id, rv.name, rv.slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? "
        "ORDER BY rv.register_id, rv.register_variant_id",
        (provider_slug,),
    ).fetchall():
        register_id, register_variant_id, name, existing_slug = row
        variants_by_reg.setdefault(register_id, []).append(
            (register_variant_id, name, existing_slug)
        )

    # A2.6: register_version is not seeded — version is not an FQID segment and
    # has no slug column anymore (§5.2). Only register + register_variant emit.
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

        for register_variant_id, vname, existing_slug in variants_by_reg.get(
            register_id, []
        ):
            v_candidate = existing_slug or (
                derive_variable_slug(vname) if vname else None
            )
            v_candidate = v_candidate or "TODO"
            lines.append(
                f"[register_variant.{_toml_str(f'{register_id}.{register_variant_id}')}]"
            )
            lines.append(f"slug = {_toml_str(v_candidate)}")
            if vname:
                lines.append(f"display_group = {_toml_str(vname)}")
            lines.append("")
    return "\n".join(lines)


def seed_classifications_toml(conn: sqlite3.Connection) -> str:
    """Emit a starter TOML for classifications. The maintainer edits the
    auto-derived slug — classification short_names like ``SUN2020-NIVA``
    don't fold to a great default.

    A2.6.1: the FQID is the 2-segment ``class/<slug>`` with the vintage baked
    into the slug, so there's no separate ``version`` field. The candidate is
    folded from the short_name (which usually carries the year); the maintainer
    refines it to the canonical baked form (e.g. ``SUN2020-NIVA`` →
    ``sun-niva2020``)."""
    lines: list[str] = [
        "# Starter classification slug TOML.",
        "# Generated by `reg-meta-build seed-slugs`. Hand-review the slug",
        "# (auto-derived from short_name, often needs shortening; the vintage",
        "# bakes into the slug, §5.2 — `class/<slug>`, e.g. `sun2020`).",
        "",
    ]
    rows = conn.execute(
        "SELECT short_name FROM classification ORDER BY short_name"
    ).fetchall()
    if not rows:
        lines.append("# (no classifications populated yet)\n")
        return "\n".join(lines)
    for (short,) in rows:
        candidate = derive_variable_slug(short) or "TODO"
        lines.append(f"[classification.{_toml_str(short)}]")
        lines.append(f"slug = {_toml_str(candidate)}")
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
    # A2.6: register_version dropped from the FQID grammar — no missing/stale/
    # colliding version checks; version slugs are neither curated nor persisted.
    missing_registers: tuple[tuple[str, str, str], ...]  # (provider, id, name)
    missing_variants: tuple[tuple[str, str, str], ...]
    missing_classifications: tuple[str, ...]
    parse_errors: tuple[str, ...]
    # Reverse direction: TOML source IDs that don't (or no longer) exist in
    # the DB and would fail `populate_slugs(strict=True)` at build time.
    # Deprecated entries are excluded — they're allowed to outlive their DB
    # row. Non-fatal: precheck surfaces them so maintainers can drop or mark
    # them before a build attempt.
    stale_registers: tuple[tuple[str, str], ...] = ()  # (provider, source_id)
    stale_variants: tuple[tuple[str, str], ...] = ()
    stale_classifications: tuple[str, ...] = ()
    entries: tuple[SlugEntry, ...] = ()
    # Advisory (§5.3/#143): variables whose delivery column drifts across
    # editions, auto-slugged from a stable basis (name / earliest column). A
    # pre-v1 curation-review aid, NOT a gate — never feeds `ok`/exit. Each row:
    # (provider, register_id, provider_key, slug, name, cols-in-valid_from-order).
    # `provider_key` stays TEXT (SCB `str(var_id)`, SOS a merged name) — never
    # coerced to int, so a non-numeric SOS key can't crash the advisory.
    drifting_variables: tuple[tuple[str, int, str, str, str, tuple[str, ...]], ...] = ()
    # Advisory (A4.4a): the name-fallback curation backlog — auto-slugged
    # variables whose slug derived from the variable NAME / a `-N` disambiguator
    # / the `v<provider_key>` last resort (`_is_name_fallback_derivation`), read
    # from the `# source:` markers in each committed `<provider>.auto.toml`. Like
    # `drifting_variables` it is informational ONLY — never feeds `ok`/exit. Each
    # row: (provider, source_id, slug, derivation). `source_id` is the auto.toml
    # key (`<reg>.<pk>[.<disc>]`); `slug` is read from the auto file so the
    # worklist needs no DB join (and works even when the variable has been pruned
    # from a later delivery but its frozen slug lingers).
    name_fallback_variables: tuple[tuple[str, str, str, str], ...] = ()

    @property
    def ok(self) -> bool:
        return not (
            self.missing_registers
            or self.missing_variants
            or self.missing_classifications
            or self.parse_errors
            or self.stale_registers
            or self.stale_variants
            or self.stale_classifications
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
    for entry in entries:
        if entry.kind == "classification":
            classification_ids.add(entry.source_id)
        elif entry.provider is not None and entry.slug is not None:
            by_provider_kind.setdefault((entry.provider, entry.kind), set()).add(
                entry.source_id
            )

    # One pass per kind. The same row sets feed both the missing-slug check
    # (live row, no TOML entry) and the stale-entry check (TOML entry, no live
    # row), so we materialize each once.
    live_regs_by_provider: dict[str, set[int]] = {}
    live_vars_by_provider: dict[str, set[tuple[int, int]]] = {}
    missing_regs: list[tuple[str, str, str]] = []
    missing_variants: list[tuple[str, str, str]] = []
    for provider_slug in _live_providers(conn):
        slugged_regs = by_provider_kind.get((provider_slug, "register"), set())
        reg_rows = conn.execute(
            "SELECT r.register_id, r.name FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? ORDER BY r.register_id",
            (provider_slug,),
        ).fetchall()
        live_regs_by_provider[provider_slug] = {rid for (rid, _) in reg_rows}
        # `name` here is the renamed register.registernamn (universal English
        # column, provider-native value); the variable name mirrors the SQL.
        for register_id, name in reg_rows:
            if str(register_id) not in slugged_regs:
                missing_regs.append((provider_slug, str(register_id), name or ""))

        slugged_variants = by_provider_kind.get(
            (provider_slug, "register_variant"), set()
        )
        var_rows = conn.execute(
            "SELECT rv.register_id, rv.register_variant_id, rv.name, rv.slug "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? ORDER BY rv.register_id, rv.register_variant_id",
            (provider_slug,),
        ).fetchall()
        live_vars_by_provider[provider_slug] = {
            (rid, vid) for (rid, vid, _, _) in var_rows
        }
        for register_id, register_variant_id, name, _slug in var_rows:
            key = f"{register_id}.{register_variant_id}"
            if key not in slugged_variants:
                missing_variants.append((provider_slug, key, name or ""))

    db_classifications: set[str] = set()
    missing_classifications: list[str] = []
    for (short,) in conn.execute(
        "SELECT short_name FROM classification ORDER BY short_name"
    ).fetchall():
        db_classifications.add(short)
        if short not in classification_ids:
            missing_classifications.append(short)

    stale_regs, stale_vars, stale_cls = _stale_toml_entries(
        entries,
        live_regs_by_provider=live_regs_by_provider,
        live_vars_by_provider=live_vars_by_provider,
        db_classifications=db_classifications,
    )

    return PrecheckResult(
        missing_registers=tuple(missing_regs),
        missing_variants=tuple(missing_variants),
        missing_classifications=tuple(missing_classifications),
        parse_errors=tuple(parse_errors),
        stale_registers=tuple(stale_regs),
        stale_variants=tuple(stale_vars),
        stale_classifications=tuple(stale_cls),
        entries=tuple(entries),
        drifting_variables=_drifting_variables(conn),
        name_fallback_variables=_name_fallback_variables(conn, slug_dir),
    )


def _drifting_variables(
    conn: sqlite3.Connection,
) -> tuple[tuple[str, int, str, str, str, tuple[str, ...]], ...]:
    """Advisory list (§5.3/#143): variables whose ``delivery_column_name`` is
    NOT constant across their ``variable_state`` rows.

    ``populate_variable_slugs`` already slugs these from a stable basis (name /
    earliest column — fallback step 3), so this is a pre-v1 curation-review aid,
    NOT a build gate: a curator scans it to pin a ``[variable]`` override where
    the auto-pick is still off. Drift = the same ``COUNT(DISTINCT) > 1`` signal
    the slug derivation uses, so the two never disagree on what "drifts".

    Each row is ``(provider, register_id, provider_key, slug, name, columns)`` —
    the distinct columns in ``valid_from`` order, so the curator reads the
    edition drift directly (``('SunInr', 'sun2000inr1', 'sun2020inr1')``).
    ``provider_key`` stays the raw ``TEXT`` value (SCB ships ``str(var_id)``, SOS
    a merged variable name) — coercing it to ``int`` would crash this advisory on
    a non-numeric SOS key, and the list is supposed to never gate the command.
    """
    _DRIFT = (
        "(SELECT COUNT(DISTINCT vs.delivery_column_name) FROM variable_state vs "
        " WHERE vs.variable_id = {v} AND vs.delivery_column_name IS NOT NULL) > 1"
    )
    ident = conn.execute(
        "SELECT v.variable_id, p.slug, v.register_id, v.provider_key, v.slug, v.name "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        f"WHERE {_DRIFT.format(v='v.variable_id')} "
        "ORDER BY p.slug, v.register_id, CAST(v.provider_key AS INTEGER), v.variable_id"
    ).fetchall()
    # Distinct columns per drifting variable, earliest-edition first. Same drift
    # predicate, so only drifters are scanned (no N+1, no id `IN (...)` list).
    cols_by_vid: dict[int, list[str]] = defaultdict(list)
    for vid, col in conn.execute(
        "SELECT vs.variable_id, vs.delivery_column_name "
        "FROM variable_state vs "
        "WHERE vs.delivery_column_name IS NOT NULL "
        f"AND {_DRIFT.format(v='vs.variable_id')} "
        "GROUP BY vs.variable_id, vs.delivery_column_name "
        "ORDER BY vs.variable_id, MIN(vs.valid_from), vs.delivery_column_name"
    ).fetchall():
        cols_by_vid[vid].append(col)
    return tuple(
        (prov, reg_id, pk, slug or "", name or "", tuple(cols_by_vid[vid]))
        for vid, prov, reg_id, pk, slug, name in ident
    )


def _raw_variable_table(path: Path) -> dict[str, Any] | None:
    """The raw ``[variable]`` table from a slug TOML, or ``None`` when the file
    is absent / unparseable / carries a non-table ``variable`` value.

    RAW ``tomllib`` (not the validating ``load_provider_toml``) so the advisory
    worklist tolerates a non-numeric SOS TEXT key (mirrors ``_drifting_variables``).
    A malformed/unreadable file or an odd ``variable`` shape is precheck's job to
    report via ``parse_errors``; surfacing ``None`` here lets the caller skip it
    instead of crashing ``precheck-slugs`` (the worklist must never gate)."""
    if not path.is_file():
        return None
    try:
        table = _parse_toml(path).get("variable")
    except RegMetaError:
        return None
    return table if isinstance(table, dict) else None


def _name_fallback_variables(
    conn: sqlite3.Connection, slug_dir: Path
) -> tuple[tuple[str, str, str, str], ...]:
    """Advisory worklist (A4.4a): auto-slugged variables in the name-fallback /
    ``-N`` disambiguator / ``v<provider_key>`` last-resort derivation classes —
    the curation backlog a curator works through before the §5.4 slug freeze.

    Reads the `# source:` derivation markers `write_auto_toml` emits into each
    live provider's ``<provider>.auto.toml`` (the single source of truth — the
    selection logic is not re-run, and ``tomllib`` strips the comment so it never
    pollutes ``SlugEntry``/``snapshot_payload``). An entry without a marker
    (legacy pre-A4.4a row, or a kolumnnamn/fold/drift-column slug) is excluded,
    as is a variable a curator has already FIXED via a ``[variable."…"]`` override
    in ``<provider>.toml`` — its frozen auto entry (and marker) lingers in the
    auto file but it is no longer backlog. Slug values are read from the same auto
    file, so the worklist needs no DB join and still lists a variable whose row
    was pruned from a later delivery.

    Each row is ``(provider, source_id, slug, derivation)``, sorted by
    (provider, numeric-aware source_id) for a stable, human-scannable list.
    Like :func:`_drifting_variables` this NEVER gates — it is reported but not
    in :attr:`PrecheckResult.ok`, and a malformed/odd-shaped TOML skips the
    provider rather than raising.
    """
    out: list[tuple[str, str, str, str]] = []
    for provider_slug in _live_providers(conn):
        auto_path = slug_dir / f"{provider_slug}{AUTO_FILE_SUFFIX}"
        auto_vars = _raw_variable_table(auto_path)
        if auto_vars is None:
            continue
        slugs = {
            sid: tbl["slug"]
            for sid, tbl in auto_vars.items()
            if isinstance(tbl, dict) and isinstance(tbl.get("slug"), str)
        }
        # Only a string `slug` override RESOLVES the auto slug — drop those from
        # the backlog even though their frozen auto entry + marker linger. A
        # metadata-only entry (`same_as` / `replaced_by` / `deprecated` with NO
        # slug) leaves the auto slug unchanged (a deprecated variable stays slugged
        # so old references resolve), so it still needs curation and stays backlog.
        curated_table = _raw_variable_table(slug_dir / f"{provider_slug}.toml") or {}
        resolved = {
            sid
            for sid, tbl in curated_table.items()
            if isinstance(tbl, dict) and isinstance(tbl.get("slug"), str)
        }
        derivations = read_auto_derivations(auto_path)
        for source_id, kind in derivations.items():
            if (
                source_id in slugs
                and source_id not in resolved
                and _is_name_fallback_derivation(kind)
            ):
                out.append((provider_slug, source_id, slugs[source_id], kind))
    out.sort(key=lambda row: (row[0], _auto_source_sort_key(row[1])))
    return tuple(out)


def _auto_source_sort_key(source_id: str) -> tuple[int, int, int, str]:
    # Mirror `write_auto_toml._sort_key`: numeric-aware on SCB `<reg>.<var>`
    # keys, string fallback for a 3-part split-sibling key or a non-SCB provider.
    reg, _, var = source_id.partition(".")
    if reg.isdigit() and var.isdigit() and "." not in var:
        return (0, int(reg), int(var), "")
    return (1, 0, 0, source_id)


def _stale_toml_entries(
    entries: list[SlugEntry],
    *,
    live_regs_by_provider: dict[str, set[int]],
    live_vars_by_provider: dict[str, set[tuple[int, int]]],
    db_classifications: set[str],
) -> tuple[
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[str],
]:
    """Find TOML entries whose source IDs don't exist in the DB.

    Mirrors the `slug_unknown_source_id` check inside `populate_slugs`, but
    surfaces every stale entry at once instead of failing on the first.
    Deprecated entries are excluded — they're allowed to outlive their DB row.
    Variable entries are excluded from the staleness precheck: the bulk are
    build-generated `<provider>.auto.toml` rows (A2.1.5, §5.3), which the
    grow-only snapshot (`snapshot_payload`) already covers, and an auto entry
    may legitimately outlive a variable pruned from a later delivery. Stale
    *hand-curated* `[variable]` slug overrides (typo'd keys) are instead caught
    at build time by `populate_variable_slugs` (`slug_variable_override_stale`),
    which has the live-variable set in hand.

    Source IDs are guaranteed parseable here: `_validate_entry` calls
    `_parse_register_id` / `_parse_variant_id` at TOML load.
    """
    stale_regs: list[tuple[str, str]] = []
    stale_vars: list[tuple[str, str]] = []
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
        elif entry.kind == "classification":
            if entry.source_id not in db_classifications:
                stale_cls.append(entry.source_id)

    return stale_regs, stale_vars, stale_cls


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
        # A2.6.1: classifications key on source_id alone (short_name, globally
        # UNIQUE) — the former `|version` suffix is gone with the version field.
        if entry.provider is not None:
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
    "AUTO_FILE_SUFFIX",
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
    "populate_variable_slugs",
    "precheck_slugs",
    "read_auto_derivations",
    "read_snapshot",
    "repo_slug_dir",
    "seed_all",
    "seed_classifications_toml",
    "seed_provider_toml",
    "snapshot_payload",
    "write_auto_toml",
    "write_snapshot",
)
