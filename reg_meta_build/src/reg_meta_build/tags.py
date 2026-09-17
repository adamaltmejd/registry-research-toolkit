"""Load the existing cross-register thematic tag declarations.

Input preparation validates this TOML surface. Converted selections carry explicit
resolved tag metadata into the common catalog writer; there is no SQL curation
post-pass. Register members support thematic browse, while variable members can
carry ranked recommendations and their rationale.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    load_curation_entries,
    require_str,
)

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class TagMember:
    """One `[[tag.member]]` entry. EXACTLY ONE grain is set: a `variable`
    (3-segment `provider/register/variable` FQID) OR a `register` (2-segment
    `provider/register` FQID). `provider`/`register`/`variable` hold the parsed
    slug segments (`variable` is None for a register-grain member). `rank` orders
    members within the tag; `starred` flags a golden/recommended member; `note`
    is the one-line curation rationale (or None)."""

    provider: str
    register: str
    variable: str | None
    rank: int
    starred: bool
    note: str | None


@dataclass(frozen=True)
class CuratedTag:
    """One `[[tag]]` entry: a globally-unique `slug`, a display `label`, an
    optional `description`, and its members."""

    slug: str
    label: str
    description: str | None
    members: tuple[TagMember, ...]


_require_str = functools.partial(
    require_str,
    code="tags_invalid",
    prefix="tags",
    file_name="curation/tags.toml",
)


def _optional_str(entry: dict, field: str, context: str) -> str | None:
    value = entry.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise curation_error(
            "tags_invalid",
            f"tags {context} `{field}` must be a string, got {value!r}.",
            f"Give `{field}` as a string or omit it in "
            "reg_meta_build/curation/tags.toml.",
        )
    return value


def load_tags(path: Path | None) -> tuple[CuratedTag, ...]:
    """Parse the curated-tag TOML. Empty when no file (synthetic test builds,
    wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only `[[tag]]` top-level;
    `slug`/`label` non-empty strings, `description` optional; tag slugs unique;
    each `[[tag.member]]` sets EXACTLY ONE of `variable` (3-seg FQID) / `register`
    (2-seg FQID), with optional `rank` (int) / `starred` (bool) / `note` (str);
    no member appears twice within a tag. Reference RESOLUTION (do the
    register/variable exist?) belongs to common catalog dependency resolution."""
    entries = load_curation_entries(
        path,
        entry_key="tag",
        label="tag",
        prefix="tags",
        code_base="tags",
        file_name="curation/tags.toml",
        entry_fields="slug / label / members",
    )
    out: list[CuratedTag] = []
    seen_slugs: set[str] = set()
    for entry in entries:
        slug = _require_str(entry, "slug", "[[tag]]")
        label = _require_str(entry, "label", "[[tag]]")
        description = _optional_str(entry, "description", f"tag {slug!r}")
        if slug in seen_slugs:
            raise curation_error(
                "tags_invalid",
                f"tags duplicate slug {slug!r}.",
                "Tag slugs are a GLOBAL vocabulary — each must be unique in "
                "reg_meta_build/curation/tags.toml.",
            )
        seen_slugs.add(slug)
        raw_members = entry.get("member", [])
        if not isinstance(raw_members, list) or not raw_members:
            raise curation_error(
                "tags_invalid",
                f"tags tag {slug!r} needs a non-empty `[[tag.member]]` array.",
                "List the tag's members as `[[tag.member]]` tables.",
            )
        members: list[TagMember] = []
        seen_refs: set[tuple[str, str, str | None]] = set()
        for raw in raw_members:
            if not isinstance(raw, dict):
                raise curation_error(
                    "tags_invalid",
                    f"tags tag {slug!r} member {raw!r} must be a table.",
                    "Each member is a `[[tag.member]]` table.",
                )
            members.append(_parse_member(raw, slug, seen_refs))
        out.append(
            CuratedTag(
                slug=slug,
                label=label,
                description=description,
                members=tuple(members),
            )
        )
    return tuple(out)


def _parse_member(
    raw: dict, slug: str, seen_refs: set[tuple[str, str, str | None]]
) -> TagMember:
    """Validate one `[[tag.member]]` table → `TagMember`. Exactly one of
    `variable` (3-seg) / `register` (2-seg); `rank` int (default 0), `starred`
    bool (default False), `note` optional str; no duplicate member within a tag."""
    variable_ref = raw.get("variable")
    register_ref = raw.get("register")
    if (variable_ref is None) == (register_ref is None):
        raise curation_error(
            "tags_invalid",
            f"tags tag {slug!r} member {raw!r} must set EXACTLY ONE of "
            "`variable` / `register`.",
            'Reference a variable (`variable = "scb/lisa/dispink04"`, 3-seg) OR a '
            'register (`register = "scb/lisa"`, 2-seg), not both/neither.',
        )
    if variable_ref is not None:
        ref = _require_str(raw, "variable", f"tag {slug!r} member")
        parts = ref.split("/")
        if len(parts) != 3 or not all(parts):
            raise curation_error(
                "tags_invalid",
                f"tags tag {slug!r} member variable {ref!r} must be a 3-segment "
                "`provider/register/variable` FQID.",
                'Give `variable = "scb/lisa/dispink04"`-style 3-segment FQIDs.',
            )
        provider, register, variable = parts
    else:
        ref = _require_str(raw, "register", f"tag {slug!r} member")
        parts = ref.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "tags_invalid",
                f"tags tag {slug!r} member register {ref!r} must be a 2-segment "
                "`provider/register` FQID.",
                'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
            )
        provider, register = parts
        variable = None

    ref_key = (provider, register, variable)
    if ref_key in seen_refs:
        raise curation_error(
            "tags_invalid",
            f"tags tag {slug!r} references {ref!r} twice.",
            "List each member reference once per tag.",
        )
    seen_refs.add(ref_key)

    rank = raw.get("rank", 0)
    if not isinstance(rank, int) or isinstance(rank, bool):
        raise curation_error(
            "tags_invalid",
            f"tags tag {slug!r} member {ref!r} `rank` must be an integer, "
            f"got {rank!r}.",
            "Give `rank = <int>` or omit it (defaults to 0).",
        )
    starred = raw.get("starred", False)
    if not isinstance(starred, bool):
        raise curation_error(
            "tags_invalid",
            f"tags tag {slug!r} member {ref!r} `starred` must be a boolean, "
            f"got {starred!r}.",
            "Give `starred = true`/`false` or omit it (defaults to false).",
        )
    note = _optional_str(raw, "note", f"tag {slug!r} member {ref!r}")
    return TagMember(
        provider=provider,
        register=register,
        variable=variable,
        rank=rank,
        starred=starred,
        note=note,
    )
