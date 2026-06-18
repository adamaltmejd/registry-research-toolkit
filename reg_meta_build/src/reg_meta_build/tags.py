"""Curated cross-register THEMATIC tag layer (#311).

A maintainer-curated tag vocabulary (income, employment, education, health, …)
attached across providers/registers so a researcher can find candidates without
already knowing which register to look in. Orthogonal to `concept_groups`, which
folds column families *structurally* within ONE register; this layer is
*thematic* across registers. Both are package-root curated overlays that leave
identity untouched — same family, two files (`concept_groups.toml` /
`tags.toml`).

ONE global vocabulary (a tag slug is globally unique — cross-register discovery
is the whole point) + ONE polymorphic membership table:

* **register-grain** members → coarse thematic browse;
* **variable-grain** members → the "golden/starred" recommendations, where
  `starred` flags a recommended variable and `note` carries the one-line
  rationale curation can give (and popularity can't).

MACHINERY ONLY (#311 PR2): the tables ship EMPTY until curation content lands
(`tags.toml` absent or empty); webapp facets / tag chips / search boost are
deferred to a later consumption PR. A *structural* defect in `tags.toml` (bad
shape, duplicate member, dangling reference) fails the build (EXIT_CONFIG), like
the other curation surfaces — curation drift must be fixed, not silently dropped.
"""

from __future__ import annotations

import functools
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    load_curation_entries,
    require_str,
    resolve_register_id,
    resolve_variable_id,
)

if TYPE_CHECKING:
    from collections.abc import Callable


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


def repo_tags_path() -> Path | None:
    """`reg_meta_build/tags.toml` from a repo checkout, or None (wheel installs
    don't ship curation — it's a maintainer artifact like the slug TOMLs and
    `concept_groups.toml`). Sits at the package root, NOT under `fqid_slugs/`
    (that dir is identity curation, glob-loaded as provider-slug TOMLs; tags are a
    presentation/discovery overlay)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "tags.toml"
    return candidate if candidate.is_file() else None


_require_str = functools.partial(
    require_str,
    code="tags_invalid",
    prefix="tags",
    file_name="tags.toml",
)


def _optional_str(entry: dict, field: str, context: str) -> str | None:
    value = entry.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise curation_error(
            "tags_invalid",
            f"tags {context} `{field}` must be a string, got {value!r}.",
            f"Give `{field}` as a string or omit it in reg_meta_build/tags.toml.",
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
    register/variable exist?) happens at materialize time against the built DB."""
    entries = load_curation_entries(
        path,
        entry_key="tag",
        label="tag",
        prefix="tags",
        code_base="tags",
        file_name="tags.toml",
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
                "reg_meta_build/tags.toml.",
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


def materialize_tags(
    conn: sqlite3.Connection,
    tags: tuple[CuratedTag, ...],
    *,
    providers: frozenset[str],
    progress: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Insert the curated tag vocabulary + memberships into the built DB.

    Runs as a curation post-pass after `populate_slugs` (so member FQIDs resolve
    off stored slugs). `providers` gates members to the providers in this build (a
    `--providers=sos` build skips scb members rather than failing them all
    unresolved) — mirrors `materialize_concept_groups`. A member that does NOT
    resolve against the built DB fails the build LOUD (EXIT_CONFIG): a tag is a
    curated structural overlay, so a dangling reference is curation drift to fix,
    not a row to drop. The DB's partial UNIQUE indexes enforce per-grain
    uniqueness and the CHECK enforces exactly-one-grain; an IntegrityError surfaces
    as a `tags_invalid` curation error.

    A tag whose every member is gated out (no in-scope provider) is skipped
    entirely (no empty vocabulary row in a provider-restricted build)."""
    counts = {"tags": 0, "members": 0}
    for tag in tags:
        active = tuple(m for m in tag.members if m.provider in providers)
        if not active:
            continue
        ctx = f"[[tag]] {tag.slug!r}"
        mctx = f"{ctx} member"
        tag_id = conn.execute(
            "INSERT INTO tag (slug, label, description) VALUES (?, ?, ?)",
            (tag.slug, tag.label, tag.description),
        ).lastrowid
        for m in active:
            if m.variable is not None:
                register_id: int | None = None
                variable_id: int | None = resolve_variable_id(
                    conn, m.provider, m.register, m.variable
                )
                if variable_id is None:
                    raise curation_error(
                        "tags_unresolved",
                        f"{mctx}: variable {m.provider}/{m.register}/{m.variable!r} "
                        "does not resolve.",
                        "Fix the member FQID in reg_meta_build/tags.toml.",
                    )
            else:
                register_id = resolve_register_id(conn, m.provider, m.register)
                if register_id is None:
                    raise curation_error(
                        "tags_unresolved",
                        f"{mctx}: register {m.provider}/{m.register!r} does not "
                        "resolve.",
                        "Fix the member FQID in reg_meta_build/tags.toml.",
                    )
                variable_id = None
            try:
                conn.execute(
                    "INSERT INTO tag_member "
                    "(tag_id, register_id, variable_id, rank, starred, note) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (tag_id, register_id, variable_id, m.rank, int(m.starred), m.note),
                )
            except sqlite3.IntegrityError as exc:
                raise curation_error(
                    "tags_invalid",
                    f"{ctx}: member resolves to a (tag, register/variable) pair "
                    "already in this tag.",
                    "Each register/variable may appear at most once per tag in "
                    "reg_meta_build/tags.toml.",
                ) from exc
            counts["members"] += 1
        counts["tags"] += 1
    if progress is not None:
        progress(f"  {counts['tags']:,} tags, {counts['members']:,} tag members")
    return counts
