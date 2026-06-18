"""Concept-group fold-candidate generator (#496, PR1).

The `concept_groups` derivation (#303) folds machine-stamped SCB column families
into PRESENTATION-ONLY browse rows, but its automatic layer is patchy: the `edge`
pass only fires on A2.2 sibling edges, and the `token` pass only recognises the
exact curated month/vintage vocabularies. Everything else (digit-suffixed families
like `sun-niva2000…`, `morsak1/2/3`, the `fasit` yearly series) sits unfolded
unless a maintainer hand-lists it in `concept_groups.toml`.

This module is the GENERATOR half of the curate-then-materialize split that
`variable_same_as` (#417) established: it scans a BUILT DB for ungrouped
digit-suffixed slug families, scores each for label agreement, and emits a ranked
`[[variable_group]]` TOML worklist a maintainer reviews into a future committed
`concept_groups.auto.toml`. It materializes NOTHING and never mutates the DB.

Concept groups are cosmetic (a wrong group is a curation bug, not the identity
corruption that `same_as` risks), so the gate is lighter than same_as's tiers —
the one real hazard is OVER-folding a "battery" (a stem shared by unrelated columns,
e.g. ULF's 2-char `f1/f2/f3` survey items). The generator splits foldable families
from batteries on label agreement and reports the excluded-battery count so the
cutoff is never a silent truncation (CLAUDE.md).

The output schema (`register`/`key`/`label`/`axis` + `[[variable_group.members]]`)
is exactly `concept_groups.load_concept_groups`' input schema, so a confirmed
candidate copies across verbatim — which is why the generator also SKIPS a family
whose `(register, stem)` already names an edge/token group (it would collide on the
`idx_concept_group_key` unique index at the next build) and reports that count too.
Like the other curation TOMLs the worklist is a maintainer artifact — the generator
never writes the curated file.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .concept_groups import _VINTAGE_YEARS, _common_prefix, _trim_label
from .fqid_slugs import _toml_comment, _toml_str

if TYPE_CHECKING:
    import sqlite3

# A trailing run of digits splits a slug into (stem, suffix). The stem is
# non-greedy so the digits are the MAXIMAL trailing run: `sun-niva2000` →
# `('sun-niva', 2000)`, `morsak1` → `('morsak', 1)`. `agi1lonfink` has no
# trailing digit, so it doesn't match (correctly — the digit is internal).
_SUFFIX_RE = re.compile(r"^(.*?)(\d+)$")


@dataclass(frozen=True)
class CandidateMember:
    """One member of a fold candidate: a digit-suffixed variable, its proposed
    facet `value` (the zero-padded suffix so members sort numerically) and `label`
    (the bare suffix by default — the maintainer refines it to "1:a"/"2:a"/a year
    during curation)."""

    suffix: int
    slug: str
    name: str | None
    value: str
    label: str


@dataclass(frozen=True)
class ConceptGroupCandidate:
    """One foldable column family: a `(register, stem)` group of >= `min_siblings`
    digit-suffixed variables that agreed on a common label prefix. `axis` is a
    PROPOSED facet axis (evidence only — the maintainer overrides), and `agreement`
    is the label-prefix-to-mean-name-length ratio that ranked it. Batteries (stems
    shared by unrelated columns) fail the agreement gate and are excluded before any
    candidate is constructed, so every instance here is foldable."""

    provider: str
    register: str
    register_fqid: str
    key: str
    group_label: str
    axis: str
    agreement: float
    members: tuple[CandidateMember, ...]


@dataclass(frozen=True)
class CandidateResult:
    """The generator's output: the foldable `candidates` (ranked) plus the two
    drop counts surfaced so a cutoff is never a SILENT truncation (CLAUDE.md):
    `excluded_batteries` — `(register, stem)` families rejected for weak label
    agreement (the ULF/FRIDA over-fold magnet) — and `skipped_existing_key` —
    families whose `(register, stem)` already names an edge/token concept group, so
    emitting them verbatim would collide on `idx_concept_group_key` at the next
    build (mirrors `_derive_month_groups`' existing-key guard)."""

    candidates: list[ConceptGroupCandidate]
    excluded_batteries: int
    skipped_existing_key: int


@dataclass(frozen=True)
class _RawVar:
    """One ungrouped slugged variable read off the built DB."""

    variable_id: int
    register_id: int
    provider_slug: str
    register_slug: str
    slug: str
    name: str | None


def _load_ungrouped_variables(conn: sqlite3.Connection) -> list[_RawVar]:
    """Every slugged variable NOT already in a concept group. On a real built DB
    this excludes edge/month/curated members (the digit families those passes
    already folded); on a synthetic DB with no groups every slugged variable is
    ungrouped."""
    rows = conn.execute(
        "SELECT v.variable_id, v.register_id, p.slug AS provider_slug, "
        "r.slug AS register_slug, v.slug AS variable_slug, v.name "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND p.slug IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM concept_group_variable m "
        "                WHERE m.variable_id = v.variable_id)"
    ).fetchall()
    return [
        _RawVar(
            variable_id=row["variable_id"],
            register_id=row["register_id"],
            provider_slug=row["provider_slug"],
            register_slug=row["register_slug"],
            slug=row["variable_slug"],
            name=row["name"],
        )
        for row in rows
    ]


def _load_existing_group_keys(conn: sqlite3.Connection) -> set[tuple[int, str]]:
    """Every `(register_id, group_key)` already claimed by a variable concept group
    (edge/token/curated). A candidate keyed on the same `(register_id, stem)` would
    collide on the `idx_concept_group_key` unique index when curated verbatim, so
    `infer_concept_group_candidates` skips it (mirrors `_derive_month_groups`)."""
    return {
        (row["register_id"], row["group_key"])
        for row in conn.execute(
            "SELECT register_id, group_key FROM concept_group WHERE kind = 'variable'"
        )
    }


def _split_stem_suffix(slug: str) -> tuple[str, int] | None:
    """`('sun-niva', 2000)` for `sun-niva2000`, or None when the slug has no
    trailing-digit run or an empty stem (a bare `2000` slug is not a family
    member)."""
    m = _SUFFIX_RE.match(slug)
    if m is None:
        return None
    stem, digits = m.group(1), m.group(2)
    if not stem:
        return None
    return stem, int(digits)


def _propose_axis(suffixes: list[int], stem: str) -> str:
    """Propose a facet axis from the suffix shape (evidence; the maintainer
    overrides):

    - `vintage` — every suffix is a 4-digit year in `_VINTAGE_YEARS` and the stem
      doesn't end in a digit (so the 4 digits are a year, not a longer number's
      tail).
    - `ordinal` — the sorted distinct suffixes are a contiguous run starting at 0
      or 1 (`morsak1/2/3`, the repeated-event facet).
    - `numeric` — anything else (a sparse or non-year numeric tail)."""
    distinct = sorted(set(suffixes))
    # `_VINTAGE_YEARS` (1900..2099) is entirely 4-digit, so range membership alone
    # implies the "4-digit year" shape.
    if not stem[-1:].isdigit() and all(s in _VINTAGE_YEARS for s in distinct):
        return "vintage"
    if distinct[0] in (0, 1) and distinct == list(
        range(distinct[0], distinct[0] + len(distinct))
    ):
        return "ordinal"
    return "numeric"


def infer_concept_group_candidates(
    conn: sqlite3.Connection,
    *,
    min_siblings: int = 2,
    min_label_prefix: int = 8,
    min_agreement: float = 0.5,
) -> CandidateResult:
    """Infer fold candidates from digit-suffixed slug families on a BUILT DB.

    Ungrouped slugged variables are split into `(register_id, stem)` families by
    stripping a trailing digit run; a family with >= `min_siblings` DISTINCT
    suffixes is scored on its members' label agreement. A family is FOLDABLE when
    its common case-insensitive name prefix is >= `min_label_prefix` chars AND the
    prefix-to-mean-name-length ratio (`agreement`) is >= `min_agreement`;
    otherwise it's a BATTERY (a stem shared by unrelated columns) and is excluded,
    counted into `excluded_batteries`. A family whose names are (partly) NULL has
    no labels to agree on and is treated conservatively as a non-fold (like
    `_derive_month_groups`' NULL-name skip).

    A family whose `(register_id, stem)` already names an edge/token concept group
    is SKIPPED (counted into `skipped_existing_key`): emitting it verbatim would
    collide on `idx_concept_group_key` at the next build's `_apply_curated_groups`,
    so it isn't actually verbatim-curatable (mirrors `_derive_month_groups`'
    existing-key guard). The collision check runs FIRST — before the battery/NULL
    gates — so a colliding family is counted once, as a key-collision skip rather
    than a battery.

    NEVER mutates the DB — this is a read-only worklist generator; only the curated
    file's confirmed entries ever load. Foldable candidates are ranked
    deterministically by (-agreement, -member_count, register_fqid, key)."""
    variables = _load_ungrouped_variables(conn)
    existing_keys = _load_existing_group_keys(conn)

    # (register_id, stem) → [_RawVar]. Suffix-less slugs and bare-number slugs
    # (empty stem) drop out — they can't be a family member.
    families: dict[tuple[int, str], list[tuple[int, _RawVar]]] = {}
    for var in variables:
        split = _split_stem_suffix(var.slug)
        if split is None:
            continue
        stem, suffix = split
        families.setdefault((var.register_id, stem), []).append((suffix, var))

    candidates: list[ConceptGroupCandidate] = []
    excluded_batteries = 0
    skipped_existing_key = 0
    for (register_id, stem), members in families.items():
        if len({suffix for suffix, _ in members}) < min_siblings:
            continue

        # Key-collision FIRST: a family keyed on a `(register_id, stem)` already
        # claimed by an edge/token group can't be curated verbatim (it would fail
        # `_apply_curated_groups` on `idx_concept_group_key`). Count it once as a
        # collision skip — ahead of the battery gate — rather than letting a
        # colliding battery double-count.
        if (register_id, stem) in existing_keys:
            skipped_existing_key += 1
            continue

        names = [var.name for _, var in members]
        # Conservative skip: a family with any NULL name has no labels to agree on
        # — neither foldable nor a battery, just not a candidate at all.
        if any(n is None for n in names):
            continue
        present_names = [n for n in names if n is not None]
        # Case-insensitive prefix scores AGREEMENT only (so "Ålder"/"ålder" agree);
        # it must NOT be sliced back onto an original name, since `casefold()` can
        # change length (e.g. German ß → "ss").
        ci_prefix = _common_prefix([n.casefold() for n in present_names])
        mean_len = statistics.mean(len(n) for n in present_names)
        agreement = len(ci_prefix) / mean_len if mean_len > 0 else 0.0

        is_battery = not (
            agreement >= min_agreement and len(ci_prefix) >= min_label_prefix
        )
        if is_battery:
            excluded_batteries += 1
            continue

        # Display label comes from the ORIGINAL-case common prefix (preserves casing
        # for presentation), falling back to the family stem when `_trim_label` strips
        # the prefix to empty (e.g. an all-punctuation prefix at --min-label-prefix 0)
        # — the loader rejects an empty `label` on round-trip.
        group_label = _trim_label(_common_prefix(present_names)) or stem

        ordered = sorted(members, key=lambda m: (m[0], m[1].slug))
        suffixes = [suffix for suffix, _ in ordered]
        axis = _propose_axis(suffixes, stem)
        width = len(str(max(suffixes)))
        candidate_members = tuple(
            CandidateMember(
                suffix=suffix,
                slug=var.slug,
                name=var.name,
                value=f"{suffix:0{width}d}",
                label=str(suffix),
            )
            for suffix, var in ordered
        )
        first = ordered[0][1]
        candidates.append(
            ConceptGroupCandidate(
                provider=first.provider_slug,
                register=first.register_slug,
                register_fqid=f"{first.provider_slug}/{first.register_slug}",
                key=stem,
                group_label=group_label,
                axis=axis,
                agreement=agreement,
                members=candidate_members,
            )
        )

    candidates.sort(
        key=lambda c: (-c.agreement, -len(c.members), c.register_fqid, c.key)
    )
    return CandidateResult(
        candidates=candidates,
        excluded_batteries=excluded_batteries,
        skipped_existing_key=skipped_existing_key,
    )


def render_candidates_toml(
    result: CandidateResult,
    *,
    min_siblings: int,
    min_label_prefix: int,
    min_agreement: float,
) -> str:
    """Render the fold worklist as a `[[variable_group]]` TOML string a maintainer
    curates from. Built by hand (not `tomli_w`) so the per-candidate
    `# axis=… agreement=… members=…` provenance comments survive — `tomli_w` drops
    comments. The header records the active thresholds, the foldable count, the
    `excluded_batteries` count, and the `skipped_existing_key` count (so neither
    cutoff is ever silent).

    The output MUST re-parse cleanly through `concept_groups.load_concept_groups`:
    `register` is a 2-segment FQID, each member sets exactly `variable` +
    `value`/`label`. Every string value is emitted through the shared `_toml_str`
    (full escape set incl. control chars, returns the quotes) so a label or name
    carrying Swedish text, quotes, backslashes, or a stray newline can't break the
    round-trip; the provenance comment runs the label through `_toml_comment` so an
    embedded newline can't terminate the `#` line and let the tail parse as TOML."""
    candidates = result.candidates
    lines = [
        "# GENERATED concept-group fold candidates — "
        "reg-meta-build concept-group-candidates.",
        "#",
        "# These are INFERRED foldable column families, NOT yet folded. Concept",
        "# groups are presentation-only, so review each family and curate the",
        "# confirmed ones into reg_meta_build/concept_groups.toml (or a committed",
        "# concept_groups.auto.toml). Refine each member's facet `label` (the bare",
        "# suffix is a placeholder) and the proposed `axis` during curation.",
        "#",
        f"# thresholds: min-siblings={min_siblings} "
        f"min-label-prefix={min_label_prefix} min-agreement={min_agreement}",
        f"# foldable families: {len(candidates)}",
        f"# excluded batteries (weak label agreement): {result.excluded_batteries}",
        "# skipped (key collides with an existing group): "
        f"{result.skipped_existing_key}",
        "",
    ]
    for c in candidates:
        lines.append(
            f"# axis={c.axis} agreement={c.agreement:.2f} "
            f"members={len(c.members)}  {_toml_comment(c.group_label)}"
        )
        lines.append("[[variable_group]]")
        lines.append(f"register = {_toml_str(c.register_fqid)}")
        lines.append(f"key = {_toml_str(c.key)}")
        lines.append(f"label = {_toml_str(c.group_label)}")
        lines.append(f"axis = {_toml_str(c.axis)}")
        for m in c.members:
            lines.append("[[variable_group.members]]")
            lines.append(f"variable = {_toml_str(m.slug)}")
            lines.append(f"value = {_toml_str(m.value)}")
            lines.append(f"label = {_toml_str(m.label)}")
        lines.append("")
    return "\n".join(lines) + "\n"
