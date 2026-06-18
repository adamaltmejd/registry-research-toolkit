"""Concept-group fold-candidate generator (#496).

The `concept_groups` derivation (#303) folds machine-stamped SCB column families
into PRESENTATION-ONLY browse rows, but its automatic layer is patchy: the `edge`
pass only fires on A2.2 sibling edges, and the `token` pass only recognises the
exact curated month/vintage vocabularies. Everything else (digit-suffixed families
like `sun-niva2000…`, `morsak1/2/3`, the `fasit` yearly series) sits unfolded
unless a maintainer opts it in via `concept_groups.toml`.

This module is the GENERATOR half of the generate-then-accept split that
`variable_same_as` (#417) established: it scans a BUILT DB for ungrouped
digit-suffixed slug families, scores each for label agreement, and emits the
committed, machine-owned `concept_groups.auto.toml` — the ranked candidate
catalog. It materializes NOTHING and never mutates the DB; it only writes the
auto file (the maintainer never hand-edits that file).

Candidates fold OPT-IN: a family in `concept_groups.auto.toml` folds only when an
`[[accept]]` entry in `concept_groups.toml` references it by `(register, key)`
(see `concept_groups.load_concept_group_accepts` / `resolve_accept`) — there is no
copy-across; the accept is a thin by-reference pointer (with optional
`label`/`axis`/`exclude` overrides).

Regeneration is IDEMPOTENT: an accepted family is materialized as a `curated`
concept group during the build, which would otherwise drop it from the next scan
(grouped members, self-colliding key). So the generator READS the accept-list
(`concept_groups.toml`) and treats accepted families as still-candidate-eligible —
they re-emit into the catalog instead of vanishing, keeping the accepts resolvable.
A custom `[[variable_group]]` family and the edge/token/vintage passes are NOT
candidates and stay excluded.

Concept groups are cosmetic (a wrong group is a curation bug, not the identity
corruption that `same_as` risks), so the gate is lighter than same_as's tiers —
the one real hazard is OVER-folding a "battery" (a stem shared by unrelated columns,
e.g. ULF's 2-char `f1/f2/f3` survey items). The generator splits foldable families
from batteries on label agreement and reports the excluded-battery count so the
cutoff is never a silent truncation (CLAUDE.md).

The candidate schema (`register`/`key`/`label`/`axis` + `[[variable_group.members]]`)
is exactly `concept_groups.load_concept_groups`' input schema. The generator also
SKIPS a family whose `(register, stem)` already names an edge/token group: an
`[[accept]]` resolves against a FRESH build at materialize time, so accepting such
a candidate would collide on the `idx_concept_group_key` unique index. The family is
dropped from the catalog so the catalog stays accept-safe, and the dropped count is
reported (never a silent truncation).
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


def _strip_digits(name: str) -> str:
    """Normalize a label for AGREEMENT scoring by removing every maximal digit run,
    so a number sitting MID-label (`Åtgärdskod 1, den förlösta`) doesn't truncate the
    family's common label prefix at the digit. Stripping ALL digit runs uniformly —
    not just the member's own slot number — removes a FIXED numeric qualifier shared
    by the whole family (`Tillsyn 1 skolbarn …`, where "1" is a constant, not the
    slot) identically across every member, so the common prefix survives instead of
    breaking on the one member whose slot number equals that constant. Handles
    mid-label slot numbers, fixed qualifiers, and zero-padded numerals in one pass.
    Used ONLY to score agreement — the DISPLAY label still derives from the raw
    names."""
    return re.sub(r"\d+", "", name)


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
    is the label-prefix-to-mean-name-length ratio that ranked it — scored on each
    member's name with ALL digit runs removed, so a number sitting mid-label (or a
    fixed numeric qualifier shared by the family) no longer truncates the prefix (see
    `_strip_digits`). Batteries (stems
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


def _load_ungrouped_variables(
    conn: sqlite3.Connection,
    accepted_scopes: frozenset[tuple[str, str, str]],
) -> list[_RawVar]:
    """Every slugged variable NOT already in a concept group, EXCEPT variables
    grouped only by an accepted auto family. On a real built DB the plain exclusion
    drops edge/month/curated members (the digit families those passes already
    folded); on a synthetic DB with no groups every slugged variable is ungrouped.

    Accept-awareness (idempotent regeneration): an `[[accept]]` materializes its
    auto family as a `curated` concept group at build time, which would otherwise
    drop that family from the next regeneration (its members are now grouped). So a
    variable whose ONLY group is an accepted family — `(provider, register,
    group_key)` in `accepted_scopes` — stays candidate-eligible. Variables in a
    NON-accepted group (custom `[[variable_group]]`, edge/token/vintage) remain
    excluded. With an empty `accepted_scopes` this is byte-identical to the plain
    `NOT EXISTS` exclusion."""
    # Two-step exclusion: collect the grouped-but-NOT-accepted variable_ids, then
    # exclude only those — re-including variables grouped solely by an accepted
    # family. (A variable belongs to at most one group, so its scope is unambiguous.)
    grouped_excluded = {
        row["variable_id"]
        for row in conn.execute(
            "SELECT m.variable_id, p.slug AS provider_slug, "
            "r.slug AS register_slug, g.group_key "
            "FROM concept_group_variable m "
            "JOIN concept_group g ON m.group_id = g.group_id "
            "JOIN register r ON g.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id"
        )
        if (row["provider_slug"], row["register_slug"], row["group_key"])
        not in accepted_scopes
    }
    rows = conn.execute(
        "SELECT v.variable_id, v.register_id, p.slug AS provider_slug, "
        "r.slug AS register_slug, v.slug AS variable_slug, v.name "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND p.slug IS NOT NULL"
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
        if row["variable_id"] not in grouped_excluded
    ]


def _load_existing_group_keys(
    conn: sqlite3.Connection,
    accepted_scopes: frozenset[tuple[str, str, str]],
) -> set[tuple[int, str]]:
    """Every `(register_id, group_key)` already claimed by a variable concept group
    (edge/token/curated), EXCLUDING accepted auto families. A candidate keyed on the
    same `(register_id, stem)` would collide on the `idx_concept_group_key` unique
    index when curated verbatim, so `infer_concept_group_candidates` skips it (mirrors
    `_derive_month_groups`).

    An accepted family's own `(provider, register, group_key)` is dropped from the
    set (it's in `accepted_scopes`): without this the materialized accept would look
    like a self-collision and the generator would skip re-emitting the very family
    the accept references — breaking idempotent regeneration. With an empty
    `accepted_scopes` this returns every variable group, byte-identical to before."""
    return {
        (row["register_id"], row["group_key"])
        for row in conn.execute(
            "SELECT g.register_id, g.group_key, p.slug AS provider_slug, "
            "r.slug AS register_slug FROM concept_group g "
            "JOIN register r ON g.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE g.kind = 'variable'"
        )
        if (row["provider_slug"], row["register_slug"], row["group_key"])
        not in accepted_scopes
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
    accepted_scopes: frozenset[tuple[str, str, str]] = frozenset(),
) -> CandidateResult:
    """Infer fold candidates from digit-suffixed slug families on a BUILT DB.

    Ungrouped slugged variables are split into `(register_id, stem)` families by
    stripping a trailing digit run; a family with >= `min_siblings` DISTINCT
    suffixes is scored on its members' label agreement. Agreement is NUMBER-INVARIANT:
    each member's name is scored with ALL digit runs removed (`_strip_digits`), so a
    slot number sitting mid-label (`Åtgärdskod 1, den förlösta` … `Åtgärdskod 12, …`)
    — and a FIXED numeric qualifier shared by the whole family (`Tillsyn 1 skolbarn …`,
    where the "1" is a constant, not the slot) — no longer truncates the common prefix
    at the digit, and a genuine multi-instance family scores ~1.0. A family is FOLDABLE
    when
    its common case-insensitive (number-stripped) name prefix is >= `min_label_prefix`
    chars AND the prefix-to-mean-name-length ratio (`agreement`) is >= `min_agreement`;
    otherwise it's a BATTERY (a stem shared by unrelated columns, whose label TEXT
    genuinely differs and so still disagrees after number-stripping) and is excluded,
    counted into `excluded_batteries`. The DISPLAY label (`group_label`) still derives
    from the RAW names, so accepted families' labels are unchanged. A family whose
    names are (partly) NULL has no labels to agree on and is treated conservatively as
    a non-fold (like `_derive_month_groups`' NULL-name skip).

    A family whose `(register_id, stem)` already names an edge/token concept group
    is SKIPPED (counted into `skipped_existing_key`): emitting it verbatim would
    collide on `idx_concept_group_key` at the next build's `_apply_curated_groups`,
    so it isn't actually verbatim-curatable (mirrors `_derive_month_groups`'
    existing-key guard). The collision check runs FIRST — before the battery/NULL
    gates — so a colliding family is counted once, as a key-collision skip rather
    than a battery.

    PRESERVES accepted families (idempotent regeneration): `accepted_scopes` is the
    set of `(provider, register, key)` of the auto families currently `[[accept]]`-ed
    in `concept_groups.toml`. Each such family is MATERIALIZED as a `curated` concept
    group at build time — its members are grouped and its `(register, key)` names a
    group — so a naive regeneration against a normal built DB would DROP every
    accepted family (excluded as grouped, skipped as a self-collision) and the next
    build's `resolve_accept` would fail on the now-missing candidate. Passing the
    accepted scopes re-includes those members and exempts their own key from the
    collision guard, so accepted families re-emit and the catalog is STABLE under
    regeneration. With an empty `accepted_scopes` (no accepts) behavior is
    byte-identical to a plain scan.

    NEVER mutates the DB — this is a read-only worklist generator; only the curated
    file's confirmed entries ever load. Foldable candidates are ranked
    deterministically by (-agreement, -member_count, register_fqid, key)."""
    variables = _load_ungrouped_variables(conn, accepted_scopes)
    existing_keys = _load_existing_group_keys(conn, accepted_scopes)

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
        # Score agreement on each member's name with ALL digit runs stripped, so a
        # number sitting mid-label (`Åtgärdskod 1, …` … `Åtgärdskod 12, …`) — and a
        # fixed numeric qualifier shared by the family (`Tillsyn 1 skolbarn …`) —
        # doesn't truncate the common prefix at the digit (a genuine multi-instance
        # family then scores ~1.0). Stripping every digit run uniformly (not just each
        # member's own slot number) keeps the common prefix intact even when the shared
        # constant equals one member's suffix. The DISPLAY label below still derives
        # from the RAW names, so labels are unchanged.
        stripped = [_strip_digits(n) for n in present_names]
        # Case-insensitive prefix scores AGREEMENT only (so "Ålder"/"ålder" agree);
        # it must NOT be sliced back onto an original name, since `casefold()` can
        # change length (e.g. German ß → "ss").
        ci_prefix = _common_prefix([s.casefold() for s in stripped])
        mean_len = statistics.mean(len(s) for s in stripped)
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
    """Render the committed, machine-owned `concept_groups.auto.toml` candidate
    catalog as a `[[variable_group]]` TOML string a maintainer folds from by
    `[[accept]]` reference. Built by hand (not `tomli_w`) so the per-candidate
    `# axis=… agreement=… members=…` provenance comments survive — `tomli_w` drops
    comments. The header records the executable regenerate command, the active
    thresholds, the foldable count, the `excluded_batteries` count, and the
    `skipped_existing_key` count (so neither cutoff is ever silent).

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
        "# THIS FILE IS MACHINE-OWNED. It IS reg_meta_build/concept_groups.auto.toml,",
        "# the committed candidate catalog. Regenerate it with:",
        "#   reg-meta-build --db <built-db-dir> concept-group-candidates \\",
        "#     --output-toml reg_meta_build/concept_groups.auto.toml",
        "# (`--db` points at a built reg_meta DB to scan; `--output-toml` targets",
        "# this committed file). NEVER hand-edit it (edits are overwritten).",
        "#",
        "# These are INFERRED foldable column families, NOT folded by default.",
        "# Folding is OPT-IN: to fold a family, add an `[[accept]]` entry in",
        "# reg_meta_build/concept_groups.toml referencing its `register` + `key`",
        '# (optional `label` / `axis` overrides, optional `exclude = ["<slug>", …]`).',
        "# An unaccepted family stays unfolded. Concept groups are presentation-only,",
        "# so review each family before accepting it.",
        "#",
        "# Regeneration is IDEMPOTENT: the generator reads concept_groups.toml's",
        "# accept-list and PRESERVES already-accepted families here (an accept",
        "# materializes its family as a group, which a naive rescan would drop), so",
        "# regenerating against a normal built DB keeps every accept resolvable.",
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
