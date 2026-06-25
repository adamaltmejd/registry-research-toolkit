"""Concept-group fold-candidate generator (#496).

The `concept_groups` derivation (#303) folds machine-stamped SCB column families
into PRESENTATION-ONLY browse rows, but its automatic layer is patchy: the `edge`
pass only fires on A2.2 sibling edges, and the `token` pass only recognises the
exact curated month/vintage vocabularies. Everything else (digit-suffixed families
like `morsak1/2/3`, the `fasit` yearly series) sits unfolded
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
# non-greedy so the digits are the MAXIMAL trailing run: `delkomp-ink2000` →
# `('delkomp-ink', 2000)`, `morsak1` → `('morsak', 1)`. `agi1lonfink` has no
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
    build (mirrors `_derive_month_groups`' existing-key guard) — and
    `skipped_trim_collision` — buckets whose trailing-hyphen-trimmed stem (#645)
    collapsed >= 2 genuinely-distinct pre-trim stems that EACH independently qualify
    as a foldable family (`artal-person-1/2/3` + `artal-person4/5/6` → key
    `artal-person`), skipped rather than silently merged into a single wrong group.
    A bucket where only one raw stem qualifies and the rest are noise singletons is
    NOT a collision — the qualifying family is kept and the noise dropped."""

    candidates: list[ConceptGroupCandidate]
    excluded_batteries: int
    skipped_existing_key: int
    skipped_trim_collision: int


@dataclass(frozen=True)
class _RawVar:
    """One ungrouped slugged variable read off the built DB."""

    variable_id: int
    register_id: int
    provider_slug: str
    register_slug: str
    slug: str
    name: str | None


def _evaluate_fold(
    members: list[tuple[str, int, _RawVar]],
    *,
    min_siblings: int,
    min_label_prefix: int,
    min_agreement: float,
) -> list[tuple[str, int, _RawVar]] | None:
    """Decide whether one raw-stem subgroup is a FOLDABLE family, returning its
    members when it folds and `None` otherwise. Used ONLY by the trim-collision
    count (Codex P2 #646): a raw-stem subgroup counts as a competing family only
    when it would ACTUALLY FOLD (not merely meet the sibling floor), so a
    count-qualifying battery / NULL peer can't spuriously suppress a valid family.
    The EMIT path does NOT consume this — it re-runs the same gates on the winner
    (round-1), keeping output byte-identical for the homogeneous case.

    Gates, in order (the same pre-emit gates the emit path applies):
    - `min_siblings` DISTINCT-suffix floor (below it the group isn't a family);
    - NULL-name skip — any NULL member name means no labels to agree on, a
      conservative non-fold (mirrors `_derive_month_groups`);
    - battery gate — the case-insensitive, digit-stripped common name prefix must be
      >= `min_label_prefix` chars AND the prefix-to-mean-name-length ratio
      (`agreement`) >= `min_agreement`; otherwise it's a battery (a stem shared by
      unrelated columns) and not foldable."""
    if len({suffix for _, suffix, _ in members}) < min_siblings:
        return None
    names = [var.name for _, _, var in members]
    if any(n is None for n in names):
        return None
    present_names = [n for n in names if n is not None]
    # Score agreement on each member's name with ALL digit runs stripped (mirrors the
    # emit path); the DISPLAY label is recomputed there from the RAW names.
    stripped = [_strip_digits(n) for n in present_names]
    # Case-insensitive prefix scores AGREEMENT only (so "Ålder"/"ålder" agree); it
    # must NOT be sliced back onto an original name, since `casefold()` can change
    # length (e.g. German ß → "ss").
    ci_prefix = _common_prefix([s.casefold() for s in stripped])
    mean_len = statistics.mean(len(s) for s in stripped)
    agreement = len(ci_prefix) / mean_len if mean_len > 0 else 0.0
    if not (agreement >= min_agreement and len(ci_prefix) >= min_label_prefix):
        return None
    return members


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


def _load_accepted_group_members(
    conn: sqlite3.Connection,
    accepted_scopes: frozenset[tuple[str, str, str]],
) -> dict[tuple[int, str], frozenset[int]]:
    """For each accepted auto family, the `variable_id`s of its MATERIALIZED concept
    group, keyed by `(register_id, group_key)`. Used only by the trim-collision
    accepted-preserve path (Codex P2 #646): when a clean key is accepted AND two raw
    stems independently fold under it, this pins down WHICH folding subgroup is the
    accepted family — the one whose members include the materialized accept's members
    (the accept emitted a single raw-stem candidate, so it lands in exactly one
    subgroup). The remaining (non-accepted) folding peer is the colliding one and is
    dropped.

    The materialized member set can be a SUBSET of the auto family (an `[[accept]]`
    may `exclude` members), so the match is membership-containment, not equality:
    the regenerated subgroup is the FULL auto family and contains every materialized
    member. Empty when no accepts (then the trim-collision path is never accepted-
    exempt and the function isn't consulted)."""
    accepted_members: dict[tuple[int, str], frozenset[int]] = {}
    for row in conn.execute(
        "SELECT g.register_id, g.group_key, p.slug AS provider_slug, "
        "r.slug AS register_slug FROM concept_group g "
        "JOIN register r ON g.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE g.kind = 'variable'"
    ):
        scope = (row["provider_slug"], row["register_slug"], row["group_key"])
        if scope not in accepted_scopes:
            continue
        member_ids = frozenset(
            m["variable_id"]
            for m in conn.execute(
                "SELECT cgv.variable_id FROM concept_group_variable cgv "
                "JOIN concept_group g ON cgv.group_id = g.group_id "
                "WHERE g.kind = 'variable' AND g.register_id = ? "
                "AND g.group_key = ?",
                (row["register_id"], row["group_key"]),
            )
        )
        accepted_members[(row["register_id"], row["group_key"])] = member_ids
    return accepted_members


def _split_stem_suffix(slug: str) -> tuple[str, str, int] | None:
    """`('delkomp-ink', 'delkomp-ink', 2000)` for `delkomp-ink2000`, or None when
    the slug has no trailing-digit run or an empty stem (a bare `2000` slug is not a
    family member). Returns `(key_stem, raw_stem, suffix)`: `raw_stem` is the prefix
    before the trailing digit run (verbatim); `key_stem` is `raw_stem` with any
    trailing hyphen(s) trimmed.

    `key_stem` doubles as the candidate's URL key (`/catalog/group/<p>/<r>/<key>`),
    so a trailing hyphen left by the digit strip — `artal-person-1` →
    `artal-person-`, `bha0001a-1` → `bha0001a-` — is trimmed to a clean slug
    (#645). A stem that is hyphen-only (or empties after the trim) is degenerate:
    return None rather than mint an empty/invalid key (the loader/`_insert_group`
    reject an empty key anyway). `raw_stem` is kept so the caller can detect a
    trim that collapses two genuinely-distinct stems into one key
    (`artal-person-1` and `artal-person1` both → key `artal-person`) and skip the
    merge instead of folding unrelated families together."""
    m = _SUFFIX_RE.match(slug)
    if m is None:
        return None
    raw_stem, digits = m.group(1), m.group(2)
    key_stem = raw_stem.rstrip("-")
    if not key_stem:
        return None
    return key_stem, raw_stem, int(digits)


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
    # Materialized member ids of each accepted family, for the trim-collision
    # accepted-preserve path (#646). Empty unless accepts exist.
    accepted_members = _load_accepted_group_members(conn, accepted_scopes)

    # (register_id, key_stem) → [(raw_stem, suffix, _RawVar)]. Suffix-less slugs
    # and bare-number slugs (empty stem) drop out — they can't be a family member.
    # `key_stem` is the trailing-hyphen-TRIMMED URL key (#645); `raw_stem` (the
    # untrimmed prefix) is tracked per member so a trim that collapses two distinct
    # stems into one key is detectable.
    families: dict[tuple[int, str], list[tuple[str, int, _RawVar]]] = {}
    for var in variables:
        split = _split_stem_suffix(var.slug)
        if split is None:
            continue
        key_stem, raw_stem, suffix = split
        families.setdefault((var.register_id, key_stem), []).append(
            (raw_stem, suffix, var)
        )

    candidates: list[ConceptGroupCandidate] = []
    excluded_batteries = 0
    skipped_existing_key = 0
    skipped_trim_collision = 0
    for (register_id, stem), bucket in families.items():
        by_raw_stem: dict[str, list[tuple[str, int, _RawVar]]] = {}
        for raw_stem, suffix, var in bucket:
            by_raw_stem.setdefault(raw_stem, []).append((raw_stem, suffix, var))

        # Accepted-key handling FIRST (#646/#651), governing REGARDLESS of how many
        # raw stems fold — it subsumes the old `len(fold_qual) > 1` accepted-preserve.
        # A clean key that is currently `[[accept]]`-ed is MATERIALIZED, so the winner
        # for that key MUST be the accepted family (preserve-or-fail, never retarget):
        # identify it by member-containment against the materialized accept's members
        # (the accept emitted a single raw-stem candidate, so it lands in exactly one
        # raw-stem subgroup; containment, not equality, tolerates an `exclude` that
        # trimmed the materialized set). Two failure modes the `> 1`-only preserve
        # missed (#651):
        #   - the accepted raw-stem subgroup NO LONGER folds (labels degraded to
        #     NULL/weak after corpus drift) AND a DIFFERENT raw stem DOES fold → only
        #     one stem folds, so the old `> 1` block was skipped and the non-accepted
        #     peer was selected, emitting under the accepted scope (the `[[accept]]`
        #     then silently resolves to the WRONG variables);
        #   - the genuine two-folder collision the `> 1` block already handled.
        # In both, force the winner to the accepted subgroup: if it still folds it
        # re-emits (idempotent); if it has DEGRADED the normal emit path naturally
        # drops it (NULL→continue / battery→excluded), which is the "fail" arm —
        # `resolve_accept` fails LOUDLY at the next build, the correct outcome. A
        # non-accepted peer is NEVER selected for an accepted key. The non-accepted
        # peer is NOT counted into `skipped_trim_collision` (a peer colliding with an
        # accepted scope is not a symmetric two-family drop). The `> 1`
        # skipped_trim_collision path below applies ONLY to non-accepted keys.
        accepted_ids = accepted_members.get((register_id, stem))
        if accepted_ids:
            accepted_sub = next(
                (
                    sub
                    for sub in by_raw_stem.values()
                    if accepted_ids <= {var.variable_id for _, _, var in sub}
                ),
                None,
            )
            # The accepted members are materialized as a group, so on a normal built
            # DB exactly one raw-stem subgroup supersets them. If none does (the
            # accepted family has genuinely vanished from the catalog), emit nothing
            # for this key — `resolve_accept` will fail loudly next build (the "fail"
            # arm). Never let a peer win the accepted key.
            if accepted_sub is None:
                continue
            members = accepted_sub
        else:
            # ---- non-accepted keys: round-1 trim-collision logic (#645/#646) ----
            # A clean `stem` reached by `.rstrip("-")` can fuse pre-trim stems into one
            # bucket (`artal-person-1…` AND `artal-person1…` both → `artal-person`).
            # Use `_evaluate_fold` — the FULL foldability predicate (distinct-suffix
            # floor AND no NULL names AND the battery/label-agreement gate) — ONLY to
            # decide the COLLISION count: a raw-stem subgroup is a competing family only
            # when it would ACTUALLY FOLD, not merely meet the sibling floor (so a
            # count-qualifying battery/NULL peer can't spuriously suppress a valid
            # family). When >= 2 raw stems each fold it is a GENUINE two-family
            # collision — skip-and-count (never a silent merge, mirrors
            # `_derive_month_groups`' skip-and-warn). Otherwise the EMIT path below is
            # exactly round-1: it selects a winner and runs the original
            # existing-key/NULL/battery/label gates on it, so a homogeneous bucket (one
            # raw stem — the entire real corpus) reduces precisely to round-1 and the
            # output stays byte-identical.
            fold_qual = [
                folded
                for sub in by_raw_stem.values()
                if (
                    folded := _evaluate_fold(
                        sub,
                        min_siblings=min_siblings,
                        min_label_prefix=min_label_prefix,
                        min_agreement=min_agreement,
                    )
                )
                is not None
            ]
            if len(fold_qual) > 1:
                skipped_trim_collision += 1
                continue
            # Winner selection (round-1): the single folding subgroup when one folds,
            # else the count-floor subgroup — so a lone battery / NULL family still
            # reaches the original existing-key/battery diagnostics. `count_qual` is the
            # round-1 `qualifying` (distinct-suffix floor only); when no raw stem even
            # meets the floor the bucket is dropped (no family).
            count_qual = [
                sub
                for sub in by_raw_stem.values()
                if len({suffix for _, suffix, _ in sub}) >= min_siblings
            ]
            if fold_qual:
                members = fold_qual[0]
            elif count_qual:
                members = count_qual[0]
            else:
                continue

        # ---- round-1 emit path on the winner (unchanged) ----
        # Key-collision FIRST: a family keyed on a `(register_id, stem)` already
        # claimed by an edge/token group can't be curated verbatim (it would fail
        # `_apply_curated_groups` on `idx_concept_group_key`). Count it once as a
        # collision skip — ahead of the battery gate — rather than letting a
        # colliding battery double-count.
        if (register_id, stem) in existing_keys:
            skipped_existing_key += 1
            continue

        names = [var.name for _, _, var in members]
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

        # Sort by (suffix, slug); member tuples are (raw_stem, suffix, var).
        ordered = sorted(members, key=lambda m: (m[1], m[2].slug))
        suffixes = [suffix for _, suffix, _ in ordered]
        # Axis evidence uses the RAW stem (#645): the family is homogeneous on raw
        # stem here, and `_propose_axis` keys "vintage vs numeric" off whether the
        # stem ends in a digit. The trimmed `key` stem can drop the very trailing
        # hyphen that proved a year tail wasn't part of a longer number (`foo1-2000`
        # raw stem `foo1-` ends in `-`, but the trimmed `foo1` ends in a digit and
        # would mis-classify as numeric). The emitted `key` stays the trimmed stem.
        raw_stem = ordered[0][0]
        axis = _propose_axis(suffixes, raw_stem)
        width = len(str(max(suffixes)))
        candidate_members = tuple(
            CandidateMember(
                suffix=suffix,
                slug=var.slug,
                name=var.name,
                value=f"{suffix:0{width}d}",
                label=str(suffix),
            )
            for _, suffix, var in ordered
        )
        first = ordered[0][2]
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
        skipped_trim_collision=skipped_trim_collision,
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
    thresholds, the foldable count, the `excluded_batteries` count, the
    `skipped_existing_key` count, and the `skipped_trim_collision` count (so no
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
        "# skipped (trailing-hyphen trim collapsed two distinct stems): "
        f"{result.skipped_trim_collision}",
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
