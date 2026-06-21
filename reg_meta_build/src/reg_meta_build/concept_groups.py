"""Derived concept-group layer (#303).

Build-time grouping of near-identical catalog rows into PRESENTATION-ONLY
concept groups, so browse surfaces (webapp catalog, future CLI listings) can
collapse machine-stamped SCB column families into one row with a facet picker.
Identity is untouched: bindings/orders/stats keep leaf FQIDs, a group is not
FQID-addressable, and a wrong group is a cosmetic curation bug — not the
identity corruption that killed identity-level classification folding
(#223 part 2, 195 measured over-folds).

Three dimension sources, derived in priority order (a variable/classification
belongs to AT MOST ONE group — enforced by the member-table PKs; a later pass
never claims an already-grouped member):

0. ``edge`` — connected components of within-register
   ``same_definition_different_column`` split-sibling pairs. Ground truth: the
   A2.2 split machinery minted these between the delivery columns of ONE SCB
   variable definition, so folding them back into one browse row is
   zero-inference (e.g. ureg's sun2000inr/sun2020inr coding succession; on the
   real corpus ~2,200 components covering ~8,200 variables, 2,191/2,193
   sharing a single name). The pairs are read from the IN-BUILD sibling sets the
   triage minted (``edge_siblings``), NOT from the ``variable_related_to`` table
   — the foldable ``same_def`` edges are no longer persisted there (#591; the
   table now carries only the meaningful curated/non-foldable links). A later
   ``curated`` ``[[variable_group]]`` takes PRECEDENCE: any FQID it claims is
   subtracted from the edge components before they mint groups (so #488's
   per-population curation can re-home a näringsgren variable the edge fold would
   otherwise grab), and a component reduced below 2 survivors mints no group.
1. ``token`` — exact curated vocabularies only (NO regex name-patterns, per
   the standing curation rule): Swedish month slug tails (both the short and
   full forms SCB mixes, e.g. lisa's agi1lonfinkjan…agi1lonfinkdec) for
   variables. Guarded — a slug merely ending in "maj" must not fold: variables
   need >= ``_MIN_MONTH_SIBLINGS`` distinct months on one stem AND label-prefix
   agreement.

   Classification VINTAGE families are detected by the SAME slug-tail rule
   (4-digit year + name-agreement guard) but are NOT folded into a concept group
   (#571): editions of one classification (ssyk1996→ssyk2012, lkf1980…lkf2026)
   are a temporal SUCCESSION, not a parallel facet-picker. They materialize as
   adjacent-edition edges in ``classification_replaced_by`` instead — see
   ``derive_classification_succession``. The ``concept_group_classification``
   table and the ``kind='classification'`` machinery are RETAINED (empty of
   derived rows) for the curated umbrella groups #516 adds later.
2. ``curated`` — maintainer TOML (``reg_meta_build/concept_groups.toml``), three
   opt-in entry kinds:
   - ``[[variable_group]]`` — a hand-authored family with an exact member list:
     attaches single variables under the family's single facet ``axis`` (e.g.
     the LISA agi{1,2,3} rank facet, or the LOVA invdatum/invland ordinal facet).
   - ``[[accept]]`` (#496) — folds a candidate family from the generated,
     machine-owned ``concept_groups.auto.toml`` BY REFERENCE, located by
     ``(register, key)``, with optional ``label``/``axis``/``exclude`` overrides
     (``resolve_accept`` turns it into a ``CuratedGroup`` of ``variable=``
     attachments). An auto family folds ONLY when accepted; unaccepted ones
     never materialize.
   - ``[[classification_group]]`` (#516) — a curated ``kind='classification'``
     umbrella over genuinely-DISTINCT classification dimensions (the SUN group
     over niva/inriktning/grupp; NOT vintage editions, which are #571 succession
     edges), single ``axis`` stored on ``concept_group.facet_axis``. Catalog-
     scoped (classifications are global), materialized by
     ``_apply_curated_classification_groups``.
   All kinds fail fast on unresolvable references (EXIT_CONFIG).

When the interval-native model (#271) lands its column→variable merges, the
month groups graduate into real single variables and this layer shrinks to
edge/rank/vintage duty.
"""

from __future__ import annotations

import functools
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

from ._components import DisjointSet
from ._curation import curation_error, load_curation_entries, require_str

# A concept-group key is one URL path segment in
# `/catalog/group/<provider>/<register>/<key>` (#640). Path-safe = the RFC 3986
# *unreserved* set, lowercased (every current materialized key and auto candidate
# uses only these), excluding the `.`/`..` dot-segments. NOT `is_slug`: keys are
# intentionally not slug-validated, and the candidate generator emits valid
# trailing-hyphen keys (e.g. `artal-person-`) that `is_slug` would reject — yet
# they are path-safe, so over-rejecting them would break the `[[accept]]`
# by-reference workflow (an accepted key can't be replaced).
_PATH_SAFE_KEY_RE = re.compile(r"[a-z0-9._~-]+")


def _is_path_safe_key(key: str) -> bool:
    return bool(_PATH_SAFE_KEY_RE.fullmatch(key)) and key not in (".", "..")


# ── token vocabularies + guards ─────────────────────────────────────────────

# Exact slug-tail token → month number. SCB mixes short and full forms within
# one family (lisa agi1lonfink: jan, feb, mars, april, maj, juni, juli, aug,
# sep, okt, nov, dec), so both vocabularies are listed. No token is a suffix
# of another, so a slug tail matches at most one token. Exact strings only —
# no regex name-patterns (standing curation rule).
_MONTH_TOKENS: dict[str, int] = {
    "jan": 1,
    "januari": 1,
    "feb": 2,
    "februari": 2,
    "mar": 3,
    "mars": 3,
    "apr": 4,
    "april": 4,
    "maj": 5,
    "jun": 6,
    "juni": 6,
    "jul": 7,
    "juli": 7,
    "aug": 8,
    "augusti": 8,
    "sep": 9,
    "september": 9,
    "okt": 10,
    "oktober": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
# Canonical display label per month (the facet `label`; the facet `value` is
# the zero-padded number so lexicographic ordering is chronological).
_MONTH_LABELS: dict[int, str] = {
    1: "januari",
    2: "februari",
    3: "mars",
    4: "april",
    5: "maj",
    6: "juni",
    7: "juli",
    8: "augusti",
    9: "september",
    10: "oktober",
    11: "november",
    12: "december",
}

# Month-fold guards: >= 3 distinct month siblings on one stem AND a shared
# label prefix of >= 5 chars after trimming. A coincidental "…maj" slug has no
# sibling months and no shared label, so it never folds. Verified against the
# real corpus (2026-06-11): 8 groups / 96 variables, zero false folds.
_MIN_MONTH_SIBLINGS = 3
_MIN_LABEL_PREFIX = 5

# Classification vintage chains need only 2 editions (the catalog is tiny and
# curated — agarkat2000/2020, ssyk1996/2012 are genuine 2-vintage successions)
# but the STRONGER name guard: every member's name must contain its vintage
# year, and the year-stripped names must all be identical.
_MIN_VINTAGE_SIBLINGS = 2
_VINTAGE_YEARS = range(1900, 2100)

# The foldable split-sibling kind (#591). Public so db.py can skip persisting it
# to `variable_related_to` and key the in-build `edge_siblings` subset off it.
EDGE_RELATION_KIND = "same_definition_different_column"


# ── curated TOML ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedMember:
    """One member of a curated family: a single variable slug. `value`/`label`
    are this member's facet on the family's single `axis`."""

    variable: str
    value: str
    label: str


@dataclass(frozen=True)
class CuratedGroup:
    """One curated family the materializer applies. `origin` records how it was
    authored so `_apply_curated_groups` can tailor its EXIT_CONFIG remediations:
    a hand-authored `[[variable_group]]` (the default) points the maintainer at
    `concept_groups.toml`; an `[[accept]]`-resolved family (`resolve_accept` sets
    `origin="accept"`) points at the `[[accept]]` / `concept_groups.auto.toml`
    instead, since its key/register/members come from the generated catalog, not
    a hand-picked curated key."""

    provider: str
    register: str
    key: str
    label: str
    axis: str
    members: tuple[CuratedMember, ...]
    origin: Literal["variable_group", "accept"] = "variable_group"


@dataclass(frozen=True)
class Accept:
    """One `[[accept]]` entry from `concept_groups.toml` (#496): an OPT-IN to
    fold an auto family from `concept_groups.auto.toml` BY REFERENCE. The
    `(provider, register, key)` locates the auto family; `label`/`axis` override
    the auto family's when set; `exclude` drops member slugs (a stem that picked
    up an unrelated column). Resolved to a `CuratedGroup` at materialize time
    (`resolve_accept`) against the loaded auto families."""

    provider: str
    register: str
    key: str
    label: str | None
    axis: str | None
    exclude: tuple[str, ...]


@dataclass(frozen=True)
class ClassificationGroupMember:
    """One member of a curated CLASSIFICATION umbrella group: the `classification`
    slug (catalog-global, e.g. `sun-niva2020`) and its `value`/`label` on the
    group's single `axis` (the SUN group's `dimension` axis → 'niva'/'inriktning'/
    'grupp')."""

    classification: str
    value: str
    label: str


@dataclass(frozen=True)
class ClassificationGroup:
    """One curated `[[classification_group]]` umbrella (#516): a single-axis fold
    over genuinely-DISTINCT classification dimensions (NOT vintage editions —
    those are #571 succession edges). `axis` is the one shared facet axis stored
    on `concept_group.facet_axis`; every member sits on it. Catalog-scoped
    (classifications are global), so unlike `CuratedGroup` it carries no
    provider/register."""

    key: str
    label: str
    axis: str
    members: tuple[ClassificationGroupMember, ...]


def repo_concept_groups_path() -> Path | None:
    """`reg_meta_build/concept_groups.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir
    is glob-loaded as provider-slug TOMLs; a file there would break the
    build)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "concept_groups.toml"
    return candidate if candidate.is_file() else None


def repo_concept_groups_auto_path() -> Path | None:
    """`reg_meta_build/concept_groups.auto.toml` from a repo checkout, or None.
    The GENERATED, machine-owned catalog of fold candidates the
    `concept-group-candidates` command emits (#496); committed but never
    hand-edited. A `[[accept]]` in `concept_groups.toml` folds a family from
    here BY REFERENCE. Sibling of `repo_concept_groups_path()` at the package
    root; None on wheel installs (curation artifacts aren't shipped)."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent / "concept_groups.auto.toml"
    )
    return candidate if candidate.is_file() else None


_require_str = functools.partial(
    require_str,
    code="concept_groups_invalid",
    prefix="concept_groups",
    file_name="concept_groups.toml",
)


def load_concept_groups(path: Path | None) -> tuple[CuratedGroup, ...]:
    """Parse the curated-family TOML. Empty when no file (synthetic test
    builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only
    `[[variable_group]]` top-level; `register` is a 2-segment
    `provider/register` FQID string; `key`/`label`/`axis` non-empty strings;
    each member sets `variable` plus `value`/`label`; keys are unique.
    Reference RESOLUTION (register/variable exist) happens at materialize time
    against the built DB, not here."""
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="variable_group",
        label="concept-group",
        prefix="concept_groups",
        code_base="concept_groups",
        file_name="concept_groups.toml",
        entry_fields="register / key / label / axis / members",
        # `concept_groups.toml` carries two other entry kinds — `[[accept]]`
        # (folds an auto family by reference, `load_concept_group_accepts`) and
        # `[[classification_group]]` (curated umbrella, `load_classification_groups`)
        # — so both are legal siblings here, not unknown-top-level typos. Harmless
        # for `concept_groups.auto.toml`, which carries neither.
        sibling_keys=frozenset({"accept", "classification_group"}),
    )
    out: list[CuratedGroup] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register", "[[variable_group]]")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups register {register_fqid!r} must be a 2-segment "
                "`provider/register` FQID.",
                'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
            )
        key = _require_str(entry, "key", "[[variable_group]]")
        label = _require_str(entry, "label", "[[variable_group]]")
        axis = _require_str(entry, "axis", "[[variable_group]]")
        scope_key = (parts[0], parts[1], key)
        if scope_key in seen_keys:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups duplicate key {key!r} under {register_fqid}.",
                "Group keys must be unique per register.",
            )
        seen_keys.add(scope_key)
        raw_members = entry.get("members", [])
        if not isinstance(raw_members, list) or not raw_members:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} needs a non-empty "
                "`[[variable_group.members]]` array.",
                "List the family's members as `[[variable_group.members]]` tables.",
            )
        members: list[CuratedMember] = []
        seen_refs: set[str] = set()
        for raw in raw_members:
            if not isinstance(raw, dict):
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} member {raw!r} must be a table.",
                    "Each member is a `[[variable_group.members]]` table.",
                )
            ref = _require_str(raw, "variable", f"group {key!r} member")
            if ref in seen_refs:
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} references variable {ref!r} twice.",
                    "List each member variable once.",
                )
            seen_refs.add(ref)
            members.append(
                CuratedMember(
                    variable=ref,
                    value=_require_str(raw, "value", f"group {key!r} member"),
                    label=_require_str(raw, "label", f"group {key!r} member"),
                )
            )
        out.append(
            CuratedGroup(
                provider=parts[0],
                register=parts[1],
                key=key,
                label=label,
                axis=axis,
                members=tuple(members),
            )
        )
    return tuple(out)


def _require_opt_str(entry: dict, field: str, context: str) -> str | None:
    """Optional non-empty string: None when absent, else `require_str`'s
    stripped value (a present-but-blank `label`/`axis` is curation drift, not a
    silent fallback to the auto family's value)."""
    if entry.get(field) is None:
        return None
    return _require_str(entry, field, context)


def load_concept_group_accepts(path: Path | None) -> tuple[Accept, ...]:
    """Parse the `[[accept]]` entries from `concept_groups.toml` (#496): the
    opt-in accept-list that folds auto families from `concept_groups.auto.toml`
    by reference. Empty when no file (synthetic builds, wheel installs) or no
    `[[accept]]` tables.

    Load-time validation (all EXIT_CONFIG, actionable): `register` is a
    2-segment `provider/register` FQID; `key` non-empty; `label`/`axis` optional
    but non-empty strings if present; `exclude` optional list of non-empty
    strings; `(provider, register, key)` unique (a duplicate accept is drift).
    Resolution against the auto families (does the family exist?) happens at
    materialize time (`resolve_accept`)."""
    entries = load_curation_entries(
        path,
        entry_key="accept",
        label="concept-group",
        prefix="concept_groups",
        code_base="concept_groups",
        file_name="concept_groups.toml",
        entry_fields="register / key (+ optional label / axis / exclude)",
        sibling_keys=frozenset({"variable_group", "classification_group"}),
    )
    out: list[Accept] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register", "[[accept]]")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups accept register {register_fqid!r} must be a "
                "2-segment `provider/register` FQID.",
                'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
            )
        key = _require_str(entry, "key", "[[accept]]")
        scope_key = (parts[0], parts[1], key)
        if scope_key in seen_keys:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups duplicate accept key {key!r} under {register_fqid}.",
                "Accept each auto family once per register.",
            )
        seen_keys.add(scope_key)
        label = _require_opt_str(entry, "label", f"accept {key!r}")
        axis = _require_opt_str(entry, "axis", f"accept {key!r}")
        raw_exclude = entry.get("exclude", [])
        if not isinstance(raw_exclude, list) or not all(
            isinstance(e, str) and e.strip() for e in raw_exclude
        ):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups accept {key!r} `exclude` must be a list of "
                f"non-empty strings, got {raw_exclude!r}.",
                'Give `exclude = ["<slug>", …]` or omit it.',
            )
        out.append(
            Accept(
                provider=parts[0],
                register=parts[1],
                key=key,
                label=label,
                axis=axis,
                exclude=tuple(e.strip() for e in raw_exclude),
            )
        )
    return tuple(out)


def resolve_accept(
    accept: Accept, auto_by_scope: dict[tuple[str, str, str], CuratedGroup]
) -> CuratedGroup:
    """Resolve an `[[accept]]` against the loaded auto families → a
    `CuratedGroup` the curated-apply pass materializes. The auto family must
    exist; `label`/`axis` fall through to the auto family's when the accept
    leaves them unset; `exclude` drops members (every excluded slug must be a
    real member, else it's a stale exclude); >= 2 members must remain. Every
    failure is EXIT_CONFIG with actionable remediation — accept-list drift is
    fixed, not silently dropped."""
    scope = (accept.provider, accept.register, accept.key)
    auto = auto_by_scope.get(scope)
    if auto is None:
        raise curation_error(
            "concept_groups_unresolved",
            f"concept_groups accept {accept.key!r} ({accept.provider}/"
            f"{accept.register}) references an auto family not in "
            "concept_groups.auto.toml.",
            "Regenerate concept_groups.auto.toml with `reg-meta-build "
            "concept-group-candidates`, or fix the accept's register/key.",
        )
    if accept.exclude:
        member_slugs = {m.variable for m in auto.members}
        stale = [slug for slug in accept.exclude if slug not in member_slugs]
        if stale:
            raise curation_error(
                "concept_groups_unresolved",
                f"concept_groups accept {accept.key!r} excludes slug(s) {stale} "
                "that are not members of the auto family.",
                "Drop the stale `exclude` slug(s) or regenerate "
                "concept_groups.auto.toml.",
            )
    members = tuple(m for m in auto.members if m.variable not in accept.exclude)
    if len(members) < 2:
        raise curation_error(
            "concept_groups_unresolved",
            f"concept_groups accept {accept.key!r} resolves to {len(members)} "
            "member(s) after `exclude`; a group needs >= 2.",
            "Exclude fewer members, or remove the accept entirely.",
        )
    return CuratedGroup(
        provider=auto.provider,
        register=auto.register,
        key=auto.key,
        label=accept.label or auto.label,
        axis=accept.axis or auto.axis,
        members=members,
        origin="accept",
    )


def load_classification_groups(path: Path | None) -> tuple[ClassificationGroup, ...]:
    """Parse the curated `[[classification_group]]` umbrella tables (#516): a
    single-axis fold over genuinely-distinct classification dimensions (the SUN
    group over niva/inriktning/grupp). Empty when no file (synthetic builds,
    wheel installs) or no `[[classification_group]]` tables.

    Load-time validation (all EXIT_CONFIG, actionable): `key`/`label`/`axis`
    non-empty strings; `members` a non-empty array of tables, each setting
    non-empty `classification` (slug) / `value` / `label`; member slugs unique;
    `key` unique; >= 2 members. Slug RESOLUTION (does the classification exist?)
    happens at materialize time against the built DB."""
    entries = load_curation_entries(
        path,
        entry_key="classification_group",
        label="classification-group",
        prefix="concept_groups",
        code_base="concept_groups",
        file_name="concept_groups.toml",
        entry_fields="key / label / axis / members",
        sibling_keys=frozenset({"variable_group", "accept"}),
    )
    out: list[ClassificationGroup] = []
    seen_keys: set[str] = set()
    for entry in entries:
        key = _require_str(entry, "key", "[[classification_group]]")
        label = _require_str(entry, "label", "[[classification_group]]")
        axis = _require_str(entry, "axis", "[[classification_group]]")
        if key in seen_keys:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups duplicate classification_group key {key!r}.",
                "Classification-group keys must be unique.",
            )
        seen_keys.add(key)
        raw_members = entry.get("members", [])
        if not isinstance(raw_members, list) or not raw_members:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups classification_group {key!r} needs a non-empty "
                "`[[classification_group.members]]` array.",
                "List the umbrella's members as "
                "`[[classification_group.members]]` tables.",
            )
        members: list[ClassificationGroupMember] = []
        seen_slugs: set[str] = set()
        for raw in raw_members:
            if not isinstance(raw, dict):
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups classification_group {key!r} member {raw!r} "
                    "must be a table.",
                    "Each member is a `[[classification_group.members]]` table.",
                )
            classification = _require_str(
                raw, "classification", f"classification_group {key!r} member"
            )
            if classification in seen_slugs:
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups classification_group {key!r} references "
                    f"classification {classification!r} twice.",
                    "List each member classification once.",
                )
            seen_slugs.add(classification)
            members.append(
                ClassificationGroupMember(
                    classification=classification,
                    value=_require_str(
                        raw, "value", f"classification_group {key!r} member"
                    ),
                    label=_require_str(
                        raw, "label", f"classification_group {key!r} member"
                    ),
                )
            )
        if len(members) < 2:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups classification_group {key!r} has {len(members)} "
                "member(s); a group needs >= 2.",
                "A single-member umbrella is not a group — add members or remove it.",
            )
        out.append(
            ClassificationGroup(key=key, label=label, axis=axis, members=tuple(members))
        )
    return tuple(out)


# ── derivation passes ───────────────────────────────────────────────────────


def _insert_group(
    conn: sqlite3.Connection,
    *,
    kind: str,
    register_id: int | None,
    group_key: str,
    label: str,
    source: str,
    facet_axis: str | None = None,
) -> int:
    # By-key route contract (#640): every group_key — curated / token / edge for
    # `variable`, plus curated `classification` umbrellas — addresses its group via
    # `/catalog/group/<provider>/<register>/<key>`. Starlette decodes `%2F` before
    # matching `{key}` and the SPA helper splits on `/`, so a key must be a single
    # URL-path-safe segment. Validate at this single insert seam. This is a
    # path-safe CHARACTER check (`_is_path_safe_key`), NOT `is_slug`: the candidate
    # generator emits valid trailing-hyphen keys (e.g. `artal-person-`) that
    # `is_slug` would over-reject, breaking the `[[accept]]` by-reference workflow.
    if not _is_path_safe_key(group_key):
        raise curation_error(
            "concept_group_key_not_path_safe",
            f"concept_group {kind!r} key {group_key!r} (register_id "
            f"{register_id}) is not a URL-path-safe key.",
            "A concept-group key must use only URL-path-safe characters "
            "(lowercase letters, digits, and `-._~`) and not be `.`/`..` — it "
            "addresses the group via `/catalog/group/<provider>/<register>/<key>` "
            "(Starlette decodes `%2F` before matching, the SPA splits on `/`), so "
            "a key with `/`, `:`, spaces, uppercase, etc. is unreachable. Pick a "
            "path-safe key.",
        )
    cur = conn.execute(
        "INSERT INTO concept_group "
        "(kind, register_id, group_key, label, source, facet_axis) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (kind, register_id, group_key, label, source, facet_axis),
    )
    group_id = cur.lastrowid
    assert group_id is not None
    return group_id


def _derive_edge_groups(
    conn: sqlite3.Connection,
    edge_siblings: Iterable[tuple[int, int]],
    exclude_variable_ids: set[int],
) -> int:
    """Dimension 0: one group per connected component of within-register
    split-sibling pairs. `edge_siblings` is the IN-BUILD `same_def` subset the
    triage minted (`(variable_id, variable_id)` pairs) — NOT a table round-trip;
    the foldable edges are no longer persisted to `variable_related_to` (#591).

    Only a pair whose BOTH endpoints are slugged (`meta`) AND share a register is
    unioned — mirroring the old table query's `WHERE slug IS NOT NULL` join and
    `va.register_id = vb.register_id` guard (cross-register pairs don't fold; the
    A2.2 split is register-local, but a future curated cross-register edge must
    not mint an unrepresentable group).

    Curated precedence (#591, unblocks #488): `exclude_variable_ids` are the
    variable_ids the curated/accept pass has already claimed. An edge touching an
    excluded endpoint doesn't connect survivors — it is skipped BEFORE the union,
    so excluded vids never enter the DisjointSet. A component is therefore a
    connected component of the same_def edges among NON-excluded variables, and
    only one with >= 2 members mints a group (key/label/register computed from the
    members). A component with < 2 members mints nothing — the curated
    `[[variable_group]]` claims those FQIDs instead. Skipping excluded endpoints
    pre-union (rather than subtracting post-union) is what keeps a BRIDGE
    exclusion correct: if a claimed member is the only vertex joining two cliques,
    the survivors on either side stay disconnected (no surviving same_def path
    folds them)."""
    # Full slugged-variable scan rather than an IN(...) over the involved ids —
    # bounded (~50k rows on the real corpus) and immune to SQLite's host-
    # parameter cap. Only slugged variables ever participated (the old table
    # query joined on populated slugs), so an unslugged endpoint drops out.
    meta = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT variable_id, register_id, slug, name FROM variable "
            "WHERE slug IS NOT NULL"
        )
    }
    ds: DisjointSet[int] = DisjointSet()
    for a, b in edge_siblings:
        if a not in meta or b not in meta or meta[a][0] != meta[b][0]:
            continue
        # Skip edges touching a curated/accepted endpoint BEFORE union: an
        # excluded vid never enters the DisjointSet, so a bridge member's removal
        # genuinely disconnects the survivors on either side (no folded {a, b}).
        if a in exclude_variable_ids or b in exclude_variable_ids:
            continue
        ds.add(a)
        ds.add(b)
        ds.union(a, b)
    components = ds.components()
    if not components:
        return 0

    # Deterministic order: by (register_id, min member slug). Key = min member
    # slug (components are disjoint, so it's scope-unique); label = the
    # min-slug member's name (the shared name on 2,191/2,193 real components),
    # falling back to the key itself. Excluded vids never entered the DisjointSet,
    # so every component member is already a survivor; the < 2 guard stays
    # defensive — a component should always be >= 2, but a degenerate one mints
    # nothing.
    prepared = []
    for member_ids in components.values():
        members = sorted(member_ids, key=lambda v: meta[v][1])
        if len(members) < 2:
            continue
        register_id = meta[members[0]][0]
        key = meta[members[0]][1]
        label = meta[members[0]][2] or key
        prepared.append((register_id, key, label, members))
    for register_id, key, label, members in sorted(
        prepared, key=lambda p: (p[0], p[1])
    ):
        group_id = _insert_group(
            conn,
            kind="variable",
            register_id=register_id,
            group_key=key,
            label=label,
            source="edge",
        )
        conn.executemany(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            [(vid, group_id) for vid in members],
        )
    return len(prepared)


def _common_prefix(strings: list[str]) -> str:
    prefix = strings[0]
    for s in strings[1:]:
        while not s.startswith(prefix):
            prefix = prefix[:-1]
    return prefix


def _trim_label(prefix: str) -> str:
    """Common label prefix → group label: trim trailing whitespace/punctuation
    and a dangling Swedish connector "i" ("Löne- eller företagarinkomst i
    {månad}…" → "Löne- eller företagarinkomst")."""
    trimmed = prefix.strip().rstrip(",;:").removesuffix(" i")
    return trimmed.strip().rstrip(",;:")


def _evaluate_month_fold(
    members: list[tuple[int, str, int, str | None, str]],
) -> str | None:
    """Decide whether one raw-stem month subgroup would FOLD and, if so, return its
    group label; otherwise `None`. Single source of truth for "would this month
    subgroup fold", applied in TWO places so they can't drift (Codex P2 #646):

    1. the trim-collision check — a raw-stem subgroup counts as a competing month
       family only when it would actually fold (not merely meet the sibling floor); and
    2. the emit path — the winning subgroup reuses the returned label directly.

    Gates, in order (the pre-emit guards `_derive_month_groups` applies):
    - `_MIN_MONTH_SIBLINGS` DISTINCT-month floor (member tuple index 0 is the month);
    - NULL-name skip — any NULL member name means no labels to agree on (conservative
      non-fold);
    - label-prefix floor — the trimmed common name prefix must be >= `_MIN_LABEL_PREFIX`
      chars. The existing-key collision guard is a KEY check, NOT a foldability gate,
      so it is NOT applied here (it stays on the winning fold in the caller)."""
    if len({m[0] for m in members}) < _MIN_MONTH_SIBLINGS:
        return None
    names = [m[3] for m in members]
    if any(n is None for n in names):
        return None
    label = _trim_label(_common_prefix([n for n in names if n is not None]))
    if len(label) < _MIN_LABEL_PREFIX:
        return None
    return label


def _derive_month_groups(
    conn: sqlite3.Connection,
    warn: Callable[[str], None],
    reserved_keys: frozenset[tuple[int, str]] = frozenset(),
) -> int:
    """Dimension 1 (variables): fold month-suffixed slug families. Candidates
    are ungrouped slugged variables whose slug ends in a month token; a
    (register, stem) family folds only past BOTH guards (>= 3 distinct months
    AND a usable shared label prefix). The group's `facet_axis` is `'month'`;
    each member carries its zero-padded month `facet_value`/`facet_label` inline.
    A family dropped on a group-key collision is reported via `warn` (never
    silent).

    Key-collision guard spans BOTH the already-inserted variable groups (the edge
    pass ran first) AND the PENDING curated/accepted variable-group keys
    (`reserved_keys`, `(register_id, group_key)`) that `_apply_curated_groups` will
    insert LATER (#651). The curated/accept pass runs AFTER this one, so without the
    reservation a trimmed month key (`ink`) that collides with a pending curated key
    would be inserted here and then crash the curated insert on `idx_concept_group_key`
    — the pre-trim key `ink-` would not have collided. Reserving the pending keys turns
    that into the same cosmetic skip-and-warn an already-inserted collision triggers.

    The stem (`slug` minus its month token) doubles as the group's URL key
    (`/catalog/group/<p>/<r>/<key>`), so a trailing hyphen left by the token strip
    (`inkomst-jan` → `inkomst-`) is trimmed to a clean slug (#645). An empty/hyphen-
    only stem is degenerate and skipped (no empty key). The trim can fuse two raw
    stems into one key (`ink-jan…` raw `ink-` + `inkjan…` raw `ink` both → key
    `ink`); the collision skip-and-warn fires only when >= 2 of the colliding raw
    stems would INDEPENDENTLY FOLD — `_evaluate_month_fold` applies ALL the pre-emit
    guards (sibling floor AND no NULL names AND the label-prefix floor), the same
    predicate the emit path uses, so a peer trio with NULL/short/disagreeing labels
    (which would never fold) does NOT count as a competing family and cannot suppress
    a valid fold (Codex P2 #646). A coincidental singleton (`inkjan` alone, raw `ink`)
    or a non-folding NULL-name peer sharing the key with a real family
    (`ink-jan/feb/mars`, raw `ink-`) does NOT suppress the fold — the real family
    folds and the noise is dropped (#645)."""
    rows = conn.execute(
        "SELECT v.variable_id, v.register_id, v.slug, v.name FROM variable v "
        "WHERE v.slug IS NOT NULL AND NOT EXISTS "
        "  (SELECT 1 FROM concept_group_variable m "
        "   WHERE m.variable_id = v.variable_id) "
        "ORDER BY v.register_id, v.slug"
    ).fetchall()
    # (register_id, key_stem) → [(month, slug, variable_id, name, raw_stem)].
    # `key_stem` is the trailing-hyphen-trimmed URL key; `raw_stem` (untrimmed) is
    # carried so a trim that collapses two distinct stems into one key is caught.
    candidates: dict[tuple[int, str], list[tuple[int, str, int, str | None, str]]] = {}
    for variable_id, register_id, slug, name in rows:
        for token, month in _MONTH_TOKENS.items():
            if slug.endswith(token) and len(slug) > len(token):
                raw_stem = slug[: -len(token)]
                key_stem = raw_stem.rstrip("-")
                if not key_stem:
                    break  # hyphen-only/empty stem — not a usable URL key
                candidates.setdefault((register_id, key_stem), []).append(
                    (month, slug, variable_id, name, raw_stem)
                )
                break  # no month token is a suffix of another — single match
    # Already-inserted variable-group keys (edge pass) UNION the pending curated/
    # accept keys the later `_apply_curated_groups` will claim (#651) — a month family
    # whose trimmed key collides with either is skip-and-warned, never inserted then
    # crashed on the unique index.
    existing_keys = {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT register_id, group_key FROM concept_group WHERE kind = 'variable'"
        )
    } | set(reserved_keys)
    n_groups = 0
    for (register_id, stem), bucket in sorted(candidates.items()):
        # Trim-collision refinement (#645): a clean `stem` reached by `.rstrip("-")`
        # can fuse pre-trim stems into one bucket (`ink-jan…` raw `ink-` +
        # `inkjan…` raw `ink` both → key `ink`). Group by RAW stem (member tuple
        # index 4) and ask which raw-stem subgroups would ACTUALLY FOLD —
        # `_evaluate_month_fold` applies the FULL pre-emit gate set (month-sibling
        # floor AND no NULL names AND the label-prefix floor), the same predicate the
        # emit path uses. A peer trio that meets the sibling floor but would fail the
        # NULL/label gate never folds, so it must NOT count as a competing family
        # (Codex P2 #646): only genuinely-folding raw stems do. Only when >= 2 raw
        # stems each fold is it a GENUINE two-family collision: skip-and-warn (never
        # silently merge two families onto one key). When exactly ONE folds, the rest
        # are noise (coincidental singletons, NULL/short-label peers) sharing the
        # trimmed key — KEEP the real month fold and ignore the noise (#645). A
        # homogeneous bucket (the common case) has one raw stem and folds with no
        # behavior change.
        by_raw_stem: dict[str, list[tuple[int, str, int, str | None, str]]] = {}
        for member in bucket:
            by_raw_stem.setdefault(member[4], []).append(member)
        qualifying = [
            (sub, label)
            for sub in by_raw_stem.values()
            if (label := _evaluate_month_fold(sub)) is not None
        ]
        if len(qualifying) > 1:
            warn(
                f"  WARN concept-groups: month family {stem!r} "
                f"(register_id {register_id}, {len(bucket)} variables) NOT "
                "folded — a trailing-hyphen trim collapsed two distinct stems "
                "onto this key"
            )
            continue
        if not qualifying:
            continue
        # The single folding raw-stem subgroup is the real month family; reuse its
        # already-computed label rather than re-running the agreement/NULL gates. Noise
        # under the trimmed key is dropped.
        members, label = qualifying[0]
        if (register_id, stem) in existing_keys:
            # The key is already claimed — by an edge group inserted earlier (its
            # min member slug equals the stem) OR by a pending curated/accept group
            # `_apply_curated_groups` will insert later (`reserved_keys`, #651).
            # Cosmetic collision on a presentation key — skip the fold rather than
            # fail the build (an unreserved month insert would crash the later
            # curated insert on `idx_concept_group_key`), but say so: the family then
            # renders as ~12 flat near-identical rows (the very symptom #303 fixes)
            # and the corpus floors only count totals, so without this line the loss
            # is invisible to the maintainer.
            warn(
                f"  WARN concept-groups: month family {stem!r} "
                f"(register_id {register_id}, {len(members)} variables) NOT "
                "folded — its stem collides with an existing group key"
            )
            continue
        group_id = _insert_group(
            conn,
            kind="variable",
            register_id=register_id,
            group_key=stem,
            label=label,
            source="token",
            facet_axis="month",
        )
        ordered = sorted(members)
        conn.executemany(
            "INSERT INTO concept_group_variable "
            "(variable_id, group_id, facet_value, facet_label) VALUES (?, ?, ?, ?)",
            [
                (vid, group_id, f"{month:02d}", _MONTH_LABELS[month])
                for month, _, vid, _, _ in ordered
            ],
        )
        n_groups += 1
    return n_groups


def classification_slug_stem(slug: str | None) -> str | None:
    """The year-tail-stripped classification slug — the vintage-FAMILY key.

    Canonical stem rule (single source of truth, mirrored by
    `derive_classification_succession`'s family bucketing AND the #494
    vintage-reclaim stem guard in `classifications.link_value_set_classifications`):
    strip a trailing 4-digit vintage year ONLY when the remaining stem does not
    itself end in a digit (a digit-ending stem means the tail splits a longer
    number, not a vintage year). A non-vintage slug is its OWN stem.

    Examples: `sun-niva2000`/`sun-niva2020` → `sun-niva`; `sni2002`/`sni2007` →
    `sni`; `isced` → `isced` (no year tail); `sun1996` → `sun` (a curated split
    root collapses to the bare stem — but its siblings `sun-niva*` etc. carry the
    `-niva`/`-inriktning`/`-grupp` discriminator, so they get DISTINCT stems).

    NULL-safe so it can be a SQLite UDF over `classification.slug` (NULL under
    `--skip-slugs`, where the reclaim is inert anyway): a NULL slug → NULL stem.
    """
    if slug is None:
        return None
    tail = slug[-4:]
    if len(slug) < 5 or not tail.isdigit():
        return slug
    year = int(tail)
    stem = slug[:-4]
    # A digit-ending stem means the tail splits a longer number — not a vintage
    # year; an out-of-range year is likewise not a vintage tail.
    if year not in _VINTAGE_YEARS or stem[-1].isdigit():
        return slug
    return stem


def derive_classification_succession(conn: sqlite3.Connection) -> int:
    """Classification EDITION succession (#571): detect 4-digit vintage-year slug
    families (lkf1980…lkf2026, sni2002/sni2007, agarkat2000/2020, …) and emit a
    temporal succession chain into `classification_replaced_by`, NOT a
    presentation concept group — editions of one classification are a
    succession, not a parallel facet-picker.

    Detection is the same guarded slug-tail rule the old vintage-group fold used:
    a 4-digit year tail on a non-digit-ending stem, >= `_MIN_VINTAGE_SIBLINGS`
    editions per stem, every member's name contains its vintage year, and the
    year-stripped names all agree. Past the guards, each stem's editions are
    sorted by year and ADJACENT pairs become edges: for [y0<y1<…<yn], edges
    (slug_y0→slug_y1), (slug_y1→slug_y2), …, with `effective_year` the
    successor's year. Adjacent-chain (not predecessor→latest): succession is a
    linear hand-off between consecutive vintages, so a query for "what replaced
    ssyk1996?" must yield ssyk2012 directly, and walking the chain recovers the
    full lineage — a star to the latest edition would lose the intermediate
    hops.

    Returns the edge count (e.g. lkf with 47 editions → 46 edges)."""
    rows = conn.execute(
        "SELECT slug, name FROM classification WHERE slug IS NOT NULL ORDER BY slug"
    ).fetchall()
    # stem → [(year, slug, name)]
    families: dict[str, list[tuple[int, str, str]]] = {}
    for slug, name in rows:
        stem = classification_slug_stem(slug)
        # `slug` is non-NULL (WHERE clause), so a None stem can't occur; the
        # `stem == slug` case means no vintage tail was stripped → not an edition.
        if stem is None or stem == slug:
            continue
        families.setdefault(stem, []).append((int(slug[-4:]), slug, name))
    n_edges = 0
    for stem in sorted(families):
        editions = sorted(families[stem])
        if len(editions) < _MIN_VINTAGE_SIBLINGS:
            continue
        stripped_names: set[str] = set()
        ok = True
        for year, _slug, name in editions:
            if not name or str(year) not in name:
                ok = False
                break
            stripped_names.add(" ".join(name.replace(str(year), "", 1).split()))
        if not ok or len(stripped_names) != 1 or not next(iter(stripped_names)):
            continue
        edges = [
            (pred[1], succ[1], succ[0])
            for pred, succ in zip(editions, editions[1:], strict=False)
        ]
        conn.executemany(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES (?, ?, ?, 'derived:vintage_chain')",
            edges,
        )
        n_edges += len(edges)
    return n_edges


def _apply_curated_groups(
    conn: sqlite3.Connection, groups: tuple[CuratedGroup, ...]
) -> int:
    """Dimension 2: curated families (`[[variable_group]]`) and `[[accept]]`-resolved
    auto families (`origin="accept"`). Each `variable` member attaches one
    ungrouped variable under the family's single `axis` (stored on
    `concept_group.facet_axis`), carrying its `value`/`label` inline on
    `concept_group_variable`. Every dangling reference fails the build
    (EXIT_CONFIG) — curation drift must be fixed, not silently dropped.

    Remediations branch on `g.origin`: a hand-authored family points the
    maintainer at `concept_groups.toml`; an accepted one points at the `[[accept]]`
    / generated `concept_groups.auto.toml`, since its key/register/members come
    from the catalog (the maintainer can't hand-pick a different key)."""
    regen = (
        "reg-meta-build --db <built-db-dir> concept-group-candidates "
        "--output-toml reg_meta_build/concept_groups.auto.toml"
    )
    n_groups = 0
    for g in groups:
        is_accept = g.origin == "accept"
        ctx = (
            f"[[accept]] {g.key!r} ({g.provider}/{g.register})"
            if is_accept
            else f"[[variable_group]] {g.key!r} ({g.provider}/{g.register})"
        )
        reg = conn.execute(
            "SELECT r.register_id FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ?",
            (g.provider, g.register),
        ).fetchone()
        if reg is None:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: register does not resolve.",
                f"Regenerate concept_groups.auto.toml (`{regen}`) or drop the "
                "`[[accept]]` in reg_meta_build/concept_groups.toml."
                if is_accept
                else "Fix the `register` FQID in reg_meta_build/concept_groups.toml.",
            )
        register_id = reg[0]
        try:
            group_id = _insert_group(
                conn,
                kind="variable",
                register_id=register_id,
                group_key=g.key,
                label=g.label,
                source="curated",
                facet_axis=g.axis,
            )
        except sqlite3.IntegrityError as exc:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: key collides with a derived group in the same register."
                if not is_accept
                else f"{ctx}: the auto family {g.provider}/{g.register}/{g.key!r} "
                "collides with an edge/token group claimed since "
                "concept_groups.auto.toml was generated.",
                f"Regenerate concept_groups.auto.toml (`{regen}`) or drop the "
                "`[[accept]]`."
                if is_accept
                else "Pick a curated `key` that no edge/token group already uses.",
            ) from exc
        n_members = 0
        for m in g.members:
            var = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
                (register_id, m.variable),
            ).fetchone()
            if var is None:
                raise curation_error(
                    "concept_groups_unresolved",
                    f"{ctx}: member variable {m.variable!r} does not resolve "
                    "in that register.",
                    f"Regenerate concept_groups.auto.toml (`{regen}`) or drop "
                    "the `[[accept]]`."
                    if is_accept
                    else "Fix the member's `variable` slug in "
                    "reg_meta_build/concept_groups.toml.",
                )
            try:
                conn.execute(
                    "INSERT INTO concept_group_variable "
                    "(variable_id, group_id, facet_value, facet_label) "
                    "VALUES (?, ?, ?, ?)",
                    (var[0], group_id, m.value, m.label),
                )
            except sqlite3.IntegrityError as exc:
                raise curation_error(
                    "concept_groups_unresolved",
                    f"{ctx}: member variable {m.variable!r} already belongs "
                    "to an edge/token group."
                    if not is_accept
                    else f"{ctx}: member variable {m.variable!r} was claimed by "
                    "an edge/token group since concept_groups.auto.toml was "
                    "generated.",
                    f"Regenerate concept_groups.auto.toml (`{regen}`) or "
                    "`exclude` this member in the `[[accept]]`."
                    if is_accept
                    else "`exclude` the conflicting member or pick a different "
                    "variable in reg_meta_build/concept_groups.toml.",
                ) from exc
            n_members += 1
        if n_members < 2:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: resolves to {n_members} member variable(s); a group "
                "needs >= 2.",
                "A single-member family is not a group — remove it or add members.",
            )
        n_groups += 1
    return n_groups


def _apply_curated_classification_groups(
    conn: sqlite3.Connection, groups: tuple[ClassificationGroup, ...]
) -> int:
    """Curated classification umbrella groups (#516): the `kind='classification'`
    dual of `_apply_curated_groups`. Each group inserts a `concept_group` row
    (register_id NULL — classifications are catalog-global; `facet_axis` = the
    group's single axis), then resolves every member's `classification` slug
    globally and wires it as a `concept_group_classification` row carrying the
    member's facet `value`/`label`. Every dangling slug or already-grouped
    classification fails the build (EXIT_CONFIG) — curation drift is fixed, not
    silently dropped. Mirrors `_apply_curated_groups`' error style."""
    n_groups = 0
    for g in groups:
        ctx = f"[[classification_group]] {g.key!r}"
        group_id = _insert_group(
            conn,
            kind="classification",
            register_id=None,
            group_key=g.key,
            label=g.label,
            source="curated",
            facet_axis=g.axis,
        )
        n_members = 0
        for m in g.members:
            row = conn.execute(
                "SELECT id FROM classification WHERE slug = ?", (m.classification,)
            ).fetchone()
            if row is None:
                raise curation_error(
                    "concept_groups_unresolved",
                    f"{ctx}: member classification {m.classification!r} does not "
                    "resolve (no classification carries that slug).",
                    "Fix the member's `classification` slug in "
                    "reg_meta_build/concept_groups.toml.",
                )
            try:
                conn.execute(
                    "INSERT INTO concept_group_classification "
                    "(classification_id, group_id, facet_value, facet_label) "
                    "VALUES (?, ?, ?, ?)",
                    (row[0], group_id, m.value, m.label),
                )
            except sqlite3.IntegrityError as exc:
                raise curation_error(
                    "concept_groups_unresolved",
                    f"{ctx}: member classification {m.classification!r} already "
                    "belongs to a concept group.",
                    "A classification joins at most one group — remove the "
                    "duplicate member in reg_meta_build/concept_groups.toml.",
                ) from exc
            n_members += 1
        if n_members < 2:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: resolves to {n_members} member classification(s); a group "
                "needs >= 2.",
                "A single-member umbrella is not a group — add members or remove it.",
            )
        n_groups += 1
    return n_groups


def _resolve_curated_member_ids(
    conn: sqlite3.Connection, groups: tuple[CuratedGroup, ...]
) -> set[int]:
    """The variable_ids the curated/accept variable groups will claim — the edge
    pass's `exclude_variable_ids` (#591 curated precedence). LENIENT: a member
    whose register or variable slug doesn't resolve is skipped here (it carries no
    edge FQID to exclude); the strict EXIT_CONFIG on a dangling reference still
    fires in `_apply_curated_groups`, the authoritative pass."""
    out: set[int] = set()
    for g in groups:
        reg = conn.execute(
            "SELECT r.register_id FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ?",
            (g.provider, g.register),
        ).fetchone()
        if reg is None:
            continue
        for m in g.members:
            var = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
                (reg[0], m.variable),
            ).fetchone()
            if var is not None:
                out.add(var[0])
    return out


def _resolve_curated_group_keys(
    conn: sqlite3.Connection, groups: tuple[CuratedGroup, ...]
) -> set[tuple[int, str]]:
    """The `(register_id, key)` of every curated/accept VARIABLE group `_apply_
    curated_groups` will insert — the month pass's `reserved_keys` (#651). These keys
    are claimed LATER (curated/accept runs after the month pass), so reserving them
    lets the month pass skip-and-warn a trimmed-key collision instead of inserting a
    month group that then crashes the curated insert on `idx_concept_group_key`.

    LENIENT register resolution (mirrors `_resolve_curated_member_ids`): a group whose
    register doesn't resolve carries no key to reserve here — the strict EXIT_CONFIG on
    a dangling reference still fires in `_apply_curated_groups`, the authoritative pass.
    Classification umbrella groups are NOT included: they are `kind='classification'`
    with `register_id IS NULL`, a disjoint key space from the register-scoped variable
    month groups, so they can't collide."""
    out: set[tuple[int, str]] = set()
    for g in groups:
        reg = conn.execute(
            "SELECT r.register_id FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ?",
            (g.provider, g.register),
        ).fetchone()
        if reg is not None:
            out.add((reg[0], g.key))
    return out


def materialize_concept_groups(
    conn: sqlite3.Connection,
    curated: tuple[CuratedGroup, ...] = (),
    *,
    auto: tuple[CuratedGroup, ...] = (),
    accepts: tuple[Accept, ...] = (),
    classification_groups: tuple[ClassificationGroup, ...] = (),
    edge_siblings: Iterable[tuple[int, int]] = (),
    providers: frozenset[str] = frozenset(),
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Derive the concept-group tables (#303). Ordering contract: runs after
    `populate_variable_slugs` (the edge pass resolves slug-anchored siblings) and
    after `populate_slugs` / `populate_classifications` (classification slugs +
    rows). `providers` gates curated/accept entries to the providers in this build
    (mirrors `populate_classifications`' provider gate, so a `--providers=sos`
    build doesn't fail on an scb family).

    Dimension 0 (`edge`) folds the IN-BUILD `same_def` split-sibling pairs
    (`edge_siblings`, `(variable_id, variable_id)`), NOT a `variable_related_to`
    round-trip — those foldable edges are no longer persisted (#591). The CURATED
    pass takes PRECEDENCE over the edge fold (unblocks #488): `custom` + `accepted`
    are resolved FIRST, the variable_ids their members claim become
    `exclude_variable_ids`, and `_derive_edge_groups` subtracts them from every
    component before minting (a component left with < 2 survivors mints no edge
    group). The strict dangling-reference check stays in `_apply_curated_groups`;
    the exclusion resolution here is lenient (an unresolvable member just carries
    no FQID to exclude).

    Dimension 2 (#496) is OPT-IN over the generated `concept_groups.auto.toml`:
    `auto` is the machine-owned candidate catalog (`load_concept_groups` of the
    auto file), but an auto family folds ONLY when an `[[accept]]` in
    `concept_groups.toml` references it (`accepts`). Each gated accept resolves
    against `auto` to a `CuratedGroup` (label/axis overrides + `exclude`
    applied), then the resolved-accepted families and the custom
    `[[variable_group]]` families share the existing `_apply_curated_groups`
    path (accepted members are all `variable=` attachments — the candidate
    generator guarantees they're ungrouped + non-colliding). Unaccepted auto
    families are NEVER materialized.

    Classification VINTAGE families no longer fold here (#571) — their editions
    materialize as succession edges via `derive_classification_succession`
    (called separately in the build, beside the other `*_replaced_by` passes).
    `concept_group_classification` stays in the schema (empty of DERIVED rows)
    for the curated umbrella groups (#516): `classification_groups` is the
    maintainer's `[[classification_group]]` TOML (the SUN umbrella over its
    distinct dimensions), materialized via `_apply_curated_classification_groups`
    after the variable curated pass. Classifications are catalog-GLOBAL, so these
    are NOT provider-gated (unlike the variable curated/accept entries)."""
    auto_by_scope = {(g.provider, g.register, g.key): g for g in auto}
    accepted = tuple(
        resolve_accept(a, auto_by_scope) for a in accepts if a.provider in providers
    )
    custom = tuple(g for g in curated if g.provider in providers)
    # Curated precedence (#591): resolve the curated/accept members BEFORE the
    # edge fold so any FQID they claim is excluded from the edge components.
    exclude_variable_ids = _resolve_curated_member_ids(conn, custom + accepted)
    # The curated/accept variable-group keys land LATER (`_apply_curated_groups` runs
    # after the month pass), so reserve them now: a month family whose trailing-hyphen-
    # trimmed key collides with a pending curated key is skip-and-warned instead of
    # inserted-then-crashed on `idx_concept_group_key` (#651).
    reserved_keys = frozenset(_resolve_curated_group_keys(conn, custom + accepted))
    _derive_edge_groups(conn, edge_siblings, exclude_variable_ids)
    _derive_month_groups(conn, warn or (lambda _msg: None), reserved_keys)
    _apply_curated_groups(conn, custom + accepted)
    _apply_curated_classification_groups(conn, classification_groups)
    # Count the authoritative shipped rows from the final table after all
    # passes, rather than threading per-pass tallies (the curated passes return
    # none) — one query over the materialized state is the single source of truth.
    by_bucket = {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            "SELECT source, kind, COUNT(*) FROM concept_group GROUP BY source, kind"
        )
    }
    counts = {
        "edge_groups": by_bucket.get(("edge", "variable"), 0),
        "month_groups": by_bucket.get(("token", "variable"), 0),
        "curated_groups": by_bucket.get(("curated", "variable"), 0),
        "classification_curated_groups": by_bucket.get(
            ("curated", "classification"), 0
        ),
    }
    counts["grouped_variables"] = conn.execute(
        "SELECT COUNT(*) FROM concept_group_variable"
    ).fetchone()[0]
    counts["grouped_classifications"] = conn.execute(
        "SELECT COUNT(*) FROM concept_group_classification"
    ).fetchone()[0]
    return counts
