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
   ``same_definition_different_column`` sibling edges. Ground truth: the A2.2
   split machinery minted these edges between the delivery columns of ONE SCB
   variable definition, so folding them back into one browse row is
   zero-inference (e.g. ureg's sun2000inr/sun2020inr coding succession; on the
   real corpus ~2,200 components covering ~8,200 variables, 2,191/2,193
   sharing a single name).
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
     absorbs token groups under an extra facet axis (the LISA agi{1,2,3} rank
     facet → one month × rank matrix) or attaches single variables.
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
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

from ._components import DisjointSet
from ._curation import curation_error, load_curation_entries, require_str

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

_EDGE_RELATION_KIND = "same_definition_different_column"


# ── curated TOML ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedMember:
    """One member of a curated family: EITHER a derived token group to absorb
    (`group` = its stem key; absorbed variables keep their month facets and
    gain the family's facet) OR a single variable slug. `value`/`label` are
    this member's facet on the family's `axis`."""

    group: str | None
    variable: str | None
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
    each member sets exactly one of `group`/`variable` plus `value`/`label`;
    keys are unique. Reference RESOLUTION (register/group/variable exist)
    happens at materialize time against the built DB, not here."""
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
        seen_refs: set[tuple[str, str]] = set()
        for raw in raw_members:
            if not isinstance(raw, dict):
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} member {raw!r} must be a table.",
                    "Each member is a `[[variable_group.members]]` table.",
                )
            group_ref = raw.get("group")
            variable_ref = raw.get("variable")
            if (group_ref is None) == (variable_ref is None):
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} member {raw!r} must set exactly "
                    "one of `group` / `variable`.",
                    'Reference a derived token group (`group = "<stem>"`) or a '
                    'single variable (`variable = "<slug>"`), not both/neither.',
                )
            ref_field = "group" if group_ref is not None else "variable"
            ref = _require_str(raw, ref_field, f"group {key!r} member")
            if (ref_field, ref) in seen_refs:
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} references {ref_field} {ref!r} "
                    "twice.",
                    "List each member reference once.",
                )
            seen_refs.add((ref_field, ref))
            members.append(
                CuratedMember(
                    group=ref if ref_field == "group" else None,
                    variable=ref if ref_field == "variable" else None,
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
    cur = conn.execute(
        "INSERT INTO concept_group "
        "(kind, register_id, group_key, label, source, facet_axis) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (kind, register_id, group_key, label, source, facet_axis),
    )
    group_id = cur.lastrowid
    assert group_id is not None
    return group_id


def _derive_edge_groups(conn: sqlite3.Connection) -> int:
    """Dimension 0: one group per connected component of within-register
    `same_definition_different_column` edges. The edges are slug-anchored and
    stored both directions; resolve both endpoints back to variable_ids and
    union-find the components. Cross-register edges of this kind don't occur
    (the A2.2 split is register-local), but the WHERE guard keeps a future
    curated cross-register edge from minting an unrepresentable group."""
    rows = conn.execute(
        "SELECT va.variable_id AS a_id, vb.variable_id AS b_id "
        "FROM variable_related_to e "
        "JOIN provider pa ON pa.slug = e.a_provider "
        "JOIN register ra ON ra.provider_id = pa.provider_id "
        "  AND ra.slug = e.a_register "
        "JOIN variable va ON va.register_id = ra.register_id "
        "  AND va.slug = e.a_variable "
        "JOIN provider pb ON pb.slug = e.b_provider "
        "JOIN register rb ON rb.provider_id = pb.provider_id "
        "  AND rb.slug = e.b_register "
        "JOIN variable vb ON vb.register_id = rb.register_id "
        "  AND vb.slug = e.b_variable "
        "WHERE e.relation_kind = ? AND va.register_id = vb.register_id",
        (_EDGE_RELATION_KIND,),
    ).fetchall()
    if not rows:
        return 0

    ds: DisjointSet[int] = DisjointSet()
    for r in rows:
        a, b = r[0], r[1]
        ds.add(a)
        ds.add(b)
        ds.union(a, b)

    # Full slugged-variable scan rather than an IN(...) over the ~8k involved
    # ids — bounded (~50k rows on the real corpus) and immune to SQLite's
    # host-parameter cap.
    meta = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT variable_id, register_id, slug, name FROM variable "
            "WHERE slug IS NOT NULL"
        )
    }
    components = ds.components()

    # Deterministic order: by (register_id, min member slug). Key = min member
    # slug (components are disjoint, so it's scope-unique); label = the
    # min-slug member's name (the shared name on 2,191/2,193 real components),
    # falling back to the key itself.
    prepared = []
    for member_ids in components.values():
        members = sorted(member_ids, key=lambda v: meta[v][1])
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


def _derive_month_groups(conn: sqlite3.Connection, warn: Callable[[str], None]) -> int:
    """Dimension 1 (variables): fold month-suffixed slug families. Candidates
    are ungrouped slugged variables whose slug ends in a month token; a
    (register, stem) family folds only past BOTH guards (>= 3 distinct months
    AND a usable shared label prefix). Members get a `month` facet. A family
    dropped on a group-key collision is reported via `warn` (never silent)."""
    rows = conn.execute(
        "SELECT v.variable_id, v.register_id, v.slug, v.name FROM variable v "
        "WHERE v.slug IS NOT NULL AND NOT EXISTS "
        "  (SELECT 1 FROM concept_group_variable m "
        "   WHERE m.variable_id = v.variable_id) "
        "ORDER BY v.register_id, v.slug"
    ).fetchall()
    # (register_id, stem) → [(month, slug, variable_id, name)]
    candidates: dict[tuple[int, str], list[tuple[int, str, int, str | None]]] = {}
    for variable_id, register_id, slug, name in rows:
        for token, month in _MONTH_TOKENS.items():
            if slug.endswith(token) and len(slug) > len(token):
                stem = slug[: -len(token)]
                candidates.setdefault((register_id, stem), []).append(
                    (month, slug, variable_id, name)
                )
                break  # no month token is a suffix of another — single match
    existing_keys = {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT register_id, group_key FROM concept_group WHERE kind = 'variable'"
        )
    }
    n_groups = 0
    for (register_id, stem), members in sorted(candidates.items()):
        if len({m[0] for m in members}) < _MIN_MONTH_SIBLINGS:
            continue
        names = [m[3] for m in members]
        if any(n is None for n in names):
            continue  # no labels to agree on — conservative skip
        label = _trim_label(_common_prefix([n for n in names if n is not None]))
        if len(label) < _MIN_LABEL_PREFIX:
            continue
        if (register_id, stem) in existing_keys:
            # An edge group already claimed this key (its min member slug
            # equals the stem). Cosmetic collision on a presentation key —
            # skip the fold rather than fail the build, but say so: the family
            # then renders as ~12 flat near-identical rows (the very symptom
            # #303 fixes) and the corpus floors only count totals, so without
            # this line the loss is invisible to the maintainer.
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
        )
        ordered = sorted(members)
        conn.executemany(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            [(vid, group_id) for _, _, vid, _ in ordered],
        )
        conn.executemany(
            "INSERT INTO concept_group_variable_facet (variable_id, axis, value, "
            "label) VALUES (?, 'month', ?, ?)",
            [
                (vid, f"{month:02d}", _MONTH_LABELS[month])
                for month, _, vid, _ in ordered
            ],
        )
        n_groups += 1
    return n_groups


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
        tail = slug[-4:]
        if len(slug) < 5 or not tail.isdigit():
            continue
        year = int(tail)
        stem = slug[:-4]
        # A digit-ending stem means the tail splits a longer number — not a
        # vintage year.
        if year not in _VINTAGE_YEARS or stem[-1].isdigit():
            continue
        families.setdefault(stem, []).append((year, slug, name))
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
    auto families (`origin="accept"`). A `group` member absorbs a derived token
    group (its variables move over, keeping their month facets, and gain the
    family facet; the absorbed group row is deleted); a `variable` member
    attaches one ungrouped variable. Every dangling reference fails the build
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
            if m.group is not None:
                row = conn.execute(
                    "SELECT group_id FROM concept_group WHERE kind = 'variable' "
                    "AND register_id = ? AND group_key = ? AND source = 'token'",
                    (register_id, m.group),
                ).fetchone()
                if row is None:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: member references derived token group "
                        f"{m.group!r}, which this build did not derive (slug "
                        "rename, or the family no longer passes the fold guards).",
                        "Update the member's `group` stem in "
                        "reg_meta_build/concept_groups.toml.",
                    )
                absorbed_id = row[0]
                member_ids = [
                    r[0]
                    for r in conn.execute(
                        "SELECT variable_id FROM concept_group_variable "
                        "WHERE group_id = ? ORDER BY variable_id",
                        (absorbed_id,),
                    )
                ]
                conn.execute(
                    "UPDATE concept_group_variable SET group_id = ? WHERE group_id = ?",
                    (group_id, absorbed_id),
                )
                conn.execute(
                    "DELETE FROM concept_group WHERE group_id = ?", (absorbed_id,)
                )
                try:
                    conn.executemany(
                        "INSERT INTO concept_group_variable_facet "
                        "(variable_id, axis, value, label) VALUES (?, ?, ?, ?)",
                        [(vid, g.axis, m.value, m.label) for vid in member_ids],
                    )
                except sqlite3.IntegrityError as exc:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: axis {g.axis!r} collides with a facet the "
                        f"absorbed group {m.group!r} already assigned.",
                        "Pick a distinct `axis` for the curated family.",
                    ) from exc
                n_members += len(member_ids)
            else:
                var = conn.execute(
                    "SELECT variable_id FROM variable "
                    "WHERE register_id = ? AND slug = ?",
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
                        "INSERT INTO concept_group_variable (variable_id, group_id) "
                        "VALUES (?, ?)",
                        (var[0], group_id),
                    )
                except sqlite3.IntegrityError as exc:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: member variable {m.variable!r} already belongs "
                        "to an edge/token group — absorb that group instead."
                        if not is_accept
                        else f"{ctx}: member variable {m.variable!r} was claimed by "
                        "an edge/token group since concept_groups.auto.toml was "
                        "generated.",
                        f"Regenerate concept_groups.auto.toml (`{regen}`) or "
                        "`exclude` this member in the `[[accept]]`."
                        if is_accept
                        else 'Reference the derived group via `group = "<stem>"`.',
                    ) from exc
                conn.execute(
                    "INSERT INTO concept_group_variable_facet "
                    "(variable_id, axis, value, label) VALUES (?, ?, ?, ?)",
                    (var[0], g.axis, m.value, m.label),
                )
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


def materialize_concept_groups(
    conn: sqlite3.Connection,
    curated: tuple[CuratedGroup, ...] = (),
    *,
    auto: tuple[CuratedGroup, ...] = (),
    accepts: tuple[Accept, ...] = (),
    classification_groups: tuple[ClassificationGroup, ...] = (),
    providers: frozenset[str] = frozenset(),
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Derive the concept-group tables (#303). Ordering contract: runs after
    `populate_variable_slugs` + `_materialize_variable_related_to` (the edge
    pass resolves slug-anchored edges) and after `populate_slugs` /
    `populate_classifications` (classification slugs + rows). `providers`
    gates curated/accept entries to the providers in this build (mirrors
    `populate_classifications`' provider gate, so a `--providers=sos` build
    doesn't fail on an scb family).

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
    _derive_edge_groups(conn)
    _derive_month_groups(conn, warn or (lambda _msg: None))
    auto_by_scope = {(g.provider, g.register, g.key): g for g in auto}
    accepted = tuple(
        resolve_accept(a, auto_by_scope) for a in accepts if a.provider in providers
    )
    custom = tuple(g for g in curated if g.provider in providers)
    _apply_curated_groups(conn, custom + accepted)
    _apply_curated_classification_groups(conn, classification_groups)
    # Recount from the final table — a curated absorb DELETEs its token
    # groups, so per-pass return values would over-report the shipped state.
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
