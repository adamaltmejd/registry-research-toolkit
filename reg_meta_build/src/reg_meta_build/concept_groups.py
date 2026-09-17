"""Concept-group declarations and pure grouping rules.

The common resolver applies explicit groups, checked sibling components and guarded
month families before writing. Classification vintages form succession edges;
curated classification umbrellas remain separate declarations. Offline conversion
resolves accepted machine-generated candidates through ``resolve_accept``.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from itertools import pairwise
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

from ._curation import (
    curation_error,
    load_curation_entries,
    require_fqid,
    require_str,
)

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


# ── curated TOML ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedMember:
    """One member of a curated family at REPRESENTATION grain (#819).
    `delivery_column` is None for a whole-variable member (single-axis families:
    the variable IS the member) and the exact SCB delivery column for a
    representation member (multi-axis families, where one variable holds several
    coordinates). `coords` is this member's per-axis facet assignments as ordered
    `(axis, value, label)` triples — one entry per declared group axis. A
    single-axis member carries one coord (mapped from the legacy `value`/`label`
    TOML); a multi-axis member carries one per axis."""

    variable: str
    delivery_column: str | None
    coords: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True)
class CuratedGroup:
    """One curated family common resolution applies. `axes` is the group's ordered
    named facet axes as `(axis, axis_label)` pairs (ordinal = index); a single-axis
    family carries one, an axis-less umbrella carries zero, and a multi-axis
    family (the iot disposable-income group) N.
    `origin` records how it was authored so `_apply_curated_groups` can tailor its
    EXIT_CONFIG remediations: a hand-authored `[[variable_group]]` (the default)
    points the maintainer at `curation/concept_groups.toml`; an `[[accept]]`-resolved family
    (`resolve_accept` sets `origin="accept"`) points at the `[[accept]]` /
    `concept_groups.auto.toml` instead, since its key/register/members come from the
    generated catalog, not a hand-picked curated key."""

    provider: str
    register: str
    key: str
    label: str
    axes: tuple[tuple[str, str], ...]
    members: tuple[CuratedMember, ...]
    origin: Literal["variable_group", "accept"] = "variable_group"


@dataclass(frozen=True)
class Accept:
    """One `[[accept]]` entry from `curation/concept_groups.toml` (#496): an OPT-IN to
    fold an auto family from `concept_groups.auto.toml` BY REFERENCE. The
    `(provider, register, key)` locates the auto family; `label`/`axis` override
    the auto family's when set; `exclude` drops member slugs (a stem that picked
    up an unrelated column). Resolved to a `CuratedGroup` during offline conversion
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
    slug (catalog-global, e.g. `sun2020-niva`) and its curated short `value`/
    `label` (the picker label, e.g. 'niva'/'Utbildningsnivå'). These stay
    populated even though the umbrella is axis-less — they are the member's own
    label, not a point on a shared group axis."""

    classification: str
    value: str
    label: str


@dataclass(frozen=True)
class ClassificationGroup:
    """One curated `[[classification_group]]` umbrella (#516): a fold over
    genuinely-DISTINCT classifications (NOT vintage editions — those are #571
    succession edges). AXIS-LESS — the members are distinct classifications, not
    points on a shared scale, so `axis` is optional and normally None (zero
    `concept_group_axis` rows, #819; the webapp renders the member-noun as
    "members"). A provided `axis` is still accepted (the loader does not require
    it) and becomes one `concept_group_axis` row. Each member keeps its own short
    `value`/`label` inline regardless. Catalog-scoped (classifications are global),
    so unlike `CuratedGroup` it carries no provider/register."""

    key: str
    label: str
    axis: str | None
    members: tuple[ClassificationGroupMember, ...]


@dataclass(frozen=True)
class CodeLabelPair:
    """One curated code↔label column pair (#923): a coded variable (the `code`
    endpoint, which OWNS a `value_set`) co-delivered with its denormalized label
    column (the `label` endpoint, which owns none) — `partikod`+`partinamn`,
    `kommun`+`kommunnamn`. They are ONE concept, folded into a single register-
    scoped AXIS-LESS `edge` concept group by feeding the pair into the existing
    edge `sibling_edges` channel. Each endpoint is parsed independently as a
    3-segment `provider/register/variable` FQID; the loader does not ASSUME one
    provider/register per pair, but Guard 3 (co-delivery via a shared
    `register_variant`) requires both endpoints in one register, so in practice
    every pair is same-register/provider — this is NOT a cross-register or
    cross-provider folding mechanism. The code/label distinction is NOT stored by
    this change and NO read-side derivation ships here; it is DERIVABLE from
    value_set ownership (the `code` member owns a value_set, the `label` member
    owns none) as a future read-side affordance."""

    code_provider: str
    code_register: str
    code_variable: str
    label_provider: str
    label_register: str
    label_variable: str


_require_str = functools.partial(
    require_str,
    code="concept_groups_invalid",
    prefix="concept_groups",
    file_name="curation/concept_groups.toml",
)

_require_pair_fqid = functools.partial(
    require_fqid,
    code="code_label_pairs_invalid",
    prefix="code_label_pairs",
    entry_table="[[pair]]",
    file_name="curation/concept_groups.toml",
)


def _single_axis(axis: str) -> tuple[tuple[str, str], ...]:
    """The `axes` tuple for a single-axis group whose label IS the axis name
    (#819): the legacy single-axis shape and curated classification umbrellas have
    no separate axis label, so the pair is `(axis, axis)`."""
    return ((axis, axis),)


def _parse_group_axes(
    entry: dict, key: str
) -> tuple[tuple[tuple[str, str], ...], set[str]]:
    """The group's ordered `(axis, axis_label)` pairs + the axis-name set, from
    EITHER the legacy single-axis shape (`axis = "..."` on the group) OR the
    explicit `axes = [...]` shape. Exactly one shape may be present (a group with
    both is curation drift). The single-axis legacy form maps to one axis whose
    label IS the axis name (`axes = ((axis, axis),)`), so every existing
    `[[variable_group]]` and the whole `concept_groups.auto.toml` round-trip
    unchanged. `axes = []` is the explicit axis-less variable umbrella shape."""
    has_axes = entry.get("axes") is not None
    has_axis = entry.get("axis") is not None
    if has_axes and has_axis:
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} sets both `axis` and `axes`.",
            "Use `axis` for a single-axis family OR `axes` for a multi-axis one, "
            "not both.",
        )
    if not has_axes:
        # Legacy single-axis shape: the axis label is the axis name itself.
        axis = _require_str(entry, "axis", f"group {key!r}")
        return _single_axis(axis), {axis}
    raw_axes = entry["axes"]
    if not isinstance(raw_axes, list):
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} `axes` must be an array of "
            "`{ axis, label }` tables, or an empty array for an axis-less umbrella.",
            'List axes as `axes = [{ axis = "enhet", label = "Enhet" }, …]`, '
            "or use `axes = []` for an axis-less variable umbrella.",
        )
    axes: list[tuple[str, str]] = []
    axis_names: set[str] = set()
    for raw in raw_axes:
        if not isinstance(raw, dict):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} axis {raw!r} must be a table.",
                "Each axis is a `{ axis = …, label = … }` table.",
            )
        axis = _require_str(raw, "axis", f"group {key!r} axis")
        axis_label = _require_str(raw, "label", f"group {key!r} axis")
        if axis in axis_names:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} declares axis {axis!r} twice.",
                "List each axis once.",
            )
        axis_names.add(axis)
        axes.append((axis, axis_label))
    return tuple(axes), axis_names


def _parse_member(
    raw: dict, key: str, axes: tuple[tuple[str, str], ...], axis_names: set[str]
) -> CuratedMember:
    """One `[[variable_group.members]]` table → a `CuratedMember`, in EITHER shape:

    - Axis-less: `variable` only → no coords, whole-variable member.
    - Legacy single-axis: `variable` + flat `value`/`label` → one coord on the
      group's single axis, `delivery_column = None` (a whole-variable member).
    - Multi-axis: `variable` + `delivery_column` + `coords = [{ axis, value, label
      }, …]` → one coord per declared axis.

    Validation (EXIT_CONFIG): every coord's `axis` is a declared group axis; every
    declared axis has exactly one coord; a multi-axis member can optionally name
    `delivery_column` when the member is representation-grained."""
    if not isinstance(raw, dict):
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} member {raw!r} must be a table.",
            "Each member is a `[[variable_group.members]]` table.",
        )
    ref = _require_str(raw, "variable", f"group {key!r} member")
    if not axes:
        if (
            raw.get("coords") is not None
            or raw.get("value") is not None
            or raw.get("label") is not None
        ):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} member {ref!r} cannot set facet "
                "coords, `value`, or `label` because the group is axis-less.",
                'For `axes = []`, list each member as `variable = "..."` only.',
            )
        if raw.get("delivery_column") is not None:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} member {ref!r} cannot set "
                "`delivery_column` because the group is axis-less.",
                "Axis-less variable umbrellas are whole-variable groups; drop "
                "`delivery_column` or declare facet axes.",
            )
        return CuratedMember(variable=ref, delivery_column=None, coords=())
    multi_axis = len(axes) > 1 or raw.get("coords") is not None
    if not multi_axis:
        # Legacy single-axis member: flat value/label on the single axis.
        (axis, _axis_label) = axes[0]
        return CuratedMember(
            variable=ref,
            delivery_column=None,
            coords=(
                (
                    axis,
                    _require_str(raw, "value", f"group {key!r} member"),
                    _require_str(raw, "label", f"group {key!r} member"),
                ),
            ),
        )
    raw_delivery_column = raw.get("delivery_column")
    if raw_delivery_column is not None and (
        not isinstance(raw_delivery_column, str) or not raw_delivery_column.strip()
    ):
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} member {ref!r} needs a non-empty "
            "`delivery_column` when that field is present.",
            'Give `delivery_column = "CDISP"`.',
        )
    raw_coords = raw.get("coords")
    if not isinstance(raw_coords, list) or not raw_coords:
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} member {ref!r} needs a non-empty "
            "`coords` array.",
            "List the member's per-axis coordinates as "
            "`coords = [{ axis, value, label }, …]`.",
        )
    coords: list[tuple[str, str, str]] = []
    seen_axes: set[str] = set()
    for rc in raw_coords:
        if not isinstance(rc, dict):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} member {ref!r} coord {rc!r} must be "
                "a table.",
                "Each coord is a `{ axis, value, label }` table.",
            )
        axis = _require_str(rc, "axis", f"group {key!r} member {ref!r} coord")
        if axis not in axis_names:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} member {ref!r} coord names axis "
                f"{axis!r}, not a declared group axis.",
                "Every coord's `axis` must be one of the group's declared `axes`.",
            )
        if axis in seen_axes:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} member {ref!r} sets axis {axis!r} "
                "twice.",
                "Give each axis exactly one coord per member.",
            )
        seen_axes.add(axis)
        coords.append(
            (
                axis,
                _require_str(rc, "value", f"group {key!r} member {ref!r} coord"),
                _require_str(rc, "label", f"group {key!r} member {ref!r} coord"),
            )
        )
    if seen_axes != axis_names:
        missing = sorted(axis_names - seen_axes)
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} member {ref!r} is missing a coord for "
            f"axis/axes {missing}.",
            "Every member must declare a coord on every group axis.",
        )
    return CuratedMember(
        variable=ref,
        delivery_column=raw_delivery_column.strip()
        if isinstance(raw_delivery_column, str)
        else None,
        coords=tuple(coords),
    )


def _reject_mixed_member_grain(members: tuple[CuratedMember, ...], key: str) -> None:
    """A group may name a variable at whole-variable OR representation grain, not
    both. Mixing `delivery_column = None` with concrete columns for one variable makes
    the read surfaces ambiguous: one member claims all columns while the others claim
    specific columns."""
    by_variable: dict[str, set[bool]] = {}
    for member in members:
        by_variable.setdefault(member.variable, set()).add(
            member.delivery_column is None
        )
    mixed = sorted(
        variable for variable, grains in by_variable.items() if len(grains) > 1
    )
    if mixed:
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups group {key!r} mixes whole-variable and "
            f"delivery-column members for variable(s) {mixed}.",
            "For each variable in one group, use either whole-variable members "
            "or representation-grained members, not both.",
        )


def load_concept_groups(path: Path | None) -> tuple[CuratedGroup, ...]:
    """Parse the curated-family TOML. Empty when no file (synthetic test
    builds, wheel installs).

    Three member shapes are accepted (#819). The LEGACY single-axis shape — `axis =
    "..."` on the group, flat `value`/`label` per member — is what the candidate
    generator emits and the whole `concept_groups.auto.toml` uses; it maps to a
    one-axis group with whole-variable (`delivery_column = None`) members. The
    EXPLICIT-AXES shape — `axes = [{ axis, label }, …]` on the group, `coords =
    [{ axis, value, label }, …]` per member — supports named-axis labels and
    multi-axis groups; `delivery_column` is present only when the member is
    representation-grained. The AXIS-LESS umbrella shape sets `axes = []` and
    member tables list only `variable`.

    Load-time validation (all EXIT_CONFIG, actionable): only `[[variable_group]]`
    top-level; `register` is a 2-segment `provider/register` FQID; `key`/`label`
    non-empty; exactly one of `axis`/`axes`; each member sets `variable`; coords
    cover every declared axis; axis-less members have no coords; `(variable,
    delivery_column)` unique within the group; keys are unique. Reference
    RESOLUTION (register/variable/column exist) happens during common resolution over
    the built DB, not here."""
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="variable_group",
        label="concept-group",
        prefix="concept_groups",
        code_base="concept_groups",
        file_name="curation/concept_groups.toml",
        entry_fields="register / key / label / axis|axes / members",
        # `curation/concept_groups.toml` carries two other entry kinds — `[[accept]]`
        # (folds an auto family by reference, `load_concept_group_accepts`) and
        # `[[classification_group]]` (curated umbrella, `load_classification_groups`)
        # — so both are legal siblings here, not unknown-top-level typos. Harmless
        # for `concept_groups.auto.toml`, which carries neither.
        sibling_keys=frozenset({"accept", "classification_group", "pair"}),
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
        axes, axis_names = _parse_group_axes(entry, key)
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
        seen_refs: set[tuple[str, str | None]] = set()
        for raw in raw_members:
            member = _parse_member(raw, key, axes, axis_names)
            ref = (member.variable, member.delivery_column)
            if ref in seen_refs:
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} references "
                    f"{ref[0]!r}/{ref[1]!r} twice.",
                    "List each (variable, delivery_column) member once.",
                )
            seen_refs.add(ref)
            members.append(member)
        _reject_mixed_member_grain(tuple(members), key)
        out.append(
            CuratedGroup(
                provider=parts[0],
                register=parts[1],
                key=key,
                label=label,
                axes=axes,
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
    """Parse the `[[accept]]` entries from `curation/concept_groups.toml` (#496): the
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
        file_name="curation/concept_groups.toml",
        entry_fields="register / key (+ optional label / axis / exclude)",
        sibling_keys=frozenset({"variable_group", "classification_group", "pair"}),
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
    `CuratedGroup` for offline conversion. The auto family must
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
    # An auto family is always single-axis (the candidate generator emits the
    # legacy single-axis shape), so it carries exactly one axis pair and each member
    # one coord on it. An `axis` override renames that axis on the group AND on every
    # member coord, keeping the group/coord axis names in sync (the validator
    # requires each coord's axis to be a declared group axis). Legacy single-axis
    # label == axis name, so the override sets both.
    (auto_axis, _auto_axis_label) = auto.axes[0]
    new_axis = accept.axis or auto_axis
    return CuratedGroup(
        provider=auto.provider,
        register=auto.register,
        key=auto.key,
        label=accept.label or auto.label,
        axes=_single_axis(new_axis),
        members=tuple(
            CuratedMember(
                variable=m.variable,
                delivery_column=m.delivery_column,
                coords=tuple(
                    (new_axis, value, label) for _axis, value, label in m.coords
                ),
            )
            for m in members
        ),
        origin="accept",
    )


def load_classification_groups(path: Path | None) -> tuple[ClassificationGroup, ...]:
    """Parse the curated `[[classification_group]]` umbrella tables (#516): an
    AXIS-LESS fold over genuinely-distinct classifications (the SUN group over
    niva/inriktning/grupp). Empty when no file (synthetic builds, wheel installs)
    or no `[[classification_group]]` tables.

    Load-time validation (all EXIT_CONFIG, actionable): `key`/`label` non-empty
    strings; `axis` OPTIONAL (None when absent — the umbrella is axis-less; a
    present-but-blank `axis` is still rejected via `_require_opt_str`); `members`
    a non-empty array of tables, each setting non-empty `classification` (slug) /
    `value` / `label`; member slugs unique; `key` unique; >= 2 members. Slug
    RESOLUTION (does the classification exist?) happens during offline conversion
    against the built DB."""
    entries = load_curation_entries(
        path,
        entry_key="classification_group",
        label="classification-group",
        prefix="concept_groups",
        code_base="concept_groups",
        file_name="curation/concept_groups.toml",
        entry_fields="key / label / members (+ optional axis)",
        sibling_keys=frozenset({"variable_group", "accept", "pair"}),
    )
    out: list[ClassificationGroup] = []
    seen_keys: set[str] = set()
    for entry in entries:
        key = _require_str(entry, "key", "[[classification_group]]")
        label = _require_str(entry, "label", "[[classification_group]]")
        axis = _require_opt_str(entry, "axis", "[[classification_group]]")
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


def load_code_label_pairs(path: Path | None) -> tuple[CodeLabelPair, ...]:
    """Parse the curated code↔label pair TOML (`reg_meta_build/curation/concept_groups.toml`,
    #923). Empty when no file (synthetic test builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only `[[pair]]` top-level;
    each entry sets `code` AND `label`, each a 3-segment `provider/register/variable`
    FQID; `(code, label)` FQID tuples are unique (a duplicate pair is drift, mirroring
    the duplicate-key rejection in `load_concept_groups`). Endpoint RESOLUTION (do the
    variables exist? is the code the value-set owner? are they co-delivered?) happens
    during common resolution over the built DB (`_append_code_label_edges`), not here."""
    entries = load_curation_entries(
        path,
        entry_key="pair",
        label="code-label-pair",
        prefix="code_label_pairs",
        code_base="code_label_pairs",
        file_name="curation/concept_groups.toml",
        entry_fields="code / label (both 3-segment FQIDs)",
        sibling_keys=frozenset({"variable_group", "accept", "classification_group"}),
    )
    out: list[CodeLabelPair] = []
    seen_pairs: set[tuple[tuple[str, str, str], tuple[str, str, str]]] = set()
    for entry in entries:
        code = _require_pair_fqid(entry, "code")
        label = _require_pair_fqid(entry, "label")
        # Reject a self-pair (code == label): a variable can't be both endpoints of
        # a code↔label decode. Caught here with a clear loader error rather than
        # letting the contradictory value_set guards fire during offline conversion.
        if code == label:
            raise curation_error(
                "code_label_pairs_invalid",
                f"code_label_pairs pair has identical `code` and `label` FQID "
                f"{'/'.join(code)!r}.",
                "A code↔label pair needs two distinct variables. Fix or drop the "
                "pair in reg_meta_build/curation/concept_groups.toml.",
            )
        # Reject duplicate (code, label) FQID tuples (mirrors `load_concept_groups`
        # rejecting duplicate keys in the same file). The committed TOML is
        # deduplicated, so this is a drift guard; the full FQID keys the set since
        # two registers can share a variable slug.
        if (code, label) in seen_pairs:
            raise curation_error(
                "code_label_pairs_invalid",
                f"code_label_pairs duplicate pair {'/'.join(code)!r} <-> "
                f"{'/'.join(label)!r}.",
                "List each (code, label) pair once in "
                "reg_meta_build/curation/concept_groups.toml.",
            )
        seen_pairs.add((code, label))
        out.append(
            CodeLabelPair(
                code_provider=code[0],
                code_register=code[1],
                code_variable=code[2],
                label_provider=label[0],
                label_register=label[1],
                label_variable=label[2],
            )
        )
    return tuple(out)


# ── derivation passes ───────────────────────────────────────────────────────


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


@dataclass(frozen=True)
class MonthGroupCandidate:
    register: str
    key: str
    label: str | None
    members: tuple[tuple[int, str], ...]
    issue: Literal["stem_collision", "reserved_key"] | None = None


def _evaluate_month_fold(
    members: list[tuple[int, str, str | None, str]],
) -> str | None:
    """One raw stem needs three distinct months and a usable shared name prefix.

    Use the same predicate for collision detection and emission: a coincidental
    peer stem with too few months or missing/disagreeing names is not a competitor.
    """
    if len({m[0] for m in members}) < _MIN_MONTH_SIBLINGS:
        return None
    names = [m[2] for m in members]
    if any(n is None for n in names):
        return None
    label = _trim_label(_common_prefix([n for n in names if n is not None]))
    return label if len(label) >= _MIN_LABEL_PREFIX else None


def month_group_candidates(
    rows: Iterable[tuple[str, str, str | None]],
    *,
    reserved_keys: frozenset[tuple[str, str]] = frozenset(),
) -> tuple[MonthGroupCandidate, ...]:
    """Apply the existing month vocabulary and guards without database access.

    Rows are (opaque register key, slug, name) for variables not already grouped.
    Reserved keys include existing edge groups and pending curated groups. Return
    qualifying groups and exact collisions, never merge different raw stems just
    because trimming trailing hyphens gives them the same key.
    """
    candidates: dict[tuple[str, str], list[tuple[int, str, str | None, str]]] = {}
    seen = set()
    for register, slug, name in rows:
        if (register, slug) in seen:
            raise ValueError("duplicate variable identity in month grouping")
        seen.add((register, slug))
        for token, month in _MONTH_TOKENS.items():
            if slug.endswith(token) and len(slug) > len(token):
                raw_stem = slug[: -len(token)]
                key_stem = raw_stem.rstrip("-")
                if key_stem:
                    candidates.setdefault((register, key_stem), []).append(
                        (month, slug, name, raw_stem)
                    )
                break
    results = []
    for (register, key), bucket in sorted(candidates.items()):
        by_raw_stem: dict[str, list[tuple[int, str, str | None, str]]] = {}
        for member in bucket:
            by_raw_stem.setdefault(member[3], []).append(member)
        qualifying = [
            (sub, label)
            for sub in by_raw_stem.values()
            if (label := _evaluate_month_fold(sub)) is not None
        ]
        if not qualifying:
            continue
        if len(qualifying) > 1:
            members, label, issue = bucket, None, "stem_collision"
        else:
            members, label = qualifying[0]
            issue = "reserved_key" if (register, key) in reserved_keys else None
        results.append(
            MonthGroupCandidate(
                register,
                key,
                label,
                tuple(sorted((m[0], m[1]) for m in members)),
                issue,
            )
        )
    return tuple(results)


# SUN slugs bake the vintage MID-slug to mirror the short_name (`SUN2020-NIVA`
# → `sun2020-niva`, #747), so the general trailing-year rule below can't see the
# vintage. This SUN-scoped override maps `sun<year>-<dim>` to its vintage family
# (stem `sun-<dim>` + year) so #494 value-set reclaim and #571 succession bucket
# the editions together exactly as a trailing-year family would — WITHOUT
# touching the general rule (every other family's vintage is genuinely trailing).
_SUN_MID_VINTAGE_RE = re.compile(r"^sun(\d{4})-(niva|inriktning|grupp)\Z")


def _classification_vintage(slug: str) -> tuple[str, int] | None:
    """`(stem, year)` for a vintage EDITION slug, else `None` (a non-edition slug
    has no vintage family). Single source of truth for the vintage rule, shared by
    `classification_slug_stem` and common classification succession.

    General rule: a trailing 4-digit vintage year, stripped ONLY when the
    remaining stem does not itself end in a digit (a digit-ending stem means the
    tail splits a longer number, not a vintage year) and the year is in range —
    `ssyk2012` → `('ssyk', 2012)`, `sni2007` → `('sni', 2007)`.
    SUN override (#747): `sun2020-niva` → `('sun-niva', 2020)` (see above)."""
    m = _SUN_MID_VINTAGE_RE.match(slug)
    if m:
        year = int(m.group(1))
        return (f"sun-{m.group(2)}", year) if year in _VINTAGE_YEARS else None
    tail = slug[-4:]
    if len(slug) < 5 or not tail.isdigit():
        return None
    year = int(tail)
    stem = slug[:-4]
    if year not in _VINTAGE_YEARS or stem[-1].isdigit():
        return None
    return stem, year


def classification_slug_stem(slug: str | None) -> str | None:
    """The vintage-FAMILY key for a classification slug (the year stripped to its
    stem), or the slug itself when it is not a vintage edition. Thin wrapper over
    `_classification_vintage` (the canonical rule).

    Examples: `sun2020-niva`/`sun2000-niva` → `sun-niva` (SUN override, #747);
    `sni2002`/`sni2007` → `sni`; `isced` → `isced` (no year tail); `sun1996` →
    `sun` (the curated split root collapses to the bare `sun` stem — distinct from
    its `sun-niva`/`-inriktning`/`-grupp` dimension stems, so it stays its own
    single-member family and the 1→many split to 2000 is curated, not auto).

    NULL-safe so it can be a SQLite UDF over `classification.slug` (NULL under
    `--skip-slugs`, where the reclaim is inert anyway): a NULL slug → NULL stem.
    """
    if slug is None:
        return None
    v = _classification_vintage(slug)
    return v[0] if v is not None else slug


def classification_succession_edges(
    rows: Iterable[tuple[str, str]],
) -> tuple[tuple[str, str, int], ...]:
    """Derive adjacent editions from the existing guarded vocabulary, without IO.

    It operates on the selected canonical classifications, never source-variable
    code labels.
    """
    # stem → [(year, slug, name)]
    families: dict[str, list[tuple[int, str, str]]] = {}
    for slug, name in rows:
        # `_classification_vintage` returns (stem, year) for an edition or None
        # for a non-edition — handles both the trailing-year families and the SUN
        # mid-slug override, so the year is read from the rule, not `slug[-4:]`
        # (which is `niva` for `sun2020-niva`).
        vintage = _classification_vintage(slug)
        if vintage is None:
            continue
        stem, year = vintage
        families.setdefault(stem, []).append((year, slug, name))
    edges = []
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
        edges.extend((pred[1], succ[1], succ[0]) for pred, succ in pairwise(editions))
    return tuple(edges)
