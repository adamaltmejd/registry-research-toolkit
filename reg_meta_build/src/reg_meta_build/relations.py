"""Typed relation declarations and common graph validation.

Input loaders retain the accepted TOML grammar. The common curation stage resolves
endpoints and succession before the mechanical catalog writer persists them.
Identity, temporal succession and classification derivation remain distinct.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import (
    Fqid,
    FqidError,
    FqidKind,
    parse as parse_fqid,
    validate_slug,
)

from ._curation import (
    curation_error,
    load_curation_entries,
    require_fqid,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

# ── relation kind vocabularies ──────────────────────────────────────────────

# The legal `type` discriminators. Surfaced in the unknown-type error so a typo
# is self-correcting.
_EDGE_TYPES: frozenset[str] = frozenset({"same_as", "replaced_by", "derived_from"})

# Provenance marker for curated replaced_by edges (mirrors db.py so a consumer can
# tell curated from auto-derived). It lands in `note` for ALL three grains. For
# register/variable a row's own `note` (the human transition reason) lands in
# `beskrivning` beside it; the classification table has NO `beskrivning`, so a
# classification edge carries no transition reason at all — `note` is
# provenance-only and any human reason belongs in a `#` comment in relations.toml.
_REPLACED_BY_NOTE_CURATED = "curated:slug_toml"

# Provenance marker for the #584 derived variable vintage-succession edges:
# variable A → B lifted from a `classification_replaced_by` edition edge through
# the value-set bindings. Distinct from `auto:timeseries_event` (event-derived) /
# `curated:slug_toml` (hand-curated) so a consumer can tell the lift apart.
_REPLACED_BY_NOTE_VINTAGE_LIFT = "derived:classification_vintage_lift"

# same_as component-size guard (#522). A same_as edge MERGES two identity
# components into one; a single mistaken curated edge can therefore silently weld
# two large, genuinely-distinct concept clusters into one resolver blob. Refuse
# any edge whose merged component would exceed this many distinct FQIDs — a
# curated identity cluster that large is almost certainly a curation error, not a
# real concept. This now governs SHIPPED data: the #508 tier-1 batch carries 615
# edges across 62 identity components (max component 13 FQIDs), so the guard
# actively bounds the live data (with comfortable headroom under the cap).
_SAME_AS_MAX_COMPONENT = 32

# Replaced_by grains: register-, variable-, or classification-grain. The variant
# grain is deliberately out of scope — a variant is a delivery coordinate, not a
# curation surface for cross-provider succession. Classification grain (#579) is
# the `class/<slug>` form (a 1→many edition split the #571 auto rule can't
# produce, e.g. sun1996 → sun2000-niva + sun2000-inriktning).
_REPLACED_BY_GRAINS: frozenset[FqidKind] = frozenset(
    {FqidKind.REGISTER, FqidKind.VARIABLE_BINDING, FqidKind.CLASSIFICATION}
)

# Per-type accepted/foreign field maps (besides `type`). A field legal for one
# type is a FOREIGN key on another (e.g. `effective_year` on a same_as edge) and
# rejected — this catches a mis-typed edge (right fields, wrong `type`) at load.
_SAME_AS_FIELDS: frozenset[str] = frozenset({"a", "b", "note"})
# `from_column` / `to_column` (#843) ride a variable-grain replaced_by edge to
# name a REPRESENTATION endpoint `(variable_fqid, delivery_column)` — both or
# neither, both endpoints variable-grain. A column field on a same_as edge is
# foreign and rejected by its map.
# `variant` (#846) optionally rides a REPRESENTATION edge (one carrying
# `from_column` / `to_column`) to scope the succession to a single
# register-variant: `''`/absent = variable-level (the default, whole-variable
# semantics), a variant slug = scoped. Legal ONLY on a representation edge and
# only WITH `effective_year` (the time-monotone cycle check needs the ordering) —
# both enforced in `_load_replaced_by`.
# `from_variant` / `to_variant` (#376) ride a REGISTER-grain edge to name concrete
# register_variant endpoints while keeping variants out of the public FQID grammar.
_REPLACED_BY_FIELDS: frozenset[str] = frozenset(
    {
        "from",
        "to",
        "effective_year",
        "note",
        "from_column",
        "to_column",
        "variant",
        "from_variant",
        "to_variant",
    }
)
_DERIVED_FROM_FIELDS: frozenset[str] = frozenset({"derived", "source", "note"})

_VarKey = tuple[str, str, str]
_ClassKey = tuple[str, str]
_SLUG_TOKEN_RE = re.compile(r"[a-z]+|\d+")


# ── dataclasses ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedSameAs:
    """One `type = "same_as"` identity edge: an UNORDERED pair of FQIDs (`a` / `b`,
    same grain) plus an optional `note`. There is NO `relation_kind` — same_as
    carries no kind vocabulary; identity is identity.

    Both endpoints are either variable-grain (`a_provider/a_register/a_variable`,
    `b_*` the mirror) or classification-grain (`a_register`/`b_register` carry the
    classification slug, `a_variable`/`b_variable` are None). `grain` records
    which. Endpoint resolution happens at common resolution
    for endpoint providers included in the build. Edges remain slug-anchored, but
    an edge written by the build must resolve both endpoint slugs."""

    grain: FqidKind  # VARIABLE_BINDING or CLASSIFICATION
    a_provider: str
    a_register: str
    a_variable: str | None
    b_provider: str
    b_register: str
    b_variable: str | None
    note: str | None

    def a_fqid(self) -> str:
        return _join_fqid(self.a_provider, self.a_register, self.a_variable)

    def b_fqid(self) -> str:
        return _join_fqid(self.b_provider, self.b_register, self.b_variable)


@dataclass(frozen=True)
class CuratedReplacedBy:
    """One `type = "replaced_by"` succession edge, parsed FQID-shaped but
    DB-unverified. `predecessor` / `successor` (TOML `from` / `to`) are parsed
    `Fqid`s of the SAME grain — both register (2 segs), both variable (3 segs),
    or both classification (`class/<slug>`, #579). `note` / `effective_year` are
    optional provenance. Existence (the successor must resolve to a live, slugged
    DB entity; the predecessor MAY be dead for the register/variable grain, but the
    classification grain requires it live too — #579) is checked downstream against
    the built DB — this loader stays DB-free.

    #843 representation grain: `predecessor_column` / `successor_column` (TOML
    `from_column` / `to_column`) turn a VARIABLE-grain edge into a REPRESENTATION
    edge — succession between two `(variable_fqid, delivery_column)` pairs (a
    column-level era rename the variable grain can't express, both endpoints
    collapsing to one variable FQID). Both columns are set together or both None;
    a register/variable/classification edge leaves both None. When columns are
    present BOTH endpoints are variable-grain AND share `(provider, register)` —
    a representation rename is INTRA-register (the loader enforces both); the
    variable slug MAY differ (two sibling variables of one register) and MAY be
    equal (same variable, two columns); only the full `(fqid, column)` tuple must
    differ (no self-loop). Both endpoints' `(variable, delivery_column)` must be
    live/observed downstream — the successor-provider skip is total (both
    endpoints share one provider, so no dead-predecessor case arises): a
    within-build column rename observes both columns.

    #846 `variant`: an OPTIONAL `register_variant` slug scoping a REPRESENTATION
    edge's succession to one variant. `""` (the default) = UNSCOPED — the
    whole-variable semantics #843 shipped. A non-empty slug = variant-local: the
    rename holds only within that variant (e.g. FRIDA's firm key, delivered as
    `borgnr` then `persorgnr` then `borgnr` ONLY in `punktskatter-for-energi`,
    while sibling variants deliver `borgnr` continuously). The loader admits
    `variant` ONLY on a representation edge (column fields present) and ONLY with
    `effective_year` (the common time-monotone cycle check orders the
    round-trip by year). The slug is resolved against the built DB at common resolution (like the column endpoints), not at load."""

    predecessor: Fqid
    successor: Fqid
    note: str | None
    effective_year: int | None
    predecessor_column: str | None = None
    successor_column: str | None = None
    variant: str = ""
    predecessor_variant: str | None = None
    successor_variant: str | None = None


@dataclass(frozen=True)
class CuratedClassificationDerivedFrom:
    """One `type = "derived_from"` non-temporal classification edge. `derived`
    is the specialized classification; `source` is the classification it derives
    from. Both endpoints use the `class/<slug>` FQID form and must resolve to live
    classifications at common resolution. These edges are intentionally separate
    from `classification_replaced_by`: they are contemporaneous/semantic links,
    not edition succession."""

    derived: Fqid
    source: Fqid
    note: str | None


@dataclass(frozen=True)
class CuratedRelations:
    """The parsed `relations.toml`, grouped by relation kind. One load yields
    all groups; the build materializes each into its own table(s)."""

    same_as: tuple[CuratedSameAs, ...]
    replaced_by: tuple[CuratedReplacedBy, ...]
    derived_from: tuple[CuratedClassificationDerivedFrom, ...]


def _join_fqid(provider: str, register: str, variable: str | None) -> str:
    return f"{provider}/{register}" + (f"/{variable}" if variable is not None else "")


def _slug_tokens(slug: str) -> tuple[str, ...]:
    return tuple(_SLUG_TOKEN_RE.findall(slug.lower()))


def _classification_vintage_tokens(
    predecessor_slug: str, successor_slug: str
) -> frozenset[str]:
    """Tokens that differ between adjacent classification editions and look like
    vintage markers.

    Keep this deliberately conservative: only digit-bearing tokens are stripped
    from variable slugs. Facet / population / level words such as ``individ``,
    ``foretag``, ``grov``, or ``utokad`` stay in the stream key, so the entangled
    lift cannot cross-link parallel streams merely because the classification
    edition changed."""
    pred = set(_slug_tokens(predecessor_slug))
    succ = set(_slug_tokens(successor_slug))
    return frozenset(tok for tok in pred ^ succ if any(ch.isdigit() for ch in tok))


def _variable_vintage_stream_key(
    variable_slug: str, predecessor_class_slug: str, successor_class_slug: str
) -> tuple[str, ...]:
    """Slug-stem stream key for one adjacent classification-edition edge (#592).

    The key removes the edge's digit-bearing vintage tokens from the variable slug
    and leaves every other token intact. Examples for ``sni2002 -> sni2007``:
    ``fars-sni-2002`` and ``fars-sni-2007`` both key as ``("fars", "sni")``,
    while ``mors-*`` remains a separate stream."""
    vintage = _classification_vintage_tokens(
        predecessor_class_slug, successor_class_slug
    )
    stem = tuple(tok for tok in _slug_tokens(variable_slug) if tok not in vintage)
    return stem or ("",)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


_require_fqid_variable = functools.partial(
    require_fqid,
    code="relations_invalid",
    prefix="relations",
    entry_table="[[edge]]",
    file_name="curation/relations.toml",
)


def _reject_foreign_fields(
    entry: dict, edge_type: str, allowed: frozenset[str]
) -> None:
    """Reject any field on `entry` (besides `type`) not legal for `edge_type`. A
    field legal for ANOTHER type (e.g. `effective_year` on a same_as edge) is the
    tell of a mis-typed edge — right fields, wrong `type` — so fail loud."""
    foreign = set(entry) - {"type"} - allowed
    if foreign:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type={edge_type!r} has field(s) "
            f"{sorted(foreign)} not allowed for that type.",
            f"A {edge_type!r} edge accepts {sorted(allowed)} (plus `type`). "
            "Remove the foreign field or fix `type` in "
            "reg_meta_build/curation/relations.toml.",
        )


def _require_note(entry: dict, edge_type: str) -> str | None:
    """`note` is optional but, when present, a non-empty string."""
    note = entry.get("note")
    if note is not None and (not isinstance(note, str) or not note):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type={edge_type!r} `note` must be a non-empty "
            f"string when present, got {note!r}.",
            'Drop `note` or give it a non-empty value like `note = "curated"`.',
        )
    return note


def _classification_fqid(field: str, raw: Any) -> _ClassKey:
    """Parse a 2-segment `provider/classification_slug` classification FQID, used
    by classification-grain same_as. (The 3-seg variable form goes through
    `require_fqid`.)"""
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] `{field}` must be a non-empty FQID string, "
            f"got {raw!r}.",
            'Give a classification FQID like "scb/sun2020".',
        )
    parts = raw.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] `{field}` {raw!r} must be a 2-segment "
            "`provider/classification_slug` FQID.",
            'Give a classification FQID like "scb/sun2020".',
        )
    return (parts[0], parts[1])


def _load_same_as(entry: dict) -> CuratedSameAs:
    """Validate one `type = "same_as"` edge. `a` / `b` are FQIDs of the SAME grain
    — both 3-seg variable OR both 2-seg classification. No self-edge."""
    _reject_foreign_fields(entry, "same_as", _SAME_AS_FIELDS)

    def _require_endpoint(field: str) -> str:
        raw = entry.get(field)
        if not isinstance(raw, str) or not raw:
            raise curation_error(
                "relations_invalid",
                f"relations [[edge]] type='same_as' needs `{field}` as a "
                f"non-empty FQID string, got {raw!r}.",
                "Give variable (provider/register/variable) or classification "
                "(provider/classification_slug) FQIDs.",
            )
        return raw

    a_raw = _require_endpoint("a")
    b_raw = _require_endpoint("b")
    a_segs = a_raw.split("/")
    b_segs = b_raw.split("/")
    if len(a_segs) != len(b_segs) or len(a_segs) not in (2, 3):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='same_as' endpoints {a_raw!r} / {b_raw!r} "
            "must be the SAME grain — both 3-segment variable or both 2-segment "
            "classification FQIDs.",
            "same_as relates two entities of one grain; fix the mismatched FQID.",
        )
    note = _require_note(entry, "same_as")
    if len(a_segs) == 3:
        a = _require_fqid_variable(entry, "a")
        b = _require_fqid_variable(entry, "b")
        grain = FqidKind.VARIABLE_BINDING
        edge = CuratedSameAs(
            grain=grain,
            a_provider=a[0],
            a_register=a[1],
            a_variable=a[2],
            b_provider=b[0],
            b_register=b[1],
            b_variable=b[2],
            note=note,
        )
    else:
        a_cls = _classification_fqid("a", a_raw)
        b_cls = _classification_fqid("b", b_raw)
        grain = FqidKind.CLASSIFICATION
        edge = CuratedSameAs(
            grain=grain,
            a_provider=a_cls[0],
            a_register=a_cls[1],
            a_variable=None,
            b_provider=b_cls[0],
            b_register=b_cls[1],
            b_variable=None,
            note=note,
        )
    if (edge.a_provider, edge.a_register, edge.a_variable) == (
        edge.b_provider,
        edge.b_register,
        edge.b_variable,
    ):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='same_as' relates {edge.a_fqid()} to itself.",
            "A same_as edge connects two DISTINCT FQIDs; remove the self-edge.",
        )
    return edge


def _load_replaced_by(entry: dict) -> CuratedReplacedBy:
    """Validate one `type = "replaced_by"` edge. `from` / `to` are FQIDs of the
    SAME grain (register, variable, or classification — not variant). No
    self-loop. Neither endpoint is resolved at load (DB-free); the predecessor may
    be dead for register/variable, but common dependency resolution requires
    the classification grain's predecessor live too (#579).

    #579: classification endpoints use the `class/<slug>` form (e.g.
    `class/sun1996`), DISAMBIGUATED from the 2-segment register grain
    (`provider/register`). This differs from same_as, whose classification grain
    uses the 2-segment `provider/slug` form — replaced_by can't reuse that without
    colliding with register grain."""
    _reject_foreign_fields(entry, "replaced_by", _REPLACED_BY_FIELDS)
    predecessor = _parse_replaced_by_fqid("from", entry.get("from"))
    successor = _parse_replaced_by_fqid("to", entry.get("to"))
    if predecessor.kind is not successor.kind:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `from` {str(predecessor)!r} "
            f"({predecessor.kind.value}) and `to` {str(successor)!r} "
            f"({successor.kind.value}) are different grains.",
            "Both endpoints must be the same grain (register->register, "
            "variable->variable, or classification->classification).",
        )
    # #843 representation grain: parse optional `from_column` / `to_column`. Both
    # or neither (a representation edge names both endpoints' columns), and only on
    # a VARIABLE-grain edge (succession is column-within-variable). A self-loop is
    # re-keyed on the full `(fqid, column)` tuple below, so same-variable /
    # different-column (the common representation rename) is LEGAL.
    pred_column = _require_column(entry, "from_column")
    succ_column = _require_column(entry, "to_column")
    pred_variant = _require_variant_endpoint(entry, "from_variant")
    succ_variant = _require_variant_endpoint(entry, "to_variant")
    if (pred_column is None) != (succ_column is None):
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' has exactly one of "
            "`from_column` / `to_column` — a representation edge names BOTH "
            "endpoints' columns.",
            "Give both `from_column` and `to_column` for a representation "
            "(column-grain) edge, or neither for an entity-grain edge.",
        )
    if (pred_variant is None) != (succ_variant is None):
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' has exactly one of "
            "`from_variant` / `to_variant` — a variant edge names BOTH "
            "concrete register_variant endpoints.",
            "Give both `from_variant` and `to_variant` for a variant-grain "
            "succession, or neither for an entity-grain edge.",
        )
    if pred_variant is not None and (
        predecessor.kind is not FqidKind.REGISTER
        or successor.kind is not FqidKind.REGISTER
    ):
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries `from_variant` / "
            "`to_variant` but the endpoints are not register-grain.",
            "A concrete register_variant succession uses register FQIDs in "
            "`from` / `to` plus `from_variant` / `to_variant`. Drop the variant "
            "fields, or change the endpoints to provider/register FQIDs.",
        )
    if pred_variant is not None and pred_column is not None:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries both variant endpoints "
            "and column endpoints.",
            "Use `from_variant` / `to_variant` for a register_variant succession, "
            "or `from_column` / `to_column` for a representation succession — "
            "not both on one edge.",
        )
    if pred_column is not None and predecessor.kind is not FqidKind.VARIABLE_BINDING:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' carries `from_column` / "
            f"`to_column` but the endpoints are {predecessor.kind.value}-grain.",
            "Representation succession is column-within-variable; column fields "
            "require both endpoints to be variable (provider/register/variable) "
            "FQIDs.",
        )
    # A representation succession is an INTRA-register column rename: the
    # `column_merge` cases it expresses (#846/#196) are all keyed
    # `(register_id, var_id)`, so both endpoints must share `provider` AND
    # `register` (the variable slug MAY differ — two sibling variables of one
    # register — and MAY be equal — same variable, two columns). A cross-register
    # column rename is not a real concept; that's what the variable grain is for.
    # This also makes the materializer's all-live rule safe in partial builds:
    # both endpoints share one provider, so its `if succ.provider not in
    # providers` skip covers the WHOLE edge (no predecessor-provider asymmetry).
    if pred_column is not None and (predecessor.provider, predecessor.register) != (
        successor.provider,
        successor.register,
    ):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' representation edge "
            f"{str(predecessor)!r} -> {str(successor)!r} crosses registers "
            f"({predecessor.provider}/{predecessor.register} -> "
            f"{successor.provider}/{successor.register}).",
            "A representation (column-rename) edge is intra-register: both "
            "endpoints must share provider and register (the variable may "
            "differ). For cross-register succession use the entity (variable) "
            "grain — drop `from_column` / `to_column`.",
        )
    # Case-fold the column in the self-loop check — `from_column = "Col"` /
    # `to_column = "col"` on one variable is a case-only self-loop (the build
    # matches columns case-insensitively), so it must be rejected too.
    pred_col_fold = pred_column.lower() if pred_column is not None else None
    succ_col_fold = succ_column.lower() if succ_column is not None else None
    if (
        str(predecessor),
        pred_col_fold,
        pred_variant,
    ) == (str(successor), succ_col_fold, succ_variant):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' self-loop on "
            f"{str(predecessor)!r}"
            + (f" column {pred_column!r}" if pred_column is not None else "")
            + (f" variant {pred_variant!r}" if pred_variant is not None else "")
            + ".",
            "An entity cannot replace itself; remove the edge. (A representation "
            "edge MAY repeat the variable FQID — but then `from_column` and "
            "`to_column` must differ; a variant edge MAY repeat the register FQID "
            "but then `from_variant` and `to_variant` must differ.)",
        )
    note = _require_note(entry, "replaced_by")
    # `note` is provenance-only on a classification edge: that table has no
    # `beskrivning`, so the build stamps the fixed `curated:slug_toml` marker and a
    # human transition reason has nowhere to go. Reject `note` here rather than
    # parse-then-silently-drop it (which reads as a bug) — the reason belongs in a
    # `#` comment above the edges.
    if note is not None and predecessor.kind is FqidKind.CLASSIFICATION:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' on a classification "
            f"(`class/<slug>`) edge does not accept `note` (got {note!r}).",
            "Drop `note` — a classification edge's `note` is provenance-only "
            "(stamped `curated:slug_toml`). Put the transition reason in a `#` "
            "comment above the edges in reg_meta_build/curation/relations.toml.",
        )
    effective_year = entry.get("effective_year")
    # `isinstance(True, int)` is True in Python — reject a bare bool so an
    # `effective_year = true` typo can't masquerade as the year 1.
    if effective_year is not None and (
        isinstance(effective_year, bool) or not isinstance(effective_year, int)
    ):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `effective_year` must be an "
            f"integer when present, got {type(effective_year).__name__}.",
            "Use a bare integer year, e.g. effective_year = 2012.",
        )
    # #846: optional `variant` scope. Legal ONLY on a representation edge (column
    # fields present) — a `variant` on a plain register/variable/classification
    # edge is a mis-modeled succession (a variant is a delivery coordinate, not an
    # entity-grain curation surface). When set it REQUIRES `effective_year`: a
    # variant-scoped succession may be cyclic (the FRIDA round-trip), and the
    # time-monotone cycle check that permits the cycle orders its edges by year.
    variant = entry.get("variant")
    if variant is not None and (not isinstance(variant, str) or not variant):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `variant` must be a non-empty "
            f"register_variant slug string when present, got {variant!r}.",
            'Give a register_variant slug like `variant = "punktskatter-for-energi"`,'
            " or drop the field for a variable-level (whole-variable) succession.",
        )
    if variant is not None and pred_column is None:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries `variant` but is not a "
            "representation (column-grain) edge.",
            "`variant` scopes a column-level rename to one register_variant; it is "
            "legal only with `from_column` / `to_column`. Drop `variant`, or add "
            "the column endpoints.",
        )
    if variant is not None and effective_year is None:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries `variant` but no "
            "`effective_year`.",
            "A variant-scoped succession may be a time-monotone cycle (a column "
            "left and later returned within the variant); the cycle check orders "
            "it by year. Add `effective_year`, or drop `variant`.",
        )
    if variant is not None and pred_variant is not None:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries both `variant` and "
            "`from_variant` / `to_variant`.",
            "`variant` scopes a representation edge; `from_variant` / "
            "`to_variant` define concrete register_variant endpoints. Use only "
            "one of those shapes.",
        )
    return CuratedReplacedBy(
        predecessor=predecessor,
        successor=successor,
        note=note,
        effective_year=effective_year,
        predecessor_column=pred_column,
        successor_column=succ_column,
        variant=variant or "",
        predecessor_variant=pred_variant,
        successor_variant=succ_variant,
    )


def _load_derived_from(entry: dict) -> CuratedClassificationDerivedFrom:
    """Validate one `type = "derived_from"` classification edge. It is
    directional but NON-temporal: `derived` is the specialized classification and
    `source` is the classification it derives from. Only `class/<slug>` endpoints
    are legal so the relation cannot be confused with register/variable
    succession."""
    _reject_foreign_fields(entry, "derived_from", _DERIVED_FROM_FIELDS)
    derived = _parse_class_relation_fqid("derived", entry.get("derived"))
    source = _parse_class_relation_fqid("source", entry.get("source"))
    if derived == source:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='derived_from' self-link on {str(derived)!r}.",
            "A derived classification must name a DIFFERENT source "
            "classification; remove the edge.",
        )
    return CuratedClassificationDerivedFrom(
        derived=derived,
        source=source,
        note=_require_note(entry, "derived_from"),
    )


def _require_column(entry: dict, field: str) -> str | None:
    """#843: an optional `from_column` / `to_column` field — when present, a
    non-empty string (a delivery-column header). Returns None when absent."""
    raw = entry.get(field)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` must be a "
            f"non-empty string when present, got {raw!r}.",
            'Give a delivery-column name like `from_column = "DispInk04"`, or '
            "drop the field.",
        )
    return raw


def _require_variant_endpoint(entry: dict, field: str) -> str | None:
    """#376: an optional concrete `register_variant` endpoint slug for a
    register-grain succession. Returns None when absent."""
    raw = entry.get(field)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` must be a "
            f"non-empty register_variant slug string when present, got {raw!r}.",
            f'Give a register_variant slug like `{field} = "individer-15plus"`, '
            "or drop the field for an entity-grain succession.",
        )
    try:
        validate_slug(raw, "register_variant", allow_default=True)
    except FqidError as exc:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` is not a valid "
            f"register_variant slug: {raw!r}.",
            str(exc),
        ) from exc
    return raw


def _parse_replaced_by_fqid(field: str, raw: Any) -> Fqid:
    """Parse one replaced_by endpoint FQID string against the FQID grammar,
    restricted to the register / variable / classification grains. The
    classification grain is the `class/<slug>` form (#579)."""
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` must be a "
            f"non-empty FQID string, got {raw!r}.",
            "Quote a register, variable, or classification FQID, e.g. "
            '"scb/lisa", "scb/lisa/kon", or "class/sun1996".',
        )
    try:
        fqid = parse_fqid(raw)
    except FqidError as exc:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` {raw!r} is not a "
            f"valid FQID: {exc}.",
            "Use a register (provider/register), variable "
            "(provider/register/variable), or classification (class/<slug>) FQID.",
        ) from exc
    if fqid.kind not in _REPLACED_BY_GRAINS:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` {raw!r} is a "
            f"{fqid.kind.value}-grain FQID; only register, variable, and "
            "classification grains are supported.",
            "Use a 2-segment register, 3-segment variable, or class/<slug> "
            "classification FQID (the variant grain is out of scope).",
        )
    return fqid


def _parse_class_relation_fqid(field: str, raw: Any) -> Fqid:
    """Parse one classification-only relation endpoint in `class/<slug>` form."""
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='derived_from' `{field}` must be a "
            f"non-empty classification FQID string, got {raw!r}.",
            'Give a classification FQID like "class/ks87-p".',
        )
    try:
        fqid = parse_fqid(raw)
    except FqidError as exc:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='derived_from' `{field}` {raw!r} is not a "
            f"valid FQID: {exc}.",
            'Use the classification form `class/<slug>`, e.g. "class/ks87-p".',
        ) from exc
    if fqid.kind is not FqidKind.CLASSIFICATION:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='derived_from' `{field}` {raw!r} is a "
            f"{fqid.kind.value}-grain FQID; only classification endpoints are "
            "supported.",
            'Use the classification form `class/<slug>`, e.g. "class/ks87-p".',
        )
    return fqid


def load_relations(path: Path | None) -> CuratedRelations:
    """Parse the single `[[edge]]` array from `relations.toml`, dispatching on
    each entry's `type` to per-type validation. Empty when no file (synthetic
    test builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): `type` is one of
    `same_as` / `replaced_by` / `derived_from`; per-type required fields are present and
    well-shaped; a field legal for ANOTHER type is rejected as foreign (a
    mis-typed edge); no self-edge/self-loop; unordered duplicate same_as pairs are
    rejected. Endpoint resolution is deferred to common dependency resolution
    time (the same load/resolve split as the other curation surfaces)."""
    entries = load_curation_entries(
        path,
        entry_key="edge",
        label="relations",
        prefix="relations",
        code_base="relations",
        file_name="curation/relations.toml",
        entry_fields="type + the per-type fields (a/b, from/to, or derived/source)",
    )
    same_as: list[CuratedSameAs] = []
    replaced_by: list[CuratedReplacedBy] = []
    derived_from: list[CuratedClassificationDerivedFrom] = []
    # Unordered FQID pairs already seen — a duplicate is curation drift, not
    # something to silently dedup.
    seen_same_as: set[frozenset[str]] = set()
    seen_derived_from: set[tuple[str, str]] = set()
    for entry in entries:
        edge_type = entry.get("type")
        if not isinstance(edge_type, str) or edge_type not in _EDGE_TYPES:
            raise curation_error(
                "relations_invalid",
                f"relations [[edge]] has missing/unknown `type` {edge_type!r}.",
                f"Set `type` to one of {sorted(_EDGE_TYPES)} in "
                "reg_meta_build/curation/relations.toml.",
            )
        if edge_type == "same_as":
            edge = _load_same_as(entry)
            pair = frozenset({edge.a_fqid(), edge.b_fqid()})
            if pair in seen_same_as:
                raise curation_error(
                    "relations_invalid",
                    f"relations has a duplicate same_as pair "
                    f"{{{edge.a_fqid()}, {edge.b_fqid()}}}.",
                    "List each pair once (same_as is symmetric — a->b and b->a "
                    "are the same edge).",
                )
            seen_same_as.add(pair)
            same_as.append(edge)
        elif edge_type == "replaced_by":
            replaced_by.append(_load_replaced_by(entry))
        else:  # derived_from
            edge = _load_derived_from(entry)
            assert edge.derived.classification is not None
            assert edge.source.classification is not None
            pair = (edge.derived.classification, edge.source.classification)
            if pair in seen_derived_from:
                raise curation_error(
                    "relations_invalid",
                    f"relations has a duplicate derived_from pair "
                    f"{str(edge.derived)!r} -> {str(edge.source)!r}.",
                    "List each derived_from pair once.",
                )
            seen_derived_from.add(pair)
            derived_from.append(edge)
    return CuratedRelations(
        same_as=tuple(same_as),
        replaced_by=tuple(replaced_by),
        derived_from=tuple(derived_from),
    )


# ---------------------------------------------------------------------------
# Materialization — same_as
# ---------------------------------------------------------------------------


def _reject_same_as_cycles(edges: list[tuple[Any, Any]], *, label: str) -> None:
    """Reject cycles in the as-declared same_as graph. `edges` holds ONE direction
    per curated pair (the both-directions duplication happens only at DB insert,
    NOT here) — so a node revisited during DFS means the curated edges genuinely
    close a loop, not the harmless reciprocal of an A->B / B->A mirror pair. A node
    is the FQID key tuple. Pure + DB-free; mirrors `reject_replaced_by_cycles`."""
    if not edges:
        return
    adj: dict[Any, list[Any]] = {}
    for a, b in edges:
        adj.setdefault(a, []).append(b)
        adj.setdefault(b, [])
    color: dict[Any, int] = dict.fromkeys(adj, 0)
    parent: dict[Any, Any] = {}

    def visit(node: Any) -> None:
        color[node] = 1
        for nxt in adj[node]:
            if color[nxt] == 1:
                raise curation_error(
                    "relations_same_as_cycle",
                    f"relations {label} forms a cycle through {node!r}.",
                    "same_as must be acyclic as declared; remove the edge that "
                    "closes the loop.",
                )
            if color[nxt] == 0:
                parent[nxt] = node
                visit(nxt)
        color[node] = 2

    for start in list(adj):
        if color[start] == 0:
            visit(start)


def _reject_oversized_components(edges: list[tuple[Any, Any]], *, label: str) -> None:
    """Refuse any same_as edge that would merge two identity components into one
    larger than `_SAME_AS_MAX_COMPONENT` distinct FQIDs (#522). A union-find over
    the undirected edge set; a component above the cap is almost certainly a
    curation error welding distinct concepts, not a real identity cluster. `edges`
    holds ONE direction per curated pair (the both-directions duplication happens
    only at DB insert, NOT here); union-find is direction-agnostic, so a single
    A--B edge suffices to merge the two endpoints' components."""
    parent: dict[Any, Any] = {}

    def find(x: Any) -> Any:
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    sizes: dict[Any, int] = {}
    for node in parent:
        root = find(node)
        sizes[root] = sizes.get(root, 0) + 1
    for size in sizes.values():
        if size > _SAME_AS_MAX_COMPONENT:
            raise curation_error(
                "relations_same_as_component_too_large",
                f"relations {label} forms an identity component of {size} FQIDs "
                f"(cap {_SAME_AS_MAX_COMPONENT}); an edge likely welds two "
                "distinct concepts.",
                "Split the curated same_as cluster — a real identity group is "
                "small. Remove the bridging edge or raise _SAME_AS_MAX_COMPONENT "
                "in reg_meta_build/relations.py if the cluster is genuine.",
            )


# ---------------------------------------------------------------------------
# Materialization — classification derived_from
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Materialization — replaced_by (combined with the event-derived pass)
# ---------------------------------------------------------------------------


def reject_replaced_by_cycles(edges: list[tuple[Any, Any]]) -> None:
    """Reject directed cycles in a `replaced_by` succession graph.

    `edges` is a list of `(predecessor_node, successor_node)` pairs; a node is any
    hashable key (the build passes the FQID slug tuple — register node
    `(provider, register)`, variable node `(provider, register, variable)`). A
    cyclic succession graph has no terminal successor, so the webapp's
    successors()/predecessors() walks would contradict each other.

    Pure + DB-free so it's testable in isolation. The build runs it on the
    COMBINED per-grain graph (event-derived edges + curated edges to insert) — a
    curated edge can close a cycle with an event-derived one, which a curated-only
    view can't see."""
    if not edges:
        return
    adj: dict[Any, list[Any]] = {}
    for a, b in edges:
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
                raise curation_error(
                    "replaced_by_cycle",
                    "relations replaced_by forms a succession cycle: "
                    f"{' -> '.join(repr(n) for n in reversed(cycle))}.",
                    "A succession chain must be acyclic (it needs a terminal "
                    "successor); remove the edge that closes the loop.",
                )
            if color[nxt] == 0:
                parent[nxt] = node
                visit(nxt)
        color[node] = 2

    for start in list(adj):
        if color[start] == 0:
            visit(start)


def _strongly_connected_components(
    adj: dict[Any, list[tuple[Any, int | None]]],
) -> list[set[Any]]:
    """Tarjan's strongly-connected-components over a year-annotated adjacency map
    (the year on each edge is ignored here — only reachability matters). Returns one
    `set` per SCC. Pure + DB-free.

    Iterative (explicit stack) so a deep succession chain can't blow Python's
    recursion limit. Every node a cycle passes through lands in an SCC of size ≥2;
    a self-loop lands in a size-1 SCC (the caller then inspects its in-component
    edges). A node on no cycle is its own singleton SCC."""
    index_of: dict[Any, int] = {}
    low: dict[Any, int] = {}
    on_stack: set[Any] = set()
    stack: list[Any] = []
    components: list[set[Any]] = []
    counter = 0

    for root, root_succs in adj.items():
        if root in index_of:
            continue
        # work stack of (node, iterator over its successors)
        work: list[tuple[Any, Any]] = [(root, iter(root_succs))]
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, it = work[-1]
            descended = False
            for nxt, _year in it:
                if nxt not in index_of:
                    index_of[nxt] = low[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, iter(adj[nxt])))
                    descended = True
                    break
                if nxt in on_stack:
                    low[node] = min(low[node], index_of[nxt])
            if descended:
                continue
            # node fully explored: if it's a root of an SCC, pop the component.
            if low[node] == index_of[node]:
                comp: set[Any] = set()
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.add(w)
                    if w == node:
                        break
                components.append(comp)
            work.pop()
            if work:
                parent_node = work[-1][0]
                low[parent_node] = min(low[parent_node], low[node])
    return components


def reject_nonmonotone_representation_cycles(
    edges: list[tuple[Any, Any, int | None]],
) -> None:
    """Reject NON-time-monotone cycles in a representation `replaced_by` graph
    (#846), permitting a faithful temporal round-trip.

    `edges` is a list of `(predecessor_node, successor_node, effective_year)`; a
    node is the full representation key `(provider, register, variable, column,
    variant)`. UNLIKE the topological `reject_replaced_by_cycles` (used for the
    entity grains, which must be strictly acyclic), a representation succession MAY
    be cyclic when scoped to one variant and the cycle is a time-monotone
    round-trip: a column left and LATER returned (FRIDA's firm key
    `borgnr -(2014)-> persorgnr -(2018)-> borgnr` within `punktskatter-for-energi`).

    A cycle is PERMITTED iff it is a SINGLE simple time-monotone round-trip: the
    nodes/edges in the cyclic region form exactly one elementary cycle whose edges'
    `effective_year`s are all present and all DISTINCT and admit a single consistent
    forward ordering — rotating the cycle so it starts at its earliest-year edge
    yields strictly increasing years (one wrap at the close). For the 2-cycle (the
    only real case today) this reduces to "two distinct present years". REJECTED,
    all EXIT_CONFIG with actionable messages:
      - any edge in the cyclic region lacks `effective_year` (ordering undefined),
      - two edges in the cyclic region share an `effective_year` (ambiguous /
        impossible round-trip),
      - the distinct years don't form a single monotone wrap (impossible multi-wrap
        order), or
      - the cyclic region is more tangled than one elementary cycle (multiple
        interleaved cycles, or a node with two intra-region successors — can't be a
        clean round-trip).

    Completeness: the cyclic region is found via strongly-connected components
    (Tarjan). EVERY non-trivial SCC (≥2 nodes, or a self-loop) is validated — there
    is no DFS short-circuit that lets a later, more complex cycle slip past once an
    earlier monotone cycle has finished a shared node (the white/gray/black DFS this
    replaced had exactly that gap: an edge into a finished node fell through
    unchecked). A self-loop (`A -> A`) is a 1-node SCC with an in-component edge, so
    it is caught and rejected.

    A NON-cyclic graph (e.g. RTB's single variable-level edge) has only trivial SCCs
    and passes unchanged. Pure + DB-free so it's testable in isolation, like
    `reject_replaced_by_cycles`."""
    if not edges:
        return
    # adjacency keeps the year on each forward edge so a detected cycle can be
    # validated against the round-trip rule (not merely rejected).
    adj: dict[Any, list[tuple[Any, int | None]]] = {}
    for a, b, year in edges:
        adj.setdefault(a, []).append((b, year))
        adj.setdefault(b, [])

    def _reject(nodes: list[Any], years: list[int | None], reason: str) -> None:
        path = " -> ".join(repr(n) for n in nodes)
        raise curation_error(
            "replaced_by_cycle",
            f"relations replaced_by forms a NON-monotone representation "
            f"succession cycle ({reason}): {path} (years {years}).",
            "A variant-scoped representation cycle is permitted only as a SINGLE "
            "time-monotone round-trip — every edge needs a DISTINCT `effective_year` "
            "ordering the return, and the cyclic columns must form exactly one loop. "
            "Give each edge a distinct year, untangle multiple loops, or remove the "
            "edge that closes the loop.",
        )

    def _validate_component(nodes: set[Any]) -> None:
        # `nodes` is one strongly-connected component (≥2 nodes, or a 1-node SCC
        # with a self-loop) — it contains at least one cycle. The ONLY permitted
        # shape is a single elementary cycle: every node has exactly one successor
        # INSIDE the component (in-degree/out-degree 1), so the in-component edges
        # form one simple loop visiting all `nodes` once. Anything else (a node with
        # two in-component successors → interleaved cycles, or a self-loop) is
        # rejected before the year check.
        in_edges: list[tuple[Any, Any, int | None]] = [
            (a, b, year) for a in nodes for b, year in adj[a] if b in nodes
        ]
        ordered = sorted(nodes, key=repr)
        # A self-loop is an in-component edge a -> a; reject it explicitly (it can't
        # be a temporal round-trip — a column can't succeed itself).
        if any(a == b for a, b, _ in in_edges):
            _reject(ordered, [], "a column succeeds itself (self-loop)")
        succ_in: dict[Any, list[tuple[Any, int | None]]] = {n: [] for n in nodes}
        for a, b, year in in_edges:
            succ_in[a].append((b, year))
        if any(len(s) != 1 for s in succ_in.values()):
            _reject(
                ordered,
                [],
                "the cyclic columns don't form a single loop (a column has "
                "multiple in-cycle successors — interleaved round-trips)",
            )
        # Out-degree 1 for every node in a strongly-connected set ⇒ exactly one
        # elementary cycle covering all nodes. Walk it from an arbitrary start to
        # recover the ordered nodes + their edge-years.
        start = ordered[0]
        cycle_nodes = [start]
        cycle_years: list[int | None] = []
        cur = start
        while True:
            nxt, year = succ_in[cur][0]
            cycle_years.append(year)
            if nxt == start:
                cycle_nodes.append(nxt)
                break
            cycle_nodes.append(nxt)
            cur = nxt
        # `cycle_nodes` = [start, ..., start] (closed); `cycle_years[i]` is the year
        # of the edge cycle_nodes[i] -> cycle_nodes[i+1].
        if any(y is None for y in cycle_years):
            _reject(cycle_nodes, cycle_years, "an edge lacks effective_year")
        if len(set(cycle_years)) != len(cycle_years):
            _reject(cycle_nodes, cycle_years, "two edges share an effective_year")
        # Rotate to start at the minimum-year edge; a faithful round-trip is then
        # strictly increasing (one wrap, already cut at the min). `cycle_years` has
        # no None/dups past the guards above.
        years = [y for y in cycle_years if y is not None]
        rot = years.index(min(years))
        rotated = years[rot:] + years[:rot]
        if any(rotated[i] >= rotated[i + 1] for i in range(len(rotated) - 1)):
            _reject(
                cycle_nodes, cycle_years, "years are not a single monotone round-trip"
            )

    for component in _strongly_connected_components(adj):
        # Trivial SCC (1 node, no self-loop) has no cycle; a self-loop's SCC is a
        # single node WITH an in-component edge, caught inside _validate_component.
        if len(component) > 1 or any(
            b in component for b, _ in adj[next(iter(component))]
        ):
            _validate_component(component)


# ---------------------------------------------------------------------------
# Derivation — variable vintage succession (#584, lifts classification editions)
# ---------------------------------------------------------------------------


def variable_vintage_succession_edges(
    bindings: Iterable[tuple[str, str, str, str]],
    edition_edges: Iterable[tuple[str, str, int | None]],
) -> tuple[tuple[str, str, int | None], ...]:
    """Lift adjacent classification editions within unambiguous variable streams.

    Bindings carry (register FQID, variable name, variable slug, classification
    slug). The existing rule retains population/role/level tokens in each stream,
    excludes variables spanning multiple chained editions, and never guesses a
    match when a stream has several variables on either side.
    """
    edges = tuple(edition_edges)
    chained = {slug for a, b, _year in edges for slug in (a, b)}
    families: dict[tuple[str, str], dict[str, set[str]]] = {}
    for register, name, variable, classification in bindings:
        if classification in chained:
            families.setdefault((register, name), {}).setdefault(variable, set()).add(
                classification
            )

    pending: list[tuple[str, str, int | None]] = []
    for (register, _name), variables in sorted(families.items()):
        edition_variables: dict[str, set[str]] = {}
        for variable, classifications in variables.items():
            if len(classifications) == 1:
                edition_variables.setdefault(next(iter(classifications)), set()).add(
                    variable
                )
        for predecessor, successor, year in edges:
            streams: list[dict[tuple[str, ...], set[str]]] = []
            for edition in (predecessor, successor):
                by_stream: dict[tuple[str, ...], set[str]] = {}
                for variable in edition_variables.get(edition, ()):
                    stream = _variable_vintage_stream_key(
                        variable, predecessor, successor
                    )
                    by_stream.setdefault(stream, set()).add(variable)
                streams.append(by_stream)
            before, after = streams
            for stream in sorted(before.keys() & after.keys()):
                if len(before[stream]) == len(after[stream]) == 1:
                    a, b = next(iter(before[stream])), next(iter(after[stream]))
                    if a != b:
                        pending.append((f"{register}/{a}", f"{register}/{b}", year))
    return tuple(sorted(pending, key=lambda edge: edge[:2]))
