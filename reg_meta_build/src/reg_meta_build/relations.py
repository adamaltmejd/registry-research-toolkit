"""One typed curation surface for the curated pairwise-relation facts (#522).

Four maintainer-authored relation surfaces used to live apart — `same_as` (split
between `variable_same_as.toml` and an inline `same_as` field on the slug TOMLs),
`related_to` (`variable_related_to.toml`), and `replaced_by` (a top-level
`[[replaced_by]]` array inside the slug TOMLs). They are three *kinds* of the
same thing — a curated assertion about a pair of catalog entities — so they now
share ONE file (`reg_meta_build/curation/relations.toml`) as a single `[[edge]]`
array discriminated by `type`, and ONE loader + materializer (this module).

The three relation kinds are genuinely different relations, and the materializer
keeps every prior behavior verbatim (the DB output is byte-identical — gated by
dbdiff):

  - `same_as` — symmetric, transitive IDENTITY ("one concept, two FQIDs").
    RESOLVER-LOAD-BEARING: `Catalog.resolve` follows it transitively and the
    build cycle-checks the as-declared graph, so a wrong edge corrupts
    resolution. Variable grain (3-seg `provider/register/variable`) OR
    classification grain (2-seg `provider/classification_slug`) — never mixed.
    Lands in `variable_same_as` / `classification_same_as`, both directions.
  - `replaced_by` — directional SUCCESSION (predecessor superseded by successor).
    NOT identity: the definitions differ across eras. Register grain (2-seg) OR
    variable grain (3-seg). The predecessor MAY be dead (retired / renamed /
    cross-provider — the whole reason this exists alongside the
    `timeseries_event`-derived edges, which can express neither). Lands in
    `register_replaced_by` / `variable_replaced_by`, one direction, sharing the
    event pass's seen-PK sets so a curated edge dedups against an event one and
    the combined per-grain graph is cycle-checked.
  - `related_to` — weak "see also" DISCOVERY link between distinct concepts. Lands
    in `variable_related_to` (the same table the A2.2 triage feeds with
    `auto:triage` split-sibling edges) but on a DISJOINT relation-kind vocabulary
    (`CURATED_RELATION_KINDS`) so a curated weak link can never be mistaken for —
    or folded as — a split sibling. The auto kind `same_definition_different_column`
    is foldable by the concept-group edge pass and is REJECTED here.

The same-as candidate GENERATOR (`infer_same_as_candidates`, #508) stays in
`variable_same_as.py`; it reads structured signals off a built DB and emits a
review worklist whose schema is this loader's `[[edge]] type = "same_as"` input,
so a confirmed candidate copies across verbatim. It imports `CuratedSameAs` /
`load_relations` from here.

Like the other curation TOMLs (`concept_groups.toml`, `curation/scb/`) the file
is a maintainer artifact — absent in wheel installs and synthetic test builds.
"""

from __future__ import annotations

import functools
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import Fqid, FqidError, FqidKind, parse as parse_fqid

from ._curation import curation_error, load_curation_entries, require_fqid

if TYPE_CHECKING:
    from collections.abc import Iterable

# ── relation kind vocabularies ──────────────────────────────────────────────

# The legal `type` discriminators. Surfaced in the unknown-type error so a typo
# is self-correcting.
_EDGE_TYPES: frozenset[str] = frozenset({"same_as", "replaced_by", "related_to"})

# Curated `related_to` relation-kind vocabulary. Grows with curation needs (add
# the kind here + document its meaning). MUST stay disjoint from the auto:triage
# kind `same_definition_different_column` — that kind is foldable by the
# concept-group edge pass, and a curated "see also" must never fold.
CURATED_RELATION_KINDS: frozenset[str] = frozenset({"similar_concept"})

# The auto:triage kind the concept-group edge pass folds on. Listed here only to
# assert (load-time + in tests) that no curated kind aliases it.
_AUTO_FOLDABLE_KIND = "same_definition_different_column"

# Default `note` for a curated related_to edge that doesn't set one — provenance
# marker distinguishing these rows from the auto:triage edges in the same table.
_CURATED_RELATED_NOTE_DEFAULT = "curated"

# Provenance markers for the two replaced_by sources (mirrors db.py so a consumer
# can tell curated from auto-derived). A curated row's own `note` (the human
# transition reason) lands in `beskrivning`; this fixed marker lands in `note`.
_REPLACED_BY_NOTE_CURATED = "curated:slug_toml"

# same_as component-size guard (#522). A same_as edge MERGES two identity
# components into one; a single mistaken curated edge can therefore silently weld
# two large, genuinely-distinct concept clusters into one resolver blob. Refuse
# any edge whose merged component would exceed this many distinct FQIDs — a
# curated identity cluster that large is almost certainly a curation error, not a
# real concept. Forward-looking: the file ships EMPTY, so this guards future
# curation rather than today's data.
_SAME_AS_MAX_COMPONENT = 32

# Replaced_by grains: register- or variable-grain only. The variant grain is
# deliberately out of scope — a variant is a delivery coordinate, not a curation
# surface for cross-provider succession.
_REPLACED_BY_GRAINS: frozenset[FqidKind] = frozenset(
    {FqidKind.REGISTER, FqidKind.VARIABLE_BINDING}
)

# Per-type accepted/foreign field maps (besides `type`). A field legal for one
# type is a FOREIGN key on another (e.g. `effective_year` on a same_as edge) and
# rejected — this catches a mis-typed edge (right fields, wrong `type`) at load.
_SAME_AS_FIELDS: frozenset[str] = frozenset({"a", "b", "note"})
_REPLACED_BY_FIELDS: frozenset[str] = frozenset(
    {"from", "to", "effective_year", "note"}
)
_RELATED_TO_FIELDS: frozenset[str] = frozenset({"a", "b", "relation_kind", "note"})

_VarKey = tuple[str, str, str]
_ClassKey = tuple[str, str]


# ── dataclasses ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedSameAs:
    """One `type = "same_as"` identity edge: an UNORDERED pair of FQIDs (`a` / `b`,
    same grain) plus an optional `note`. There is NO `relation_kind` — same_as
    carries no kind vocabulary; identity is identity.

    Both endpoints are either variable-grain (`a_provider/a_register/a_variable`,
    `b_*` the mirror) or classification-grain (`a_register`/`b_register` carry the
    classification slug, `a_variable`/`b_variable` are None). `grain` records
    which. Endpoint resolution happens at materialize time against the built DB,
    not at load; the variable/classification slug is NOT validated — same_as is
    slug-anchored and survives renames."""

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
    `Fqid`s of the SAME grain — both register (2 segs) or both variable (3 segs).
    `note` / `effective_year` are optional provenance. Existence (the successor
    must resolve to a live, slugged DB entity; the predecessor MAY be dead) is
    checked downstream against the built DB — this loader stays DB-free."""

    predecessor: Fqid
    successor: Fqid
    note: str | None
    effective_year: int | None


@dataclass(frozen=True)
class CuratedRelatedTo:
    """One `type = "related_to"` "see also" edge: an UNORDERED pair of variable
    FQIDs (`a` / `b`, 3-segment), the curated `relation_kind`, and an optional
    `note`. Endpoint resolution happens at materialize time against the built
    DB."""

    a_provider: str
    a_register: str
    a_variable: str
    b_provider: str
    b_register: str
    b_variable: str
    relation_kind: str
    note: str | None


@dataclass(frozen=True)
class CuratedRelations:
    """The parsed `relations.toml`, grouped by relation kind. One load yields all
    three; the build materializes each into its own table(s)."""

    same_as: tuple[CuratedSameAs, ...]
    replaced_by: tuple[CuratedReplacedBy, ...]
    related_to: tuple[CuratedRelatedTo, ...]


def _join_fqid(provider: str, register: str, variable: str | None) -> str:
    return f"{provider}/{register}" + (f"/{variable}" if variable is not None else "")


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def repo_relations_path() -> Path | None:
    """`reg_meta_build/curation/relations.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs). Lives under `curation/` (cross-provider), parallel to
    `curation/scb/`."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent / "curation" / "relations.toml"
    )
    return candidate if candidate.is_file() else None


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
    SAME grain (register or variable — not variant). No self-loop. The
    predecessor may be dead (not resolved at load)."""
    _reject_foreign_fields(entry, "replaced_by", _REPLACED_BY_FIELDS)
    predecessor = _parse_replaced_by_fqid("from", entry.get("from"))
    successor = _parse_replaced_by_fqid("to", entry.get("to"))
    if predecessor.kind is not successor.kind:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `from` {str(predecessor)!r} "
            f"({predecessor.kind.value}) and `to` {str(successor)!r} "
            f"({successor.kind.value}) are different grains.",
            "Both endpoints must be the same grain (register->register or "
            "variable->variable).",
        )
    if str(predecessor) == str(successor):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' self-loop on {str(predecessor)!r}.",
            "An entity cannot replace itself; remove the edge.",
        )
    note = _require_note(entry, "replaced_by")
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
    return CuratedReplacedBy(
        predecessor=predecessor,
        successor=successor,
        note=note,
        effective_year=effective_year,
    )


def _parse_replaced_by_fqid(field: str, raw: Any) -> Fqid:
    """Parse one replaced_by endpoint FQID string against the FQID grammar,
    restricted to the register / variable grains."""
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` must be a "
            f"non-empty FQID string, got {raw!r}.",
            'Quote a register or variable FQID, e.g. "scb/lisa" or "scb/lisa/kon".',
        )
    try:
        fqid = parse_fqid(raw)
    except FqidError as exc:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` {raw!r} is not a "
            f"valid FQID: {exc}.",
            "Use a register (provider/register) or variable "
            "(provider/register/variable) FQID.",
        ) from exc
    if fqid.kind not in _REPLACED_BY_GRAINS:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` {raw!r} is a "
            f"{fqid.kind.value}-grain FQID; only register and variable grains "
            "are supported.",
            "Use a 2-segment register or 3-segment variable FQID (the variant "
            "grain is out of scope).",
        )
    return fqid


def _load_related_to(entry: dict) -> CuratedRelatedTo:
    """Validate one `type = "related_to"` edge. `a` / `b` are 3-seg variable
    FQIDs; `relation_kind` is in `CURATED_RELATION_KINDS` (the auto foldable kind
    is rejected so a weak link can never fold). No self-edge."""
    _reject_foreign_fields(entry, "related_to", _RELATED_TO_FIELDS)
    a = _require_fqid_variable(entry, "a")
    b = _require_fqid_variable(entry, "b")
    if a == b:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='related_to' relates {'/'.join(a)} to itself.",
            "A see-also edge connects two DISTINCT variables; remove the self-edge.",
        )
    kind = entry.get("relation_kind")
    if not isinstance(kind, str) or not kind:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='related_to' needs `relation_kind` as a "
            f"non-empty string, got {kind!r}.",
            f'Use `relation_kind = "<kind>"` with a kind in '
            f"{sorted(CURATED_RELATION_KINDS)}.",
        )
    if kind not in CURATED_RELATION_KINDS:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='related_to' relation_kind {kind!r} is not "
            f"a curated kind {sorted(CURATED_RELATION_KINDS)} (the auto:triage "
            f"kind {_AUTO_FOLDABLE_KIND!r} is foldable and is rejected here).",
            "Use a curated relation_kind, or add the new kind to "
            "CURATED_RELATION_KINDS in reg_meta_build/relations.py.",
        )
    note = _require_note(entry, "related_to")
    return CuratedRelatedTo(
        a_provider=a[0],
        a_register=a[1],
        a_variable=a[2],
        b_provider=b[0],
        b_register=b[1],
        b_variable=b[2],
        relation_kind=kind,
        note=note,
    )


def load_relations(path: Path | None) -> CuratedRelations:
    """Parse the single `[[edge]]` array from `relations.toml`, dispatching on
    each entry's `type` to per-type validation. Empty when no file (synthetic
    test builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): `type` is one of
    `same_as` / `replaced_by` / `related_to`; per-type required fields are
    present and well-shaped; a field legal for ANOTHER type is rejected as
    foreign (a mis-typed edge); no self-edge/self-loop; unordered duplicate pairs
    within same_as and related_to are rejected. Endpoint RESOLUTION against the
    built DB is deferred to materialize time (the same load/resolve split as the
    other curation surfaces)."""
    entries = load_curation_entries(
        path,
        entry_key="edge",
        label="relations",
        prefix="relations",
        code_base="relations",
        file_name="curation/relations.toml",
        entry_fields="type + the per-type fields (a/b or from/to)",
    )
    same_as: list[CuratedSameAs] = []
    replaced_by: list[CuratedReplacedBy] = []
    related_to: list[CuratedRelatedTo] = []
    # Unordered FQID pairs already seen per pair-typed kind — a duplicate is
    # curation drift, not something to silently dedup.
    seen_same_as: set[frozenset[str]] = set()
    seen_related: set[frozenset[tuple[str, str, str]]] = set()
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
        else:  # related_to
            rel = _load_related_to(entry)
            rpair = frozenset(
                {
                    (rel.a_provider, rel.a_register, rel.a_variable),
                    (rel.b_provider, rel.b_register, rel.b_variable),
                }
            )
            if rpair in seen_related:
                raise curation_error(
                    "relations_invalid",
                    f"relations has a duplicate related_to pair "
                    f"{{{'/'.join((rel.a_provider, rel.a_register, rel.a_variable))}, "
                    f"{'/'.join((rel.b_provider, rel.b_register, rel.b_variable))}}}.",
                    "List each variable pair once (the edge is symmetric).",
                )
            seen_related.add(rpair)
            related_to.append(rel)
    return CuratedRelations(
        same_as=tuple(same_as),
        replaced_by=tuple(replaced_by),
        related_to=tuple(related_to),
    )


# ---------------------------------------------------------------------------
# Materialization — same_as
# ---------------------------------------------------------------------------


def _register_exists(
    conn: sqlite3.Connection, provider_slug: str, register_slug: str
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM register r "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND r.slug = ?",
        (provider_slug, register_slug),
    ).fetchone()
    return row is not None


def _classification_slugs(conn: sqlite3.Connection) -> set[_ClassKey]:
    """Live `(provider, classification_slug)` pairs — the universe a curated
    classification same_as endpoint must resolve into. Classifications carry no
    provider in classifications.toml; the publisher field is the provider."""
    out: set[_ClassKey] = set()
    for slug, publisher in conn.execute(
        "SELECT slug, publisher FROM classification WHERE slug IS NOT NULL"
    ):
        out.add(((publisher or "scb").lower(), slug))
    return out


def _reject_same_as_cycles(edges: list[tuple[Any, Any]], *, label: str) -> None:
    """Reject directed cycles in the as-declared same_as graph (which is stored
    both directions, so any genuine reciprocal IS a 2-cycle). A node is the FQID
    key tuple. Pure + DB-free; mirrors `reject_replaced_by_cycles`."""
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
    curation error welding distinct concepts, not a real identity cluster. Edges
    are stored both directions, so each undirected pair appears twice — the
    union-find collapses that harmlessly."""
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


def materialize_same_as(
    conn: sqlite3.Connection,
    same_as: Iterable[CuratedSameAs],
    *,
    providers: frozenset[str] = frozenset(),
) -> dict[str, int]:
    """Write curated `same_as` identity edges (both directions) into
    `variable_same_as` / `classification_same_as`. Provider-gated (an edge whose
    endpoint provider isn't built is SKIPPED — a partial `--providers` build
    can't represent it, deferral not drift). Variable endpoints validate
    provider+register existence (the variable slug is NOT checked — same_as is
    slug-anchored); classification endpoints validate `(provider, slug)`
    presence. The combined as-declared graph is cycle-checked and the
    component-size guard refuses a runaway identity cluster, BOTH before any
    INSERT. Returns `{"variable": n, "classification": n}` (one per pair; both
    directions written)."""
    var_edges: list[tuple[_VarKey, _VarKey]] = []
    class_edges: list[tuple[_ClassKey, _ClassKey]] = []
    class_universe: set[_ClassKey] | None = None

    for e in same_as:
        if e.a_provider not in providers or e.b_provider not in providers:
            continue
        if e.grain is FqidKind.VARIABLE_BINDING:
            if not _register_exists(conn, e.a_provider, e.a_register):
                raise _unknown_same_as_endpoint(e.a_fqid(), "a", "register")
            if not _register_exists(conn, e.b_provider, e.b_register):
                raise _unknown_same_as_endpoint(e.b_fqid(), "b", "register")
            a_key: _VarKey = (e.a_provider, e.a_register, e.a_variable or "")
            b_key: _VarKey = (e.b_provider, e.b_register, e.b_variable or "")
            var_edges.append((a_key, b_key))
        else:  # CLASSIFICATION
            if class_universe is None:
                class_universe = _classification_slugs(conn)
            a_ck: _ClassKey = (e.a_provider, e.a_register)
            b_ck: _ClassKey = (e.b_provider, e.b_register)
            if a_ck not in class_universe:
                raise _unknown_same_as_endpoint(e.a_fqid(), "a", "classification")
            if b_ck not in class_universe:
                raise _unknown_same_as_endpoint(e.b_fqid(), "b", "classification")
            class_edges.append((a_ck, b_ck))

    _reject_same_as_cycles(list(var_edges), label="variable same_as")
    _reject_same_as_cycles(list(class_edges), label="classification same_as")
    _reject_oversized_components(
        [(a, b) for a, b in var_edges], label="variable same_as"
    )
    _reject_oversized_components(
        [(a, b) for a, b in class_edges], label="classification same_as"
    )

    for a, b in var_edges:
        for src_t, tgt_t in ((a, b), (b, a)):
            conn.execute(
                "INSERT INTO variable_same_as ("
                "a_provider, a_register, a_variable, "
                "b_provider, b_register, b_variable) VALUES (?, ?, ?, ?, ?, ?)",
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


def _unknown_same_as_endpoint(fqid: str, side: str, grain: str) -> Exception:
    return curation_error(
        "relations_same_as_unknown_endpoint",
        f"relations same_as edge endpoint {fqid!r} names a {grain} that does "
        "not exist in this build.",
        f"Fix the `{side}` FQID in reg_meta_build/curation/relations.toml.",
    )


# ---------------------------------------------------------------------------
# Materialization — related_to
# ---------------------------------------------------------------------------


def _resolve_variable_id(
    conn: sqlite3.Connection, provider: str, register: str, variable: str
) -> int | None:
    row = conn.execute(
        "SELECT v.variable_id FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
        (provider, register, variable),
    ).fetchone()
    return row[0] if row is not None else None


def materialize_related_to(
    conn: sqlite3.Connection,
    related_to: Iterable[CuratedRelatedTo],
    *,
    providers: frozenset[str] = frozenset(),
) -> int:
    """Write curated "see also" edges (both directions) into `variable_related_to`
    on the curated (non-foldable) relation-kind vocabulary. Provider-gated (an
    out-of-build endpoint is SKIPPED). An edge whose providers ARE built but whose
    variable doesn't resolve IS drift -> fail fast. A PK collision with an
    existing edge (auto:triage sibling or another curated edge) is curation drift,
    not a benign re-add -> fail loud. Returns the row count inserted (both
    directions counted)."""
    n_inserted = 0
    for e in related_to:
        if e.a_provider not in providers or e.b_provider not in providers:
            continue
        a_fqid = f"{e.a_provider}/{e.a_register}/{e.a_variable}"
        b_fqid = f"{e.b_provider}/{e.b_register}/{e.b_variable}"
        if _resolve_variable_id(conn, e.a_provider, e.a_register, e.a_variable) is None:
            raise curation_error(
                "relations_related_to_unresolved",
                f"relations related_to edge endpoint {a_fqid!r} does not resolve "
                "to a variable.",
                "Fix the `a` FQID in reg_meta_build/curation/relations.toml.",
            )
        if _resolve_variable_id(conn, e.b_provider, e.b_register, e.b_variable) is None:
            raise curation_error(
                "relations_related_to_unresolved",
                f"relations related_to edge endpoint {b_fqid!r} does not resolve "
                "to a variable.",
                "Fix the `b` FQID in reg_meta_build/curation/relations.toml.",
            )
        note = e.note if e.note is not None else _CURATED_RELATED_NOTE_DEFAULT
        # Plain INSERT (NOT OR IGNORE): a PK collision is curation drift, not a
        # benign re-add — fail loud rather than silently drop the curated
        # kind/note.
        try:
            conn.executemany(
                "INSERT INTO variable_related_to "
                "(a_provider, a_register, a_variable, b_provider, b_register, "
                " b_variable, relation_kind, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        e.a_provider,
                        e.a_register,
                        e.a_variable,
                        e.b_provider,
                        e.b_register,
                        e.b_variable,
                        e.relation_kind,
                        note,
                    ),
                    (
                        e.b_provider,
                        e.b_register,
                        e.b_variable,
                        e.a_provider,
                        e.a_register,
                        e.a_variable,
                        e.relation_kind,
                        note,
                    ),
                ],
            )
        except sqlite3.IntegrityError as exc:
            raise curation_error(
                "relations_related_to_collision",
                f"relations related_to curated edge {{{a_fqid}, {b_fqid}}} "
                "collides with an edge already present (auto:triage sibling or "
                "another curated edge).",
                "Remove the duplicate edge from "
                "reg_meta_build/curation/relations.toml.",
            ) from exc
        n_inserted += 2
    return n_inserted


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


def _slugged_register_fqids(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    return {
        (p_slug, r_slug)
        for r_slug, p_slug in conn.execute(
            "SELECT r.slug, p.slug FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.slug IS NOT NULL"
        )
    }


def _slugged_variable_fqids(conn: sqlite3.Connection) -> set[tuple[str, str, str]]:
    return {
        (p_slug, r_slug, v_slug)
        for v_slug, r_slug, p_slug in conn.execute(
            "SELECT v.slug, r.slug, p.slug FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL"
        )
    }


def _unresolved_curated_successor(successor: Fqid, grain_noun: str) -> Exception:
    return curation_error(
        "replaced_by_unresolved_successor",
        f"Curated replaced_by successor {str(successor)!r} does not resolve to a "
        f"live, slugged {grain_noun} in this build.",
        f"A curated successor must exist; fix the FQID or add the {grain_noun} slug.",
    )


def materialize_curated_replaced_by(
    conn: sqlite3.Connection,
    edges: Iterable[CuratedReplacedBy],
    seen_register: set[tuple[str, str, str, str]],
    seen_variable: set[tuple[str, str, str, str, str, str]],
    *,
    providers: frozenset[str],
    progress: Any,
) -> dict[str, int]:
    """Materialize curated `replaced_by` succession edges (#440 — now from
    `relations.toml`). Runs right after the event-derived pass
    (`_materialize_replaced_by_edges` in db.py), SHARING its `seen_register` /
    `seen_variable` PK sets so a curated edge dedups against an event-derived one
    (and against another curated row). The rows are parsed/shape-validated DB-free
    by `load_relations`; this pass does the DB-aware existence checks, the
    COMBINED-graph cycle check, and the INSERTs.

    Acyclicity: the load-time check sees only the curated edges, so it can't catch
    a curated edge that closes a cycle WITH an event-derived edge (event A->B +
    curated B->A). This pass reconstructs the event edges from the shared `seen_*`
    PK tuples and runs `reject_replaced_by_cycles` on the combined per-grain graph
    (event + curated-to-insert) BEFORE any INSERT, so a cycle aborts cleanly.

    Resolution rules: the SUCCESSOR must resolve to a live, slugged DB entity (a
    non-resolving successor is a CURATION ERROR -> fail fast), EXCEPT a successor
    whose PROVIDER isn't in this (partial) build, which is SKIPPED. The
    PREDECESSOR MAY be dead — inserted VERBATIM (slug-anchored); its provider is
    never gated. `note = 'curated:slug_toml'` marks provenance; the row's own
    `note` lands in `beskrivning`. Returns `{"register", "variable",
    "skipped_duplicate", "skipped_inactive_provider"}`."""
    edges = list(edges)
    if not edges:
        return {
            "register": 0,
            "variable": 0,
            "skipped_duplicate": 0,
            "skipped_inactive_provider": 0,
        }

    progress("Materializing curated replaced_by edges from relations.toml...")
    live_registers = _slugged_register_fqids(conn)
    live_variables = _slugged_variable_fqids(conn)

    # Reconstruct the event-derived edges from the shared seen-set PK tuples
    # snapshotted at pass entry, so the combined-graph cycle check sees them:
    #   register PK (pp, pr, sp, sr)         -> (pp, pr) -> (sp, sr)
    #   variable PK (pp, pr, pv, sp, sr, sv) -> (pp, pr, pv) -> (sp, sr, sv)
    register_cycle_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
        (pk[:2], pk[2:]) for pk in seen_register
    ]
    variable_cycle_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
        (pk[:3], pk[3:]) for pk in seen_variable
    ]

    n_skipped_duplicate = 0
    n_skipped_inactive_provider = 0
    pending_register: list[tuple] = []
    pending_variable: list[tuple] = []

    for edge in edges:
        pred = edge.predecessor
        succ = edge.successor
        assert succ.provider is not None
        if succ.provider not in providers:
            n_skipped_inactive_provider += 1
            continue
        if succ.kind is FqidKind.REGISTER:
            assert succ.provider is not None and succ.register is not None
            assert pred.provider is not None and pred.register is not None
            succ_key = (succ.provider, succ.register)
            if succ_key not in live_registers:
                raise _unresolved_curated_successor(succ, "register")
            pk = (pred.provider, pred.register, succ.provider, succ.register)
            if pk in seen_register:
                n_skipped_duplicate += 1
                continue
            seen_register.add(pk)
            register_cycle_edges.append(((pred.provider, pred.register), succ_key))
            pending_register.append(
                (*pk, edge.effective_year, _REPLACED_BY_NOTE_CURATED, edge.note)
            )
        else:  # FqidKind.VARIABLE_BINDING (the loader admits only these two grains)
            assert (
                succ.provider is not None
                and succ.register is not None
                and succ.variable is not None
            )
            assert (
                pred.provider is not None
                and pred.register is not None
                and pred.variable is not None
            )
            succ_key = (succ.provider, succ.register, succ.variable)
            if succ_key not in live_variables:
                raise _unresolved_curated_successor(succ, "variable")
            pk = (
                pred.provider,
                pred.register,
                pred.variable,
                succ.provider,
                succ.register,
                succ.variable,
            )
            if pk in seen_variable:
                n_skipped_duplicate += 1
                continue
            seen_variable.add(pk)
            variable_cycle_edges.append(
                ((pred.provider, pred.register, pred.variable), succ_key)
            )
            pending_variable.append(
                (*pk, edge.effective_year, _REPLACED_BY_NOTE_CURATED, edge.note)
            )

    reject_replaced_by_cycles(register_cycle_edges)
    reject_replaced_by_cycles(variable_cycle_edges)

    conn.executemany(
        "INSERT INTO register_replaced_by ("
        "predecessor_provider, predecessor_register, "
        "successor_provider, successor_register, "
        "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?)",
        pending_register,
    )
    conn.executemany(
        "INSERT INTO variable_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, "
        "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        pending_variable,
    )
    n_register = len(pending_register)
    n_variable = len(pending_variable)

    progress(
        f"  {n_register:,} register / {n_variable:,} variable curated "
        f"replaced_by edges ({n_skipped_duplicate:,} dedup-collapsed, "
        f"{n_skipped_inactive_provider:,} skipped — successor provider "
        f"not in this build)"
    )
    return {
        "register": n_register,
        "variable": n_variable,
        "skipped_duplicate": n_skipped_duplicate,
        "skipped_inactive_provider": n_skipped_inactive_provider,
    }
