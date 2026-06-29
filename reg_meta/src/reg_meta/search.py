"""Typed search-result models for `reg_meta.queries.search` (#701).

The ONE typed schema for the search surface, consumed directly by the webapp
(FastAPI response models), the reg_meta CLI, and the SPA via codegen — the
search-surface analog of the catalog-typing move (#681). `queries.search` builds
its result rows as plain dicts through the whole internal pipeline (LIKE/FTS arms,
succession + concept-group folds, year filter, pagination, value-page annotation)
and converts ONCE at the end into these models, so the dict pipeline stays
untouched and the wire contract is defined in one place.

All models inherit `_CatalogModel` (frozen, `populate_by_name`, `extra="forbid"`,
`serialize_by_alias`) — same base, and same `Fqid`-string wire behavior, as the
catalog models. Every result model carries a `type: Literal[...]` discriminator
and a `rank: float` (the FTS rank the CLI's doc-merge interleaves by). `fqid`
fields are `Fqid | None` (None when the entity isn't slugged) — the `Fqid` core
schema parses the string `try_emit` produces, mirroring `ClassificationEdition.fqid`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from .catalog import ConceptGroupMember, Fqid, _CatalogModel


class RegisterSearchResult(_CatalogModel):
    """A register hit (`register_fts` name/purpose)."""

    type: Literal["register"] = "register"
    fqid: Fqid | None
    name: str | None = None
    purpose: str | None = None
    rank: float


class VariableSearchResult(_CatalogModel):
    """A variable hit (`variable_fts` name/definition/description/
    operational_definition/delivery_column_names). `register` is the owning
    register's display name (context for the omnibox). When the hit is a LONE
    member of a concept group (#322 — the family didn't fold because only one
    member matched), `concept_group`/`concept_group_label` annotate the family so
    it stays discoverable; both None otherwise."""

    type: Literal["variable"] = "variable"
    fqid: Fqid | None
    name: str | None = None
    # `register_name` aliased to the wire key `register` — a bare `register` field
    # shadows `BaseModel.register` (see catalog's `VariableRef`). The alias is the
    # canonical init param; serialized by alias, so the JSON key is `register`.
    register_name: str | None = Field(default=None, alias="register")
    definition: str | None = None
    operational_definition: str | None = None
    delivery_column_names: tuple[str, ...] = ()
    concept_group: str | None = None
    concept_group_label: str | None = None
    rank: float


class ClassificationSearchResult(_CatalogModel):
    """A classification hit (`classification_fts` short_name/name/name_en/
    description — #350 activates this previously-unsearched index). When the hit
    is a LONE member of a vintage group (the family didn't fold because only one
    member matched), `concept_group`/`concept_group_label` annotate the family so
    it stays discoverable — symmetric with `VariableSearchResult`; both None
    otherwise."""

    type: Literal["classification"] = "classification"
    fqid: Fqid | None
    short_name: str | None = None
    name: str | None = None
    concept_group: str | None = None
    concept_group_label: str | None = None
    terminal_fqid: Fqid | None = Field(
        default=None,
        description="When this is a non-current edition that the query hit alone, "
        "the fqid of the current/terminal edition in its succession chain (#571) — "
        "lets the UI link to the current edition; None for a current edition or a "
        "non-edition classification.",
    )
    rank: float


class SearchClassificationEdition(_CatalogModel):
    """One edition of a folded classification succession chain (#571): a vintage
    of the same classification (e.g. `sun1996`, `sun2000`). Carried by
    `ClassificationSuccessionSearchResult.editions`, terminal-first then descending
    `effective_year`. Every edition is a live `classification` row (the build
    validator guarantees succession editions are live), so `fqid` is None only when
    the slug is malformed/unresolvable. A nested node — no `type`/`rank`."""

    slug: str = Field(description="The edition's literal slug (e.g. 'sun2000').")
    fqid: Fqid | None = Field(
        default=None,
        description="The edition's 2-seg classification FQID, None only when the "
        "slug is malformed/unresolvable (succession editions are live rows).",
    )
    name: str | None = Field(
        default=None, description="The edition's display name, None when un-hydrated."
    )
    effective_year: int | None = Field(
        default=None,
        description="The year this edition was superseded by its successor (from its "
        "outbound succession edge); None for the terminal (head) edition, which has "
        "no outbound edge.",
    )


class ClassificationSuccessionSearchResult(_CatalogModel):
    """A folded classification-succession row (#571): a query hit ≥2 distinct
    editions of one classification chain (the vintages, e.g. SUN 1996/2000), so they
    collapse to a single result keyed on the TERMINAL (current) edition. `editions`
    is the full chain (terminal-first, descending year) so the SPA can render "this
    classification has editions …"; `matched_count` is how many editions the query
    actually hit. A succession row is NOT itself a concept group — the terminal
    `fqid` is the navigable target."""

    type: Literal["classification_succession"] = "classification_succession"
    fqid: Fqid | None = Field(
        description="The terminal (current) edition's classification FQID — the "
        "navigable target. None only when the slug is malformed/unresolvable (the "
        "terminal is always a live classification row)."
    )
    short_name: str | None = Field(
        default=None, description="The terminal edition's short name (e.g. 'SUN')."
    )
    name: str | None = Field(
        default=None, description="The terminal edition's display name."
    )
    editions: tuple[SearchClassificationEdition, ...] = ()
    matched_count: int = Field(
        default=0, description="How many editions in the chain the query hit."
    )
    rank: float


class ConceptGroupSearchResult(_CatalogModel):
    """A folded concept-group row (#322): ≥2 sibling members matched OR the
    group's own label matched, so the family collapses to one result. `kind` is
    'variable' or 'classification' (which group bucket it belongs to);
    `member_count` is the family's full size, `matched_count` how many members the
    query hit, `label_matched` whether the group label/key matched directly.
    `members` is the full facet-ordered member list (each a real leaf FQID) so the
    SPA can derive a group-page link when available, or fall back to member links.
    A group is not itself an FQID-addressable catalog leaf."""

    type: Literal["group"] = "group"
    kind: Literal["variable", "classification"]
    group_key: str
    group_label: str
    source: str | None = None
    # `register_name` aliased to the wire key `register` (avoids the
    # `BaseModel.register` shadow — see catalog's `VariableRef`). None for a
    # classification-kind group (catalog-scoped, no owning register).
    register_name: str | None = Field(default=None, alias="register")
    member_count: int = 0
    matched_count: int = 0
    label_matched: bool = False
    # catalog's `ConceptGroupMember` embedded directly (#681).
    members: tuple[ConceptGroupMember, ...] = ()
    rank: float


# ── Code/value search (#352) ─────────────────────────────────────────────────
# A code hit's actionable target is the VARIABLE or CLASSIFICATION carrying the
# code, not the bare (code, label) pair — so each hit surfaces a bounded
# representative slice of its owners plus the full count (the SPA shows "+N more").


class CodeOwnerVariable(_CatalogModel):
    """A variable that carries a code (#352). `register` is the owning register's
    display name (context for the omnibox); the Python attr is `register_name` to
    avoid the `BaseModel.register` method shadow (see catalog's `VariableRef`)."""

    fqid: Fqid | None
    name: str | None = None
    register_name: str | None = Field(default=None, alias="register")


class CodeOwnerClassification(_CatalogModel):
    """A classification that carries a code (#352) — catalog-scoped (no owning
    register)."""

    fqid: Fqid | None
    short_name: str | None = None
    name: str | None = None


class CodeSearchResult(_CatalogModel):
    """A code/value hit (`value_code_fts` label match + code-shape match, #352).
    `code`/`label` are the SCB value pair; `variables`/`classifications` are a
    bounded representative slice of the owning entities (the researcher's actual
    target), and `variable_count`/`classification_count` are the full totals before
    the slice cap."""

    type: Literal["code"] = "code"
    code: str
    label: str
    variables: tuple[CodeOwnerVariable, ...] = ()
    variable_count: int = 0
    classifications: tuple[CodeOwnerClassification, ...] = ()
    classification_count: int = 0
    # Inferred from the owning classification: the primary/first owning
    # classification's `short_name` (fall back to its `name`). None for
    # register-local / bespoke codes with no owning classification (#393 item 3).
    # The SPA groups the codes group into per-code-system subsections off this.
    code_system: str | None = None
    rank: float


class DatacolumnSearchResult(_CatalogModel):
    """A datacolumn (delivery-column-alias) hit — CLI-only (`field=datacolumn`),
    a LIKE match over `variable_alias.delivery_column_name`. Unlike the
    fqid-navigable `variable` arm, datacolumn/varname are pre-fold raw-column CLI
    lookups with no fqid: `var_id`/`name` ARE the variable's identity here (which
    is why this arm carries `var_id` and the `variable` arm omits it)."""

    type: Literal["datacolumn"] = "datacolumn"
    datacolumn: str
    register_name: str | None = Field(default=None, alias="register")
    # The SCB legacy numeric variable id (None for non-SCB providers; see
    # queries.py `_VAR_ID_EXPR`). An INT on the wire — the DB produces an integer
    # or None, and Pydantic v2 does not coerce int↔str, so `int | None` is the
    # contract that matches the data (the spec's `str | None` would reject `44`).
    var_id: int | None = None
    name: str | None = None
    # Lone concept-group member annotation (#322), symmetric with
    # `VariableSearchResult`: a datacolumn hit is variable-keyed, so the fold
    # annotates a non-folded lone member here too (both None otherwise).
    concept_group: str | None = None
    concept_group_label: str | None = None
    rank: float


class VarnameSearchResult(_CatalogModel):
    """A varname (canonical variable name) hit — CLI-only (`field=varname`), a LIKE
    match over `variable.name`. Like `DatacolumnSearchResult`, a pre-fold raw
    lookup with no fqid: `var_id`/`name` are the identity."""

    type: Literal["varname"] = "varname"
    register_name: str | None = Field(default=None, alias="register")
    # INT on the wire — see `DatacolumnSearchResult.var_id`.
    var_id: int | None = None
    name: str | None = None
    # Lone concept-group member annotation (#322) — see `DatacolumnSearchResult`.
    concept_group: str | None = None
    concept_group_label: str | None = None
    rank: float


SearchResult = Annotated[
    RegisterSearchResult
    | VariableSearchResult
    | ClassificationSearchResult
    | ClassificationSuccessionSearchResult
    | ConceptGroupSearchResult
    | CodeSearchResult
    | DatacolumnSearchResult
    | VarnameSearchResult,
    Field(discriminator="type"),
]


class SearchResults(_CatalogModel):
    """The `reg_meta.queries.search` envelope: the full match count plus the
    sliced/folded result page (a `type`-discriminated union of result models)."""

    total_count: int
    results: tuple[SearchResult, ...]
