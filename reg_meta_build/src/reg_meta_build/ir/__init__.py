"""Provider-neutral intermediate representation for `reg_meta_build`.

The IR is the contract every provider adapter (SCB, SOS, future: FK,
Skatteverket, …) speaks to the universal materializer in `db.py`.
Pydantic v2 models so model-level validators can catch builder bugs at
construction (e.g. "state validity range crosses zero", "variable
references non-existent variant"). See DESIGN.md → IR + adapter architecture.

Build-time only. **Never imported by `reg_meta` runtime, by the webapp,
or by any future MONA-side runner** — those surfaces stick to stdlib
dataclasses (CLAUDE.md "Stack" /
DESIGN.md → IR + adapter architecture). The Pydantic-on-IR carve-out
matches `reg_schema`'s existing carve-out: build-time validation
where it pays for itself, dataclasses everywhere else.

The materializer is provider-blind: it consumes an IR stream and writes
the universal SQLite catalog. Provider-specific oddities are normalized
into the universal shape inside each adapter; the universal schema
carries no provider-specific tables or columns. Maintainer-only debug
data (SCB's `kolumnnamn` history, SOS's per-row tidsperiod ranges, …)
lives in the sibling provenance DB (see DESIGN.md → Provenance DB sibling).
"""

from __future__ import annotations

# `date` must be a runtime import (not a TYPE_CHECKING guard): Pydantic v2
# resolves string annotations against module globals at model-build time, so
# the symbol has to be importable at runtime even with `from __future__ import
# annotations` in force. See pydantic-core ref-resolution rules.
from datetime import date  # noqa: TC003
from typing import Literal

from pydantic import BaseModel, ConfigDict


class _IRBase(BaseModel):
    """Shared config for every IR model.

    `extra="forbid"` fails fast on adapter typos. Pydantic's default
    (`extra="ignore"`) silently drops unknown keys, which is dangerous
    for IR fields that have defaults — a misspelled `is_sensitive=True`
    would be discarded and the field would quietly remain `False`,
    corrupting downstream catalog output with no exception. Adapters
    speak a strict contract; unknown keys must raise.
    """

    model_config = ConfigDict(extra="forbid")


class IRRegister(_IRBase):
    register_id: int  # universal ID (=SCB RegisterId or hash-minted SOS)
    provider: str  # 'scb', 'sos', ...
    slug: str
    name: str  # canonical native title
    description: str | None
    purpose: str | None  # short prose for catalog browse cards


class IRVariant(_IRBase):
    register_variant_id: int
    register_id: int
    slug: str  # '_default' for variant-less registers
    name: str
    description: str | None
    # Optional source-level coverage window for adapters whose delivery tables have
    # variant-specific lifetimes. The SQLite schema stores validity on
    # variable_state, so materializers do not persist these fields directly.
    valid_from: str | None = None
    valid_to: str | None = None
    # True when adapter invented this variant from var.deldatamangd:
    synthesized: bool = False
    # Natural panel structure for this variant (panel_template; see DESIGN.md → Slug curation):
    panel_entity_key: str | tuple[str, ...] | None = None  # variable slug(s)
    panel_time_key: str | tuple[str, ...] | None = None  # "period" OR slug(s)
    panel_time_grain: Literal["delivery", "row"] | None = None


class IRVariable(_IRBase):
    # IRVariable is register-scoped (see reg_meta/DESIGN.md → Two-level variable
    # model) — the "define once" addressable variable. The variant coordinate
    # moved DOWN to IRVariableState.register_variant_id; the A1.3-shipped
    # `variant_id` field is gone. `provider_key` (SCB str(var_id); SOS the merged
    # name) is a NON-unique join hint, not the key — a triage split shares it. The
    # (register_id, slug) natural key is the unique one (DECISION POINT 1).
    #
    # `provider_key` is REQUIRED (not `str | None`): every variable originates
    # from a provider source row, so a source-natural key always exists (SCB's
    # var_id, SOS's merged variable name). Triage folds/splits derive from source
    # rows and preserve (share) that key — they never mint a provider-source-less
    # variable. The `synthesized` flag is VARIANT-only (a register may invent a
    # `_default` variant); there is no variable-level analogue, so no variable is
    # forced to an empty-key sentinel. Matches `variable.provider_key TEXT NOT
    # NULL`. Toolkit-computed variables would be a deliberate spec change
    # (field + column → nullable).
    variable_id: int
    register_id: int
    provider_key: str
    slug: str
    name: str
    definition: str | None
    description: str | None
    # SCB's `VariabelOperationell_definition` — the per-column distinguishing
    # meaning (owner vs previous-owner; establishment-SNI vs individual-SNI),
    # kept DISTINCT from `description` so it survives the parallel-column split:
    # each split sibling carries ITS column's operational definition (#892).
    operational_definition: str | None = None
    measurement_unit: str | None  # NULL when source was "Okänd"
    is_sensitive: bool = False
    is_identifier: bool = False
    source_register_id: int | None
    # Human-readable attribution (when source not resolved or for display):
    source_register_text: str | None
    # Resolved display label for the source register (the matched register's
    # short name when source_register_id resolved, else the raw attribution
    # text). Universal `variable.source_label`. Distinct from
    # `source_register_text` (the raw, unresolved attribution).
    source_label: str | None


class IRVariableState(_IRBase):
    state_id: int
    variable_id: int  # FK → IRVariable (the addressable identity)
    # A2.1.5: the variant is an explicit per-state delivery coordinate (the
    # variant moved off IRVariable). FK → IRVariant.register_variant_id.
    register_variant_id: int
    # ISO 8601 ('YYYY' | 'YYYY-MM' | 'YYYY-MM-DD'); materializer expands
    # coarser forms to full-date ranges.
    valid_from: str | None
    # ISO 8601; None = open-ended. The materializer writes the '9999-12-31'
    # sentinel; the IR contract carries None to keep adapters
    # honest about which dates they actually know.
    valid_to: str | None
    # Nullable to mirror the nullable `variable_state.data_type` column — the
    # A4.3a flip CONSUMES this field, so the IR type must match the column (SCB
    # never writes NULL, but a provider that does must be representable, not
    # raise at emit). Normalized lowercase canonical set.
    data_type: str | None
    # TEXT — SCB `datalangd` may carry precision/scale ("8,2"), not just an int.
    data_length: str | None
    # The state's LATEST-era delivery column (`variable_state.delivery_column_name`).
    # A coalesced state carries only its newest column; the FULL historical column
    # set rides on IRVariableAlias (the `variable_alias ⊇ states` invariant). None
    # when the source row had no column header.
    delivery_column_name: str | None
    # Raw source attribution/code for this state when it varies by SCB edition.
    source_register_text: str | None = None
    value_set_id: int | None
    value_set_version_label: str | None  # overlap discriminator (multi-vintage)


class IRVariableAlias(_IRBase):
    """The FULL delivery-column history of a variable (universal `variable_alias`).

    One row per historical `(variable_id, register_variant_id,
    delivery_column_name)`. Distinct from `IRVariableState.delivery_column_name`,
    which carries only the variable's LATEST-era column: a variable that was
    delivered under several column headers over its life emits one
    IRVariableState (latest column) but several IRVariableAlias rows (every
    column). This is the carrier behind the STRUCTURAL `variable_alias ⊇
    variable_state delivery columns` invariant (validate.py) — the materializer
    inserts these verbatim into `variable_alias`, and every state's
    `delivery_column_name` is one of its group's alias rows.
    """

    variable_id: int  # FK → IRVariable
    register_variant_id: int  # the delivering variant (FK → IRVariant)
    delivery_column_name: str


class IRValueCode(_IRBase):
    # The universal `value_code.code_id` (explicit PK, per the IRAdapter
    # contract). value_code is deduplicated by (code, label) across all
    # value_sets, so the same code_id recurs across IRValueSets that share a
    # code — the materializer INSERT-OR-IGNOREs value_code on this PK and writes
    # one value_set_member (value_set_id, code_id) per appearance.
    code_id: int
    value_set_id: int
    code: str
    label: str
    valid_from: str | None  # per-code temporal validity (ISO 8601)
    valid_to: str | None


class IRValueSet(_IRBase):
    value_set_id: int
    # Raw 32-byte SHA-256 digest of the normalized code list; dedup key.
    # Materializer writes this verbatim into universal
    # `value_set.member_hash`, which is a BLOB with
    # `CHECK (length(member_hash) = 32)`. Adapters compute via
    # `reg_meta_build.db._value_set_hash` (which returns `bytes`); the
    # IR contract carries raw bytes to keep wire and storage encodings
    # identical — no hex encode/decode at the boundary.
    member_hash: bytes
    # Set when this value_set is a (possibly year-projected) subset of a
    # named classification:
    classification_id: int | None
    codes: tuple[IRValueCode, ...]


class IRClassification(_IRBase):
    classification_id: int
    slug: str  # version baked in: 'sun2020', 'icd10', 'lkf2007'
    name: str
    publisher: str | None
    provider: str | None  # NULL for cross-provider classifications


class IRLineageEdge(_IRBase):
    consumer_state_id: int
    source_state_id: int
    valid_from: str  # ISO 8601 (intersection of consumer + source validity)
    valid_to: str


class IRReplacedByEdge(_IRBase):
    predecessor_variable_id: int
    successor_variable_id: int
    effective_year: int | None
    note: str | None


class IRWarning(_IRBase):
    entity_kind: str
    entity_id: int
    code: str
    detail: str | None = None


class IRDeliveryProvenance(_IRBase):
    """Goes to provenance DB only, not to the published catalog.

    A4.2 re-grain (resolved fork (c)): keyed per `register_variant`, not per
    register. `register_version` is grained by `register_variant_id`, so two
    variants of a register delivering an edition under the same
    `registerversionnamn` token previously collapsed into one dict slot (the
    A4.1 known issue). One IRDeliveryProvenance is now emitted per variant.
    """

    register_id: int
    register_variant_id: int
    source_file: str
    delivery_version: str | None
    delivery_date: date | None
    template_version: str | None
    # For SCB: maps period_token → first-approval date (registerversion_
    # forstagodkannandedatum).
    first_approval_dates: dict[str, str] | None = None
    # For SCB: maps period_token → last-approval date (registerversion_
    # senastgodkanddatum).
    last_approval_dates: dict[str, str] | None = None
    # When the approval-token union is empty, still emit one bare-token
    # delivery_approval row so the variant's delivery metadata is recorded.
    # SCB leaves this False (empty dicts → zero rows, the A4.2 behavior); SOS
    # carries no approval tokens and sets it True. Keeps the materializer
    # provider-blind — no `provider == ...` branch at the write site.
    emit_when_no_tokens: bool = False


__all__ = [
    "IRClassification",
    "IRDeliveryProvenance",
    "IRLineageEdge",
    "IRRegister",
    "IRReplacedByEdge",
    "IRValueCode",
    "IRValueSet",
    "IRVariable",
    "IRVariableAlias",
    "IRVariableState",
    "IRVariant",
    "IRWarning",
]
