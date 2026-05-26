"""Provider-neutral intermediate representation for `reg_meta_build`.

The IR is the contract every provider adapter (SCB, SOS, future: FK,
Skatteverket, …) speaks to the universal materializer in `db.py`.
Pydantic v2 models so model-level validators can catch builder bugs at
construction (e.g. "state validity range crosses zero", "variable
references non-existent variant"). See REFACTOR_SPEC.md §4.4.

Build-time only. **Never imported by `reg_meta` runtime, by
`reg_monabundle.runtime`, by the MONA bundle, or by the webapp** —
those surfaces stick to stdlib dataclasses (CLAUDE.md "Stack" /
REFACTOR_SPEC.md §4.4 lines 407-410). The Pydantic-on-IR carve-out
matches `reg_schema`'s existing carve-out: build-time validation
where it pays for itself, dataclasses everywhere else.

The materializer is provider-blind: it consumes an IR stream and writes
the universal SQLite catalog. Provider-specific oddities are normalized
into the universal shape inside each adapter; the universal schema
carries no provider-specific tables or columns. Maintainer-only debug
data (SCB's `kolumnnamn` history, SOS's per-row tidsperiod ranges, …)
lives in the sibling provenance DB (§5.1, §5.8).
"""

from __future__ import annotations

# `date` must be a runtime import (not a TYPE_CHECKING guard): Pydantic v2
# resolves string annotations against module globals at model-build time, so
# the symbol has to be importable at runtime even with `from __future__ import
# annotations` in force. See pydantic-core ref-resolution rules.
from datetime import date  # noqa: TC003
from typing import Literal

from pydantic import BaseModel


class IRRegister(BaseModel):
    register_id: int  # universal ID (=SCB RegisterId or hash-minted SOS)
    provider: str  # 'scb', 'sos', ...
    slug: str
    name: str  # canonical native title
    description: str | None
    purpose: str | None  # short prose for catalog browse cards


class IRVariant(BaseModel):
    variant_id: int
    register_id: int
    slug: str  # '_default' for variant-less registers
    name: str
    description: str | None
    # True when adapter invented this variant from var.deldatamangd:
    synthesized: bool = False
    # Natural panel structure for this variant (§5.3 panel_template):
    panel_entity_key: str | tuple[str, ...] | None = None  # variable slug(s)
    panel_time_key: str | None = None  # "period" sentinel OR variable slug
    panel_time_grain: Literal["delivery", "row"] | None = None


class IRVariable(BaseModel):
    variable_id: int
    register_id: int
    variant_id: int  # variant-scoped identity
    slug: str
    name: str
    definition: str | None
    # description includes inlined operational_definition when present:
    description: str | None
    measurement_unit: str | None  # NULL when source was "Okänd"
    is_sensitive: bool = False
    is_identifier: bool = False
    source_register_id: int | None
    # Human-readable attribution (when source not resolved or for display):
    source_register_text: str | None


class IRVariableState(BaseModel):
    state_id: int
    variable_id: int  # variant-scope is implied via variable FK
    # ISO 8601 ('YYYY' | 'YYYY-MM' | 'YYYY-MM-DD'); materializer expands
    # coarser forms to full-date ranges.
    valid_from: str | None
    # ISO 8601; None = open-ended. The materializer writes the '9999-12-31'
    # sentinel per §5.1; the IR contract carries None to keep adapters
    # honest about which dates they actually know.
    valid_to: str | None
    data_type: str  # normalized lowercase canonical set
    data_length: int | None
    value_set_id: int | None
    value_set_version_label: str | None  # overlap discriminator (rare; multi-vintage)


class IRValueCode(BaseModel):
    value_set_id: int
    code: str
    label: str
    valid_from: str | None  # per-code temporal validity (ISO 8601)
    valid_to: str | None


class IRValueSet(BaseModel):
    value_set_id: int
    # Hash of normalized code list; dedup key. Materializer writes this
    # verbatim into universal `value_set.member_hash` (§5.1).
    member_hash: str
    # Set when this value_set is a (possibly year-projected) subset of a
    # named classification:
    classification_id: int | None
    codes: tuple[IRValueCode, ...]


class IRClassification(BaseModel):
    classification_id: int
    slug: str  # version baked in: 'sun2020', 'icd10', 'lkf2007'
    name: str
    publisher: str | None
    version: str | None
    provider: str | None  # NULL for cross-provider classifications


class IRLineageEdge(BaseModel):
    consumer_state_id: int
    source_state_id: int
    valid_from: str  # ISO 8601 (intersection of consumer + source validity)
    valid_to: str


class IRReplacedByEdge(BaseModel):
    predecessor_variable_id: int
    successor_variable_id: int
    effective_year: int | None
    note: str | None


class IRRelatedToEdge(BaseModel):
    a_variable_id: int
    b_variable_id: int
    relation_kind: str
    note: str | None


class IRWarning(BaseModel):
    entity_kind: str
    entity_id: int
    code: str
    detail: str | None = None


class IRDeliveryProvenance(BaseModel):
    """Goes to provenance DB only, not to the published catalog."""

    register_id: int
    source_file: str
    delivery_version: str | None
    delivery_date: date | None
    template_version: str | None
    # For SCB: maps period_token → last_approved_date.
    approval_dates: dict[str, str] | None = None


__all__ = [
    "IRClassification",
    "IRDeliveryProvenance",
    "IRLineageEdge",
    "IRRegister",
    "IRRelatedToEdge",
    "IRReplacedByEdge",
    "IRValueCode",
    "IRValueSet",
    "IRVariable",
    "IRVariableState",
    "IRVariant",
    "IRWarning",
]
