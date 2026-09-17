"""Actual-format source readers and the steward extension IR contract.

Global preparation uses scb_records, sos_records and curated_records to preserve
source evidence. Common resolution and resolved_catalog produce the global DB.
The IR protocol below is retained for the separate steward extend-db path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from reg_meta_build.ir import (
    IRClassification,
    IRDeliveryProvenance,
    IRLineageEdge,
    IRRegister,
    IRReplacedByEdge,
    IRValueCode,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
    IRWarning,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

# Union of every IR object an adapter can emit. Adapters do not emit any
# other type — the materializer dispatches on the IR class.
IRObject = (
    IRRegister
    | IRVariant
    | IRVariable
    | IRVariableState
    | IRVariableAlias
    | IRVariableAliasWindow
    | IRValueCode
    | IRClassification
    | IRLineageEdge
    | IRReplacedByEdge
    | IRWarning
    | IRDeliveryProvenance
)


class IRAdapter(Protocol):
    """Steward source adapter emitting an FK-ordered extension graph."""

    provider: str  # short identifier: 'scb', 'sos', 'fk', ...

    def emit(self, source_dir: Path) -> Iterator[IRObject]:
        """Emit parents before child entities and edges referencing them."""
        ...


__all__ = ["IRAdapter", "IRObject"]
