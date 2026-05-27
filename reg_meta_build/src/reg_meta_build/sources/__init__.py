"""Provider-specific parsers that feed the reg_meta DB.

reg_meta is intentionally data-provider-agnostic: one metadata DB, one docs
DB, one query surface. Each upstream provider has its own parser module
here that reads the provider's native delivery format and yields a
structured representation intended for downstream ingestion (e.g. by
`build-db`, currently a planned consumer for the SoS parser).

Current providers:

- `sos` — Socialstyrelsen metadata Excel workbooks.
  (`scb` import logic currently lives in `reg_meta_build.db`; migrating it
  here under the IR-adapter contract is tracked under MIGRATION_PLAN A4.x.)

The post-refactor contract every adapter must implement is the `IRAdapter`
protocol below — concrete adapters live at
`reg_meta_build/sources/<provider>.py` (e.g. `scb.py`, future: `fk.py`,
etc.) and emit a stream of IR objects (`reg_meta_build.ir.*`) consumed
by the provider-blind materializer in `reg_meta_build.db`. See
REFACTOR_SPEC.md §4.4.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from reg_meta_build.ir import (
    IRClassification,
    IRDeliveryProvenance,
    IRLineageEdge,
    IRRegister,
    IRRelatedToEdge,
    IRReplacedByEdge,
    IRValueCode,
    IRValueSet,
    IRVariable,
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
    | IRValueSet
    | IRValueCode
    | IRClassification
    | IRLineageEdge
    | IRReplacedByEdge
    | IRRelatedToEdge
    | IRWarning
    | IRDeliveryProvenance
)


class IRAdapter(Protocol):
    """Provider-specific source parser. Emits a stream of IR objects.

    Concrete adapters live at `reg_meta_build/sources/<provider>.py`
    (e.g. `scb.py`, `sos.py`). The materializer in `reg_meta_build.db`
    is provider-blind — it consumes the IR stream and writes the
    universal SQLite catalog. See REFACTOR_SPEC.md §4.4.
    """

    provider: str  # short identifier: 'scb', 'sos', 'fk', ...

    def emit(self, source_dir: Path) -> Iterator[IRObject]:
        """Parse the provider's native source files and emit IR objects."""
        ...


__all__ = ["IRAdapter", "IRObject"]
