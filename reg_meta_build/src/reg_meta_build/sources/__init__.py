"""Provider-specific parsers that feed the reg_meta DB.

reg_meta is intentionally data-provider-agnostic: one metadata DB, one docs
DB, one query surface. Each upstream provider has its own parser module
here that reads the provider's native delivery format and yields a
structured representation consumed by `build-db`.

Current providers:

- `scb` — Statistics Sweden microdata-catalog CSV/SQL/xlsx exports
  (`reg_meta_build.sources.scb.SCBAdapter`, A4.1).
- `sos` — Socialstyrelsen metadata Excel workbooks.
- `fohm` — Folkhälsomyndigheten (SmiNet + national vaccination register);
  thin curated provider with no machine-readable export, read from
  `input_data/Folkhalsomyndigheten/fohm.toml` via `CuratedAdapter`.
- `fk` — Försäkringskassan (MiDAS social-insurance benefit registers); thin
  curated provider read from `input_data/Forsakringskassan/fk.toml` via
  `CuratedAdapter`.
- `lakemedelsverket` — Läkemedelsverket (suspected adverse-drug-reaction
  register); thin curated provider read from
  `input_data/Lakemedelsverket/lakemedelsverket.toml` via `CuratedAdapter`.
- `pliktverket` — Pliktverket / Plikt- och prövningsverket (enlistment/
  conscription assessment register, 1997-2010); thin curated provider read from
  `input_data/Pliktverket/pliktverket.toml` via `CuratedAdapter`.
- `riksarkivet` — Riksarkivet / Krigsarkivet (historical conscription/mönstring
  inskrivningsregister predating Pliktverket); thin curated provider read from
  `input_data/Riksarkivet/riksarkivet.toml` via `CuratedAdapter`.
- `umu` — Umeå universitet (högskoleprovet / SweSAT provresultat database); thin
  curated provider read from `input_data/UMU/umu.toml` via `CuratedAdapter`.

The post-refactor contract every adapter must implement is the `IRAdapter`
protocol below. Native-format providers get their own module (`scb.py`,
`sos.py`); thin curated providers — agencies without machine-readable
exports (FOHM and Försäkringskassan today; Skatteverket etc. to follow) —
share `curated.py`, parameterized by provider. All adapters emit a stream of
IR objects (`reg_meta_build.ir.*`) consumed by the provider-blind
materializer in `reg_meta_build.db`. See DESIGN.md → IR + adapter
architecture, and → Curated thin providers.
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
    IRVariableAlias,
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
    universal SQLite catalog. See DESIGN.md → IR + adapter architecture.
    """

    provider: str  # short identifier: 'scb', 'sos', 'fk', ...

    def emit(self, source_dir: Path) -> Iterator[IRObject]:
        """Parse the provider's native source files and emit IR objects.

        Emit order is the FK-topological order so the materializer can insert
        in stream order and FK targets always exist when a child is seen:

          1. ``IRRegister``        (all)
          2. ``IRClassification``  (all; reference for value-set linkage)
          3. ``IRVariant``         (FK → register)
          4. ``IRValueSet`` (+ nested ``IRValueCode``; referenced by states)
          5. ``IRVariable``        (FK → register, optional source_register_id)
          6. ``IRVariableState`` / ``IRVariableAlias`` (FK → variable + variant
             [+ value_set for states])
          7. ``IRLineageEdge`` / ``IRReplacedByEdge`` / ``IRRelatedToEdge``
          8. ``IRWarning`` / ``IRDeliveryProvenance`` (order-free sinks)

        The order constrains only the types an adapter actually emits — an
        adapter MAY emit a subset (e.g. in A4.1 ``SCBAdapter`` leaves
        ``IRClassification`` and ``IRLineageEdge`` materializer-derived and does
        not emit them). Conformance is about ordering what you do emit, not
        emitting every type.

        Every IR ``*_id`` field is an explicit int the adapter bakes in the
        provider's native ID-assignment order; emit order is independent of ID
        assignment (it only governs FK-referential safety).
        """
        ...


__all__ = ["IRAdapter", "IRObject"]
