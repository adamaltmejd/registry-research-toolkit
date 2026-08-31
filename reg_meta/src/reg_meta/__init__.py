from importlib import import_module

from .catalog import (
    BindingGroupRef,
    Catalog,
    ClassificationCode,
    ClassificationDerivedFromRef,
    ClassificationEdition,
    ClassificationRef,
    LineageEdge,
    LineageWarning,
    Period,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
    ValueSetMember,
    VariableEdition,
    VariableRef,
    VariableState,
)
from .db import db_path_from_args, default_db_dir, open_db
from .doc_db import RelatedDocument, RelatedDocumentContent
from .fqid import (
    Fqid,
    FqidError,
    FqidKind,
    derive_period,
    derive_variable_slug,
    parse as parse_fqid,
)
from .inventory import (
    ColumnMapping,
    DeliveryInventory,
    EditionRange,
    InventoryColumn,
    InventoryTable,
    edition_bounds,
    load_inventory,
)
from .queries import (
    compare,
    extract_year,
    get_availability,
    get_classification_concept_groups,
    get_coded_variables,
    get_concept_groups,
    get_datacolumns,
    get_diff,
    get_lineage,
    get_register,
    get_schema,
    get_values_by_variable,
    get_varinfo,
    resolve,
    resolve_register_ids,
    search,
)
from .search import (
    ClassificationSearchResult,
    ClassificationSuccessionSearchResult,
    CodeOwnerClassification,
    CodeOwnerVariable,
    CodeSearchResult,
    ConceptGroupSearchResult,
    DatacolumnSearchResult,
    RegisterSearchResult,
    SearchClassificationEdition,
    SearchResult,
    SearchResults,
    VariableSearchResult,
    VarnameSearchResult,
)

__all__ = [
    "BindingGroupRef",
    "Catalog",
    "ClassificationCode",
    "ClassificationDerivedFromRef",
    "ClassificationEdition",
    "ClassificationRef",
    "ClassificationSearchResult",
    "ClassificationSuccessionSearchResult",
    "ClipReport",
    "CodeOwnerClassification",
    "CodeOwnerVariable",
    "CodeSearchResult",
    "ColumnMapping",
    "ConceptGroupSearchResult",
    "DatacolumnSearchResult",
    "DeliveryInventory",
    "EditionRange",
    "Fqid",
    "FqidError",
    "FqidKind",
    "InventoryColumn",
    "InventoryTable",
    "LineageEdge",
    "LineageWarning",
    "LogicalCoordinate",
    "OrderEntry",
    "OrderFinding",
    "OrderManifest",
    "OrderProvenance",
    "OrderResult",
    "Period",
    "PhysicalCoordinate",
    "RegisterSearchResult",
    "RelatedDocument",
    "RelatedDocumentContent",
    "ResolvedClassification",
    "ResolvedProvider",
    "ResolvedRegister",
    "ResolvedVariable",
    "SearchClassificationEdition",
    "SearchResult",
    "SearchResults",
    "ValueSetMember",
    "VariableEdition",
    "VariableRef",
    "VariableSearchResult",
    "VariableState",
    "VarnameSearchResult",
    "compare",
    "db_path_from_args",
    "default_db_dir",
    "derive_period",
    "derive_variable_slug",
    "download_db",
    "edition_bounds",
    "extract_year",
    "extraction_filenames",
    "get_availability",
    "get_classification_concept_groups",
    "get_coded_variables",
    "get_concept_groups",
    "get_datacolumns",
    "get_diff",
    "get_lineage",
    "get_register",
    "get_schema",
    "get_values_by_variable",
    "get_varinfo",
    "load_inventory",
    "materialize_order",
    "open_db",
    "parse_fqid",
    "resolve",
    "resolve_register_ids",
    "search",
]

__version__ = "0.39.1"


# Public names resolved on first access instead of at import. `download_db`
# keeps `zstandard` + `urllib.request` (~13ms of cold startup) off every
# query-side CLI invocation; the `order` surface keeps `reg_schema` (models plus
# its ~1.6k-line structural validator, ~6ms) off it for the same reason — the
# materializer's consumers are the webapp and the order CLI adapter, not every
# `reg-meta search`.
_LAZY_ATTRS = {
    "download_db": "download",
    **dict.fromkeys(
        (
            "ClipReport",
            "LogicalCoordinate",
            "OrderEntry",
            "OrderFinding",
            "OrderManifest",
            "OrderProvenance",
            "OrderResult",
            "PhysicalCoordinate",
            "extraction_filenames",
            "materialize_order",
        ),
        "order",
    ),
}


def __getattr__(name: str):
    """Resolve a `_LAZY_ATTRS` name by importing its module on first access.
    Callers inside the package (the CLI's `_prompt_first_run_download`,
    `update.py`) import from `.download` / `.order` directly; only library
    consumers using `from reg_meta import ...` take this path."""
    module = _LAZY_ATTRS.get(name)
    if module is None:
        raise AttributeError(f"module 'reg_meta' has no attribute {name!r}")
    return getattr(import_module(f".{module}", __name__), name)
