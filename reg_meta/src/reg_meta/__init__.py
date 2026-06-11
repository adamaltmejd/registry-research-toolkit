from .catalog import (
    Catalog,
    LineageEdge,
    LineageWarning,
    Period,
    RelatedRef,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
    VariableRef,
    VariableState,
)
from .db import db_path_from_args, default_db_dir, open_db
from .fqid import (
    Fqid,
    FqidError,
    FqidKind,
    derive_period,
    derive_variable_slug,
    parse as parse_fqid,
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

__all__ = [
    "Catalog",
    "compare",
    "db_path_from_args",
    "default_db_dir",
    "derive_period",
    "derive_variable_slug",
    "download_db",
    "extract_year",
    "Fqid",
    "FqidError",
    "FqidKind",
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
    "LineageEdge",
    "LineageWarning",
    "open_db",
    "parse_fqid",
    "Period",
    "RelatedRef",
    "resolve",
    "resolve_register_ids",
    "ResolvedClassification",
    "ResolvedProvider",
    "ResolvedRegister",
    "ResolvedVariable",
    "search",
    "VariableRef",
    "VariableState",
]

__version__ = "0.10.0"


def __getattr__(name: str):
    """Lazy-load `download_db` so the eager `import reg_meta` doesn't pay
    for `zstandard` + `urllib.request` (~13ms of cold startup) on every
    query-side CLI invocation. The CLI's `_prompt_first_run_download` and
    `update.py` import from `.download` directly; only library consumers
    using `from reg_meta import download_db` trigger the lazy path."""
    if name == "download_db":
        from .download import download_db

        return download_db
    raise AttributeError(f"module 'reg_meta' has no attribute {name!r}")
