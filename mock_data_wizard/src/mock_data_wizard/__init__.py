"""mock_data_wizard — generate mock CSV data from MONA project metadata."""

from pathlib import Path

from .enrich import EnrichedColumn, EnrichedSource, enrich
from .generate import Manifest, OutputFile, generate
from .stats import (
    ColumnStats,
    ProjectStats,
    SharedColumn,
    SourceStats,
    StatsValidationError,
    parse_stats,
)

# Bundle-amalgamation layout consumed by ``reg_monabundle.build_bundle``.
# Dep-ordered: each module imports only earlier ones (intra-mdw) or
# modules already amalgamated from reg_schema / reg_monabundle. Top-
# level statements run in order, so e.g. ``spec.py``'s
# ``assert set(INLINE_HINT_KEYS) == set(COLUMN_TYPES)`` requires
# ``classify`` to be loaded first. Phase 2c of §15 step 5 moves these
# modules under ``reg_monabundle.runtime``; the constant moves with them.
BUNDLE_PKG_DIR: Path = Path(__file__).resolve().parent
BUNDLE_MODULE_ORDER: tuple[str, ...] = (
    "classify",
    "sql_emit",
    "sources",
    "summarize",
    "spec",
    "scan",
    "extract",
)

__all__ = [
    "BUNDLE_MODULE_ORDER",
    "BUNDLE_PKG_DIR",
    "ColumnStats",
    "EnrichedColumn",
    "EnrichedSource",
    "Manifest",
    "OutputFile",
    "ProjectStats",
    "SharedColumn",
    "SourceStats",
    "StatsValidationError",
    "enrich",
    "generate",
    "parse_stats",
]

__version__ = "0.7.0"
