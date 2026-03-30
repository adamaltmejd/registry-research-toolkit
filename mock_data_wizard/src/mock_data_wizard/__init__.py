"""mock_data_wizard — generate mock CSV data from MONA project metadata."""

from .enrich import EnrichedColumn, EnrichedFile, enrich
from .generate import Manifest, OutputFile, generate
from .script_gen import generate_script
from .stats import (
    ColumnStats,
    FileStats,
    ProjectStats,
    SharedColumn,
    StatsValidationError,
    parse_stats,
)

__all__ = [
    "ColumnStats",
    "EnrichedColumn",
    "EnrichedFile",
    "FileStats",
    "Manifest",
    "OutputFile",
    "ProjectStats",
    "SharedColumn",
    "StatsValidationError",
    "enrich",
    "generate",
    "generate_script",
    "parse_stats",
]

__version__ = "0.4.0a1"
