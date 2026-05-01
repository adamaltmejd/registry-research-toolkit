"""mock_data_wizard — generate mock CSV data from MONA project metadata."""

from .enrich import EnrichedColumn, EnrichedSource, enrich
from .generate import Manifest, OutputFile, generate
from .script_gen import generate_script
from .stats import (
    ColumnStats,
    ProjectStats,
    SharedColumn,
    SourceStats,
    StatsValidationError,
    parse_stats,
)

__all__ = [
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
    "generate_script",
    "parse_stats",
]

__version__ = "0.4.1"
