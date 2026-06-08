"""mock_data_wizard — generate mock CSV data from MONA project metadata.

The on-MONA bundle runtime (``classify``, ``sql_emit``, ``sources``,
``summarize``, ``spec``, ``extract``) lives under
``reg_monabundle.runtime``. mdw is now the local CLI surface
(``compare``, ``generate``, ``update``, ``scan``, ``build-bundle``) plus
the stats-parsing + enrichment + mock-generation pipeline. Bundle
amalgamation is owned end-to-end by ``reg_monabundle.build_bundle``.
"""

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
    "parse_stats",
]

__version__ = "0.8.0"
