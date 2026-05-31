"""reg_monabundle: MONA bundle builder + bundle runtime + PII scanner.

Two-half surface (§15 step 5, phase 2c shipped):

- **Lightweight top-level** (this module): ``build_bundle`` (bundle
  amalgamator), ``scan_file`` / ``write_export`` (pre-export PII
  scanner), ``validate_block`` (namespaced-block validator),
  ``SUPPRESS_K`` (k-anonymity floor constant). Pure-stdlib —
  importable without pulling duckdb / pyodbc / reg_schema (Pydantic).
  reg_schema is used only by the build-time ``build.spec_loader``
  validation gate (imported directly by callers, never via this
  top-level), so it stays out of both the lightweight surface and the
  amalgamated bundle (§9.6).
- **Heavy bundle runtime** (``reg_monabundle.runtime.*``): classify,
  sql_emit, sources, summarize, spec, extract — the on-MONA pipeline
  amalgamated into a single-file bundle by ``build_bundle`` and run on
  MONA's WinPython env (which ships duckdb / pyodbc / numpy).

See ``DESIGN.md`` for the dependency direction and amalgamation rules.
"""

from .build import DEFAULT_OUTPUT_NAME, build_bundle
from .constants import SUPPRESS_K
from .scan import PIIScannerError, scan_file, write_export
from .validate import VALID_OPTION_KEYS, validate_block

__all__ = [
    "DEFAULT_OUTPUT_NAME",
    "PIIScannerError",
    "SUPPRESS_K",
    "VALID_OPTION_KEYS",
    "build_bundle",
    "scan_file",
    "validate_block",
    "write_export",
]

__version__ = "0.4.0"
