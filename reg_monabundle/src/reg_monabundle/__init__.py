"""reg_monabundle: MONA bundle builder + bundle runtime + PII scanner.

Phase 2a surface (§15 step 5): the ``reg_monabundle`` namespaced-block
validator + the SUPPRESS_K floor constant (phase 1) plus the bundle
builder (phase 2a — relocated from ``mock_data_wizard._bundle``). PII
scanner, type compatibility map, and bundle-runtime amalgamation
modules still pending in phase 2b / 2c.

See ``DESIGN.md`` for the two-half split (lightweight pure-python vs.
``reg_monabundle.runtime.*`` MONA-side modules) and dependency direction.
"""

from .build import DEFAULT_OUTPUT_NAME, build_bundle
from .constants import SUPPRESS_K
from .validate import VALID_OPTION_KEYS, validate_block

__all__ = [
    "DEFAULT_OUTPUT_NAME",
    "SUPPRESS_K",
    "VALID_OPTION_KEYS",
    "build_bundle",
    "validate_block",
]

__version__ = "0.2.0"
