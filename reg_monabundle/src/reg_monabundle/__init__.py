"""reg_monabundle: MONA bundle builder + bundle runtime + PII scanner.

Phase 2b surface (§15 step 5): the ``reg_monabundle`` namespaced-block
validator + the SUPPRESS_K floor constant (phase 1), the bundle builder
(phase 2a — relocated from ``mock_data_wizard._bundle``), and the
pre-export PII scanner (phase 2b — relocated from
``mock_data_wizard.scan``). Type compatibility map and bundle-runtime
amalgamation modules still pending in the remainder of phase 2 / 2c.

See ``DESIGN.md`` for the two-half split (lightweight pure-python vs.
``reg_monabundle.runtime.*`` MONA-side modules) and dependency direction.
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

__version__ = "0.3.0"
