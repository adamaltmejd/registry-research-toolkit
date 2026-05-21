"""reg_monabundle: MONA bundle builder + bundle runtime + PII scanner.

Phase 1 surface (§15 step 5 phase 1): only the ``reg_monabundle``
namespaced-block validator and the SUPPRESS_K floor constant. The
bundle builder, PII scanner, type compatibility map, and bundle-runtime
amalgamation modules move here in phase 2.

See ``DESIGN.md`` for the two-half split (lightweight pure-python vs.
``reg_monabundle.runtime.*`` MONA-side modules) and dependency direction.
"""

from .constants import SUPPRESS_K
from .validate import VALID_OPTION_KEYS, validate_block

__all__ = [
    "SUPPRESS_K",
    "VALID_OPTION_KEYS",
    "validate_block",
]

__version__ = "0.1.0"
