"""Shared internal utilities for mock_data_wizard.

The MONA project-prefix helpers (``strip_project_prefix``,
``lookup_with_prefix_fallback``) live in
``reg_monabundle.runtime._util`` — they're consumed by bundle-runtime
classify code as well as mdw's local CLI / enrichment. mdw reaches in
to keep one source of truth for the prefix regex.
"""

from __future__ import annotations

import sys


def progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
