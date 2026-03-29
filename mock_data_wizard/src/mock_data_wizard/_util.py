"""Shared internal utilities for mock_data_wizard."""

from __future__ import annotations

import re
import sys

_PROJECT_PREFIX_RE = re.compile(r"^P\d+_", re.IGNORECASE)


def strip_project_prefix(col: str) -> str:
    """Strip MONA project prefix (e.g. 'P1105_LopNr_PersonNr' → 'LopNr_PersonNr')."""
    return _PROJECT_PREFIX_RE.sub("", col)


def progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
