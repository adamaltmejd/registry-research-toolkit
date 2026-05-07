"""Shared internal utilities for mock_data_wizard."""

from __future__ import annotations

import re
import sys
from typing import TypeVar

_PROJECT_PREFIX_RE = re.compile(r"^P\d+_", re.IGNORECASE)

_V = TypeVar("_V")


def strip_project_prefix(col: str) -> str:
    """Strip MONA project prefix (e.g. 'P1105_LopNr_PersonNr' → 'LopNr_PersonNr')."""
    return _PROJECT_PREFIX_RE.sub("", col)


def lookup_with_prefix_fallback(d: dict[str, _V], col_name: str) -> _V | None:
    """Lookup ``d[col_name.lower()]`` falling back to the prefix-stripped form."""
    return d.get(col_name.lower()) or d.get(strip_project_prefix(col_name).lower())


def progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
