"""Shared internal utilities for mock_data_wizard."""

from __future__ import annotations

import sys


def progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
