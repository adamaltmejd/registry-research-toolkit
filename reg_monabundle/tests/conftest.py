"""Test scaffolding for reg_monabundle.

Adds this directory to ``sys.path`` so bare-name helper modules
(``_stats_fixtures``) can be imported by individual tests.

This directory deliberately has no ``__init__.py``: pytest's
rootdir-relative module discovery breaks when multiple package
``tests/`` directories register as proper packages. Keep it that way.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
