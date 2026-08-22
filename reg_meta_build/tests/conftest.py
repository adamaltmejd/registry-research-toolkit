"""Test scaffolding for reg_meta_build.

Adds this directory to ``sys.path`` so the bare-name helper modules
(``_csv_fixtures``, ``_slugged_db``, ``_shared_fixtures``) can be
imported by individual tests.

This directory deliberately has no ``__init__.py``: pytest's rootdir-relative
module discovery breaks when ``reg_meta/tests/`` and ``reg_meta_build/tests/``
both register as proper packages. Keep it that way.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _shared_fixtures import (  # noqa: F401
    _no_repo_curation,
    db_conn,
    db_path,
    fixture_db,
)
