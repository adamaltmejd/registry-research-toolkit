"""Test scaffolding for reg_meta_build.

Adds this directory to ``sys.path`` so the bare-name helper modules
(``_csv_fixtures``, ``_slugged_db``, ``_shared_fixtures``) can be
imported by individual tests.

This directory deliberately has no ``__init__.py``: pytest's rootdir-relative
module discovery breaks when ``reg_meta/tests/`` and ``reg_meta_build/tests/``
both register as proper packages. Keep it that way.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from hypothesis import settings

if TYPE_CHECKING:
    import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Property tests run a fast profile unless HYPOTHESIS_PROFILE=thorough (CI's
# reg_meta_build leg) keeps Hypothesis's full example count. Both derive from the
# profile Hypothesis already loaded, so its automatic `ci` profile (derandomized, no
# deadline) still applies on CI. Loaded here, before any test module is imported,
# because a module's `@settings(...)` resolves its unset fields against the profile
# active at import time. `load_profile` is process-global: a run that also collects
# reg_meta/tests gives its property tests this profile too.
settings.register_profile("fast", settings.default, max_examples=15)
settings.register_profile("thorough", settings.default)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE") or "fast")

# The slowest single test (it loads the committed curation tree, ~20 s CPU). Run
# first so an xdist worker starts it at once instead of it trailing the run.
_FIRST = "test_committed_curation.py::test_committed_curation_loads"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    items.sort(key=lambda item: not item.nodeid.endswith(_FIRST))


# The build-pipeline `catalog` fixture (an accepted synthetic SCB source plus curation)
# is shared by the build-db test modules.
from _pipeline_catalog_support import catalog  # noqa: F401

# The committed curation tree, loaded once per session (test_committed_curation).
from _repo_curation_support import repo_tree  # noqa: F401
from _shared_fixtures import (  # noqa: F401
    db_conn,
    db_path,
    fixture_db,
)
