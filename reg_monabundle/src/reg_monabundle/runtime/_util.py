"""Shared internal utilities for the bundle-runtime modules.

Lives under ``reg_monabundle.runtime`` because the SCB MONA project
prefix (``P\\d+_``) is bundle-tier knowledge: the runtime sees column
names with the prefix on MONA and the same names without it locally in
mdw. ``mock_data_wizard.enrich`` / ``mock_data_wizard.cli`` reach in
here for the same helpers — that's an intentional dependency from the
post-mdw downstream (``reg_mockdata``) into the runtime.
"""

from __future__ import annotations

import re

_PROJECT_PREFIX_RE = re.compile(r"^P\d+_", re.IGNORECASE)


def strip_project_prefix(col: str) -> str:
    """Strip MONA project prefix (e.g. 'P1105_LopNr_PersonNr' → 'LopNr_PersonNr')."""
    return _PROJECT_PREFIX_RE.sub("", col)


def lookup_with_prefix_fallback[V](d: dict[str, V], col_name: str) -> V | None:
    """Lookup ``d[col_name.lower()]`` falling back to the prefix-stripped form.

    Uses ``in``/``[]`` rather than ``get(...) or get(...)`` so that falsy
    values (0, "", False, []) are returned as hits — None is reserved as
    the "not found" signal.
    """
    key = col_name.lower()
    if key in d:
        return d[key]
    return d.get(strip_project_prefix(col_name).lower())
