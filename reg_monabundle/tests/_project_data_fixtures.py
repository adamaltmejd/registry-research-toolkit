"""Shared project_data.json builders for the reg_monabundle test suite.

``make_project_data`` builds a fully structurally-valid project_data
payload from a terse per-source / per-column shorthand. Each column's
binding FQID (``name``) is synthesized from the source's
``register_version`` + the sanitized ``display_name`` — tests rarely
care about the FQID itself, only that the structural validator accepts
the shape and the runtime adapter resolves lookups by display_name.

mdw's ``mock_data_wizard/tests/conftest.py`` no longer carries these
helpers post-phase-2c — the moved runtime tests (test_extract,
test_sources, test_spec, test_build_mona_bundle) all live here, so the
helpers live here too. Bare-name module rather than ``conftest.py``
addition because ``reg_monabundle/tests/`` deliberately has no
``__init__.py`` and the existing conftest already does the
``sys.path.insert`` dance for ``_stats_fixtures``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path


def make_project_data(
    *,
    sources: Sequence[Mapping[str, Any]] = (),
    panels: Sequence[Mapping[str, Any]] = (),
    reg_monabundle: Mapping[str, Any] | None = None,
    schema_version: str = "1.0.0",
    steward: str = "global",
    reg_meta_version: str = "test",
    name: str = "test-project",
    register_version: str = "scb/test/_default/2020",
) -> dict[str, Any]:
    """Build a project_data.json-shaped dict for tests.

    ``sources`` items are dicts of the form::

        {"name": "lisa_2018.csv",
         "register_version": "scb/lisa/individer-15plus/2018",  # optional
         "columns": [
             {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
             {"display_name": "Kon",   "type": "categorical"},
         ]}

    Each column entry's binding FQID (``name``) is synthesized from the
    source's ``register_version`` + the sanitized ``display_name``.
    ``register_version`` defaults to the function-level
    ``register_version`` argument for sources that omit it.
    """

    def _sanitize(s: str) -> str:
        # 5th-segment slug: lowercase + non-alnum → hyphen; structural
        # validator (§5.2) accepts ``[a-z0-9]+(?:[-_][a-z0-9]+)*``.
        out = "".join(c.lower() if c.isalnum() else "-" for c in s).strip("-")
        return out.replace("--", "-") or "x"

    src_list = []
    for s in sources:
        rv = s.get("register_version", register_version)
        columns_out = []
        for c in s.get("columns", []):
            col = dict(c)
            if "display_name" not in col and "name" not in col:
                raise ValueError(
                    "make_project_data column needs 'display_name' or 'name'"
                )
            display = col.get("display_name")
            if "name" not in col:
                col["name"] = f"{rv}/{_sanitize(display)}"
            columns_out.append(col)
        src_list.append(
            {"name": s["name"], "register_version": rv, "columns": columns_out}
        )
    out: dict[str, Any] = {
        "schema_version": schema_version,
        "steward": steward,
        "reg_meta_version": reg_meta_version,
        "name": name,
        "sources": src_list,
        "panels": list(panels),
    }
    if reg_monabundle is not None:
        out["reg_monabundle"] = dict(reg_monabundle)
    return out


def write_project_data(directory: Path, project_data: Mapping[str, Any]) -> Path:
    """Write a project_data.json next to a stats file. Returns the path."""
    path = directory / "project_data.json"
    path.write_text(json.dumps(project_data), encoding="utf-8")
    return path
