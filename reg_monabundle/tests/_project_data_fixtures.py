"""Shared project_data.json builders for the reg_monabundle test suite.

``make_project_data`` builds a fully structurally-valid Model A
project_data payload from a terse per-source / per-binding shorthand.
Each binding's FQID (``variable``) is synthesized from the source's
``register_variant`` provider/register prefix + the sanitized
``display_name`` — tests rarely care about the FQID itself, only that
the structural validator accepts the shape and the runtime adapter
resolves lookups by display_name. The binding FQID's first two segments
match the source ``register_variant`` prefix, as 6.3 requires.

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
    schema_version: str = "2.0.0",
    steward: str = "global",
    reg_meta_version: str = "reg_meta/v1.0.0",
    name: str = "test-project",
    register_variant: str = "scb/test/_default",
    period: Any = 2020,
) -> dict[str, Any]:
    """Build a Model A project_data.json-shaped dict for tests.

    ``sources`` items are dicts of the form::

        {"name": "lisa_2018.csv",
         "register_variant": "scb/lisa/individer-15plus",  # optional
         "period": 2018,                                    # optional
         "bindings": [
             {"display_name": "LopNr", "type": "id", "id_subtype": "integer"},
             {"display_name": "Kon",   "type": "categorical"},
         ]}

    Each binding entry's FQID (``variable``) is synthesized from the
    source ``register_variant``'s provider/register prefix + the
    sanitized ``display_name`` (e.g. ``scb/test/lopnr``).
    ``register_variant`` / ``period`` default to the function-level
    arguments for sources that omit them. A binding may instead pass an
    explicit ``variable`` to pin the FQID directly.
    """

    def _sanitize(s: str) -> str:
        # 3rd-segment slug: lowercase + non-alnum -> hyphen; structural
        # validator (5.2) accepts ``[a-z0-9]+(?:[-_][a-z0-9]+)*``.
        out = "".join(c.lower() if c.isalnum() else "-" for c in s).strip("-")
        return out.replace("--", "-") or "x"

    src_list = []
    for s in sources:
        rv = s.get("register_variant", register_variant)
        per = s.get("period", period)
        # The binding FQID prefix is provider/register -- the variant's
        # first two segments (6.3), not the full 3-part coordinate.
        prefix = "/".join(rv.split("/")[:2])
        bindings_out = []
        for c in s.get("bindings", []):
            binding = dict(c)
            if "display_name" not in binding and "variable" not in binding:
                raise ValueError(
                    "make_project_data binding needs 'display_name' or 'variable'"
                )
            display = binding.get("display_name")
            if "variable" not in binding:
                binding["variable"] = f"{prefix}/{_sanitize(display)}"
            bindings_out.append(binding)
        src_list.append(
            {
                "name": s["name"],
                "register_variant": rv,
                "period": per,
                "bindings": bindings_out,
            }
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
