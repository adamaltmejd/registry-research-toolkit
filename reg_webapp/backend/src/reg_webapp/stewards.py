"""Steward configuration loader (§9.1).

A steward is configured by ``reg_webapp/stewards/<id>/``:

- ``steward.toml`` — identity and branding (required).
- ``steward.project_data.json`` — the catalog filter (optional). Its
  *absence* selects full-universe mode — the special ``global`` deployment
  (§9.1). A5.1a only reads ``steward.toml`` + detects the project file's
  presence; loading/validating the project catalog is A5.1b.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

# stewards/ is a sibling of backend/ and frontend/ (REFACTOR_SPEC §9). The
# default resolves it relative to this module — parents[3] is the reg_webapp/
# root — which holds for the source/workspace layout (tests, `uv run`, the
# OpenAPI dumper). A wheel/Docker image packages only src/reg_webapp, where that
# sibling path doesn't exist, so REG_WEBAPP_STEWARDS_DIR overrides it for
# deployment (mirrors reg_meta's REG_META_DB); load_steward(root=...) is the
# per-call override used by tests.
_DEFAULT_STEWARDS_DIR = Path(__file__).resolve().parents[3] / "stewards"
STEWARDS_DIR = (
    Path(env)
    if (env := os.environ.get("REG_WEBAPP_STEWARDS_DIR"))
    else _DEFAULT_STEWARDS_DIR
)

STEWARD_TOML = "steward.toml"
STEWARD_PROJECT_DATA = "steward.project_data.json"

DEFAULT_STEWARD_ID = "global"


@dataclass(frozen=True)
class Steward:
    """A loaded steward config.

    ``has_catalog_filter`` is False for the ``global`` deployment (no
    ``steward.project_data.json`` → full universe). A5.1b reads the project
    file when this is True.
    """

    id: str
    name: str
    long_name: str
    hostname: str
    has_catalog_filter: bool


def load_steward(
    steward_id: str = DEFAULT_STEWARD_ID, *, root: Path | None = None
) -> Steward:
    """Load ``steward.toml`` for ``steward_id`` and detect the catalog filter.

    Raises ``FileNotFoundError`` if the steward directory or ``steward.toml``
    is missing, and ``ValueError`` (naming the file + the absent fields) if a
    required identity field is missing — fail fast (CLAUDE.md), the deployment
    is misconfigured.
    """
    base = (root or STEWARDS_DIR) / steward_id
    toml_path = base / STEWARD_TOML
    if not toml_path.is_file():
        raise FileNotFoundError(f"steward config not found: {toml_path}")

    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    if missing := [
        key for key in ("id", "name", "long_name", "hostname") if key not in data
    ]:
        raise ValueError(
            f"{toml_path}: missing required field(s): {', '.join(missing)}"
        )
    # The declared id must match the directory name — keeps the identity contract
    # explicit before steward selection becomes dynamic (A5.1b/A5.2).
    if data["id"] != steward_id:
        raise ValueError(
            f"{toml_path}: id {data['id']!r} does not match directory name {steward_id!r}"
        )
    return Steward(
        id=data["id"],
        name=data["name"],
        long_name=data["long_name"],
        hostname=data["hostname"],
        has_catalog_filter=(base / STEWARD_PROJECT_DATA).is_file(),
    )
