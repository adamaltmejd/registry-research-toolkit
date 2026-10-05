"""Load steward identity and branding from ``steward.toml``.

Compiled catalog manifests own deployment identity and holdings. This loader reads
only presentation configuration; artifact admission happens in the app lifespan.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

# stewards/ is a sibling of backend/ and frontend/ (see DESIGN.md → Layout). The
# default resolves it relative to this module — parents[3] is the reg_webapp/
# root — which holds for the source/workspace layout (tests, `uv run`, the
# OpenAPI dumper). A wheel/Docker image packages only src/reg_webapp, where that
# sibling path doesn't exist, so REG_WEBAPP_STEWARDS_DIR overrides it for
# deployment (mirrors reg_meta's REG_META_DB); load_steward(root=...) is the
# per-call override used by tests.
_DEFAULT_STEWARDS_DIR = Path(__file__).resolve().parents[3] / "stewards"


def _stewards_dir() -> Path:
    """The stewards/ root, resolved at CALL time so REG_WEBAPP_STEWARDS_DIR can be
    set after import (the env-override path the boot tests + deployment use)."""
    if env := os.environ.get("REG_WEBAPP_STEWARDS_DIR"):
        return Path(env)
    return _DEFAULT_STEWARDS_DIR


STEWARD_TOML = "steward.toml"

DEFAULT_STEWARD_ID = "global"


def _selected_steward_id() -> str:
    """Which steward this process serves. Static per deployment (one Docker image
    fronts one steward; dynamic Host-header dispatch is a later concern).
    ``REG_WEBAPP_STEWARD`` overrides the ``global`` default — mirrors the
    ``REG_META_DB`` / ``REG_WEBAPP_STEWARDS_DIR`` env-override pattern and is the
    seam the boot tests use to select a steward deployment. Read at call time (not
    import) so tests can ``monkeypatch.setenv`` before boot."""
    return os.environ.get("REG_WEBAPP_STEWARD", DEFAULT_STEWARD_ID)


@dataclass(frozen=True)
class Steward:
    """A loaded steward config — identity and branding only.

    The compiled catalog artifact owns physical holdings.
    """

    id: str
    name: str
    long_name: str
    hostname: str


def load_steward(steward_id: str | None = None, *, root: Path | None = None) -> Steward:
    """Load ``steward.toml`` for ``steward_id``.

    ``steward_id`` defaults to ``_selected_steward_id()`` (the
    ``REG_WEBAPP_STEWARD`` env or ``global``) so the lifespan picks up the
    deployment's steward; callers may pass an explicit id.

    Raises ``FileNotFoundError`` if the steward directory or ``steward.toml``
    is missing, and ``ValueError`` (naming the file + the absent fields) if a
    required identity field is missing — fail fast (CLAUDE.md), the deployment
    is misconfigured.
    """
    if steward_id is None:
        steward_id = _selected_steward_id()
    base = (root or _stewards_dir()) / steward_id
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
    )
