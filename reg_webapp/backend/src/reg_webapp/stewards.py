"""Steward configuration loader (§9.1).

A steward is configured by ``reg_webapp/stewards/<id>/``:

- ``steward.toml`` — identity and branding (required).
- ``steward.project_data.json`` — the catalog filter (optional). Its
  *absence* selects full-universe mode — the special ``global`` deployment
  (§9.1).

``load_steward`` reads ``steward.toml`` (identity) and detects the project
file's presence. ``load_catalog_index`` (A5.2b-i) actually parses + validates
that project file against a live reg_meta ``Catalog`` and builds the §9.1
in-memory index — called once at FastAPI startup with the boot connection (see
``app.py``). The two are split because index-building needs the reg_meta DB,
which only exists once the lifespan opens it.
"""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural

from .catalog_index import build_catalog_index
from .semantic import validate_semantic

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog

    from .catalog_index import CatalogIndex

# stewards/ is a sibling of backend/ and frontend/ (REFACTOR_SPEC §9). The
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
STEWARD_PROJECT_DATA = "steward.project_data.json"

DEFAULT_STEWARD_ID = "global"


def _selected_steward_id() -> str:
    """Which steward this process serves. Static per deployment (one Docker image
    fronts one steward; dynamic Host-header dispatch is a later concern, §9.1).
    ``REG_WEBAPP_STEWARD`` overrides the ``global`` default — mirrors the
    ``REG_META_DB`` / ``REG_WEBAPP_STEWARDS_DIR`` env-override pattern and is the
    seam the boot tests use to select a filtered steward. Read at call time (not
    import) so tests can ``monkeypatch.setenv`` before boot."""
    return os.environ.get("REG_WEBAPP_STEWARD", DEFAULT_STEWARD_ID)


class StewardCatalogError(ValueError):
    """A steward's committed ``steward.project_data.json`` is itself broken —
    malformed JSON or a STRUCTURAL (§6.8.1) violation. Distinct from reg_meta
    *drift* (§6.8.3 semantic warnings), which does NOT raise: drift is a
    steward-vs-reg_meta version skew the deployment boots through (bindings drop,
    warnings surface). A structural break is a misconfigured deployment — fail
    fast (CLAUDE.md), it can't be reasoned about as drift."""


@dataclass(frozen=True)
class Steward:
    """A loaded steward config.

    ``has_catalog_filter`` is False for the ``global`` deployment (no
    ``steward.project_data.json`` → full universe). ``load_catalog_index`` reads
    + validates the project file when this is True.
    """

    id: str
    name: str
    long_name: str
    hostname: str
    has_catalog_filter: bool


def load_steward(steward_id: str | None = None, *, root: Path | None = None) -> Steward:
    """Load ``steward.toml`` for ``steward_id`` and detect the catalog filter.

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
        has_catalog_filter=(base / STEWARD_PROJECT_DATA).is_file(),
    )


def load_catalog_index(
    steward: Steward, catalog: Catalog, *, root: Path | None = None
) -> CatalogIndex | None:
    """Load + validate ``steward.project_data.json`` and build the §9.1 index.

    Returns ``None`` for the ``global`` deployment (``has_catalog_filter=False``)
    — no filter, reg_meta's full universe. Otherwise:

    1. parse the JSON (malformed → ``StewardCatalogError``, fail fast);
    2. run ``validate_structural`` — a STRUCTURAL error means the steward
       committed a broken file → ``StewardCatalogError`` (fail fast; this is NOT
       drift);
    3. construct the ``reg_schema.ProjectData`` model (structurally valid, so it
       builds);
    4. run ``validate_semantic`` in **steward-caller** mode — ``fqid_unresolved``
       / ``value_set_missing`` / ``period_outside_state_validity`` are downgraded
       to ``warning`` (§6.8.3), so reg_meta drift does NOT crash startup;
    5. build the index, DROPPING bindings the validator warned on, and carry the
       warnings for ``/api/context``.

    ⚠️ Boot-availability (§6.8.3): a steward catalog referencing an FQID reg_meta
    no longer admits must still BOOT. The steward-mode downgrade keeps
    ``result.ok`` True even when bindings drop, so we key on the WARNINGS list
    (not ``.ok``) — ``build_catalog_index`` drops the flagged bindings and the
    drift surfaces via ``/api/context``.
    """
    if not steward.has_catalog_filter:
        return None

    base = (root or _stewards_dir()) / steward.id
    project_path = base / STEWARD_PROJECT_DATA
    try:
        raw = json.loads(project_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StewardCatalogError(
            f"{project_path}: could not read/parse steward catalog: {exc}"
        ) from exc

    structural = validate_structural(raw)
    if not structural.ok:
        errors = [i for i in structural.issues if i.level == "error"]
        raise StewardCatalogError(
            f"{project_path}: steward catalog is structurally invalid "
            f"({len(errors)} error(s)): "
            + "; ".join(f"{i.code}@{i.path}" for i in errors)
        )

    # validate_structural passed, but the reg_schema models are `extra="forbid"`
    # and validate_structural does NOT flag an unrecognized key on a nested Source /
    # Binding — so model_validate can still raise on a typo'd field. That's a broken
    # committed catalog (user error), so fail fast with a CLEAR StewardCatalogError,
    # not an opaque pydantic traceback out of the FastAPI lifespan.
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        raise StewardCatalogError(
            f"{project_path}: steward catalog passed structural validation but "
            f"failed model construction (an unrecognized or invalid field?): {exc}"
        ) from exc

    # Steward-caller mode: reg_meta-backed misses downgrade error→warning so the
    # deployment boots through drift. We read result.issues (the warnings),
    # NOT just result.ok — a drift downgrade keeps ok=True while bindings drop.
    result = validate_semantic(project, catalog, caller="steward")
    return build_catalog_index(project, result.issues)
