"""Steward configuration loader.

See DESIGN.md → Steward layering and the in-memory catalog index (stewards.py +
catalog_index.py). A steward is configured by ``reg_webapp/stewards/<id>/``:

- ``steward.toml`` — identity and branding (required).
- ``steward.project_data.json`` — the catalog filter (optional). Its
  *absence* selects full-universe mode — the special ``global`` deployment.
- ``inventory.toml`` — the steward's delivery inventory, loaded at boot for the
  order materializer. REQUIRED for a named steward (absent → boot fails). Only
  the ``global`` deployment may omit it, and there its *absence* selects §12's
  global-deployment fallback (``inventory=None``).

``load_steward`` reads ``steward.toml`` (identity) and detects the project
file's presence. ``load_catalog_index`` (A5.2b-i) actually parses + validates
that project file against a live reg_meta ``Catalog`` and builds the
in-memory index — called once at FastAPI startup with the boot connection (see
``app.py``). The two are split because index-building needs the reg_meta DB,
which only exists once the lifespan opens it. ``load_delivery_inventory`` is a
third, DB-free boot read (see ``reg_meta/DESIGN.md`` → Steward delivery
inventory).
"""

from __future__ import annotations

import json
import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, cast

from pydantic import ValidationError
from reg_meta.inventory import load_inventory
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural

from .catalog_index import build_catalog_index
from .semantic import validate_semantic
from .steward_catalog import StewardBootCatalog

if TYPE_CHECKING:
    from reg_meta.catalog import Catalog
    from reg_meta.inventory import DeliveryInventory

    from .catalog_index import CatalogIndex

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
STEWARD_PROJECT_DATA = "steward.project_data.json"
STEWARD_INVENTORY = "inventory.toml"

DEFAULT_STEWARD_ID = "global"

logger = logging.getLogger(__name__)


def _selected_steward_id() -> str:
    """Which steward this process serves. Static per deployment (one Docker image
    fronts one steward; dynamic Host-header dispatch is a later concern).
    ``REG_WEBAPP_STEWARD`` overrides the ``global`` default — mirrors the
    ``REG_META_DB`` / ``REG_WEBAPP_STEWARDS_DIR`` env-override pattern and is the
    seam the boot tests use to select a filtered steward. Read at call time (not
    import) so tests can ``monkeypatch.setenv`` before boot."""
    return os.environ.get("REG_WEBAPP_STEWARD", DEFAULT_STEWARD_ID)


class StewardCatalogError(ValueError):
    """A steward's committed ``steward.project_data.json`` is itself broken —
    malformed JSON or a STRUCTURAL (see reg_schema/DESIGN.md → Structural rules
    and issue codes) violation. Distinct from reg_meta *drift* (semantic
    warnings, see DESIGN.md → Semantic validation (semantic.py)), which does NOT
    raise: drift is a
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
    """Load + validate ``steward.project_data.json`` and build the index.

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
       to ``warning``, so reg_meta drift does NOT crash startup;
    5. build the index, DROPPING bindings the validator warned on, and carry the
       warnings for ``/api/context``.

    ⚠️ Boot-availability: a steward catalog referencing an FQID reg_meta
    no longer admits must still BOOT. The steward-mode downgrade keeps
    ``result.ok`` True even when bindings drop, so we key on the WARNINGS list
    (not ``.ok``) — ``build_catalog_index`` drops the flagged bindings and the
    drift surfaces via ``/api/context``.
    """
    if not steward.has_catalog_filter:
        return None

    base = (root or _stewards_dir()) / steward.id
    project_path = base / STEWARD_PROJECT_DATA
    start = perf_counter()
    try:
        raw = json.loads(project_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StewardCatalogError(
            f"{project_path}: could not read/parse steward catalog: {exc}"
        ) from exc
    json_seconds = perf_counter() - start

    phase_start = perf_counter()
    structural = validate_structural(raw)
    structural_seconds = perf_counter() - phase_start
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
    phase_start = perf_counter()
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        raise StewardCatalogError(
            f"{project_path}: steward catalog passed structural validation but "
            f"failed model construction (an unrecognized or invalid field?): {exc}"
        ) from exc
    model_seconds = perf_counter() - phase_start

    boot_adapter = StewardBootCatalog(catalog)
    boot_adapter.preload_project(project)
    boot_catalog = cast("Catalog", boot_adapter)

    # Steward-caller mode: the three reg_meta-DRIFT codes downgrade
    # error→warning so the deployment boots through reg_meta evolution (those
    # bindings drop from the index + surface as drift; ok stays True). Any OTHER
    # remaining error — e.g. a bare binding_value_set_version_ambiguous the steward
    # must pin — means the committed catalog is genuinely INVALID: fail fast like a
    # structural break (CLAUDE.md), don't boot a catalog-with-errors as if valid
    # (that would admit the broken binding to the index and never surface it).
    phase_start = perf_counter()
    result = validate_semantic(project, boot_catalog, caller="steward")
    semantic_seconds = perf_counter() - phase_start
    if not result.ok:
        errors = [i for i in result.issues if i.level == "error"]
        raise StewardCatalogError(
            f"{project_path}: steward catalog has unresolved semantic error(s) "
            "after reg_meta-drift downgrades — fix the catalog (e.g. pin an "
            "ambiguous binding's @<version>): "
            + "; ".join(f"{i.code}@{i.path}" for i in errors)
        )
    phase_start = perf_counter()
    index = build_catalog_index(project, result.issues, boot_catalog)
    index_seconds = perf_counter() - phase_start
    logger.info(
        "loaded steward catalog %s: json=%.3fs structural=%.3fs model=%.3fs "
        "semantic=%.3fs index=%.3fs warnings=%d variants=%d bindings=%d",
        steward.id,
        json_seconds,
        structural_seconds,
        model_seconds,
        semantic_seconds,
        index_seconds,
        sum(1 for issue in result.issues if issue.level == "warning"),
        len(index.bindings_by_variant),
        sum(len(bindings) for bindings in index.bindings_by_variant.values()),
    )
    return index


def load_delivery_inventory(
    steward: Steward, *, root: Path | None = None
) -> DeliveryInventory | None:
    """Load this deployment's ``inventory.toml`` — the order materializer's
    physical delivery topology (``reg_meta.inventory``).

    Returns ``None`` ONLY for the ``global`` deployment — the one with no
    steward configured, whose absent inventory is REFACTOR_SPEC.md §12's
    **global-deployment fallback**: the exact ``inventory=None``
    ``materialize_order`` takes, not a degraded mode this adapter invents.
    DB-free, so the lifespan can read it outside the boot connection.

    Fail fast (CLAUDE.md) on a misconfigured deployment, in the same posture as
    ``load_steward``'s missing-config and id-vs-directory checks:

    - A NAMED steward with no ``inventory.toml`` raises ``FileNotFoundError``.
      Booting it into the global fallback instead would leave every one of that
      steward's projects blocked on ``steward_mismatch`` (the fallback demands
      ``ProjectData.steward == "global"``) from an endpoint that reported
      itself healthy at startup — a deployment error deferred to, and paid by,
      each researcher in turn.
    - A malformed inventory raises reg_meta's ``RegMetaError``.
    - An inventory declaring a DIFFERENT steward than the directory it sits in
      raises ``ValueError`` — the materializer's provenance gate compares
      ``ProjectData.steward`` against the inventory's, so a mismatch here would
      silently reject every upload with the same confusing
      ``steward_mismatch``.
    """
    path = (root or _stewards_dir()) / steward.id / STEWARD_INVENTORY
    if not path.is_file():
        if steward.id != DEFAULT_STEWARD_ID:
            raise FileNotFoundError(
                f"delivery inventory not found: {path} — the {steward.id!r} "
                "deployment must declare what it delivers before it can "
                "materialize orders. Author the inventory (see reg_meta/DESIGN.md "
                "→ Steward delivery inventory); only the 'global' deployment may "
                "run without one (REFACTOR_SPEC.md §12's global fallback)."
            )
        return None
    inventory = load_inventory(path)
    if inventory.steward != steward.id:
        raise ValueError(
            f"{path}: inventory steward {inventory.steward!r} does not match "
            f"deployment steward {steward.id!r}"
        )
    return inventory
