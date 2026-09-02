"""Steward configuration loader.

See DESIGN.md → Steward layering and the in-memory catalog index (stewards.py +
catalog_index.py). A steward is configured by ``reg_webapp/stewards/<id>/``:

- ``steward.toml`` — identity and branding (required).
- ``inventory.toml`` — the steward's delivery inventory: the SINGLE source of
  truth for what the deployment holds (REFACTOR_SPEC.md §12). It is both the
  catalog filter (``catalog_index.build_catalog_index`` derives admission from
  it) and the order materializer's physical delivery topology. REQUIRED for a
  named steward (absent → boot fails), and FORBIDDEN for the ``global``
  deployment (present → boot fails): global takes §12's global-deployment
  fallback (``inventory=None``, no filter, reg_meta's full universe)
  unconditionally, until a physical global inventory is introduced deliberately.

``load_steward`` reads ``steward.toml`` (identity); ``load_delivery_inventory``
is a second, DB-free boot read (see ``reg_meta/DESIGN.md`` → Steward delivery
inventory) whose result the lifespan feeds to ``build_catalog_index``.
``check_delivery_inventory`` is that read's DB-backed half, run on the same boot
connection.
"""

from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING

from reg_meta.inventory import load_inventory
from reg_meta.inventory_check import check_inventory, unresolved_message

if TYPE_CHECKING:
    import sqlite3

    from reg_meta.inventory import DeliveryInventory

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


@dataclass(frozen=True)
class Steward:
    """A loaded steward config — identity and branding only.

    What the deployment HOLDS is the delivery inventory's business
    (``load_delivery_inventory``), not this record's.
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


def load_delivery_inventory(
    steward: Steward, *, root: Path | None = None
) -> DeliveryInventory | None:
    """Load this deployment's ``inventory.toml`` — the steward's holdings
    statement (``reg_meta.inventory``): the catalog filter
    ``catalog_index.build_catalog_index`` derives admission from, and the order
    materializer's physical delivery topology.

    Returns ``None`` ONLY for the ``global`` deployment — the one with no
    steward configured, which takes REFACTOR_SPEC.md §12's
    **global-deployment fallback**: the exact ``inventory=None``
    ``materialize_order`` takes, not a degraded mode this adapter invents.
    Global takes that path UNCONDITIONALLY — §12 keeps the fallback until a
    physical global inventory is introduced deliberately, so the presence of one
    is a misconfiguration, not a mode switch. DB-free, so the lifespan can read
    it outside the boot connection.

    Fail fast (CLAUDE.md) on a misconfigured deployment, in the same posture as
    ``load_steward``'s missing-config and id-vs-directory checks:

    - The ``global`` deployment WITH an ``inventory.toml`` raises ``ValueError``.
      Loading it would silently swap that deployment out of the fallback and
      into steward-inventory mode — narrowing the full universe it exists to
      serve down to whatever the stray file happens to list.
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
    if steward.id == DEFAULT_STEWARD_ID:
        if path.is_file():
            raise ValueError(
                f"{path}: the {DEFAULT_STEWARD_ID!r} deployment takes no "
                "delivery inventory — it serves reg_meta's full universe over "
                "REFACTOR_SPEC.md §12's global-deployment fallback. Remove the "
                "file, or move it to the named steward's directory that delivers "
                "those tables."
            )
        return None
    if not path.is_file():
        raise FileNotFoundError(
            f"delivery inventory not found: {path} — the {steward.id!r} "
            "deployment must declare what it delivers before it can "
            "materialize orders. Author the inventory (see reg_meta/DESIGN.md "
            "→ Steward delivery inventory); only the 'global' deployment runs "
            "without one (REFACTOR_SPEC.md §12's global fallback)."
        )
    inventory = load_inventory(path)
    if inventory.steward != steward.id:
        raise ValueError(
            f"{path}: inventory steward {inventory.steward!r} does not match "
            f"deployment steward {steward.id!r}"
        )
    return inventory


def check_delivery_inventory(
    steward: Steward, inventory: DeliveryInventory, conn: sqlite3.Connection
) -> None:
    """Fail boot when this deployment's inventory names coordinates its own
    catalog DB cannot resolve (REFACTOR_SPEC.md §12's standing consistency
    gate; the check itself is ``reg_meta.inventory_check``).

    A steward deployment must never SERVE an inventory its DB cannot resolve.
    The flavored DB and the committed inventory are cut separately and pre-v1
    slug churn is legal, so they CAN drift: a catalog release that renames a
    slug leaves the inventory well-formed and silently stranded — every
    affected order blocked, and admission/coverage stated over holdings the
    catalog no longer names. That is a misconfigured deployment, so it raises
    ``ValueError`` in the same fail-fast posture as ``load_delivery_inventory``
    (CLAUDE.md), rather than being deferred to each researcher in turn.

    Unlike the catalog INDEX this same inventory builds, there is no drift
    downgrade here: a dropped mapping degrades the browse, but an unresolvable
    inventory coordinate is a holdings statement about something that does not
    exist.
    """
    start = perf_counter()
    findings = check_inventory(inventory, conn)
    if findings:
        raise ValueError(
            f"steward {steward.id!r} inventory does not resolve against the "
            f"catalog DB this deployment serves: {unresolved_message(findings)}"
        )
    logger.info(
        "checked steward inventory %s against the catalog: %d tables, %.3fs",
        steward.id,
        len(inventory.tables),
        perf_counter() - start,
    )
