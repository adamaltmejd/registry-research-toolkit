"""precheck-slugs on a pipeline-built catalog: the catalog-to-pin join, the
snapshot freshness gate, and the grow-only refusal for frozen zones.

The catalog is built from the synthetic SCB source with two registers,
``sample`` (``1``, variant ``1.10``) and ``other`` (``2``, variant ``2.20``),
whose variants share the slug ``people``. The uncommitted-pin refusal is proven
in ``test_slug_snapshot.py``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _fqid_slug_support import assert_precheck_clean, run_precheck
from _pipeline_catalog_support import built_db_dir

from reg_meta_build.fqid_slugs import (
    FREEZE_STATE_FILE,
    snapshot_path,
    write_snapshot,
)

if TYPE_CHECKING:
    from pathlib import Path

    from _pipeline_catalog_support import CatalogFixture

pytestmark = pytest.mark.parametrize("catalog", [True], indirect=True)

_PINS = (
    '[register."1"]\nslug = "sample"\n'
    '[register."2"]\nslug = "other"\n'
    '[register_variant."1.10"]\nslug = "people"\n'
    '[register_variant."2.20"]\nslug = "people"\n'
)
_CURRENT = {
    "register": {"scb/1": "sample", "scb/2": "other"},
    "register_variant": {"scb/1.10": "people", "scb/2.20": "people"},
    "variable": {},
}
_EMPTY: dict[str, dict[str, str]] = {
    "register": {},
    "register_variant": {},
    "variable": {},
}


def _layout(
    catalog: CatalogFixture,
    tmp_path: Path,
    snapshot: dict[str, dict[str, str]],
    *,
    pins: str = _PINS,
    freeze: str | None = None,
) -> tuple[Path, Path]:
    """The two-register built catalog and a flat ``scb`` slug dir holding
    ``pins`` and a ``snapshot`` baseline."""
    db_dir = built_db_dir(catalog, tmp_path, registers=("1", "2"))
    slug_dir = tmp_path / "slugs"
    slug_dir.mkdir()
    (slug_dir / "scb.toml").write_text(pins, encoding="utf-8")
    if freeze is not None:
        (slug_dir / FREEZE_STATE_FILE).write_text(
            f'scb = "{freeze}"\n', encoding="utf-8"
        )
    write_snapshot(snapshot_path(slug_dir), snapshot)
    return db_dir, slug_dir


def _snapshot(slug_dir: Path) -> dict[str, dict[str, str]]:
    return json.loads(snapshot_path(slug_dir).read_text(encoding="utf-8"))


def test_stale_snapshot_fails_the_read_only_check(catalog, tmp_path, capsys):
    """A clean tree whose pins the snapshot lacks still exits 10 without
    ``--update-snapshot``, so CI keeps the snapshot current. Fails if the
    read-only check stops treating ``added`` as fatal."""
    db_dir, slug_dir = _layout(catalog, tmp_path, _EMPTY)

    code, data = run_precheck(db_dir, slug_dir, capsys, update=False)

    assert code == 10
    assert data["missing_registers"] == data["stale_registers"] == []
    assert sorted(data["snapshot"]["added"]) == [
        "register/scb/1 = 'sample'",
        "register/scb/2 = 'other'",
        "register_variant/scb/1.10 = 'people'",
        "register_variant/scb/2.20 = 'people'",
    ]
    assert _snapshot(slug_dir) == _EMPTY


def test_update_snapshot_records_a_clean_tree(catalog, tmp_path, capsys):
    """The pins that built the catalog cover every row, so ``--update-snapshot``
    exits 0 and writes them. Fails (exit 10, every row missing, every pin stale)
    if precheck keys the catalog by its surrogate ``register_id`` instead of the
    slug path (#1215)."""
    db_dir, slug_dir = _layout(catalog, tmp_path, _EMPTY)

    code, data = run_precheck(db_dir, slug_dir, capsys)

    assert_precheck_clean(code, data)
    assert data["snapshot"]["updated"] is True
    assert _snapshot(slug_dir) == _CURRENT


def test_unpinned_rows_and_dead_pins_fail_under_update(catalog, tmp_path, capsys):
    """``--update-snapshot`` still exits 10 on a catalog row no pin names and a
    pin that names no row. ``other/people`` is missing although ``people`` is
    pinned under ``sample``; a pin whose register is unpinned (``3.30``) and
    pins absent from the catalog are stale; a deprecated pin may outlive its
    row. Fails if variants match on their bare slug or the stale check drops the
    deprecated exemption."""
    pins = (
        '[register."1"]\nslug = "sample"\n'
        '[register."2"]\nslug = "other"\n'
        '[register."999"]\nslug = "ghost"\n'
        '[register."998"]\nslug = "retired"\ndeprecated = true\n'
        '[register_variant."1.10"]\nslug = "people"\n'
        '[register_variant."1.999"]\nslug = "ghost-variant"\n'
        '[register_variant."3.30"]\nslug = "people"\n'
    )
    db_dir, slug_dir = _layout(catalog, tmp_path, _EMPTY, pins=pins)

    code, data = run_precheck(db_dir, slug_dir, capsys)

    assert code == 10
    assert data["missing_registers"] == []
    assert [(m["provider"], m["slug"]) for m in data["missing_variants"]] == [
        ("scb", "other/people")
    ]
    assert data["stale_registers"] == [{"provider": "scb", "source_id": "999"}]
    assert data["stale_variants"] == [
        {"provider": "scb", "source_id": "1.999"},
        {"provider": "scb", "source_id": "3.30"},
    ]


def test_update_snapshot_refuses_on_parse_error(catalog, tmp_path, capsys):
    """A TOML that fails to load leaves the baseline untouched: the truncated
    entry set would otherwise wipe it. Fails if ``--update-snapshot`` writes
    despite a parse error."""
    db_dir, slug_dir = _layout(
        catalog, tmp_path, _CURRENT, pins='[register."1"]\nslug = "Bad_Slug"\n'
    )

    code, data = run_precheck(db_dir, slug_dir, capsys)

    assert code == 10
    assert [e["code"] for e in data["parse_errors"]] == ["slug_toml_invalid"]
    assert data["snapshot"]["update_skipped_reason"] == "parse_errors"
    assert _snapshot(slug_dir) == _CURRENT


_RENAMED = {**_CURRENT, "register": {"scb/1": "lisa", "scb/2": "other"}}
_REMOVED = {
    **_CURRENT,
    "register": {"scb/1": "sample", "scb/2": "other", "scb/3": "retired"},
}


@pytest.mark.parametrize("baseline", [_RENAMED, _REMOVED], ids=["rename", "removal"])
def test_frozen_zone_refuses_non_additive_update(catalog, tmp_path, capsys, baseline):
    """In a frozen zone ``--update-snapshot`` refuses to bless a rename or a
    removal of a published slug and leaves the baseline untouched; the tree
    itself is clean, so the refusal is the only failure. Fails if a frozen
    zone writes non-additive drift through (#470)."""
    db_dir, slug_dir = _layout(catalog, tmp_path, baseline, freeze="frozen")

    code, data = run_precheck(db_dir, slug_dir, capsys)

    assert code == 10
    assert data["missing_registers"] == data["stale_registers"] == []
    assert data["snapshot"]["update_skipped_reason"] == "frozen_zone_violation"
    assert _snapshot(slug_dir) == baseline


def test_churning_zone_writes_a_rename_through(catalog, tmp_path, capsys):
    """Without a freeze state the zone is churning: ``--update-snapshot`` writes
    the rename through, exits 0 and still reports it. Fails if churning zones
    refuse like frozen ones (#470)."""
    db_dir, slug_dir = _layout(catalog, tmp_path, _RENAMED)

    code, data = run_precheck(db_dir, slug_dir, capsys)

    assert_precheck_clean(code, data)
    assert data["snapshot"]["renamed"] == ["register/scb/1: 'lisa' -> 'sample'"]
    assert _snapshot(slug_dir) == _CURRENT
