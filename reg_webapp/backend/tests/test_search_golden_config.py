"""Boot-time validation of the packaged golden-pin config (`search_golden.toml`).

Pins load when `reg_webapp` is imported, so each case copies the installed package
into a fresh runtime directory, edits its packaged TOML, and boots the app in a
subprocess. A bad config must refuse to start, never silently disable the pins.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import reg_webapp

_REPO_ROOT = Path(__file__).resolve().parents[3]
_BOOT = (
    "from fastapi.testclient import TestClient; "
    "from reg_webapp.app import create_app; "
    "TestClient(create_app()).__enter__()"
)


@pytest.fixture
def runtime_package(tmp_path: Path) -> Path:
    """A copy of the installed `reg_webapp` package under `<tmp>/runtime`."""
    package = tmp_path / "runtime" / "reg_webapp"
    shutil.copytree(
        Path(reg_webapp.__file__).parent,
        package,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    return package


def _boot(package: Path, catalog_db: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _BOOT],
        text=True,
        capture_output=True,
        check=False,
        env={
            "PYTHONPATH": str(package.parent),
            "REG_META_DB": str(catalog_db.parent),
            "REG_WEBAPP_STEWARD": "global",
            "REG_WEBAPP_STEWARDS_DIR": str(_REPO_ROOT / "reg_webapp" / "stewards"),
        },
    )


def test_unmodified_package_boots(runtime_package, catalog_db):
    """The copied package with its committed golden config starts cleanly."""
    completed = _boot(runtime_package, catalog_db)
    assert completed.returncode == 0, completed.stderr


def test_missing_golden_config_refuses_to_start(runtime_package, catalog_db):
    """A package shipped without `search_golden.toml` fails at boot."""
    (runtime_package / "search_golden.toml").unlink()
    completed = _boot(runtime_package, catalog_db)
    assert completed.returncode != 0
    assert "golden config missing" in completed.stderr


def test_duplicate_pin_fqids_refuse_to_start(runtime_package, catalog_db):
    """A pin listing the same fqid twice fails at boot."""
    (runtime_package / "search_golden.toml").write_text(
        '[[pin]]\nquery = "gizmo"\ngroup = "register"\n'
        'fqids = ["scb/rams", "scb/rams"]\n',
        encoding="utf-8",
    )
    completed = _boot(runtime_package, catalog_db)
    assert completed.returncode != 0
    assert "duplicate `fqids`" in completed.stderr
