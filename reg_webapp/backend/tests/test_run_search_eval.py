"""Unit tests for the maintainer search-eval runner's pure helpers.

The runner itself needs a real catalog-scale ``reg_meta.db`` (it lives in ``scripts/``,
not ``tests/``, for that reason), but its ``--db`` resolution and group-guard logic are
pure and DB-free, so they get focused coverage here.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load the sibling script directly, without mutating sys.path (mirrors
# test_openapi_snapshot.py), so the runner's bare-name imports don't leak.
_RUNNER_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_search_eval.py"
_spec = importlib.util.spec_from_file_location(
    "reg_webapp_run_search_eval", _RUNNER_PATH
)
assert _spec is not None and _spec.loader is not None
run_search_eval = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_search_eval)


def test_resolve_db_honors_file_path(tmp_path: Path) -> None:
    """An explicit FILE path is used directly, not treated as a directory."""
    db_file = tmp_path / "reg_meta.db"
    db_file.write_bytes(b"")
    assert run_search_eval._resolve_db(str(db_file)) == db_file


def test_resolve_db_treats_directory_as_container(tmp_path: Path) -> None:
    """A directory arg resolves to ``<dir>/reg_meta.db`` via reg_meta's rules."""
    assert run_search_eval._resolve_db(str(tmp_path)) == tmp_path / "reg_meta.db"


def test_resolve_db_expands_tilde_file_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``~``-prefixed FILE path is expanded before the is-file check, so the
    documented ``--db ~/.local/share/reg_meta/reg_meta.db`` form resolves."""
    monkeypatch.setenv("HOME", str(tmp_path))
    db_file = tmp_path / "reg_meta.db"
    db_file.write_bytes(b"")
    assert run_search_eval._resolve_db("~/reg_meta.db") == db_file


def test_group_call_known_groups() -> None:
    assert run_search_eval._group_call("register") == ("description", False)
    assert run_search_eval._group_call("variable") == ("description", True)
    assert run_search_eval._group_call("classification") == ("description", True)


def test_group_call_rejects_value_group() -> None:
    """`value` was dropped: a code row has no fqid, so it could never match."""
    with pytest.raises(ValueError, match="unsupported eval group 'value'"):
        run_search_eval._group_call("value")


def test_group_call_rejects_unknown_group_names_supported_set() -> None:
    with pytest.raises(ValueError) as exc:
        run_search_eval._group_call("bogus")
    msg = str(exc.value)
    assert "bogus" in msg
    assert "register | variable | classification" in msg
