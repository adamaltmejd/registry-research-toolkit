"""precheck-slugs refuses a pinned auto file that a clean checkout would lose.

A curating/frozen zone reads its variable slugs back from its gitignored
``*.auto.toml``. A copy that is on disk but absent from or different from the
committed HEAD tree passes every on-disk check and then does not survive a clean
checkout, so ``precheck-slugs`` reports it as a ``slug_pin_uncommitted`` parse
error and refuses ``--update-snapshot``. A ``.git`` that git cannot read fails
the command rather than reporting nothing. The grow-only refusal for frozen zones
is proven in ``test_fqid_slugs_precheck_cli.py``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.cli import run

from reg_meta_build.fqid_slugs import (
    FREEZE_STATE_FILE,
    GLOBAL_FREEZE_STATE_FILE,
    snapshot_path,
)

if TYPE_CHECKING:
    from pathlib import Path

    from _pipeline_catalog_support import CatalogFixture

_AUTO = "scb.auto.toml"
_PIN = '[variable."1.44"]\nslug = "kon"\n'


def _git(cwd: Path, *args: str) -> None:
    # Strip inherited routing (a git hook's GIT_DIR) so setup targets ``cwd``.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    subprocess.run(
        ["git", "-c", "user.email=t@example.com", "-c", "user.name=t", *args],
        cwd=cwd,
        capture_output=True,
        check=True,
        env=env,
    )


def _commit_all(root: Path, message: str) -> None:
    _git(root, "init")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", message)


def _built_db(catalog: CatalogFixture, tmp_path: Path) -> Path:
    """The `--db` dir of a catalog built from the synthetic SCB source."""
    output = tmp_path / "db" / "reg_meta.db"
    output.parent.mkdir()
    assert (
        catalog.build(output, tmp_path / "report", registers=("1",))["status"]
        == "complete"
    )
    return output.parent


def _layout(
    catalog: CatalogFixture, tmp_path: Path, *, freeze: str, auto: bool
) -> tuple[Path, Path]:
    """The built catalog and a committed flat slug dir pinning ``scb`` to
    ``freeze``, one level below its repo root as in the toolkit checkout. The auto
    file, when written, is left uncommitted."""
    db_dir = _built_db(catalog, tmp_path)
    slug_dir = tmp_path / "repo" / "slugs"
    slug_dir.mkdir(parents=True)
    (slug_dir / FREEZE_STATE_FILE).write_text(f'scb = "{freeze}"\n', encoding="utf-8")
    (slug_dir / "scb.toml").write_text(
        '[register."1"]\nslug = "sample"\n[register_variant."1.10"]\nslug = "people"\n',
        encoding="utf-8",
    )
    _commit_all(slug_dir.parent, "init")
    if auto:
        (slug_dir / _AUTO).write_text(_PIN, encoding="utf-8")
    return db_dir, slug_dir


def _precheck(
    db_dir: Path, slug_dir: Path, capsys: pytest.CaptureFixture[str]
) -> tuple[int, dict]:
    """Run ``precheck-slugs --update-snapshot``.

    The exit code alone cannot tell a refused pin from an accepted one: the
    precheck's missing/stale check keys the catalog by surrogate ``register_id``
    and the slug TOMLs by native id, so every run against a built catalog exits
    10. The tests read the refusal from ``parse_errors`` and the snapshot status."""
    capsys.readouterr()
    code = run(
        [
            "--db",
            str(db_dir),
            "precheck-slugs",
            "--slug-dir",
            str(slug_dir),
            "--update-snapshot",
        ]
    )
    return code, json.loads(capsys.readouterr().out)


def _snapshot_variables(slug_dir: Path) -> dict[str, str]:
    return json.loads(snapshot_path(slug_dir).read_text(encoding="utf-8"))["variable"]


def _assert_refused(code: int, data: dict, slug_dir: Path, pin: Path) -> None:
    assert code == 10
    (error,) = data["parse_errors"]
    assert error["code"] == "slug_pin_uncommitted"
    assert str(pin) in error["message"]
    assert data["snapshot"]["update_skipped_reason"] == "parse_errors"
    assert not snapshot_path(slug_dir).exists()


@pytest.mark.parametrize("state", ["untracked", "staged", "edited"])
def test_uncommitted_pinned_auto_refused(catalog, tmp_path, capsys, state):
    """Fails if the guard reads the index instead of HEAD (staged), or checks
    only that the path is in HEAD rather than its content (edited)."""
    db_dir, slug_dir = _layout(catalog, tmp_path, freeze="curating", auto=True)
    if state != "untracked":
        # Staging is not enough: the push publishes HEAD, not the index.
        _git(slug_dir, "add", "-f", _AUTO)
    if state == "edited":
        # The committed pin is edited but not committed: a clean checkout would
        # restore the old slugs.
        _git(slug_dir, "commit", "-m", "pin")
        with (slug_dir / _AUTO).open("a", encoding="utf-8") as fh:
            fh.write('[variable."1.45"]\nslug = "alder"\n')

    code, data = _precheck(db_dir, slug_dir, capsys)

    _assert_refused(code, data, slug_dir, slug_dir / _AUTO)


def test_committed_pinned_auto_accepted(catalog, tmp_path, capsys):
    db_dir, slug_dir = _layout(catalog, tmp_path, freeze="curating", auto=True)
    _git(slug_dir, "add", "-f", _AUTO)
    _git(slug_dir, "commit", "-m", "pin")

    _, data = _precheck(db_dir, slug_dir, capsys)

    assert data["parse_errors"] == []
    assert data["snapshot"]["updated"] is True
    assert _snapshot_variables(slug_dir) == {"scb/1.44": "kon"}


@pytest.mark.parametrize(
    ("freeze", "auto"),
    [("churning", True), ("curating", False)],
    ids=["churning-leftover", "pinned-without-auto"],
)
def test_unpinned_or_absent_auto_not_refused(catalog, tmp_path, capsys, freeze, auto):
    """A churning zone's leftover auto file is ephemeral: not refused, and not
    loaded into the snapshot either. A pinned zone with no auto file is the build
    guard's case (it exempts providers without variables), not this check's."""
    db_dir, slug_dir = _layout(catalog, tmp_path, freeze=freeze, auto=auto)

    _, data = _precheck(db_dir, slug_dir, capsys)

    assert data["parse_errors"] == []
    assert data["snapshot"]["updated"] is True
    assert _snapshot_variables(slug_dir) == {}


def test_inherited_git_routing_ignored(catalog, tmp_path, capsys, monkeypatch):
    """A git hook exports GIT_DIR/GIT_INDEX_FILE, and a config block can carry a
    routing key. The check must still read the slug dir's own repo: a retarget at
    the unborn outer repo would find no HEAD and report nothing."""
    db_dir, slug_dir = _layout(catalog, tmp_path, freeze="curating", auto=True)
    outer = tmp_path / "outer"
    outer.mkdir()
    _git(outer, "init")
    monkeypatch.setenv("GIT_DIR", str(outer / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(outer / ".git" / "index"))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "2")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.worktree")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(outer))
    monkeypatch.setenv("GIT_CONFIG_KEY_1", "safe.directory")
    monkeypatch.setenv("GIT_CONFIG_VALUE_1", str(slug_dir.parent))

    code, data = _precheck(db_dir, slug_dir, capsys)

    _assert_refused(code, data, slug_dir, slug_dir / _AUTO)


def test_unreadable_git_fails_closed(catalog, tmp_path, capsys):
    """A ``.git`` that git cannot read (here a worktree link to a missing gitdir;
    in a container, "dubious ownership") is a configuration error, not "no
    uncommitted pins": reporting nothing would let ``--update-snapshot`` bake in
    the pin this check refuses."""
    db_dir, slug_dir = _layout(catalog, tmp_path, freeze="curating", auto=True)
    shutil.rmtree(slug_dir.parent / ".git")
    (slug_dir.parent / ".git").write_text(
        f"gitdir: {tmp_path / 'missing'}\n", encoding="utf-8"
    )

    code, data = _precheck(db_dir, slug_dir, capsys)

    assert code == 10
    error = data["error"]
    assert error["code"] == "slug_dir_git_unreadable"
    assert error["class"] == "configuration"
    assert str(slug_dir) in error["message"]
    assert "safe.directory" in error["remediation"]
    assert not snapshot_path(slug_dir).exists()


def test_register_tree_guard_reads_the_loaders_pin_path(catalog, tmp_path, capsys):
    """In the register-owned layout a register's pin sits at the provider root,
    keyed by register slug, even when its TOML is in a family folder. An auto file
    beside the nested TOML pins nothing, so it is neither refused nor loaded."""
    db_dir = _built_db(catalog, tmp_path)
    root = catalog.curation
    provider = root / "registers" / "scb"
    (provider / "family").mkdir()
    (provider / "sample.toml").rename(provider / "family" / "sample.toml")
    (root / GLOBAL_FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
    _commit_all(root, "init")
    pin = provider / "sample.auto.toml"
    pin.write_text('[[variable]]\nnative_id = "1.44"\nslug = "kon"\n', "utf-8")
    (provider / "family" / "sample.auto.toml").write_text(
        '[[variable]]\nnative_id = "1.45"\nslug = "alder"\n', encoding="utf-8"
    )

    code, data = _precheck(db_dir, root, capsys)

    _assert_refused(code, data, root, pin)

    _git(root, "add", "-f", str(pin))
    _git(root, "commit", "-m", "pin")
    _, data = _precheck(db_dir, root, capsys)

    assert data["parse_errors"] == []
    assert data["snapshot"]["updated"] is True
    assert _snapshot_variables(root) == {"scb/1.101": "value", "scb/1.44": "kon"}
