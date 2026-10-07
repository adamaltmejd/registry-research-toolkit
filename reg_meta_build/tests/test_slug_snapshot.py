"""precheck-slugs refuses a pinned auto file that a clean checkout would lose.

A curating/frozen zone reads its variable slugs back from its gitignored
``*.auto.toml``. A copy that is on disk but not in the committed HEAD tree passes
every on-disk check and then vanishes on a clean checkout, so ``precheck-slugs``
reports it and refuses ``--update-snapshot``. The grow-only refusal for frozen zones
is proven in ``test_fqid_slugs_precheck_cli.py``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.cli import run
from reg_meta_build.db import DDL, seed_providers

from reg_meta_build.fqid_slugs import (
    FREEZE_STATE_FILE,
    SNAPSHOT_FILENAME,
    repo_slug_dir,
    untracked_pinned_autos,
)

if TYPE_CHECKING:
    from pathlib import Path

_AUTO = "scb.auto.toml"


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


def _layout(tmp_path: Path, *, freeze: str, auto: bool) -> tuple[Path, Path]:
    """A one-register DB and a committed flat slug dir pinning ``scb`` to
    ``freeze``. The auto file, when written, is left uncommitted."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    conn = sqlite3.connect(db_dir / "reg_meta.db")
    conn.executescript(DDL)
    seed_providers(conn)
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name, slug) "
        "VALUES (1, 1, 'LISA', 'lisa')"
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, slug, name) "
        "VALUES (10, 1, 'individer', 'Individer')"
    )
    conn.execute("INSERT INTO import_manifest VALUES ('schema_version', '3.1.0')")
    conn.commit()
    conn.close()

    slug_dir = tmp_path / "slugs"
    slug_dir.mkdir()
    (slug_dir / FREEZE_STATE_FILE).write_text(f'scb = "{freeze}"\n', encoding="utf-8")
    (slug_dir / "scb.toml").write_text(
        '[register."1"]\nslug = "lisa"\n[register_variant."1.10"]\nslug = "individer"\n',
        encoding="utf-8",
    )
    _git(slug_dir, "init")
    _git(slug_dir, "add", "-A")
    _git(slug_dir, "commit", "-m", "init")
    if auto:
        (slug_dir / _AUTO).write_text(
            '[variable."1.44"]\nslug = "kon"\n', encoding="utf-8"
        )
    return db_dir, slug_dir


def _precheck(
    db_dir: Path, slug_dir: Path, capsys: pytest.CaptureFixture[str]
) -> tuple[int, dict]:
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


@pytest.mark.parametrize("staged", [False, True], ids=["untracked", "staged"])
def test_uncommitted_pinned_auto_refused(tmp_path, capsys, staged):
    db_dir, slug_dir = _layout(tmp_path, freeze="curating", auto=True)
    if staged:
        # Staging is not enough: the push publishes HEAD, not the index.
        _git(slug_dir, "add", "-f", _AUTO)

    code, data = _precheck(db_dir, slug_dir, capsys)

    assert code == 10
    (error,) = data["parse_errors"]
    assert str(slug_dir / _AUTO) in error
    assert "git add -f" in error
    assert not (slug_dir / SNAPSHOT_FILENAME).exists()


def test_committed_pinned_auto_accepted(tmp_path, capsys):
    db_dir, slug_dir = _layout(tmp_path, freeze="curating", auto=True)
    _git(slug_dir, "add", "-f", _AUTO)
    _git(slug_dir, "commit", "-m", "pin")

    code, data = _precheck(db_dir, slug_dir, capsys)

    assert code == 0, data["parse_errors"]
    snapshot = json.loads((slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8"))
    assert snapshot["variable"] == {"scb/1.44": "kon"}


@pytest.mark.parametrize(
    ("freeze", "auto"),
    [("churning", True), ("curating", False)],
    ids=["churning-leftover", "pinned-without-auto"],
)
def test_unpinned_or_absent_auto_not_refused(tmp_path, capsys, freeze, auto):
    """A churning zone's leftover auto file is ephemeral: not refused, and not
    loaded into the snapshot either. A pinned zone with no auto file is the build
    guard's case (it exempts providers without variables), not this check's."""
    db_dir, slug_dir = _layout(tmp_path, freeze=freeze, auto=auto)

    code, data = _precheck(db_dir, slug_dir, capsys)

    assert code == 0, data["parse_errors"]
    snapshot = json.loads((slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8"))
    assert snapshot["variable"] == {}


def test_inherited_git_routing_ignored(tmp_path, capsys, monkeypatch):
    """A git hook exports GIT_DIR/GIT_INDEX_FILE, and a config block can carry a
    routing key. The check must still read the slug dir's own repo: a retarget at
    the unborn outer repo would find no HEAD and report nothing."""
    db_dir, slug_dir = _layout(tmp_path, freeze="curating", auto=True)
    outer = tmp_path / "outer"
    outer.mkdir()
    _git(outer, "init")
    monkeypatch.setenv("GIT_DIR", str(outer / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(outer / ".git" / "index"))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "2")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.worktree")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(outer))
    monkeypatch.setenv("GIT_CONFIG_KEY_1", "safe.directory")
    monkeypatch.setenv("GIT_CONFIG_VALUE_1", str(slug_dir))

    code, data = _precheck(db_dir, slug_dir, capsys)

    assert code == 10
    assert len(data["parse_errors"]) == 1


def test_register_tree_pins_found_at_any_depth(tmp_path):
    """In the register-owned layout the zone is the provider directory, and a
    register family nests its files one directory deeper."""
    root = tmp_path / "curation"
    family = root / "registers" / "scb" / "komvux"
    family.mkdir(parents=True)
    (root / "registers" / "sos").mkdir()
    (root / "slug_state.toml").write_text('scb = "curating"\n', encoding="utf-8")
    (family / "komvux-a.toml").write_text("", encoding="utf-8")
    _git(root, "init")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "init")
    autos = [
        root / "registers" / "scb" / "lisa.auto.toml",
        family / "komvux-a.auto.toml",
    ]
    for path in [*autos, root / "registers" / "sos" / "lmed.auto.toml"]:
        path.write_text("", encoding="utf-8")

    assert untracked_pinned_autos(root) == sorted(autos)

    _git(root, "add", "-f", *map(str, autos))
    _git(root, "commit", "-m", "pin")
    assert untracked_pinned_autos(root) == []


def test_checkout_pins_are_committed():
    """The same check on this checkout: cheap (git ls-tree plus a glob), so a local
    run catches a pin that was never committed. A clean CI checkout has no
    uncommitted files, so there it can only pass."""
    slug_dir = repo_slug_dir()
    if slug_dir is None:
        pytest.skip("curation tree not present (wheel install)")
    assert untracked_pinned_autos(slug_dir) == []
