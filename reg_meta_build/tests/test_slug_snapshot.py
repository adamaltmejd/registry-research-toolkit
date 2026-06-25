"""Immutability snapshot for committed slug TOMLs (grow-only; see DESIGN.md → Slug immutability).

Compares the live TOMLs at ``reg_meta_build/fqid_slugs/`` against the
committed snapshot at ``reg_meta_build/fqid_slugs/.snapshot.json``. Adds
are allowed; removals and slug renames fail the build.

After legitimate additions, regenerate the snapshot with:

    reg-meta-build precheck-slugs --update-snapshot

and commit ``.snapshot.json`` alongside the new TOML rows.
"""

from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

import pytest

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    SNAPSHOT_FILENAME,
    diff_snapshot,
    frozen_zones,
    load_freeze_states,
    load_slug_dir,
    pinned_zones,
    read_snapshot,
    repo_slug_dir,
    snapshot_payload,
)

if TYPE_CHECKING:
    from pathlib import Path


def _git_env() -> dict[str, str]:
    """Environment for git subprocesses with all GIT_* vars stripped, so git
    discovers the repo from cwd alone. Git hooks export GIT_DIR / GIT_INDEX_FILE
    / GIT_WORK_TREE into the hook process; inheriting them would redirect these
    cwd-scoped calls at the hook's repo instead of the intended one."""
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def _all_slug_dirs() -> list:
    """The global slug dir plus every steward subdir that carries its own
    snapshot (e.g. ``fqid_slugs/swecov/``, #421). Steward dirs use the SAME
    grow-only snapshot machinery (DESIGN.md → Per-steward slug snapshot), so the
    immutability guards must cover them too — ``load_slug_dir`` only globs one
    dir's immediate ``*.toml``, so a nested steward dir is invisible to CI unless
    enumerated here."""
    root = repo_slug_dir()
    if root is None:
        return []
    steward = sorted(
        d for d in root.iterdir() if d.is_dir() and (d / SNAPSHOT_FILENAME).is_file()
    )
    return [root, *steward]


@pytest.fixture(
    scope="module",
    params=_all_slug_dirs() or [None],
    ids=lambda d: d.name if d is not None else "missing",
)
def slug_dir(request):
    if request.param is None:
        pytest.skip("reg_meta_build/fqid_slugs/ not present (wheel install)")
    return request.param


def test_committed_slugs_parse(slug_dir):
    """Every committed slug TOML parses without RegMetaError."""
    load_slug_dir(slug_dir)


def test_no_removed_or_renamed_slugs(slug_dir):
    """Committed slugs in a ``frozen`` zone are grow-only. Removals or slug
    renames there rot every project_data.json that pinned the old FQID.

    Per-provider (#470): only ``frozen`` zones are guarded — a rename/removal in
    a ``churning``/``curating`` zone is allowed (curators iterate pre-seal).
    The repo ships all-churning (no ``freeze.toml``), so this actively verifies
    that no zone advanced to ``frozen`` has been violated.
    """
    fz = frozen_zones(load_freeze_states(slug_dir))
    previous = read_snapshot(slug_dir / SNAPSHOT_FILENAME)
    current = snapshot_payload(load_slug_dir(slug_dir))
    diff = diff_snapshot(previous, current, frozen_zones=fz)
    if diff["blocked"]:
        msgs = ["Removed/renamed slugs in a frozen zone (forbidden):"]
        msgs.extend(f"  {b}" for b in diff["blocked"])
        msgs.append(
            "Restore the entry or mark the old row deprecated=true with a "
            "replaced_by link."
        )
        pytest.fail("\n".join(msgs))


def test_snapshot_covers_committed_additions(slug_dir):
    """After review of new slug entries, the snapshot must be regenerated so
    future PRs branch from a clean baseline. Surfaces drift as a separate
    failure mode from removals/renames."""
    previous = read_snapshot(slug_dir / SNAPSHOT_FILENAME)
    current = snapshot_payload(load_slug_dir(slug_dir))
    diff = diff_snapshot(previous, current)
    if diff["added"]:
        listed = "\n".join(f"  {a}" for a in diff["added"][:10])
        remainder = (
            f"\n  ... and {len(diff['added']) - 10} more"
            if len(diff["added"]) > 10
            else ""
        )
        pytest.fail(
            "New slug entries present without snapshot refresh:\n"
            f"{listed}{remainder}\n"
            "Run: reg-meta-build precheck-slugs --update-snapshot"
        )


def _git_tracked_filenames(directory: Path) -> set[str] | None:
    """The filenames git tracks DIRECTLY under ``directory`` (top-level only),
    or ``None`` when ``directory`` is not inside a git work tree.

    Staged-but-uncommitted counts as tracked — ``git ls-files`` reads the index —
    which is exactly what we want: the guard catches present-but-UNTRACKED files.
    """
    inside = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=directory,
        capture_output=True,
        text=True,
        env=_git_env(),
    )
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        return None
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "."],
        cwd=directory,
        capture_output=True,
        text=True,
        env=_git_env(),
    )
    if listed.returncode != 0:
        return None
    names = (n for n in listed.stdout.split("\0") if n)
    return {n for n in names if "/" not in n}  # top-level entries only


def _untracked_pinned_autos(slug_dir: Path) -> list[str] | None:
    """The committed-auto filenames a pinned (curating/frozen) zone OWES but git
    does not track in ``slug_dir`` — sorted. ``[]`` when nothing is pinned or
    every pinned zone's auto file is tracked; ``None`` when ``slug_dir`` is not
    inside a git work tree (caller skips rather than false-fails)."""
    pinned = pinned_zones(load_freeze_states(slug_dir))
    if not pinned:
        return []
    tracked = _git_tracked_filenames(slug_dir)
    if tracked is None:
        return None
    return sorted(
        name for zone in pinned if (name := f"{zone}{AUTO_FILE_SUFFIX}") not in tracked
    )


def test_pinned_providers_auto_toml_git_tracked(slug_dir):
    """A provider pinned ``curating``/``frozen`` reads its slugs back from a
    committed ``<provider>.auto.toml``. That file is gitignored, so if the
    maintainer set the state without force-adding (or negating the ignore) the
    file, a present-but-untracked artifact from a prior ``churning`` build passes
    here yet vanishes on a clean checkout — where the build-time
    ``slug_freeze_auto_missing`` guard fires. Catch it git-side instead.

    The build is git-agnostic by design, so this lives in the test layer. Today
    the repo ships all-churning (no ``freeze.toml``), so this passes vacuously.
    """
    missing = _untracked_pinned_autos(slug_dir)
    if missing is None:
        pytest.skip("slug_dir is not inside a git work tree")
    if missing:
        listed = "\n".join(f"  {name}" for name in missing)
        pytest.fail(
            "Pinned provider(s) have a present-but-untracked auto.toml "
            f"(would vanish on a clean checkout):\n{listed}\n"
            "Force-add the generated file "
            "(git add -f reg_meta_build/fqid_slugs/<name>) or add a per-provider "
            ".gitignore negation (!reg_meta_build/fqid_slugs/<name>)."
        )


def test_untracked_pinned_auto_detected(tmp_path):
    """Regression for the guard: a pinned zone whose auto file is on disk but
    untracked is flagged; staging it clears the flag. A churning zone's untracked
    auto file is never flagged. Self-contained — does not read the repo's state."""
    subprocess.run(
        ["git", "init"], cwd=tmp_path, capture_output=True, check=True, env=_git_env()
    )
    # `fk` is curating, `scb` is frozen, `umu` is churning — all with a known
    # provider stem so load_freeze_states accepts the zones (an unknown zone
    # would raise). `frozen` is pinned exactly like `curating`.
    (tmp_path / "freeze.toml").write_text(
        'fk = "curating"\nscb = "frozen"\numu = "churning"\n', encoding="utf-8"
    )
    (tmp_path / "fk.toml").write_text(
        '[register."1"]\nslug = "fk-reg"\n', encoding="utf-8"
    )
    (tmp_path / "scb.toml").write_text(
        '[register."1"]\nslug = "scb-reg"\n', encoding="utf-8"
    )
    (tmp_path / "umu.toml").write_text(
        '[register."1"]\nslug = "umu-reg"\n', encoding="utf-8"
    )
    # A churning zone's untracked auto file must NOT be flagged.
    (tmp_path / "umu.auto.toml").write_text(
        '[variable."1.x"]\nslug = "umu-x"\n', encoding="utf-8"
    )

    # 1. Both pinned autos on disk but untracked → flagged, sorted lexically
    #    across both pinned providers ("fk" < "scb"); churning `umu` is absent.
    (tmp_path / "fk.auto.toml").write_text(
        '[variable."1.personnummer"]\nslug = "personnummer"\n', encoding="utf-8"
    )
    (tmp_path / "scb.auto.toml").write_text(
        '[variable."1.kon"]\nslug = "kon"\n', encoding="utf-8"
    )
    assert _untracked_pinned_autos(tmp_path) == ["fk.auto.toml", "scb.auto.toml"]

    # 2. Stage both (no commit needed — git ls-files reads the index) → cleared.
    subprocess.run(
        ["git", "add", "fk.auto.toml", "scb.auto.toml"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    assert _untracked_pinned_autos(tmp_path) == []


def test_guard_isolated_from_inherited_git_dir(tmp_path, monkeypatch):
    """The git calls must discover the repo from cwd, not an inherited GIT_DIR.
    Git hooks export GIT_DIR/GIT_INDEX_FILE; without env=_git_env() scrubbing
    them, these cwd-scoped calls would retarget the hook's repo (and `git init`
    can corrupt it — it once set core.bare=true on the shared config). Plain
    pytest/CI have no GIT_DIR, so only this test makes the scrub CI-catchable."""
    # A *different* repo, pointed at by an inherited GIT_DIR (the hook scenario).
    outer = tmp_path / "outer"
    outer.mkdir()
    subprocess.run(
        ["git", "init"], cwd=outer, capture_output=True, check=True, env=_git_env()
    )
    monkeypatch.setenv("GIT_DIR", str(outer / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(outer / ".git" / "index"))

    # The guard, run against a SEPARATE inner repo, must read inner's OWN index.
    # `fk` is pinned (curating) and its auto file is STAGED in inner, so a
    # cwd-discovered git sees it as tracked → nothing owed → [].
    inner = tmp_path / "inner"
    inner.mkdir()
    subprocess.run(
        ["git", "init"], cwd=inner, capture_output=True, check=True, env=_git_env()
    )
    (inner / "freeze.toml").write_text('fk = "curating"\n', encoding="utf-8")
    (inner / "fk.toml").write_text(
        '[register."1"]\nslug = "fk-reg"\n', encoding="utf-8"
    )
    (inner / "fk.auto.toml").write_text(
        '[variable."1.x"]\nslug = "x"\n', encoding="utf-8"
    )
    subprocess.run(
        ["git", "add", "fk.auto.toml"],
        cwd=inner,
        capture_output=True,
        check=True,
        env=_git_env(),
    )

    # With the scrub: `git ls-files` reads inner's index, sees the staged
    # fk.auto.toml as tracked → []. Without it (regression): inherited GIT_DIR
    # points ls-files at outer's EMPTY index → fk.auto.toml reads as untracked
    # → ["fk.auto.toml"]. The staged-but-cross-repo case is what the scrub fixes.
    assert _untracked_pinned_autos(inner) == []
