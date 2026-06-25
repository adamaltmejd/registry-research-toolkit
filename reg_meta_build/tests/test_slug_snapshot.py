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


def _git_committed_filenames(directory: Path) -> set[str] | None:
    """The filenames present in the committed HEAD tree DIRECTLY under
    ``directory`` (top-level only), or ``None`` when ``directory`` is not inside a
    git work tree OR HEAD is unborn (no commits) — the caller skips in that case.

    Reads the committed tree (``git ls-tree HEAD``), NOT the staging index: a
    staged-but-uncommitted file is deliberately NOT counted, because it won't
    survive a clean checkout. That is the whole point — the guard must catch a
    ``git add -f``'d-but-uncommitted file, which the index would falsely report as
    fine.
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
        ["git", "ls-tree", "-z", "-r", "--name-only", "HEAD", "--", "."],
        cwd=directory,
        capture_output=True,
        text=True,
        env=_git_env(),
    )
    if listed.returncode != 0:
        return None  # also covers an unborn HEAD (no commit) — can't assert
    names = (n for n in listed.stdout.split("\0") if n)
    return {n for n in names if "/" not in n}  # top-level entries only


def _untracked_pinned_autos(slug_dir: Path) -> list[str] | None:
    """The ``<provider>.auto.toml`` filenames a pinned (curating/frozen) zone has
    PRESENT on disk but NOT in the committed HEAD tree of ``slug_dir`` — i.e. they
    won't survive a clean checkout (a leftover from a prior ``churning`` build, OR
    a ``git add -f``'d-but-uncommitted file) — sorted.

    Scoped to present-but-uncommitted on purpose: that is the case the build-time
    ``slug_freeze_auto_missing`` guard CANNOT see (``is_file()`` is true locally,
    so the file looks fine to the build, yet it vanishes on a clean checkout). A
    pinned zone with NO auto file on disk is deliberately NOT flagged here — the
    absent case is the build guard's job, and it tolerates a variable-less pinned
    provider (gated on ``_provider_has_variables``, which has no variable slugs to
    pin and so writes no auto file).

    ``[]`` when nothing is pinned or every present pinned auto file is committed;
    ``None`` when ``slug_dir`` is not in a git work tree or HEAD is unborn (caller
    skips rather than false-fails)."""
    pinned = pinned_zones(load_freeze_states(slug_dir))
    if not pinned:
        return []
    committed = _git_committed_filenames(slug_dir)
    if committed is None:
        return None
    return sorted(
        name
        for zone in pinned
        if (name := f"{zone}{AUTO_FILE_SUFFIX}") not in committed
        and (slug_dir / name).is_file()
    )


def test_pinned_providers_auto_toml_git_tracked(slug_dir):
    """A provider pinned ``curating``/``frozen`` reads its slugs back from a
    committed ``<provider>.auto.toml``. That file is gitignored, so a
    present-but-uncommitted artifact — a leftover from a prior ``churning`` build,
    OR a ``git add -f``'d-but-uncommitted file — reports ``is_file()`` true locally
    and passes the build-time check yet vanishes on a clean checkout. This guard
    reads the committed HEAD tree (not the staging index), so it catches exactly
    that present-but-uncommitted window git-side — including a staged-but-uncommitted
    file the index would have reported as fine.

    The ABSENT-auto case is deliberately out of scope here: it stays the
    build-time ``slug_freeze_auto_missing`` guard's responsibility, which
    tolerates a variable-less pinned provider (it has no variable slugs to pin and
    so writes no auto file) via ``_provider_has_variables``.

    The build is git-agnostic by design, so this lives in the test layer. Today
    the repo ships all-churning (no ``freeze.toml``), so this passes vacuously.
    """
    missing = _untracked_pinned_autos(slug_dir)
    if missing is None:
        pytest.skip("slug_dir is not inside a git work tree")
    if missing:
        listed = "\n".join(f"  {name}" for name in missing)
        pytest.fail(
            "Pinned provider(s) have a present-but-uncommitted auto.toml "
            f"(would vanish on a clean checkout):\n{listed}\n"
            "Force-add AND commit the generated file "
            "(git add -f reg_meta_build/fqid_slugs/<name>, then commit it), or add "
            "a per-provider .gitignore negation "
            "(!reg_meta_build/fqid_slugs/<name>) and commit the file. Staging alone "
            "is not enough — the guard checks the committed tree."
        )


def test_untracked_pinned_auto_detected(tmp_path):
    """Regression for the guard: a pinned zone whose auto file is on disk but not
    in the committed HEAD tree is flagged; committing it clears the flag. A
    churning zone's auto file is never flagged. Self-contained — does not read the
    repo's state."""
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
    # A churning zone's auto file must NOT be flagged (churning zones are never
    # pinned). Committed here or not is irrelevant — included in the initial commit.
    (tmp_path / "umu.auto.toml").write_text(
        '[variable."1.x"]\nslug = "umu-x"\n', encoding="utf-8"
    )
    # Establish HEAD: the guard reads the committed tree, so without an initial
    # commit there is no HEAD to assert against (unborn → guard returns None).
    subprocess.run(
        ["git", "add", "-A"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=test",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )

    # 1. Both pinned autos on disk but uncommitted → flagged, sorted lexically
    #    across both pinned providers ("fk" < "scb"); churning `umu` is absent.
    (tmp_path / "fk.auto.toml").write_text(
        '[variable."1.personnummer"]\nslug = "personnummer"\n', encoding="utf-8"
    )
    (tmp_path / "scb.auto.toml").write_text(
        '[variable."1.kon"]\nslug = "kon"\n', encoding="utf-8"
    )
    assert _untracked_pinned_autos(tmp_path) == ["fk.auto.toml", "scb.auto.toml"]

    # 2. Commit both (staging alone no longer clears it — the guard reads HEAD,
    #    not the index) → cleared.
    subprocess.run(
        ["git", "add", "fk.auto.toml", "scb.auto.toml"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=test",
            "commit",
            "-m",
            "pin autos",
        ],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    assert _untracked_pinned_autos(tmp_path) == []


def test_staged_but_uncommitted_pinned_auto_flagged(tmp_path):
    """The exact Codex P2: the suite runs as a pre-push hook, so a pinned auto file
    that was ``git add -f``'d but NEVER committed sits in the index yet is absent
    from the committed tree the push will publish. Reading the index (the old
    ``git ls-files``) would treat it as fine and let the push through; the pushed
    commit lacks the file, so it vanishes on a clean checkout. Reading HEAD
    (``git ls-tree``) flags it. Self-contained — does not read the repo's state."""
    subprocess.run(
        ["git", "init"], cwd=tmp_path, capture_output=True, check=True, env=_git_env()
    )
    # `bar` is pinned (curating); the provider stub makes it a known zone.
    (tmp_path / "freeze.toml").write_text('bar = "curating"\n', encoding="utf-8")
    (tmp_path / "bar.toml").write_text(
        '[register."1"]\nslug = "bar-reg"\n', encoding="utf-8"
    )
    # Establish HEAD WITHOUT the auto file (the committed tree the push publishes).
    subprocess.run(
        ["git", "add", "-A"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=test",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    # Stage the auto file but DO NOT commit — the bypass the index check missed.
    (tmp_path / "bar.auto.toml").write_text(
        '[variable."1.x"]\nslug = "bar-x"\n', encoding="utf-8"
    )
    subprocess.run(
        ["git", "add", "bar.auto.toml"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    # Staged-but-uncommitted → not in HEAD → flagged. (Passes only against the
    # ls-tree HEAD reader; the old ls-files index reader would return [].)
    assert _untracked_pinned_autos(tmp_path) == ["bar.auto.toml"]


def test_variable_less_pinned_provider_not_flagged(tmp_path):
    """A pinned provider with NO ``<provider>.auto.toml`` on disk is NOT flagged —
    the variable-less case. ``write_auto_toml`` only writes an auto file when there
    are variable auto slugs, so a pinned provider with no variable rows legitimately
    has none; flagging it would be STRICTER than the build-time
    ``slug_freeze_auto_missing`` guard (gated on ``_provider_has_variables``) and
    would demand a synthetic empty auto file. The absent case is the build guard's
    job. Self-contained — does not read the repo's state."""
    subprocess.run(
        ["git", "init"], cwd=tmp_path, capture_output=True, check=True, env=_git_env()
    )
    # `foo` is pinned (curating); the provider stub makes it a known zone so
    # load_freeze_states accepts it. No `foo.auto.toml` is written → absent case.
    (tmp_path / "freeze.toml").write_text('foo = "curating"\n', encoding="utf-8")
    (tmp_path / "foo.toml").write_text(
        '[register."1"]\nslug = "foo-reg"\n', encoding="utf-8"
    )
    # Establish HEAD so the guard has a committed tree to read (unborn → None).
    subprocess.run(
        ["git", "add", "-A"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=test",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        capture_output=True,
        check=True,
        env=_git_env(),
    )
    assert _untracked_pinned_autos(tmp_path) == []


def _write_provider_with_auto(slug_dir: Path, provider: str, freeze: str | None) -> str:
    """Lay down a self-contained slug dir for ``provider`` with a committed-style
    ``<provider>.toml`` (one register) and a ``<provider>.auto.toml`` carrying one
    variable slug; optionally a ``freeze.toml`` pinning the zone to ``freeze``
    (omitted ⇒ the default churning state). Returns the variable's snapshot key
    (``<provider>/<source_id>``) so callers can assert its presence/absence."""
    if freeze is not None:
        (slug_dir / "freeze.toml").write_text(
            f'{provider} = "{freeze}"\n', encoding="utf-8"
        )
    (slug_dir / f"{provider}.toml").write_text(
        f'[register."1"]\nslug = "{provider}-reg"\n', encoding="utf-8"
    )
    (slug_dir / f"{provider}.auto.toml").write_text(
        '[variable."1.kon"]\nslug = "kon-auto"\n', encoding="utf-8"
    )
    return f"{provider}/1.kon"


@pytest.mark.parametrize("freeze", [None, "churning"])
def test_churning_auto_toml_not_loaded(tmp_path, freeze):
    """A churning zone's ``<provider>.auto.toml`` is gitignored/ephemeral, so a
    leftover untracked file from a prior build must NOT enter the in-memory slug
    index — otherwise its phantom variable slugs inflate ``snapshot_payload`` and
    false-fail ``test_snapshot_covers_committed_additions`` (the ``git clean -fdX``
    footgun). Covers both the explicit ``churning`` state and the default
    (no ``freeze.toml`` ⇒ churning)."""
    key = _write_provider_with_auto(tmp_path, "umu", freeze)
    payload = snapshot_payload(load_slug_dir(tmp_path))
    assert key not in payload["variable"]


@pytest.mark.parametrize("freeze", ["curating", "frozen"])
def test_pinned_auto_toml_loaded(tmp_path, freeze):
    """A pinned (curating/frozen) zone's committed ``<provider>.auto.toml`` IS the
    baseline, so its variable slugs must still flow into the snapshot."""
    key = _write_provider_with_auto(tmp_path, "fk", freeze)
    payload = snapshot_payload(load_slug_dir(tmp_path))
    assert payload["variable"][key] == "kon-auto"


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

    # The guard, run against a SEPARATE inner repo, must read inner's OWN HEAD.
    # `fk` is pinned (curating) and its auto file is COMMITTED in inner, so a
    # cwd-discovered git sees it in inner's committed tree → nothing owed → [].
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
        ["git", "add", "-A"], cwd=inner, capture_output=True, check=True, env=_git_env()
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=test",
            "commit",
            "-m",
            "pin fk auto",
        ],
        cwd=inner,
        capture_output=True,
        check=True,
        env=_git_env(),
    )

    # With the scrub: `git ls-tree HEAD` reads inner's committed tree, sees the
    # committed fk.auto.toml → []. Without it (regression): inherited GIT_DIR
    # points the calls at the outer repo (unborn HEAD / no such committed file) →
    # the guard returns None, not []. The cross-repo redirect is what the scrub
    # fixes — and only this test makes that CI-catchable.
    assert _untracked_pinned_autos(inner) == []
