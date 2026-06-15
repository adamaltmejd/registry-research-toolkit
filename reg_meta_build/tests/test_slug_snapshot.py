"""Immutability snapshot for committed slug TOMLs (grow-only; see DESIGN.md → Slug immutability).

Compares the live TOMLs at ``reg_meta_build/fqid_slugs/`` against the
committed snapshot at ``reg_meta_build/fqid_slugs/.snapshot.json``. Adds
are allowed; removals and slug renames fail the build.

After legitimate additions, regenerate the snapshot with:

    reg-meta-build precheck-slugs --update-snapshot

and commit ``.snapshot.json`` alongside the new TOML rows.
"""

from __future__ import annotations

import pytest

from reg_meta_build.fqid_slugs import (
    SNAPSHOT_FILENAME,
    diff_snapshot,
    is_unfrozen,
    load_slug_dir,
    read_snapshot,
    repo_slug_dir,
    snapshot_payload,
)


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
    """Committed slugs are grow-only. Removals or slug renames here
    rot every project_data.json that pinned the old FQID.

    Skipped while the pre-v1 ``UNFROZEN`` sentinel exists in ``slug_dir``.
    Delete the sentinel at v1 release to re-arm this guard.
    """
    if is_unfrozen(slug_dir):
        pytest.skip(
            f"{slug_dir}/UNFROZEN present — pre-v1 curation iteration; "
            "rename guard re-arms when the sentinel is removed at v1."
        )
    previous = read_snapshot(slug_dir / SNAPSHOT_FILENAME)
    current = snapshot_payload(load_slug_dir(slug_dir))
    diff = diff_snapshot(previous, current)
    msgs = []
    if diff["removed"]:
        msgs.append("Removed entries (forbidden):")
        msgs.extend(f"  {r}" for r in diff["removed"])
    if diff["renamed"]:
        msgs.append("Renamed slugs (forbidden):")
        msgs.extend(f"  {r}" for r in diff["renamed"])
    if msgs:
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
