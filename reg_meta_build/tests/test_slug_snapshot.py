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
    frozen_zones,
    load_freeze_states,
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
