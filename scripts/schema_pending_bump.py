#!/usr/bin/env python3
"""schema-pending-bump — does a build-image bake failure mean a *pending* release?

The `build-image` job (`.github/workflows/container-build.yml`) bakes the newest
`reg_meta/v*` release's DB + doc assets via `reg-meta update`. That refuses an asset
whose schema is BEHIND the code (`incompatible_docs_asset`, exit 10) → red build →
all deploys pause. When `main`'s `SCHEMA_VERSION` / `DOC_SCHEMA_VERSION` is ahead of
the latest released asset, that refusal is **expected and self-clearing**: the owed
reg_meta release will ship a matching asset. A major mismatch, a missing asset, or a
genuinely broken bake must STILL fail red (the #343 loud-failure behavior).

This helper is the pure, unit-testable comparison the workflow can't exercise on a
normal commit (main's schema usually equals the latest release). It classifies each
axis (db, doc) of code-vs-asset and prints `true` (pending — neutralize) or `false`
(not pending — let the bake fail / proceed).

Source of truth for the compat rule is ``_check_schema_compat`` in
``reg_meta/src/reg_meta/db.py`` (mirrored in ``doc_db.py``): code ``M.m.p`` requires
an asset with the SAME major M and minor >= m. This module re-implements only the
version arithmetic (a deviation from DRY — the workflow can't import reg_meta at the
released tag's schema); keep the two in sync if that rule ever changes.

Usage:
    python3 scripts/schema_pending_bump.py \\
        --code-db 5.4.0 --code-doc 1.1.0 --asset-db 5.4.0 --asset-doc 1.0.0
"""

from __future__ import annotations

import argparse
import sys


def _parse(version: str) -> tuple[int, int]:
    """Return ``(major, minor)`` for an ``M.m.p`` version string.

    Raises ``ValueError`` on anything unparseable — callers fail loud rather than
    silently treating a malformed version as "not pending".
    """
    parts = version.split(".")
    if len(parts) < 2:
        raise ValueError(f"not an M.m.p version: {version!r}")
    return int(parts[0]), int(parts[1])


def classify_axis(code: str, asset: str) -> str:
    """Classify one schema axis (db or doc) comparing code vs released asset.

    - ``"break"``      — major differs: a genuine incompatibility (must fail red).
    - ``"pending"``    — same major AND asset minor < code minor: code is ahead, the
                         asset will catch up at the owed release.
    - ``"compatible"`` — otherwise (asset == code, or asset ahead on the same major).
    """
    code_major, code_minor = _parse(code)
    asset_major, asset_minor = _parse(asset)
    if asset_major != code_major:
        return "break"
    if asset_minor < code_minor:
        return "pending"
    return "compatible"


def is_pending_bump(
    code_db: str, code_doc: str, asset_db: str, asset_doc: str
) -> tuple[bool, str]:
    """Overall verdict across both axes plus a one-line human explanation.

    A ``break`` on EITHER axis suppresses any ``pending`` — a genuine
    incompatibility is never neutralized. Pending only when at least one axis is
    pending and no axis breaks.
    """
    db = classify_axis(code_db, asset_db)
    doc = classify_axis(code_doc, asset_doc)

    breaks = [
        (label, code, asset)
        for label, axis, code, asset in (
            ("main DB", db, code_db, asset_db),
            ("doc", doc, code_doc, asset_doc),
        )
        if axis == "break"
    ]
    if breaks:
        label, code, asset = breaks[0]
        return False, (
            f"{label} schema: major mismatch code {code} vs asset {asset} "
            f"→ genuine break, will fail bake"
        )

    pendings = [
        (label, code, asset)
        for label, axis, code, asset in (
            ("main DB", db, code_db, asset_db),
            ("doc", doc, code_doc, asset_doc),
        )
        if axis == "pending"
    ]
    if pendings:
        label, code, asset = pendings[0]
        return True, (
            f"{label} schema: code {code} ahead of asset {asset} "
            f"→ pending reg_meta release"
        )

    return False, (
        f"schemas compatible: DB code {code_db} / asset {asset_db}, "
        f"doc code {code_doc} / asset {asset_doc} → not pending"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-db", required=True, help="code SCHEMA_VERSION")
    parser.add_argument("--code-doc", required=True, help="code DOC_SCHEMA_VERSION")
    parser.add_argument("--asset-db", required=True, help="released asset DB schema")
    parser.add_argument("--asset-doc", required=True, help="released asset doc schema")
    args = parser.parse_args(argv)

    try:
        pending, explanation = is_pending_bump(
            args.code_db, args.code_doc, args.asset_db, args.asset_doc
        )
    except ValueError as exc:
        print(f"schema_pending_bump: {exc}", file=sys.stderr)
        return 2

    print(explanation, file=sys.stderr)
    print("true" if pending else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
