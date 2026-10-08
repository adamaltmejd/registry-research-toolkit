#!/usr/bin/env python3
"""schema-pending-bump — classify code-vs-released-asset schema: break / pending / compatible.

The `build-image` job (`.github/workflows/container-build.yml`) bakes the newest
`reg_meta/v*` release's DB + doc assets via `reg-meta update`. That refuses an asset
whose schema is BEHIND the code (`incompatible_docs_asset`, exit 10), and the image's
Rust server (`reg-meta serve`) refuses to boot on a DB behind its own minimum. When
`main`'s `SCHEMA_VERSION` / `DOC_SCHEMA_VERSION` or the Rust minimum is merely AHEAD of
the latest released asset (same major, higher minor), that refusal is **expected and
self-clearing**: the owed reg_meta release will ship a matching asset. A MAJOR mismatch
— or a schema-behind release that is ALSO missing an asset — is a genuine #343
incompatibility.

This helper is the pure, unit-testable comparison the workflow can't exercise on a
normal commit (main's schema usually equals the latest release). It classifies each
axis (Python reader DB, doc, Rust server DB) of code-vs-asset and folds them into a
THREE-way verdict, printed as a single token to stdout:

- ``break``      — any axis is a major mismatch: a genuine incompatibility.
- ``pending``    — no break, ≥1 axis is code-ahead (same major): the neutralizable
                   pending-release case.
- ``compatible`` — otherwise (asset == code, or asset ahead on the same major).

The explanation goes to stderr; exit is 0 for any well-formed verdict (a malformed
version still exits non-zero). The WORKFLOW turns ``break`` (and a ``pending`` whose
release is missing a DB asset) into a FAILED `schema-guard` job — failing red blocks
build-image + deploy + edge-deploy (all gated on `schema-guard` success), closing the
edge-only hole where a skipped bake left nothing to fail. Only ``pending`` with both
assets present is green-neutralized; ``compatible`` proceeds normally.

Source of truth for the compat rule is ``_check_schema_compat`` in
``reg_meta/src/reg_meta/db.py`` (mirrored in ``doc_db.py``) and the schema gate on
``SCHEMA`` in ``crates/reg-catalog/src/lib.rs``: code ``M.m`` requires an asset with
the SAME major M and minor >= m. This module re-implements only the version arithmetic
(a deviation from DRY — the workflow can't import reg_meta at the released tag's
schema); keep them in sync if that rule ever changes. The Python axes go when the
bake's fetch moves to Rust (RUST_RUNTIME_SPEC.md stage 4).

Usage:
    python3 scripts/schema_pending_bump.py --code-db 5.4.0 --code-doc 1.1.0 \\
        --code-rust 5.4 --asset-db 5.4.0 --asset-doc 1.0.0
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


def classify_overall(
    code_db: str, code_doc: str, code_rust: str, asset_db: str, asset_doc: str
) -> tuple[str, str]:
    """Fold the axes into a three-way verdict plus a one-line human explanation.

    A ``break`` on ANY axis dominates — a genuine incompatibility is never
    neutralized. Absent a break, a ``pending`` on any axis makes the overall
    verdict pending; otherwise compatible.

    Returns ``(verdict, explanation)`` where verdict is ``"break"`` | ``"pending"``
    | ``"compatible"``.
    """
    axes = tuple(
        (label, classify_axis(code, asset), code, asset)
        for label, code, asset in (
            ("main DB", code_db, asset_db),
            ("doc", code_doc, asset_doc),
            ("Rust server DB", code_rust, asset_db),
        )
    )

    breaks = [
        (label, code, asset) for label, axis, code, asset in axes if axis == "break"
    ]
    if breaks:
        label, code, asset = breaks[0]
        return "break", (
            f"{label} schema: major mismatch code {code} vs asset {asset} "
            f"→ genuine break"
        )

    pendings = [
        (label, code, asset) for label, axis, code, asset in axes if axis == "pending"
    ]
    if pendings:
        label, code, asset = pendings[0]
        return "pending", (
            f"{label} schema: code {code} ahead of asset {asset} "
            f"→ pending reg_meta release"
        )

    return "compatible", (
        f"schemas compatible: DB code {code_db} (Rust {code_rust}) / asset "
        f"{asset_db}, doc code {code_doc} / asset {asset_doc}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-db", required=True, help="code SCHEMA_VERSION")
    parser.add_argument("--code-doc", required=True, help="code DOC_SCHEMA_VERSION")
    parser.add_argument(
        "--code-rust", required=True, help="Rust server minimum DB schema (M.m)"
    )
    parser.add_argument("--asset-db", required=True, help="released asset DB schema")
    parser.add_argument("--asset-doc", required=True, help="released asset doc schema")
    args = parser.parse_args(argv)

    try:
        verdict, explanation = classify_overall(
            args.code_db, args.code_doc, args.code_rust, args.asset_db, args.asset_doc
        )
    except ValueError as exc:
        print(f"schema_pending_bump: {exc}", file=sys.stderr)
        return 2

    print(explanation, file=sys.stderr)
    print(verdict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
