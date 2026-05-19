"""Print every single-variant register, classified as an `_default` candidate.

Disposable bootstrap helper for the pre-v1 slug-curation push (issue #95,
following PR #90's sweep). Reads the live reg_meta DB, walks every register
that has exactly one variant, and groups them into three classes:

  * exact       — variant name mirrors register name verbatim.
  * near        — mirror after stripping parenthetical abbrevs, swapping
                  Survey/Statistics siblings, or substring of canonical form.
  * kept        — variant name carries genuinely-different info; leave it
                  descriptive.

Run against a slugged or `--skip-slugs` build (it joins on names, not slugs).
After v1 freeze (§5.4 *Activation*) this script becomes redundant: new rows
will be picked up by the one-line stderr hint folded into `precheck-slugs`.

Usage:
    uv run python scripts/suggest_default_slugs.py [DB_PATH]

DB_PATH defaults to the resolved reg_meta default (REG_META_DB env or XDG).
"""

from __future__ import annotations

import sys
from pathlib import Path

from reg_meta.db import db_path_from_args, open_db
from reg_meta_build.fqid_slugs import iter_default_slug_candidates


def main(argv: list[str]) -> int:
    if len(argv) > 1:
        arg = Path(argv[1]).expanduser()
        # `.db` suffix → treat as file path even if missing, so a mistyped
        # filename reports `no reg_meta DB at <typo>` instead of resolving
        # to a nonsense `<typo>/reg_meta.db`.
        db_path = (
            arg if arg.suffix == ".db" or arg.is_file() else db_path_from_args(str(arg))
        )
    else:
        db_path = db_path_from_args(None)
    if not db_path.is_file():
        print(f"error: no reg_meta DB at {db_path}", file=sys.stderr)
        return 2

    conn = open_db(db_path)
    try:
        candidates = list(iter_default_slug_candidates(conn))
    finally:
        conn.close()

    by_class: dict[str, list] = {"exact": [], "near": [], "kept": []}
    for cand in candidates:
        by_class[cand.classification].append(cand)

    total = len(candidates)
    print(f"# `_default` candidates — {total} single-variant register(s)")
    print(f"# DB: {db_path}")
    print(
        f"# exact={len(by_class['exact'])}  "
        f"near={len(by_class['near'])}  "
        f"kept={len(by_class['kept'])}"
    )
    print()

    for cls, label in (
        ("exact", "EXACT MATCH — variant name mirrors register name"),
        ("near", "NEAR MATCH — heuristic suggests `_default`"),
        ("kept", "KEPT DESCRIPTIVE — variant carries unique info"),
    ):
        rows = by_class[cls]
        if not rows:
            continue
        print(f"## {label}  ({len(rows)})")
        for cand in rows:
            already = (
                "  [already _default]"
                if cand.current_slug == "_default"
                else f"  [current: {cand.current_slug!r}]"
                if cand.current_slug
                else "  [unslugged]"
            )
            print(
                f"  {cand.provider}/{cand.source_id}{already}\n"
                f"    register: {cand.register_name!r}\n"
                f"    variant : {cand.variant_name!r}\n"
                f"    reason  : {cand.reason}"
            )
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
