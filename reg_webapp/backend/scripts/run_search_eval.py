"""Measure search relevance against the eval set (`search_eval.toml`, #393 item 10).

The eval set is ~steward-authored `(query -> intended result)` pairs; this runner
makes ranking changes *measurable* (the precondition #393 item 10 calls out): for
each case it runs the query the way `/api/search` does — one `reg_meta.queries.search`
call per result group (register / variable / classification via the FTS
`field="description"` path; codes via `field="value"`) — and reports whether the
case's `intended` result appears in its expected group, and at what rank.

This is a MAINTAINER tool, not a CI test: it needs a real `reg_meta.db` (the synthetic
test fixtures don't carry catalog-scale content), so it is a `scripts/` runner rather
than a `tests/` module. It resolves the DB via reg_meta's normal rules
(``$REG_META_DB`` > ``$XDG_DATA_HOME/reg_meta`` > platform default); pass ``--db`` to
override.

`intended` grammar: an FQID (``scb/iot``, ``class/icd-10-se``) for a leaf result, or
``group:<group_key>`` for a folded concept-group row.

`expect`:
  - ``hit`` — the intended result should rank well TODAY (a regression guard).
  - ``gap`` — a confirmed-correct intended that does NOT surface today: a curation /
    concept-group target (e.g. golden-boost #311, or folding a classification family).
    A `gap` that becomes a hit is progress (`closed!`), not a failure.

Usage:
    uv run python reg_webapp/backend/scripts/run_search_eval.py [--db DIR_OR_FILE] [--limit N]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import tomllib
from pathlib import Path

from reg_meta.db import db_path_from_args
from reg_meta.queries import search
from reg_webapp.golden import apply_golden_boost

EVAL_PATH = Path(__file__).resolve().parents[1] / "search_eval.toml"

# Map a case's `group` to the (field, fold_groups) reg_meta.search uses for that arm,
# mirroring reg_webapp/backend/src/reg_webapp/routes/search.py.
_GROUP_CALL = {
    "register": ("description", False),
    "variable": ("description", True),
    "classification": ("description", True),
    "value": ("value", False),
}


def _result_id(row: dict) -> str | None:
    """The identifier a case's `intended` is matched against: ``group:<key>`` for a
    folded concept-group row, else the leaf FQID."""
    if row.get("type") == "group":
        return f"group:{row.get('group_key')}"
    return row.get("fqid")


def _rank_of(results: list[dict], intended: str) -> int | None:
    """1-based rank of `intended` among `results`, or None if absent."""
    for i, row in enumerate(results, start=1):
        if _result_id(row) == intended:
            return i
    return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Measure search relevance vs search_eval.toml"
    )
    ap.add_argument("--db", default=None, help="reg_meta.db file or its directory")
    ap.add_argument(
        "--limit", type=int, default=10, help="per-group result cap (rank window)"
    )
    args = ap.parse_args()

    db_file = db_path_from_args(args.db)
    if not db_file.exists():
        print(
            f"error: no reg_meta.db at {db_file} (set --db or $REG_META_DB)",
            file=sys.stderr,
        )
        return 2

    cases = tomllib.loads(EVAL_PATH.read_text(encoding="utf-8"))["case"]
    conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    rows: list[tuple] = []
    hit_total = hit_found = gap_total = gap_closed = 0
    for c in cases:
        field, fold = _GROUP_CALL[c["group"]]
        res = search(
            conn,
            c["query"],
            field=field,
            type=c["group"],
            limit=args.limit,
            fold_groups=fold,
        )
        # Apply golden-boost the same way `/api/search` does (over the raw dicts),
        # so the eval reflects the route's true post-boost ranking. The net-new
        # injection bumps the displayed total too, matching the route's
        # total_count adjustment.
        boosted = apply_golden_boost(conn, c["query"], c["group"], res["results"])
        total = res["total_count"] + (len(boosted) - len(res["results"]))
        rank = _rank_of(boosted, c["intended"])
        found = rank is not None
        if c["expect"] == "hit":
            hit_total += 1
            hit_found += found
            status = "ok" if found else "MISS"
        else:  # gap
            gap_total += 1
            gap_closed += found
            status = "closed!" if found else "gap"
        rows.append(
            (
                c["query"],
                c["group"],
                c["intended"],
                c["expect"],
                str(rank) if found else "-",
                f"{total}",
                status,
            )
        )
    conn.close()

    w = [
        max(
            len(r[i])
            for r in [
                ("query", "group", "intended", "expect", "rank", "total", "status"),
                *rows,
            ]
        )
        for i in range(7)
    ]
    hdr = ("query", "group", "intended", "expect", "rank", "total", "status")
    print("  ".join(h.ljust(w[i]) for i, h in enumerate(hdr)))
    print("  ".join("-" * w[i] for i in range(7)))
    for r in rows:
        print("  ".join(str(r[i]).ljust(w[i]) for i in range(7)))

    print()
    print(
        f"hit cases:  {hit_found}/{hit_total} intended result found in top {args.limit}"
    )
    print(f"gap cases:  {gap_closed}/{gap_total} now surfaced (closed)")
    if hit_total and hit_found < hit_total:
        print(
            "\nMISS on an `expect=hit` case = a likely relevance REGRESSION (the intended "
            "is steward-confirmed) — investigate the ranking change that dropped it."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
