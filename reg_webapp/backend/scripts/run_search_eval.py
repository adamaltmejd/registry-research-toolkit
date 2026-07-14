"""Measure search relevance against the eval set (`search_eval.toml`, #393 item 10).

The eval set is ~steward-authored `(query -> intended result)` pairs; this runner
makes ranking changes *measurable* (the precondition #393 item 10 calls out): for
each case it runs the query the way `/api/search` does — one `reg_meta.queries.search`
call per result group (register / variable / classification via the FTS
`field="description"` path) — and reports whether the case's `intended` result appears
in its expected group, and at what rank.

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
    uv run python reg_webapp/backend/scripts/run_search_eval.py [--db PATH] [--limit N]
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.db import db_path_from_args, open_db
from reg_meta.queries import search
from reg_webapp.golden import apply_golden_boost, pinned_fqids

if TYPE_CHECKING:
    from reg_meta.search import SearchResult

EVAL_PATH = Path(__file__).resolve().parents[1] / "search_eval.toml"

# Map a case's `group` to the (field, fold_groups) reg_meta.search uses for that arm,
# mirroring reg_webapp/backend/src/reg_webapp/routes/search.py.
#
# `value` is deliberately absent: reg_meta.search(..., type="value") returns type:"code"
# rows (code/label, NO fqid), so `_result_id` is None and a `value` case can never match
# (false miss). The actionable target is always the owning entity (register / variable /
# classification), not a bare code, so the eval has no `value` group.
_GROUP_CALL = {
    "register": ("description", False),
    "variable": ("description", True),
    "classification": ("description", True),
}


def _resolve_db(db_arg: str | None) -> Path:
    """Resolve ``--db``: an explicit FILE path is used directly; otherwise treat the arg
    (or None) as a directory and apply reg_meta's resolution rules."""
    if db_arg:
        p = Path(db_arg).expanduser()
        if p.is_file():
            return p
    # `db_path_from_args` handles its own expansion for the directory/None case.
    return db_path_from_args(db_arg)


def _group_call(group: str) -> tuple[str, bool]:
    """The (field, fold_groups) for a case's `group`, failing fast on an unknown group."""
    try:
        return _GROUP_CALL[group]
    except KeyError:
        supported = " | ".join(_GROUP_CALL)
        raise ValueError(
            f"unsupported eval group {group!r}; supported: {supported}"
        ) from None


def _result_id(row: SearchResult) -> str | None:
    """The identifier a case's `intended` is matched against: ``group:<key>`` for a
    folded concept-group row, else the leaf FQID (as a canonical string). Operates on
    the reg_meta typed search models (#701)."""
    if row.type == "group":
        return f"group:{row.group_key}"
    fqid = getattr(row, "fqid", None)
    return str(fqid) if fqid is not None else None


def _rank_of(results: list[SearchResult], intended: str) -> int | None:
    """1-based rank of `intended` among `results`, or None if absent."""
    for i, row in enumerate(results, start=1):
        if _result_id(row) == intended:
            return i
    return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Measure search relevance vs search_eval.toml"
    )
    ap.add_argument(
        "--db",
        default=None,
        help=(
            "path to reg_meta.db, or the directory containing it (default: "
            "reg_meta's resolution — $REG_META_DB > XDG > platform default)"
        ),
    )
    ap.add_argument(
        "--limit", type=int, default=10, help="per-group result cap (rank window)"
    )
    args = ap.parse_args()

    db_file = _resolve_db(args.db)
    if not db_file.exists():
        print(
            f"error: no reg_meta.db at {db_file} (set --db or $REG_META_DB)",
            file=sys.stderr,
        )
        return 2

    cases = tomllib.loads(EVAL_PATH.read_text(encoding="utf-8"))["case"]
    # Open the release DB the way the app/CLI does — `open_db` uses `immutable=1`
    # so a published WAL-mode DB on a read-only/shared install doesn't try to touch
    # `-wal`/`-shm` sidecars (see reg_meta.db.open_db). `check_schema=False` mirrors
    # the webapp's `catalog_conn` and `open_db` sets `row_factory = sqlite3.Row`.
    conn = open_db(db_file, check_schema=False)

    rows: list[tuple] = []
    hit_total = hit_found = gap_total = gap_closed = 0
    for c in cases:
        field, fold = _group_call(c["group"])
        res = search(
            conn,
            c["query"],
            field=field,
            type=c["group"],
            limit=args.limit,
            fold_groups=fold,
        )
        # Apply golden boost over one bounded result window, matching the route's
        # page-sized pin work. Exact totals are intentionally unavailable: report the
        # returned page size and whether another origin/boosted row exists.
        pin_fqids = pinned_fqids(c["query"], c["group"])
        boosted = apply_golden_boost(
            conn,
            c["query"],
            c["group"],
            res.results,
            fqids=pin_fqids,
            limit=args.limit,
        )
        page = boosted[: args.limit]
        page_has_more = (
            res.has_more or len(boosted) > args.limit or len(pin_fqids) > args.limit
        )
        rank = _rank_of(page, c["intended"])
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
                str(len(page)),
                str(page_has_more).lower(),
                status,
            )
        )
    conn.close()

    hdr = (
        "query",
        "group",
        "intended",
        "expect",
        "rank",
        "returned",
        "has_more",
        "status",
    )
    w = [max(len(r[i]) for r in [hdr, *rows]) for i in range(len(hdr))]
    print("  ".join(h.ljust(w[i]) for i, h in enumerate(hdr)))
    print("  ".join("-" * width for width in w))
    for r in rows:
        print("  ".join(str(value).ljust(w[i]) for i, value in enumerate(r)))

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
