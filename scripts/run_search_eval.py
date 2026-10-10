"""Measure search relevance against the eval set (`search_eval.toml`, #393 item 10).

The eval set is ~steward-authored `(query -> intended result)` pairs; this runner
makes ranking changes *measurable* (the precondition #393 item 10 calls out): for
each case it asks a running Rust server (`reg-meta serve`) for the first page of the
case's arm (`GET /api/search?q=&type=&limit=`) and reports whether the case's
`intended` result appears in it, and at what rank.

This is a MAINTAINER tool, not a CI test: it needs a server over a real catalog (the
synthetic test fixtures don't carry catalog-scale content), so it is a `scripts/`
runner rather than a `tests/` module. Start the server first, for example
``reg-meta serve --db <dir> --catalog global --stewards reg_webapp/stewards --port 8001``.

`intended` grammar: an FQID (``scb/iot``, ``class/icd-10-se``) for a leaf result, or
``group:<key>`` for a folded concept-group row.

`expect`:
  - ``hit`` — the intended result should rank well TODAY (a regression guard).
  - ``gap`` — a confirmed-correct intended that does NOT surface today: a curation /
    concept-group target (e.g. a search pin, #311, or folding a classification
    family). A `gap` that becomes a hit is progress (`closed!`), not a failure.

Usage:
    uv run --no-project scripts/run_search_eval.py [--url URL] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

EVAL_PATH = Path(__file__).resolve().parent / "search_eval.toml"

# A case's `group` is the arm it searches. `value` is deliberately absent: a code
# hit has no FQID, so a `value` case could never match. The actionable target is
# always the owning entity (register / variable / classification).
GROUPS = ("register", "variable", "classification")


def _page(url: str, query: str, group: str, limit: int) -> dict:
    """The first page of `group`'s arm for `query`: `{items, next_cursor}`."""
    if group not in GROUPS:
        raise ValueError(
            f"unsupported eval group {group!r}; supported: {' | '.join(GROUPS)}"
        )
    params = urllib.parse.urlencode({"q": query, "type": group, "limit": limit})
    with urllib.request.urlopen(f"{url}/api/search?{params}") as response:
        return json.load(response)["data"]


def _result_id(item: dict) -> str | None:
    """The identifier a case's `intended` is matched against: ``group:<key>`` for a
    folded concept-group row, else the leaf FQID."""
    if item["type"] == "group":
        return f"group:{item['key']}"
    return item.get("fqid")


def _rank_of(items: list[dict], intended: str) -> int | None:
    """1-based rank of `intended` among `items`, or None if absent."""
    for i, item in enumerate(items, start=1):
        if _result_id(item) == intended:
            return i
    return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Measure search relevance vs search_eval.toml"
    )
    ap.add_argument(
        "--url",
        default="http://127.0.0.1:8001",
        help="base URL of a running `reg-meta serve` (default: %(default)s)",
    )
    ap.add_argument(
        "--limit", type=int, default=10, help="per-group result cap (rank window)"
    )
    args = ap.parse_args()

    cases = tomllib.loads(EVAL_PATH.read_text(encoding="utf-8"))["case"]
    rows: list[tuple] = []
    hit_total = hit_found = gap_total = gap_closed = 0
    for c in cases:
        try:
            page = _page(args.url, c["query"], c["group"], args.limit)
        except urllib.error.URLError as exc:
            print(f"error: no search answer from {args.url}: {exc}", file=sys.stderr)
            return 2
        rank = _rank_of(page["items"], c["intended"])
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
                str(len(page["items"])),
                str(page["next_cursor"] is not None).lower(),
                status,
            )
        )

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
