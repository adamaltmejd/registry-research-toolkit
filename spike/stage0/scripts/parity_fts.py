"""Spike parity: Rust variable-FTS arm vs the same arm in Python on the pinned catalog.

Python side: `reg_meta.queries._fts_match_query` plus the arm's SQL run through
`sqlite3` (reference scope, no register/year filters). Rust side: `reg-meta-spike
once`. Compares the ordered (variable_id, fqid, rank) lists per query.

Run from the worktree root:
  uv run python spike/stage0/scripts/parity_fts.py <catalog-dir> <spike-binary>
"""

import json
import random
import sqlite3
import subprocess
import sys
import tomllib
from pathlib import Path

from reg_meta import queries

SQL = (
    "SELECT vf.rowid, vf.name, bm25(variable_fts, 0.2, 0.2, 6.0, 4.0, 2.0, 1.0, 0.4) AS rank, "
    "r.slug, p.slug, v.slug FROM variable_fts vf "
    "JOIN register r ON vf.register_id = r.register_id "
    "JOIN provider p ON p.provider_id = r.provider_id "
    "JOIN variable v ON v.variable_id = vf.rowid "
    "WHERE variable_fts MATCH ? ORDER BY rank, vf.rowid LIMIT 50"
)


def corpus(conn: sqlite3.Connection, repo: Path) -> list[str]:
    evals = tomllib.loads((repo / "reg_webapp/backend/search_eval.toml").read_text())
    queries_ = [case["query"] for case in evals.get("case", []) if "query" in case]
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM variable WHERE name IS NOT NULL ORDER BY variable_id"
        )
    ]
    words = sorted({w for n in names for w in n.split() if len(w) > 2})
    random.Random(0).shuffle(words)
    edge = [
        "kön",
        "år",
        "Kön",
        "KÖN",
        '"',
        "a-b",
        "0115",
        "inkomst 2019",
        "sni2007",
        "ålder",
        "x*",
        "NOT",
        "(",
        "",
        "   ",
        "född år",
        "ß",
        "İstanbul",
    ]
    return queries_ + words[:300] + edge


def python_arm(conn: sqlite3.Connection, query: str) -> list[list]:
    fts = queries._fts_match_query(query)
    if fts is None:
        return []
    rows = conn.execute(SQL, (fts,)).fetchall()
    return [
        [r[0], f"{r[4]}/{r[3]}/{r[5]}" if r[3] and r[4] and r[5] else None, r[2]]
        for r in rows
    ]


def rust_arm(binary: str, db: str, query: str) -> list[list]:
    out = subprocess.run(
        [binary, "once", "--db", db, query], capture_output=True, text=True, check=True
    )
    items = json.loads(out.stdout)["data"]["items"]
    return [[i["variable_id"], i["fqid"], i["rank"]] for i in items]


def main() -> None:
    db, binary = sys.argv[1], sys.argv[2]
    repo = Path(__file__).resolve().parents[3]
    conn = sqlite3.connect(
        f"file:{Path(db) / 'reg_meta.db'}?mode=ro&immutable=1", uri=True
    )
    print("python sqlite", sqlite3.sqlite_version)
    qs = corpus(conn, repo)
    order_mismatch, rank_drift, empty = [], 0, 0
    for q in qs:
        py, rs = python_arm(conn, q), rust_arm(binary, db, q)
        if not py:
            empty += 1
        if [r[:2] for r in py] != [r[:2] for r in rs]:
            order_mismatch.append(q)
        elif any(abs(a[2] - b[2]) > 1e-9 for a, b in zip(py, rs)):
            rank_drift += 1
    print(f"queries {len(qs)}, empty results {empty}")
    print(f"order/identity mismatches {len(order_mismatch)}: {order_mismatch[:10]}")
    print(f"same order but bm25 drift > 1e-9: {rank_drift}")


if __name__ == "__main__":
    main()
