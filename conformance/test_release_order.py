"""Tier 3 order agreement over a selected artifact (`--artifact-dir`; the `base`
fixture's artifact without one): whole-table projects on a deterministic sample of
tables, against an independent SQL census of the artifact.

Failure messages carry public ids and counts, never row content.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date, timedelta

import pytest
from http_cases import artifact_env

SAMPLE = 20
MAX_BINDINGS = 50
MCP_TABLES = 3
# Day numbers count from 1970-01-01 (crates/DESIGN.md, "The catalog builder").
EPOCH = date(1970, 1, 1)
# The period grammar's day tokens hold years 1900..=2099: a hull past them (an open
# sentinel above all) is ordered over the part inside them.
FIRST, LAST = date(1900, 1, 1), date(2099, 12, 31)
MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "mcp-protocol-version": "2025-11-25",
}


def day(iso: str) -> int:
    return (date.fromisoformat(iso) - EPOCH).days


def iso(number: int) -> str:
    return (EPOCH + timedelta(days=number)).isoformat()


def serving(db, table_id):
    """The table and its dated `continues` successors, followed transitively, as
    (table id, table pid, window start, window end), `None` for an open bound: each
    day goes to the table whose `at_start` window holds it (crates/DESIGN.md, "The
    logical order")."""
    chain, queue = [(None, table_id)], [table_id]
    seen = {table_id}
    while queue:
        rows = db.execute(
            "SELECT r.subject_id, r.at_start FROM relation r"
            " JOIN tbl t ON t.id = r.subject_id"
            " WHERE r.kind = 'continues' AND r.subject_kind = 'table'"
            " AND r.object_kind = 'table' AND r.object_id = ?"
            " AND r.at_start IS NOT NULL ORDER BY r.at_start, t.pid",
            (queue.pop(0),),
        ).fetchall()
        for subject, at in rows:
            if subject not in seen:
                seen.add(subject)
                chain.append((at, subject))
                queue.append(subject)
    chain.sort(key=lambda link: -1 << 62 if link[0] is None else link[0])
    ends = [at for at, _ in chain[1:]] + [None]
    return [
        (
            table,
            db.execute("SELECT pid FROM tbl WHERE id = ?", (table,)).fetchone()[0],
            at,
            end,
        )
        for (at, table), end in zip(chain, ends, strict=True)
    ]


def census(db_path):
    """Per sampled table: (pid, project period, bound variable pids, blocked, the
    segments of the tables serving the period, each clipped to its serving window,
    as (table pid, variable, start, end, spelling)), read from the artifact alone;
    and `meta`'s `open_start`."""
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as db:
        pids = [row[0] for row in db.execute("SELECT pid FROM tbl ORDER BY pid")]
        step = max(len(pids) // SAMPLE, 1)
        tables = []
        for pid in pids[::step][:SAMPLE]:
            table_id, timeless, lo, hi = db.execute(
                "SELECT t.id, t.timeless, min(s.start), max(s.end)"
                " FROM tbl t JOIN tbl_span s ON s.table_id = t.id WHERE t.pid = ?",
                (pid,),
            ).fetchone()
            if timeless:
                period = "_default"
                lo = hi = None
            else:
                lo = max(lo, day(FIRST.isoformat()))
                hi = min(hi, day(LAST.isoformat()) + 1)
                period = {"from": iso(lo), "to": iso(hi - 1)}
            variables = [
                row[0]
                for row in db.execute(
                    "SELECT DISTINCT v.pid FROM segment s"
                    " JOIN variable v ON v.id = s.variable_id"
                    " WHERE s.table_id = ? ORDER BY v.pid LIMIT ?",
                    (table_id, MAX_BINDINGS),
                )
            ]
            segments = []
            for serving_id, serving_pid, w_lo, w_hi in serving(db, table_id):
                bounds = [b for b in (lo, w_lo) if b is not None]
                start = max(bounds) if bounds else None
                bounds = [b for b in (hi, w_hi) if b is not None]
                end = min(bounds) if bounds else None
                for v, s_lo, s_hi, state, exact in db.execute(
                    "SELECT v.pid, s.start, s.end, s.state, sp.exact FROM segment s"
                    " JOIN variable v ON v.id = s.variable_id"
                    " JOIN spelling sp ON sp.id = s.spelling_id"
                    " WHERE s.table_id = ?",
                    (serving_id,),
                ):
                    s_lo = s_lo if start is None else max(s_lo, start)
                    s_hi = s_hi if end is None else min(s_hi, end)
                    if s_lo < s_hi:
                        segments.append((serving_pid, v, s_lo, s_hi, state, exact))
            # No edition is named, so an ambiguous or preliminary segment blocks, and
            # so does a bound variable with no segment over the period.
            covered = {s[1] for s in segments}
            blocked = any(
                s[1] in variables and s[4] in ("ambiguous", "preliminary")
                for s in segments
            ) or any(v not in covered for v in variables)
            spans = [
                (t, v, s_lo, s_hi, exact) for t, v, s_lo, s_hi, _, exact in segments
            ]
            tables.append((pid, period, variables, blocked, spans))
        (open_start,) = db.execute(
            "SELECT value FROM meta WHERE key = 'open_start'"
        ).fetchone()
    return tables, int(open_start)


def project(pid, period, variables):
    return {
        "schema_version": "5.0.0",
        "reg_meta_version": "0.64.0",
        "name": "Release order",
        "sources": [
            {
                "name": "s",
                "table": pid,
                "period": period,
                "bindings": [{"variable": v, "type": "categorical"} for v in variables],
            }
        ],
    }


@pytest.mark.release
def test_sampled_whole_table_orders_agree_with_census(artifact_dir, http_servers):
    """Fails when `validate` or `order` blocks a table the census finds orderable
    (or orders one it finds blocked: an ambiguous or preliminary window let through),
    when an entry's spelling is not the one the artifact holds at the entry's start,
    or when the MCP `order` tool answers a different document than HTTP."""
    db_path = artifact_dir / "reg_meta.db"
    tables, open_start = census(db_path)
    assert tables, "the artifact has no table to sample"
    client = http_servers.client(artifact_env(db_path))
    for index, (pid, period, variables, blocked, segments) in enumerate(tables):
        body = project(pid, period, variables)
        validated = client.post("/api/project/validate", json=body)
        assert validated.status_code == 200, f"{pid}: validate {validated.status_code}"
        ok = validated.json()["data"]["ok"]
        assert ok == (not blocked), (
            f"{pid}: validate ok={ok}, census blocked={blocked}"
            f" over {len(variables)} bindings"
        )
        ordered = client.post("/api/project/order", json=body)
        assert ordered.status_code == (422 if blocked else 200), (
            f"{pid}: order {ordered.status_code}, census blocked={blocked}"
        )
        if not blocked:
            entries = ordered.json()["data"]["entries"]
            for number, entry in enumerate(entries):
                start = entry["period"]["start"]
                at = open_start if start is None else day(start)
                spellings = [
                    exact
                    for table, variable, lo, hi, exact in segments
                    if variable == entry["variable"]
                    and table == entry["table"]
                    and lo <= at < hi
                ]
                assert spellings == [entry["spelling"]], (
                    f"{pid}: entry {number} of {len(entries)} ({entry['variable']})"
                    f" has no single census spelling at its start"
                )
        if index < MCP_TABLES:
            reply = client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "order",
                        "arguments": {"operation": "order", "project": body},
                    },
                },
                headers=MCP_HEADERS,
            ).json()["result"]
            assert reply["isError"] == blocked, f"{pid}: MCP order isError"
            assert reply["structuredContent"] == ordered.json(), (
                f"{pid}: MCP order differs from HTTP"
            )
