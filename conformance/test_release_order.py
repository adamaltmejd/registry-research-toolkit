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


def census(db_path):
    """Per sampled table: (pid, project period, bound variable pids, blocked, its
    segments over the period as (variable, start, end, spelling)), read from the
    artifact alone; and `meta`'s `open_start`."""
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
            segments = db.execute(
                "SELECT v.pid, s.start, s.end, s.state, sp.exact FROM segment s"
                " JOIN variable v ON v.id = s.variable_id"
                " JOIN spelling sp ON sp.id = s.spelling_id"
                " WHERE s.table_id = ?",
                (table_id,),
            ).fetchall()
            if not timeless:
                segments = [s for s in segments if s[1] < hi and s[2] > lo]
            # No edition is named, so an ambiguous or preliminary segment blocks, and
            # so does a bound variable with no segment over the period.
            covered = {s[0] for s in segments}
            blocked = any(
                s[0] in variables and s[3] in ("ambiguous", "preliminary")
                for s in segments
            ) or any(v not in covered for v in variables)
            spans = [(v, start, end, exact) for v, start, end, _, exact in segments]
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
                    for variable, lo, hi, exact in segments
                    if variable == entry["variable"]
                    and entry["table"] == pid
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
