"""Spike benchmark: Rust `serve` (HTTP, MCP over HTTP, MCP over stdio) vs Python.

All Python numbers are in-process and warm (no start-up). Run from the worktree root:
  uv run --with mcp python spike/stage0/scripts/bench.py <catalog-dir> <spike-binary>
"""

import asyncio
import json
import socket
import sqlite3
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from reg_meta.db import open_db

from reg_meta import queries

sys.path.insert(0, str(Path(__file__).parent))
from parity_fts import SQL, corpus


def stats(label: str, ms: list[float]) -> None:
    ms = sorted(ms)
    p95 = ms[int(len(ms) * 0.95) - 1]
    print(
        f"{label:<44} n={len(ms):4d}  median {statistics.median(ms):7.2f} ms  p95 {p95:7.2f} ms  max {ms[-1]:7.2f} ms"
    )


def timed(fn, items) -> list[float]:
    out = []
    for item in items:
        t = time.perf_counter()
        fn(item)
        out.append((time.perf_counter() - t) * 1000)
    return out


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def mcp_http(url: str, qs: list[str]) -> tuple[int, list[float]]:
    async with (
        streamable_http_client(url) as streams,
        ClientSession(streams[0], streams[1]) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        size = len(
            json.dumps(
                [t.model_dump(mode="json", exclude_none=True) for t in tools.tools]
            )
        )
        ms = []
        for q in qs:
            t = time.perf_counter()
            await session.call_tool("search", {"query": q, "limit": 50})
            ms.append((time.perf_counter() - t) * 1000)
        return size, ms


async def mcp_stdio(binary: str, db: str, qs: list[str]) -> tuple[float, list[float]]:
    t0 = time.perf_counter()
    params = StdioServerParameters(command=binary, args=["mcp", "--db", db])
    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        ready = (time.perf_counter() - t0) * 1000
        ms = []
        for q in qs:
            t = time.perf_counter()
            await session.call_tool("search", {"query": q, "limit": 50})
            ms.append((time.perf_counter() - t) * 1000)
        return ready, ms


def main() -> None:
    db, binary = sys.argv[1], sys.argv[2]
    repo = Path(__file__).resolve().parents[3]
    raw = sqlite3.connect(
        f"file:{Path(db) / 'reg_meta.db'}?mode=ro&immutable=1", uri=True
    )
    qs = [q for q in corpus(raw, repo) if queries._fts_match_query(q)][:150]

    port = free_port()
    server = subprocess.Popen(
        [binary, "serve", "--db", db, "--port", str(port)],
        stderr=subprocess.PIPE,
        text=True,
    )
    print(server.stderr.readline().strip())
    base = f"http://127.0.0.1:{port}"

    def get(q: str) -> bytes:
        url = f"{base}/api/search?" + urllib.parse.urlencode({"query": q, "limit": 50})
        with urllib.request.urlopen(url) as r:
            return r.read()

    try:
        timed(get, qs)  # warm the page cache and connections
        stats("rust HTTP /api/search (warm)", timed(get, qs))
        t = time.perf_counter()
        with ThreadPoolExecutor(8) as pool:
            list(pool.map(get, qs * 4))
        print(
            f"{'rust HTTP, 8 concurrent clients':<44} {len(qs) * 4 / (time.perf_counter() - t):7.0f} req/s"
        )
        rss = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(server.pid)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        print(f"{'rust serve RSS after load':<44} {int(rss) / 1024:7.1f} MB")
        size, ms = asyncio.run(mcp_http(f"{base}/mcp", qs[:40]))
        stats("rust MCP over HTTP, tools/call search", ms)
        print(f"{'MCP tools/list payload (1 tool)':<44} {size} bytes")
    finally:
        server.terminate()

    ready, ms = asyncio.run(mcp_stdio(binary, db, qs[:40]))
    print(f"{'rust MCP stdio: spawn + initialize':<44} {ready:7.1f} ms")
    stats("rust MCP stdio, tools/call search", ms)

    conn = open_db(Path(db) / "reg_meta.db")

    def py_arm(q: str) -> None:
        raw.execute(SQL, (queries._fts_match_query(q),)).fetchall()

    timed(py_arm, qs)
    stats("python same arm SQL, in-process", timed(py_arm, qs))
    narrow = lambda q: queries.search(
        conn, q, field="description", type="variable", fold_groups=False
    )
    timed(narrow, qs[:40])
    stats("python search(description, variable), in-proc", timed(narrow, qs[:40]))
    full = lambda q: queries.search(conn, q)
    stats("python search() default, in-process", timed(full, qs[:40]))


if __name__ == "__main__":
    main()
