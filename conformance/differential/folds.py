"""G1's exhaustive fold sweep: the baseline reader's ``fold_search`` against
``reg-core-py`` (RUST_RUNTIME_SPEC.md section 5).

Inputs, in a fixed order: every Unicode scalar value, bare and in the stage-0
contexts, then every distinct string the pinned artifacts hold in a full-text index
(every column of every FTS5 table, read with ``sqlite3``). The baseline interpreter
folds them in chunks over a pipe; this process folds them with ``reg_core_py``, the
checkout's build.

One named exception, ``baseline-ucd-unassigned``: an input holding a scalar the
baseline interpreter's ``unicodedata`` leaves unassigned (category ``Cn``). reg-core is
on Unicode 17.0 and Python 3.14 on 16.0, so those inputs may fold differently. The
baseline computes the set, so it always matches the baseline's own UCD.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
import subprocess
import time
from typing import TYPE_CHECKING

import reg_core_py

from conformance.differential import cache

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

EXCEPTION = "baseline-ucd-unassigned"
CHUNK = 100_000
MAX_EXAMPLES = 20

# The stage-0 sweep's contexts, as in `crates/reg-core/tools/gen_fold_corpus.py`:
# bare, Final_Sigma on both sides, token boundaries, strip.
CONTEXTS = (
    lambda c: c,
    lambda c: "ΑΣ" + c,
    lambda c: "Α" + c + "Σ",
    lambda c: c + " x",
    lambda c: " " + c + " ",
)

# Runs in the baseline interpreter, so it imports only the stdlib and its reg_meta.
BASELINE = """\
import json, sys, unicodedata
from reg_meta.queries import fold_search
unassigned = [c for c in range(0x110000) if unicodedata.category(chr(c)) == "Cn"]
print(json.dumps(unassigned), flush=True)
for line in sys.stdin:
    print(json.dumps([fold_search(s) for s in json.loads(line)]), flush=True)
"""


def _indexed_strings(db: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    try:
        tables = [
            name
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND sql LIKE 'CREATE VIRTUAL TABLE % USING fts5%' ORDER BY name"
            )
        ]
        return {
            str(value)
            for table in tables
            for row in conn.execute(f"SELECT * FROM {table}")
            for value in row
            if value is not None
        }
    finally:
        conn.close()


def _inputs(dirs: dict[str, Path]) -> Iterator[str]:
    scalars = [chr(c) for c in range(0x110000) if not 0xD800 <= c <= 0xDFFF]
    indexed: set[str] = set()
    for directory in dirs.values():
        indexed |= _indexed_strings(directory / cache.DB_FILENAME)
    return itertools.chain(
        (context(c) for context in CONTEXTS for c in scalars), sorted(indexed)
    )


def sweep(baseline_python: Path, dirs: dict[str, Path]) -> dict:
    """The sweep's report section: input count, differences and excepted count."""
    started = time.monotonic()
    proc = subprocess.Popen(
        [str(baseline_python), "-I", "-c", BASELINE],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=cache.isolated_env(),
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None
    count, excepted, differences = 0, 0, []
    try:
        unassigned = set(json.loads(proc.stdout.readline()))
        for chunk in itertools.batched(_inputs(dirs), CHUNK):
            proc.stdin.write(json.dumps(chunk) + "\n")
            proc.stdin.flush()
            expected = json.loads(proc.stdout.readline())
            count += len(chunk)
            for text, base in zip(chunk, expected, strict=True):
                checkout = reg_core_py.fold_search(text)
                if checkout == base:
                    continue
                if any(ord(c) in unassigned for c in text):
                    excepted += 1
                else:
                    differences.append(
                        {"input": text, "baseline": base, "checkout": checkout}
                    )
    finally:
        proc.stdin.close()
        proc.wait()
    return {
        "inputs": count,
        "differences": len(differences),
        "excepted": {EXCEPTION: excepted},
        "wall_seconds": round(time.monotonic() - started, 1),
        "diffs": differences[:MAX_EXAMPLES],
    }
