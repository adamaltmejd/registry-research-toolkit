"""One arm of the G1 harness: run ``reg-meta`` argv cases in one process.

Run as a script by the reader's own interpreter (the baseline venv, or the checkout's
``uv run python``), so it imports only the standard library and ``reg_meta``. Each
input line is a JSON case ``{"id", "argv", "page2"?}``; each output line is the
case's ``exit``, ``stdout``, ``stderr`` and ``seconds`` (reported, never compared).

Each case calls ``reg_meta.cli.run(argv)``: the function behind the ``reg-meta``
console script (``main`` returns it), with the same argv, stdout bytes and exit code.
Looping in one process avoids ~0.4 s of interpreter and import start-up per case,
which would put thousands of cases far over the G1 budget. ``run`` opens and closes
its own connection per call, so no state is shared between cases.

``page2``: when the first page's JSON carries a ``next_cursor``, run the same argv
again with ``--cursor`` and report it as case ``<id>/page2``. Each arm follows its own
cursor, so a cursor difference also shows up as a page difference.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import time


def _run(run, argv: list[str]) -> dict:
    out, err = io.StringIO(), io.StringIO()
    start = time.perf_counter()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(argv)
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 2
        except Exception as exc:  # noqa: BLE001 — an arm crash is a reported result
            code = "crash"
            err.write(f"{type(exc).__name__}: {exc}\n")
    return {
        "exit": code,
        "stdout": out.getvalue(),
        "stderr": err.getvalue(),
        "seconds": time.perf_counter() - start,
    }


def main() -> int:
    os.environ["REG_META_QUIET"] = "1"
    from reg_meta.cli import run

    sink = sys.stdout
    for line in sys.stdin:
        case = json.loads(line)
        results = [{"id": case["id"], **_run(run, case["argv"])}]
        if case.get("page2"):
            try:
                cursor = json.loads(results[0]["stdout"]).get("next_cursor")
            except ValueError, AttributeError:
                cursor = None
            if cursor:
                results.append(
                    {
                        "id": case["id"] + "/page2",
                        **_run(run, [*case["argv"], "--cursor", cursor]),
                    }
                )
        sink.write(json.dumps(results, ensure_ascii=False) + "\n")
        sink.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
