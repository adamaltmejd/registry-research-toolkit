"""G1: the pinned baseline reader against the checkout's reader, on the pinned artifacts.

    uv run python -m conformance.differential

Fetches (once) the pinned artifacts and the baseline reader environment into the
shared cache, derives a copy of each artifact with the checkout's builder (once per
base and builder source, so on a committed tree), generates the seeded cases, runs
each case through both readers' ``reg-meta`` CLI JSON in parallel worker processes
(the baseline on the originals, the checkout on the derived copies), and compares
exit code, stdout bytes and stderr per case. Writes ``report.json`` into
``<cache>/report/`` and prints a plain-text summary. Exit 0 when no case differs
(outside a named exception), 1 when any does.

The arm under test is this interpreter's ``reg_meta`` with the caller's environment,
so a perturbed copy is tested with ``PYTHONPATH=<copy>/src uv run python -m
conformance.differential``; the baseline arm runs isolated and never sees it.

Normalization: none. Every case passes ``--format json`` without ``-v``, which
prints only the payload's ``data``; the envelope fields the CLI contract marks as
volatile (``generated_at``, ``run.duration_ms``, ``database``) are never printed.
``order`` and ``validate`` print their canonical bytes. Outputs are compared byte for
byte.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import tomllib
from collections import Counter
from pathlib import Path

from conformance.differential import cache, cases

HERE = Path(__file__).resolve().parent
DRIVER = HERE / "driver.py"
MAX_PATHS = 20


def _diff_paths(a, b, path: str = "") -> list[str]:
    """Every JSON pointer where ``a`` and ``b`` differ, depth-first."""
    if type(a) is not type(b):
        return [path or "/"]
    if isinstance(a, dict):
        out: list[str] = []
        for key in sorted(a.keys() | b.keys()):
            sub = f"{path}/{str(key).replace('~', '~0').replace('/', '~1')}"
            if key not in a or key not in b:
                out.append(sub)
            else:
                out += _diff_paths(a[key], b[key], sub)
        return out
    if isinstance(a, list):
        out = [] if len(a) == len(b) else [f"{path}/length"]
        for i, (x, y) in enumerate(zip(a, b, strict=False)):
            out += _diff_paths(x, y, f"{path}/{i}")
        return out
    return [] if a == b else [path or "/"]


def _summary(result: dict | None) -> dict | None:
    if result is None:
        return None
    return {
        "exit": result["exit"],
        "stdout_sha256": hashlib.sha256(result["stdout"].encode()).hexdigest(),
        "stdout_bytes": len(result["stdout"].encode()),
        "stderr": result["stderr"][:2000],
    }


def compare(case_id: str, base: dict | None, cand: dict | None) -> dict | None:
    """The difference record for one case, or None when both arms agree."""
    if base is not None and cand is not None:
        if all(base[k] == cand[k] for k in ("exit", "stderr", "stdout")):
            return None
        try:
            paths = _diff_paths(json.loads(base["stdout"]), json.loads(cand["stdout"]))
        except ValueError:
            paths = ["(stdout is not JSON)"]
        fields = [
            name for name in ("exit", "stderr", "stdout") if base[name] != cand[name]
        ]
    else:
        paths, fields = (
            [],
            ["missing in " + ("baseline" if base is None else "checkout")],
        )
    return {
        "id": case_id,
        "fields": fields,
        "paths": paths,
        "baseline": _summary(base),
        "checkout": _summary(cand),
    }


def _excepted(diff: dict, exceptions: list[dict]) -> str | None:
    """Name of the exception covering every differing path of ``diff``, if any."""
    for exc in exceptions:
        if not fnmatch.fnmatchcase(diff["id"], exc["case"]):
            continue
        prefixes = exc.get("paths", [""])
        if diff["fields"] == ["stdout"] and all(
            any(not pre or p == pre or p.startswith(pre + "/") for pre in prefixes)
            for p in diff["paths"]
        ):
            return exc["name"]
    return None


def _worker(
    python: list[str],
    env: dict[str, str],
    db_dirs: dict[str, str],
    todo: queue.Queue,
    done: queue.Queue,
    arm: str,
) -> None:
    proc = subprocess.Popen(
        [*python, str(DRIVER)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
        text=True,
        encoding="utf-8",
    )
    assert proc.stdin is not None and proc.stdout is not None
    try:
        while True:
            try:
                case = todo.get_nowait()
            except queue.Empty:
                break
            case = cases.Case(
                case.id, [db_dirs.get(arg, arg) for arg in case.argv], case.page2
            )
            proc.stdin.write(case.to_json() + "\n")
            proc.stdin.flush()
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError(f"{arm} driver exited during {case.id}")
            for result in json.loads(line):
                done.put((arm, result))
    finally:
        proc.stdin.close()
        proc.wait()
        done.put((arm, None))


def run(config: dict) -> int:
    started = time.monotonic()
    pins = cache.Pins.from_config(config)
    baseline_python = cache.ensure_baseline(pins)
    dirs = cache.ensure_artifacts(pins)
    derived = cache.ensure_derived(pins, dirs)
    setup_seconds = time.monotonic() - started
    report_dir = cache.cache_root() / "report"
    shutil.rmtree(report_dir, ignore_errors=True)
    all_cases = cases.generate(dirs, config, report_dir / "projects")

    # Half the cores per arm; both arms run at once.
    workers = max(1, (os.cpu_count() or 2) // 2)
    arms = {
        "baseline": ([str(baseline_python), "-I"], cache.isolated_env(), {}),
        "checkout": (
            [sys.executable, "-P"],
            dict(os.environ),
            {str(dirs[c]): str(derived[c]) for c in dirs},
        ),
    }
    done: queue.Queue = queue.Queue()
    threads = []
    for arm, (python, env, db_dirs) in arms.items():
        todo: queue.Queue = queue.Queue()
        # Holdings-scope cases hold the slowest reads; queue them first so they
        # do not form the tail.
        for case in sorted(all_cases, key=lambda c: "/holdings/" not in c.id):
            todo.put(case)
        for _ in range(workers):
            t = threading.Thread(
                target=_worker,
                args=(python, env, db_dirs, todo, done, arm),
                daemon=True,
            )
            t.start()
            threads.append(t)

    pending: dict[str, dict[str, dict]] = {}
    differences: list[dict] = []
    compared: Counter[str] = Counter()
    seconds: Counter[str] = Counter()
    finished = 0
    while finished < len(threads):
        arm, result = done.get()
        if result is None:
            finished += 1
            continue
        slot = pending.setdefault(result["id"], {})
        slot[arm] = result
        if len(slot) == 2:
            del pending[result["id"]]
            command = result["id"].split("/")[2]
            compared[command] += 1
            seconds[command] += slot["baseline"]["seconds"]
            diff = compare(result["id"], slot["baseline"], slot["checkout"])
            if diff is not None:
                differences.append(diff)
    for case_id, slot in pending.items():
        compared[case_id.split("/")[2]] += 1
        differences.append(compare(case_id, slot.get("baseline"), slot.get("checkout")))
    for t in threads:
        t.join()

    exceptions = config["exception"]
    for diff in differences:
        diff["exception"] = _excepted(diff, exceptions)
        # The report keeps the first paths; the exception check above saw all.
        diff["path_count"] = len(diff["paths"])
        diff["paths"] = diff["paths"][:MAX_PATHS]
    differences.sort(key=lambda d: d["id"])
    unexcepted = [d for d in differences if d["exception"] is None]
    wall = time.monotonic() - started
    report = {
        "baseline_commit": pins.baseline_commit,
        "release_tag": pins.tag,
        "seed": config["seed"],
        "cases": sum(compared.values()),
        "cases_by_command": dict(sorted(compared.items())),
        "differences": len(unexcepted),
        "excepted": len(differences) - len(unexcepted),
        "wall_seconds": round(wall, 1),
        "setup_seconds": round(setup_seconds, 1),
        "workers_per_arm": workers,
        "baseline_seconds_by_command": {
            k: round(v, 1) for k, v in seconds.most_common()
        },
        "diffs": differences,
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    print(
        f"G1 {pins.tag} | baseline {pins.baseline_commit[:12]} vs checkout | "
        f"seed {config['seed']}"
    )
    print(
        f"{report['cases']} cases compared in {report['wall_seconds']} s "
        f"({report['setup_seconds']} s fetching and deriving, {workers} workers "
        f"per arm): {len(unexcepted)} differences, "
        f"{report['excepted']} excepted"
    )
    if unexcepted:
        by_command = Counter(d["id"].split("/")[2] for d in unexcepted)
        for command, n in sorted(by_command.items()):
            print(f"  {command}: {n} of {compared[command]}")
        for diff in unexcepted[:MAX_PATHS]:
            where = ", ".join(diff["paths"][:3]) or ", ".join(diff["fields"])
            print(f"  {diff['id']}: {where}")
    print(f"report: {report_path}")
    return 1 if unexcepted else 0


def main() -> int:
    config = tomllib.loads((HERE / "config.toml").read_text(encoding="utf-8"))
    with cache.locked(cache.cache_root()):
        return run(config)


if __name__ == "__main__":
    sys.exit(main())
