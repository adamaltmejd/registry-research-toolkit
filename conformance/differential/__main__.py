"""G1: the pinned release's server against the checkout's, on the pinned artifacts.

    uv run python -m conformance.differential

Fetches (once) the pinned artifacts and builds the release tag's ``reg-meta`` binary
into the shared cache, derives a candidate copy of each artifact with the checkout's
builder (once per base and builder source, so on a committed tree), writes the seeded
projects from the originals (``cases.py``), and sends every served request
(``served.py``) to both servers: the release tag's on the originals and the
checkout's on the candidate copies. Both arms run the same server, so every request
goes to both unchanged and the answers compare as raw bytes: the status as ``exit``,
the body as ``stdout``. Writes ``report.json`` into ``<cache>/report/`` and prints a
plain-text summary. Exit 0 when no case differs (outside a named exception), 1 when
any does.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import shutil
import sys
import time
import tomllib
from collections import Counter
from pathlib import Path

from conformance.differential import cache, cases, served

HERE = Path(__file__).resolve().parent
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
        globs = exc["case"] if isinstance(exc["case"], list) else [exc["case"]]
        if not any(fnmatch.fnmatchcase(diff["id"], glob) for glob in globs):
            continue
        prefixes = exc.get("paths", [""])
        if diff["fields"] == ["stdout"] and all(
            any(
                not pre
                # A glob without wildcards matches only itself, so this also
                # covers an exact pointer.
                or fnmatch.fnmatchcase(p, pre)
                or p.startswith(pre + "/")
                for pre in prefixes
            )
            for p in diff["paths"]
        ):
            return exc["name"]
    return None


def run(config: dict) -> int:
    started = time.monotonic()
    pins = cache.Pins.from_config(config)
    baseline_tree = cache.ensure_baseline(pins)
    dirs = cache.ensure_artifacts(pins)
    derived = cache.ensure_derived(pins, dirs, cache.derive_source())
    server = cache.ensure_server()
    setup_seconds = time.monotonic() - started
    report_dir = cache.cache_root() / "report"
    shutil.rmtree(report_dir, ignore_errors=True)
    cases.write_projects(dirs, config, report_dir / "projects")
    results = served.served_cases(
        cache.baseline_server(baseline_tree),
        baseline_tree / "reg_webapp" / "stewards",
        server,
        dirs,
        derived,
        report_dir / "servers",
        report_dir / "projects",
        config["seed"],
    )
    differences: list[dict] = []
    compared: Counter[str] = Counter()
    for case_id, base, cand in results:
        compared[case_id.split("/")[2]] += 1
        if (diff := compare(case_id, base, cand)) is not None:
            differences.append(diff)

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
        "release_tag": pins.tag,
        "seed": config["seed"],
        "cases": sum(compared.values()),
        "cases_by_command": dict(sorted(compared.items())),
        "differences": len(unexcepted),
        "excepted": len(differences) - len(unexcepted),
        "wall_seconds": round(wall, 1),
        "setup_seconds": round(setup_seconds, 1),
        "diffs": differences,
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    print(f"G1 {pins.tag} | baseline {pins.tag} vs checkout | seed {config['seed']}")
    print(
        f"{report['cases']} cases compared in {report['wall_seconds']} s "
        f"({report['setup_seconds']} s fetching and deriving): "
        f"{len(unexcepted)} differences, {report['excepted']} excepted"
    )
    if unexcepted:
        by_command = Counter(d["id"].split("/")[2] for d in unexcepted)
        for command, n in sorted(by_command.items()):
            print(f"  {command}: {n} of {compared[command]}")
        for diff in unexcepted[:MAX_PATHS]:
            where = ", ".join(diff["paths"][:3]) or ", ".join(diff["fields"])
            print(f"  {diff['id']}: {where}")
    print(f"report: {report_path}")
    if not compared:
        # A broken case discovery compares nothing and would otherwise pass.
        print(
            "G1 failed: 0 cases compared; case discovery found nothing", file=sys.stderr
        )
        return 1
    return 1 if unexcepted else 0


def main() -> int:
    config = tomllib.loads((HERE / "config.toml").read_text(encoding="utf-8"))
    with cache.locked(cache.cache_root()):
        return run(config)


if __name__ == "__main__":
    sys.exit(main())
