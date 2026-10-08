#!/usr/bin/env python3
"""gate — the full verification gate, the regenerate command and the heavy-job lock.

    uv run --no-project scripts/gate.py all            # before opening a PR
    uv run --no-project scripts/gate.py g0 --packages reg_meta
    uv run --no-project scripts/gate.py regen          # then commit the diff

Each named step runs its commands in order from the repository root and stops at the
first failure. `all` runs g0, rust, release, flows and frontend; `crates` is g0's Rust
part, for CI's `rust` job; `regen` and `g1` run only when named. The CI jobs in
`.github/workflows/ci.yml` call these steps, so the commands live here once.

Steps that build or run the Rust workspace hold one machine-wide advisory lock
(`flock` on `$XDG_CACHE_HOME/registry-research-toolkit-heavy.lock`), so parallel
sessions run one heavy job at a time instead of overloading the machine. Stdlib only:
`--no-project` keeps the frontend CI job free of the workspace build.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from functools import partial
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "reg_webapp/frontend"
CONFORMANCE = "uv run python -m pytest conformance -q -n auto"
SERVER_CMD = (
    "--server-cmd='target/debug/reg-meta serve --db {db} --catalog {catalog}"
    " --stewards reg_webapp/stewards --port {port}'"
)
MCP_CMD = "--mcp-cmd='target/debug/reg-meta mcp --db {db} --catalog {catalog}'"
ALL = ("g0", "rust", "release", "flows", "frontend")
HEAVY = {"g0", "crates", "rust", "release", "flows", "regen", "g1"}


def run(command, cwd=ROOT, env=None, **kwargs):
    """Run `command` (a shell-quoted string or an argv list); raise on failure."""
    argv = shlex.split(command) if isinstance(command, str) else command
    print("$", shlex.join(map(str, argv)), flush=True)
    return subprocess.run(
        argv, cwd=cwd, env=env and os.environ | env, check=True, **kwargs
    )


def reader_artifact(kind: str) -> Path:
    """The synthetic `reader` artifact directory of `kind`, from the fixture cache."""
    out = run(
        f"uv run python conformance/fixture_cache.py reader {kind}",
        stdout=subprocess.PIPE,
        text=True,
    )
    return Path(out.stdout.strip()).parent


def crates() -> None:
    run("cargo fmt --all --check")
    run("cargo clippy --workspace --all-targets --locked -- -D warnings")
    run("cargo test --workspace --locked")
    # Unused crate dependencies; a false positive is ignored in that crate's
    # Cargo.toml (`[package.metadata.cargo-machete] ignored`) with a reason.
    run("cargo machete")


def g0(packages: list[str] | None) -> None:
    run("uv run ruff check")
    run("uv run ruff format --check")
    run("uvx --from panache-cli==3.9.0 panache format --check .")
    run("uvx --from panache-cli==3.9.0 panache lint .")
    run("uvx --from ty==0.0.79 ty check")
    crates()
    # No paths runs every root `testpaths` entry, conformance included.
    paths = ["conformance", *packages] if packages else []
    run(["uv", "run", "python", "-m", "pytest", *paths, "-n", "auto", "-q"])


def rust() -> None:
    run("cargo build --workspace --locked")
    run(f"{CONFORMANCE} {SERVER_CMD} {MCP_CMD}")


def release() -> None:
    # The steward artifact only: on a selected catalog artifact `validate_built_db`
    # applies the real-corpus floors (register, edge and FTS counts), which the
    # synthetic catalog cannot meet. Every other artifact check already runs on both
    # synthetic kinds in `rust`.
    artifact = reader_artifact("steward")
    run("cargo build --workspace --locked")
    run(f"{CONFORMANCE} --run-release --artifact-dir={artifact} {SERVER_CMD}")


def flows() -> None:
    out = Path(tempfile.mkdtemp(prefix="gate-flows-"))
    dev = ROOT / "reg_webapp/.claude/skills/run-reg-webapp/dev.sh"
    run(["bash", dev, "--fixture-db", "flows", out], stdin=subprocess.DEVNULL)
    # Kept on failure: the screenshots are the evidence.
    shutil.rmtree(out)


def frontend() -> None:
    for script in ("check", "lint", "test", "build", "gen:types"):
        run(["bun", "run", script], cwd=FRONTEND)
    # A diff means the SPA's types drifted from the committed API contracts.
    run(
        "git diff --exit-code -- src/lib/api-types.ts src/lib/api-types-rust.ts",
        cwd=FRONTEND,
    )


def regen() -> None:
    run("cargo build --workspace --locked")
    run("cargo test -p reg-meta --test openapi_snapshot", env={"REG_META_BLESS": "1"})
    # The MCP `tools/list` golden, as conformance/test_mcp.py's stdio session lists it.
    artifact = reader_artifact("steward")
    messages = [
        {
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "gate", "version": "0"},
            },
        },
        {"method": "notifications/initialized"},
        {"id": 1, "method": "tools/list"},
    ]
    session = run(
        ["target/debug/reg-meta", "mcp", "--db", artifact, "--catalog", "swecov"],
        input="".join(json.dumps({"jsonrpc": "2.0"} | m) + "\n" for m in messages),
        stdout=subprocess.PIPE,
        text=True,
    )
    (tools,) = (
        reply["result"]["tools"]
        for reply in map(json.loads, session.stdout.splitlines())
        if reply.get("id") == 1
    )
    golden = ROOT / "conformance/cases/mcp/tools-list.json"
    golden.write_text(json.dumps(tools, indent=2) + "\n", encoding="utf-8")
    run("uv run python reg_webapp/backend/scripts/gen_openapi.py")
    run("bun run gen:types", cwd=FRONTEND)


def g1() -> None:
    run("uv run python -m conformance.differential")


STEPS = {f.__name__: f for f in (g0, crates, rust, release, flows, frontend, regen, g1)}


@contextmanager
def heavy_lock():
    cache = os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    path = Path(cache) / "registry-research-toolkit-heavy.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"gate: waiting for the heavy-job lock {path}", flush=True)
            fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("steps", nargs="+", choices=[*STEPS, "all"])
    parser.add_argument(
        "--packages",
        nargs="+",
        help="g0's pytest paths besides conformance (default: every testpaths entry)",
    )
    args = parser.parse_args()
    names = [n for step in args.steps for n in (ALL if step == "all" else [step])]
    times: list[str] = []
    failed = None
    for name in names:
        print(f"== gate {name}", flush=True)
        step = partial(g0, args.packages) if name == "g0" else STEPS[name]
        started = time.monotonic()
        try:
            if name in HEAVY:
                with heavy_lock():
                    step()
            else:
                step()
        except subprocess.CalledProcessError as exc:
            failed = (
                f"{name}: exit {exc.returncode} from {shlex.join(map(str, exc.cmd))}"
            )
        times.append(f"{name} {time.monotonic() - started:.0f} s")
        if failed:
            break
    print("== gate " + ", ".join(times))
    print(f"== gate FAILED {failed}" if failed else "== gate passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
