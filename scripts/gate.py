#!/usr/bin/env python3
"""gate — the full verification gate and the regenerate command.

    uv run --no-project scripts/gate.py all            # before opening a PR
    uv run --no-project scripts/gate.py g0 --packages reg_meta_build
    uv run --no-project scripts/gate.py regen          # then commit the diff

Each named step runs its commands in order from the repository root and stops at the
first failure. `all` runs g0, rust, release, flows, frontend, runner and export_gate;
`lint` and `crates` are g0's static checks and Rust part, for CI's `lint` and `rust`
jobs; `regen` and `g1` run only when named. CI calls `lint`, `crates`, `rust`,
`release`, `frontend`, `runner` and `export_gate`; `flows` runs only locally. Stdlib only: `--no-project` keeps the frontend CI job free
of the workspace build.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "reg_webapp/frontend"
# The repo takes new releases immediately (`exclude-newer = false` in pyproject.toml), but
# project-less `uvx` reads only the host uv config, which may delay them; without this a
# freshly bumped tool pin fails locally until the host cutoff passes.
os.environ["UV_EXCLUDE_NEWER"] = "false"
CONFORMANCE = "uv run python -m pytest conformance -q -n auto"
# A raised write limit: the suite and release admission replay many projects from one
# address (conformance/test_mcp.py's burst case drops it to pin the default).
SERVER_CMD = (
    "--server-cmd='target/debug/reg-meta serve --db {db} --catalog {catalog}"
    " --stewards reg_webapp/stewards --port {port} --write-limit 100000'"
)
MCP_CMD = "--mcp-cmd='target/debug/reg-meta mcp --db {db} --catalog {catalog}'"
ALL = ("g0", "rust", "release", "flows", "frontend", "runner", "export_gate")
# The R formatter and linter `runner` and `export_gate` check their packages with; CI's
# `runner` and `export-gate` jobs install these releases (bump all three).
AIR = "0.12.0"
JARL = "0.7.1"
# The SPA's reg-core .wasm, gzipped (crates/DESIGN.md, "Crates and run modes"). Over
# it, the frontend step fails: raising the budget is a maintainer call. It includes the
# search fold (about 145 KB gz measured with it).
WASM_GZ_BUDGET = 200_000


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


def lint() -> None:
    # The home of the ty pin; the panache pin is repeated in scripts/parse_lisa_docs.py
    # (bump both). Rust formatting is in `crates`.
    run("uv run ruff check")
    run("uv run ruff format --check")
    run("uvx --from panache-cli==3.17.0 panache format --check .")
    run("uvx --from panache-cli==3.17.0 panache lint .")
    run("uvx --from ty==0.0.90 ty check")


def g0(packages: list[str] | None = None) -> None:
    lint()
    crates()
    # No packages runs every root `testpaths` entry. Conformance needs the Rust
    # server, so it runs in `rust`, not here.
    run(["uv", "run", "python", "-m", "pytest", *(packages or []), "-n", "auto", "-q"])


def rust() -> None:
    run("cargo build --workspace --locked")
    run(f"{CONFORMANCE} {SERVER_CMD} {MCP_CMD}")


def release() -> None:
    # The steward artifact only: on a selected catalog artifact `validate_built_db`
    # applies the real-corpus floors (register, edge and FTS counts), which the
    # synthetic catalog cannot meet. Every other artifact check already runs on both
    # synthetic kinds in `rust`. `-m release` runs only the selected-artifact checks:
    # the rest of the suite does not read `--artifact-dir` and already ran in `rust`.
    artifact = reader_artifact("steward")
    run("cargo build --workspace --locked")
    run(
        f"{CONFORMANCE} -m release --run-release --artifact-dir={artifact} {SERVER_CMD}"
    )


def flows() -> None:
    out = Path(tempfile.mkdtemp(prefix="gate-flows-"))
    dev = ROOT / "reg_webapp/dev/dev.sh"
    run(["bash", dev, "--fixture-db", "flows", out], stdin=subprocess.DEVNULL)
    # Kept on failure: the screenshots are the evidence.
    shutil.rmtree(out)


def frontend() -> None:
    # On demand, so a fresh worktree needs no setup; a no-op when in sync.
    run(["bun", "install", "--frozen-lockfile"], cwd=FRONTEND)
    run(["bun", "run", "gen:wasm"], cwd=FRONTEND)
    wasm = FRONTEND / "src/lib/reg-core-wasm/reg_core_wasm_bg.wasm"
    size = len(gzip.compress(wasm.read_bytes(), compresslevel=9))
    print(f"gate: {wasm.name} {size} bytes gzipped (budget {WASM_GZ_BUDGET})")
    if size > WASM_GZ_BUDGET:
        sys.exit(f"gate: {wasm.name} is over its gzipped budget; see WASM_GZ_BUDGET")
    for script in ("check", "lint", "test", "build", "gen:types"):
        run(["bun", "run", script], cwd=FRONTEND)
    # A diff means the SPA's types, or the plugin's project_data.json schema, drifted
    # from the committed API contract. `ls-files` fails on an untracked one, which
    # `git diff` ignores.
    generated = (
        "src/lib/api-types-rust.ts"
        " ../../plugins/microdata-tools-se/skills/register-project/project_data.schema.json"
    )
    run(f"git ls-files --error-unmatch -- {generated}", cwd=FRONTEND)
    run(f"git diff --exit-code -- {generated}", cwd=FRONTEND)
    # The built app's bootstrap: mounts, serves the .wasm, alerts without it.
    run(["bun", "scripts/smoke_build.ts"], cwd=FRONTEND)


def r_package(package: str) -> None:
    """air and jarl at their pins over an R package, then its testthat suite."""
    for tool, pin in (("air", AIR), ("jarl", JARL)):
        version = run([tool, "--version"], stdout=subprocess.PIPE, text=True).stdout
        if version.split()[-1] != pin:
            sys.exit(f"gate: {tool} {pin} is pinned; found {version.strip()}")
    run(f"air format --check {package}")
    run(f"jarl check {package}")
    run(
        [
            "Rscript",
            "-e",
            f'testthat::test_dir("{package}/tests/testthat", stop_on_failure = TRUE)',
        ]
    )


def runner() -> None:
    # The MONA extract runner. The SQL Server cases run only where
    # SWECOV_TEST_CONNECTION is set (CI's `runner` job); elsewhere they skip.
    r_package("reg_mona_runner")


def export_gate() -> None:
    # The export gate's MONA check (reg_export_gate/): its case corpus, which needs
    # R with jsonlite, arrow and testthat.
    r_package("reg_export_gate")


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
    run("bun run gen:types", cwd=FRONTEND)


def g1() -> None:
    run("uv run python -m conformance.differential")


STEPS = {
    f.__name__: f
    for f in (
        lint,
        g0,
        crates,
        rust,
        release,
        flows,
        frontend,
        runner,
        export_gate,
        regen,
        g1,
    )
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("steps", nargs="+", choices=[*STEPS, "all"])
    parser.add_argument(
        "--packages",
        nargs="+",
        help="g0's pytest paths (default: every testpaths entry)",
    )
    args = parser.parse_args()
    names = [n for step in args.steps for n in (ALL if step == "all" else [step])]
    steps = STEPS | {"g0": lambda: g0(args.packages)}
    times: list[str] = []
    failed = None
    for name in names:
        print(f"== gate {name}", flush=True)
        started = time.monotonic()
        try:
            steps[name]()
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
