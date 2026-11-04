#!/usr/bin/env python3
"""release_base_reuse — may a release reuse an earlier evidence base build?

    uv run --no-project scripts/release_base_reuse.py <earlier evidence dir>

Run from the release checkout (clean, at the release commit). The release skill's step 8
builds the real-seed base once per release; a later commit in the same release reuses
that base when nothing the base build reads has changed (maintainer ruling 2026-11-04).
The base is a deterministic function of the `reg-build` binary, the curation tree
(including the inputs pin, `curation/inputs.toml`), the toolchain and the build flags,
so a reuse is safe exactly when all of those are unchanged:

- Builder closure: the workspace crates `reg-build` depends on (normal and build
  dependencies, not dev), read from `cargo metadata` at the release commit, so a new
  crate joins without editing this script. A new dependency edge changes a closure
  crate's `Cargo.toml`, which the diff sees. Every other member's `Cargo.toml` too:
  the `--workspace` build unifies features, so a reader crate's manifest can change
  what the builder links. Plus `curation/`, the root `Cargo.toml` and
  `Cargo.lock`, `.cargo/` and any `rust-toolchain*`.
- Toolchain: `rustc -vV` at the release checkout equals the recorded `rustc.txt`.
- Flags: the recorded `build-args.txt` has `--release` and `--catalog-version` equal to
  this release's `reg-meta` version and to the base's `meta.catalog_version`.
- Bytes: `catalog/reg_meta.db` still hashes to the recorded `base.sha256`.

A step's result carries over only when the evidence's `passed` file lists it against
this base digest (each step appends `<step> <base sha>` once it passes), and the pair
only when it still hashes to `holdings.sha256`, recorded when 8e passed, and pins this
base's `catalog_sha256`. It also says what the diff does touch:

- `stewards.toml` changed: rebuild the holdings and redo their checks (8d-8g).
- A reader-side path changed (a workspace crate outside the builder closure,
  `conformance/`, `uv.lock`, `pyproject.toml`): rerun the 8f conformance.

The scope check (8h) and the 8i `base_sha` check always run at the release commit.

The evidence directory must hold what step 8a records: `source-revision`, `rustc.txt`,
`build-args.txt`, `base.sha256` and `catalog/reg_meta.db`. A missing or inconsistent
record fails closed. Exit 0: reuse the base. Exit 1: rebuild it. Exit 2: the records do
not support a reuse; rebuild it.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tomllib
from pathlib import Path

ALWAYS_BUILDER = [
    "curation",
    "Cargo.toml",
    "Cargo.lock",
    ".cargo",
    ":(glob)rust-toolchain*",
]
READER_EXTRA = ["conformance", "uv.lock", "pyproject.toml"]
HOLDINGS = ["stewards.toml"]


class Refused(Exception):
    pass


def run(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def workspace_closure(meta: dict, root_name: str) -> set[str]:
    """Workspace package ids `root_name` reaches through normal and build deps."""
    members = set(meta["workspace_members"])
    by_name = {p["name"]: p["id"] for p in meta["packages"] if p["id"] in members}
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    seen, todo = set(), [by_name[root_name]]
    while todo:
        pid = todo.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            kinds = {k["kind"] for k in dep["dep_kinds"]}
            if dep["pkg"] in members and kinds - {"dev"}:
                todo.append(dep["pkg"])
    return seen


def changed(built: str, release: str, paths: list[str]) -> list[str]:
    return run("git", "diff", "--name-only", built, release, "--", *paths).split()


def record(evidence: Path, name: str) -> str:
    path = evidence / name
    if not path.is_file():
        raise Refused(f"{path} is missing: record it in step 8a")
    return path.read_text().strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 24):
            digest.update(chunk)
    return digest.hexdigest()


BUILD_VALUES = {
    "--inputs",
    "--assets",
    "--curation",
    "--out",
    "--catalog-version",
    "--report",
}


def build_options(args: list[str]) -> dict[str, str]:
    """Step 8a's `reg-build build` arguments, exactly: every value option once, plus
    `--release`. Anything else (a subset, a diagnostic flag) is not a release build."""
    if args[:1] != ["build"]:
        raise Refused("build-args.txt is not a `reg-build build` command")
    options: dict[str, str] = {}
    rest = args[1:]
    while rest:
        flag = rest.pop(0)
        if flag == "--release" and flag not in options:
            options[flag] = ""
        elif flag in BUILD_VALUES and flag not in options and rest:
            options[flag] = rest.pop(0)
        else:
            raise Refused(f"build-args.txt has an unexpected argument {flag!r}")
    if set(options) != BUILD_VALUES | {"--release"}:
        raise Refused("build-args.txt is not step 8a's full release build")
    return options


def check_records(evidence: Path, repo: Path) -> tuple[str, str]:
    """The built commit and base digest, once every record supports reusing it."""
    built = record(evidence, "source-revision")
    try:
        run("git", "cat-file", "-e", f"{built}^{{commit}}")
    except subprocess.CalledProcessError:
        raise Refused(
            f"built commit {built} is not in this clone: git fetch it"
        ) from None
    if record(evidence, "rustc.txt") != run("rustc", "-vV").strip():
        raise Refused("rustc -vV differs from the recorded rustc.txt")
    args = record(evidence, "build-args.txt").split("\n")
    version = tomllib.loads((repo / "crates/reg-meta/Cargo.toml").read_text())[
        "package"
    ]["version"]
    options = build_options(args)
    if options.get("--catalog-version") != version:
        raise Refused(f"the recorded build's --catalog-version is not {version}")
    if options["--curation"] != "curation":
        raise Refused("the recorded build did not read this checkout's curation/")
    # The catalog file itself is not resolved: only its directory names the evidence.
    out = Path(options["--out"])
    if out.parent.resolve() / out.name != evidence / "catalog/reg_meta.db":
        raise Refused("the recorded build did not write this evidence's catalog")
    db = evidence / "catalog/reg_meta.db"
    if not db.is_file():
        raise Refused(f"{db} is missing")
    with sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True) as conn:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'catalog_version'"
        ).fetchone()
    if row is None or row[0] != version:
        raise Refused(f"{db} meta.catalog_version is not {version}")
    recorded = record(evidence, "base.sha256").split()[0]
    if sha256(db) != recorded:
        raise Refused(f"{db} no longer matches base.sha256")
    return built, recorded


def passed_steps(evidence: Path, base_sha: str) -> set[str]:
    """Steps `passed` records against this base digest (`<step> <base sha>` lines)."""
    path = evidence / "passed"
    if not path.is_file():
        return set()
    lines = (line.split() for line in path.read_text().splitlines())
    return {f[0] for f in lines if len(f) == 2 and f[1] == base_sha}


def holdings_reason(
    evidence: Path, passed: set[str], steward_change: list[str], base_sha: str
) -> str:
    """Why the earlier pair cannot be reused, or "" when it can."""
    if steward_change:
        return "stewards.toml changed"
    pair = evidence / "catalog/reg_meta_swecov.db"
    if not pair.is_file() or "8e" not in passed:
        return "no pair that passed 8e against this base in the evidence"
    digest = evidence / "holdings.sha256"
    if not digest.is_file() or sha256(pair) != digest.read_text().split()[0]:
        return "the pair no longer matches holdings.sha256"
    with sqlite3.connect(f"{pair.as_uri()}?mode=ro", uri=True) as conn:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'catalog_sha256'"
        ).fetchone()
        pin = dict(
            conn.execute(
                "SELECT key, value FROM meta WHERE key IN "
                "('steward_repo', 'steward_commit', 'steward_config')"
            ).fetchall()
        )
    if row is None or row[0] != base_sha:
        return "the pair does not pin this base's catalog_sha256"
    registry = tomllib.loads(Path("stewards.toml").read_text())["swecov"]
    wanted = {
        "steward_repo": registry["repo"],
        "steward_commit": registry["commit"],
        "steward_config": registry["config"],
    }
    if pin != wanted:
        return "the pair's steward pin is not this release's stewards.toml"
    return ""


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    evidence = Path(argv[1]).resolve()
    repo = Path(run("git", "rev-parse", "--show-toplevel").strip())
    # Every pathspec below is repository-relative.
    os.chdir(repo)
    if run("git", "status", "--porcelain"):
        print("refused: the release checkout is dirty", file=sys.stderr)
        return 2
    release = run("git", "rev-parse", "HEAD").strip()
    try:
        built, base_sha = check_records(evidence, repo)
    except Refused as refused:
        print(f"refused: {refused}; rebuild the base", file=sys.stderr)
        return 2

    meta = json.loads(run("cargo", "metadata", "--format-version", "1", "--locked"))
    members = set(meta["workspace_members"])
    dirs = {
        p["id"]: str(Path(p["manifest_path"]).parent.relative_to(repo))
        for p in meta["packages"]
        if p["id"] in members
    }
    builder = workspace_closure(meta, "reg-build")
    reader = members - builder
    # `--workspace` builds unify features across every member, so any member's
    # manifest can change what the builder links (e.g. a reader crate enabling a
    # feature of a shared dependency) without touching Cargo.lock.
    manifests = sorted(f"{dirs[i]}/Cargo.toml" for i in reader)
    builder_paths = sorted(dirs[i] for i in builder) + manifests + ALWAYS_BUILDER
    reader_paths = sorted(dirs[i] for i in reader) + READER_EXTRA

    print(f"built   {built} ({evidence})")
    print(f"release {release}")
    print(f"builder closure: {' '.join(sorted(dirs[i] for i in builder))}")
    base = changed(built, release, builder_paths)
    if base:
        print("base: rebuild (builder input changed):", *base, sep="\n  ")
        return 1
    passed = passed_steps(evidence, base_sha)
    print("base: reuse")
    for step in ("8b", "8c"):
        print(
            f"{step}: carries over"
            if step in passed
            else f"{step}: run it (not passed in the evidence)"
        )
    holdings = holdings_reason(
        evidence, passed, changed(built, release, HOLDINGS), base_sha
    )
    if holdings:
        print(f"holdings: rebuild, redo 8d-8g ({holdings})")
    else:
        print("holdings: reuse; 8d and 8e carry over")
        print(
            "8g: carries over"
            if "8g" in passed
            else "8g: run it (not passed in the evidence)"
        )
    readers = changed(built, release, reader_paths)
    if readers:
        print("8f: rerun (reader-side change):", *readers, sep="\n  ")
    elif holdings or "8f" not in passed:
        print("8f: run it (holdings rebuilt, or not passed in the evidence)")
    else:
        print("8f: carries over")
    print("always: 8h scope check and the 8i base_sha check at the release commit")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
