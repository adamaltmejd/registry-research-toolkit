#!/usr/bin/env python3
"""real_seed_cache — reuse real-seed `prepare-sources` and `build-db` outputs by input key.

    uv run --no-project scripts/real_seed_cache.py prepare --input-bundle DIR \\
        --input-commit SHA --input-manifest-sha256 SHA256 --output-dir NEW_DIR
    uv run --no-project scripts/real_seed_cache.py build --prepared DIR \\
        --input-commit SHA --input-manifest-sha256 SHA256 [--diagnostic] [--registers SPEC]

Each command looks its key up and prints one JSON object on stdout. A hit returns the
stored output without running anything. A miss takes the machine-wide real-seed lock
(`gate.real_seed_lock`), looks again (a queued session may have stored it meanwhile),
runs the real `reg-meta-build` command, and stores the output before releasing the
lock, only if the run completed:

- a prepare that exited 0 with the manifest digest it reported;
- a diagnostic build with exit 10 and `status = diagnostic_complete`;
- a strict build with exit 0 and `status = complete`, plus `publication_ready` for a
  full build (a `--registers` subset is never publication-ready);
- and in every case only if the code the key hashes is unchanged after the run, and a
  build's `curation_tree_sha256` equals the key's.

A failed run is never stored; its directory is kept for diagnosis and reaped after
6 hours. `--key` prints a key and its fields. `build --verify` rebuilds uncached and
compares the database and decompressed event-ledger bytes with the stored entry (exit
1 on a difference): the check that the key covers every input. For a prepare, run
`reg-meta-build prepare-sources` into a new directory and compare its
`prepared_manifest_sha256` with the stored one.

Prepare key: the raw bundle commit, `catalog-bundle.json` digest and the bundle's path
in its repository (the manifest records it); the content of every file in the code
boundary (`prepare_code_files`: a static import walk, `uv.lock`, and the native
extension sources because a walked module imports `reg_core_py`); the Python and
SQLite versions. A prepare entry is an index record (prepared path and top-level
manifest digest), not a copy of the 14 GB tree the maintainer commits to a local
acceptance repository. A lookup runs the builder's own warm-build check
(`open_prepared_catalog_sources`) against that repository's current HEAD; a hit
returns HEAD as the acceptance commit. A tree not yet committed is reported as
awaiting acceptance, without a rerun. Any other failed check drops the record and
misses.

Build key: the content of `reg_meta_build/src`, `reg_meta/src`, `reg_schema/src`,
`crates/reg-core`, `crates/reg-core-py`, `Cargo.toml`, `Cargo.lock` and `uv.lock`; the
builder's own `curation_tree_sha256` of the selected curation tree; the prepared
commit and manifest digest; the mode and `--registers`; the Python and SQLite
versions; and for a publishable build the checkout's HEAD, which the database records.
Every build lookup, hit or miss, first runs the admission checks the builder runs
before it resolves anything (the prepared tree at the pinned commit and, for a
publishable build, a clean builder checkout at the keyed commit); a failure exits 10
with the builder's error code instead of returning the entry. A build entry holds the
report directory and the database. The `KEEP` most recently
used entries stay, and so does any entry used within 6 hours.

The cache lives in `$REG_REAL_SEED_CACHE`, else `$XDG_CACHE_HOME/reg-meta-real-seed`,
else `~/.cache/reg-meta-real-seed`. `$REG_REAL_SEED_BUILDER` replaces the
`uv run reg-meta-build` command line and `$REG_REAL_SEED_PYTHON` the `uv run python`
that runs the project-environment probe (the tool's contract tests stub both).
Stdlib only, like `gate.py`.
"""

from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from gate import real_seed_lock
from keyed_cache import (
    BUILDER_SOURCES,
    NATIVE_SOURCES,
    STAGING_PREFIX,
    STAGING_RETENTION_SECONDS,
    cache_home,
    evict,
    file_sha256,
    owned_root,
    tree_digest,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = Path(__file__).resolve().parents[1]
MARKER = ".real-seed-cache"
# Build entries kept: a main-tip baseline and one candidate (~1.4 GB each).
# simplify: evicts by entry count, not bytes; switch to a byte budget if entries grow
# well past the catalog size. Prepare records (a few KB) are dropped only when they no
# longer hold; prune them by age if the directory passes a few hundred.
KEEP = 2
EXIT_CONFIG = 10  # reg-meta-build's exit for a diagnostic completion
EXIT_AWAITING = 3
PACKAGES = {
    "reg_meta_build": "reg_meta_build/src",
    "reg_meta": "reg_meta/src",
    "reg_schema": "reg_schema/src",
}
# The prepare key's code boundary, as roots of a static import walk
# (`prepare_code_files`): the preparation entry point, and the modules the CLI's
# `prepare-sources` handler and its output-confinement check import from.
#
# Revisit when `prepare-sources` gains an option or a new input, when the CLI handler
# starts calling a module outside this walk, or when a module in the walk starts
# reading a repository file outside its package (today only `_curation`,
# `curation_tree` and `fqid_slugs` can read the curation tree, and the prepare path
# calls none of those readers).
#
# Stage 4 of RUST_RUNTIME_SPEC.md moves the retained `reg_meta` modules into
# `reg_meta_build` (4.4) and deletes `reg_meta/` and `reg_schema/` (4.9a): update
# these roots and `PACKAGES` then; a missing root stops the tool rather than
# shrinking the key.
PREPARE_ROOTS = (
    "reg_meta_build.prepared_catalog",
    "reg_meta_build.input_snapshot",
    "reg_meta_build.db",
    "reg_meta_build._curation",
    "reg_meta.cli_common",
    "reg_meta.db",
    "reg_meta.errors",
)
# Keyed by content but not walked: the CLI imports every subcommand's module at load,
# so walking it would key prepare on the whole builder. Their import-time code runs
# but does not reach the prepared output.
PREPARE_ENTRY_FILES = ("reg_meta_build/src/reg_meta_build/cli.py",)
# What this stdlib-only script needs from the project environment: the interpreter
# and SQLite the builder runs on, the builder's own curation digest (the value a
# build records as `curation_tree_sha256`), the builder's warm-build check of an
# accepted prepared tree at its repository's HEAD, and the admission checks a build
# runs before it resolves anything (the same check of the prepared pins and, for a
# publishable build, the clean builder source).
PROBE = """
import json, sqlite3, subprocess, sys
from pathlib import Path
request = json.loads(sys.argv[1])
facts = {"python": sys.version, "sqlite": sqlite3.sqlite_version}
if request.get("curation"):
    from reg_meta_build.curation_compile import tree_sha256
    facts["curation_tree_sha256"] = tree_sha256(Path(request["curation"]))
if request.get("prepared"):
    from reg_meta_build.prepared_catalog import open_prepared_catalog_sources
    prepared = Path(request["prepared"])
    try:
        head = subprocess.run(
            ["git", "-C", str(prepared), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        open_prepared_catalog_sources(
            prepared, input_commit=head, expected_sha256=request["manifest_sha256"]
        )
        facts["accepted_commit"] = head
    except Exception as exc:
        facts["accepted_error"] = str(exc) or type(exc).__name__
if request.get("admit"):
    admit = request["admit"]
    try:
        from reg_meta_build.prepared_catalog import open_prepared_catalog_sources
        open_prepared_catalog_sources(
            Path(admit["prepared"]),
            input_commit=admit["input_commit"],
            expected_sha256=admit["manifest_sha256"],
        )
        if admit["builder_commit"] is not None:
            from reg_meta_build.artifact_identity import builder_commit
            if builder_commit() != admit["builder_commit"]:
                raise ValueError("Builder revision changed during compilation")
    except Exception as exc:
        facts["admission_error"] = {
            "code": getattr(exc, "code", None) or "pipeline_build_failed",
            "message": str(exc),
        }
print(json.dumps(facts))
"""


def builder() -> list[str]:
    return shlex.split(os.environ.get("REG_REAL_SEED_BUILDER", "uv run reg-meta-build"))


def probe(**request) -> dict:
    python = shlex.split(
        os.environ.get("REG_REAL_SEED_PYTHON", "uv run --quiet python")
    )
    argv = [*python, "-c", PROBE, json.dumps(request)]
    return json.loads(
        subprocess.run(
            argv, cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout
    )


def git(directory: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(directory), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def digest(fields: dict) -> str:
    return hashlib.sha256(
        json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def code_digests(paths) -> dict[str, str]:
    # `tree_digest` hashes a missing tree as empty; a key must refuse it instead, so a
    # moved or deleted source (the stage-4 moves) names the path to update.
    if missing := [path for path in paths if not (ROOT / path).exists()]:
        sys.exit(f"real-seed-cache: keyed source missing, update the key: {missing}")
    return {path: tree_digest(ROOT / path) for path in paths}


def emit(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def cache_dir(kind: str) -> Path:
    home = owned_root(cache_home("reg-meta-real-seed", "REG_REAL_SEED_CACHE"), MARKER)
    (home / kind).mkdir(exist_ok=True)
    return home / kind


def write_json(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


# -- the prepare code boundary ---------------------------------------------------


def _module_file(module: str) -> Path | None:
    top = module.split(".")[0]
    if top not in PACKAGES:
        return None
    base = ROOT / PACKAGES[top] / Path(*module.split("."))
    if (base / "__init__.py").is_file():
        return base / "__init__.py"
    return base.with_suffix(".py") if base.with_suffix(".py").is_file() else None


def _imports(tree: ast.Module) -> Iterator[ast.Import | ast.ImportFrom]:
    """Every import statement, function-local ones included; the bodies of
    `if TYPE_CHECKING:` blocks are skipped (they never run)."""
    stack: list[ast.AST] = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.If) and (
            getattr(node.test, "id", None) == "TYPE_CHECKING"
            or getattr(node.test, "attr", None) == "TYPE_CHECKING"
        ):
            stack.extend(node.orelse)
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            yield node
        stack.extend(ast.iter_child_nodes(node))


def _imports_dynamically(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.Call)
        and (
            getattr(node.func, "id", None) in {"import_module", "__import__"}
            or getattr(node.func, "attr", None) == "import_module"
        )
        for node in ast.walk(tree)
    )


def prepare_code_files() -> list[str]:
    """The repo-relative files `prepare-sources` runs, generously.

    Walks every import from `PREPARE_ROOTS` through the workspace packages, including
    function-local imports, whether or not the prepare path reaches them. A module
    that imports by name at run time (`import_module`) pulls in its whole package.
    Data files beside any walked module count, as do the native extension sources
    when a walked module imports `reg_core_py`, and `uv.lock` always.
    """
    if missing := [m for m in PREPARE_ROOTS if _module_file(m) is None]:
        sys.exit(f"real-seed-cache: prepare walk root missing, update it: {missing}")
    seen: dict[str, Path] = {}
    native = False
    pending = list(PREPARE_ROOTS)
    while pending:
        module = pending.pop()
        if module in seen or (path := _module_file(module)) is None:
            continue
        seen[module] = path
        parts = module.split(".")
        pending += [".".join(parts[:i]) for i in range(1, len(parts))]
        tree = ast.parse(path.read_text(encoding="utf-8"))
        package = module if path.name == "__init__.py" else module.rpartition(".")[0]
        if _imports_dynamically(tree):
            root = ROOT / PACKAGES[parts[0]]
            pending += [
                ".".join(p.relative_to(root).with_suffix("").parts).removesuffix(
                    ".__init__"
                )
                for p in path.parent.rglob("*.py")
            ]
        for node in _imports(tree):
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            else:
                base = package.split(".")[: len(package.split(".")) - node.level + 1]
                target = (
                    ".".join([*base, *([node.module] if node.module else [])])
                    if node.level
                    else node.module or ""
                )
                targets = [target, *(f"{target}.{a.name}" for a in node.names)]
            native |= any(t.split(".")[0] == "reg_core_py" for t in targets)
            pending += targets
    files = {path.relative_to(ROOT).as_posix() for path in seen.values()}
    for directory in {path.parent for path in seen.values()}:
        files.update(
            p.relative_to(ROOT).as_posix()
            for p in directory.iterdir()
            if p.is_file() and p.suffix not in {".py", ".pyc"}
        )
    files.update(PREPARE_ENTRY_FILES)
    files.add("uv.lock")
    if native:
        files.update(NATIVE_SOURCES)
    return sorted(files)


# -- prepare -----------------------------------------------------------------------


def prepare_fields(args: argparse.Namespace) -> dict:
    return {
        "step": "prepare",
        "bundle_commit": args.input_commit,
        "bundle_manifest_sha256": args.input_manifest_sha256,
        "bundle_path": git(Path(args.input_bundle), "rev-parse", "--show-prefix"),
        "code": code_digests(prepare_code_files()),
        "runtime": probe(),
    }


def lookup_prepare(home: Path, key: str) -> dict | None:
    """The stored preparation as a hit result, or None on a miss. Exits when the
    stored tree is intact but not yet committed (a rerun would only repeat it)."""
    path = home / f"{key}.json"
    if not path.is_file():
        return None
    record = json.loads(path.read_text())
    prepared = Path(record["prepared_path"])
    facts = probe(prepared=str(prepared), manifest_sha256=record["manifest_sha256"])
    if commit := facts.get("accepted_commit"):
        return {
            "hit": True,
            "key": key,
            "prepared_path": str(prepared),
            "prepared_manifest_sha256": record["manifest_sha256"],
            "prepared_commit": commit,
        }
    manifest = prepared / "manifest.json"
    tracked = subprocess.run(
        ["git", "-C", str(prepared), "ls-files", "--error-unmatch", "manifest.json"],
        capture_output=True,
        check=False,
    )
    if (
        tracked.returncode
        and manifest.is_file()
        and file_sha256(manifest) == record["manifest_sha256"]
    ):
        emit(
            {
                "hit": False,
                "awaiting_acceptance": True,
                "key": key,
                "prepared_path": str(prepared),
                "prepared_manifest_sha256": record["manifest_sha256"],
            }
        )
        sys.exit(EXIT_AWAITING)
    sys.stderr.write(
        f"real-seed-cache: dropping {path}: {facts.get('accepted_error')}\n"
    )
    path.unlink()
    return None


def run_prepare(args: argparse.Namespace, output: Path) -> dict | None:
    """Run prepare-sources into `output`; its CLI JSON, or None if it failed."""
    proc = subprocess.run(
        [
            *builder(),
            "prepare-sources",
            "--input-bundle",
            args.input_bundle,
            "--input-commit",
            args.input_commit,
            "--input-manifest-sha256",
            args.input_manifest_sha256,
            "--output-dir",
            str(output),
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode:
        sys.stderr.write(f"real-seed-cache: prepare-sources exited {proc.returncode}\n")
        sys.stderr.write(proc.stdout)
        return None
    result = json.loads(proc.stdout)
    if result.get("status") != "prepared" or file_sha256(
        output / "manifest.json"
    ) != result.get("prepared_manifest_sha256"):
        sys.stderr.write(f"real-seed-cache: incomplete preparation: {proc.stdout}\n")
        return None
    return result


def cmd_prepare(args: argparse.Namespace) -> int:
    fields = prepare_fields(args)
    key = digest(fields)
    if args.key:
        emit({"key": key, "fields": fields})
        return 0
    home = cache_dir("prepare")
    if hit := lookup_prepare(home, key):
        emit(hit)
        return 0
    if args.output_dir is None:
        sys.exit("real-seed-cache: a miss needs a new --output-dir")
    output = Path(args.output_dir)
    with real_seed_lock():
        if hit := lookup_prepare(home, key):
            emit(hit)
            return 0
        result = run_prepare(args, output)
        if result is None:
            reason = "prepare-sources failed"
        elif code_digests(fields["code"]) != fields["code"]:
            reason = "the prepare code changed during the run"
        else:
            reason = None
        if reason is not None:
            sys.stderr.write(f"real-seed-cache: not stored ({reason})\n")
            emit({"hit": False, "stored": False, "reason": reason})
            return 1
        write_json(
            home / f"{key}.json",
            {
                "key": key,
                "fields": fields,
                "prepared_path": str(output),
                "manifest_sha256": result["prepared_manifest_sha256"],
            },
        )
    emit(
        {
            "hit": False,
            "stored": True,
            "key": key,
            "prepared_path": str(output),
            "prepared_manifest_sha256": result["prepared_manifest_sha256"],
        }
    )
    return 0


# -- build -------------------------------------------------------------------------


def build_code(args: argparse.Namespace) -> dict:
    """The builder code a build runs. A publishable build also records the
    checkout's commit in the database, so that commit is keyed too."""
    return {
        # `reg_schema/src` goes with stage 4.9a; drop it here then.
        "code": code_digests((*BUILDER_SOURCES, PACKAGES["reg_schema"])),
        "builder_commit": (
            git(ROOT, "rev-parse", "HEAD")
            if not args.diagnostic and not args.registers
            else None
        ),
    }


def build_fields(args: argparse.Namespace) -> dict:
    code = build_code(args)
    facts = probe(
        curation=args.curation_dir or str(ROOT / "reg_meta_build/curation"),
        # Run on every lookup, hit or miss: a hit must refuse exactly what the build
        # would refuse before it starts.
        admit=None
        if args.key
        else {
            "prepared": args.prepared,
            "input_commit": args.input_commit,
            "manifest_sha256": args.input_manifest_sha256,
            "builder_commit": code["builder_commit"],
        },
    )
    if error := facts.pop("admission_error", None):
        emit({"hit": False, "error": error})
        sys.exit(EXIT_CONFIG)
    return {
        "step": "build",
        "mode": "diagnostic" if args.diagnostic else "strict",
        "registers": args.registers.split(",") if args.registers else [],
        "prepared_commit": args.input_commit,
        "prepared_manifest_sha256": args.input_manifest_sha256,
        **code,
        "curation_tree_sha256": facts.pop("curation_tree_sha256"),
        "runtime": facts,
    }


def database_name(args: argparse.Namespace) -> str:
    return "diagnostic.db" if args.diagnostic else "catalog/reg_meta.db"


def lookup_build(home: Path, key: str) -> dict | None:
    entry = home / key
    if not (entry / "entry.json").is_file():
        return None
    record = json.loads((entry / "entry.json").read_text())
    database = entry / record["database"]
    if (
        not database.is_file()
        or [database.stat().st_size, database.stat().st_mtime_ns]
        != [record["database_size"], record["database_mtime_ns"]]
        or not (entry / "report/summary.json").is_file()
    ):
        sys.stderr.write(f"real-seed-cache: dropping a changed entry: {entry}\n")
        shutil.rmtree(entry, ignore_errors=True)
        return None
    os.utime(entry)
    return record


def build_result(home: Path, record: dict, *, hit: bool) -> dict:
    entry = home / record["key"]
    return {
        "hit": hit,
        "key": record["key"],
        "entry": str(entry),
        "database": str(entry / record["database"]),
        "report": str(entry / "report"),
        "status": record["status"],
        "publication_ready": record["publication_ready"],
    }


def run_build(args: argparse.Namespace, home: Path) -> tuple[Path, int]:
    """Run build-db in a new staging directory under `home`; it and the exit code."""
    run_dir = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=home))
    argv = [*builder()]
    if not args.diagnostic:
        (run_dir / "catalog").mkdir()
        argv += ["--db", str(run_dir / "catalog")]
    argv += [
        "build-db",
        "--prepared",
        args.prepared,
        "--input-commit",
        args.input_commit,
        "--input-manifest-sha256",
        args.input_manifest_sha256,
        "--report-dir",
        str(run_dir / "report"),
        "--timing",
    ]
    if args.diagnostic:
        argv += ["--diagnostic", "--diagnostic-db-path", str(run_dir / "diagnostic.db")]
    if args.registers:
        argv += ["--registers", args.registers]
    if args.curation_dir:
        argv += ["--curation-dir", args.curation_dir]
    sys.stderr.write(f"real-seed-cache: running in {run_dir}\n")
    with (run_dir / "result.json").open("w") as out:
        code = subprocess.run(argv, cwd=ROOT, stdout=out, check=False).returncode
    return run_dir, code


def incomplete(args: argparse.Namespace, run_dir: Path, code: int, fields: dict):
    """Why the run in `run_dir` must not be stored, or None if it completed."""
    summary_path = run_dir / "report/summary.json"
    if not summary_path.is_file():
        return f"exit {code} without a report summary"
    summary = json.loads(summary_path.read_text())
    status = summary.get("status")
    if args.diagnostic:
        if code != EXIT_CONFIG or status != "diagnostic_complete":
            return f"diagnostic exit {code}, status {status}"
    elif (
        code
        or status != "complete"
        or not (args.registers or summary.get("publication_ready") is True)
    ):
        return (
            f"strict exit {code}, status {status}, "
            f"publication_ready {summary.get('publication_ready')}"
        )
    if summary.get("curation_tree_sha256") != fields["curation_tree_sha256"]:
        return "the build read a different curation tree than the key"
    if not (run_dir / database_name(args)).is_file():
        return "no database"
    if build_code(args) != {name: fields[name] for name in ("code", "builder_commit")}:
        return "the builder code changed during the run"
    return None


def events_sha256(report: Path) -> str:
    with gzip.open(report / "events.jsonl.gz", "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def cmd_build(args: argparse.Namespace) -> int:
    fields = build_fields(args)
    key = digest(fields)
    if args.key:
        emit({"key": key, "fields": fields})
        return 0
    home = cache_dir("build")
    if args.verify:
        return verify_build(args, home, key, fields)
    if record := lookup_build(home, key):
        emit(build_result(home, record, hit=True))
        return 0
    with real_seed_lock():
        if record := lookup_build(home, key):
            emit(build_result(home, record, hit=True))
            return 0
        run_dir, code = run_build(args, home)
        if reason := incomplete(args, run_dir, code, fields):
            sys.stderr.write(f"real-seed-cache: not stored ({reason})\n")
            emit(
                {
                    "hit": False,
                    "stored": False,
                    "reason": reason,
                    "run_dir": str(run_dir),
                }
            )
            return code or 1
        summary = json.loads((run_dir / "report/summary.json").read_text())
        database = run_dir / database_name(args)
        database.chmod(0o444)
        stat = database.stat()
        record = {
            "key": key,
            "fields": fields,
            "database": database_name(args),
            "database_size": stat.st_size,
            "database_mtime_ns": stat.st_mtime_ns,
            "status": summary["status"],
            "publication_ready": summary.get("publication_ready"),
        }
        write_json(run_dir / "entry.json", record)
        entry = home / key
        run_dir.rename(entry)
        evict(home, entry, KEEP, "real-seed-cache", min_idle=STAGING_RETENTION_SECONDS)
    emit(build_result(home, record, hit=False))
    return 0


def verify_build(args: argparse.Namespace, home: Path, key: str, fields: dict) -> int:
    """Rebuild uncached and compare the database and decompressed ledger bytes."""
    record = lookup_build(home, key)
    if record is None:
        sys.exit("real-seed-cache: nothing stored under this key to verify")
    entry = home / key
    with real_seed_lock():
        run_dir, code = run_build(args, home)
    if reason := incomplete(args, run_dir, code, fields):
        emit({"identical": False, "reason": reason, "run_dir": str(run_dir)})
        return 1
    comparison = {
        "database": [
            file_sha256(entry / record["database"]),
            file_sha256(run_dir / database_name(args)),
        ],
        "events": [events_sha256(entry / "report"), events_sha256(run_dir / "report")],
    }
    identical = all(stored == rerun for stored, rerun in comparison.values())
    if identical:
        shutil.rmtree(run_dir, ignore_errors=True)
    emit(
        {
            "identical": identical,
            **{f"{name}_sha256": pair for name, pair in comparison.items()},
            "run_dir": None if identical else str(run_dir),
        }
    )
    return 0 if identical else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="prepare-sources, or its stored record")
    prepare.add_argument("--input-bundle", required=True)
    prepare.add_argument("--input-commit", required=True, help="raw bundle commit")
    prepare.add_argument(
        "--input-manifest-sha256", required=True, help="raw catalog-bundle.json digest"
    )
    prepare.add_argument("--output-dir", help="new directory to prepare into on a miss")
    prepare.add_argument(
        "--key", action="store_true", help="print the key and its fields only"
    )
    build = sub.add_parser("build", help="build-db, or its stored entry")
    build.add_argument("--prepared", required=True)
    build.add_argument(
        "--input-commit", required=True, help="prepared acceptance commit"
    )
    build.add_argument(
        "--input-manifest-sha256",
        required=True,
        help="prepared top-level manifest.json digest",
    )
    build.add_argument("--diagnostic", action="store_true")
    build.add_argument("--registers", metavar="SPEC[,SPEC...]")
    build.add_argument("--curation-dir")
    mode = build.add_mutually_exclusive_group()
    mode.add_argument(
        "--key", action="store_true", help="print the key and its fields only"
    )
    mode.add_argument(
        "--verify",
        action="store_true",
        help="rebuild uncached and compare with the stored entry",
    )
    args = parser.parse_args()
    # Resolved once, so the key and the builder (run from the repository root) read
    # the same paths.
    for name in ("input_bundle", "output_dir", "prepared", "curation_dir"):
        if getattr(args, name, None):
            setattr(args, name, str(Path(getattr(args, name)).expanduser().resolve()))
    return cmd_prepare(args) if args.command == "prepare" else cmd_build(args)


if __name__ == "__main__":
    raise SystemExit(main())
