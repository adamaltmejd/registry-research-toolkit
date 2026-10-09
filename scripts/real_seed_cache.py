#!/usr/bin/env python3
"""real_seed_cache — reuse real-seed `prepare-sources` and `build-db` outputs by input key.

    uv run --no-project scripts/real_seed_cache.py prepare --input-bundle DIR \\
        --input-commit SHA --input-manifest-sha256 SHA256 --output-dir NEW_DIR
    uv run --no-project scripts/real_seed_cache.py accept-prepared \\
        --prepared DIR --prepared-commit SHA
    uv run --no-project scripts/real_seed_cache.py build --prepared DIR \\
        --input-commit SHA --input-manifest-sha256 SHA256 [--diagnostic] [--registers SPEC]

Each command looks its key up first and prints one JSON object on stdout. On a hit it
returns the stored result without running anything. On a miss it takes the
machine-wide real-seed lock (`gate.py real-seed`), looks again (a queued session may
have produced it meanwhile), runs the real `reg-meta-build` command and stores the
result only if the run completed: a prepare that exited 0, a diagnostic build with
`status = diagnostic_complete`, a strict build with exit 0, `status = complete` and,
for a full build, `publication_ready`. A failed run is never stored; its outputs stay
where it ran, for diagnosis.

`--key` prints the key and its fields without looking anything up. `--verify` reruns
the command uncached and compares it with the stored entry byte for byte (exit 1 on a
difference): a build's database bytes and decompressed event-ledger bytes, a prepare's
top-level prepared manifest digest. That is the check that a key covers every input.

A prepare entry is an index record, not a copy: prepare writes a 14 GB tree that the
maintainer commits to a local acceptance repository. The record names the prepared
directory, its top-level `manifest.json` digest and, once `accept-prepared` records
it, the acceptance commit. A lookup re-checks all three; a record that no longer holds
is dropped and the lookup misses.

A build entry holds the report directory and the database. The `KEEP` most recently
used build entries stay (about 1.4 GB each); a hit marks its entry used, and its paths
stay valid until `KEEP` newer entries are stored. A strict `--registers` subset is
never publication-ready, so it is stored on exit 0 with `status = complete`.

The cache lives in `$REG_REAL_SEED_CACHE`, else `$XDG_CACHE_HOME/reg-meta-real-seed`,
else `~/.cache/reg-meta-real-seed`. `$REG_REAL_SEED_BUILDER` replaces the
`uv run reg-meta-build` command line (the tool's contract tests run a stub there).
Stdlib only, like `gate.py`.
"""

from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from contextlib import ExitStack, contextmanager, redirect_stdout
from pathlib import Path
from typing import TYPE_CHECKING

from gate import real_seed_lock
from keyed_cache import (
    BUILDER_SOURCES,
    NATIVE_SOURCES,
    cache_home,
    evict,
    file_sha256,
    owned_root,
    staged,
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
# calls none of those readers). A run under `--verify` is the check.
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
# The project-environment facts a key needs and this stdlib-only script cannot read:
# the interpreter and SQLite the builder runs on, and the builder's own curation
# digest (`curation_tree_sha256`, the value a build records in `summary.json`).
PROBE = """
import json, sqlite3, sys
from pathlib import Path
facts = {"python": sys.version, "sqlite": sqlite3.sqlite_version}
if len(sys.argv) > 1:
    from reg_meta_build.curation_compile import tree_sha256
    facts["curation_tree_sha256"] = tree_sha256(Path(sys.argv[1]).resolve())
print(json.dumps(facts))
"""


def builder() -> list[str]:
    return shlex.split(os.environ.get("REG_REAL_SEED_BUILDER", "uv run reg-meta-build"))


def probe(curation: Path | None = None) -> dict[str, str]:
    argv = ["uv", "run", "--quiet", "python", "-c", PROBE]
    if curation is not None:
        argv.append(str(curation))
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


def emit(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


@contextmanager
def locked() -> Iterator[None]:
    """The real-seed lock, with its waiting notice on stderr (stdout is the result)."""
    with ExitStack() as stack:
        with redirect_stdout(sys.stderr):
            stack.enter_context(real_seed_lock())
        yield


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
        "code": {path: tree_digest(ROOT / path) for path in prepare_code_files()},
        "runtime": probe(),
    }


def prepare_home() -> Path:
    home = owned_root(cache_home("reg-meta-real-seed", "REG_REAL_SEED_CACHE"), MARKER)
    (home / "prepare").mkdir(exist_ok=True)
    return home / "prepare"


def _record_holds(record: dict) -> bool:
    """The prepared tree still has the recorded manifest, and the recorded acceptance
    commit (if any) still holds that manifest."""
    prepared = Path(record["prepared_path"])
    manifest = prepared / "manifest.json"
    if not manifest.is_file() or file_sha256(manifest) != record["manifest_sha256"]:
        return False
    if record["prepared_commit"] is None:
        return True
    try:
        prefix = git(prepared, "rev-parse", "--show-prefix")
        committed = subprocess.run(
            [
                "git",
                "-C",
                str(prepared),
                "cat-file",
                "blob",
                f"{record['prepared_commit']}:{prefix}manifest.json",
            ],
            capture_output=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError:
        return False
    return hashlib.sha256(committed).hexdigest() == record["manifest_sha256"]


def lookup_prepare(home: Path, key: str) -> dict | None:
    path = home / f"{key}.json"
    if not path.is_file():
        return None
    record = json.loads(path.read_text())
    if _record_holds(record):
        return record
    sys.stderr.write(
        f"real-seed-cache: dropping a record that no longer holds: {path}\n"
    )
    path.unlink()
    return None


def prepare_result(record: dict, *, hit: bool) -> dict:
    result = {
        "hit": hit,
        "key": record["key"],
        "prepared_path": record["prepared_path"],
        "prepared_manifest_sha256": record["manifest_sha256"],
        "prepared_commit": record["prepared_commit"],
    }
    if record["prepared_commit"] is None:
        result["next"] = (
            "commit the prepared tree in its local acceptance repository, then run "
            "accept-prepared with that commit"
        )
    return result


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
    home = prepare_home()
    record = lookup_prepare(home, key)
    if record is not None and not args.verify:
        emit(prepare_result(record, hit=True))
        return 0
    if args.output_dir is None:
        sys.exit("real-seed-cache: a miss or --verify needs a new --output-dir")
    output = Path(args.output_dir).expanduser().resolve()
    if args.verify:
        if record is None:
            sys.exit("real-seed-cache: nothing stored under this key to verify")
        with locked():
            result = run_prepare(args, output)
        if result is None:
            emit({"identical": False, "reason": "prepare-sources failed"})
            return 1
        identical = result["prepared_manifest_sha256"] == record["manifest_sha256"]
        if identical:
            shutil.rmtree(output)
        emit(
            {
                "identical": identical,
                "stored": record["manifest_sha256"],
                "rerun": result["prepared_manifest_sha256"],
                "rerun_path": None if identical else str(output),
            }
        )
        return 0 if identical else 1
    with locked():
        if record := lookup_prepare(home, key):
            emit(prepare_result(record, hit=True))
            return 0
        result = run_prepare(args, output)
    reason = (
        "prepare-sources failed"
        if result is None
        else "inputs changed during the run"
        if digest(prepare_fields(args)) != key
        else None
    )
    if result is None or reason is not None:
        sys.stderr.write(f"real-seed-cache: not stored ({reason})\n")
        emit(
            {"hit": False, "stored": False, "reason": reason, "output_dir": str(output)}
        )
        return 1
    record = {
        "key": key,
        "fields": fields,
        "prepared_path": str(output),
        "manifest_sha256": result["prepared_manifest_sha256"],
        "prepared_commit": None,
    }
    write_json(home / f"{key}.json", record)
    emit(prepare_result(record, hit=False))
    return 0


def cmd_accept_prepared(args: argparse.Namespace) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", args.prepared_commit):
        sys.exit("real-seed-cache: --prepared-commit must be a full lowercase SHA")
    home = prepare_home()
    prepared = str(Path(args.prepared).expanduser().resolve())
    for path in sorted(home.glob("*.json")):
        record = json.loads(path.read_text())
        if record["prepared_path"] != prepared:
            continue
        accepted = {**record, "prepared_commit": args.prepared_commit}
        if not _record_holds(accepted):
            sys.exit(
                f"real-seed-cache: {args.prepared_commit} does not hold the prepared "
                f"manifest {record['manifest_sha256']} at {prepared}"
            )
        write_json(path, accepted)
        emit(prepare_result(accepted, hit=True))
        return 0
    sys.exit(f"real-seed-cache: no stored preparation at {prepared}")


# -- build -------------------------------------------------------------------------


def build_code(args: argparse.Namespace) -> dict:
    """The builder code a build runs. A publishable build also records the
    checkout's commit in the database, so that commit is keyed too."""
    return {
        "code": {
            path: tree_digest(ROOT / path)
            for path in (*BUILDER_SOURCES, PACKAGES["reg_schema"])
        },
        "builder_commit": (
            git(ROOT, "rev-parse", "HEAD")
            if not args.diagnostic and not args.registers
            else None
        ),
    }


def build_fields(args: argparse.Namespace) -> dict:
    curation = (
        Path(args.curation_dir).expanduser().resolve()
        if args.curation_dir
        else ROOT / "reg_meta_build/curation"
    )
    facts = probe(curation)
    return {
        "step": "build",
        "mode": "diagnostic" if args.diagnostic else "strict",
        "registers": args.registers.split(",") if args.registers else [],
        "prepared_commit": args.input_commit,
        "prepared_manifest_sha256": args.input_manifest_sha256,
        **build_code(args),
        "curation_tree_sha256": facts.pop("curation_tree_sha256"),
        "runtime": facts,
    }


def build_home() -> Path:
    home = owned_root(cache_home("reg-meta-real-seed", "REG_REAL_SEED_CACHE"), MARKER)
    (home / "build").mkdir(exist_ok=True)
    return home / "build"


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


def run_build(args: argparse.Namespace, run_dir: Path) -> int:
    report = run_dir / "report"
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
        str(report),
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
        return subprocess.run(argv, cwd=ROOT, stdout=out, check=False).returncode


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
    home = build_home()
    if args.verify:
        return verify_build(args, home, key, fields)
    if record := lookup_build(home, key):
        emit(build_result(home, record, hit=True))
        return 0
    with locked():
        if record := lookup_build(home, key):
            emit(build_result(home, record, hit=True))
            return 0
        run_dir = Path(tempfile.mkdtemp(prefix="regmeta-build."))
        code = run_build(args, run_dir)
    reason = incomplete(args, run_dir, code, fields)
    # The summary check above covers the curation tree; the code is checked here.
    if reason is None and build_code(args) != {
        name: fields[name] for name in ("code", "builder_commit")
    }:
        reason = "the builder code changed during the run"
    if reason is not None:
        sys.stderr.write(f"real-seed-cache: not stored ({reason})\n")
        emit({"hit": False, "stored": False, "reason": reason, "run_dir": str(run_dir)})
        return code or 1
    summary = json.loads((run_dir / "report/summary.json").read_text())
    entry = home / key
    with staged(entry) as staging:
        for name in ("report", "result.json", database_name(args).split("/")[0]):
            shutil.move(run_dir / name, staging / name)
        database = staging / database_name(args)
        database.chmod(0o444)
        stat = database.stat()
        record = {
            "key": key,
            "fields": fields,
            "database": database_name(args),
            "database_size": stat.st_size,
            "database_mtime_ns": stat.st_mtime_ns,
            "database_sha256": file_sha256(database),
            "events_sha256": events_sha256(staging / "report"),
            "status": summary["status"],
            "publication_ready": summary.get("publication_ready"),
        }
        write_json(staging / "entry.json", record)
    shutil.rmtree(run_dir, ignore_errors=True)
    evict(home, entry, KEEP, "real-seed-cache")
    emit(build_result(home, record, hit=False))
    return 0


def verify_build(args: argparse.Namespace, home: Path, key: str, fields: dict) -> int:
    """Rebuild uncached and compare the database and decompressed ledger bytes."""
    record = lookup_build(home, key)
    if record is None:
        sys.exit("real-seed-cache: nothing stored under this key to verify")
    entry = home / key
    with locked():
        run_dir = Path(tempfile.mkdtemp(prefix="regmeta-build."))
        code = run_build(args, run_dir)
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
    prepare.add_argument(
        "--output-dir", help="new directory to prepare into on a miss or --verify"
    )
    accept = sub.add_parser(
        "accept-prepared", help="record the acceptance commit of a stored preparation"
    )
    accept.add_argument("--prepared", required=True)
    accept.add_argument("--prepared-commit", required=True)
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
    for command in (prepare, build):
        mode = command.add_mutually_exclusive_group()
        mode.add_argument(
            "--key", action="store_true", help="print the key and its fields only"
        )
        mode.add_argument(
            "--verify",
            action="store_true",
            help="rerun uncached and compare with the stored entry",
        )
    args = parser.parse_args()
    return {
        "prepare": cmd_prepare,
        "accept-prepared": cmd_accept_prepared,
        "build": cmd_build,
    }[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
