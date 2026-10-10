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
6 hours. `--key` prints a key and its fields (for a build, the build and resolve
keys). `build --verify` rebuilds uncached and compares the database and decompressed
event-ledger bytes with the stored entry (exit 1 on a difference, which moves the entry
out of reach of lookups and keeps it beside the rebuild for 6 hours): the check that
the key covers every input. For a prepare, run `reg-meta-build prepare-sources` into a
new directory and compare its `prepared_manifest_sha256` with the stored one.

The prepare and build keys hash code as the working-tree content of the files git
would commit: tracked files, uncommitted edits included, and untracked files
`.gitignore` does not exclude (a new module the code imports), but not ignored junk
(`.DS_Store`, build output). The native extension sources are the uv `cache-keys` of
`crates/reg-core-py` (`keyed_cache.NATIVE_SOURCES`); since the extension is compiled,
every key also holds `rustc -V`, run from the repository root.

Prepare key: the raw bundle commit, `catalog-bundle.json` digest and the bundle's path
in its repository (the manifest records it); the content of every file in the code
boundary (`prepare_code_files`: the builder's own static import walk, which the probe
runs, `uv.lock`, and the native extension sources because a walked module imports
`reg_core_py`); the Python, SQLite
and Rust toolchain versions. A prepare entry is an index record (prepared path and
top-level manifest digest), not a copy of the 14 GB tree the maintainer commits to a
local acceptance repository. A lookup runs the builder's own warm-build check
(`open_prepared_catalog_sources`) against that repository's current HEAD; a hit
returns HEAD as the acceptance commit. A tree not yet committed at HEAD, with its
payload inventory complete, is reported as awaiting acceptance, without a rerun. Any
other failed check drops the record and misses.

Build key: the content of `reg_meta_build/src`, `uv.lock` and the native extension
sources; the builder's own `curation_tree_sha256` of the selected curation tree; the
prepared commit and manifest digest; the mode and the `--registers` scopes as a sorted
set (the builder selects by set); the Python, SQLite and Rust toolchain versions; and
for a publishable build the checkout's HEAD, which the database records.
Every build lookup, hit or miss, first runs the admission checks the builder runs
before it resolves anything (the prepared tree at the pinned commit and, for a
publishable build, a clean builder checkout at the keyed commit); a failure exits 10
with the builder's error code instead of returning the entry; a probe that cannot
import the builder's admission code exits 4 with `probe_environment_failed`. A build
entry holds the report directory and the database; the report's path fields are
rewritten to the entry's location when it is stored. A hit checks the database's and
the ledger's sizes (not their hashes) against the entry. The `KEEP` most recently used
entries stay, and so does any entry used within 6 hours.

Resolve key: the builder's own `resolve_code_sha256` (`reg_meta_build.resolve_code`:
the static import closure of `pipeline`, the `reg_core_py` package files and the
Python version, reported by the probe), `uv.lock` (third-party versions, which that
fingerprint leaves out), the `curation_tree_sha256`, the prepared commit and manifest
digest, the mode, the `--registers` scopes as a sorted set, and the Python, SQLite and
Rust toolchain versions; never HEAD (`materialize-db` stamps the running commit). A
resolve entry holds the `bundle` a full miss wrote with `build-db --resolved-out`; it
is stored only with its completed build, and only if `bundle.json` records the key's
resolve fields (`RESOLVE_RECORDED`). A blocked strict build stores neither entry. The
same `KEEP` and 6-hour rules apply.

A build miss whose resolve key hits runs only `materialize-db --resolved` from that
bundle, under the lock, and stores the build entry by the rules above, with
`phased_from` naming the resolve key in its `entry.json`. If `materialize-db` refuses
the bundle (`resolved_bundle_*`: stale resolve code, damaged payload, invalid
record), the resolve entry is dropped and the build runs in full; a stale bundle is
never placed and never an error. Any other failure of a phased run is a failed run
like a full one (a full build would reach the same writer). `--verify` compares
decompressed ledger bytes, so it checks a phased entry against a full build even if
their gzip members differ.

The cache lives in `$REG_REAL_SEED_CACHE`, else `$XDG_CACHE_HOME/reg-meta-real-seed`,
else `~/.cache/reg-meta-real-seed`. `$REG_REAL_SEED_BUILDER` replaces the
`uv run reg-meta-build` command line and `$REG_REAL_SEED_PYTHON` the `uv run python`
that runs the project-environment probe (the tool's contract tests stub both).
Stdlib only, like `gate.py`.
"""

from __future__ import annotations

import argparse
import functools
import gzip
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import tomllib
import zlib
from pathlib import Path

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
    staged,
    tree_digest,
)

ROOT = Path(__file__).resolve().parents[1]
MARKER = ".real-seed-cache"
# Build entries kept: a main-tip baseline and one candidate (~1.4 GB each).
# simplify: evicts by entry count, not bytes; switch to a byte budget if entries grow
# well past the catalog size. Prepare records (a few KB) are dropped only when they no
# longer hold; prune them by age if the directory passes a few hundred.
KEEP = 2
EXIT_CONFIG = 10  # reg-meta-build's exit for a diagnostic completion
EXIT_AWAITING = 3
EXIT_PROBE = 4
# The builder's `resolved_bundle` layout: `bundle.json` names and digests the rest.
BUNDLE_RECORD = "bundle.json"
# Resolve-key fields `bundle.json` also records, under the same names; a bundle is
# stored only if they agree.
RESOLVE_RECORDED = (
    "resolve_code_sha256",
    "mode",
    "registers",
    "prepared_commit",
    "prepared_manifest_sha256",
    "curation_tree_sha256",
)
# `materialize-db`'s refusals of a bundle it will not place (stale code, damaged or
# invalid payload, another mode): the entry is dropped and the build runs in full.
BUNDLE_REFUSAL_PREFIX = "resolved_bundle_"
# The source root of the one workspace package the prepare walk follows (the
# builder's `resolve_code.import_closure` walks only `reg_meta_build`); the key
# refuses any other workspace package the walk reaches.
PACKAGE_SOURCE = "reg_meta_build/src"
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
# A missing root stops the tool rather than shrinking the key.
PREPARE_ROOTS = (
    "reg_meta_build.prepared_catalog",
    "reg_meta_build.input_snapshot",
    "reg_meta_build.db",
    "reg_meta_build.source_files",
    "reg_meta_build._curation",
)
# Keyed by content but not walked: the CLI imports every subcommand's module at load,
# so walking it would key prepare on the whole builder. Their import-time code runs
# but does not reach the prepared output.
PREPARE_ENTRY_FILES = ("reg_meta_build/src/reg_meta_build/cli.py",)
# What this stdlib-only script needs from the project environment: the interpreter
# and SQLite the builder runs on, the builder's own curation digest (the value a
# build records as `curation_tree_sha256`), the builder's resolve-code fingerprint
# (the value a resolved bundle records), the builder's own import walk from the
# prepare roots (the files it reaches and the other packages it imports), the
# builder's warm-build check of an
# accepted prepared tree at its repository's HEAD, and the admission checks a build
# runs before it resolves anything (the same check of the prepared pins and, for a
# publishable build, the clean builder source).
PROBE = """
import json, sqlite3, subprocess, sys
from pathlib import Path
import importlib.util
request = json.loads(sys.argv[1])
facts = {"python": sys.version, "sqlite": sqlite3.sqlite_version}
root = Path(request["root"]).resolve()
for package in ("reg_meta_build",):
    spec = importlib.util.find_spec(package)
    if spec is None:
        continue
    origin = Path(spec.origin).resolve()
    if not origin.is_relative_to(root):
        print(json.dumps({"environment_error": (
            f"uv run imports {package} from {origin}, not from the keyed checkout "
            f"{root}; check PYTHONPATH and $REG_REAL_SEED_PYTHON"
        )}))
        sys.exit(0)
if request.get("curation"):
    from reg_meta_build.curation_compile import tree_sha256
    facts["curation_tree_sha256"] = tree_sha256(Path(request["curation"]))
if request.get("resolve_code"):
    try:
        from reg_meta_build.resolve_code import resolve_code_sha256
        facts["resolve_code_sha256"] = resolve_code_sha256()
    except Exception as exc:
        # A writer module in the resolve walk, say: `build-db --resolved-out` would
        # refuse the same way.
        facts["probe_error"] = {
            "code": "probe_resolve_code_failed",
            "message": f"{type(exc).__name__}: {exc}",
        }
if request.get("prepare_roots"):
    from reg_meta_build.resolve_code import code_files, import_closure
    try:
        modules, imports = import_closure(
            Path(request["source_root"]), request["prepare_roots"]
        )
        facts["prepare_code"] = {
            "files": sorted(
                p.relative_to(root).as_posix() for p in code_files(modules.values())
            ),
            "imports": sorted(imports),
        }
    except Exception as exc:
        facts["probe_error"] = {
            "code": "probe_prepare_code_failed",
            "message": f"{type(exc).__name__}: {exc}",
        }
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
    except ImportError as exc:
        # The environment, not the inputs: no admission check ran.
        facts["probe_error"] = {
            "code": "probe_environment_failed",
            "message": f"{type(exc).__name__}: {exc}",
        }
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
    argv = [*python, "-c", PROBE, json.dumps({**request, "root": str(ROOT)})]
    facts = json.loads(
        subprocess.run(
            argv, cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout
    )
    # The keys hash this checkout; a probe (and so a builder) importing another copy
    # would key one tree and run another.
    if error := facts.get("environment_error"):
        sys.exit(f"real-seed-cache: {error}")
    return facts


def rustc_version() -> str:
    """`rustc -V` from the repository root, which picks the toolchain that builds
    `reg_core_py`."""
    try:
        return subprocess.run(
            ["rustc", "-V"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        sys.exit(
            f"real-seed-cache: `rustc -V` failed ({exc}); the keys need the Rust "
            "toolchain that builds reg_core_py on PATH"
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


def repo_files(paths) -> set[str]:
    """The files under `paths` that git would commit and that exist on disk: tracked
    ones and untracked ones not ignored. Repo-relative."""
    listed = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            *paths,
        ],
        capture_output=True,
        check=True,
    ).stdout.decode()
    return {path for path in listed.split("\0") if path and (ROOT / path).is_file()}


def code_digests(paths) -> dict[str, str]:
    files = repo_files(paths)
    under = {
        path: [ROOT / f for f in files if f == path or f.startswith(f"{path}/")]
        for path in paths
    }
    # An empty tree would hash as a constant; a key must refuse it instead, so a
    # moved or deleted source names the path to update.
    if missing := [path for path, found in under.items() if not found]:
        sys.exit(f"real-seed-cache: keyed source missing, update the key: {missing}")
    return {path: tree_digest(ROOT / path, found) for path, found in under.items()}


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


@functools.cache
def workspace_packages() -> frozenset[str]:
    """The top-level modules in the `src/` of each uv workspace member."""
    members = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["uv"][
        "workspace"
    ]["members"]
    return frozenset(
        path.name.removesuffix(".py")
        for member in members
        if (ROOT / member / "src").is_dir()
        for path in (ROOT / member / "src").iterdir()
        if path.is_dir() or path.suffix == ".py"
    )


def prepare_code_files(walk: dict) -> list[str]:
    """The repo-relative files `prepare-sources` runs, generously, from the probe's
    `prepare_code`.

    The builder's own import walk (`resolve_code.import_closure`) follows every
    import from `PREPARE_ROOTS` through `reg_meta_build`, including function-local
    imports, whether or not the prepare path reaches them. A module that imports by
    name at run time (`import_module`) pulls in its whole package. Data files beside
    any walked module count, as do the native extension sources when a walked module
    imports `reg_core_py`, and `uv.lock` always.
    """
    if unkeyed := sorted(set(walk["imports"]) & workspace_packages()):
        sys.exit(
            f"real-seed-cache: the prepare walk reaches workspace packages {unkeyed} "
            "that it does not key; extend the walk to them"
        )
    # A walked module git ignores stays listed, so `code_digests` refuses it; ignored
    # data beside a module is junk, never keyed.
    files = {path for path in walk["files"] if path.endswith(".py")}
    files |= repo_files(walk["files"])
    files.update(PREPARE_ENTRY_FILES)
    files.add("uv.lock")
    if "reg_core_py" in walk["imports"]:
        files.update(NATIVE_SOURCES)
    return sorted(files)


# -- prepare -----------------------------------------------------------------------


def prepare_fields(args: argparse.Namespace) -> dict:
    facts = probe(source_root=str(ROOT / PACKAGE_SOURCE), prepare_roots=PREPARE_ROOTS)
    if error := facts.pop("probe_error", None):
        sys.exit(f"real-seed-cache: {error['code']}: {error['message']}")
    return {
        "step": "prepare",
        "bundle_commit": args.input_commit,
        "bundle_manifest_sha256": args.input_manifest_sha256,
        "bundle_path": git(Path(args.input_bundle), "rev-parse", "--show-prefix"),
        "code": code_digests(prepare_code_files(facts.pop("prepare_code"))),
        "runtime": {**facts, "rustc": rustc_version()},
    }


def payload_complete(prepared: Path) -> bool:
    """The `files/` inventory has exactly the manifest's paths and sizes.

    The builder's own inventory check (`check_accepted_files`) compares against an
    accepted commit, which an uncommitted tree does not have yet.
    """
    try:
        listed = {
            item["path"]: item["size"]
            for item in json.loads((prepared / "manifest.json").read_text())["files"]
        }
    except OSError, ValueError, KeyError, TypeError:
        return False
    actual = {
        path.relative_to(prepared).as_posix(): path.stat().st_size
        for path in (prepared / "files").rglob("*")
        if path.is_file()
    }
    return listed == actual


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
    # Committed at HEAD, not merely tracked: a staged but uncommitted tree still
    # awaits acceptance.
    committed = subprocess.run(
        ["git", "-C", str(prepared), "cat-file", "-e", "HEAD:./manifest.json"],
        capture_output=True,
        check=False,
    )
    if (
        committed.returncode
        and manifest.is_file()
        and file_sha256(manifest) == record["manifest_sha256"]
        and payload_complete(prepared)
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
        # The inputs or checkout may have changed while this session waited.
        fields = prepare_fields(args)
        key = digest(fields)
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
        "code": code_digests(BUILDER_SOURCES),
        "builder_commit": (
            git(ROOT, "rev-parse", "HEAD")
            if not args.diagnostic and not args.registers
            else None
        ),
    }


def build_fields(args: argparse.Namespace) -> tuple[dict, dict]:
    """The build key's fields and the resolve key's, from one probe."""
    code = build_code(args)
    facts = probe(
        curation=args.curation_dir or str(ROOT / "reg_meta_build/curation"),
        resolve_code=True,
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
    if error := facts.pop("probe_error", None):
        emit({"hit": False, "error": error})
        sys.exit(EXIT_PROBE)
    if error := facts.pop("admission_error", None):
        emit({"hit": False, "error": error})
        sys.exit(EXIT_CONFIG)
    resolve_code = facts.pop("resolve_code_sha256")
    shared = {
        "mode": "diagnostic" if args.diagnostic else "strict",
        "registers": sorted(set(args.registers.split(","))) if args.registers else [],
        "prepared_commit": args.input_commit,
        "prepared_manifest_sha256": args.input_manifest_sha256,
        "curation_tree_sha256": facts.pop("curation_tree_sha256"),
        "runtime": {**facts, "rustc": rustc_version()},
    }
    # No HEAD: `materialize-db` stamps the running builder's commit, so a bundle
    # serves every commit whose resolve code is the same.
    resolve = {
        "step": "resolve",
        **shared,
        "resolve_code_sha256": resolve_code,
        "uv_lock": code["code"]["uv.lock"],
    }
    return {"step": "build", **shared, **code}, resolve


def database_name(args: argparse.Namespace) -> str:
    return "diagnostic.db" if args.diagnostic else "catalog/reg_meta.db"


def lookup_build(home: Path, key: str) -> dict | None:
    entry = home / key
    if not (entry / "entry.json").is_file():
        return None
    record = json.loads((entry / "entry.json").read_text())
    database = entry / record["database"]
    events = entry / "report/events.jsonl.gz"
    # Sizes and an mtime, not hashes: a hit must stay cheap. `--verify` reads both.
    if (
        not database.is_file()
        or [database.stat().st_size, database.stat().st_mtime_ns]
        != [record["database_size"], record["database_mtime_ns"]]
        or not events.is_file()
        or events.stat().st_size != record["events_size"]
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


def lookup_resolve(home: Path, key: str) -> Path | None:
    """The stored bundle, or None on a miss. Its payload digests are checked by
    `materialize-db`, which refuses a damaged bundle."""
    entry = home / key
    if (
        not (entry / "entry.json").is_file()
        or not (entry / "bundle" / BUNDLE_RECORD).is_file()
    ):
        if entry.exists():
            sys.stderr.write(f"real-seed-cache: dropping a changed entry: {entry}\n")
            shutil.rmtree(entry, ignore_errors=True)
        return None
    os.utime(entry)
    return entry / "bundle"


def run_build(
    args: argparse.Namespace,
    home: Path,
    *,
    resolved: Path | None = None,
    resolved_out: bool = False,
) -> tuple[Path, int]:
    """Run build-db in a new staging directory under `home`; it and the exit code.

    With `resolved`, run `materialize-db` from that bundle instead; with
    `resolved_out`, build-db also writes its bundle to `<run_dir>/resolved`.
    """
    run_dir = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=home))
    argv = [*builder()]
    if not args.diagnostic:
        (run_dir / "catalog").mkdir()
        argv += ["--db", str(run_dir / "catalog")]
    if resolved is None:
        argv += [
            "build-db",
            "--prepared",
            args.prepared,
            "--input-commit",
            args.input_commit,
            "--input-manifest-sha256",
            args.input_manifest_sha256,
        ]
        if args.curation_dir:
            argv += ["--curation-dir", args.curation_dir]
        if resolved_out:
            argv += ["--resolved-out", str(run_dir / "resolved")]
    else:
        argv += ["materialize-db", "--resolved", str(resolved)]
    argv += ["--report-dir", str(run_dir / "report"), "--timing"]
    if args.diagnostic:
        argv += ["--diagnostic", "--diagnostic-db-path", str(run_dir / "diagnostic.db")]
    if args.registers:
        argv += ["--registers", args.registers]
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


def relocate(run_dir: Path, entry: Path) -> None:
    """Point the path fields of `report/summary.json` and `result.json` at `entry`.

    The builder records the database path it wrote, which is the staging directory
    about to be renamed; a stored report must name where the entry lives. Only JSON
    string values that start with the staging path change; every other byte of the
    builder's report, and the event ledger, stays as written.
    """
    old, new = json.dumps(str(run_dir))[:-1], json.dumps(str(entry))[:-1]
    for path in (run_dir / "report/summary.json", run_dir / "result.json"):
        path.write_text(path.read_text().replace(old, new))


def events_sha256(report: Path) -> str:
    with gzip.open(report / "events.jsonl.gz", "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def refused_bundle(run_dir: Path) -> str | None:
    """The error code of a `materialize-db` run that refused its bundle, else None."""
    try:
        error = json.loads((run_dir / "result.json").read_text()).get("error")
    except OSError, ValueError, AttributeError:
        return None
    code = error.get("code") if isinstance(error, dict) else None
    if isinstance(code, str) and code.startswith(BUNDLE_REFUSAL_PREFIX):
        return code
    return None


def store_resolve(home: Path, key: str, fields: dict, bundle: Path) -> None:
    """Move a completed build's `bundle` into the resolve entry `key`, if the bundle
    records the resolve inputs the key holds."""
    try:
        record = json.loads((bundle / BUNDLE_RECORD).read_text())
        recorded = {name: record.get(name) for name in RESOLVE_RECORDED}
    except (OSError, ValueError, AttributeError) as exc:
        recorded = {"unreadable": str(exc)}
    if recorded != {name: fields[name] for name in RESOLVE_RECORDED}:
        sys.stderr.write(
            "real-seed-cache: resolve entry not stored (the bundle records other "
            f"resolve inputs than the key: {recorded})\n"
        )
        shutil.rmtree(bundle, ignore_errors=True)
        return
    entry = home / key
    with staged(entry) as staging:
        bundle.rename(staging / "bundle")
        write_json(staging / "entry.json", {"key": key, "fields": fields})
    evict(home, entry, KEEP, "real-seed-cache", min_idle=STAGING_RETENTION_SECONDS)


def cmd_build(args: argparse.Namespace) -> int:
    fields, resolve_fields = build_fields(args)
    key = digest(fields)
    if args.key:
        emit(
            {
                "key": key,
                "fields": fields,
                "resolve_key": digest(resolve_fields),
                "resolve_fields": resolve_fields,
            }
        )
        return 0
    home = cache_dir("build")
    if args.verify:
        # Held throughout, so concurrent verifications cannot quarantine the same
        # entry twice.
        with real_seed_lock():
            return verify_build(args, home, key, fields, digest(resolve_fields))
    if record := lookup_build(home, key):
        emit(build_result(home, record, hit=True))
        return 0
    with real_seed_lock():
        # The inputs or checkout may have changed while this session waited;
        # `build_fields` also reruns admission.
        fields, resolve_fields = build_fields(args)
        key, resolve_key = digest(fields), digest(resolve_fields)
        if record := lookup_build(home, key):
            emit(build_result(home, record, hit=True))
            return 0
        resolve_home = cache_dir("resolve")
        phased = False
        if bundle := lookup_resolve(resolve_home, resolve_key):
            run_dir, code = run_build(args, home, resolved=bundle)
            phased = True
            if refusal := refused_bundle(run_dir):
                # A stale or damaged entry: never an error, never placed.
                sys.stderr.write(
                    f"real-seed-cache: materialize-db refused {bundle} ({refusal}); "
                    "dropping it and building in full\n"
                )
                shutil.rmtree(bundle.parent, ignore_errors=True)
                shutil.rmtree(run_dir, ignore_errors=True)
                phased = False
        if not phased:
            run_dir, code = run_build(args, home, resolved_out=True)
        if reason := incomplete(args, run_dir, code, fields):
            sys.stderr.write(f"real-seed-cache: not stored ({reason})\n")
            # A bundle of a run that did not complete is never stored, nor needed
            # to diagnose it (the report holds its events); it is 0.3-0.4 GB.
            shutil.rmtree(run_dir / "resolved", ignore_errors=True)
            # Reaps failed runs past their retention, so repeated failures cannot
            # fill the disk. This one's retention starts now (its mtime is from
            # creation, and a build can outlast the grace period), so it stays.
            os.utime(run_dir)
            evict(
                home,
                run_dir,
                KEEP,
                "real-seed-cache",
                min_idle=STAGING_RETENTION_SECONDS,
            )
            emit(
                {
                    "hit": False,
                    "stored": False,
                    "reason": reason,
                    "run_dir": str(run_dir),
                }
            )
            return code or 1
        if not phased:
            store_resolve(
                resolve_home, resolve_key, resolve_fields, run_dir / "resolved"
            )
        summary = json.loads((run_dir / "report/summary.json").read_text())
        database = run_dir / database_name(args)
        database.chmod(0o444)
        stat = database.stat()
        entry = home / key
        relocate(run_dir, entry)
        record = {
            "key": key,
            "fields": fields,
            "database": database_name(args),
            "database_size": stat.st_size,
            "database_mtime_ns": stat.st_mtime_ns,
            "events_size": (run_dir / "report/events.jsonl.gz").stat().st_size,
            "status": summary["status"],
            "publication_ready": summary.get("publication_ready"),
            # The resolve entry a phased run materialized; None for a full build.
            "phased_from": resolve_key if phased else None,
        }
        write_json(run_dir / "entry.json", record)
        run_dir.rename(entry)
        evict(home, entry, KEEP, "real-seed-cache", min_idle=STAGING_RETENTION_SECONDS)
    emit({**build_result(home, record, hit=False), "phased": phased})
    return 0


def verify_build(
    args: argparse.Namespace, home: Path, key: str, fields: dict, resolve_key: str
) -> int:
    """Rebuild uncached and compare the database and decompressed ledger bytes.

    A mismatch also quarantines the resolve entries that could replay the
    rejected output: the one this entry was placed from, and the one under the
    current resolve key (written by the same build when it was not phased).
    """
    record = lookup_build(home, key)
    if record is None:
        sys.exit("real-seed-cache: nothing stored under this key to verify")
    entry = home / key
    # Read the stored side first: an unreadable ledger is a broken entry, found
    # before a full rebuild is spent on it.
    try:
        stored = {
            "database": file_sha256(entry / record["database"]),
            "events": events_sha256(entry / "report"),
        }
    except (OSError, EOFError, zlib.error) as exc:
        shutil.rmtree(entry, ignore_errors=True)
        emit({"identical": False, "reason": f"stored entry unreadable, dropped: {exc}"})
        return 1
    run_dir, code = run_build(args, home)
    if reason := incomplete(args, run_dir, code, fields):
        emit({"identical": False, "reason": reason, "run_dir": str(run_dir)})
        return 1
    comparison = {
        "database": [stored["database"], file_sha256(run_dir / database_name(args))],
        "events": [stored["events"], events_sha256(run_dir / "report")],
    }
    identical = all(stored == rerun for stored, rerun in comparison.values())
    quarantine = None
    if identical:
        shutil.rmtree(run_dir, ignore_errors=True)
    else:
        # Out of reach of lookups, kept for diagnosis; the staging prefix lets
        # `evict` reap it after the grace period, counted from now.
        quarantine = home / f"{STAGING_PREFIX}quarantine-{key}"
        shutil.rmtree(quarantine, ignore_errors=True)
        entry.rename(quarantine)
        os.utime(quarantine)
        resolve_home = cache_dir("resolve")
        for suspect in {record.get("phased_from"), resolve_key} - {None}:
            if (resolve_home / suspect).exists():
                held = resolve_home / f"{STAGING_PREFIX}quarantine-{suspect}"
                shutil.rmtree(held, ignore_errors=True)
                (resolve_home / suspect).rename(held)
                os.utime(held)
    emit(
        {
            "identical": identical,
            **{f"{name}_sha256": pair for name, pair in comparison.items()},
            "run_dir": None if identical else str(run_dir),
            "quarantined_entry": quarantine and str(quarantine),
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
    # Checked before any `uv run`: the probe's import check only sees the damage after
    # `uv run` has synced this checkout's editable packages into that environment, so
    # another checkout's .venv would run this tree's code until re-synced (#1337).
    env = os.environ.get("UV_PROJECT_ENVIRONMENT")
    if env and (ROOT / env).resolve() != (ROOT / ".venv").resolve():
        sys.exit(
            f"real-seed-cache: UV_PROJECT_ENVIRONMENT names {env}, not {ROOT}/.venv; "
            "uv run would install this checkout's packages there. Unset it."
        )
    # Resolved once, so the key and the builder (run from the repository root) read
    # the same paths.
    for name in ("input_bundle", "output_dir", "prepared", "curation_dir"):
        if getattr(args, name, None):
            setattr(args, name, str(Path(getattr(args, name)).expanduser().resolve()))
    return cmd_prepare(args) if args.command == "prepare" else cmd_build(args)


if __name__ == "__main__":
    raise SystemExit(main())
