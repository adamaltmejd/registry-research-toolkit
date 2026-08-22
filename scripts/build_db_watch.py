#!/usr/bin/env python3
"""Run and monitor a real `reg-meta-build build-db` rebuild.

This is agent tooling, not part of the package runtime. It keeps the fragile
full-corpus rebuild workflow in one place: scratch DB dir, copied slug TOMLs,
timestamped log, sparse milestone output, quiet-period process health, and
post-build SQLite checks.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

KEY_TABLES = (
    "provider",
    "register",
    "register_variant",
    "variable",
    "variable_state",
    "value_code",
    "value_set",
    "value_set_member",
    "code_variable_map",
    "concept_group",
    "representation_replaced_by",
)

MILESTONE_PREFIXES = (
    "[timing]",
    "[FAIL]",
    "Building ",
    "Coalescing ",
    "Computing ",
    "Database ",
    "Dropped ",
    "Importing ",
    "Linking ",
    "Loading ",
    "Materializing ",
    "Populating ",
    "Projecting ",
    "Resolving ",
    "SCB value prestage ",
    "Skipping ",
    "Validating ",
)
MILESTONE_CONTAINS = (
    " cvids ",
    " rows",
    " value_sets",
    " variable_state rows",
    " lineage edges",
    " classifications",
    " concept groups",
    " code×variable mappings",
    " FTS indexes built",
    "Database written to",
    "SCB value prestage",
)
QUEUE_EOF = object()


class SigtermReceived(Exception):
    """Raised by the SIGTERM handler so child cleanup still runs."""


def handle_sigterm(_signum: int, _frame: Any) -> None:
    raise SigtermReceived


@dataclass(frozen=True)
class RunPaths:
    db_dir: Path
    slug_dir: Path | None
    prestage_cache: Path | None
    log_path: Path
    summary_path: Path
    created_db_dir: bool
    created_slug_dir: bool
    dbdiff_path: Path | None = None


def now_stamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def slug_stamp() -> str:
    return datetime.now().astimezone().strftime("regmeta-build-%Y%m%d-%H%M%S")


def repo_root() -> Path:
    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        text=True,
        capture_output=True,
    )
    return Path(proc.stdout.strip())


def emit(kind: str, message: str) -> None:
    print(f"[{now_stamp()}] {kind}: {message}", flush=True)


def is_milestone(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith(("[OK]", "[INFO]")):
        return False
    if stripped.startswith(MILESTONE_PREFIXES):
        return True
    if stripped.startswith(("...", "  ...")):
        return True
    return any(token in stripped for token in MILESTONE_CONTAINS)


def format_bytes(n_bytes: int) -> str:
    value = float(n_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    raise AssertionError("unreachable")


def path_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except FileNotFoundError:
                continue
    return total


def process_health(pid: int) -> dict[str, str]:
    try:
        proc = subprocess.run(
            ["ps", "-o", "%cpu=", "-o", "rss=", "-p", str(pid)],
            check=True,
            text=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        return {"process": f"unavailable ({type(exc).__name__})"}
    parts = proc.stdout.split()
    if len(parts) < 2:
        return {"process": "unavailable"}
    return {"cpu": f"{parts[0]}%", "rss": f"{format_bytes(int(parts[1]) * 1024)}"}


def format_process_health(health: dict[str, str]) -> str:
    if "cpu" in health and "rss" in health:
        return f"cpu={health['cpu']} rss={health['rss']}"
    return f"process={health.get('process', 'unavailable')}"


def copy_slug_dir(source: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest)


def providers_include_scb(providers: str | None) -> bool:
    if providers is None:
        return True
    return "scb" in {provider.strip() for provider in providers.split(",")}


def prepare_paths(args: argparse.Namespace, root: Path) -> RunPaths:
    slug = args.slug or slug_stamp()
    created_db_dir = args.db_dir is None
    db_dir = (
        Path(args.db_dir)
        if args.db_dir
        else Path(tempfile.mkdtemp(prefix=f"{slug}.", dir=args.tmp_dir))
    )
    db_dir = db_dir.expanduser().resolve()

    if args.log:
        log_path = Path(args.log).expanduser().resolve()
    else:
        log_path = Path(args.tmp_dir).expanduser().resolve() / f"{slug}.log"
    summary_path = (
        Path(args.summary).expanduser().resolve()
        if args.summary
        else log_path.with_suffix(".summary.json")
    )
    dbdiff_path = (
        Path(args.dbdiff_json).expanduser().resolve()
        if args.dbdiff_json
        else log_path.with_suffix(".dbdiff.json")
        if args.dbdiff_against
        else None
    )

    if args.slug_dir:
        slug_dir = Path(args.slug_dir).expanduser().resolve()
        created_slug_dir = False
    elif args.use_repo_slug_dir:
        slug_dir = None
        created_slug_dir = False
    else:
        source = root / "reg_meta_build" / "fqid_slugs"
        slug_dir = Path(
            tempfile.mkdtemp(prefix=f"{slug}-slugs.", dir=args.tmp_dir)
        ).resolve()
        copy_slug_dir(source, slug_dir)
        created_slug_dir = True

    if args.no_prestage_cache or not providers_include_scb(args.providers):
        prestage_cache = None
    elif args.prestage_cache:
        prestage_cache = Path(args.prestage_cache).expanduser().resolve()
    else:
        prestage_cache = (
            Path(args.tmp_dir).expanduser().resolve()
            / "regmeta-build-prestage"
            / "scb-value-prestage.sqlite"
        )

    log_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    if dbdiff_path is not None:
        dbdiff_path.parent.mkdir(parents=True, exist_ok=True)
    if prestage_cache is not None:
        prestage_cache.parent.mkdir(parents=True, exist_ok=True)
    db_dir.mkdir(parents=True, exist_ok=True)
    return RunPaths(
        db_dir=db_dir,
        slug_dir=slug_dir,
        prestage_cache=prestage_cache,
        log_path=log_path,
        summary_path=summary_path,
        created_db_dir=created_db_dir,
        created_slug_dir=created_slug_dir,
        dbdiff_path=dbdiff_path,
    )


def build_command(args: argparse.Namespace, paths: RunPaths) -> list[str]:
    cmd = [
        "uv",
        "run",
        "reg-meta-build",
        "--db",
        str(paths.db_dir),
        "build-db",
        "--input-dir",
        str(Path(args.input_dir).expanduser()),
    ]
    if paths.slug_dir is not None:
        cmd.extend(["--slug-dir", str(paths.slug_dir)])
    if args.no_validate:
        cmd.append("--no-validate")
    if args.no_timing is False:
        cmd.append("--timing")
    if args.providers:
        cmd.extend(["--providers", args.providers])
    if paths.prestage_cache is not None:
        cmd.extend(["--scb-value-prestage-cache", str(paths.prestage_cache)])
        if args.refresh_prestage_cache:
            cmd.append("--refresh-scb-value-prestage-cache")
    return cmd


def build_dbdiff_command(args: argparse.Namespace, built_db: Path) -> list[str]:
    cmd = [
        "uv",
        "run",
        "python",
        "-m",
        "reg_meta_build.dbdiff",
        str(Path(args.dbdiff_against).expanduser()),
        str(built_db),
        "--json",
        "--sample-rows",
        str(args.dbdiff_sample_rows),
    ]
    if args.dbdiff_no_default_ignore:
        cmd.append("--no-default-ignore")
    return cmd


def reader_thread(stream: Any, out: queue.Queue[Any]) -> None:
    try:
        for line in stream:
            out.put(line.rstrip("\n"))
    finally:
        out.put(QUEUE_EOF)


def run_build(cmd: list[str], paths: RunPaths, quiet_seconds: int) -> int:
    emit("start", " ".join(cmd))
    emit("log", str(paths.log_path))
    start = time.monotonic()
    last_output = start
    last_health = start
    lines: queue.Queue[Any] = queue.Queue()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    thread = threading.Thread(
        target=reader_thread, args=(proc.stdout, lines), daemon=True
    )
    thread.start()

    try:
        with paths.log_path.open("a", encoding="utf-8") as log:
            log.write(f"[{now_stamp()}] $ {' '.join(cmd)}\n")
            while True:
                try:
                    line = lines.get(timeout=0.25)
                except queue.Empty:
                    line = None

                if line is QUEUE_EOF:
                    break
                if line is not None:
                    last_output = time.monotonic()
                    log.write(f"[{now_stamp()}] {line}\n")
                    log.flush()
                    if is_milestone(line):
                        emit("build", line.strip())

                now = time.monotonic()
                if (
                    proc.poll() is None
                    and now - last_health >= quiet_seconds
                    and now - last_output >= quiet_seconds
                ):
                    health = process_health(proc.pid)
                    emit(
                        "quiet",
                        (
                            f"no output for {int(now - last_output)}s; "
                            f"elapsed {int(now - start)}s; "
                            f"{format_process_health(health)} "
                            f"db_dir={format_bytes(path_size(paths.db_dir))} "
                            f"log={format_bytes(path_size(paths.log_path))}"
                        ),
                    )
                    last_health = now
    except KeyboardInterrupt, SigtermReceived:
        emit("interrupt", "terminating child build process")
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            emit("interrupt", "child ignored SIGTERM; sending SIGKILL")
            proc.kill()
            proc.wait(timeout=20)
        raise

    rc = proc.wait()
    emit("exit", f"build-db exited {rc} after {int(time.monotonic() - start)}s")
    return rc


def table_count(conn: sqlite3.Connection, table: str) -> int | None:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    if exists is None:
        return None
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def sqlite_checks(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
        counts = {
            table: count
            for table in KEY_TABLES
            if (count := table_count(conn, table)) is not None
        }
        return {
            "integrity_check": integrity,
            "foreign_key_violations": len(fk_rows),
            "table_counts": counts,
        }
    finally:
        conn.close()


def summarize_dbdiff_report(report: dict[str, Any]) -> dict[str, Any]:
    schema = report.get("schema", {})
    column_diffs = schema.get("column_diffs", [])
    differing_tables = [
        table["table"] for table in report.get("tables", []) if not table["identical"]
    ]
    return {
        "identical": bool(report.get("identical")),
        "schema_differs": bool(
            schema.get("tables_only_in_a")
            or schema.get("tables_only_in_b")
            or schema.get("indexes_only_in_a")
            or schema.get("indexes_only_in_b")
            or schema.get("index_mismatches")
            or column_diffs
        ),
        "content_differs": bool(differing_tables),
        "differing_table_count": len(differing_tables),
        "differing_tables": differing_tables[:25],
    }


def run_dbdiff(cmd: list[str], report_path: Path) -> dict[str, Any]:
    emit("dbdiff", " ".join(cmd))
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    report_path.write_text(proc.stdout, encoding="utf-8")
    result: dict[str, Any] = {
        "command": cmd,
        "return_code": proc.returncode,
        "report_path": str(report_path),
    }
    if proc.stderr:
        result["stderr"] = proc.stderr.strip()
    try:
        report = json.loads(proc.stdout)
    except json.JSONDecodeError:
        result["error"] = "dbdiff did not emit JSON"
        if proc.stdout:
            result["stdout_excerpt"] = proc.stdout[:2000]
        emit("dbdiff", f"error; report written to {report_path}")
        return result

    result.update(summarize_dbdiff_report(report))
    if result["identical"]:
        emit("dbdiff", f"identical; report written to {report_path}")
    else:
        emit(
            "dbdiff",
            (
                f"differs ({result['differing_table_count']} content table(s)); "
                f"report written to {report_path}"
            ),
        )
    return result


def write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def cleanup(paths: RunPaths, *, cleanup_db: bool, cleanup_slug: bool) -> None:
    if cleanup_db and paths.created_db_dir:
        shutil.rmtree(paths.db_dir, ignore_errors=True)
        emit("cleanup", f"removed {paths.db_dir}")
    if cleanup_slug and paths.created_slug_dir and paths.slug_dir is not None:
        shutil.rmtree(paths.slug_dir, ignore_errors=True)
        emit("cleanup", f"removed {paths.slug_dir}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run reg-meta-build build-db with timestamped logs and post-build checks."
    )
    parser.add_argument(
        "--input-dir", required=True, help="Real or overlay input_data root."
    )
    parser.add_argument(
        "--db-dir",
        default=None,
        help="Scratch DB output dir. Default: mkdtemp in --tmp-dir.",
    )
    parser.add_argument(
        "--slug-dir",
        default=None,
        help="Slug TOML dir. Default: copied scratch slug dir.",
    )
    parser.add_argument(
        "--use-repo-slug-dir",
        action="store_true",
        help="Let build-db use the repo slug dir directly.",
    )
    parser.add_argument(
        "--providers",
        default=None,
        help="Optional build-db --providers subset. Omit for full global build.",
    )
    parser.add_argument(
        "--prestage-cache",
        default=None,
        help=(
            "SCB value prestage cache path. Default: "
            "<tmp-dir>/regmeta-build-prestage/scb-value-prestage.sqlite when scb is built."
        ),
    )
    parser.add_argument(
        "--no-prestage-cache",
        action="store_true",
        help="Do not pass an SCB value prestage cache to build-db.",
    )
    parser.add_argument(
        "--refresh-prestage-cache",
        action="store_true",
        help="Force rebuilding the SCB value prestage cache before using it.",
    )
    parser.add_argument(
        "--no-validate", action="store_true", help="Pass build-db --no-validate."
    )
    parser.add_argument(
        "--no-timing", action="store_true", help="Do not pass build-db --timing."
    )
    parser.add_argument(
        "--dbdiff-against",
        default=None,
        help="Optional baseline reg_meta.db to compare against after a successful build.",
    )
    parser.add_argument(
        "--dbdiff-json",
        default=None,
        help="Full dbdiff JSON report path. Default: alongside log.",
    )
    parser.add_argument(
        "--dbdiff-sample-rows",
        type=int,
        default=10,
        help="Max differing rows per direction per table in the dbdiff report.",
    )
    parser.add_argument(
        "--dbdiff-no-default-ignore",
        action="store_true",
        help="Pass dbdiff --no-default-ignore.",
    )
    parser.add_argument(
        "--tmp-dir",
        default=os.environ.get("TMPDIR", "/tmp"),
        help="Parent for scratch dirs/logs.",
    )
    parser.add_argument(
        "--slug", default=None, help="Stable name segment for scratch dirs/logs."
    )
    parser.add_argument(
        "--log", default=None, help="Timestamped log path. Default: /tmp/<slug>.log."
    )
    parser.add_argument(
        "--summary", default=None, help="Summary JSON path. Default: alongside log."
    )
    parser.add_argument(
        "--quiet-seconds",
        type=int,
        default=300,
        help="Emit health if the build is silent this long.",
    )
    parser.add_argument(
        "--cleanup-on-success",
        action="store_true",
        help="Remove scratch DB and copied slugs after checks pass.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    root = repo_root()
    paths = prepare_paths(args, root)
    cmd = build_command(args, paths)
    started = now_stamp()
    rc = 130
    checks: dict[str, Any] | None = None
    dbdiff: dict[str, Any] | None = None
    try:
        rc = run_build(cmd, paths, args.quiet_seconds)
        db_path = paths.db_dir / "reg_meta.db"
        if rc == 0:
            emit("check", f"running SQLite checks on {db_path}")
            checks = sqlite_checks(db_path)
            emit("check", f"PRAGMA integrity_check: {checks['integrity_check']}")
            emit(
                "check",
                f"PRAGMA foreign_key_check rows: {checks['foreign_key_violations']}",
            )
            for table, count in checks["table_counts"].items():
                emit("count", f"{table}: {count:,}")
            if checks["integrity_check"] != "ok" or checks["foreign_key_violations"]:
                rc = 20
        if rc == 0 and args.dbdiff_against:
            assert paths.dbdiff_path is not None
            dbdiff = run_dbdiff(build_dbdiff_command(args, db_path), paths.dbdiff_path)
            if dbdiff.get("return_code") == 1:
                rc = 30
            elif dbdiff.get("return_code") != 0:
                rc = 31
    except SigtermReceived:
        rc = 143
    except KeyboardInterrupt:
        rc = 130
    finally:
        payload = {
            "command": cmd,
            "started": started,
            "finished": now_stamp(),
            "return_code": rc,
            "db_dir": str(paths.db_dir),
            "slug_dir": str(paths.slug_dir) if paths.slug_dir else None,
            "prestage_cache": (
                str(paths.prestage_cache) if paths.prestage_cache else None
            ),
            "log_path": str(paths.log_path),
            "checks": checks,
            "dbdiff": dbdiff,
        }
        write_summary(paths.summary_path, payload)
        emit("summary", str(paths.summary_path))
        if rc == 0 and args.cleanup_on_success:
            cleanup(paths, cleanup_db=True, cleanup_slug=True)
        elif paths.created_slug_dir:
            emit("scratch", f"copied slug dir kept at {paths.slug_dir}")
        emit("done", f"rc={rc}")
    if rc < 0:
        return 128 + abs(rc)
    if rc > 255:
        return 1
    return rc


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    raise SystemExit(main())
