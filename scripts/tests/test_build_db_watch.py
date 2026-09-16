from __future__ import annotations

import json
import sqlite3
import subprocess
import time
from argparse import Namespace
from typing import TYPE_CHECKING, ClassVar

import pytest

from conftest import load_scripts_module

if TYPE_CHECKING:
    from pathlib import Path

build_db_watch = load_scripts_module("build_db_watch")


def test_is_milestone_keeps_progress_and_suppresses_ok_lines() -> None:
    assert build_db_watch.is_milestone(
        "Importing Vardemangder.csv (this may take a while)..."
    )
    assert build_db_watch.is_milestone("  ...100,000,000 rows read")
    assert build_db_watch.is_milestone("[timing] scb:coalesce_variable_states: 612.3s")
    assert build_db_watch.is_milestone("[FAIL] foreign_key_check returned rows")
    assert not build_db_watch.is_milestone("[OK] value_code.mapping_count non-negative")
    assert not build_db_watch.is_milestone("")


def test_build_command_defaults_to_timing_and_copied_slug_dir(tmp_path: Path) -> None:
    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=tmp_path / "slugs",
        prestage_cache=tmp_path / "prestage.sqlite",
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=True,
        created_slug_dir=True,
    )
    args = Namespace(
        input_dir="/seed/input_data",
        input_bundle=None,
        input_commit=None,
        input_manifest_sha256=None,
        no_validate=False,
        no_timing=False,
        providers=None,
        refresh_prestage_cache=False,
        dbdiff_against=None,
    )

    cmd = build_db_watch.build_command(args, paths)

    assert cmd[:5] == ["uv", "run", "reg-meta-build", "--db", str(paths.db_dir)]
    assert "--timing" in cmd
    assert "--slug-dir" in cmd
    assert str(paths.slug_dir) in cmd
    assert "--providers" not in cmd
    assert "--scb-value-prestage-cache" in cmd
    assert str(paths.prestage_cache) in cmd


def test_build_command_threads_optional_flags(tmp_path: Path) -> None:
    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=tmp_path / "prestage.sqlite",
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )
    args = Namespace(
        input_dir="/seed/input_data",
        input_bundle=None,
        input_commit=None,
        input_manifest_sha256=None,
        no_validate=True,
        no_timing=True,
        providers="scb,sos",
        refresh_prestage_cache=True,
        dbdiff_against=None,
    )

    cmd = build_db_watch.build_command(args, paths)

    assert "--no-validate" in cmd
    assert "--timing" not in cmd
    assert "--slug-dir" not in cmd
    assert "--providers" in cmd
    assert "--refresh-scb-value-prestage-cache" in cmd


def test_build_command_omits_prestage_when_path_is_none(tmp_path: Path) -> None:
    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )
    args = Namespace(
        input_dir="/seed/input_data",
        input_bundle=None,
        input_commit=None,
        input_manifest_sha256=None,
        no_validate=False,
        no_timing=True,
        providers="sos",
        refresh_prestage_cache=False,
        dbdiff_against=None,
    )

    cmd = build_db_watch.build_command(args, paths)

    assert "--scb-value-prestage-cache" not in cmd


def test_build_command_forwards_scb_trace_selection(tmp_path: Path) -> None:
    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )
    args = Namespace(
        input_dir="/seed/input_data",
        input_bundle=None,
        input_commit=None,
        input_manifest_sha256=None,
        no_validate=False,
        no_timing=False,
        providers="scb",
        trace_scb_cvids="421800,590946",
        refresh_prestage_cache=False,
        dbdiff_against=None,
    )

    cmd = build_db_watch.build_command(args, paths)

    trace_index = cmd.index("--trace-scb-cvids")
    assert cmd[trace_index + 1] == "421800,590946"


def test_trace_selection_refuses_cleanup_that_would_delete_result() -> None:
    with pytest.raises(SystemExit):
        build_db_watch.parse_args(
            [
                "--input-dir",
                "/seed/input_data",
                "--trace-scb-cvids",
                "421800",
                "--cleanup-on-success",
            ]
        )


def test_trace_summary_retains_build_result_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=True,
        created_slug_dir=False,
    )
    paths.db_dir.mkdir()

    def fake_run_build(
        _cmd: list[str], run_paths: build_db_watch.RunPaths, _quiet_seconds: int
    ) -> int:
        build_db_watch.build_result_path(run_paths).write_text(
            json.dumps({"scb_trace": {"selection": {"native_cvids": [421800]}}}),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(build_db_watch, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(build_db_watch, "prepare_paths", lambda _args, _root: paths)
    monkeypatch.setattr(build_db_watch, "run_build", fake_run_build)
    monkeypatch.setattr(
        build_db_watch,
        "sqlite_checks",
        lambda _path: {
            "integrity_check": "ok",
            "foreign_key_violations": 0,
            "table_counts": {},
        },
    )

    assert (
        build_db_watch.main(
            [
                "--input-dir",
                "/seed/input_data",
                "--trace-scb-cvids",
                "421800",
                "--no-prestage-cache",
                "--summary",
                str(paths.summary_path),
            ]
        )
        == 0
    )
    summary = json.loads(paths.summary_path.read_text(encoding="utf-8"))
    assert summary["build_result"] == str(build_db_watch.build_result_path(paths))
    assert build_db_watch.build_result_path(paths).is_file()


def test_build_command_threads_complete_bundle_without_slug_override(
    tmp_path: Path,
) -> None:
    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )
    args = Namespace(
        input_dir=None,
        input_bundle="/host/catalog-inputs/bundle",
        input_commit="a" * 40,
        input_manifest_sha256="b" * 64,
        no_validate=False,
        no_timing=False,
        providers=None,
        refresh_prestage_cache=False,
        dbdiff_against=None,
    )

    cmd = build_db_watch.build_command(args, paths)

    assert "--input-dir" not in cmd
    assert cmd[cmd.index("--input-bundle") + 1] == "/host/catalog-inputs/bundle"
    assert cmd[cmd.index("--input-commit") + 1] == "a" * 40
    assert cmd[cmd.index("--input-manifest-sha256") + 1] == "b" * 64
    assert "--slug-dir" not in cmd


def test_prepare_paths_keeps_bundle_outputs_and_slugs_outside_input_repo(
    tmp_path: Path,
) -> None:
    input_repo = tmp_path / "catalog-inputs"
    bundle = input_repo / "bundle"
    bundle.mkdir(parents=True)
    subprocess.run(["git", "-C", str(input_repo), "init", "-q"], check=True)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    args = build_db_watch.parse_args(
        [
            "--input-bundle",
            str(bundle),
            "--input-commit",
            "a" * 40,
            "--input-manifest-sha256",
            "b" * 64,
            "--tmp-dir",
            str(scratch),
            "--no-prestage-cache",
        ]
    )

    paths = build_db_watch.prepare_paths(args, tmp_path)

    assert paths.slug_dir is None
    assert not paths.db_dir.is_relative_to(input_repo)
    assert not paths.log_path.is_relative_to(input_repo)

    args.tmp_dir = str(input_repo)
    with pytest.raises(ValueError, match="outside the accepted input repository"):
        build_db_watch.prepare_paths(args, tmp_path)


def test_cleanup_preserves_changed_bundle_slug_workspace_in_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    db_dir = scratch / "db"
    db_dir.mkdir()
    summary_path = scratch / "build.summary.json"
    paths = build_db_watch.RunPaths(
        db_dir=db_dir,
        slug_dir=None,
        prestage_cache=None,
        log_path=scratch / "build.log",
        summary_path=summary_path,
        created_db_dir=True,
        created_slug_dir=False,
    )
    slug_workspace = scratch / "regmeta-slugs-generated"
    slug_changes = {
        "added": ["scb.auto.toml"],
        "changed": [],
        "removed": [],
    }

    def fake_run_build(
        cmd: list[str], paths: build_db_watch.RunPaths, _quiet_seconds: int
    ) -> int:
        assert cmd[cmd.index("--output") + 1] == str(
            build_db_watch.build_result_path(paths)
        )
        slug_workspace.mkdir()
        (slug_workspace / "scb.auto.toml").write_text(
            "# generated change\n", encoding="utf-8"
        )
        build_db_watch.build_result_path(paths).write_text(
            json.dumps(
                {
                    "slug_workspace": str(slug_workspace),
                    "slug_changes": slug_changes,
                }
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(build_db_watch, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(build_db_watch, "prepare_paths", lambda _args, _root: paths)
    monkeypatch.setattr(build_db_watch, "run_build", fake_run_build)
    monkeypatch.setattr(
        build_db_watch,
        "sqlite_checks",
        lambda _path: {
            "integrity_check": "ok",
            "foreign_key_violations": 0,
            "table_counts": {},
        },
    )

    assert (
        build_db_watch.main(
            [
                "--input-bundle",
                str(tmp_path / "bundle"),
                "--input-commit",
                "a" * 40,
                "--input-manifest-sha256",
                "b" * 64,
                "--tmp-dir",
                str(scratch),
                "--summary",
                str(summary_path),
                "--no-prestage-cache",
                "--cleanup-on-success",
            ]
        )
        == 0
    )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["db_dir"] == str(paths.db_dir)
    assert not paths.db_dir.exists()
    assert summary["slug_workspace"] == str(slug_workspace)
    assert summary["slug_changes"] == slug_changes
    assert (slug_workspace / "scb.auto.toml").is_file()


@pytest.mark.parametrize(
    "argv",
    [
        ["--input-bundle", "bundle"],
        [
            "--input-dir",
            "raw",
            "--input-bundle",
            "bundle",
            "--input-commit",
            "a" * 40,
            "--input-manifest-sha256",
            "b" * 64,
        ],
        [
            "--input-bundle",
            "bundle",
            "--input-commit",
            "a" * 40,
            "--input-manifest-sha256",
            "b" * 64,
            "--slug-dir",
            "slugs",
        ],
    ],
)
def test_parse_args_rejects_incomplete_or_ambiguous_bundle(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        build_db_watch.parse_args(argv)


def test_build_dbdiff_command_threads_options(tmp_path: Path) -> None:
    args = Namespace(
        dbdiff_against="/baseline/reg_meta.db",
        dbdiff_sample_rows=3,
        dbdiff_no_default_ignore=True,
    )

    cmd = build_db_watch.build_dbdiff_command(args, tmp_path / "reg_meta.db")

    assert cmd[:5] == ["uv", "run", "python", "-m", "reg_meta_build.dbdiff"]
    assert cmd[5:7] == ["/baseline/reg_meta.db", str(tmp_path / "reg_meta.db")]
    assert "--json" in cmd
    assert cmd[cmd.index("--sample-rows") + 1] == "3"
    assert cmd[-1] == "--no-default-ignore"


def test_sqlite_checks_reports_integrity_fk_and_counts(tmp_path: Path) -> None:
    db_path = tmp_path / "reg_meta.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE register(register_id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE variable(variable_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO register(register_id) VALUES (1), (2)")
        conn.execute("INSERT INTO variable(variable_id) VALUES (10)")
        conn.commit()
    finally:
        conn.close()

    result = build_db_watch.sqlite_checks(db_path)

    assert result["integrity_check"] == "ok"
    assert result["foreign_key_violations"] == 0
    assert result["table_counts"]["register"] == 2
    assert result["table_counts"]["variable"] == 1
    assert "value_code" not in result["table_counts"]


def test_format_bytes() -> None:
    assert build_db_watch.format_bytes(12) == "12 B"
    assert build_db_watch.format_bytes(1536) == "1.5 KiB"


def test_format_process_health() -> None:
    assert (
        build_db_watch.format_process_health({"cpu": "99.0%", "rss": "1.5 GiB"})
        == "cpu=99.0% rss=1.5 GiB"
    )
    assert (
        build_db_watch.format_process_health(
            {"process": "unavailable (CalledProcessError)"}
        )
        == "process=unavailable (CalledProcessError)"
    )


def test_run_build_drains_late_tail_output(monkeypatch, tmp_path: Path) -> None:
    class DelayedStdout:
        def __iter__(self):
            time.sleep(0.35)
            yield "late actionable error\n"

    class FakeProc:
        pid = 12345
        stdout = DelayedStdout()

        def poll(self):
            return 1

        def wait(self, timeout=None):
            return 1

    monkeypatch.setattr(
        build_db_watch.subprocess, "Popen", lambda *_a, **_kw: FakeProc()
    )

    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )

    rc = build_db_watch.run_build(["fake-build"], paths, quiet_seconds=999)

    assert rc == 1
    assert "late actionable error" in paths.log_path.read_text(encoding="utf-8")


def test_run_build_does_not_emit_quiet_health_when_output_is_current(
    monkeypatch, tmp_path: Path
) -> None:
    class ScriptedQueue:
        def __init__(self) -> None:
            self.items = ["still working", build_db_watch.QUEUE_EOF]

        def get(self, timeout=None):
            return self.items.pop(0)

    class FakeThread:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def start(self) -> None:
            pass

    class FakeProc:
        pid = 12345
        stdout: ClassVar[list] = []

        def poll(self):
            return None

        def wait(self, timeout=None):
            return 0

    times = iter([0.0, 2.0, 2.0, 3.0])
    events: list[tuple[str, str]] = []
    monkeypatch.setattr(build_db_watch.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(build_db_watch.queue, "Queue", ScriptedQueue)
    monkeypatch.setattr(build_db_watch.threading, "Thread", FakeThread)
    monkeypatch.setattr(
        build_db_watch.subprocess, "Popen", lambda *_a, **_kw: FakeProc()
    )
    monkeypatch.setattr(
        build_db_watch, "emit", lambda kind, message: events.append((kind, message))
    )

    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )

    rc = build_db_watch.run_build(["fake-build"], paths, quiet_seconds=1)

    assert rc == 0
    assert not any(kind == "quiet" for kind, _message in events)


def test_run_build_sigterm_path_terminates_child(monkeypatch, tmp_path: Path) -> None:
    class InterruptingQueue:
        def get(self, timeout=None):
            raise build_db_watch.SigtermReceived

        def put(self, _item):
            pass

    class FakeProc:
        pid = 12345
        stdout: ClassVar[list] = []
        terminated = False
        killed = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

        def wait(self, timeout=None):
            return 143

    proc = FakeProc()
    monkeypatch.setattr(build_db_watch.subprocess, "Popen", lambda *_a, **_kw: proc)
    monkeypatch.setattr(build_db_watch.queue, "Queue", lambda: InterruptingQueue())

    paths = build_db_watch.RunPaths(
        db_dir=tmp_path / "db",
        slug_dir=None,
        prestage_cache=None,
        log_path=tmp_path / "build.log",
        summary_path=tmp_path / "summary.json",
        created_db_dir=False,
        created_slug_dir=False,
    )

    with pytest.raises(build_db_watch.SigtermReceived):
        build_db_watch.run_build(["fake-build"], paths, quiet_seconds=999)

    assert proc.terminated is True
    assert proc.killed is False


def test_sigterm_handler_raises_cleanup_exception() -> None:
    with pytest.raises(build_db_watch.SigtermReceived):
        build_db_watch.handle_sigterm(15, None)


def test_run_dbdiff_writes_report_and_summarizes(monkeypatch, tmp_path: Path) -> None:
    report = {
        "identical": False,
        "schema": {
            "tables_only_in_a": [],
            "tables_only_in_b": [],
            "indexes_only_in_a": [],
            "indexes_only_in_b": [],
            "index_mismatches": [],
            "column_diffs": [],
        },
        "tables": [
            {"table": "register", "identical": True},
            {"table": "variable", "identical": False},
        ],
    }

    class FakeProc:
        returncode = 1
        stdout = json.dumps(report)
        stderr = ""

    def fake_run(*_args, **_kwargs):
        return FakeProc()

    monkeypatch.setattr(build_db_watch.subprocess, "run", fake_run)

    result = build_db_watch.run_dbdiff(
        ["uv", "run", "python", "-m", "reg_meta_build.dbdiff"], tmp_path / "diff.json"
    )

    assert (tmp_path / "diff.json").read_text(encoding="utf-8") == FakeProc.stdout
    assert result["return_code"] == 1
    assert result["identical"] is False
    assert result["content_differs"] is True
    assert result["differing_tables"] == ["variable"]
