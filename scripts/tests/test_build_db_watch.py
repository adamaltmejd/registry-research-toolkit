from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "build_db_watch", Path(__file__).parents[1] / "build_db_watch.py"
)
assert _SPEC and _SPEC.loader
build_db_watch = importlib.util.module_from_spec(_SPEC)
sys.modules["build_db_watch"] = build_db_watch
_SPEC.loader.exec_module(build_db_watch)


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
        no_validate=False,
        no_timing=True,
        providers="sos",
        refresh_prestage_cache=False,
        dbdiff_against=None,
    )

    cmd = build_db_watch.build_command(args, paths)

    assert "--scb-value-prestage-cache" not in cmd


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


def test_run_build_sigterm_path_terminates_child(monkeypatch, tmp_path: Path) -> None:
    class InterruptingQueue:
        def get(self, timeout=None):
            raise build_db_watch.SigtermReceived

        def put(self, _item):
            pass

    class FakeProc:
        pid = 12345
        stdout = []
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
