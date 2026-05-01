"""Integration tests that execute the generated R script end-to-end.

These tests require R with the `data.table`, `jsonlite`, `DBI`, and `RSQLite`
packages available. They're skipped when Rscript or any package is missing.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from mock_data_wizard.script_gen import generate_script
from mock_data_wizard.stats import parse_stats


def _have_rscript_and_packages() -> bool:
    if shutil.which("Rscript") is None:
        return False
    probe = subprocess.run(
        [
            "Rscript",
            "-e",
            "quit(status = as.integer(!(requireNamespace('data.table', quietly=TRUE) && "
            "requireNamespace('jsonlite', quietly=TRUE) && "
            "requireNamespace('DBI', quietly=TRUE) && "
            "requireNamespace('RSQLite', quietly=TRUE))))",
        ],
        capture_output=True,
    )
    return probe.returncode == 0


requires_r = pytest.mark.skipif(
    not _have_rscript_and_packages(),
    reason="Rscript with data.table/jsonlite/DBI/RSQLite not available",
)


def _patch_script_for_local(
    r_path: Path, sources_block: str, sqlite_db: Path | None = None
) -> Path:
    """Replace the generator-emitted SOURCES block with a local test block,
    un-Windowsify any test paths, and inject an RSQLite connect hook."""
    content = r_path.read_text()
    # Fix the generator's backslash-windowsification of the probe path so
    # the file_source() path resolves on macOS/Linux test runs.
    content = content.replace(
        '"\\\\tmp\\\\mdw_probe\\\\_"', '"/tmp/mdw_probe_not_used"'
    )
    # Replace the default SOURCES block
    import re

    content = re.sub(
        r"SOURCES <- list\(.*?\n\)",
        sources_block.rstrip(),
        content,
        count=1,
        flags=re.DOTALL,
    )
    if sqlite_db is not None:
        hook = (
            f"\noptions(mdw.sql_connect = function(src) "
            f'DBI::dbConnect(RSQLite::SQLite(), dbname = "{sqlite_db}"))\n'
        )
        # Inject right after the SOURCES block
        content = content.replace(
            sources_block.rstrip(), sources_block.rstrip() + hook, 1
        )
    r_path.write_text(content)
    return r_path


def _setup_sqlite(db_path: Path) -> None:
    """Create a sqlite DB with two tables for tests."""
    setup_r = f"""
        suppressMessages({{library(DBI); library(RSQLite)}})
        con <- dbConnect(SQLite(), "{db_path}")
        dbWriteTable(con, "population", data.frame(
          LopNr = 1:100,
          Kon = rep(c(1,2), 50),
          FodelseAr = rep(1950:1999, 2)
        ))
        dbWriteTable(con, "employment", data.frame(
          LopNr = 1:100,
          salary = rep(30000, 100) + (1:100) * 100
        ))
        dbDisconnect(con)
    """
    r = subprocess.run(["Rscript", "-e", setup_r], capture_output=True, text=True)
    assert r.returncode == 0, f"sqlite setup failed: {r.stderr}"


@requires_r
def test_sql_source_end_to_end(tmp_path: Path):
    """Generated script pulls from RSQLite, writes valid v2 stats.json."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"
    stats_out = tmp_path / "stats.json"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    sources_block = """SOURCES <- list(
  sql_source(
    dsn = "testdb",
    tables = c("population", "employment")
  )
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    # Run in tmp_path so OUTPUT_PATH (getwd()/stats.json) lands predictably
    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"R script failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert stats_out.exists(), f"stats.json missing. stderr:\n{result.stderr}"

    # Parse via the real Python parser — round-trip through v2 contract.
    stats = parse_stats(stats_out)
    assert stats.contract_version.startswith("2.")
    names = {s.source_name for s in stats.sources}
    assert names == {"population", "employment"}
    # Both are SQL sources
    assert all(s.source_type == "sql" for s in stats.sources)
    # Shared column LopNr should be detected across the two tables
    shared = {s.column_name for s in stats.shared_columns}
    assert "LopNr" in shared


@requires_r
def test_discovery_mode_writes_timestamped_sources_file(tmp_path: Path):
    """Discovery mode writes SOURCES to mdw_sources_<timestamp>.R."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"
    stats_out = tmp_path / "stats.json"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    sources_block = """SOURCES <- list(
  sql_source(dsn = "testdb")
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"R script failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert not stats_out.exists(), "discovery mode must not write stats.json"
    # Console output is short and points at the timestamped file
    assert "DISCOVERY MODE" in result.stdout
    assert "mdw_sources_" in result.stdout

    # Exactly one mdw_sources_*.R was written
    written = sorted(tmp_path.glob("mdw_sources_*.R"))
    assert len(written) == 1
    suggestion_text = written[0].read_text()
    assert "population" in suggestion_text
    assert "employment" in suggestion_text
    assert "sql_source(" in suggestion_text
    assert "SOURCES <- list(" in suggestion_text


@requires_r
def test_auto_load_mdw_sources_file_on_rerun(tmp_path: Path):
    """Re-running with an edited mdw_sources file skips discovery and uses it."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"
    stats_out = tmp_path / "stats.json"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    # Start with a SOURCES block that would trigger discovery...
    sources_block = """SOURCES <- list(
  sql_source(dsn = "testdb")
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    # ...but also pre-write an mdw_sources_*.R with explicit tables.
    # The script should pick up this file instead of running discovery.
    edited_sources = tmp_path / "mdw_sources_20260101_000000.R"
    edited_sources.write_text(
        'SOURCES <- list(sql_source(dsn = "testdb", tables = c("population")))\n'
    )

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"R script failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    # Discovery did NOT run — the edited file was used
    assert "DISCOVERY MODE" not in result.stdout
    # stats.json was produced, and only 'population' is in it
    assert stats_out.exists()
    stats = parse_stats(stats_out)
    names = {s.source_name for s in stats.sources}
    assert names == {"population"}

    # No NEW mdw_sources file was written (we had one; it's still the only one)
    written = sorted(tmp_path.glob("mdw_sources_*.R"))
    assert len(written) == 1
    assert written[0].name == "mdw_sources_20260101_000000.R"


@requires_r
def test_loaded_file_still_un_narrowed_errors(tmp_path: Path):
    """If the loaded mdw_sources file still has discovery-triggering gaps, error."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    sources_block = """SOURCES <- list(
  sql_source(dsn = "testdb")
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    # Existing mdw_sources file that's STILL in discovery state (no tables)
    edited_sources = tmp_path / "mdw_sources_20260101_000000.R"
    edited_sources.write_text('SOURCES <- list(sql_source(dsn = "testdb"))\n')

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Edit that file to narrow" in combined or "narrow" in combined
    # No new mdw_sources file was created
    written = sorted(tmp_path.glob("mdw_sources_*.R"))
    assert len(written) == 1


@requires_r
def test_latest_mdw_sources_file_wins(tmp_path: Path):
    """When multiple mdw_sources files exist, the latest by filename is used."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"
    stats_out = tmp_path / "stats.json"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    sources_block = """SOURCES <- list(
  sql_source(dsn = "testdb")
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    # Two files; newer one selects only 'employment', older one selects only 'population'
    (tmp_path / "mdw_sources_20260101_000000.R").write_text(
        'SOURCES <- list(sql_source(dsn = "testdb", tables = c("population")))\n'
    )
    (tmp_path / "mdw_sources_20260423_120000.R").write_text(
        'SOURCES <- list(sql_source(dsn = "testdb", tables = c("employment")))\n'
    )

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    stats = parse_stats(stats_out)
    names = {s.source_name for s in stats.sources}
    assert names == {"employment"}  # latest file won


@requires_r
def test_sql_where_and_queries_escape_hatch(tmp_path: Path):
    """WHERE clause narrows the pull; queries= supports custom SELECT."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"
    stats_out = tmp_path / "stats.json"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    sources_block = """SOURCES <- list(
  sql_source(
    dsn = "testdb",
    tables = c("population"),
    where = "FodelseAr >= 1980"
  ),
  sql_source(
    dsn = "testdb",
    queries = c(
      joined = "SELECT p.LopNr, p.Kon, e.salary FROM population p JOIN employment e ON p.LopNr = e.LopNr"
    )
  )
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    stats = parse_stats(stats_out)
    names = {s.source_name for s in stats.sources}
    assert names == {"population", "joined"}

    pop = next(s for s in stats.sources if s.source_name == "population")
    # Population filtered to FodelseAr >= 1980 — 20/50 years remain, 100*0.4=40 rows
    assert pop.row_count == 40
    assert "FodelseAr >= 1980" in pop.source_detail["query"]

    joined = next(s for s in stats.sources if s.source_name == "joined")
    joined_cols = {c.column_name for c in joined.columns}
    assert joined_cols == {"LopNr", "Kon", "salary"}


@requires_r
def test_all_flag_pulls_everything_without_discovery(tmp_path: Path):
    """`sql_source(dsn=..., all = TRUE)` skips discovery and pulls every view."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"
    stats_out = tmp_path / "stats.json"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    sources_block = """SOURCES <- list(
  sql_source(dsn = "testdb", all = TRUE)
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"R script failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    # Discovery did NOT run — processing went straight through
    assert "DISCOVERY MODE" not in result.stdout
    # Both tables in the sqlite DB ended up in the stats
    stats = parse_stats(stats_out)
    names = {s.source_name for s in stats.sources}
    assert names == {"population", "employment"}
    # No mdw_sources_*.R was written
    assert not list(tmp_path.glob("mdw_sources_*.R"))


@requires_r
def test_alias_collision_raises(tmp_path: Path):
    """Two tables with the same bare name across schemas must force aliasing."""
    r_path = tmp_path / "extract.R"
    sqlite_db = tmp_path / "test.sqlite"

    generate_script(["/tmp/mdw_probe_not_used"], r_path)
    _setup_sqlite(sqlite_db)

    # Even though RSQLite has no schemas, the resolve_table_aliases() check
    # triggers if the user writes two fully-qualified names with the same
    # trailing segment.
    sources_block = """SOURCES <- list(
  sql_source(
    dsn = "testdb",
    tables = c("dbo.population", "P1105.population")
  )
)"""
    _patch_script_for_local(r_path, sources_block, sqlite_db)

    result = subprocess.run(
        ["Rscript", str(r_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "Ambiguous" in result.stderr or "Ambiguous" in result.stdout
