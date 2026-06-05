"""MONA-shape integration test: the bundle's emitted MSSQL extract
queries run end-to-end against a real SQL Server (``REFACTOR_SPEC.md`` §16).

The MONA runtime aggregates over the project's ODBC views on Statistics
Sweden's MSSQL backend. Every unit test of that path runs against DuckDB
or a hand-rolled fake cursor; nothing exercises the *T-SQL* the emitter
produces (``TOP N``, ``STEV``, ``PERCENTILE_CONT ... OVER ()``,
``HASHBYTES``, ``LEN``, ``INFORMATION_SCHEMA.COLUMNS``) against a server
that actually parses and executes it. A dialect typo in ``sql_emit`` or
``extract`` would pass every DuckDB test and only surface on MONA — where
we cannot iterate. This test closes that gap.

What it does:

1. Stands up ``mcr.microsoft.com/mssql/server`` in Docker and polls for
   readiness.
2. Creates INFORMATION_SCHEMA-shaped fixtures — plain ``CREATE TABLE`` +
   ``INSERT`` populate ``INFORMATION_SCHEMA.TABLES`` / ``.COLUMNS``
   exactly as MONA's project views do — covering every column type the
   emitter dispatches on (id, numeric, categorical, opaque, date).
3. Drives the bundle's real extract pipeline against that server via
   ``pyodbc``: ``iter_sql_source(..., conn=<live conn>)`` yields MSSQL
   handles and ``extract.process_handle`` runs every emitted query
   (column listing, distinct/null counts, the deterministic
   ``HASHBYTES`` sample, and the per-type aggregation SQL from
   ``sql_emit.queries_for_column``). It also exercises the discover-mode
   ``INFORMATION_SCHEMA.COLUMNS`` walk.
4. Asserts the round-trip produces a structurally sane
   ``mock_data_stats.json`` fragment (k-anonymity, perturbed numeric
   bounds, jittered dates, id subtype).

Gating: module-level ``pytestmark = pytest.mark.integration`` — the root
``conftest.py`` skips it unless ``--run-integration`` is passed. Docker
and pyodbc are probed *inside* the fixtures (``shutil.which`` + ``docker
info``, ``pytest.importorskip("pyodbc")``), so on a host without either
the test SKIPS cleanly rather than erroring at collection. ``pyodbc`` is
deliberately **not** a project/dev dependency: it needs system ODBC
drivers and would break ``uv sync`` / CI on hosts that lack them. The
importorskip keeps collection green without it.

    pytest --run-integration reg_monabundle/tests/test_integration_mssql.py
"""

from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from random import Random
from typing import TYPE_CHECKING, Any

import pytest
from reg_monabundle.runtime.extract import (
    _describe_columns_sql,
    process_handle,
)
from reg_monabundle.runtime.sources import iter_sql_source, sql_source, sql_table
from reg_monabundle.runtime.spec import LoadedSpec, loadedspec_from_dict

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

# SA password must satisfy the server's complexity policy or the
# container exits during init. Documented env contract for the image:
# ACCEPT_EULA=Y + MSSQL_SA_PASSWORD.
SA_PASSWORD = "regMona_Integration1!"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
CONTAINER_PORT = 11433  # host side; avoid clashing with a local 1433
SCHEMA = "dbo"
TABLE = "lisa_2018"

# One row per person; shaped so each column type has a meaningful summary
# and categorical frequencies clear the k-anonymity floor (SUPPRESS_K=10).
N_ROWS = 40


def _docker() -> str:
    path = shutil.which("docker")
    if not path:
        pytest.skip("Docker not available")
    result = subprocess.run([path, "info"], capture_output=True, timeout=10)
    if result.returncode != 0:
        pytest.skip("Docker daemon not running")
    return path


def _odbc_driver() -> str:
    """Pick an installed MS SQL ODBC driver, or skip.

    pyodbc can connect only if the host has an ODBC driver for SQL Server
    (``msodbcsql18`` / ``17``). On hosts that have pyodbc but no driver,
    skip rather than fail — the driver is a system package, not a Python
    dep we can pin.
    """
    pyodbc = pytest.importorskip("pyodbc")
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        pytest.skip("No 'ODBC Driver ... for SQL Server' installed")
    # Prefer the newest by name (…18 > …17 > 'SQL Server').
    return sorted(drivers)[-1]


def _connect(driver: str) -> Any:
    pyodbc = pytest.importorskip("pyodbc")
    conn_str = (
        f"DRIVER={{{driver}}};SERVER=127.0.0.1,{CONTAINER_PORT};"
        f"UID=sa;PWD={SA_PASSWORD};"
        # msodbcsql18 defaults to Encrypt=yes; the throwaway container has
        # a self-signed cert, so trust it explicitly.
        "Encrypt=no;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True, timeout=10)


@pytest.fixture(scope="module")
def mssql_conn(_odbc_driver_name: str) -> Iterator[Any]:
    """Run the SQL Server container, wait for readiness, yield a live conn."""
    docker = _docker()
    name = f"regmona-mssql-{uuid.uuid4().hex[:8]}"
    run = subprocess.run(
        [
            docker,
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            f"MSSQL_SA_PASSWORD={SA_PASSWORD}",
            "-p",
            f"{CONTAINER_PORT}:1433",
            MSSQL_IMAGE,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if run.returncode != 0:
        pytest.skip(f"Could not start MSSQL container: {run.stderr.strip()}")

    try:
        conn = _wait_for_server(_odbc_driver_name, timeout_s=120)
        try:
            yield conn
        finally:
            conn.close()
    finally:
        subprocess.run([docker, "stop", name], capture_output=True, timeout=60)


@pytest.fixture(scope="module")
def _odbc_driver_name() -> str:
    return _odbc_driver()


def _wait_for_server(driver: str, timeout_s: int) -> Any:
    """Poll the container until it accepts a connection (init takes seconds)."""
    pyodbc = pytest.importorskip("pyodbc")
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = _connect(driver)
            conn.cursor().execute("SELECT 1").fetchone()
            return conn
        except pyodbc.Error as exc:  # not ready yet
            last_err = exc
            time.sleep(2)
    pytest.skip(f"MSSQL never became ready within {timeout_s}s: {last_err}")


def _load_fixture_table(conn: Any) -> None:
    """Create a MONA-shaped table + rows. Populates INFORMATION_SCHEMA."""
    cur = conn.cursor()
    cur.execute(
        f"IF OBJECT_ID('{SCHEMA}.{TABLE}', 'U') IS NOT NULL DROP TABLE {SCHEMA}.{TABLE}"
    )
    cur.execute(
        f"""
        CREATE TABLE {SCHEMA}.{TABLE} (
            LopNr       BIGINT       NOT NULL,   -- id
            Lon         FLOAT        NULL,        -- numeric
            Kon         VARCHAR(1)   NOT NULL,    -- categorical
            Yrke        NVARCHAR(64) NULL,        -- opaque (free text)
            Indatum     DATE         NOT NULL     -- date
        )
        """
    )
    rows = []
    for i in range(N_ROWS):
        lopnr = 100000 + i
        lon = 25000.0 + i * 137.5
        kon = "1" if i % 2 == 0 else "2"  # 20 each -> clears SUPPRESS_K
        yrke = f"occupation_text_{i:04d}"  # high-cardinality opaque
        month = (i % 12) + 1
        indatum = f"2018-{month:02d}-15"
        rows.append((lopnr, lon, kon, yrke, indatum))
    cur.executemany(
        f"INSERT INTO {SCHEMA}.{TABLE} (LopNr, Lon, Kon, Yrke, Indatum) "
        f"VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    cur.close()


def _spec() -> LoadedSpec:
    """A LoadedSpec whose display_names match the fixture's SQL headers."""
    payload = {
        "schema_version": "2.0.0",
        "steward": "global",
        "reg_meta_version": "reg_meta/v1.0.0",
        "name": "mssql-integration",
        "sources": [
            {
                "name": TABLE,  # iter_sql_source aliases on the unqualified name
                "register_variant": "scb/lisa/_default",
                "period": 2018,
                "bindings": [
                    {
                        "variable": "scb/lisa/lopnr",
                        "type": "id",
                        "display_name": "LopNr",
                        "id_subtype": "integer",
                    },
                    {
                        "variable": "scb/lisa/lon",
                        "type": "numeric",
                        "display_name": "Lon",
                        "numeric_subtype": "double",
                    },
                    {
                        "variable": "scb/lisa/kon",
                        "type": "categorical",
                        "display_name": "Kon",
                    },
                    {
                        "variable": "scb/lisa/yrke",
                        "type": "opaque",
                        "display_name": "Yrke",
                    },
                    {
                        "variable": "scb/lisa/indatum",
                        "type": "date",
                        "display_name": "Indatum",
                        "date_format": "%Y-%m-%d",
                    },
                ],
            }
        ],
        "panels": [],
    }
    return loadedspec_from_dict(payload)


def test_emitted_extract_queries_run_against_real_mssql(mssql_conn: Any) -> None:
    """Drive the bundle's real MSSQL extract path end-to-end.

    Uses ``iter_sql_source(conn=<live pyodbc conn>)`` so the live
    connection is injected (bypassing DSN/Trusted_Connection
    auth, which doesn't fit a throwaway SA container), then runs
    ``process_handle`` — the same per-handle pipeline ``run_extract_typed``
    invokes — so every emitted T-SQL query executes server-side.
    """
    _load_fixture_table(mssql_conn)
    spec = _spec()

    src = sql_source("integration", tables=[sql_table(f"{SCHEMA}.{TABLE}")])
    handles = list(iter_sql_source(src, conn=mssql_conn))
    assert len(handles) == 1
    handle = handles[0]
    assert handle.dialect == "mssql"
    assert handle.table == f"[{SCHEMA}].[{TABLE}]"

    result = process_handle(handle, Random(0), spec, classifier_seed=0)

    assert result["source_name"] == TABLE
    assert result["source_type"] == "sql"
    assert result["row_count"] == N_ROWS
    by_name = {c["column_name"]: c for c in result["columns"]}
    assert set(by_name) == {"LopNr", "Lon", "Kon", "Yrke", "Indatum"}

    # id: distinct count is exact (no perturbation on counts), subtype honored.
    lopnr = by_name["LopNr"]
    assert lopnr["inferred_type"] == "id"
    assert lopnr["n_distinct"] == N_ROWS
    assert lopnr["stats"]["id_subtype"] == "integer"

    # numeric: AVG/STEV/MIN/MAX + PERCENTILE_CONT OVER () executed; bounds
    # are perturbed but stay ordered and span the seeded range.
    lon = by_name["Lon"]
    assert lon["inferred_type"] == "numeric"
    assert lon["stats"]["numeric_subtype"] == "double"
    assert lon["stats"]["min"] <= lon["stats"]["max"]
    assert lon["stats"]["mean"] is not None
    quant = lon["stats"]["quantiles"]
    assert quant["p01"] <= quant["p50"] <= quant["p99"]

    # categorical: GROUP BY freqs survive k-anonymity (20 each > SUPPRESS_K).
    kon = by_name["Kon"]
    assert kon["inferred_type"] == "categorical"
    assert kon["stats"]["frequencies"] == {"1": 20, "2": 20}

    # opaque: LEN()-based length stats (every value is the same length).
    yrke = by_name["Yrke"]
    assert yrke["inferred_type"] == "opaque"
    assert yrke["stats"]["min_length"] == len("occupation_text_0000")
    assert yrke["stats"]["max_length"] == len("occupation_text_0000")

    # date: MIN/MAX parsed + jittered; ISO output, ordered.
    indatum = by_name["Indatum"]
    assert indatum["inferred_type"] == "date"
    assert indatum["stats"]["min"] <= indatum["stats"]["max"]
    assert indatum["stats"]["date_format"] == "%Y-%m-%d"


def test_information_schema_columns_walk(mssql_conn: Any) -> None:
    """The discover-mode ``INFORMATION_SCHEMA.COLUMNS`` query round-trips.

    ``_describe_columns_sql`` is the parameterized catalog read the
    discover walk uses on SQL sources. Run it against the real catalog so
    the column metadata shape (name / sql_type / nullable, ordinal order)
    is exercised, not just mocked.
    """
    _load_fixture_table(mssql_conn)
    cols = _describe_columns_sql(mssql_conn, f"{SCHEMA}.{TABLE}")
    names = [c["name"] for c in cols]
    assert names == ["LopNr", "Lon", "Kon", "Yrke", "Indatum"]
    by_name = {c["name"]: c for c in cols}
    assert by_name["LopNr"]["nullable"] is False
    assert by_name["Lon"]["nullable"] is True
    assert by_name["Indatum"]["sql_type"] == "date"
