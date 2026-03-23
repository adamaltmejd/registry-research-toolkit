"""Shared fixtures for mock_data_wizard tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from regmeta.db import DDL


MINIMAL_STATS = {
    "contract_version": "1.0.0",
    "generated_at": "2026-03-15T10:00:00Z",
    "project_paths": ["\\\\micro.intra\\projekt\\P1405$\\P1405_Data"],
    "files": [
        {
            "file_name": "persons.csv",
            "relative_path": "persons.csv",
            "row_count": 1000,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 1000,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Kon",
                    "inferred_type": "categorical",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 2,
                    "stats": {"frequencies": {"1": 500, "2": 500}},
                },
                {
                    "column_name": "FodelseAr",
                    "inferred_type": "numeric",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 80,
                    "stats": {
                        "min": 1940,
                        "max": 2005,
                        "mean": 1975,
                        "sd": 15,
                        "quantiles": {
                            "p01": 1942,
                            "p05": 1948,
                            "p25": 1963,
                            "p50": 1975,
                            "p75": 1987,
                            "p95": 2002,
                            "p99": 2005,
                        },
                    },
                },
                {
                    "column_name": "Kommun",
                    "inferred_type": "categorical",
                    "nullable": True,
                    "null_count": 50,
                    "null_rate": 0.05,
                    "n_distinct": 10,
                    "stats": {
                        "frequencies": {
                            "0180": 200,
                            "1480": 150,
                            "1280": 100,
                            "0380": 80,
                            "0580": 70,
                            "0680": 60,
                            "0780": 55,
                            "0880": 50,
                            "0980": 45,
                            "1080": 40,
                        }
                    },
                },
                {
                    "column_name": "Datum",
                    "inferred_type": "date",
                    "nullable": True,
                    "null_count": 10,
                    "null_rate": 0.01,
                    "n_distinct": 365,
                    "stats": {
                        "min": "2020-01-01",
                        "max": "2023-12-31",
                        "date_format": "%Y-%m-%d",
                    },
                },
                {
                    "column_name": "Namn",
                    "inferred_type": "high_cardinality",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 950,
                    "stats": {
                        "min_length": 3,
                        "max_length": 25,
                        "mean_length": 10.5,
                    },
                },
            ],
        },
    ],
    "shared_columns": [],
}

SPINE_STATS = {
    "contract_version": "1.0.0",
    "generated_at": "2026-03-15T10:00:00Z",
    "project_paths": ["\\\\micro.intra\\projekt\\P1405$\\P1405_Data"],
    "files": [
        {
            "file_name": "pop.csv",
            "relative_path": "pop.csv",
            "row_count": 500,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 500,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Kon",
                    "inferred_type": "categorical",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 2,
                    "stats": {"frequencies": {"1": 250, "2": 250}},
                },
            ],
        },
        {
            "file_name": "edu.csv",
            "relative_path": "edu.csv",
            "row_count": 300,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 300,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Kon",
                    "inferred_type": "categorical",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 2,
                    "stats": {"frequencies": {"1": 150, "2": 150}},
                },
                {
                    "column_name": "Grade",
                    "inferred_type": "categorical",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 3,
                    "stats": {"frequencies": {"7": 100, "8": 100, "9": 100}},
                },
            ],
        },
    ],
    "shared_columns": [
        {
            "column_name": "LopNr",
            "files": ["pop.csv", "edu.csv"],
            "max_n_distinct": 500,
        },
        {
            "column_name": "Kon",
            "files": ["pop.csv", "edu.csv"],
            "max_n_distinct": 2,
        },
    ],
}

MULTI_FILE_STATS = {
    "contract_version": "1.0.0",
    "generated_at": "2026-03-15T10:00:00Z",
    "project_paths": ["\\\\micro.intra\\projekt\\P1405$\\P1405_Data"],
    "files": [
        {
            "file_name": "file_a.csv",
            "relative_path": "file_a.csv",
            "row_count": 500,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 500,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Value",
                    "inferred_type": "numeric",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 200,
                    "stats": {"min": 0, "max": 100, "mean": 50, "sd": 20},
                },
            ],
        },
        {
            "file_name": "file_b.csv",
            "relative_path": "file_b.csv",
            "row_count": 300,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 300,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Status",
                    "inferred_type": "categorical",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 3,
                    "stats": {"frequencies": {"A": 100, "B": 100, "C": 100}},
                },
            ],
        },
    ],
    "shared_columns": [
        {
            "column_name": "LopNr",
            "files": ["file_a.csv", "file_b.csv"],
            "max_n_distinct": 500,
        }
    ],
}


@pytest.fixture
def stats_path(tmp_path: Path) -> Path:
    p = tmp_path / "stats.json"
    p.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    return p


@pytest.fixture
def spine_stats_path(tmp_path: Path) -> Path:
    p = tmp_path / "stats.json"
    p.write_text(json.dumps(SPINE_STATS), encoding="utf-8")
    return p


@pytest.fixture
def multi_file_stats_path(tmp_path: Path) -> Path:
    p = tmp_path / "stats.json"
    p.write_text(json.dumps(MULTI_FILE_STATS), encoding="utf-8")
    return p


@pytest.fixture
def regmeta_db(tmp_path: Path) -> Path:
    """Build a minimal regmeta DB with one register, one variable, and value codes."""
    db_path = tmp_path / "regmeta.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(DDL)
    conn.execute(
        "INSERT INTO register (register_id, registernamn, registerrubrik, registersyfte) "
        "VALUES (1, 'TESTREG', 'Testregistret', 'Testing')"
    )
    conn.execute(
        "INSERT INTO register_variant (regvar_id, register_id, registervariantnamn, registervariantsekretess) "
        "VALUES (10, 1, 'Individer', 'Nej')"
    )
    conn.execute(
        "INSERT INTO register_version (regver_id, regvar_id, registerversionnamn) "
        "VALUES (100, 10, '2020')"
    )
    conn.execute(
        "INSERT INTO variable (register_id, var_id, variabelnamn, variabeldefinition) "
        "VALUES (1, 44, 'Kön', 'Kön enligt folkbokföring')"
    )
    conn.execute(
        "INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id, var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva) "
        "VALUES (1001, 1, 10, 100, 44, 'int', '1', 'Kön', '1')"
    )
    conn.execute("INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1001, 'Kon')")
    # Two value codes: 1=Man, 2=Kvinna
    conn.execute(
        "INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (1, '1', 'Man')"
    )
    conn.execute(
        "INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (2, '2', 'Kvinna')"
    )
    conn.execute("INSERT INTO cvid_value_code (cvid, code_id) VALUES (1001, 1)")
    conn.execute("INSERT INTO cvid_value_code (cvid, code_id) VALUES (1001, 2)")
    conn.commit()
    conn.close()
    return db_path
