"""Realistic ``mock_data_stats.json`` shapes used as scanner false-positive
regression fixtures: the bundle's PII scanner must NOT flag anything in a
clean stats export.

Mirrored from ``mock_data_wizard/tests/conftest.py`` — ``test_cli.py`` over
there still consumes them. Both packages emit / consume this shape today;
one source of truth lands when ``mock_data_wizard`` is retired in
REFACTOR_SPEC.md step 7.
"""

from __future__ import annotations

MINIMAL_STATS = {
    "contract_version": "2.0.0",
    "generated_at": "2026-03-15T10:00:00Z",
    "sources": [
        {
            "source_name": "persons.csv",
            "source_type": "file",
            "source_detail": {
                "path": "\\\\micro.intra\\projekt\\P1405$\\P1405_Data\\persons.csv"
            },
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
                    "inferred_type": "opaque",
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
    "contract_version": "2.0.0",
    "generated_at": "2026-03-15T10:00:00Z",
    "sources": [
        {
            "source_name": "pop.csv",
            "source_type": "file",
            "source_detail": {"path": "pop.csv"},
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
            "source_name": "edu.csv",
            "source_type": "file",
            "source_detail": {"path": "edu.csv"},
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
            "sources": ["pop.csv", "edu.csv"],
            "max_n_distinct": 500,
        },
        {
            "column_name": "Kon",
            "sources": ["pop.csv", "edu.csv"],
            "max_n_distinct": 2,
        },
    ],
}
