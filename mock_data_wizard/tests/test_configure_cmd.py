"""Tests for the local ``configure`` step (discover.json -> mdw_config.json).

The unit tests cover the name-pattern classifier and the file IO around
``configure_from_discover``. CLI integration is covered via ``argparse``
parsing in test_cli.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.configure import (
    _classify_name,
    build_config,
    configure_from_discover,
)


# -- _classify_name patterns ---------------------------------------------


@pytest.mark.parametrize(
    "name, expected",
    [
        ("lopnr", "id"),
        ("LopNr_PersonNr", "id"),
        ("Kommun", "categorical"),
        ("ssyk_4digit", "categorical"),
        ("Sun2000Inr", "categorical"),
        ("Sun2020Inr", "categorical"),
        ("FodelseLand", "categorical"),
        ("MedborgarLand", "categorical"),
        ("CivilStand", "categorical"),
        ("Lan", "categorical"),
        ("Yrke_KOD", "categorical"),
        ("Kon", "categorical"),
        ("InDatum", "date"),
        ("Tidpunkt", "date"),
        ("AR", "numeric"),
        ("Belopp", "numeric"),
        ("InkomstSumma", "numeric"),
        ("PensionAr", "numeric"),
        ("Erstattning", "numeric"),
        ("Alder", "numeric"),
        ("RandomString", "high_cardinality"),
        ("FelPersonNr", "high_cardinality"),
    ],
)
def test_classify_name_known_patterns(name: str, expected: str):
    assert _classify_name(name) == expected


# -- build_config from discover payload ----------------------------------


def test_build_config_routes_columns_per_source():
    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int", "nullable": False},
                    {"name": "Kommun", "sql_type": "char(4)", "nullable": True},
                    {"name": "InkomstSumma", "sql_type": "decimal", "nullable": True},
                    {"name": "WhateverElse", "sql_type": "varchar", "nullable": True},
                ],
            }
        ],
    }
    out = build_config(discover)
    assert out["contract_version"] == "mdw-config-1.0.0"
    cols = out["column_types"]["lisa_2018"]
    assert cols["LopNr"] == {"type": "id"}
    assert cols["Kommun"] == {"type": "categorical"}
    assert cols["InkomstSumma"] == {"type": "numeric"}
    assert cols["WhateverElse"] == {"type": "high_cardinality"}


# -- configure_from_discover end-to-end ---------------------------------


def _write_discover(tmp_path: Path, payload: dict) -> Path:
    p = tmp_path / "discover.json"
    full = {"contract_version": "discover-1.0.0", **payload}
    p.write_text(json.dumps(full), encoding="utf-8")
    return p


def test_configure_from_discover_writes_next_to_input(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        {
            "sources": [
                {"source_name": "a", "columns": [{"name": "lopnr"}, {"name": "x"}]},
            ]
        },
    )
    out = configure_from_discover(discover_path)
    assert out == tmp_path / "mdw_config.json"
    assert out.exists()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}
    assert payload["column_types"]["a"]["x"] == {"type": "high_cardinality"}


def test_configure_from_discover_respects_explicit_output(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "x"}]}]},
    )
    target = tmp_path / "subdir" / "config.json"
    out = configure_from_discover(discover_path, output_path=target)
    assert out == target
    assert target.exists()


def test_configure_refuses_to_overwrite(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "x"}]}]},
    )
    (tmp_path / "mdw_config.json").write_text("{}", encoding="utf-8")
    with pytest.raises(FileExistsError, match="already exists"):
        configure_from_discover(discover_path)


def test_configure_overwrite_replaces_existing(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "lopnr"}]}]},
    )
    target = tmp_path / "mdw_config.json"
    target.write_text("{}", encoding="utf-8")
    configure_from_discover(discover_path, overwrite=True)
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


def test_configure_raises_when_discover_has_no_sources(tmp_path: Path):
    discover_path = _write_discover(tmp_path, {"sources": []})
    with pytest.raises(ValueError, match="no sources"):
        configure_from_discover(discover_path)


def test_configure_raises_when_input_missing(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        configure_from_discover(tmp_path / "nope.json")


def test_configure_rejects_stats_json_with_actionable_error(tmp_path: Path):
    """Pointing configure at stats.json (or anything without a discover
    contract_version) must fail with a controlled message, not KeyError."""
    bad = tmp_path / "stats.json"
    bad.write_text(
        json.dumps(
            {
                "contract_version": "2.0.0",
                "sources": [{"source_name": "a", "columns": []}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="discover.json"):
        configure_from_discover(bad)


def test_configure_rejects_payload_missing_columns_key(tmp_path: Path):
    """Partial discover file: a column entry without 'name'."""
    p = tmp_path / "discover.json"
    p.write_text(
        json.dumps(
            {
                "contract_version": "discover-1.0.0",
                "sources": [{"source_name": "a", "columns": [{"sql_type": "int"}]}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="'name' key"):
        configure_from_discover(p)


def test_configure_rejects_source_missing_columns_field(tmp_path: Path):
    """A truncated discover.json with no 'columns' field must fail at the
    contract boundary, not silently emit an incomplete mdw_config.json."""
    p = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a"}]},
    )
    with pytest.raises(ValueError, match="missing 'columns'"):
        configure_from_discover(p)


def test_configure_rejects_duplicate_source_names(tmp_path: Path):
    """Two sources sharing source_name would silently drop one source's
    column map (column_types is keyed by source_name)."""
    p = _write_discover(
        tmp_path,
        {
            "sources": [
                {"source_name": "data.csv", "columns": [{"name": "LopNr"}]},
                {"source_name": "data.csv", "columns": [{"name": "Belopp"}]},
            ]
        },
    )
    with pytest.raises(ValueError, match="duplicate source_name"):
        configure_from_discover(p)


def test_cli_configure_invokes_module(tmp_path: Path):
    """Smoke test the CLI dispatch through to configure_from_discover."""
    from mock_data_wizard.cli import main as cli_main

    discover_path = _write_discover(
        tmp_path,
        {
            "sources": [
                {
                    "source_name": "lisa_2018",
                    "columns": [{"name": "LopNr"}, {"name": "Kommun"}],
                }
            ]
        },
    )
    rc = cli_main(["configure", str(discover_path)])
    assert rc == 0
    target = tmp_path / "mdw_config.json"
    assert target.exists()
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["column_types"]["lisa_2018"]["LopNr"] == {"type": "id"}
    assert payload["column_types"]["lisa_2018"]["Kommun"] == {"type": "categorical"}


def test_cli_configure_refuses_overwrite_without_flag(tmp_path: Path):
    from mock_data_wizard.cli import main as cli_main

    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "x"}]}]},
    )
    (tmp_path / "mdw_config.json").write_text("{}", encoding="utf-8")
    rc = cli_main(["configure", str(discover_path)])
    assert rc == 1


def test_cli_configure_overwrite_flag(tmp_path: Path):
    from mock_data_wizard.cli import main as cli_main

    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "lopnr"}]}]},
    )
    target = tmp_path / "mdw_config.json"
    target.write_text("{}", encoding="utf-8")
    rc = cli_main(["configure", str(discover_path), "--overwrite"])
    assert rc == 0
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


def test_build_config_carries_source_year_from_discover():
    """#24: discover.json's source_detail.year flows into mdw_config's
    sources block. Users edit there to fix mis-detections."""
    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "source_detail": {"path": "/x/lisa_2018.csv", "year": 2018},
                "columns": [{"name": "LopNr"}],
            },
            {
                "source_name": "no_year_table",
                "source_detail": {"path": "/x/no_year_table.csv"},
                "columns": [{"name": "LopNr"}],
            },
        ],
    }
    out = build_config(discover)
    assert out["sources"] == {"lisa_2018": {"year": 2018}}
    assert "no_year_table" not in out["sources"]


def test_build_config_omits_sources_block_when_no_years():
    discover = {
        "sources": [
            {
                "source_name": "x",
                "source_detail": {"path": "/p/x.csv"},
                "columns": [{"name": "a"}],
            }
        ]
    }
    out = build_config(discover)
    # Empty sources block is omitted to keep the config tidy.
    assert "sources" not in out


def test_configure_output_is_valid_mdw_config(tmp_path: Path):
    """Round-trip: configure output must parse cleanly via load_config."""
    from mock_data_wizard.config import parse_config

    discover_path = _write_discover(
        tmp_path,
        {
            "sources": [
                {
                    "source_name": "lisa_2018",
                    "columns": [
                        {"name": "LopNr"},
                        {"name": "Kommun"},
                        {"name": "InkomstSumma"},
                    ],
                }
            ]
        },
    )
    out = configure_from_discover(discover_path)
    payload = json.loads(out.read_text(encoding="utf-8"))
    cfg = parse_config(payload)
    assert cfg.lookup_type("lisa_2018", "LopNr").type == "id"
    assert cfg.lookup_type("lisa_2018", "Kommun").type == "categorical"
    assert cfg.lookup_type("lisa_2018", "InkomstSumma").type == "numeric"
