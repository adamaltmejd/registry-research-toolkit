"""Tests for the local ``configure`` step (mdw_step1_discovery.json -> mdw_step2_config.json).

Cover the per-column classifier, the file IO around
``configure_from_discover``, and the regmeta classification-lookup
hook (mocked so the test suite doesn't depend on a live regmeta DB).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard import configure as cfg_mod
from mock_data_wizard.configure import (
    _classify,
    _sql_type_kind,
    build_config,
    configure_from_discover,
)


# -- _sql_type_kind --------------------------------------------------------


@pytest.mark.parametrize(
    "sql_type, expected",
    [
        ("BIGINT", "numeric"),
        ("bigint", "numeric"),
        ("Integer", "numeric"),
        ("DECIMAL(18,2)", "numeric"),
        ("numeric(10,4)", "numeric"),
        ("DOUBLE", "numeric"),
        ("FLOAT", "numeric"),
        ("MONEY", "numeric"),
        ("DATE", "date"),
        ("TIMESTAMP", "date"),
        ("TIMESTAMP WITH TIME ZONE", "date"),
        ("datetime2", "date"),
        ("VARCHAR", None),
        ("char(4)", None),
        ("nvarchar(255)", None),
        ("text", None),
        ("", None),
        ("   ", None),  # whitespace-only must not raise IndexError
        ("\t\n", None),
        (None, None),
    ],
)
def test_sql_type_kind(sql_type: str | None, expected: str | None):
    assert _sql_type_kind(sql_type) == expected


# -- _classify priority chain ---------------------------------------------


_REGMETA_CLASSIFIED = cfg_mod.RegmetaSignal(
    datatyp_kind=None, classification_short_name="SUN2000"
)
_REGMETA_VALUE_CODES = cfg_mod.RegmetaSignal(
    datatyp_kind="numeric", classification_short_name=None, has_value_codes=True
)
_REGMETA_TEXT_NO_EVIDENCE = cfg_mod.RegmetaSignal(
    datatyp_kind=None, classification_short_name=None
)
_REGMETA_NUMERIC = cfg_mod.RegmetaSignal(
    datatyp_kind="numeric", classification_short_name=None
)
_REGMETA_DATE = cfg_mod.RegmetaSignal(
    datatyp_kind="date", classification_short_name=None
)


@pytest.mark.parametrize(
    "name, sql_type, signal, expected",
    [
        # Layer 1: known id name beats everything
        ("LopNr", "BIGINT", None, "id"),
        ("LopNr", "VARCHAR", _REGMETA_CLASSIFIED, "id"),
        # Layer 2: regmeta evidence wins over CSV sql_type
        # ("ALKod"/"Kon" have enumerated value codes in regmeta even though
        # storage is BIGINT/tinyint → categorical)
        ("ALKod", "BIGINT", _REGMETA_VALUE_CODES, "categorical"),
        ("Kon", "BIGINT", _REGMETA_VALUE_CODES, "categorical"),
        ("RandomName", "INTEGER", _REGMETA_CLASSIFIED, "categorical"),
        ("Sun2000Inr", "VARCHAR", _REGMETA_CLASSIFIED, "categorical"),
        # Regmeta says numeric → numeric (ForvErs is int even though SCB
        # codes some "missing" sentinels)
        ("ForvErs", "BIGINT", _REGMETA_NUMERIC, "numeric"),
        ("BirthMoment", "VARCHAR", _REGMETA_DATE, "date"),
        # A char/varchar column without value codes and without a
        # classification is NOT enough to call categorical — storage type
        # alone doesn't carry that semantic. Falls through to sql_type
        # → text. Tester overrides in the inspector if wrong.
        ("MysteryString", "VARCHAR", _REGMETA_TEXT_NO_EVIDENCE, "text"),
        # Layer 3 used to be a loose "known categorical name" pass
        # (Kommun / Sun2000Inr / FodelseLand / Kon / ...). Removed: the
        # value_set schema makes regmeta authoritative for these, and
        # the false-positive risk on common Swedish stems was too high.
        # Anything regmeta doesn't know now falls through to sql_type
        # or text and surfaces in the inspector for review.
        # Layer 4: sql_type drives numeric / date for unrecognised names
        ("SammanInk", "BIGINT", None, "numeric"),  # the bug 2 case
        ("SomeAmount", "DECIMAL(18,2)", None, "numeric"),
        ("Whatever", "DOUBLE", None, "numeric"),
        ("InDatum", "DATE", None, "date"),
        ("Tidpunkt", "TIMESTAMP", None, "date"),
        # Layer 5: fallthrough
        ("RandomString", "VARCHAR", None, "text"),
        ("Mystery", None, None, "text"),
    ],
)
def test_classify_priority_chain(
    name: str,
    sql_type: str | None,
    signal: cfg_mod.RegmetaSignal | None,
    expected: str,
):
    assert _classify(name, sql_type, signal) == expected


def test_classify_id_pattern_beats_regmeta_classification():
    """`is_known_id` runs before the regmeta branch — even if regmeta
    flags a column as classified, an `lopnr` name should stay `id`."""
    assert _classify("lopnr", "BIGINT", _REGMETA_CLASSIFIED) == "id"


@pytest.mark.parametrize(
    "name, register, expected",
    [
        # Exact-name match (case-insensitive) under any register string
        # that contains "RTB". Variants like "ater_anv" or "AterAnvalt"
        # do *not* match — those fall through to sql_type / fallback.
        ("AterAnv", "RTB", "categorical"),
        ("ateranv", "rtb", "categorical"),
        ("FELPERSONNR", "Registret över totalbefolkningen (RTB)", "categorical"),
        ("LopNrByte", "RTB", "categorical"),
        ("FodelseAr", "RTB", "categorical"),
        ("FodelseArMan", "RTB", "categorical"),
        # Outside RTB, the same names fall through (BIGINT → numeric).
        ("AterAnv", "LISA", "numeric"),
        ("LopNrByte", "LISA", "numeric"),
        ("FelPersonNr", None, "numeric"),
        ("FodelseAr", "LISA", "numeric"),
        # Variants don't match even under RTB
        ("ater_anv", "RTB", "numeric"),
        ("AterAnvalt", "RTB", "numeric"),
        ("FodelseArManed", "RTB", "numeric"),
    ],
)
def test_classify_rtb_named_categorical(name: str, register: str | None, expected: str):
    """RTB-scoped exact-name allowlist: classified as categorical only
    when the register string identifies the source as RTB."""
    assert _classify(name, "BIGINT", None, register) == expected


def test_classify_lopnr_id_excludes_lopnrbyte():
    """`LopNrByte` is the RTB pid-change flag, not an identifier — the
    `lopnr` ID pattern explicitly excludes it so the register-scoped
    categorical rule can take effect, and outside RTB it doesn't get
    silently typed as `id` via the unanchored `lopnr` substring match."""
    assert _classify("LopNrByte", "BIGINT", None, "RTB") == "categorical"
    assert _classify("LopNrByte", "BIGINT", None, None) == "numeric"


# -- build_config from discover payload ------------------------------------


def test_build_config_routes_columns_per_source():
    """Without --register, classification falls back to sql_type +
    name patterns. SammanInk used to land in `text` — with
    sql_type-aware classification it lands in `numeric` because BIGINT."""
    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "LopNr", "sql_type": "int", "nullable": False},
                    {"name": "Kommun", "sql_type": "char(4)", "nullable": True},
                    {"name": "SammanInk", "sql_type": "BIGINT", "nullable": True},
                    {"name": "InkomstSumma", "sql_type": "decimal", "nullable": True},
                    {"name": "WhateverElse", "sql_type": "varchar", "nullable": True},
                    {"name": "BirthDate", "sql_type": "DATE", "nullable": True},
                ],
            }
        ],
    }
    out = build_config(discover)
    assert out["contract_version"] == "mdw-config-1.0.0"
    cols = out["column_types"]["lisa_2018"]
    assert cols["LopNr"] == {"type": "id"}
    # No --register and no name-pattern fallback: Kommun falls through
    # on its sql_type (char → text) and the user reviews.
    assert cols["Kommun"] == {"type": "text"}
    assert cols["SammanInk"] == {"type": "numeric"}
    assert cols["InkomstSumma"] == {"type": "numeric"}
    assert cols["WhateverElse"] == {"type": "text"}
    assert cols["BirthDate"] == {"type": "date"}


def test_build_config_uses_regmeta_classification(monkeypatch):
    """When --register is set and regmeta says a column has a
    classification *or* enumerated value codes, that column is
    `categorical` regardless of name. Storage type (char/tinyint) alone
    is not enough."""

    def fake_resolve(conn, register):
        assert register == "LISA"
        return [34]

    def fake_regmeta_lookup(conn, col_names, register_ids):
        assert register_ids == [34]
        # lowercase keys; mirrors the real impl
        return {
            "sun2000inr": cfg_mod.RegmetaSignal(
                datatyp_kind=None,
                classification_short_name="SUN2000",
            ),
            # ALKod: SCB enumerates 5 value codes in the PDF; the cvid
            # carries a non-null value_set_id. Stored as char in regmeta
            # but the CSV scan saw BIGINT — the value-codes signal wins.
            "alkod": cfg_mod.RegmetaSignal(
                datatyp_kind=None,
                classification_short_name=None,
                has_value_codes=True,
            ),
            # ForvErs: SCB stores as int with no value codes — numeric.
            "forvers": cfg_mod.RegmetaSignal(
                datatyp_kind="numeric", classification_short_name=None
            ),
        }

    class FakeConn:
        def close(self):
            pass

    monkeypatch.setattr(cfg_mod, "_regmeta_lookup", fake_regmeta_lookup)
    monkeypatch.setattr("regmeta.resolve_register_ids", fake_resolve, raising=True)
    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn(), raising=True)
    monkeypatch.setattr(
        "regmeta.db.db_path_from_args",
        lambda _x: Path("/fake/regmeta.db"),
        raising=True,
    )

    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "columns": [
                    {"name": "Sun2000Inr", "sql_type": "char(4)"},
                    {"name": "ALKod", "sql_type": "BIGINT"},
                    {"name": "ForvErs", "sql_type": "BIGINT"},
                    # MysteryCode has no name pattern and no regmeta entry —
                    # falls back to text (VARCHAR).
                    {"name": "MysteryCode", "sql_type": "varchar"},
                ],
            }
        ],
    }
    out = build_config(discover, register="LISA")
    cols = out["column_types"]["lisa_2018"]
    assert cols["Sun2000Inr"] == {"type": "categorical"}
    assert cols["ALKod"] == {"type": "categorical"}
    assert cols["ForvErs"] == {"type": "numeric"}
    assert cols["MysteryCode"] == {"type": "text"}


def test_build_config_empty_register_string_raises():
    """Codex P1: `--register ""` from an unset env var must not silently
    match every register via regmeta's LIKE-fallback. The over-typing
    would corrupt the inferred config without any visible error."""
    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {"source_name": "x", "columns": [{"name": "a", "sql_type": "int"}]}
        ],
    }
    with pytest.raises(ValueError, match="register must be a non-empty"):
        build_config(discover, register="")
    with pytest.raises(ValueError, match="register must be a non-empty"):
        build_config(discover, register="   ")


def test_build_config_db_path_uses_directory_semantics(monkeypatch):
    """Aligns with `compare` / `generate`: `--db` is a *directory*; the
    code appends `regmeta.db` via `db_path_from_args`. Passing a path
    that already ends in `regmeta.db` would otherwise produce a weird
    `<dir>/regmeta.db/regmeta.db` -- but the user-facing flag must be
    consistent across subcommands."""
    seen: dict = {}

    def fake_from_args(arg, filename="regmeta.db"):
        seen["arg"] = arg
        return Path("/fake/dir") / filename

    monkeypatch.setattr("regmeta.db.db_path_from_args", fake_from_args, raising=True)

    class FakeConn:
        def close(self):
            pass

    monkeypatch.setattr("regmeta.resolve_register_ids", lambda c, r: [34], raising=True)
    monkeypatch.setattr("regmeta.open_db", lambda p: FakeConn(), raising=True)
    monkeypatch.setattr(cfg_mod, "_regmeta_lookup", lambda *a, **k: {})

    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {"source_name": "x", "columns": [{"name": "a", "sql_type": "int"}]}
        ],
    }
    build_config(discover, register="LISA", db_path=Path("/some/dir"))
    assert seen["arg"] == "/some/dir"
    # When db_path is None, default-dir resolution kicks in (arg=None).
    seen.clear()
    build_config(discover, register="LISA", db_path=None)
    assert seen["arg"] is None


def test_cli_configure_handles_regmeta_error(monkeypatch, tmp_path: Path):
    """Codex P2 / Copilot suppressed: a regmeta-side failure during
    configure (missing DB, schema mismatch, ...) must surface as a
    clean CLI Error line, not a stack trace."""
    from mock_data_wizard.cli import main as cli_main
    from regmeta.errors import RegmetaError

    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "x"}]}]},
    )

    def boom(*a, **k):
        raise RegmetaError(
            exit_code=10,
            code="DB_NOT_FOUND",
            error_class="config",
            message="regmeta.db not found at /fake/path",
            remediation="Run `regmeta init` to create the database.",
        )

    monkeypatch.setattr(cfg_mod, "configure_from_discover", boom)
    rc = cli_main(["configure", "--register", "LISA", str(discover_path)])
    assert rc == 1


def test_build_config_register_unresolved_raises(monkeypatch):
    monkeypatch.setattr(
        "regmeta.resolve_register_ids", lambda conn, r: [], raising=True
    )

    class FakeConn:
        def close(self):
            pass

    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn(), raising=True)
    monkeypatch.setattr(
        "regmeta.db.db_path_from_args",
        lambda _x: Path("/fake/regmeta.db"),
        raising=True,
    )

    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {"source_name": "x", "columns": [{"name": "a", "sql_type": "int"}]}
        ],
    }
    with pytest.raises(ValueError, match="not found in regmeta"):
        build_config(discover, register="DOES_NOT_EXIST")


def test_regmeta_lookup_strips_project_prefix():
    """`P1105_LopNr_PersonNr` should lookup as both raw and stripped name
    so the regmeta side can match the bare `LopNr_PersonNr` form."""
    captured: dict = {}

    class FakeConn:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return self

        def fetchall(self):
            return []

    cfg_mod._regmeta_lookup(FakeConn(), {"P1105_LopNr_PersonNr"}, [34])
    # Both raw and stripped lowercase variants must appear in the bound
    # parameters; otherwise project-prefixed columns silently miss the
    # regmeta join.
    lower_params = [p for p in captured["params"] if isinstance(p, str)]
    assert "p1105_lopnr_personnr" in lower_params
    assert "lopnr_personnr" in lower_params


# -- configure_from_discover end-to-end ------------------------------------


def _write_discover(tmp_path: Path, payload: dict) -> Path:
    p = tmp_path / "mdw_step1_discovery.json"
    full = {"contract_version": "discover-1.0.0", **payload}
    p.write_text(json.dumps(full), encoding="utf-8")
    return p


def test_configure_from_discover_writes_next_to_input(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        {
            "sources": [
                {
                    "source_name": "a",
                    "columns": [{"name": "lopnr"}, {"name": "x"}],
                },
            ]
        },
    )
    out = configure_from_discover(discover_path)
    assert out == tmp_path / "mdw_step2_config.json"
    assert out.exists()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}
    # Without sql_type and without a name pattern, x falls to
    # text — the safe default for unknown VARCHAR-like data.
    assert payload["column_types"]["a"]["x"] == {"type": "text"}


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
    (tmp_path / "mdw_step2_config.json").write_text("{}", encoding="utf-8")
    with pytest.raises(FileExistsError, match="already exists"):
        configure_from_discover(discover_path)


def test_configure_overwrite_replaces_existing(tmp_path: Path):
    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "lopnr"}]}]},
    )
    target = tmp_path / "mdw_step2_config.json"
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
    """Pointing configure at mdw_step3_stats.json (or anything without a discover
    contract_version) must fail with a controlled message, not KeyError."""
    bad = tmp_path / "mdw_step3_stats.json"
    bad.write_text(
        json.dumps(
            {
                "contract_version": "2.0.0",
                "sources": [{"source_name": "a", "columns": []}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="mdw_step1_discovery.json"):
        configure_from_discover(bad)


def test_configure_rejects_payload_missing_columns_key(tmp_path: Path):
    """Partial discover file: a column entry without 'name'."""
    p = tmp_path / "mdw_step1_discovery.json"
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
    """A truncated mdw_step1_discovery.json with no 'columns' field must fail at the
    contract boundary, not silently emit an incomplete mdw_step2_config.json."""
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
    target = tmp_path / "mdw_step2_config.json"
    assert target.exists()
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["column_types"]["lisa_2018"]["LopNr"] == {"type": "id"}
    # No --register: Kommun has no sql_type, falls through to text.
    assert payload["column_types"]["lisa_2018"]["Kommun"] == {"type": "text"}


def test_cli_configure_refuses_overwrite_without_flag(tmp_path: Path):
    from mock_data_wizard.cli import main as cli_main

    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "x"}]}]},
    )
    (tmp_path / "mdw_step2_config.json").write_text("{}", encoding="utf-8")
    rc = cli_main(["configure", str(discover_path)])
    assert rc == 1


def test_cli_configure_overwrite_flag(tmp_path: Path):
    from mock_data_wizard.cli import main as cli_main

    discover_path = _write_discover(
        tmp_path,
        {"sources": [{"source_name": "a", "columns": [{"name": "lopnr"}]}]},
    )
    target = tmp_path / "mdw_step2_config.json"
    target.write_text("{}", encoding="utf-8")
    rc = cli_main(["configure", str(discover_path), "--overwrite"])
    assert rc == 0
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["column_types"]["a"]["lopnr"] == {"type": "id"}


def test_build_config_carries_source_year_from_discover():
    """#24: mdw_step1_discovery.json's source_detail.year flows into mdw_config's
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
    """Round-trip: configure output must parse cleanly via parse_config."""
    from mock_data_wizard.config import parse_config

    discover_path = _write_discover(
        tmp_path,
        {
            "sources": [
                {
                    "source_name": "lisa_2018",
                    "columns": [
                        {"name": "LopNr", "sql_type": "int"},
                        {"name": "Kommun", "sql_type": "char(4)"},
                        {"name": "InkomstSumma", "sql_type": "decimal(18,2)"},
                    ],
                }
            ]
        },
    )
    out = configure_from_discover(discover_path)
    payload = json.loads(out.read_text(encoding="utf-8"))
    cfg = parse_config(payload)
    assert cfg.lookup_type("lisa_2018", "LopNr").type == "id"
    # No --register: Kommun lands at text (char → fallthrough).
    assert cfg.lookup_type("lisa_2018", "Kommun").type == "text"
    assert cfg.lookup_type("lisa_2018", "InkomstSumma").type == "numeric"


# -- Schema family grouping ------------------------------------------------


def test_group_schema_families_buckets_identical_schemas():
    """Annual snapshots with identical (name, sql_type) tuples land in
    one family; a different-shaped source gets its own bucket."""
    discover = {
        "sources": [
            {
                "source_name": "slutbetyg_2018.csv",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "Betyg", "sql_type": "varchar"},
                ],
            },
            {
                "source_name": "slutbetyg_2019.csv",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "Betyg", "sql_type": "varchar"},
                ],
            },
            {
                "source_name": "kursprov_2019.csv",
                "columns": [
                    {"name": "LopNr", "sql_type": "int"},
                    {"name": "Betyg", "sql_type": "varchar"},
                    {"name": "Kurs", "sql_type": "varchar"},
                ],
            },
        ]
    }
    families = cfg_mod.group_schema_families(discover)
    assert len(families) == 2
    sizes = sorted(len(srcs) for srcs in families.values())
    assert sizes == [1, 2]


def test_group_schema_families_distinguishes_sql_types():
    """Same column names but different sql_types → different families.
    Catches the case where a CSV gets re-typed across delivery years."""
    discover = {
        "sources": [
            {
                "source_name": "a",
                "columns": [{"name": "x", "sql_type": "int"}],
            },
            {
                "source_name": "b",
                "columns": [{"name": "x", "sql_type": "bigint"}],
            },
        ]
    }
    families = cfg_mod.group_schema_families(discover)
    assert len(families) == 2


def test_build_config_register_per_source_overrides_global(monkeypatch):
    """Per-source override beats the global ``register`` argument.
    Sources without a per-source entry fall back to the global default.
    """
    discover = {
        "contract_version": "discover-1.0.0",
        "sources": [
            {
                "source_name": "lisa_2018",
                "columns": [{"name": "Kommun", "sql_type": "varchar"}],
            },
            {
                "source_name": "rams_2018",
                "columns": [{"name": "Yrke", "sql_type": "varchar"}],
            },
            {
                "source_name": "custom",
                "columns": [{"name": "Foo", "sql_type": "varchar"}],
            },
        ],
    }
    seen_registers: list[str] = []

    class FakeConn:
        def execute(self, *_a, **_k):
            return self

        def fetchall(self):
            return []

        def close(self):
            pass

    def fake_resolve(_conn, register):
        seen_registers.append(register)
        return {"LISA": [34], "RAMS": [37]}.get(register, [])

    import mock_data_wizard.configure as cfg

    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn())
    monkeypatch.setattr("regmeta.resolve_register_ids", fake_resolve)
    monkeypatch.setattr(
        "regmeta.db.db_path_from_args", lambda _x: Path("/fake/regmeta.db")
    )
    monkeypatch.setattr(cfg, "_regmeta_lookup", lambda *a, **k: {})

    out = cfg.build_config(
        discover,
        register="LISA",
        register_per_source={"rams_2018": "RAMS", "custom": None},
    )

    assert sorted(seen_registers) == ["LISA", "RAMS"]
    # `custom` had register=None so its column falls through to
    # name-pattern classification (varchar, no pattern → text).
    assert out["column_types"]["custom"]["Foo"] == {"type": "text"}


def test_resolve_register_to_id_and_name_rejects_ambiguous(monkeypatch):
    """Substring matches can return multiple registers — the inspector
    helper must surface the candidate list as ValueError instead of
    silently picking ids[0] and applying the wrong register."""
    from mock_data_wizard.configure import resolve_register_to_id_and_name

    class FakeConn:
        def __init__(self):
            self._next: list = []

        def execute(self, sql, params=()):
            self._sql = sql
            self._params = params
            return self

        def fetchall(self):
            assert "WHERE register_id IN" in self._sql
            return [
                {"register_id": 34, "registernamn": "LISA"},
                {"register_id": 60, "registernamn": "LISA-Plus"},
            ]

        def close(self):
            pass

    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn())
    monkeypatch.setattr("regmeta.resolve_register_ids", lambda _c, _v: [34, 60])
    monkeypatch.setattr("regmeta.db.db_path_from_args", lambda _x: Path("/fake.db"))

    with pytest.raises(ValueError, match="ambiguous"):
        resolve_register_to_id_and_name("LISA")


def test_resolve_register_to_id_and_name_unique_match(monkeypatch):
    from mock_data_wizard.configure import resolve_register_to_id_and_name

    class FakeConn:
        def execute(self, _sql, _params=()):
            return self

        def fetchone(self):
            return {"registernamn": "LISA"}

        def close(self):
            pass

    monkeypatch.setattr("regmeta.open_db", lambda _p: FakeConn())
    monkeypatch.setattr("regmeta.resolve_register_ids", lambda _c, _v: [34])
    monkeypatch.setattr("regmeta.db.db_path_from_args", lambda _x: Path("/fake.db"))

    assert resolve_register_to_id_and_name("LISA") == (34, "LISA")
