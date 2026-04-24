"""Tests for the Socialstyrelsen Excel parser.

Two tiers:

- Unit tests for helpers. Always run.
- Integration tests over the real input files under
  `regmeta/input_data/Socialstyrelsen/`. Skipped when the directory is
  absent (CI, fresh checkouts) since the input is gitignored. These
  tests exist to catch regressions against real deliveries during local
  development.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("openpyxl")

from regmeta.sources.sos import (  # noqa: E402
    SosDcatAp,
    SosParseError,
    SosRegister,
    _as_date,
    _as_int,
    _clean,
    _normalise,
    parse_directory,
    parse_register_file,
)

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_normalise_strips_separators_and_case() -> None:
    assert _normalise("Metadata - Variabelnivå") == "metadatavariabelnivå"
    assert _normalise("Kodlista_DIAGNOS") == "kodlistadiagnos"
    assert _normalise("Metadata-Datamängd (DCAT-AP) ") == "metadatadatamängddcatap"


def test_clean_empty_returns_none() -> None:
    assert _clean(None) is None
    assert _clean("") is None
    assert _clean("   ") is None
    assert _clean("  hi ") == "hi"
    assert _clean(42) == "42"


def test_as_int_handles_common_shapes() -> None:
    assert _as_int(1964) == 1964
    assert _as_int("1964") == 1964
    assert _as_int("  1964 ") == 1964
    assert _as_int(1964.0) == 1964
    assert _as_int(1964.5) is None
    assert _as_int(None) is None
    assert _as_int("") is None
    assert _as_int("not a year") is None


def test_as_date_accepts_datetime_and_date() -> None:
    from datetime import date, datetime

    assert _as_date(datetime(2026, 3, 26)) == date(2026, 3, 26)
    assert _as_date(date(2026, 3, 26)) == date(2026, 3, 26)
    assert _as_date("2026-03-26") is None  # strings aren't implicitly parsed
    assert _as_date(None) is None


def test_lock_file_rejected() -> None:
    with pytest.raises(SosParseError):
        parse_register_file(Path("~$something.xlsx"))


def test_missing_file_raises() -> None:
    with pytest.raises(SosParseError):
        parse_register_file(Path("/does/not/exist.xlsx"))


def test_dcat_ap_extras_roundtrip() -> None:
    ap = SosDcatAp(title_sv="Foo", extras={"Unknown attribute": "value"})
    assert ap.extras == {"Unknown attribute": "value"}
    assert ap.title_sv == "Foo"


# ---------------------------------------------------------------------------
# Integration tests against real deliveries
# ---------------------------------------------------------------------------


def _locate_sos_data() -> Path | None:
    """Find `regmeta/input_data/Socialstyrelsen/` — gitignored, so it may
    live in the main checkout rather than the current worktree.

    Honour `SOS_METADATA_FIXTURES` first. Otherwise walk upward so
    worktrees can find the sibling main checkout.
    """
    env = os.environ.get("REGMETA_SOS_DATA")
    if env:
        p = Path(env)
        return p if p.exists() else None

    anchor = Path(__file__).resolve()
    candidates = [anchor.parents[1] / "input_data" / "Socialstyrelsen"]
    for parent in anchor.parents:
        candidates.append(parent / "regmeta" / "input_data" / "Socialstyrelsen")

    for c in candidates:
        if c.exists() and list(c.glob("*.xlsx")):
            return c
    return None


SOS_DATA = _locate_sos_data()


requires_sos_data = pytest.mark.skipif(
    SOS_DATA is None,
    reason=(
        "Socialstyrelsen input data not present (gitignored); set "
        "REGMETA_SOS_DATA to run integration tests"
    ),
)


@requires_sos_data
def test_parses_every_register_without_error() -> None:
    results = parse_directory(SOS_DATA)
    assert len(results) >= 13, f"expected ≥13 registers, got {len(results)}"
    for r in results:
        assert isinstance(r, SosRegister)
        assert r.variables, f"{r.source_file.name}: no variables parsed"


@requires_sos_data
def test_skips_office_lock_files() -> None:
    results = parse_directory(SOS_DATA)
    for r in results:
        assert not r.source_file.name.startswith("~$")


@requires_sos_data
def test_par_shape() -> None:
    par = _find_register(parse_directory(SOS_DATA), "Patientregistret")
    assert par.dataset_name == "Patientregistret"
    assert par.dcat_ap.title_sv == "Patientregistret"
    assert par.dcat_ap.title_en == "National Patient Register"
    names = {d.name for d in par.deldatamangder}
    assert names == {"PAR_SV", "PAR_OV", "PAR_TV"}
    assert par.dcat_ap.legislation_sv
    assert "hälsodataregister" in par.dcat_ap.legislation_sv.lower()
    assert any(v.name == "HDIA" for v in par.variables)


@requires_sos_data
def test_mfr_kodlista_with_per_row_variable_column() -> None:
    mfr = _find_register(parse_directory(SOS_DATA), "Medicinska födelseregistret")
    butsatt = next(
        (k for k in mfr.kodlistor if k.variable_hint.lower() == "butsatt"), None
    )
    assert butsatt is not None, "Kodlista_butsatt missing"
    assert butsatt.rows
    assert butsatt.rows[0].variable_name == "BUTSATT"


@requires_sos_data
def test_unrecognised_kodlista_preserves_raw_rows() -> None:
    results = parse_directory(SOS_DATA)
    skipped = [k for r in results for k in r.kodlistor if not k.rows and k.raw_rows]
    assert skipped, "expected at least one kodlista with empty rows + raw_rows"


@requires_sos_data
def test_registers_without_deldatamangder_warn() -> None:
    results = parse_directory(SOS_DATA)
    missing = [r for r in results if not r.deldatamangder]
    assert missing, (
        "expected at least one register without deldatamängd sheet (LSS/BU/SOL)"
    )
    for r in missing:
        assert any("Deldatamängder" in w for w in r.warnings)


@requires_sos_data
def test_bu_phantom_rows_do_not_inflate_variable_count() -> None:
    bu = _find_register(parse_directory(SOS_DATA), "Insatser till barn och unga")
    assert 50 <= len(bu.variables) <= 2000, (
        f"BU variable count implausible: {len(bu.variables)}"
    )


@requires_sos_data
def test_dcat_ap_covers_expected_fields() -> None:
    par = _find_register(parse_directory(SOS_DATA), "Patientregistret")
    d: SosDcatAp = par.dcat_ap
    assert d.title_sv and d.title_en
    assert d.description_sv and d.description_en
    assert d.temporal_coverage_sv == "1964-"
    assert d.publisher_sv == "Socialstyrelsen"
    assert d.landing_page_sv and d.landing_page_sv.startswith("https://")
    assert d.access_rights_sv


@requires_sos_data
def test_variable_fields_populated() -> None:
    par = _find_register(parse_directory(SOS_DATA), "Patientregistret")
    hdia = next(v for v in par.variables if v.name == "HDIA")
    assert hdia.deldatamangd in {"PAR_SV", "PAR_OV", "PAR_TV"}
    assert hdia.label
    assert hdia.data_type
    external_refs = [
        v.external_classification for v in par.variables if v.external_classification
    ]
    assert any("icd" in r.lower() for r in external_refs)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _find_register(results: list[SosRegister], dataset_name: str) -> SosRegister:
    for r in results:
        if r.dataset_name and dataset_name.lower() in r.dataset_name.lower():
            return r
    raise AssertionError(f"register {dataset_name!r} not in parse results")
