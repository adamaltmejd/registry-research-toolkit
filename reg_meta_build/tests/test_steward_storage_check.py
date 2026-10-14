"""Steward storage check: curated global data types against the steward's storage
schema (#1562, #1594).

The global build reads no steward storage, so a steward refresh is the only place a
storage disagreement surfaces. The boundaries are the findings worklist the refresh
writes and the generator's refusal to run without the export; the fixture under
`cases/steward_storage/` is their readable source.
"""

from __future__ import annotations

from pathlib import Path

from _swecov_fixtures import GENERATOR, run_generator
from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.steward_spec import load_steward_spec
from reg_meta_build.steward_storage import (
    read_storage_types,
    storage_policy_from_spec,
    storage_type_findings,
    write_storage_findings,
)

_CASE = Path(__file__).parent / "cases" / "steward_storage"


def test_storage_check_reports_each_disagreement_kind(tmp_path: Path) -> None:
    """Pins the worklist: HELD is an uncurated steward holding, MIXED a curated
    integer with a text storage wave, WIDE a documented (scb-docs) decimal over
    integer storage, and Guid (folded onto storage GUID) a storage type outside
    the catalog vocabulary. ELSEWHERE, a steward holding, is stored only under a
    table outside the register's series, so it is unmatched, and so is
    Outside, a typed delivered erratum stored only there; DOCONLY, a
    documented column storage does not hold, is no finding. AGREES matches its
    storage, and Renamed matches the PHYS header the series' storage_header
    names, so neither is a finding. Fails if the check drops a kind, stops
    folding the column, ignores the series' tables or header mapping, skips an
    unmatched steward-held column or typed delivered erratum silently (as it
    would if typed delivered entries stopped counting as steward-held), stops
    comparing a documented type that storage holds, reports a documented column
    absent from storage, or reports an agreeing type."""
    findings = storage_type_findings(
        load_register_files(_CASE / "curation"),
        storage_policy_from_spec(load_steward_spec(_CASE / "spec")),
        read_storage_types(_CASE / "swecov_column_types.csv"),
    )
    output = tmp_path / "findings.tsv"
    write_storage_findings(findings, output)
    assert output.read_text(encoding="utf-8") == (_CASE / "expected.tsv").read_text(
        encoding="utf-8"
    )


def test_inventory_refuses_to_run_without_the_storage_export(tmp_path: Path) -> None:
    """The twin of the run above with the export absent. Fails if `inventory`
    treats a missing export as "skip the storage check" (#1594), or reads the
    other inputs first: the census, base and providers do not exist either, so only
    the export's refusal can name the export."""
    missing = tmp_path / "absent_column_types.csv"
    result = run_generator(
        GENERATOR,
        "--csv",
        str(tmp_path / "absent_census.csv"),
        "inventory",
        "--base-db",
        str(tmp_path / "absent.db"),
        "--providers",
        str(tmp_path / "absent-providers"),
        "--storage-csv",
        str(missing),
        "--out",
        str(tmp_path / "out"),
    )
    assert result.returncode != 0
    assert f"storage-schema export {missing} is unreadable" in result.stderr
    assert "--storage-csv" in result.stderr
    assert not (tmp_path / "out").exists()
