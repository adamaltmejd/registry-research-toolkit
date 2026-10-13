"""SWECOV steward check: curated global data types against MONA storage (#1562).

The global build reads no steward storage, so a SWECOV refresh is the only place a
storage disagreement surfaces. The boundary is the findings worklist the refresh
writes; the fixture under `cases/swecov_storage/` is its readable source.
"""

from __future__ import annotations

from pathlib import Path

from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.swecov_storage import (
    load_storage_policy,
    read_storage_types,
    storage_type_findings,
    write_storage_findings,
)

_CASE = Path(__file__).parent / "cases" / "swecov_storage"
_REPO = Path(__file__).resolve().parents[2]


def test_storage_check_reports_each_disagreement_kind(tmp_path: Path) -> None:
    """Pins the worklist: HELD is an uncurated steward holding, MIXED a curated
    integer with a text storage wave, WIDE a curated decimal over integer storage,
    and Guid (folded onto storage GUID) a storage type outside the catalog
    vocabulary. ELSEWHERE is stored only under a table outside the register's
    prefixes, so it is unmatched. AGREES matches its storage, and Renamed matches
    the PHYS header the policy's storage_columns names, so neither is a finding.
    Fails if the check drops a kind, stops folding the column, ignores the policy
    prefixes or header mapping, skips an unmatched entry silently, or reports an
    agreeing type."""
    findings = storage_type_findings(
        load_register_files(_CASE / "curation"),
        load_storage_policy(_CASE / "storage_policy.toml"),
        read_storage_types(_CASE / "swecov_column_types.csv"),
    )
    output = tmp_path / "findings.tsv"
    write_storage_findings(findings, output)
    assert output.read_text(encoding="utf-8") == (_CASE / "expected.tsv").read_text(
        encoding="utf-8"
    )


def test_committed_storage_policy_names_curated_registers() -> None:
    """Fails if a register rename or removal leaves the steward policy naming a
    register the curation no longer has, which would silently skip its check."""
    policy = load_storage_policy(
        _REPO / "reg_webapp" / "stewards" / "swecov" / "storage_policy.toml"
    )
    curated = load_register_files(_REPO / "reg_meta_build" / "curation")
    registers = {
        f"{register.register_info.provider}/{register.register_info.slug}"
        for register in curated
    }
    assert policy
    assert set(policy) <= registers, sorted(set(policy) - registers)
