"""Curated ``[[acknowledge]]`` entries matched against build issues, at the build boundary.

Every case prepares a synthetic SCB Registerinformation export (rows from
``_csv_fixtures.var_row``; no real register content) through the real input pipeline,
appends one acknowledgement to the register's curation TOML and asserts on the build
report ledger. An acknowledgement names one error by its code, subject, refs, fields
and period; a match turns that error into a warning that names the entry, and an entry
that matches nothing (or whose pinned evidence changed) is itself a
``stale_curation_entry`` error (``AcknowledgeDecision`` docstring, ``source_curation.py``).
The issue coordinates below are the ones the build reports for these inputs; they were
read from a diagnostic run of the fixture, and the oracle is that only the exact
coordinates acknowledge.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row, write_input_bundle, write_scb_input
from _pipeline_catalog_support import report_issues
from _prepared_fixtures import accept_prepared
from reg_meta_build.pipeline import build_catalog
from reg_meta_build.prepared_catalog import (
    open_prepared_catalog_sources,
    prepare_catalog_sources,
)
from reg_meta_build.source_curation import acknowledgement_evidence_sha256

if TYPE_CHECKING:
    from pathlib import Path

REGISTER_TOML = (
    '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
    '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
    '[[variable]]\nnative_id = "1.101"\nslug = "value"\n'
)
CASE_ID = "curation/registers/scb/sample.toml#/acknowledge/1"
STALE_EVIDENCE = "has changed original or coding evidence"


def ref(member: int, *, variable: int = 101, edition: int = 110) -> str:
    return json.dumps(
        {
            "source": "scb-registerinformation",
            "semantic_record_key": [
                "register:1",
                "variant:10",
                f"edition:{edition}",
                f"variable:{variable}",
                f"member:{member}",
            ],
        }
    )


def acknowledge(
    code: str,
    subject: str,
    refs: list[str],
    fields: list[str],
    period: tuple[str, str] | None = None,
    evidence_sha256: str | None = None,
) -> str:
    text = (
        f"\n[[acknowledge]]\ncode = {json.dumps(code)}\n"
        f"subject = {json.dumps(subject)}\nrefs = {json.dumps(refs)}\n"
        f"fields = {json.dumps(fields)}\n"
    )
    if period is not None:
        text += f'valid_from = "{period[0]}"\nvalid_to = "{period[1]}"\n'
    if evidence_sha256 is not None:
        text += f'expected_evidence_sha256 = "{evidence_sha256}"\n'
    return (
        text + 'reason = "Accepted while the source stays unresolved."\n'
        'evidence = "Synthetic fixture issue."\n'
    )


def prepare(tmp_path: Path, rows: list[str], *, coded: bool = False):
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=rows,
        include=("registerinformation", "vardemangder", "valid_dates")
        if coded
        else ("registerinformation",),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    return prepared, accept_prepared(prepared), manifest.sha256


def build(tmp_path: Path, rows: list[str], curation_text: str, *, coded=False):
    """Prepare ``rows`` and build them diagnostically; return (summary, issues)."""
    prepared, commit, digest = prepare(tmp_path, rows, coded=coded)
    curation = tmp_path / "curation"
    (curation / "registers" / "scb").mkdir(parents=True)
    (curation / "classifications").mkdir()
    (curation / "registers/scb/sample.toml").write_text(
        REGISTER_TOML + curation_text, encoding="utf-8"
    )
    summary = build_catalog(
        prepared,
        commit,
        digest,
        tmp_path / "catalog.db",
        tmp_path / "report",
        curation_dir=curation,
        diagnostic=True,
    )
    return summary, report_issues(tmp_path / "report")


def settled(issues: list[dict], code: str) -> list[tuple]:
    """The named issues and every stale entry, as (code, severity, acknowledged_by)."""
    return [
        (i["code"], i["severity"], i["acknowledged_by"] or i["case_id"])
        for i in issues
        if i["code"] in {code, "stale_curation_entry"}
    ]


# --- period ---------------------------------------------------------------------

# Two 2020 members of one native variable disagree on their literal unit, so the
# 2020 column segment reports a dated occurrence conflict.
UNIT_CONFLICT = [
    var_row(cvid=1001, var_id=101, colname="VALUE", unit="kg"),
    var_row(cvid=1002, var_id=101, colname="VALUE", unit="ton"),
]
UNIT_CONFLICT_ACK = (
    "conflicting_occurrence_facts",
    "scb/sample/value",
    [ref(1001), ref(1002)],
    ["measurement_unit"],
)


@pytest.mark.parametrize(
    ("period", "acknowledged"),
    [
        pytest.param(("2020-01-01", "2020-12-31"), True, id="exact"),
        pytest.param(None, False, id="no-dates"),
        pytest.param(("2020-01-01", "2020-06-30"), False, id="contained-window"),
    ],
)
def test_a_dated_issue_is_acknowledged_only_by_its_exact_period(
    tmp_path: Path, period, acknowledged: bool
) -> None:
    summary, issues = build(
        tmp_path, UNIT_CONFLICT, acknowledge(*UNIT_CONFLICT_ACK, period=period)
    )
    assert settled(issues, "conflicting_occurrence_facts") == (
        [("conflicting_occurrence_facts", "warning", CASE_ID)]
        if acknowledged
        else [
            ("conflicting_occurrence_facts", "error", None),
            ("stale_curation_entry", "error", CASE_ID),
        ]
    )
    assert summary["acknowledged"] == (
        {"conflicting_occurrence_facts": 1} if acknowledged else {}
    )


# --- field order ----------------------------------------------------------------

# One native variable delivered under two literal column spellings in one edition:
# the build reports an unresolved identity over the fields (identity, column_name).
TWO_SPELLINGS = [
    var_row(cvid=1001, var_id=101, colname="VALUE"),
    var_row(cvid=1002, var_id=101, colname="VALUE2"),
]


@pytest.mark.parametrize(
    ("fields", "acknowledged"),
    [
        pytest.param(["identity", "column_name"], True, id="issue-order"),
        pytest.param(["column_name", "identity"], False, id="reversed"),
    ],
)
def test_acknowledged_fields_match_only_in_the_issue_order(
    tmp_path: Path, fields: list[str], acknowledged: bool
) -> None:
    _, issues = build(
        tmp_path,
        TWO_SPELLINGS,
        acknowledge(
            "unresolved_native_identity",
            "scb/sample/value",
            [ref(1001), ref(1002)],
            fields,
        ),
    )
    assert settled(issues, "unresolved_native_identity") == (
        [("unresolved_native_identity", "warning", CASE_ID)]
        if acknowledged
        else [
            ("unresolved_native_identity", "error", None),
            ("stale_curation_entry", "error", CASE_ID),
        ]
    )


# --- evidence guard -------------------------------------------------------------

# Variable 1.102 has no catalog naming, so its one member reports an unresolved
# catalog identity; 1.101 is the named neighbour.
UNNAMED_SUBJECT = (
    "('scb-registerinformation', 'scb', 'register', 'native-int', 1, "
    "'variable', 'native-int', 102)"
)


def unnamed_rows(definition: str = "A generic family label") -> list[str]:
    return [
        var_row(cvid=1001, var_id=101, colname="VALUE"),
        var_row(cvid=1003, var_id=102, colname="OTHER", vardef=definition),
    ]


def original_rows_guard(tmp_path: Path, rows: list[str], *, coded: bool) -> str:
    """The evidence fingerprint over the acknowledged member's original rows alone."""
    prepared, commit, digest = prepare(tmp_path / "guard", rows, coded=coded)
    records = open_prepared_catalog_sources(
        prepared, expected_sha256=digest, input_commit=commit
    ).records.records
    return acknowledgement_evidence_sha256(
        r
        for r in records
        if r.source == "scb-registerinformation" and r.subject.member.native_id == 1003
    )


def guarded_ack(guard: str) -> str:
    return acknowledge(
        "unresolved_catalog_identity",
        UNNAMED_SUBJECT,
        [ref(1003, variable=102)],
        ["identity"],
        evidence_sha256=guard,
    )


@pytest.mark.parametrize(
    ("definition", "acknowledged"),
    [
        pytest.param("A generic family label", True, id="unchanged"),
        pytest.param("Changed source meaning", False, id="changed-definition"),
    ],
)
def test_a_guarded_acknowledgement_lapses_when_its_original_rows_change(
    tmp_path: Path, definition: str, acknowledged: bool
) -> None:
    """The guard is pinned over the original fixture rows; the build then sees
    either those rows or rows whose source definition changed. The changed
    definition alters no coordinate of the issue itself."""
    guard = original_rows_guard(tmp_path, unnamed_rows(), coded=False)
    summary, issues = build(tmp_path, unnamed_rows(definition), guarded_ack(guard))
    assert settled(issues, "unresolved_catalog_identity") == (
        [("unresolved_catalog_identity", "warning", CASE_ID)]
        if acknowledged
        else [
            ("unresolved_catalog_identity", "error", None),
            ("stale_curation_entry", "error", CASE_ID),
        ]
    )
    if not acknowledged:
        (stale,) = (i for i in issues if i["code"] == "stale_curation_entry")
        assert STALE_EVIDENCE in stale["detail"]
    assert summary["acknowledged"] == (
        {"unresolved_catalog_identity": 1} if acknowledged else {}
    )


def test_a_guarded_acknowledgement_also_pins_the_members_bound_coding(
    tmp_path: Path,
) -> None:
    """Member 1003 carries a bound source code list (the fixture's Vardemangder
    rows), so a fingerprint over its original rows alone is not its full
    evidence and the entry lapses."""
    rows = unnamed_rows()
    guard = original_rows_guard(tmp_path, rows, coded=True)
    _, issues = build(tmp_path, rows, guarded_ack(guard), coded=True)
    assert settled(issues, "unresolved_catalog_identity") == [
        ("unresolved_catalog_identity", "error", None),
        ("stale_curation_entry", "error", CASE_ID),
    ]
    (stale,) = (i for i in issues if i["code"] == "stale_curation_entry")
    assert STALE_EVIDENCE in stale["detail"]
