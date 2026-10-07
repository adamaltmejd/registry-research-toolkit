"""Coding curation checked against its own reviewed period, at the build boundary.

Every case prepares synthetic SCB Registerinformation rows (`_csv_fixtures.var_row`;
register TESTREG = native register 1, variant 1.10, variable 1.101) and Vardemangder
rows through the real pipeline, appends one coding entry to the register's curation
TOML and asserts on the built catalog and the report ledger.

Guard digests and source-authority blocks are authored the way a curator authors them:
from the accepted prepared records and their bound source lists, through the public
capture helpers. A marker certificate's binding fingerprints are taken over the
reviewed year's binding alone, so the authored value does not depend on the window
filter under test.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import PIPE, var_row
from _curation_support_boundary_support import (
    SCB_SAMPLE,
    SCB_VARIANT,
    prepare_sources,
    toml_inline,
)
from reg_meta_build.prepared_catalog import open_prepared_catalog_sources
from reg_meta_build.source_coding import coding_source_sha256
from reg_meta_build.source_curation import (
    acknowledgement_evidence_sha256,
    capture_expectations,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import SourceFields
from reg_meta_build.source_value_bindings import (
    ValueBindingResult,
    bind_code_lists,
    marker_binding_fingerprints,
    open_value_bindings,
)

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_records import SourceRecord

VALUE = '[[variable]]\nnative_id = "1.101"\nslug = "value"\n'


def bindings(sources, record: SourceRecord) -> ValueBindingResult:
    """The source lists the prepared value store binds to one prepared record."""
    opened = open_prepared_catalog_sources(
        sources.prepared, expected_sha256=sources.digest, input_commit=sources.commit
    )
    with open_value_bindings(opened.value_sources) as sessions:
        return bind_code_lists(record, sessions)


def state_codes(built) -> list[tuple]:
    """Each VALUE state's period with its published codes (None when uncoded)."""
    return built.rows(
        "SELECT s.valid_from, s.valid_to, c.code, c.label FROM variable_state s "
        "LEFT JOIN value_set_member m ON m.value_set_id = s.value_set_id "
        "LEFT JOIN value_code c ON c.code_id = m.code_id "
        "WHERE s.delivery_column_name = 'VALUE' ORDER BY 1, 3"
    )


# --- compact choice --------------------------------------------------------------


def test_a_choice_over_a_period_with_one_complete_list_is_stale(
    tmp_path: Path,
) -> None:
    """A choice keeps one of several competing complete lists. With one complete
    list the period is uncontested, so the choice is stale for that reason, even
    though its evidence digest and its kept label both still match the source."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=[var_row(cvid=1001, var_id=101, colname="VALUE")],
        vardemangder_rows=["keep|1|01|Label|1001|7001"],
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + VALUE},
    )
    records = sources.records()
    digest = acknowledgement_evidence_sha256(
        records,
        (
            coding_source_sha256(claim)
            for record in records
            for claim in bindings(sources, record).claims
        ),
    )
    sources.append(
        "scb/sample.toml",
        '\n[[coding.choice]]\nvariable = "1.101"\nvariant = "people"\n'
        'column = "VALUE"\nperiods = [["2020-01-01", "2020-12-31"]]\n'
        f'keep = "keep"\nover = ["other"]\nexpected_evidence_sha256 = "{digest}"\n'
        'reason = "Reviewed competing lists"\nsource = "fixture"\n',
    )
    built = sources.build(tmp_path)
    case = "curation/registers/scb/sample.toml#/coding.choice/1/period/1"
    assert [(i["code"], i["case_id"], i["detail"]) for i in built.errors()] == [
        (
            "stale_curation_entry",
            case,
            f"{case}: period is no longer contested by complete lists",
        )
    ]


# --- enumerated marker certificate -------------------------------------------------

# Both years define VALUE by the same numbered enumeration and deliver a `Tal`
# descriptor row: a recognized marker, not a code list (`_csv_fixtures`'s
# VARDEMANGDER_SENTINEL_ROWS shape).
DEFINITION = '"Which amount?\n1. Less than 500\n2. At least 500"'
MEMBERS = [["1", "Less than 500"], ["2", "At least 500"]]
MARKERS = [
    PIPE.join(["Tal", "Tal", "Tal", "Some descriptionSCB\\SCBLEOT", cvid, "5900"])
    for cvid in ("1001", "1002")
]
ROWS = [
    var_row(cvid=1001, var_id=101, colname="VALUE", vardef=DEFINITION),
    var_row(
        cvid=1002,
        var_id=101,
        colname="VALUE",
        vardef=DEFINITION,
        year="2021",
        regver_id=2021,
    ),
]


def marker_certificate(sources) -> str:
    """A `[[coding.documented]]` 2020 entry certified by the column's source rows,
    its enumerated definition and the 2020 marker binding."""
    records = sources.records()
    (reviewed,) = (r for r in records if r.subject.member.native_id == 1001)
    opened = open_prepared_catalog_sources(
        sources.prepared, expected_sha256=sources.digest, input_commit=sources.commit
    )
    (revision,) = (
        v
        for v in opened.records.manifest.revisions
        if v.revision_id == reviewed.source_revision_id
    )
    scope = source_occurrence(reviewed).edition_period_scope
    markers = marker_binding_fingerprints(
        ((scope, binding) for binding in bindings(sources, reviewed).bindings),
        "2020-01-01",
        "2020-12-31",
    )
    assert markers is not None
    authority = {
        "revision": revision.model_dump(mode="json"),
        "locators": [
            locator.model_dump(mode="json")
            for record in records
            for locator in record.locators
        ],
        "records": [
            expectation.model_dump(mode="json")
            for expectation in capture_expectations(
                records,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ],
        "codings": [],
        "enumeration": {
            "field": "definition",
            "syntax": "ascii-decimal-dot-space",
            "lines": [f"{code}. {label}" for code, label in MEMBERS],
        },
        "marker_bindings": list(markers),
    }
    return (
        '\n[[coding.documented]]\nvariable = "1.101"\nvariant = "people"\n'
        'column = "VALUE"\nperiods = [["2020-01-01", "2020-12-31"]]\n'
        f"members = {toml_inline(MEMBERS)}\n"
        'version_label = "Exact enumerated source"\n'
        'reason = "Reviewed enumeration"\nsource = "fixture"\n'
        f"source_authority = {toml_inline(authority)}\n"
    )


def test_a_marker_certificate_checks_only_the_markers_bound_in_its_period(
    tmp_path: Path,
) -> None:
    """The 2021 delivery carries its own marker binding outside the reviewed 2020
    period; the certificate still matches and states the enumerated 2020 codes."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=ROWS,
        vardemangder_rows=MARKERS,
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + VALUE},
    )
    sources.append("scb/sample.toml", marker_certificate(sources))
    built = sources.build(tmp_path)
    assert built.issues() == []
    assert state_codes(built) == [
        ("2020-01-01", "2020-12-31", "1", "Less than 500"),
        ("2020-01-01", "2020-12-31", "2", "At least 500"),
        ("2021-01-01", "2021-12-31", None, None),
    ]
