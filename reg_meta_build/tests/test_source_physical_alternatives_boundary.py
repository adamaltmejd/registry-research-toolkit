"""Missing-coding issues of physical alternatives, settled at the build boundary.

A physical alternative is one SCB source member (one semantic record: register,
variant, edition, variable and CVID) delivered in two literal columns; both columns
then report the same missing-coding issue, which settles once. Issues of distinct
members, or of one member in distinct periods, stay separate.

Every case prepares synthetic Registerinformation rows (`_csv_fixtures.var_row`) and
one Vardemangder code list through the real pipeline. Only one member carries the
code list, so every period the column delivers without it reports a
``missing_coding_period`` issue naming the uncoded member. One acknowledgement names
one such issue exactly. Assertions read the build report ledger and the built catalog.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from _csv_fixtures import PIPE, var_row
from _curation_support_boundary_support import (
    SCB_SAMPLE,
    SCB_VARIANT,
    prepare_sources,
)

if TYPE_CHECKING:
    from pathlib import Path

CASE_ID = "curation/registers/scb/sample.toml#/acknowledge/1"
INCOME = '[[variable]]\nnative_id = "1.1"\nslug = "income"\n'
PARTITION = (
    '[[identity.partition]]\nvariable = "1.1"\n'
    'columns = { First = "1.1", Second = "1.1" }\n'
    'columns_ref = "reviewed physical alternatives"\n'
)
# A co-delivered representation accepts one member delivered in both columns.
CO_DELIVERED = (
    '[[representation.parallel]]\nvariable = "1.1"\nvariant = "1.10"\n'
    'co_delivered = true\ncolumn_metadata = "per_column"\n'
    'valid_from = "2020-01-01"\nvalid_to = "2021-12-31"\n'
    'evidence = "Reviewed physical alternatives"\nnoted = "2026-10-06"\n'
    'columns = [{column = "First", valid_from = "2020-01-01", '
    'valid_to = "2021-12-31", source_editions = ["2020", "2021"]}, '
    '{column = "Second", valid_from = "2020-01-01", '
    'valid_to = "2021-12-31", source_editions = ["2020", "2021"]}]\n'
)


def acknowledge(edition: int, member: int, year: int) -> str:
    """The acknowledgement of the ``year`` issue of one member, exactly."""
    ref = {
        "source": "scb-registerinformation",
        "semantic_record_key": [
            "register:1",
            "variant:10",
            f"edition:{edition}",
            "variable:1",
            f"member:{member}",
        ],
    }
    return (
        '\n[[acknowledge]]\ncode = "missing_coding_period"\n'
        'subject = "scb/sample/income"\n'
        f"refs = [{json.dumps(json.dumps(ref))}]\n"
        'fields = ["coding"]\n'
        f'valid_from = "{year}-01-01"\nvalid_to = "{year}-12-31"\n'
        'reason = "Accepted while the source stays unresolved."\n'
        'evidence = "Synthetic fixture issue."\n'
    )


def build(tmp_path: Path, rows: list[str], coded_member: int, curation: str):
    sources = prepare_sources(
        tmp_path,
        scb_rows=rows,
        vardemangder_rows=[
            PIPE.join(["Kön", "1", "1", "Man", str(coded_member), "5001"])
        ],
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + INCOME + curation},
    )
    return sources.build(tmp_path)


def parallel_rows(second_2021_member: int) -> list[str]:
    """Both columns deliver member 1001 in 2020; in 2021 First delivers 1002."""
    return [
        var_row(colname="First", cvid=1001, var_id=1),
        var_row(colname="Second", cvid=1001, var_id=1),
        var_row(colname="First", cvid=1002, var_id=1, year="2021", regver_id=2021),
        var_row(
            colname="Second",
            cvid=second_2021_member,
            var_id=1,
            year="2021",
            regver_id=2021,
        ),
    ]


def settled(built) -> list[tuple]:
    """Each missing-coding issue as (members, period start, severity, acknowledged_by)."""
    return sorted(
        (
            tuple(ref["semantic_record_key"][-1] for ref in issue["refs"]),
            issue["valid_from"],
            issue["severity"],
            issue["acknowledged_by"],
        )
        for issue in built.issues("missing_coding_period")
    )


def assert_one_exact_acknowledgement(built) -> None:
    assert built.result["acknowledged"] == {"missing_coding_period": 1}
    assert not built.issues("stale_curation_entry")
    assert not built.issues("overbroad_curation_entry")


def test_physical_alternatives_report_one_missing_coding_issue_acknowledged_once(
    tmp_path: Path,
) -> None:
    built = build(
        tmp_path,
        parallel_rows(1002),
        1001,
        PARTITION + CO_DELIVERED + acknowledge(2021, 1002, 2021),
    )
    assert settled(built) == [(("member:1002",), "2021-01-01", "warning", CASE_ID)]
    assert_one_exact_acknowledgement(built)
    # The acknowledged issue still withholds the 2021 response domain.
    assert built.rows(
        "SELECT delivery_column_name, valid_from, value_set_id IS NULL"
        " FROM variable_state ORDER BY 1, 2"
    ) == [("First", "2020-01-01", 0), ("First", "2021-01-01", 1)]


def test_distinct_members_keep_their_own_missing_coding_issues(tmp_path: Path) -> None:
    built = build(
        tmp_path, parallel_rows(1003), 1001, PARTITION + acknowledge(2021, 1002, 2021)
    )
    assert settled(built) == [
        (("member:1002",), "2021-01-01", "warning", CASE_ID),
        (("member:1003",), "2021-01-01", "error", None),
    ]
    assert_one_exact_acknowledgement(built)


def test_one_member_keeps_a_missing_coding_issue_per_period(tmp_path: Path) -> None:
    """Pooled member 1005 delivers 2020-2022; the annual 2021 member 1006 wins its
    year and carries the code list, so 1005 lacks coding in 2020 and in 2022."""
    rows = [
        var_row(colname="VALUE", cvid=1005, var_id=1, versionname="2020-2022"),
        var_row(colname="VALUE", cvid=1006, var_id=1, year="2021", regver_id=2021),
    ]
    built = build(tmp_path, rows, 1006, acknowledge(110, 1005, 2020))
    assert settled(built) == [
        (("member:1005",), "2020-01-01", "warning", CASE_ID),
        (("member:1005",), "2022-01-01", "error", None),
    ]
    assert_one_exact_acknowledgement(built)
