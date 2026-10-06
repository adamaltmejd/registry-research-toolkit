"""SCB column partitions and column owners at the build boundary.

Every case prepares synthetic SCB Registerinformation rows (`_csv_fixtures.var_row`,
register TESTREG = native register 1, variant 1.10, variable 1.5) and a curation
TOML through the real pipeline, then asserts on the built catalog, the issue
ledger and each source occurrence's ledger disposition.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row
from _curation_support_boundary_support import (
    SCB_SAMPLE,
    SCB_VARIANT,
    prepare_sources,
    toml_inline,
)
from reg_meta_build.source_curation import (
    acknowledgement_evidence_sha256,
    capture_expectations,
)
from reg_meta_build.source_records import SourceFields

if TYPE_CHECKING:
    from pathlib import Path

PARTITION = "accepted-column-partitions:scb-registerinformation:1.5"
OTHER = (
    '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "2"\n'
    '[[variant]]\nnative_id = "2.20"\nslug = "people"\n'
    '[[variable]]\nnative_id = "2.201"\nslug = "value"\n'
)
OTHER_ROW = var_row(cvid=90, var_id=201, colname="OTHER", register=("OTHERREG", 2, 20))
NATIVE_PARTITION = (
    '[[variable]]\nnative_id = "1.5"\nslug = "quantity"\n'
    '[[identity.partition]]\nvariable = "1.5"\n'
    'columns = { OLD = "1.5", NEW = "1.5" }\n'
    'columns_ref = "reviewed same source-native quantity"\n'
)
GUARDED_ROLE = (
    '[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n'
    '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.10"\n'
    'column = "ANSWER"\nowner = "1.5.amount"\nref = "explicit amount role"\n'
    'source_editions = ["2020"]\n'
    'expected_fields = [{ name = "measurement_unit", status = "value", value = "SEK" }, '
    '{ name = "operational_definition", status = "value", value = "Amount" }, '
    '{ name = "source_attribution", status = "value", value = "Authority" }]\n'
)
SCOPED_CODE = (
    '[[variable]]\nnative_id = "1.5.code"\nslug = "code"\n'
    '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.10"\n'
    'column = "CODE"\nowner = "1.5.code"\nref = "exact supplied physical code"\n'
    'source_editions = ["2020"]\n'
    'expected_fields = [{ name = "column_name", status = "value", value = "CODE" }]\n'
)


def _row(column: str, cvid: int, year: str = "2020", **fields) -> str:
    return var_row(
        cvid=cvid, var_id=5, colname=column, year=year, regver_id=int(year), **fields
    )


def _literal_owner(column: str, slug: str) -> str:
    return (
        f'[[variable]]\nnative_id = "1.5.{slug}"\nslug = "{slug}"\n'
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.10"\n'
        f'column = "{column}"\nowner = "1.5.{slug}"\nref = "{slug} construct"\n'
        'source_editions = ["2020"]\n'
    )


# Two literal columns delivered under one source record (one CVID).
SHARED_REF_ROWS = [_row("FIRST", 20), _row("SECOND", 20)]
SHARED_REF_OWNERS = _literal_owner("FIRST", "first") + _literal_owner(
    "SECOND", "second"
)


def _stale_entries(built) -> list[str]:
    return [
        issue["detail"].split(":", 1)[0]
        for issue in built.issues("stale_curation_entry")
    ]


@pytest.mark.parametrize("blank", [False, True], ids=["reviewed", "with-blank-column"])
def test_native_partition_keeps_the_base_slug_for_every_reviewed_column(
    tmp_path: Path, blank: bool
):
    """A partition mapping every column back to the base ID keeps one base variable."""
    rows = [_row("OLD", 21, "2020"), _row("NEW", 22, "2021")]
    if blank:
        # A quoted empty Kolumnnamn: the literal blank of a member without column.
        rows.append(_row('""', 23, "2022"))
    sources = prepare_sources(
        tmp_path,
        scb_rows=rows,
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + NATIVE_PARTITION},
    )
    built = sources.build(tmp_path)
    assert not built.issues("stale_curation_entry")
    assert built.columns("sample") == {"quantity": {"OLD", "NEW"}}
    reviewed = [u for u in built.uses(sources.records()) if u[0] in {"OLD", "NEW"}]
    assert reviewed == [
        ("NEW", "member:22", "catalog", "scb/sample/quantity"),
        ("OLD", "member:21", "catalog", "scb/sample/quantity"),
    ]


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param([_row("OLD", 21, "2020")], id="reviewed-column-missing"),
        pytest.param(
            [_row("OLD", 21, "2020"), _row("NEW", 22, "2021"), _row("OTHER", 24)],
            id="unreviewed-column-added",
        ),
    ],
)
def test_native_partition_is_stale_when_its_reviewed_columns_change(
    tmp_path: Path, rows
):
    """A native partition guards its complete column set; drift withholds the family."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=rows,
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + NATIVE_PARTITION},
    )
    built = sources.build(tmp_path)
    assert _stale_entries(built) == [
        "curation/registers/scb/sample.toml#/identity.partition/1"
    ]
    assert built.columns("sample") == {}


def test_shared_ref_literal_owners_each_keep_their_own_column(tmp_path: Path):
    """Two owners of one shared source record each own only their literal column."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=SHARED_REF_ROWS,
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + SHARED_REF_OWNERS},
    )
    built = sources.build(tmp_path)
    assert not built.issues()
    assert built.columns("sample") == {"first": {"FIRST"}, "second": {"SECOND"}}
    assert built.uses(sources.records()) == [
        ("FIRST", "member:20", "catalog", "scb/sample/first"),
        ("SECOND", "member:20", "catalog", "scb/sample/second"),
    ]


@pytest.mark.parametrize(
    "future", [False, True], ids=["owned-edition", "later-edition"]
)
def test_scoped_owner_withholds_the_unowned_projection_of_a_shared_record(
    tmp_path: Path, future: bool
):
    """An edition-scoped owner takes only its literal column at its editions.

    The other projection of the same source record, and the same column at a later
    edition, stay with the unnamed base family, whose catalog formation is withheld.
    """
    rows = [_row("CODE", 20), _row("NAME", 20)]
    if future:
        rows.append(_row("CODE", 21, "2021"))
    sources = prepare_sources(
        tmp_path,
        scb_rows=rows,
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + SCOPED_CODE},
    )
    built = sources.build(tmp_path)
    assert not built.issues("stale_curation_entry")
    assert [i["code"] for i in built.issues()] == ["unresolved_catalog_identity"]
    assert built.rows(
        "SELECT v.slug, s.delivery_column_name, s.valid_from, s.valid_to "
        "FROM variable v JOIN variable_state s USING (variable_id)"
    ) == [("code", "CODE", "2020-01-01", "2020-12-31")]
    unowned = [("CODE", "member:21", "catalog", None)] if future else []
    assert built.uses(sources.records()) == [
        ("CODE", "member:20", "catalog", "scb/sample/code"),
        *unowned,
        ("NAME", "member:20", "catalog", None),
    ]


@pytest.mark.parametrize("changed", [None, "unit", "varopdef", "varsource"], ids=str)
def test_guarded_column_owner_is_stale_when_a_role_fact_changes(
    tmp_path: Path, changed: str | None
):
    """An owner guarded by expected role facts applies only while every fact holds."""
    facts = {"unit": "SEK", "varopdef": "Amount", "varsource": "Authority"}
    if changed is not None:
        facts[changed] = "Changed"
    sources = prepare_sources(
        tmp_path,
        scb_rows=[_row("ANSWER", 20, **facts)],
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + GUARDED_ROLE},
    )
    built = sources.build(tmp_path)
    if changed is None:
        assert not built.issues()
        assert built.columns("sample") == {"amount": {"ANSWER"}}
        return
    assert _stale_entries(built) == [
        "curation/registers/scb/sample.toml#/identity.column_owner/1"
    ]
    assert built.columns("sample") == {}


ALTERNATIVE_ROWS = [
    _row("ANSWER", 20, varopdef="Derived amount"),
    _row("ANSWER", 21, varopdef="Sum of components"),
]
ALTERNATIVE_VALUES = ["Belopp|1|1|Ett|20|7001", "Belopp|1|1|Ett|21|7001"]


def _alternatives_owner(records, *, digest_only: bool) -> str:
    """The curator's owner entry, guarded by the complete originals it was read from."""
    text = (
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.10"\n'
        'column = "ANSWER"\nowner = "1.5.amount"\n'
        'ref = "Same physical amount, retain both source operations"\n'
        'source_editions = ["2020"]\n'
        f'expected_evidence_sha256 = "{acknowledgement_evidence_sha256(records)}"\n'
    )
    if digest_only:
        return text
    expected = capture_expectations(
        records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    return (
        text
        + "expected_records = "
        + toml_inline([e.model_dump(mode="json") for e in expected])
        + "\n"
    )


@pytest.mark.parametrize(
    "digest_only", [False, True], ids=["expected-records", "digest-only"]
)
def test_owner_alternatives_keep_every_same_literal_original(
    tmp_path: Path, digest_only: bool
):
    """A guarded owner of one column keeps both same-edition originals as catalog."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=ALTERNATIVE_ROWS,
        vardemangder_rows=ALTERNATIVE_VALUES,
        curation={
            "scb/sample.toml": SCB_SAMPLE
            + SCB_VARIANT
            + '[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n'
        },
    )
    records = sources.records()
    sources.append(
        "scb/sample.toml", _alternatives_owner(records, digest_only=digest_only)
    )
    built = sources.build(tmp_path)
    assert not built.issues()
    assert built.columns("sample") == {"amount": {"ANSWER"}}
    assert built.uses(records) == [
        ("ANSWER", "member:20", "catalog", "scb/sample/amount"),
        ("ANSWER", "member:21", "catalog", "scb/sample/amount"),
    ]


def test_owner_alternatives_are_stale_when_an_original_changes(tmp_path: Path):
    """The owner's recorded originals are a complete guard: a changed one is stale."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=ALTERNATIVE_ROWS,
        vardemangder_rows=ALTERNATIVE_VALUES,
        curation={
            "scb/sample.toml": SCB_SAMPLE
            + SCB_VARIANT
            + '[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n'
        },
    )
    owner = _alternatives_owner(sources.records(), digest_only=False)
    changed = prepare_sources(
        tmp_path / "changed",
        scb_rows=[ALTERNATIVE_ROWS[0], _row("ANSWER", 21, varopdef="Changed")],
        vardemangder_rows=ALTERNATIVE_VALUES,
        curation={
            "scb/sample.toml": SCB_SAMPLE
            + SCB_VARIANT
            + '[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n'
            + owner
        },
    )
    built = changed.build(tmp_path)
    assert _stale_entries(built) == [
        "curation/registers/scb/sample.toml#/identity.column_owner/1"
    ]
    assert built.columns("sample") == {}


@pytest.mark.parametrize(
    ("rows", "declaration", "slug"),
    [
        pytest.param(
            [_row("OLD", 21, "2020"), _row("NEW", 22, "2021")],
            NATIVE_PARTITION,
            "quantity",
            id="native-partition",
        ),
        pytest.param(
            SHARED_REF_ROWS, SHARED_REF_OWNERS, "first", id="shared-ref-first"
        ),
        pytest.param(
            SHARED_REF_ROWS, SHARED_REF_OWNERS, "second", id="shared-ref-second"
        ),
        pytest.param(
            [_row("CODE", 20), _row("NAME", 20)], SCOPED_CODE, "code", id="scoped"
        ),
        pytest.param(
            [_row("ANSWER", 20, unit="SEK", varopdef="Amount", varsource="Authority")],
            GUARDED_ROLE,
            "amount",
            id="guarded-role",
        ),
    ],
)
def test_partition_owner_resolves_a_reference_from_an_unselected_register(
    tmp_path: Path, rows, declaration: str, slug: str
):
    """A scoped build defers a reference to a split owner of an unselected register.

    The unselected register's partition naming is compiled outside the slice; an
    owner it declares is a deferred reference, not a missing catalog dependency.
    """
    sources = prepare_sources(
        tmp_path,
        scb_rows=[*rows, OTHER_ROW],
        curation={
            "scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + declaration,
            "scb/other.toml": OTHER,
        },
    )
    (sources.curation / "relations.toml").write_text(
        f'[[edge]]\ntype = "same_as"\na = "scb/sample/{slug}"\nb = "scb/other/value"\n',
        encoding="utf-8",
    )
    built = sources.build(tmp_path, registers=("2",))
    assert [(i["code"], i["severity"], i["subject"]) for i in built.issues()] == [
        ("deferred_out_of_slice_reference", "warning", "variable_same_as:0")
    ]
    assert f"('variable', 'scb/sample/{slug}')" in built.issues()[0]["detail"]
    assert built.columns("other") == {"value": {"OTHER"}}


def test_owner_alternatives_resolve_a_reference_from_an_unselected_register(
    tmp_path: Path,
):
    """Outside the slice, a guarded owner is checked against its complete originals."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=[*ALTERNATIVE_ROWS, OTHER_ROW],
        vardemangder_rows=ALTERNATIVE_VALUES,
        curation={
            "scb/sample.toml": SCB_SAMPLE
            + SCB_VARIANT
            + '[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n',
            "scb/other.toml": OTHER,
        },
    )
    family = tuple(r for r in sources.records() if r.subject.native.variable_id == 5)
    sources.append("scb/sample.toml", _alternatives_owner(family, digest_only=False))
    (sources.curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/amount"\nb = "scb/other/value"\n',
        encoding="utf-8",
    )
    built = sources.build(tmp_path, registers=("2",))
    assert [(i["code"], i["subject"]) for i in built.issues()] == [
        ("deferred_out_of_slice_reference", "variable_same_as:0")
    ]
