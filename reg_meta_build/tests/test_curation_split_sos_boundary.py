"""SOS identity splits and renames at the build boundary.

Every case writes a synthetic SOS workbook (register PAR, derived from the default
SYN fixture by `sos_register`) and a curation TOML, builds through the real
pipeline, and asserts on the built catalog, the issue ledger and each source
occurrence's ledger disposition. In a workbook, every row of one variable in one
Deldatamängd shares one semantic source record key.
"""

from __future__ import annotations

from ast import literal_eval
from typing import TYPE_CHECKING

import pytest
from _curation_support_boundary_support import (
    prepare_sources,
    sos_head,
    sos_id,
    sos_register,
)

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "Socialstyrelsen/Metadata Patientregistret (PAR)_webb.xlsx"
TITLE = "Patientregistret"
REG = sos_id("par", "register")
SUBSET_PARTS = (
    f'{{ deldatamangd = "PAR_OV", owner = "{REG}.ATC.shared" }}',
    f'{{ deldatamangd = "PAR_SV", owner = "{REG}.ATC.shared" }}',
    f'{{ deldatamangd = "PAR_TV", owner = "{REG}.ATC.other" }}',
)
OWNERS = (
    f'[[variable]]\nnative_id = "{REG}.ATC.shared"\nslug = "shared"\n'
    f'[[variable]]\nnative_id = "{REG}.ATC.other"\nslug = "other"\n'
)
TYPE_SPLIT = (
    '[[identity.split]]\nvariable = "ATC"\nby = "data_type"\n'
    f'parts = [{{ data_type = "integer", owner = "{REG}.ATC.atc" }}, '
    f'{{ data_type = "text", owner = "{REG}.ATC.atc-1" }}]\n'
    f'[[variable]]\nnative_id = "{REG}.ATC.atc"\nslug = "atc"\n'
    f'[[variable]]\nnative_id = "{REG}.ATC.atc-1"\nslug = "atc-1"\n'
)
TYPE_ROWS = (
    {"name": "ATC", "deldatamangd": "PAR_OV", "label": "ATC", "data_type": "Heltal"},
    {"name": "ATC", "deldatamangd": "PAR_OV", "label": "ATC"},
)
RENAME = (
    '[[identity.rename]]\ndeldatamangd = "PAR_OV"\nvariable = "INVARN8"\n'
    'name = "target"\ncolumn = "INVARN9"\n'
    f'[[variable]]\nnative_id = "{REG}.INVARN9"\nslug = "new-name"\n'
)
RENAME_ROWS = (
    {
        "name": "INVARN8",
        "deldatamangd": "PAR_OV",
        "label": "target",
        "data_type": "Heltal",
    },
)
SUBSETS = ("PAR_OV", "PAR_SV", "PAR_TV")


def _subset_split(parts=SUBSET_PARTS) -> str:
    return (
        '[[identity.split]]\nvariable = "ATC"\nby = "deldatamangd"\n'
        f"parts = [{', '.join(parts)}]\n" + OWNERS
    )


def _text_split(field: str, parts=("First", "Second", "Other")) -> str:
    owners = {"First": "shared", "Second": "shared", "Other": "other"}
    return (
        f'[[identity.split]]\nvariable = "ATC"\nby = "{field}"\n'
        "parts = ["
        + ", ".join(
            f'{{ {field} = "{text}", owner = "{REG}.ATC.{owners[text]}" }}'
            for text in parts
        )
        + "]\n"
        + OWNERS
    )


def _text_rows(field: str, texts) -> tuple[dict, ...]:
    """One PAR_OV ``ATC`` row per text; all rows share one source record key."""
    other = "description" if field == "label" else "label"
    return tuple(
        {"name": "ATC", "deldatamangd": "PAR_OV", field: text, other: "ATC"}
        for text in texts
    )


def _sources(tmp_path: Path, rows, declaration: str, subsets=("PAR_OV",)):
    return prepare_sources(
        tmp_path,
        sos_registers=(sos_register("PAR", TITLE, rows, subsets),),
        curation={"sos/par.toml": sos_head("par", TITLE, subsets) + declaration},
    )


def _atc(sources) -> tuple:
    """The prepared workbook records of variable ATC."""
    return tuple(
        r for r in sources.records(SOURCE) if r.subject.variable.native_id == "ATC"
    )


def _states(built) -> list[tuple]:
    return built.rows(
        "SELECT v.slug, rv.slug, s.delivery_column_name, s.data_type, v.name "
        "FROM variable v JOIN register r USING (register_id) "
        "JOIN variable_state s USING (variable_id) "
        "JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id "
        "WHERE r.slug = 'par' ORDER BY 1, 2, 4"
    )


def _errors(built) -> list[tuple[str, str]]:
    return [
        (issue["code"], issue["detail"].split(":", 1)[0]) for issue in built.errors()
    ]


def test_type_split_gives_each_delivered_type_its_owner(tmp_path: Path):
    """A data-type split sends each typed row of one source record to its owner."""
    sources = _sources(tmp_path, TYPE_ROWS, TYPE_SPLIT)
    built = sources.build(tmp_path)
    assert not _errors(built)
    assert _states(built) == [
        ("atc", "par-ov", "ATC", "integer", "ATC"),
        ("atc-1", "par-ov", "ATC", "text", "ATC"),
    ]
    assert built.uses(_atc(sources), "data_type") == [
        ("integer", "variable:ATC", "catalog", "sos/par/atc"),
        ("text", "variable:ATC", "catalog", "sos/par/atc-1"),
    ]


def test_rename_moves_the_delivered_column_to_its_named_owner(tmp_path: Path):
    """A rename gives the exact named subset row its corrected column and owner."""
    sources = _sources(tmp_path, RENAME_ROWS, RENAME)
    built = sources.build(tmp_path)
    assert not _errors(built)
    assert not built.issues("stale_curation_entry")
    assert _states(built) == [("new-name", "par-ov", "INVARN9", "integer", "target")]


@pytest.mark.parametrize("reverse", [False, True], ids=["declared", "reversed"])
def test_subset_split_groups_subsets_by_declared_owner(tmp_path: Path, reverse):
    """Subsets declared for one owner share it, in either declaration order."""
    parts = SUBSET_PARTS[::-1] if reverse else SUBSET_PARTS
    rows = tuple(
        {"name": "ATC", "deldatamangd": subset, "label": "ATC"} for subset in SUBSETS
    )
    built = _sources(tmp_path, rows, _subset_split(parts), SUBSETS).build(tmp_path)
    assert not _errors(built)
    assert _states(built) == [
        ("other", "par-tv", "ATC", "text", "ATC"),
        ("shared", "par-ov", "ATC", "text", "ATC"),
        ("shared", "par-sv", "ATC", "text", "ATC"),
    ]
    # The shared owner's native identity carries its first declared subset
    # (PAR_OV, the deleted IR test's literal) whichever order the parts are
    # declared in; each row's classification warning subject in the ledger (a
    # tuple rendered as text) names the row's subset and that native key.
    native_subset = sorted(
        (
            subject[subject.index("variant") + 2],
            subject[subject.index("accepted-shape") + 1],
        )
        for subject in (
            literal_eval(issue["subject"])
            for issue in built.issues("unknown_classification_declaration")
        )
    )
    assert native_subset == [
        ("PAR_OV", "PAR_OV"),
        ("PAR_SV", "PAR_OV"),
        ("PAR_TV", "PAR_TV"),
    ]


@pytest.mark.parametrize(
    "delivered",
    [("PAR_OV", "PAR_TV"), ("PAR_OV", "PAR_SV", "PAR_TV", "UNDECLARED")],
    ids=["declared-subset-missing", "undeclared-subset"],
)
def test_subset_split_is_stale_unless_the_delivered_subsets_match(
    tmp_path: Path, delivered
):
    """A subset split applies only to exactly its declared subsets; otherwise stale."""
    rows = tuple(
        {"name": "ATC", "deldatamangd": subset, "label": "ATC"} for subset in delivered
    )
    built = _sources(tmp_path, rows, _subset_split(), delivered).build(tmp_path)
    assert [code for code, _ in _errors(built)] == [
        "stale_curation_entry",
        "unresolved_catalog_identity",
    ]
    assert _errors(built)[0][1] == "curation/registers/sos/par.toml#/identity.split/1"
    assert _states(built) == []


@pytest.mark.parametrize("reverse", [False, True], ids=["declared", "reversed"])
@pytest.mark.parametrize("field", ["name", "description"])
def test_text_split_assigns_rows_of_one_source_record_by_text(
    tmp_path: Path, field: str, reverse: bool
):
    """A name or description split routes rows sharing one source key by their text.

    Two different texts under one owner in one subset conflict, so the shared
    owner's catalog output carries resolution errors (a name split withholds it:
    its rows' ledger variable is empty); every error is the shared owner's.
    """
    column = "label" if field == "name" else "description"
    parts = ("First", "Second", "Other")
    sources = _sources(
        tmp_path,
        _text_rows(column, parts),
        _text_split(field, parts[::-1] if reverse else parts),
    )
    built = sources.build(tmp_path)
    assert not built.issues("stale_curation_entry")
    shared = None if field == "name" else "sos/par/shared"
    assert built.uses(_atc(sources), field) == [
        ("First", "variable:ATC", "catalog", shared),
        ("Other", "variable:ATC", "catalog", "sos/par/other"),
        ("Second", "variable:ATC", "catalog", shared),
    ]
    assert {i["subject"] for i in built.issues() if i["severity"] == "error"} == {
        "sos/par/shared"
    }
    assert "other" in [row[0] for row in _states(built)]


@pytest.mark.parametrize(
    "texts",
    [
        ("First", "Other"),
        ("First", "Second", "Other", "New"),
        ("First", "Second", None),
    ],
    ids=["declared-text-missing", "undeclared-text", "blank-text"],
)
@pytest.mark.parametrize("field", ["name", "description"])
def test_text_split_is_stale_unless_the_delivered_texts_match(
    tmp_path: Path, field: str, texts
):
    """A text split applies only when the record's texts are exactly the declared."""
    column = "label" if field == "name" else "description"
    sources = _sources(tmp_path, _text_rows(column, texts), _text_split(field))
    built = sources.build(tmp_path)
    assert [code for code, _ in _errors(built)] == [
        "stale_curation_entry",
        "unresolved_catalog_identity",
    ]
    assert _errors(built)[0][1] == "curation/registers/sos/par.toml#/identity.split/1"
    assert {use[3] for use in built.uses(_atc(sources), field)} == {None}


@pytest.mark.parametrize(
    ("rows", "declaration", "subsets", "slug"),
    [
        pytest.param(TYPE_ROWS, TYPE_SPLIT, ("PAR_OV",), "atc-1", id="type-split"),
        pytest.param(RENAME_ROWS, RENAME, ("PAR_OV",), "new-name", id="rename"),
        pytest.param(
            tuple({"name": "ATC", "deldatamangd": s, "label": "ATC"} for s in SUBSETS),
            _subset_split(),
            SUBSETS,
            "other",
            id="subset-split",
        ),
        pytest.param(
            _text_rows("label", ("First", "Second", "Other")),
            _text_split("name"),
            ("PAR_OV",),
            "other",
            id="name-split",
        ),
    ],
)
def test_split_owner_resolves_a_reference_from_an_unselected_register(
    tmp_path: Path, rows, declaration: str, subsets, slug: str
):
    """A scoped SCB build defers a reference to an unselected SOS split owner."""
    sources = _sources(tmp_path, rows, declaration, subsets)
    (sources.curation / "relations.toml").write_text(
        f'[[edge]]\ntype = "same_as"\na = "sos/par/{slug}"\nb = "scb/sample/value"\n',
        encoding="utf-8",
    )
    built = sources.build(tmp_path, registers=("1",))
    assert [(i["code"], i["subject"]) for i in built.issues()] == [
        ("deferred_out_of_slice_reference", "variable_same_as:0")
    ]
    assert f"('variable', 'sos/par/{slug}')" in built.issues()[0]["detail"]
