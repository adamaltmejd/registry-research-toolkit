"""Concept groups on the catalog/CLI surfaces (#322 / #325).

`get groups` lists a register's families with member facets, `get schema`
annotates member columns inline, and the CLI renders group rows in its
`--format json` envelope and `--format list` text.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from groups_test_support import seeded_conn as _seeded_conn
from reader_artifacts import build_reader_artifact, stamp_catalog_identity
from reg_meta.catalog import Catalog, GroupAxis
from reg_meta.cli import run
from reg_meta.db import SCHEMA_VERSION
from reg_meta.errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from reg_meta.queries import (
    get_classification_concept_groups,
    get_concept_groups,
    get_schema,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestGetConceptGroups:
    def test_lists_register_groups_with_members_and_facets(self) -> None:
        conn = _seeded_conn()
        data = get_concept_groups(conn, "LISA")
        (reg,) = data["registers"]
        assert reg["register_id"] == 1
        assert reg["register_name"] == "LISA"
        assert reg["fqid"] == "scb/lisa"
        (group,) = reg["groups"]
        assert group["key"] == "agiink"
        assert group["source"] == "curated"
        # #819: axes serialize as {name, label} — the authored label ('månad') is
        # exposed distinct from the stable match name.
        assert group["axes"] == [{"name": "month", "label": "månad"}]
        assert group["member_count"] == 3
        jan = group["members"][0]
        assert jan["fqid"] == "scb/lisa/agiinkjan"
        assert jan["name"] == "Lönesumma januari"
        assert jan["facets"] == [{"axis": "month", "value": "01", "label": "januari"}]

    def test_multi_axis_representation_members(self) -> None:
        # #819: a multi-axis group declares N axes; members are
        # (variable, delivery_column) representations carrying one facet per axis,
        # and two members can share an fqid (one variable, two delivery columns).
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (30, 'variable', 1, 'disp', 'Disp', 'curated')"
        )
        for ordinal, axis in enumerate(("enhet", "kapitalvinst")):
            conn.execute(
                "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
                "VALUES (30, ?, ?, ?)",
                (axis, ordinal, axis.title()),
            )
        add_variable(conn, register_id=1, var_id=950, name="Disp ink", slug="cdisp")
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE slug = 'cdisp'"
        ).fetchone()[0]
        for dcn, kap in (("CDISP", ("inkl", "Inkl")), ("CDISP5", ("exkl", "Exkl"))):
            cur = conn.execute(
                "INSERT INTO concept_group_variable "
                "(group_id, variable_id, delivery_column_name) VALUES (30, ?, ?)",
                (vid, dcn),
            )
            conn.executemany(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, ?, ?, ?)",
                [
                    (cur.lastrowid, "enhet", "individ", "Individ"),
                    (cur.lastrowid, "kapitalvinst", kap[0], kap[1]),
                ],
            )
        conn.commit()
        groups = Catalog(conn).list_concept_groups("scb", "lisa")
        (group,) = [g for g in groups if g.key == "disp"]
        # The authored labels ride each axis (title-cased here), keyed on the stable
        # axis name (#819).
        assert group.axes == (
            GroupAxis(name="enhet", label="Enhet"),
            GroupAxis(name="kapitalvinst", label="Kapitalvinst"),
        )
        assert len(group.members) == 2
        # Both members share the fqid but differ by delivery_column.
        assert {str(m.fqid) for m in group.members} == {"scb/lisa/cdisp"}
        assert {m.delivery_column for m in group.members} == {"CDISP", "CDISP5"}
        # Each member carries one facet per declared axis, ordered by axis ordinal.
        for m in group.members:
            assert [f.axis for f in m.facets] == ["enhet", "kapitalvinst"]

    def test_resolves_register_by_numeric_id(self) -> None:
        conn = _seeded_conn()
        data = get_concept_groups(conn, "1")
        assert data["registers"][0]["groups"][0]["key"] == "agiink"

    def test_register_without_groups_is_empty_list(self) -> None:
        conn = build_slugged_db()  # no groups seeded
        data = get_concept_groups(conn, "LISA")
        assert data["registers"][0]["groups"] == []

    def test_unknown_register_raises_not_found(self) -> None:
        conn = _seeded_conn()
        with pytest.raises(RegMetaError) as exc:
            get_concept_groups(conn, "NOPE")
        assert exc.value.exit_code == EXIT_NOT_FOUND

    def test_classification_groups(self) -> None:
        conn = _seeded_conn()
        data = get_classification_concept_groups(conn)
        (group,) = data["groups"]
        assert group["key"] == "sun"
        assert group["axes"] == [{"name": "vintage", "label": "vintage"}]
        assert [m["fqid"] for m in group["members"]] == [
            "class/sun2000",
            "class/sun2020",
        ]


class TestSchemaAnnotation:
    def test_member_column_carries_group_key_and_label(self) -> None:
        conn = _seeded_conn()
        data = get_schema(conn, register="LISA")
        cols = {
            c["variable_name"]: c
            for v in data["variants"]
            for ver in v["versions"]
            for c in ver["columns"]
        }
        grouped = cols["Lönesumma januari"]
        assert grouped["concept_group"] == "agiink"
        assert grouped["concept_group_label"] == "Lönesumma per månad"
        ungrouped = cols["Kön"]
        assert ungrouped["concept_group"] is None
        assert ungrouped["concept_group_label"] is None

    def test_multi_representation_member_yields_one_schema_column(self) -> None:
        # #819 regression: a grouped variable with N representation members (one
        # `concept_group_variable` row per delivery column, the multi-axis shape)
        # must surface as ONE schema column, not N. The plain
        # `LEFT JOIN concept_group_variable` fanned each `variable_state` out into
        # one column per member row; the `SELECT DISTINCT variable_id, group_id`
        # subquery collapses it. The single NULL-member fixture in the test above
        # can't catch this — it needs two member rows on one variable.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (30, 'variable', 1, 'disp', "
            "'Disponibel inkomst', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (30, 'kapitalvinst', 0, 'Kapitalvinst')"
        )
        add_variable(conn, register_id=1, var_id=950, name="Disp ink", slug="cdisp")
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'cdisp'"
        ).fetchone()[0]
        # Two representation members: same variable_id, two delivery columns →
        # two `concept_group_variable` rows in the one group.
        for dcn, (value, label) in (
            ("CDISP", ("inkl", "Inkl")),
            ("CDISP5", ("exkl", "Exkl")),
        ):
            cur = conn.execute(
                "INSERT INTO concept_group_variable "
                "(group_id, variable_id, delivery_column_name) VALUES (30, ?, ?)",
                (vid, dcn),
            )
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, 'kapitalvinst', ?, ?)",
                (cur.lastrowid, value, label),
            )
        # One `variable_state` for the variable under variant 10.
        add_state(
            conn,
            register_id=1,
            variable_slug="cdisp",
            register_variant_id=10,
            valid_from="2018-01-01",
            valid_to="9999-12-31",
            delivery_column_name="CDISP",
        )
        conn.commit()

        data = get_schema(conn, register="LISA")
        disp_cols = [
            c
            for v in data["variants"]
            for ver in v["versions"]
            for c in ver["columns"]
            if c["variable_id"] == vid
        ]
        # Exactly ONE column for the variable — not one per member row.
        assert len(disp_cols) == 1
        assert disp_cols[0]["concept_group"] == "disp"
        assert disp_cols[0]["concept_group_label"] == "Disponibel inkomst"


# ── CLI surface (envelope + flags); the doc DB is optional for these commands ──


@pytest.fixture(scope="module")
def groups_db_dir(tmp_path_factory: pytest.TempPathFactory) -> str:
    conn = _seeded_conn()
    conn.execute(
        "INSERT INTO import_manifest VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    stamp_catalog_identity(conn)
    conn.commit()
    db_dir = tmp_path_factory.mktemp("groups_db")
    on_disk = sqlite3.connect(db_dir / "reg_meta.db")
    conn.backup(on_disk)
    on_disk.close()
    conn.close()
    return str(db_dir)


def _run_json(argv: list[str]) -> tuple[dict, int]:
    import io
    import sys

    from reg_meta.cli import run

    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        exit_code = run(["--format", "json", *argv])
    finally:
        sys.stdout = old_stdout
    output = buf.getvalue()
    return (json.loads(output) if output.strip() else {}), exit_code


class TestCliGroups:
    def test_get_groups_register(self, groups_db_dir: str) -> None:
        data, code = _run_json(["--db", groups_db_dir, "get", "groups", "LISA"])
        assert code == 0
        (reg,) = data["registers"]
        assert reg["fqid"] == "scb/lisa"
        assert [g["key"] for g in reg["groups"]] == ["agiink"]
        assert reg["groups"][0]["members"][0]["fqid"] == "scb/lisa/agiinkjan"

    def test_get_groups_classifications(self, groups_db_dir: str) -> None:
        data, code = _run_json(
            ["--db", groups_db_dir, "get", "groups", "--classifications"]
        )
        assert code == 0
        assert [g["key"] for g in data["groups"]] == ["sun"]

    def test_get_groups_requires_exactly_one_target(self, groups_db_dir: str) -> None:
        data, code = _run_json(["--db", groups_db_dir, "get", "groups"])
        assert code == EXIT_USAGE
        assert data["error"]["code"] == "usage_error"
        data, code = _run_json(
            ["--db", groups_db_dir, "get", "groups", "LISA", "--classifications"]
        )
        assert code == EXIT_USAGE

    def test_search_folds_by_default(self, groups_db_dir: str) -> None:
        data, code = _run_json(
            ["--db", groups_db_dir, "search", "--query", "Lönesumma"]
        )
        assert code == 0
        groups = [r for r in data["results"] if r["type"] == "group"]
        assert [g["group_key"] for g in groups] == ["agiink"]

    def test_search_no_fold_flag(self, groups_db_dir: str) -> None:
        data, code = _run_json(
            ["--db", groups_db_dir, "search", "--query", "Lönesumma", "--no-fold"]
        )
        assert code == 0
        assert all(r["type"] != "group" for r in data["results"])
        assert len(data["results"]) == 3


# ── CLI list text over a readable source ─────────────────────────────────────


@pytest.fixture(scope="module")
def group_cli_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_reader_artifact(
        tmp_path_factory.mktemp("concept-group-cli"),
        "reader/concept-group-cli",
        "catalog",
    ).parent


class TestCliListDisplay:
    def test_search_group_rows_render_with_counts(
        self, group_cli_db: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        capsys.readouterr()
        code = run(
            [
                "--db",
                str(group_cli_db),
                "--format",
                "list",
                "search",
                "--query",
                "Lönesumma",
                "--field",
                "varname",
            ]
        )
        assert code == 0
        # Pure-group results use the dedicated column set: identity + counts.
        assert capsys.readouterr().out == (
            "  group_key      payroll\n"
            "  group_label    Lönesumma per månad\n"
            "  source         curated\n"
            "  register_name  Example\n"
            "  matched        3\n"
            "  members        3\n"
        )

    def test_search_group_row_projects_match_label_in_mixed_results(
        self, group_cli_db: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        capsys.readouterr()
        code = run(
            [
                "--db",
                str(group_cli_db),
                "--format",
                "list",
                "search",
                "--query",
                "Lön",
                "--field",
                "varname",
            ]
        )
        assert code == 0
        # Mixed with a leaf, the group row shares the leaf columns and projects its
        # match count into the name column.
        assert capsys.readouterr().out == (
            "  type           group\n"
            "  register_name  Example\n"
            "  var_id         \n"
            "  variable_name  Lönesumma per månad (3/3 members matched)\n"
            "  group          payroll\n"
            "\n"
            "  type           varname\n"
            "  register_name  Example\n"
            "  var_id         60\n"
            "  variable_name  Lön totalt\n"
            "  group          \n"
        )

    def test_get_groups_list_renders_one_record_per_group(
        self, group_cli_db: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        capsys.readouterr()
        code = run(
            [
                "--db",
                str(group_cli_db),
                "--format",
                "list",
                "get",
                "groups",
                "scb/example",
            ]
        )
        assert code == 0
        # #819: the axes column shows the authored axis LABEL ('månad'), not the
        # stable match key ('month').
        assert capsys.readouterr().out == (
            "  register   Example\n"
            "  group_key  payroll\n"
            "  label      Lönesumma per månad\n"
            "  source     curated\n"
            "  axes       månad\n"
            "  members    3\n"
        )
