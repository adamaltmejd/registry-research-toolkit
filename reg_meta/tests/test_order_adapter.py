"""Order behavior at the library return and manifest-byte boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from order_fixture import (
    conn as conn,  # noqa: PLC0414 - pytest fixture registration
    install_test_holdings,
    inventory as inventory,  # noqa: PLC0414 - pytest fixture registration
    order_project,
    raw_project,
    resolve_project_binding,
)
from reg_meta.catalog import Catalog
from reg_meta.cli import run
from reg_meta.errors import EXIT_CONFIG, EXIT_NO_MATCH, RegMetaError
from reg_meta.inventory import load_inventory
from reg_meta.order import (
    SUPPORTED_SCHEMA_VERSION,
    OrderManifest,
    extraction_filenames,
    load_project,
    materialize_order,
    project_from_raw,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


class TestSupportedSchemaVersion:
    """`project_from_raw`'s supported-version decision — the ONE place any
    consumer decides whether a raw project claims the contract this build reads.

    Synthetic fixtures only: `raw_project`, the same well-shaped finite-period
    project the CLI cases order, with `schema_version` as the sole variable. The
    webapp's `/validate` and `/order` reuse this decision
    (`order.schema_version_issue`), so the fixtures that reject here reject there
    — pinned in `reg_webapp/backend/tests/test_project_order.py`.
    """

    def test_supported_version_is_reg_schemas_own_declaration(self) -> None:
        """Never a second spelling of the contract version: the gate reads the
        schema package's declaration, so the two cannot drift."""
        import reg_schema

        assert SUPPORTED_SCHEMA_VERSION == reg_schema.__version__ == "3.0.0"

    def test_the_current_contract_is_accepted(self) -> None:
        """The ONE accepted claim: exact equality, nothing around it."""
        project = project_from_raw(raw_project(schema_version=SUPPORTED_SCHEMA_VERSION))

        assert project.schema_version == SUPPORTED_SCHEMA_VERSION

    @pytest.mark.parametrize(
        "version",
        ["1.0.0", "2.0.0", "3.0.1", "3.1.0", "99.0.0", "not-a-version", "3.0"],
    )
    def test_every_other_version_is_rejected(self, version) -> None:
        """Old majors, a neighbouring patch, an unreleased minor, an unknown major
        and an unparseable version alike — nothing but the current contract is
        read: one stable code, and a message naming both the rejected claim and
        the contract this build reads."""
        with pytest.raises(RegMetaError) as exc_info:
            project_from_raw(raw_project(schema_version=version))

        error = exc_info.value
        assert error.code == "unsupported_schema_version"
        assert error.exit_code == EXIT_CONFIG
        assert version in error.message
        assert SUPPORTED_SCHEMA_VERSION in error.message

    def test_the_version_decision_precedes_structural_interpretation(self) -> None:
        """A project written for another contract is named as such, not reported
        as a pile of structural noise from reading it as the current one."""
        broken = raw_project(schema_version="1.0.0")
        broken["sources"][0]["period"] = "notaperiod"

        with pytest.raises(RegMetaError) as exc_info:
            project_from_raw(broken)

        assert exc_info.value.code == "unsupported_schema_version"
        assert "invalid_period" not in exc_info.value.message

    @pytest.mark.parametrize("value", [None, 3])
    def test_an_absent_or_mistyped_version_keeps_its_structural_error(
        self, value
    ) -> None:
        """Not this layer's call: no `schema_version` string is a malformed
        DOCUMENT, and the structural layer already says so precisely."""
        raw = raw_project()
        if value is None:
            del raw["schema_version"]
        else:
            raw["schema_version"] = value

        with pytest.raises(RegMetaError) as exc_info:
            project_from_raw(raw)

        assert exc_info.value.code == "project_invalid"
        assert "@/schema_version" in exc_info.value.message


class TestCliAdapter:
    """`reg-meta order` — the CLI adapter over `materialize_order`.

    A thin adapter, so what is pinned here is the adapter contract only:
    the manifest's own canonical bytes reach stdout/`--output` UNCHANGED (never
    the CLI envelope, never `--format`), and each failure gets a stable exit
    code from the existing error classes. The materializer's rules are pinned by
    the classes above; the byte-identity with the FastAPI adapter is pinned in
    `reg_webapp/backend/tests/test_project_order.py` (only there do both
    adapters exist).
    """

    @staticmethod
    def _db_dir(conn: sqlite3.Connection, tmp_path: Path) -> str:
        """The in-memory synthetic catalog, on disk where `--db` can open it."""
        import sqlite3 as _sqlite3

        from reg_meta.db import DB_FILENAME

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        target = _sqlite3.connect(db_dir / DB_FILENAME)
        try:
            conn.backup(target)
        finally:
            target.close()
        return str(db_dir)

    @staticmethod
    def _project_file(tmp_path: Path, path_name: str = "project_data.json", **over):
        import json

        path = tmp_path / path_name
        path.write_text(json.dumps(raw_project(**over)), encoding="utf-8")
        return path

    def test_stdout_carries_the_manifest_bytes_verbatim(
        self, conn, tmp_path, capsys
    ) -> None:
        project = self._project_file(tmp_path)
        expected = materialize_order(load_project(project), conn)

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == 0
        assert expected.manifest is not None
        assert capsys.readouterr().out == expected.manifest.to_json()

    def test_output_flag_writes_the_manifest_to_a_file(
        self, conn, tmp_path, capsys
    ) -> None:
        project = self._project_file(tmp_path)
        out = tmp_path / "order.json"

        code = run(
            [
                "order",
                str(project),
                "--db",
                self._db_dir(conn, tmp_path),
                "--output",
                str(out),
            ]
        )

        assert code == 0
        assert capsys.readouterr().out == ""
        assert OrderManifest.model_validate_json(out.read_text(encoding="utf-8"))

    def test_blocked_order_exits_no_match_naming_every_finding(
        self, conn, tmp_path, capsys
    ) -> None:
        """Fail-closed: a blocked order is the error envelope + exit 17
        (`EXIT_NO_MATCH`), never a partial manifest on stdout."""
        import json

        project = self._project_file(tmp_path, steward="swecov")

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == EXIT_NO_MATCH
        error = json.loads(capsys.readouterr().out)["error"]
        assert error["code"] == "order_blocked"
        assert "steward_mismatch" in error["message"]

    def test_unreadable_project_exits_config(self, conn, tmp_path, capsys) -> None:
        import json

        code = run(
            ["order", str(tmp_path / "nope.json"), "--db", self._db_dir(conn, tmp_path)]
        )

        assert code == EXIT_CONFIG
        assert json.loads(capsys.readouterr().out)["error"]["code"] == (
            "project_unreadable"
        )

    @pytest.mark.parametrize("period", ["notaperiod"])
    def test_structurally_invalid_project_exits_config(
        self, conn, tmp_path, capsys, period
    ) -> None:
        """The shared gate runs before the DB is even opened: a model-valid but
        structurally invalid spec (a bad period token) never materializes."""
        import json

        project = self._project_file(tmp_path)
        spec = json.loads(project.read_text(encoding="utf-8"))
        spec["sources"][0]["period"] = period
        project.write_text(json.dumps(spec), encoding="utf-8")

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == EXIT_CONFIG
        payload = json.loads(capsys.readouterr().out)
        assert payload["error"]["code"] == "project_invalid"
        assert "entries" not in payload

    def test_unsupported_schema_version_exits_config_writing_no_manifest(
        self, conn, tmp_path, capsys
    ) -> None:
        """A project written for another schema contract is rejected at the same
        shared door, before the DB is opened: the error envelope, never a
        manifest for a spec this build cannot read."""
        import json

        project = self._project_file(tmp_path, schema_version="1.0.0")

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == EXIT_CONFIG
        payload = json.loads(capsys.readouterr().out)
        assert payload["error"]["code"] == "unsupported_schema_version"
        assert "entries" not in payload


def independent_kon(conn):
    conn.execute(
        "DELETE FROM variable_state WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, period_scope, valid_from, valid_to, delivery_column_name) SELECT variable_id, 10, 'year_independent', NULL, NULL, 'Kon' FROM variable WHERE slug = 'kon'"
    )


def test_year_independent_reader_and_global_order(conn):
    independent_kon(conn)
    catalog = Catalog(conn)
    assert catalog.resolve_at("scb/lisa/kon", 2020, variant="individer-15plus") == []
    assert catalog.resolve_at("scb/lisa/kon", "_default") == []
    states = catalog.resolve_at("scb/lisa/kon", "_default", variant="individer-15plus")
    assert len(states) == 1
    assert states[0].valid_from is None and states[0].valid_to is None
    assert states[0].period_scope == "year_independent" and not states[0].pooled
    assert len(catalog.states("scb/lisa/kon")) == 1
    resolution = resolve_project_binding(conn, "scb/lisa/kon", "_default")
    assert resolution.finding is None and resolution.columns == {"Kon"}
    assert (
        resolution.availability == ()
        and resolution.slices == ()
        and resolution.clip is None
    )
    result = materialize_order(
        order_project("scb/lisa/kon", period="_default", steward="global"), conn
    )
    assert result.findings == ()
    entry = result.manifest.entries[0]
    assert entry.requested_period == entry.physical.edition == "_default"
    assert entry.physical.column == "Kon"
    assert extraction_filenames(entry) == ("lisa_individer-15plus__default.csv",)
    delivery = catalog.register_variable_deliveries("scb", "lisa")["kon"][0]
    assert delivery.period_scope == "year_independent" and delivery.windows == ()
    assert (
        delivery.coverage.coverage_from is None
        and delivery.coverage.coverage_to is None
    )


def test_year_independent_inventory_order_and_scope_guard(conn, tmp_path):
    independent_kon(conn)
    path = tmp_path / "independent.toml"
    path.write_text("""version = 1
steward = "swecov"
[[table]]
id = "country_groups.csv"
period_scope = "year_independent"
edition = "_default"
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

""")
    inventory = load_inventory(path)
    result = materialize_order(
        order_project("scb/lisa/kon", period="_default"),
        install_test_holdings(conn, inventory),
    )
    assert result.findings == ()
    assert result.manifest.entries[0].physical.table == "country_groups.csv"
    conn.execute(
        "UPDATE variable_state SET period_scope='intervals', valid_from='2020-01-01', valid_to='2020-12-31' WHERE delivery_column_name='Kon'"
    )
    assert (
        resolve_project_binding(conn, "scb/lisa/kon", "_default").finding.code
        == "binding_unavailable"
    )


@pytest.mark.parametrize(
    "period", ["_default", ["_default"], {"from": "_default", "to": 2020}]
)
def test_year_independent_source_requires_whole_period_and_concrete_variant(period):
    raw = raw_project()
    raw["sources"][0]["period"] = period
    if period == "_default":
        assert project_from_raw(raw).sources[0].period == "_default"
        raw["sources"][0]["register_variant"] = "scb/lisa/_default"
    with pytest.raises(RegMetaError):
        project_from_raw(raw)


def test_year_independent_delivery_counts_all_source_states(conn):
    independent_kon(conn)
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, period_scope, valid_from, valid_to, delivery_column_name, value_set_version_label) SELECT variable_id, 10, 'year_independent', NULL, NULL, 'Kon', 'second-version' FROM variable WHERE slug='kon'"
    )
    delivery = Catalog(conn).register_variable_deliveries("scb", "lisa")["kon"][0]
    assert delivery.coverage.state_count == 2
    assert delivery.windows == () and delivery.coverage.coverage_from is None
