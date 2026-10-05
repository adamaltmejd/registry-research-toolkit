"""Holdings read paths: indexed query plans and per-generation provider pages.

Query plans are artifact-plus-reader behavior: the statements are captured from public
reader operations on the test's own connection and planned against the same artifact.
Runs on both synthetic kinds or on a tier-3 artifact. Failure messages carry counts,
never identifiers.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from pathlib import Path

import pytest
from artifact_requests import require
from fastapi.testclient import TestClient
from reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import get_manifest, open_db
from reg_webapp.app import create_app

STEWARDS = Path(__file__).resolve().parents[1] / "reg_webapp/stewards"
_KEYWORDS = {"JOIN", "WHERE", "ON", "USING", "LEFT", "INNER", "GROUP", "ORDER"}


def _held_binding(conn):
    return conn.execute(
        "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM holding_mapping hm "
        "JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) "
        "JOIN variable v USING(variable_id) JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) "
        "WHERE ht.scope != 'unknown' AND v.slug IS NOT NULL AND r.slug IS NOT NULL "
        "AND EXISTS (SELECT 1 FROM variable_state vs WHERE vs.variable_id = v.variable_id "
        "AND vs.delivery_column_name IS NOT NULL) "
        "ORDER BY 1 LIMIT 1"
    ).fetchone()[0]


def _plans(conn, statements):
    plans = []
    for sql in dict.fromkeys(statements):
        if sql.lstrip().upper().startswith(("SELECT", "WITH")):
            detail = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql)]
            plans.append((sql, detail))
    return plans


def test_holdings_admission_and_spellings_use_indexed_plans(artifact_dir):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        if get_manifest(conn)["catalog_artifact_kind"] != "steward":
            pytest.skip("holdings scope needs a steward artifact")
        binding = _held_binding(conn)
        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        catalog = Catalog(conn, scope="holdings")
        catalog.catalog_sizes()
        for provider in catalog.list_providers():
            catalog.resolve(provider.fqid)
            for register in catalog.list_registers(str(provider.fqid)):
                catalog.resolve(register.fqid)
        catalog.resolve(binding)
        conn.set_trace_callback(None)
        plans = _plans(conn, statements)

    scans = 0
    for sql, detail in plans:
        aliases = {"holding_mapping"} | (
            set(re.findall(r"\bholding_mapping\s+(?:AS\s+)?(\w+)", sql, re.IGNORECASE))
            - _KEYWORDS
        )
        scans += sum(
            1
            for step in detail
            if step.startswith("SCAN ") and step.split()[1] in aliases
        )
    require(
        scans == 0,
        f"{scans} full holding_mapping scans in holdings admission plans",
    )
    spellings = [
        detail
        for sql, detail in plans
        if re.search(r"SELECT delivery_column_name FROM variable_state\b", sql)
    ]
    require(spellings, "No delivery-column spelling lookup was captured")
    require(
        all(
            any(re.search(r"\bidx_variable_state_variable\b", step) for step in detail)
            for detail in spellings
        ),
        "Delivery-column spelling lookup is not keyed by variable",
    )


@contextmanager
def _client(monkeypatch, directory: Path):
    """A freshly booted app over `directory`: a new app holds no earlier memo."""
    with open_db(directory / "reg_meta.db") as conn:
        steward = get_manifest(conn).get("steward", "global")
    monkeypatch.setenv("REG_META_DB", str(directory))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", steward)
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(STEWARDS))
    with TestClient(create_app(rate_limit_per_minute=100000)) as client:
        yield client


def _assert_provider_pages(client, directory: Path, scopes) -> dict:
    """Each provider page repeats byte for byte and its register coverage agrees
    with the reader's `provider_register_coverage` for the requested scope."""
    expected_by_scope = {}
    with open_db(directory / "reg_meta.db") as conn:
        for scope in scopes:
            catalog = Catalog(conn, scope=scope)
            for provider in catalog.list_providers():
                slug = str(provider.fqid)
                expected = {
                    register: coverage.model_dump(mode="json")
                    for register, coverage in catalog.provider_register_coverage(
                        slug
                    ).items()
                }
                first, again = (
                    client.get("/api/catalog/" + slug, params={"scope": scope})
                    for _ in range(2)
                )
                require(
                    first.status_code == 200 and first.content == again.content,
                    "Repeated provider request changed its response",
                )
                served = {
                    child["fqid"].split("/")[1]: child["coverage"]
                    for child in first.json()["children"]
                }
                require(
                    all(served[r] == expected.get(r) for r in served),
                    "Provider page coverage disagrees with the reader",
                )
                expected_by_scope[(scope, slug)] = expected
    return expected_by_scope


def test_provider_pages_repeat_and_follow_the_requested_scope(
    artifact_dir, monkeypatch
):
    with open_db(artifact_dir / "reg_meta.db") as conn:
        held = get_manifest(conn)["catalog_artifact_kind"] == "steward"
    scopes = ("holdings", "reference", "holdings") if held else ("reference",)
    with _client(monkeypatch, artifact_dir) as client:
        _assert_provider_pages(client, artifact_dir, scopes)


def test_provider_pages_are_not_shared_across_generations(tmp_path, monkeypatch):
    """Two artifacts whose reference coverage differs, booted one after the other
    in one process: the second serves its own coverage, not the first's."""

    def build(name, fixture, kind):
        return build_reader_artifact(
            tmp_path / name,
            fixture,
            kind,
            identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
        ).parent

    expected = []
    for directory in (
        build("steward", "reader", "steward"),
        build("catalog", "reader/open-ended", "catalog"),
    ):
        with _client(monkeypatch, directory) as client:
            expected.append(_assert_provider_pages(client, directory, ("reference",)))
    require(expected[0] != expected[1], "Fixture generations do not differ")
