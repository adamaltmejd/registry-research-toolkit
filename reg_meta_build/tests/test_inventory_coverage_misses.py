"""Delivery-inventory coverage misses against a catalog's delivery states."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

from _shared_fixtures import connect_built_db

if TYPE_CHECKING:
    from pathlib import Path


def test_year_independent_inventory_requires_exact_physical_column(
    fixture_db: Path, tmp_path: Path
) -> None:
    """A year-independent inventory column is covered only by a year-independent
    delivery state of exactly that physical column. Fails if coverage matches a
    year-independent table against another column, or reports such a miss as an
    errata stanza instead of an independent-state worklist line."""
    from reg_meta_build.inventory import DeliveryInventory
    from reg_meta_build.inventory_coverage import (
        coverage_misses,
        errata_stanzas,
        errata_worklist,
    )

    db = tmp_path / "reg_meta.db"
    db.write_bytes(fixture_db.read_bytes())
    inventory = DeliveryInventory.model_validate(
        {
            "version": 1,
            "steward": "example",
            "table": [
                {
                    "id": "dbo.Birth",
                    "edition": "_default",
                    "period_scope": "year_independent",
                    "column": [
                        {
                            "name": "Foo",
                            "mapping": [
                                {
                                    "register_variant": "scb/testreg/individer",
                                    "variable": "scb/testreg/kon",
                                    "representation": "Foo",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    )
    with closing(connect_built_db(db)) as conn:
        # The fixture's `kon` (variable 1, variant 10) gains a year-independent
        # delivery of column Foo beside its dated states.
        state = conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "period_scope, delivery_column_name) "
            "VALUES (1, 10, 'year_independent', 'Foo')"
        ).lastrowid
        assert not coverage_misses(conn, inventory).misses
        conn.execute(
            "UPDATE variable_state SET delivery_column_name = 'Other' "
            "WHERE state_id = ?",
            (state,),
        )
        report = coverage_misses(conn, inventory)

    assert len(report.misses) == 1
    miss = report.misses[0]
    assert miss.period_scope == "year_independent"
    assert not miss.editions and not miss.versions
    assert not report.delivered_misses and not report.errata_column_misses
    assert errata_stanzas(report.misses) == ""
    assert "no exact independent delivery state" in errata_worklist(report)
