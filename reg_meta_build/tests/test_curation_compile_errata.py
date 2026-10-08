"""SCB errata and enrichment compile independently of loaded register order."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from _curation_compile_support import (
    ALIAS as _ALIAS,
    DESCRIPTION as _DESCRIPTION,
    enrichment_fixture as _enrichment_fixture,
    errata_fixture as _errata_fixture,
    errata_record as _errata_record,
)
from reg_meta_build.curation_compile import compile_enrichment, compile_errata

if TYPE_CHECKING:
    from pathlib import Path


_DELIVERED = (
    '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "A"\n'
    'versions = ["2021"]\nevidence = "accepted delivery"\nnoted = "2026-09-25"\n'
)


def test_errata_and_enrichment_compile_is_order_independent(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    other = _errata_record(column="B", year="2021", variable=6, member=21)
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, other), _DELIVERED)

    def errata_bytes(candidate):
        return repr(
            compile_errata(candidate, prepared, (scope,), {}, subset=False)
        ).encode()

    assert (
        errata_bytes(tree)
        == errata_bytes(tree)
        == errata_bytes(replace(tree, registers=tuple(reversed(tree.registers))))
    )

    record = _errata_record(column="A", year="2020")
    tree, prepared, scope, naming = _enrichment_fixture(
        tmp_path / "enrichment", (record,), _DESCRIPTION + _ALIAS
    )

    def enrichment_bytes(candidate):
        return repr(
            compile_enrichment(
                candidate, prepared, (scope,), naming, {}, {}, subset=False
            )
        ).encode()

    assert (
        enrichment_bytes(tree)
        == enrichment_bytes(tree)
        == enrichment_bytes(replace(tree, registers=tuple(reversed(tree.registers))))
    )
