"""Complete-scope composition checks curation before catalog dependencies: ordinary formation, delivery metadata, checked lists and copied coding."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from _source_scope_support import names, record, resolve
from reg_meta_build.curation_compile import (
    compile_scb_preliminary,
)
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coordinates import (
    native_variable_key,
)


def test_superseded_scb_preliminary_is_support_and_final_alone_forms_state():
    preliminary = record(
        edition_name="2020, preliminär version", edition_id=10, data_length="2"
    )
    preliminary_only = record(
        2,
        variable=6,
        column="OTHER",
        edition_name="2020, preliminär version",
        edition_id=10,
    )
    final = record(3, edition_name="2020, slutlig version", edition_id=11)
    records = (preliminary, preliminary_only, final)
    prepared = SimpleNamespace(
        records=SimpleNamespace(iter_records=lambda *, source: iter(records))
    )
    scope = CompiledScope(
        source=preliminary.source, register_key=None, naming=names(records)
    )
    (case,) = compile_scb_preliminary(cast("Any", prepared), (scope,))[
        preliminary.source, None
    ]
    result = resolve(records, cases=(case,))
    assert result.corrections.accounting[0].disposition == "applied"
    assert [item.use for item in result.corrections.occurrences] == [
        "support",
        "catalog",
        "catalog",
    ]
    assert result.corrections.occurrences[0].source_records == (preliminary,)
    shared = result.variables[native_variable_key(final)]
    exclusive = result.variables[native_variable_key(preliminary_only)]
    assert shared is not None and exclusive is not None
    assert len(shared.states) == len(exclusive.states) == 1
    assert shared.states[0].data_length == "1"
    assert exclusive.states[0].delivery_column_name == "OTHER"
