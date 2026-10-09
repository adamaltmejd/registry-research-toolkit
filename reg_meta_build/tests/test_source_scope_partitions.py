"""Complete-scope composition: an alias window checks competing owners across the whole scope.

The file's other tests moved to build cases (stage 7b-2). This one stays until its
replacement, the CONTESTED claim of #1306's
`representation-alias-windows-apply-only-to-owned-covered-uncontested-columns` build
case, is on main; delete the file with or after that case.
"""

from __future__ import annotations

from _source_scope_support import guard, record, resolve
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    AliasWindowDecision,
    CurationCase,
    capture_expectations,
)


def test_alias_window_checks_competing_variables_in_the_whole_scope():
    first, second = record(), record(2, variable=6)
    variable_key, variant_key = native_variable_key(first), native_variant_key(first)
    assert variable_key is not None and variant_key is not None
    case = CurationCase(
        case_id="accepted-window",
        targets=capture_expectations((first,), fields=("column_name",)),
        peer_guards=(guard(first),),
        decision=AliasWindowDecision(
            reviewed=True,
            variable_key=variable_key,
            variant_key=variant_key,
            column="VALUE",
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            reason="Existing bounded representation",
            provenance="Fixture decision",
        ),
    )
    result = resolve((first, second), cases=(case,))
    assert [e.status for e in result.evaluations] == ["applicable"]
    assert [d.code for d in result.diagnostics] == ["conflicting_alias_window_owner"]
    assert all(not v.aliases for v in result.variables.values())
