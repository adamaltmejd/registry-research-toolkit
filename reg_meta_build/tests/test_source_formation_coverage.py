"""Year-independent native formation: the one formation leg no build case reaches.

Only the LISA reader delivers a year-independent edition (`sources/lisa.py`), and the
`cases/build` runner has no LISA source, so this stays a direct formation test (the
same reason `test_catalog_lineage.py` keeps its year-independent legs). The other
formation behaviors this file pinned are legs of
`cases/build/formation-native-families-keep-literal-deliveries-or-stay-unresolved` or
build cases that already asserted them.
"""

from __future__ import annotations

from _source_formation_support import (
    form_family as _form,
    formation_record as _record,
)
from reg_meta_build.source_coding import CodeListClaim, CodeMembershipClaim
from reg_meta_build.source_records import TemporalScope


def test_independent_delivery_forms_without_calendar_dates_and_retains_source():
    """Fails if formation dates a year-independent delivery, marks it pooled, drops
    its year-independent list, or owes the catalog a dated coverage claim for it."""
    scope = TemporalScope(kind="year_independent")
    record = _record(2020).model_copy(
        update={"edition_scope": scope, "edition_period_scope": scope}
    )
    claim = CodeListClaim(
        "native-list",
        scope,
        (CodeMembershipClaim("02", "EU25 utom Norden", scope),),
        "EU25",
    )
    formed = _form((record,), claims=(claim,))
    assert formed.diagnostics == ()
    assert formed.variable is not None
    (state,) = formed.variable.states
    assert state.period_scope == "year_independent"
    assert state.valid_from is state.valid_to is None
    assert state.pooled is False
    assert state.value_set is not None
    assert state.value_set.members == (("02", "EU25 utom Norden"),)
    (obligation,) = formed.coverage
    assert obligation.period_scope == "year_independent"
    assert obligation.valid_from is obligation.valid_to is None
    assert obligation.coding_claim == (state.value_set, "EU25")
