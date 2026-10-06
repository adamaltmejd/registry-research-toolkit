"""Checked corrections: search aliases and alias windows are guarded metadata that add no availability."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_annotations import apply_alias_cases
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_curation import (
    SearchAliasDecision,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    SourceFields,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path

from _source_effects_support import effect_case as _case, effect_record as _record


def _search_alias_fixture():
    record = _record(column="VALUE")
    occurrence = source_occurrence(record)
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    assert occurrence.column_key is not None
    variant = ResolvedVariant(slug="people", name="People")
    result = form_native_variable(
        (record,),
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={occurrence.variant_key: variant},
        slug="value",
        provider_key="5",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={occurrence.column_key: resolve_code_membership(())},
    )
    assert result.variable is not None
    case = _case(
        record,
        decision=SearchAliasDecision(
            reviewed=True,
            variable_key=occurrence.variable_key,
            variant_keys=(occurrence.variant_key,),
            column="ALTERNATIVE",
            reason="Existing delivery-list search alias",
            provenance="fixture",
        ),
    )
    return (
        record,
        case,
        result.variable,
        occurrence.variable_key,
        {occurrence.variant_key: variant},
    )


def test_search_alias_is_guarded_metadata_without_added_availability(
    tmp_path: Path,
) -> None:
    record, case, variable, key, variants = _search_alias_fixture()
    result = apply_alias_cases(
        (record,), (case,), variables={key: variable}, variants=variants
    )
    assert result.diagnostics == ()
    updated = result.variables[key]
    assert updated is not None and updated.states == variable.states
    assert [
        (alias.delivery_column_name, alias.windows) for alias in updated.aliases
    ] == [("ALTERNATIVE", ())]
    output = tmp_path / "aliases.db"
    write_resolved_catalog((updated,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert (
            conn.execute(
                "SELECT delivery_column_name FROM variable_alias WHERE delivery_column_name='ALTERNATIVE'"
            ).fetchone()[0]
            == "ALTERNATIVE"
        )
        assert (
            conn.execute("SELECT count(*) FROM variable_alias_window").fetchone()[0]
            == 0
        )
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 1
    same = apply_alias_cases(
        (_record(column=" VALUE "),),
        (case,),
        variables={key: variable},
        variants=variants,
    )
    assert same.diagnostics == () and same.variables == result.variables
    stale = apply_alias_cases(
        (record, _record(cvid=21, column="VALUE", year="2021")),
        (case,),
        variables={key: variable},
        variants=variants,
    )
    assert stale.variables[key] == variable
    assert stale.evaluations[0].status == "stale"


def test_search_alias_preserves_existing_precise_windows() -> None:
    record, case, variable, key, variants = _search_alias_fixture()
    alias = ResolvedAlias(
        variant=next(iter(variants.values())),
        delivery_column_name="ALTERNATIVE",
        windows=(ResolvedAliasWindow(valid_from="2020-03-01", valid_to="2020-04-30"),),
    )
    variable = variable.model_copy(update={"aliases": (alias,)})
    result = apply_alias_cases(
        (record,), (case,), variables={key: variable}, variants=variants
    )
    assert result.diagnostics == () and result.variables[key] == variable


def test_search_alias_distinguishes_withheld_dependencies_from_missing_conversion() -> (
    None
):
    record, case, variable, key, variants = _search_alias_fixture()
    withheld = apply_alias_cases(
        (record,), (case,), variables={key: None}, variants=variants
    )
    assert withheld.variables[key] is None
    assert [issue.code for issue in withheld.diagnostics] == [
        "withheld_alias_dependency"
    ]
    assert withheld.evaluations[0].status == "applicable"
    for variables, parents in (({}, variants), ({key: variable}, {})):
        with pytest.raises(ValueError, match="unconverted"):
            apply_alias_cases((record,), (case,), variables=variables, variants=parents)


def _window_case(
    case, key, variant_key, *, start="2020-03-01", end="2020-04-30", name="window"
):
    from reg_meta_build.source_curation import AliasWindowDecision

    return case.model_copy(
        update={
            "case_id": name,
            "decision": AliasWindowDecision(
                reviewed=True,
                variable_key=key,
                variant_key=variant_key,
                column="ALTERNATIVE",
                valid_from=start,
                valid_to=end,
                reason="Existing accepted omitted representation",
                provenance="accepted alias_windows entry",
            ),
        }
    )


def test_alias_window_needs_owned_alias_and_does_not_expand_states(
    tmp_path: Path,
) -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    case = _window_case(search, key, variant_key)
    result = apply_alias_cases(
        (record,), (case,), variables={key: owned}, variants=variants
    )
    assert result.diagnostics == ()
    updated = result.variables[key]
    assert updated is not None and updated.states == variable.states
    assert [(w.valid_from, w.valid_to) for w in updated.aliases[0].windows] == [
        ("2020-03-01", "2020-04-30"),
    ]
    assert updated.aliases[0].windows[0].provenance is not None
    output = tmp_path / "window.db"
    write_resolved_catalog((updated,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 1
        assert tuple(
            conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to FROM variable_alias_window"
            ).fetchone()
        ) == ("ALTERNATIVE", "2020-03-01", "2020-04-30")
    unowned = apply_alias_cases(
        (record,), (case,), variables={key: variable}, variants=variants
    )
    assert unowned.variables[key] == variable
    assert [d.code for d in unowned.diagnostics] == ["unowned_alias_window"]
    future = _window_case(
        search, key, variant_key, start="2021-01-01", end="2021-12-31"
    )
    outside = apply_alias_cases(
        (record,), (future,), variables={key: owned}, variants=variants
    )
    assert outside.variables[key] == owned
    assert [d.code for d in outside.diagnostics] == ["unsupported_alias_window"]


def test_alias_window_checks_original_ownership_and_complete_period_coverage() -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    window = _window_case(search, key, variant_key)
    result = apply_alias_cases(
        (record,), (search, window), variables={key: variable}, variants=variants
    )
    assert result == apply_alias_cases(
        (record,), (window, search), variables={key: variable}, variants=variants
    )
    assert [d.code for d in result.diagnostics] == ["unowned_alias_window"]
    annotated = result.variables[key]
    assert annotated is not None
    assert annotated.aliases[0].windows == ()
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    split = owned.model_copy(
        update={
            "states": (
                owned.states[0].model_copy(update={"valid_to": "2020-03-31"}),
                owned.states[0].model_copy(update={"valid_from": "2020-04-01"}),
            )
        }
    )
    assert (
        apply_alias_cases(
            (record,), (window,), variables={key: split}, variants=variants
        ).diagnostics
        == ()
    )
    gap = split.model_copy(
        update={
            "states": (
                split.states[0],
                split.states[1].model_copy(update={"valid_from": "2020-04-02"}),
            )
        }
    )
    result = apply_alias_cases(
        (record,), (window,), variables={key: gap}, variants=variants
    )
    assert [d.code for d in result.diagnostics] == ["unsupported_alias_window"]
    assert result.variables[key] == gap


def test_overlapping_alias_windows_compose_with_scoped_provenance_and_stale_guards() -> (
    None
):
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    first = _window_case(search, key, variant_key, name="first")
    second = _window_case(
        search, key, variant_key, start="2020-04-01", end="2020-05-31", name="second"
    )
    result = apply_alias_cases(
        (record,), (first, second), variables={key: owned}, variants=variants
    )
    assert result == apply_alias_cases(
        (record,), (second, first), variables={key: owned}, variants=variants
    )
    updated = result.variables[key]
    assert updated is not None and result.diagnostics == ()
    windows = updated.aliases[0].windows
    assert [(w.valid_from, w.valid_to) for w in windows] == [
        ("2020-03-01", "2020-03-31"),
        ("2020-04-01", "2020-04-30"),
        ("2020-05-01", "2020-05-31"),
    ]
    assert windows[1].provenance is not None
    assert "first:" in windows[1].provenance and "second:" in windows[1].provenance
    stale = apply_alias_cases(
        (_record(column="OTHER"),), (first,), variables={key: owned}, variants=variants
    )
    assert stale.variables[key] == owned and stale.evaluations[0].status == "stale"


def test_alias_window_rejects_another_supported_owner_in_the_same_period() -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    other = variable.model_copy(
        update={
            "slug": "other",
            "states": (
                variable.states[0].model_copy(
                    update={"delivery_column_name": "ALTERNATIVE"}
                ),
            ),
        }
    )
    window = _window_case(search, key, variant_key)
    result = apply_alias_cases(
        (record,),
        (window,),
        variables={key: owned, (*key, "other"): other},
        variants=variants,
    )
    assert [d.code for d in result.diagnostics] == ["conflicting_alias_window_owner"]
    assert result.variables[key] == owned
    future = other.model_copy(
        update={
            "states": (
                other.states[0].model_copy(
                    update={"valid_from": "2021-01-01", "valid_to": "2021-12-31"}
                ),
            )
        }
    )
    assert (
        apply_alias_cases(
            (record,),
            (window,),
            variables={key: owned, (*key, "other"): future},
            variants=variants,
        ).diagnostics
        == ()
    )


def test_competing_alias_window_decisions_withhold_only_their_overlap() -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    other_key = (*key, "other")
    other = owned.model_copy(update={"slug": "other"})
    first = _window_case(search, key, variant_key, name="first")
    second = _window_case(
        search,
        other_key,
        variant_key,
        start="2020-04-01",
        end="2020-05-31",
        name="second",
    )
    result = apply_alias_cases(
        (record,),
        (first, second),
        variables={key: owned, other_key: other},
        variants=variants,
    )
    assert result == apply_alias_cases(
        (record,),
        (second, first),
        variables={key: owned, other_key: other},
        variants=variants,
    )
    assert len(result.diagnostics) == 2
    assert {(d.code, d.valid_from, d.valid_to) for d in result.diagnostics} == {
        ("conflicting_alias_window_decisions", "2020-04-01", "2020-04-30"),
    }
    assert [
        [(w.valid_from, w.valid_to) for w in v.aliases[0].windows]
        for v in (result.variables[key], result.variables[other_key])
        if v is not None
    ] == [[("2020-03-01", "2020-03-31")], [("2020-05-01", "2020-05-31")]]


def test_dated_alias_window_cannot_annualize_independent_delivery():
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key = next(iter(variants))
    independent = variable.model_copy(
        update={
            "states": tuple(
                state.model_copy(
                    update={
                        "period_scope": "year_independent",
                        "valid_from": None,
                        "valid_to": None,
                    }
                )
                for state in variable.states
            )
        }
    )
    result = apply_alias_cases(
        (record,),
        (_window_case(search, key, variant_key),),
        variables={key: independent},
        variants=variants,
    )
    assert result.variables[key] == independent
    assert [issue.code for issue in result.diagnostics] == ["unsupported_alias_scope"]
