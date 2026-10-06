"""Accepted parallel columns: per-column coding and classification books keep native domains."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta_build.catalog_dependencies import (
    CoverageObligation,
    check_delivery_coverage,
)
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationLink,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import column_identity
from reg_meta_build.source_curation import (
    RepresentationDecision,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_records import (
    value_field,
)
from reg_meta_build.source_representations import (
    form_representations,
    resolve_representation_cases,
)

if TYPE_CHECKING:
    from pathlib import Path
from _source_representation_support import (
    KEY,
    coding_claim as _claim,
    column_storage_setup as _column_storage_setup,
    form_parallel_columns as _form,
    representation_setup as _setup,
)


def _column_coding_setup():
    from reg_meta_build.source_coding import copied_coding_fingerprints

    setup = _column_storage_setup(
        "text",
        "18",
        "integer",
        "0",
        claims={"First": (_claim("first", "01"),), "Second": (_claim("second", "02"),)},
    )
    decision = setup[2].decision
    assert isinstance(decision, RepresentationDecision)
    variant_key = next(iter(setup[3]))
    decision = decision.model_copy(
        update={
            "coding_metadata": "per_column",
            "columns": tuple(
                column.model_copy(
                    update={
                        "expected_codings": copied_coding_fingerprints(
                            setup[4][
                                column_identity(KEY, variant_key, column.column)
                            ].claims
                        ),
                    }
                )
                for column in decision.columns
            ),
        }
    )
    return (*setup[:2], setup[2].model_copy(update={"decision": decision}), *setup[3:])


def test_per_column_coding_preserves_native_domains_and_alias_only_index(
    tmp_path: Path,
) -> None:
    setup = _column_coding_setup()
    formed, proof = _form(setup)
    assert proof.diagnostics == () and formed.diagnostics == ()
    variable = formed.variable
    assert variable is not None
    assert variable.states[0].value_set is None
    assert {
        alias.delivery_column_name: alias.windows[0].value_set.members
        for alias in variable.aliases
    } == {
        "First": (("01", "Label"),),
        "Second": (("02", "Label"),),
    }
    assert all(
        window.coding_metadata == "per_column" and window.provenance is None
        for alias in variable.aliases
        for window in alias.windows
    )
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM value_set").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 2
        assert (
            conn.execute(
                "SELECT count(*) FROM variable_alias_window WHERE value_set_id IS NOT NULL"
            ).fetchone()[0]
            == 2
        )


def test_per_column_coding_source_domain_drift_fails_closed() -> None:
    setup = _column_coding_setup()
    coding = dict(setup[4])
    key = column_identity(KEY, next(iter(setup[3])), "First")
    coding[key] = resolve_code_membership((_claim("first", "changed"),))
    proof = resolve_representation_cases(setup[0], (setup[2],), coding=coding)
    assert proof.cases == ()
    assert [issue.code for issue in proof.diagnostics] == [
        "stale_representation_coding"
    ]


@pytest.mark.parametrize("unsupported", ["missing", "conflict"])
def test_per_column_coding_withholds_unsupported_domain_without_shared_fallback(
    unsupported,
) -> None:
    setup = _column_coding_setup()
    formed, _ = _form(setup)
    variable = formed.variable
    assert variable is not None
    base = variable.states[0]
    states = [
        base.model_copy(
            update={
                "delivery_column_name": alias.delivery_column_name,
                "value_set": alias.windows[0].value_set,
                "value_set_version_label": alias.windows[0].value_set_version_label,
            }
        )
        for alias in variable.aliases
    ]
    if unsupported == "missing":
        states[0] = states[0].model_copy(update={"value_set": None})
    else:
        states.append(states[0].model_copy(update={"value_set": states[1].value_set}))
    later = states[1].model_copy(
        update={"valid_from": "2021-01-01", "valid_to": "2021-12-31"}
    )
    result, aliases, issues, withheld, _ = form_representations(
        [*states, later],
        (setup[2],),
        variable_key=KEY,
        variants=setup[3],
        subject="fixture",
    )
    assert result == [later] and aliases == ()
    assert [issue.code for issue in issues] == ["unsupported_representation_coding"]
    assert set(withheld) == {
        ("people", "First", "2020-01-01", "2020-12-31"),
        ("people", "Second", "2020-01-01", "2020-12-31"),
    }
    retained = variable.model_copy(update={"states": tuple(result), "aliases": ()})
    nearby = CoverageObligation(
        "scb/example/income",
        "people",
        "Second",
        "2021-01-01",
        "2021-12-31",
        (record_ref(setup[0][1]),),
    )
    check_delivery_coverage((retained,), (nearby,), withheld={})
    missing = retained.model_copy(update={"states": ()})
    with pytest.raises(ValueError, match="supported delivery coverage was lost"):
        check_delivery_coverage((missing,), (nearby,), withheld={})
    unrelated = replace(
        nearby, column="Other", valid_from="2020-01-01", valid_to="2020-12-31"
    )
    with pytest.raises(ValueError, match="supported delivery coverage was lost"):
        check_delivery_coverage((retained,), (unrelated,), withheld={})


@pytest.mark.parametrize("second_source", [None, "Question 2 in another edition"])
def test_per_column_operation_and_attribution_do_not_borrow_sibling_facts(
    tmp_path, second_source
):
    setup = _column_storage_setup("integer", "0", "integer", "0")
    records = setup[0]
    # Rebuild target guards after changing the original source fixture.
    records = tuple(
        r.model_copy(
            update={
                "fields": r.fields.model_copy(
                    update={
                        "operational_definition": value_field(
                            "First operation" if i == 0 else "Second operation"
                        ),
                        "source_attribution": value_field("Question 1")
                        if i == 0
                        else value_field(second_source)
                        if second_source
                        else None,
                    }
                )
            }
        )
        for i, r in enumerate(records)
    )
    setup = _setup(records)
    case = setup[2].model_copy(
        update={
            "decision": setup[2].decision.model_copy(
                update={"column_metadata": "per_column"}
            )
        }
    )
    formed, proof = _form((*setup[:2], case, *setup[3:]))
    assert not proof.diagnostics and not formed.diagnostics
    variable = formed.variable
    assert variable.states[0].operational_definition is None
    assert variable.states[0].source_register_text is None
    assert {
        a.delivery_column_name: (
            a.windows[0].operational_definition,
            a.windows[0].source_register_text,
        )
        for a in variable.aliases
    } == {
        "First": ("First operation", "Question 1"),
        "Second": ("Second operation", second_source),
    }
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    write_resolved_catalog(
        (variable,), tmp_path / "reg_meta.db", manifest=synthetic_manifest()
    )
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert {
            tuple(row)
            for row in conn.execute(
                "SELECT delivery_column_name, operational_definition, source_register_text FROM variable_alias_window"
            )
        } == {
            ("First", "First operation", "Question 1"),
            ("Second", "Second operation", second_source),
        }
    from reg_meta_build.source_curation import evaluate_cases

    changed = (
        records[0].model_copy(
            update={
                "fields": records[0].fields.model_copy(
                    update={"source_attribution": None}
                )
            }
        ),
        records[1],
    )
    assert evaluate_cases((case,), changed)[0].status != "applicable"
    aliases = tuple(
        a.model_copy(
            update={
                "windows": (
                    a.windows[0].model_copy(
                        update={"source_register_text": "Question 1"}
                    ),
                )
            }
        )
        if a.delivery_column_name == "Second"
        else a
        for a in variable.aliases
    )
    with pytest.raises(ValueError, match="supported delivery facts changed"):
        check_delivery_coverage(
            (variable.model_copy(update={"aliases": aliases}),),
            formed.coverage,
            withheld={},
        )


def test_per_column_classifications_keep_independent_books_and_domains(tmp_path):
    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedClassificationCode,
        ResolvedConformance,
    )

    records, occurrences, case, variants, coding = _column_coding_setup()
    books = tuple(
        ResolvedClassification(
            slug=f"codes-{column.lower()}",
            short_name=column,
            name=column,
            codes=(ResolvedClassificationCode(code=code, label="Canonical"),),
        )
        for column, code in (("First", "01"), ("Second", "02"))
    )
    links = {
        column: ResolvedClassificationLink(
            classification=book.slug,
            conformance=ResolvedConformance(
                declared_classification=book.slug,
                status="conforming",
                checked_codes=(code,),
            ),
        )
        for column, code, book in zip(
            ("First", "Second"), ("01", "02"), books, strict=True
        )
    }
    coding = {
        key: replace(
            resolution,
            segments=tuple(
                replace(segment, classification_links=(links[key[-1]],))
                for segment in resolution.segments
            ),
        )
        for key, resolution in coding.items()
    }
    formed, proof = _form((records, occurrences, case, variants, coding))
    assert not proof.diagnostics and not formed.diagnostics
    assert formed.variable is not None
    variable = formed.variable
    assert variable.states[0].classification_links == ()
    assert {
        a.delivery_column_name: a.windows[0].classification_links
        for a in variable.aliases
    } == {column: (link,) for column, link in links.items()}
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    output = tmp_path / "per-column-books.db"
    write_resolved_catalog(
        (variable,), output, manifest=synthetic_manifest(), classifications=books
    )
    with closing(open_built_db(output)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT a.delivery_column_name,c.slug,v.code FROM alias_window_classification a JOIN classification c ON c.id=a.classification_id JOIN variable_alias_window w USING(variable_id,register_variant_id,delivery_column_name,valid_from) JOIN value_set_member m ON m.value_set_id=w.value_set_id JOIN value_code v USING(code_id) ORDER BY a.delivery_column_name"
            )
        ] == [("First", "codes-first", "01"), ("Second", "codes-second", "02")]
