"""Register-scoped representation-period loader validation."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from catalog_manifest import synthetic_manifest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.curation_compile import compile_matrix_repr, compile_period_families
from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_coordinates import column_identity, source_register_key
from reg_meta_build.source_curation import RepresentationDecision
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import TemporalScope
from reg_meta_build.source_representations import resolve_representation_cases
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def _write_register(tmp_path: Path, body: str) -> Path:
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "lisa.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n' + body,
        encoding="utf-8",
    )
    return root


def test_load_period_family_parses_explicit_slug(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\n'
        'slug = "lone-eller-foretagarinkomst-manad"\n',
    )
    (register,) = load_register_files(root)
    (family,) = register.representation.period_family
    assert (family.register_fqid, family.family_stem, family.label, family.slug) == (
        "scb/lisa",
        "lonfink",
        "Lön per månad",
        "lone-eller-foretagarinkomst-manad",
    )


def test_load_period_family_requires_authored_slug(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "curation/registers/scb/lisa.toml" in exc.value.message
    assert "[[representation.period_family.slug]] entry 1" in exc.value.message
    assert "slug" in exc.value.message


def test_load_period_family_rejects_unknown_key(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\nslug = "lonfink"\nunknown = "x"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "entry 1" in exc.value.message


def test_load_period_family_rejects_wrong_register(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/rams"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\nslug = "lonfink"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "does not match" in exc.value.message


def test_load_period_family_rejects_duplicate_stem(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "x"\nlabel = "A"\nslug = "x"\n\n'
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "x"\nlabel = "B"\nslug = "x"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "duplicate period-family stem" in exc.value.message


def _month_records(count: int):
    revision = SourceRevision.create(
        dataset="scb-registerinformation",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    months = (
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "Maj",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Okt",
        "Nov",
        "Dec",
    )
    return tuple(
        clean_scb_row(
            header,
            index,
            {
                name: (True, value, value)
                for name, value in zip(
                    header,
                    var_row(
                        colname=f"LonFink{month}",
                        cvid=100 + index,
                        var_id=index,
                        year="2020",
                        register=("LISA", 34, 10),
                    ).split("|"),
                    strict=True,
                )
            },
            revision,
        ).record
        for index, month in enumerate(months[:count], 1)
    )


def test_compile_twelve_months_and_missing_month(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\n'
        'slug = "lone-per-manad"\n',
    )
    (register,) = load_register_files(root)
    records = _month_records(12)
    cases, names, keys, issues = compile_period_families(register, records)
    assert issues == ()
    assert len(cases) == 2
    assert names[0].naming.slug == "lone-per-manad"
    assert keys[0][1] == "period-family:lisa:lonfink"
    decision = next(
        case.decision
        for case in cases
        if isinstance(case.decision, RepresentationDecision)
    )
    assert len(decision.columns) == 12
    assert decision.columns[1].valid_to == "2020-02-29"
    result = apply_occurrence_cases(records, (cases[0],))
    assert result.diagnostics == ()
    assert {item.variable_key for item in result.occurrences} == {decision.variable_key}
    coding = {
        column_identity(
            decision.variable_key, decision.variant_key, column.column
        ): resolve_code_membership(())
        for column in decision.columns
    }
    resolution = resolve_representation_cases(records, (cases[1],), coding=coding)
    assert resolution.diagnostics == ()
    cases, names, keys, issues = compile_period_families(register, _month_records(11))
    assert cases == names == keys == ()
    assert [issue.code for issue in issues] == ["stale_curation_entry"]


def test_month_without_assignable_year_is_stale(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\nslug = "lonfink"\n',
    )
    (register,) = load_register_files(root)
    records = _month_records(12)
    undated = records[0].model_copy(
        update={
            "edition_scope": TemporalScope(kind="unknown", label="year unavailable")
        }
    )
    cases, names, keys, issues = compile_period_families(
        register, (undated, *records[1:])
    )
    assert cases == names == keys == ()
    assert [issue.code for issue in issues] == ["stale_curation_entry"]
    assert (
        "curation/registers/scb/lisa.toml#/representation.period_family/1"
        in issues[0].detail
    )
    assert "LonFinkJan" in issues[0].detail


def test_matrix_repr_wires_period_cases_into_selected_scope(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\nslug = "lonfink"\n',
    )
    (register,) = load_register_files(root)
    records = _month_records(12)
    register_key = source_register_key(records[0])
    named_register = NamingDeclaration(
        target=NativeNamingTarget(
            kind="register", provider="scb", source_key=register_key
        ),
        naming=SlugEntry(kind="register", provider="scb", source_id="34", slug="lisa"),
        contributors=(),
    )
    scope = CompiledScope(
        source=records[0].source,
        register_key=register_key,
        naming=(named_register,),
    )

    class Records:
        def iter_register_slices(self, source, wanted):
            assert source == scope.source and register_key in wanted
            yield register_key, records

    tree = SimpleNamespace(root=root, registers=(register,))
    prepared = SimpleNamespace(records=Records(), value_sources=())
    cases, names, keys, issues = compile_matrix_repr(
        tree, prepared, (scope,), {(scope.source, register_key): (named_register,)}
    )
    key = scope.source, register_key
    assert issues == ()
    assert len(cases[key]) == 2
    assert len(names[key]) == len(keys[key]) == 1


def _defined_month_family(tmp_path):
    import json

    from reg_meta_build.source_records import value_field

    definitions = {
        f"{month:02}": f"Salary paid in month {month:02}; annual business income / 12."
        for month in range(1, 13)
    }
    body = (
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\nslug = "lonfink"\n'
        "expected_definitions = { "
        + ", ".join(
            json.dumps(month) + " = " + json.dumps(text)
            for month, text in definitions.items()
        )
        + " }\n"
    )
    root = _write_register(tmp_path, body)
    (register,) = load_register_files(root)
    records = tuple(
        r.model_copy(
            update={
                "fields": r.fields.model_copy(
                    update={"definition": value_field(definitions[f"{month:02}"])}
                )
            }
        )
        for month, r in enumerate(_month_records(12), 1)
    )
    return root, register, records, definitions


def _form_defined_months(register, records, *, omit_target=False, key_override=None):
    from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
    from reg_meta_build.source_formation import form_native_variable
    from reg_meta_build.source_records import SourceFields, value_field

    cases, _, _, issues = compile_period_families(register, records)
    assert issues == ()
    corrected = apply_occurrence_cases(records, (cases[0],))
    assert corrected.diagnostics == ()
    case = cases[1]
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    if omit_target:
        case = case.model_copy(update={"targets": case.targets[:-1]})
    if key_override is not None:
        case = case.model_copy(
            update={
                "decision": decision.model_copy(update={"variable_key": key_override})
            }
        )
        corrected = replace(
            corrected,
            occurrences=tuple(
                replace(r, variable_key=key_override) for r in corrected.occurrences
            ),
        )
        decision = case.decision
    coding = {
        column_identity(
            decision.variable_key, decision.variant_key, c.column
        ): resolve_code_membership(())
        for c in decision.columns
    }
    proof = resolve_representation_cases(records, (case,), coding=coding)
    assert proof.diagnostics == ()
    formed = form_native_variable(
        corrected.occurrences,
        register=ResolvedRegister(provider="scb", slug="lisa", name="LISA"),
        variants={decision.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="lonfink",
        provider_key="monthly",
        flags=SourceFields(
            identifier=value_field(False), sensitivity=value_field(False)
        ),
        coding=coding,
        representations=proof.cases,
    )
    return formed, cases


def test_checked_month_units_are_captured_before_alias_projection(tmp_path):
    from reg_meta_build.source_curation import evaluate_cases
    from reg_meta_build.source_records import value_field

    _, register, records, _ = _defined_month_family(tmp_path)
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"measurement_unit": value_field("Kronor (SEK)")}
                )
            }
        )
        for record in records
    )
    formed, cases = _form_defined_months(register, records)
    assert formed.variable is not None
    assert all(
        window.measurement_unit == "Kronor (SEK)"
        for alias in formed.variable.aliases
        for window in alias.windows
    )
    changed = records[0].model_copy(
        update={
            "fields": records[0].fields.model_copy(
                update={"measurement_unit": value_field("100-tal kronor")}
            )
        }
    )
    assert all(
        result.status == "stale"
        for result in evaluate_cases(cases, (changed, *records[1:]))
    )


def test_checked_month_definitions_keep_literal_text_and_source_scopes(tmp_path):
    import sqlite3

    from reg_meta_build.catalog_dependencies import check_delivery_coverage
    from reg_meta_build.resolved_catalog import write_resolved_catalog

    _, register, records, definitions = _defined_month_family(tmp_path)
    formed, _ = _form_defined_months(register, records)
    assert [d.code for d in formed.diagnostics] == [
        "period_family_definition_projected"
    ]
    variable = formed.variable
    assert variable is not None and variable.definition is None
    assert len(variable.states) == 1 and variable.states[0].definition is None
    assert (variable.states[0].valid_from, variable.states[0].valid_to) == (
        "2020-01-01",
        "2020-12-31",
    )
    assert len(variable.aliases) == 12
    for alias in variable.aliases:
        window = alias.windows[0]
        assert window.column_metadata == "per_column"
        assert window.definition == definitions[window.valid_from[5:7]]
    assert [r.edition_scope for r in formed.occurrences] == [
        r.edition_scope for r in records
    ]
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    alias = variable.aliases[0]
    bad_alias = alias.model_copy(
        update={"windows": (alias.windows[0].model_copy(update={"definition": None}),)}
    )
    with pytest.raises(ValueError, match="literal definition changed"):
        check_delivery_coverage(
            (
                variable.model_copy(
                    update={"aliases": (bad_alias, *variable.aliases[1:])}
                ),
            ),
            formed.coverage,
            withheld={},
        )
    path = write_resolved_catalog(
        (variable,), tmp_path / "catalog.db", manifest=synthetic_manifest()
    )
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT definition FROM variable_state").fetchone() == (
            None,
        )
        assert conn.execute(
            "SELECT count(*) FROM variable_alias_window WHERE definition IS NOT NULL"
        ).fetchone() == (12,)
        assert conn.execute(
            "SELECT count(*) FROM variable_fts WHERE variable_fts MATCH 'business'"
        ).fetchone() == (1,)


@pytest.mark.parametrize("change", ["changed", "missing", "new"])
def test_month_definition_map_refuses_incomplete_or_drifted_family(tmp_path, change):
    from reg_meta_build.source_records import value_field

    _, register, records, _ = _defined_month_family(tmp_path)
    if change == "missing":
        records = records[:-1]
    else:
        drift = records[0].model_copy(
            update={
                "fields": records[0].fields.model_copy(
                    update={"definition": value_field("Different source meaning")}
                )
            }
        )
        records = (drift, *records[1:]) if change == "changed" else (*records, drift)
    cases, names, keys, issues = compile_period_families(register, records)
    assert cases == names == keys == ()
    assert [d.code for d in issues] == ["stale_curation_entry"]


@pytest.mark.parametrize("condition", ["uncovered", "ordinary"])
def test_definition_conflict_requires_complete_checked_period_family(
    tmp_path, condition
):
    _, register, records, _ = _defined_month_family(tmp_path)
    formed, _ = _form_defined_months(
        register,
        records,
        omit_target=condition == "uncovered",
        key_override=("accepted", "ordinary", "quantity")
        if condition == "ordinary"
        else None,
    )
    assert "conflicting_variable_fact" in [d.code for d in formed.diagnostics]
    assert "period_family_definition_projected" not in [
        d.code for d in formed.diagnostics
    ]


def test_unreviewed_shared_month_family_keeps_common_definition(tmp_path):
    from reg_meta_build.source_records import value_field

    _, register, records, _ = _defined_month_family(tmp_path)
    register = register.model_copy(
        update={
            "representation": register.representation.model_copy(
                update={
                    "period_family": [
                        register.representation.period_family[0].model_copy(
                            update={"expected_definitions": None}
                        )
                    ]
                }
            )
        }
    )
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"definition": value_field("A stable common definition")}
                )
            }
        )
        for record in records
    )
    formed, _ = _form_defined_months(register, records)
    assert formed.diagnostics == ()
    assert formed.variable is not None
    assert formed.variable.definition == "A stable common definition"
    assert formed.variable.states[0].definition == "A stable common definition"
    assert all(
        window.column_metadata == "shared" and window.definition is None
        for alias in formed.variable.aliases
        for window in alias.windows
    )


@pytest.mark.parametrize("invalid", ["missing_month", "wrong_month", "blank"])
def test_definition_map_requires_twelve_exact_positive_literals(invalid):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import PeriodFamilyEntry

    definitions = {f"{month:02}": f"Definition {month}" for month in range(1, 13)}
    if invalid == "missing_month":
        del definitions["12"]
    elif invalid == "wrong_month":
        definitions["13"] = definitions.pop("12")
    else:
        definitions["12"] = ""
    with pytest.raises(ValidationError):
        PeriodFamilyEntry.model_validate(
            {
                "register": "scb/lisa",
                "family_stem": "lonfink",
                "label": "Monthly income",
                "slug": "lonfink",
                "expected_definitions": definitions,
            }
        )
