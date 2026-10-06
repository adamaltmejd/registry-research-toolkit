"""Accepted matrix conversion preserves source evidence and rejects changed scope."""

from __future__ import annotations

import json

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row as _var_row
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.cis2016_matrix import Cis2014Matrix, Cis2016Matrix, convert_matrix
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    copied_coding_fingerprints,
)
from reg_meta_build.source_curation import OccurrenceCorrectionDecision, evaluate_case
from reg_meta_build.source_effects import apply_occurrence_cases, record_ref
from reg_meta_build.source_naming import check_naming_target
from reg_meta_build.source_records import value_field
from reg_meta_build.sources.scb_records import clean_scb_row

_REVISION = SourceRevision.create(
    dataset="scb-fixture",
    publisher="SCB",
    purpose="matrix conversion fixture",
    upstream_revision="1",
    artifact_path="Registerinformation.csv",
    artifact_size=1,
    artifact_sha256="b" * 64,
)


def _matrix(*, blank: bool = False) -> Cis2014Matrix | Cis2016Matrix:
    payload = {
        "selector": {
            "register": "scb/innovation-foretag",
            "register_id": 257,
            "variant": "_default",
            "register_variant_id": 553,
            "edition": "2012 - 2014" if blank else "2014 - 2016",
            "regver_id": 7293 if blank else 11529,
            "var_id": 15662,
            "cvid": 400684 if blank else 469456,
        },
        "evidence": {
            "document": "Reviewed fixture",
            "url": "https://example.test/evidence",
            "sha256": "a" * 64,
            "question": "Question 18",
            "noted": "2026-09-13",
        },
        "question_label": "Partner location",
        "axes": [
            {"key": name, "label_en": name.title()} for name in ("partner", "response")
        ],
        "answers": [
            {
                "key": key,
                "slug": f"answer-{key}",
                "columns": [column],
                "label_en": key.title(),
                "definition_en": f"Definition of {key}.",
                "partner": {"key": "group", "label_en": "Group"},
                "response": {"key": key, "label_en": key.title()},
                "source_pages": {column: 23},
            }
            for key, column in (("sweden", "CO11"), ("abroad", "CO12"))
        ],
    }
    if blank:
        payload["source_mode"] = "documented_blank"
        payload["answer_facts"] = {
            "data_type": "decimal",
            "data_type_evidence": "SWECOV CIS2014=float",
            "is_identifier": False,
            "is_sensitive": False,
            "flag_evidence": "SCB adjacent wave declares both flags 0",
        }
        return Cis2014Matrix.model_validate_json(json.dumps(payload))
    return Cis2016Matrix.model_validate_json(json.dumps(payload))


def _rows(matrix, columns, *, cvid=None, edition=None):
    selector = matrix.selector
    header = REGISTERINFORMATION_HEADER.split("|")
    result = []
    for index, column in enumerate(columns, 1):
        values = _var_row(
            cvid=cvid or selector.cvid,
            var_id=selector.var_id,
            colname=column,
            register=("Innovation", selector.register_id, selector.register_variant_id),
            regver_id=edition or selector.regver_id,
            year=selector.edition,
            data_type="varchar",
        ).split("|")
        cells: dict[str, tuple[bool, str | None, str]] = {
            name: (True, value, value)
            for name, value in zip(header, values, strict=True)
        }
        result.append(clean_scb_row(header, index, cells, _REVISION).record)
    return tuple(result)


def test_named_answers_preserve_exact_occurrences_and_pooled_coverage() -> None:
    matrix = _matrix()
    records = _rows(matrix, ("CO11", "CO12"))
    converted = convert_matrix(
        matrix, records, case_id="accepted-matrix", provenance="pinned input"
    )
    result = apply_occurrence_cases(records, (converted.case,))
    assert not result.diagnostics
    assert len(result.occurrences) == 2
    assert len({item.variable_key for item in result.occurrences}) == 2
    for item, original, answer in zip(
        result.occurrences, records, matrix.answers, strict=True
    ):
        assert item.source_records == (original,)
        assert item.fields.column_name == original.fields.column_name
        assert item.fields.data_type == original.fields.data_type
        assert item.fields.name is not None
        assert item.fields.definition is not None
        assert item.fields.name.status == "value"
        assert item.fields.name.value == answer.label_en
        assert item.fields.definition.value == answer.definition_en
        assert item.edition_scope == original.edition_scope
        assert item.edition_scope.kind == "pooled"
    assert set(converted.provider_keys.values()) == {"15662"}
    assert {item.naming.slug for item in converted.naming} == {
        a.slug for a in matrix.answers
    }
    assert isinstance(converted.case.decision, OccurrenceCorrectionDecision)
    assert "source_pages" in converted.case.decision.provenance


def test_blank_source_is_retained_and_only_declared_answers_are_added() -> None:
    matrix = _matrix(blank=True)
    records = _rows(matrix, ("", ""))
    donor = record_ref(records[0])
    claims = (
        CodeListClaim(
            "original",
            records[0].edition_scope,
            (CodeMembershipClaim("1", "Yes", records[0].edition_scope),),
        ),
    )
    with pytest.raises(ValueError, match="one complete bound coding list"):
        convert_matrix(
            matrix, records, case_id="accepted-blank", provenance="pinned input"
        )
    converted = convert_matrix(
        matrix,
        records,
        case_id="accepted-blank",
        provenance="pinned input",
        coding={donor: claims},
    )
    with pytest.raises(ValueError, match="one complete bound coding list"):
        convert_matrix(
            matrix,
            records,
            case_id="accepted-blank",
            provenance="pinned input",
            coding={donor: (claims[0], claims[0])},
        )
    witness = {(donor, records[0].edition_period_scope): claims}
    result = apply_occurrence_cases(records, (converted.case,), coding=witness)
    assert not result.diagnostics
    assert len([o for o in result.occurrences if o.use == "support"]) == 2
    added = [o for o in result.occurrences if o.use == "catalog"]
    assert len(added) == 2
    assert {
        o.fields.column_name.value for o in added if o.fields.column_name
    } == matrix.columns
    for item in added:
        assert not item.source_records
        assert item.support_records == records
        assert item.coding_records == records
        assert item.fields.identifier == value_field(matrix.answer_facts.is_identifier)
        assert item.fields.sensitivity == value_field(matrix.answer_facts.is_sensitive)
        assert item.edition_scope == records[0].edition_scope
        assert item.fields.data_type == value_field(matrix.answer_facts.data_type)
    assert isinstance(converted.case.decision, OccurrenceCorrectionDecision)
    assert all(
        {"data_type", "identifier", "sensitivity"}.isdisjoint(effect.copied_fields)
        for effect in converted.case.decision.effects
        if effect.kind == "addition"
    )
    assert "SWECOV CIS2014=float" in converted.case.decision.provenance
    assert (
        "SCB adjacent wave declares both flags 0" in converted.case.decision.provenance
    )
    assert all(
        effect.expected_codings == copied_coding_fingerprints(claims)
        for effect in converted.case.decision.effects
        if effect.kind == "addition"
    )
    changed = apply_occurrence_cases(
        records, (converted.case,), coding={next(iter(witness)): ()}
    )
    assert changed.accounting[0].disposition == "stale"
    assert all(item.source_records for item in changed.occurrences)


@pytest.mark.parametrize(
    "change",
    ("missing_column", "changed_definition", "competing_cvid", "changed_period"),
)
def test_original_changes_invalidate_the_whole_reviewed_partition(change: str) -> None:
    matrix = _matrix()
    records = _rows(matrix, ("CO11", "CO12"))
    converted = convert_matrix(
        matrix, records, case_id="accepted-matrix", provenance="pinned input"
    )
    match change:
        case "missing_column":
            changed = records[:1]
        case "changed_definition":
            changed = (
                records[0].model_copy(
                    update={
                        "fields": records[0].fields.model_copy(
                            update={"definition": value_field("Changed")}
                        )
                    }
                ),
                records[1],
            )
        case "competing_cvid":
            changed = (*records, *_rows(matrix, ("NEW",), cvid=999))
        case "changed_period":
            changed = (
                records[0].model_copy(
                    update={
                        "edition_period_scope": records[0].edition_scope.model_copy(
                            update={"label": "2016 - 2018"}
                        )
                    }
                ),
                records[1],
            )
    assert evaluate_case(converted.case, changed).status == "stale"
    for name in converted.naming:
        assert check_naming_target(name.target, changed)


def test_other_editions_and_physical_layout_do_not_change_answer_ownership() -> None:
    matrix = _matrix()
    records = _rows(matrix, ("CO11", "CO12"))
    other = _rows(matrix, ("OTHER",), edition=9000, cvid=999)
    converted = convert_matrix(
        matrix, records, case_id="accepted-matrix", provenance="pinned input"
    )
    moved = tuple(
        r.model_copy(
            update={
                "locators": tuple(
                    l.model_copy(update={"physical_record": "row:999"})
                    for l in r.locators
                )
            }
        )
        for r in records
    )
    assert evaluate_case(converted.case, (*moved, *other)).status == "applicable"
    result = apply_occurrence_cases((*moved, *other), (converted.case,))
    assert result.occurrences[-1].source_records == other
    assert not result.occurrences[-1].identity_checked


@pytest.mark.parametrize("blank", (False, True))
def test_converter_rejects_unreviewed_columns_or_competing_members(blank: bool) -> None:
    matrix = _matrix(blank=blank)
    records = _rows(matrix, ("",) if blank else ("CO11", "CO12"))
    with pytest.raises(ValueError, match="CVID partition"):
        convert_matrix(
            matrix,
            (*records, *_rows(matrix, ("NEW",), cvid=999)),
            case_id="matrix",
            provenance="input",
        )
    with pytest.raises(ValueError, match="column partition"):
        convert_matrix(
            matrix,
            (*records, *_rows(matrix, ("NEW",))),
            case_id="matrix",
            provenance="input",
        )


def _matrix_activation(tmp_path, *, declared=True, records=None, blank=False):
    from types import SimpleNamespace

    from reg_meta_build.curation_compile import compile_matrix_repr
    from reg_meta_build.curation_tree import load_register_files
    from reg_meta_build.pipeline import CompiledScope
    from reg_meta_build.source_coordinates import source_register_key
    from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget

    from reg_meta_build.fqid_slugs import SlugEntry

    matrix = _matrix(blank=blank)
    root = tmp_path / "curation"
    evidence = root / "registers/scb/innovation-foretag/answers.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text(matrix.model_dump_json(by_alias=True), encoding="utf-8")
    path = evidence.parent.with_suffix(".toml")
    body = (
        '[register]\nprovider = "scb"\nslug = "innovation-foretag"\nnative_id = "257"\n'
        '[[variant]]\nnative_id = "257.553"\nslug = "_default"\n'
    )
    selector = ", ".join(
        f"{key} = {json.dumps(value)}"
        for key, value in matrix.selector.model_dump(by_alias=True).items()
    )
    if declared:
        body += (
            f'[[representation.matrix]]\nsource_mode = "{"documented_blank" if blank else "named"}"\n'
            'evidence_file = "registers/scb/innovation-foretag/answers.json"\n'
            f"selector = {{ {selector} }}\n"
        )
    path.write_text(body, encoding="utf-8")
    records = (
        records
        if records is not None
        else _rows(matrix, ("", "") if blank else ("CO11", "CO12"))
    )
    register_key = source_register_key(records[0])
    named = NamingDeclaration(
        target=NativeNamingTarget(
            kind="register", provider="scb", source_key=register_key
        ),
        naming=SlugEntry(
            kind="register", provider="scb", source_id="257", slug="innovation-foretag"
        ),
        contributors=(),
    )
    scope = CompiledScope(
        source=records[0].source, register_key=register_key, naming=(named,)
    )

    class Records:
        def iter_register_slices(self, source, wanted):
            assert source == scope.source and register_key in wanted
            yield register_key, records

    def compile():
        (register,) = load_register_files(root)
        return compile_matrix_repr(
            SimpleNamespace(root=root, registers=(register,)),
            SimpleNamespace(records=Records(), value_sources=()),
            (scope,),
            {(scope.source, register_key): (named,)},
        )

    return compile, evidence, path, matrix, records, (scope.source, register_key)


def test_matrix_activation_preserves_existing_case_and_checked_conversion(
    tmp_path,
) -> None:
    compile, _, _, matrix, records, key = _matrix_activation(tmp_path)
    cases, names, keys, issues = compile()
    ref = "curation/registers/scb/innovation-foretag.toml#/matrix/2014 - 2016"
    expected = convert_matrix(matrix, records, case_id=ref, provenance=ref)
    assert issues == ()
    assert cases[key] == (expected.case,)
    assert names[key] == expected.naming
    assert keys[key] == tuple(expected.provider_keys.items())


def test_matrix_evidence_does_not_activate_without_declaration(tmp_path) -> None:
    compile, _, _, _, _, _ = _matrix_activation(tmp_path, declared=False)
    assert compile() == ({}, {}, {}, ())


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "selector",
        "meaning",
        "parent_path",
        "symlink",
        "missing_selector",
        "wrong_register",
        "wrong_variant",
        "absolute_path",
        "duplicate",
    ],
)
def test_matrix_activation_refuses_missing_or_changed_checked_evidence(
    tmp_path, change
) -> None:
    from reg_meta.errors import EXIT_CONFIG, RegMetaError

    compile, evidence, path, _, _, _ = _matrix_activation(tmp_path)
    if change == "missing":
        evidence.unlink()
    elif change in {"selector", "meaning"}:
        payload = json.loads(evidence.read_text())
        if change == "selector":
            payload["selector"]["cvid"] += 1
        else:
            payload["answers"][0]["columns"].append("CO13")
            payload["answers"][0]["source_pages"]["CO13"] = 23
        evidence.write_text(json.dumps(payload), encoding="utf-8")
    elif change == "parent_path":
        path.write_text(
            path.read_text().replace(
                "innovation-foretag/answers.json", "innovation-foretag/../answers.json"
            )
        )
    elif change == "symlink":
        external = tmp_path / "external.json"
        evidence.rename(external)
        evidence.symlink_to(external)
    else:
        content = path.read_text()
        if change == "missing_selector":
            content = "\n".join(
                line
                for line in content.splitlines()
                if not line.startswith("selector =")
            )
        elif change == "wrong_register":
            content = content.replace("register_id = 257", "register_id = 258")
        elif change == "wrong_variant":
            content = content.replace(
                "register_variant_id = 553", "register_variant_id = 554"
            )
        elif change == "absolute_path":
            content = content.replace(
                "registers/scb/innovation-foretag/answers.json", "/answers.json"
            )
        else:
            content += (
                "[[representation.matrix]]"
                + content.split("[[representation.matrix]]", 1)[1]
            )
        path.write_text(content)
    with pytest.raises(RegMetaError) as exc:
        compile()
    assert exc.value.exit_code == EXIT_CONFIG


def test_matrix_activation_refuses_changed_complete_source_members(tmp_path) -> None:
    matrix = _matrix()
    records = (*_rows(matrix, ("CO11", "CO12")), *_rows(matrix, ("NEW",), cvid=999))
    compile, _, _, _, _, _ = _matrix_activation(tmp_path, records=records)
    cases, names, keys, issues = compile()
    assert (
        not any(cases.values()) and not any(names.values()) and not any(keys.values())
    )
    assert [issue.code for issue in issues] == ["stale_curation_entry"]
    assert "complete CVID partition changed" in issues[0].detail
