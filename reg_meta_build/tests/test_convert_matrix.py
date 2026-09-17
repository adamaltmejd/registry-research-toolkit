"""Accepted matrix conversion preserves source evidence and rejects changed scope."""

from __future__ import annotations

import json

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta_build.cis2016_matrix import Cis2014Matrix, Cis2016Matrix
from reg_meta_build.convert_matrix import convert_matrix
from reg_meta_build.source_curation import OccurrenceCorrectionDecision, evaluate_case
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_naming import check_naming_target
from reg_meta_build.source_records import SourceRevision, value_field
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
    converted = convert_matrix(
        matrix, records, case_id="accepted-blank", provenance="pinned input"
    )
    result = apply_occurrence_cases(records, (converted.case,))
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
        assert item.fields.identifier is None
        assert item.fields.sensitivity is None
        assert item.edition_scope == records[0].edition_scope
        assert item.fields.data_type == records[0].fields.data_type


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
