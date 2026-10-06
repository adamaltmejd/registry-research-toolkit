"""SCB observation traversal and source-field regressions, plus focused LISA inspection."""

from __future__ import annotations

import gzip
import hashlib
import json
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    REGISTERINFORMATION_HEADER,
    replace_registerinformation_cell,
    scb_interpretation_rows,
    var_row,
    write_input_bundle,
    write_scb_input,
    write_scb_snapshot,
)
from _lisa_fixtures import write_lisa_workbook
from _source_inspection_fixtures import field_text
from pydantic import ValidationError
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.source_evidence import SourceField, SourceRevision
from reg_meta_build.input_snapshot import (
    LisaWorkbookSelection,
    open_input_bundle,
    open_scb_snapshot,
)
from reg_meta_build.source_inspection import (
    CensusCompletion,
    inspect_bundle_source_records,
    write_scb_observation_census,
)
from reg_meta_build.source_periods import source_scopes
from reg_meta_build.source_records import SourceFields, TemporalScope, value_field
from reg_meta_build.sources.scb_records import LISA_REGISTER_ID, iter_scb_observations

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def _read_census(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        return [json.loads(line) for line in source]


def _census(tmp_path: Path, name: str, rows: list[str]) -> list[dict[str, Any]]:
    input_dir = tmp_path / name / "source"
    write_scb_input(input_dir, registerinformation_rows=rows)
    selection = write_input_bundle(tmp_path / name / "accepted", input_dir)
    destination = tmp_path / f"{name}.jsonl.gz"
    write_scb_observation_census(
        open_input_bundle(selection), destination, code_commit="c" * 40
    )
    return _read_census(destination)


def _observation(lines: list[dict[str, Any]], member_id: int) -> dict[str, Any]:
    return next(
        line
        for line in lines
        if line["type"] == "observation"
        and line["record"]["subject"]["native"]["member_id"] == member_id
    )


def test_focused_lisa_inspection_skips_unrelated_malformed_native_row(
    tmp_path: Path,
) -> None:
    unrelated = var_row(colname="Other", cvid=1, var_id=1)
    unrelated_fields = unrelated.split("|")
    header = REGISTERINFORMATION_HEADER.split("|")
    unrelated_fields[header.index("RegVerID")] = "not-an-id"
    lisa = var_row(
        colname="AmPolTyp",
        cvid=2,
        var_id=31619,
        year="2020",
        regver_id=200,
        register=("LISA", 34, 153),
    )
    input_dir = tmp_path / "source"
    write_scb_input(
        input_dir,
        registerinformation_rows=["|".join(unrelated_fields), lisa],
    )
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )

    report = inspect_bundle_source_records(
        open_input_bundle(selection), code_commit="c" * 40
    )

    scb_records = [
        record
        for record in report.source_records
        if record.source == "scb-registerinformation"
    ]
    assert len(scb_records) == 1
    assert scb_records[0].subject.native.register_id == 34
    assert scb_records[0].locators[0].physical_record == "row:3"
    assert report.summary.scb_total_occurrences == 1


def test_scb_identity_survives_reorder_revision_and_unrelated_edit(
    tmp_path: Path,
) -> None:
    target = var_row(
        colname="Signal",
        cvid=2181,
        var_id=1880,
        vardef="Selected definition",
    )
    unrelated = var_row(colname="Other", cvid=9001, var_id=9000)
    unrelated_edit = var_row(
        colname="Other",
        cvid=9001,
        var_id=9000,
        vardef="An unrelated edit",
    )

    original = _census(tmp_path, "identity-original", [target, unrelated])
    reordered = _census(tmp_path, "identity-reordered", [unrelated_edit, target])
    changed_payload = _census(
        tmp_path,
        "identity-payload",
        [
            unrelated_edit,
            var_row(
                colname="Signal",
                cvid=2181,
                var_id=1880,
                vardef="Changed definition",
            ),
        ],
    )
    changed_context = _census(
        tmp_path,
        "identity-context",
        [
            unrelated_edit,
            var_row(
                colname="Signal",
                cvid=2181,
                var_id=1880,
                vardef="Selected definition",
                population_name="Another population",
            ),
        ],
    )

    original_record = _observation(original, 2181)["record"]
    reordered_record = _observation(reordered, 2181)["record"]
    assert original_record["record_id"] == reordered_record["record_id"]
    assert (
        original_record["source_revision_id"] != reordered_record["source_revision_id"]
    )
    assert original_record["locators"][0]["physical_record"] == "row:2"
    assert reordered_record["locators"][0]["physical_record"] == "row:3"

    original_completion = original[-1]
    reordered_completion = reordered[-1]
    for record, completion in (
        (original_record, original_completion),
        (reordered_record, reordered_completion),
    ):
        assert record["source_revision_id"] == completion["pins"]["source_revision_id"]
        assert (
            completion["pins"]["source_revision_id"]
            == completion["source_revision"]["revision_id"]
        )
    for pin in (
        "bundle_manifest_sha256",
        "scb_snapshot_manifest_sha256",
        "source_revision_id",
    ):
        assert original_completion["pins"][pin] != reordered_completion["pins"][pin]
    assert (
        original_completion["source_revision"]["artifact_sha256"]
        != reordered_completion["source_revision"]["artifact_sha256"]
    )

    assert (
        _observation(changed_payload, 2181)["record"]["record_id"]
        != original_record["record_id"]
    )
    assert (
        _observation(changed_context, 2181)["record"]["record_id"]
        != original_record["record_id"]
    )


def test_context_group_keeps_members_when_one_context_has_own_alternatives(
    tmp_path: Path,
) -> None:
    def row(data_length: str, population_name: str) -> str:
        return var_row(
            colname="Contextual",
            cvid=7001,
            var_id=7000,
            year="2004",
            regver_id=700,
            data_type="char",
            data_length=data_length,
            population_name=population_name,
        )

    rows = [
        row("10", "Population A"),
        row("11", "Population A"),
        row("10", "Population B"),
    ]

    lines = _census(tmp_path, "contexts", rows)
    same_context = [
        line for line in lines if line["type"] == "same_complete_context_alternatives"
    ]
    separated = [
        line for line in lines if line["type"] == "context_separated_alternatives"
    ]

    assert len(same_context) == 1
    assert len(separated) == 1
    assert {
        member["locator"]["physical_record"]
        for alternative in separated[0]["alternatives"]
        for member in alternative["members"]
    } == {"row:2", "row:3", "row:4"}
    assert (
        len(
            {
                member["context_fingerprint"]
                for alternative in separated[0]["alternatives"]
                for member in alternative["members"]
            }
        )
        == 2
    )
    completion = CensusCompletion.model_validate_json(
        json.dumps(lines[-1], ensure_ascii=False)
    )
    assert completion.counts.context_separated_groups == 1
    assert completion.affected_memberships.context_separated_cvids == (7001,)


def test_temporal_group_crosses_edition_specific_cvids_without_expanding_pool(
    tmp_path: Path,
) -> None:
    rows = [
        var_row(
            colname="Temporal",
            cvid=8101,
            var_id=8100,
            year="2001",
            versionname="2001-2003",
            regver_id=801,
            data_type="char",
            data_length="10",
        ),
        var_row(
            colname="Temporal",
            cvid=8102,
            var_id=8100,
            year="2004",
            regver_id=802,
            data_type="char",
            data_length="11",
        ),
    ]

    lines = _census(tmp_path, "temporal", rows)
    groups = [
        line for line in lines if line["type"] == "unproved_temporal_alternatives"
    ]

    assert len(groups) == 1
    assert not any(part.startswith("member:") for part in groups[0]["comparison_key"])
    members = [
        member
        for alternative in groups[0]["alternatives"]
        for member in alternative["members"]
    ]
    assert {member["native"]["member_id"] for member in members} == {8101, 8102}
    assert {member["native"]["edition_id"] for member in members} == {801, 802}
    assert len(members) == 2
    pooled = next(
        line
        for line in lines
        if line["type"] == "observation"
        and line["record"]["subject"]["native"]["member_id"] == 8101
    )
    assert pooled["record"]["edition_scope"]["kind"] == "pooled"
    assert pooled["interpretation_issue"]["kind"] == "pooled_period"
    completion = CensusCompletion.model_validate_json(
        json.dumps(lines[-1], ensure_ascii=False)
    )
    assert completion.counts.unproved_temporal_groups == 1
    assert completion.counts.unproved_temporal_cvids == 2
    assert completion.affected_memberships.unproved_temporal_cvids == (8101, 8102)


def test_source_fields_distinguish_missing_unknown_negative_and_sensitivity() -> None:
    missing = SourceFields()
    unknown = SourceField(status="unknown", raw_value="")
    negative = SourceField(status="negative", raw_value="Nej")

    assert missing.column_name is None
    assert unknown.status == "unknown"
    assert negative.status == "negative"
    assert SourceFields(availability=negative).availability == negative
    sensitivity = SourceFields(sensitivity=value_field(False)).sensitivity
    assert sensitivity is not None
    assert sensitivity.status == "value"
    with pytest.raises(ValidationError, match="negative is supported only"):
        SourceFields(sensitivity=negative)
    assert SourceFields(
        sensitivity=value_field("conditional", raw="I vissa fall")
    ).sensitivity == SourceField(
        status="value", value="conditional", raw_value="I vissa fall"
    )
    with pytest.raises(ValidationError, match="use negative status"):
        SourceFields(availability=value_field(False))


def test_raw_scb_reader_preserves_native_instances_fields_and_period_limits(
    tmp_path: Path,
) -> None:
    rows = [
        var_row(
            colname="AmPolTyp",
            cvid=1,
            var_id=31619,
            year="2020",
            regver_id=200,
            register=("LISA", 34, 153),
            vardef="Definition 2020",
            varopdef="Operational 2020",
        ),
        var_row(
            colname="",
            cvid=2,
            var_id=31619,
            year="2021",
            regver_id=201,
            register=("LISA", 34, 153),
            data_type="text",
            data_length="3",
        ),
        var_row(
            colname="Pooled",
            cvid=3,
            var_id=3,
            year="2018",
            versionname="2018-2019",
            regver_id=202,
            register=("LISA", 34, 1335),
        ),
        var_row(
            colname="UnknownPeriod",
            cvid=4,
            var_id=4,
            year="2020",
            versionname="okänd utgåva",
            regver_id=203,
            register=("LISA", 34, 153),
        ),
    ]
    scb_dir = write_scb_input(tmp_path / "source", registerinformation_rows=rows)
    selection = write_scb_snapshot(tmp_path / "accepted", scb_dir)
    snapshot = open_scb_snapshot(selection)
    item = next(
        item
        for item in snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    assert item.raw_size is not None and item.raw_sha256 is not None
    revision = SourceRevision.create(
        dataset="scb-registerinformation",
        publisher="SCB",
        purpose="fixture raw rows",
        upstream_revision=snapshot.manifest.edition,
        artifact_path="Registerinformation.csv",
        artifact_size=item.raw_size,
        artifact_sha256=item.raw_sha256,
    )

    observations = tuple(
        observation
        for observation in iter_scb_observations(snapshot, revision)
        if observation.record.subject.native.register_id == LISA_REGISTER_ID
    )
    records = tuple(observation.record for observation in observations)
    issues = tuple(
        observation.issue
        for observation in observations
        if observation.issue is not None
    )

    assert len(records) == 4
    blank = next(record for record in records if record.subject.native.member_id == 2)
    assert blank.fields.column_name == SourceField(status="unknown", raw_value="")
    assert blank.fields.availability == value_field(True)
    assert blank.subject.native.variable_id == 31619
    assert blank.subject.native.register_variant_id == 153
    assert field_text(blank, "data_type") == "text"
    assert field_text(blank, "data_length") == "3"
    pooled = next(record for record in records if record.subject.native.member_id == 3)
    assert pooled.edition_scope == TemporalScope(
        kind="pooled",
        label="2018-2019",
        pooled_start="2018-01-01",
        pooled_end="2019-12-31",
    )
    unknown = next(record for record in records if record.subject.native.member_id == 4)
    assert unknown.edition_scope.kind == "unknown"
    assert [(issue.kind, issue.record_id) for issue in issues] == [
        ("pooled_period", pooled.record_id),
        ("unparseable_period", unknown.record_id),
    ]


def test_selected_scb_observation_invalid_native_id_names_field_and_row(
    tmp_path: Path,
) -> None:
    row = replace_registerinformation_cell(
        scb_interpretation_rows()[0], "VarId", "broken"
    )
    scb_dir = write_scb_input(tmp_path / "source", registerinformation_rows=[row])
    snapshot = open_scb_snapshot(write_scb_snapshot(tmp_path / "accepted", scb_dir))
    item = next(
        item
        for item in snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    assert item.raw_size is not None and item.raw_sha256 is not None
    revision = SourceRevision.create(
        dataset="scb-registerinformation",
        publisher="SCB",
        purpose="invalid native ID fixture",
        upstream_revision=snapshot.manifest.edition,
        artifact_path="Registerinformation.csv",
        artifact_size=item.raw_size,
        artifact_sha256=item.raw_sha256,
    )

    with pytest.raises(RegMetaError) as error:
        tuple(iter_scb_observations(snapshot, revision, register_id=34))

    assert error.value.code == "scb_native_id_invalid"
    assert error.value.exit_code == EXIT_CONFIG
    assert "row 2, field VarId: 'broken'" in error.value.message


@pytest.mark.parametrize(
    ("version_name", "expected_kind", "expected_issue"),
    (
        ("1990, 2000", "unknown", "unparseable_period"),
        ("LISA 2011 och 2019", "unknown", "unparseable_period"),
        ("1990-2000", "pooled", "pooled_period"),
        ("LISA 2011", "intervals", None),
    ),
)
def test_scb_scope_rejects_multi_year_tokens_only_on_single_claim_fallback(
    version_name: str, expected_kind: str, expected_issue: str | None
) -> None:
    edition_scope, reference_scope, issue = source_scopes(version_name)

    assert edition_scope.kind == expected_kind
    assert reference_scope.kind == expected_kind
    assert issue == expected_issue
