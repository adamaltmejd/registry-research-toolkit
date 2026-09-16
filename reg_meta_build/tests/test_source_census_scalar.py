"""Focused coverage for non-shape SCB observation disagreements."""

from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING

from _csv_fixtures import _var_row, write_input_bundle, write_scb_input
from reg_meta_build.input_snapshot import open_input_bundle
from reg_meta_build.source_inspection import (
    CENSUS_INTERPRETATION_ID,
    CensusCompletion,
    write_scb_observation_census,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def _read_census(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        return [json.loads(line) for line in source]


def test_census_reports_same_shape_non_shape_scalar_disagreements(
    tmp_path: Path,
) -> None:
    first = _var_row(
        colname="Scalar",
        cvid=4242,
        var_id=77,
        year="2004",
        regver_id=9,
        data_type="char",
        data_length="10",
        vardef="Definition A",
    )
    changed = _var_row(
        colname="Scalar",
        cvid=4242,
        var_id=77,
        year="2004",
        regver_id=9,
        data_type="char",
        data_length="10",
        vardef="Definition B",
    )
    input_dir = tmp_path / "source"
    write_scb_input(input_dir, registerinformation_rows=[first, first, changed])
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    destination = tmp_path / "census.jsonl.gz"

    write_scb_observation_census(
        open_input_bundle(selection), destination, code_commit="c" * 40
    )

    lines = _read_census(destination)
    observations = [line for line in lines if line["type"] == "observation"]
    groups = [
        line for line in lines if line["type"] == "non_shape_scalar_disagreements"
    ]
    assert len(groups) == 1
    group = groups[0]
    assert group["column"]["interpreted_value"] == "Scalar"
    assert group["comparison_scope"] == "same_native_edition_and_exact_column"
    assert sorted(
        len(alternative["members"]) for alternative in group["alternatives"]
    ) == [1, 2]
    assert all(
        member["context_fingerprint"] == alternative["context_fingerprint"]
        for alternative in group["alternatives"]
        for member in alternative["members"]
    )

    grouped_record_ids = {
        member["record_id"]
        for alternative in group["alternatives"]
        for member in alternative["members"]
    }
    assert grouped_record_ids == {
        observation["record"]["record_id"] for observation in observations
    }
    definitions = {
        cell["interpreted_value"]
        for observation in observations
        for cell in observation["record"]["delivered_cells"]
        if cell["name"] == "Variabeldefinition"
    }
    assert definitions == {"Definition A", "Definition B"}

    completion = CensusCompletion.model_validate_json(
        json.dumps(lines[-1], ensure_ascii=False)
    )
    assert completion.schema_version == 2
    assert completion.pins.interpretation_id == CENSUS_INTERPRETATION_ID
    assert completion.counts.unique_observations == 2
    assert completion.counts.duplicate_occurrences == 1
    assert completion.counts.same_complete_context_groups == 0
    assert completion.counts.context_separated_groups == 0
    assert completion.counts.non_shape_scalar_groups == 1
    assert completion.counts.non_shape_scalar_cvids == 1
    assert completion.affected_memberships.non_shape_scalar_cvids == (4242,)
