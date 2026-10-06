"""SCB observation census: complete alternatives, order independence and the --all-scb CLI."""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from typing import TYPE_CHECKING

from _csv_fixtures import (
    var_row,
    write_input_bundle,
    write_scb_input,
)
from _source_inspection_fixtures import (
    InterpreterCheckout,
    record_path_opens,
    snapshot_record_files,
)
from reg_meta.errors import EXIT_CONFIG, EXIT_USAGE
from reg_meta_build.input_snapshot import (
    open_input_bundle,
)
from reg_meta_build.source_inspection import (
    CensusCompletion,
    write_scb_observation_census,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    import pytest


def _census_rows() -> list[str]:
    populations = (
        "Byggnader",
        "Fastigheter",
        "Taxeringsenheter",
    )
    ftr = [
        var_row(
            colname=column,
            cvid=6487,
            var_id=3275,
            year="2004",
            versionname="2004-01-01",
            regver_id=164,
            data_type="numeric",
            data_length=length,
            population_name=population,
            object_name="Fastighet",
            object_definition="Fast egendom",
            register=("FTR", 7, 195),
        )
        for column, length in (("SumYtaTot", "8"), ("YtaByggTot", "7"))
        for population in populations
    ]
    hamn = var_row(
        colname="Signal",
        cvid=2181,
        var_id=1880,
        year="2003",
        regver_id=204,
        data_type="char",
        data_length="10",
        population_name="Havsgående fartyg",
        object_name="Flyttbart objekt",
        object_definition="Flyttbart objekt",
        register=("HAMN", 161, 232),
    )
    hamn_alternative = hamn.replace("|char|10|2181|", "|char|11|2181|")
    bast = [
        var_row(
            colname="Lopnr",
            cvid=12049,
            var_id=2179,
            year="2004",
            regver_id=329,
            data_type=data_type,
            data_length=length,
            population_name=population,
            object_name="Organisation",
            object_definition="Organisation",
            register=("BAST", 165, 238),
        )
        for population in (
            "Icke-finansiella företag ",
            (
                "Icke-finansiella företag med summa finansiella tillgångar "
                "och skulder över 20 milj kr."
            ),
        )
        for data_type, length in (("int", "0"), ("numeric", "8"))
    ]
    context_separated = [
        var_row(
            colname="Contextual",
            cvid=999,
            var_id=999,
            year="2004",
            regver_id=330,
            data_type="char",
            data_length=length,
            population_name=population,
        )
        for population, length in (("Population A", "10"), ("Population B", "11"))
    ]
    temporal = [
        var_row(
            colname="Temporal",
            cvid=1000,
            var_id=1000,
            year=year,
            regver_id=regver_id,
            data_type="char",
            data_length=length,
        )
        for year, regver_id, length in (("2001", 331, "10"), ("2002", 332, "11"))
    ]
    pooled = var_row(
        colname="Pooled",
        cvid=1001,
        var_id=1001,
        year="2001",
        versionname="2001-2003",
        regver_id=333,
    ).replace("||", '|""|', 1)
    return [
        *ftr,
        hamn,
        hamn,
        hamn_alternative,
        *bast,
        *context_separated,
        *temporal,
        pooled,
    ]


def _read_census(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        return [json.loads(line) for line in source]


def test_scb_census_preserves_complete_alternatives_memberships_and_scopes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = _census_rows()
    input_dir = tmp_path / "source"
    write_scb_input(input_dir, registerinformation_rows=rows)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    bundle = open_input_bundle(selection)
    second_bundle = open_input_bundle(selection)
    record_files = snapshot_record_files(bundle.snapshot, "Registerinformation.csv")

    opened = record_path_opens(monkeypatch)
    first = tmp_path / "first.jsonl.gz"
    second = tmp_path / "second.jsonl.gz"
    summary = write_scb_observation_census(bundle, first, code_commit="c" * 40)
    write_scb_observation_census(second_bundle, second, code_commit="c" * 40)

    # One streaming pass per census: each prepared record file is opened once.
    assert Counter(path for path in opened if path in record_files) == dict.fromkeys(
        record_files, 2
    )
    assert first.read_bytes() == second.read_bytes()
    lines = _read_census(first)
    observations = [line for line in lines if line["type"] == "observation"]
    groups = [line for line in lines if line["type"].endswith("alternatives")]
    completion = CensusCompletion.model_validate_json(
        json.dumps(lines[-1], ensure_ascii=False)
    )
    assert completion.complete is True
    assert summary["evidence_sha256"] == hashlib.sha256(first.read_bytes()).hexdigest()
    assert completion.counts.rows == len(rows) == 18
    assert completion.counts.unique_observations == 17
    assert completion.counts.duplicate_occurrences == 1
    assert completion.counts.cvids == 6
    assert completion.counts.same_complete_context_groups == 3
    assert completion.counts.same_complete_context_cvids == 2
    assert completion.counts.context_separated_groups == 1
    assert completion.counts.context_separated_cvids == 1
    assert completion.counts.unproved_temporal_groups == 1
    assert completion.counts.unproved_temporal_cvids == 1
    assert completion.counts.across_column_only_groups == 1
    assert completion.counts.across_column_only_cvids == 1
    assert completion.counts.interpretation_issues == {"pooled_period": 1}
    assert completion.registerinformation_logical_sha256 == next(
        item.logical_sha256
        for item in bundle.snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )

    assert len(observations) == len(rows)
    assert all(len(item["record"]["delivered_cells"]) == 36 for item in observations)
    pooled = next(
        item
        for item in observations
        if item["record"]["subject"]["native"]["member_id"] == 1001
    )
    assert pooled["record"]["edition_scope"]["kind"] == "pooled"
    assert pooled["interpretation_issue"]["kind"] == "pooled_period"
    measurement = next(
        cell
        for cell in pooled["record"]["delivered_cells"]
        if cell["name"] == "Registerversionmätinformation"
    )
    assert measurement == {
        "name": "Registerversionmätinformation",
        "present": True,
        "raw_value": "",
        "interpreted_value": "",
    }
    missing = next(
        cell
        for cell in observations[0]["record"]["delivered_cells"]
        if cell["name"] == "Registerversionmätinformation"
    )
    assert missing["present"] is False
    assert "raw_value" not in missing

    hamn = next(
        group
        for group in groups
        if group["type"] == "same_complete_context_alternatives"
        and group["alternatives"][0]["members"][0]["native"]["member_id"] == 2181
    )
    by_length = {
        alternative["data_length"]["interpreted_value"]: alternative
        for alternative in hamn["alternatives"]
    }
    assert len(by_length["10"]["members"]) == 2
    assert len(by_length["11"]["members"]) == 1
    assert all(
        len(member["locator"]["physical_cells"]) == 36
        for alternative in hamn["alternatives"]
        for member in alternative["members"]
    )
    assert {group["type"] for group in groups} == {
        "same_complete_context_alternatives",
        "context_separated_alternatives",
        "unproved_temporal_alternatives",
        "across_column_only_alternatives",
    }
    across = next(
        group for group in groups if group["type"] == "across_column_only_alternatives"
    )
    assert {column["interpreted_value"] for column in across["columns"]} == {
        "SumYtaTot",
        "YtaByggTot",
    }


def test_scb_census_semantics_survive_source_row_reordering(tmp_path: Path) -> None:
    rows = _census_rows()
    summaries: list[dict[str, Any]] = []
    group_shapes: list[set[tuple[str, tuple[tuple[str, str], ...]]]] = []
    for label, ordered_rows in (("forward", rows), ("reverse", list(reversed(rows)))):
        input_dir = tmp_path / label / "source"
        write_scb_input(input_dir, registerinformation_rows=ordered_rows)
        selection = write_input_bundle(tmp_path / label / "accepted", input_dir)
        destination = tmp_path / f"{label}.jsonl.gz"
        summary = write_scb_observation_census(
            open_input_bundle(selection), destination, code_commit="c" * 40
        )
        summaries.append(summary["counts"])
        group_shapes.append(
            {
                (
                    line["type"],
                    tuple(
                        sorted(
                            (
                                alternative["data_type"]["interpreted_value"],
                                alternative["data_length"]["interpreted_value"],
                            )
                            for alternative in line["alternatives"]
                        )
                    ),
                )
                for line in _read_census(destination)
                if line["type"].endswith("alternatives")
            }
        )

    assert summaries[0] == summaries[1]
    assert group_shapes[0] == group_shapes[1]


def test_all_scb_cli_guards_census_destination(tmp_path: Path) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir, registerinformation_rows=_census_rows())
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    evidence = tmp_path / "census.jsonl.gz"
    summary = tmp_path / "summary.json"
    checkout = InterpreterCheckout(tmp_path / "interpreter")
    args = [
        "--output",
        str(summary),
        "inspect-source-records",
        "--input-bundle",
        str(selection.path),
        "--input-commit",
        selection.input_commit,
        "--input-manifest-sha256",
        selection.manifest_sha256,
        "--all-scb",
        "--evidence",
        str(evidence),
    ]

    first = checkout.run(args)
    assert first.returncode == 0, first.stderr
    assert first.stdout == ""
    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["complete"] is True
    assert payload["evidence"] == str(evidence.resolve())
    before = evidence.read_bytes()

    assert checkout.run(args).returncode == EXIT_CONFIG
    assert evidence.read_bytes() == before
    conflict = tmp_path / "conflict.jsonl.gz"
    conflict_args = [
        "--output",
        str(conflict),
        *args[2:-1],
        str(conflict),
    ]
    assert checkout.run(conflict_args).returncode == EXIT_USAGE
    assert not conflict.exists()
