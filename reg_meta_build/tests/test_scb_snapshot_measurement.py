"""Codec-sample and Git-history measurements of prepared SCB snapshots."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from _csv_fixtures import write_scb_input
from _snapshot_fixtures import scb_inventory
from reg_meta_build.input_snapshot import (
    load_manifest,
    measure_codec_sample,
    measure_git_history,
    prepare_snapshot,
)

if TYPE_CHECKING:
    import pytest

CODEC_CONTROL = Path(__file__).parent / "cases/scb-snapshot/codec-control.expected.json"


def test_codec_measurement_sqlite_control_covers_all_normalized_tables(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    measurement = measure_codec_sample(
        scb_inventory(tmp_path, source_dir),
        limits={"Registerinformation.csv": 2, "Vardemangder.csv": 3},
        codec_sample_lines=10,
    )
    assert measurement["sample_records"]["Registerinformation.csv"] == 2
    assert measurement["sample_records"]["Vardemangder.csv"] == 3
    control = measurement["sqlite_normalized_tables_control"]
    # The SQLite file size depends on the page size, so only its sign is pinned.
    assert control.pop("bytes") > 0
    assert control == json.loads(CODEC_CONTROL.read_text(encoding="utf-8"))


def test_content_keys_stay_stable_and_git_measurement_reports_update_growth(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    inventory = scb_inventory(tmp_path, source_dir)
    codec_measurement = measure_codec_sample(
        inventory,
        limits={"Registerinformation.csv": 2, "Vardemangder.csv": 3},
        codec_sample_lines=10,
    )
    assert codec_measurement["population_records"]["Registerinformation.csv"] == 11
    assert codec_measurement["population_records"]["Vardemangder.csv"] == 14
    assert codec_measurement["sample_records"]["Registerinformation.csv"] == 2
    assert codec_measurement["sample_records"]["Vardemangder.csv"] == 3
    assert codec_measurement["sampling"] == {
        "method": "evenly-spaced-source-ordinals-v1",
        "source_passes": 2,
    }
    assert codec_measurement["codec_sample"]
    initial = tmp_path / "initial"
    prepare_snapshot(inventory, initial, converter_commit="9" * 40)
    initial_manifest = load_manifest(initial)
    initial_values = next(
        item for item in initial_manifest.files if item.name == "Vardemangder.csv"
    )
    initial_dictionary = next(
        group.dictionary.path
        for group in initial_values.groups
        if group.name == "value"
    )

    with (source_dir / "Vardemangder.csv").open("ab") as handle:
        handle.write(b"NEW|NEW|007|New label|999999|0007\r\n")
    update = tmp_path / "update"
    prepare_snapshot(inventory, update, converter_commit="9" * 40)
    update_manifest = load_manifest(update)
    update_values = next(
        item for item in update_manifest.files if item.name == "Vardemangder.csv"
    )
    update_dictionary = next(
        group.dictionary.path for group in update_values.groups if group.name == "value"
    )

    assert set(
        (initial / initial_dictionary).read_text(encoding="utf-8").splitlines()
    ) < set((update / update_dictionary).read_text(encoding="utf-8").splitlines())
    measurement = measure_git_history(initial, update)
    assert measurement["changed_files"] >= 3
    assert measurement["inserted_lines"] > 0
    assert (
        measurement["two_commit_packed_object_bytes"]
        >= measurement["initial_packed_object_bytes"]
    )
    assert measurement["git_gc"] == "git gc --prune=now"
    assert measurement["git_version"].startswith("git version ")
    assert measurement["git_pack_settings"] == {
        "core.compression": "9",
        "pack.compression": "9",
        "pack.depth": "50",
        "pack.threads": "1",
        "pack.useSparse": "true",
        "pack.window": "10",
        "pack.windowMemory": "0",
        "repack.useDeltaBaseOffset": "true",
        "repack.writeBitmaps": "false",
    }


def test_git_measurement_reports_effective_command_scope_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = tmp_path / "initial"
    update = tmp_path / "update"
    initial.mkdir()
    update.mkdir()
    (initial / "records.tsv").write_text("initial\n", encoding="utf-8")
    (update / "records.tsv").write_text("updated\n", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "pack.window")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "0")

    measurement = measure_git_history(initial, update)

    assert measurement["git_pack_settings"]["pack.window"] == "0"


def test_codec_sample_ordinals_span_the_whole_stream(tmp_path: Path) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    sample = source_dir / "Sample.csv"
    # Ten records of widths 1, 2, 4, ..., 512: every set of the same number of
    # records has a distinct escaped byte count, so the count names the ordinals.
    widths = [2**ordinal for ordinal in range(10)]
    sample.write_bytes(b"v\r\n" + b"".join(b"x" * width + b"\r\n" for width in widths))
    inventory = scb_inventory(tmp_path, source_dir, extra=("Sample.csv",))

    def sampled(limit: int) -> tuple[int, int]:
        measurement = measure_codec_sample(inventory, limits={"Sample.csv": limit})
        return (
            measurement["sample_records"]["Sample.csv"],
            measurement["codec_sample"]["Sample.csv:records"]["escaped_tsv_bytes"],
        )

    # Each record is escaped as "t<value>\n" (width + 2 bytes).
    assert sampled(3) == (3, (1 + 2) + (16 + 2) + (512 + 2))  # records 0, 4 and 9
    assert sampled(1) == (1, 32 + 2)  # the middle record 5
    assert sampled(20) == (10, sum(width + 2 for width in widths))
