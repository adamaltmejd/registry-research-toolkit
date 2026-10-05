"""CLI `get values`: code lists, multi-state views and value-set groups."""

from __future__ import annotations

import json

import pytest
from cli_test_support import build_cli_source
from reg_meta.cli import run


@pytest.fixture(scope="module")
def groups_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_cli_source(tmp_path_factory.mktemp("groups"), "cli-values-groups")


def _year_groups(db: str, capsys: pytest.CaptureFixture[str]) -> dict:
    argv = ["--db", db, "--format", "json", "get", "values", "Sex", "--year", "2017"]
    assert run(argv) == 0
    return json.loads(capsys.readouterr().out)


def test_year_groups_disagreeing_value_sets_largest_first(
    groups_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = _year_groups(groups_db, capsys)
    assert (
        payload["value_set_count"],
        payload["instance_count"],
        payload["register_count"],
    ) == (3, 15, 14)
    adults = [f"Reg{i:02d}" for i in range(1, 13)] + ["RegD"]
    assert [
        (
            [(value["code"], value["label"]) for value in group["values"]],
            group["instance_count"],
            group["register_count"],
            group["registers"],
            group["variable_slugs"],
        )
        for group in payload["groups"]
    ] == [
        ([("1", "Man"), ("2", "Woman")], 13, 13, adults, ["sex"]),
        ([("1", "Boy"), ("2", "Girl")], 1, 1, ["RegC"], ["sex-child"]),
        ([("F", "Female"), ("M", "Male")], 1, 1, ["RegD"], ["sex"]),
    ]


def test_year_groups_keep_per_column_owner_coordinates(
    groups_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = _year_groups(groups_db, capsys)
    owners = [
        instance
        for group in payload["groups"]
        for instance in group["instances"]
        if instance["register_name"] == "RegD"
    ]
    assert [
        (owner["delivery_column_name"], owner["value_set_version_label"])
        for owner in owners
    ] == [("SexA", "native-a"), ("SexQ", "native-q")]
    assert owners[0]["state_id"] == owners[1]["state_id"]


def test_year_groups_render_summary_listing_and_capped_registers(
    groups_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    argv = ["--db", groups_db, "--format", "list"]
    assert run([*argv, "get", "values", "Sex", "--year", "2017"]) == 0
    shown = ", ".join(f"Reg{i:02d}" for i in range(1, 11))
    assert capsys.readouterr().out.splitlines() == [
        "Variable 'Sex' — year 2017 — 3 distinct value set(s) across 15 "
        "instance(s) in 14 register(s)",
        "",
        "[Group 1] 13 instance(s) across 13 register(s)",
        "  1         Man",
        "  2         Woman",
        f"  Registers: {shown} (+3 more)",
        "",
        "[Group 2] 1 instance(s) across 1 register(s)",
        "  1         Boy",
        "  2         Girl",
        "  Registers: RegC",
        "",
        "[Group 3] 1 instance(s) across 1 register(s)",
        "  F         Female",
        "  M         Male",
        "  Registers: RegD",
    ]
