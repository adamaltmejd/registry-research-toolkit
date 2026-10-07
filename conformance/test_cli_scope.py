"""CLI JSON and order bytes observed against independently authored case data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case", sorted((CASES / "cli_scope").iterdir()), ids=lambda p: p.name
)
def test_scoped_cli_json(case: Path, tmp_path: Path, capsys) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    argv = ["--db", str(path.parent), "--format", "json", *request["argv"]]
    code = run(argv)
    output = json.loads(capsys.readouterr().out)
    if request.get("page") == 2:
        assert output["next_cursor"]
        code = run([*argv, "--cursor", output["next_cursor"]])
        output = json.loads(capsys.readouterr().out)
    observe = request["observe"]
    if observe == "error":
        actual = {"exit_code": code, "error": output["error"]}
    else:
        assert code == 0, output
        if observe == "register-variants":
            actual = {
                "fqid": output["fqid"],
                "variants": [v["slug"] for v in output["variants"]],
            }
        elif observe == "schema-columns":
            versions = [
                version
                for variant in output["variants"]
                for version in variant["versions"]
            ]
            actual = {
                "columns": sorted({c["fqid"] for v in versions for c in v["columns"]}),
                "windows": sorted({(v["valid_from"], v["valid_to"]) for v in versions}),
            }
            actual["windows"] = [list(window) for window in actual["windows"]]
        elif observe == "classification-variables":
            actual = {
                "names": sorted(row["variable_name"] for row in output["variables"])
            }
        elif observe == "logical-groups":
            actual = {
                "members": sorted(
                    member["fqid"]
                    for group in output["registers"][0]["groups"]
                    for member in group["members"]
                )
            }
        elif observe == "logical-varinfo":
            actual = {
                "columns": sorted(
                    {
                        column
                        for state in output["instances"]
                        for column in state["aliases"]
                    }
                ),
                "years": sorted({state["year"] for state in output["instances"]}),
            }
        elif observe == "logical-values":
            actual = {
                "codes": sorted(
                    {
                        code["code"]
                        for state in output["instances"]
                        for code in state["values"]
                    }
                ),
                "years": sorted({state["year"] for state in output["instances"]}),
            }
        elif observe == "logical-datacolumns":
            actual = {
                "columns": sorted({row["delivery_column_name"] for row in output})
            }
        elif observe == "logical-diff":
            actual = {
                "changed": sorted(
                    {
                        row["variable_name"]
                        for variant in output["variants"]
                        for row in variant["changed"]
                    }
                )
            }
        elif observe == "logical-coded-variables":
            rows = sorted(output, key=lambda row: row["variable_name"])
            actual = {
                "names": [row["variable_name"] for row in rows],
                "code_counts": [row["n_distinct_codes"] for row in rows],
            }
        elif observe == "logical-resolve":
            actual = {"statuses": [row["status"] for row in output["columns"]]}
        elif observe == "search-page":
            actual = {
                "names": [row["name"] for row in output["results"]],
                "has_more": output["has_more"],
            }
        elif observe == "search-groups":
            actual = {
                "groups": [
                    {
                        key: row[key]
                        for key in ("group_key", "matched_count", "member_count")
                    }
                    for row in output["results"]
                    if row["type"] == "group"
                ]
            }
        else:
            actual = {field: output[field] for field in observe}
    assert actual == expected
