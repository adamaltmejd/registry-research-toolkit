"""Logical exports share compiled owner, variant and representation admission."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import open_db
from reg_meta.errors import RegMetaError

from reg_meta import queries

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case", sorted((CASES / "logical").iterdir()), ids=lambda p: p.name
)
def test_logical_export_scope(case: Path, tmp_path: Path) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    result: Any
    with open_db(path) as conn:
        try:
            if request["operation"] == "search_pages":
                result = cast("Any", {"codes": [], "has_more": []})
                cursor = None
                while True:
                    page = queries.search(
                        conn, *request["args"], **request["kwargs"], cursor=cursor
                    )
                    result["codes"].extend(
                        cast("Any", row).code for row in page.results
                    )
                    result["has_more"].append(page.has_more)
                    if not page.has_more:
                        break
                    cursor = page.next_cursor
                    assert cursor is not None
            elif request["operation"] == "catalog_method":
                result = getattr(Catalog(conn), request["method"])(
                    *request["args"], **request["kwargs"]
                )
            elif request["operation"] == "catalog_states":
                result = cast("Any", Catalog(conn).states(*request["args"]))
            else:
                result = getattr(queries, request["operation"])(
                    conn, *request["args"], **request["kwargs"]
                )
        except RegMetaError as exc:
            actual = {"exit_code": exc.exit_code, "code": exc.code}
        else:
            observe = request["observe"]
            if observe == "value-pages":
                actual = result
            elif observe == "state-warnings":
                actual = {
                    "codes": [
                        sorted(
                            request["warning_codes"][identity]
                            for identity in state.warning_ids
                        )
                        for state in result
                    ]
                }
            elif observe == "datacolumn-search":
                actual = {"columns": sorted(r.datacolumn for r in result.results)}
            elif observe == "fts-search":
                actual = {
                    "names": [r.name for r in result.results],
                    "columns": sorted(
                        {
                            column
                            for r in result.results
                            for column in r.delivery_column_names
                        }
                    ),
                }
            elif observe == "resolve-columns":
                actual = {
                    "columns": sorted(
                        {m["matched_column"] for r in result for m in r["matches"]}
                    )
                }
            elif observe == "search-group":
                actual = {
                    "members": sorted(
                        str(m.fqid)
                        for r in result.results
                        for m in getattr(r, "members", ())
                    ),
                    "has_more": result.has_more,
                }
            elif observe == "warnings":
                actual = {"codes": sorted(w.code for w in result)}
            elif observe == "coverage":
                actual = {"registers": sorted(result)}
            elif observe == "terminal":
                actual = {"target": str(result) if result is not None else None}
            elif observe == "varinfo":
                actual = {
                    "names": [v["name"] for v in result],
                    "columns": sorted(
                        {
                            alias
                            for v in result
                            for i in v["instances"]
                            for alias in i["aliases"]
                        }
                    ),
                    "years": sorted(
                        {i["year"] for v in result for i in v["instances"]}
                    ),
                }
            elif observe == "values":
                actual = {
                    "codes": sorted(
                        {v["code"] for i in result["instances"] for v in i["values"]}
                    ),
                    "years": sorted({i["year"] for i in result["instances"]}),
                }
            elif observe == "datacolumns":
                actual = {
                    "columns": sorted({r["delivery_column_name"] for r in result})
                }
            elif observe == "resolve":
                actual = {"statuses": [r["status"] for r in result]}
            elif observe == "diff":
                actual = {
                    "changed": sorted(
                        {
                            c["variable_name"]
                            for v in result["variants"]
                            for c in v["changed"]
                        }
                    ),
                    "unchanged": result.get("unchanged", []),
                }
            elif observe == "coded":
                result = sorted(result, key=lambda r: r["variable_name"])
                actual = {
                    "names": [r["variable_name"] for r in result],
                    "code_counts": [r["n_distinct_codes"] for r in result],
                }
            elif observe == "coded-ranked":
                # Result order is the contract here: tiers, ties and the cut.
                actual = {
                    "rows": [
                        [r["variable_name"], r["n_registers"], r["n_distinct_codes"]]
                        for r in result
                    ]
                }
            elif observe == "state-tokens":
                actual = {
                    "windows": [
                        [s.valid_from, s.valid_to, s.period_token] for s in result
                    ]
                }
            else:
                pytest.fail(f"{observe!r} is not an observe, and no error was raised")
    assert actual == expected
