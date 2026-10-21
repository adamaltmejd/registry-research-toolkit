"""Run one `cases/build/` case: readable sources and curation through the real build.

`cases/build/README.md` is the case format. This module turns a case directory into
the boundary outputs its `expected.json` names: it prepares the source spec
(`_build_case_sources`), renders the curation tree's authoring placeholders
(`_build_case_curation`), runs `build_catalog`, and reads the result, the report
ledger and the built artifact through a fixed set of named projections
(`_build_case_tables`).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

from _build_case_curation import render_curation
from _build_case_sources import PreparedCache, source_spec
from _build_case_tables import Outcome, sorted_rows
from _pipeline_catalog_support import (
    report_events,
    write_authored,
)
from reg_meta_build.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.materialize import build_catalog
from reg_meta_build.pipeline import check_curation

CASES = Path(__file__).with_name("cases") / "build"


def case_dirs() -> list[Path]:
    """Every case directory, sorted; `_`-prefixed directories hold shared data."""
    return sorted(
        path
        for path in CASES.iterdir()
        if path.is_dir() and not path.name.startswith("_")
    )


def case_steps(case: Path) -> list[Path]:
    """A case is one step (its own `request.json`) or step subdirectories, run in the
    numeric order of their prefixes (`9-`, `10-`; `3b-` after `3-`)."""
    if (case / "request.json").is_file():
        return [case]

    def order(path: Path) -> tuple[int, str]:
        prefix = re.match(r"\d+", path.name)
        assert prefix, f"{case.name}: step {path.name} has no numeric prefix"
        return int(prefix.group()), path.name

    return sorted((path for path in case.iterdir() if path.is_dir()), key=order)


# -- Projections ----------------------------------------------------------------


# The fields of each table, as cases/build/README.md documents them.
FIELDS: dict[str, frozenset[str]] = {
    name: frozenset(fields.split())
    for name, fields in {
        "issues": "code severity subject case_id locator detail acknowledged_by "
        "valid_from valid_to fields withheld_output",
        "issue_refs": "code severity subject case_id locator detail acknowledged_by "
        "valid_from valid_to fields withheld_output source key ref",
        "cases": "case_id status reason",
        "uses": "source native_variable key column_name data_type name description "
        "use variable",
        "case_uses": "case_id source key use variable",
        "states": "register variable variant column valid_from valid_to data_type "
        "name state_name provenance pooled data_length definition measurement_unit "
        "description operational_definition source_register_text "
        "value_set_version_label",
        "state_codes": "register variable variant column valid_from valid_to code label",
        "variables": "register variable column provider_key name definition "
        "description measurement_unit operational_definition is_identifier "
        "is_sensitive deprecated source_register source_label source_register_text",
        "variants": "register variant name panel_entity_key panel_time_key "
        "panel_time_grain description display_group",
        "registers": "register name purpose",
        "editions": "register variant name description measurement_information "
        "documentation_status first_approved_at last_approved_at populations "
        "object_types",
        "tags": "slug member",
        "aliases": "register variable variant column",
        "alias_windows": "register variable variant column valid_from valid_to provenance "
        "column_metadata coding_metadata data_type data_length definition "
        "measurement_unit name description operational_definition "
        "source_register_text codes classifications",
        "concept_groups": "variables",
        "group_axes": "group_key axis ordinal label",
        "group_labels": "group_key label source",
        "group_members": "group_key register variable column facets",
        "warnings": "register variable variant column valid_from valid_to code "
        "severity detail summary detail_hash_of fields refs withheld_output "
        "acknowledged_by case_id warning_id",
        "report_warnings": "register variable variant column valid_from valid_to "
        "code severity detail summary detail_hash_of fields refs withheld_output "
        "acknowledged_by case_id warning_id",
        "search_pins": "query type position entity",
        "search_text": "register variable name definition description "
        "delivery_column_names",
        "manifest": "key value",
        "edges": "type a b",
        "successions": "grain predecessor successor predecessor_variant "
        "successor_variant predecessor_column successor_column variant "
        "effective_year note description",
        "classification_derivations": "derived source note",
        "lineage": "register variable variant column source_register "
        "source_variable source_variant source_column valid_from valid_to",
        "lineage_warnings": "register variable variant column valid_from valid_to "
        "kind message",
        "state_classifications": "register variable column valid_from valid_to "
        "classification provenance",
        "conformance": "register variable column valid_from valid_to window "
        "classification status checked matched nonconforming overlap",
        "conformance_codes": "register variable column valid_from valid_to window "
        "classification code label member_kind sentinel_meaning scoped_windows",
        "code_index": "register variable code label mapping_count",
        "value_sets": "id members variables",
        "classifications": "slug short_name name name_en publisher valid_from "
        "valid_to description url code_count valid_code_count supersedes",
        "classification_successions": "predecessor successor effective_year note",
        "classification_codes": "slug code label level is_valid parent",
        "relationships": "kind binding_status source_dataset owner",
        "evidence": "kind disposition",
        "source_issues": "kind severity descriptor_key physical_associations refs",
    }.items()
}


def _matches(row: dict, where: dict) -> bool:
    for key, wanted in where.items():
        value = row[key]
        if isinstance(wanted, dict):
            if wanted.keys() != {"contains"}:
                raise ValueError(f"unknown where operator: {wanted}")
            texts = wanted["contains"]
            if value is None or not all(
                text in str(value)
                for text in ([texts] if isinstance(texts, str) else texts)
            ):
                return False
        elif isinstance(wanted, list):
            if value not in wanted:
                return False
        elif value != wanted:
            return False
    return True


def _cited_refs(outcome: Outcome, selector: dict) -> list[str]:
    """Every source ref the rows of ``selector`` cite, in the `_refs` string form."""
    known = FIELDS[selector["table"]]
    where = selector.get("where", {})
    if selector.keys() - {"table", "where"} or not set(where) <= known:
        raise ValueError(f"unknown field in refs_of {selector}")
    if not known & {"ref", "refs"}:
        raise ValueError(f"refs_of names a table without refs: {selector}")
    return [
        ref
        for row in outcome.table(selector["table"])
        if _matches(row, where)
        for ref in (row["refs"] if "refs" in known else [row["ref"]])
    ]


def expected_rows(
    outcome: Outcome, spec: dict, earlier: dict[str, Outcome] | None = None
) -> list[list]:
    """A projection's expected rows: literal, the refs another projection cites, or
    the same projection of an earlier step of the case.

    `{"refs_of": {"table": ..., "where": ...}}` states a relation instead of values:
    the projection's one `ref` field must list exactly the refs those rows cite. Both
    sides are read from the same build, so no source ref is written as a literal.
    `{"step": "<earlier step>"}` expects the rows this projection (its table, `where`
    and `fields`) reads from that step's build, so a value a case must not write as a
    literal, such as a stored id, is still compared across builds.
    """
    rows = spec["rows"]
    if isinstance(rows, list):
        return rows
    if rows.keys() == {"step"}:
        if rows["step"] not in (earlier or {}):
            raise ValueError(f"no earlier step {rows['step']!r} in projection {spec}")
        found = project(earlier[rows["step"]], {**spec, "match": "exact"})["rows"]
        if not found and spec.get("match") == "includes":
            # Nothing to include would compare nothing.
            raise ValueError(f"earlier step reads no rows for projection {spec}")
        return found
    if rows.keys() != {"refs_of"} or spec["fields"] != ["ref"]:
        raise ValueError(f"unknown expected rows in projection {spec}")
    return [[ref] for ref in _cited_refs(outcome, rows["refs_of"])]


def project(outcome: Outcome, spec: dict) -> dict:
    """The actual value of one expected projection, shaped like ``spec``."""
    known = FIELDS[spec["table"]]
    where = spec.get("where", {})
    if not set(spec["fields"]) | set(where) <= known:
        raise ValueError(f"unknown field in projection {spec}")
    if any(isinstance(w, dict) and w.keys() != {"contains"} for w in where.values()):
        raise ValueError(f"unknown where operator in projection {spec}")
    if spec.get("match", "exact") not in {"exact", "set", "includes"}:
        raise ValueError(f"unknown match in projection {spec}")
    table = outcome.table(spec["table"])
    # Every row must carry exactly the documented fields, so an empty table
    # cannot hide a misspelled field.
    assert all(row.keys() == known for row in table), spec["table"]
    rows = [
        [row[field] for field in spec["fields"]]
        for row in table
        if _matches(row, spec.get("where", {}))
    ]
    match = spec.get("match", "exact")
    if match == "set":
        unique: list[list] = []
        for row in rows:
            if row not in unique:
                unique.append(row)
        rows = unique
    elif match == "includes":
        rows = [row for row in spec["rows"] if row in rows]
    elif match != "exact":
        raise ValueError(f"unknown match: {match}")
    return {**spec, "rows": sorted_rows(rows)}


# -- Running a step ---------------------------------------------------------------


def _subset(actual, expected):
    """``actual`` cut to the shape of ``expected``: only the keys it names, nested.

    An expected list names members that must be present, in its own order.
    """
    if isinstance(expected, dict) and isinstance(actual, dict):
        return {key: _subset(actual.get(key), value) for key, value in expected.items()}
    if isinstance(expected, list) and isinstance(actual, list):
        return [item for item in expected if item in actual]
    return actual


def _refusal(error: Exception, expected: dict) -> dict:
    """A refusal as `reg-meta-build build-db` and `check-curation` report it, cut to the
    keys ``expected`` names.

    Both commands wrap a `ValueError`, `OSError` or `KeyError` from the pipeline in the
    configuration error `pipeline_build_failed` (`cli._cmd_build_db`,
    `cli._cmd_check_curation`).
    """
    if isinstance(error, RegMetaError):
        refusal = {
            "code": error.code,
            "exit_code": error.exit_code,
            "message": error.message,
        }
    else:
        refusal = {
            "code": "pipeline_build_failed",
            "exit_code": EXIT_CONFIG,
            "message": str(error),
        }
    refusal["message_contains"] = [
        text for text in expected["message_contains"] if text in refusal["message"]
    ]
    return {key: refusal[key] for key in expected if key in refusal}


_REBUILD = """
import json, sys
from pathlib import Path
from reg_meta_build.materialize import build_catalog
a = json.loads(sys.argv[1])
build_catalog(
    Path(a["prepared"]), a["commit"], a["digest"], Path(a["output"]),
    Path(a["report"]), curation_dir=Path(a["curation_dir"]),
    diagnostic=a["diagnostic"], registers=tuple(a["registers"]),
    dump_decisions=Path(a["dump_decisions"]),
    resolved_out=Path(a["resolved_out"]),
)
"""


def _rebuild_in_fresh_process(args: dict) -> None:
    """Build again in a new interpreter under a different string-hash seed.

    The first build ran under this process's seed: random unless `PYTHONHASHSEED`
    fixes it. The rebuild takes seed 0, or 1 when this process already runs under 0,
    so the two builds never share a fixed seed and an output that depends on set or
    dict iteration order over strings shows up as a byte difference.
    """
    seed = "1" if os.environ.get("PYTHONHASHSEED") == "0" else "0"
    subprocess.run(
        [sys.executable, "-c", _REBUILD, json.dumps(args)],
        env={**os.environ, "PYTHONHASHSEED": seed},
        check=True,
        capture_output=True,
    )


def _tree_bytes(path: Path) -> dict[str, bytes]:
    """Every file under ``path`` (or ``path`` itself), by relative name.

    A missing or empty output is refused: two builds that both skip an output would
    otherwise compare equal and pass the rebuild check.
    """
    if path.is_file():
        return {"": path.read_bytes()}
    files = {
        str(file.relative_to(path)): file.read_bytes()
        for file in sorted(path.rglob("*"))
        if file.is_file()
    }
    if not files:
        raise AssertionError(f"rebuilt_identical: {path} holds no output to compare")
    return files


def _bundle_payloads(bundle: Path) -> dict:
    """The payload digests a resolve bundle's `bundle.json` records."""
    record = json.loads((bundle / "bundle.json").read_text(encoding="utf-8"))
    return record["payloads"]


def run_step(
    step: Path,
    cache: PreparedCache,
    scratch: Path,
    earlier: dict[str, Outcome] | None = None,
) -> tuple[dict, dict]:
    """Run one case step; return ``(actual, expected)`` in the same shape.

    ``earlier`` maps the case's already-run step names to their builds. A step that
    builds a catalog joins it, so a later step's `{"step": ...}` rows can read it.
    """
    request = json.loads((step / "request.json").read_text(encoding="utf-8"))
    # A case states the product change that makes it fail, so a reviewer can check
    # that its oracle can fail at all (cases/build/README.md).
    fails_if = request.get("fails_if")
    if not isinstance(fails_if, str) or not fails_if.strip():
        raise ValueError(f"{step}/request.json: fails_if must be a non-empty string")
    expected = json.loads((step / "expected.json").read_text(encoding="utf-8"))
    built_spec = source_spec(request["sources"])
    built = cache.get(built_spec)
    authored = cache.get(source_spec(request.get("authored_from", request["sources"])))
    curation = render_curation(step / "curation", scratch / "curation", authored, built)
    write_authored(built_spec, curation)
    output, report = scratch / "reg_meta.db", scratch / "report"
    registers = tuple(request.get("registers", ()))
    rebuild = bool(expected.get("rebuilt_identical"))

    def build(output: Path, report: Path, decisions: Path | None) -> dict:
        return build_catalog(
            built.prepared,
            built.commit,
            built.digest,
            output,
            report,
            curation_dir=curation,
            diagnostic=request.get("diagnostic", True),
            registers=registers,
            dump_decisions=decisions,
            resolved_out=scratch / "resolved" if rebuild else None,
        )

    try:
        if request.get("mode", "build") == "check":
            result = check_curation(
                built.prepared,
                built.commit,
                built.digest,
                report,
                curation_dir=curation,
                registers=registers,
            )
        else:
            result = build(output, report, scratch / "decisions" if rebuild else None)
    except (RegMetaError, ValueError, OSError, KeyError) as error:
        if "error" not in expected:
            raise
        return {"error": _refusal(error, expected["error"])}, expected
    actual: dict = {}
    if rebuild:
        again = scratch / "rebuild"
        again.mkdir()
        _rebuild_in_fresh_process(
            {
                "prepared": str(built.prepared),
                "commit": built.commit,
                "digest": built.digest,
                "output": str(again / "reg_meta.db"),
                "report": str(again / "report"),
                "curation_dir": str(curation),
                "diagnostic": request.get("diagnostic", True),
                "registers": list(registers),
                "dump_decisions": str(again / "decisions"),
                "resolved_out": str(again / "resolved"),
            }
        )
        actual["rebuilt_identical"] = all(
            _tree_bytes(scratch / name) == _tree_bytes(again / name)
            for name in ("reg_meta.db", "report/events.jsonl.gz", "decisions")
        ) and _bundle_payloads(scratch / "resolved") == _bundle_payloads(
            again / "resolved"
        )
    if "error" in expected:
        actual["error"] = None
    if "status" in expected:
        actual["status"] = result["status"]
    if "result" in expected:
        actual["result"] = _subset(result, expected["result"])
    outcome = Outcome(result, report_events(report), output, built)
    if earlier is not None and request.get("mode", "build") == "build":
        earlier[step.name] = outcome
    if "projections" in expected:
        specs = [
            {**spec, "rows": expected_rows(outcome, spec, earlier)}
            for spec in expected["projections"]
        ]
        actual["projections"] = [project(outcome, spec) for spec in specs]
        expected = {
            **expected,
            "projections": [
                {**spec, "rows": sorted_rows(spec["rows"])} for spec in specs
            ],
        }
    return actual, expected
