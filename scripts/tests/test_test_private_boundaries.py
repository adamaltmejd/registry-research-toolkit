"""Enforce public test boundaries. The frozen file allowlist only shrinks.

Existing package violations are drained in plan 06; conformance has zero tolerance.
Public protocol dunders are allowed. Bare private fixture modules importing public
names are legacy test support; private imported names are still violations.
Only network, filesystem, subprocess, environment, clock and platform patches pass.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = {
    "reg_meta_build/tests/test_classifications.py",
    "reg_meta/tests/test_alias_window_representations.py",
    "reg_meta/tests/test_commands.py",
    "reg_meta/tests/test_concept_group_surfaces.py",
    "reg_meta/tests/test_coverage.py",
    "reg_meta/tests/test_fqid_properties.py",
    "reg_meta/tests/test_graph.py",
    "reg_meta/tests/test_integration_harness.py",
    "reg_meta/tests/test_inventory.py",
    "reg_meta/tests/test_schema8_catalog.py",
    "reg_meta/tests/test_search_classifications.py",
    "reg_meta/tests/test_search_row_model.py",
    "reg_meta/tests/test_update.py",
    "reg_meta/tests/test_value_code_search.py",
    "reg_meta/tests/test_var_id_nonnumeric.py",
    "reg_meta/tests/test_var_id_uncollapse.py",
    "reg_meta_build/tests/_lisa_fixtures.py",
    "reg_meta_build/tests/_representation_fixtures.py",
    "reg_meta_build/tests/_shared_fixtures.py",
    "reg_meta_build/tests/test_alias_windows.py",
    "reg_meta_build/tests/test_build_db.py",
    "reg_meta_build/tests/test_catalog_lineage.py",
    "reg_meta_build/tests/test_catalog_resolution.py",
    "reg_meta_build/tests/test_concept_group_candidates.py",
    "reg_meta_build/tests/test_convert_matrix.py",
    "reg_meta_build/tests/test_curated_source_records.py",
    "reg_meta_build/tests/test_curation_compile.py",
    "reg_meta_build/tests/test_dbdiff.py",
    "reg_meta_build/tests/test_doc_db.py",
    "reg_meta_build/tests/test_entity_key_pins.py",
    "reg_meta_build/tests/test_extend_db.py",
    "reg_meta_build/tests/test_fqid_slugs.py",
    "reg_meta_build/tests/test_fqid_slugs_properties.py",
    "reg_meta_build/tests/test_input_snapshot.py",
    "reg_meta_build/tests/test_period_family_merges.py",
    "reg_meta_build/tests/test_pipeline.py",
    "reg_meta_build/tests/test_prepared_catalog.py",
    "reg_meta_build/tests/test_prepared_sources.py",
    "reg_meta_build/tests/test_prepared_values.py",
    "reg_meta_build/tests/test_repo_curation_tomls.py",
    "reg_meta_build/tests/test_resolved_catalog.py",
    "reg_meta_build/tests/test_semantic_diff.py",
    "reg_meta_build/tests/test_sos_adapter.py",
    "reg_meta_build/tests/test_sos_evidence.py",
    "reg_meta_build/tests/test_sos_parser.py",
    "reg_meta_build/tests/test_source_census_scalar.py",
    "reg_meta_build/tests/test_source_classification_bindings.py",
    "reg_meta_build/tests/test_source_coding_choices.py",
    "reg_meta_build/tests/test_source_curation.py",
    "reg_meta_build/tests/test_source_documentary.py",
    "reg_meta_build/tests/test_source_effects.py",
    "reg_meta_build/tests/test_source_inspection.py",
    "reg_meta_build/tests/test_source_naming.py",
    "reg_meta_build/tests/test_source_observation_repairs.py",
    "reg_meta_build/tests/test_source_parent_facts.py",
    "reg_meta_build/tests/test_source_records.py",
    "reg_meta_build/tests/test_source_representations.py",
    "reg_meta_build/tests/test_source_scope.py",
    "reg_meta_build/tests/test_source_value_bindings.py",
    "reg_meta_build/tests/test_split_sibling_suspects.py",
    "reg_meta_build/tests/test_succession_candidates.py",
    "reg_meta_build/tests/test_swecov_build_catalog.py",
    "reg_meta_build/tests/test_swecov_column_types.py",
    "reg_meta_build/tests/test_tags.py",
    "reg_meta_build/tests/test_triage.py",
    "reg_meta_build/tests/test_validate.py",
    "reg_meta_build/tests/test_value_code_fts.py",
    "reg_schema/tests/test_structural.py",
    "reg_webapp/backend/tests/test_docs.py",
    "reg_webapp/backend/tests/test_period_grammar_parity.py",
    "reg_webapp/backend/tests/test_project_validate.py",
    "reg_webapp/backend/tests/test_run_search_eval.py",
    "reg_webapp/backend/tests/test_search.py",
    "scripts/tests/test_gh_issue.py",
    "scripts/tests/test_prototype_scb_inputs.py",
}
PROCESS_ROOTS = {
    "os",
    "sys",
    "subprocess",
    "time",
    "tempfile",
    "shutil",
    "fcntl",
    "urllib",
    "requests",
    "httpx",
    "pathlib",
    "sqlite3",
    "socket",
    "builtins",
    "io",
    "zipfile",
    "tarfile",
}


def private(name):
    return name.startswith("_") and not (name.startswith("__") and name.endswith("__"))


def dotted(node, aliases):
    if isinstance(node, ast.Name):
        return aliases.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        return f"{dotted(node.value, aliases)}.{node.attr}"
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return ast.unparse(node)


def violations(source, *, conformance=False):
    tree = ast.parse(source)
    aliases = {}
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                aliases[alias.asname or alias.name.split(".")[0]] = (
                    alias.name if alias.asname else alias.name.split(".")[0]
                )
                if any(private(part) for part in alias.name.split(".")):
                    hits.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                aliases[alias.asname or alias.name] = (
                    f"{node.module}.{alias.name}" if node.module else alias.name
                )
                if private(alias.name) or (
                    node.module
                    and node.module != "__future__"
                    and any(private(part) for part in node.module.split("."))
                    and (conformance or not node.module.startswith("_"))
                ):
                    hits.append((node.lineno, f"{node.module}.{alias.name}"))
    for node in ast.walk(tree):
        if conformance and isinstance(node, ast.Attribute) and private(node.attr):
            hits.append((node.lineno, dotted(node, aliases)))
        if not isinstance(node, ast.Call):
            continue
        if (
            conformance
            and isinstance(node.func, ast.Name)
            and node.func.id in {"getattr", "hasattr"}
            and len(node.args) > 1
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
            and private(node.args[1].value)
        ):
            hits.append(
                (
                    node.lineno,
                    f"{node.func.id}({dotted(node.args[0], aliases)}, {node.args[1].value})",
                )
            )
        called = dotted(node.func, aliases)
        is_patch = called in {
            "patch",
            "unittest.mock.patch",
            "mock.patch",
            "mocker.patch",
        } or called.endswith((".patch.object", ".patch.dict"))
        is_monkey = called.startswith("monkeypatch.") and called.split(".")[-1] in {
            "setattr",
            "delattr",
            "setitem",
            "delitem",
        }
        if not (is_patch or is_monkey):
            continue
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        target_node = node.args[0] if node.args else keywords.get("target")
        if target_node is None:
            hits.append((node.lineno, f"{called}(unresolved target)"))
            continue
        target = dotted(target_node, aliases)
        if called.endswith(
            (".setattr", ".delattr", ".patch.object")
        ) and not isinstance(target_node, ast.Constant):
            attribute = (
                node.args[1]
                if len(node.args) > 1
                else keywords.get("name", keywords.get("attribute"))
            )
            if attribute is not None:
                target += "." + dotted(attribute, aliases)
        # Only direct process imports or literal process paths qualify. Calls,
        # subscripts and product-module re-exports cannot prove a process boundary.
        # The module registry is an internal-module patch even through imported sys.
        structured = isinstance(target_node, (ast.Name, ast.Attribute, ast.Constant))
        candidate = target_node
        while isinstance(candidate, ast.Attribute):
            candidate = candidate.value
        structured = structured and isinstance(candidate, (ast.Name, ast.Constant))
        parts = target.split(".")
        boundary = (
            structured
            and all(part.isidentifier() for part in parts)
            and parts[0] in PROCESS_ROOTS
            and parts[:2] != ["sys", "modules"]
            and (
                isinstance(target_node, ast.Constant)
                or isinstance(candidate, ast.Name)
                and candidate.id in aliases
            )
        )
        if not boundary:
            hits.append((node.lineno, f"{called}({target})"))
    return sorted(hits)


def test_existing_tests_use_public_boundaries(python_test_files):
    files = python_test_files
    assert files, "No test files scanned"
    observed = {}
    for path in files:
        relative = path.relative_to(ROOT).as_posix()
        hits = violations(
            path.read_text(), conformance="conformance" in path.relative_to(ROOT).parts
        )
        if hits:
            observed[relative] = hits
    new = set(observed) - ALLOWLIST
    stale = ALLOWLIST - set(observed)
    assert not new and not stale, (
        "New violations: "
        + repr({name: observed[name] for name in sorted(new)})
        + "; remove stale allowlist entries: "
        + repr(sorted(stale))
    )


@pytest.mark.parametrize(
    "source",
    [
        "from reg_meta.queries import _private",
        "import reg_meta._private as alias",
        "from reg_meta._private import public",
        "from unittest.mock import patch as replace\nreplace('reg_meta.queries.search')",
        "from reg_meta import queries as q\nmonkeypatch.setattr(q, 'search', replacement)",
        "monkeypatch.setitem(golden._PIN_BUILDERS, 'key', value)",
        "mocker.patch.object(module, 'name', replacement)",
        "from reg_meta import queries\nqueries._helper()",
        "from reg_meta import queries\ngetattr(queries, '_helper')()",
        "from reg_meta import queries\nhasattr(queries, '_helper')",
        "import sys\nmonkeypatch.setattr(sys.modules['reg_meta.queries'], 'search', fake)",
        "import sys\nmonkeypatch.setitem(sys.modules, 'reg_meta.queries', fake)",
        "from reg_meta import queries\nmonkeypatch.setattr(queries.os, 'getenv', fake)",
        "from reg_meta import queries\nmonkeypatch.setattr(queries.time, 'time', fake)",
        "from reg_meta import queries\nmonkeypatch.setattr(queries.sqlite3, 'connect', fake)",
        "monkeypatch.setattr(type(output), 'replace', fail_replace)",
        "from unittest.mock import patch as replace\nreplace(target='reg_meta.queries.search')",
        "from unittest.mock import patch as replace\nreplace.object(target=module, attribute='search', new=fake)",
    ],
)
def test_lint_rejects_private_imports_and_internal_patches(source):
    assert violations(source, conformance=True)


@pytest.mark.parametrize(
    "source",
    [
        "from reg_meta import __version__",
        "from reg_meta.cli import run",
        "import urllib.request as network\nmonkeypatch.setattr(network, 'urlopen', replacement)",
        "import subprocess\nmonkeypatch.setattr(subprocess, 'run', replacement)",
        "from pathlib import Path\nmonkeypatch.setattr(Path, 'replace', replacement)",
        "from unittest.mock import patch as replace\nreplace(target='subprocess.run')",
        "from unittest.mock import patch as replace\nimport subprocess\nreplace.object(target=subprocess, attribute='run', new=fake)",
        "monkeypatch.setenv('REG_META_DB', directory)",
    ],
)
def test_lint_allows_public_imports_and_process_boundaries(source):
    assert not violations(source, conformance=True)
