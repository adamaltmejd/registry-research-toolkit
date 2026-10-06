"""Enforce public test boundaries. The frozen file allowlist only shrinks.

Existing package violations are drained in plan 06; conformance has zero tolerance.
Public protocol dunders are allowed. Bare private fixture modules importing public
names are legacy test support; private imported names are still violations.
Only network, filesystem, subprocess, environment, clock and platform patches pass.
The AST scan covers explicit imports/access/mutations, not arbitrary dynamic execution.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = {
    "reg_meta_build/tests/_shared_fixtures.py",
    "reg_meta_build/tests/test_alias_windows.py",
    "reg_meta_build/tests/test_convert_matrix.py",
    "reg_meta_build/tests/test_curated_source_records.py",
    "reg_meta_build/tests/test_curation_compile.py",
    "reg_meta_build/tests/test_period_family_merges.py",
    "reg_meta_build/tests/test_repo_curation_tomls.py",
    "reg_meta_build/tests/test_source_classification_bindings.py",
    "reg_meta_build/tests/test_source_coding_choices.py",
    "reg_meta_build/tests/test_source_curation.py",
    "reg_meta_build/tests/test_source_documentary.py",
    "reg_meta_build/tests/test_source_effects.py",
    "reg_meta_build/tests/test_source_naming.py",
    "reg_meta_build/tests/test_source_parent_facts.py",
    "reg_meta_build/tests/test_source_representations.py",
    "reg_meta_build/tests/test_source_scope.py",
    "reg_meta_build/tests/test_source_value_bindings.py",
    "reg_meta_build/tests/test_swecov_build_catalog.py",
    "reg_meta_build/tests/test_tags.py",
    "reg_meta_build/tests/test_triage.py",
    "reg_schema/tests/test_structural.py",
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

PRODUCT_ROOTS = {"reg_meta", "reg_meta_build", "reg_schema", "reg_webapp"}


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


def imported_path(node, aliases):
    if isinstance(node, ast.Name):
        return aliases.get(node.id)
    if isinstance(node, ast.Attribute):
        path = imported_path(node.value, aliases)
        return f"{path}.{node.attr}" if path else None
    if isinstance(node, ast.Subscript):
        return imported_path(node.value, aliases)
    return None


def imported_root(node, aliases):
    path = imported_path(node, aliases)
    return path.split(".")[0] if path else None


def process_boundary(node, aliases, *, target=None):
    target = dotted(node, aliases) if target is None else target
    candidate = node
    while isinstance(candidate, ast.Attribute):
        candidate = candidate.value
    parts = target.split(".")
    return (
        isinstance(candidate, (ast.Name, ast.Constant))
        and all(part.isidentifier() for part in parts)
        and parts[0] in PROCESS_ROOTS
        and parts[:2] != ["sys", "modules"]
        and (isinstance(node, ast.Constant) or imported_root(node, aliases) is not None)
    )


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
        if conformance and isinstance(node, (ast.Attribute, ast.Subscript)):
            target = dotted(node, aliases)
            imported = imported_path(node, aliases)
            if (
                isinstance(node, ast.Attribute)
                and private(node.attr)
                or (
                    isinstance(node.ctx, (ast.Store, ast.Del))
                    and (
                        imported_root(node, aliases) in PRODUCT_ROOTS
                        or (imported or "").split(".")[:2] == ["sys", "modules"]
                    )
                )
            ):
                hits.append((node.lineno, target))
        if not isinstance(node, ast.Call):
            continue
        called = dotted(node.func, aliases)
        if (
            conformance
            and called in {"getattr", "hasattr", "builtins.getattr", "builtins.hasattr"}
            and len(node.args) > 1
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
            and private(node.args[1].value)
        ):
            hits.append(
                (
                    node.lineno,
                    f"{called}({dotted(node.args[0], aliases)}, {node.args[1].value})",
                )
            )
        if conformance and called in {
            "importlib.import_module",
            "__import__",
            "builtins.__import__",
        }:
            module = (
                node.args[0]
                if node.args
                else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "name"
                    ),
                    None,
                )
            )
            if (
                isinstance(module, ast.Constant)
                and isinstance(module.value, str)
                and any(private(part) for part in module.value.split("."))
            ):
                hits.append((node.lineno, f"{called}({module.value})"))
        if (
            conformance
            and called in {"setattr", "delattr", "builtins.setattr", "builtins.delattr"}
            and node.args
            and imported_root(node.args[0], aliases) is not None
        ):
            target = dotted(node.args[0], aliases)
            if len(node.args) > 1:
                target += "." + dotted(node.args[1], aliases)
            if not process_boundary(node.args[0], aliases, target=target):
                hits.append((node.lineno, f"{called}({target})"))
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
        if not process_boundary(target_node, aliases, target=target):
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
        "from reg_meta import queries as q\nq.search = fake",
        "from reg_meta import queries as q\nq.search += fake",
        "from reg_meta import queries as q\nq.os.getenv = fake",
        "from reg_meta import queries as q\nq.registry['search'] = fake",
        "from reg_meta import queries as q\nq.registry['search'] += fake",
        "from reg_meta import queries as q\ndel q.registry['search']",
        "from reg_meta import queries as q\nq.registry['search'].handler = fake",
        "from reg_meta import queries as q\nq.search: object = fake",
        "from reg_meta import queries as q\ndel q.search",
        "from reg_meta import queries as q\n(q.search, local.value) = (fake, value)",
        "from reg_meta import queries as q\n[q.search, local.value] = [fake, value]",
        "from reg_meta import queries as q\nsetattr(q, 'search', fake)",
        "from reg_meta import queries as q\ndelattr(q, 'search')",
        "from reg_meta import queries as q\nfrom builtins import setattr as mutate\nmutate(q, 'search', fake)",
        "from reg_meta import queries as q\nimport builtins as b\nb.delattr(q, 'search')",
        "import sys\nsetattr(sys.modules['reg_meta.queries'], 'search', fake)",
        "import sys\nsys.modules['reg_meta.queries'] = fake",
        "import sys as runtime\nruntime.modules['reg_meta.queries'] = fake",
        "from sys import modules as registry\nregistry['reg_meta.queries'] = fake",
        "import sys\ndel sys.modules['reg_meta.queries']",
        "import sys\nsys.modules = fake",
        "import sys\nsetattr(sys, 'modules', fake)",
        "import sys\ndelattr(sys, 'modules')",
        "import sys as runtime\nfrom builtins import setattr as mutate\nmutate(runtime, 'modules', fake)",
        "import sys\ndelattr(sys.modules['reg_meta.queries'], 'search')",
        "import sys\nfrom builtins import setattr as mutate\nmutate(sys.modules['reg_meta.queries'], 'search', fake)",
        "import importlib\nimportlib.import_module('reg_meta._x')",
        "import importlib as loader\nloader.import_module('reg_meta._x')",
        "from importlib import import_module as load\nload(name='reg_meta._x')",
        "__import__('reg_meta._x')",
        "import builtins\nbuiltins.__import__('reg_meta._x')",
        "from builtins import __import__ as load\nload(name='reg_meta._x')",
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
        "fixture_model.value = value",
        "fixture_model.registry['value'] = value",
        "fixture_model.registry['value'].handler = value",
        "setattr(fixture_model.registry['value'], 'handler', value)",
        "fixture_model.value += value",
        "fixture_model.value: object = value",
        "del fixture_model.value",
        "setattr(fixture_model, 'value', value)",
        "delattr(fixture_model, 'value')",
        "from reg_meta.models import CatalogModel\nfixture_model = CatalogModel()\nfixture_model.value = value",
        "import os\nsetattr(os, 'getenv', fake)",
        "import subprocess\nsubprocess.run = fake",
        "from sys import modules as registry\nregistry = fake",
        "from reg_meta import queries as q\nq = fake",
        "import sys\nsetattr(sys, 'platform', fake)",
        "import subprocess\nfrom builtins import setattr as mutate\nmutate(subprocess, 'run', fake)",
        "import importlib\nimportlib.import_module('reg_meta.queries')",
        "__import__('reg_meta.queries')",
    ],
)
def test_lint_allows_public_imports_and_process_boundaries(source):
    assert not violations(source, conformance=True)
