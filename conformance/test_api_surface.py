"""The surface inventory (`conformance/api/surface.toml`) matches the source.

Routes come from the committed OpenAPI snapshot; `reg-meta` subcommands and
`reg_meta` imports are discovered with `ast`; the skill's commands are matched against
its SKILL.md text. A discovered item without a row fails, and so does a row whose item
no longer exists. Discovery fails closed on source forms it does not model.
"""

from __future__ import annotations

import ast
import json
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SURFACE = ROOT / "conformance/api/surface.toml"
OPENAPI = ROOT / "reg_webapp/backend/openapi.json"
CLI = ROOT / "reg_meta/src/reg_meta/cli.py"
SKILL = ROOT / "plugins/microdata-tools-se/skills/register-metadata-search/SKILL.md"
PROVIDER = "reg_meta"

KINDS = {"route", "command", "import", "skill"}
DISPOSITIONS = {"retained", "replaced", "removed"}
OWNERS = {"3a", "3b", "3c", "3d", "3e", "4", "5"}
REQUIRED = {"kind", "id", "disposition", "owner", "covered_by"}
# `operation` is added by package 1.1 and checked by its test_api_spec.py.
OPTIONAL = {"note", "used_by", "operation"}


def _rows(kind: str | None = None) -> list[dict]:
    rows = tomllib.loads(SURFACE.read_text(encoding="utf-8"))["row"]
    return [r for r in rows if kind is None or r["kind"] == kind]


def _discover_routes() -> set[str]:
    """Routes as the committed OpenAPI schema lists them.

    `reg_webapp/backend/tests/test_openapi_snapshot.py` keeps the snapshot equal to
    the app's rendered schema, so a route cannot exist without appearing here.
    """
    paths = json.loads(OPENAPI.read_text(encoding="utf-8"))["paths"]
    return {f"{method.upper()} {path}" for path, ops in paths.items() for method in ops}


def _discover_commands() -> set[str]:
    """Argv paths of every `add_parser` in the CLI.

    Fails closed on any form the walk does not model: a parser or subparsers variable
    assigned twice, `aliases=`, a non-literal name, a result bound to anything but a
    plain name, or a subparsers action that does not hang off a known parser.
    """
    tree = ast.parse(CLI.read_text(encoding="utf-8"), str(CLI))
    parents = {
        child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)
    }
    subparsers: dict[str, str] = {}  # subparsers-action var -> owning parser var
    parsers: dict[str, tuple[str, str]] = {}  # parser var -> (subparsers var, name)
    leaves: list[tuple[str, str]] = []  # (subparsers var, name) of discarded results
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"add_parser", "add_subparsers"}
        ):
            continue
        where = f"{CLI.name}:{node.lineno}"
        assert isinstance(node.func.value, ast.Name), f"{where} non-name receiver"
        receiver = node.func.value.id
        parent = parents[node]
        if isinstance(parent, ast.Assign):
            assert len(parent.targets) == 1, where
            assert isinstance(parent.targets[0], ast.Name), where
            target = parent.targets[0].id
            assert target not in parsers and target not in subparsers, (
                f"{where} {target} assigned twice"
            )
        else:
            assert isinstance(parent, ast.Expr), f"{where} unmodelled result binding"
            target = None
        if node.func.attr == "add_subparsers":
            assert target is not None, f"{where} unbound subparsers action"
            subparsers[target] = receiver
            continue
        first = node.args[0] if node.args else None
        name = first.value if isinstance(first, ast.Constant) else None
        assert isinstance(name, str), f"{where} non-literal subcommand name"
        assert all(k.arg != "aliases" for k in node.keywords), f"{where} aliases="
        if target is None:
            leaves.append((receiver, name))
        else:
            parsers[target] = (receiver, name)
    roots = {owner for owner in subparsers.values() if owner not in parsers}
    assert len(roots) == 1, f"expected one root parser, found {sorted(roots)}"

    def path(action: str, name: str) -> str:
        assert action in subparsers, f"{action} is not a subparsers action"
        owner = subparsers[action]
        prefix = path(*parsers[owner]) + " " if owner in parsers else ""
        return prefix + name

    return {path(action, name) for action, name in [*parsers.values(), *leaves]}


def _consumer_files() -> list[Path]:
    """Every git-visible `.py` file (tracked, or untracked and not ignored) outside
    `reg_meta/` itself, so ignored trees such as the build seed and virtualenvs are
    never walked. The throwaway stage-0 spike is skipped; package 1.5 deletes it."""
    listed = subprocess.run(
        ["git", "ls-files", "-z", "-co", "--exclude-standard"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    ).stdout.decode()
    return sorted(
        ROOT / p
        for p in listed.split("\0")
        if p.endswith(".py")
        and not p.startswith((f"{PROVIDER}/", "spike/"))
        and (ROOT / p).exists()
    )


def _is_reg_meta(module: str) -> bool:
    return module == "reg_meta" or module.startswith("reg_meta.")


def _discover_imports() -> dict[str, set[str]]:
    """Map each imported `reg_meta` name to its consumer roots.

    `from M import N` is `M.N`; `import M` is `M`; so `from reg_meta import queries`
    and `import reg_meta.queries` are one row.
    """
    found: dict[str, set[str]] = {}
    for path in _consumer_files():
        consumer = path.relative_to(ROOT).parts[0]
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                if _is_reg_meta(node.module):
                    names = [f"{node.module}.{a.name}" for a in node.names]
            elif isinstance(node, ast.Import):
                names = [a.name for a in node.names if _is_reg_meta(a.name)]
            for name in names:
                found.setdefault(name, set()).add(consumer)
    return found


def _ids(kind: str) -> set[str]:
    return {r["id"] for r in _rows(kind)}


def _assert_same(kind: str, discovered: set[str]) -> None:
    rows = _ids(kind)
    assert not discovered - rows, f"{kind} without a row: {sorted(discovered - rows)}"
    stale = rows - discovered
    assert not stale, f"stale {kind} rows: {sorted(stale)}"


def test_rows_are_well_formed():
    rows = _rows()
    keys = [(r.get("kind"), r.get("id")) for r in rows]
    assert len(keys) == len(set(keys)), "duplicate rows"
    for row in rows:
        label = f"{row.get('kind')} {row.get('id')!r}"
        assert REQUIRED <= row.keys() <= REQUIRED | OPTIONAL, label
        assert row["kind"] in KINDS, label
        assert row["disposition"] in DISPOSITIONS, label
        assert row["owner"] in OWNERS, label
        missing = [p for p in row["covered_by"] if not (ROOT / p).exists()]
        assert not missing, f"{label} covered_by paths do not exist: {missing}"
        if row["kind"] == "import":
            assert row["used_by"] and PROVIDER not in row["used_by"], label
            # Build-side names move (or go) when stage 4 deletes reg_meta.
            if "reg_meta_build" in row["used_by"]:
                assert row["owner"] == "4", label
        else:
            assert "used_by" not in row, label


def test_every_route_has_a_row():
    _assert_same("route", _discover_routes())


def test_every_subcommand_has_a_row():
    _assert_same("command", _discover_commands())


def test_every_reg_meta_import_has_a_row():
    discovered = _discover_imports()
    _assert_same("import", set(discovered))
    wrong = {
        r["id"]: sorted(discovered[r["id"]])
        for r in _rows("import")
        if r["id"] in discovered and set(r["used_by"]) != discovered[r["id"]]
    }
    assert not wrong, f"used_by differs from the importing roots: {wrong}"


def test_skill_rows_name_documented_commands():
    text = SKILL.read_text(encoding="utf-8")
    absent = sorted(i for i in _ids("skill") if i not in text)
    assert not absent, f"skill rows no longer in {SKILL.name}: {absent}"
