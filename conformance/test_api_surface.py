"""The surface inventory (`conformance/api/surface.toml`) matches the source.

Routes, `reg-meta` subcommands and `reg_meta` imports are discovered with `ast` from
the source; the skill's commands are matched against its SKILL.md text. A discovered
item without a row fails, and so does a row whose item no longer exists. Imports are
read from every git-visible `.py` file (tracked or untracked, not ignored) under the
consumer roots, so the 14 GB untracked seed under `reg_meta_build/input_data` and
virtualenvs are never walked.
"""

from __future__ import annotations

import ast
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SURFACE = ROOT / "conformance/api/surface.toml"
ROUTES = ROOT / "reg_webapp/backend/src/reg_webapp/routes"
CLI = ROOT / "reg_meta/src/reg_meta/cli.py"
SKILL = ROOT / "plugins/microdata-tools-se/skills/register-metadata-search/SKILL.md"
CONSUMERS = ("conformance", "reg_meta_build", "reg_webapp")
HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}

KINDS = {"route", "command", "import", "skill"}
DISPOSITIONS = {"retained", "replaced", "removed"}
OWNERS = {"3a", "3b", "3c", "3d", "3e", "4", "5"}
REQUIRED = {"kind", "id", "disposition", "owner", "covered_by"}
OPTIONAL = {"note", "used_by"}


def _rows(kind: str | None = None) -> list[dict]:
    rows = tomllib.loads(SURFACE.read_text(encoding="utf-8"))["row"]
    return [r for r in rows if kind is None or r["kind"] == kind]


def _str_arg(call: ast.Call) -> str | None:
    if call.args and isinstance(call.args[0], ast.Constant):
        value = call.args[0].value
        return value if isinstance(value, str) else None
    return None


def _discover_routes() -> set[str]:
    found = set()
    for path in sorted(ROUTES.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        prefixes = {}
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id == "APIRouter"
            ):
                prefix = next(
                    (
                        k.value.value
                        for k in node.value.keywords
                        if k.arg == "prefix" and isinstance(k.value, ast.Constant)
                    ),
                    "",
                )
                for target in node.targets:
                    assert isinstance(target, ast.Name), path
                    prefixes[target.id] = prefix
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for deco in node.decorator_list:
                if (
                    isinstance(deco, ast.Call)
                    and isinstance(deco.func, ast.Attribute)
                    and deco.func.attr in HTTP_METHODS
                    and isinstance(deco.func.value, ast.Name)
                    and deco.func.value.id in prefixes
                ):
                    route = _str_arg(deco)
                    assert route is not None, f"{path}:{deco.lineno} non-literal route"
                    prefix = prefixes[deco.func.value.id]
                    found.add(f"{deco.func.attr.upper()} {prefix}{route}")
    return found


def _discover_commands() -> set[str]:
    tree = ast.parse(CLI.read_text(encoding="utf-8"), str(CLI))
    subparsers: dict[str, str] = {}  # subparsers-action var -> owning parser var
    parsers: dict[str, tuple[str, str]] = {}  # parser var -> (subparsers var, name)
    calls: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and isinstance(node.value.func.value, ast.Name)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            owner, target = node.value.func.value.id, node.targets[0].id
            if node.value.func.attr == "add_subparsers":
                subparsers[target] = owner
            elif node.value.func.attr == "add_parser":
                parsers[target] = (owner, _str_arg(node.value) or "")
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_parser"
            and isinstance(node.func.value, ast.Name)
        ):
            name = _str_arg(node)
            assert name is not None, f"{CLI}:{node.lineno} non-literal subcommand"
            calls.append((node.func.value.id, name))

    def parser_path(var: str) -> list[str]:
        if var not in parsers:
            return []
        action, name = parsers[var]
        return [*parser_path(subparsers[action]), name]

    return {
        " ".join([*parser_path(subparsers[action]), name]) for action, name in calls
    }


def _consumer_files() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "-z", "-co", "--exclude-standard", "--", *CONSUMERS],
        cwd=ROOT,
        capture_output=True,
        check=True,
    ).stdout.decode()
    return sorted(
        ROOT / p
        for p in listed.split("\0")
        if p.endswith(".py") and (ROOT / p).exists()
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
    assert not rows - discovered, f"stale {kind} rows: {sorted(rows - discovered)}"


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
            assert set(row["used_by"]) <= set(CONSUMERS), label
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
        if set(r["used_by"]) != discovered[r["id"]]
    }
    assert not wrong, f"used_by differs from the importing roots: {wrong}"


def test_skill_rows_name_documented_commands():
    text = SKILL.read_text(encoding="utf-8")
    absent = sorted(i for i in _ids("skill") if i not in text)
    assert not absent, f"skill rows no longer in {SKILL.name}: {absent}"
