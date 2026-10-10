"""The resolve phase's code fingerprint, which a resolved bundle records.

A bundle replayed by other resolve code would place stale resolutions under the
current builder commit, so `materialize-db` refuses any bundle whose fingerprint is
not the running builder's. The fingerprint covers what resolution runs and nothing
the writer alone runs, so a writer-only change still reuses a bundle:

- every `reg_meta_build` source file in the static import closure of
  `reg_meta_build.pipeline`: function-local imports count, `if TYPE_CHECKING:` bodies
  do not, and a module that imports by name at run time pulls in its whole package
  (`import_closure`, which the real-seed cache's prepare key also walks), plus the
  non-Python files beside a walked module. A `__version__ = "..."` value in a source
  file is normalized away: a release bumps it, and no resolution reads it;
- every file of the imported native `reg_core_py` package, extension included;
- the Python version.

Third-party Python dependencies are not covered; the real-seed cache keys `uv.lock`.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import reg_core_py

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

PACKAGE = "reg_meta_build"
RESOLVE_ROOT = "reg_meta_build.pipeline"
# The writer side. `pipeline`'s walk must never reach them: a writer-only change
# would then miss every bundle, and the fingerprint would silently widen.
RESOLVE_EXCLUDED = frozenset(
    {
        "reg_meta_build.artifact_identity",
        "reg_meta_build.db",
        "reg_meta_build.derive",
        "reg_meta_build.materialize",
        "reg_meta_build.validate",
    }
)
_VERSION = re.compile(rb'^__version__ = "[^"\n]*"$', re.MULTILINE)


def _imports(tree: ast.Module) -> Iterator[ast.Import | ast.ImportFrom]:
    stack: list[ast.AST] = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.If) and (
            getattr(node.test, "id", None) == "TYPE_CHECKING"
            or getattr(node.test, "attr", None) == "TYPE_CHECKING"
        ):
            stack.extend(node.orelse)
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            yield node
        stack.extend(ast.iter_child_nodes(node))


def _imports_dynamically(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.Call)
        and (
            getattr(node.func, "id", None) in {"import_module", "__import__"}
            or getattr(node.func, "attr", None) == "import_module"
        )
        for node in ast.walk(tree)
    )


def import_closure(
    source_root: Path,
    roots: Iterable[str],
    *,
    excluded: frozenset[str] = frozenset(),
) -> tuple[dict[str, Path], frozenset[str]]:
    """The `reg_meta_build` modules `roots` statically reach, by name, and the
    top-level names of the other modules they import.

    `source_root` holds the `reg_meta_build` package directory. A name under the
    package that is neither a module nor a package is an imported attribute. A root
    that is not a module raises, and so does reaching a module in `excluded` or under
    one.
    """

    def module_file(module: str) -> Path | None:
        if module.split(".")[0] != PACKAGE:
            return None
        base = source_root.joinpath(*module.split("."))
        if (base / "__init__.py").is_file():
            return base / "__init__.py"
        return base.with_suffix(".py") if base.with_suffix(".py").is_file() else None

    roots = tuple(roots)
    # A missing root would shrink the walk, and every key built on it, silently.
    if missing := [root for root in roots if module_file(root) is None]:
        raise RuntimeError(f"import walk roots are not modules: {missing}")
    seen: dict[str, Path] = {}
    foreign: set[str] = set()
    pending = list(roots)
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        if (path := module_file(module)) is None:
            if (top := module.split(".")[0]) != PACKAGE:
                foreign.add(top)
            continue
        if any(module == name or module.startswith(name + ".") for name in excluded):
            raise RuntimeError(
                f"the import walk from {', '.join(roots)} reaches excluded module "
                f"{module}; keep it out of that walk"
            )
        seen[module] = path
        parts = module.split(".")
        pending += [".".join(parts[:i]) for i in range(1, len(parts))]
        # A file the walk resolved but cannot read or parse raises here.
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        package = module if path.name == "__init__.py" else module.rpartition(".")[0]
        if _imports_dynamically(tree):
            pending += [
                ".".join(p.relative_to(source_root).with_suffix("").parts).removesuffix(
                    ".__init__"
                )
                for p in path.parent.rglob("*.py")
            ]
        for node in _imports(tree):
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            else:
                base = package.split(".")[: len(package.split(".")) - node.level + 1]
                target = (
                    ".".join([*base, *([node.module] if node.module else [])])
                    if node.level
                    else node.module or ""
                )
                targets = [target, *(f"{target}.{a.name}" for a in node.names)]
            pending += targets
    return seen, frozenset(foreign)


def code_files(modules: Iterable[Path]) -> set[Path]:
    """`modules` and the data files beside them. Dotfiles (editor and OS litter) are
    never package data."""
    files = set(modules)
    return files | {
        path
        for directory in {path.parent for path in files}
        for path in directory.iterdir()
        if path.is_file()
        and path.suffix not in {".py", ".pyc"}
        and not path.name.startswith(".")
    }


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _source_sha256(path: Path) -> str:
    """A walked file's digest, with any `__version__` value normalized away.

    A release bumps `reg_meta_build/__init__.py`'s `__version__`, which no resolution
    reads (only the CLI's help line does), so a release build keeps reusing the
    bundle its candidate resolved.
    """
    data = path.read_bytes()
    if path.suffix == ".py":
        data = _VERSION.sub(b'__version__ = ""', data)
    return hashlib.sha256(data).hexdigest()


def resolve_code_digests(source_root: Path | None = None) -> dict[str, object]:
    """The fingerprint's inputs: file digests by relative path, and the Python version.

    `source_root` defaults to the directory this package is imported from.
    """
    if source_root is None:
        source_root = Path(__file__).resolve().parents[1]
    modules, _ = import_closure(source_root, (RESOLVE_ROOT,), excluded=RESOLVE_EXCLUDED)
    native_root = Path(reg_core_py.__file__).resolve().parent
    return {
        "python": sys.version,
        "sources": {
            path.relative_to(source_root).as_posix(): _source_sha256(path)
            for path in sorted(code_files(modules.values()))
        },
        "native": {
            path.relative_to(native_root.parent).as_posix(): _sha256(path)
            for path in sorted(native_root.rglob("*"))
            if path.is_file() and "__pycache__" not in path.parts
        },
    }


def resolve_code_sha256(source_root: Path | None = None) -> str:
    """The digest of `resolve_code_digests`; `source_root` as there."""
    if source_root is None:
        return _running_resolve_code_sha256()
    return _digest(resolve_code_digests(source_root))


@functools.cache
def _running_resolve_code_sha256() -> str:
    # Cached: a build captures it before resolving, and the running code does not
    # change under it.
    return _digest(resolve_code_digests())


def _digest(digests: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(digests, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
