"""The resolve phase's code fingerprint, which a resolved bundle records.

A bundle replayed by other resolve code would place stale resolutions under the
current builder commit, so `materialize-db` refuses any bundle whose fingerprint is
not the running builder's. The fingerprint covers what resolution runs and nothing
the writer alone runs, so a writer-only change still reuses a bundle:

- every `reg_meta_build` source file in the static import closure of
  `reg_meta_build.pipeline`: function-local imports count, `if TYPE_CHECKING:` bodies
  do not, and a module that imports by name at run time pulls in its whole package
  (the walk of `scripts/real_seed_cache.py`'s `prepare_code_files`), plus the
  non-Python files beside a walked module;
- every file of the imported native `reg_core_py` package, extension included;
- the Python version.

Third-party Python dependencies are not covered; the real-seed cache keys `uv.lock`.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import reg_core_py

if TYPE_CHECKING:
    from collections.abc import Iterator

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


def resolve_modules(source_root: Path) -> dict[str, Path]:
    """The `reg_meta_build` modules `pipeline` statically reaches, by name.

    `source_root` holds the `reg_meta_build` package directory. A name under the
    package that is neither a module nor a package is an imported attribute.
    """

    def module_file(module: str) -> Path | None:
        if module.split(".")[0] != PACKAGE:
            return None
        base = source_root.joinpath(*module.split("."))
        if (base / "__init__.py").is_file():
            return base / "__init__.py"
        return base.with_suffix(".py") if base.with_suffix(".py").is_file() else None

    seen: dict[str, Path] = {}
    pending = [RESOLVE_ROOT]
    while pending:
        module = pending.pop()
        if module in seen or (path := module_file(module)) is None:
            continue
        if any(
            module == excluded or module.startswith(excluded + ".")
            for excluded in RESOLVE_EXCLUDED
        ):
            raise RuntimeError(
                f"the resolve import walk from {RESOLVE_ROOT} reaches writer module "
                f"{module}; keep the writer out of the resolve phase"
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
    return seen


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def resolve_code_digests(source_root: Path | None = None) -> dict[str, object]:
    """The fingerprint's inputs: file digests by relative path, and the Python version.

    `source_root` defaults to the directory this package is imported from.
    """
    if source_root is None:
        source_root = Path(__file__).resolve().parents[1]
    modules = resolve_modules(source_root)
    files = set(modules.values())
    # Data files beside a walked module, as `prepare_code_files` keys them; dotfiles
    # (editor and OS litter) are never package data.
    files |= {
        path
        for directory in {path.parent for path in modules.values()}
        for path in directory.iterdir()
        if path.is_file()
        and path.suffix not in {".py", ".pyc"}
        and not path.name.startswith(".")
    }
    native_root = Path(reg_core_py.__file__).resolve().parent
    return {
        "python": sys.version,
        "sources": {
            path.relative_to(source_root).as_posix(): _sha256(path)
            for path in sorted(files)
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
