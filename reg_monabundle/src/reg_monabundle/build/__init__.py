"""Build a single-file ``.py`` bundle for upload to MONA.

Amalgamates the caller-supplied runtime modules + the lightweight
``reg_monabundle`` slices (``constants``, ``scan``) into one
self-contained Python file. The user uploads that file, edits the
``configure()`` block near the top, and runs::

    python mdw_runner.py

The bundle is self-contained — only stdlib + duckdb + pyodbc + numpy
(all pre-installed on the WinPython distribution shipped with MONA's
batch client; see ``DESIGN.md`` for the runtime probe results).

§9.6 boundary: the bundle carries **no Pydantic and no ``reg_schema``**.
Structural validation (§6.8.1) is the **bundle-build gate**, not an
on-MONA step — but it does NOT run inside ``build_bundle`` (which only
embeds the JSON it is given). **Callers** (the mdw CLI, ``reg_webapp``)
run ``reg_monabundle.build.spec_loader.validate_project_data`` (the full
Pydantic ``reg_schema`` validator) on the spec *before* calling
``build_bundle``. The bundle's runtime deserializes the embedded /
sidecar JSON into a stdlib ``LoadedSpec`` via ``spec.loadedspec_from_dict``
and does **not** re-run structural validation (it does re-run the
pure-stdlib §6.8.2 block validator). ``reg_schema`` is therefore never
amalgamated; the source-scan gate in ``test_build_mona_bundle.py``
enforces the no-Pydantic invariant.

Each module's docstrings (module-, class-, and function-level) and all
``#`` comments are dropped during amalgamation: ``ast.unparse`` does not
preserve comments, and ``_slice_module`` strips the leading
string-literal statement from the module body and from every nested
class / function body. Class/method docstrings carry no import, so they
never affected the §9.6 boundary — but several mentioned ``reg_schema``
/ Pydantic, so dropping them keeps the artifact text-clean (and slightly
smaller). The repo source remains the documentation; the bundle is the
artifact.

The ``project_data.json`` may be embedded at build time via
``build_bundle(..., project_data=...)``; the runner deserializes it and
hands the resulting ``LoadedSpec`` directly to ``extract.main()``.
When not embedded, the runner falls back to reading
``project_data.json`` from the same directory as the bundle. The
embedded spec wins when both are present. Both load paths route through
``spec.loadedspec_from_dict`` (deserialize-only, no validation).

The runtime modules amalgamated by default live in
``reg_monabundle/runtime/`` (``classify``, ``sql_emit``, ``sources``,
``summarize``, ``spec``, ``extract``). Callers can still override via
``runtime_pkg_dir`` + ``runtime_module_order`` to wire in a custom
runtime (e.g. a steward-private extract pipeline), but the in-package
default is what mdw / reg_webapp / reg_mockdata all rely on.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import TYPE_CHECKING

# Imported for ``__file__`` only — used to derive the package-root dir for
# the ``constants`` / ``scan`` / runtime slices. ``reg_monabundle.__init__``
# imports this module (``from .build import build_bundle``), so this is a
# partial-init back-reference; ``__file__`` is set before the package body
# runs, so reading it here is safe and never triggers re-entry.
import reg_monabundle as _reg_monabundle

if TYPE_CHECKING:
    from collections.abc import Sequence

DEFAULT_OUTPUT_NAME = "mdw_runner.py"

# reg_monabundle modules amalgamated ahead of the runtime modules so
# summarize.py's ``SUPPRESS_K`` reference and extract.py's
# ``write_export(...)`` call (both via ``from reg_monabundle… import`` in
# the source) resolve inside the bundle. ``constants`` precedes ``scan``
# because the runtime modules amalgamated after this tuple (in particular
# ``extract``) call ``write_export``.
#
# ``validate`` is intentionally NOT amalgamated: post-A3.4 the runtime
# ``spec`` no longer calls ``validate_block`` — the namespaced-block
# validator runs at bundle-build time in ``spec_loader``, not on MONA
# (§9.6). Keeping it out trims the bundle and removes the last
# ``reg_monabundle.validate`` reference from the runtime slices.
#
# Derive from the package, not ``__file__``: this module now lives in
# ``reg_monabundle/build/__init__.py``, so ``Path(__file__).parent`` is
# the ``build/`` subdir, not the package root. The runtime modules and
# the ``constants`` / ``validate`` / ``scan`` slices live one level up,
# next to ``reg_monabundle.__init__``.
REG_MONABUNDLE_DIR = Path(_reg_monabundle.__file__).resolve().parent
# ``validate`` is amalgamated so the §6.8.2 namespaced-block validator
# (``validate_block`` — option keys + suppress_k floor, pure-stdlib) runs at
# bundle LOAD time on MONA too, per §6.8.2 (the §6.8.1 STRUCTURAL validator is
# build-time only). ``constants`` before ``validate`` (it reads SUPPRESS_K).
REG_MONABUNDLE_MODULE_ORDER = ("constants", "validate", "scan")

# The in-package runtime amalgamated into the bundle by default. Order
# is dep-locked: each module imports only earlier ones (intra-runtime)
# or the ``reg_monabundle`` slices amalgamated above.
# spec.py's ``assert set(INLINE_HINT_KEYS) == set(COLUMN_TYPES)``
# requires ``classify`` to be loaded first; ``extract`` is last because
# it depends on everything.
DEFAULT_RUNTIME_DIR = REG_MONABUNDLE_DIR / "runtime"
# ``_util`` leads the order because ``classify`` imports
# ``strip_project_prefix`` / ``lookup_with_prefix_fallback`` from it
# via ``from ._util import …`` — the slicer drops that relative
# import, so the helpers must already be defined at top level in the
# amalgamation by the time ``classify``'s body lands. (The leading
# underscore is a Python-namespace convention; once sliced into the
# bundle the modules are flat top-level code, so the underscore is
# irrelevant.)
DEFAULT_RUNTIME_MODULE_ORDER = (
    "_util",
    "classify",
    "sql_emit",
    "sources",
    "summarize",
    "spec",
    "extract",
)

BUNDLE_HEADER = '''\
"""mock-data-wizard MONA discover/extract bundle.

Self-contained single-file Python script. Edit configure() and the MODE
flag in the user-config block, then run on MONA's batch client:

    python mdw_runner.py

Two modes, one bundle:

  MODE = "discover"  (default)
    Cheap metadata-only walk -- INFORMATION_SCHEMA / DuckDB DESCRIBE
    plus COUNT(*). Output: mock_data_discovery.json next to this script.
    Copy mock_data_discovery.json off MONA, author project_data.json
    locally against REFACTOR_SPEC.md §6 (the §15 step 7 webapp will
    own this authoring once it lands), then either embed it via
    `mock-data-wizard build-bundle --project-data path/to/project_data.json`
    or upload it next to the bundle. Re-run with MODE = "extract".

  MODE = "extract"
    Typed aggregation. Reads project_data.json (embedded in this
    bundle if non-empty below; otherwise from the same directory as
    this script). Every column must carry a type override. Writes
    mock_data_stats.json.

PII discipline: only aggregate values cross the JSON boundary. Cell
suppression (k-anonymity, default threshold = 10), uniform noise
injection (+/- 0.5%) on numeric aggregates, and +/- 7-day jitter on
date min/max/quantiles are applied after server-side aggregation.
null_count is censored when 0 < null_count < k. The discovery JSON
carries metadata only -- column names, SQL types, row counts -- no
values.

This file is built from the mock_data_wizard package by
`mock-data-wizard build-bundle`. DO NOT edit code mid-bundle by hand --
edit the source modules and re-bundle.
"""
from __future__ import annotations

import os as _boot_os
import socket as _boot_socket
import sys as _boot_sys
import traceback as _boot_traceback
from datetime import datetime as _boot_datetime
from pathlib import Path as _boot_Path

_BOOT_HOST = _boot_socket.gethostname()
_BOOT_HERE = _boot_Path(__file__).resolve().parent


# ===========================================================================
# USER CONFIGURATION -- edit before running on MONA.
# ===========================================================================
# This is the only block you need to edit. Everything below this point is
# the bundled mock_data_wizard runtime (regenerate via
# `mock-data-wizard build-bundle` -- DO NOT edit module bodies by hand).
#
# DEBUG=False (default): no log file is written. On MBS-prefixed hosts
# (batch / RDP) stdout+stderr are still redirected to /dev/null to avoid
# the well-documented BatchClient in-memory buffer hang.
#
# DEBUG=True: a combined log file mdw_log_<HOST>_<TS>.txt is written next
# to this script. It captures the boot trace, our structured logging, AND
# whatever stdout / stderr emit (pyodbc / MSSQL driver / duckdb / numpy
# warnings). Single file, line-buffered, flushed per record.
#
# VERBOSE=True: in addition to DEBUG, enables per-column progress lines
# inside the log file. Worth it on a long sql_source extract; noisy
# otherwise. Has no effect when DEBUG=False.
#
# MODE picks the run flavour:
#   "discover" -- emits mock_data_discovery.json (metadata only).
#                 Default. SQL sources without `tables=` / `pattern=` /
#                 `all=True` are listed permissively here.
#   "extract"  -- emits mock_data_stats.json. Requires project_data.json,
#                 either embedded in this bundle (see __MDW_PROJECT_DATA_BLOCK__
#                 below) or sitting next to this script.
#                 Authoritative SOURCES filtering is required (no
#                 permissive listing in this mode).
#
# Override at runtime without editing the bundle:
#   MDW_MODE=extract python mdw_runner.py
#
# DISCOVER SHAPE: leave SOURCES wide and let discover walk everything:
#
#     return [sql_source(dsn="P1105")]
#     return [file_source(path=r"\\\\micro.intra\\projekt\\P1105$\\P1105_Data")]
#
# EXTRACT SHAPE: declare exactly what to aggregate. project_data.json
# (embedded or sidecar) declares per-column types.
#
#     return [
#         sql_source(
#             dsn="P1105",
#             tables=(
#                 sql_table("dbo.lisa_2018", where="AR > 2015"),
#                 sql_table("dbo.par",       where="INDATUM > '2015-01-01'"),
#                 "dbo.fodelse",
#             ),
#         ),
#         file_source(path=r"<unc-path>", include=("a.csv", "b.csv")),
#     ]
#
# configure() is called AFTER the bundle modules load, so file_source(),
# sql_source(), and sql_table() are all in scope here.
#
# CLASSIFIER_SEED controls the per-column sample used for subtype /
# date-format detection in extract mode. Same data + same seed -> same
# subtypes across reruns and across same-shape sibling tables (e.g.
# lisa_2015..2019). Vary it only if you have a reason to.
MODE = _boot_os.environ.get("MDW_MODE", "discover")
DEBUG = False
VERBOSE = False
CLASSIFIER_SEED = 0


# __MDW_CONFIGURE_BLOCK__


# ===========================================================================
# EMBEDDED PROJECT CONFIG (extract mode)
# ===========================================================================
# When this string is non-empty, the runner parses it as project_data.json
# and passes the resulting LoadedSpec directly to extract.main(); the
# sidecar project_data.json (if any) is ignored. When empty, extract mode
# falls back to reading project_data.json from this directory.
#
# Embed at build time:
#   mock-data-wizard build-bundle --project-data path/to/project_data.json
#
# Do NOT hand-edit the literal below; rebuild the bundle.
_PROJECT_DATA_JSON = r"""__MDW_PROJECT_DATA_JSON__"""


# ===========================================================================
# Boot wiring -- stdlib only. Runs before any package imports below so it
# can capture a heavy-import or dataclass-init crash.
# ===========================================================================
_BOOT_TS = _boot_datetime.now().strftime("%Y%m%d_%H%M%S")
_BOOT_PATH = _BOOT_HERE / f"mdw_log_{_BOOT_HOST}_{_BOOT_TS}.txt"
_BOOT_ON_MBS = _BOOT_HOST.upper().startswith("MBS")


def _boot_log(msg: str) -> None:
    if not DEBUG:
        return
    try:
        with _BOOT_PATH.open("a", encoding="utf-8") as fp:
            fp.write(f"[{_boot_datetime.now().isoformat()}] {msg}\\n")
    except Exception:
        pass


# Console redirect:
#   DEBUG=True  -> redirect stdout/stderr to the log file so library
#                  warnings / driver chatter land alongside our logs.
#   DEBUG=False -> on MBS hosts redirect to /dev/null (the documented
#                  fix for the BatchClient buffer hang); on non-MBS
#                  hosts leave the console alone (interactive use).
# Both paths also dup2 fd 1/fd 2 so C-extensions writing directly to
# the OS file descriptors are caught.
if DEBUG:
    _boot_redir_fp = open(_BOOT_PATH, "a", encoding="utf-8", buffering=1)  # noqa: SIM115
    _boot_sys.stdout = _boot_redir_fp
    _boot_sys.stderr = _boot_redir_fp
    try:
        _boot_os.dup2(_boot_redir_fp.fileno(), 1)
        _boot_os.dup2(_boot_redir_fp.fileno(), 2)
    except Exception:
        pass
elif _BOOT_ON_MBS:
    _boot_redir_fp = open(_boot_os.devnull, "w")  # noqa: SIM115
    _boot_sys.stdout = _boot_redir_fp
    _boot_sys.stderr = _boot_redir_fp
    try:
        _boot_devnull_fd = _boot_os.open(_boot_os.devnull, _boot_os.O_WRONLY)
        _boot_os.dup2(_boot_devnull_fd, 1)
        _boot_os.dup2(_boot_devnull_fd, 2)
    except Exception:
        pass


_boot_log(f"boot start host={_BOOT_HOST} cwd={_boot_os.getcwd()}")
_boot_log(f"script={_boot_Path(__file__).resolve()}")
_boot_log(f"python={_boot_sys.version.splitlines()[0]}")
_boot_log(f"MODE={MODE} DEBUG={DEBUG} VERBOSE={VERBOSE}")


def _boot_excepthook(exc_type, exc_val, exc_tb) -> None:
    _boot_log("UNCAUGHT EXCEPTION (excepthook):")
    _boot_log("".join(_boot_traceback.format_exception(exc_type, exc_val, exc_tb)))
    _boot_sys.__excepthook__(exc_type, exc_val, exc_tb)


_boot_sys.excepthook = _boot_excepthook
_boot_log("boot trace installed; loading bundle modules...")
'''

BUNDLE_RUNNER = """\
# ===========================================================================
# Runner -- everything user-editable is in the configure block above.
# ===========================================================================

import json as _runner_json
import logging

_boot_log("bundle modules loaded; configuring runner")

# Logging:
#   DEBUG=True  -> FileHandler on the combined log file. VERBOSE=True
#                  drops the level to DEBUG so per-column progress lines
#                  are emitted; otherwise we stay at INFO.
#   DEBUG=False -> NullHandler. No log file written.
_log_root = logging.getLogger()
if DEBUG:
    _log_root.setLevel(logging.DEBUG if VERBOSE else logging.INFO)
    _h = logging.FileHandler(_BOOT_PATH, mode="a", encoding="utf-8")
    _h.setFormatter(
        logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    )
    _log_root.addHandler(_h)
else:
    _log_root.setLevel(logging.CRITICAL + 1)
    _log_root.addHandler(logging.NullHandler())

_log = logging.getLogger("mdw.bundle")


def _load_embedded_spec():
    \"\"\"Deserialize the embedded project_data.json literal when non-empty.

    Returns None when the literal is empty (the runner falls back to
    reading project_data.json from this directory). Raises on invalid
    embedded JSON -- a structurally bad bundle should fail loudly, not
    silently fall through to the sidecar.

    No structural validation here (§9.6): the embedded JSON was
    validated at bundle-build time (spec_loader.validate_project_data);
    the runtime trusts it and only deserializes via loadedspec_from_dict.

    No duplicate-key guard here: the CLI build path (``_cmd_build_bundle``)
    parses the source file with ``_reject_duplicate_keys`` and then
    re-serializes via ``json.dumps``, so the literal embedded in the
    bundle is by construction a single-valued dict. Re-checking would
    be guarding generated output against a producer we control.
    \"\"\"
    stripped = _PROJECT_DATA_JSON.strip()
    if not stripped:
        return None
    payload = _runner_json.loads(stripped)
    return loadedspec_from_dict(payload)


if __name__ == "__main__":
    _log.info("output_dir=%s mode=%s", _BOOT_HERE, MODE)
    if MODE not in ("discover", "extract"):
        _log.error("MODE must be 'discover' or 'extract'; got %r", MODE)
        _boot_sys.exit(2)
    SOURCES = configure()
    _log.info("configure() returned %d source(s)", len(SOURCES))
    if not SOURCES:
        _log.error(
            "configure() returned []. Edit configure() in this file (e.g. "
            '`return [sql_source(dsn=\"<your_project_dsn>\")]`).'
        )
        _boot_sys.exit(2)
    embedded_spec = _load_embedded_spec() if MODE == "extract" else None
    try:
        result = main(
            SOURCES,
            output_dir=_BOOT_HERE,
            mode=MODE,
            classifier_seed=CLASSIFIER_SEED,
            spec=embedded_spec,
        )
    except Exception:
        _log.error("mdw bundle failed:\\n%s", _boot_traceback.format_exc())
        _boot_sys.exit(1)
    if MODE == "discover":
        _log.info(
            "discover complete: %d source(s), %s -> %s",
            len(result.get("sources", [])),
            DISCOVER_FILENAME,
            _BOOT_HERE / DISCOVER_FILENAME,
        )
    else:
        _log.info(
            "extract complete: %d source(s), %s -> %s",
            len(result.get("sources", [])),
            STATS_FILENAME,
            _BOOT_HERE / STATS_FILENAME,
        )
    if DEBUG:
        _log.info("done. log file: %s", _BOOT_PATH)
"""


def _is_type_checking_block(node: ast.stmt) -> bool:
    """``if TYPE_CHECKING:`` is typing-only -- never executes, but if
    left in the bundle its body still gets unparsed and intra-pkg
    imports inside leak into the artifact."""
    return (
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "TYPE_CHECKING"
    )


# Imports from these packages are always dropped during slicing — they
# are amalgamated into the bundle directly. The caller's runtime
# package name is added on top inside ``build_bundle`` so that a
# non-mdw runtime (``reg_mockdata``, a steward-private package, …)
# can plug its own intra-runtime imports through the same drop logic.
#
# ``reg_schema`` is NOT here (§9.6): it is never amalgamated, so a
# ``from reg_schema import …`` in a runtime module would NOT be dropped
# and would leak into the bundle as a live import — which is exactly the
# failure the source-scan gate in ``test_build_mona_bundle.py`` catches.
# The runtime is reg_schema-free by construction (``spec`` deserializes
# into stdlib dataclasses), so no such import exists to drop.
_STATIC_AMALGAMATED_PREFIXES: tuple[str, ...] = ("reg_monabundle",)


def _is_amalgamated_module(name: str, prefixes: tuple[str, ...]) -> bool:
    """``True`` if ``name`` is exactly an amalgamated package or a
    submodule of one. Tighter than a raw ``startswith`` — guards
    against e.g. ``reg_monabundle_v2`` matching the ``reg_monabundle``
    prefix.
    """
    return any(name == p or name.startswith(p + ".") for p in prefixes)


def _strip_def_docstrings(module: ast.Module) -> None:
    """Drop the leading docstring from every class / function in ``module``.

    ``ast.unparse`` preserves string-literal expression statements, so a
    class or method docstring would otherwise survive into the bundle as
    inert text. Several of those docstrings mention ``reg_schema`` /
    Pydantic; the text carries no import (so it never breached the §9.6
    boundary), but dropping it keeps the artifact text-clean and slightly
    smaller. The module-level docstring is stripped separately by
    ``_slice_module`` (it precedes the import filtering).

    A def whose entire body was only a docstring keeps a ``pass`` so the
    unparsed result stays valid Python. Walks every nesting depth
    (methods, nested functions/classes). Materialize the node list before
    mutating so removing a leaf docstring mid-walk can't perturb the walk.
    """
    defs = [
        node
        for node in ast.walk(module)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    for node in defs:
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            del body[0]
            if not body:
                body.append(ast.Pass())


def _slice_module(path: Path, *, drop_prefixes: tuple[str, ...]) -> str:
    """Read a module, drop docstrings + __future__ + intra-pkg imports.

    Drops the module docstring AND every nested class/function docstring
    (see ``_strip_def_docstrings``). Also drops top-level imports of any
    amalgamated package (``reg_monabundle`` and the caller's runtime
    package name). Both ``from X import Y`` and ``import X`` forms are
    handled. The remaining body (functions, classes, constants,
    dataclasses, stdlib imports) is rendered via ``ast.unparse`` -- ``#``
    comments are not preserved (they live in the source modules in the
    repo).
    """
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)

    body = list(tree.body)
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body = body[1:]

    kept: list[ast.stmt] = []
    for node in body:
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("__future__"):
                continue
            if node.level > 0:
                continue
            # Keyed on ``node.module`` only (not ``node.names``): an
            # ImportFrom sources every name from a single module, so
            # ``node.module`` is the correct drop key. The §9.6 gate
            # additionally scans ``node.names``, but that asymmetry can't
            # hide a leak — ``reg_schema`` is not an amalgamated prefix
            # (see ``_STATIC_AMALGAMATED_PREFIXES``), so a stray
            # ``from reg_schema import X`` is NOT dropped here, survives
            # into the bundle, and the gate catches it. Scanning names
            # here would only let us drop a re-export like
            # ``from amalgamated_pkg import reg_schema`` — which doesn't
            # exist and isn't an intra-package import to inline.
            if node.module and _is_amalgamated_module(node.module, drop_prefixes):
                continue
        elif isinstance(node, ast.Import) and all(
            _is_amalgamated_module(alias.name, drop_prefixes) for alias in node.names
        ):
            # ``import a, b`` is rare in our runtime modules; treating the
            # all-amalgamated case as a drop keeps the slicer simple. A
            # mixed multi-alias import (one amalgamated, one not) is left
            # intact and would leak — flag that if it ever appears.
            continue
        if _is_type_checking_block(node):
            continue
        kept.append(node)

    module = ast.Module(body=kept, type_ignores=[])
    _strip_def_docstrings(module)
    return ast.unparse(module)


CONFIGURE_PLACEHOLDER = "# __MDW_CONFIGURE_BLOCK__"
DEFAULT_CONFIGURE_BODY = "def configure():\n    return []"
PROJECT_DATA_PLACEHOLDER = "__MDW_PROJECT_DATA_JSON__"

# Fail at import if the header literal drifts from a placeholder constant:
# str.replace() silently no-ops on a missing substring, which would emit a
# bundle missing the configure() function or the spec block and crash on
# MONA at runtime instead of at build.
assert CONFIGURE_PLACEHOLDER in BUNDLE_HEADER, (
    f"BUNDLE_HEADER is missing the {CONFIGURE_PLACEHOLDER!r} marker"
)
assert PROJECT_DATA_PLACEHOLDER in BUNDLE_HEADER, (
    f"BUNDLE_HEADER is missing the {PROJECT_DATA_PLACEHOLDER!r} marker"
)


def build_bundle(
    output: Path,
    *,
    runtime_pkg_dir: Path | None = None,
    runtime_module_order: Sequence[str] | None = None,
    configure_body: str | None = None,
    project_data: dict | None = None,
) -> Path:
    """Amalgamate the runtime modules into a single ``.py``.

    ``runtime_pkg_dir`` + ``runtime_module_order`` default to
    ``reg_monabundle.runtime`` (the in-package runtime amalgamated by
    mdw / reg_webapp / reg_mockdata). To plug in a steward-private
    runtime that lives outside this package, pass **both** —
    overriding only one would silently apply the default module list
    to a different directory and crash later with a missing-file
    error.

    When ``configure_body`` is supplied (a complete ``def configure(): ...``
    function source), it fills the configure slot in ``BUNDLE_HEADER``.
    Otherwise, the editable empty stub is used.

    When ``project_data`` is supplied, the dict is JSON-serialized and
    embedded into ``_PROJECT_DATA_JSON``; the runner parses it on load
    and hands the resulting ``LoadedSpec`` to ``extract.main()``. When
    ``None``, the placeholder is replaced with an empty string and the
    runner falls back to reading ``project_data.json`` from the bundle
    directory at extract time.
    """
    if (runtime_pkg_dir is None) != (runtime_module_order is None):
        raise ValueError(
            "build_bundle: runtime_pkg_dir and runtime_module_order must "
            "be supplied together (or both omitted to use the in-package "
            "reg_monabundle.runtime). Passing only one would silently "
            "apply the default module list to a different directory."
        )
    if runtime_pkg_dir is None:
        runtime_pkg_dir = DEFAULT_RUNTIME_DIR
        runtime_module_order = DEFAULT_RUNTIME_MODULE_ORDER
    assert runtime_module_order is not None  # paired check above
    output.parent.mkdir(parents=True, exist_ok=True)
    body = (configure_body or DEFAULT_CONFIGURE_BODY).rstrip()
    header = BUNDLE_HEADER.replace(CONFIGURE_PLACEHOLDER, body, 1)
    # json.dumps escapes embedded backslashes and triple-quotes; the
    # outer r""" raw string preserves the result verbatim so json.loads
    # at runtime sees the same bytes.
    project_data_str = "" if project_data is None else json.dumps(project_data)
    header = header.replace(PROJECT_DATA_PLACEHOLDER, project_data_str, 1)
    # Static prefixes plus the caller's runtime — see _STATIC_AMALGAMATED_PREFIXES.
    drop_prefixes: tuple[str, ...] = (
        *_STATIC_AMALGAMATED_PREFIXES,
        runtime_pkg_dir.name,
    )
    parts: list[str] = [header, ""]
    for name in REG_MONABUNDLE_MODULE_ORDER:
        parts.append(f"# {'=' * 75}")
        parts.append(f"# reg_monabundle/{name}.py")
        parts.append(f"# {'=' * 75}")
        parts.append("")
        parts.append(
            _slice_module(
                REG_MONABUNDLE_DIR / f"{name}.py", drop_prefixes=drop_prefixes
            )
        )
        parts.append("")
    for name in runtime_module_order:
        parts.append(f"# {'=' * 75}")
        parts.append(f"# {name}.py")
        parts.append(f"# {'=' * 75}")
        parts.append("")
        parts.append(
            _slice_module(runtime_pkg_dir / f"{name}.py", drop_prefixes=drop_prefixes)
        )
        parts.append("")
    parts.append(BUNDLE_RUNNER)
    output.write_text("\n".join(parts), encoding="utf-8")
    return output
