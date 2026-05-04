"""Build a single-file .py bundle for upload to MONA.

Concatenates the wizard runtime modules (classify, sql_emit, sources,
summarize, extract) into one file. The user uploads that file, edits
the ``configure()`` block near the top, and runs::

    python mock_data_wizard_extract.py

The bundle is self-contained -- only stdlib + duckdb + pyodbc + numpy
(all pre-installed on the WinPython distribution shipped with MONA's
batch client; see ``DESIGN.md`` for the runtime probe results).

Per-module docstrings and ``#`` comments are dropped during
amalgamation. The repo source remains the documentation; the bundle is
the artifact.
"""

from __future__ import annotations

import ast
from pathlib import Path

PKG_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_NAME = "mock_data_wizard_extract.py"

# Dependency-ordered: each module imports only earlier ones.
MODULE_ORDER = (
    "classify",
    "sql_emit",
    "sources",
    "summarize",
    "config",
    "scan",
    "extract",
)

BUNDLE_HEADER = '''\
"""mock-data-wizard MONA discover/extract bundle.

Self-contained single-file Python script. Edit configure() and the MODE
flag in the user-config block, then run on MONA:

    python mock_data_wizard_extract.py

Two modes, one bundle:

  MODE = "discover"  (default)
    Cheap metadata-only walk -- INFORMATION_SCHEMA / DuckDB DESCRIBE
    plus COUNT(*). Output: discover.json next to this script.
    Copy discover.json off MONA, run `mock-data-wizard configure
    discover.json` locally to author mdw_config.json, upload it back
    next to the bundle, then re-run with MODE = "extract".

  MODE = "extract"
    Typed aggregation. Reads mdw_config.json from this directory
    (every column must carry a type override) and writes stats.json.

PII discipline: only aggregate values cross the JSON boundary. Cell
suppression (k-anonymity, default threshold = 10), uniform noise
injection (+/- 0.5%) on numeric aggregates, and +/- 7-day jitter on
date min/max/quantiles are applied after server-side aggregation.
null_count is censored when 0 < null_count < k. discover.json carries
metadata only -- column names, SQL types, row counts -- no values.

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
#   "discover" -- emits discover.json (metadata only). Default. SQL
#                 sources without `tables=` / `pattern=` / `all=True`
#                 are listed permissively here.
#   "extract"  -- emits stats.json. Requires mdw_config.json sitting
#                 next to this script. Authoritative SOURCES filtering
#                 is required (no permissive listing in this mode).
#
# Override at runtime without editing the bundle:
#   MDW_MODE=extract python mock_data_wizard_extract.py
#
# DISCOVER SHAPE: leave SOURCES wide and let discover walk everything:
#
#     return [sql_source(dsn="P1105")]
#     return [file_source(path=r"\\\\micro.intra\\projekt\\P1105$\\P1105_Data")]
#
# EXTRACT SHAPE: declare exactly what to aggregate. mdw_config.json
# next to this script declares per-column types.
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
    try:
        result = main(
            SOURCES,
            output_dir=_BOOT_HERE,
            mode=MODE,
            classifier_seed=CLASSIFIER_SEED,
        )
    except Exception:
        _log.error("mdw bundle failed:\\n%s", _boot_traceback.format_exc())
        _boot_sys.exit(1)
    if MODE == "discover":
        _log.info(
            "discover complete: %d source(s), discover.json -> %s",
            len(result.get("sources", [])),
            _BOOT_HERE / "discover.json",
        )
    else:
        _log.info(
            "extract complete: %d source(s), stats.json -> %s",
            len(result.get("sources", [])),
            _BOOT_HERE / "stats.json",
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


def _slice_module(name: str) -> str:
    """Read a module, drop docstring + __future__ + intra-pkg imports.

    The remaining body (functions, classes, constants, dataclasses,
    stdlib imports) is rendered via ``ast.unparse`` -- ``#`` comments
    are not preserved (they live in the source modules in the repo).
    """
    src = (PKG_DIR / f"{name}.py").read_text(encoding="utf-8")
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
            if node.module and node.module.startswith("mock_data_wizard"):
                continue
        if _is_type_checking_block(node):
            continue
        kept.append(node)

    return ast.unparse(ast.Module(body=kept, type_ignores=[]))


CONFIGURE_PLACEHOLDER = "# __MDW_CONFIGURE_BLOCK__"
DEFAULT_CONFIGURE_BODY = "def configure():\n    return []"

# Fail at import if the header literal drifts from the placeholder constant:
# str.replace() silently no-ops on a missing substring, which would emit a
# bundle with no configure() function and crash on MONA at runtime instead.
assert CONFIGURE_PLACEHOLDER in BUNDLE_HEADER, (
    f"BUNDLE_HEADER is missing the {CONFIGURE_PLACEHOLDER!r} marker"
)


def build_bundle(output: Path, *, configure_body: str | None = None) -> Path:
    """Amalgamate the wizard runtime modules into a single ``.py``.

    When ``configure_body`` is supplied (a complete ``def configure(): ...``
    function source), it fills the configure slot in ``BUNDLE_HEADER``.
    Otherwise, the editable empty stub is used.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    body = (configure_body or DEFAULT_CONFIGURE_BODY).rstrip()
    header = BUNDLE_HEADER.replace(CONFIGURE_PLACEHOLDER, body, 1)
    parts: list[str] = [header, ""]
    for name in MODULE_ORDER:
        parts.append(f"# {'=' * 75}")
        parts.append(f"# {name}.py")
        parts.append(f"# {'=' * 75}")
        parts.append("")
        parts.append(_slice_module(name))
        parts.append("")
    parts.append(BUNDLE_RUNNER)
    output.write_text("\n".join(parts), encoding="utf-8")
    return output
