"""MONA Python probe -- mirrors probe_mona.R but tests the Python stack.

Question this answers: would it be simpler to ship mock_data_wizard
itself to MONA (Python on the batch client) instead of generating an
R script that re-implements aggregation logic in R?

Run on the MONA batch client however the batch server runs Python
scripts. Writes mdw_python_probe_<timestamp>.log alongside itself.
Both stdout/stderr capture and the log file are PII-clean: no
row-level data is ever logged. Only schema metadata and aggregate
counts (single integers, never frequency cell values).

Optional: set PROJECT_DSN and SAMPLE_TABLE below to enable §6
(MS SQL via pyodbc).
"""

from __future__ import annotations

import datetime
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import traceback
from pathlib import Path

# MONA batch footgun: Python's stdout is buffered to a memory buffer in
# batch mode (no console). When that buffer fills, the script hangs in
# BatchClient with no error. Redirect stdout to devnull on the batch
# server (hostnames start with "MBS"); leave it alone in interactive
# desktop sessions. Source: MONA's "Inaktivera python-konsolen i batch".
# We log to stderr (line-buffered) and to a file regardless, so this
# doesn't affect the probe's output.
if socket.gethostname()[:3].upper() == "MBS":
    sys.stdout = open(os.devnull, "w")

PROJECT_DSN = "P1105"  # empty -> skip MS SQL probes
SAMPLE_TABLE = "Individ_2018"  # ~8M rows. Empty -> skip table probe.

# MONA has no internet. So pip can only work via (a) an internal mirror
# configured at the system level, (b) wheels supplied via --find-links,
# or (c) packages already installed in site-packages. The probe hunts
# for all three.

# Likely internal Python-mirror locations (by symmetry with MonaCRAN at
# file://micro.intra/apps/R/MonaCRAN). Edit if you know better paths.
CANDIDATE_MIRROR_PATHS = (
    r"\\micro.intra\apps\Python",
    r"\\micro.intra\apps\python",
    r"\\micro.intra\apps\PyPI",
    r"\\micro.intra\apps\pypi",
    r"R:\Python",
    r"R:\PyPI",
)


# ---- 0. setup --------------------------------------------------------

ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = Path.cwd() / f"mdw_python_probe_{ts}.log"
log_file = log_path.open("wt", encoding="utf-8")


def log(msg: str = "") -> None:
    print(msg, file=sys.stderr, flush=True)
    log_file.write(msg + "\n")
    log_file.flush()


def section(title: str) -> None:
    log("")
    log("=" * 70)
    log(title)
    log("=" * 70)


def probe(name: str, fn):
    log("")
    log(f"[probe] {name}")
    t0 = time.monotonic()
    try:
        fn()
        log(f"  -> OK ({time.monotonic() - t0:.2f}s)")
        return True
    except Exception as e:
        log(f"  -> FAIL ({time.monotonic() - t0:.2f}s)")
        # First line of traceback message, then class+message
        msg = str(e).splitlines()[0] if str(e) else type(e).__name__
        log(f"     {type(e).__name__}: {msg}")
        # Full traceback indented for debugging, but keep it bounded
        tb = traceback.format_exc().strip().splitlines()
        for line in tb[-6:]:
            log(f"     | {line}")
        return False


# ---- 1. environment --------------------------------------------------

section("1. ENVIRONMENT")
log(f"Python:          {sys.version.splitlines()[0]}")
log(f"Executable:      {sys.executable}")
log(f"Platform:        {platform.platform()}")
log(f"Machine:         {platform.machine()}")
log(f"OS:              {platform.system()} {platform.release()}")
log(f"cwd():           {Path.cwd()}")
log(f"Probe log file:  {log_path}")


# Free space
def free_mb(path: str | Path) -> str:
    try:
        usage = shutil.disk_usage(str(path))
        mb = usage.free / 1024 / 1024
        return f"{mb:.1f} MB ({mb / 1024:.2f} GB)"
    except Exception as e:
        return f"<unknown: {type(e).__name__}>"


for p in (Path.cwd(), Path(os.environ.get("TEMP", "/tmp"))):
    log(f"Free in          {str(p):<50s} : {free_mb(p)}")

log(f"sys.prefix:      {sys.prefix}")
log(f"PIP_INDEX_URL:   {os.environ.get('PIP_INDEX_URL', '<unset>')}")
log(f"PIP_FIND_LINKS:  {os.environ.get('PIP_FIND_LINKS', '<unset>')}")
log(f"PYTHONPATH:      {os.environ.get('PYTHONPATH', '<unset>')}")


# ---- 2. pip availability ---------------------------------------------

section("2. PIP")


# Try every reasonable way of getting at pip on Windows -- bare `pip`,
# `python -m pip`, the py launcher, and the `Scripts\pip.exe` shim.
# User reported that `pip` alone returns nothing in PowerShell + Git Bash,
# which usually means PATH doesn't include the Python Scripts folder.
PIP_INVOCATIONS = [
    [sys.executable, "-m", "pip", "--version"],
    ["pip", "--version"],
    ["pip3", "--version"],
    ["py", "-m", "pip", "--version"],
    [str(Path(sys.prefix) / "Scripts" / "pip.exe"), "--version"],
    [str(Path(sys.prefix) / "Scripts" / "pip3.exe"), "--version"],
]


def _try_pip_invocations():
    found = []
    for cmd in PIP_INVOCATIONS:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            ok = r.returncode == 0 and r.stdout.strip()
            tag = "OK " if ok else "FAIL"
            line = (r.stdout.strip() or r.stderr.strip() or "<no output>")[:120]
            log(f"     [{tag}] {' '.join(cmd):<70s} -> {line}")
            if ok:
                found.append(cmd)
        except FileNotFoundError:
            log(f"     [N/A] {' '.join(cmd):<70s} -> executable not found")
        except Exception as e:
            log(f"     [ERR] {' '.join(cmd):<70s} -> {type(e).__name__}: {e}")
    if not found:
        raise RuntimeError("no pip invocation worked")


probe("try every way to invoke pip", _try_pip_invocations)


def _ensurepip_bootstrap():
    # If pip really isn't installed, ensurepip from the stdlib can lay
    # it down without needing the network.
    r = subprocess.run(
        [sys.executable, "-m", "ensurepip", "--version"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    log(f"     stdout: {r.stdout.strip() or '<empty>'}")
    if r.stderr.strip():
        log(f"     stderr: {r.stderr.strip()}")
    if r.returncode != 0:
        raise RuntimeError(f"ensurepip exited {r.returncode}")


probe("python -m ensurepip --version (bootstrap fallback)", _ensurepip_bootstrap)


def _list_scripts_dir():
    # If pip is anywhere, it's probably here. Listing this folder is the
    # cheapest possible directory probe.
    scripts = Path(sys.prefix) / "Scripts"
    log(f"     scripts dir: {scripts}")
    if not scripts.exists():
        log("     <missing>")
        return
    entries = sorted(p.name for p in scripts.iterdir())
    log(f"     {len(entries)} entries; pip-related:")
    for e in entries:
        if any(k in e.lower() for k in ("pip", "wheel", "easy_install")):
            log(f"       {e}")


probe(f"list contents of {Path(sys.prefix) / 'Scripts'}", _list_scripts_dir)


def _check_pip_config():
    r = subprocess.run(
        [sys.executable, "-m", "pip", "config", "list"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    out = (r.stdout or "").strip() or "<no pip config>"
    for line in out.splitlines():
        log(f"     {line}")


probe("pip config list", _check_pip_config)


def _check_pip_freeze():
    # Lists installed packages -- shows whether duckdb/pyodbc are
    # already present without needing any network access.
    r = subprocess.run(
        [sys.executable, "-m", "pip", "list", "--format=freeze"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if r.returncode != 0:
        raise RuntimeError(f"pip list exited {r.returncode}: {r.stderr.strip()[:200]}")
    pkgs = [
        line
        for line in r.stdout.splitlines()
        if any(
            line.lower().startswith(p)
            for p in ("duckdb", "pyodbc", "numpy", "pandas", "polars", "pyarrow")
        )
    ]
    log(f"     {len(r.stdout.splitlines())} packages installed total")
    log("     of interest:")
    for p in pkgs:
        log(f"       {p}")
    if not pkgs:
        log("       (none of duckdb/pyodbc/numpy/pandas/polars/pyarrow)")


probe("pip list (what's already installed)", _check_pip_freeze)


# ---- 2b. discover internal Python mirror -----------------------------
#
# Symmetry argument: MonaCRAN is at file://micro.intra/apps/R/MonaCRAN.
# A Python mirror would likely live somewhere similar. Hunt for it.

section("2b. INTERNAL PYTHON MIRROR DISCOVERY")


def _check_mirror_paths():
    found = []
    for raw in CANDIDATE_MIRROR_PATHS:
        p = Path(raw)
        try:
            exists = p.exists()
        except Exception as e:
            log(f"     {raw:<50s} : <error: {type(e).__name__}>")
            continue
        if exists:
            try:
                # Sample a few entries so we don't spam the log on a huge mirror
                kids = []
                for i, child in enumerate(p.iterdir()):
                    if i >= 6:
                        kids.append("...")
                        break
                    kids.append(child.name)
                log(f"     {raw:<50s} : EXISTS, contents: {kids}")
                found.append(raw)
            except Exception as e:
                log(f"     {raw:<50s} : EXISTS but iterdir failed: {type(e).__name__}")
                found.append(raw)
        else:
            log(f"     {raw:<50s} : not found")
    if not found:
        log("")
        log("     No internal Python mirror found at common paths.")
        log("     If MONA has one, please tell us where -- the rework can")
        log("     point pip at it via PIP_INDEX_URL=file://... or we ship")
        log("     vendored wheels via --find-links.")


probe("scan likely internal mirror paths", _check_mirror_paths)


def _check_pip_dry_install_no_index():
    # Probe whether `pip install --no-index --find-links <somewhere>` would
    # find duckdb if we were to ship vendored wheels alongside the package.
    # We use the cwd as a stand-in --find-links target (almost certainly
    # empty), which lets us see exactly what error pip raises -- this tells
    # us whether the offline path is even structurally available.
    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--dry-run",
            "--no-index",
            f"--find-links={Path.cwd().as_posix()}",
            "duckdb",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    log(f"     return code: {r.returncode}")
    out = (r.stdout + r.stderr).strip()
    for line in out.splitlines()[:10]:
        log(f"     {line}")
    # We expect failure here (no wheel available); the *kind* of failure is
    # the signal: "Could not find a version" means the offline path works,
    # we just need wheels. "ERROR: --no-index ..." would mean a deeper issue.


probe(
    "pip install --dry-run --no-index --find-links=. duckdb (would offline mode work?)",
    _check_pip_dry_install_no_index,
)


# ---- 3. import already-installed packages ---------------------------

section("3. PRE-INSTALLED PACKAGES")

for pkg in ("duckdb", "pyodbc", "numpy", "pandas", "polars", "pyarrow"):

    def _make_probe(p=pkg):
        def fn():
            mod = __import__(p)
            ver = getattr(mod, "__version__", "<unknown>")
            log(f"     {p} {ver}")

        return fn

    probe(f"import {pkg}", _make_probe())


# ---- 4. duckdb basics -------------------------------------------------

section("4. DUCKDB BASIC")

duck_con = None


def _duck_open():
    global duck_con
    import duckdb

    duck_con = duckdb.connect(":memory:")
    log(f"     duckdb {duckdb.__version__}, in-memory connection opened")


probe("open in-memory DuckDB", _duck_open)

if duck_con is not None:

    def _duck_aggs():
        r = duck_con.execute("""
            SELECT
              MIN(n) AS min_n, MAX(n) AS max_n, AVG(n) AS mean_n,
              STDDEV(n) AS sd_n,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) AS p50,
              APPROX_COUNT_DISTINCT(n) AS approx_nd
            FROM (SELECT range AS n FROM range(0, 100000))
        """).fetchone()
        log(
            f"     min={r[0]} max={r[1]} mean={r[2]:.2f} "
            f"sd={r[3]:.2f} p50={r[4]:.0f} approx_nd={r[5]}"
        )

    probe(
        "server-side aggregations (STDDEV / PERCENTILE_CONT / APPROX_COUNT_DISTINCT)",
        _duck_aggs,
    )

    def _duck_policy():
        # Same policy the rework would set: temp_directory + preserve_insertion_order
        # but NOT memory_limit (let DuckDB use 80% of RAM by default).
        duck_con.execute(
            f"SET temp_directory = '{Path(os.environ.get('TEMP', '/tmp')).as_posix()}'"
        )
        duck_con.execute("SET preserve_insertion_order = false")
        ml = duck_con.execute("SELECT current_setting('memory_limit')").fetchone()
        log(f"     effective memory_limit = {ml[0]}")

    probe("rework policy SET commands accepted", _duck_policy)


# ---- 5. duckdb CSV reading + memory behaviour -----------------------

section("5. DUCKDB CSV READING")

csv_small = Path.cwd() / "mdw_py_probe_small.csv"
csv_big = Path.cwd() / "mdw_py_probe_big.csv"


def _write_small():
    # Synthetic, neutral identifiers
    import csv

    with csv_small.open("wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "value", "group"])
        for i in range(1000):
            w.writerow([f"ROW{i:07d}", i % 100, "ABC"[i % 3]])
    log(f"     wrote {csv_small} ({csv_small.stat().st_size / 1024:.1f} KB)")


probe("write small synthetic CSV", _write_small)


if duck_con is not None:

    def _duck_small_csv():
        path = csv_small.as_posix().replace("'", "''")
        n = duck_con.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{path}')"
        ).fetchone()[0]
        log(f"     read_csv_auto count: {n}")

    probe("read_csv_auto on small CSV", _duck_small_csv)

    def _write_big():
        # 5M rows ~ 100MB. Use duckdb itself to write the CSV reliably.
        path = csv_big.as_posix().replace("'", "''")
        duck_con.execute(f"""
            COPY (
              SELECT
                'ID' || LPAD(range::VARCHAR, 7, '0') AS id,
                (range % 101) AS age,
                ('ABCD'[(range % 4) + 1]) AS grp
              FROM range(0, 5000000)
            ) TO '{path}' (HEADER, FORMAT CSV)
        """)
        log(f"     wrote {csv_big} ({csv_big.stat().st_size / 1024 / 1024:.1f} MB)")

    probe("write 5M-row CSV via duckdb COPY", _write_big)

    def _duck_big_csv():
        path = csv_big.as_posix().replace("'", "''")
        t0 = time.monotonic()
        r = duck_con.execute(f"""
            SELECT
              COUNT(*) AS n,
              COUNT(DISTINCT id) AS nd_id,
              AVG(age) AS mean_age,
              STDDEV(age) AS sd_age,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) AS p50,
              PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY age) AS p99
            FROM read_csv_auto('{path}')
        """).fetchone()
        elapsed = time.monotonic() - t0
        log(
            f"     n={r[0]} nd_id={r[1]} "
            f"mean_age={r[2]:.2f} sd_age={r[3]:.2f} p50={r[4]:.1f} p99={r[5]:.1f}"
        )
        log(f"     elapsed={elapsed:.2f}s")
        # Was anything spilled?
        spills = list(Path(os.environ.get("TEMP", "/tmp")).rglob("duckdb*.tmp"))
        log(f"     duckdb temp files left in tempdir: {len(spills)}")

    probe("aggregate 5M-row CSV (rework policy, exact aggs)", _duck_big_csv)


# ---- 6. MS SQL via pyodbc (optional) --------------------------------

section("6. MS SQL VIA PYODBC (optional)")

if PROJECT_DSN:
    try:
        import pyodbc
    except ImportError:
        log("pyodbc not available, skipping MS SQL probes")
        pyodbc = None  # type: ignore[assignment]
else:
    log("PROJECT_DSN is empty, skipping MS SQL probes")
    pyodbc = None  # type: ignore[assignment]

sql_con = None
if PROJECT_DSN and pyodbc is not None:
    log(f"Using DSN: {PROJECT_DSN}")
    log(f"Drivers visible to pyodbc: {pyodbc.drivers()}")

    # Two connection styles to try -- DSN-based (what the R script uses)
    # and the direct driver/server/database string from the MONA docs.
    # Whichever works, we use; if both work the rework prefers DSN.
    def _sql_connect_dsn():
        global sql_con
        sql_con = pyodbc.connect(f"DSN={PROJECT_DSN}")

    if not probe(f"pyodbc.connect(DSN={PROJECT_DSN})", _sql_connect_dsn):

        def _sql_connect_driver():
            # MONA-docs style. Database name == project number.
            # Server name unknown to us (MQ02\B is just an example in the
            # docs); pyodbc.drivers() may also give hints. If this fails
            # the log shows the exact connection string for the user to
            # adjust.
            global sql_con
            conn_str = (
                "Driver={ODBC Driver 17 for SQL Server};"
                f"Server=MQ02\\B;Database={PROJECT_DSN};"
                "Trusted_Connection=yes;"
            )
            log(f"     trying conn_str: {conn_str}")
            sql_con = pyodbc.connect(conn_str)

        probe(
            "pyodbc.connect (driver+server+database, MONA-docs style)",
            _sql_connect_driver,
        )

if sql_con is not None:

    def _sql_select_one():
        cur = sql_con.cursor()
        cur.execute("SELECT 1")
        r = cur.fetchone()
        log(f"     ok, got {r[0]}")

    probe("SELECT 1 against MS SQL", _sql_select_one)

    def _sql_version():
        cur = sql_con.cursor()
        cur.execute("SELECT @@VERSION")
        v = cur.fetchone()[0]
        log(f"     {str(v)[:200]}")

    probe("MS SQL @@VERSION", _sql_version)

    if SAMPLE_TABLE:
        sample_cols: list[tuple[str, str]] = []

        def _sql_schema():
            cur = sql_con.cursor()
            cur.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{SAMPLE_TABLE}'
                ORDER BY ORDINAL_POSITION
            """)
            for row in cur.fetchall():
                sample_cols.append((row[0], row[1]))
            log(f"     {len(sample_cols)} columns")
            for name, dtype in sample_cols[:10]:
                log(f"     {name:<30s} {dtype}")
            if len(sample_cols) > 10:
                log(f"     ... ({len(sample_cols) - 10} more)")

        probe(
            f"INFORMATION_SCHEMA.COLUMNS for {SAMPLE_TABLE}",
            _sql_schema,
        )

        # PII safety: aggregate values are NEVER logged.
        # Just timing + a finite-flag boolean.
        def _sql_count_and_aggs():
            numeric_types = {
                "int",
                "bigint",
                "smallint",
                "tinyint",
                "decimal",
                "numeric",
                "float",
                "real",
                "money",
                "smallmoney",
            }
            num_col = next(
                (c for c, t in sample_cols if t.lower() in numeric_types),
                None,
            )
            cur = sql_con.cursor()
            t0 = time.monotonic()
            if num_col:
                cur.execute(f"""
                    SELECT
                      COUNT_BIG(*),
                      MIN(CAST([{num_col}] AS FLOAT)),
                      MAX(CAST([{num_col}] AS FLOAT)),
                      AVG(CAST([{num_col}] AS FLOAT)),
                      STDEV(CAST([{num_col}] AS FLOAT))
                    FROM {SAMPLE_TABLE}
                """)
                r = cur.fetchone()
                elapsed = time.monotonic() - t0
                n_rows = r[0]
                aggs_finite = all(v is not None for v in r[1:])
                log(
                    f"     col='{num_col}' n_rows={n_rows} "
                    f"elapsed={elapsed:.2f}s aggs_finite={aggs_finite}"
                )
            else:
                cur.execute(f"SELECT COUNT_BIG(*) FROM {SAMPLE_TABLE}")
                n_rows = cur.fetchone()[0]
                elapsed = time.monotonic() - t0
                log(f"     n_rows={n_rows} elapsed={elapsed:.2f}s")

        probe(
            f"server-side aggregation on {SAMPLE_TABLE} (timing only)",
            _sql_count_and_aggs,
        )


# ---- 7. cleanup -----------------------------------------------------

section("7. CLEANUP")


def _cleanup_csvs():
    for p in (csv_small, csv_big):
        if p.exists():
            p.unlink()


probe("remove probe CSVs", _cleanup_csvs)

if duck_con is not None:

    def _close_duck():
        duck_con.close()

    probe("close DuckDB", _close_duck)

if sql_con is not None:

    def _close_sql():
        sql_con.close()

    probe("close MS SQL", _close_sql)


# ---- 8. interpretation ----------------------------------------------
#
# Summarise the findings into a clear "next step" so we don't have to
# read the whole log to know what to do.

section("8. WHAT THIS MEANS FOR THE REWORK")


def _has(mod: str) -> bool:
    try:
        __import__(mod)
        return True
    except Exception:
        return False


duckdb_ok = _has("duckdb")
pyodbc_ok = _has("pyodbc")

# pip availability -- tries every invocation form
pip_ok = False
for cmd in PIP_INVOCATIONS:
    try:
        if subprocess.run(cmd, capture_output=True, timeout=15).returncode == 0:
            pip_ok = True
            break
    except (FileNotFoundError, OSError):
        continue

# Was any candidate mirror present?
mirror_found = any(Path(m).exists() for m in CANDIDATE_MIRROR_PATHS)

log(f"  duckdb importable:   {duckdb_ok}")
log(f"  pyodbc importable:   {pyodbc_ok}")
log(f"  pip available:       {pip_ok}")
log(f"  mirror found:        {mirror_found}")
log("")

if duckdb_ok and pyodbc_ok:
    log("  -> Both core deps already installed. The Python rewrite is")
    log("     immediately viable: ship mock_data_wizard, run as a CLI on")
    log("     MONA, no pip work needed.")
elif duckdb_ok and not pyodbc_ok:
    log("  -> duckdb works but pyodbc is missing. file_source path is")
    log("     fully covered; sql_source needs pyodbc.")
    if pip_ok and mirror_found:
        log("     Mirror present + pip available -> try `pip install pyodbc`.")
    elif pip_ok:
        log("     pip exists but no mirror -> ship pyodbc as a vendored wheel")
        log("     and install with `pip install --no-index --find-links <dir>`.")
    else:
        log("     No pip -> need help from MONA admins to add pyodbc.")
elif not duckdb_ok and pip_ok and mirror_found:
    log("  -> pip + mirror both work. `pip install duckdb pyodbc` should")
    log("     work as a one-time setup step in the project.")
elif not duckdb_ok and pip_ok:
    log("  -> pip exists but no mirror -> ship vendored wheels with")
    log("     mock_data_wizard and install with --no-index --find-links.")
    log("     Need cp313 win_amd64 wheels for: duckdb, pyodbc, numpy.")
elif not duckdb_ok and not pip_ok:
    log("  -> Neither duckdb nor pip available. The Python rewrite is not")
    log("     viable without admin help. Stay on the R-on-MONA path.")

log("")
log("=" * 70)
log("DONE")
log(f"Log written to: {log_path}")
log("=" * 70)

log_file.close()
