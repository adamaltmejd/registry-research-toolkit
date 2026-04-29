"""Classification seed loading and build-time population.

A classification is a normalized code system (SUN2000, SSYK2012, SNI2007, ...)
that groups the value codes produced by many variable instances. The seed at
``regmeta/classifications.toml`` declares one entry per code system and lists
the raw ``variable_instance.vardemangdsversion`` strings that map to it.

Runtime never loads the seed — query commands read the already-populated
``classification`` / ``classification_code`` tables. ``populate_classifications``
is only called during ``maintain build-db``.
"""

from __future__ import annotations

import csv
import sqlite3
import sys
import time
import tomllib
from pathlib import Path
from typing import Any

from .errors import EXIT_CONFIG, RegmetaError


_REQUIRED_FIELDS = ("short_name", "name", "vardemangdsversion")
_VALID_CODES_HEADER = ("vardekod", "vardebenamning")

# level = number of digits for all-digit codes, NULL otherwise. Used in
# multiple INSERTs against classification_code; keep the SQL identical so
# canonical-only rows and observed rows agree on level.
_LEVEL_EXPR = (
    "CASE WHEN {col} GLOB '[0-9]*' AND NOT {col} GLOB '*[^0-9]*' "
    "THEN length({col}) ELSE NULL END"
)


def repo_seed_path() -> Path | None:
    """Return the in-repo classifications seed, for build-time use only.

    Mirrors ``doc_db.repo_docs_dir``: located from ``regmeta/src/regmeta/`` up to
    the ``regmeta/`` package root and down to ``classifications.toml``. Installed
    wheels do not ship the seed — it is a maintainer artifact, same as
    ``regmeta/docs/``.
    """
    pkg_dir = Path(__file__).resolve().parent
    candidate = pkg_dir.parent.parent / "classifications.toml"
    return candidate if candidate.is_file() else None


def load_valid_codes(path: Path) -> dict[str, str]:
    """Read a canonical valid-codes CSV and return ``{vardekod: vardebenamning}``.

    The CSV must have a header ``vardekod,vardebenamning``. Codes are stripped
    of leading/trailing whitespace before use (matches the rule used at query
    time). Duplicate codes raise.
    """
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if (
                header is None
                or tuple(h.strip() for h in header) != _VALID_CODES_HEADER
            ):
                raise RegmetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_csv_invalid",
                    error_class="configuration",
                    message=(
                        f"{path}: header must be 'vardekod,vardebenamning' "
                        f"(got {header!r})."
                    ),
                    remediation="Fix the CSV header.",
                )
            out: dict[str, str] = {}
            for lineno, row in enumerate(reader, start=2):
                if not row or all(not c.strip() for c in row):
                    continue
                if len(row) < 2:
                    raise RegmetaError(
                        exit_code=EXIT_CONFIG,
                        code="classification_csv_invalid",
                        error_class="configuration",
                        message=f"{path}:{lineno}: expected 2 columns, got {len(row)}.",
                        remediation="Each row must be 'vardekod,vardebenamning'.",
                    )
                code = row[0].strip()
                label = row[1].strip()
                if not code:
                    raise RegmetaError(
                        exit_code=EXIT_CONFIG,
                        code="classification_csv_invalid",
                        error_class="configuration",
                        message=f"{path}:{lineno}: empty vardekod.",
                        remediation="Remove the row or supply a code.",
                    )
                if code in out:
                    raise RegmetaError(
                        exit_code=EXIT_CONFIG,
                        code="classification_csv_invalid",
                        error_class="configuration",
                        message=f"{path}:{lineno}: duplicate vardekod {code!r}.",
                        remediation="Each vardekod must appear once.",
                    )
                out[code] = label
            if not out:
                raise RegmetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_csv_invalid",
                    error_class="configuration",
                    message=f"{path}: no data rows.",
                    remediation="The CSV must contain at least one code.",
                )
            return out
    except OSError as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="classification_csv_unreadable",
            error_class="configuration",
            message=f"Could not read {path}: {exc}",
            remediation="Check the file path and permissions.",
        ) from exc


def load_seed(path: Path) -> list[dict[str, Any]]:
    """Parse and validate the classification seed file.

    Raises ``RegmetaError`` on structural issues (missing required fields,
    duplicate short_names, duplicate vardemangdsversion strings across
    classifications). Does not touch the DB.
    """
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="classification_seed_unreadable",
            error_class="configuration",
            message=f"Could not parse classification seed {path}: {exc}",
            remediation="Ensure the file is valid TOML.",
        ) from exc

    entries = data.get("classification") or []
    if not entries:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="classification_seed_empty",
            error_class="configuration",
            message=f"Classification seed {path} has no [[classification]] entries.",
            remediation="Add at least one classification entry.",
        )

    seen_short_names: set[str] = set()
    seen_versions: dict[str, str] = {}
    for entry in entries:
        for field in _REQUIRED_FIELDS:
            if not entry.get(field):
                raise RegmetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_seed_invalid",
                    error_class="configuration",
                    message=(
                        f"Classification entry is missing required field "
                        f"{field!r}: {entry!r}"
                    ),
                    remediation=f"Add {field} to every [[classification]] entry.",
                )
        short = entry["short_name"]
        if short in seen_short_names:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=f"Duplicate classification short_name: {short!r}",
                remediation="Each short_name must be unique in the seed.",
            )
        seen_short_names.add(short)

        versions = entry["vardemangdsversion"]
        if not isinstance(versions, list) or not all(
            isinstance(v, str) for v in versions
        ):
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=(f"{short}: vardemangdsversion must be a list of strings."),
                remediation="Use a TOML array of quoted strings.",
            )
        for v in versions:
            if v in seen_versions:
                raise RegmetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_seed_invalid",
                    error_class="configuration",
                    message=(
                        f"vardemangdsversion {v!r} is claimed by both "
                        f"{seen_versions[v]!r} and {short!r}."
                    ),
                    remediation=(
                        "A vardemangdsversion string belongs to exactly one "
                        "classification. Remove the duplicate."
                    ),
                )
            seen_versions[v] = short

        vcf = entry.get("valid_codes_file")
        if vcf is not None and not isinstance(vcf, str):
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=f"{short}: valid_codes_file must be a string.",
                remediation="Use a relative filename like 'sun2000-niva.csv'.",
            )

    # Resolve supersedes references now that all short_names are known.
    for entry in entries:
        sup = entry.get("supersedes")
        if sup is not None and sup not in seen_short_names:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=(
                    f"{entry['short_name']}: supersedes {sup!r} which is not "
                    f"declared in the seed."
                ),
                remediation=(
                    "Add the superseded classification to the seed, or remove "
                    "the supersedes reference."
                ),
            )

    return entries


def _progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _resolve_valid_codes_paths(
    entries: list[dict[str, Any]], valid_codes_dir: Path | None
) -> dict[str, Path]:
    """Return ``{short_name: resolved_path}`` for entries with a canonical CSV.

    A seed entry with ``valid_codes_file`` set but no ``valid_codes_dir``
    available, or a missing/non-file path, is a build-stop error.
    """
    resolved: dict[str, Path] = {}
    for entry in entries:
        rel = entry.get("valid_codes_file")
        if rel is None:
            continue
        if valid_codes_dir is None:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_csv_dir_missing",
                error_class="configuration",
                message=(
                    f"{entry['short_name']}: valid_codes_file is set but no "
                    "valid_codes_dir is configured for the build."
                ),
                remediation=(
                    "Place the canonical CSV under <input_dir>/classifications/."
                ),
            )
        base = valid_codes_dir.resolve()
        path = (base / rel).resolve()
        if not path.is_relative_to(base):
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_csv_invalid",
                error_class="configuration",
                message=(
                    f"{entry['short_name']}: valid_codes_file {rel!r} escapes "
                    f"{base} (resolved to {path})."
                ),
                remediation="Use a plain filename, not a path with '..' segments.",
            )
        if not path.is_file():
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="classification_csv_not_found",
                error_class="configuration",
                message=(
                    f"{entry['short_name']}: valid_codes_file {rel!r} "
                    f"resolved to {path}, which does not exist."
                ),
                remediation="Create the CSV at that path or fix the seed entry.",
            )
        resolved[entry["short_name"]] = path
    return resolved


def _apply_valid_codes(
    conn: sqlite3.Connection,
    csv_paths: dict[str, Path],
    id_by_short: dict[str, int],
) -> None:
    """Mark canonical/observed codes per CSV; insert canonical-only codes.

    Strategy (designed to scale to 50+ classifications with 1000s of codes
    each — the previous TRIM(vardekod) IN (?, ?, ...) pattern was O(N×M) and
    didn't use the value_code index). Steps below are numbered to match the
    log lines.

      1. build _vc_trim(code_id, kod, label) — pre-trimmed value_code mirror.
      2. stage _canon(cls_id, vardekod, label) from every CSV.
      3. insert canonical (vardekod, label) pairs missing from value_code.
      4a. materialize _cc_kods(cls_id, vardekod) — pairs already in CC.
      4b. materialize _canon_pairs(cls_id, code_id) — for is_valid lookup.
      5. insert canonical-but-unobserved classification_code rows.
      6. UPDATE classification_code SET is_valid = ... for CSV-backed cls.
      7. rollup valid_code_count and emit per-classification report.

    Per-classification operations (the old hot path) are eliminated.
    """
    if not csv_paths:
        return

    def _step(msg: str) -> None:
        _progress(f"  [{time.strftime('%H:%M:%S')}] {msg}")

    _step(f"Applying canonical codes for {len(csv_paths)} classifications...")

    # 1. Pre-trimmed value_code mirror. We carry the label too: step 5 binds
    # canonical-only CC rows to the value_code row whose (kod, label) matches
    # the canonical CSV, not just the kod — without label scoping a shared
    # vardekod across classifications could attach the wrong label.
    _step("  step 1/7: build _vc_trim (mirror of value_code with TRIM)...")
    conn.execute("DROP TABLE IF EXISTS _vc_trim")
    conn.execute(
        "CREATE TEMP TABLE _vc_trim (code_id INTEGER PRIMARY KEY, kod TEXT, label TEXT)"
    )
    conn.execute(
        "INSERT INTO _vc_trim "
        "SELECT code_id, TRIM(vardekod), TRIM(vardebenamning) FROM value_code"
    )
    conn.execute("CREATE INDEX _vc_trim_kod ON _vc_trim(kod)")
    conn.execute("CREATE INDEX _vc_trim_kod_label ON _vc_trim(kod, label)")
    n = conn.execute("SELECT COUNT(*) FROM _vc_trim").fetchone()[0]
    _step(f"    _vc_trim has {n:,} rows")

    # 2. Stage all canonical codes once.
    _step("  step 2/7: stage _canon from CSVs...")
    conn.execute("DROP TABLE IF EXISTS _canon")
    conn.execute(
        "CREATE TEMP TABLE _canon ("
        "  cls_id INTEGER NOT NULL,"
        "  vardekod TEXT NOT NULL,"
        "  label TEXT NOT NULL,"
        "  PRIMARY KEY (cls_id, vardekod)"
        ") WITHOUT ROWID"
    )
    canon_by_cls: dict[int, dict[str, str]] = {}
    canon_rows: list[tuple[int, str, str]] = []
    for short_name, csv_path in csv_paths.items():
        cls_id = id_by_short[short_name]
        canon = load_valid_codes(csv_path)
        canon_by_cls[cls_id] = canon
        canon_rows.extend((cls_id, code, label) for code, label in canon.items())
    conn.executemany(
        "INSERT INTO _canon (cls_id, vardekod, label) VALUES (?, ?, ?)",
        canon_rows,
    )
    conn.execute("CREATE INDEX _canon_kod ON _canon(vardekod)")
    n = conn.execute("SELECT COUNT(*) FROM _canon").fetchone()[0]
    _step(f"    _canon has {n:,} rows")

    # 3. Insert any canonical (vardekod, label) pair missing from value_code.
    # Scoping by both kod and label (not just kod) guarantees that step 5
    # finds a value_code row whose label matches the canonical CSV: when two
    # classifications share a vardekod with different canonical labels, each
    # gets its own value_code row to bind to.
    _step("  step 3/7: insert missing value_code rows...")
    cur = conn.execute(
        """
        INSERT INTO value_code (vardekod, vardebenamning)
        SELECT DISTINCT c.vardekod, c.label
        FROM _canon c
        WHERE NOT EXISTS (
            SELECT 1 FROM _vc_trim t
            WHERE t.kod = c.vardekod AND t.label = c.label
        )
        """
    )
    _step(f"    inserted {cur.rowcount} canonical (vardekod, label) value_code rows")
    if cur.rowcount > 0:
        # Refresh _vc_trim to include the inserts.
        conn.execute(
            "INSERT INTO _vc_trim "
            "SELECT vc.code_id, TRIM(vc.vardekod), TRIM(vc.vardebenamning) "
            "FROM value_code vc "
            "WHERE NOT EXISTS (SELECT 1 FROM _vc_trim t WHERE t.code_id = vc.code_id)"
        )

    # 4a. Materialize observed (cls_id, vardekod) pairs already present in CC
    # for the CSV-backed classifications. This lets us tell which canonical
    # vardekods are NOT yet represented in CC for a given classification.
    _step("  step 4a/7: materialize _cc_kods...")
    conn.execute("DROP TABLE IF EXISTS _cc_kods")
    conn.execute(
        "CREATE TEMP TABLE _cc_kods ("
        "  cls_id INTEGER NOT NULL,"
        "  vardekod TEXT NOT NULL,"
        "  PRIMARY KEY (cls_id, vardekod)"
        ") WITHOUT ROWID"
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO _cc_kods (cls_id, vardekod)
        SELECT cc.classification_id, t.kod
        FROM classification_code cc
        JOIN _vc_trim t ON t.code_id = cc.code_id
        WHERE cc.classification_id IN (SELECT DISTINCT cls_id FROM _canon)
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM _cc_kods").fetchone()[0]
    _step(f"    _cc_kods has {n:,} rows")

    # 4b. Materialize (cls_id, code_id) pairs where the code's vardekod is
    # canonical for cls_id. Used by step 6's is_valid lookup. WITHOUT ROWID
    # PK gives O(log n) EXISTS check.
    _step("  step 4b/7: materialize _canon_pairs...")
    conn.execute("DROP TABLE IF EXISTS _canon_pairs")
    conn.execute(
        "CREATE TEMP TABLE _canon_pairs ("
        "  cls_id INTEGER NOT NULL,"
        "  code_id INTEGER NOT NULL,"
        "  PRIMARY KEY (cls_id, code_id)"
        ") WITHOUT ROWID"
    )
    conn.execute(
        """
        INSERT INTO _canon_pairs (cls_id, code_id)
        SELECT DISTINCT c.cls_id, t.code_id
        FROM _canon c JOIN _vc_trim t ON t.kod = c.vardekod
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM _canon_pairs").fetchone()[0]
    _step(f"    _canon_pairs has {n:,} rows")

    # 5. Insert canonical-but-unobserved CC rows: ONE representative per
    # (cls_id, vardekod) where no CC row exists yet for that pair (i.e. no
    # observed instance for cls_id used a code with that vardekod). Joining
    # on (kod, label) ensures we pick a value_code row that matches the
    # canonical CSV's label, not some other classification's variant of the
    # same vardekod. Step 3 already guaranteed at least one such row exists.
    # MIN(code_id) is just a deterministic tiebreaker for the rare case
    # where multiple value_code rows share the same (kod, label) pair.
    # is_valid stays NULL here; step 6 sets it for all CSV-backed rows.
    _step("  step 5/7: insert canonical-but-unobserved CC representatives...")
    cur = conn.execute(
        f"""
        INSERT OR IGNORE INTO classification_code (classification_id, code_id, level, is_valid)
        SELECT c.cls_id, MIN(t.code_id),
               {_LEVEL_EXPR.format(col="c.vardekod")},
               NULL
        FROM _canon c
        JOIN _vc_trim t ON t.kod = c.vardekod AND t.label = c.label
        WHERE NOT EXISTS (
            SELECT 1 FROM _cc_kods k
            WHERE k.cls_id = c.cls_id AND k.vardekod = c.vardekod
        )
        GROUP BY c.cls_id, c.vardekod
        """
    )
    _step(f"    inserted {cur.rowcount} canonical-but-unobserved rows")

    # 6. Mark is_valid on every CC row belonging to a CSV-backed classification.
    # Vardekod-based: every label variant of a canonical code is treated as
    # canonical. (This matches our convention "is_valid is about the code, not
    # the label". Year-specific label distinctions, e.g. LKF, are handled by
    # the per-year classification split, not by per-label is_valid.)
    _step("  step 6/7: UPDATE classification_code SET is_valid...")
    conn.execute(
        """
        UPDATE classification_code
        SET is_valid = CASE WHEN EXISTS (
            SELECT 1 FROM _canon_pairs cp
            WHERE cp.cls_id = classification_code.classification_id
              AND cp.code_id = classification_code.code_id
        ) THEN 1 ELSE 0 END
        WHERE classification_id IN (SELECT DISTINCT cls_id FROM _canon_pairs)
        """
    )
    _step("    UPDATE done")
    _step("  step 7/7: rollup and reporting...")

    # Distinct vardekods, not CC rows: step 6 marks every label variant of a
    # canonical code as is_valid=1 (intentional — validity is keyed on the
    # code, not the label), so COUNT(*) would inflate beyond canonical CSV
    # cardinality whenever value_code holds multiple labels for one code.
    conn.execute(
        """
        UPDATE classification SET valid_code_count = (
            SELECT COUNT(DISTINCT TRIM(vc.vardekod))
            FROM classification_code cc
            JOIN value_code vc ON cc.code_id = vc.code_id
            WHERE cc.classification_id = classification.id AND cc.is_valid = 1
        )
        WHERE id IN (SELECT DISTINCT cls_id FROM _canon_pairs)
        """
    )

    counts = {
        row[0]: (row[1] or 0, row[2] or 0)
        for row in conn.execute(
            """
            SELECT cls.short_name,
                   SUM(CASE WHEN cc.is_valid = 1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN cc.is_valid = 0 THEN 1 ELSE 0 END)
            FROM classification cls
            JOIN classification_code cc ON cc.classification_id = cls.id
            WHERE cls.id IN (SELECT DISTINCT cls_id FROM _canon_pairs)
            GROUP BY cls.short_name
            """
        ).fetchall()
    }
    for short_name, csv_path in csv_paths.items():
        valid, observed_only = counts.get(short_name, (0, 0))
        cls_id = id_by_short[short_name]
        _progress(
            f"    {short_name}: {valid} canonical, {observed_only} observed-only "
            f"(from {csv_path.name}, {len(canon_by_cls[cls_id])} CSV codes)"
        )

    conn.execute("DROP TABLE IF EXISTS _canon_pairs")
    conn.execute("DROP TABLE IF EXISTS _cc_kods")
    conn.execute("DROP TABLE IF EXISTS _canon")
    conn.execute("DROP TABLE IF EXISTS _vc_trim")


def populate_classifications(
    conn: sqlite3.Connection,
    seed_path: Path,
    *,
    valid_codes_dir: Path | None = None,
) -> int:
    """Populate classification / classification_code / variable_instance.classification_id.

    Called once per ``build_db`` run, after value codes are imported so that
    ``cvid_value_code`` is complete. Strict failure modes:

    - A seed ``vardemangdsversion`` string that matches no instance → fail
    - A classification resolving to zero instances → fail
    - A classification resolving to zero value codes → fail
    - A seed ``valid_codes_file`` that doesn't resolve under
      ``valid_codes_dir`` → fail

    ``valid_codes_dir`` is the directory containing per-classification CSVs of
    canonical codes. When an entry has ``valid_codes_file = "<name>.csv"``, the
    CSV is loaded and used to mark each ``classification_code`` row as
    ``is_valid=1`` (canonical) or ``is_valid=0`` (observed-only). Canonical
    codes that don't appear in observed data are still inserted (they get a
    fresh ``value_code`` row with no ``cvid_value_code`` linkage).

    Returns the number of classifications inserted.
    """
    entries = load_seed(seed_path)
    csv_paths = _resolve_valid_codes_paths(entries, valid_codes_dir)
    _progress(
        f"Populating classifications from {seed_path.name} ({len(entries)} entries)..."
    )

    # Insert classification rows. supersedes_id is resolved in a second pass
    # once every row has a primary key.
    id_by_short: dict[str, int] = {}
    for entry in entries:
        cur = conn.execute(
            """
            INSERT INTO classification (
                short_name, name, name_en, publisher, version,
                valid_from, valid_to, description, url, supersedes_id, code_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
            """,
            (
                entry["short_name"],
                entry["name"],
                entry.get("name_en"),
                entry.get("publisher"),
                entry.get("version"),
                entry.get("valid_from"),
                entry.get("valid_to"),
                entry.get("description"),
                entry.get("url"),
            ),
        )
        id_by_short[entry["short_name"]] = cur.lastrowid

    for entry in entries:
        sup = entry.get("supersedes")
        if sup is not None:
            conn.execute(
                "UPDATE classification SET supersedes_id = ? WHERE id = ?",
                (id_by_short[sup], id_by_short[entry["short_name"]]),
            )

    # Tag matching variable instances. The seed has ~100+ vardemangdsversion
    # strings and the table has ~500k rows — without an index on
    # vardemangdsversion the UPDATE would full-scan the table. Build the
    # index once, drop it after population (it's not useful at query time).
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vi_vardemangdsversion_tmp "
        "ON variable_instance(vardemangdsversion)"
    )
    try:
        conn.execute("DROP TABLE IF EXISTS _vmap")
        conn.execute(
            "CREATE TEMP TABLE _vmap (vers TEXT PRIMARY KEY, cls_id INTEGER NOT NULL) "
            "WITHOUT ROWID"
        )
        conn.executemany(
            "INSERT INTO _vmap (vers, cls_id) VALUES (?, ?)",
            [
                (v, id_by_short[entry["short_name"]])
                for entry in entries
                for v in entry["vardemangdsversion"]
            ],
        )
        conn.execute(
            """
            UPDATE variable_instance
            SET classification_id = (SELECT cls_id FROM _vmap WHERE vers = vardemangdsversion)
            WHERE vardemangdsversion IN (SELECT vers FROM _vmap)
            """
        )
        # Drift detection: any seed string that matches no instance.
        # load_seed already rejects strings claimed by two classifications,
        # so a non-match here means the data lacks the version entirely.
        unmatched = conn.execute(
            """
            SELECT cls.short_name, m.vers
            FROM _vmap m
            JOIN classification cls ON cls.id = m.cls_id
            WHERE NOT EXISTS (
                SELECT 1 FROM variable_instance vi
                WHERE vi.vardemangdsversion = m.vers
            )
            ORDER BY cls.short_name, m.vers
            """
        ).fetchall()
        conn.execute("DROP TABLE IF EXISTS _vmap")
    finally:
        conn.execute("DROP INDEX IF EXISTS idx_vi_vardemangdsversion_tmp")

    if unmatched:
        details = "\n".join(f"  - {short}: {vers!r}" for short, vers in unmatched)
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="classification_seed_drift",
            error_class="configuration",
            message=(
                "Classification seed references vardemangdsversion strings "
                "that don't occur in the input data:\n" + details
            ),
            remediation=(
                "Either remove the stale entries from classifications.toml or "
                "re-export metadata so the strings match. Enumerate live "
                "values with: SELECT DISTINCT vardemangdsversion FROM "
                "variable_instance;"
            ),
        )

    # Populate classification_code with the deduplicated union of codes
    # reachable through tagged instances. is_valid is filled in afterwards
    # by _apply_valid_codes when a CSV is provided.
    _progress("  Building classification_code junction...")
    conn.execute(
        f"""
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
        SELECT DISTINCT
            vi.classification_id,
            cvc.code_id,
            {_LEVEL_EXPR.format(col="vc.vardekod")},
            NULL
        FROM variable_instance vi
        JOIN cvid_value_code cvc ON vi.cvid = cvc.cvid
        JOIN value_code vc ON cvc.code_id = vc.code_id
        WHERE vi.classification_id IS NOT NULL
        """
    )

    _apply_valid_codes(conn, csv_paths, id_by_short)

    # Cache code_count for every classification; valid_code_count was set
    # by _apply_valid_codes for classifications with a CSV (NULL otherwise).
    conn.execute(
        """
        UPDATE classification
        SET code_count = (
            SELECT COUNT(*) FROM classification_code
            WHERE classification_id = classification.id
        )
        """
    )

    empty = conn.execute(
        "SELECT short_name FROM classification WHERE code_count = 0 ORDER BY short_name"
    ).fetchall()
    if empty:
        details = "\n".join(f"  - {r[0]}" for r in empty)
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="classification_empty",
            error_class="configuration",
            message=("Classification(s) resolved to zero value codes:\n" + details),
            remediation=(
                "Tagged instances exist but have no codes in cvid_value_code. "
                "Either broaden the vardemangdsversion list or remove the "
                "entry from the seed."
            ),
        )

    # Populate classification_fts (content-synced: rowid == classification.id).
    conn.execute(
        """
        INSERT INTO classification_fts(rowid, short_name, name, name_en, description)
        SELECT id, short_name, name, name_en, description FROM classification
        """
    )

    n_cls, total_codes = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(code_count), 0) FROM classification"
    ).fetchone()
    _progress(f"  {n_cls} classifications, {total_codes:,} codes tagged")

    return len(entries)
