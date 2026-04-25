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
import tomllib
from pathlib import Path
from typing import Any

from .errors import EXIT_CONFIG, RegmetaError


_REQUIRED_FIELDS = ("short_name", "name", "vardemangdsversion")
_VALID_CODES_HEADER = ("vardekod", "vardebenamning")


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
) -> None:
    """Mutate each entry to add ``_valid_codes_path`` (resolved Path or None).

    A seed entry with ``valid_codes_file`` set but no ``valid_codes_dir``
    available, or a missing/non-file path, is a build-stop error.
    """
    for entry in entries:
        rel = entry.get("valid_codes_file")
        if rel is None:
            entry["_valid_codes_path"] = None
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
                    "Pass --classifications-dir to maintain build-db, or place "
                    "the CSVs under <csv_dir>/../classifications/."
                ),
            )
        path = (valid_codes_dir / rel).resolve()
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
        entry["_valid_codes_path"] = path


def _apply_valid_codes(
    conn: sqlite3.Connection,
    entries: list[dict[str, Any]],
    id_by_short: dict[str, int],
) -> None:
    """Mark canonical/observed codes per CSV; insert canonical-only codes.

    For each classification with a resolved CSV path:
      - Read the canonical code → label map.
      - For each canonical code, ensure a ``value_code`` row exists. Codes
        that aren't already in ``value_code`` (i.e. canonical-but-unobserved)
        are inserted with the CSV-supplied label.
      - Insert any canonical-but-unobserved codes into ``classification_code``
        (they'd be missing because the JOIN on ``cvid_value_code`` produced
        nothing for them).
      - Mark every ``classification_code`` row in this classification as
        ``is_valid=1`` (vardekod stripped) when the code appears in the CSV,
        else ``is_valid=0``.
      - Set ``classification.valid_code_count`` to the canonical count.
    """
    for entry in entries:
        path = entry.get("_valid_codes_path")
        if path is None:
            continue
        short = entry["short_name"]
        cls_id = id_by_short[short]
        canon = load_valid_codes(path)
        _progress(
            f"  Applying canonical codes for {short} from {path.name} ({len(canon)} codes)..."
        )

        # 1. Ensure every canonical code has a value_code row. Cache code_id.
        canon_code_ids: dict[str, int] = {}
        for code, label in canon.items():
            row = conn.execute(
                "SELECT code_id FROM value_code "
                "WHERE TRIM(vardekod) = ? AND vardebenamning = ? LIMIT 1",
                (code, label),
            ).fetchone()
            if row is not None:
                canon_code_ids[code] = row[0]
                continue
            # Fallback: any value_code row with the same code (label may differ
            # in observed data — CSV is canonical, so we still consider it the
            # canonical code).
            row = conn.execute(
                "SELECT code_id FROM value_code WHERE TRIM(vardekod) = ? LIMIT 1",
                (code,),
            ).fetchone()
            if row is not None:
                canon_code_ids[code] = row[0]
                continue
            cur = conn.execute(
                "INSERT INTO value_code (vardekod, vardebenamning) VALUES (?, ?)",
                (code, label),
            )
            canon_code_ids[code] = cur.lastrowid

        # 2. Insert canonical-but-unobserved codes into classification_code.
        for code, code_id in canon_code_ids.items():
            level = len(code) if code.isdigit() else None
            conn.execute(
                "INSERT OR IGNORE INTO classification_code "
                "(classification_id, code_id, level, is_valid) "
                "VALUES (?, ?, ?, 1)",
                (cls_id, code_id, level),
            )

        # 3. Mark every existing classification_code row in this classification.
        # is_valid=1 if the value_code's stripped vardekod matches a canonical
        # code, else 0.
        conn.execute(
            """
            UPDATE classification_code
            SET is_valid = (
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM value_code vc
                    WHERE vc.code_id = classification_code.code_id
                      AND TRIM(vc.vardekod) IN (%s)
                ) THEN 1 ELSE 0 END
            )
            WHERE classification_id = ?
            """
            % ",".join("?" * len(canon_code_ids)),
            (*canon_code_ids.keys(), cls_id),
        )

        valid_count = conn.execute(
            "SELECT COUNT(*) FROM classification_code "
            "WHERE classification_id = ? AND is_valid = 1",
            (cls_id,),
        ).fetchone()[0]
        observed_only = conn.execute(
            "SELECT COUNT(*) FROM classification_code "
            "WHERE classification_id = ? AND is_valid = 0",
            (cls_id,),
        ).fetchone()[0]
        unobserved = conn.execute(
            """
            SELECT COUNT(*) FROM classification_code cc
            JOIN value_code vc ON cc.code_id = vc.code_id
            WHERE cc.classification_id = ? AND cc.is_valid = 1
              AND NOT EXISTS (
                SELECT 1 FROM cvid_value_code cvc
                JOIN variable_instance vi ON cvc.cvid = vi.cvid
                WHERE cvc.code_id = vc.code_id
                  AND vi.classification_id = cc.classification_id
              )
            """,
            (cls_id,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE classification SET valid_code_count = ? WHERE id = ?",
            (valid_count, cls_id),
        )
        _progress(
            f"    {valid_count} canonical, {observed_only} observed-only, "
            f"{unobserved} canonical-but-unobserved"
        )


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
    _resolve_valid_codes_paths(entries, valid_codes_dir)
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
    # vardemangdsversion each UPDATE would full-scan the table. Build the
    # index once, drop it after population (it's not useful at query time).
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vi_vardemangdsversion_tmp "
        "ON variable_instance(vardemangdsversion)"
    )
    try:
        # Validate that every seed string hits at least one instance —
        # otherwise either the seed has drifted or the underlying data
        # changed, and we should not silently ship an incomplete
        # classification.
        unmatched: list[tuple[str, str]] = []
        for entry in entries:
            cls_id = id_by_short[entry["short_name"]]
            for version_str in entry["vardemangdsversion"]:
                cur = conn.execute(
                    "UPDATE variable_instance SET classification_id = ? "
                    "WHERE vardemangdsversion = ? AND classification_id IS NULL",
                    (cls_id, version_str),
                )
                if cur.rowcount == 0:
                    # rowcount==0 either because no row matches OR because
                    # another classification already claimed it (caught by
                    # the duplicate check in load_seed). Verify via SELECT
                    # so the error message is precise.
                    exists = conn.execute(
                        "SELECT 1 FROM variable_instance "
                        "WHERE vardemangdsversion = ? LIMIT 1",
                        (version_str,),
                    ).fetchone()
                    if exists is None:
                        unmatched.append((entry["short_name"], version_str))
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
    # reachable through tagged instances. level = numeric code length for
    # all-digit codes, NULL otherwise (see DESIGN.md). is_valid is filled
    # in afterwards by _apply_valid_codes when a CSV is provided.
    _progress("  Building classification_code junction...")
    conn.execute(
        """
        INSERT INTO classification_code (classification_id, code_id, level, is_valid)
        SELECT DISTINCT
            vi.classification_id,
            cvc.code_id,
            CASE
                WHEN vc.vardekod GLOB '[0-9]*'
                     AND NOT vc.vardekod GLOB '*[^0-9]*'
                THEN length(vc.vardekod)
                ELSE NULL
            END,
            NULL
        FROM variable_instance vi
        JOIN cvid_value_code cvc ON vi.cvid = cvc.cvid
        JOIN value_code vc ON cvc.code_id = vc.code_id
        WHERE vi.classification_id IS NOT NULL
        """
    )

    _apply_valid_codes(conn, entries, id_by_short)

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
