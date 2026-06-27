"""Classification seed loading and build-time population.

A classification is a normalized code system (SUN2000, SSYK2012, SNI2007, ...)
that groups the value codes produced by many variable instances. The seed at
``reg_meta_build/classifications.toml`` declares one entry per code system and
lists the raw ``variable_instance.vardemangdsversion`` strings that map to it.

Runtime never loads the seed — query commands read the already-populated
``classification`` / ``classification_code`` tables. ``populate_classifications``
is only called during ``reg-meta-build build-db``.
"""

from __future__ import annotations

import csv
import sys
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta.errors import EXIT_CONFIG, RegMetaError

from .concept_groups import classification_slug_stem
from .fqid_slugs import _toml_comment, _toml_str

if TYPE_CHECKING:
    import sqlite3

# vardemangdsversion is OPTIONAL: a provider-seeded entry may carry canonical
# codes (via valid_codes_file) with no observed instance-label linkage.
_REQUIRED_FIELDS = ("short_name", "name")
# Accepted first-two-column headers for a valid-codes CSV. SCB CSVs use the
# native `vardekod,vardebenamning`; the universal `code,label` shape is what
# the SOS classification CSVs ship (with extra trailing columns we drop).
_VALID_CODES_HEADERS = (("vardekod", "vardebenamning"), ("code", "label"))

# level = number of digits for all-digit codes, NULL otherwise. Used in
# multiple INSERTs against classification_code; keep the SQL identical so
# canonical-only rows and observed rows agree on level.
_LEVEL_EXPR = (
    "CASE WHEN {col} GLOB '[0-9]*' AND NOT {col} GLOB '*[^0-9]*' "
    "THEN length({col}) ELSE NULL END"
)

# --- code-set-containment detector (#416) -----------------------------------
# Thresholds for `link_value_set_classifications`. Measured on a real scb,sos
# build (2026-06-15, issue #416): a value set whose code STRINGS are ≥0.90
# contained in a classification's canonical codes (≥8 distinct codes) "looks
# like" that classification. Code-set SIZE — not labels — is the de-ambiguator:
# at ≥8 codes 930 of 1532 candidates are family-ambiguous; ≥15 codes collapses
# that to 78, so a single-family set with ≥15 codes is treated as confident.
# A shorter single-family set is rescued only when its (code,label) pairs also
# agree (≥0.90) — relabeled SCB sets share no labels, so label agreement is a
# precision lever, not a recall one.
_MIN_CONTAINMENT = 0.90  # consideration floor: matched / n_codes
_MIN_CODES = 8  # consideration floor: distinct codes in the value set
_CONFIDENT_MIN_CODES = 15  # single-family at/above this auto-links on size alone
_CONFIDENT_LABEL_AGREE = 0.90  # else rescue a shorter single-family set on labels


def repo_seed_path() -> Path | None:
    """Return the in-repo classifications seed, for build-time use only.

    Located from ``reg_meta_build/src/reg_meta_build/`` up two levels to the
    ``reg_meta_build/`` package root and down to ``classifications.toml``.
    Installed wheels do not ship the seed — it is a maintainer artifact,
    same as ``reg_meta_build/docs/``.
    """
    pkg_dir = Path(__file__).resolve().parent
    candidate = pkg_dir.parent.parent / "classifications.toml"
    return candidate if candidate.is_file() else None


def declared_short_names(seed_path: Path | None = None) -> frozenset[str]:
    """Every classification ``short_name`` declared in the seed, provider-agnostic
    — includes provider-tagged entries (e.g. ``provider = "sos"``), all of which
    are seeded (the ``provider`` tag is only their label-source). For build-time
    validation of references to a classification (e.g. a curated thin-provider's
    ``classification`` link).

    ``seed_path`` defaults to the in-repo seed via ``repo_seed_path()``; pass the
    build's own seed (``build_db(seed_path=...)``) so validation matches what
    ``populate_classifications`` seeds. Resolution mirrors
    ``materialize``'s ``seed_path or repo_seed_path()`` exactly, keeping the two
    consistent. Build-time only; raises if the seed is not locatable (an
    installed wheel doesn't ship it — but no build runs there)."""
    path = seed_path or repo_seed_path()
    if path is None:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="classification_seed_unreadable",
            error_class="configuration",
            message=(
                "classifications.toml seed not found; cannot validate "
                "classification references."
            ),
            remediation=(
                "Run build-db from the maintainer repo checkout where "
                "classifications.toml is present."
            ),
        )
    return frozenset(entry["short_name"] for entry in load_seed(path))


def load_valid_codes(path: Path) -> dict[str, str]:
    """Read a canonical valid-codes CSV and return ``{code: label}``.

    The first two columns must be headed ``vardekod,vardebenamning`` (SCB) or
    ``code,label`` (universal/SOS). Any further columns (``label_en``,
    ``parent_code``, validity dates) are ignored — per-code en-labels, validity
    and hierarchy are a future enhancement, not modeled here. Codes are
    stripped of leading/trailing whitespace before use (matches the rule used
    at query time). Duplicate codes raise.
    """
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if (
                header is None
                or tuple(h.strip() for h in header[:2]) not in _VALID_CODES_HEADERS
            ):
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_csv_invalid",
                    error_class="configuration",
                    message=(
                        f"{path}: first two columns must be "
                        f"'vardekod,vardebenamning' or 'code,label' "
                        f"(got {header!r})."
                    ),
                    remediation="Fix the CSV header.",
                )
            out: dict[str, str] = {}
            for lineno, row in enumerate(reader, start=2):
                if not row or all(not c.strip() for c in row):
                    continue
                if len(row) < 2:
                    raise RegMetaError(
                        exit_code=EXIT_CONFIG,
                        code="classification_csv_invalid",
                        error_class="configuration",
                        message=f"{path}:{lineno}: expected 2 columns, got {len(row)}.",
                        remediation="Each row must be 'vardekod,vardebenamning'.",
                    )
                code = row[0].strip()
                label = row[1].strip()
                if not code:
                    raise RegMetaError(
                        exit_code=EXIT_CONFIG,
                        code="classification_csv_invalid",
                        error_class="configuration",
                        message=f"{path}:{lineno}: empty vardekod.",
                        remediation="Remove the row or supply a code.",
                    )
                if code in out:
                    raise RegMetaError(
                        exit_code=EXIT_CONFIG,
                        code="classification_csv_invalid",
                        error_class="configuration",
                        message=f"{path}:{lineno}: duplicate vardekod {code!r}.",
                        remediation="Each vardekod must appear once.",
                    )
                out[code] = label
            if not out:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_csv_invalid",
                    error_class="configuration",
                    message=f"{path}: no data rows.",
                    remediation="The CSV must contain at least one code.",
                )
            return out
    except OSError as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="classification_csv_unreadable",
            error_class="configuration",
            message=f"Could not read {path}: {exc}",
            remediation="Check the file path and permissions.",
        ) from exc


def load_seed(path: Path) -> list[dict[str, Any]]:
    """Parse and validate the classification seed file.

    Raises ``RegMetaError`` on structural issues (missing required fields,
    duplicate short_names, duplicate vardemangdsversion strings across
    classifications). Does not touch the DB.
    """
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="classification_seed_unreadable",
            error_class="configuration",
            message=f"Could not parse classification seed {path}: {exc}",
            remediation="Ensure the file is valid TOML.",
        ) from exc

    entries = data.get("classification") or []
    if not entries:
        raise RegMetaError(
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
                raise RegMetaError(
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
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=f"Duplicate classification short_name: {short!r}",
                remediation="Each short_name must be unique in the seed.",
            )
        seen_short_names.add(short)

        # vardemangdsversion is optional: a missing key or an empty list means
        # the entry tags no instances (provider-seeded canonical codes only).
        # When present it must be a list of strings.
        versions = entry.get("vardemangdsversion")
        if versions is not None:
            if not isinstance(versions, list) or not all(
                isinstance(v, str) for v in versions
            ):
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_seed_invalid",
                    error_class="configuration",
                    message=(f"{short}: vardemangdsversion must be a list of strings."),
                    remediation="Use a TOML array of quoted strings.",
                )
            for v in versions:
                if v in seen_versions:
                    raise RegMetaError(
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

        # Every classification carries a git-tracked canonical-codes CSV. This
        # is what makes seeding provider-agnostic safe: a thin --providers build
        # seeds every classification regardless of which provider is built, and
        # the CSV always supplies codes so the `classification_empty` guard never
        # trips. An entry without `valid_codes_file` would silently break that
        # guarantee, so require it (fail-fast) rather than discover it on a build.
        vcf = entry.get("valid_codes_file")
        if not vcf:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=(
                    f"{short}: every classification must declare a "
                    "valid_codes_file (canonical codes are always seeded)."
                ),
                remediation=(
                    "Add valid_codes_file = '<name>.csv' and place the CSV under "
                    "<input_dir>/classifications/."
                ),
            )
        if not isinstance(vcf, str):
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=f"{short}: valid_codes_file must be a string.",
                remediation="Use a relative filename like 'sun2000-niva.csv'.",
            )

        # provider is an optional LABEL-SOURCE tag (not a DB column): which
        # provider's instance value-set-version-label strings carry it. It no
        # longer gates seeding (classifications are always seeded) — its sole
        # role is scoping the #597 seed-drift demotion.
        prov = entry.get("provider")
        if prov is not None and not isinstance(prov, str):
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="classification_seed_invalid",
                error_class="configuration",
                message=f"{short}: provider must be a string.",
                remediation='Use a provider slug like provider = "sos".',
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
            raise RegMetaError(
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
            raise RegMetaError(
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
            raise RegMetaError(
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
        "INSERT INTO _vc_trim SELECT code_id, TRIM(code), TRIM(label) FROM value_code"
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
        INSERT INTO value_code (code, label)
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
            "SELECT vc.code_id, TRIM(vc.code), TRIM(vc.label) "
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
            SELECT COUNT(DISTINCT TRIM(vc.code))
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
    built_providers: frozenset[str] | None = None,
) -> int:
    """Populate classification / classification_code / variable_instance.classification_id.

    Called once per ``build_db`` run, after value codes are imported and
    ``variable_instance.value_set_id`` has been linked. Strict failure modes:

    - A seed ``vardemangdsversion`` string that matches no instance → fail
    - A classification resolving to zero value codes → fail
    - A seed ``valid_codes_file`` that doesn't resolve under
      ``valid_codes_dir`` → fail

    ``valid_codes_dir`` is the directory containing per-classification CSVs of
    canonical codes. Every entry has ``valid_codes_file = "<name>.csv"`` (the
    seed loader requires it): the CSV is loaded and used to mark each
    ``classification_code`` row as ``is_valid=1`` (canonical) or ``is_valid=0``
    (observed-only). Canonical codes that don't appear in observed data are still
    inserted (they get a fresh ``value_code`` row with no ``value_set_member``
    linkage).

    EVERY classification is seeded, regardless of ``--providers``. Classifications
    are SHARED standards (e.g. ``ICD-10-SE``/``ATC`` are tagged ``provider="sos"``
    yet referenced by FOHM, FK, Läkemedelsverket, Pliktverket) and each carries a
    git-tracked ``valid_codes_file`` CSV, so a classification's canonical codes
    are always available — provider-gating the seed is unnecessary. The
    ``provider`` tag's SOLE remaining role is scoping the #597 seed-drift
    demotion below.

    Seed-drift demotion is decided PER-CLASSIFICATION on label-source (#597). An
    entry's ``provider`` field is its LABEL-SOURCE declaration — which provider's
    ``variable_instance.value_set_version_label`` strings carry it; untagged
    entries are implicitly SCB-sourced. For each unmatched version string of a
    classification: it is a HARD drift (a real typo/stale on a built provider)
    when the label-source provider IS built (``built_providers is None`` or
    label-source in ``built_providers``) OR the classification is MIXED (≥1 of
    its strings matched — a partial-present source). It is DEMOTED (a
    ``_progress`` note, no error) only when the label-source provider isn't built
    AND the whole classification is absent (zero strings matched) — the expected
    shape on a subset build. Without the per-classification rule, a
    ``--providers scb`` build would wrongly relax drift for the untagged
    (SCB-sourced) classifications even though SCB IS built, demoting a real typo.
    ``built_providers`` ``None`` (the default = full build) is strict everywhere.
    Either way every classification is seeded — its CSV ``valid_codes_file``
    supplies codes — so the downstream ``classification_empty`` guard is
    unaffected.

    Returns ``n_seeded`` — the count of classifications inserted.
    """
    entries = load_seed(seed_path)
    csv_paths = _resolve_valid_codes_paths(entries, valid_codes_dir)
    _progress(
        f"Populating classifications from {seed_path.name} ({len(entries)} entries)..."
    )

    # Insert classification rows. supersedes_id stays NULL at insert — it is a
    # DERIVED projection of `classification_replaced_by` (the canonical succession
    # surface), set later in the build by `derive_supersedes_from_edges` once the
    # auto + curated edges exist. The seed no longer carries succession.
    id_by_short: dict[str, int] = {}
    for entry in entries:
        cur = conn.execute(
            """
            INSERT INTO classification (
                short_name, name, name_en, publisher,
                valid_from, valid_to, description, url, supersedes_id, code_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
            """,
            (
                entry["short_name"],
                entry["name"],
                entry.get("name_en"),
                entry.get("publisher"),
                entry.get("valid_from"),
                entry.get("valid_to"),
                entry.get("description"),
                entry.get("url"),
            ),
        )
        assert cur.lastrowid is not None  # sqlite always populates after INSERT
        id_by_short[entry["short_name"]] = cur.lastrowid

    # Tag matching variable instances. The seed has ~100+ version-label
    # strings and the table has ~500k rows — without an index on
    # value_set_version_label the UPDATE would full-scan the table. Build
    # the index once, drop it after population (it's not useful at query
    # time). (Column name follows the universal-vocabulary rename from
    # `vardemangdsversion`; see reg_meta/DESIGN.md → Glossary and Swedish↔English crosswalk.)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vi_value_set_version_label_tmp "
        "ON variable_instance(value_set_version_label)"
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
                for v in (entry.get("vardemangdsversion") or [])
            ],
        )
        conn.execute(
            """
            UPDATE variable_instance
            SET classification_id = (SELECT cls_id FROM _vmap WHERE vers = value_set_version_label)
            WHERE value_set_version_label IN (SELECT vers FROM _vmap)
            """
        )
        # Drift detection: per seed string, whether it matched any instance.
        # load_seed already rejects strings claimed by two classifications, so a
        # non-match here means the data lacks the version entirely. We need the
        # per-classification matched count (computed BEFORE dropping _vmap) to
        # partition unmatched strings into hard-drift vs partial-build demotions.
        version_rows = conn.execute(
            """
            SELECT cls.short_name,
                   m.vers,
                   EXISTS (
                       SELECT 1 FROM variable_instance vi
                       WHERE vi.value_set_version_label = m.vers
                   ) AS matched
            FROM _vmap m
            JOIN classification cls ON cls.id = m.cls_id
            ORDER BY cls.short_name, m.vers
            """
        ).fetchall()
        conn.execute("DROP TABLE IF EXISTS _vmap")
    finally:
        conn.execute("DROP INDEX IF EXISTS idx_vi_value_set_version_label_tmp")

    # Per classification, how many of its seed version strings matched ≥1
    # instance; then partition the unmatched strings.
    matched_count: dict[str, int] = {}
    for short, _vers, matched in version_rows:
        matched_count[short] = matched_count.get(short, 0) + (1 if matched else 0)

    # Per-classification label-source: its `provider` tag, or "scb" if untagged
    # (untagged entries are implicitly SCB-sourced). #597: the demote decision is
    # made on THIS, not on a whole-build partial flag — so a built label-source
    # (e.g. scb on a `--providers scb` build) stays strict even on a provider
    # subset.
    label_source_by_short = {
        e["short_name"]: (e.get("provider") or "scb") for e in entries
    }

    hard_drift: list[tuple[str, str]] = []
    demoted: list[tuple[str, str]] = []
    for short, vers, matched in version_rows:
        if matched:
            continue
        label_source = label_source_by_short.get(short, "scb")
        source_built = built_providers is None or label_source in built_providers
        # Hard error when the label-source IS built (a real typo/stale on a built
        # provider) OR the classification is MIXED (≥1 matched → a partly-present
        # source). Demote only when the label-source provider isn't built AND the
        # whole classification is absent (its expected shape on a subset build).
        if source_built or matched_count.get(short, 0) > 0:
            hard_drift.append((short, vers))
        else:
            demoted.append((short, vers))

    if demoted:
        n_demoted = len(demoted)
        m_classes = len({short for short, _ in demoted})
        _progress(
            f"  {n_demoted} seed version-label(s) across {m_classes} "
            "classification(s) unmatched — expected on a partial --providers "
            "build (label-source provider not built); classifications kept "
            "(CSV codes)"
        )

    if hard_drift:
        details = "\n".join(f"  - {short}: {vers!r}" for short, vers in hard_drift)
        raise RegMetaError(
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
                "values with: SELECT DISTINCT value_set_version_label FROM "
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
            vsm.code_id,
            {_LEVEL_EXPR.format(col="vc.code")},
            NULL
        FROM variable_instance vi
        JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id
        JOIN value_code vc ON vsm.code_id = vc.code_id
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
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="classification_empty",
            error_class="configuration",
            message=("Classification(s) resolved to zero value codes:\n" + details),
            remediation=(
                "Tagged instances exist but have no codes reachable via "
                "value_set_member. Either broaden the vardemangdsversion "
                "list, check that year-projection isn't excluding every "
                "code (rare), or remove the entry from the seed."
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


def derive_supersedes_from_edges(conn: sqlite3.Connection) -> int:
    """Project `classification.supersedes_id` from `classification_replaced_by`.

    `classification_replaced_by` is the single canonical succession surface — fed
    by `derive_classification_succession` (auto year-tail editions) plus the
    curated `relations.toml` `class/<slug>` edges (e.g. the sun1996 → niva /
    inriktning / grupp split). `supersedes_id` is a DERIVED back-pointer onto it:
    for each classification `c`, set it to the id of `c`'s predecessor — the
    classification on the `predecessor_slug` side of the edge whose
    `successor_slug = c.slug`. A classification with no predecessor edge keeps
    NULL. A classification with MULTIPLE predecessor edges (a hypothetical merge —
    none today) picks the deterministic-first predecessor `ORDER BY
    predecessor_slug` so the projection is reproducible.

    Slug-anchored: `classification_replaced_by.predecessor_slug / successor_slug`
    ↔ `classification.slug`. MUST run AFTER the auto + curated edges are
    materialized and AFTER `populate_slugs` (the join needs non-NULL slugs), and
    BEFORE `link_value_set_classifications` — its `_chain_root` recursive CTE
    walks `supersedes_id`. Resets `supersedes_id` to NULL first so the projection
    is a pure function of the edge table (no stale carry-over). Returns the count
    of classifications that gained a non-NULL `supersedes_id`.
    """
    conn.execute("UPDATE classification SET supersedes_id = NULL")
    cur = conn.execute(
        """
        UPDATE classification AS c
        SET supersedes_id = (
            SELECT p.id
            FROM classification_replaced_by e
            JOIN classification p ON p.slug = e.predecessor_slug
            WHERE e.successor_slug = c.slug
            ORDER BY e.predecessor_slug
            LIMIT 1
        )
        WHERE c.slug IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM classification_replaced_by e
              WHERE e.successor_slug = c.slug
          )
        """
    )
    n_set = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
    _progress(f"  {n_set:,} classification supersedes_id derived from edges")
    return n_set


def _build_containment_temp_tables(conn: sqlite3.Connection) -> None:
    """Build the shared `_canon_codes` / `_vs_codes` / `_vs_stats` / `_vs_cls` /
    `_vs_label_agree` temp tables (steps 1-3 of the #416 code-set-containment
    detector, plus the shared label_agree projection) on `conn`.

    READ-ONLY against the persistent schema: it only CREATEs TEMP tables and their
    indexes, never touching `classification_candidate` / `variable_state`. Both
    `link_value_set_classifications` (the producer) and `dump_classification_residue`
    (the read-only diagnostic) call this so the containment SQL — the canonical-code
    projection, per-value-set `n_codes`/`dom_level`, and the grain-filtered
    `_vs_cls` containment under the `_MIN_CODES` / `_MIN_CONTAINMENT` thresholds —
    lives in ONE place. Each caller DROPs these tables in its own lifecycle (the
    detector reuses them through step 7; the diagnostic drops them once it has read
    `_vs_cls`).

    `_vs_label_agree(value_set_id, cls_id, label_agree)` is the shared exact-(kod,
    label) agreement projection over the `_vs_cls` pairs, read by BOTH step 5's
    confident-link filter and the #513 residue diagnostic so the formula has ONE
    home (it was duplicated verbatim in both callers — #738).

    Behaviour-preserving: the SQL here is moved verbatim from the detector, so the
    linkage output is byte-identical. Do NOT change the thresholds or grain logic.

    Steps 4+ (`_vs_single` / `_vs_confident` / the vintage reclaim) stay in
    `link_value_set_classifications` — they are the WRITE side; the residue
    diagnostic recomputes multi-family from `_vs_cls` directly."""
    for tmp in ("_canon_codes", "_vs_codes", "_vs_stats", "_vs_cls", "_vs_label_agree"):
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    # 1. Canonical (cls_id, kod, level, label). is_valid IS NOT 0 keeps both the
    # CSV-canonical rows (is_valid=1) and the no-CSV classifications (is_valid
    # NULL — their observed codes are the only code set we have). TRIM mirrors the
    # query-time rule and `_apply_valid_codes`.
    conn.execute(
        "CREATE TEMP TABLE _canon_codes ("
        "  cls_id INTEGER NOT NULL,"
        "  kod TEXT NOT NULL,"
        "  level INTEGER,"
        "  label TEXT NOT NULL"
        ")"
    )
    conn.execute(
        """
        INSERT INTO _canon_codes (cls_id, kod, level, label)
        SELECT cc.classification_id, TRIM(vc.code), cc.level, TRIM(vc.label)
        FROM classification_code cc
        JOIN value_code vc ON cc.code_id = vc.code_id
        WHERE cc.is_valid IS NOT 0
        """
    )
    conn.execute("CREATE INDEX _canon_codes_kod ON _canon_codes(kod, level)")
    conn.execute(
        "CREATE INDEX _canon_codes_kod_label ON _canon_codes(kod, label, cls_id)"
    )

    # 2. Per value set: the distinct (kod, label) pairs, then n_codes and
    # dom_level. dom_level is the single digit-length shared by EVERY code (an
    # all-digit string of that length) — the grain key — else NULL.
    conn.execute(
        "CREATE TEMP TABLE _vs_codes ("
        "  value_set_id INTEGER NOT NULL,"
        "  kod TEXT NOT NULL,"
        "  label TEXT NOT NULL"
        ")"
    )
    conn.execute(
        """
        INSERT INTO _vs_codes (value_set_id, kod, label)
        SELECT DISTINCT vsm.value_set_id, TRIM(vc.code), TRIM(vc.label)
        FROM value_set_member vsm
        JOIN value_code vc ON vsm.code_id = vc.code_id
        """
    )
    conn.execute("CREATE INDEX _vs_codes_vs ON _vs_codes(value_set_id, kod)")

    # dom_level: COUNT(DISTINCT digit-length over all-digit codes) = 1 AND every
    # code is all-digit (no NULL length) → that one length; else NULL. Reuses the
    # `_LEVEL_EXPR` digit-length idea (length when all-digit, NULL otherwise).
    conn.execute(
        f"""
        CREATE TEMP TABLE _vs_stats AS
        SELECT
            value_set_id,
            COUNT(DISTINCT kod) AS n_codes,
            CASE
                WHEN COUNT(DISTINCT CASE WHEN {_LEVEL_EXPR.format(col="kod")} IS NULL
                                         THEN kod END) = 0
                 AND COUNT(DISTINCT {_LEVEL_EXPR.format(col="kod")}) = 1
                THEN MAX({_LEVEL_EXPR.format(col="kod")})
                ELSE NULL
            END AS dom_level
        FROM _vs_codes
        GROUP BY value_set_id
        """
    )
    conn.execute("CREATE UNIQUE INDEX _vs_stats_pk ON _vs_stats(value_set_id)")

    # 3. Containment per (value_set_id, cls_id) under the grain filter: a value-set
    # code matches a canonical row on `kod`, and when dom_level IS NOT NULL also on
    # `level = dom_level` (no level restriction when dom_level IS NULL). Kept when
    # n_codes >= _MIN_CODES AND matched/n_codes >= _MIN_CONTAINMENT.
    conn.execute(
        f"""
        CREATE TEMP TABLE _vs_cls AS
        SELECT
            v.value_set_id,
            c.cls_id,
            COUNT(DISTINCT v.kod) AS matched,
            s.n_codes,
            (CAST(COUNT(DISTINCT v.kod) AS REAL) / s.n_codes) AS containment
        FROM _vs_codes v
        JOIN _vs_stats s ON s.value_set_id = v.value_set_id
        JOIN _canon_codes c
          ON c.kod = v.kod
         AND (s.dom_level IS NULL OR c.level = s.dom_level)
        WHERE s.n_codes >= {_MIN_CODES}
        GROUP BY v.value_set_id, c.cls_id
        HAVING containment >= {_MIN_CONTAINMENT}
        """
    )
    conn.execute("CREATE INDEX _vs_cls_vs ON _vs_cls(value_set_id)")

    # Shared exact-(kod, label) label_agree per (value_set_id, cls_id), over the
    # `_vs_cls` pairs (#738 — was duplicated verbatim in step 5 and the #513
    # diagnostic). label_agree = the number of DISTINCT value-set codes (kods) that
    # have at least one exact (kod, label) match against the candidate cls's
    # canonical (kod, label) pairs, divided by n_codes. The numerator is
    # COUNT(DISTINCT v.kod) — not COUNT(*) — because `_vs_codes` holds distinct
    # (kod, label) rows, so a single code carried under two matching labels would
    # otherwise count twice against a DISTINCT-kod denominator and let label_agree
    # exceed 1.0. Distinct-kod keeps it bounded ≤ 1.0 and matches the issue's
    # intended "exact code+label ≥0.90" metric.
    #
    # Byte-identity: `vc.n_codes` here is `_vs_stats.n_codes` joined into `_vs_cls`
    # (see the `_vs_cls` CREATE: `s.n_codes`), so dividing by `vc.n_codes` equals
    # step 5's old `/ st.n_codes` and the diagnostic's old `/ vc.n_codes` — same
    # denominator. The numerator subquery is identical to both old inline copies.
    # Coverage is identical: step 5's cls always comes from `_vs_single` ⊆ `_vs_cls`,
    # and the diagnostic's candidates ARE `_vs_cls` rows — both only ever need
    # label_agree for `_vs_cls` pairs.
    conn.execute(
        """
        CREATE TEMP TABLE _vs_label_agree AS
        SELECT
            vc.value_set_id,
            vc.cls_id,
            (CAST((
                SELECT COUNT(DISTINCT v.kod)
                FROM _vs_codes v
                WHERE v.value_set_id = vc.value_set_id
                  AND EXISTS (
                      SELECT 1 FROM _canon_codes c
                      WHERE c.cls_id = vc.cls_id
                        AND c.kod = v.kod
                        AND c.label = v.label
                  )
            ) AS REAL) / vc.n_codes) AS label_agree
        FROM _vs_cls vc
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX _vs_label_agree_pk "
        "ON _vs_label_agree(value_set_id, cls_id)"
    )


def link_value_set_classifications(conn: sqlite3.Connection) -> dict[str, int]:
    """Detect value sets that enumerate a known classification's codes inline and
    feed the confident matches into `classification_candidate` (#416).

    Many value sets list a classification's codes verbatim (e.g. an ULF health
    variable carrying 2043/2057 ICD-10-SE codes) but sit unlinked
    (`variable_state.classification_id IS NULL`) because SCB declared no
    classification for them. SCB's `populate_classifications` name-map and the SOS
    / #446 feeds only catch the DECLARED cases; this pass catches the rest by their
    CODES — a deterministic containment test, no name patterns.

    Additive producer: it INSERTs `(variable_id, value_set_id, classification_id)`
    rows into the provider-blind `classification_candidate` table (the same seam
    `_backfill_state_classifications` folds), guarded so it NEVER emits a candidate
    for a `(variable_id, value_set_id)` state key that already has one. Curated /
    name-based / SCB / SOS / #446 links therefore always win; this only fills gaps.
    Inline value codes are never deleted or re-pointed — linkage is additive.

    Algorithm (temp-table SQL, mirroring `_apply_valid_codes` — no Python row
    loops over the ~60k value sets):

      1. `_canon_codes`: canonical (cls_id, kod, level, label) from
         `classification_code` where `is_valid IS NOT 0` (1 OR NULL; NULL = a
         no-CSV classification whose observed codes ARE its code set).
      2. `_vs_codes` / `_vs_stats`: per value set, the distinct code strings, the
         distinct-code count `n_codes`, and `dom_level` = the single digit-length
         when EVERY code is an all-digit string of that length, else NULL.
      3. `_vs_cls`: containment per (value_set_id, cls_id) with a GRAIN filter —
         when `dom_level` is set, a value-set code matches a canonical row only at
         the same `level` (a 4-digit set matches the cls's 4-digit codes, not its
         2-digit ones); kept when `n_codes >= 8 AND containment >= 0.90`.
      4. Single-family value sets (EXACTLY one surviving cls), with label agreement
         for that one candidate.
      5. `_vs_confident`: single-family AND (`n_codes >= 15` OR `label_agree >=
         0.90`).
      6. Emit the confident map into `classification_candidate`, additively.
      7. Vintage-period reclaim (#494 PART 1, dominant-chain #514): much of the
         multi-family residue is ONE family across vintages (SNI2002↔SNI2007,
         SSYK96↔SSYK2012, SUN/LKF editions) — distinct `classification` rows on one
         `supersedes_id` chain. Group a value set's candidate cls (those in `_vs_cls`)
         into "vintage families" keyed on BOTH the supersedes-chain root
         (`_chain_root`) AND the slug STEM (`classification_stem`, the
         year-tail-stripped slug). The chain root alone is NOT enough: the #579
         sun1996 → {niva, inriktning, grupp} curated split puts three ORTHOGONAL SUN
         dimensions under one chain root, so the shared-stem requirement keeps two
         dimensions apart (sun-niva* vs sun-inriktning* are different stems). A family
         is a MULTI-VINTAGE CHAIN when the value set matches ≥2 of its editions.
         Reclaim a multi-family value set when (1) EXACTLY ONE family is a
         multi-vintage chain — the DOMINANT chain — so a single off-chain stray that
         only coincidentally matches one edition (the LKF county residue: ≥2 LKF
         year-editions PLUS a stray SNI2007/MDC) no longer blocks the collapse; and
         (2) the dominant family's label agreement is ≥ that of EVERY off-chain
         candidate (the label lever, #514) — a coincidental dominant chain can't beat
         a stray that actually matches labels (county labels match LKF, not SNI).
         If ≥2 distinct families are multi-vintage chains it is a genuine cross-family
         span → stays in the residue for curation. The OLD all-on-chain rule is the
         zero-stray special case (one family, all candidates). For each reclaimed
         (variable_id, value_set_id), pick the LATEST DOMINANT-family vintage whose
         [valid_from,valid_to] overlaps AT LEAST ONE of the pair's real state windows
         (per-state, NOT the aggregate MIN/MAX span — a disjoint-states span would
         falsely "overlap" a gap vintage) — the off-chain strays are never emitted —
         then emit it additively too.

    Returns counts (also logged) — value sets / variables auto-linked, the
    single-family-below-threshold and multi-family (ambiguous) populations, plus the
    vintage-reclaimed counts and the still-ambiguous residue after reclaim, so drift
    in the curated tail stays visible. No row-level content is logged.
    """
    _progress("Linking inline classification-coded value sets (#416)...")

    # Stem function for the #494 vintage-reclaim family guard (7b). Deterministic;
    # scoped to this connection. `classification_slug_stem` is the canonical
    # year-tail-stripping rule shared with `derive_classification_succession`.
    conn.create_function(
        "classification_stem", 1, classification_slug_stem, deterministic=True
    )

    for tmp in (
        "_vs_single",
        "_vs_confident",
        "_chain_root",
        "_vs_dominant_chain",
        "_vs_vintage",
        "_vs_vintage_emit",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    # Steps 1-3 (canonical codes, per-value-set stats, grain-filtered containment)
    # live in the shared `_build_containment_temp_tables` helper so the read-only
    # `dump_classification_residue` diagnostic computes `_vs_cls` identically. It
    # DROPs and recreates `_canon_codes`/`_vs_codes`/`_vs_stats`/`_vs_cls`; this
    # function owns the steps-4+ temp tables (dropped above and at the end).
    _build_containment_temp_tables(conn)

    # 4. Single-family value sets: EXACTLY one surviving candidate cls.
    conn.execute(
        """
        CREATE TEMP TABLE _vs_single AS
        SELECT value_set_id, MIN(cls_id) AS cls_id
        FROM _vs_cls
        GROUP BY value_set_id
        HAVING COUNT(*) = 1
        """
    )
    conn.execute("CREATE UNIQUE INDEX _vs_single_pk ON _vs_single(value_set_id)")

    # 5. Confident auto-link: single-family AND (n_codes >= _CONFIDENT_MIN_CODES OR
    # label_agree >= _CONFIDENT_LABEL_AGREE). label_agree comes from the shared
    # `_vs_label_agree` projection (built in `_build_containment_temp_tables`,
    # alongside the diagnostic's reader — the formula and its distinct-kod rationale
    # live at that table's definition). The single-family set has exactly one
    # `_vs_cls` row, so the join to `_vs_label_agree` is 1:1.
    conn.execute(
        f"""
        CREATE TEMP TABLE _vs_confident AS
        SELECT sg.value_set_id, sg.cls_id
        FROM _vs_single sg
        JOIN _vs_stats st ON st.value_set_id = sg.value_set_id
        JOIN _vs_label_agree la
          ON la.value_set_id = sg.value_set_id
         AND la.cls_id = sg.cls_id
        WHERE st.n_codes >= {_CONFIDENT_MIN_CODES}
           OR la.label_agree >= {_CONFIDENT_LABEL_AGREE}
        """
    )
    conn.execute("CREATE UNIQUE INDEX _vs_confident_pk ON _vs_confident(value_set_id)")

    # 6. Emit candidates additively: one INSERT...SELECT from variable_state joined
    # to the confident map, guarded by NOT EXISTS against an already-present
    # candidate for the same (variable_id, value_set_id) state key. `IS` is
    # NULL-safe; the join already constrains value_set_id non-NULL (a confident
    # value set has members), but the guard mirrors the backfill's IS-keying so the
    # additive contract holds for any state key.
    #
    # classification_candidate is an unindexed scratch table (by design), so the
    # NOT EXISTS guard would full-scan it per candidate row — O(N×M) at real-corpus
    # scale. Index the state key for the guard, then drop it after BOTH emits
    # (confident + vintage) — mirrors the temp-index lifecycle in
    # `_apply_valid_codes` / `populate_classifications`. The vintage emit (step 7)
    # also guards against this index, so it must outlive that INSERT.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS _cc_state_key "
        "ON classification_candidate(variable_id, value_set_id)"
    )
    conn.execute(
        """
        INSERT INTO classification_candidate (variable_id, value_set_id, classification_id)
        SELECT vs.variable_id, vs.value_set_id, cf.cls_id
        FROM variable_state vs
        JOIN _vs_confident cf ON cf.value_set_id = vs.value_set_id
        WHERE NOT EXISTS (
            SELECT 1 FROM classification_candidate c
            WHERE c.variable_id = vs.variable_id
              AND c.value_set_id IS vs.value_set_id
        )
        """
    )

    # 7. Vintage-period reclaim (#494 PART 1). The multi-family residue from step 3
    # is largely ONE classification family across vintages (SNI2002↔SNI2007 etc.),
    # distinct `classification` rows chained by `supersedes_id`. Collapse that
    # by-vintage ambiguity and auto-link; genuine cross-family coincidences stay in
    # the residue for curation.

    # 7a. Every classification's chain root, via a recursive CTE walking
    # `supersedes_id` UP from each chain root (supersedes_id IS NULL). A standalone
    # classification (no predecessor, no successor) is its own root. reg_meta_build
    # is maintainer-local (not MONA-runtime), so the recursive CTE is fine here.
    # Also carry the slug STEM (year-tail-stripped, via `classification_stem`) —
    # the chain root alone is NOT a fine-enough "same vintage family" key since the
    # #579 sun1996 → {niva, inriktning, grupp} curated split puts three ORTHOGONAL
    # SUN dimensions under ONE chain root; their slug stems (sun-niva / sun-inriktning
    # / sun-grupp) differ, so the stem disambiguates them (7b).
    conn.execute(
        """
        CREATE TEMP TABLE _chain_root AS
        WITH RECURSIVE chain(id, root) AS (
            SELECT id, id FROM classification WHERE supersedes_id IS NULL
            UNION ALL
            SELECT c.id, ch.root
            FROM classification c
            JOIN chain ch ON c.supersedes_id = ch.id
        )
        SELECT ch.id AS cls_id, ch.root, classification_stem(c.slug) AS stem
        FROM chain ch
        JOIN classification c ON c.id = ch.id
        """
    )
    conn.execute("CREATE UNIQUE INDEX _chain_root_pk ON _chain_root(cls_id)")

    # 7b. Dominant-chain gate (#514). Group each value set's candidate cls (those in
    # `_vs_cls` — i.e. whose codes the value set actually matches) into VINTAGE
    # FAMILIES keyed on (chain root AND slug stem). The chain root gates off-chain
    # cross-family coincidences (SNI vs SSYK); the stem gates cross-DIMENSION splits
    # that share a root but are NOT one vintage family — the #579 sun1996 split
    # chains sun-niva* / sun-inriktning* / sun-grupp* under one root, so their
    # differing stems keep two dimensions apart (a code-ambiguous label-less value
    # set spanning two of them must stay in the residue, not collapse to one).
    #
    # A family is a MULTI-VINTAGE CHAIN when the value set matches >=2 of its
    # editions (`n_matched >= 2`). We reclaim a value set when EXACTLY ONE family is
    # a multi-vintage chain — the DOMINANT chain — and carry that family's (root,
    # stem) so 7c restricts the latest-vintage pick to it.
    #
    # Permitted strays must be OFF-CHAIN — a DIFFERENT chain ROOT than the dominant
    # family. Any number of single-edition off-chain families pass: this is what
    # unblocks the large county/län (LKF) residue, where a county code set matches
    # >=2 LKF year-editions (one multi-vintage chain) plus STRAY off-chain
    # coincidences (the 2-digit codes also hit one SNI2007 division + one MDC, both a
    # DIFFERENT root than LKF) that the OLD all-on-chain rule treated as disqualifying
    # cross-family candidates. If >=2 distinct families are multi-vintage chains it is
    # a genuine cross-family span → not reclaimed.
    #
    # But a family that shares the dominant's chain root and differs only in slug STEM
    # is NOT an off-chain coincidence — it is a curated ORTHOGONAL dimension (the #579
    # sun1996 → {sun-niva, sun-inriktning, sun-grupp} split: three SUN dimensions under
    # one chain root, distinguished by stem). The final-SELECT `NOT EXISTS` guard
    # disqualifies the value set whenever ANY other family shares the dominant family's
    # root with a different stem, so the dominant family must be the UNIQUE family on
    # its root. This keeps orthogonal SUN dimensions apart even when one is ITSELF a
    # multi-vintage chain — a label-less set matching 2 sun-niva editions (dominant) +
    # 1 sun-inriktning edition stays ambiguous, where the label lever could not save it
    # (the COALESCE-0 path passes a label-less set on structure alone). The off-chain
    # strays (different root) are unaffected: LKF's root differs from SNI's and MDC's,
    # so no same-root family exists and the LKF case still passes, as does the OLD
    # all-on-chain case (one family, no others on its root).
    #
    # Label lever (#514, "structural + label-agreement lever now"): the dominant
    # family's best label_agree (`fam_max_la`, from the shared `_vs_label_agree`
    # projection) must be >= the best label_agree of every OFF-chain candidate, so a
    # coincidental dominant chain can't beat a stray that actually label-agrees
    # (county labels match LKF, not SNI). The `COALESCE(..., 0)` makes a value set
    # with NO off-chain stray, or a wholly label-less set (all label_agree 0), pass
    # on structure alone — preserving the OLD all-on-chain behaviour as the zero-stray
    # special case.
    #
    # The `rootless` guard defensively refuses any value set with a candidate that
    # resolved no chain root, so a hypothetical root-less classification can't slip a
    # value set through. (In a `--skip-slugs` build a slug-less classification has a
    # NULL stem while its root = its own cls id is non-NULL; such builds make the
    # whole reclaim inert anyway — 7c's `cr.stem = dom_stem` matches nothing when
    # dom_stem is NULL — and don't ship, so the NULL stem is harmless here.)
    # `dominant` guarantees exactly one multi-vintage family per value set, so
    # `multi m JOIN dominant d` yields exactly one (dom_root, dom_stem) row per value
    # set (asserted by the UNIQUE index).
    conn.execute(
        """
        CREATE TEMP TABLE _vs_dominant_chain AS
        WITH fam AS (                  -- per value set, per (root,stem): matched count + best label_agree
            SELECT vc.value_set_id, cr.root, cr.stem,
                   COUNT(*) AS n_matched,
                   MAX(la.label_agree) AS fam_max_la
            FROM _vs_cls vc
            JOIN _chain_root cr ON cr.cls_id = vc.cls_id
            JOIN _vs_label_agree la
              ON la.value_set_id = vc.value_set_id AND la.cls_id = vc.cls_id
            GROUP BY vc.value_set_id, cr.root, cr.stem
        ),
        rootless AS (                  -- DEFENSIVE: any candidate with no resolved chain root
            SELECT DISTINCT vc.value_set_id
            FROM _vs_cls vc
            LEFT JOIN _chain_root cr ON cr.cls_id = vc.cls_id
            WHERE cr.root IS NULL
        ),
        multi AS (                     -- multi-vintage chains: >=2 matched editions
            SELECT value_set_id, root, stem, fam_max_la FROM fam WHERE n_matched >= 2
        ),
        dominant AS (                  -- value sets with EXACTLY ONE such chain
            SELECT value_set_id FROM multi GROUP BY value_set_id HAVING COUNT(*) = 1
        )
        SELECT m.value_set_id, m.root AS dom_root, m.stem AS dom_stem
        FROM multi m
        JOIN dominant d ON d.value_set_id = m.value_set_id
        WHERE m.value_set_id NOT IN (SELECT value_set_id FROM rootless)
          -- off-chain only: the dominant must be the UNIQUE family on its root, so a
          -- same-root different-stem orthogonal dimension (#579) keeps the set ambiguous
          AND NOT EXISTS (
                SELECT 1 FROM fam f3
                WHERE f3.value_set_id = m.value_set_id
                  AND f3.root = m.root
                  AND f3.stem <> m.stem
          )
          AND m.fam_max_la >= COALESCE(   -- label lever: dominant >= every off-chain candidate
                (SELECT MAX(f2.fam_max_la) FROM fam f2
                 WHERE f2.value_set_id = m.value_set_id
                   AND (f2.root <> m.root OR f2.stem <> m.stem)),
                0)
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX _vs_dominant_chain_pk ON _vs_dominant_chain(value_set_id)"
    )

    # 7c. For each (variable_id, value_set_id) pair of those value sets, pick the
    # LATEST candidate vintage that overlaps AT LEAST ONE of the pair's REAL state
    # windows (max valid_from, deterministic tie-break by max id).
    #
    # Overlap is per-real-state, NOT against the pair's aggregate MIN/MAX span — the
    # aggregate span is a TRAP when a pair has DISJOINT states (e.g. 2003–2006 and
    # 2018–2020): the span collapses to a continuous [2003, 2020], so a candidate
    # vintage sitting in the GAP (a closed 2008–2015 edition) would "overlap" the
    # span though NO actual state touches it — the emit would then tag the variable
    # with a vintage none of its states fall in. Anchoring overlap to a single real
    # state window closes the gap trap; the "latest overlapping" intent is unchanged.
    #
    # We still emit ONE row per (variable_id, value_set_id) BECAUSE the emit grain is
    # the pair: `_backfill_state_classifications` folds candidates to
    # min(classification_id) per (variable_id, value_set_id) and applies ONE
    # classification to ALL that pair's states (it is NOT per-state-period). So we
    # resolve to one vintage per pair — the latest among those overlapping a real
    # state — and emit exactly one row; do not "fix" this to per-state.
    #
    # JOIN `variable_state vs` (restricted to the dominant-chain value sets) to
    # `_vs_cls vc` (the candidate vintages the value set actually matches), then to
    # `_chain_root cr` filtered to the DOMINANT family (cr.root = dom_root AND
    # cr.stem = dom_stem) so ONLY the dominant chain's editions are eligible — the
    # off-chain strays (#514) are never emitted — to `classification cl`, keeping
    # rows where cl overlaps THAT state's year range. vs.valid_from /
    # vs.valid_to are TEXT NOT NULL 'YYYY-MM-DD' (open-ended uses the '9999-12-31'
    # sentinel → year 9999); extract the year with substr+CAST.
    # classification.valid_from/valid_to are INTEGER years, NULLABLE (NULL =
    # unbounded on that side); per-state overlap of [year(vs.valid_from),
    # year(vs.valid_to)] with cls [c_from, c_to] is
    # (c_from IS NULL OR c_from <= year(vs.valid_to))
    #   AND (c_to IS NULL OR c_to >= year(vs.valid_from)).
    # DISTINCT collapses the same (variable_id, value_set_id, cls_id) reached via
    # several overlapping states; ROW_NUMBER then picks rank 1 (latest) per pair. If
    # NO candidate vintage overlaps any real state, the pair emits nothing — it stays
    # in the residue (safe by omission).
    conn.execute(
        """
        CREATE TEMP TABLE _vs_vintage AS
        SELECT variable_id, value_set_id, cls_id
        FROM (
            SELECT
                pc.variable_id,
                pc.value_set_id,
                pc.cls_id,
                ROW_NUMBER() OVER (
                    PARTITION BY pc.variable_id, pc.value_set_id
                    ORDER BY pc.valid_from DESC, pc.cls_id DESC
                ) AS rn
            FROM (
                SELECT DISTINCT
                    vs.variable_id,
                    vs.value_set_id,
                    cl.id AS cls_id,
                    cl.valid_from AS valid_from
                FROM variable_state vs
                JOIN _vs_dominant_chain mo ON mo.value_set_id = vs.value_set_id
                JOIN _vs_cls vc ON vc.value_set_id = vs.value_set_id
                JOIN _chain_root cr
                  ON cr.cls_id = vc.cls_id
                 AND cr.root = mo.dom_root
                 AND cr.stem = mo.dom_stem
                JOIN classification cl ON cl.id = vc.cls_id
                WHERE (
                        cl.valid_from IS NULL
                        OR cl.valid_from <= CAST(substr(vs.valid_to, 1, 4) AS INTEGER)
                      )
                  AND (
                        cl.valid_to IS NULL
                        OR cl.valid_to >= CAST(substr(vs.valid_from, 1, 4) AS INTEGER)
                      )
            ) pc
        )
        WHERE rn = 1
        """
    )

    # 7e. Materialize the post-guard emit set BEFORE emitting, so both the INSERT
    # and the reclaim counts reflect what is ACTUALLY emitted (#494 Codex P2). The
    # NOT EXISTS guard is the same the confident emit uses — curated/feed/SCB/SOS
    # candidates (and the confident emit above) always win; this only fills gaps.
    # Critically, evaluate the guard against the PRE-vintage state (feeds + curated +
    # confident emit), so a vintage pick already claimed by another candidate is
    # excluded here — and therefore NOT counted as reclaimed below. The
    # `value_set_id IS ...` predicate is NULL-safe; the `_cc_state_key` index
    # (created before step 6) is still live, which keeps the NOT EXISTS cheap, so it
    # MUST be materialized before the `DROP INDEX` below. Confident and multi-family
    # sets are disjoint in `_vs_cls`, so the two emits never target the same value set.
    conn.execute(
        """
        CREATE TEMP TABLE _vs_vintage_emit AS
        SELECT vv.variable_id, vv.value_set_id, vv.cls_id
        FROM _vs_vintage vv
        WHERE NOT EXISTS (
            SELECT 1 FROM classification_candidate c
            WHERE c.variable_id = vv.variable_id
              AND c.value_set_id IS vv.value_set_id
        )
        """
    )
    conn.execute(
        """
        INSERT INTO classification_candidate (variable_id, value_set_id, classification_id)
        SELECT variable_id, value_set_id, cls_id
        FROM _vs_vintage_emit
        """
    )

    # The TRUE post-vintage residue (#494 Codex P2): the count of DISTINCT
    # multi-family value sets that STILL have at least one (variable_id,
    # value_set_id) state with NO `classification_candidate` row after the vintage
    # emit. The naive `multi_family - vintage_value_sets_linked` is WRONG when a
    # multi-family value_set_id is reused across variables — reclaimed for one
    # (variable_id, value_set_id) pair but not another — because subtracting distinct
    # emitted value sets removes it from the residue though unresolved pairs remain.
    # Grain-consistent: a value set counts as residual iff EXISTS a (variable_id,
    # value_set_id) state lacking a candidate. Computed BEFORE dropping
    # `_cc_state_key`, whose index keeps the per-state EXISTS cheap at real scale.
    multi_family_after = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT mc.value_set_id
            FROM (
                SELECT value_set_id FROM _vs_cls GROUP BY value_set_id HAVING COUNT(*) > 1
            ) mc
            JOIN variable_state vs ON vs.value_set_id = mc.value_set_id
            WHERE NOT EXISTS (
                SELECT 1 FROM classification_candidate c
                WHERE c.variable_id = vs.variable_id
                  AND c.value_set_id IS vs.value_set_id
            )
            GROUP BY mc.value_set_id
        )
        """
    ).fetchone()[0]
    conn.execute("DROP INDEX IF EXISTS _cc_state_key")

    # Counts for the report (no row-level content). value_sets/variables linked
    # are measured off the confident map joined to variable_state, so they reflect
    # what was actually emitted. The two unresolved tallies size the curated tail:
    # single-family-but-below-threshold and multi-family (ambiguous).
    value_sets_linked = conn.execute("SELECT COUNT(*) FROM _vs_confident").fetchone()[0]
    variables_linked = conn.execute(
        "SELECT COUNT(DISTINCT vs.variable_id) "
        "FROM variable_state vs "
        "JOIN _vs_confident cf ON cf.value_set_id = vs.value_set_id"
    ).fetchone()[0]
    single_below_threshold = conn.execute(
        "SELECT COUNT(*) FROM _vs_single sg "
        "WHERE NOT EXISTS (SELECT 1 FROM _vs_confident cf "
        "WHERE cf.value_set_id = sg.value_set_id)"
    ).fetchone()[0]
    multi_family = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT value_set_id FROM _vs_cls GROUP BY value_set_id HAVING COUNT(*) > 1"
        ")"
    ).fetchone()[0]

    # Vintage-reclaim counts (#494): distinct value sets / variables resolved by
    # step 7 (genuinely-new links). Counted off `_vs_vintage_emit` (the post-guard
    # set), NOT `_vs_vintage` (Codex P2): a vintage pick already claimed by a
    # feed/curated/confident candidate is skipped by the emit guard, so it is NOT
    # counted as reclaimed and stays in the residue. `multi_family` keeps its
    # pre-reclaim meaning (total multi-family BEFORE reclaim); `multi_family_after`
    # (computed above, before the index drop) is the precise post-vintage residue.
    vintage_value_sets_linked = conn.execute(
        "SELECT COUNT(DISTINCT value_set_id) FROM _vs_vintage_emit"
    ).fetchone()[0]
    vintage_variables_linked = conn.execute(
        "SELECT COUNT(DISTINCT variable_id) FROM _vs_vintage_emit"
    ).fetchone()[0]

    for tmp in (
        "_vs_vintage_emit",
        "_vs_vintage",
        "_vs_dominant_chain",
        "_chain_root",
        "_vs_confident",
        "_vs_single",
        "_vs_label_agree",
        "_vs_cls",
        "_vs_stats",
        "_vs_codes",
        "_canon_codes",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    _progress(
        f"  {value_sets_linked:,} value sets / {variables_linked:,} variables "
        f"auto-linked by code-set containment "
        f"({single_below_threshold:,} single-family below threshold, "
        f"{multi_family:,} multi-family ambiguous → curation)"
    )
    _progress(
        f"  {vintage_value_sets_linked:,} value sets / "
        f"{vintage_variables_linked:,} variables reclaimed by vintage-period "
        f"({multi_family_after:,} multi-family still ambiguous → curation)"
    )
    return {
        "value_sets_linked": value_sets_linked,
        "variables_linked": variables_linked,
        "single_below_threshold": single_below_threshold,
        "multi_family": multi_family,
        "vintage_value_sets_linked": vintage_value_sets_linked,
        "vintage_variables_linked": vintage_variables_linked,
        "multi_family_after": multi_family_after,
    }


# --- classification-linkage residue diagnostic (#513) ------------------------
# Productizes the multi-family residue `link_value_set_classifications` leaves for
# curation (the #494 throwaway recompute, made reusable). It recomputes the SHARED
# `_vs_cls` containment read-only (NEVER mutates the DB) and joins it to the SHIPPED
# `variable_state.classification_id IS NULL` final-fold signal — `classification_
# candidate` is build-scratch DROPPED before ship, so on a built DB the "still
# unclassified" fact is the NULL state, not a scratch-table miss.

# Label-unambiguity gate for the SAFE subset: a residual value set is curatable
# when EXACTLY ONE of its candidate classifications is a STANDALONE (no
# supersedes-chain neighbour) whose exact-(kod,label) agreement is >= this floor,
# AND every other candidate is < this floor. Mirrors `_CONFIDENT_LABEL_AGREE`
# (the detector's own label-precision lever) — a label-unambiguous standalone is
# exactly the #494-part-2 tier a maintainer copied verbatim into
# classification_links.toml.
_SAFE_LABEL_AGREE = _CONFIDENT_LABEL_AGREE


@dataclass(frozen=True)
class ResidueCandidate:
    """One candidate classification for a residual value set: its `short_name`,
    containment (matched / n_codes), label_agree (the exact-(kod,label) agreement
    step 5 uses), and whether it is STANDALONE (no `supersedes_id` chain neighbour
    — not part of a vintage succession). Evidence for the worklist comment."""

    short_name: str
    containment: float
    label_agree: float
    standalone: bool


@dataclass(frozen=True)
class ResidualState:
    """One unclassified `(variable_id, value_set_id)` state on a residual value
    set, addressed by its variable FQID + name so a maintainer can act on it."""

    variable_id: int
    fqid: str  # provider/register/variable, or None-slug → empty segments
    name: str | None


@dataclass(frozen=True)
class ResidueValueSet:
    """A residual (multi-family, still-unclassified) value set: its `n_codes`, the
    distinct unclassified states, the candidate classifications with evidence, and
    `safe_target` — the single label-unambiguous standalone candidate (the one
    >= _SAFE_LABEL_AGREE while all others are below it) when the set is curatable,
    else None. A safe value set is exactly one with a non-None `safe_target`; `safe`
    is that boolean, so the qualifying-candidate predicate lives in ONE place
    (`dump_classification_residue`) and the renderer reads the resolved target rather
    than re-deriving it. The safe set is the curatable tier (the #494-part-2 shape)."""

    value_set_id: int
    n_codes: int
    candidates: tuple[ResidueCandidate, ...]
    states: tuple[ResidualState, ...]
    safe_target: ResidueCandidate | None

    @property
    def safe(self) -> bool:
        """True iff this value set has a single label-unambiguous standalone
        candidate (the curatable tier). Derived from `safe_target`."""
        return self.safe_target is not None


@dataclass(frozen=True)
class ResidueResult:
    """The diagnostic's output: every residual value set plus the headline counts
    (total residue value sets, the safe-subset size). Read-only — nothing is
    materialized.

    `mixed_state_variable_ids` is the P2 copyability gate: a curated `[[link]]` is
    VARIABLE-grain — `materialize_classification_links` applies the chosen
    classification to EVERY value-set state of the variable (DELETE-then-INSERT per
    state key), not just the safe one. So a variable that has a safe value set but
    ALSO another state that would be wrongly reclassified variable-wide (a state on
    an ambiguous/non-safe value set, or one already classified to a DIFFERENT
    classification) must NOT be emitted as a bare copyable link. This set holds the
    variable_ids of exactly those non-conflict safe variables whose variable-wide
    link is NOT provably safe; the renderer routes them to the comment-only section
    flagged `# MIXED-STATE` instead of emitting a copyable `[[link]]`. Conflict
    variables (safe value sets resolving to different classifications) are handled
    separately by the renderer and are NOT included here."""

    value_sets: tuple[ResidueValueSet, ...]
    total: int
    safe_count: int
    mixed_state_variable_ids: frozenset[int] = frozenset()


def dump_classification_residue(conn: sqlite3.Connection) -> ResidueResult:
    """Emit the #416 classification-linkage RESIDUE worklist from a BUILT DB
    (read-only — NEVER mutates).

    A value set is RESIDUAL iff it is MULTI-FAMILY (>1 candidate classification in
    the shared `_vs_cls` containment) AND >= 1 of its `variable_state` rows is still
    unclassified (`classification_id IS NULL`). On a shipped DB that NULL is the
    final folded signal — `classification_candidate` (the build-scratch table the
    detector feeds) is DROPPED before ship, so it cannot be the read-side signal.

    For each residual value set it gathers `n_codes`, the distinct unclassified
    `(variable_id, value_set_id)` states (variable FQID + name), and the candidate
    classifications — per candidate the `containment`, the exact-(kod,label)
    `label_agree` (the SAME metric step 5 uses for the confident tier), and whether
    the candidate is STANDALONE (no `supersedes_id` chain neighbour). The SAFE
    subset (`safe=True`) is the curatable tier: EXACTLY ONE candidate is a standalone
    with label_agree >= `_SAFE_LABEL_AGREE` and every other candidate is below it —
    the #494-part-2 label-unambiguous-single-standalone shape.

    Builds (and drops) the shared `_vs_cls` containment via
    `_build_containment_temp_tables`, so it stays byte-consistent with the detector.
    No row-level sensitive content beyond codes/labels/FQIDs (the same exposure the
    detector already logs)."""
    _build_containment_temp_tables(conn)
    try:
        # Multi-family value sets (>1 candidate cls) that still have >= 1
        # unclassified state. `variable_state.classification_id IS NULL` is the
        # shipped final-fold signal; restrict to value-set-bearing states (a
        # code-less NULL-value_set state can't carry a classification link).
        multi_unclassified = {
            row[0]
            for row in conn.execute(
                """
                SELECT mc.value_set_id
                FROM (
                    SELECT value_set_id FROM _vs_cls
                    GROUP BY value_set_id HAVING COUNT(*) > 1
                ) mc
                WHERE EXISTS (
                    SELECT 1 FROM variable_state vs
                    WHERE vs.value_set_id = mc.value_set_id
                      AND vs.classification_id IS NULL
                )
                """
            )
        }

        if not multi_unclassified:
            return ResidueResult(value_sets=(), total=0, safe_count=0)

        # Materialize the residue value_set_id set so the candidate and state queries
        # can be RESTRICTED to it in SQL (a JOIN), instead of fetching every `_vs_cls`
        # candidate row / every unclassified `variable_state` across the whole DB and
        # discarding non-residue ones in Python. Results are identical; the full-table
        # fetch is avoided. Maintainer diagnostic, not a hot path — a plain temp table
        # + JOIN is enough.
        conn.execute("DROP TABLE IF EXISTS _residue_vs")
        conn.execute(
            "CREATE TEMP TABLE _residue_vs (value_set_id INTEGER PRIMARY KEY) "
            "WITHOUT ROWID"
        )
        conn.executemany(
            "INSERT INTO _residue_vs (value_set_id) VALUES (?)",
            [(vs_id,) for vs_id in multi_unclassified],
        )

        # Candidate classifications per residual value set, with containment,
        # standalone flag (no supersedes_id chain neighbour — neither a predecessor
        # nor a successor), and the exact-(kod,label) label_agree (step 5's metric:
        # COUNT(DISTINCT v.kod matching a canonical (kod,label) of THIS cls) /
        # n_codes), read from the shared `_vs_label_agree` projection built in
        # `_build_containment_temp_tables` (same formula step 5 uses — #738).
        # `standalone` = supersedes_id IS NULL (no predecessor) AND no
        # other classification supersedes it (no successor). `vc.n_codes` is the
        # per-value-set distinct-code count (constant across a value set's candidate
        # rows — it comes from `_vs_stats`), so we capture it once per value set here
        # rather than running a separate MAX(n_codes) GROUP BY query.
        cand_rows = conn.execute(
            """
            SELECT
                vc.value_set_id,
                cl.id AS cls_id,
                cl.short_name,
                vc.containment,
                vc.n_codes,
                (
                    cl.supersedes_id IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM classification s WHERE s.supersedes_id = cl.id
                    )
                ) AS standalone,
                la.label_agree AS label_agree
            FROM _vs_cls vc
            JOIN _residue_vs rv ON rv.value_set_id = vc.value_set_id
            JOIN _vs_label_agree la
              ON la.value_set_id = vc.value_set_id
             AND la.cls_id = vc.cls_id
            JOIN classification cl ON cl.id = vc.cls_id
            ORDER BY vc.value_set_id, cl.short_name
            """
        ).fetchall()
        candidates_by_vs: dict[int, list[ResidueCandidate]] = {}
        # cls_id per candidate, index-aligned with `candidates_by_vs`, so the safe
        # target's classification_id can be recovered (the P2 variable-wide gate
        # compares it against a variable's other states' classification_id). Kept
        # parallel rather than on `ResidueCandidate` so the public dataclass — and
        # the worklist comment it feeds — stays short_name-only.
        cand_cls_ids_by_vs: dict[int, list[int]] = {}
        # n_codes is constant across a value set's candidate rows; first row wins.
        n_codes_by_vs: dict[int, int] = {}
        for (
            value_set_id,
            cls_id,
            short_name,
            containment,
            n_codes,
            standalone,
            label_agree,
        ) in cand_rows:
            candidates_by_vs.setdefault(value_set_id, []).append(
                ResidueCandidate(
                    short_name=short_name,
                    containment=float(containment),
                    label_agree=float(label_agree),
                    standalone=bool(standalone),
                )
            )
            cand_cls_ids_by_vs.setdefault(value_set_id, []).append(int(cls_id))
            n_codes_by_vs.setdefault(value_set_id, int(n_codes))

        # Unclassified states per residual value set: the distinct (variable_id,
        # value_set_id) state keys with classification_id NULL, joined to the
        # variable FQID + name. A NULL slug segment (a partial/--skip-slugs build)
        # renders as an empty segment — the FQID is still informative.
        state_rows = conn.execute(
            """
            SELECT DISTINCT
                vs.value_set_id,
                vs.variable_id,
                p.slug AS provider_slug,
                r.slug AS register_slug,
                v.slug AS variable_slug,
                v.name AS name
            FROM variable_state vs
            JOIN _residue_vs rv ON rv.value_set_id = vs.value_set_id
            JOIN variable v ON v.variable_id = vs.variable_id
            JOIN register r ON r.register_id = v.register_id
            JOIN provider p ON p.provider_id = r.provider_id
            WHERE vs.classification_id IS NULL
            ORDER BY vs.value_set_id, vs.variable_id
            """
        ).fetchall()
        states_by_vs: dict[int, list[ResidualState]] = {}
        for value_set_id, variable_id, p_slug, r_slug, v_slug, name in state_rows:
            fqid = f"{p_slug or ''}/{r_slug or ''}/{v_slug or ''}"
            states_by_vs.setdefault(value_set_id, []).append(
                ResidualState(variable_id=variable_id, fqid=fqid, name=name)
            )

        value_sets: list[ResidueValueSet] = []
        # safe value set → its safe-target classification_id, for the P2 gate below.
        safe_target_cls_id_by_vs: dict[int, int] = {}
        for value_set_id in sorted(multi_unclassified):
            candidates = tuple(candidates_by_vs.get(value_set_id, ()))
            states = tuple(states_by_vs.get(value_set_id, ()))
            # SAFE: exactly one standalone candidate clears the label floor and
            # every OTHER candidate is below it (label-unambiguous single standalone).
            # Key the partition by candidate INDEX, not value-equality membership, so
            # two candidates with coincidentally-equal fields can't conflate (a
            # `c not in qualifying` test on frozen dataclasses dedups by value). This
            # is the SOLE site of the qualifying-candidate predicate: when safe, the
            # one qualifying candidate is stored as `safe_target` and the renderer
            # reads it back rather than re-deriving the predicate.
            qualifying = [
                i
                for i, c in enumerate(candidates)
                if c.standalone and c.label_agree >= _SAFE_LABEL_AGREE
            ]
            others_below = all(
                c.label_agree < _SAFE_LABEL_AGREE
                for i, c in enumerate(candidates)
                if i not in qualifying
            )
            safe = len(qualifying) == 1 and others_below
            safe_target = candidates[qualifying[0]] if safe else None
            if safe:
                safe_target_cls_id_by_vs[value_set_id] = cand_cls_ids_by_vs[
                    value_set_id
                ][qualifying[0]]
            value_sets.append(
                ResidueValueSet(
                    value_set_id=value_set_id,
                    n_codes=int(n_codes_by_vs.get(value_set_id, 0)),
                    candidates=candidates,
                    states=states,
                    safe_target=safe_target,
                )
            )

        mixed_state_variable_ids = _mixed_state_variable_ids(
            conn, value_sets, safe_target_cls_id_by_vs
        )
    finally:
        for tmp in (
            "_residue_vs",
            "_vs_label_agree",
            "_vs_cls",
            "_vs_stats",
            "_vs_codes",
            "_canon_codes",
        ):
            conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    safe_count = sum(1 for vs in value_sets if vs.safe)
    _progress(
        f"  {len(value_sets):,} residual value sets "
        f"({safe_count:,} safe-subset: single label-unambiguous standalone)"
    )
    return ResidueResult(
        value_sets=tuple(value_sets),
        total=len(value_sets),
        safe_count=safe_count,
        mixed_state_variable_ids=mixed_state_variable_ids,
    )


def _mixed_state_variable_ids(
    conn: sqlite3.Connection,
    value_sets: list[ResidueValueSet],
    safe_target_cls_id_by_vs: dict[int, int],
) -> frozenset[int]:
    """P2: the variable_ids whose safe `[[link]]` is NOT provably safe variable-wide.

    A curated `[[link]]` is VARIABLE-grain: `materialize_classification_links`
    applies the chosen classification to EVERY non-NULL value-set state of the
    variable (DELETE-then-INSERT per state key). So a copyable link is only correct
    when applying it variable-wide reclassifies nothing wrongly. We gather, per
    variable that surfaces as an unclassified state on a SAFE value set, its FULL
    distinct `(value_set_id, classification_id)` state set (not just the residual
    ones) and require, for the SINGLE safe target `T` it resolves to:

      - every state already classified is classified to `T` (no DIFFERENT class), and
      - every UNCLASSIFIED state sits on a safe value set whose safe-target is `T`
        (no unproven family on an ambiguous/non-safe/non-residual value set).

    A variable whose safe value sets resolve to MORE THAN ONE target is a CONFLICT,
    handled by the renderer; it is excluded here (returns it un-flagged so the
    renderer's conflict path owns it). Read-only — only SELECTs."""
    # Per variable: the set of safe targets it resolves to (across the safe value
    # sets it is an unclassified state on), and the safe value sets themselves.
    safe_targets_by_var: dict[int, set[int]] = {}
    safe_vs_by_var: dict[int, set[int]] = {}
    for vs in value_sets:
        if not vs.safe:
            continue
        target = safe_target_cls_id_by_vs[vs.value_set_id]
        for st in vs.states:
            safe_targets_by_var.setdefault(st.variable_id, set()).add(target)
            safe_vs_by_var.setdefault(st.variable_id, set()).add(vs.value_set_id)

    mixed: set[int] = set()
    for variable_id, targets in safe_targets_by_var.items():
        # Conflict (>1 safe target) is the renderer's path, not a mixed-state flag.
        if len(targets) != 1:
            continue
        (target,) = tuple(targets)
        safe_vs = safe_vs_by_var[variable_id]
        # The variable's FULL distinct state set (value-set-bearing states only — a
        # NULL-value_set state can't carry a classification link and is never touched
        # by materialize). One row per (value_set_id, classification_id).
        full_states = conn.execute(
            "SELECT DISTINCT value_set_id, classification_id FROM variable_state "
            "WHERE variable_id = ? AND value_set_id IS NOT NULL",
            (variable_id,),
        ).fetchall()
        for value_set_id, classification_id in full_states:
            if classification_id is not None:
                # Already classified: safe only if to the SAME target T.
                if classification_id != target:
                    mixed.add(variable_id)
                    break
            elif value_set_id not in safe_vs:
                # Unclassified state on a value set that is NOT a safe target for T
                # (ambiguous residue, non-residual, or a different safe set): its
                # family is unproven, so a variable-wide link would mis-tag it.
                mixed.add(variable_id)
                break
    return frozenset(mixed)


def render_residue_toml(result: ResidueResult) -> str:
    """Render the residue worklist as a `[[link]]` TOML string a maintainer curates
    from — the exact shape `classification_links.toml` accepts, so a CONFIRMED
    candidate copies across verbatim. Built by hand (not `tomli_w`) so the
    per-value-set evidence `#` comments survive (`tomli_w` drops comments); every
    emitted string value (`variable`, `classification`, `note`) and every value
    interpolated into a `#` comment goes through the shared `_toml_str` /
    `_toml_comment` leaves (`fqid_slugs.py`) for round-trip safety, the same as
    `concept_group_candidates.render_candidates_toml`.

    The SAFE subset (single label-unambiguous standalone candidate) is emitted FIRST
    and clearly marked — that's the curatable tier (#494 part 2). It is emitted as
    one `[[link]]` per DISTINCT variable FQID (NOT per state): the curated loader
    `load_classification_links` rejects a duplicate `variable`, and one variable can
    surface as an unclassified state on several safe value sets, so a per-state emit
    would produce duplicate `variable` blocks that fail to load verbatim. When a
    variable's safe value sets all resolve to the SAME classification it gets ONE
    block; when they resolve to DIFFERENT classifications that is a genuine conflict —
    it is NOT emitted as a copyable link but routed to the ambiguous section as a
    comment-flagged conflict for human resolution. Each `[[link]]` carries the
    standalone candidate's `classification` and a `note = "residue:safe"` provenance
    marker; the preceding `#` comment shows n_codes and every candidate's containment
    / label_agree / standalone so a reviewer sees the full evidence.

    A copyable `[[link]]` is VARIABLE-grain (it reclassifies EVERY value-set state of
    the variable), so it is held back to comment-only — never uncommented — in two
    further cases, since copying it verbatim would either mis-tag other states or fail
    to load:

    - MIXED-STATE (P2): the variable has a safe value set but ALSO a state that a
      variable-wide link would wrongly reclassify (an ambiguous/non-safe state, or one
      already classified to a different class). `result.mixed_state_variable_ids` flags
      these; curate the scoping by hand.
    - UNSLUGGED (P3): a NULL slug segment (a `--skip-slugs` / partial build) makes the
      FQID carry an empty segment (e.g. `scb//var`), which `load_classification_links`
      rejects — so the advertised copyable worklist would not load.

    The AMBIGUOUS residue (no single safe standalone, plus the safe-but-conflicting /
    mixed-state / unslugged variables) is comment-only — NOTHING uncommented to copy,
    since its true family or scoping needs a human call. NOTHING materializes —
    read-only worklist only."""
    safe = [vs for vs in result.value_sets if vs.safe]
    ambiguous = [vs for vs in result.value_sets if not vs.safe]

    # Group the safe states by variable FQID so each FQID emits at most one
    # `[[link]]` — `load_classification_links` rejects a duplicate `variable`, and a
    # variable can be an unclassified state on several safe value sets. Carry the
    # variable name (first seen) for the evidence comment, and the variable_ids behind
    # each FQID (for the P2 mixed-state gate). A variable whose safe value sets resolve
    # to MORE THAN ONE classification is a genuine conflict: it is pulled out of the
    # copyable set and reported as a comment-flagged conflict below.
    safe_targets_by_fqid: dict[str, set[str]] = {}
    safe_name_by_fqid: dict[str, str | None] = {}
    var_ids_by_fqid: dict[str, set[int]] = {}
    for vs in safe:
        # vs.safe is True here, so safe_target is the stored qualifying candidate.
        assert vs.safe_target is not None
        for st in vs.states:
            safe_targets_by_fqid.setdefault(st.fqid, set()).add(
                vs.safe_target.short_name
            )
            safe_name_by_fqid.setdefault(st.fqid, st.name)
            var_ids_by_fqid.setdefault(st.fqid, set()).add(st.variable_id)

    def _unslugged(fqid: str) -> bool:
        # A NULL slug segment renders as an empty piece (e.g. `scb//var`); such an
        # FQID is rejected by `load_classification_links`, so it can't be copyable.
        return any(seg == "" for seg in fqid.split("/"))

    def _mixed(fqid: str) -> bool:
        # P2: a copyable link is VARIABLE-grain; hold it back if ANY variable behind
        # the FQID has a state a variable-wide link would wrongly reclassify.
        return bool(var_ids_by_fqid.get(fqid, set()) & result.mixed_state_variable_ids)

    conflicts = {
        fqid: sorted(targets)
        for fqid, targets in safe_targets_by_fqid.items()
        if len(targets) > 1
    }
    # Copyable: single safe target, not a conflict, provably safe variable-wide (P2),
    # and fully slugged (P3). The held-back single-target variables (mixed / unslugged)
    # are flagged comment-only below.
    safe_links = {
        fqid: next(iter(targets))
        for fqid, targets in safe_targets_by_fqid.items()
        if len(targets) == 1 and not _mixed(fqid) and not _unslugged(fqid)
    }
    held_back = sorted(
        fqid
        for fqid, targets in safe_targets_by_fqid.items()
        if len(targets) == 1 and (_mixed(fqid) or _unslugged(fqid))
    )

    lines = [
        "# GENERATED classification-linkage residue worklist — "
        "reg-meta-build classification-residue.",
        "#",
        "# The #416 code-set-containment detector auto-links the confident tier and",
        "# vintage-reclaims one-family residue; what remains is MULTI-FAMILY value",
        "# sets with >= 1 still-unclassified (classification_id IS NULL) state. These",
        "# are INFERRED candidates, NOT confirmed links. NOTHING here loads into a",
        "# build — review each and copy ONLY confirmed links into",
        "# reg_meta_build/classification_links.toml (drop/replace the residue note).",
        "#",
        f"# {result.total} residual value set(s); "
        f"{result.safe_count} safe-subset (single label-unambiguous standalone).",
        "#",
        "# label_agree = exact (code,label) agreement vs the candidate's canonical",
        "# pairs (the detector's step-5 metric); standalone = not on a supersedes",
        "# vintage chain. SAFE = exactly one standalone candidate >= "
        f"{_SAFE_LABEL_AGREE:.2f} label_agree, all others below.",
    ]

    def _evidence_comment(vs: ResidueValueSet) -> list[str]:
        out = [
            "",
            f"# value_set {vs.value_set_id}: n_codes={vs.n_codes}, "
            f"{len(vs.states)} unclassified state(s)",
        ]
        for c in vs.candidates:
            out.append(
                f"#   candidate {_toml_comment(c.short_name)}: "
                f"containment={c.containment:.2f}, "
                f"label_agree={c.label_agree:.2f}, "
                f"standalone={'yes' if c.standalone else 'no'}"
            )
        for st in vs.states:
            name = f" ({_toml_comment(st.name)})" if st.name else ""
            out.append(
                f"#   state: variable = {_toml_comment(st.fqid)}{name} "
                f"(variable_id {st.variable_id})"
            )
        return out

    lines.append("")
    lines.append("# === SAFE subset (curatable: copy the [[link]] blocks below) ===")
    for vs in safe:
        lines.extend(_evidence_comment(vs))
    if not safe_links:
        lines.append("")
        lines.append("# (no copyable links)")
    # One [[link]] per distinct variable FQID (deduped above) so the block loads
    # verbatim. Sorted for deterministic output.
    for fqid in sorted(safe_links):
        name = safe_name_by_fqid.get(fqid)
        lines.append("")
        if name:
            lines.append(f"# {_toml_comment(name)}")
        lines.append("[[link]]")
        lines.append(f"variable = {_toml_str(fqid)}")
        lines.append(f"classification = {_toml_str(safe_links[fqid])}")
        lines.append(f"note = {_toml_str('residue:safe')}")

    lines.append("")
    lines.append(
        "# === AMBIGUOUS residue (evidence only — a human must pick the family) ==="
    )
    if not ambiguous and not conflicts and not held_back:
        lines.append("# (none)")
    # Variables whose safe value sets resolve to MORE THAN ONE classification: a
    # genuine conflict, not copyable. Flagged for human resolution.
    for fqid in sorted(conflicts):
        name = safe_name_by_fqid.get(fqid)
        suffix = f" ({_toml_comment(name)})" if name else ""
        lines.append("")
        lines.append(
            f"# CONFLICT: variable {_toml_comment(fqid)}{suffix} maps to multiple "
            f"safe classifications {conflicts[fqid]} across its value sets — "
            "a human must pick one."
        )
    # Single-target safe variables held back from the copyable set: a variable-wide
    # link would mis-tag another state (MIXED-STATE, P2) or the FQID is unslugged so
    # it can't load (UNSLUGGED, P3). Flag the target as a comment so a maintainer can
    # scope it by hand; never uncommented.
    for fqid in held_back:
        name = safe_name_by_fqid.get(fqid)
        suffix = f" ({_toml_comment(name)})" if name else ""
        target = next(iter(safe_targets_by_fqid[fqid]))
        flag = "UNSLUGGED" if _unslugged(fqid) else "MIXED-STATE"
        lines.append("")
        if flag == "UNSLUGGED":
            lines.append(
                f"# UNSLUGGED: variable {_toml_comment(fqid)}{suffix} has a safe value "
                f"set (target {_toml_comment(target)}) but a NULL slug segment — a "
                "partial/--skip-slugs build; rebuild with slugs, then curate manually."
            )
        else:
            lines.append(
                f"# MIXED-STATE: variable {_toml_comment(fqid)}{suffix} has a safe value "
                f"set (target {_toml_comment(target)}) but other states need scoping — "
                "a variable-wide link would mis-tag them; curate manually."
            )
    for vs in ambiguous:
        lines.extend(_evidence_comment(vs))

    return "\n".join(lines) + "\n"
