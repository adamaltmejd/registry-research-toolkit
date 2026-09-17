"""Classification input validation and read-only curation worklists.

Canonical membership and state bindings resolve in the common pipeline. The
containment worklist suggests candidates for review; it never changes a catalog.
"""

from __future__ import annotations

import csv
import sys
import tomllib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from reg_meta.errors import EXIT_CONFIG, RegMetaError

from ._curation import repo_curation_path
from .fqid_slugs import _toml_comment, _toml_str

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

# vardemangdsversion is OPTIONAL: a provider-seeded entry may carry canonical
# codes (via valid_codes_file) with no observed instance-label linkage.
_REQUIRED_FIELDS = ("short_name", "name")
# Accepted first-two-column headers for a valid-codes CSV. SCB CSVs use the
# native `vardekod,vardebenamning`; the universal `code,label` shape is what
# the SOS classification CSVs ship (with extra trailing columns we drop).
_VALID_CODES_HEADERS = (("vardekod", "vardebenamning"), ("code", "label"))

_LEVEL_EXPR = (
    "CASE WHEN {col} GLOB '[0-9]*' AND NOT {col} GLOB '*[^0-9]*' "
    "THEN length({col}) ELSE NULL END"
)

# Read-only worklist thresholds. Suggestions still require reviewed curation;
# containment and label agreement never bind classifications during a build.
_MIN_CONTAINMENT = 0.90
_MIN_CODES = 8
_SAFE_LABEL_AGREE = 0.90


def declared_short_names(seed_path: Path | None = None) -> frozenset[str]:
    """Declared names for validation of references in maintainer input files.

    Pass the selected seed when converting external inputs. The default supports
    the checked-in declaration loaders and offline inspection tools.
    """
    path = seed_path or repo_curation_path("classifications.toml")
    if path is None:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="classification_seed_unreadable",
            error_class="configuration",
            message=(
                "curation/classifications.toml seed not found; cannot validate "
                "classification references."
            ),
            remediation=(
                "Run build-db from the maintainer repo checkout where "
                "curation/classifications.toml is present."
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
    """Return ``{short_name: resolved_path}`` for each entry's canonical CSV.

    A seed entry with ``valid_codes_file`` set but no ``valid_codes_dir``
    available, or a missing/non-file path, is a build-stop error.
    """
    resolved: dict[str, Path] = {}
    for entry in entries:
        rel = entry["valid_codes_file"]
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


def _build_containment_temp_tables(conn: sqlite3.Connection) -> None:
    """Build TEMP projections for the read-only classification worklist.

    The caller drops the tables after reading. Persistent catalog rows are never
    changed; containment and exact label agreement only rank review candidates.
    """
    for tmp in ("_canon_codes", "_vs_codes", "_vs_stats", "_vs_cls", "_vs_label_agree"):
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")

    # 1. Canonical (cls_id, kod, level, label). Every seeded classification is
    # CSV-backed, so fresh builds only produce `is_valid = 1` rows. TRIM mirrors
    # the query-time rule.
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
        WHERE cc.is_valid = 1
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


# Read-only suggestions for states that have no declared classification.
# The safe suggestion tier requires exactly one standalone candidate with label
# agreement above _SAFE_LABEL_AGREE; the operator still decides whether to accept it.


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
    VARIABLE-grain — an existing variable-wide declaration covers EVERY value-set state, not just
    the safe one. Offline conversion must capture exact state-level guards. So a variable that has a safe value set but
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

    A curated `[[link]]` is VARIABLE-grain: a variable-wide declaration
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
    from — the exact shape `curation/classifications.toml` accepts, so a CONFIRMED
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
        "# reg_meta_build/curation/classifications.toml (drop/replace the residue note).",
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
