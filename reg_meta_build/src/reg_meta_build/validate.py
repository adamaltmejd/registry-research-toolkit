"""Post-build invariant checks for `reg-meta-build build-db`.

Run against a freshly built `reg_meta.db` to catch value-set dedup or
year-projection drift before the build is shipped. Logic mirrors what
`scripts/validate_valueset_dedup.py` used to do as a sibling process;
both that script and the `--validate` flag on `build-db` call into this
module so the checks stay in one place.

Schema shape:
  - value_set / value_set_member / variable_instance.value_set_id present
  - cvid_value_code / value_item / value_item_validity absent
  - member_hash uniqueness invariant

Year projection correctness:
  - ArbSokNov LISA spot-check (cvid-1998 must NOT contain code 4 or 5)
  - Andel/grad av aktivitetsersättning, vilande (cvid 421764, year 2010):
    must contain codes 01-04, must NOT contain 00 or 05
  - PRAGMA foreign_key_check returns no rows
  - PRAGMA freelist_count is < 1% of page_count (no staging-page bloat)
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from reg_meta.db import open_db

LineKind = Literal["section", "ok", "fail", "info"]


@dataclass(frozen=True)
class ValidationLine:
    kind: LineKind
    text: str

    def format(self) -> str:
        if self.kind == "section":
            return self.text
        if self.kind == "ok":
            return f"  [OK] {self.text}"
        if self.kind == "fail":
            return f"  [FAIL] {self.text}"
        return f"  · {self.text}"


@dataclass
class ValidationResult:
    lines: list[ValidationLine] = field(default_factory=list)

    @property
    def failures(self) -> list[str]:
        return [ln.text for ln in self.lines if ln.kind == "fail"]

    @property
    def passed(self) -> bool:
        return not self.failures

    def section(self, name: str) -> None:
        self.lines.append(ValidationLine("section", name))

    def ok(self, msg: str) -> None:
        self.lines.append(ValidationLine("ok", msg))

    def fail(self, msg: str) -> None:
        self.lines.append(ValidationLine("fail", msg))

    def info(self, msg: str) -> None:
        self.lines.append(ValidationLine("info", msg))

    def format_report(self) -> str:
        # Blank line before each section (except the first) so sections
        # are visually separated in the rendered report.
        parts: list[str] = []
        for ln in self.lines:
            if ln.kind == "section" and parts:
                parts.append("")
            parts.append(ln.format())
        return "\n".join(parts)


def validate_built_db(db_path: Path) -> ValidationResult:
    """Run all value-set dedup invariants against ``db_path``.

    The result records ``[OK]`` / ``[FAIL]`` lines per check; callers
    decide how to surface them. Raises FileNotFoundError if ``db_path``
    does not exist.

    Opens with ``check_schema=False``: this validator exists to catch
    schema drift, so it must not depend on the schema-version sanity
    check passing first.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    result = ValidationResult()
    result.section(f"Validating {db_path}")
    conn = open_db(db_path, check_schema=False)
    try:
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        has_projection = {"value_set", "value_set_member"}.issubset(tables)
        _check_schema_shape(conn, result, tables)
        _check_arbsoknov_projection(conn, result, has_projection)
        _check_cvid_421764_projection(conn, result, has_projection)
        _check_operational(conn, result)
    finally:
        conn.close()
    return result


def _codes_for_cvid(conn: sqlite3.Connection, cvid: int) -> set[str]:
    """Return the set of ``vardekod`` values projected onto ``cvid`` via
    its value_set. Shared by both anchor checks."""
    return {
        r[0]
        for r in conn.execute(
            "SELECT vc.vardekod FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vi.cvid = ?",
            (cvid,),
        )
    }


def _cvid_exists(conn: sqlite3.Connection, cvid: int) -> bool:
    """True when ``cvid`` is present in variable_instance. Used to tell
    "anchor truly absent" (skip) from "anchor present but projection
    yields no codes" (FAIL) — see PR #99 Codex review."""
    return (
        conn.execute(
            "SELECT 1 FROM variable_instance WHERE cvid = ? LIMIT 1", (cvid,)
        ).fetchone()
        is not None
    )


def _check_schema_shape(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    result.section("[schema]")
    for required in ("value_set", "value_set_member"):
        if required in tables:
            result.ok(f"{required} present")
        else:
            result.fail(f"{required} missing")
    for absent in ("cvid_value_code", "value_item", "value_item_validity"):
        if absent in tables:
            result.fail(f"{absent} should have been dropped")
        else:
            result.ok(f"{absent} absent")

    cols = {r["name"] for r in conn.execute("PRAGMA table_info(variable_instance)")}
    if "value_set_id" in cols:
        result.ok("variable_instance.value_set_id present")
    else:
        result.fail("variable_instance.value_set_id missing")

    # The remaining checks depend on value_set / value_set_member existing;
    # skip them if a required table is missing rather than crashing.
    if {"value_set", "value_set_member"} - tables:
        return

    # member_hash uniqueness — UNIQUE enforces it; query asserts intent.
    dup_hashes = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM value_set "
        "GROUP BY member_hash HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if dup_hashes == 0:
        result.ok("member_hash unique across value_set")
    else:
        result.fail(f"member_hash has {dup_hashes} duplicate group(s)")

    # Counts — record only; no thresholds without a baseline.
    n_sets = conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0]
    n_members = conn.execute("SELECT COUNT(*) FROM value_set_member").fetchone()[0]
    n_cvids_with_set = conn.execute(
        "SELECT COUNT(*) FROM variable_instance WHERE value_set_id IS NOT NULL"
    ).fetchone()[0]
    n_cvids_total = conn.execute("SELECT COUNT(*) FROM variable_instance").fetchone()[0]
    result.info(
        f"{n_sets:,} value_sets / {n_members:,} members / "
        f"{n_cvids_with_set:,} cvids linked / {n_cvids_total:,} total"
    )


def _check_arbsoknov_projection(
    conn: sqlite3.Connection, result: ValidationResult, has_projection: bool
) -> None:
    result.section("[projection: ArbSokNov LISA]")
    if not has_projection:
        result.ok("value_set tables absent — anchor skipped")
        return
    # ArbSokNov is variable_id=31554 in LISA. Codes 4 and 5 should not
    # appear in cvids before their introduction (~2006-2007). Fetched
    # in one pass so the ArbSokNov-specific version-name filter can run
    # over the (small) result set in Python.
    cvid_versions = {
        r["cvid"]: r["registerversionnamn"]
        for r in conn.execute(
            "SELECT vi.cvid, rv.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rv ON vi.regver_id = rv.regver_id "
            "WHERE vi.var_id = 31554"
        )
    }
    if not cvid_versions:
        result.ok("ArbSokNov (var_id=31554) not present — skipping")
        return
    early = [
        cvid for cvid, version in cvid_versions.items() if version and "1998" in version
    ]
    if not early:
        result.ok("ArbSokNov 1998 cvid not present in DB — anchor skipped")
        return
    for cvid in early:
        kods = _codes_for_cvid(conn, cvid)
        # An empty kod set on a cvid that *is* in variable_instance
        # signals a broken projection (NULL value_set_id, or no joined
        # members) — not the absence we'd want to silently pass.
        if not kods:
            result.fail(
                f"ArbSokNov 1998 cvid {cvid} has no projected codes "
                f"(broken value_set_id?)"
            )
            continue
        if "4" in kods or "5" in kods:
            result.fail(
                f"ArbSokNov 1998 cvid {cvid} contains 4 or 5 "
                f"(should be excluded by validity): {sorted(kods)}"
            )
        else:
            result.ok(f"ArbSokNov 1998 cvid {cvid} excludes 4/5")


def _check_cvid_421764_projection(
    conn: sqlite3.Connection, result: ValidationResult, has_projection: bool
) -> None:
    result.section("[projection: cvid 421764 anchor]")
    if not has_projection:
        result.ok("value_set tables absent — anchor skipped")
        return
    if not _cvid_exists(conn, 421764):
        result.ok("cvid 421764 not present — anchor skipped")
        return
    kods = _codes_for_cvid(conn, 421764)
    if not kods:
        # cvid 421764 is in variable_instance but the join yields zero
        # codes — broken projection, not a legitimate skip.
        result.fail(
            "cvid 421764 present but yields no projected codes (broken value_set_id?)"
        )
        return
    expected = {"01", "02", "03", "04"}
    forbidden = {"00", "05"}
    if expected.issubset(kods):
        result.ok(f"cvid 421764 contains {sorted(expected)}")
    else:
        result.fail(
            f"cvid 421764 missing codes {sorted(expected - kods)} "
            f"(present: {sorted(kods)})"
        )
    if kods & forbidden:
        result.fail(f"cvid 421764 contains forbidden codes {sorted(kods & forbidden)}")
    else:
        result.ok("cvid 421764 excludes 00/05")


def _check_operational(conn: sqlite3.Connection, result: ValidationResult) -> None:
    result.section("[operational]")
    fk_violations = list(conn.execute("PRAGMA foreign_key_check"))
    if not fk_violations:
        result.ok("PRAGMA foreign_key_check returns 0 rows")
    else:
        result.fail(
            f"PRAGMA foreign_key_check returned {len(fk_violations)} violation(s)"
        )

    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    pages = conn.execute("PRAGMA page_count").fetchone()[0]
    pct = (freelist / pages * 100) if pages else 0
    if pct < 1.0:
        result.ok(f"freelist {freelist:,} / {pages:,} pages ({pct:.2f}%)")
    else:
        result.fail(f"freelist {pct:.2f}% of pages — staging bloat? (>= 1%)")
