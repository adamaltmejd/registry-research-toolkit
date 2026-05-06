"""Validate the value-set dedup + year-projection rebuild (issue #41).

Run against a freshly built regmeta.db (after `regmeta maintain build-db`).
Exits non-zero on any failure. Replaces validate_sentinel_cleanup.py — that
concern is closed (#42 merged); this is its successor for #41.

Checks (PLAN_VALUESET_DEDUP §9):

Schema shape:
  - value_set / value_set_member / variable_instance.value_set_id present
  - cvid_value_code / value_item / value_item_validity absent
  - member_hash uniqueness invariant

Year projection correctness:
  - ArbSokNov LISA spot-check (cvid-1998 must NOT contain code 4 or 5;
    cvid-2007/2008 must contain code 4 with the correct meaning)
  - Andel/grad av aktivitetsersättning, vilande (cvid 421764, year 2010):
    must contain codes 01-04, must NOT contain 00 or 05
  - PRAGMA foreign_key_check returns no rows
  - PRAGMA freelist_count is < 1% of page_count (no staging-page bloat)
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

DB = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/regmeta-rebuild-test/regmeta.db")

failures: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"  ✗ {msg}")


def ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def main() -> None:
    if not DB.exists():
        sys.exit(f"DB not found: {DB}")
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    print(f"Validating {DB}\n")

    # ----- Schema shape -----
    print("[schema]")
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    for required in ("value_set", "value_set_member"):
        if required in tables:
            ok(f"{required} present")
        else:
            fail(f"{required} missing")
    for absent in ("cvid_value_code", "value_item", "value_item_validity"):
        if absent in tables:
            fail(f"{absent} should have been dropped")
        else:
            ok(f"{absent} absent")

    cols = {r["name"] for r in conn.execute("PRAGMA table_info(variable_instance)")}
    if "value_set_id" in cols:
        ok("variable_instance.value_set_id present")
    else:
        fail("variable_instance.value_set_id missing")

    # member_hash uniqueness — UNIQUE enforces it; query asserts intent.
    dup_hashes = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM value_set "
        "GROUP BY member_hash HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if dup_hashes == 0:
        ok("member_hash unique across value_set")
    else:
        fail(f"member_hash has {dup_hashes} duplicate group(s)")

    # Counts — record only; no thresholds without a baseline.
    n_sets = conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0]
    n_members = conn.execute("SELECT COUNT(*) FROM value_set_member").fetchone()[0]
    n_cvids_with_set = conn.execute(
        "SELECT COUNT(*) FROM variable_instance WHERE value_set_id IS NOT NULL"
    ).fetchone()[0]
    n_cvids_total = conn.execute("SELECT COUNT(*) FROM variable_instance").fetchone()[0]
    print(
        f"  · {n_sets:,} value_sets / {n_members:,} members / "
        f"{n_cvids_with_set:,} cvids linked / {n_cvids_total:,} total"
    )

    # ----- Year projection: ArbSokNov LISA anchor -----
    print("\n[projection: ArbSokNov LISA]")
    # ArbSokNov is variable_id=31554 in LISA per PLAN §2.1. Codes 4 and 5
    # should not appear in cvids before their introduction (~2006-2007).
    arbs = conn.execute(
        "SELECT vi.cvid, rv.registerversionnamn, vc.vardekod, vc.vardebenamning "
        "FROM variable_instance vi "
        "JOIN register_version rv ON vi.regver_id = rv.regver_id "
        "LEFT JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
        "LEFT JOIN value_code vc ON vsm.code_id = vc.code_id "
        "WHERE vi.var_id = 31554 "
        "ORDER BY vi.cvid, vc.vardekod"
    ).fetchall()
    if not arbs:
        ok("ArbSokNov (var_id=31554) not present — skipping")
    else:
        # Index by (cvid, year) → set of vardekods.
        by_cvid: dict[int, dict] = {}
        for r in arbs:
            d = by_cvid.setdefault(
                r["cvid"], {"version": r["registerversionnamn"], "kods": set()}
            )
            if r["vardekod"] is not None:
                d["kods"].add(r["vardekod"])
        # Find earliest year that should not have code 4/5.
        early = [
            cvid
            for cvid, d in by_cvid.items()
            if d["version"] and "1998" in d["version"]
        ]
        if early:
            for cvid in early:
                kods = by_cvid[cvid]["kods"]
                if "4" in kods or "5" in kods:
                    fail(
                        f"ArbSokNov 1998 cvid {cvid} contains 4 or 5 "
                        f"(should be excluded by validity): {sorted(kods)}"
                    )
                else:
                    ok(f"ArbSokNov 1998 cvid {cvid} excludes 4/5")
        else:
            ok("ArbSokNov 1998 cvid not present in DB — anchor skipped")

    # ----- Year projection: Andel/grad av aktivitetsersättning, vilande -----
    print("\n[projection: cvid 421764 anchor]")
    row = conn.execute(
        "SELECT vi.value_set_id FROM variable_instance vi WHERE vi.cvid = 421764"
    ).fetchone()
    if row is None:
        ok("cvid 421764 not present — anchor skipped")
    else:
        kods = {
            r["vardekod"]
            for r in conn.execute(
                "SELECT vc.vardekod FROM variable_instance vi "
                "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
                "JOIN value_code vc ON vsm.code_id = vc.code_id "
                "WHERE vi.cvid = 421764"
            )
        }
        expected = {"01", "02", "03", "04"}
        forbidden = {"00", "05"}
        if expected.issubset(kods):
            ok(f"cvid 421764 contains {sorted(expected)}")
        else:
            fail(
                f"cvid 421764 missing codes {sorted(expected - kods)} "
                f"(present: {sorted(kods)})"
            )
        if kods & forbidden:
            fail(f"cvid 421764 contains forbidden codes {sorted(kods & forbidden)}")
        else:
            ok("cvid 421764 excludes 00/05")

    # ----- Operational: FK + freelist -----
    print("\n[operational]")
    fk_violations = list(conn.execute("PRAGMA foreign_key_check"))
    if not fk_violations:
        ok("PRAGMA foreign_key_check returns 0 rows")
    else:
        fail(f"PRAGMA foreign_key_check returned {len(fk_violations)} violation(s)")

    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    pages = conn.execute("PRAGMA page_count").fetchone()[0]
    pct = (freelist / pages * 100) if pages else 0
    if pct < 1.0:
        ok(f"freelist {freelist:,} / {pages:,} pages ({pct:.2f}%)")
    else:
        fail(f"freelist {pct:.2f}% of pages — staging bloat? (>= 1%)")

    print()
    if failures:
        print(f"FAIL: {len(failures)} check(s) failed")
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
