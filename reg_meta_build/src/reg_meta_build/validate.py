"""Post-build invariant checks for `reg-meta-build build-db`.

Run against a freshly built `reg_meta.db` to catch value-set dedup or
year-projection drift before the build is shipped. Logic mirrors what
`scripts/validate_valueset_dedup.py` used to do as a sibling process;
both that script and the `--validate` flag on `build-db` call into this
module so the checks stay in one place.

Schema shape:
  - value_set / value_set_member / variable_state.value_set_id present
  - cvid_value_code / value_item / value_item_validity absent
  - variable_instance / variable_alias_build / variable_context dropped
    before ship (A2.7)
  - member_hash uniqueness invariant

Year projection correctness (two layers):
  - Corpus-wide: every `variable_state` that names a `value_set_id` must
    project >= 1 code (`_check_state_projection_integrity`). Catches a
    year-projection that minted a dangling/empty value_set link.
  - Code-membership anchor: Andel/grad av aktivitetsersättning, vilande
    (var_id 24193, year 2010) — its 2010-overlapping state must contain
    codes 01-04 and must NOT contain 00 or 05
    (`_check_var_year_codes_anchor`). This is the wrong-code-membership
    guard (the original ArbSokNov 4/5 bug class); the corpus-wide check
    alone would pass a state that wrongly *includes* an out-of-window code.
  - PRAGMA foreign_key_check returns no rows
  - Freelist fraction < 1% of pages (`_check_operational`): the build drops
    several large build-only staging tables (`variable_instance`,
    `variable_alias_build`, `register_version`, …) before ship, so a high
    freelist count means the post-drop VACUUM didn't reclaim them — staging bloat
    riding along in the shipped DB.

A2.7: the validator runs on the POST-drop shipped DB (the `pre_rename_hook`),
so it can no longer read `variable_instance`. Both projection checks resolve a
`value_set_id` through `variable_state` (which has `variable_id`) instead of a
cvid. The code-membership anchor re-homes onto `variable_state.valid_from`/
`valid_to` (now that the validity window survives in the shipped DB) joined to
`variable.provider_key` for the var_id, so it no longer needs the dropped
`variable_instance`/`register_version`. The ArbSokNov 1998-edition anchor is
dropped: it keyed on `register_version.registerversionnamn`, which left the model
in A2.6 (version is no longer in the FQID grammar) — there is no per-edition
column on `variable_state` to re-key it against. Both anchors self-skip cleanly
when the var_id is absent, so the synthetic fixture (which has neither var_id)
stays green; they bite on the orchestrator's full-corpus build.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from reg_meta.db import open_db

if TYPE_CHECKING:
    import sqlite3

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
        _check_state_projection_integrity(conn, result, has_projection)
        _check_var_year_codes_anchor(conn, result, has_projection)
        _check_variable_alias_covers_state_columns(conn, result, tables)
        _check_operational(conn, result)
    finally:
        conn.close()
    return result


def _codes_for_value_set(conn: sqlite3.Connection, value_set_id: int) -> set[str]:
    """Return the set of ``value_code.code`` values (previously SCB ``vardekod``)
    that a ``value_set`` projects to. A2.7: keyed by ``value_set_id`` (resolved
    from ``variable_state``) instead of a cvid, since ``variable_instance`` is
    dropped before ship."""
    return {
        r[0]
        for r in conn.execute(
            "SELECT vc.code FROM value_set_member vsm "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vsm.value_set_id = ?",
            (value_set_id,),
        )
    }


def _check_schema_shape(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    result.section("[schema]")
    for required in ("value_set", "value_set_member"):
        if required in tables:
            result.ok(f"{required} present")
        else:
            result.fail(f"{required} missing")
    # A2.6 added register_version / population / object_type; A2.7 adds
    # variable_instance / variable_alias_build / variable_context to the
    # dropped-before-ship set (build-time-only, like unika_summary).
    for absent in (
        "cvid_value_code",
        "value_item",
        "value_item_validity",
        "register_version",
        "population",
        "object_type",
        "variable_instance",
        "variable_alias_build",
        "variable_context",
    ):
        if absent in tables:
            result.fail(f"{absent} should have been dropped")
        else:
            result.ok(f"{absent} absent")

    # A2.7: value_set_id moved to variable_state (variable_instance is gone).
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(variable_state)")}
    if "value_set_id" in cols:
        result.ok("variable_state.value_set_id present")
    else:
        result.fail("variable_state.value_set_id missing")

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

    # Counts — record only; no thresholds without a baseline. A2.7: states
    # replace cvids (variable_instance is dropped before ship).
    n_sets = conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0]
    n_members = conn.execute("SELECT COUNT(*) FROM value_set_member").fetchone()[0]
    n_states_with_set = conn.execute(
        "SELECT COUNT(*) FROM variable_state WHERE value_set_id IS NOT NULL"
    ).fetchone()[0]
    n_states_total = conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[0]
    result.info(
        f"{n_sets:,} value_sets / {n_members:,} members / "
        f"{n_states_with_set:,} states linked / {n_states_total:,} total"
    )


def _check_state_projection_integrity(
    conn: sqlite3.Connection, result: ValidationResult, has_projection: bool
) -> None:
    """A2.7: corpus-wide year-projection integrity on `variable_state`.

    The v0.x cvid anchors (ArbSokNov-1998, cvid-421764) keyed on
    `variable_instance` + `register_version.registerversionnamn` — both dropped
    before ship (A2.6/A2.7). This replacement guards the same failure mode
    without a magic cvid: every state that claims a `value_set_id` must project
    to >= 1 code. A `value_set_id` pointing at an empty/missing value_set means
    the year-projection minted a dangling link — a build regression, not a
    legitimate code-less state (which carries NULL `value_set_id`).
    """
    result.section("[projection: variable_state integrity]")
    if not has_projection:
        result.ok("value_set tables absent — projection check skipped")
        return
    # States that name a value_set whose member→code projection is empty. A
    # dangling FK would already be caught by foreign_key_check; this catches a
    # value_set row that exists but yields zero codes after projection.
    empty = conn.execute(
        "SELECT vs.state_id, vs.value_set_id FROM variable_state vs "
        "WHERE vs.value_set_id IS NOT NULL "
        "  AND NOT EXISTS ("
        "    SELECT 1 FROM value_set_member vsm "
        "    JOIN value_code vc ON vsm.code_id = vc.code_id "
        "    WHERE vsm.value_set_id = vs.value_set_id"
        "  ) "
        "LIMIT 5"
    ).fetchall()
    if empty:
        sample = ", ".join(
            f"state {r['state_id']}→vs {r['value_set_id']}" for r in empty
        )
        result.fail(
            f"{len(empty)}+ variable_state row(s) link a value_set that yields "
            f"no projected codes (broken value_set_id?): {sample}"
        )
    else:
        n_linked = conn.execute(
            "SELECT COUNT(*) FROM variable_state WHERE value_set_id IS NOT NULL"
        ).fetchone()[0]
        result.ok(f"all {n_linked:,} code-linked states project >= 1 code")


# Code-membership anchor: a single hand-verified (var_id, year) → expected /
# forbidden codes. The corpus-wide integrity check above only asserts >= 1 code,
# so a year-projection that wrongly INCLUDES an out-of-window code would pass it;
# this anchor is the wrong-code-membership guard (the original ArbSokNov 4/5 bug
# class). A2.7 re-homed it off the dropped `variable_instance`/`register_version`
# onto `variable_state.valid_from`/`valid_to` + `variable.provider_key`.
# lisa/aktersviland "Andel/grad av aktivitet" (register 34): its value set is
# 01-04 in 2009-2010 but gains 00 + 05 from 2011, so year 2010 must contain
# 01-04 and EXCLUDE 00/05 — the wrong-code-membership / year-projection guard
# (this is the variable the old cvid-421764 anchor's 2010 edition belonged to).
# provider_key is non-unique across registers (A2.2 splits), so pin the register.
_ANCHOR_REGISTER_ID = 34  # lisa
_ANCHOR_VAR_ID = "24193"
_ANCHOR_YEAR = 2010
_ANCHOR_EXPECTED = frozenset({"01", "02", "03", "04"})
_ANCHOR_FORBIDDEN = frozenset({"00", "05"})


def _check_var_year_codes_anchor(
    conn: sqlite3.Connection, result: ValidationResult, has_projection: bool
) -> None:
    """A2.7: year-bounded code-membership anchor on `variable_state`.

    Resolves var_id 24193's state(s) overlapping calendar `_ANCHOR_YEAR` via the
    canonical overlap predicate (`valid_from <= year-end AND valid_to >=
    year-start`, NOT start-year-only — mirrors `queries._state_covers_year`),
    unions their projected codes, and asserts the expected codes are present and
    the forbidden ones absent. A split sibling (post-A2.2) may give the var_id
    several states; the union is the right grain here because the anchor checks
    that the year-projection emits the correct code *window*, not which sibling
    owns it.

    Self-skips when var_id 24193 (or value_set tables) are absent — the synthetic
    test fixture has neither, so this only bites on the full-corpus build. The
    skip is distinguished from a present-but-empty projection (a FAIL) so a broken
    value_set link can't masquerade as a legitimate skip (cf. PR #99)."""
    result.section("[projection: var_id 24193 codes anchor]")
    if not has_projection:
        result.ok("value_set tables absent — anchor skipped")
        return
    # Year-2010-overlapping states for var_id 24193, joined variable→state. The
    # full-date contract makes the string bounds lexically == chronological.
    year_lo = f"{_ANCHOR_YEAR}-01-01"
    year_hi = f"{_ANCHOR_YEAR}-12-31"
    state_rows = conn.execute(
        "SELECT vs.value_set_id FROM variable v "
        "JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "WHERE v.register_id = ? AND v.provider_key = ? "
        "  AND vs.valid_from <= ? AND vs.valid_to >= ?",
        (_ANCHOR_REGISTER_ID, _ANCHOR_VAR_ID, year_hi, year_lo),
    ).fetchall()
    if not state_rows:
        # Distinguish "var absent" (legit skip — the synthetic fixture has no
        # such var) from "var PRESENT but no state overlaps the anchor year": the
        # latter is a year-window/coalescing regression — exactly what this anchor
        # guards — so FAIL rather than let it masquerade as a skip (Codex P2 #149).
        present = conn.execute(
            "SELECT 1 FROM variable WHERE register_id = ? AND provider_key = ? LIMIT 1",
            (_ANCHOR_REGISTER_ID, _ANCHOR_VAR_ID),
        ).fetchone()
        if present:
            result.fail(
                f"var_id {_ANCHOR_VAR_ID} present but no state overlaps "
                f"{_ANCHOR_YEAR} (year-window/coalescing regression?)"
            )
        else:
            result.ok(f"var_id {_ANCHOR_VAR_ID} not present — anchor skipped")
        return
    value_set_ids = {r[0] for r in state_rows if r[0] is not None}
    if not value_set_ids:
        # The state(s) exist but carry NULL value_set_id — the year-projection
        # dropped the code list entirely. For a code-list variable that is a
        # build regression, not a legitimate skip.
        result.fail(
            f"var_id {_ANCHOR_VAR_ID} year {_ANCHOR_YEAR} state(s) carry no "
            "value_set_id (year-projection dropped the code list?)"
        )
        return
    codes: set[str] = set()
    for vs_id in value_set_ids:
        codes |= _codes_for_value_set(conn, vs_id)
    if not codes:
        result.fail(
            f"var_id {_ANCHOR_VAR_ID} year {_ANCHOR_YEAR} yields no projected "
            "codes (broken value_set_id?)"
        )
        return
    missing = _ANCHOR_EXPECTED - codes
    if missing:
        result.fail(
            f"var_id {_ANCHOR_VAR_ID} year {_ANCHOR_YEAR} missing codes "
            f"{sorted(missing)} (present: {sorted(codes)})"
        )
    else:
        result.ok(
            f"var_id {_ANCHOR_VAR_ID} year {_ANCHOR_YEAR} contains "
            f"{sorted(_ANCHOR_EXPECTED)}"
        )
    present_forbidden = codes & _ANCHOR_FORBIDDEN
    if present_forbidden:
        result.fail(
            f"var_id {_ANCHOR_VAR_ID} year {_ANCHOR_YEAR} contains forbidden "
            f"codes {sorted(present_forbidden)}"
        )
    else:
        result.ok(f"var_id {_ANCHOR_VAR_ID} year {_ANCHOR_YEAR} excludes 00/05")


def _check_variable_alias_covers_state_columns(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """A2.7 invariant: every delivery column a `variable_state` carries must be
    present in `variable_alias` (the source `get_datacolumns`/`resolve` read).

    `_reparent_variable_alias`'s cvid column-tie SKIPS genuinely-ambiguous split
    cvids (skip-not-guess), which could otherwise drop a column the state still
    uses; the build's state-column backstop closes that gap. This guards the
    backstop against regression — a missing state-column means a column the data
    actively uses would be invisible to the catalog API."""
    result.section("[projection: variable_alias covers state columns]")
    if not {"variable_alias", "variable_state"}.issubset(tables):
        result.ok("variable_alias / variable_state absent — check skipped")
        return
    missing = conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT vs.variable_id, "
        "  vs.register_variant_id, vs.delivery_column_name "
        "  FROM variable_state vs WHERE vs.delivery_column_name IS NOT NULL "
        "  AND NOT EXISTS (SELECT 1 FROM variable_alias va "
        "    WHERE va.variable_id = vs.variable_id "
        "    AND va.register_variant_id = vs.register_variant_id "
        "    AND LOWER(va.delivery_column_name) = LOWER(vs.delivery_column_name)))"
    ).fetchone()[0]
    if missing:
        result.fail(
            f"{missing} variable_state delivery column(s) missing from "
            "variable_alias (reparent backstop regression?)"
        )
    else:
        result.ok("all variable_state delivery columns present in variable_alias")


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
