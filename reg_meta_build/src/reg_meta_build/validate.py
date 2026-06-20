"""Post-build invariant checks for `reg-meta-build build-db`.

Run against a freshly built `reg_meta.db` to catch value-set dedup or
year-projection drift before the build is shipped. Logic mirrors what
`scripts/validate_valueset_dedup.py` used to do as a sibling process;
both that script and `build-db` (which validates by default; opt out with
`--no-validate`) call into this module so the checks stay in one place.

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
  - Open-ended window sentinel: every 9999-prefixed `valid_to` in
    `variable_state` / `variable_state_lineage` equals '9999-12-31' exactly
    (`_check_open_ended_sentinel`) — downstream display branches on the literal
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

from reg_meta.catalog import _decode_panel_entity_key
from reg_meta.db import open_db

from reg_meta_build.db import (
    _PROVIDER_SEED,
    _VALID_TO_SENTINEL,
    PROVIDER_ID_SCB,
    PROVIDER_ID_SOS,
)
from reg_meta_build.id import _MINT_BIT
from reg_meta_build.relations import _REPLACED_BY_NOTE_VINTAGE_LIFT

# Seeded non-SCB provider ids (SOS, FOHM, … — every built-in provider that
# mints into the high band). The global build's minted-id band check enforces
# the high band for these; the flavored check additionally covers dynamically
# minted STEWARD providers (provider_id != SCB), which are NOT seeded here. New
# curated providers (#422) join automatically by appending to `_PROVIDER_SEED`.
_GLOBAL_NONSCB_PROVIDER_IDS: tuple[int, ...] = tuple(
    pid for pid, _slug, _name in _PROVIDER_SEED if pid != PROVIDER_ID_SCB
)

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


def validate_built_db(
    db_path: Path,
    *,
    corpus: bool = False,
    flavored: bool = False,
    slug_dir: Path | None = None,
) -> ValidationResult:
    """Run the build invariants against ``db_path``.

    The result records ``[OK]`` / ``[FAIL]`` lines per check; callers
    decide how to surface them. Raises FileNotFoundError if ``db_path``
    does not exist.

    Opens with ``check_schema=False``: this validator exists to catch
    schema drift, so it must not depend on the schema-version sanity
    check passing first.

    ``corpus`` gates the real-corpus volume checks — currently only
    ``_check_sos_sanity`` (the provider-specific ">= 13 SOS registers,
    1,400-2,000 variables" gate), which holds only on a real maintainer
    build and false-fails on the small synthetic fixtures. CI validates
    synthetic builds with ``corpus=False``; the real ``build-db`` runs
    ``corpus=True``. Every other check is corpus-independent and always
    runs, so synthetic CI exercises the full structural suite without any
    real data.

    ``flavored`` (#365 PR2) validates a steward-flavored DB: an ``extend-db``
    overlay (global core + steward registers/variables/grafts/aliases) on top
    of a released global DB. It runs the SAME full structural suite as
    ``corpus=False`` (the real-corpus volume floors stay OFF — a flavor adds a
    small, steward-specific tail, not the SCB/SOS bulk), but TIGHTENS the
    minted-id band check: every non-SCB provider's ids must be in the high
    minted band ``[2^62, 2^63)`` (steward providers are ``mint()``-ed). SCB
    stays unchanged — SCB-register grafts legitimately keep low-band sequential
    ids. ``flavored`` is independent of ``corpus``; a flavor build never sets
    ``corpus`` (it has no full SCB/SOS corpus to floor-check).

    ``slug_dir`` (#546) is the resolved curation directory the build loaded; it
    feeds the mandatory entity-key curation gate
    (``_check_entity_key_vars_curated``), which fails if a panel entity-key
    variable has no curated ``[variable]`` pin. ``None`` (the default, used by
    synthetic CI) SKIPS that gate — the synthetic fixtures carry no curated slug
    dir. On the flavored extend-db path (#559) the hook threads the STEWARD
    ``slug_dir`` (the dir the overlay populated) so the gate runs, scoped to the
    steward providers that dir covers; the global base's entity-key vars stay out
    of scope (validated at global build, and ``_variable_source_ids`` is unsafe on
    a flavored DB for global registers).
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
        _check_one_value_set_per_period(conn, result, tables)
        _check_open_ended_sentinel(conn, result, tables)
        _check_variable_alias_covers_state_columns(conn, result, tables)
        _check_delivery_column_hygiene(conn, result, tables)
        _check_name_field_hygiene(conn, result, tables)
        _check_panel_refs_resolve(conn, result, tables)
        _check_panel_refs_have_states(conn, result, tables)
        _check_entity_key_vars_curated(
            conn, result, tables, slug_dir, flavored=flavored
        )
        _check_minted_id_bands(conn, result, tables, flavored=flavored)
        # No SOS-specific code_variable_map coverage check: code_variable_map IS
        # the DISTINCT projection of `variable_state ⨝ value_set_member`, and SOS
        # writes variable_state directly (no scratch intermediary like SCB's
        # variable_instance), so any state-vs-map check is a tautology. The real
        # invariant — every state with a value_set projects >= 1 code — is already
        # covered for ALL providers by _check_state_projection_integrity above.
        # Real-corpus volume gate: provider-specific, needs the real delivery.
        # Skipped under corpus=False so synthetic CI builds don't false-fail.
        if corpus:
            _check_sos_sanity(conn, result, tables)
        _check_value_code_search(conn, result, tables, corpus=corpus)
        _check_tags(conn, result, tables)
        _check_variable_alias_window(conn, result, tables, corpus=corpus)
        _check_sos_stateless_variables(conn, result, tables)
        _check_concept_groups(conn, result, tables, corpus=corpus)
        _check_classification_replaced_by(conn, result, tables, corpus=corpus)
        _check_variable_replaced_by_vintage_lift(conn, result, tables, corpus=corpus)
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

    # #352: code/value search additions — value_code.mapping_count column +
    # value_code_fts index.
    if "value_code" in tables:
        vc_cols = {r["name"] for r in conn.execute("PRAGMA table_info(value_code)")}
        if "mapping_count" in vc_cols:
            result.ok("value_code.mapping_count present")
        else:
            result.fail("value_code.mapping_count missing")
    if "value_code_fts" in tables:
        result.ok("value_code_fts present")
    else:
        result.fail("value_code_fts missing")

    # #311: curated thematic tag layer — tag + tag_member tables.
    for required in ("tag", "tag_member"):
        if required in tables:
            result.ok(f"{required} present")
        else:
            result.fail(f"{required} missing")

    # #319: monthly-family per-month alias windows.
    if "variable_alias_window" in tables:
        result.ok("variable_alias_window present")
    else:
        result.fail("variable_alias_window missing")

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


def _check_one_value_set_per_period(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """The co-delivery invariant (see DESIGN.md → Build-time triage (SCB)): a `(variable, register_variant, period,
    delivery_column)` must resolve to EXACTLY ONE value set.

    Concretely: no two `variable_state` rows for the same `(variable_id,
    register_variant_id, delivery_column_name)` may have OVERLAPPING validity and
    DISTINCT non-null `value_set_id`. The key includes the DELIVERY COLUMN because
    a column is the representation handle: a concept (FQID) may carry several
    co-existing columns (SSYK 3/5-digit, age 5/10-yr brackets) — those legitimately
    overlap and a binding picks the column. But a SINGLE physical column holds ONE
    coding per period; two distinct value sets on one column in one period is an
    unresolvable conflict (vintage drift, sub-annual collision) — the catalog
    resolver would land a period on both. The coalescer's per-(variable, variant)
    column-aware year timeline (`sources/scb.py`) eliminates these by construction;
    a survivor is a coalescer regression or a genuine same-column co-delivery that
    needs curation. Either way the build must FAIL rather than ship it.

    NULL `value_set_id` (code-less) is exempt — only distinct code-lists conflict.
    Same value set with different `value_set_version_label`s is fine (the
    representation discriminator, one code-list). Distinct columns are fine
    (parallel representations of the concept).
    """
    result.section("[invariant: one value_set per (variable, variant, period, column)]")
    if "variable_state" not in tables:
        result.ok("variable_state absent — invariant skipped")
        return
    # Overlapping distinct-value_set state pairs on the SAME delivery column under
    # one (variable, variant). `a.state_id < b.state_id` dedups the symmetric pair;
    # `IS` is SQLite's null-safe equality so two NULL-column states still match
    # column-wise (rare; both code-less states are already excluded by the
    # value_set_id guards). Overlap is the closed-interval intersection (mirrors
    # `catalog._states_in_bounds`).
    rows = conn.execute(
        "SELECT v.register_id, v.slug, a.delivery_column_name, "
        "       a.valid_from, a.valid_to, a.value_set_id, "
        "       b.valid_from, b.valid_to, b.value_set_id "
        "FROM variable_state a "
        "JOIN variable_state b "
        "  ON a.variable_id = b.variable_id "
        " AND a.register_variant_id = b.register_variant_id "
        " AND a.delivery_column_name IS b.delivery_column_name "
        " AND a.state_id < b.state_id "
        " AND a.value_set_id IS NOT NULL AND b.value_set_id IS NOT NULL "
        " AND a.value_set_id <> b.value_set_id "
        " AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to "
        "JOIN variable v ON v.variable_id = a.variable_id "
        "ORDER BY v.register_id, v.slug"
    ).fetchall()
    if not rows:
        result.ok("no (variable, variant, column) resolves a period to >1 value set")
        return
    affected = {(r[1], r[2]) for r in rows}
    sample = "; ".join(
        f"{r[1]}/{r[2]} [{r[3][:4]}-{r[4][:4]}]vs{r[5]} ∩ [{r[6][:4]}-{r[7][:4]}]vs{r[8]}"
        for r in rows[:5]
    )
    result.fail(
        f"{len(rows)} overlapping distinct-value_set state pair(s) on one column "
        f"across {len(affected)} (variable, column) — a period resolves to >1 "
        f"value set: {sample}"
    )


def _check_open_ended_sentinel(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """Open-ended validity windows must use the exact '9999-12-31' sentinel.

    Downstream display code branches on the literal — reg_webapp's catalog
    routes (`OPEN_ENDED_VALID_TO`) emit `period_token: None` for open
    windows, and the SPA's `formatWindow`/`windowTitle` hide it the same
    way. A near-sentinel like '9999-06-30' would slip past both and render
    as a garbage token ('2016-01-01..9999-06-30') instead of an open
    window. The DDL only CHECKs length/ordering, so the validator pins
    exactness: any 9999-prefixed `valid_to` must be the sentinel itself.
    Prefix match, not a `>= '9999-01-01'` range: a malformed value like
    '9999-00-00' sorts below January yet still misses the downstream
    literal branch. Applies to both tables that carry ISO-date windows
    (`classification`'s valid_to is an integer year, out of scope).
    """
    result.section("[window: open-ended valid_to sentinel]")
    for table in ("variable_state", "variable_state_lineage"):
        if table not in tables:
            result.ok(f"{table} absent — sentinel check skipped")
            continue
        bad = conn.execute(
            f"SELECT valid_to, COUNT(*) FROM {table} "
            "WHERE valid_to LIKE '9999%' AND valid_to != ? "
            "GROUP BY valid_to ORDER BY valid_to LIMIT 5",
            (_VALID_TO_SENTINEL,),
        ).fetchall()
        if bad:
            sample = ", ".join(f"'{r[0]}' x{r[1]}" for r in bad)
            result.fail(
                f"{table}: 9999-prefixed valid_to that is not exactly "
                f"'{_VALID_TO_SENTINEL}': {sample}"
            )
        else:
            n_open = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE valid_to = ?",
                (_VALID_TO_SENTINEL,),
            ).fetchone()[0]
            result.ok(
                f"{table}: all 9999-prefixed valid_to are exactly "
                f"'{_VALID_TO_SENTINEL}' ({n_open:,} open-ended)"
            )


def _check_variable_alias_covers_state_columns(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """A2.7 invariant: every delivery column a `variable_state` carries must be
    present in `variable_alias` (the source `get_datacolumns`/`resolve` read).

    `SCBAdapter._emit_variable_aliases` projects each cvid's alias columns onto
    the cvid's OWNING `variable_id` (the ground truth the coalescer stamps onto
    `variable_instance.variable_id` from triage), and the materializer writes
    `variable_alias` from that IR (A4.3a). That makes this invariant
    STRUCTURAL — a state's `delivery_column_name` is always one of its group's
    cvids' alias columns, and that cvid shares the state's `variable_id`, so the
    column lands under the same key. This check guards against a regression in
    that re-parent: a missing state-column means a column the data actively uses
    would be invisible to the catalog API."""
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
            "variable_alias (reparent regression?)"
        )
    else:
        result.ok("all variable_state delivery columns present in variable_alias")


def _check_delivery_column_hygiene(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """No `delivery_column_name` ships with surrounding whitespace, and
    `variable_alias` carries real headers only (no empty strings).

    The SCB exports contain a handful of Kolumnnamn values with stray
    leading/trailing spaces ('  Pris', 'Lan ') plus ~3.3K blank values
    (variables delivered with no column header). Both are normalized at the
    SCB read boundary (`_import_registerinformation` / `_import_unika`:
    trim, and skip blanks for `variable_alias_build`); SOS strips at parse
    (`_clean`). A dirty spelling that slips through shards rule-2
    connectivity into bogus split-sibling variables, so this is a build
    regression, not cosmetics. "No delivery header" is represented as NULL
    on `variable_state` and as row-absence in `variable_alias` — never as
    '' (empty string)."""
    result.section("[delivery-column hygiene]")
    if not {"variable_alias", "variable_state"}.issubset(tables):
        result.ok("variable_alias / variable_state absent — check skipped")
        return
    # Counted in Python, not SQL: the read boundary trims with `str.strip()`
    # (all Unicode whitespace — tabs, NBSP, newlines), while SQLite's TRIM()
    # strips ASCII spaces only. The tripwire must use the SAME definition of
    # "trimmed" as the build, or a tab-padded regression slips through.
    untrimmed = sum(
        1
        for (col,) in conn.execute(
            "SELECT delivery_column_name FROM variable_state "
            "WHERE delivery_column_name IS NOT NULL "
            "UNION ALL SELECT delivery_column_name FROM variable_alias"
        )
        if col != col.strip()
    )
    if untrimmed:
        result.fail(
            f"{untrimmed} delivery_column_name value(s) with surrounding "
            "whitespace (read-boundary trim regression?)"
        )
    else:
        result.ok("no delivery_column_name with surrounding whitespace")
    empty = conn.execute(
        "SELECT "
        "  (SELECT COUNT(*) FROM variable_alias WHERE delivery_column_name = '') "
        "+ (SELECT COUNT(*) FROM variable_state WHERE delivery_column_name = '')"
    ).fetchone()[0]
    if empty:
        result.fail(
            f"{empty} empty-string delivery_column_name value(s) "
            "(no-header rows must be NULL states / absent alias rows)"
        )
    else:
        result.ok("no empty-string delivery_column_name")


def _check_name_field_hygiene(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """No `variable` / `register` / `register_variant` `name` ships with
    surrounding whitespace.

    The SCB exports carry stray leading/trailing spaces on a subset of name
    fields (#366: ~1,503 Variabelnamn, 9 Registernamn, 12 Registervariantnamn
    rows; 'Slag utbildning  ', 'Allmänna val ... '). All three are normalized
    at the SCB read boundary (`_import_registerinformation` / `_import_unika`),
    and the same values key the `unika_join` / sensitivity-flag
    (`v.name = us.variabelnamn`) / coalescer joins — trimmed in lockstep so a
    padded export can't silently shard those joins. A padded value in a
    shipped DB is a read-boundary regression."""
    result.section("[name-field hygiene]")
    # Counted in Python, not SQL, to match `str.strip()` (all Unicode
    # whitespace) rather than SQLite TRIM() (ASCII space only) — same
    # definition of "trimmed" as the read boundary, or a tab-padded
    # regression slips through. See `_check_delivery_column_hygiene`.
    checks = [
        ("variable", "name"),
        ("register", "name"),
        ("register_variant", "name"),
    ]
    for table, column in checks:
        if table not in tables:
            result.ok(f"{table} absent — {table}.{column} check skipped")
            continue
        untrimmed = sum(
            1
            for (val,) in conn.execute(
                f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL"
            )
            if val != val.strip()
        )
        if untrimmed:
            result.fail(
                f"{untrimmed} {table}.{column} value(s) with surrounding "
                "whitespace (read-boundary trim regression?)"
            )
        else:
            result.ok(f"no {table}.{column} with surrounding whitespace")


def _check_panel_refs_resolve(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """A4.4c: every curated panel reference on `register_variant` must resolve to
    a real `variable.slug` in the variant's OWN register.

    `panel_entity_key` / `panel_time_key` are curated slug strings (the panel
    shape). The TOML loader (`fqid_slugs._validate_panel_slug_ref`) grammar-checks
    them at build time, but grammar alone can't catch a *dangling* reference — a
    well-formed slug that names no actual variable, or one that lives in a
    different register (slug is only register-unique: `idx_variable_slug` on
    `(register_id, slug)`, so the same slug — e.g. `kon` — exists under several
    registers). A dangling ref would surface as an empty panel axis in the webapp,
    not a build error, so this is the resolution gate.

    Resolution scope is the variant's register: `register_variant.register_id`.
    - `panel_entity_key`: a bare slug OR a json-array string (composite key);
      decode element-wise via `_decode_panel_entity_key` (the catalog's read-side
      decoder — same JSON semantics) and resolve EACH element. ANY element that
      fails to resolve is a finding.
    - `panel_time_key`: a single slug, EXCEPT the literal "period" sentinel
      (delivery-aligned time, not a variable) which is exempt.
    """
    result.section("[panel: refs resolve to register-scoped variable slugs]")
    if not {"register_variant", "variable"}.issubset(tables):
        result.ok("register_variant / variable absent — panel-ref check skipped")
        return
    rows = conn.execute(
        "SELECT register_variant_id, register_id, slug, "
        "       panel_entity_key, panel_time_key "
        "FROM register_variant "
        "WHERE panel_entity_key IS NOT NULL OR panel_time_key IS NOT NULL"
    ).fetchall()
    if not rows:
        result.ok("no variant carries panel refs — nothing to resolve")
        return
    failures: list[str] = []
    n_refs = 0
    for r in rows:
        # (field label, slug to resolve) pairs for this variant.
        refs: list[tuple[str, str]] = []
        entity = _decode_panel_entity_key(r["panel_entity_key"])
        if isinstance(entity, tuple):
            refs.extend(("panel_entity_key", s) for s in entity)
        elif entity is not None:
            refs.append(("panel_entity_key", entity))
        time = _decode_panel_entity_key(r["panel_time_key"])
        if isinstance(time, tuple):
            refs.extend(("panel_time_key", s) for s in time)
        elif time is not None and time != "period":
            refs.append(("panel_time_key", time))
        for field_name, slug in refs:
            n_refs += 1
            hit = conn.execute(
                "SELECT 1 FROM variable WHERE register_id = ? AND slug = ? LIMIT 1",
                (r["register_id"], slug),
            ).fetchone()
            if hit is None:
                failures.append(
                    f"variant {r['register_variant_id']} ({r['slug']!r}, "
                    f"register {r['register_id']}) {field_name} {slug!r} resolves "
                    "to no variable.slug in that register"
                )
    if failures:
        for msg in failures[:10]:
            result.fail(msg)
        if len(failures) > 10:
            result.info(f"... and {len(failures) - 10} more unresolved panel ref(s)")
    else:
        result.ok(
            f"all {n_refs:,} panel ref(s) across {len(rows):,} variant(s) resolve"
        )


def _check_panel_refs_have_states(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """#287: every curated panel reference must have ``variable_state`` rows in
    the variant it decorates — not just resolve to a register-scoped slug.

    `_check_panel_refs_resolve` (above) catches *dangling* refs; this is the
    deeper mis-point class it is blind to: a key resolving to a sibling
    fragment or register-mate whose states all live in OTHER variants. Such a
    key passes resolution but renders an empty panel axis in the webapp —
    the #285 audit found 97 of these manually. Requiring >= 1 ``variable_state``
    row with the variant's own ``register_variant_id`` pins the key to a column
    that actually ships in that variant's delivery.

    Same reference decoding as the resolution check: composite
    ``panel_entity_key`` / ``panel_time_key`` JSON arrays check element-wise;
    the literal ``panel_time_key = "period"`` sentinel (delivery-aligned time,
    not a variable) is exempt. A ref that doesn't resolve at all is the resolution
    check's finding, not double-reported here (the NOT EXISTS would also fire,
    but resolution failures fail the build first anyway).
    """
    result.section("[panel: entity key has states in the variant]")
    if not {"register_variant", "variable", "variable_state"}.issubset(tables):
        result.ok("panel tables absent — panel-states check skipped")
        return
    rows = conn.execute(
        "SELECT register_variant_id, register_id, slug, "
        "       panel_entity_key, panel_time_key "
        "FROM register_variant "
        "WHERE panel_entity_key IS NOT NULL OR panel_time_key IS NOT NULL"
    ).fetchall()
    if not rows:
        result.ok("no variant carries panel refs — nothing to check")
        return
    failures: list[str] = []
    n_refs = 0
    for r in rows:
        refs: list[tuple[str, str]] = []
        entity = _decode_panel_entity_key(r["panel_entity_key"])
        if isinstance(entity, tuple):
            refs.extend(("panel_entity_key", s) for s in entity)
        elif entity is not None:
            refs.append(("panel_entity_key", entity))
        time = _decode_panel_entity_key(r["panel_time_key"])
        if isinstance(time, tuple):
            refs.extend(("panel_time_key", s) for s in time)
        elif time is not None and time != "period":
            refs.append(("panel_time_key", time))
        for field_name, slug in refs:
            n_refs += 1
            hit = conn.execute(
                "SELECT 1 FROM variable v "
                "JOIN variable_state vs ON vs.variable_id = v.variable_id "
                "WHERE v.register_id = ? AND v.slug = ? "
                "  AND vs.register_variant_id = ? LIMIT 1",
                (r["register_id"], slug, r["register_variant_id"]),
            ).fetchone()
            if hit is None:
                failures.append(
                    f"variant {r['register_variant_id']} ({r['slug']!r}, "
                    f"register {r['register_id']}) {field_name} {slug!r} has no "
                    "variable_state rows in that variant"
                )
    if failures:
        for msg in failures[:10]:
            result.fail(msg)
        if len(failures) > 10:
            result.info(f"... and {len(failures) - 10} more state-less panel ref(s)")
    else:
        result.ok(
            f"all {n_refs:,} panel ref(s) across {len(rows):,} variant(s) "
            "have states in their variant"
        )


def _check_entity_key_vars_curated(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    slug_dir: Path | None,
    *,
    flavored: bool = False,
) -> None:
    """#546/#554: every panel entity-key variable must carry a curated
    ``[variable]`` slug pin so the slug its ``panel_entity_key`` ref binds to
    can't drift.

    A variable's auto-slug CHURNS each build (the default freeze zone re-derives
    it from the latest delivery column). A ``panel_entity_key`` ref binds to that
    slug, so a reslug silently dangles the ref — caught only by
    ``_check_panel_refs_resolve``, after a full real build. The pin (precedence 1
    in ``populate_variable_slugs``) freezes the slug; this gate makes it
    MANDATORY, so a newly-onboarded entity-key variable can't ship un-pinned.

    Generate the missing pins with ``reg-meta-build entity-key-pins --out-dir``
    and commit each ``<provider>.toml`` block into ``fqid_slugs/<provider>.toml``.

    GLOBAL build (``flavored=False``): scope = ALL global providers (#554). Every
    provider's entity-key slug can churn and dangle a panel ref, so every provider
    present in the build-db DB is enforced — no provider filter (the set is
    whatever the DB holds, so onboarding a provider can't silently drop it).

    FLAVORED extend-db (``flavored=True``, #559): the steward overlay threads its
    own ``slug_dir`` here, and the gate scopes to the STEWARD REGISTERS that dir
    curates (its ``[register]`` entries). The global base's entity-key vars are
    NOT re-enforced — they were validated at global-build time and live in the
    global slug dir, not the steward one. Scoping by register (not provider)
    matters because ``extend_db`` lets a steward overlay reuse a provider slug
    that ALSO has global base registers; a provider-slug scope would re-pull those
    global registers, and ``iter_entity_key_variables``'s ``_variable_source_ids``
    is unsafe on a flavored DB for GLOBAL registers, so the register filter skips
    them. The enumeration is shared with the generator
    (``fqid_slugs.iter_entity_key_variables`` / ``infer_entity_key_pins``) so gate
    and generator can't disagree on which variables need a pin.

    ``slug_dir is None`` (synthetic CI, direct ``validate_built_db(corpus=False)``
    calls) SKIPS the gate — there's no curated dir to read. Local imports dodge
    any build-time import cycle (the pattern this module already uses for
    build-side helpers)."""
    result.section("[panel: entity-key variables are curated]")
    if slug_dir is None:
        result.ok("entity-key curation gate skipped (no slug_dir)")
        return
    if not {"register_variant", "variable"}.issubset(tables):
        result.ok("register_variant / variable absent — entity-key gate skipped")
        return
    # Local import: the gate is build-side only and `fqid_slugs` pulls in the
    # slug-population machinery, so importing it lazily keeps `validate`'s module
    # import light and avoids any cycle through the build graph.
    from reg_meta_build.fqid_slugs import (
        _entity_key_curation_basis,
        iter_entity_key_variables,
    )

    # Glob the curated dir once: the curated slug map and (flavored) the steward
    # register scope both come from the same entry list. Shared with the generator
    # so gate and generator read the identical basis.
    curated, scope = _entity_key_curation_basis(slug_dir, flavored=flavored)
    entity_key_vars = list(iter_entity_key_variables(conn, register_ids=scope))
    if not entity_key_vars:
        result.ok("no variant carries an entity key — nothing to curate")
        return
    failures: list[str] = []
    for ek in entity_key_vars:
        if (ek.provider_slug, ek.source_id) not in curated:
            failures.append(
                f"{ek.register_slug}/{ek.variable_slug} "
                f"(source_id {ek.source_id}, panel_entity_key {ek.variable_slug!r}) "
                "has no curated [variable] slug pin"
            )
    if failures:
        for msg in failures[:10]:
            result.fail(msg)
        if len(failures) > 10:
            result.info(
                f"... and {len(failures) - 10} more un-pinned entity-key var(s)"
            )
        # The remediation scope differs by build: the global gate curates into the
        # repo-root fqid_slugs/<provider>.toml; the flavored (steward) gate curates
        # into the nested fqid_slugs/<steward>/<provider>.toml and MUST regenerate
        # via `--flavored --slug-dir <steward dir>` (the global `--out-dir` path
        # would emit the wrong, global-scoped pins).
        if flavored:
            result.info(
                "run `reg-meta-build --db <flavored-db> entity-key-pins --flavored "
                "--slug-dir <steward dir>` and fold each <provider>.toml block into "
                "fqid_slugs/<steward>/<provider>.toml"
            )
        else:
            result.info(
                "run `reg-meta-build entity-key-pins --out-dir <dir>` and commit each "
                "<provider>.toml block to fqid_slugs/<provider>.toml"
            )
    else:
        result.ok(f"all {len(entity_key_vars):,} entity-key var(s) are curated")


def _check_minted_id_bands(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    *,
    flavored: bool = False,
) -> None:
    """A4.3b: every minted-provider id (register -> ... -> variable_state) is in
    the band [2^62, 2^63); every SCB-provider id is below 2^62.

    Catches an adapter that forgot to `mint()` (its id would land in the SCB low
    band and risk an id collision) and, symmetrically, an SCB id that overflowed
    into the minted band. value_set/value_code/code_variable_map.code_id are
    EXCLUDED — they are content-addressed, autoincrement, PROVIDER-SHARED, so
    they belong to neither band. Self-skips when no minted rows are present (the
    SCB-only fixture / `--providers=scb` build), so it only bites on a combined
    build.

    GLOBAL build (``flavored=False``): every SEEDED non-SCB provider (SOS, FOHM,
    … — #422 generalized this from the original SOS-only rule) must be high-band.
    The set is derived from ``_PROVIDER_SEED``, so a new curated provider is
    covered the moment it is seeded.

    ``flavored=True`` (#365 PR2) additionally covers dynamically minted STEWARD
    providers (an ``extend-db`` overlay): EVERY non-SCB provider's ids must be
    high-band (``provider_id != SCB``), catching a steward overlay that forgot to
    mint. Steward providers are not in ``_PROVIDER_SEED``, so they are out of
    scope for the global check — hence the two predicates. The SCB rule is
    UNCHANGED in both modes: SCB-register grafts legitimately keep low-band
    sequential ids, so SCB ids stay ``< 2^62``.
    """
    result.section("[bands: minted-id disjointness]")
    if "register" not in tables:
        result.ok("register table absent — band check skipped")
        return
    # Per-grain id extrema joined to the owning provider. Each tuple:
    # (label, SQL selecting that grain's id + provider_id).
    grains = [
        ("register", "SELECT register_id AS id, provider_id FROM register"),
        (
            "register_variant",
            "SELECT rv.register_variant_id AS id, r.provider_id "
            "FROM register_variant rv JOIN register r USING (register_id)",
        ),
        (
            "variable",
            "SELECT v.variable_id AS id, r.provider_id "
            "FROM variable v JOIN register r USING (register_id)",
        ),
        (
            "variable_state",
            "SELECT vs.state_id AS id, r.provider_id "
            "FROM variable_state vs JOIN variable v USING (variable_id) "
            "JOIN register r USING (register_id)",
        ),
    ]
    failures = 0
    n_high = 0  # rows expected in the minted band (seeded non-SCB, or all non-SCB if flavored)
    for label, sql in grains:
        # SCB ids must be < 2^62 (UNCHANGED in both modes — grafts stay low).
        bad_scb = conn.execute(
            f"SELECT COUNT(*) FROM ({sql}) WHERE provider_id = ? AND id >= ?",
            (PROVIDER_ID_SCB, _MINT_BIT),
        ).fetchone()[0]
        if flavored:
            # Every NON-SCB provider's ids must be >= 2^62 (steward + SOS, all
            # minted). A single predicate covers them: provider_id != SCB.
            bad_high = conn.execute(
                f"SELECT COUNT(*) FROM ({sql}) WHERE provider_id != ? AND id < ?",
                (PROVIDER_ID_SCB, _MINT_BIT),
            ).fetchone()[0]
            n_high += conn.execute(
                f"SELECT COUNT(*) FROM ({sql}) WHERE provider_id != ?",
                (PROVIDER_ID_SCB,),
            ).fetchone()[0]
            high_label = "non-SCB"
        else:
            # Every SEEDED non-SCB provider (SOS, FOHM, … — #422) must be
            # >= 2^62 in the GLOBAL build. This generalizes the original
            # SOS-only rule: a global build's non-SCB providers all mint. Steward
            # providers are only present in a flavored overlay and are NOT seeded,
            # so they stay out of scope here (the flavored branch covers them via
            # `!= SCB`), preserving the global/flavored distinction.
            placeholders = ",".join("?" * len(_GLOBAL_NONSCB_PROVIDER_IDS))
            bad_high = conn.execute(
                f"SELECT COUNT(*) FROM ({sql}) "
                f"WHERE provider_id IN ({placeholders}) AND id < ?",
                (*_GLOBAL_NONSCB_PROVIDER_IDS, _MINT_BIT),
            ).fetchone()[0]
            n_high += conn.execute(
                f"SELECT COUNT(*) FROM ({sql}) WHERE provider_id IN ({placeholders})",
                _GLOBAL_NONSCB_PROVIDER_IDS,
            ).fetchone()[0]
            high_label = "non-SCB"
        if bad_scb:
            result.fail(
                f"{bad_scb} {label} SCB id(s) overflow the minted band (>= 2^62)"
            )
            failures += 1
        if bad_high:
            result.fail(
                f"{bad_high} {label} {high_label} id(s) below the minted band "
                "(< 2^62) — un-minted?"
            )
            failures += 1
    if failures == 0:
        if n_high == 0:
            result.ok("no non-SCB rows — minted-id band check trivially holds")
        else:
            result.ok("all SCB ids < 2^62 and all non-SCB ids >= 2^62")


# A4.3b sanity bands for the combined build. The 13 SOS workbooks merge (by
# (register, name)) to ~1,730 distinct variables — the spec's "~2,300" counts the
# PRE-merge (deldatamängd, name) occurrences (2,314); after the Model A
# register-scoped merge the variable grain is ~1,730 (+2 for the two known
# splits). The band is TWO-SIDED: the upper bound (below the 2,314 un-merged
# count) catches a merge-key regression that SPLITS a merge group — distinct
# names mint distinct ids with no PK collision, so an over-split would otherwise
# pass the lower bound silently. Both bounds are generous so a workbook drift
# doesn't false-fail.
_SOS_MIN_REGISTERS = 13
_SOS_MIN_VARIABLES = 1_400
_SOS_MAX_VARIABLES = 2_000


def _check_value_code_search(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    *,
    corpus: bool,
) -> None:
    """#352 code/value search invariants.

    Structural (corpus-independent):
      - the indexed-label count <= `value_code` row count — the stoplist hides
        some labels from the index, so the index is a subset; it can never exceed
        the leaf table. NB: `COUNT(*) FROM value_code_fts` reads the CONTENT table
        (external-content FTS5), so it always equals value_code and can't see the
        exclusion; the honest indexed count is the `_docsize` shadow table.
      - `value_code.mapping_count` is non-negative everywhere.
    Volume floor (corpus only): a real build indexes > 0 labels; a tiny synthetic
    fixture may stoplist its whole label set, so this floor would false-fail there."""
    result.section("[value-code search]")
    if "value_code" not in tables or "value_code_fts" not in tables:
        result.ok("value_code / value_code_fts absent — search check skipped")
        return
    n_vc = conn.execute("SELECT COUNT(*) FROM value_code").fetchone()[0]
    n_idx = conn.execute("SELECT COUNT(*) FROM value_code_fts_docsize").fetchone()[0]
    if n_idx <= n_vc:
        result.ok(f"value_code_fts indexes {n_idx:,} of {n_vc:,} labels")
    else:
        result.fail(f"value_code_fts indexes {n_idx:,} > value_code {n_vc:,}")
    n_neg = conn.execute(
        "SELECT COUNT(*) FROM value_code WHERE mapping_count < 0"
    ).fetchone()[0]
    if n_neg == 0:
        result.ok("value_code.mapping_count non-negative")
    else:
        result.fail(f"{n_neg:,} value_code rows have negative mapping_count")
    if corpus:
        if n_idx > 0:
            result.ok(f"value_code_fts populated ({n_idx:,} labels)")
        else:
            result.fail("value_code_fts is EMPTY on a corpus build")


def _check_tags(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """#311 curated tag-layer structural closure (corpus-independent; NO volume
    floor — tables ship EMPTY until curation content lands). Asserts:
      - every `tag_member.tag_id` references an existing `tag`;
      - every member has EXACTLY ONE grain (the DDL CHECK, re-asserted);
      - every `register_id` / `variable_id` resolves to a live row."""
    result.section("[tags]")
    if "tag" not in tables or "tag_member" not in tables:
        result.ok("tag / tag_member absent — tag check skipped")
        return
    n_tags = conn.execute("SELECT COUNT(*) FROM tag").fetchone()[0]
    n_members = conn.execute("SELECT COUNT(*) FROM tag_member").fetchone()[0]
    result.info(f"{n_tags:,} tags / {n_members:,} tag members")

    orphan_tag = conn.execute(
        "SELECT COUNT(*) FROM tag_member tm "
        "WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.tag_id = tm.tag_id)"
    ).fetchone()[0]
    if orphan_tag == 0:
        result.ok("every tag_member.tag_id resolves to a tag")
    else:
        result.fail(f"{orphan_tag:,} tag_member row(s) reference a missing tag")

    bad_grain = conn.execute(
        "SELECT COUNT(*) FROM tag_member "
        "WHERE (register_id IS NULL) = (variable_id IS NULL)"
    ).fetchone()[0]
    if bad_grain == 0:
        result.ok("every tag_member has exactly one grain")
    else:
        result.fail(f"{bad_grain:,} tag_member row(s) violate exactly-one-grain")

    orphan_reg = conn.execute(
        "SELECT COUNT(*) FROM tag_member tm WHERE tm.register_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM register r WHERE r.register_id = tm.register_id)"
    ).fetchone()[0]
    orphan_var = conn.execute(
        "SELECT COUNT(*) FROM tag_member tm WHERE tm.variable_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM variable v WHERE v.variable_id = tm.variable_id)"
    ).fetchone()[0]
    if orphan_reg == 0 and orphan_var == 0:
        result.ok("every tag_member register_id/variable_id resolves")
    else:
        result.fail(
            f"{orphan_reg:,} dangling register_id + {orphan_var:,} dangling "
            "variable_id in tag_member"
        )


# The 4 LISA + 4 non-LISA monthly families merged today (#319/#383); absolute
# floor like the lkf one — adding/removing a family forces a gate update. Corpus-only,
# and additionally gated on SCB being in the build (#595): the merged families are all
# SCB-sourced (period_family_merges.toml is entirely scb/...), so a non-SCB
# `--providers` subset carries no families and would false-fail.
_AW_MIN_MERGED_FAMILIES = 8


def _check_variable_alias_window(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    *,
    corpus: bool,
) -> None:
    """#319 monthly-family alias-window structural closure (corpus-independent).
    EMPTY without `curation/period_family_merges.toml`, so no volume floor on
    synthetic builds.

    Asserts: every window's (variable_id, register_variant_id) resolves to a live
    variable / register_variant; valid_from <= valid_to; the window's
    delivery_column_name is present in `variable_alias` for that variable+variant
    (the merged variable retains all 12 columns there — same invariant
    `_check_variable_alias_covers_state_columns` enforces for states). Corpus: a
    real maintainer build merges the 8 monthly families (#319/#383), so this is
    also the regression floor for family-merge (>= `_AW_MIN_MERGED_FAMILIES`
    survivors) now that the month-token-group floor in `_check_concept_groups`
    is gone — the merge consumes every month-suffixed family pre-fold. The floor is
    additionally gated on SCB being in the build (#595) — the merged families are all
    SCB-sourced, so a non-SCB `--providers` subset SKIPS rather than false-fails."""
    result.section("[monthly-family windows]")
    if "variable_alias_window" not in tables:
        result.ok("variable_alias_window absent — window check skipped")
        return
    n = conn.execute("SELECT COUNT(*) FROM variable_alias_window").fetchone()[0]
    result.info(f"{n:,} alias windows")

    bad_range = conn.execute(
        "SELECT COUNT(*) FROM variable_alias_window WHERE valid_from > valid_to"
    ).fetchone()[0]
    if bad_range == 0:
        result.ok("every window has valid_from <= valid_to")
    else:
        result.fail(f"{bad_range:,} window(s) with valid_from > valid_to")

    orphan = conn.execute(
        "SELECT COUNT(*) FROM variable_alias_window w WHERE NOT EXISTS "
        "(SELECT 1 FROM variable v WHERE v.variable_id = w.variable_id) "
        "OR NOT EXISTS (SELECT 1 FROM register_variant rv "
        "  WHERE rv.register_variant_id = w.register_variant_id)"
    ).fetchone()[0]
    if orphan == 0:
        result.ok("every window's variable_id/register_variant_id resolves")
    else:
        result.fail(f"{orphan:,} window(s) with a dangling variable/variant")

    if "variable_alias" in tables:
        uncovered = conn.execute(
            "SELECT COUNT(*) FROM variable_alias_window w WHERE NOT EXISTS "
            "(SELECT 1 FROM variable_alias va "
            "  WHERE va.variable_id = w.variable_id "
            "  AND va.register_variant_id = w.register_variant_id "
            "  AND LOWER(va.delivery_column_name) = LOWER(w.delivery_column_name))"
        ).fetchone()[0]
        if uncovered == 0:
            result.ok("every window column present in variable_alias")
        else:
            result.fail(f"{uncovered:,} window column(s) missing from variable_alias")

    if corpus:
        n_families = conn.execute(
            "SELECT COUNT(DISTINCT variable_id) FROM variable_alias_window"
        ).fetchone()[0]
        # Gated on SCB presence (#595): the merged monthly families are all
        # SCB-sourced (period_family_merges.toml is entirely scb/...), so a non-SCB
        # `--providers` subset SKIPS rather than false-fails this floor.
        if not _scb_in_build(conn):
            result.info(
                f"{n_families} merged monthly families — SCB not in this build, "
                f"floor (>= {_AW_MIN_MERGED_FAMILIES}) skipped (#595)"
            )
        elif n_families >= _AW_MIN_MERGED_FAMILIES:
            result.ok(
                f"{n_families} merged monthly families (>= {_AW_MIN_MERGED_FAMILIES})"
            )
        else:
            result.fail(
                f"only {n_families} merged monthly families "
                f"(< {_AW_MIN_MERGED_FAMILIES}) — family-merge regression?"
            )


def _check_sos_sanity(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """A4.3b: the combined build carries the expected SOS volume (>= 13 registers,
    a variable count in the post-merge band). Self-skips when no SOS rows are
    present (SCB-only build)."""
    result.section("[sanity: SOS volume]")
    if "register" not in tables or "variable" not in tables:
        result.ok("register/variable absent — SOS sanity skipped")
        return
    n_reg = conn.execute(
        "SELECT COUNT(*) FROM register WHERE provider_id = ?", (PROVIDER_ID_SOS,)
    ).fetchone()[0]
    if n_reg == 0:
        result.ok("no SOS registers — SOS sanity skipped (SCB-only build)")
        return
    n_var = conn.execute(
        "SELECT COUNT(*) FROM variable v JOIN register r USING (register_id) "
        "WHERE r.provider_id = ?",
        (PROVIDER_ID_SOS,),
    ).fetchone()[0]
    if n_reg >= _SOS_MIN_REGISTERS:
        result.ok(f"{n_reg} SOS registers (>= {_SOS_MIN_REGISTERS})")
    else:
        result.fail(f"only {n_reg} SOS registers (< {_SOS_MIN_REGISTERS})")
    if _SOS_MIN_VARIABLES <= n_var <= _SOS_MAX_VARIABLES:
        result.ok(
            f"{n_var:,} SOS variables (in [{_SOS_MIN_VARIABLES:,}, "
            f"{_SOS_MAX_VARIABLES:,}])"
        )
    elif n_var < _SOS_MIN_VARIABLES:
        result.fail(f"only {n_var:,} SOS variables (< {_SOS_MIN_VARIABLES:,})")
    else:
        result.fail(
            f"{n_var:,} SOS variables (> {_SOS_MAX_VARIABLES:,}) — merge-key "
            "regression? (un-merged (deldatamängd, name) grain is ~2,314)"
        )


def _check_sos_stateless_variables(
    conn: sqlite3.Connection, result: ValidationResult, tables: set[str]
) -> None:
    """A4.3b P2#1: surface SOS variables with ZERO variable_state rows (and the
    registers left entirely state-less).

    WARN-ONLY — this is an `info` line, never a `fail`. A state-less variable is
    a known A4.3b gap: variable-sheet deldatamängd tokens with no Deldatamängder
    row (LOVA/LVM) drop every member's state (the adapter emits
    `sos_deldatamangd_unresolved`); the code->variant mapping is A4.4 curation.
    The build must still ship, so this never gates. Self-skips when no SOS rows
    are present (SCB-only build)."""
    result.section("[warn: SOS state-less variables]")
    if "variable" not in tables or "variable_state" not in tables:
        result.ok("variable/variable_state absent — SOS state-less check skipped")
        return
    n_sos_var = conn.execute(
        "SELECT COUNT(*) FROM variable v JOIN register r USING (register_id) "
        "WHERE r.provider_id = ?",
        (PROVIDER_ID_SOS,),
    ).fetchone()[0]
    if n_sos_var == 0:
        result.ok("no SOS variables — state-less check skipped (SCB-only build)")
        return
    n_stateless = conn.execute(
        "SELECT COUNT(*) FROM variable v JOIN register r USING (register_id) "
        "WHERE r.provider_id = ? "
        "AND NOT EXISTS (SELECT 1 FROM variable_state vs "
        "WHERE vs.variable_id = v.variable_id)",
        (PROVIDER_ID_SOS,),
    ).fetchone()[0]
    if n_stateless == 0:
        result.ok(f"all {n_sos_var:,} SOS variables have >= 1 variable_state")
        return
    # WARN: list it, but do NOT fail the gate.
    result.info(
        f"{n_stateless:,} of {n_sos_var:,} SOS variables have ZERO variable_state "
        "rows (A4.3b gap — A4.4 curation, see sos_deldatamangd_unresolved)"
    )
    empty_regs = conn.execute(
        "SELECT r.name FROM register r WHERE r.provider_id = ? "
        "AND NOT EXISTS (SELECT 1 FROM variable v "
        "JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "WHERE v.register_id = r.register_id) "
        "ORDER BY r.name",
        (PROVIDER_ID_SOS,),
    ).fetchall()
    if empty_regs:
        names = ", ".join(r[0] for r in empty_regs)
        result.info(
            f"{len(empty_regs)} SOS register(s) with ZERO states across all "
            f"variables: {names}"
        )


# Real-corpus floors for the derived concept-group layer (#303). The EDGE
# dimension carries a corpus VOLUME floor (`_CG_MIN_EDGE_GROUPS` below): the old
# exact-parity check recomputed the components from the persisted
# `same_definition_different_column` rows, but #591 retired those rows (the fold
# now reads the in-build sibling sets), so there is nothing to recompute against
# — a volume floor replaces it, catching a derivation collapse (slug drift, an
# empty `edge_siblings`) without false-failing on legitimate corpus churn or the
# new curated-precedence exclusions. There is NO token month-group floor here:
# the #319/#383 family merge runs before the concept-group month-fold and
# consumes every month-suffixed family, so the 8 monthly families are guarded at
# their true home — `_check_variable_alias_window` (>= `_AW_MIN_MERGED_FAMILIES`
# survivors). The curated floor below stays absolute: its candidate set isn't
# recomputable without replaying the vocabulary guards, and the family is a small
# enumerable fact (1 curated family measured 2026-06-11). Classification VINTAGE
# families no longer fold into concept groups (#571) — they materialize as
# succession edges, floored in `_check_classification_replaced_by` (the lkf
# vintage chain ships the bulk of those edges), so there is no longer a
# classification-vintage-group floor here.

# Edge-group volume floor (#591, corpus only): the within-register
# split-sibling components dominate the variable concept groups — a real-corpus
# build measured 2,191 edge groups (CONFIRMED 2026-06-19 on the current corpus).
# The floor sits at ~82% of that count: low enough to catch a derivation collapse
# — an empty `edge_siblings`, a slug regression that drops every endpoint — and
# high enough to leave headroom for the curated-precedence exclusions (#488 will
# re-home a handful of components, never thousands) and routine corpus churn.
# Synthetic builds carry few/no sibling edges, so the floor is corpus-gated; it is
# additionally gated on SCB being in the build (#595) — a non-SCB `--providers`
# subset carries no split siblings and would false-fail.
_CG_MIN_EDGE_GROUPS = 1800

# Classification succession floor (#571, corpus only): the lkf vintage chain
# (lkf1980…lkf2026, ~47 editions → ~46 adjacent edges) dominates the corpus
# succession edges, so a real build that newly stopped deriving them (slug-tail
# drift, name-guard regression) drops well below this floor. Synthetic builds
# carry no vintage classifications, so the floor is corpus-gated; it is additionally
# gated on SCB being in the build (#595) — a non-SCB `--providers` subset carries no
# lkf chain and would false-fail.
_CG_MIN_CLASSIFICATION_SUCCESSION_EDGES = 40

# Variable vintage-lift floor (#584, corpus only): the clean tier lifts same-name
# families (one variable per edition) from `classification_replaced_by` editions to
# the variable grain, ~32 derived edges on the current corpus (the issue estimated
# ~53; the conservative bijection — gaps and edition-spanning variables excluded —
# lands lower). A floor well under the measured count catches a regression that
# silently stops lifting (classification-binding backfill drift, bijection-guard
# inversion) without false-failing on legitimate corpus churn (the entangled tier
# moving in/out). Synthetic builds carry no vintage classifications, so the floor is
# corpus-gated; it is additionally gated on SCB being in the build (#595) — a non-SCB
# `--providers` subset carries no SCB-derived lifts and would false-fail.
_MIN_VARIABLE_VINTAGE_LIFT_EDGES = 25


def _scb_in_build(conn: sqlite3.Connection) -> bool:
    """True iff the SCB provider has built register rows in this DB.

    The corpus volume floors below all source their bulk from SCB (within-register
    split siblings, the lkf vintage chain, SCB classification-derived vintage lifts),
    so a `--providers` subset that excludes SCB legitimately carries only a handful
    of those rows. `build-db` always validates `corpus=True` regardless of
    `--providers`, so the floors must SKIP (not false-fail) when SCB wasn't built
    (#595, mirroring #563's gate-to-built-providers precedent).

    Gate on REGISTER rows, not the `provider` row: `seed_providers` inserts the SCB
    provider row regardless of `--providers`, so provider-row presence is not enough;
    SCB registers exist only when the SCB adapter actually ran. The check is
    independent of the floored counts, so a real derivation collapse on a full SCB
    build still fails the floor.
    """
    return (
        conn.execute(
            "SELECT 1 FROM register WHERE provider_id = ? LIMIT 1",
            (PROVIDER_ID_SCB,),
        ).fetchone()
        is not None
    )


def _check_concept_groups(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    *,
    corpus: bool,
) -> None:
    """#303 derived concept-group invariants (presentation-only layer; see
    `concept_groups.py`).

    Structural (always): the member-kind wiring is consistent (variable
    members point at `kind='variable'` groups in the SAME register;
    classification members at `kind='classification'` groups) and every group
    has >= 2 members — a 1-member group is a derivation bug (the passes only
    mint groups from >= 2 candidates; a curated family authored with a single
    member, or an `exclude` that drops a family below two, would surface here).

    Corpus (real build only): volume floors per derivation source, so a pass
    that silently stops matching (slug-vocabulary drift, edge-kind rename)
    fails the gate instead of shipping an ungrouped browse."""
    result.section("[concept groups]")
    required = {
        "concept_group",
        "concept_group_variable",
        "concept_group_classification",
    }
    missing_tables = required - tables
    if missing_tables:
        for name in sorted(missing_tables):
            result.fail(f"{name} missing (schema 5.3.0 concept-group layer)")
        return

    undersized = conn.execute(
        "SELECT COUNT(*) FROM concept_group g WHERE "
        "(SELECT COUNT(*) FROM concept_group_variable m "
        " WHERE m.group_id = g.group_id) + "
        "(SELECT COUNT(*) FROM concept_group_classification c "
        " WHERE c.group_id = g.group_id) < 2"
    ).fetchone()[0]
    if undersized:
        result.fail(f"{undersized} concept group(s) with < 2 members")
    else:
        n_groups = conn.execute("SELECT COUNT(*) FROM concept_group").fetchone()[0]
        result.ok(f"all {n_groups:,} concept groups have >= 2 members")

    kind_mismatch = conn.execute(
        "SELECT "
        "(SELECT COUNT(*) FROM concept_group_variable m "
        " JOIN concept_group g ON g.group_id = m.group_id "
        " WHERE g.kind != 'variable') + "
        "(SELECT COUNT(*) FROM concept_group_classification c "
        " JOIN concept_group g ON g.group_id = c.group_id "
        " WHERE g.kind != 'classification')"
    ).fetchone()[0]
    if kind_mismatch:
        result.fail(f"{kind_mismatch} group member(s) under a wrong-kind group")
    else:
        result.ok("member tables wire to matching-kind groups")

    cross_register = conn.execute(
        "SELECT COUNT(*) FROM concept_group_variable m "
        "JOIN concept_group g ON g.group_id = m.group_id "
        "JOIN variable v ON v.variable_id = m.variable_id "
        "WHERE v.register_id != g.register_id"
    ).fetchone()[0]
    if cross_register:
        result.fail(
            f"{cross_register} variable member(s) outside their group's register"
        )
    else:
        result.ok("variable members stay in their group's register")

    # #585 inline-facet invariant: a variable member's facet_value/facet_label
    # are non-NULL iff its group has a non-NULL facet_axis (token/curated), and
    # NULL for edge groups (facet_axis NULL). A member half-set (one of
    # value/label NULL) or set against a NULL-axis group (or vice versa) is a
    # materializer bug — single-axis is schema-shaped now, but the NULLability
    # contract still needs asserting.
    facet_mismatch = conn.execute(
        "SELECT COUNT(*) FROM concept_group_variable m "
        "JOIN concept_group g ON g.group_id = m.group_id "
        "WHERE (g.facet_axis IS NULL) != "
        "      (m.facet_value IS NULL AND m.facet_label IS NULL) "
        "   OR (m.facet_value IS NULL) != (m.facet_label IS NULL)"
    ).fetchone()[0]
    if facet_mismatch:
        result.fail(
            f"{facet_mismatch} variable member(s) violate the facet/axis "
            "NULLability invariant (#585)"
        )
    else:
        result.ok("variable member facets agree with their group's facet_axis")

    if not corpus:
        return
    by_source = {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            "SELECT source, kind, COUNT(*) FROM concept_group GROUP BY source, kind"
        )
    }
    n_edge = by_source.get(("edge", "variable"), 0)
    n_month = by_source.get(("token", "variable"), 0)
    n_curated = by_source.get(("curated", "variable"), 0)
    # Edge-group volume floor (#591): replaces the retired exact-parity check —
    # the foldable sibling rows are no longer persisted, so there's nothing to
    # recompute; a volume floor catches a derivation collapse instead. Gated on
    # SCB presence (#595): the split-sibling bulk is SCB-sourced, so a non-SCB
    # `--providers` subset SKIPS rather than false-fails this floor.
    if not _scb_in_build(conn):
        result.info(
            f"{n_edge:,} edge group(s) — SCB not in this build, floor "
            f"(>= {_CG_MIN_EDGE_GROUPS:,}) skipped (#595)"
        )
    elif n_edge >= _CG_MIN_EDGE_GROUPS:
        result.ok(f"{n_edge:,} edge group(s) (>= {_CG_MIN_EDGE_GROUPS:,})")
    else:
        result.fail(
            f"{n_edge:,} edge group(s) (< {_CG_MIN_EDGE_GROUPS:,}) — edge "
            "derivation collapse (empty sibling sets / slug regression)?"
        )
    # No month-token-group floor: the #319/#383 family merge runs BEFORE the
    # concept-group month-fold and consumes every month-suffixed family in the
    # corpus, so `source='token'` month groups are 0 by design (a non-zero count
    # would mean family-merge silently stopped merging). The merged families are
    # guarded at their true home — `_check_variable_alias_window` (>= N survivors).
    result.info(f"{n_month} token month group(s) (superseded by family merge)")
    if n_curated >= 1:
        result.ok(f"{n_curated} curated group(s) (>= 1)")
    else:
        result.fail("no curated concept groups (concept_groups.toml not applied?)")
    # Derived (`source='token'`) classification vintage families no longer fold
    # here (#571) — they materialize as succession edges, asserted-empty
    # (structural) above and floored in `_check_classification_replaced_by`. Only
    # the TOKEN source is derived; CURATED umbrella classification groups (#516,
    # e.g. a SUN group with `source='curated'`) are intentionally retained and
    # must be allowed through. The corpus build carries zero token classification
    # groups now.
    n_token_cls = conn.execute(
        "SELECT COUNT(*) FROM concept_group "
        "WHERE kind = 'classification' AND source = 'token'"
    ).fetchone()[0]
    if n_token_cls == 0:
        result.ok(
            "no derived (token) classification concept groups (#571 succession edges)"
        )
    else:
        result.fail(
            f"{n_token_cls} derived (token) classification concept group(s) present "
            "— the #571 vintage groups should have become succession edges "
            "(curated umbrella classification groups are permitted)"
        )


def _check_classification_replaced_by(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    *,
    corpus: bool,
) -> None:
    """#571 classification EDITION succession invariants.

    Structural (always): the table exists, every edge is directional and
    non-self (predecessor != successor) and slug-anchored to live classification
    slugs.

    Corpus (real build only): the lkf vintage chain dominates, so a real build
    must carry >= `_CG_MIN_CLASSIFICATION_SUCCESSION_EDGES` edges — a pass that
    silently stops deriving (slug-tail drift, name-guard regression) fails the
    gate. Synthetic builds carry no vintage classifications, so this floor is
    corpus-gated."""
    result.section("[classification succession]")
    if "classification_replaced_by" not in tables:
        result.fail("classification_replaced_by missing (schema 5.5.0 #571 table)")
        return

    self_loops = conn.execute(
        "SELECT COUNT(*) FROM classification_replaced_by "
        "WHERE predecessor_slug = successor_slug"
    ).fetchone()[0]
    if self_loops:
        result.fail(f"{self_loops} self-loop succession edge(s)")
    else:
        result.ok("no self-loop succession edges")

    dangling = conn.execute(
        "SELECT COUNT(*) FROM classification_replaced_by e "
        "WHERE NOT EXISTS (SELECT 1 FROM classification c WHERE c.slug = e.predecessor_slug) "
        "   OR NOT EXISTS (SELECT 1 FROM classification c WHERE c.slug = e.successor_slug)"
    ).fetchone()[0]
    if dangling:
        result.fail(
            f"{dangling} succession edge(s) reference an unknown classification slug"
        )
    else:
        result.ok("succession edges resolve to live classification slugs")

    # #579: `classification.supersedes_id` is a DERIVED projection of this table
    # (`derive_supersedes_from_edges`), not a seed field — so the two must agree.
    # (1) every non-NULL supersedes_id must back onto an edge whose
    # predecessor_slug/successor_slug match the classification pair; (2) every
    # classification that IS a successor of >= 1 edge must carry a non-NULL
    # supersedes_id. A mismatch means the derive pass drifted or was skipped.
    orphan_ptr = conn.execute(
        """
        SELECT COUNT(*) FROM classification c
        JOIN classification p ON p.id = c.supersedes_id
        WHERE NOT EXISTS (
            SELECT 1 FROM classification_replaced_by e
            WHERE e.predecessor_slug = p.slug AND e.successor_slug = c.slug
        )
        """
    ).fetchone()[0]
    missing_ptr = conn.execute(
        """
        SELECT COUNT(*) FROM classification c
        WHERE c.supersedes_id IS NULL
          AND EXISTS (
              SELECT 1 FROM classification_replaced_by e
              WHERE e.successor_slug = c.slug
          )
        """
    ).fetchone()[0]
    if orphan_ptr or missing_ptr:
        result.fail(
            f"supersedes_id out of sync with classification_replaced_by "
            f"({orphan_ptr} pointer(s) with no backing edge, "
            f"{missing_ptr} successor(s) missing a derived pointer)"
        )
    else:
        result.ok("supersedes_id is a faithful projection of succession edges")

    n_edges = conn.execute(
        "SELECT COUNT(*) FROM classification_replaced_by"
    ).fetchone()[0]
    if not corpus:
        result.info(f"{n_edges} classification succession edge(s)")
        return
    # Gated on SCB presence (#595): the lkf vintage chain is SCB-sourced, so a
    # non-SCB `--providers` subset SKIPS rather than false-fails this floor.
    if not _scb_in_build(conn):
        result.info(
            f"{n_edges} classification succession edge(s) — SCB not in this build, "
            f"floor (>= {_CG_MIN_CLASSIFICATION_SUCCESSION_EDGES}) skipped (#595)"
        )
    elif n_edges >= _CG_MIN_CLASSIFICATION_SUCCESSION_EDGES:
        result.ok(
            f"{n_edges} classification succession edge(s) "
            f"(>= {_CG_MIN_CLASSIFICATION_SUCCESSION_EDGES})"
        )
    else:
        result.fail(
            f"{n_edges} classification succession edge(s) "
            f"(< {_CG_MIN_CLASSIFICATION_SUCCESSION_EDGES}) — vintage-chain "
            "derivation regression?"
        )


def _check_variable_replaced_by_vintage_lift(
    conn: sqlite3.Connection,
    result: ValidationResult,
    tables: set[str],
    *,
    corpus: bool,
) -> None:
    """#584 derived variable vintage-succession invariants — the clean-tier lift
    of `classification_replaced_by` editions to the variable grain (rows in
    `variable_replaced_by` with `note = 'derived:classification_vintage_lift'`).

    Structural (always): every derived edge is directional and non-self
    (predecessor != successor) and both endpoints resolve to live, slugged
    variables (a derived edge MUST point at real variables — unlike a curated
    succession whose predecessor may be dead).

    Corpus (real build only): the clean tier carries >=
    `_MIN_VARIABLE_VINTAGE_LIFT_EDGES` derived edges, so a pass that silently
    stops lifting (classification-binding backfill drift, bijection-guard
    regression) fails the gate. Synthetic builds carry no vintage classifications,
    so this floor is corpus-gated."""
    result.section("[variable vintage lift]")
    if "variable_replaced_by" not in tables:
        result.fail("variable_replaced_by missing")
        return

    note = _REPLACED_BY_NOTE_VINTAGE_LIFT
    self_loops = conn.execute(
        "SELECT COUNT(*) FROM variable_replaced_by "
        "WHERE note = ? "
        "  AND predecessor_provider = successor_provider "
        "  AND predecessor_register = successor_register "
        "  AND predecessor_variable = successor_variable",
        (note,),
    ).fetchone()[0]
    if self_loops:
        result.fail(f"{self_loops} self-loop vintage-lift edge(s)")
    else:
        result.ok("no self-loop vintage-lift edges")

    # Both endpoints must resolve to a live, slugged variable (FQID grain).
    dangling = conn.execute(
        "SELECT COUNT(*) FROM variable_replaced_by e "
        "WHERE e.note = ? AND ("
        "  NOT EXISTS ("
        "    SELECT 1 FROM variable v "
        "    JOIN register r ON v.register_id = r.register_id "
        "    JOIN provider p ON r.provider_id = p.provider_id "
        "    WHERE p.slug = e.predecessor_provider "
        "      AND r.slug = e.predecessor_register "
        "      AND v.slug = e.predecessor_variable"
        "  ) OR NOT EXISTS ("
        "    SELECT 1 FROM variable v "
        "    JOIN register r ON v.register_id = r.register_id "
        "    JOIN provider p ON r.provider_id = p.provider_id "
        "    WHERE p.slug = e.successor_provider "
        "      AND r.slug = e.successor_register "
        "      AND v.slug = e.successor_variable"
        "  ))",
        (note,),
    ).fetchone()[0]
    if dangling:
        result.fail(
            f"{dangling} vintage-lift edge(s) reference an unknown variable slug"
        )
    else:
        result.ok("vintage-lift edges resolve to live variable slugs")

    n_edges = conn.execute(
        "SELECT COUNT(*) FROM variable_replaced_by WHERE note = ?", (note,)
    ).fetchone()[0]
    if not corpus:
        result.info(f"{n_edges} variable vintage-lift edge(s)")
        return
    # Gated on SCB presence (#595): the SCB classification-derived lifts are
    # SCB-sourced, so a non-SCB `--providers` subset SKIPS rather than
    # false-fails this floor.
    if not _scb_in_build(conn):
        result.info(
            f"{n_edges} variable vintage-lift edge(s) — SCB not in this build, "
            f"floor (>= {_MIN_VARIABLE_VINTAGE_LIFT_EDGES}) skipped (#595)"
        )
    elif n_edges >= _MIN_VARIABLE_VINTAGE_LIFT_EDGES:
        result.ok(
            f"{n_edges} variable vintage-lift edge(s) "
            f"(>= {_MIN_VARIABLE_VINTAGE_LIFT_EDGES})"
        )
    else:
        result.fail(
            f"{n_edges} variable vintage-lift edge(s) "
            f"(< {_MIN_VARIABLE_VINTAGE_LIFT_EDGES}) — vintage-lift derivation "
            "regression?"
        )


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
