"""Tests for the value-set dedup validator.

Exercises the module-level entry point (`validate_built_db`) and the
argparse wiring for `reg-meta-build build-db` (validates by default,
opt out with `--no-validate`). The CLI
handler itself is two lines of glue around `validate_built_db` and
`RegMetaError`; the validator module is the part with logic worth
testing in depth.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from _shared_fixtures import connect_built_db
from reg_meta_build.validate import validate_built_db


class TestValidateModule:
    def test_passes_on_fresh_fixture_db(self, fixture_db: Path):
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[schema]" in report
        assert "[OK] value_set present" in report

    def test_value_code_search_checks_pass(self, fixture_db: Path):
        """#352: the schema-shape + value-code-search sections recognize
        value_code.mapping_count and value_code_fts on a fresh build."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[OK] value_code.mapping_count present" in report
        assert "[OK] value_code_fts present" in report
        assert "[value-code search]" in report

    def test_classification_succession_section_renders(self, fixture_db: Path):
        """#571: the classification-succession structural section runs on a fresh
        synthetic build (corpus=False) so a regression can't drop it silently."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert "[classification succession]" in result.format_report()

    def test_representation_succession_section_renders(self, fixture_db: Path):
        """#843: the representation-succession structural section runs on a fresh
        synthetic build (corpus=False). The grain ships empty (curated-only, no
        edges until #846/#838), so the section reports 0 edges (info) and passes —
        a regression can't drop the section silently."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert "[representation succession]" in result.format_report()

    def test_variable_vintage_lift_section_renders(self, fixture_db: Path):
        """#584: the variable vintage-lift structural section runs on a fresh
        synthetic build (corpus=False) — the synthetic corpus carries no vintage
        classifications, so the section reports 0 edges (info) and passes."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert "[variable vintage lift]" in result.format_report()

    def test_corpus_volume_floors_skip_when_scb_absent(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#595: the four SCB-sourced corpus volume floors (edge-group,
        classification-succession, variable-vintage-lift, merged-monthly-family) must
        SKIP (report info, not FAIL) when SCB isn't in the build — `build-db` always
        validates `corpus=True` regardless of `--providers`, so a non-SCB subset must not
        false-fail. Simulate "SCB not built" by deleting the SCB register rows
        (a real non-SCB `--providers` build has no SCB registers) so
        `_scb_in_build` returns False, then call each check directly with
        corpus=True and assert (a) the skip text renders and (b) the gated floor's
        FAIL substring is absent. Mirrors #563's gate-to-built-providers
        precedent."""
        from reg_meta_build.db import PROVIDER_ID_SCB
        from reg_meta_build.validate import (
            ValidationResult,
            _check_classification_replaced_by,
            _check_concept_groups,
            _check_variable_alias_window,
            _check_variable_replaced_by_vintage_lift,
            _scb_in_build,
        )

        broken = tmp_path / "noscb.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = connect_built_db(broken)
        conn.row_factory = sqlite3.Row
        # Precondition: the fixture IS an SCB build.
        assert _scb_in_build(conn)
        conn.execute("DELETE FROM register WHERE provider_id = ?", (PROVIDER_ID_SCB,))
        conn.commit()
        assert not _scb_in_build(conn)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

        # Each tuple: the check, and the gated floor's FAIL substring that must be
        # ABSENT when SCB is gated out. `_check_concept_groups` is asserted on the
        # specific edge-floor substring (not blanket `passed`) because its corpus
        # call also fails the unrelated curated floor, which the #595 gate leaves
        # running.
        cases = (
            (_check_concept_groups, "edge derivation collapse"),
            (
                _check_classification_replaced_by,
                "vintage-chain derivation regression",
            ),
            (
                _check_variable_replaced_by_vintage_lift,
                "vintage-lift derivation regression",
            ),
            (_check_variable_alias_window, "family-merge regression"),
        )
        for check, gated_floor_fail in cases:
            result = ValidationResult()
            check(conn, result, tables, corpus=True)
            report = result.format_report()
            assert "SCB not in this build" in report, (check.__name__, report)
            assert not any(gated_floor_fail in f for f in result.failures), (
                check.__name__,
                result.failures,
            )
        conn.close()

    def test_edge_group_floor_fires_when_scb_present(self, fixture_db: Path):
        """#595: the gate must NOT neuter the floor when SCB IS built. The
        synthetic fixture is an SCB build but carries no split-sibling edge groups
        (well below 1,800), so `_check_concept_groups` with corpus=True must emit
        the edge-derivation-collapse FAIL — assert on that SPECIFIC substring (the
        corpus call also fails the curated floor, so blanket `passed` is wrong)."""
        from reg_meta_build.validate import ValidationResult, _check_concept_groups

        conn = sqlite3.connect(fixture_db)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_concept_groups(conn, result, tables, corpus=True)
        conn.close()
        assert any("edge derivation collapse" in f for f in result.failures), (
            result.failures
        )

    def test_classification_succession_floor_fires_when_scb_present(
        self, fixture_db: Path
    ):
        """#595: with SCB built but no vintage classifications (synthetic), the
        classification-succession floor must FAIL — the gate preserves the floor."""
        from reg_meta_build.validate import (
            ValidationResult,
            _check_classification_replaced_by,
        )

        conn = sqlite3.connect(fixture_db)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_classification_replaced_by(conn, result, tables, corpus=True)
        conn.close()
        assert any(
            "vintage-chain derivation regression" in f for f in result.failures
        ), result.failures

    def test_variable_vintage_lift_floor_fires_when_scb_present(self, fixture_db: Path):
        """#595: with SCB built but no vintage classifications (synthetic), the
        variable-vintage-lift floor must FAIL — the gate preserves the floor."""
        from reg_meta_build.validate import (
            ValidationResult,
            _check_variable_replaced_by_vintage_lift,
        )

        conn = sqlite3.connect(fixture_db)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_variable_replaced_by_vintage_lift(conn, result, tables, corpus=True)
        conn.close()
        assert any(
            "vintage-lift derivation regression" in f for f in result.failures
        ), result.failures

    def test_monthly_family_floor_fires_when_scb_present(self, fixture_db: Path):
        """#595: with SCB built but no merged monthly families (synthetic builds
        carry no `period_family_merges.toml`, so 0 windows < floor of 8), the
        merged-monthly-family floor must FAIL — the gate preserves the floor."""
        from reg_meta_build.validate import (
            ValidationResult,
            _check_variable_alias_window,
        )

        conn = connect_built_db(fixture_db)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_variable_alias_window(conn, result, tables, corpus=True)
        conn.close()
        assert any("family-merge regression" in f for f in result.failures), (
            result.failures
        )

    def test_curated_floor_skips_when_no_scb_sos_source(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#600: the curated concept-group floor (`n_curated >= 1`) asserts
        `concept_groups.toml` was applied, but that file curates only scb/* and
        sos/* register families. A thin-provider-only build (no scb AND no sos
        registers) legitimately has zero curated groups and must SKIP (report info)
        rather than false-fail. Simulate it by deleting all scb+sos registers so
        `_curated_source_in_build` returns False, then assert (a) the skip text
        renders and (b) the curated-floor FAIL substring is ABSENT. Surfaced once
        #597 let non-SCB builds reach validation."""
        from reg_meta_build.db import PROVIDER_ID_SCB, PROVIDER_ID_SOS
        from reg_meta_build.validate import (
            ValidationResult,
            _check_concept_groups,
            _curated_source_in_build,
        )

        broken = tmp_path / "noscbsos.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.row_factory = sqlite3.Row
        # Precondition: the fixture carries a curated (scb/sos) source.
        assert _curated_source_in_build(conn)
        conn.execute(
            "DELETE FROM register WHERE provider_id IN (?, ?)",
            (PROVIDER_ID_SCB, PROVIDER_ID_SOS),
        )
        conn.commit()
        assert not _curated_source_in_build(conn)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_concept_groups(conn, result, tables, corpus=True)
        conn.close()
        report = result.format_report()
        assert "no scb/sos source in this build" in report, report
        # The gated curated floor must NOT have fired (assert on its specific
        # substring — the corpus call also hits the edge floor etc.).
        assert not any("no curated concept groups" in f for f in result.failures), (
            result.failures
        )

    def test_curated_floor_fires_when_scb_present(self, fixture_db: Path):
        """#600: the gate must NOT neuter the floor when a curated (scb/sos) source
        IS built. The synthetic fixture is an scb build but carries zero curated
        concept groups, so `_check_concept_groups` with corpus=True must emit the
        curated-floor FAIL — assert on that SPECIFIC substring."""
        from reg_meta_build.validate import ValidationResult, _check_concept_groups

        conn = sqlite3.connect(fixture_db)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_concept_groups(conn, result, tables, corpus=True)
        conn.close()
        assert any("no curated concept groups" in f for f in result.failures), (
            result.failures
        )

    @staticmethod
    def _seed_classification(
        conn: sqlite3.Connection, short_name: str, slug: str
    ) -> None:
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
            (short_name, short_name, slug),
        )

    def test_classification_succession_self_loop_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#571: a self-loop succession edge (predecessor == successor) fails the
        structural check."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._seed_classification(conn, "SSYK2012", "ssyk2012")
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('ssyk2012', 'ssyk2012', 2020, 'derived:vintage_chain')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("self-loop succession edge" in f for f in result.failures)

    def test_classification_succession_dangling_slug_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#571: a succession edge pointing at an unknown classification slug
        fails the structural resolution check."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._seed_classification(conn, "SSYK1996", "ssyk1996")
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('ssyk1996', 'no-such-slug-9999', 2020, 'derived:vintage_chain')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("unknown classification slug" in f for f in result.failures)

    def test_representation_succession_self_loop_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#843: a representation succession edge whose full
        `(provider, register, variable, column)` endpoint is identical on both
        sides is a self-loop and fails the structural check (same variable /
        DIFFERENT column would be legal)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # scb/testreg/kon carries the observed delivery column `Kon` — a real,
        # otherwise-resolvable endpoint, repeated on both sides → self-loop.
        conn.execute(
            "INSERT INTO representation_replaced_by "
            "(predecessor_provider, predecessor_register, predecessor_variable, "
            "predecessor_column, successor_provider, successor_register, "
            "successor_variable, successor_column, effective_year, note) "
            "VALUES ('scb', 'testreg', 'kon', 'Kon', "
            "'scb', 'testreg', 'kon', 'Kon', 2010, 'curated:slug_toml')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "self-loop representation succession edge" in f for f in result.failures
        )

    def test_representation_succession_dangling_endpoint_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#843: a representation succession edge whose `predecessor_column` is not
        an OBSERVED `variable_alias.delivery_column_name` for that variable fails the
        structural endpoint-resolution check."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # scb/testreg/testcol observes `TestCol` + `TestKolumn`; `NoSuchColumn` is
        # not an observed delivery column → the predecessor endpoint is unknown.
        conn.execute(
            "INSERT INTO representation_replaced_by "
            "(predecessor_provider, predecessor_register, predecessor_variable, "
            "predecessor_column, successor_provider, successor_register, "
            "successor_variable, successor_column, effective_year, note) "
            "VALUES ('scb', 'testreg', 'testcol', 'NoSuchColumn', "
            "'scb', 'testreg', 'testcol', 'TestKolumn', 2010, 'curated:slug_toml')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "representation succession edge(s) reference an unknown" in f
            for f in result.failures
        )

    def test_dead_predecessor_edge_keeps_supersedes_null_without_missing_ptr(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#579 (forward-looking): a `classification_replaced_by` edge whose
        `predecessor_slug` has NO live `classification` row (a cross-provider /
        retired predecessor — allowed verbatim by the curated `relations.toml`
        arm) leaves the live successor's `supersedes_id` NULL, because
        `derive_supersedes_from_edges` joins on `p.slug = e.predecessor_slug` and
        finds no live row. The `supersedes_id`-projection guard must NOT count that
        as a `missing_ptr` failure — only successors of a LIVE-predecessor edge owe
        a non-NULL pointer.

        Call the check directly: a full `validate_built_db` run trips the upstream
        structural `dangling` check on the dead predecessor (a separate invariant),
        so we assert specifically that the `missing a derived pointer` substring is
        absent here."""
        from reg_meta_build.validate import (
            ValidationResult,
            _check_classification_replaced_by,
        )

        broken = tmp_path / "dead_pred.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.row_factory = sqlite3.Row
        # Live successor, supersedes_id NULL; predecessor slug is NOT a live row.
        self._seed_classification(conn, "SUN-NIVA2000", "sun-niva2000")
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('sun1996', 'sun-niva2000', 2000, 'curated:slug_toml')"
        )
        conn.commit()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_classification_replaced_by(conn, result, tables, corpus=False)
        conn.close()
        assert not any("missing a derived pointer" in f for f in result.failures), (
            result.failures
        )

    def test_variable_vintage_lift_self_loop_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#584: a derived vintage-lift edge whose predecessor FQID == successor
        FQID (a self-loop) fails the structural check. Both endpoints point at the
        SAME live, slugged variable so only the self-loop invariant trips."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # scb/testreg/kon is a real slugged variable in the fixture build.
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note) "
            "VALUES ('scb', 'testreg', 'kon', 'scb', 'testreg', 'kon', "
            "2012, 'derived:classification_vintage_lift')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("self-loop vintage-lift edge" in f for f in result.failures)

    def test_variable_vintage_lift_dangling_slug_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#584: a derived vintage-lift edge pointing at a non-existent variable
        slug fails the structural resolution check (a derived edge MUST point at
        live, slugged variables — unlike a curated succession)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # Real predecessor (scb/testreg/kon), bogus successor variable slug.
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note) "
            "VALUES ('scb', 'testreg', 'kon', 'scb', 'testreg', "
            "'no-such-var-9999', 2012, 'derived:classification_vintage_lift')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("unknown variable slug" in f for f in result.failures)

    def test_missing_value_code_fts_surfaces_failure(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#352: dropping value_code_fts must fail the schema-shape check."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute("DROP TABLE value_code_fts")
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("value_code_fts missing" in f for f in result.failures)

    def test_empty_fts_index_fails_corpus_check(self, fixture_db: Path, tmp_path: Path):
        """#352: a populated-schema build whose value_code_fts INDEX is empty (table
        present, content cleared) must FAIL the corpus value-code-search check. Uses
        the FTS5 'delete-all' command so the table stays but the `_docsize` shadow
        count drops to 0 — the honest indexed-row count the check reads (COUNT(*)
        would still read the content table and miss this). Calls the check directly
        so the synthetic fixture's SOS-volume corpus gate doesn't muddy the result."""
        from reg_meta_build.validate import ValidationResult, _check_value_code_search

        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.row_factory = sqlite3.Row
        # Precondition: the index is non-empty before we clear it.
        assert (
            conn.execute("SELECT COUNT(*) FROM value_code_fts_docsize").fetchone()[0]
            > 0
        )
        conn.execute("INSERT INTO value_code_fts(value_code_fts) VALUES('delete-all')")
        conn.commit()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_value_code_search(conn, result, tables, corpus=True)
        conn.close()
        assert not result.passed
        assert any("EMPTY" in f for f in result.failures), result.failures

    def test_missing_db_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            validate_built_db(tmp_path / "no_such.db")

    def test_dropped_value_set_table_surfaces_failures(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A DB missing `value_set` must fail the schema-shape check
        without crashing the dependent projection queries."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute("DROP TABLE value_set_member")
        conn.execute("DROP TABLE value_set")
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("value_set missing" in f for f in result.failures)
        assert any("value_set_member missing" in f for f in result.failures)

    def test_legacy_table_resurrection_is_failure(
        self, fixture_db: Path, tmp_path: Path
    ):
        """The schema invariant requires `cvid_value_code` / `value_item` /
        `value_item_validity` to be absent post-rebuild."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute("CREATE TABLE cvid_value_code (cvid INTEGER)")
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "cvid_value_code should have been dropped" in f for f in result.failures
        )

    def test_state_value_set_with_no_codes_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the projection-integrity check FAILs when a `variable_state`
        names a `value_set` that yields zero codes (a dangling year-projection
        link), not a legitimately code-less state (NULL value_set_id).

        Mint an empty value_set and point an existing state at it."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # An empty value_set (a row with no value_set_member children).
        conn.execute("INSERT INTO value_set (member_hash) VALUES (?)", (b"\xee" * 32,))
        empty_vs = conn.execute("SELECT MAX(value_set_id) FROM value_set").fetchone()[0]
        # Point one state at it → projection yields zero codes for that state.
        state_id = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[
            0
        ]
        conn.execute(
            "UPDATE variable_state SET value_set_id = ? WHERE state_id = ?",
            (empty_vs, state_id),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "yield" in f and "no projected codes" in f for f in result.failures
        ), result.failures

    def test_open_ended_sentinel_passes_and_reports_on_fixture(self, fixture_db: Path):
        """The sentinel-exactness check runs on the fixture and emits its
        section, so a regression can't silently drop it from the suite."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert "[window: open-ended valid_to sentinel]" in result.format_report()

    def test_codeless_codebearing_overlap_section_passes_on_fixture(
        self, fixture_db: Path
    ):
        """The code-less↔code-bearing overlap guard (#858) runs on the fixture and
        emits its section, so a regression can't silently drop it; the close-out
        keeps the fixture clean so it reports OK."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert (
            "[invariant: no code-less ↔ code-bearing window overlap]"
            in result.format_report()
        )

    def test_codeless_codebearing_overlap_fails(self, fixture_db: Path, tmp_path: Path):
        """The #858 backstop: a code-less state (`value_set_id IS NULL`)
        overlapping a code-bearing state on the same
        `(variable, variant, delivery_column)` must FAIL. Injected directly into
        `variable_state` (bypassing the build close-out), proving the guard is a
        real regression backstop — not a no-op that only the close-out satisfies."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # Take an existing code-bearing state and clone a code-less twin onto the
        # same key with an overlapping window (a NULL value_set_id, a distinct
        # valid_from / version_label so the uniqueness index doesn't collide).
        cb = conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name, "
            "       valid_from, valid_to "
            "FROM variable_state WHERE value_set_id IS NOT NULL LIMIT 1"
        ).fetchone()
        assert cb is not None, "fixture has no code-bearing state to clone"
        variable_id, register_variant_id, column, valid_from, valid_to = cb
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            " delivery_column_name, value_set_id, value_set_version_label) "
            "VALUES (?, ?, '0001-01-01', ?, ?, NULL, 'codeless-twin')",
            (variable_id, register_variant_id, valid_to, column),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("code-less" in f and "code-bearing" in f for f in result.failures), (
            result.failures
        )

    def test_near_sentinel_state_valid_to_fails(self, fixture_db: Path, tmp_path: Path):
        """A 9999-prefixed `variable_state.valid_to` that is not exactly
        '9999-12-31' must FAIL: downstream display (reg_webapp catalog routes
        + the SPA's formatWindow) branches on the exact literal, so a
        near-sentinel like '9999-06-30' would render as a garbage period
        token instead of an open window."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        state_id = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[
            0
        ]
        conn.execute(
            "UPDATE variable_state SET valid_to = '9999-06-30' WHERE state_id = ?",
            (state_id,),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "variable_state" in f and "9999-06-30" in f for f in result.failures
        ), result.failures

    def test_near_sentinel_lineage_valid_to_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """Same exactness invariant on `variable_state_lineage.valid_to` (the
        lineage edge windows carry the same '9999-12-31' open-end sentinel).

        Plants '9999-00-00': malformed, sorts BELOW '9999-01-01', and lineage
        has no date CHECK at all — so this locks the prefix match (a
        `>= '9999-01-01'` range predicate would miss it)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        state_id = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[
            0
        ]
        conn.execute(
            "INSERT INTO variable_state_lineage "
            "(consumer_state_id, source_state_id, valid_from, valid_to) "
            "VALUES (?, ?, '2000-01-01', '9999-00-00')",
            (state_id, state_id),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "variable_state_lineage" in f and "9999-00-00" in f for f in result.failures
        ), result.failures

    def test_var_year_codes_anchor_self_skips_on_fixture(self, fixture_db: Path):
        """A2.7: the var_id-24193 code-membership anchor self-skips cleanly when
        the var_id is absent (the synthetic fixture has no var_id 24193), so it
        never falses on a corpus that legitimately lacks the anchor variable.

        It still EMITS its section + an [OK] skip line so a future fixture that
        happens to grow the var_id can't silently drop the check."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[projection: var_id 24193 codes anchor]" in report
        assert "var_id 24193 not present" in report

    @staticmethod
    def _anchor_value_set(conn: sqlite3.Connection, codes: list[str]) -> int:
        """Mint a fresh value_set stocked with ``codes`` and return its id.
        ``value_code`` is content-addressed (UNIQUE code), so reuse-or-insert."""
        conn.execute("INSERT INTO value_set (member_hash) VALUES (?)", (b"\xab" * 32,))
        vs_id = conn.execute("SELECT MAX(value_set_id) FROM value_set").fetchone()[0]
        for code in codes:
            row = conn.execute(
                "SELECT code_id FROM value_code WHERE code = ?", (code,)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO value_code (code, label) VALUES (?, ?)", (code, code)
                )
                code_id = conn.execute(
                    "SELECT code_id FROM value_code WHERE code = ?", (code,)
                ).fetchone()[0]
            else:
                code_id = row[0]
            conn.execute(
                "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
                (vs_id, code_id),
            )
        return vs_id

    def _plant_anchor(self, conn: sqlite3.Connection, codes: list[str]) -> None:
        """Repoint variable_id=1 to provider_key 24193 and give its state a
        2010-overlapping window linked to a value_set carrying ``codes`` — so the
        anchor resolves the var_id → state → codes path exactly as it does on the
        real corpus."""
        # Match the anchor's (register 34, provider_key 24193) pin. validate runs
        # PRAGMA foreign_key_check (reports dangling FKs regardless of the
        # pragma), so register 34 must exist — insert it borrowing variable_id=1's
        # provider.
        prov = conn.execute(
            "SELECT r.provider_id FROM variable v "
            "JOIN register r ON v.register_id = r.register_id WHERE v.variable_id = 1"
        ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO register (register_id, provider_id, name, slug) "
            "VALUES (34, ?, 'Anchor register', 'anchor-reg')",
            (prov,),
        )
        conn.execute(
            "UPDATE variable SET provider_key = '24193', register_id = 34 "
            "WHERE variable_id = 1"
        )
        vs_id = self._anchor_value_set(conn, codes)
        conn.execute(
            "UPDATE variable_state SET valid_from = '2010-01-01', "
            "valid_to = '2010-12-31', value_set_id = ? WHERE variable_id = 1",
            (vs_id,),
        )

    def test_var_year_codes_anchor_passes_on_correct_codes(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the anchor PASSES when var_id 24193's 2010 state projects exactly
        the expected codes (01-04) and none of the forbidden ones (00/05)."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        self._plant_anchor(conn, ["01", "02", "03", "04"])
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "var_id 24193 year 2010 contains ['01', '02', '03', '04']" in report
        assert "var_id 24193 year 2010 excludes 00/05" in report

    def test_var_year_codes_anchor_fails_on_forbidden_code(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the anchor FAILs when the 2010 year-projection wrongly INCLUDES a
        forbidden code (05) — the wrong-code-membership bug class the corpus-wide
        >= 1-code check cannot catch (it would pass any non-empty projection)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._plant_anchor(conn, ["01", "02", "03", "04", "05"])
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("forbidden codes ['05']" in f for f in result.failures), (
            result.failures
        )

    def test_var_year_codes_anchor_fails_on_missing_code(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the anchor FAILs when an expected code (04) is dropped from the
        2010 projection — guards a year-projection that under-includes."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._plant_anchor(conn, ["01", "02", "03"])
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("missing codes ['04']" in f for f in result.failures), (
            result.failures
        )

    def test_var_year_codes_anchor_fails_when_present_but_no_year_overlap(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7 (Codex P2 #149): when var_id 24193 is PRESENT (register 34) but no
        `variable_state` overlaps the anchor year, that is a year-window/coalescing
        regression — a FAIL — not the 'variable absent' skip. Distinguishing the
        two is the whole point: a broken validity window must not masquerade as a
        legitimate skip on a corpus that does carry the anchor variable."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._plant_anchor(conn, ["01", "02", "03", "04"])
        # Shove the planted state's window off 2010 entirely: the variable
        # (register 34, provider_key 24193) still exists, but nothing overlaps.
        conn.execute(
            "UPDATE variable_state SET valid_from = '2015-01-01', "
            "valid_to = '2015-12-31' WHERE variable_id = 1"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "present but no state overlaps 2010" in f for f in result.failures
        ), result.failures

    def test_variable_alias_missing_state_column_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7 (Codex P2 #149): the invariant FAILs when a `variable_state`
        carries a delivery column absent from `variable_alias` — i.e. the reparent
        regressed and the catalog API would miss a column the data actively
        uses."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        sid = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[0]
        conn.execute(
            "UPDATE variable_state SET delivery_column_name = 'GHOSTCOL_NO_ALIAS' "
            "WHERE state_id = ?",
            (sid,),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("missing from variable_alias" in f for f in result.failures), (
            result.failures
        )

    def test_delivery_column_whitespace_fails(self, fixture_db: Path, tmp_path: Path):
        """Hygiene invariant: a delivery_column_name with surrounding
        whitespace (on a state OR an alias) fails — the SCB read boundary
        trims, so any padded value in a shipped DB is a build regression.
        Tab-padded deliberately: the check must match `str.strip()` semantics
        (all whitespace), not SQLite TRIM() (ASCII space only)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # Pad a matched alias+state pair in lockstep so only the hygiene
        # check (not the alias-covers-state-columns projection) fires.
        sid, col = conn.execute(
            "SELECT state_id, delivery_column_name FROM variable_state "
            "WHERE delivery_column_name IS NOT NULL LIMIT 1"
        ).fetchone()
        conn.execute(
            "UPDATE variable_state SET delivery_column_name = ? WHERE state_id = ?",
            (col + "\t", sid),
        )
        conn.execute(
            "UPDATE variable_alias SET delivery_column_name = ? "
            "WHERE delivery_column_name = ?",
            (col + "\t", col),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("surrounding whitespace" in f for f in result.failures), (
            result.failures
        )

    def test_empty_delivery_column_alias_fails(self, fixture_db: Path, tmp_path: Path):
        """Hygiene invariant: '' is not a delivery header — a no-header
        variable is a NULL state column + alias-row absence, never an
        empty-string alias row."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        vid, rvid = conn.execute(
            "SELECT variable_id, register_variant_id FROM variable_alias LIMIT 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, ?, '')",
            (vid, rvid),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("empty-string delivery_column_name" in f for f in result.failures), (
            result.failures
        )

    def test_name_field_whitespace_fails(self, fixture_db: Path, tmp_path: Path):
        """Hygiene invariant (#366): a variable/register/register_variant
        `name` with surrounding whitespace fails — the SCB read boundary
        trims, so any padded name in a shipped DB is a build regression.
        Tab-padded deliberately: the check must match `str.strip()` semantics
        (all whitespace), not SQLite TRIM() (ASCII space only)."""
        for table, column in (
            ("variable", "name"),
            ("register", "name"),
            ("register_variant", "name"),
        ):
            broken = tmp_path / f"broken_{table}.db"
            broken.write_bytes(fixture_db.read_bytes())
            conn = sqlite3.connect(broken)
            rowid, val = conn.execute(
                f"SELECT rowid, {column} FROM {table} "
                f"WHERE {column} IS NOT NULL LIMIT 1"
            ).fetchone()
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                (val + "\t", rowid),
            )
            conn.commit()
            conn.close()
            result = validate_built_db(broken)
            assert not result.passed, (table, column)
            assert any(
                f"{table}.{column} value(s) with surrounding whitespace" in f
                for f in result.failures
            ), (table, result.failures)

    def test_panel_refs_skip_when_no_variant_carries_them(self, fixture_db: Path):
        """A4.4c: the unmodified fixture carries NO panel refs (all variants have
        NULL panel keys), so the resolution check self-passes but still EMITS its
        section + an [OK] line so a future fixture that grows panel data can't
        silently drop the gate."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[panel: refs resolve to register-scoped variable slugs]" in report
        assert "no variant carries panel refs" in report

    def test_panel_ref_good_and_period_exempt_pass(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A4.4c: a `panel_entity_key` naming a real variable slug in the variant's
        register resolves, and the literal "period" `panel_time_key` sentinel is
        exempt (it's delivery-aligned time, not a variable slug). Variant 10 lives
        in register 1 (TESTREG), whose variables include slug `kon`."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'kon', "
            "panel_time_key = 'period' WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures
        assert "all 1 panel ref(s)" in result.format_report()

    def test_panel_ref_dangling_fails(self, fixture_db: Path, tmp_path: Path):
        """A4.4c: a `panel_time_key` naming a slug that exists in NO variable for
        the variant's register is a dangling reference — a FAIL."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_time_key = 'nonexistent_slug' "
            "WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_time_key 'nonexistent_slug' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures

    def test_panel_ref_wrong_register_fails(self, fixture_db: Path, tmp_path: Path):
        """A4.4c: slug is only register-unique. `parencol` exists under register 2
        (OTHERREG) but NOT register 1 (TESTREG). A panel ref on variant 10
        (register 1) pointing at `parencol` must FAIL — resolution is scoped to the
        variant's own register, so a cross-register slug is dangling."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'parencol' "
            "WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'parencol' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures

    def test_panel_ref_composite_array_element_miss_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A4.4c: a composite `panel_entity_key` is a json-array string; it resolves
        element-wise. ANY element that fails to resolve is a finding. Here `kon`
        resolves (register 1) but `ghost` does not — the array must FAIL on the
        bad element while leaving the good one alone."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = ? "
            "WHERE register_variant_id = 10",
            (json.dumps(["kon", "ghost"]),),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'ghost' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures
        # The resolving element must NOT produce a finding.
        assert not any("'kon' resolves to no" in f for f in result.failures), (
            result.failures
        )

    def test_panel_time_key_composite_resolves(self, fixture_db: Path, tmp_path: Path):
        """#567: a composite `panel_time_key` is a json-array string; it resolves
        element-wise like the composite entity key. Both `kon` and `testcol` exist
        under register 1, so the array fully resolves."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        conn.execute(
            "UPDATE register_variant SET panel_time_key = ? "
            "WHERE register_variant_id = 10",
            (json.dumps(["kon", "testcol"]),),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures

    def test_panel_time_key_composite_element_miss_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#567: a composite `panel_time_key` fails on ANY dangling element while
        leaving the resolving one alone (mirrors the composite entity-key check)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_time_key = ? "
            "WHERE register_variant_id = 10",
            (json.dumps(["kon", "ghost"]),),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_time_key 'ghost' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures
        assert not any(
            "panel_time_key 'kon' resolves to no" in f for f in result.failures
        ), result.failures

    def test_panel_ref_resolves_but_stateless_in_variant_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#287: the strict check's signature mis-point — a key that RESOLVES
        (the variable exists in the variant's register) but whose states all
        live in a SIBLING variant. Variant 999 is added next to variant 10 in
        register 1; `kon`'s states are in variant 10 only, so the resolution
        check passes and the states check must FAIL."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        reg = conn.execute(
            "SELECT register_id FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, "
            "slug, name, panel_entity_key, panel_time_key, panel_time_grain) "
            "VALUES (999, ?, 'sibling', 'Sibling', 'kon', 'period', 'delivery')",
            (reg,),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'kon' has no variable_state rows in that variant" in f
            for f in result.failures
        ), result.failures
        # The resolution check must NOT have fired — the slug resolves fine.
        assert not any("resolves to no variable.slug" in f for f in result.failures), (
            result.failures
        )

    def test_panel_ref_with_states_in_variant_passes_strict(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#287: `kon` carries states in variant 10 itself, so the strict check
        emits its [OK] line (and the period time-key sentinel stays exempt)."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'kon', "
            "panel_time_key = 'period' WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[panel: entity key has states in the variant]" in report
        assert "have states in their variant" in report

    def test_panel_ref_composite_stateless_element_fails_strict(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#287: composite keys check element-wise in the strict pass too. The
        json-array key decodes and its `kon` element resolves in register 1,
        but carries no states in the fresh sibling variant — the element fails
        the states check while the resolution check stays clean."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        reg = conn.execute(
            "SELECT register_id FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, "
            "slug, name, panel_entity_key) VALUES (998, ?, 'sib2', 'Sib2', ?)",
            (reg, json.dumps(["kon"])),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'kon' has no variable_state rows" in f
            for f in result.failures
        ), result.failures

    # ── entity-key curation gate (#546) ───────────────────────────────────────

    @staticmethod
    def _db_with_kon_entity_key(fixture_db: Path, dst: Path) -> Path:
        """Copy the fixture DB and key variant 10's panel on `kon` (register 1's
        variable, source_id `1.44`). Returns the DB path."""
        dst.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(dst)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'kon' "
            "WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        return dst

    @staticmethod
    def _slug_dir(tmp_path: Path, scb_body: str) -> Path:
        d = tmp_path / "egk_slugs"
        d.mkdir()
        (d / "scb.toml").write_text(scb_body, encoding="utf-8")
        (d / "classifications.toml").write_text("", encoding="utf-8")
        return d

    def test_entity_key_gate_skipped_without_slug_dir(self, fixture_db: Path):
        """`slug_dir=None` (synthetic CI / `validate_built_db(corpus=False)`'s
        existing call sites) SKIPS the gate — even with an entity key present, no
        false failure, but the section + skip line still emit."""
        result = validate_built_db(fixture_db, slug_dir=None)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[panel: entity-key variables are curated]" in report
        assert "entity-key curation gate skipped (no slug_dir)" in report

    def test_entity_key_gate_no_keys_passes(self, fixture_db: Path, tmp_path: Path):
        """A slug_dir is present but the fixture carries no entity key → the gate
        emits an [OK] (nothing to curate), not a skip."""
        slug_dir = self._slug_dir(tmp_path, "")
        result = validate_built_db(fixture_db, slug_dir=slug_dir)
        assert result.passed, result.failures
        assert "nothing to curate" in result.format_report()

    def test_entity_key_gate_fails_when_unpinned(
        self, fixture_db: Path, tmp_path: Path
    ):
        """An entity-key variable with NO curated `[variable]` pin FAILS the gate,
        with the source_id + a remediation pointing at `entity-key-pins`."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "unpinned.db")
        slug_dir = self._slug_dir(tmp_path, "")
        result = validate_built_db(db, slug_dir=slug_dir)
        assert not result.passed
        assert any(
            "source_id 1.44" in f and "no curated [variable] slug pin" in f
            for f in result.failures
        ), result.failures
        assert "reg-meta-build entity-key-pins" in result.format_report()

    def test_entity_key_gate_passes_when_pinned(self, fixture_db: Path, tmp_path: Path):
        """The same DB passes once the entity-key variable carries a curated pin
        binding its source_id (`1.44`) to its slug (`kon`)."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "pinned.db")
        slug_dir = self._slug_dir(tmp_path, '[variable."1.44"]\nslug = "kon"\n')
        result = validate_built_db(db, slug_dir=slug_dir)
        assert result.passed, result.failures
        assert "all 1 entity-key var(s) are curated" in result.format_report()

    @staticmethod
    def _add_sos_entity_key(conn: sqlite3.Connection) -> None:
        """Inject a NON-SCB (sos, provider_id 2) register/variant/variable whose
        panel keys on `sosvar` (un-pinned, source_id `500.LOPNR`). The gate is run
        directly on the connection here — a synthetic low-band non-SCB register
        would otherwise trip the unrelated minted-id-band gate in full
        `validate_built_db`."""
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (500, 2, 'dors', 'Dodsorsaker')"
        )
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (500, CAST('LOPNR' AS TEXT), 'Lopnummer', 'sosvar')"
        )
        vid = cur.lastrowid
        conn.execute(
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name, panel_entity_key) "
            "VALUES (5000, 500, 'grund', 'G', 'sosvar')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, 5000, '0001-01-01', '9999-12-31', 'int', 'Lopnr')",
            (vid,),
        )
        conn.commit()

    def _run_gate(self, db: Path, slug_dir: Path, *, flavored: bool = False):
        """Run only `_check_entity_key_vars_curated` against `db` — isolates the
        #554 all-provider scope from the rest of the suite (notably the band
        gate, which a synthetic low-band non-SCB register would trip).

        `flavored=True` (#559) threads the flavored steward-scope through to the
        gate so it enforces ONLY the providers `slug_dir` covers."""
        from reg_meta_build.validate import (
            ValidationResult,
            _check_entity_key_vars_curated,
        )

        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        try:
            result = ValidationResult()
            _check_entity_key_vars_curated(
                conn,
                result,
                {"register_variant", "variable"},
                slug_dir,
                flavored=flavored,
            )
            return result
        finally:
            conn.close()

    def test_entity_key_gate_fails_unpinned_non_scb(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#554: a NON-SCB (sos) entity-key var DOES trip the gate when un-pinned —
        all global providers are under mandatory curation, so the gate is no longer
        SCB-scoped."""
        db = tmp_path / "sos_only.db"
        db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(db)
        self._add_sos_entity_key(conn)
        conn.close()
        slug_dir = self._slug_dir(tmp_path, "")  # nothing pinned
        result = self._run_gate(db, slug_dir)
        assert not result.passed
        assert any(
            "source_id 500.LOPNR" in f and "no curated [variable] slug pin" in f
            for f in result.failures
        ), result.failures

    def test_entity_key_gate_enforces_all_providers(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#554: with BOTH an un-pinned SCB key (`1.44`/`kon`) and an un-pinned sos
        key (`500.LOPNR`/`sosvar`) present, BOTH fail the gate — no provider is out
        of scope."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "both.db")
        conn = sqlite3.connect(db)
        self._add_sos_entity_key(conn)
        conn.close()
        slug_dir = self._slug_dir(tmp_path, "")  # nothing pinned
        result = self._run_gate(db, slug_dir)
        assert not result.passed
        assert any("source_id 1.44" in f for f in result.failures), result.failures
        assert any("source_id 500.LOPNR" in f for f in result.failures), result.failures

    # ── flavored (steward-scoped) gate (#559) ─────────────────────────────────

    @staticmethod
    def _steward_slug_dir(tmp_path: Path, sos_body: str) -> Path:
        """A STEWARD slug dir scoping to provider `sos` (the flavored stand-in for
        a real steward provider). The `[register]` entry puts `sos` in the
        flavored provider scope; `sos_body` adds any `[variable]` pins on top."""
        d = tmp_path / "steward_slugs"
        d.mkdir()
        (d / "sos.toml").write_text(
            '[register."500"]\nslug = "dors"\n' + sos_body, encoding="utf-8"
        )
        return d

    def test_flavored_gate_scopes_to_steward_provider(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#559: the flavored gate scoped to provider `sos` (the steward dir) does
        NOT fail on the un-pinned SCB (`1.44`/`kon`) var — it belongs to the global
        base, out of the steward scope — but DOES fail on the un-pinned non-global
        (`500.LOPNR`/`sosvar`) var that the steward dir covers."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "flavored.db")
        conn = sqlite3.connect(db)
        self._add_sos_entity_key(conn)
        conn.close()
        steward_dir = self._steward_slug_dir(tmp_path, "")  # no variable pins
        result = self._run_gate(db, steward_dir, flavored=True)
        assert not result.passed
        # The non-global steward var fails; the global SCB var is NOT enforced.
        assert any("source_id 500.LOPNR" in f for f in result.failures), result.failures
        assert not any("source_id 1.44" in f for f in result.failures), result.failures
        # #559 Fix A: the flavored remediation points at `--flavored --slug-dir`
        # and the nested steward fold location — NOT the global `--out-dir` /
        # repo-root fqid_slugs/<provider>.toml path the global gate emits.
        report = result.format_report()
        assert "--flavored" in report, report
        assert "--slug-dir <steward dir>" in report, report
        assert "fqid_slugs/<steward>/<provider>.toml" in report, report
        assert "--out-dir" not in report, report

    def test_flavored_gate_passes_when_steward_var_pinned(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#559: pinning the steward var's `[variable]` slug in the steward dir
        clears the flavored gate — even with the un-pinned SCB var still present
        (it's out of the steward scope, so it never enforces)."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "flavored_ok.db")
        conn = sqlite3.connect(db)
        self._add_sos_entity_key(conn)
        conn.close()
        steward_dir = self._steward_slug_dir(
            tmp_path, '[variable."500.LOPNR"]\nslug = "sosvar"\n'
        )
        result = self._run_gate(db, steward_dir, flavored=True)
        assert result.passed, result.failures
        assert "all 1 entity-key var(s) are curated" in result.format_report()

    @staticmethod
    def _add_sos_global_base_entity_key(conn: sqlite3.Connection) -> None:
        """Add a SECOND register (501) under the SAME provider (sos, provider_id 2)
        carrying an un-pinned entity-key var (source_id `501.GLOBALK`). It stands in
        for a GLOBAL BASE register that shares a provider slug with a steward overlay
        — the case Fix 2 guards: a provider-slug scope would wrongly re-pull it."""
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (501, 2, 'global-base', 'GlobalBase')"
        )
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (501, CAST('GLOBALK' AS TEXT), 'GlobalKey', 'globalk')"
        )
        vid = cur.lastrowid
        conn.execute(
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name, panel_entity_key) "
            "VALUES (5010, 501, 'base', 'B', 'globalk')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, 5010, '0001-01-01', '9999-12-31', 'int', 'Globalk')",
            (vid,),
        )
        conn.commit()

    def test_flavored_gate_excludes_same_provider_global_register(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#559 Fix 2: the flavored gate scopes by STEWARD REGISTER, not provider.

        One provider (sos) carries BOTH a steward-overlay register (500, the steward
        dir curates it) AND a global-base register (501, the steward dir does NOT).
        The steward dir's `[register]` entry names only 500. The gate must enforce
        ONLY 500's entity-key var; 501's (same provider) is excluded entirely — no
        failure for it, and (per `iter_entity_key_variables`) its row is skipped
        before the flavored-unsafe `_variable_source_ids` ever runs."""
        db = tmp_path / "same_provider.db"
        db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(db)
        self._add_sos_entity_key(conn)  # steward register 500 (500.LOPNR), un-pinned
        self._add_sos_global_base_entity_key(conn)  # global register 501, un-pinned
        conn.close()
        # Steward dir curates ONLY register 500; provider scope would also grab 501.
        steward_dir = self._steward_slug_dir(tmp_path, "")
        result = self._run_gate(db, steward_dir, flavored=True)
        assert not result.passed
        # The steward register's var fails (un-pinned, in scope) …
        assert any("source_id 500.LOPNR" in f for f in result.failures), result.failures
        # … but the same-provider GLOBAL register's var is NOT enforced at all.
        assert not any("source_id 501.GLOBALK" in f for f in result.failures), (
            result.failures
        )


class TestBuildDbProvidersDefault:
    def test_cli_default_is_combined_global_build(self):
        """The CLI `--providers` default is the full global build: `scb,sos`
        (A4.5) plus the thin curated `fohm` + `fk` providers (#422).
        `--providers scb` still selects the SCB-only DB (the A4.3b byte-identical
        gate). Only the CLI surface carries the default — `build_db()`'s function
        default stays `('scb',)` so synthetic SCB-only fixtures need no extra
        inputs."""
        from reg_meta_build.cli import _build_parser

        parser = _build_parser()
        ns = parser.parse_args(["build-db", "--input-dir", "x"])
        assert (
            ns.providers
            == "scb,sos,fohm,fk,lakemedelsverket,pliktverket,riksarkivet,umu"
        )
        ns = parser.parse_args(["build-db", "--input-dir", "x", "--providers", "scb"])
        assert ns.providers == "scb"


class TestBuildDbValidateFlag:
    def test_argparse_exposes_no_validate(self):
        """Validation is on by default; `--no-validate` is the opt-out wired
        into `reg-meta-build build-db`'s argparse subparser."""
        from reg_meta_build.cli import _build_parser

        parser = _build_parser()
        ns = parser.parse_args(["build-db", "--input-dir", "x"])
        assert ns.no_validate is False
        ns = parser.parse_args(["build-db", "--input-dir", "x", "--no-validate"])
        assert ns.no_validate is True

    def test_failed_validation_does_not_replace_installed_db(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Regression for Copilot review on PR #99: a failing validation
        run must not leave the staging DB installed at `<db_dir>/reg_meta.db`.
        Pre-populates the install path with a sentinel, builds with a hook
        that always fails, and asserts the sentinel is preserved."""
        import sys as _sys

        _sys.path.insert(0, str(Path(__file__).parent))
        from _csv_fixtures import write_scb_input
        from reg_meta.db import DB_FILENAME
        from reg_meta.errors import RegMetaError
        from reg_meta_build.db import build_db

        from reg_meta_build import validate as validate_mod

        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        input_dir.mkdir()
        db_dir.mkdir()
        write_scb_input(input_dir)

        sentinel = db_dir / DB_FILENAME
        sentinel_bytes = b"SENTINEL-PREVIOUS-DB-MUST-SURVIVE"
        sentinel.write_bytes(sentinel_bytes)

        def always_fail(
            _db_path: Path,
            *,
            corpus: bool = False,
            flavored: bool = False,
            slug_dir: Path | None = None,
        ) -> validate_mod.ValidationResult:
            r = validate_mod.ValidationResult()
            r.fail("synthetic invariant breach")
            return r

        monkeypatch.setattr(validate_mod, "validate_built_db", always_fail)
        # Also patch the re-export in the build CLI module so the handler
        # closure sees the fake.
        from reg_meta_build import cli as cli_mod

        monkeypatch.setattr(cli_mod, "validate_built_db", always_fail)

        with pytest.raises(RegMetaError) as exc_info:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                skip_classifications=True,
                skip_slugs=True,
                pre_rename_hook=cli_mod._build_validate_hook(None),
            )
        assert exc_info.value.code == "validation_failed"
        # The prior DB is untouched and the failed staging file is gone.
        assert sentinel.read_bytes() == sentinel_bytes
        tmp_file = sentinel.with_suffix(".db.tmp")
        assert not tmp_file.exists()


class TestConceptGroupChecks:
    """#303 `_check_concept_groups` — exercised against NON-EMPTY group tables
    so CI covers the invariants (the e2e fixture build derives zero groups:
    its synthetic corpus has no sibling edges / month families, and the
    curated TOML is nulled by `_no_repo_curation`). Each test corrupts one
    invariant on a hand-built slugged DB and asserts the gate bites."""

    @staticmethod
    def _grouped_db():
        # #591: the edge fold no longer round-trips `variable_related_to`, so the
        # check recomputes nothing from sibling rows — a hand-built `edge` group
        # row is the whole fixture (no companion edges needed).
        from _slugged_db import add_variable, build_slugged_db

        conn = build_slugged_db(classification=None)  # scb/lisa (register 1)
        add_variable(conn, register_id=1, var_id=901, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=901, name="A", slug="varb")
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (10, 'variable', 1, 'vara', 'A', 'edge')"
        )
        conn.executemany(
            "INSERT INTO concept_group_variable (variable_id, group_id) "
            "SELECT variable_id, 10 FROM variable WHERE slug = ?",
            [("vara",), ("varb",)],
        )
        return conn

    @staticmethod
    def _run(conn):
        from reg_meta_build.validate import ValidationResult, _check_concept_groups

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_concept_groups(conn, result, tables, corpus=False)
        return result

    def test_passes_on_coherent_groups(self):
        result = self._run(self._grouped_db())
        assert result.passed, result.failures

    def test_undersized_group_fails(self):
        conn = self._grouped_db()
        conn.execute(
            "DELETE FROM concept_group_variable WHERE variable_id IN "
            "(SELECT variable_id FROM variable WHERE slug = 'varb')"
        )
        result = self._run(conn)
        assert any("< 2 members" in f for f in result.failures)

    def test_cross_register_member_fails(self):
        conn = self._grouped_db()
        conn.execute("UPDATE concept_group SET register_id = 99 WHERE group_id = 10")
        result = self._run(conn)
        assert any("outside their group's register" in f for f in result.failures)

    def test_wrong_kind_wiring_fails(self):
        conn = self._grouped_db()
        conn.execute(
            "UPDATE concept_group SET kind = 'classification', register_id = NULL "
            "WHERE group_id = 10"
        )
        result = self._run(conn)
        assert any("wrong-kind group" in f for f in result.failures)

    def test_edge_group_floor_fails_below_threshold(self):
        # #591 corpus floor: a real build whose edge fold collapsed (here only the
        # single fixture group, well under `_CG_MIN_EDGE_GROUPS`) fails the gate.
        result = self._run_corpus(self._grouped_db())
        assert any(
            "edge group" in f and "derivation collapse" in f for f in result.failures
        ), result.failures

    def test_edge_group_floor_passes_at_threshold(self, monkeypatch):
        # With the floor lowered to the fixture's single edge group, the corpus
        # edge check reports OK (proves the floor is wired to the `source='edge'`
        # count, not always-failing).
        import reg_meta_build.validate as validate_mod

        monkeypatch.setattr(validate_mod, "_CG_MIN_EDGE_GROUPS", 1)
        result = self._run_corpus(self._grouped_db())
        assert any(
            "edge group" in ln.text and ">=" in ln.text
            for ln in result.lines
            if ln.kind == "ok"
        ), [ln.text for ln in result.lines]

    # #819 multi-axis facet invariants. `_grouped_db` seeds an EDGE group (group 10,
    # zero axes) whose members carry no facets — the valid edge shape. Each test
    # corrupts the new tables one way and asserts the gate bites.

    @staticmethod
    def _member_id(conn, slug: str) -> int:
        return conn.execute(
            "SELECT m.member_id FROM concept_group_variable m "
            "JOIN variable v ON v.variable_id = m.variable_id WHERE v.slug = ?",
            (slug,),
        ).fetchone()[0]

    def test_facet_naming_undeclared_axis_fails(self):
        # A member facet whose axis is NOT one of its group's `concept_group_axis`
        # axes violates the invariant. The edge group has zero axes, so any facet is
        # undeclared.
        conn = self._grouped_db()
        conn.execute(
            "INSERT INTO concept_group_variable_facet (member_id, axis, value, label) "
            "VALUES (?, 'rank', '1', 'A')",
            (self._member_id(conn, "vara"),),
        )
        result = self._run(conn)
        assert any("name an axis not declared" in f for f in result.failures), (
            result.failures
        )

    def test_member_missing_a_facet_per_axis_fails(self):
        # A group with one declared axis whose member carries zero facets violates
        # the one-facet-per-axis coverage invariant.
        conn = self._grouped_db()
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (10, 'rank', 0, 'Rank')"
        )
        result = self._run(conn)
        assert any("one facet per declared group axis" in f for f in result.failures), (
            result.failures
        )

    def test_variable_spanning_two_groups_fails(self):
        # The one-group-per-variable invariant the surrogate PK no longer enforces:
        # add a second group claiming `vara`, which already belongs to group 10.
        conn = self._grouped_db()
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (11, 'variable', 1, 'other', 'B', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_variable (group_id, variable_id, "
            "delivery_column_name) SELECT 11, variable_id, NULL FROM variable "
            "WHERE slug = 'vara'"
        )
        result = self._run(conn)
        assert any("one-group-per-variable" in f for f in result.failures), (
            result.failures
        )

    def test_duplicate_whole_variable_member_rejected_by_index(self):
        # The COALESCE unique index closes the NULL-distinctness footgun: a second
        # whole-variable (NULL delivery_column) member of `vara` in group 10 — which
        # a bare composite UNIQUE would silently admit — is rejected at insert time.
        conn = self._grouped_db()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO concept_group_variable (group_id, variable_id, "
                "delivery_column_name) SELECT 10, variable_id, NULL FROM variable "
                "WHERE slug = 'vara'"
            )

    def test_variable_mixed_grain_within_group_fails(self):
        # `vara` is already a WHOLE-variable member of group 10 (NULL
        # delivery_column). Add a SECOND, REPRESENTATION member for the same
        # variable_id in the same group — the two grains for one variable trip the
        # mixed-grain check. The edge group has zero axes and the new member carries
        # zero facets, so the coverage check is trivially satisfied (0 == 0) and the
        # mixed-grain failure is what bites, isolated.
        conn = self._grouped_db()
        conn.execute(
            "INSERT INTO concept_group_variable (group_id, variable_id, "
            "delivery_column_name) SELECT 10, variable_id, 'CDISP' FROM variable "
            "WHERE slug = 'vara'"
        )
        result = self._run(conn)
        assert any(
            "mixing whole-variable and representation members" in f
            for f in result.failures
        ), result.failures

    @staticmethod
    def _add_classification_group(conn, *, source: str):
        """Seed a 2-member `kind='classification'` group with the given source.

        Adds two `classification` rows (the umbrella's vintages) and wires both
        as `concept_group_classification` members so the group clears the
        >= 2-member floor — isolating the derived-empty (#571) check on `source`.
        """
        ids = []
        for short, name, slug in (
            ("SUN2000", "Svensk utbildningsnomenklatur 2000", "sun2000"),
            ("SUN2020", "Svensk utbildningsnomenklatur 2020", "sun2020"),
        ):
            cur = conn.execute(
                "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
                (short, name, slug),
            )
            ids.append(cur.lastrowid)
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (20, 'classification', NULL, 'sun', 'SUN', ?)",
            (source,),
        )
        for vintage, cid in zip(("2000", "2020"), ids):
            conn.execute(
                "INSERT INTO concept_group_classification "
                "(classification_id, group_id, facet_value, facet_label) "
                "VALUES (?, 20, ?, ?)",
                (cid, vintage, f"SUN {vintage}"),
            )
        conn.commit()

    def test_curated_classification_group_passes_derived_empty_check(self):
        # #516 umbrella: a CURATED classification group is retained and must not
        # trip the #571 derived-empty assertion (corpus path).
        conn = self._grouped_db()
        self._add_classification_group(conn, source="curated")
        result = self._run_corpus(conn)
        assert not any(
            "derived (token) classification concept group" in f for f in result.failures
        ), result.failures
        assert any(
            "no derived (token) classification concept groups" in ln.text
            for ln in result.lines
            if ln.kind == "ok"
        )

    def test_token_classification_group_fails_derived_empty_check(self):
        # A DERIVED (#571 token) classification group must still fail — those
        # vintage families belong in the succession-edge table, not here.
        conn = self._grouped_db()
        self._add_classification_group(conn, source="token")
        result = self._run_corpus(conn)
        assert any(
            "derived (token) classification concept group" in f for f in result.failures
        ), result.failures

    def test_classification_group_with_two_axes_fails(self):
        # The #819 "classification umbrellas carry at most one axis" check: a
        # classification group with two `concept_group_axis` rows would fan out its
        # members on the LEFT-JOIN read path. Always-run (not corpus-gated).
        conn = self._grouped_db()
        self._add_classification_group(conn, source="curated")
        conn.executemany(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (20, ?, ?, ?)",
            [("vintage", 0, "Vintage"), ("region", 1, "Region")],
        )
        result = self._run(conn)
        assert any(
            "classification group(s) declare >1 axis" in f for f in result.failures
        ), result.failures

    @staticmethod
    def _run_corpus(conn):
        from reg_meta_build.validate import ValidationResult, _check_concept_groups

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_concept_groups(conn, result, tables, corpus=True)
        return result


class TestTagChecks:
    """#311 `_check_tags` closure — exercised against NON-EMPTY tag tables (the
    e2e fixture build ships zero tags, so CI needs hand-built data). Each test
    corrupts one invariant with FKs OFF (so the corrupting DELETE can leave a
    dangling reference the check is meant to catch) and asserts the gate bites."""

    @staticmethod
    def _tagged_db():
        from _slugged_db import add_variable, build_slugged_db
        from reg_meta_build.tags import CuratedTag, TagMember, materialize_tags

        conn = build_slugged_db(classification=None)  # scb/lisa (register 1), `kon`
        add_variable(conn, register_id=1, var_id=90, name="Income", slug="dispink")
        materialize_tags(
            conn,
            (
                CuratedTag(
                    slug="income",
                    label="Income",
                    description=None,
                    members=(
                        TagMember("scb", "lisa", "dispink", 0, True, "primary"),
                        TagMember("scb", "lisa", None, 1, False, None),
                    ),
                ),
            ),
            providers=frozenset({"scb"}),
        )
        return conn

    @staticmethod
    def _run(conn):
        from reg_meta_build.validate import ValidationResult, _check_tags

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_tags(conn, result, tables)
        return result

    def test_passes_on_coherent_tags(self):
        result = self._run(self._tagged_db())
        assert result.passed, result.failures

    def test_missing_tag_row_fails(self):
        conn = self._tagged_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DELETE FROM tag")
        result = self._run(conn)
        assert any("missing tag" in f for f in result.failures), result.failures

    def test_dangling_register_member_fails(self):
        conn = self._tagged_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        # Drop the register a register-grain member points at.
        conn.execute(
            "DELETE FROM register WHERE register_id IN "
            "(SELECT register_id FROM tag_member WHERE register_id IS NOT NULL)"
        )
        result = self._run(conn)
        assert any("dangling register_id" in f for f in result.failures), (
            result.failures
        )

    def test_dangling_variable_member_fails(self):
        conn = self._tagged_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        # Drop the variable a variable-grain member points at.
        conn.execute(
            "DELETE FROM variable WHERE variable_id IN "
            "(SELECT variable_id FROM tag_member WHERE variable_id IS NOT NULL)"
        )
        result = self._run(conn)
        assert any("dangling" in f and "variable_id" in f for f in result.failures), (
            result.failures
        )


def test_tags_section_present_in_report(fixture_db: Path):
    """#311: the `[tags]` section + its empty info line must appear in the full
    report so `_check_tags` can't be silently de-registered from
    `validate_built_db` (the fixture ships empty tags)."""
    report = validate_built_db(fixture_db).format_report()
    assert "[tags]" in report
    assert "0 tags / 0 tag members" in report


class TestVariableAliasWindowChecks:
    """#319 `_check_variable_alias_window` — exercised against NON-EMPTY window
    rows (the e2e fixture ships none). Each test corrupts one invariant with FKs
    OFF (so the corrupting DELETE can leave the dangling row the check catches)."""

    @staticmethod
    def _windowed_db():
        from _slugged_db import build_slugged_db

        conn = build_slugged_db(classification=None)  # scb/lisa, variable `kon`
        # kon has a variable_alias row under variant 10 (its delivery column). Seed
        # a window row reusing that exact (variable_id, variant, column).
        vid, rvid, col = conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name "
            "FROM variable_alias LIMIT 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) "
            "VALUES (?, ?, ?, '2018-01-01', '2018-01-31')",
            (vid, rvid, col),
        )
        return conn

    @staticmethod
    def _run(conn):
        from reg_meta_build.validate import (
            ValidationResult,
            _check_variable_alias_window,
        )

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_variable_alias_window(conn, result, tables, corpus=False)
        return result

    def test_passes_on_coherent_window(self):
        assert self._run(self._windowed_db()).passed

    def test_orphan_variable_id_fails(self):
        conn = self._windowed_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "DELETE FROM variable WHERE variable_id IN "
            "(SELECT variable_id FROM variable_alias_window)"
        )
        result = self._run(conn)
        assert any("dangling variable/variant" in f for f in result.failures), (
            result.failures
        )

    def test_orphan_register_variant_id_fails(self):
        conn = self._windowed_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "DELETE FROM register_variant WHERE register_variant_id IN "
            "(SELECT register_variant_id FROM variable_alias_window)"
        )
        result = self._run(conn)
        assert any("dangling variable/variant" in f for f in result.failures), (
            result.failures
        )

    def test_window_column_not_in_alias_fails(self):
        conn = self._windowed_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        # Make the window's column absent from variable_alias (rename the alias).
        conn.execute("UPDATE variable_alias SET delivery_column_name = 'OtherCol'")
        result = self._run(conn)
        assert any("missing from variable_alias" in f for f in result.failures), (
            result.failures
        )


def test_variable_alias_window_section_present_in_report(fixture_db: Path):
    """#319: the `[monthly-family windows]` section must appear so the check can't
    be silently de-registered (the fixture ships zero windows)."""
    report = validate_built_db(fixture_db).format_report()
    assert "[monthly-family windows]" in report
    assert "0 alias windows" in report
