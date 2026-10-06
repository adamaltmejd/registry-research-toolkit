"""Succession and vintage-lift relation invariants at the validate_built_db boundary."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


class TestValidateModule:
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

    def test_classification_derived_from_dangling_slug_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#779: a non-temporal derived_from edge pointing at an unknown
        classification slug fails the structural resolution check."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._seed_classification(conn, "KS87-P", "ks87-p")
        conn.execute(
            "INSERT INTO classification_derived_from "
            "(derived_slug, source_slug, note) "
            "VALUES ('ks87-p', 'no-such-slug-9999', 'variant')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("derived_from edge" in f for f in result.failures)

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

    def test_representation_succession_dangling_variant_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#846: a representation succession edge whose endpoints resolve but whose
        non-empty `variant` slug does not name a live register_variant of the edge's
        register fails the structural variant-resolution check (the post-build
        analog of the build-time `replaced_by_unresolved_variant` fail-fast)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # scb/testreg/testcol observes both `TestCol` and `TestKolumn` (a resolvable
        # within-build rename), so only the bogus `variant` trips: testreg's live
        # variant slug is `individer`, not `no-such-variant`.
        conn.execute(
            "INSERT INTO representation_replaced_by "
            "(predecessor_provider, predecessor_register, predecessor_variable, "
            "predecessor_column, successor_provider, successor_register, "
            "successor_variable, successor_column, variant, effective_year, note) "
            "VALUES ('scb', 'testreg', 'testcol', 'TestCol', "
            "'scb', 'testreg', 'testcol', 'TestKolumn', "
            "'no-such-variant', 2010, 'curated:slug_toml')"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "scope to a `variant` that is not a live register_variant" in f
            for f in result.failures
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
