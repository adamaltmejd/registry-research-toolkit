"""precheck-slugs CLI exit codes, --update-snapshot, and the grow-only refusal for frozen zones."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from _fqid_slug_support import write_text_file as _write

from reg_meta_build.fqid_slugs import (
    FREEZE_STATE_FILE,
    SNAPSHOT_FILENAME,
    write_snapshot,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestPrecheckCli:
    """CLI exit codes mirror the per-test contract: added rows are not fatal
    in the maintainer's interactive view, but they must fail CI so the
    snapshot stays current."""

    def _seed_layout(self, tmp_path: Path) -> tuple[Path, Path]:
        """Build a DB with one provider + register + variant, slug TOMLs that
        cover both, and a `db` arg compatible with `reg_meta --db`."""
        from reg_meta_build.db import DDL, seed_providers

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        db_path = db_dir / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'LISA', 'lisa')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, "
            "name) VALUES (10, 1, 'individer-15plus', 'Individer 15+')"
        )
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SUN2020', 'Svensk utbildning', 'sun2020')"
        )
        conn.execute("INSERT INTO import_manifest VALUES ('schema_version', '3.1.0')")
        conn.commit()
        conn.close()

        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        return db_dir, slug_dir

    def test_added_entries_exit_non_zero(self, tmp_path: Path):
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Snapshot is empty — the two entries above show as `added`.
        write_snapshot(
            slug_dir / SNAPSHOT_FILENAME,
            {
                kind: {}
                for kind in (
                    "register",
                    "register_variant",
                    "variable",
                    "classification",
                )
            },
        )
        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
            ]
        )
        assert exit_code != 0

    def test_update_snapshot_clears_added(self, tmp_path: Path):
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        write_snapshot(
            slug_dir / SNAPSHOT_FILENAME,
            {
                kind: {}
                for kind in (
                    "register",
                    "register_variant",
                    "variable",
                    "classification",
                )
            },
        )
        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        # All TOMLs valid, all live rows covered — `--update-snapshot` skips
        # the diff so the run exits clean.
        assert exit_code == 0
        # Snapshot file was rewritten.
        contents = (slug_dir / SNAPSHOT_FILENAME).read_text()
        assert "lisa" in contents

    def test_update_snapshot_still_exits_on_missing(self, tmp_path: Path):
        """`--update-snapshot` is review-only for snapshot drift but still
        exits non-zero on real problems (parse errors / missing slugs) so a
        broken state can't be snapshot-frozen."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Drop the register entry — DB still has register_id=1, so precheck
        # surfaces a missing slug.
        _write(
            slug_dir / "scb.toml",
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0

    def test_update_snapshot_refuses_on_parse_error(self, tmp_path: Path):
        """A corrupted TOML must not silently blow away the baseline snapshot
        — `precheck_slugs` truncates `entries` at the first parse error and
        the partial set would otherwise wipe genuine prior entries."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        snapshot_before = (
            "{"
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        # Corrupt the scb.toml so load_slug_dir raises.
        _write(slug_dir / "scb.toml", '[register."1"]\nslug = "Bad_Slug"\n')

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0
        # Snapshot file is unchanged — the prior baseline survives.
        assert (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8") == (
            snapshot_before
        )


class TestPrecheckCliGrowOnly:
    """--update-snapshot must refuse to bless a removal or rename — otherwise
    the grow-only contract is bypassable in one command."""

    def _seed_layout(self, tmp_path: Path) -> tuple[Path, Path]:
        """Reuse the layout from TestPrecheckCli but local to keep dependencies
        explicit."""
        from reg_meta_build.db import DDL, seed_providers

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        db_path = db_dir / "reg_meta.db"
        conn = sqlite3.connect(db_path)
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'LISA', 'lisa')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, "
            "name) VALUES (10, 1, 'individer-15plus', 'Individer 15+')"
        )
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SUN2020', 'Svensk utbildning', 'sun2020')"
        )
        conn.execute("INSERT INTO import_manifest VALUES ('schema_version', '3.1.0')")
        conn.commit()
        conn.close()

        slug_dir = tmp_path / "slugs"
        slug_dir.mkdir()
        return db_dir, slug_dir

    def test_rename_refused_even_with_update(self, tmp_path: Path):
        """A maintainer renames a previously-published slug in a FROZEN zone,
        then tries to bless it via --update-snapshot. The CLI must refuse and
        leave the snapshot unchanged (#470: only frozen zones refuse)."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # The `scb` zone is sealed → its rename is blocked.
        (slug_dir / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        # Baseline has `lisa`; the new TOML renames it to `lisa-individuals`.
        snapshot_before = (
            "{"
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        # But the register row in the DB also needs the matching slug, since
        # populate runs first. Rewrite the DB row to match the TOML so the
        # only divergence is between TOML and snapshot.
        import sqlite3 as _sql

        conn = _sql.connect(db_dir / "reg_meta.db")
        conn.execute(
            "UPDATE register SET slug = 'lisa-individuals' WHERE register_id = 1"
        )
        conn.commit()
        conn.close()
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa-individuals"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0
        assert (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8") == (
            snapshot_before
        )

    def test_removal_refused_even_with_update(self, tmp_path: Path):
        """Maintainer drops a previously-published row from a FROZEN zone's TOML
        (#470: only frozen zones refuse)."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # The `scb` zone is sealed → dropping its variant row is blocked.
        (slug_dir / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        snapshot_before = (
            "{"
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code != 0
        assert (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8") == (
            snapshot_before
        )

    def test_rename_accepted_under_update_when_churning(self, tmp_path: Path):
        """#470: a rename in a CHURNING zone (the default — no freeze.toml)
        flips `--update-snapshot` from refuse-and-fail to write-through. The
        rename is still reported in the envelope so drift stays visible.
        """
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        snapshot_before = (
            "{"
            '"register":{"scb/1":"lisa"},'
            '"register_variant":{"scb/1.10":"individer-15plus"},'
            '"variable":{}}'
        )
        (slug_dir / SNAPSHOT_FILENAME).write_text(snapshot_before, encoding="utf-8")
        # No freeze.toml ⇒ the `scb` zone is churning ⇒ rename writes through.

        import sqlite3 as _sql

        conn = _sql.connect(db_dir / "reg_meta.db")
        conn.execute(
            "UPDATE register SET slug = 'lisa-individuals' WHERE register_id = 1"
        )
        conn.commit()
        conn.close()
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa-individuals"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code == 0
        contents = (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8")
        assert "lisa-individuals" in contents
        assert '"lisa"' not in contents

    def test_pure_addition_accepted_under_update(self, tmp_path: Path):
        """The legitimate use case still works — adding a new slug refreshes
        the snapshot."""
        from reg_meta_build.cli import run

        db_dir, slug_dir = self._seed_layout(tmp_path)
        # Empty baseline; the two live entries are pure additions.
        (slug_dir / SNAPSHOT_FILENAME).write_text(
            '{"register":{},"register_variant":{},"variable":{}}\n',
            encoding="utf-8",
        )
        _write(
            slug_dir / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )

        exit_code = run(
            [
                "--db",
                str(db_dir),
                "precheck-slugs",
                "--slug-dir",
                str(slug_dir),
                "--update-snapshot",
            ]
        )
        assert exit_code == 0
        # Snapshot now contains the live entries.
        contents = (slug_dir / SNAPSHOT_FILENAME).read_text(encoding="utf-8")
        assert "lisa" in contents
        assert "individer-15plus" in contents
