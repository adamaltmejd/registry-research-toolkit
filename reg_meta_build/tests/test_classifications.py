"""Committed classification CSV snapshots load through the curation tree."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from catalog_manifest import synthetic_manifest
from reg_meta_build.classifications import load_valid_codes

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# Repo CSV snapshots round-trip through the curation tree
# ---------------------------------------------------------------------------


def _repo_classifications_dir() -> Path:
    from pathlib import Path

    return Path(__file__).resolve().parents[1] / "input_data" / "classifications"


class TestRepoClassificationCsvSnapshots:
    def test_kva_csv_round_trips_without_duplicate_codes(self, tmp_path: Path):
        """The real merged `sos/kva.csv` (KMÅ ∪ KKÅ, deduped on the 50 shared
        chapter headers) loads into ONE `KVA` classification with codes and
        WITHOUT a duplicate-code `RegMetaError` — proving the merge deduped."""
        from reg_meta_build.resolved_catalog import (
            ResolvedClassification,
            ResolvedClassificationCode,
            write_resolved_catalog,
        )

        kva_csv = _repo_classifications_dir() / "sos" / "kva.csv"
        assert kva_csv.is_file(), "merged kva.csv must exist"

        codes = load_valid_codes(kva_csv)
        book = ResolvedClassification(
            slug="kva",
            short_name="KVA",
            name="KVÅ",
            codes=tuple(
                ResolvedClassificationCode(code=code, label=label)
                for code, label in codes.items()
            ),
        )
        output = write_resolved_catalog(
            (),
            tmp_path / "catalog.db",
            manifest=synthetic_manifest() | {"fixture": "canonical-kva"},
            diagnostic=True,
            classifications=(book,),
        )
        conn = sqlite3.connect(output)
        n_seeded = conn.execute("SELECT count(*) FROM classification").fetchone()[0]
        assert n_seeded == 1
        row = conn.execute(
            "SELECT short_name, code_count FROM classification"
        ).fetchone()
        assert row[0] == "KVA"
        assert row[1] > 0, "KVA must seed canonical codes"
