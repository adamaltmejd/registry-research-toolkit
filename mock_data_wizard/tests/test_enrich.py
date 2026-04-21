"""Tests for stats enrichment."""

from __future__ import annotations

from pathlib import Path

import pytest
from regmeta.errors import RegmetaError

from mock_data_wizard.enrich import (
    EnrichedColumn,
    EnrichedFile,
    _check_value_code_drift,
    _vote_register,
    enrich,
)
from mock_data_wizard.stats import parse_stats


def test_enrich_without_db(stats_path: Path):
    """Enrichment without db_path returns unenriched results."""
    stats = parse_stats(stats_path)
    result = enrich(stats)
    assert len(result) == 1
    assert len(result[0].columns) == 6
    assert result[0].file_name == "persons.csv"
    for col in result[0].columns:
        assert col.register_id is None
        assert col.value_codes is None


def test_enrich_nonexistent_db_raises(stats_path: Path):
    """Enrichment raises when db_path is given but doesn't exist."""
    stats = parse_stats(stats_path)
    with pytest.raises(RegmetaError):
        enrich(stats, db_path=Path("/nonexistent/db"))


def test_enrich_preserves_stats(stats_path: Path):
    stats = parse_stats(stats_path)
    result = enrich(stats)
    cols = {c.column_name: c for c in result[0].columns}
    assert cols["Kon"].inferred_type == "categorical"
    assert cols["Kon"].stats["frequencies"]["1"] == 500
    assert cols["FodelseAr"].inferred_type == "numeric"
    assert cols["FodelseAr"].stats["mean"] == 1975


def test_enrich_multi_file(multi_file_stats_path: Path):
    stats = parse_stats(multi_file_stats_path)
    result = enrich(stats)
    assert len(result) == 2
    names = {f.file_name for f in result}
    assert names == {"file_a.csv", "file_b.csv"}


def _make_enriched(
    file_name: str,
    col_name: str,
    frequencies: dict[str, int],
    value_codes: dict[str, str] | None,
) -> EnrichedFile:
    return EnrichedFile(
        file_name=file_name,
        relative_path=file_name,
        row_count=100,
        columns=[
            EnrichedColumn(
                column_name=col_name,
                inferred_type="categorical",
                nullable=False,
                null_rate=0.0,
                n_distinct=len(frequencies),
                stats={"frequencies": frequencies},
                value_codes=value_codes,
            )
        ],
    )


def test_drift_warns_on_unknown_codes():
    ef = _make_enriched(
        "f.csv", "Kon", {"1": 50, "2": 40, "3": 10}, {"1": "Man", "2": "Kvinna"}
    )
    warnings = _check_value_code_drift([ef])
    assert len(warnings) == 1
    assert "3" in warnings[0]
    assert "f.csv/Kon" in warnings[0]


def test_drift_no_warning_when_all_codes_match():
    ef = _make_enriched("f.csv", "Kon", {"1": 50, "2": 50}, {"1": "Man", "2": "Kvinna"})
    assert _check_value_code_drift([ef]) == []


def test_drift_ignores_other_bucket():
    ef = _make_enriched(
        "f.csv", "Kon", {"1": 50, "_other": 10}, {"1": "Man", "2": "Kvinna"}
    )
    assert _check_value_code_drift([ef]) == []


def test_drift_skipped_without_value_codes():
    ef = _make_enriched("f.csv", "Status", {"A": 50, "B": 50}, None)
    assert _check_value_code_drift([ef]) == []


def test_enrich_resolves_from_db(stats_path: Path, regmeta_db: Path):
    """Enrichment against a real regmeta DB resolves columns and fetches value codes."""
    stats = parse_stats(stats_path)
    result = enrich(stats, register="TESTREG", db_path=regmeta_db)
    cols = {c.column_name: c for c in result[0].columns}

    kon = cols["Kon"]
    assert kon.register_id == 1
    assert kon.var_id == 44
    assert kon.variable_name == "Kön"
    assert kon.value_codes == {"1": "Man", "2": "Kvinna"}


# ---------------------------------------------------------------------------
# _vote_register confidence and candidate reporting
# ---------------------------------------------------------------------------


def test_vote_confident_winner():
    """When the winner covers ≥40% of columns and has a clear lead, it's picked."""
    col_to_registers = {
        "a": [10],
        "b": [10],
        "c": [10],
        "d": [10, 99],
    }
    result = _vote_register(["a", "b", "c", "d"], col_to_registers, "f.csv")
    assert result.register_id == 10
    assert result.candidates[0].register_id == 10
    assert result.candidates[0].match_count == 4
    assert result.candidates[0].total_nonid_cols == 4


def test_vote_low_match_rate_clears_winner():
    """When the winner covers <40% of non-id cols, hint is cleared (issue #9)."""
    # 6 non-id columns; winner reg=366 matches only 2 → 2/6 = 33% < 40%
    col_to_registers = {
        "ar": [366],
        "hman": [366, 190],
        "lar2021_veckor_netto": [],
        "wk": [],
        "foo": [],
        "bar": [],
    }
    result = _vote_register(
        ["Ar", "Hman", "LAR2021_veckor_netto", "wk", "Foo", "Bar"],
        col_to_registers,
        "Distansutb_grund_HT20_VT21.csv",
    )
    assert result.register_id is None
    # Candidates surface the ambiguity: 366 is the (low-confidence) top candidate
    top = result.candidates[0]
    assert top.register_id == 366
    assert top.match_count == 2
    assert top.total_nonid_cols == 6
    # 190 only matches one column, so appears lower
    ids = [c.register_id for c in result.candidates]
    assert ids.index(366) < ids.index(190)


def test_vote_candidates_sorted_by_match_count():
    col_to_registers = {
        "a": [1],
        "b": [1, 2],
        "c": [1, 2, 3],
    }
    result = _vote_register(["a", "b", "c"], col_to_registers, "f.csv")
    match_counts = [c.match_count for c in result.candidates]
    assert match_counts == sorted(match_counts, reverse=True)
    assert result.candidates[0].register_id == 1


def test_vote_no_matches_empty_candidates():
    result = _vote_register(["x", "y"], {"x": [], "y": []}, "unknown.csv")
    assert result.register_id is None
    assert result.candidates == []


def test_vote_filename_fallback_when_low_confidence():
    """Filename-based fallback kicks in when the column vote is low-confidence."""
    # Vote produces nothing; fallback recognizes the Flergen delivery table.
    result = _vote_register([], {}, "FlergenUppg.csv")
    assert result.register_id == 349  # Flergenerationsregistret


def test_enrich_exposes_candidates_on_enriched_file(stats_path: Path, regmeta_db: Path):
    """Voted enrichment populates register_hint_candidates."""
    stats = parse_stats(stats_path)
    # Don't pass `register=` so enrich takes the voting path.
    result = enrich(stats, db_path=regmeta_db)
    assert result[0].register_hint_candidates  # at least one candidate
