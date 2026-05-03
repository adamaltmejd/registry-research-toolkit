"""Tests for stats enrichment."""

from __future__ import annotations

from pathlib import Path

import pytest
from regmeta.errors import RegmetaError

from mock_data_wizard.enrich import (
    EnrichedColumn,
    EnrichedSource,
    _bulk_fetch_value_codes,
    _check_value_code_drift,
    _name_score,
    _tokenize,
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
    assert result[0].source_name == "persons.csv"
    assert result[0].source_type == "file"
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
    names = {f.source_name for f in result}
    assert names == {"file_a.csv", "file_b.csv"}


def _make_enriched(
    source_name: str,
    col_name: str,
    frequencies: dict[str, int],
    value_codes: dict[str, str] | None,
) -> EnrichedSource:
    return EnrichedSource(
        source_name=source_name,
        source_type="file",
        source_detail={"path": source_name},
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


def test_drift_ignores_whitespace_only_observed_codes():
    # SCB tables sometimes encode "no value" as a blank-padded string
    # (e.g. "    "). These are sentinels, not drift.
    ef = _make_enriched(
        "f.csv", "Kon", {"1": 50, "    ": 5}, {"1": "Man", "2": "Kvinna"}
    )
    assert _check_value_code_drift([ef]) == []


def test_drift_strips_trailing_whitespace_for_comparison():
    # Fixed-width columns store padded codes ('1 ', '2 '); regmeta has the
    # clean code ('1', '2'). Compare on stripped form.
    ef = _make_enriched(
        "f.csv", "SsykStatus", {"1 ": 50, "2 ": 40}, {"1": "Foo", "2": "Bar"}
    )
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


def test_bulk_fetch_value_codes_filters_by_register_and_overlap(regmeta_db: Path):
    """Same var_id in two registers with conflicting code schemes: pick the
    CVID under the resolved register that matches the observed codes.

    This regression covers the production failure where var_id=15 (Civilstånd)
    in RTB has 1991-style numeric codes and in LISA has alphabetic codes — the
    old "max code count" picker silently mis-resolved.
    """
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a second register, same var_id=44, different CVID with conflicting codes.
    conn.executescript(
        """
        INSERT INTO register (register_id, registernamn) VALUES (2, 'OTHER');
        INSERT INTO register_variant (regvar_id, register_id, registervariantnamn,
            registervariantsekretess) VALUES (20, 2, 'Other variant', 'Nej');
        INSERT INTO register_version (regver_id, regvar_id, registerversionnamn)
            VALUES (200, 20, '1991');
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva)
            VALUES (2001, 2, 20, 200, 44, 'int', '1', 'Kön', '1');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (3, 'A', 'Alpha');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (4, 'B', 'Beta');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (5, 'C', 'Gamma');
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (2001, 3);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (2001, 4);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (2001, 5);
        """
    )
    conn.commit()

    # var_id=44 in reg=1: observed {"1","2"} matches CVID 1001's {"1","2"}.
    # var_id=44 in reg=2: observed {"A","B"} matches CVID 2001's {"A","B","C"}.
    requests = {
        ("a.csv", "Kon"): (44, 1, {"1", "2"}, "Kon"),
        ("b.csv", "Kon"): (44, 2, {"A", "B"}, "Kon"),
    }
    out = _bulk_fetch_value_codes(conn, requests)
    assert out[("a.csv", "Kon")] == {"1": "Man", "2": "Kvinna"}
    assert out[("b.csv", "Kon")] == {"A": "Alpha", "B": "Beta", "C": "Gamma"}


def test_bulk_fetch_value_codes_skips_when_no_overlap(regmeta_db: Path):
    """No name signal and no overlap → omit the entry. Better to leave
    value_codes unset than to enrich with an unrelated code universe.

    The column name `FooBar` shares no tokens with the fixture CVID's
    label `Kön`, so tier 1 doesn't fire; observed codes {X,Y,Z} also
    don't overlap CVID codes {1,2}, so tier 2 fails the floor.
    """
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    requests = {("f.csv", "FooBar"): (44, 1, {"X", "Y", "Z"}, "FooBar")}
    out = _bulk_fetch_value_codes(conn, requests)
    assert ("f.csv", "FooBar") not in out


def test_bulk_fetch_value_codes_name_match_beats_overlap_tie(regmeta_db: Path):
    """Two CVIDs under the same (var_id, register_id) with overlapping code
    sets — name/classification metadata picks the right one even when raw
    overlap ties. Issue #26: SUN2000Inr-style columns whose observed codes
    are a subset of both SUN2000 and SUN2020 must resolve to SUN2000.
    """
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Two CVIDs under the same (var=44, reg=1). CVID 1101 is SUN2000 (5 codes
    # 1..5); CVID 1102 is SUN2020 (7 codes 1..7). Observed {1,2,3,4,5} is a
    # full subset of both — tier-2 overlap ties at 5/5; without name signal
    # the larger code_count (1102) wins. With classification metadata, the
    # name-match picker prefers 1101.
    conn.executescript(
        """
        INSERT INTO classification (id, short_name, name)
            VALUES (1, 'SUN2000', 'Svensk utbildningsnomenklatur 2000');
        INSERT INTO classification (id, short_name, name)
            VALUES (2, 'SUN2020', 'Svensk utbildningsnomenklatur 2020');
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva,
            classification_id)
            VALUES (1101, 1, 10, 100, 44, 'int', '4', 'SUN2000', '4', 1);
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva,
            classification_id)
            VALUES (1102, 1, 10, 100, 44, 'int', '4', 'SUN2020', '4', 2);
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (101, '1', 'A');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (102, '2', 'B');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (103, '3', 'C');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (104, '4', 'D');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (105, '5', 'E');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (106, '6', 'F');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (107, '7', 'G');
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1101, 101);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1101, 102);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1101, 103);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1101, 104);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1101, 105);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 101);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 102);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 103);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 104);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 105);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 106);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1102, 107);
        """
    )
    conn.commit()

    requests = {
        ("f.csv", "Sun2000Inr"): (44, 1, {"1", "2", "3", "4", "5"}, "Sun2000Inr"),
    }
    out = _bulk_fetch_value_codes(conn, requests)
    # SUN2000 has 5 codes; SUN2020 has 7. Picking by name → SUN2000 (5 codes).
    assert out[("f.csv", "Sun2000Inr")] == {
        "1": "A",
        "2": "B",
        "3": "C",
        "4": "D",
        "5": "E",
    }


def test_bulk_fetch_value_codes_overlap_below_threshold_omits(regmeta_db: Path):
    """Issue #25: when no CVID has a name signal AND none meets
    MIN_OVERLAP_RATIO, omit the entry rather than mis-enriching with an
    unrelated code universe.

    Regression for the BTYP failure: observed {0..9, B, F, H, L, P} (15
    codes) against a CVID with {A, E, I, S} — zero overlap. The old picker
    would silently emit the {A, E, I, S} universe; we want no enrichment.
    """
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a CVID for var_id=44 in register 1 with codes {A,E,I,S} and no
    # classification metadata (so name-match has nothing to latch onto).
    conn.executescript(
        """
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva)
            VALUES (1201, 1, 10, 100, 44, 'char', '1', 'FamStF', '1');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (201, 'A', 'Alpha');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (202, 'E', 'Echo');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (203, 'I', 'India');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (204, 'S', 'Sierra');
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1201, 201);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1201, 202);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1201, 203);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1201, 204);
        """
    )
    conn.commit()

    requests = {
        ("f.csv", "BTyp"): (
            44,
            1,
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "F", "H", "L", "P"},
            "BTyp",
        ),
    }
    out = _bulk_fetch_value_codes(conn, requests)
    # The fixture's pre-existing CVID 1001 is also in scope: codes {"1","2"}.
    # 2/15 = 0.13 < 0.5 → also fails. CVID 1201: 0/15 = 0 < 0.5 → fails.
    # Both candidates rejected, no entry emitted.
    assert ("f.csv", "BTyp") not in out


def test_bulk_fetch_value_codes_per_column_when_var_reg_shared(regmeta_db: Path):
    """Two columns resolving to the same (var_id, register_id) but with
    different observed codes must each get their own CVID pick.

    Real case: Individ_2019 has both Sun2000Inr and Sun2020Inr, both
    resolved to (var=784, reg=34). Sun2000-only CVIDs include codes like
    `314z, 762g, 863c` that newer Sun2020-only CVIDs don't, and vice
    versa. Sharing a single CVID across both columns silently drops one
    side's codes.
    """
    import sqlite3

    conn = sqlite3.connect(str(regmeta_db))
    conn.row_factory = sqlite3.Row
    # Add a second CVID under the same register/variable with different codes.
    conn.executescript(
        """
        INSERT INTO variable_instance (cvid, register_id, regvar_id, regver_id,
            var_id, datatyp, datalangd, vardemangdsversion, vardemangdsniva)
            VALUES (1002, 1, 10, 100, 44, 'int', '1', 'Kön', '1');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (3, '3', 'X');
        INSERT INTO value_code (code_id, vardekod, vardebenamning) VALUES (4, '4', 'Y');
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1002, 3);
        INSERT INTO cvid_value_code (cvid, code_id) VALUES (1002, 4);
        """
    )
    conn.commit()

    # Both columns resolve to (var=44, reg=1) but observe disjoint codes.
    requests = {
        ("f.csv", "ColA"): (44, 1, {"1", "2"}, "ColA"),
        ("f.csv", "ColB"): (44, 1, {"3", "4"}, "ColB"),
    }
    out = _bulk_fetch_value_codes(conn, requests)
    assert out[("f.csv", "ColA")] == {"1": "Man", "2": "Kvinna"}
    assert out[("f.csv", "ColB")] == {"3": "X", "4": "Y"}


# ---------------------------------------------------------------------------
# _tokenize / _name_score: Unicode and SCB naming conventions
# ---------------------------------------------------------------------------


def test_tokenize_handles_camelcase_with_digits():
    assert _tokenize("Sun2000Inr") == ["sun", "2000", "inr"]


def test_tokenize_handles_uppercase_runs():
    # All-caps runs were the original blind spot: SUN/SSYK/SNI vanished.
    assert _tokenize("SUN2000") == ["sun", "2000"]
    assert _tokenize("SSYK4") == ["ssyk"]  # '4' dropped by ≥2 filter
    assert _tokenize("SNI2007") == ["sni", "2007"]


def test_tokenize_strips_swedish_diacritics():
    # SCB column names typically strip diacritics (`Kon`, `Fodelseland`)
    # while regmeta labels keep them (`Kön`, `Födelseland`). Both forms
    # must produce the same token set so the picker can match across them.
    assert _tokenize("Kön") == ["kon"]
    assert _tokenize("Födelseland") == _tokenize("Fodelseland") == ["fodelseland"]
    assert _tokenize("Älder") == _tokenize("Alder") == ["alder"]


def test_name_score_matches_diacritic_stripped_form():
    # Column "Kon" should match a CVID labeled "Kön" via diacritic folding,
    # even though the raw strings differ in their middle character.
    score = _name_score("Kon", "Kön")
    assert score[0] >= 1  # at least one shared token


def test_name_score_matches_uppercase_abbreviation():
    # Column SsykYrkeskod against CVID with vardemangdsversion "SSYK2012":
    # tokens [ssyk, yrkeskod] vs [ssyk, 2012] share 'ssyk'.
    score = _name_score("SsykYrkeskod", "SSYK2012")
    assert score[0] >= 1


def test_name_score_no_match_returns_zero():
    # Unrelated names produce no signal, leaving (0, 0) → tier-2 fallback.
    assert _name_score("BTyp", "FamStF") == (0, 0)


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
