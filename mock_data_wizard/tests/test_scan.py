"""Tests for scan.py -- pre-export PII scanner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mock_data_wizard.scan import (
    PIIScannerError,
    SCANNER_VERSION,
    _luhn_valid,
    scan_file,
    scan_payload,
    scan_string,
    write_export,
)
from .conftest import MINIMAL_STATS, SPINE_STATS


# -- helpers / building blocks -------------------------------------------


def test_luhn_recognises_valid_personnummer():
    # 198112289874 is a documented test personnummer (Luhn-valid).
    assert _luhn_valid("198112289874") is True


def test_luhn_rejects_arbitrary_runs():
    # Plain row-count-shaped digit run; very unlikely to be Luhn-valid.
    assert _luhn_valid("196402033657") is False
    assert _luhn_valid("123456789012") is False


# -- scan_string: positive matches ---------------------------------------


def test_personnummer_12_digit_match():
    hits = scan_string("Customer 198112289874 was here.")
    assert hits == ["personnummer"]


def test_personnummer_10_digit_with_separator_match():
    hits = scan_string("ID: 811228-9874")
    assert hits == ["personnummer"]


def test_personnummer_10_digit_bare_whole_string_match():
    """A bare 10-digit personnummer that IS the entire string (the leak
    vector: a misclassified column emitting it as a frequency-table key)."""
    hits = scan_string("8112289874")
    assert hits == ["personnummer"]
    # Surrounding whitespace must still match (whole-stripped-string).
    assert scan_string("  8112289874\n") == ["personnummer"]


def test_personnummer_10_digit_bare_in_narrative_text_does_not_match():
    """A 10-digit run inside surrounding text is too FP-prone (~0.4% of
    random 10-digit strings pass date+Luhn). We deliberately decline to
    match these -- the whole-string anchor catches the leak vector
    without expanding to row counts, paths, log lines, etc."""
    assert scan_string("Customer 8112289874 was here.") == []
    assert scan_string("row count: 8112289874") == []


def test_email_match():
    hits = scan_string("contact: jane.doe@example.com today")
    assert "email" in hits


def test_mobile_match():
    hits = scan_string("call 0701234567 anytime")
    assert "mobile" in hits


def test_personnummer_with_invalid_luhn_does_not_match():
    # Same shape as a personnummer, fails Luhn -> not flagged.
    hits = scan_string("198112289873")  # last digit off by one
    assert hits == []


def test_personnummer_with_invalid_date_does_not_match():
    # YYYY=1981, MM=99 -> reject before Luhn.
    hits = scan_string("198199289874")
    assert hits == []


def test_personnummer_with_calendar_invalid_day_does_not_match():
    """Feb 31 / Apr 31 etc. are mm/dd-in-range but not real dates -- the
    tighter validity gate must catch them before reaching Luhn."""
    # Iterate impossible mm/dd combinations; none of these can ever be a
    # real personnummer regardless of the last-4 checksum.
    for impossible in ("19810231", "19810431", "19810230", "20211131"):
        # Pad with arbitrary 4 digits; Luhn doesn't matter -- the date
        # gate rejects first.
        assert scan_string(impossible + "0000") == [], impossible


# -- scan_string: negative (must NOT match) ------------------------------


@pytest.mark.parametrize(
    "value",
    [
        # ICD-10 codes
        "J18.9",
        "A00.0",
        # KVA / SSYK codes
        "AC012",
        "1234",
        # 4-digit kommun code
        "0114",
        # 5-digit postnr
        "11434",
        # 6-digit YYYYMM
        "202401",
        # 8-digit YYYYMMDD that's a row count, not a personnummer (no LL part)
        "19501231",
        # UNC path with project number
        r"\\micro.intra\projekt\P1405$\P1405_Data\persons.csv",
    ],
)
def test_scan_string_negatives(value: str):
    assert scan_string(value) == []


# -- payload walkers -----------------------------------------------------


def test_scan_payload_clean_minimal_stats():
    matches = scan_payload(MINIMAL_STATS)
    assert matches == [], f"unexpected matches: {[str(m) for m in matches]}"


def test_scan_payload_clean_spine_stats():
    matches = scan_payload(SPINE_STATS)
    assert matches == []


def test_scan_payload_walks_into_dict_keys():
    """A misclassified column would write personnummer as frequency keys."""
    payload = {
        "frequencies": {
            "198112289874": 5,  # PII as a key, count as value
            "ok": 100,
        }
    }
    matches = scan_payload(payload)
    assert any(m.pattern == "personnummer" for m in matches)


def test_scan_payload_walks_into_lists():
    payload = {"items": [{"x": 1}, {"x": "jane@example.com"}]}
    matches = scan_payload(payload)
    assert any(m.pattern == "email" for m in matches)


def test_scan_payload_does_not_match_numeric_scalars():
    # 198112289874 as an integer should NOT trigger -- numbers are not
    # scanned (too noisy without a reason to opt in).
    payload = {"row_count": 198112289874}
    assert scan_payload(payload) == []


# -- write_export: temp-file + atomic rename -----------------------------


def test_write_export_clean_payload_creates_target_atomically(tmp_path: Path):
    out = tmp_path / "mdw_step3_stats.json"
    write_export(out, dict(MINIMAL_STATS))  # copy: write_export mutates
    assert out.exists()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["pii_scan"]["scanner_version"] == SCANNER_VERSION
    assert payload["pii_scan"]["matches_found"] == 0
    # Temp file must not be left behind.
    assert not (tmp_path / "mdw_step3_stats.json.tmp").exists()


def test_write_export_dirty_payload_blocks_target_creation(tmp_path: Path):
    out = tmp_path / "mdw_step3_stats.json"
    payload = dict(MINIMAL_STATS)
    payload["sources"] = list(MINIMAL_STATS["sources"])
    payload["sources"][0] = dict(MINIMAL_STATS["sources"][0])
    payload["sources"][0]["columns"] = list(MINIMAL_STATS["sources"][0]["columns"])
    payload["sources"][0]["columns"].append(
        {
            "column_name": "Personnr",
            "inferred_type": "categorical",
            "nullable": False,
            "n_distinct": 2,
            "stats": {"frequencies": {"198112289874": 50, "ok": 50}},
        }
    )
    with pytest.raises(PIIScannerError) as exc:
        write_export(out, payload)
    assert any(m.pattern == "personnummer" for m in exc.value.matches)
    # Target was never created; temp was cleaned up.
    assert not out.exists()
    assert not (tmp_path / "mdw_step3_stats.json.tmp").exists()


def test_write_export_redacts_matched_value_in_error_message(tmp_path: Path):
    """The matched value is redacted to "<prefix>***" in the message.

    Note: the JSON path can still contain the literal value when the
    match is on a dict key -- the operator needs *some* anchor to
    locate the offending column. The export is rejected either way."""
    out = tmp_path / "mdw_step3_stats.json"
    payload = {"Notes": {"comment": "jane.doe@example.com"}}
    with pytest.raises(PIIScannerError) as exc:
        write_export(out, payload)
    msg = str(exc.value)
    # The full email must NOT appear in the message.
    assert "jane.doe@example.com" not in msg
    # The redaction prefix shows the operator enough to identify the field.
    assert "jan***" in msg
    assert "email" in msg


# -- scan_file: standalone helper ----------------------------------------


def test_scan_file_returns_empty_on_clean(tmp_path: Path):
    p = tmp_path / "clean.json"
    p.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    assert scan_file(p) == []


def test_scan_file_finds_planted_pii(tmp_path: Path):
    p = tmp_path / "dirty.json"
    p.write_text(json.dumps({"frequencies": {"198112289874": 5}}), encoding="utf-8")
    matches = scan_file(p)
    assert any(m.pattern == "personnummer" for m in matches)
