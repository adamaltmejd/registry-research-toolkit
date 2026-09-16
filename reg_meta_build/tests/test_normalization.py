"""Field-aware cleaning preserves meaning and is idempotent."""

from hypothesis import given, strategies as st
from reg_meta_build.normalization import (
    canonical_value_set_content,
    normalize_text,
    normalize_token,
)


def test_labels_fold_whitespace_and_unicode_without_folding_spelling() -> None:
    assert normalize_text("  A\u030arsinkomst\u00a0 \t fo\u0308re\r\nskatt  ") == (
        "Årsinkomst före skatt"
    )
    assert normalize_text("Kön — Ålder ≤ 20") == "Kön — Ålder ≤ 20"
    assert normalize_text("a\u200bb &amp; c\u00add") == "a\u200bb &amp; c\u00add"


def test_multiline_text_retains_paragraphs_lists_and_table_alignment() -> None:
    raw = "\r\nRubrik\u00a0\t\r\n\r\n  - A\r\n    Fortsättning\r\nA  B\tC \r\n"
    assert normalize_text(raw, multiline=True) == (
        "Rubrik\n\n  - A\n    Fortsättning\nA  B\tC"
    )


def test_tokens_preserve_significant_code_characters() -> None:
    assert normalize_token(" 001.0 ") == "001.0"
    assert normalize_token(" A  B ") == "A  B"
    assert normalize_token(" Ko\u0308n ") == "Kön"
    assert normalize_token("NULL") == "NULL"
    assert normalize_token("-") == "-"
    assert normalize_token("01") != normalize_token("1")
    assert normalize_token("A") != normalize_token("a")


def test_value_sets_share_clean_content_but_keep_conflicting_labels_and_codes() -> None:
    original = [("01", " A\u030ar "), ("2", "Uppgift\u00a0saknas"), ("01", "År")]
    expected = (("01", "År"), ("2", "Uppgift saknas"))
    assert canonical_value_set_content(original) == expected
    assert canonical_value_set_content(reversed(original)) == expected
    assert canonical_value_set_content([*original, ("01", "Månad"), ("1", "År")]) == (
        ("01", "Månad"),
        ("01", "År"),
        ("1", "År"),
        ("2", "Uppgift saknas"),
    )


@given(st.text())
def test_cleaning_is_idempotent(text: str) -> None:
    for multiline in (False, True):
        cleaned = normalize_text(text, multiline=multiline)
        assert normalize_text(cleaned, multiline=multiline) == cleaned
    assert normalize_token(normalize_token(text)) == normalize_token(text)
