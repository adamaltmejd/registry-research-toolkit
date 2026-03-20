"""Tests for R script generation."""

from __future__ import annotations

from pathlib import Path

from mock_data_wizard.script_gen import generate_script


def test_generates_r_file(tmp_path: Path):
    out = tmp_path / "test.R"
    result = generate_script(
        ["\\\\micro.intra\\projekt\\P1405$\\P1405_Data"],
        out,
    )
    assert result == out
    assert out.exists()
    content = out.read_text()
    assert "data.table" in content
    assert "jsonlite" in content


def test_contains_project_path(tmp_path: Path):
    out = tmp_path / "test.R"
    generate_script(
        ["\\\\micro.intra\\projekt\\P1405$\\P1405_Data"],
        out,
    )
    content = out.read_text()
    assert "micro.intra" in content
    assert "P1405" in content


def test_multiple_paths(tmp_path: Path):
    out = tmp_path / "test.R"
    generate_script(
        [
            "\\\\micro.intra\\projekt\\P1405$\\P1405_Data",
            "\\\\micro.intra\\projekt\\P1405$\\P1405_Extra",
        ],
        out,
    )
    content = out.read_text()
    assert "P1405_Data" in content
    assert "P1405_Extra" in content


def test_valid_r_syntax(tmp_path: Path):
    """Check the script has balanced braces and parens."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert content.count("{") == content.count("}")
    assert content.count("(") == content.count(")")


def test_pii_safety_thresholds(tmp_path: Path):
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "FREQ_CAP" in content
    assert "FREQ_RATIO" in content


def test_empty_paths_raises():
    import pytest

    with pytest.raises(ValueError, match="(?i)at least one"):
        generate_script([], Path("out.R"))


def test_creates_parent_dir(tmp_path: Path):
    out = tmp_path / "subdir" / "nested" / "test.R"
    generate_script(["\\\\server\\share"], out)
    assert out.exists()


def test_cli_project_shorthand(tmp_path: Path):
    from mock_data_wizard.cli import _parse_project_number, PROJECT_PATH_TEMPLATE

    assert _parse_project_number("P1405") == "1405"
    assert _parse_project_number("1405") == "1405"
    assert _parse_project_number("p1405") == "1405"

    path = PROJECT_PATH_TEMPLATE.format(num="1405")
    assert "P1405$" in path
    assert "P1405_Data" in path


def test_cli_project_invalid():
    import pytest

    from mock_data_wizard.cli import _parse_project_number

    with pytest.raises(ValueError, match="Invalid project number"):
        _parse_project_number("not-a-number")
