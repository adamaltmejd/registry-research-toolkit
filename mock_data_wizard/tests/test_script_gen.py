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
    # Both paths must appear as file_source() entries
    assert content.count("file_source(") >= 2


def _extract_sources_block(content: str) -> str:
    """Pull just the `SOURCES <- list( ... )` block from an R script."""
    start = content.index("SOURCES <- list(")
    depth = 0
    for i in range(start + len("SOURCES <- list"), len(content)):
        if content[i] == "(":
            depth += 1
        elif content[i] == ")":
            depth -= 1
            if depth == 0:
                return content[start : i + 1]
    raise AssertionError("unterminated SOURCES block")


def test_sql_dsn_emits_sql_source_skeleton(tmp_path: Path):
    """When a DSN is provided, SOURCES includes both file_source and sql_source."""
    out = tmp_path / "test.R"
    generate_script(
        ["\\\\micro.intra\\projekt\\P1405$\\P1405_Data"],
        out,
        sql_dsn="P1405",
    )
    block = _extract_sources_block(out.read_text())
    assert "file_source(" in block
    assert 'sql_source(dsn = "P1405")' in block


def test_no_sql_dsn_omits_sql_source(tmp_path: Path):
    """Without a DSN, SOURCES has only file_source entries (sql_source not invoked)."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    block = _extract_sources_block(out.read_text())
    assert "file_source(" in block
    # Constructor still defined in library code; just not invoked in SOURCES
    assert "sql_source(" not in block


def test_user_config_and_library_sections_present(tmp_path: Path):
    """The generated script splits into USER CONFIGURATION and LIBRARY CODE."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "USER CONFIGURATION" in content
    assert "LIBRARY CODE" in content
    # SOURCES must be defined in the user section
    assert "SOURCES <- list(" in content
    # Both constructors must be defined above SOURCES
    assert "file_source <- function(" in content
    assert "sql_source <- function(" in content


def test_source_iter_dispatch_present(tmp_path: Path):
    """Sources dispatch via source_iter() -- a streaming iterator so Main
    can fetch, process, and free one table at a time (prevents peak-RSS
    blowup on projects with many large SQL tables)."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "source_iter <- function(src)" in content
    assert "source_iter_file <- function(src)" in content
    assert "source_iter_sql <- function(src)" in content
    # Main loop emits source_name / source_type / source_detail into stats.json
    assert "source_name" in content
    assert "source_type" in content
    assert "source_detail" in content


def test_sql_source_uses_dbi_and_odbc(tmp_path: Path):
    """sql_source() connects via DBI + odbc with DSN-only credentials."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    # Production connect path uses odbc driver
    assert "odbc::odbc()" in content
    assert "DBI::dbConnect" in content
    # DSN is a required arg on sql_source
    assert "sql_source <- function(dsn" in content
    # Credential-safety: no raw password parameter on the sql_source() signature
    import re

    sig_match = re.search(r"sql_source <- function\((.*?)\) \{", content, re.DOTALL)
    assert sig_match, "couldn't find sql_source signature"
    params = sig_match.group(1).lower()
    assert "password" not in params, "sql_source must not accept a password= arg"
    assert "pwd" not in params, "sql_source must not accept a pwd= arg"


def test_sql_discovery_uses_information_schema(tmp_path: Path):
    """sql_list_tables queries information_schema.tables WHERE TABLE_TYPE='VIEW'."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "information_schema.tables" in content
    assert "TABLE_TYPE = 'VIEW'" in content
    # Archived x_* views are excluded by default
    assert "exclude_archived" in content


def test_ensure_package_bootstraps_missing_deps(tmp_path: Path):
    """ensure_package() is defined and used for data.table/jsonlite/DBI/odbc."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    # Helper is defined once
    assert content.count("ensure_package <- function(pkg)") == 1
    # Applied to both always-needed and SQL-only packages
    assert 'ensure_package("data.table")' in content
    assert 'ensure_package("jsonlite")' in content
    assert 'ensure_package("DBI")' in content
    assert 'ensure_package("odbc")' in content
    # Guards against the "@CRAN@" sentinel so Batch mode doesn't prompt
    assert "@CRAN@" in content


def test_sql_test_hook_present(tmp_path: Path):
    """sql_connect respects options(mdw.sql_connect) so RSQLite tests can override it."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "mdw.sql_connect" in content
    assert "getOption" in content


def test_discovery_mode_gate_present(tmp_path: Path):
    """Main loop gates on needs_discovery() and emits a suggested SOURCES block."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "needs_discovery <- function(src)" in content
    assert "DISCOVERY MODE" in content
    # Exits cleanly without writing stats.json in discovery mode
    assert 'quit(save = "no"' in content
    # Suggested block includes both source types
    assert "format_discovery_suggestion" in content
    # Writes suggestion to a timestamped file (avoids console encoding
    # mangling + huge .Rout scrolling for projects with many tables).
    assert "mdw_sources_" in content
    assert 'format(Sys.time(), "%Y%m%d_%H%M%S")' in content
    assert "writeLines(enc2utf8(" in content
    # Dedupes basenames so the same filename doesn't appear twice
    assert "duplicated(names_vec)" in content


def test_all_flag_opts_out_of_discovery(tmp_path: Path):
    """`all = TRUE` on a source opts out of discovery mode."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    # Constructors accept `all` parameter
    assert "all = FALSE" in content  # default in both signatures
    # needs_discovery() short-circuits on isTRUE(src$all)
    assert "if (isTRUE(src$all)) return(FALSE)" in content
    # source_iter_sql handles the all-tables branch
    assert "All-tables mode" in content
    # Comment block documents the shortcut
    assert "all = TRUE" in content
    assert "no discovery dance" in content.lower()


def test_auto_load_mdw_sources_file(tmp_path: Path):
    """The generated script auto-loads mdw_sources_<ts>.R if present."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    # Looks for the timestamped file, takes the latest by filename, sources it
    assert "mdw_sources_" in content
    assert "list.files" in content
    assert "source(.mdw_loaded_sources_file" in content
    # Refuses to overwrite an existing loaded file if it's still un-narrowed
    assert "Loaded" in content and "Edit that file to narrow" in content


def test_small_population_warning_present(tmp_path: Path):
    """PII guardrail: script warns when a source has fewer than SMALL_POP_MULT*SUPPRESS_K rows."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert "SMALL_POP_MULT" in content
    assert "identifiable" in content


def test_contract_version_v2(tmp_path: Path):
    """The generated R script emits contract_version 2.x."""
    out = tmp_path / "test.R"
    generate_script(["\\\\server\\share"], out)
    content = out.read_text()
    assert 'contract_version = "2.' in content


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
    assert "SUPPRESS_K" in content
    assert "NOISE_PCT" in content


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


def test_cli_project_emits_sql_source_by_default(tmp_path: Path):
    """`generate-script -p P1405` puts both file_source and sql_source in SOURCES."""
    from mock_data_wizard.cli import main

    out = tmp_path / "extract.R"
    rc = main(["generate-script", "-p", "P1405", "-o", str(out)])
    assert rc == 0
    block = _extract_sources_block(out.read_text())
    assert "file_source(" in block
    assert 'sql_source(dsn = "P1405")' in block


def test_cli_no_sql_suppresses_skeleton(tmp_path: Path):
    """`--no-sql` omits the sql_source even when --project implies a DSN."""
    from mock_data_wizard.cli import main

    out = tmp_path / "extract.R"
    rc = main(["generate-script", "-p", "P1405", "--no-sql", "-o", str(out)])
    assert rc == 0
    block = _extract_sources_block(out.read_text())
    assert "file_source(" in block
    assert "sql_source(" not in block


def test_cli_explicit_sql_dsn_overrides_project(tmp_path: Path):
    """`--sql-dsn CUSTOM` overrides the P<num> default."""
    from mock_data_wizard.cli import main

    out = tmp_path / "extract.R"
    rc = main(
        [
            "generate-script",
            "-p",
            "P1405",
            "--sql-dsn",
            "MyCustomDSN",
            "-o",
            str(out),
        ]
    )
    assert rc == 0
    block = _extract_sources_block(out.read_text())
    assert 'sql_source(dsn = "MyCustomDSN")' in block
    assert 'sql_source(dsn = "P1405")' not in block
