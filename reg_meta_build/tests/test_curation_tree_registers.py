"""Strict per-register TOML contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.curation_tree import AcknowledgeEntry, load_register_files

if TYPE_CHECKING:
    from pathlib import Path


_TABLES = (
    (
        "errata.delivered",
        '[[errata.delivered]]\nvariant = "v"\ncolumn = "C"\n'
        'versions = ["2020"]\nevidence = "source"\nnoted = "2026-09-24"\n',
        None,
    ),
    (
        "errata.column",
        '[[errata.column]]\nvariant = "v"\ncolumn = "C"\nname = "Name"\n'
        'definition = "Definition"\nsource = "scb-docs"\nevidence = "source"\n'
        'noted = "2026-09-24"\n',
        None,
    ),
    (
        "errata.version",
        '[[errata.version]]\nvariant = "v"\nname = "2020"\n'
        'evidence = "source"\nnoted = "2026-09-24"\n',
        None,
    ),
    (
        "enrichment.description",
        '[[enrichment.description]]\nregister = "scb/test"\n'
        'variable = "v"\ndescription = "Description"\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "enrichment.alias",
        '[[enrichment.alias]]\nregister = "scb/test"\n'
        'variable = "v"\ndelivery_column = "C"\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "group",
        '[[group]]\nregister = "scb/test"\nkey = "family"\nlabel = "Family"\n'
        'axis = "rank"\nmembers = [{ variable = "v", value = "1", label = "One" }]\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "code_label_pair",
        '[[code_label_pair]]\ncode = "scb/test/code"\nlabel = "scb/test/label"\n',
        ('code = "scb/test/code"', 'code = "scb/other/code"'),
    ),
    (
        "representation.period_family",
        '[[representation.period_family]]\nregister = "scb/test"\n'
        'family_stem = "income"\nlabel = "Income"\nslug = "income"\n',
        ('register = "scb/test"', 'register = "scb/other"'),
    ),
    (
        "representation.alias_window",
        '[[representation.alias_window]]\nvariable = "scb/test/v"\n'
        'variant = "variant"\ncolumn = "C"\nsource_editions = ["2020"]\n'
        'evidence = "source"\nnoted = "2026-09-24"\n',
        ('variable = "scb/test/v"', 'variable = "scb/other/v"'),
    ),
    (
        "identity.partition",
        '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n'
        'columns_ref = "source"\n',
        ('variable = "1.2"', 'variable = "2.2"'),
    ),
    (
        "identity.column_owner",
        '[[identity.column_owner]]\nvariable = "1.2"\nvariant = "1.3"\n'
        'column = "C"\nowner = "1.2.c"\nref = "source"\n',
        ('variable = "1.2"', 'variable = "2.2"'),
    ),
    (
        "identity.route",
        '[[identity.route]]\ndeldatamangd = "TOKEN"\nvariants = ["Variant"]\n',
        ("__provider__", "scb"),
    ),
    (
        "identity.split",
        '[[identity.split]]\nvariable = "NAME"\nby = "data_type"\n'
        'parts = [{ data_type = "text", owner = "1.NAME.text" }]\n',
        ('owner = "1.NAME.text"', 'owner = "2.NAME.text"'),
    ),
    (
        "identity.rename",
        '[[identity.rename]]\ndeldatamangd = "TOKEN"\nvariable = "OLD"\n'
        'name = "Name"\ncolumn = "NEW"\n',
        ("__provider__", "scb"),
    ),
    (
        "acknowledge",
        '[[acknowledge]]\ncode = "code"\nsubject = "scb/test/subject"\n'
        'refs = ["source"]\nreason = "reason"\nevidence = "evidence"\n',
        None,
    ),
)


def _write_register(
    root: Path,
    body: str,
    *,
    slug: str = "test",
    provider: str = "scb",
    native_id: str = "1",
) -> Path:
    path = root / "registers" / provider / f"{slug}.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'[register]\nprovider = "{provider}"\nslug = "{slug}"\nnative_id = "{native_id}"\n\n'
        + body,
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(("table", "body", "wrong_register"), _TABLES)
def test_each_register_table_loads(
    tmp_path: Path, table: str, body: str, wrong_register: tuple[str, str] | None
) -> None:
    root = tmp_path / "curation"
    provider = "sos" if table in {"identity.route", "identity.rename"} else "scb"
    _write_register(root, body, provider=provider)
    (entry,) = load_register_files(root)
    assert entry.register_info.slug == "test"
    assert table


@pytest.mark.parametrize(
    ("override", "message"),
    (
        (
            {"valid_from": "2020-02-01", "valid_to": "2020-01-31"},
            "valid_to must not precede",
        ),
        ({"valid_from": "2020-01-01"}, "both be set or both omitted"),
        ({"valid_to": "2020-01-01"}, "both be set or both omitted"),
        ({"valid_from": "2020-1-01", "valid_to": "2020-12-31"}, "canonical ISO date"),
        ({"valid_from": "2020-02-30", "valid_to": "2020-12-31"}, "canonical ISO date"),
        ({"fields": ["name", "name"]}, "fields must be unique"),
        ({"fields": [" name"]}, "trimmed"),
    ),
)
def test_acknowledge_rejects_invalid_fields_and_period(override, message) -> None:
    values = {
        "code": "issue",
        "subject": "scb/test/subject",
        "refs": ["source"],
        "reason": "reason",
        "evidence": "evidence",
        **override,
    }
    with pytest.raises(ValidationError, match=message):
        AcknowledgeEntry.model_validate(values)


def test_acknowledge_keeps_ordered_fields_and_period() -> None:
    entry = AcknowledgeEntry(
        code="issue",
        subject="scb/test/subject",
        refs=["source"],
        fields=["description", "name"],
        valid_from="2020-01-01",
        valid_to="2020-12-31",
        reason="reason",
        evidence="evidence",
    )
    assert entry.fields == ["description", "name"]
    assert (entry.valid_from, entry.valid_to) == ("2020-01-01", "2020-12-31")


def test_edition_split_requires_declared_native_split_and_unique_names(
    tmp_path: Path,
) -> None:
    root = tmp_path / "curation"
    base = (
        '[[variant]]\nnative_id = "1.2.stock"\nslug = "stock"\n'
        '[[identity.edition_split]]\nvariant = "1.2"\n'
        'split = "1.2.stock"\neditions = ["2007-12-31"]\n'
        'evidence = "SCB population text"\nnoted = "2026-09-27"\n'
    )
    path = _write_register(root, base)
    assert load_register_files(root)[0].identity.edition_split[0].editions == [
        "2007-12-31"
    ]
    path.write_text(
        path.read_text().replace(
            'editions = ["2007-12-31"]',
            'editions = ["2007-12-31", "2007-12-31"]',
        )
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "editions must be non-empty and unique" in exc.value.message
    path.write_text(
        path.read_text().replace(
            'editions = ["2007-12-31", "2007-12-31"]',
            'editions = ["2007-12-31"]',
        )
    )
    path.write_text(path.read_text() + base[base.index("[[identity.edition_split]]") :])
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "editions assigned twice" in exc.value.message
    path.write_text(
        path.read_text().replace('split = "1.2.stock"', 'split = "1.3.stock"')
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "three-part key under variant" in exc.value.message


@pytest.mark.parametrize(("table", "body", "wrong_register"), _TABLES)
def test_each_register_table_rejects_unknown_keys(
    tmp_path: Path, table: str, body: str, wrong_register: tuple[str, str] | None
) -> None:
    root = tmp_path / "curation"
    provider = "sos" if table in {"identity.route", "identity.rename"} else "scb"
    _write_register(root, body + 'unknown_key = "typo"\n', provider=provider)
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert f"registers/{provider}/test.toml" in exc.value.message
    assert "entry 1" in exc.value.message


@pytest.mark.parametrize(
    ("table", "body", "wrong_register"),
    [(table, body, mismatch) for table, body, mismatch in _TABLES if mismatch],
)
def test_register_scoped_entries_reject_wrong_register(
    tmp_path: Path,
    table: str,
    body: str,
    wrong_register: tuple[str, str],
) -> None:
    root = tmp_path / "curation"
    if wrong_register[0] == "__provider__":
        provider = "scb"
    else:
        provider = "sos" if table in {"identity.route", "identity.rename"} else "scb"
        body = body.replace(*wrong_register)
        if table == "identity.partition":
            body = body.replace('"1.2.c"', '"2.2.c"')
        if table == "identity.column_owner":
            body = body.replace('owner = "1.2.c"', 'owner = "2.2.c"')
    _write_register(root, body, provider=provider)
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert table in exc.value.message
    assert "entry 1" in exc.value.message
    if table in {"identity.route", "identity.rename"}:
        assert "SOS registers" in exc.value.message
    else:
        assert (
            "does not match" in exc.value.message
            or "does not belong" in exc.value.message
        )


def test_register_path_mismatch_fails(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, "", slug="other")
    path = root / "registers" / "scb" / "test.toml"
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "1"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "does not match its path" in exc.value.message


def test_two_files_for_one_register_fail(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, "")
    duplicate = root / "registers" / "scb" / "family" / "test.toml"
    duplicate.parent.mkdir(parents=True)
    duplicate.write_text(
        '[register]\nprovider = "scb"\nslug = "test"\nnative_id = "1"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "duplicate register scb/test" in exc.value.message


def test_duplicate_native_id_fails(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, "", slug="first")
    _write_register(root, "", slug="second")
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "duplicate native_id '1'" in exc.value.message


def test_register_source_labels_load(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, 'source_labels = ["Former register name (AGI)"]\n')
    (entry,) = load_register_files(root)
    assert entry.register_info.source_labels == ["Former register name (AGI)"]


def test_duplicate_source_labels_across_registers_fail(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, 'source_labels = ["Shared source (AGI)"]\n', slug="first")
    _write_register(
        root,
        'source_labels = ["shared source (agi)"]\n',
        slug="second",
        native_id="2",
    )
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "duplicate source label" in exc.value.message
    assert "registers/scb/first.toml" in exc.value.message
    assert "registers/scb/second.toml" in exc.value.message


def test_unknown_top_level_table_fails(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    _write_register(root, '[unexpected]\nvalue = "typo"\n')
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "unexpected" in exc.value.message
    assert "registers/scb/test.toml" in exc.value.message


def test_duplicate_table_entry_fails_with_file_and_index(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    body = (
        '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n'
        'columns_ref = "source"\n'
        '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n'
        'columns_ref = "source"\n'
    )
    _write_register(root, body)
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "registers/scb/test.toml" in exc.value.message
    assert "identity.partition" in exc.value.message
    assert "entry 2" in exc.value.message


@pytest.mark.parametrize(
    ("body", "expected"),
    (
        (
            '[[identity.partition]]\nvariable = "1.2"\ncolumns = {}\n'
            'columns_ref = "source"\n',
            "at least one owned literal",
        ),
        (
            '[[identity.partition]]\nvariable = "1.2"\n'
            'columns = { " C" = "1.2.c" }\ncolumns_ref = "source"\n',
            "must be non-empty and trimmed",
        ),
        (
            '[[identity.partition]]\nvariable = "1.2"\n'
            'columns = { C = "1.3.c" }\ncolumns_ref = "source"\n',
            "canonical split key in family",
        ),
        (
            '[[identity.partition]]\nvariable = "1.2"\ncolumns = { C = "1.2.c" }\n',
            "columns_ref",
        ),
    ),
)
def test_identity_partition_requires_literal_map_and_reference(
    tmp_path: Path, body: str, expected: str
) -> None:
    root = tmp_path / "curation"
    _write_register(root, body)
    with pytest.raises(RegMetaError) as exc:
        load_register_files(root)
    assert "registers/scb/test.toml" in exc.value.message
    assert "identity.partition" in exc.value.message
    assert "entry 1" in exc.value.message
    assert expected in exc.value.message
