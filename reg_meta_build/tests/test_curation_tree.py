"""The curation tree reader rejects malformed classification files."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_tree import (
    ErrataClassificationReferenceEntry,
    ErrataDeliveredEntry,
    load_classification_families,
    load_classifications,
    load_curation_tree,
    load_register_files,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_delivered_anchor_is_optional_strict_int_and_rejects_extra_keys() -> None:
    entry = {
        "variant": "people",
        "column": "VALUE",
        "versions": ["2020"],
        "evidence": "Steward holdings",
        "noted": "2026-09-12",
    }
    assert ErrataDeliveredEntry.model_validate(entry).native_variable_id is None
    assert (
        ErrataDeliveredEntry.model_validate(
            {**entry, "native_variable_id": 39310}
        ).native_variable_id
        == 39310
    )
    for invalid in ("39310", True):
        with pytest.raises(ValidationError):
            ErrataDeliveredEntry.model_validate(
                {**entry, "native_variable_id": invalid}
            )
    with pytest.raises(ValidationError):
        ErrataDeliveredEntry.model_validate({**entry, "unknown": "value"})


def test_top_level_flags_table_is_unknown(tmp_path: Path) -> None:
    directory = tmp_path / "registers/scb"
    directory.mkdir(parents=True)
    (directory / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[flags]]\nvariable = "1.5"\nis_sensitive = true\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "registers/scb/sample.toml" in excinfo.value.message
    assert "flags" in excinfo.value.message
    assert "Extra inputs are not permitted" in excinfo.value.message


def _book(short_name: str, slug: str, *, binding: str = "", extra: str = "") -> str:
    return (
        f'[classification]\nshort_name = "{short_name}"\nslug = "{slug}"\n'
        f'name = "{short_name}"\ncodes_file = "{slug}.csv"\n{extra}\n'
        f"[binding]\n{binding}"
    )


def _write(root: Path, **files: str) -> Path:
    directory = root / "classifications"
    directory.mkdir(exist_ok=True)
    for short_name, text in files.items():
        (directory / f"{short_name}.toml").write_text(text, encoding="utf-8")
    return root


def _error(root: Path) -> RegMetaError:
    with pytest.raises(RegMetaError) as excinfo:
        load_classifications(root)
    assert excinfo.value.code == "classification_curation_invalid"
    return excinfo.value


def test_reads_files_in_sorted_order(tmp_path: Path) -> None:
    _write(
        tmp_path,
        B=_book("B", "b", binding='value_set_labels = ["Bee"]\n'),
        A=_book(
            "A",
            "a",
            extra='sentinel_codes = [{ code = "99", meaning = "Uppgift saknas" }]',
            binding='[[binding.variable]]\nvariable = "scb/ulf/kod"\n',
        ),
    )
    entries = load_classifications(tmp_path)
    assert [entry.classification.short_name for entry in entries] == ["A", "B"]
    assert [code.code for code in entries[0].classification.sentinel_codes] == ["99"]
    assert [bound.variable for bound in entries[0].binding.variable] == ["scb/ulf/kod"]
    assert entries[1].binding.value_set_labels == ("Bee",)


def test_aliases_and_family_members_are_strict_curated_data(tmp_path: Path) -> None:
    _write(
        tmp_path,
        A=_book(
            "A",
            "a",
            extra='aliases = ["Source A"]\nfamily = "pair"\n'
            'family_aliases = ["Source family"]\n',
        ),
        B=_book("B", "b", extra='family = "pair"\n'),
    )
    books = load_classifications(tmp_path)
    assert books[0].classification.aliases == ("Source A",)
    families = load_classification_families(books).family
    assert families[0].members == ("a", "b")
    assert families[0].aliases == ("Source family",)


def test_single_member_family_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", extra='family = "pair"\n'))
    with pytest.raises(RegMetaError) as excinfo:
        load_classification_families(load_classifications(tmp_path))
    assert "at least two distinct slugs" in excinfo.value.message


def test_duplicate_alias_in_one_classification_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", extra='aliases = ["same", "same"]\n'))
    assert "classification.aliases" in _error(tmp_path).message


def test_unknown_key_names_file_and_key(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", extra='version = "1"'))
    error = _error(tmp_path)
    assert "classifications/A.toml" in error.message
    assert "classification.version" in error.message


def test_unknown_top_level_table_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a") + '\n[[link]]\nvariable = "scb/ulf/kod"\n')
    assert "`link`" in _error(tmp_path).message


def test_unnormalized_label_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a", binding='value_set_labels = ["Bee "]\n'))
    error = _error(tmp_path)
    assert "classifications/A.toml" in error.message
    assert "'Bee'" in error.message


def test_label_in_two_files_fails(tmp_path: Path) -> None:
    _write(
        tmp_path,
        A=_book("A", "a", binding='value_set_labels = ["Bee"]\n'),
        B=_book("B", "b", binding='value_set_labels = ["Bee"]\n'),
    )
    assert _error(tmp_path).message == (
        "classifications/B.toml: label 'Bee' is also declared in "
        "classifications/A.toml."
    )


def test_slug_in_two_files_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "same"), B=_book("B", "same"))
    assert _error(tmp_path).message == (
        "classifications/B.toml: slug 'same' is also declared in "
        "classifications/A.toml."
    )


def test_variable_bound_twice_in_one_file_fails(tmp_path: Path) -> None:
    bound = '[[binding.variable]]\nvariable = "scb/ulf/kod"\n'
    _write(tmp_path, A=_book("A", "a", binding=bound + bound))
    assert _error(tmp_path).message == (
        "classifications/A.toml: variable 'scb/ulf/kod' is also declared in "
        "classifications/A.toml."
    )


def test_duplicate_sentinel_code_fails(tmp_path: Path) -> None:
    sentinels = (
        'sentinel_codes = [{ code = "99", meaning = "x" }, '
        '{ code = "99", meaning = "y" }]'
    )
    _write(tmp_path, A=_book("A", "a", extra=sentinels))
    error = _error(tmp_path)
    assert "classifications/A.toml" in error.message
    assert "more than once" in error.message


def test_short_name_must_match_file_name(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("B", "b"))
    assert "does not match the file name" in _error(tmp_path).message


def test_lineage_override_fails(tmp_path: Path) -> None:
    _write(tmp_path, A=_book("A", "a"))
    (tmp_path / "lineage.toml").write_text(
        '[lineage."scb/lisa/kon"]\nsource_register = "rams"\n'
        'source_variant = "individregister"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_curation_tree(tmp_path)
    assert excinfo.value.code == "lineage_invalid"


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ('valid_to = "2017-12-31"', "valid_to must not precede valid_from"),
        ('valid_from = "2018-02-30"', "valid_from must be an ISO date"),
        ("unexpected = true", "Extra inputs are not permitted"),
    ],
)
def test_edition_period_entry_is_strict(
    tmp_path: Path, replacement: str, message: str
) -> None:
    directory = tmp_path / "registers/scb"
    directory.mkdir(parents=True)
    entry = (
        '[[errata.edition_period]]\nvariant = "people"\nname = "Födelseland"\n'
        'valid_from = "2018-01-01"\nvalid_to = "2018-12-31"\n'
        'evidence = "Documentation"\nnoted = "2026-09-26"\n'
    )
    if replacement == "unexpected = true":
        entry += replacement + "\n"
    else:
        field = replacement.split(" = ", 1)[0]
        original = (
            f'{field} = "2018-01-01"'
            if field == "valid_from"
            else f'{field} = "2018-12-31"'
        )
        entry = entry.replace(original, replacement)
    (directory / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.2"\nslug = "people"\n' + entry,
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert (
        "curation/registers/scb/sample.toml [[errata.edition_period"
        in excinfo.value.message
    )
    assert "entry 1" in excinfo.value.message
    assert message in excinfo.value.message


def test_edition_period_rejects_duplicate_named_edition(tmp_path: Path) -> None:
    directory = tmp_path / "registers/scb"
    directory.mkdir(parents=True)
    (directory / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.2"\nslug = "people"\n'
        '[[errata.edition_period]]\nvariant = "people"\nname = "Födelseland"\n'
        'valid_from = "2018-01-01"\nvalid_to = "2018-12-31"\n'
        'evidence = "Document A"\nnoted = "2026-09-26"\n'
        '[[errata.edition_period]]\nvariant = "people"\nname = "Födelseland"\n'
        'valid_from = "2019-01-01"\nvalid_to = "2019-12-31"\n'
        'evidence = "Document B"\nnoted = "2026-09-27"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "[[errata.edition_period]] entry 2: duplicate edition period" in (
        excinfo.value.message
    )


_SOS_TYPE_ROW = (
    '[[errata.data_type]]\ndeldatamangd = "A_LOVA_HOSP"\n'
    'variable = "DESLEG_DATUM"\ncolumn = "DESLEG_DATUM"\n'
    'expected_type = "Decimal"\nexpected_representation = "YYYY-MM-DD"\n'
    'data_type = "date"\nevidence = "Workbook row 28"\nnoted = "2026-09-29"\n'
)


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ('expected_type = "date"', "expected_type and data_type must differ"),
        ('expected_representation = " "', "nonempty and trimmed"),
        ('noted = "20260929"', "canonical ISO date"),
        ('unknown = "x"', "Extra inputs are not permitted"),
    ],
)
def test_sos_data_type_row_is_checked_at_load(
    tmp_path: Path, replacement: str, message: str
) -> None:
    path = tmp_path / "registers/sos/sample.toml"
    path.parent.mkdir(parents=True)
    row = _SOS_TYPE_ROW
    if replacement.startswith("unknown"):
        row += replacement + "\n"
    else:
        field = replacement.split(" = ", 1)[0]
        row = row.replace(
            next(line for line in row.splitlines() if line.startswith(field + " = ")),
            replacement,
        )
    path.write_text(
        '[register]\nprovider = "sos"\nslug = "sample"\nnative_id = "1"\n' + row,
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "[[errata.data_type" in excinfo.value.message
    assert "entry 1" in excinfo.value.message
    assert message in excinfo.value.message


def test_sos_data_type_duplicate_target_and_non_sos_rejected(tmp_path: Path) -> None:
    path = tmp_path / "registers/sos/sample.toml"
    path.parent.mkdir(parents=True)
    header = '[register]\nprovider = "sos"\nslug = "sample"\nnative_id = "1"\n'
    path.write_text(
        header
        + _SOS_TYPE_ROW
        + _SOS_TYPE_ROW.replace(
            'evidence = "Workbook row 28"', 'evidence = "Other evidence"'
        ),
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "duplicate data type target" in excinfo.value.message
    path.unlink()
    other = tmp_path / "registers/scb/sample.toml"
    other.parent.mkdir(parents=True)
    other.write_text(
        header.replace('provider = "sos"', 'provider = "scb"') + _SOS_TYPE_ROW,
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "applies to SOS registers" in excinfo.value.message


def test_sos_classification_reference_entry_is_bounded_and_keeps_inner_spaces() -> None:
    row = {
        "deldatamangd": "MFR",
        "variable": "SECMARK",
        "column": "SECMARK",
        "expected_reference": "Kodlista_förlossningssätt!A1",
        "expected_representation": "1 = ja" + " " * 538 + "0 = nej",
        "evidence": "Workbook row 179 and derivation sheet rows 6-7.",
        "noted": "2026-09-29",
    }
    entry = ErrataClassificationReferenceEntry.model_validate(row)
    assert len(entry.expected_representation) == 551
    assert entry.expected_representation == row["expected_representation"]
    for field, value in (
        ("deldatamangd", " MFR"),
        ("variable", ""),
        ("column", "SECMARK "),
        ("expected_reference", " "),
        ("expected_representation", " 1 = ja"),
        ("evidence", ""),
        ("noted", "20260929"),
        ("replacement", "FIX"),
    ):
        with pytest.raises(ValidationError):
            ErrataClassificationReferenceEntry.model_validate({**row, field: value})


def test_sos_classification_reference_duplicate_and_non_sos_rejected(
    tmp_path: Path,
) -> None:
    row = (
        '[[errata.classification_reference]]\ndeldatamangd = "MFR"\n'
        'variable = "SECMARK"\ncolumn = "SECMARK"\n'
        'expected_reference = "Kodlista_förlossningssätt!A1"\n'
        'expected_representation = "1 = ja  0 = nej"\n'
        'evidence = "Workbook row 179"\nnoted = "2026-09-29"\n'
    )
    path = tmp_path / "registers/sos/sample.toml"
    path.parent.mkdir(parents=True)
    header = '[register]\nprovider = "sos"\nslug = "sample"\nnative_id = "1"\n'
    path.write_text(header + row + row.replace("row 179", "row 194"))
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "duplicate classification reference target" in excinfo.value.message
    path.unlink()
    other = tmp_path / "registers/scb/sample.toml"
    other.parent.mkdir(parents=True)
    other.write_text(header.replace('provider = "sos"', 'provider = "scb"') + row)
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "applies to SOS registers" in excinfo.value.message


@pytest.mark.parametrize(
    "by, part",
    [
        ("deldatamangd", 'data_type = "text", deldatamangd = "PAR_OV"'),
        ("deldatamangd", ""),
        ("deldatamangd", 'data_type = "text"'),
        ("data_type", 'deldatamangd = "PAR_OV"'),
        ("name", 'name = "Diagnosis", data_type = "text"'),
        ("name", 'data_type = "text"'),
    ],
)
def test_identity_split_part_requires_matching_discriminator(
    tmp_path: Path, by: str, part: str
) -> None:
    path = tmp_path / "registers/sos/par.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "sos"\nslug = "par"\n'
        'native_id = "5891427617861710725"\nname = "Patientregistret"\n'
        f'[[identity.split]]\nvariable = "ATC"\nby = "{by}"\n'
        f"parts = [{{ {part}{', ' if part else ''}"
        'owner = "5891427617861710725.ATC.atc" }]\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_register_files(tmp_path)
    assert "registers/sos/par.toml" in excinfo.value.message
    assert "identity.split" in excinfo.value.message


@pytest.mark.parametrize(
    "provider, coverage, edition, error",
    [
        ("scb", False, False, "require an exact native edition"),
        ("sos", False, False, "require both supplied coverage guards"),
        ("sos", True, False, None),
        ("scb", False, True, None),
    ],
)
def test_period_correction_requires_provider_source_coordinates(
    tmp_path: Path, provider, coverage, edition, error
):
    path = tmp_path / "registers" / provider / "sample.toml"
    path.parent.mkdir(parents=True)
    fields = '{ name = "name", status = "value", value = "Name" }, { name = "description", status = "absent" }, { name = "definition", status = "absent" }, { name = "operational_definition", status = "absent" }'
    if coverage:
        fields += ', { name = "coverage_from", status = "value", value = "2011 och 2013" }, { name = "coverage_to", status = "value", value = "2011 och 2013" }'
    path.write_text(
        f'[register]\nprovider = "{provider}"\nslug = "sample"\nnative_id = "1"\n'
        '[[errata.occurrence_period]]\nvariable = "1.ATC"\nvariant = "PAR_OV"\ncolumn = "ATC"\n'
        + ('edition = "99"\n' if edition else "")
        + f"expected_fields = [{fields}]\n"
        'expected_scope = { kind = "unknown", label = "Original" }\n'
        'expected_period = { kind = "not_applicable" }\n'
        'edition_scope = { kind = "intervals", intervals = [{ start = "2011", end = "2011" }, { start = "2013", end = "2013" }] }\n'
        'edition_period_scope = { kind = "not_applicable" }\n'
        'evidence = "Exact original source bounds"\nnoted = "2026-09-30"\n'
    )
    if error is not None:
        with pytest.raises(RegMetaError) as excinfo:
            load_register_files(tmp_path)
        assert error in excinfo.value.message
    else:
        assert len(load_register_files(tmp_path)[0].errata.occurrence_period) == 1
