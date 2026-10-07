"""Steward delivery-inventory contract (reg_meta/DESIGN.md → Inventory TOML
authoring contract, Holdings resolution invariants).

The fixture below is a synthetic inventory — no real steward holdings are
committed here — covering the four shapes the contract calls out: a mapped column, an
unresolved (zero-mapping) column, one column mapped to two register variants
(the combined Utrikeshandel shape), and a table whose edition is a finite
multi-period list. The rejection cases pin the fail-fast guards, including
the one-to-one cell→column resolution invariant (last section).
"""

from __future__ import annotations

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.inventory import load_inventory

FIXTURE_INVENTORY = """
version = 1
steward = "swecov"

# A CSV delivery: the identifier is the exact delivered filename; the edition is
# curated explicitly even though this filename happens to carry its year.
[[table]]
id = "LISA_Individ_2019.csv"
edition = 2019

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

[[table.column]]
name = "DispInk04"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/disponibel-inkomst"
representation = "DispInk04"

# Delivered but not yet mapped: stays in the coverage denominator.
[[table.column]]
name = "LopNr"

# One SQL table serving two register variants (the Utrikeshandel shape).
[[table]]
id = "dbo.Utrikeshandel"
edition = { from = 2015, to = 2020 }

[[table.column]]
name = "Varukod"
[[table.column.mapping]]
register_variant = "scb/utrikeshandel/import"
variable = "scb/utrikeshandel/varukod"
representation = "Varukod"

[[table.column.mapping]]
register_variant = "scb/utrikeshandel/export"
variable = "scb/utrikeshandel/varukod"

# An interrupted series: a finite list of edition segments.
representation = "Varukod"

[[table]]
id = "SCB_Foretag_2005-2010_2015.csv"
edition = [{ from = 2005, to = 2010 }, "2015"]

[[table.column]]
name = "PeOrgNr"
"""


def _write(tmp_path, text: str):
    path = tmp_path / "inventory.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize("reason", ["", "   ", "\\t"])
def test_unmapped_reason_refuses_blank_text(tmp_path, reason: str) -> None:
    text = FIXTURE_INVENTORY.replace(
        'name = "LopNr"', f'name = "LopNr"\nunmapped_reason = "{reason}"'
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "unmapped_reason must be nonblank" in excinfo.value.message


def test_unmapped_reason_refuses_active_mappings(tmp_path) -> None:
    text = FIXTURE_INVENTORY.replace(
        'name = "Kon"', 'name = "Kon"\nunmapped_reason = "Unresolved owner"'
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "unmapped_reason cannot accompany mappings" in excinfo.value.message


def test_school_year_conflict_names_the_period_as_its_token(tmp_path) -> None:
    """The located conflict line renders the shared school-year period back as
    its own token, not as its ISO bounds."""
    mapping = """
[[table.column]]
name = "Betyg"
[[table.column.mapping]]
register_variant = "scb/grundskola/elever"
variable = "scb/grundskola/betyg"
representation = "Betyg"
"""
    text = (
        'version = 1\nsteward = "swecov"\n'
        '\n[[table]]\nid = "Grundskola_a.csv"\nedition = "LA2004"\n'
        + mapping
        + '\n[[table]]\nid = "Grundskola_b.csv"\nedition = "LA2004"\n'
        + mapping
    )
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert (
        "both map scb/grundskola/elever scb/grundskola/betyg over LA2004"
        in excinfo.value.message
    )


@pytest.mark.parametrize(
    ("edition", "message"),
    [
        ('edition = "_default"', "requires year-independent period_scope"),
        ('edition = "all"', "not a period token"),
        ("edition = 2019.5", "Input should be"),
        ("edition = { from = 2020, to = 2015 }", "'from' is after 'to'"),
        ("edition = []", "must not be empty"),
        ('edition = ["2015", "2010"]', "sorted ascending and non-overlapping"),
        (
            'edition = [{ from = 2005, to = 2012 }, "2010"]',
            "sorted ascending and non-overlapping",
        ),
    ],
)
def test_rejects_non_finite_or_malformed_editions(
    tmp_path, edition: str, message: str
) -> None:
    text = f"""
version = 1
steward = "swecov"

[[table]]
id = "SoS_Patientregister.csv"
{edition}

[[table.column]]
name = "Diagnos"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert excinfo.value.exit_code == EXIT_CONFIG
    # The error names the offending table, not an array index.
    location = "table['SoS_Patientregister.csv']"
    if edition != 'edition = "_default"':
        location += ".edition"
    assert location in excinfo.value.message
    assert message in excinfo.value.message


def test_rejects_table_without_an_edition(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "Holdings_Extract.csv"

[[table.column]]
name = "Diagnos"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert "table['Holdings_Extract.csv'].edition: Field required" in (
        excinfo.value.message
    )


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        ('variable = "scb/lisa"', "3-segment binding FQID"),
        ('variable = "scb/fek/kon"', "does not belong to register_variant"),
        ('variable = "scb/lisa/kon"\nvariabel = "typo"', "Extra inputs"),
    ],
)
def test_rejects_malformed_mappings(tmp_path, mapping: str, message: str) -> None:
    text = f"""
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2019.csv"
edition = 2019

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
{mapping}
representation = "Kon"

"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    # The error names the offending table AND column.
    assert "table['LISA_Individ_2019.csv'].column['Kon'].mapping[0]" in (
        excinfo.value.message
    )
    assert message in excinfo.value.message


def test_rejects_duplicate_table_identifier(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
[[table.column]]
name = "Kon"

[[table]]
id = "LISA_Individ.csv"
edition = 2020
[[table.column]]
name = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "duplicate table 'LISA_Individ.csv'" in excinfo.value.message


def test_rejects_duplicate_physical_column(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
[[table.column]]
name = "Kon"
[[table.column]]
name = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "duplicate physical column 'Kon'" in excinfo.value.message


def test_rejects_an_inventory_with_no_tables(tmp_path) -> None:
    """The inventory is the authoritative holdings statement: an empty one is a
    curation error, never a steward that delivers nothing."""
    text = """
version = 1
steward = "swecov"
table = []
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert excinfo.value.exit_code == EXIT_CONFIG
    assert "table: Value error, inventory declares no tables" in excinfo.value.message


def test_rejects_a_table_with_no_columns(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
column = []
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    # The error names the offending table, not an array index.
    assert (
        "table['LISA_Individ.csv'].column: Value error, table declares no columns"
        in (excinfo.value.message)
    )


def test_rejects_unknown_contract_version(tmp_path) -> None:
    text = """
version = 2
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
[[table.column]]
name = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "version:" in excinfo.value.message


def test_rejects_unreadable_toml(tmp_path) -> None:
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, "version = 1\nsteward = swecov\n"))
    assert excinfo.value.code == "inventory_toml_unreadable"
    assert excinfo.value.exit_code == EXIT_CONFIG


def test_rejects_a_non_utf8_inventory(tmp_path) -> None:
    """TOML is UTF-8 by definition; a mis-encoded file is unreadable input on
    the documented path, not an uncaught `UnicodeDecodeError`."""
    path = tmp_path / "inventory.toml"
    path.write_bytes(b'version = 1\nsteward = "swecov"\n# \xff\xfe not utf-8\n')
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(path)
    assert excinfo.value.code == "inventory_toml_unreadable"
    assert excinfo.value.exit_code == EXIT_CONFIG


# ── one-to-one cell→column resolution ──────────────────────────────────────
#
# Every admitted `(register_variant, variable, representation, period)` cell
# must resolve to exactly ONE physical `(table, column)`: the extraction tool
# never chooses between sources, so two mappings that could each serve one cell
# are a supersession left uncurated, not a second holding.

# The motivating shape: two cumulative `FHM_NVR_Covid*` snapshots mapping the
# same coordinate over overlapping editions. Ordering both would emit the same
# observations twice, from two layouts.
CONFLICTING_SNAPSHOTS = """
version = 1
steward = "swecov"

[[table]]
id = "FHM_NVR_Covid_2021-03-15.csv"
edition = { from = 2020, to = 2021 }
[[table.column]]
name = "Vaccinationsdatum"
[[table.column.mapping]]
register_variant = "fohm/nvr/_default"
variable = "fohm/nvr/vaccinationsdatum"
representation = "Vaccinationsdatum"

[[table]]
id = "FHM_NVR_Covid_2021-06-30.csv"
edition = { from = 2021, to = 2022 }
[[table.column]]
name = "Vaccinationsdatum"
[[table.column.mapping]]
register_variant = "fohm/nvr/_default"
variable = "fohm/nvr/vaccinationsdatum"
representation = "Vaccinationsdatum"
"""


def test_rejects_two_tables_serving_one_cell(tmp_path) -> None:
    """The supersession worklist line: BOTH physical locations, the coordinate,
    and the overlapping period, so the curator knows which two tables to choose
    between."""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, CONFLICTING_SNAPSHOTS))
    message = excinfo.value.message
    assert excinfo.value.code == "inventory_invalid"
    assert excinfo.value.exit_code == EXIT_CONFIG
    assert (
        "table['FHM_NVR_Covid_2021-03-15.csv'].column['Vaccinationsdatum'] "
        "(representation 'Vaccinationsdatum') and "
        "table['FHM_NVR_Covid_2021-06-30.csv'].column['Vaccinationsdatum'] "
        "(representation 'Vaccinationsdatum') both map fohm/nvr/_default "
        "fohm/nvr/vaccinationsdatum over 2021" in message
    )
    # The curation rules forbid an auto-picked survivor: the maintainer curates.
    assert "filename date is not proof of supersession" in message


def test_rejects_two_columns_of_one_table_serving_one_cell(tmp_path) -> None:
    """A table's single edition always overlaps itself, so the same triple in
    two of its columns is the same conflict — the invariant's across-columns arm."""
    text = """
version = 1
steward = "swecov"

[[table]]
id = "dbo.LISA_Individ"
edition = [{ from = 2005, to = 2010 }, "2015"]
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"
[[table.column]]
name = "Kon_recode"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert (
        "table['dbo.LISA_Individ'].column['Kon'] (representation 'Kon') and "
        "table['dbo.LISA_Individ'].column['Kon_recode'] (representation 'Kon') "
        "both map scb/lisa/individer-15plus scb/lisa/kon over 2005..2010,2015"
        in excinfo.value.message
    )


def test_rejects_a_duplicate_mapping_within_one_column(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2019.csv"
edition = 2019
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    # Located at the offending column, not an array index.
    assert (
        "table['LISA_Individ_2019.csv'].column['Kon']: Value error, duplicate "
        "mapping scb/lisa/individer-15plus scb/lisa/kon "
        "(representation 'Kon')" in excinfo.value.message
    )


def test_accepts_two_representations_of_one_variable_over_one_period(tmp_path) -> None:
    """Two DIFFERENT explicit representations are two cells, not one: parallel
    representations (SSYK 3- and 4-digit) delivered for the same period are
    legal, and choosing between them is the binding's pin, not the inventory's
    period arithmetic."""
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Yrke3_2019-2020.csv"
edition = { from = 2019, to = 2020 }
[[table.column]]
name = "Ssyk3"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
representation = "Ssyk3"

[[table]]
id = "LISA_Yrke4_2019-2020.csv"
edition = { from = 2019, to = 2020 }
[[table.column]]
name = "Ssyk4"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
representation = "Ssyk4"
"""
    inventory = load_inventory(_write(tmp_path, text))
    assert len(inventory.tables) == 2


# ── disjoint-partition arm ─────────────────────────────────────────────────
#
# Some registers arrive as several tables partitioned by SUB-POPULATION within
# one edition (the `Arb_`/`Soc_AGIIndivid` reporter streams below), deliberately
# unified as ONE user-facing variant. The one-to-one invariant then holds per
# `(cell × partition)`: distinct labels are shards, everything else is a
# conflict. Labels are explicit curated facts, never inferred, so a true
# re-delivery cannot hide behind them.


def _agi_shards(first: str, second: str) -> str:
    """The two AGI reporter streams mapping one cell over one edition, each
    `[[table]]` carrying the given `partition` line (`""` for none)."""
    return f"""
version = 1
steward = "swecov"

[[table]]
id = "Arb_AGIIndivid_2021-03.csv"
edition = "2021-03"
{first}
[[table.column]]
name = "Belopp"
[[table.column.mapping]]
register_variant = "skv/agi/individuppgifter-agi"
variable = "skv/agi/utbetalt-belopp"
representation = "Belopp"

[[table]]
id = "Soc_AGIIndivid_2021-03.csv"
edition = "2021-03"
{second}
[[table.column]]
name = "Belopp"
[[table.column.mapping]]
register_variant = "skv/agi/individuppgifter-agi"
variable = "skv/agi/utbetalt-belopp"
representation = "Belopp"

"""


def test_rejects_two_tables_sharing_one_partition_label(tmp_path) -> None:
    """The same shard delivered twice is the ordinary supersession conflict: a
    partition label separates cells only when the two labels DIFFER."""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(
            _write(tmp_path, _agi_shards('partition = "arb"', 'partition = "arb"'))
        )
    message = excinfo.value.message
    assert (
        "table['Arb_AGIIndivid_2021-03.csv'].column['Belopp'] (representation 'Belopp') "
        "and table['Soc_AGIIndivid_2021-03.csv'].column['Belopp'] "
        "(representation 'Belopp') both map skv/agi/individuppgifter-agi "
        "skv/agi/utbetalt-belopp over 2021-03" in message
    )
    # Same shard twice is a supersession decision, not a labelling slip, so the
    # line carries the standing supersession remediation and no label hint.
    assert "carries a `partition` label" not in message
    assert "filename date is not proof of supersession" in message


def test_rejects_a_partition_label_opposite_an_unlabelled_table(tmp_path) -> None:
    """An unlabelled table claims the WHOLE population of its edition, so it
    necessarily overlaps a shard of it. The invariant requires the label on both sides,
    and the conflict line says so — one side labelled is the diagnostic case
    where a curated split was left half-stated."""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, _agi_shards('partition = "arb"', "")))
    message = excinfo.value.message
    assert "both map skv/agi/individuppgifter-agi skv/agi/utbetalt-belopp" in message
    assert (
        "only one of these carries a `partition` label; label both when they "
        "are genuinely disjoint shards of one sub-population split" in message
    )


def test_rejects_a_non_slug_partition_label(tmp_path) -> None:
    """A label becomes a filename token, so it rides the shared slug grammar —
    and the error names the `partition` slot the author has to fix."""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(
            _write(
                tmp_path, _agi_shards('partition = "Arb Stream"', 'partition = "soc"')
            )
        )
    assert (
        "table['Arb_AGIIndivid_2021-03.csv'].partition: Value error, invalid "
        "slug in partition: 'Arb Stream'" in excinfo.value.message
    )
