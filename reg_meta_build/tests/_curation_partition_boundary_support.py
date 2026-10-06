"""Build one synthetic SCB register with curated identity, errata and enrichment.

The fixture is synthetic: Registerinformation rows come from ``_csv_fixtures.var_row``
for register ``TEST`` (register id 1, variant id 2, plus variant 3 where a case needs a
second variant) and native variable 5, the same geometry the compiler-level tests in
``test_curation_compile.py`` built as in-memory records. Each case writes its own
curation TOML and runs the real prepare → ``build_catalog`` pipeline, so the readouts
below are built-artifact rows and the build report's issue ledger.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

from _csv_fixtures import var_row, write_input_bundle, write_scb_input
from _pipeline_catalog_support import CatalogFixture, report_issues
from _prepared_fixtures import accept_prepared
from reg_meta_build.prepared_catalog import prepare_catalog_sources

if TYPE_CHECKING:
    from pathlib import Path

REGISTER = (
    '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
    '[[variant]]\nnative_id = "1.2"\nslug = "people"\n'
)
SECOND_VARIANT = '[[variant]]\nnative_id = "1.3"\nslug = "others"\n'
DELIVERED = (
    '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "A"\n'
    'versions = ["2021"]\nevidence = "accepted delivery"\nnoted = "2026-09-25"\n'
)
DESCRIPTION = (
    '\n[[enrichment.description]]\nregister = "scb/sample"\n'
    'variable = "a"\ndescription = "Accepted prose"\n'
    'provenance = "delivery list"\n'
)
ALIAS = (
    '\n[[enrichment.alias]]\nregister = "scb/sample"\n'
    'variable = "a"\ndelivery_column = "FormerA"\n'
    'provenance = "delivery list"\n'
)
# The native family every case partitions or reassigns: register 1, variable 5.
NATIVE = (
    "('scb-registerinformation', 'scb', 'register', 'native-int', 1, "
    "'variable', 'native-int', 5)"
)


def partition(
    columns: dict[str, str], *, variable: str = "1.5", ref: str = "fixture"
) -> str:
    """Name each split owner and declare the literal column map of ``variable``."""
    names = "".join(
        f'[[variable]]\nnative_id = "{owner}"\nslug = "{owner.rsplit(".", 1)[1]}"\n'
        for owner in sorted(set(columns.values()))
    )
    mapping = ", ".join(f'{column} = "{owner}"' for column, owner in columns.items())
    return (
        names + f'[[identity.partition]]\nvariable = "{variable}"\n'
        f'columns = {{ {mapping} }}\ncolumns_ref = "{ref}"\n'
    )


def row(
    column: str,
    cvid: int,
    *,
    year: str = "2020",
    variable: int = 5,
    variant: int = 2,
    register: tuple[str, int] = ("TEST", 1),
    **fields: str,
) -> str:
    """One Registerinformation row; ``column='""'`` delivers a literal blank cell."""
    return var_row(
        colname=column,
        cvid=cvid,
        var_id=variable,
        year=year,
        regver_id=int(year),
        register=(*register, variant),
        **fields,
    )


def summary(column: str, *, year: str = "2020", register: str = "TEST") -> str:
    """The Unika summary row that supports one named delivery column."""
    return (
        f"{register}|Testregistret|Individer|Individer|GenericVar|{column}|"
        f"{year}|{year}|0|0|0"
    )


@dataclass(frozen=True)
class Built:
    counts: dict
    issues: list[dict]
    db: Path

    def codes(self) -> list[tuple[str, str]]:
        """Each reported issue as ``(code, subject)``, sorted."""
        return sorted((issue["code"], str(issue["subject"])) for issue in self.issues)

    def details(self, code: str) -> list[str]:
        return sorted(
            str(issue["detail"]) for issue in self.issues if issue["code"] == code
        )

    def states(self) -> list[tuple[str, str, str, str]]:
        """``(variable, variant, valid_from, delivery column)`` of every built state."""
        return self._rows(
            "SELECT v.slug, rv.slug, s.valid_from, s.delivery_column_name "
            "FROM variable_state s JOIN variable v USING (variable_id) "
            "JOIN register_variant rv USING (register_variant_id)"
        )

    def provenance(self, variable: str, valid_from: str) -> str | None:
        rows = self._rows(
            "SELECT s.provenance FROM variable_state s JOIN variable v "
            "USING (variable_id) WHERE v.slug = ? AND s.valid_from = ?",
            (variable, valid_from),
        )
        assert len(rows) == 1, rows
        return rows[0][0]

    def variables(self) -> list[tuple[str, str, str | None]]:
        """``(slug, provider_key, description)`` of every built variable."""
        return self._rows("SELECT slug, provider_key, description FROM variable")

    def aliases(self) -> list[tuple[str, str, str]]:
        """``(variable, variant, delivery column)`` of every shipped alias row."""
        return self._rows(
            "SELECT v.slug, rv.slug, a.delivery_column_name "
            "FROM variable_alias a JOIN variable v USING (variable_id) "
            "JOIN register_variant rv USING (register_variant_id)"
        )

    def same_as(self) -> list[tuple[str, str]]:
        return self._rows(
            "SELECT a_register || '/' || a_variable, b_register || '/' || b_variable "
            "FROM variable_same_as"
        )

    def _rows(self, sql: str, params: tuple = ()) -> list:
        with sqlite3.connect(self.db) as conn:
            return sorted(conn.execute(sql, params).fetchall())


def build(
    tmp_path: Path,
    rows: list[str],
    summaries: list[str],
    curation: dict[str, str],
    *,
    registers: tuple[str, ...] | None = None,
) -> Built:
    """Prepare the synthetic SCB input, write ``curation`` (path → TOML text), build.

    The build runs with ``diagnostic=True`` so curation errors land in the report
    ledger instead of aborting; ``registers`` selects a register slice.
    """
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=rows,
        unika_rows=summaries,
        include=("registerinformation", "unika"),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    root = tmp_path / "curation"
    (root / "classifications").mkdir(parents=True)
    for relative, text in curation.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    fixture = CatalogFixture(prepared, commit, manifest.sha256, root)
    output, report = tmp_path / "catalog.db", tmp_path / "report"
    kwargs = {} if registers is None else {"registers": registers}
    result = fixture.build(output, report, diagnostic=True, **kwargs)
    return Built(result["counts"], report_issues(report), output)


def build_sample(
    tmp_path: Path, rows: list[str], toml: str, *, summaries: list[str] | None = None
) -> Built:
    """Build register ``scb/sample`` from ``rows`` with ``toml`` as its curation file."""
    return build(
        tmp_path,
        rows,
        summaries if summaries is not None else [],
        {"registers/scb/sample.toml": toml},
    )


def build_referenced(
    tmp_path: Path,
    rows: list[str],
    summaries: list[str],
    curation: dict[str, str],
    target: str,
    *,
    slice_anchor: bool = False,
) -> Built:
    """Add register ``scb/anchor`` (id 9) whose `value` is same-as ``scb/sample/<target>``.

    With ``slice_anchor`` the build selects register 9 only, so ``scb/sample`` is out
    of the slice and the edge is classified through the deferred naming of the
    unselected register: known names defer, unknown names fail the build.
    """
    return build(
        tmp_path,
        [*rows, row("VALUE", 9001, variable=901, variant=90, register=("ANCHOR", 9))],
        [*summaries, summary("VALUE", register="ANCHOR")],
        {
            **curation,
            "registers/scb/anchor.toml": (
                '[register]\nprovider = "scb"\nslug = "anchor"\nnative_id = "9"\n'
                '[[variant]]\nnative_id = "9.90"\nslug = "people"\n'
                '[[variable]]\nnative_id = "9.901"\nslug = "value"\n'
            ),
            "relations.toml": (
                '[[edge]]\ntype = "same_as"\na = "scb/anchor/value"\n'
                f'b = "scb/sample/{target}"\n'
            ),
        },
        registers=("9",) if slice_anchor else None,
    )
