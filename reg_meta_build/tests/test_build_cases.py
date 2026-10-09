"""The `cases/build/` corpus: readable sources and curation through the real build.

Each case directory is one boundary claim (`cases/build/README.md`): its request
names the source set and build options, its curation tree is what a curator would
commit, and its `expected.json` is the oracle, read from the replaced test it
names. A case fails when the build's result status, refusal, report ledger or
built artifact stops matching that oracle.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _build_case_runner import case_dirs, case_steps, run_step
from reg_meta_build.source_naming import authored_naming_id

if TYPE_CHECKING:
    from _build_case_runner import PreparedCache

LOVA_TOML = Path(__file__).resolve().parents[1] / "curation/registers/sos/lova.toml"


@pytest.mark.parametrize("case", case_dirs(), ids=lambda case: case.name)
def test_build_case(case: Path, prepared_cache: PreparedCache, tmp_path: Path) -> None:
    earlier: dict = {}
    for number, step in enumerate(case_steps(case)):
        actual, expected = run_step(
            step, prepared_cache, tmp_path / str(number), earlier
        )
        assert actual == expected, step.relative_to(case.parent)


def test_committed_lova_routes_choose_distinct_labelled_rows(
    prepared_cache: PreparedCache, tmp_path: Path
) -> None:
    """The committed LOVA routes send A_LOVA and A_LOVA_LISA to two different named
    subset rows, each a declared LOVA variant, and the routed variables land there.

    The case is written from the committed `sos/lova.toml`, so it cannot be a static
    case directory. Fails if a committed route edit sends both tokens to one row or
    names a row the register does not declare.
    """
    curated = tomllib.loads(LOVA_TOML.read_text(encoding="utf-8"))
    routes = {
        entry["deldatamangd"]: entry["variants"]
        for entry in curated["identity"]["route"]
    }
    slugs = {entry["native_id"]: entry["slug"] for entry in curated["variant"]}
    ((main,), (lisa,)) = routes["A_LOVA"], routes["A_LOVA_LISA"]
    assert main != lisa

    def native(kind: str, member: str | None = None) -> str:
        return authored_naming_id(
            kind, provider="sos", register_key="lova", member_key=member
        )

    def slug(row: str) -> str:
        return slugs[native("register_variant", row)]

    step = tmp_path / "case"
    register = step / "curation" / "registers"
    (register / "sos").mkdir(parents=True)
    (register / "scb").mkdir()
    (register / "scb" / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    (register / "sos" / "lova.toml").write_text(
        '[register]\nprovider = "sos"\nslug = "lova"\nname = "Syntetisk LOVA"\n'
        f'native_id = "{native("register")}"\n'
        + "".join(
            f'[[variant]]\nslug = "{slug(row)}"\n'
            f'native_id = "{native("register_variant", row)}"\n'
            for row in (lisa, main)
        )
        + "".join(
            f'[[variable]]\nslug = "{name.lower()}"\n'
            f'native_id = "{native("variable", name)}"\n'
            for name in ("HUVUD", "LISA")
        )
        + "".join(
            f'[[identity.route]]\ndeldatamangd = "{token}"\n'
            f"variants = {json.dumps(routes[token], ensure_ascii=False)}\n"
            for token in ("A_LOVA", "A_LOVA_LISA")
        ),
        encoding="utf-8",
    )
    window = {"data_from": 2005, "data_to": 2015}
    request = {
        "fails_if": "a committed sos/lova.toml route edit sends A_LOVA and "
        "A_LOVA_LISA to one subset row, or names a row the register does not declare",
        "sources": {
            "description": "Two LOVA subset rows named by the committed routes "
            "(labelled without their 'LOVA / ' prefix), one variable under each "
            "routed token, plus the SCB sample VALUE column.",
            "scb": {
                "registerinformation": [
                    {"cvid": 1001, "var_id": 101, "colname": "VALUE"}
                ]
            },
            "sos": [
                {
                    "abbrev": "LOVA",
                    "title": "Syntetisk LOVA",
                    "subsets": [
                        {"name": row, "label": row.removeprefix("LOVA / "), **window}
                        for row in (lisa, main)
                    ],
                    "variables": [
                        {
                            "name": name,
                            "deldatamangd": token,
                            "label": name.title(),
                            **window,
                        }
                        for name, token in (
                            ("HUVUD", "A_LOVA"),
                            ("LISA", "A_LOVA_LISA"),
                        )
                    ],
                }
            ],
        },
    }
    expected = {
        "projections": [
            {
                "table": "states",
                "where": {"register": "lova"},
                "fields": ["variable", "variant"],
                "rows": [["huvud", slug(main)], ["lisa", slug(lisa)]],
            },
            {
                "table": "issues",
                "where": {
                    "code": "stale_curation_entry",
                    "case_id": {"contains": "/lova.toml"},
                },
                "fields": ["case_id"],
                "rows": [],
            },
        ]
    }
    (step / "request.json").write_text(json.dumps(request), encoding="utf-8")
    (step / "expected.json").write_text(json.dumps(expected), encoding="utf-8")
    actual, expected = run_step(step, prepared_cache, tmp_path / "run")
    assert actual == expected
