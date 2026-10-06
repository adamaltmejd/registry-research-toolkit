"""SOS `[[identity.route]]` and styrtabell lookup tables at the build boundary.

Synthetic SOS workbooks (derived from the `_sos_fixtures` SYN/SYT shapes), one register
per routing shape, are prepared and built once with an authored curation tree. Cases
assert on the report event ledger and the built variable states. Expected placements
follow the documented rule (`curation_compile` SOS routing): a variant-less workbook
places every variable in the synthesized default variant; a routed Deldatamängd token
places its variables in every named native subset; a route whose token or named subset
is absent is stale; a subset is a styrtabell lookup only when its label starts with
"Styrtabell" and its aggregation level is "Ej relevant" (one signal alone is an error),
and the lookup subset and every variable routed only to lookups are source support.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _curation_sos_boundary_support import (
    prepare,
    sos_register_toml,
    subset,
    variable,
    write_workbook,
)
from reg_meta_build.source_naming import authored_naming_id

if TYPE_CHECKING:
    from _curation_sos_boundary_support import Build

LOVA_TOML = Path(__file__).resolve().parents[1] / "curation/registers/sos/lova.toml"
LOOKUP_SOURCE = "Socialstyrelsen/Metadata Uppslag (SYL)_webb.xlsx"
SIGNAL_SOURCE = "Socialstyrelsen/Metadata Signaler (SYS)_webb.xlsx"


def _committed_lova() -> tuple[dict[str, list[str]], dict[str, str]]:
    """The committed LOVA routes by token and its variant slugs by native id."""
    curated = tomllib.loads(LOVA_TOML.read_text(encoding="utf-8"))
    routes = {
        entry["deldatamangd"]: entry["variants"]
        for entry in curated["identity"]["route"]
    }
    return routes, {entry["native_id"]: entry["slug"] for entry in curated["variant"]}


LOVA_ROUTES, LOVA_SLUGS = _committed_lova()
(LOVA_MAIN,) = LOVA_ROUTES["A_LOVA"]
(LOVA_LISA,) = LOVA_ROUTES["A_LOVA_LISA"]


def _lova_slug(row: str) -> str:
    return LOVA_SLUGS[
        authored_naming_id(
            "register_variant", provider="sos", register_key="lova", member_key=row
        )
    ]


def _route(token: str, variants: list[str]) -> str:
    return (
        f'[[identity.route]]\ndeldatamangd = "{token}"\n'
        f"variants = {json.dumps(variants, ensure_ascii=False)}\n"
    )


def _write(source: Path) -> None:
    # SYT shape: no Deldatamängder sheet, the variable still names a token.
    write_workbook(source, "SYD", "Variantlöst", (variable("COL", "TOKEN"),))
    write_workbook(
        source,
        "SYR",
        "Routat",
        (variable("COL", "TOKEN"), variable("HALV", "HALV")),
        (subset("A"), subset("B")),
    )
    write_workbook(
        source,
        "SYS",
        "Signaler",
        (variable("KOD", "A"),),
        (
            subset("A", label="Styrtabell A"),
            subset("B", label="Vy B", aggregation="Ej relevant"),
        ),
    )
    write_workbook(
        source,
        "SYL",
        "Uppslag",
        (variable("COL", "TOKEN"), variable("KOD", "A")),
        (subset("A", label="Styrtabell A", aggregation="Ej relevant"), subset("V")),
    )
    # Two LOVA-named subset rows (the committed route targets, labelled without
    # their "LOVA / " prefix) and one variable under each routed token.
    write_workbook(
        source,
        "LOVA",
        "Syntetisk LOVA",
        (variable("HUVUD", "A_LOVA"), variable("LISA", "A_LOVA_LISA")),
        tuple(
            subset(row, label=row.removeprefix("LOVA / "))
            for row in (LOVA_LISA, LOVA_MAIN)
        ),
    )


@pytest.fixture(scope="module")
def build(tmp_path_factory) -> Build:
    root = tmp_path_factory.mktemp("sos-routes")
    curation = {
        "sos/syd.toml": sos_register_toml(
            "syd", "Variantlöst", variants=("_default",), variables=("COL",)
        ),
        "sos/syr.toml": sos_register_toml(
            "syr",
            "Routat",
            variants=("A", "B"),
            variables=("COL", "HALV"),
            body=_route("TOKEN", ["A", "B"])
            + _route("MISSING", ["A"])
            + _route("HALV", ["A", "SAKNAS"]),
        ),
        "sos/sys.toml": sos_register_toml(
            "sys", "Signaler", variants=("A", "B"), variables=("KOD",)
        ),
        "sos/syl.toml": sos_register_toml(
            "syl",
            "Uppslag",
            variants=("A", "V"),
            variables=("COL", "KOD"),
            body=_route("TOKEN", ["A"]),
        ),
        "sos/lova.toml": sos_register_toml(
            "lova",
            "Syntetisk LOVA",
            variants=tuple((row, _lova_slug(row)) for row in (LOVA_LISA, LOVA_MAIN)),
            variables=("HUVUD", "LISA"),
            body="".join(
                _route(token, LOVA_ROUTES[token]) for token in ("A_LOVA", "A_LOVA_LISA")
            ),
        ),
    }
    return prepare(root, _write).build(root, "build", curation)


def _placements(build: Build, register: str) -> list[tuple[str, str]]:
    return [(name, variant) for name, variant, *_ in build.states(register)]


def test_variantless_workbook_places_token_variables_in_the_default_variant(
    build: Build,
) -> None:
    assert _placements(build, "syd") == [("col", "_default")]
    assert build.case_status()["accepted-sos-routes:syd:COL"] == "applicable"


def test_routed_token_places_its_variable_in_every_named_subset(
    build: Build,
) -> None:
    assert ("col", "a") in _placements(build, "syr")
    assert ("col", "b") in _placements(build, "syr")


def test_route_with_an_absent_token_or_subset_is_stale(build: Build) -> None:
    """MISSING names no delivered token; HALV names one absent subset and still
    places its variable in the delivered one."""
    assert [
        (e["case_id"], e["subject"])
        for e in build.issues("stale_curation_entry")
        if "/syr.toml" in e["case_id"]
    ] == [
        ("curation/registers/sos/syr.toml#/identity.route/2", "MISSING"),
        ("curation/registers/sos/syr.toml#/identity.route/3", "HALV"),
    ]
    assert _placements(build, "syr") == [("col", "a"), ("col", "b"), ("halv", "a")]


def test_committed_lova_routes_choose_distinct_labelled_rows(build: Build) -> None:
    """The committed LOVA routes send A_LOVA and A_LOVA_LISA to two different named
    subset rows, each a declared LOVA variant; the routed variables land there."""
    assert LOVA_MAIN != LOVA_LISA
    assert _placements(build, "lova") == [
        ("huvud", _lova_slug(LOVA_MAIN)),
        ("lisa", _lova_slug(LOVA_LISA)),
    ]
    assert not [
        e for e in build.issues("stale_curation_entry") if "/lova.toml" in e["case_id"]
    ]


def test_styrtabell_lookup_needs_both_label_and_aggregation_signals(
    build: Build,
) -> None:
    assert sorted(
        (e["subject"], e["severity"])
        for e in build.issues("invalid_sos_lookup_signals")
        if e["refs"][0]["source"] == SIGNAL_SOURCE
    ) == [("A", "error"), ("B", "error")]
    assert not [
        e
        for e in build.issues("invalid_sos_lookup_signals")
        if e["refs"][0]["source"] != SIGNAL_SOURCE
    ]


def test_lookup_subset_and_its_routed_variables_are_source_support(
    build: Build,
) -> None:
    """The styrtabell subset row, its own variable and the variable routed to it
    are support records, so the register publishes no variable state."""
    support = [
        disposition
        for e in build.events
        if e["kind"] == "source_occurrence"
        and e["record_id"].startswith(f"{LOOKUP_SOURCE}:")
        for disposition in e["dispositions"]
        if disposition["use"] == "support"
    ]
    assert len(support) == 3
    assert {case for d in support for case in d["cases"]} == {
        f"existing-source-use:{LOOKUP_SOURCE}"
    }
    assert build.states("syl") == []
