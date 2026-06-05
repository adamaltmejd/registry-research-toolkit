"""Generate the 200-binding load-test ``input.json`` fixture.

Writes ``input.json`` + ``expected_ValidationResult.json`` to this
directory — ``input.json`` carries a ``project_data.json``-shaped
payload, but the filename follows the corpus harness contract (see
``reg_schema/test_corpus/README.md``).

A realistic-shape SCB project: LISA + LOUISE + RTB across a handful of
years, ~25 bindings per source, a panel linking the LISA years, and a
populated ``reg_monabundle.binding_options`` block with ``suppress_k``
overrides. Used by two consumers:

- ``reg_monabundle/tests/test_bundle_size_budget.py`` embeds this fixture
  into ``build_bundle(..., project_data=...)`` and asserts the emitted
  ``.py`` stays under ``REFACTOR_SPEC.md`` §12's 1 MB v1 cap. The size
  budget gate fires only on real regressions; today's bundle on this
  fixture is well under cap (the v1 ceiling is forward-looking, not a
  tight bound on current shape). Its ``LOAD_FIXTURE_EXPECTED_COLUMNS``
  constant pins the total binding count emitted here.
- The structural corpus harness in
  ``reg_schema/tests/test_corpus.py`` picks it up automatically (the
  case directory carries ``input.json`` + ``expected_ValidationResult.json``)
  and pins that the fixture stays structurally valid.

Re-run with ``uv run python reg_schema/test_corpus/load_test_200col/build.py``
after editing the binding lists; commit the regenerated JSON.
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent

# 8 source (register_variant, period) coordinates × 25 bindings = 200
# bindings total. Mirrors the shape of a real-MONA project: a few LISA
# yearly extracts, a couple of LOUISE years, two RTB siblings — broadly
# the kit a labour-market researcher would request. Under Model A the
# period lives in its own field, not as a 4th FQID segment (§6.2).
SOURCES: tuple[tuple[str, str, int], ...] = (
    ("lisa_2015", "scb/lisa/individer-15plus", 2015),
    ("lisa_2016", "scb/lisa/individer-15plus", 2016),
    ("lisa_2017", "scb/lisa/individer-15plus", 2017),
    ("lisa_2018", "scb/lisa/individer-15plus", 2018),
    ("louise_2017", "scb/louise/_default", 2017),
    ("louise_2018", "scb/louise/_default", 2018),
    ("rtb_2018", "scb/rtb/_default", 2018),
    ("rtb_2019", "scb/rtb/_default", 2019),
)

# ~25 bindings per source: 1 id, ~5 categorical-with-classification, ~10
# numeric, ~3 date, ~4 categorical-ad-hoc, ~2 opaque. The mix exercises
# every binding-type code path in the validator and the bundle slicer.
# ``value_set`` classification FQIDs are 2-segment, version baked into
# the slug (§5.2): ``class/sun2020``, not the old ``class/sun/2020``.
BINDING_TEMPLATES: tuple[tuple[str, str, dict[str, str | None]], ...] = (
    ("lopnr", "id", {"id_subtype": "integer"}),
    ("kon", "categorical", {"value_set": "class/sun2020"}),
    ("sun2000inr", "categorical", {"value_set": "class/sun2000"}),
    ("ssyk2012", "categorical", {"value_set": "class/ssyk2012"}),
    ("ast_sni2007", "categorical", {"value_set": "class/sni2007"}),
    ("kommun", "categorical", {"value_set": "class/kommun2020"}),
    ("dispink04", "numeric", {"numeric_subtype": "double"}),
    ("dispinkfam04", "numeric", {"numeric_subtype": "double"}),
    ("kontaktink", "numeric", {"numeric_subtype": "double"}),
    ("loneink", "numeric", {"numeric_subtype": "double"}),
    ("foretagarink", "numeric", {"numeric_subtype": "double"}),
    ("transferer", "numeric", {"numeric_subtype": "double"}),
    ("studieskuld", "numeric", {"numeric_subtype": "integer"}),
    ("antalbarn", "numeric", {"numeric_subtype": "integer"}),
    ("antalsysselsatta", "numeric", {"numeric_subtype": "integer"}),
    ("alder", "numeric", {"numeric_subtype": "integer"}),
    ("indatum", "date", {"date_format": "%Y-%m-%d"}),
    ("utdatum", "date", {"date_format": "%Y-%m-%d"}),
    ("fodelsedatum", "date", {"date_format": "%Y%m%d"}),
    ("civilstand", "categorical", {}),
    ("hush_type", "categorical", {}),
    ("foddland", "categorical", {}),
    ("hoglutb", "categorical", {}),
    ("anstforh", "opaque", {}),
    ("yrkesstatus", "opaque", {}),
)
assert len(BINDING_TEMPLATES) == 25, "expected 25 binding templates per source"


def _provider_register(register_variant: str) -> str:
    """The ``<provider>/<register>`` prefix that scopes a source's bindings.

    A binding FQID is ``<provider>/<register>/<slug>`` (3 segments, §6.3);
    its first 2 segments must equal the source ``register_variant`` prefix.
    The variant segment is dropped — it lives once on the Source (§6.2).
    """
    provider, register, _variant = register_variant.split("/")
    return f"{provider}/{register}"


def _display_name(slug: str) -> str:
    """Convert lowercase-hyphen slug to PascalCase-ish display name.

    Mirrors the convention SCB extracts use: column headers come back
    PascalCase (``LopNr_PersonNr``, ``DispInk04``). The runtime never
    inspects display_name semantically — it's just the SQL header — so
    a deterministic upper-camel transform suffices.
    """
    parts = slug.replace("-", "_").split("_")
    return "".join(p.capitalize() for p in parts)


def build() -> dict[str, object]:
    sources_out: list[dict[str, object]] = []
    for source_name, register_variant, period in SOURCES:
        prefix = _provider_register(register_variant)
        bindings: list[dict[str, object]] = []
        for slug, btype, extras in BINDING_TEMPLATES:
            entry: dict[str, object] = {
                "variable": f"{prefix}/{slug}",
                "type": btype,
                "display_name": _display_name(slug),
            }
            for k, v in extras.items():
                if v is not None:
                    entry[k] = v
            bindings.append(entry)
        sources_out.append(
            {
                "name": source_name,
                "register_variant": register_variant,
                "period": period,
                "bindings": bindings,
            }
        )

    # One panel linking the LISA years by person-id. ``entity_key``
    # must match a ``display_name`` on every member source (structural
    # validator §6.4 cross-reference rule), so it uses the value
    # ``_display_name("lopnr")`` produces.
    panel = {
        "panel_id": "lisa_panel",
        "entity_key": _display_name("lopnr"),
        "members": [
            {"source": "lisa_2015", "time_key": 2015},
            {"source": "lisa_2016", "time_key": 2016},
            {"source": "lisa_2017", "time_key": 2017},
            {"source": "lisa_2018", "time_key": 2018},
        ],
    }

    # ~15 ``suppress_k`` overrides — categorical bindings where the
    # researcher wants a tighter k-anonymity floor than the global
    # default. Spread across registers so the block exercises lookup
    # paths. Block keys are 3-segment binding FQIDs (§6.1).
    binding_options: dict[str, dict[str, int]] = {}
    for _source_name, register_variant, _period in SOURCES:
        prefix = _provider_register(register_variant)
        for slug in ("kommun", "ssyk2012", "ast_sni2007"):
            binding_options[f"{prefix}/{slug}"] = {"suppress_k": 25}

    return {
        "schema_version": "2.0.0",
        "steward": "global",
        "reg_meta_version": "reg_meta/v1.0.0",
        "name": "load_test_200col",
        "sources": sources_out,
        "panels": [panel],
        "reg_monabundle": {"binding_options": binding_options},
    }


def main() -> None:
    payload = build()
    n_bindings = sum(len(s["bindings"]) for s in payload["sources"])
    print(f"sources: {len(payload['sources'])}, bindings: {n_bindings}")
    (HERE / "input.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    # The fixture is structurally valid by construction, so the expected
    # ValidationResult is empty. The structural corpus harness verifies
    # this every run and fails fast on drift.
    (HERE / "expected_ValidationResult.json").write_text(
        json.dumps({"issues": []}, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote {HERE}/input.json")
    print(f"wrote {HERE}/expected_ValidationResult.json")


if __name__ == "__main__":
    main()
