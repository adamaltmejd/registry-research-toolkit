"""Generate the 200-column load-test ``project_data.json`` fixture.

A realistic-shape SCB project: LISA + LOUISE + RTB across a handful of
years, ~25 columns per source, a panel linking the LISA years, and a
populated ``reg_monabundle.column_options`` block with ``suppress_k``
overrides. Used by two consumers:

- ``reg_monabundle/tests/test_bundle_size_budget.py`` embeds this fixture
  into ``build_bundle(..., project_data=...)`` and asserts the emitted
  ``.py`` stays under ``REFACTOR_SPEC.md`` §12's 1 MB v1 cap. The size
  budget gate fires only on real regressions; today's bundle on this
  fixture is well under cap (the v1 ceiling is forward-looking, not a
  tight bound on current shape).
- The structural corpus harness in
  ``reg_schema/tests/test_corpus.py`` picks it up automatically (the
  case directory carries ``input.json`` + ``expected_ValidationResult.json``)
  and pins that the fixture stays structurally valid.

Re-run with ``uv run python reg_schema/test_corpus/load_test_200col/build.py``
after editing the column lists; commit the regenerated JSON.
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent

# 8 source register-versions × 25 columns = 200 columns total. Mirrors
# the shape of a real-MONA project: a few LISA yearly extracts, a couple
# of LOUISE years, two RTB siblings — broadly the kit a labour-market
# researcher would request.
SOURCES: tuple[tuple[str, str], ...] = (
    ("lisa_2015", "scb/lisa/individer-15plus/2015"),
    ("lisa_2016", "scb/lisa/individer-15plus/2016"),
    ("lisa_2017", "scb/lisa/individer-15plus/2017"),
    ("lisa_2018", "scb/lisa/individer-15plus/2018"),
    ("louise_2017", "scb/louise/_default/2017"),
    ("louise_2018", "scb/louise/_default/2018"),
    ("rtb_2018", "scb/rtb/_default/2018"),
    ("rtb_2019", "scb/rtb/_default/2019"),
)

# ~25 cols per source: 1 id, ~5 categorical-with-classification, ~10
# numeric, ~3 date, ~4 categorical-ad-hoc, ~2 opaque. The mix exercises
# every column-type code path in the validator and the bundle slicer.
COL_TEMPLATES: tuple[tuple[str, str, dict[str, str | None]], ...] = (
    ("lopnr", "id", {"id_subtype": "integer"}),
    ("kon", "categorical", {"value_set": "class/sun/2020"}),
    ("sun2000inr", "categorical", {"value_set": "class/sun/2000"}),
    ("ssyk2012", "categorical", {"value_set": "class/ssyk/2012"}),
    ("ast_sni2007", "categorical", {"value_set": "class/sni/2007"}),
    ("kommun", "categorical", {"value_set": "class/kommun/2020"}),
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
assert len(COL_TEMPLATES) == 25, "expected 25 column templates per source"


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
    for source_name, rv in SOURCES:
        cols: list[dict[str, object]] = []
        for slug, ctype, extras in COL_TEMPLATES:
            entry: dict[str, object] = {
                "name": f"{rv}/{slug}",
                "type": ctype,
                "display_name": _display_name(slug),
            }
            for k, v in extras.items():
                if v is not None:
                    entry[k] = v
            cols.append(entry)
        sources_out.append(
            {"name": source_name, "register_version": rv, "columns": cols}
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

    # ~15 ``suppress_k`` overrides — categorical FQIDs where the
    # researcher wants a tighter k-anonymity floor than the global
    # default. Spread across registers so the block exercises lookup
    # paths.
    column_options: dict[str, dict[str, int]] = {}
    for source_name, rv in SOURCES:
        for slug in ("kommun", "ssyk2012", "ast_sni2007"):
            column_options[f"{rv}/{slug}"] = {"suppress_k": 25}

    return {
        "schema_version": "1.0.0",
        "steward": "global",
        "reg_meta_version": "reg_meta/v0.12.0",
        "name": "load_test_200col",
        "sources": sources_out,
        "panels": [panel],
        "reg_monabundle": {"column_options": column_options},
    }


def main() -> None:
    payload = build()
    n_cols = sum(len(s["columns"]) for s in payload["sources"])
    print(f"sources: {len(payload['sources'])}, columns: {n_cols}")
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
