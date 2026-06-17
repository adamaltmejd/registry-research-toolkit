"""The REAL repo curation TOMLs must always parse.

Synthetic builds run with EMPTY curation maps (`_no_repo_curation`, the
session-scoped autouse fixture in `_shared_fixtures.py`) because the repo TOMLs
are keyed on real SCB source ids that collide with fixture register ids. That
fixture removed the incidental coverage the fixture build used to provide: a
malformed entry in the maintainer-edited TOMLs would otherwise surface only on
a real-data `build-db`. This test loads the actual files by DIRECT path — the
autouse fixture only nulls the `repo_*_path` helpers, not the loaders.

Scope: load-time validation only (TOML shape, canonical ints, folded-column
group rules). The build-time half (named columns exist for the var) needs the
real corpus and stays maintainer-build-only by design.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.codelivery import load_codelivery
from reg_meta_build.column_merges import load_column_merges
from reg_meta_build.concept_groups import load_concept_groups
from reg_meta_build.delivery_enrichment import load_delivery_enrichment
from reg_meta_build.doc_db import _require_doc_source_str, load_doc_sources
from reg_meta_build.family_merges import load_family_merges
from reg_meta_build.fold_overrides import load_fold_overrides
from reg_meta_build.variable_grafts import load_variable_grafts
from reg_meta_build.variable_related_to import load_related_to
from reg_meta_build.variable_same_as import load_same_as

# reg_meta_build/ package root (tests/ sits beside the TOMLs).
_ROOT = Path(__file__).resolve().parent.parent


def test_repo_codelivery_parses() -> None:
    assert load_codelivery(_ROOT / "codelivery.toml")


def test_repo_fold_overrides_parses() -> None:
    assert load_fold_overrides(_ROOT / "fold_overrides.toml")


def test_repo_column_merges_parses() -> None:
    assert load_column_merges(_ROOT / "column_merges.toml")


def test_repo_concept_groups_parses() -> None:
    groups = load_concept_groups(_ROOT / "concept_groups.toml")
    assert groups  # the LISA agi rank family ships with the repo
    # Build-time resolution (register/group/variable exist) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(len(g.members) >= 2 for g in groups)


def test_repo_delivery_enrichment_parses() -> None:
    enr = load_delivery_enrichment(_ROOT / "delivery_enrichment.toml")
    # the #365 global description backfills + delivery-column aliases ship together
    assert enr.descriptions
    assert enr.aliases
    # Slug RESOLUTION is lenient + maintainer-build territory; load-time shape
    # (2-segment FQID, unique keys) is this gate.
    assert all(d.provider and d.register and d.variable for d in enr.descriptions)
    assert all(a.provider and a.register and a.delivery_column for a in enr.aliases)


def test_repo_family_merges_parses() -> None:
    families = load_family_merges(_ROOT / "family_merges.toml")
    assert families  # the #319 LISA monthly families ship with the repo
    # Member RESOLUTION (12 month columns exist for the stem) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(
        f.provider and f.register and f.family_stem and f.label for f in families
    )


def test_repo_variable_related_to_parses() -> None:
    # The first curated "see also" edges landed (#403) — the regression lock
    # flips from an EMPTY assertion to truthiness + shape, like the other repo
    # curation gates above. Endpoint RESOLUTION (both FQIDs exist) is
    # maintainer-build territory (the materializer fails fast on a dangling FQID).
    edges = load_related_to(_ROOT / "variable_related_to.toml")
    assert edges
    assert all(
        e.a_provider
        and e.a_register
        and e.a_variable
        and e.b_provider
        and e.b_register
        and e.b_variable
        and e.relation_kind
        for e in edges
    )


def test_repo_variable_same_as_parses() -> None:
    # `variable_same_as.toml` (#417) ships EMPTY (resolver-load-bearing identity;
    # only confirmed edges ever land). The gate is that the header-only file
    # PARSES cleanly — a malformed entry would otherwise surface only on a real
    # build. Empty is correct, so assert it parses to the empty tuple (no error).
    assert load_same_as(_ROOT / "variable_same_as.toml") == ()


def test_repo_variable_grafts_parses() -> None:
    grafts = load_variable_grafts(_ROOT / "variable_grafts.toml")
    assert grafts  # the #365 SWECOV grafts ship with the repo
    # Load-time shape (2-segment FQID, non-empty variant/column/description,
    # unique triple); variant/column RESOLUTION is maintainer-build territory.
    assert all(g.provider and g.register and g.variant and g.column for g in grafts)


def test_repo_doc_sources_parses() -> None:
    # `doc_sources.toml` (#372) maps a doc `source` slug → public SCB PDF; a
    # missing `url`/`title` key would otherwise surface only at doc-DB build
    # time. `load_doc_sources` resolves its own path relative to __file__, so
    # the in-repo file is what's exercised here. URL RESOLUTION (the PDFs 200)
    # is out of scope — load-time shape is this gate.
    sources = load_doc_sources()
    assert sources  # the #372 LISA source map ships with the repo
    assert all(entry["url"] and entry["title"] for entry in sources.values())


def test_doc_sources_malformed_entry_raises_curation_error() -> None:
    # A missing/empty/wrong-type `url`/`title` is an actionable config error
    # (EXIT_CONFIG), not a bare KeyError. `load_doc_sources` resolves the repo
    # file by __file__, so exercise its per-entry validator directly.
    for bad in ({"title": "T"}, {"url": "", "title": "T"}, {"url": 1, "title": "T"}):
        with pytest.raises(RegMetaError) as exc_info:
            _require_doc_source_str(bad, "url", "some-slug")
        assert exc_info.value.code == "doc_sources_invalid"
        assert exc_info.value.exit_code == EXIT_CONFIG
        assert "some-slug" in exc_info.value.message
