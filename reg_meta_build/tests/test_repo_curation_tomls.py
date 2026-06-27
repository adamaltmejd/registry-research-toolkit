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
from reg_meta.fqid import FqidKind
from reg_meta_build.canonical_attach import load_canonical_attach
from reg_meta_build.codelivery import load_codelivery
from reg_meta_build.concept_groups import (
    load_classification_groups,
    load_concept_group_accepts,
    load_concept_groups,
)
from reg_meta_build.delivery_enrichment import load_delivery_enrichment
from reg_meta_build.doc_db import _require_doc_source_str, load_doc_sources
from reg_meta_build.period_family_merges import load_period_family_merges
from reg_meta_build.relations import _SAME_AS_MAX_COMPONENT, load_relations
from reg_meta_build.source_column_repairs import load_column_merges
from reg_meta_build.variable_grafts import load_variable_grafts

# reg_meta_build/ package root (tests/ sits beside the TOMLs).
_ROOT = Path(__file__).resolve().parent.parent


def test_repo_codelivery_parses() -> None:
    assert load_codelivery(_ROOT / "codelivery.toml")


def test_repo_column_merges_parses() -> None:
    assert load_column_merges(_ROOT / "curation" / "scb" / "source_column_repairs.toml")


def test_repo_concept_groups_parses() -> None:
    groups = load_concept_groups(_ROOT / "concept_groups.toml")
    assert groups  # the LISA agi rank family ships with the repo
    # Build-time resolution (register/group/variable exist) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(len(g.members) >= 2 for g in groups)


def test_repo_concept_groups_auto_parses() -> None:
    # `concept_groups.auto.toml` (#496) is the GENERATED, build-critical candidate
    # catalog — an `[[accept]]` resolves against it at materialize time, so a
    # parse-incompatible regeneration would break a real build. This catches that
    # without a full build-db. Direct path (not `repo_concept_groups_auto_path`)
    # matches the other repo-TOML tests; the loader re-validates the shape.
    groups = load_concept_groups(_ROOT / "concept_groups.auto.toml")
    assert groups  # the generator emits >0 foldable families
    assert all(len(g.members) >= 2 for g in groups)


def test_repo_concept_group_accepts_parses() -> None:
    # The `[[accept]]` opt-in list lives in `concept_groups.toml` (the same file
    # as `[[variable_group]]`). Its load-time shape is build-critical too; resolution
    # against the auto catalog is maintainer-build territory. The gate is that it
    # PARSES with a valid load-time shape; the count grows as curation batches land
    # (the #496 batch-1 SOS families ship now), so assert presence + shape, not an
    # exact count.
    accepts = load_concept_group_accepts(_ROOT / "concept_groups.toml")
    assert accepts
    assert all(a.provider and a.register and a.key for a in accepts)


def test_repo_classification_groups_parses() -> None:
    # Curated `[[classification_group]]` umbrellas (#516) live in the same
    # `concept_groups.toml`. The SUN umbrella ships with the repo; slug RESOLUTION
    # (the classifications exist) is maintainer-build territory — this gate is the
    # load-time shape (>= 2 members, unique keys/slugs). The umbrellas are now
    # AXIS-LESS (axis is None — members are distinct classifications, not points
    # on a scale), so this no longer asserts a truthy axis.
    groups = load_classification_groups(_ROOT / "concept_groups.toml")
    assert groups
    assert {g.key for g in groups} >= {"sun"}
    assert all(len(g.members) >= 2 for g in groups)
    assert all(g.axis is None for g in groups)


def test_repo_delivery_enrichment_parses() -> None:
    enr = load_delivery_enrichment(_ROOT / "delivery_enrichment.toml")
    # the #365 global description backfills + delivery-column aliases ship together
    assert enr.descriptions
    assert enr.aliases
    # Slug RESOLUTION is lenient + maintainer-build territory; load-time shape
    # (2-segment FQID, unique keys) is this gate.
    assert all(d.provider and d.register and d.variable for d in enr.descriptions)
    assert all(a.provider and a.register and a.delivery_column for a in enr.aliases)


def test_repo_period_family_merges_parses() -> None:
    families = load_period_family_merges(
        _ROOT / "curation" / "period_family_merges.toml"
    )
    assert families  # the #319 LISA monthly families ship with the repo
    # Member RESOLUTION (12 month columns exist for the stem) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(
        f.provider and f.register and f.family_stem and f.label for f in families
    )


def test_repo_relations_parses() -> None:
    # The single typed `[[edge]]` surface (#522). It ships with the #375 variable
    # succession edges + the #579 sun1996 classification split (both
    # `type = "replaced_by"`) + the #403 see-also edges (`type = "related_to"`) +
    # the #508 tier-1 curated `same_as` identity batch. The gate is load-time shape
    # — a malformed entry would otherwise surface only on a real build. Endpoint
    # RESOLUTION is maintainer-build territory (the materializers fail fast).
    relations = load_relations(_ROOT / "curation" / "relations.toml")
    # The #508 tier-1 batch: 615 curated variable-grain identity edges.
    assert len(relations.same_as) == 615
    assert all(
        e.grain is FqidKind.VARIABLE_BINDING and e.a_variable and e.b_variable
        for e in relations.same_as
    )
    # The two load-bearing same_as invariants the bare count doesn't pin:
    # (1) DISTINCT unordered pairs — no edge repeats a {a, b} pair (the loader
    #     rejects duplicates, so a regression here means the loader's dedup broke
    #     or the file was hand-edited to bypass it).
    pairs = {frozenset((e.a_fqid(), e.b_fqid())) for e in relations.same_as}
    assert len(pairs) == len(relations.same_as)
    # (2) COMPONENT CAP — the actual safety property same_as exists to protect: a
    #     mistaken edge welds two identity components into a runaway resolver blob.
    #     Recompute connected components (union-find over the endpoint FQIDs) and
    #     assert the max <= _SAME_AS_MAX_COMPONENT, so a bad curation edge fails the
    #     unit suite, not only the maintainer build's `materialize_same_as` guard.
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for e in relations.same_as:
        ra, rb = find(e.a_fqid()), find(e.b_fqid())
        if ra != rb:
            parent[ra] = rb
    sizes: dict[str, int] = {}
    for node in parent:
        sizes[find(node)] = sizes.get(find(node), 0) + 1
    assert max(sizes.values()) <= _SAME_AS_MAX_COMPONENT
    # 11 #375 variable succession edges + 2 #400 SSYK J16 succession edges
    # + 3 #579 classification split edges
    # + 2 #770 ICD/KS disease-classification succession edges
    # + 7 #814 iot disponibel-inkomst 2004-års-definition succession edges
    # + 1 #846 RTB PNR → PersonNr representation-grain rename edge.
    assert len(relations.replaced_by) == 26
    assert len(relations.related_to) == 3  # the moved #403 see-also edges
    assert all(
        e.a_provider
        and e.a_register
        and e.a_variable
        and e.b_provider
        and e.b_register
        and e.b_variable
        and e.relation_kind
        for e in relations.related_to
    )
    assert all(str(e.predecessor) and str(e.successor) for e in relations.replaced_by)


def test_repo_canonical_attach_parses() -> None:
    # The #400 PR2 canonical-attach seed lives beside the other canonical-SCB
    # seed (input_data/scb_canonical/). It is authored separately and may not
    # ship yet — skip when absent; when present, the load-time shape (2-segment
    # FQID, required keys, ISO dates, declared classifications) must hold. The
    # loader tolerates a missing path (returns []), but here we want a real gate
    # on the in-repo file, so resolve the direct path and skip if it's not there.
    path = _ROOT / "input_data" / "scb_canonical" / "lisa_canonical.toml"
    if not path.is_file():
        pytest.skip("lisa_canonical.toml not present in this checkout")
    entries = load_canonical_attach(path)
    assert entries  # a present seed must carry at least one [[attach]]
    assert all(e.provider == "scb" and e.register and e.variant for e in entries)
    assert all(e.column and e.name and e.definition for e in entries)


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
