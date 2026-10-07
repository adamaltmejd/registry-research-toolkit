"""The committed curation inputs load through the build's public loaders.

Load-time validation only: TOML shape, strict models, cross-file references the
loaders resolve, classification code CSVs and reviewed matrix evidence. Whether the
committed content is *right* (named owners, counts, endpoints that resolve against
real exports) is checked by the maintainer's real-seed strict build, not here.
"""

from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, get_origin

from _repo_curation_support import REPO_CURATION, REPO_ROOT
from pydantic import BaseModel
from reg_meta_build.cis2016_matrix import load_matrix
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.concept_groups import load_worklist_concept_groups
from reg_meta_build.doc_db import load_doc_sources, load_related_documents
from reg_meta_build.scb_errata import resolve_scb_errata

from reg_meta_build.fqid_slugs import (
    load_freeze_states,
    load_slug_dir,
    pinned_zones,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from reg_meta_build.curation_tree import CurationTree


# Overlay families the model accepts but no committed register declares yet. Remove
# an entry when its first declaration lands.
_NOT_YET_DECLARED = {"errata.edition_period"}


def _overlay_families(
    model: type[BaseModel], path: tuple[str, ...] = ()
) -> Iterator[tuple[str, ...]]:
    for name, info in model.model_fields.items():
        if get_origin(info.annotation) is list:
            yield (*path, name)
        elif isinstance(info.annotation, type) and issubclass(
            info.annotation, BaseModel
        ):
            yield from _overlay_families(info.annotation, (*path, name))


def _field(register: object, path: tuple[str, ...]) -> object:
    for name in path:
        register = getattr(register, name)
    return register


# One test on purpose: xdist spreads a module's tests across workers, and every
# worker that runs one would pay the committed-tree load again.
def test_committed_curation_loads(repo_tree: CurationTree) -> None:
    tree = repo_tree
    # Several loaders return empty on a missing path, so each result must be
    # non-empty for "loads" to mean anything.
    assert tree.registers
    assert tree.classifications
    # Every relation kind (same_as, replaced_by, derived_from, ...) stays declared.
    assert all(getattr(tree.relations, kind.name) for kind in fields(tree.relations)), (
        tree.relations
    )
    assert tree.tags
    assert tree.classification_groups.classification_group
    assert tree.lineage.defaults
    # Register-scoped overlay families default to empty lists, and a strict real-seed
    # build accepts their absence, so deleting every declaration of one would pass
    # silently. Each family the register model defines must be declared somewhere.
    families = _overlay_families(type(tree.registers[0]))
    undeclared = {
        ".".join(path)
        for path in families
        if not any(_field(register, path) for register in tree.registers)
    }
    assert undeclared == _NOT_YET_DECLARED, sorted(undeclared ^ _NOT_YET_DECLARED)

    slug_dir = repo_slug_dir()
    assert slug_dir == REPO_CURATION
    # A missing or emptied slug_state.toml reads as every provider churning, which
    # skips the committed auto pins and leaves the snapshot guards nothing to guard.
    # The committed file advances the global providers to curating (#759).
    assert pinned_zones(load_freeze_states(slug_dir))
    assert load_slug_dir(slug_dir)

    # The build's own call: errata resolve against the already-loaded registers.
    errata = resolve_scb_errata(
        tree.registers,
        classifications=frozenset(
            entry.classification.short_name for entry in tree.classifications
        ),
    )
    assert errata.delivered and errata.columns and errata.versions

    books = REPO_ROOT / "input_data" / "classifications"
    for entry in tree.classifications:
        assert load_valid_codes(books / entry.classification.codes_file), (
            entry.classification.short_name
        )

    matrices = [
        (register, declaration)
        for register in tree.registers
        for declaration in register.representation.matrix
    ]
    assert matrices
    for register, declaration in matrices:
        assert load_matrix(
            tree.root / declaration.evidence_file,
            source_mode=declaration.source_mode,
            expected_selector=declaration.selector,
        ).answers, register.source_file

    # simplify: the accepted [[group]] tables are checked by the tree's strict model
    # only; load_concept_groups re-parses every register file (~10 s), so its extra
    # key/axis checks run for the concept-group-candidates CLI, not here. Add it if
    # that CLI ever fails on committed groups.
    assert load_worklist_concept_groups(
        REPO_ROOT / "worklists" / "concept_groups.auto.toml"
    )

    assert load_doc_sources()
    assert load_related_documents(REPO_ROOT / "related_documents.toml")
