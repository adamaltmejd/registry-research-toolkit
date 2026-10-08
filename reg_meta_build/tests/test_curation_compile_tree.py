"""Curation compile event-source pairing, thin default-variant naming and the tree hash."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from _curation_compile_support import (
    make_naming_reader as _naming_reader,
    make_prepared as _prepared,
    make_revision as _revision,
    make_scope as _scope,
    make_tree as _tree,
)
from reg_meta_build.curation_compile import (
    compile_curation,
    compile_native_naming,
    tree_sha256,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
)
from reg_meta_build.id import mint
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.resolved_catalog import ResolvedVariant
from reg_meta_build.source_coordinates import (
    source_register_key,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    SourceCoordinate,
)

from reg_meta_build.fqid_slugs import SlugEntry


def test_event_sources_pair_within_same_snapshot_revision(tmp_path):
    inputs = tuple(
        SimpleNamespace(
            origin="snapshot",
            role="scb_events" if path == "Timeseries.csv" else "scb_records",
            path=path,
            revision=_revision(
                dataset,
                artifact_path=f"{snapshot}:source/{path}",
                upstream_revision=edition,
            ),
        )
        for snapshot, edition, path, dataset in (
            ("snapshot-a", "v1", "Timeseries.csv", "timeseries-a"),
            ("snapshot-b", "v2", "Timeseries.csv", "timeseries-b"),
            ("snapshot-b", "v2", "Registerinformation.csv", "register-b"),
            ("snapshot-a", "v1", "Registerinformation.csv", "register-a"),
        )
    )
    prepared = SimpleNamespace(
        manifest=SimpleNamespace(inputs=inputs),
        iter_evidence=lambda: iter(()),
        records=_prepared().records,
    )
    scopes = tuple(
        _scope().model_copy(update={"source": source})
        for source in ("register-a", "register-b")
    )
    result = compile_curation(
        _tree(tmp_path / "curation"), cast("Any", prepared), scopes, subset=True
    )
    assert result.fields["event_sources"] == (
        ("timeseries-a", "register-a"),
        ("timeseries-b", "register-b"),
    )


def test_thin_default_variant_carries_panel_fields(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    register_id = mint("fk", "activity")
    variant_id = mint("fk", "activity", "_default")
    path = root / "registers" / "fk" / "activity.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "fk"\nslug = "activity"\n'
        f'native_id = "{register_id}"\n'
        "[[variant]]\n"
        f'native_id = "{register_id}.{variant_id}"\n'
        'slug = "_default"\ndisplay_group = "Activity"\n'
        'panel_entity_key = "person"\npanel_time_key = "period"\n'
        'panel_time_grain = "delivery"\n'
    )
    coordinate = SourceCoordinate(status="value", native_id="activity")
    record = SimpleNamespace(
        source="fk-source",
        subject=SimpleNamespace(
            provider="fk",
            register_name=coordinate,
            variant=SourceCoordinate(status="not_applicable"),
        ),
        parent_facts=(
            SimpleNamespace(kind="register", register_name=coordinate, variant=None),
        ),
    )

    class Reader:
        def iter_native_families(self, source, registers=None):
            return iter(())

        def iter_records(self, *, source):
            return iter((record,))

        def iter_register_slices(self, source, registers):
            return iter(((None, (record,)),))

    native_register = source_register_key(cast("Any", record))
    assert native_register is not None
    scope = CompiledScope(
        source=record.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="fk",
                    source_key=native_register,
                ),
                naming=SlugEntry(
                    kind="register",
                    provider="fk",
                    source_id=str(register_id),
                    slug="activity",
                ),
                contributors=(),
            ),
        ),
    )
    names, variants, _, diagnostics, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
        (scope,),
        subset=True,
    )
    assert not diagnostics
    default_key = (*native_register, "variant", "not-applicable")
    assert names[(record.source, None)][1].target.source_key == default_key
    assert variants[(record.source, None)][0][1] == ResolvedVariant(
        slug="_default",
        name="_default",
        display_group="Activity",
        panel_entity_key="person",
        panel_time_key="period",
        panel_time_grain="delivery",
    )
    # The build cases cannot reach these two claims yet. The panel fields need a
    # `variants` projection field (cases/build `variants` has register/variant/name
    # only). The untracked-default stale diagnostic never reaches the ledger: the
    # build stops first (cases/build/thin-default-variant-without-a-tracked-slug-fails-the-build).
    path.write_text(path.read_text().split("[[variant]]")[0])
    _, _, _, missing, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
        (scope,),
        subset=True,
    )
    assert any(
        issue.code == "stale_curation_entry"
        and "no tracked default slug" in issue.detail
        for issue in missing
    )


def test_tree_hash_covers_curation_and_source_slug_files(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    first = tree_sha256(root)
    tags = root / "tags.toml"
    tags.write_text(tags.read_text() + "# editorial change\n")
    second = tree_sha256(root)
    assert second != first
    slugs = tmp_path / "fqid_slugs"
    slugs.mkdir()
    (slugs / "scb.toml").write_text('[register."1"]\nslug = "sample"\n')
    assert tree_sha256(root) != second
