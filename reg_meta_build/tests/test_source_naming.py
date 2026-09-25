"""Finite naming conversion preserves intent without inventing native identity."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Literal

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from _curation_fixtures import write_fdb_partition_curation
from pydantic import ValidationError
from reg_meta_build.curation_compile import convert_column_partitions
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.id import mint, mint_canonical_scb
from reg_meta_build.source_coordinates import (
    native_parent_key,
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SourceEvidence,
    SourceRecordRef,
)
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    LegacyNamingBinding,
    NamingConversionError,
    NamingFreezeSetting,
    NamingSelection,
    NativeNamingTarget,
    authored_naming_id,
    check_naming_target,
    convert_naming,
    native_scb_naming_id,
    read_naming_selection,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceRevision,
    canonical_sha256,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry, declared_column_ownership

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_naming import NativeKey
    from reg_meta_build.source_records import SourceRecord


def _revision(path: Path | None = None) -> SourceRevision:
    payload = path.read_bytes() if path else b"fixture"
    return SourceRevision.create(
        dataset="test-source",
        publisher="fixture",
        purpose="naming test",
        upstream_revision="fixture",
        artifact_path=f"fqid_slugs/{path.name}" if path else "fixture",
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _entry(
    key: str = "1.101",
    slug: str | None = "variable-a",
    *,
    origin: Literal["authored", "generated"] = "authored",
    kind: Literal["register", "register_variant", "variable"] = "variable",
    **metadata: Any,
) -> AcceptedNamingEntry:
    raw = {**({"slug": slug} if slug is not None else {}), **metadata}
    naming = SlugEntry(kind, key, slug, provider="scb", **metadata)
    revision = f"fqid_slugs/scb{'.auto' if origin == 'generated' else ''}.toml"
    return AcceptedNamingEntry(
        revision=revision,
        origin=origin,
        entry=naming,
        supplied_fields=tuple(sorted(raw)),
        content_sha256=canonical_sha256(raw),
    )


def _selection(
    *entries: AcceptedNamingEntry,
    state: Literal["churning", "curating", "frozen"] = "curating",
) -> NamingSelection:
    return NamingSelection(
        files=(),
        entries=entries,
        freeze=(NamingFreezeSetting(zone="scb", state=state),),
    )


def _target(
    key: NativeKey = ("source", "variable", 101),
    *,
    register: NativeKey = ("source", "register", 1),
    kind: Literal["register", "register_variant", "variable"] = "variable",
) -> NativeNamingTarget:
    return NativeNamingTarget(
        kind=kind,
        provider="scb",
        source_key=key,
        register_key=register if kind in ("variable", "register_variant") else None,
    )


def _binding(
    entry: AcceptedNamingEntry, target: NativeNamingTarget | None = None
) -> LegacyNamingBinding:
    return LegacyNamingBinding(
        kind=entry.entry.kind,
        provider=entry.entry.provider,
        source_id=entry.entry.source_id,
        target=target or _target(),
    )


def _record(
    cvid: int = 1001, var_id: int = 101, *, description: str = "label"
) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    row = _var_row(colname="COL", cvid=cvid, var_id=var_id, vardesc=description).split(
        "|"
    )
    cells: dict[str, tuple[bool, str | None, str]] = {
        name: (True, value, value) for name, value in zip(header, row, strict=True)
    }
    return clean_scb_row(header, 2, cells, _revision()).record


def test_reader_uses_tracked_register_files_and_relative_provenance(
    tmp_path: Path,
) -> None:
    root = tmp_path / "curation"
    register = root / "registers" / "scb"
    register.mkdir(parents=True)
    (root / "classifications").mkdir()
    path = register / "sample.toml"
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "authored"\ndeprecated = true\n',
        encoding="utf-8",
    )
    (register / "sample.auto.toml").write_text(
        '[[variable]]\nnative_id = "1.101"\nslug = "generated"\n',
        encoding="utf-8",
    )
    selection = read_naming_selection(load_curation_tree(root))
    assert len(selection.entries) == 3
    assert {file.role for file in selection.files} == {"authored", "generated"}
    assert sum(file.entry_count for file in selection.files) == 3
    authored = next(
        entry
        for entry in selection.entries
        if entry.revision == "curation/registers/scb/sample.toml"
        and entry.entry.kind == "variable"
    )
    assert authored.content_sha256 == canonical_sha256(
        {"slug": "authored", "deprecated": True}
    )
    assert authored.supplied_fields == ("deprecated", "slug")
    result = convert_naming(selection, [_binding(authored)])
    assert result.declarations[0].naming.slug == "authored"
    assert result.declarations[0].naming.deprecated
    assert len(result.dispositions) == 3
    assert {row.status for row in result.dispositions} == {
        "bound",
        "shadowed",
        "pending_source_binding",
    }
    path.write_text(path.read_text(encoding="utf-8").replace("authored", "modified"))
    assert any(
        entry.entry.slug == "modified"
        for entry in read_naming_selection(load_curation_tree(root)).entries
    )


def test_absent_authored_slug_preserves_generated_name_and_authored_metadata() -> None:
    authored = _entry(slug=None, deprecated=True, replaced_by="1.102")
    generated = _entry(origin="generated")
    result = convert_naming(_selection(authored, generated), [_binding(authored)])
    assert result.complete
    (declaration,) = result.declarations
    assert declaration.naming.slug == "variable-a"
    assert declaration.naming.deprecated
    assert declaration.naming.replaced_by == "1.102"
    assert {entry.entry.slug for entry in declaration.contributors} == {
        None,
        "variable-a",
    }
    assert {row.status for row in result.dispositions} == {"bound"}
    without_auto = convert_naming(_selection(authored), [_binding(authored)])
    assert without_auto.declarations[0].naming.slug is None
    assert without_auto.declarations[0].contributors == (authored,)


def test_variant_metadata_and_typo_pointer_remain_exact_intent() -> None:
    authored = _entry(
        "1.10",
        "subset",
        kind="register_variant",
        display_group="Group",
        panel_entity_key=("person", "household"),
        panel_time_key="period",
        panel_time_grain="delivery",
        deprecated=True,
        replaced_by="1.11",
    )
    result = convert_naming(
        _selection(authored), [_binding(authored, _target(kind="register_variant"))]
    )
    assert result.complete
    assert result.declarations[0].naming == authored.entry


def test_churning_generated_files_are_accounted_but_never_converted() -> None:
    generated = _entry(origin="generated")
    result = convert_naming(_selection(generated, state="churning"), ())
    assert result.complete
    assert not result.declarations
    assert result.dispositions[0].status == "inactive_generated"


def test_missing_split_bridge_remains_engineering_conversion_gap() -> None:
    ordinary, split = _entry(), _entry("1.101.sibling", "sibling")
    result = convert_naming(_selection(ordinary, split), [_binding(ordinary)])
    assert len(result.declarations) == 1
    assert result.dispositions[1].status == "pending_source_binding"
    assert result.diagnostics[0].code == "naming_source_binding_pending"
    assert "split-member" in result.diagnostics[0].detail
    converted = convert_naming(
        _selection(split),
        [_binding(split, _target(("explicit-split", 101, "member:5")))],
    )
    assert converted.complete
    assert converted.declarations[0].naming.source_id == "1.101.sibling"


_Y167_REF = "Y-167 fixture reference"
_Y167_COLUMNS = {
    "GatuRest": "1.830.gaturest",
    "Gaturest": "1.830.gaturest",
    "PGaturest": "1.830.pgaturest",
}
_Y167_SPLITS = ("1.830.gaturest", "1.830.pgaturest")


def _declaration_selection(tmp_path: Path) -> tuple[NamingSelection, Path]:
    """Read tracked split names beside their register ownership declaration."""
    curation_dir = write_fdb_partition_curation(tmp_path / "curation")
    (curation_dir / "classifications").mkdir()
    register = curation_dir / "registers" / "scb" / "fdb.toml"
    register.write_text(
        register.read_text(encoding="utf-8")
        + '[[variable]]\nnative_id = "1.830.gaturest"\n'
        + '[[variable]]\nnative_id = "1.830.pgaturest"\nslug = "pgaturest"\n',
        encoding="utf-8",
    )
    register.with_name("fdb.auto.toml").write_text(
        '[[variable]]\nnative_id = "1.830.gaturest"\nslug = "gaturest"\n'
        '[[variable]]\nnative_id = "1.830.pgaturest"\nslug = "pgaturest"\n',
        encoding="utf-8",
    )
    return read_naming_selection(load_curation_tree(curation_dir)), curation_dir


def test_tracked_column_ownership_survives_naming_conversion(
    tmp_path: Path,
) -> None:
    # Ownership lives on the register file; the slug selection remains naming-only.
    selection, curation_dir = _declaration_selection(tmp_path)
    authored = next(
        entry
        for entry in selection.entries
        if entry.origin == "authored" and entry.entry.source_id == "1.830.gaturest"
    )
    assert authored.supplied_fields == ()
    assert authored.content_sha256 == canonical_sha256({})
    assert not hasattr(authored.entry, "columns")
    assert not hasattr(authored.entry, "columns_ref")
    ownership = declared_column_ownership(
        [entry.entry for entry in selection.entries],
        provider="scb",
        source_id="1.830",
        curation_dir=curation_dir,
    )
    assert ownership.declaration_reference == _Y167_REF
    assert dict(ownership.declared_columns) == _Y167_COLUMNS
    register_entry = next(
        entry for entry in selection.entries if entry.entry.kind == "register"
    )
    bindings = [
        _binding(
            register_entry,
            _target(("source", "register", 1), kind="register"),
        ),
        *[
            LegacyNamingBinding(
                kind="variable",
                provider="scb",
                source_id=source_id,
                target=_target((source_id, "native")),
            )
            for source_id in _Y167_SPLITS
        ],
    ]
    result = convert_naming(selection, bindings)
    assert not result.diagnostics
    by_disposition = {row.entry_id: row.status for row in result.dispositions}
    # The empty authored row contributes no slug, so the generated gaturest
    # pin remains active. The authored pgaturest slug still shadows its
    # generated row under the existing override rule.
    assert (
        by_disposition["curation/registers/scb/fdb.toml#variable/1.830.gaturest"]
        == "bound"
    )
    assert (
        by_disposition["curation/registers/scb/fdb.auto.toml#variable/1.830.gaturest"]
        == "bound"
    )
    by_id = {item.naming.source_id: item for item in result.declarations}
    gaturest = by_id["1.830.gaturest"]
    assert gaturest.naming.slug == "gaturest"
    assert not hasattr(gaturest.naming, "columns")
    assert not hasattr(gaturest.naming, "columns_ref")
    assert {entry.origin for entry in gaturest.contributors} == {
        "authored",
        "generated",
    }
    assert not hasattr(by_id["1.830.pgaturest"].naming, "columns")


def test_naming_selection_entries_feed_declared_partition_conversion(
    tmp_path: Path,
) -> None:
    # The same selection entries are the complete entry set the declared
    # partition converter consumes: the operator's scope transcription needs
    # no hand-plumbed map.
    selection, curation_dir = _declaration_selection(tmp_path)
    header = REGISTERINFORMATION_HEADER.split("|")

    def record(column: str, cvid: int) -> SourceRecord:
        row = _var_row(colname=column, cvid=cvid, var_id=830).split("|")
        cells: dict[str, tuple[bool, str | None, str]] = {
            name: (True, value, value) for name, value in zip(header, row, strict=True)
        }
        return clean_scb_row(header, cvid, cells, _revision()).record

    records = (record("GatuRest", 20), record("Gaturest", 21), record("PGaturest", 22))
    ownership = declared_column_ownership(
        [entry.entry for entry in selection.entries],
        provider="scb",
        source_id="1.830",
        curation_dir=curation_dir,
    )
    converted = convert_column_partitions(
        records,
        source_id="1.830",
        split_ids=_Y167_SPLITS,
        declared_columns=dict(ownership.declared_columns),
        declaration_reference=ownership.declaration_reference,
    )
    assert converted.case is not None and converted.diagnostics == ()
    assert [binding.source_id for binding in converted.bindings] == list(_Y167_SPLITS)
    assert converted.case.decision.kind == "correct_occurrences"
    assert converted.case.decision.provenance.endswith(f"column ownership: {_Y167_REF}")


def test_nonidentical_duplicate_bindings_block_and_identical_duplicates_coalesce() -> (
    None
):
    entry = _entry()
    binding = _binding(entry)
    assert convert_naming(_selection(entry), (binding, binding)).complete
    changed_guard = binding.target.model_copy(
        update={"source_key": ("source", "variable", 102)}
    )
    result = convert_naming(
        _selection(entry), (binding, _binding(entry, changed_guard))
    )
    assert not result.declarations
    assert result.dispositions[0].status == "blocked"
    assert result.diagnostics[0].code == "naming_binding_ambiguous"


def test_distinct_effective_names_for_one_exact_target_are_blocked() -> None:
    first, second = _entry(), _entry("1.102", "variable-b")
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second)]
    )
    assert not result.declarations
    assert {row.status for row in result.dispositions} == {"blocked"}
    assert result.diagnostics[0].code == "naming_target_multiple_bindings"


def test_identical_accepted_assignments_keep_every_contributor() -> None:
    first, second = _entry(), _entry("1.102")
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second)]
    )
    assert result.complete
    assert len(result.declarations) == 1
    assert result.declarations[0].contributors == (first, second)
    assert len(result.dispositions) == 2


def test_collisions_use_exact_register_scope_and_keep_integer_string_keys_distinct() -> (
    None
):
    first, second = _entry(), _entry("2.101")
    target_b = _target(("source", "variable", "101"))
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second, target_b)]
    )
    assert not result.declarations
    assert result.diagnostics[0].code == "naming_slug_collision"
    target_b = target_b.model_copy(update={"register_key": ("source", "register", 2)})
    result = convert_naming(
        _selection(first, second), [_binding(first), _binding(second, target_b)]
    )
    assert result.complete
    assert len(result.declarations) == 2


def test_native_and_existing_hashed_bridges_reproduce_current_keys() -> None:
    assert native_scb_naming_id("register", 1) == "1"
    assert native_scb_naming_id("register_variant", 1, 10) == "1.10"
    assert native_scb_naming_id("variable", 1, 101) == "1.101"
    assert authored_naming_id("register", provider="sos", register_key="par") == str(
        mint("sos", "par")
    )
    assert (
        authored_naming_id(
            "register_variant",
            provider="sos",
            register_key="par",
            member_key="Slutenvård",
        )
        == f"{mint('sos', 'par')}.{mint('sos', 'par', 'Slutenvård')}"
    )
    assert (
        authored_naming_id(
            "variable", provider="fk", register_key="midas", member_key="BENAMNING"
        )
        == f"{mint('fk', 'midas')}.BENAMNING"
    )
    assert authored_naming_id(
        "register", provider="scb", register_key="utland", canonical_scb=True
    ) == str(mint_canonical_scb("scb", "utland"))
    with pytest.raises(NamingConversionError, match="split"):
        authored_naming_id(
            "variable", provider="sos", register_key="par", member_key="CODE.sibling"
        )
    with pytest.raises(NamingConversionError, match="explicit"):
        authored_naming_id("register", provider="scb", register_key="utland")
    with pytest.raises(NamingConversionError, match="integer"):
        native_scb_naming_id("register", True)


def test_naming_guards_check_identity_and_new_peers_without_unrelated_fields() -> None:
    record = _record()
    ref = SourceRecordRef(
        source=record.source, semantic_record_key=record.locators[0].semantic_record_key
    )
    expectation = RecordExpectation(
        ref=ref, alternatives=(RecordProjection(subject=record.subject),)
    )
    guard = PeerGuard(
        guard_id="variable-101",
        source=record.source,
        native=NativeCoordinates(register_id=1, variable_id=101),
        expected_members=(ref,),
    )
    target = NativeNamingTarget(
        kind="variable",
        provider="scb",
        source_key=(
            "test-source",
            "scb",
            "register",
            "native-int",
            1,
            "variable",
            "native-int",
            101,
        ),
        register_key=("test-source", "scb", "register", "native-int", 1),
        expectations=(expectation,),
        peer_guards=(guard,),
    )
    assert not check_naming_target(target, [record])
    assert not check_naming_target(
        target, [_record(description="changed unrelated prose")]
    )
    changed = check_naming_target(target, [_record(var_id=102)])
    assert changed and any(
        issue.applicability_issue and issue.applicability_issue.code == "target_missing"
        for issue in changed
    )
    added = check_naming_target(target, [record, _record(cvid=1002)])
    assert len(added) == 1
    assert added[0].applicability_issue is not None
    assert added[0].applicability_issue.code == "peer_membership_changed"
    with pytest.raises(ValidationError, match="cover exactly"):
        NativeNamingTarget.model_validate(
            {
                **target.model_dump(),
                "peer_guards": (
                    guard.model_copy(
                        update={
                            "expected_members": (
                                SourceRecordRef(
                                    source=record.source,
                                    semantic_record_key=("different",),
                                ),
                            )
                        }
                    ),
                ),
            }
        )


def test_native_name_depends_on_existing_identity_not_delivery_membership() -> None:
    original = _record()
    target = NativeNamingTarget(
        kind="variable",
        provider="scb",
        source_key=native_variable_key(original),
        register_key=source_register_key(original),
    )
    assert not check_naming_target(target, SourceEvidence((original,)))
    changed = SourceEvidence((_record(description="changed prose"), _record(cvid=1002)))
    assert not check_naming_target(target, changed)
    assert (
        check_naming_target(target, (_record(var_id=102),))[0].code
        == "naming_native_identity_missing"
    )
    assert check_naming_target(target.model_copy(update={"provider": "other"}), changed)
    assert check_naming_target(
        target.model_copy(
            update={"source_key": (*target.source_key, "accepted-partition", "other")}
        ),
        changed,
    )


@pytest.mark.parametrize("kind", ["register", "register_variant"])
def test_parent_naming_checks_identity_anchor_without_claiming_peer_membership(
    kind: Literal["register", "register_variant"],
) -> None:
    record = _record()
    ref = SourceRecordRef(
        source=record.source, semantic_record_key=record.locators[0].semantic_record_key
    )
    target = NativeNamingTarget(
        kind=kind,
        provider="scb",
        source_key=("source", kind, 1),
        register_key=("source", "register", 1) if kind == "register_variant" else None,
        expectations=(
            RecordExpectation(
                ref=ref, alternatives=(RecordProjection(subject=record.subject),)
            ),
        ),
    )
    assert not check_naming_target(target, (record, _record(cvid=1002)))
    assert not check_naming_target(target, (_record(description="irrelevant prose"),))
    assert check_naming_target(target, (_record(cvid=1002),))
    changed = record.model_copy(
        update={
            "subject": record.subject.model_copy(
                update={
                    "register_name": record.subject.register_name.model_copy(
                        update={"native_id": 2}
                    )
                }
            )
        }
    )
    assert check_naming_target(target, (changed,))
    guarded = target.model_copy(
        update={
            "peer_guards": (
                PeerGuard(
                    guard_id="explicit-parent-membership",
                    source=record.source,
                    native=NativeCoordinates(register_id=1),
                    expected_members=(ref,),
                ),
            )
        }
    )
    assert check_naming_target(guarded, (record, _record(cvid=1002)))
    with pytest.raises(ValidationError, match="complete peer guards"):
        NativeNamingTarget.model_validate(
            {
                **target.model_dump(),
                "kind": "variable",
                "register_key": ("source", "register", 1),
            }
        )


def test_expectation_free_parent_naming_checks_parent_facts() -> None:
    record = _record()
    for parent in record.parent_facts:
        if parent.kind not in {"register", "variant"}:
            continue
        key = native_parent_key(record.source, record.subject.provider, parent)
        assert key is not None
        kind = "register" if parent.kind == "register" else "register_variant"
        target = NativeNamingTarget(
            kind=kind,
            provider=record.subject.provider,
            source_key=key,
            register_key=source_register_key(record)
            if kind == "register_variant"
            else None,
        )
        assert not check_naming_target(target, (record,))
        assert check_naming_target(
            target.model_copy(update={"source_key": (*key, "missing")}), (record,)
        )
