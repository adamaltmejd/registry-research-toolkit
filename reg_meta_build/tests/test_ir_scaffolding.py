"""Contract tests for the A1.3 IR module + adapter scaffolding.

These tests verify only the scaffolding: import surface, round-trip,
Protocol conformance, provenance DB schema, and `.prev` rotation.
Concrete adapter wiring lives in later stages (A4.x).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import TYPE_CHECKING, Protocol, get_type_hints, runtime_checkable

import pytest
from pydantic import BaseModel
from reg_meta.errors import RegMetaError
from reg_meta_build.db import (
    PROVENANCE_DB_FILENAME,
    create_empty_provenance_db,
    rotate_db_to_prev,
)
from reg_meta_build.ir import (
    IRClassification,
    IRDeliveryProvenance,
    IRLineageEdge,
    IRRegister,
    IRReplacedByEdge,
    IRValueCode,
    IRValueSet,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
    IRWarning,
)
from reg_meta_build.sources import IRAdapter, IRObject

from reg_meta_build import ir

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

# Every IR class + a minimal-valid factory for it. New classes must be added
# here for the round-trip suite to cover them.
_IR_FACTORIES: dict[str, tuple[type[BaseModel], dict[str, object]]] = {
    "IRRegister": (
        IRRegister,
        {
            "register_id": 1,
            "provider": "scb",
            "slug": "lisa",
            "name": "LISA",
            "description": None,
            "purpose": None,
        },
    ),
    "IRVariant": (
        IRVariant,
        {
            "register_variant_id": 10,
            "register_id": 1,
            "slug": "_default",
            "name": "LISA default",
            "description": None,
        },
    ),
    "IRVariable": (
        IRVariable,
        {
            "variable_id": 100,
            "register_id": 1,
            "provider_key": "44",
            "slug": "kon",
            "name": "Kön",
            "definition": None,
            "description": None,
            "measurement_unit": None,
            "source_register_id": None,
            "source_register_text": None,
            "source_label": None,
        },
    ),
    "IRVariableState": (
        IRVariableState,
        {
            "state_id": 1000,
            "variable_id": 100,
            "register_variant_id": 10,
            "valid_from": "2018",
            "valid_to": None,
            "data_type": "text",
            "data_length": None,
            "delivery_column_name": "Kon",
            "value_set_id": None,
            "value_set_version_label": None,
        },
    ),
    "IRVariableAlias": (
        IRVariableAlias,
        {
            "variable_id": 100,
            "register_variant_id": 10,
            "delivery_column_name": "Kon",
        },
    ),
    "IRValueCode": (
        IRValueCode,
        {
            "code_id": 7,
            "value_set_id": 50,
            "code": "1",
            "label": "Man",
            "valid_from": None,
            "valid_to": None,
        },
    ),
    "IRValueSet": (
        IRValueSet,
        {
            "value_set_id": 50,
            # 32-byte raw SHA-256 digest (matches the universal
            # `value_set.member_hash` BLOB column's CHECK constraint).
            "member_hash": bytes.fromhex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            ),
            "classification_id": None,
            "codes": (
                IRValueCode(
                    code_id=7,
                    value_set_id=50,
                    code="1",
                    label="Man",
                    valid_from=None,
                    valid_to=None,
                ),
            ),
        },
    ),
    "IRClassification": (
        IRClassification,
        {
            "classification_id": 7,
            "slug": "sun2020",
            "name": "SUN 2020",
            "publisher": "SCB",
            "provider": None,
        },
    ),
    "IRLineageEdge": (
        IRLineageEdge,
        {
            "consumer_state_id": 1001,
            "source_state_id": 999,
            "valid_from": "2018-01-01",
            "valid_to": "2018-12-31",
        },
    ),
    "IRReplacedByEdge": (
        IRReplacedByEdge,
        {
            "predecessor_variable_id": 10,
            "successor_variable_id": 11,
            "effective_year": 2020,
            "note": None,
        },
    ),
    "IRWarning": (
        IRWarning,
        {
            "entity_kind": "variable",
            "entity_id": 42,
            "code": "missing_definition",
        },
    ),
    "IRDeliveryProvenance": (
        IRDeliveryProvenance,
        {
            "register_id": 1,
            "register_variant_id": 10,
            "source_file": "Registerinformation.csv",
            "delivery_version": "2024-08",
            "delivery_date": date(2024, 8, 15),
            "template_version": None,
            "first_approval_dates": {"2020": "2020-06-01"},
            "last_approval_dates": {"2020": "2021-01-15"},
        },
    ),
}


def test_ir_module_exports_all_classes() -> None:
    """`reg_meta_build.ir.__all__` lists every BaseModel subclass in the module."""
    public = set(ir.__all__)
    expected = set(_IR_FACTORIES)
    assert public == expected, (
        f"__all__ drift; missing={expected - public} extra={public - expected}"
    )


@pytest.mark.parametrize("cls_name", sorted(_IR_FACTORIES))
def test_ir_classes_are_basemodel_subclasses(cls_name: str) -> None:
    cls, _ = _IR_FACTORIES[cls_name]
    assert issubclass(cls, BaseModel), f"{cls_name} is not a Pydantic BaseModel"


@pytest.mark.parametrize("cls_name", sorted(_IR_FACTORIES))
def test_ir_round_trip(cls_name: str) -> None:
    """Construct → model_dump → model_validate → equality."""
    cls, kwargs = _IR_FACTORIES[cls_name]
    instance = cls(**kwargs)
    dumped = instance.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == instance


@pytest.mark.parametrize("cls_name", sorted(_IR_FACTORIES))
def test_ir_rejects_unknown_keys(cls_name: str) -> None:
    """`extra="forbid"` on `_IRBase` must propagate so adapter typos
    raise instead of silently dropping into defaulted fields.

    Example failure mode this guards: a misspelled `is_sensitiv=True`
    on `IRVariable` under Pydantic's default `extra="ignore"` would
    be dropped without warning, and the field would quietly stay
    `False` — corrupting downstream catalog output with no exception.
    """
    from pydantic import ValidationError

    cls, kwargs = _IR_FACTORIES[cls_name]
    bad_kwargs = {**kwargs, "definitely_not_a_real_field": "oops"}
    with pytest.raises(ValidationError) as exc_info:
        cls(**bad_kwargs)
    # Pydantic 2.x reports unknown fields with the `extra_forbidden` code.
    assert any(err["type"] == "extra_forbidden" for err in exc_info.value.errors()), (
        exc_info.value.errors()
    )


def test_iradapter_is_protocol() -> None:
    """IRAdapter must be a typing.Protocol, not a regular class."""
    # PEP 544: Protocol classes have `_is_protocol = True` on the class object.
    assert getattr(IRAdapter, "_is_protocol", False), (
        "IRAdapter must be declared as typing.Protocol"
    )


def test_iradapter_emit_signature() -> None:
    """The IRAdapter contract exposes `provider: str` and `emit(source_dir) -> Iterator[IRObject]`."""
    hints = get_type_hints(IRAdapter)
    assert "provider" in hints
    assert hints["provider"] is str
    assert callable(IRAdapter.emit)


def test_iradapter_runtime_conformance() -> None:
    """A concrete class implementing the protocol shape satisfies IRAdapter at runtime."""

    # `runtime_checkable` decoration applies to the local Protocol; the source
    # IRAdapter intentionally is not runtime-checkable (it's a build-time
    # static type contract). We mirror the shape locally to assert that a
    # plausible adapter satisfies its public surface via isinstance.
    @runtime_checkable
    class _IRAdapterMirror(Protocol):
        provider: str

        def emit(self, source_dir: Path) -> Iterator[IRObject]: ...

    class _DummyAdapter:
        provider = "dummy"

        def emit(self, source_dir: Path) -> Iterator[IRObject]:
            yield IRRegister(
                register_id=1,
                provider=self.provider,
                slug="x",
                name="X",
                description=None,
                purpose=None,
            )

    adapter = _DummyAdapter()
    assert isinstance(adapter, _IRAdapterMirror)
    # Pass a real Path so the call shape matches the protocol contract;
    # `_DummyAdapter.emit` ignores `source_dir` but a future tightening
    # that starts reading from it wouldn't surprise this test.
    from pathlib import Path as _Path

    objs = list(adapter.emit(_Path()))
    assert len(objs) == 1
    assert isinstance(objs[0], IRRegister)


def test_irobject_union_covers_every_ir_class() -> None:
    """IRObject must be a union of every Pydantic IR class — drift guard."""
    # `IRObject` is a `types.UnionType` (PEP 604); its `__args__` is the tuple
    # of union members.
    members = set(IRObject.__args__)  # type: ignore[attr-defined]
    expected = {cls for cls, _ in _IR_FACTORIES.values()}
    assert members == expected, (
        f"IRObject drift; missing={expected - members} extra={members - expected}"
    )


def test_rotate_db_to_prev_renames_existing_file(tmp_path: Path) -> None:
    db = tmp_path / "reg_meta.db"
    db.write_bytes(b"current generation")

    rotate_db_to_prev(db)

    assert not db.exists()
    assert (tmp_path / "reg_meta.db.prev").read_bytes() == b"current generation"


def test_rotate_db_to_prev_evicts_older_prev(tmp_path: Path) -> None:
    """A second rotation overwrites the previous `.prev` — single-generation."""
    db = tmp_path / "reg_meta.db"
    prev = tmp_path / "reg_meta.db.prev"
    db.write_bytes(b"gen-2")
    prev.write_bytes(b"gen-1-old")

    rotate_db_to_prev(db)

    assert not db.exists()
    assert prev.read_bytes() == b"gen-2"


def test_rotate_db_to_prev_no_op_when_missing(tmp_path: Path) -> None:
    """First-ever build: nothing to rotate, no error."""
    rotate_db_to_prev(tmp_path / "does_not_exist.db")
    assert not (tmp_path / "does_not_exist.db.prev").exists()


def test_create_empty_provenance_db_schema(tmp_path: Path) -> None:
    path = tmp_path / PROVENANCE_DB_FILENAME
    create_empty_provenance_db(path)

    assert path.exists()
    conn = sqlite3.connect(path)
    try:
        # build_manifest table exists with the four expected columns.
        cols = {
            row[1]: row[2]  # name → declared type
            for row in conn.execute("PRAGMA table_info(build_manifest)")
        }
        assert cols == {
            "schema_version": "TEXT",
            "universal_db_path": "TEXT",
            "universal_db_sha256": "TEXT",
            "build_date": "TEXT",
        }
        # A4.2 added the population tables. All present, all empty in the
        # create-empty helper (write_provenance_db is the populating variant).
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert tables == {
            "build_manifest",
            "scb_register_id_map",
            "adapter_warning",
            "delivery_approval",
        }
        for table in tables:
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
            assert count == 0, f"{table} should be empty in create_empty"
    finally:
        conn.close()


def test_create_empty_provenance_db_refuses_to_overwrite(tmp_path: Path) -> None:
    """The helper refuses to clobber an existing file — caller must rotate first."""
    path = tmp_path / PROVENANCE_DB_FILENAME
    create_empty_provenance_db(path)
    # RegMetaError is a dataclass-based Exception with an empty str(); the
    # discriminator is the `.code` field.
    with pytest.raises(RegMetaError) as excinfo:
        create_empty_provenance_db(path)
    assert excinfo.value.code == "provenance_db_exists"
