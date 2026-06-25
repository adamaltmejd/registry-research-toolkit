"""Tests for the canonical-SCB attach post-pass (#400 PR2; `canonical_attach.py`).

Covers the TOML loader (structural validation, EXIT_CONFIG) and the
materialization pass: minting variable+state+alias onto an existing
(register, variant) with canonical-SCB-banded ids, gap-fill skip, lenient
unresolved, the provider gate, verbatim validity window, the open-ended sentinel
when valid_to is omitted, source_label, and the classification backfill
side-channel (a candidate fed to the shared backfill tags
variable_state.classification_id)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import build_slugged_db
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.canonical_attach import (
    CANONICAL_ATTACH_SOURCE_LABEL,
    _CanonicalAttach,
    canonical_attach_path,
    load_canonical_attach,
    materialize_canonical_attach,
)
from reg_meta_build.db import (
    _backfill_state_classifications,
    _feed_classification_candidates,
)
from reg_meta_build.id import _CANONICAL_SCB_BIT, _MINT_BIT, is_canonical_scb

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

_SCB = frozenset({"scb"})


def _a(
    column: str,
    *,
    register: str = "lisa",
    variant: str = "individer-15plus",
    name: str = "Yrke enligt SSYK 96",
    definition: str = "Yrkeskod på 4-siffernivå.",
    data_type: str = "text",
    valid_from: str = "2010-01-01",
    valid_to: str | None = "2013-12-31",
    classification: str | None = None,
    is_identifier: bool = False,
    is_sensitive: bool = False,
) -> _CanonicalAttach:
    return _CanonicalAttach(
        provider="scb",
        register=register,
        variant=variant,
        column=column,
        name=name,
        definition=definition,
        data_type=data_type,
        valid_from=valid_from,
        valid_to=valid_to,
        classification=classification,
        is_identifier=is_identifier,
        is_sensitive=is_sensitive,
    )


def _variable(conn: sqlite3.Connection, provider_key: str):
    return conn.execute(
        "SELECT variable_id, name, description, source_label, slug FROM variable "
        "WHERE provider_key = ?",
        (provider_key,),
    ).fetchone()


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


_FULL_ENTRY = (
    '[[attach]]\nregister = "scb/lisa"\nvariant = "individer-15plus"\n'
    'column = "Ssyk4_J16"\nname = "Yrke enligt SSYK 96"\n'
    'definition = "Yrkeskod."\ndata_type = "text"\nvalid_from = "2010-01-01"\n'
    'valid_to = "2013-12-31"\nclassification = "SSYK96"\n'
)


# ── loader ───────────────────────────────────────────────────────────────────


class TestLoader:
    def test_none_path_is_empty(self) -> None:
        assert load_canonical_attach(None) == []

    def test_valid_entry_parses(self, tmp_path: Path) -> None:
        toml = _write(tmp_path / "lisa_canonical.toml", _FULL_ENTRY)
        entries = load_canonical_attach(toml)
        assert len(entries) == 1
        a = entries[0]
        assert (a.provider, a.register, a.variant, a.column) == (
            "scb",
            "lisa",
            "individer-15plus",
            "Ssyk4_J16",
        )
        assert a.name == "Yrke enligt SSYK 96"
        assert a.definition == "Yrkeskod."
        assert (a.data_type, a.valid_from, a.valid_to) == (
            "text",
            "2010-01-01",
            "2013-12-31",
        )
        assert a.classification == "SSYK96"

    def test_valid_to_optional_open_ended(self, tmp_path: Path) -> None:
        body = (
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\n'
            'name = "N"\ndefinition = "D"\ndata_type = "integer"\n'
            'valid_from = "2010-01-01"\n'
        )
        entries = load_canonical_attach(_write(tmp_path / "x.toml", body))
        assert entries[0].valid_to is None  # materializer writes the sentinel

    def test_identifier_flags_default_false(self, tmp_path: Path) -> None:
        # Omitted → False (DDL default 0). Guards against an attached row silently
        # downgrading a sibling's identifier/sensitive flag.
        entries = load_canonical_attach(_write(tmp_path / "x.toml", _FULL_ENTRY))
        assert (entries[0].is_identifier, entries[0].is_sensitive) == (False, False)

    def test_identifier_flags_parse(self, tmp_path: Path) -> None:
        body = (
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "PeOrgNrSregJ"\n'
            'name = "Org.nr"\ndefinition = "D"\ndata_type = "text"\n'
            'valid_from = "2003-01-01"\nis_identifier = true\nis_sensitive = false\n'
        )
        entries = load_canonical_attach(_write(tmp_path / "x.toml", body))
        assert (entries[0].is_identifier, entries[0].is_sensitive) == (True, False)

    @pytest.mark.parametrize("field", ["is_identifier", "is_sensitive"])
    def test_non_bool_identifier_flag_fails(self, tmp_path: Path, field: str) -> None:
        # A string ("true") must NOT silently coerce — bool("false") is True, which
        # would flip a PII guardrail. Demand a real TOML boolean.
        body = (
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2010-01-01"\n'
            f'{field} = "true"\n'
        )
        with pytest.raises(RegMetaError) as exc:
            load_canonical_attach(_write(tmp_path / "x.toml", body))
        assert exc.value.exit_code == EXIT_CONFIG

    def test_required_keys(self, tmp_path: Path) -> None:
        # Drop each required key in turn and assert the load fails EXIT_CONFIG.
        base = {
            "register": '"scb/lisa"',
            "variant": '"v"',
            "column": '"C"',
            "name": '"N"',
            "definition": '"D"',
            "data_type": '"text"',
            "valid_from": '"2010-01-01"',
        }
        for missing in base:
            lines = "\n".join(f"{k} = {v}" for k, v in base.items() if k != missing)
            toml = _write(tmp_path / "m.toml", f"[[attach]]\n{lines}\n")
            with pytest.raises(RegMetaError) as exc:
                load_canonical_attach(toml)
            assert exc.value.exit_code == EXIT_CONFIG, missing

    @pytest.mark.parametrize(
        "body",
        [
            # bad (1-segment) register FQID
            '[[attach]]\nregister = "lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2010-01-01"\n',
            # unknown key
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2010-01-01"\n'
            'value_set = "X"\n',
            # bad data_type
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "txt"\nvalid_from = "2010-01-01"\n',
            # malformed ISO valid_from
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2010-1-1"\n',
            # calendar-impossible date
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2010-13-01"\n',
            # valid_from after valid_to
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2013-01-01"\n'
            'valid_to = "2010-12-31"\n',
            # unknown top-level table
            '[[attaches]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\n',
        ],
    )
    def test_malformed_fails(self, tmp_path: Path, body: str) -> None:
        toml = _write(tmp_path / "lisa_canonical.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_canonical_attach(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_non_scb_register_fails(self, tmp_path: Path) -> None:
        # The pass is canonical-SCB-only: the materializer mints every id via
        # mint_canonical_scb("scb", …) and stamps source_label="canonical-scb"
        # unconditionally, so a non-scb provider would mint into the SCB namespace
        # and mislabel the row. Reject it at load time.
        body = (
            '[[attach]]\nregister = "fohm/sminet"\nvariant = "v"\ncolumn = "C"\n'
            'name = "N"\ndefinition = "D"\ndata_type = "text"\n'
            'valid_from = "2010-01-01"\n'
        )
        toml = _write(tmp_path / "lisa_canonical.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_canonical_attach(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "scb/..." in exc.value.message

    def test_undeclared_classification_fails(self, tmp_path: Path) -> None:
        body = (
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "C"\nname = "N"\n'
            'definition = "D"\ndata_type = "text"\nvalid_from = "2010-01-01"\n'
            'classification = "NOT-A-REAL-CLASSIFICATION"\n'
        )
        with pytest.raises(RegMetaError) as exc:
            load_canonical_attach(_write(tmp_path / "x.toml", body))
        assert exc.value.exit_code == EXIT_CONFIG

    def test_declared_classification_passes(self, tmp_path: Path) -> None:
        # SSYK96 is a declared catalog classification (classifications.toml).
        entries = load_canonical_attach(_write(tmp_path / "x.toml", _FULL_ENTRY))
        assert entries[0].classification == "SSYK96"

    def test_duplicate_triple_fails(self, tmp_path: Path) -> None:
        body = (
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "Col"\n'
            'name = "A"\ndefinition = "D"\ndata_type = "text"\n'
            'valid_from = "2010-01-01"\n\n'
            '[[attach]]\nregister = "scb/lisa"\nvariant = "v"\ncolumn = "col"\n'
            'name = "B"\ndefinition = "D"\ndata_type = "text"\n'
            'valid_from = "2010-01-01"\n'  # case-folds to the same column
        )
        with pytest.raises(RegMetaError) as exc:
            load_canonical_attach(_write(tmp_path / "x.toml", body))
        assert exc.value.exit_code == EXIT_CONFIG


# ── seed-path resolution (stale-seed fail-loud) ──────────────────────────────────


class TestSeedPath:
    def test_none_dir_is_noop(self) -> None:
        # No canonical-SCB adapter at all (synthetic / SCB-only / SOS-only build) →
        # legitimately no canonical-attach seed; None, no error.
        assert canonical_attach_path(None) is None

    def test_present_seed_resolves(self, tmp_path: Path) -> None:
        (tmp_path / "lisa_canonical.toml").write_text(_FULL_ENTRY, encoding="utf-8")
        assert canonical_attach_path(tmp_path) == tmp_path / "lisa_canonical.toml"

    def test_present_dir_missing_seed_fails_loud(self, tmp_path: Path) -> None:
        # The canonical-SCB adapter IS active (dir present) but the committed
        # lisa_canonical.toml is absent → a stale --input-dir checkout. Mirror the
        # #556 scb_canonical_seed_missing discipline: fail EXIT_CONFIG, don't
        # silently mint 0 attaches and omit the 32 documented LISA variables.
        with pytest.raises(RegMetaError) as exc:
            canonical_attach_path(tmp_path)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "canonical_attach_seed_missing"


# ── materialize ────────────────────────────────────────────────────────────────


class TestMaterialize:
    def test_mints_variable_state_alias_banded(self) -> None:
        conn = build_slugged_db()  # scb/lisa/individer-15plus (variant 10), var Kön
        candidates: list[tuple[int, int | None, str]] = []
        counts = materialize_canonical_attach(
            conn,
            [_a("Ssyk4_J16", valid_from="2010-01-01", valid_to="2013-12-31")],
            providers=_SCB,
            classification_candidates=candidates,
        )
        assert counts == {"minted": 1, "skipped": 0, "unresolved": 0}
        row = _variable(conn, "Ssyk4_J16")
        assert row is not None
        vid, name, desc, source_label, slug = row
        assert name == "Yrke enligt SSYK 96"
        assert desc == "Yrkeskod på 4-siffernivå."
        assert source_label == CANONICAL_ATTACH_SOURCE_LABEL
        assert slug is None  # populate_variable_slugs auto-derives it later
        # Canonical-SCB sub-band [2^61, 2^62), NOT a graft's MAX+1 sequence.
        assert is_canonical_scb(vid)
        assert _CANONICAL_SCB_BIT <= vid < _MINT_BIT
        state = conn.execute(
            "SELECT state_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name FROM variable_state WHERE variable_id = ?",
            (vid,),
        ).fetchone()
        state_id, rvid, vfrom, vto, dtype, col = state
        assert is_canonical_scb(state_id)
        # Window lands verbatim (closed era).
        assert (rvid, vfrom, vto, dtype, col) == (
            10,
            "2010-01-01",
            "2013-12-31",
            "text",
            "Ssyk4_J16",
        )
        assert conn.execute(
            "SELECT 1 FROM variable_alias WHERE variable_id = ? "
            "AND delivery_column_name = ?",
            (vid, "Ssyk4_J16"),
        ).fetchone()
        assert candidates == []  # no classification on this entry

    def test_identifier_flags_propagate_to_variable(self) -> None:
        # PII guard: an identifier attach (PeOrgNrSregJ analog) must land
        # is_identifier=1 on the minted variable row — NOT the DDL default 0.
        conn = build_slugged_db()
        materialize_canonical_attach(
            conn,
            [_a("PeOrgNrSregJ", is_identifier=True, is_sensitive=False)],
            providers=_SCB,
            classification_candidates=[],
        )
        vid = _variable(conn, "PeOrgNrSregJ")[0]
        ident, sens = conn.execute(
            "SELECT is_identifier, is_sensitive FROM variable WHERE variable_id = ?",
            (vid,),
        ).fetchone()
        assert (ident, sens) == (1, 0)

    def test_default_flags_land_zero(self) -> None:
        # A non-identifier attach (no flags) lands the DDL default 0/0.
        conn = build_slugged_db()
        materialize_canonical_attach(
            conn, [_a("Ssyk4_J16")], providers=_SCB, classification_candidates=[]
        )
        vid = _variable(conn, "Ssyk4_J16")[0]
        ident, sens = conn.execute(
            "SELECT is_identifier, is_sensitive FROM variable WHERE variable_id = ?",
            (vid,),
        ).fetchone()
        assert (ident, sens) == (0, 0)

    def test_open_ended_sentinel_when_valid_to_omitted(self) -> None:
        conn = build_slugged_db()
        materialize_canonical_attach(
            conn,
            [_a("Sni2007", valid_to=None)],
            providers=_SCB,
            classification_candidates=[],
        )
        vto = conn.execute(
            "SELECT valid_to FROM variable_state WHERE delivery_column_name = 'Sni2007'"
        ).fetchone()[0]
        assert vto == "9999-12-31"

    def test_deterministic_ids(self) -> None:
        # Same (register, variant, column) → same minted ids on a fresh build.
        ids = []
        for _ in range(2):
            conn = build_slugged_db()
            materialize_canonical_attach(
                conn, [_a("Ssyk4_J16")], providers=_SCB, classification_candidates=[]
            )
            ids.append(_variable(conn, "Ssyk4_J16")[0])
        assert ids[0] == ids[1]

    def test_gap_fill_skips_existing_column(self) -> None:
        conn = build_slugged_db()  # Kön already delivers column 'Kon' in variant 10
        counts = materialize_canonical_attach(
            conn, [_a("Kon")], providers=_SCB, classification_candidates=[]
        )
        assert counts == {"minted": 0, "skipped": 1, "unresolved": 0}

    def test_gap_fill_skip_is_case_insensitive(self) -> None:
        conn = build_slugged_db()
        counts = materialize_canonical_attach(
            conn, [_a("kON")], providers=_SCB, classification_candidates=[]
        )
        assert counts["skipped"] == 1 and counts["minted"] == 0

    def test_unresolved_variant_counted(self) -> None:
        conn = build_slugged_db()
        warnings: list[str] = []
        counts = materialize_canonical_attach(
            conn,
            [_a("X", variant="no-such-variant")],
            providers=_SCB,
            classification_candidates=[],
            warn=warnings.append,
        )
        assert counts == {"minted": 0, "skipped": 0, "unresolved": 1}
        assert any("did not resolve" in w for w in warnings)

    def test_provider_gate_skips_inactive(self) -> None:
        conn = build_slugged_db()
        counts = materialize_canonical_attach(
            conn,
            [_a("X")],
            providers=frozenset({"sos"}),
            classification_candidates=[],
        )
        assert counts == {"minted": 0, "skipped": 0, "unresolved": 0}
        assert _variable(conn, "X") is None

    def test_classification_tagged_after_backfill(self) -> None:
        # The default fixture seeds a SUN2020 classification row. An attach naming
        # it appends a candidate; the shared feed + backfill (the SAME db.py passes
        # `materialize` runs) must tag the minted state's classification_id.
        conn = build_slugged_db()
        candidates: list[tuple[int, int | None, str]] = []
        materialize_canonical_attach(
            conn,
            [_a("Sun2020Niva", classification="SUN2020")],
            providers=_SCB,
            classification_candidates=candidates,
        )
        vid = _variable(conn, "Sun2020Niva")[0]
        assert candidates == [(vid, None, "SUN2020")]
        # Run the shared backfill side-channel exactly as `materialize` does.
        _feed_classification_candidates(conn, candidates)
        _backfill_state_classifications(conn)
        cls_id = conn.execute(
            "SELECT classification_id FROM variable_state WHERE variable_id = ?",
            (vid,),
        ).fetchone()[0]
        expected = conn.execute(
            "SELECT id FROM classification WHERE short_name = 'SUN2020'"
        ).fetchone()[0]
        assert cls_id == expected
