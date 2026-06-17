"""Tests for the curated identity loader + candidate generator (#417;
`variable_same_as.py`).

Three layers, mirroring `test_variable_related_to.py`:
  - `TestLoader` — the curated loader's good parse + every load-time failure
    mode (FQID arity, self-edge, duplicate unordered pair, note shape). No
    relation_kind: same_as has no kind vocabulary.
  - `TestMaterialize` — DB-backed (`_slugged_db`): a curated cross-register edge
    materializes BOTH directions; an unknown provider/register fails fast; an
    out-of-build provider is skipped; a curated edge that closes a cycle with an
    inline edge is rejected by the SHARED `_reject_same_as_cycles`.
  - `TestGenerator` — `infer_same_as_candidates` over a synthetic DB: a Tier-1
    pair (shared classification + value set + name), a Tier-4 pair (shared
    >=15-code classification-NULL value set), a within-register pair is NOT
    emitted, an already-edged pair is excluded, and `render_candidates_toml`
    re-parses via `load_same_as`.

Fully synthetic (CLAUDE.md): builds its own TOMLs/DBs (tmp_path) and never reads
the shipped `variable_same_as.toml`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_register,
    add_state,
    add_value_set,
    add_variable,
    build_slugged_db,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.variable_same_as import (
    CuratedSameAs,
    infer_same_as_candidates,
    load_same_as,
    render_candidates_toml,
)

from reg_meta_build.fqid_slugs import materialize_same_as_edges

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

_SCB = frozenset({"scb"})


class TestLoader:
    @staticmethod
    def _load(tmp_path: Path, text: str) -> tuple[CuratedSameAs, ...]:
        path = tmp_path / "variable_same_as.toml"
        path.write_text(text, encoding="utf-8")
        return load_same_as(path)

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_same_as(None) == ()
        assert load_same_as(tmp_path / "absent.toml") == ()

    def test_present_but_empty_file_is_empty(self, tmp_path: Path) -> None:
        assert self._load(tmp_path, "# no edges yet\n") == ()

    def test_parses_valid_edge(self, tmp_path: Path) -> None:
        edges = self._load(
            tmp_path,
            """
            [[same_as]]
            a = "scb/lisa/inkomst"
            b = "scb/rams/inkomst"
            note = "candidate:tier1"
            """,
        )
        assert len(edges) == 1
        e = edges[0]
        assert (e.a_provider, e.a_register, e.a_variable) == ("scb", "lisa", "inkomst")
        assert (e.b_provider, e.b_register, e.b_variable) == ("scb", "rams", "inkomst")
        assert e.note == "candidate:tier1"

    def test_note_is_optional(self, tmp_path: Path) -> None:
        edges = self._load(
            tmp_path,
            '[[same_as]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n',
        )
        assert edges[0].note is None

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "[[same_as]]\na = = 1\n")
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_same_as_toml_unreadable"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[same_ases]]` (typo) would silently disable curation → loud error.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[same_ases]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_same_as_invalid"
        assert "same_ases" in exc.value.message

    def test_scalar_same_as_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "same_as = 5\n")
        assert exc.value.code == "variable_same_as_invalid"

    @pytest.mark.parametrize(
        "fqid",
        [
            "scb",  # 1-segment
            "scb/lisa",  # 2-segment
            "scb/lisa/x/y",  # 4-segment
            "scb//x",  # empty middle segment
            "",  # empty
        ],
    )
    def test_bad_fqid_arity_rejected(self, tmp_path: Path, fqid: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                f'[[same_as]]\na = "{fqid}"\nb = "scb/rams/y"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_same_as_invalid"

    def test_self_edge_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[same_as]]\na = "scb/lisa/x"\nb = "scb/lisa/x"\n',
            )
        assert exc.value.code == "variable_same_as_invalid"

    def test_duplicate_unordered_pair_rejected(self, tmp_path: Path) -> None:
        # The same pair in REVERSED a/b order is still a duplicate (symmetric).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[same_as]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n\n'
                '[[same_as]]\na = "scb/rams/y"\nb = "scb/lisa/x"\n',
            )
        assert exc.value.code == "variable_same_as_invalid"

    def test_empty_note_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[same_as]]\na = "scb/lisa/x"\nb = "scb/rams/y"\nnote = ""\n',
            )
        assert exc.value.code == "variable_same_as_invalid"


# ---------------------------------------------------------------------------
# Materialize (curated edges merged into materialize_same_as_edges)
# ---------------------------------------------------------------------------


def _slug_dir(tmp_path: Path) -> Path:
    """An empty-but-scannable slug dir (no inline same_as) so the curated path is
    the only edge source. `load_slug_dir` scans the whole directory."""
    (tmp_path / "scb.toml").write_text("", encoding="utf-8")
    (tmp_path / "classifications.toml").write_text("", encoding="utf-8")
    return tmp_path


def _cross_register_db() -> sqlite3.Connection:
    """scb/lisa/<v1> + scb/rams/<v2>, both resolvable, no edges yet."""
    conn = build_slugged_db(classification=None)  # scb/lisa with `kon`
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=1, var_id=900, name="Inkomst", slug="inkomst")
    add_variable(conn, register_id=2, var_id=901, name="Inkomst", slug="rinkomst")
    conn.commit()
    return conn


def _edge(
    a: str = "scb/lisa/inkomst",
    b: str = "scb/rams/rinkomst",
    *,
    note: str | None = "candidate:tier1",
) -> CuratedSameAs:
    pa = a.split("/")
    pb = b.split("/")
    return CuratedSameAs(
        a_provider=pa[0],
        a_register=pa[1],
        a_variable=pa[2],
        b_provider=pb[0],
        b_register=pb[1],
        b_variable=pb[2],
        note=note,
    )


def _same_as_rows(conn: sqlite3.Connection) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT a_provider, a_register, a_variable, b_provider, b_register, "
            "b_variable FROM variable_same_as ORDER BY a_register, b_register"
        )
    ]


class TestMaterialize:
    def test_curated_cross_register_edge_writes_both_directions(
        self, tmp_path: Path
    ) -> None:
        conn = _cross_register_db()
        counts = materialize_same_as_edges(
            conn, _slug_dir(tmp_path), curated_same_as=(_edge(),), providers=_SCB
        )
        # One curated edge, no inline edges; both directions land.
        assert counts == {"variable": 1, "variable_curated": 1, "classification": 0}
        assert _same_as_rows(conn) == [
            ("scb", "lisa", "inkomst", "scb", "rams", "rinkomst"),
            ("scb", "rams", "rinkomst", "scb", "lisa", "inkomst"),
        ]

    def test_unknown_register_fails_fast(self, tmp_path: Path) -> None:
        conn = _cross_register_db()
        edge = _edge(b="scb/nonexistent/x")
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(
                conn, _slug_dir(tmp_path), curated_same_as=(edge,), providers=_SCB
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_same_as_unknown_register"
        assert "nonexistent" in exc.value.message
        assert _same_as_rows(conn) == []  # nothing written

    def test_unknown_a_register_fails_fast(self, tmp_path: Path) -> None:
        # Symmetric to the b case — guards against dropping the `a` check.
        conn = _cross_register_db()
        edge = _edge(a="scb/nope/x")
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(
                conn, _slug_dir(tmp_path), curated_same_as=(edge,), providers=_SCB
            )
        assert exc.value.code == "variable_same_as_unknown_register"
        assert "nope" in exc.value.message

    def test_variable_slug_not_validated(self, tmp_path: Path) -> None:
        # same_as is slug-anchored — a not-yet-present variable slug survives
        # (the link follows renames). Provider+register exist, so it writes.
        conn = _cross_register_db()
        edge = _edge(b="scb/rams/renamed-tomorrow")
        counts = materialize_same_as_edges(
            conn, _slug_dir(tmp_path), curated_same_as=(edge,), providers=_SCB
        )
        assert counts["variable_curated"] == 1
        assert ("scb", "rams", "renamed-tomorrow") in {
            (r[3], r[4], r[5]) for r in _same_as_rows(conn)
        }

    def test_out_of_build_provider_is_skipped(self, tmp_path: Path) -> None:
        conn = _cross_register_db()
        # scb endpoints, but this build only carries sos → skip, don't fail.
        counts = materialize_same_as_edges(
            conn,
            _slug_dir(tmp_path),
            curated_same_as=(_edge(),),
            providers=frozenset({"sos"}),
        )
        assert counts["variable_curated"] == 0
        assert _same_as_rows(conn) == []

    def test_curated_edge_closing_cycle_with_inline_is_rejected(
        self, tmp_path: Path
    ) -> None:
        # Inline edge inkomst → rinkomst; curated edge rinkomst → inkomst closes a
        # directed 2-cycle. The SHARED cycle check (curated merged before it) must
        # reject it, exactly as it rejects a reciprocal inline declaration.
        conn = _cross_register_db()
        (tmp_path / "scb.toml").write_text(
            '[variable."1.900"]\n'
            'same_as = [{ provider = "scb", register = "rams", '
            'variable_slug = "rinkomst" }]\n',
            encoding="utf-8",
        )
        (tmp_path / "classifications.toml").write_text("", encoding="utf-8")
        # Curated edge in the reverse direction.
        curated = (_edge(a="scb/rams/rinkomst", b="scb/lisa/inkomst"),)
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as_edges(
                conn, tmp_path, curated_same_as=curated, providers=_SCB
            )
        assert exc.value.code == "slug_same_as_cycle"


# ---------------------------------------------------------------------------
# Generator (infer_same_as_candidates / render_candidates_toml)
# ---------------------------------------------------------------------------


def _big_codes(prefix: str, n: int = 20) -> list[tuple[str, str]]:
    """`n` distinct (code, label) pairs (>= the default 15-code floor so the value
    set corroborates a pair at any tier). `prefix` keeps the codes globally unique
    — `value_code` has a UNIQUE(code, label) constraint, so two value sets sharing
    code text would collide at insert."""
    return [(f"{prefix}-{i}", f"{prefix}-label-{i}") for i in range(n)]


def _generator_db() -> sqlite3.Connection:
    """Synthetic DB exercising the generator's tiers + the value-set floor:

    - scb/lisa/diag + scb/par/pdiag: SAME name, share classification 1 AND a
      >=15-code value set 15 → Tier 1.
    - scb/lisa/munic + scb/par/region: DIFFERENT names, share a 20-code
      classification-NULL value set 20 → Tier 4.
    - scb/lisa/diag + scb/lisa/bidiag: WITHIN-register share → must NOT emit.
    - scb/lisa/civ + scb/par/pciv: SAME name, share classification 1 but their
      only shared value set is SMALL (value set 10, 2 codes) → Tier 2 (the small
      set must NOT lift it to Tier 1).
    """
    conn = build_slugged_db(classification=None)
    add_register(conn, register_id=2, slug="par", name="PAR")
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (1, 'ICD-10-SE', 'ICD-10-SE', 'icd-10-se')"
    )
    # Value sets: 10 (small, 2 codes), 15 + 20 (>=15 codes, distinct code text).
    add_value_set(conn, value_set_id=10, codes=[("A", "a"), ("B", "b")])
    add_value_set(conn, value_set_id=15, codes=_big_codes("vs15"))
    add_value_set(conn, value_set_id=20, codes=_big_codes("vs20"))

    # Tier-1 pair: same name "Diagnos", shared classification 1 + BIG value set 15.
    add_variable(conn, register_id=1, var_id=800, name="Diagnos", slug="diag")
    add_variable(conn, register_id=2, var_id=801, name="Diagnos", slug="pdiag")
    # PAR has no variant in the fixture; reuse variant 10 (FK is unenforced in the
    # in-memory fixture and the generator never joins variant).
    for reg, slug in ((1, "diag"), (2, "pdiag")):
        add_state(
            conn,
            register_id=reg,
            variable_slug=slug,
            register_variant_id=10,
            value_set_id=15,
            classification_id=1,
        )

    # Within-register sibling sharing big value set 15 (must NOT be emitted).
    add_variable(conn, register_id=1, var_id=802, name="Bidiagnos", slug="bidiag")
    add_state(
        conn,
        register_id=1,
        variable_slug="bidiag",
        register_variant_id=10,
        value_set_id=15,
        classification_id=1,
    )

    # Re-tier pair: same name "Civilstand", shared classification 1, only shared
    # value set is the SMALL one (10) → must score Tier 2, not Tier 1.
    add_variable(conn, register_id=1, var_id=810, name="Civilstand", slug="civ")
    add_variable(conn, register_id=2, var_id=811, name="Civilstand", slug="pciv")
    for reg, slug in ((1, "civ"), (2, "pciv")):
        add_state(
            conn,
            register_id=reg,
            variable_slug=slug,
            register_variant_id=10,
            value_set_id=10,
            classification_id=1,
        )

    # Tier-4 pair: different names, share classification-NULL value set 20.
    add_variable(conn, register_id=1, var_id=900, name="Kommun", slug="munic")
    add_variable(conn, register_id=2, var_id=901, name="Region", slug="region")
    for reg, slug in ((1, "munic"), (2, "region")):
        add_state(
            conn,
            register_id=reg,
            variable_slug=slug,
            register_variant_id=10,
            value_set_id=20,
            classification_id=None,
        )

    conn.commit()
    return conn


def _hub_db(n_registers: int, *, name_agree: bool) -> sqlite3.Connection:
    """A DB where one classification (id 1) AND one >=15-code value set (id 50)
    are carried across `n_registers` registers — a high-fanout HUB. Each register
    has one variable bound to both, so every cross-register pair scores Tier 3
    (shared class + value set) when names DISAGREE, or Tier 1 when they agree.
    The shared value set means a name-disagreeing pair still scores a tier
    uncapped, so the hub cap's suppression is observable (not masked by a None
    score). When `name_agree` is True every variable shares the name "Kon".

    Returns a DB whose only cross-register pairs come from the hub signals, so the
    fanout cap's effect is isolated."""
    conn = build_slugged_db(classification=None, variable=None, version=None)
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (1, 'HUBCLASS', 'Hub', 'hubclass')"
    )
    add_value_set(conn, value_set_id=50, codes=_big_codes("vs50"))
    for r in range(2, 2 + n_registers):
        add_register(conn, register_id=r, slug=f"reg{r}", name=f"REG{r}")
        # Each register needs a variant for add_state's variant FK arg; reuse a
        # per-register id (unenforced in-memory). Use variant id = r.
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, name) "
            "VALUES (?, ?, ?, ?)",
            (r, r, f"v{r}", f"V{r}"),
        )
        name = "Kon" if name_agree else f"Var{r}"
        add_variable(conn, register_id=r, var_id=700 + r, name=name, slug=f"kon{r}")
        add_state(
            conn,
            register_id=r,
            variable_slug=f"kon{r}",
            register_variant_id=r,
            value_set_id=50,
            classification_id=1,
        )
    conn.commit()
    return conn


class TestGenerator:
    def test_tier1_and_tier4_pairs_emitted(self) -> None:
        conn = _generator_db()
        cands = infer_same_as_candidates(conn).candidates
        pairs = {(c.a_fqid, c.b_fqid): c.tier for c in cands}

        # Tier 1: diag pair (shared class + BIG value set + name).
        assert pairs.get(("scb/lisa/diag", "scb/par/pdiag")) == 1
        # Tier 4: munic/region pair (shared >=15-code classification-NULL set).
        assert pairs.get(("scb/lisa/munic", "scb/par/region")) == 4

    def test_small_value_set_does_not_lift_tier(self) -> None:
        # The civ pair shares classification 1 + name, but its only shared value
        # set (10) is below the floor → Tier 2, NOT Tier 1. No pair lost, re-tiered.
        conn = _generator_db()
        cands = infer_same_as_candidates(conn).candidates
        pairs = {(c.a_fqid, c.b_fqid): c.tier for c in cands}
        assert pairs.get(("scb/lisa/civ", "scb/par/pciv")) == 2

    def test_within_register_pair_not_emitted(self) -> None:
        conn = _generator_db()
        cands = infer_same_as_candidates(conn).candidates
        pairs = {(c.a_fqid, c.b_fqid) for c in cands}
        # diag + bidiag share value set 15 but live in the SAME register.
        assert ("scb/lisa/bidiag", "scb/lisa/diag") not in pairs
        assert ("scb/lisa/diag", "scb/lisa/bidiag") not in pairs

    def test_already_edged_pair_excluded(self) -> None:
        conn = _generator_db()
        # Pre-seed the Tier-1 pair as an existing edge → must be excluded.
        conn.execute(
            "INSERT INTO variable_same_as (a_provider, a_register, a_variable, "
            "b_provider, b_register, b_variable) VALUES "
            "('scb', 'lisa', 'diag', 'scb', 'par', 'pdiag')"
        )
        conn.commit()
        cands = infer_same_as_candidates(conn).candidates
        pairs = {(c.a_fqid, c.b_fqid) for c in cands}
        assert ("scb/lisa/diag", "scb/par/pdiag") not in pairs

    def test_max_tier_caps_emission(self) -> None:
        conn = _generator_db()
        cands = infer_same_as_candidates(conn, max_tier=1).candidates
        assert all(c.tier == 1 for c in cands)
        assert any(c.a_fqid == "scb/lisa/diag" for c in cands)

    def test_min_value_set_codes_excludes_small_tier4(self) -> None:
        conn = _generator_db()
        # Raise the floor above the 20-code set → the Tier-4 pair drops out.
        cands = infer_same_as_candidates(conn, min_value_set_codes=100).candidates
        pairs = {(c.a_fqid, c.b_fqid) for c in cands}
        assert ("scb/lisa/munic", "scb/par/region") not in pairs

    # --- Hub suppression -----------------------------------------------------

    def test_hub_pair_without_name_agreement_suppressed(self) -> None:
        # 6 registers carry the hub signals (fanout 6); names DISAGREE. With a cap
        # of 5 both signals are hubs and every pair is suppressed (no exemption).
        conn = _hub_db(6, name_agree=False)
        result = infer_same_as_candidates(conn, max_signal_fanout=5)
        assert result.candidates == []
        # Name-disagreeing → C(6,2) = 15 cross-register pairs removed from output.
        assert result.hub_suppressed == 15

    def test_hub_pair_with_name_agreement_kept(self) -> None:
        # Same hub fanout, but names AGREE → the exemption keeps every pair (each
        # scores Tier 1: shared classification + value set + name). Nothing dropped.
        conn = _hub_db(6, name_agree=True)
        result = infer_same_as_candidates(conn, max_signal_fanout=5)
        assert len(result.candidates) == 15  # C(6,2)
        assert all(c.tier == 1 for c in result.candidates)
        assert result.hub_suppressed == 0

    def test_disabled_cap_reincludes_hub_pairs(self) -> None:
        # max_signal_fanout=None (CLI 0) disables the cap → the name-disagreeing
        # hub pairs reappear (each Tier 3: shared class + value set, no name), and
        # nothing is reported as suppressed.
        conn = _hub_db(6, name_agree=False)
        result = infer_same_as_candidates(conn, max_signal_fanout=None)
        assert len(result.candidates) == 15  # C(6,2)
        assert all(c.tier == 3 for c in result.candidates)
        assert result.hub_suppressed == 0

    def test_non_hub_signal_below_cap_not_suppressed(self) -> None:
        # 4 registers (fanout 4) under a cap of 5 → NOT a hub; the name-disagreeing
        # pairs still emit (Tier 3: shared class + value set) and nothing is
        # suppressed (the cap removed no pair because the signal isn't a hub).
        conn = _hub_db(4, name_agree=False)
        result = infer_same_as_candidates(conn, max_signal_fanout=5)
        assert len(result.candidates) == 6  # C(4,2)
        assert all(c.tier == 3 for c in result.candidates)
        assert result.hub_suppressed == 0

    def test_render_roundtrips_through_loader(self, tmp_path: Path) -> None:
        conn = _generator_db()
        result = infer_same_as_candidates(conn)
        cands = result.candidates
        counts_by_tier: dict[int, int] = {}
        for c in cands:
            counts_by_tier[c.tier] = counts_by_tier.get(c.tier, 0) + 1
        toml = render_candidates_toml(
            cands,
            counts_by_tier=counts_by_tier,
            max_signal_fanout=12,
            hub_suppressed=result.hub_suppressed,
        )

        path = tmp_path / "candidates.toml"
        path.write_text(toml, encoding="utf-8")
        reparsed = load_same_as(path)
        # Every emitted candidate re-parses as a curated entry, same unordered set.
        emitted = {frozenset({c.a_fqid, c.b_fqid}) for c in cands}
        roundtripped = {
            frozenset(
                {
                    f"{e.a_provider}/{e.a_register}/{e.a_variable}",
                    f"{e.b_provider}/{e.b_register}/{e.b_variable}",
                }
            )
            for e in reparsed
        }
        assert emitted == roundtripped
        assert len(reparsed) == len(cands)
