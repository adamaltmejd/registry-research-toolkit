"""Tests for the same_as candidate generator (#417; `variable_same_as.py`).

The curated same_as loader/materializer moved to `relations.py` (#522) and is
tested in `test_relations.py`; only the generator stays here. `TestGenerator`
exercises `infer_same_as_candidates` over a synthetic DB: a Tier-1 pair (shared
classification + value set + name), a Tier-4 pair (shared >=15-code
classification-NULL value set), a within-register pair is NOT emitted, an
already-edged pair is excluded, hub-suppression, and `render_candidates_toml`
re-parsing through the relations loader (the generator's `[[edge]]
type = "same_as"` output is exactly the curated surface's input).

Fully synthetic (CLAUDE.md): builds its own DBs (tmp_path) and never reads the
shipped curation TOMLs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import (
    add_register,
    add_state,
    add_value_set,
    add_variable,
    build_slugged_db,
)
from reg_meta_build.relations import load_relations
from reg_meta_build.variable_same_as import (
    infer_same_as_candidates,
    render_candidates_toml,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


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
        # The generator emits the `[[edge]] type = "same_as"` shape — exactly the
        # curated relations loader's input — so a confirmed candidate copies into
        # relations.toml verbatim. Round-trip through `load_relations`.
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
        reparsed = load_relations(path).same_as
        # Every emitted candidate re-parses as a curated entry, same unordered set.
        emitted = {frozenset({c.a_fqid, c.b_fqid}) for c in cands}
        roundtripped = {frozenset({e.a_fqid(), e.b_fqid()}) for e in reparsed}
        assert emitted == roundtripped
        assert len(reparsed) == len(cands)
