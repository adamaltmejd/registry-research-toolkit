"""Namespace-invariant property tests for `reg_meta_build.id.mint`.

The minted-ID band must be STRUCTURALLY disjoint from the SCB source-ID band:
every minted id lands in ``[2^62, 2^63)`` (bit 62 set, bit 63 clear), and SCB
ids — being source-derived small integers — are ``< 2^62``. The disjointness
assertion is against the structural invariant ``< 2^62``, NOT a hardcoded
2^32 the test never measures (DESIGN.md → Deterministic ID minting names the nominal SCB band
[0, 2^32), but the proof only needs max_scb_id < 2^62).
"""

from __future__ import annotations

import random

from reg_meta_build.id import mint, mint_canonical_scb

_LOW = 1 << 62
_HIGH = 1 << 63
_CANON_LOW = 1 << 61  # canonical-SCB sub-band [2^61, 2^62)


def test_minted_ids_land_in_band() -> None:
    """10k random inputs all mint into [2^62, 2^63) with bit 63 clear."""
    rng = random.Random(20260601)
    for _ in range(10_000):
        n_parts = rng.randint(1, 4)
        parts = [
            "".join(rng.choices("abcdefghijklmnop0123456789-/", k=rng.randint(1, 24)))
            for _ in range(n_parts)
        ]
        r = mint(*parts)
        assert _LOW <= r < _HIGH, f"{parts!r} → {r} out of band"
        assert r & _HIGH == 0, "bit 63 must be clear (fits signed 64-bit INTEGER)"


def test_minted_ids_are_disjoint_from_scb_band() -> None:
    """Structural disjointness: every minted id >= 2^62, and the SCB band is
    < 2^62. The bands cannot overlap regardless of SCB's actual max id."""
    rng = random.Random(1)
    minted = [mint("sos", str(rng.random())) for _ in range(1000)]
    assert min(minted) >= _LOW
    # SCB ids are source-derived small integers; the invariant the proof needs.
    assert _LOW >= (1 << 32)  # the nominal SCB band [0, 2^32) is comfortably below


def test_mint_is_deterministic() -> None:
    """Same input → same id (the regenerate-not-migrate determinism guarantee)."""
    assert mint("sos", "par") == mint("sos", "par")
    assert mint("sos", "par", "deldatamangd") == mint("sos", "par", "deldatamangd")
    # Distinct inputs mint distinct ids (no accidental collapse on common keys).
    assert mint("sos", "par") != mint("sos", "par", "deldatamangd")
    assert mint("sos", "par") != mint("scb", "par")


def test_sos_mint_grammar_tuples_pinned() -> None:
    """A4.3b SOS mint grammar (resolved fork d), pinned so a refactor that
    changes the key tuple is caught (it would silently re-id the whole SOS
    subset and break rebuild stability):
      register = mint("sos", <abbrev>)
      variant  = mint("sos", <abbrev>, <deldatamangd>)  (synth -> "_default")
      variable = mint("sos", <abbrev>, <var.name>)
      split sibling = mint("sos", <abbrev>, <var.name>, <shape discriminator>)
      state    = mint("sos", "state", <variable_id>, <variant_id>, <from>, <label>)
    The four grains are distinct for a shared key, and each lands in the minted
    band [2^62, 2^63)."""
    register = mint("sos", "par")
    variant = mint("sos", "par", "PAR_SV")
    variable = mint("sos", "par", "ATC")
    sibling = mint("sos", "par", "ATC", "heltal:")
    state = mint("sos", "state", str(variable), str(variant), "2001-01-01", "")
    ids = {register, variant, variable, sibling, state}
    assert len(ids) == 5, "the five SOS grains mint distinct ids"
    for v in ids:
        assert _LOW <= v < _HIGH
    # The synthesized-variant token is the literal "_default".
    assert mint("sos", "lss", "_default") != mint("sos", "lss")


def test_canonical_scb_ids_land_in_reserved_sub_band() -> None:
    """Canonical-SCB ids (#444) land in [2^61, 2^62): LOW-band (so they pass the
    SCB-provider band check `id < 2^62`) yet above every real source-derived SCB
    id and disjoint from the minted band [2^62, 2^63)."""
    rng = random.Random(20260618)
    for _ in range(10_000):
        parts = [
            "".join(rng.choices("abcdefghijklmnop0123456789-", k=rng.randint(1, 24)))
            for _ in range(rng.randint(1, 4))
        ]
        c = mint_canonical_scb(*parts)
        assert _CANON_LOW <= c < _LOW, f"{parts!r} → {c} out of the sub-band"
        assert c < _LOW, "must be low-band (< 2^62) to pass the SCB band check"
        assert not (_LOW <= c < _HIGH), "must be disjoint from the minted band"


def test_canonical_scb_disjoint_from_mint_and_deterministic() -> None:
    """Same parts → same canonical-SCB id, and never equal to the high-band
    `mint` of the same parts."""
    assert mint_canonical_scb("scb", "utrikeshandel-tjanster") == mint_canonical_scb(
        "scb", "utrikeshandel-tjanster"
    )
    assert mint_canonical_scb("scb", "x") != mint("scb", "x")
    assert mint_canonical_scb("scb", "a") != mint_canonical_scb("scb", "a", "_default")


def test_mint_encoding_is_unambiguous() -> None:
    """Length-prefixing the parts makes the encoding unambiguous: tuples that
    would collide under a plain ``/``-join (the same joined string) mint
    DISTINCT ids. Guards the SOS key grammar landing in A4.3 (Codex P2)."""
    assert mint("a/b", "c") != mint("a", "b/c")  # both "/"-join to "a/b/c"
    assert mint("a", "b") != mint("a/b")  # both "/"-join to "a/b"
    assert mint("x/", "y") != mint("x", "/y")  # both "/"-join to "x//y"
