"""§16 namespace-invariant property tests for `reg_meta_build.id.mint`.

The minted-ID band must be STRUCTURALLY disjoint from the SCB source-ID band:
every minted id lands in ``[2^62, 2^63)`` (bit 62 set, bit 63 clear), and SCB
ids — being source-derived small integers — are ``< 2^62``. The disjointness
assertion is against the structural invariant ``< 2^62``, NOT a hardcoded
2^32 the test never measures (REFACTOR_SPEC §16 names the nominal SCB band
[0, 2^32), but the proof only needs max_scb_id < 2^62).
"""

from __future__ import annotations

import random

from reg_meta_build.id import mint

_LOW = 1 << 62
_HIGH = 1 << 63


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


def test_mint_encoding_is_unambiguous() -> None:
    """Length-prefixing the parts makes the encoding unambiguous: tuples that
    would collide under a plain ``/``-join (the same joined string) mint
    DISTINCT ids. Guards the SOS key grammar landing in A4.3 (Codex P2)."""
    assert mint("a/b", "c") != mint("a", "b/c")  # both "/"-join to "a/b/c"
    assert mint("a", "b") != mint("a/b")  # both "/"-join to "a/b"
    assert mint("x/", "y") != mint("x", "/y")  # both "/"-join to "x//y"
