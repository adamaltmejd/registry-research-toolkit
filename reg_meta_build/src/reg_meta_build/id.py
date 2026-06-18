"""Deterministic ID minting for provider-synthesized universal IDs.

See DESIGN.md → Deterministic ID minting. Build-side only —
consumed by the SOS adapter and the namespace property test. NOT imported by
`reg_meta` runtime, `reg_monabundle.runtime`, the MONA bundle, or the webapp;
keeping it in `reg_meta_build` respects the build/runtime boundary even though
it is pure stdlib.

The two ID bands are STRUCTURALLY disjoint:

  - SCB IDs are source-derived (``int(RegisterId)`` etc.) and live in the low
    band ``[0, 2^62)`` — they are small SCB integers, far below 2^62.
  - Minted IDs (SOS today, any future provider lacking native int keys) set
    bit 62, so every value lands in ``[2^62, 2^63)``.

Bit 63 stays clear so the value fits a signed 64-bit SQLite INTEGER without
overflow. Disjointness is therefore arithmetic, not a runtime check.
"""

from __future__ import annotations

from hashlib import blake2b

_MINT_BIT = 1 << 62
# Curated CANONICAL-SCB content (#444) — registers/variants/variables/states that
# belong to the `scb` provider but are not in SCB's machine export (e.g.
# Utrikeshandel med tjänster). They MUST stay in the SCB low band (`< 2^62`, the
# band check forbids a high-band scb id), yet be disjoint from real source-derived
# SCB ids. Setting bit 61 (and clearing bit 62) puts them in the reserved sub-band
# ``[2^61, 2^62)``: still low-band, but far above every real SCB id (small
# source integers, all ``< 2^61``). Disjoint from the minted band by construction.
_CANONICAL_SCB_BIT = 1 << 61


def _digest(*parts: str) -> int:
    data = b"".join(
        len(b).to_bytes(4, "big") + b for b in (p.encode("utf-8") for p in parts)
    )
    return int.from_bytes(blake2b(data, digest_size=8, person=b"regmeta-id").digest())


def mint(*parts: str) -> int:
    """Deterministically mint a universal ID in ``[2^62, 2^63)`` from ``parts``.

    Each part is **length-prefixed** (4-byte big-endian length + UTF-8 bytes)
    before hashing, so the encoding is unambiguous: a plain separator (e.g.
    ``/``) would collapse distinct key tuples whose parts contain the separator
    — ``mint("a/b", "c")`` and ``mint("a", "b/c")`` would hash identical bytes.
    Length-prefixing makes every distinct tuple of parts hash to distinct bytes
    regardless of content. Hashed with blake2b (8-byte digest, personalized
    ``regmeta-id``); the low 62 bits become the ID body and bit 62 is set to mark
    it as minted (disjoint from the SCB source-ID band ``[0, 2^62)``).
    Deterministic: identical ``parts`` always mint the same ID.
    """
    return (_digest(*parts) & (_MINT_BIT - 1)) | _MINT_BIT


def mint_canonical_scb(*parts: str) -> int:
    """Deterministically mint a CANONICAL-SCB id in the reserved sub-band
    ``[2^61, 2^62)`` from ``parts`` (#444).

    Same blake2b hashing as :func:`mint`, but the body is the low 61 bits and bit
    61 (not bit 62) is set. The result is **low-band** (``< 2^62``), so it passes
    the SCB-provider band check, while staying above every real source-derived SCB
    id and disjoint from the minted band ``[2^62, 2^63)``. Use for registers /
    variants / variables / states curated onto the `scb` provider but absent from
    SCB's machine export. (value_set / value_code ids are unbanded — they keep
    their content-addressed AUTOINCREMENT ids.)
    """
    return (_digest(*parts) & (_CANONICAL_SCB_BIT - 1)) | _CANONICAL_SCB_BIT
