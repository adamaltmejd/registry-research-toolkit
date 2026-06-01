"""Deterministic ID minting for provider-synthesized universal IDs.

REFACTOR_SPEC.md §5 (568-597) / §16 namespace invariant. Build-side only —
consumed by the SOS adapter (A4.3) and the §16 property test. NOT imported by
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
    data = b"".join(
        len(b).to_bytes(4, "big") + b for b in (p.encode("utf-8") for p in parts)
    )
    digest = blake2b(data, digest_size=8, person=b"regmeta-id").digest()
    return (int.from_bytes(digest, "big") & (_MINT_BIT - 1)) | _MINT_BIT
