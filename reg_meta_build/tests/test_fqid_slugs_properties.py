"""Property-based tests for the slug-population leaf helpers.

Covers the panel-key encode↔decode round-trip (the populate writer in
``fqid_slugs.py`` encodes a composite key as ``json.dumps(list(tuple))``;
``catalog._decode_panel_entity_key`` is the matching reader, generic over both
panel keys), plus the name-fallback / uniquify / last-resort slug seeds. The
example suite in ``test_fqid_slugs.py`` pins specific cases; these assert the
structural invariants for *any* input.
"""

from __future__ import annotations

import json

from hypothesis import given, strategies as st
from reg_meta.catalog import _decode_panel_entity_key
from reg_meta.fqid import _SLUG_RE, derive_variable_slug

from reg_meta_build.fqid_slugs import (
    _NAME_SLUG_MAX_LEN,
    _fallback_slug,
    _name_slug,
    _uniquify,
)


def _encode_panel_key(key: str | tuple[str, ...] | None) -> str | None:
    """Mirror the inline populate-path encode in ``fqid_slugs.py`` (~line 986):
    a tuple → ``json.dumps(list(...))``, a bare str / ``"period"`` / None passes
    through unchanged. Kept in lockstep with the source (not forked logic — the
    source is inline, so the test re-states the same two-line rule it applies)."""
    if isinstance(key, tuple):
        return json.dumps(list(key))
    return key


# A slug-string component of a composite panel key. Real stored keys are only a
# variable slug (matches ``_SLUG_RE``, leading ``[a-z]``), the ``"period"``
# sentinel, or a JSON-array of slugs — so mirror that domain. Generating bare
# ``st.text()`` would emit ``"["``-leading strings that the decoder's
# ``startswith("[")`` reads as JSON arrays, a false round-trip failure.
slug_component = st.from_regex(_SLUG_RE, fullmatch=True)
panel_keys = st.one_of(
    st.none(),
    st.just("period"),
    slug_component,  # bare-slug / sentinel string
    st.lists(slug_component, min_size=1, max_size=4).map(tuple),  # composite
)


@given(panel_keys)
def test_panel_key_round_trip_lossless(key: str | tuple[str, ...] | None) -> None:
    """``decode(encode(x)) == x`` for tuples, bare strings, "period", and None;
    tuple order is preserved (decode yields a tuple, encode took one)."""
    assert _decode_panel_entity_key(_encode_panel_key(key)) == key


# -- _name_slug --------------------------------------------------------------

names = st.one_of(st.none(), st.text())


@given(names)
def test_name_slug_shape(name: str | None) -> None:
    """Output is None, or a valid slug within the cap, or the full uncapped
    base (the documented fallback when truncation isn't usable)."""
    out = _name_slug(name)
    if out is None:
        return
    assert _SLUG_RE.match(out), f"{out!r} fails _SLUG_RE"
    base = derive_variable_slug(name)
    assert len(out) <= _NAME_SLUG_MAX_LEN or out == base


@given(names, st.integers(min_value=1, max_value=80))
def test_name_slug_idempotent_under_fixed_cap(name: str | None, cap: int) -> None:
    """Re-folding a name-slug under the same cap returns it unchanged."""
    out = _name_slug(name, cap=cap)
    if out is not None:
        assert _name_slug(out, cap=cap) == out


# -- _uniquify ---------------------------------------------------------------

slug_words = st.text(
    alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
    min_size=1,
    max_size=8,
)


@given(slug_words, st.sets(st.text(min_size=1), max_size=20))
def test_uniquify_never_collides(base: str, used: set[str]) -> None:
    """Result is never already in ``used``; if ``base`` is free it is returned
    unchanged; deterministic."""
    out = _uniquify(base, used)
    assert out not in used
    if base not in used:
        assert out == base
    assert out == _uniquify(base, set(used))


# -- _fallback_slug ----------------------------------------------------------


@given(st.text())
def test_fallback_slug_is_valid_v_prefixed(provider_key: str) -> None:
    """Always a valid slug starting with ``v`` (the leading-letter seed)."""
    out = _fallback_slug(provider_key)
    assert _SLUG_RE.match(out), f"{out!r} fails _SLUG_RE"
    assert out.startswith("v")
