"""Property-based tests for `derive_variable_slug` (see DESIGN.md → Slug curation).

Hypothesis stresses the auto-slug fold (NFKD→ASCII→lowercase→hyphenate→validate)
with full-Unicode input — the example suite in ``test_fqid.py`` pins specific
cases, these assert the structural invariants hold for *any* input.
"""

from __future__ import annotations

from hypothesis import given, strategies as st
from reg_meta.fqid import _SLUG_RE, derive_variable_slug

# Full Unicode: diacritics, non-Latin scripts, control chars, whitespace — the
# fold has to survive all of it. Include None to exercise the empty-input guard.
slug_inputs = st.one_of(st.none(), st.text())


@given(slug_inputs)
def test_output_is_none_or_valid_slug(name: str | None) -> None:
    """Any non-None output fully matches the module slug grammar."""
    out = derive_variable_slug(name)
    if out is not None:
        assert _SLUG_RE.match(out), f"{out!r} from {name!r} fails _SLUG_RE"


@given(slug_inputs)
def test_idempotence(name: str | None) -> None:
    """Feeding a derived slug back in returns it unchanged."""
    out = derive_variable_slug(name)
    if out is not None:
        assert derive_variable_slug(out) == out


@given(slug_inputs)
def test_determinism(name: str | None) -> None:
    """Two calls on the same input agree."""
    assert derive_variable_slug(name) == derive_variable_slug(name)


@given(slug_inputs)
def test_output_is_ascii_lowercase(name: str | None) -> None:
    """Non-None output is pure-ASCII lowercase (NFKD case/diacritic fold)."""
    out = derive_variable_slug(name)
    if out is not None:
        assert out.isascii()
        assert out == out.lower()
