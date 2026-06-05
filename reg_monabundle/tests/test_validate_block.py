"""Tests for ``reg_monabundle.validate_block``.

Mirrors the validator coverage that lived in
``mock_data_wizard.tests.test_spec`` before §15 step 5 phase 1
relocated the function. The cross-block referential checks
(orphan FQID, suppress_k-on-non-categorical) live in
``reg_monabundle.runtime.spec`` because they need the resolved
column dataclasses — see ``DESIGN.md`` "Scope".
"""

from __future__ import annotations

import pytest

from reg_monabundle import SUPPRESS_K, validate_block


def test_accepts_none():
    validate_block(None)


def test_accepts_well_formed_options():
    validate_block({"binding_options": {"scb/test/lopnr": {"suppress_k": 25}}})


def test_rejects_non_dict_block():
    with pytest.raises(ValueError, match="must be an object"):
        validate_block(["binding_options"])


def test_rejects_unknown_top_level_key():
    with pytest.raises(ValueError, match="unknown key"):
        validate_block({"unknown": {}})


def test_accepts_missing_binding_options():
    # The block is allowed to be {} or {binding_options absent}.
    validate_block({})


def test_rejects_non_dict_binding_options():
    with pytest.raises(ValueError, match="binding_options must be an object"):
        validate_block({"binding_options": ["scb/test/lopnr"]})


def test_rejects_non_fqid_key():
    with pytest.raises(ValueError, match="binding FQID"):
        validate_block({"binding_options": {"LopNr": {"suppress_k": 25}}})


@pytest.mark.parametrize(
    "bad_key",
    [
        # Whitespace inside a segment — would silently no-op at runtime.
        "scb/test/lop nr",
        # Empty segment.
        "scb//lopnr",
        # Wrong segment count (2).
        "scb/lopnr",
        # Wrong segment count (4).
        "scb/test/lopnr/extra",
        # Classification FQID, not a binding.
        "class/sun2020",
        # Disallowed character (period).
        "scb/test/lop.nr",
        # `@` is a disallowed character now that the @version pin is retired —
        # the leaf is a bare slug; any `@` in it is rejected.
        "scb/lisa/naringsgren@sni2007",
        "scb/test/lopnr@",
        "scb/test/@sni2007",
    ],
)
def test_rejects_malformed_fqid_variants(bad_key):
    with pytest.raises(ValueError, match="binding FQID"):
        validate_block({"binding_options": {bad_key: {"suppress_k": 25}}})


def test_rejects_non_dict_per_column_opts():
    with pytest.raises(ValueError, match="must be an object"):
        validate_block({"binding_options": {"scb/test/lopnr": ["suppress_k"]}})


def test_rejects_unknown_option():
    with pytest.raises(ValueError, match="unknown option"):
        validate_block(
            {
                "binding_options": {
                    "scb/test/lopnr": {"unknown_opt": 1},
                }
            }
        )


def test_rejects_suppress_k_below_floor():
    with pytest.raises(ValueError, match="below the global minimum"):
        validate_block(
            {
                "binding_options": {
                    "scb/test/lopnr": {"suppress_k": 1},
                }
            }
        )


def test_rejects_bool_suppress_k():
    # `True`/`False` are `isinstance(int)` in Python — explicit guard.
    with pytest.raises(ValueError, match="must be an int"):
        validate_block(
            {
                "binding_options": {
                    "scb/test/lopnr": {"suppress_k": True},
                }
            }
        )


def test_rejects_non_int_suppress_k():
    with pytest.raises(ValueError, match="must be an int"):
        validate_block(
            {
                "binding_options": {
                    "scb/test/lopnr": {"suppress_k": "25"},
                }
            }
        )


def test_accepts_suppress_k_at_floor():
    validate_block(
        {
            "binding_options": {
                "scb/test/lopnr": {"suppress_k": SUPPRESS_K},
            }
        }
    )
