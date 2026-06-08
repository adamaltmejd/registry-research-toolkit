"""Unit tests for the ``?period`` / ``?variant`` / ``?value_set_version``
syntactic allow-list (``period_param.py``).

See DESIGN.md → query allow-list (period_param.py). The pure-function layer (no
app, no DB). The SECURITY GATE through the live
app — 422 + zero SQL + zero opens — lives in ``test_fqid_validation.py``. Here we
pin the parse RESULT shapes (the polymorphic ``Period`` mapping) and the reject
set (SQLi / traversal / NUL probes raise before any reg_meta call).
"""

from __future__ import annotations

import pytest
from reg_webapp.period_param import (
    PeriodParamError,
    ValueSetVersionParamError,
    VariantParamError,
    parse_period,
    parse_value_set_version,
    parse_variant,
)

# (raw wire value, expected Period). A bare year → int; every other token → str;
# a range → {"from","to"} dict; the snapshot sentinel → "_default".
_ACCEPT_PERIODS = [
    ("2020", 2020),
    ("HT2020", "HT2020"),
    ("VT2020", "VT2020"),
    ("2020-Q3", "2020-Q3"),
    ("2020-H1", "2020-H1"),
    ("2020-08", "2020-08"),
    ("2018-12-31", "2018-12-31"),
    ("_default", "_default"),
    ("2018..2020", {"from": 2018, "to": 2020}),
    ("2020-Q1..2020-Q4", {"from": "2020-Q1", "to": "2020-Q4"}),
    ("2018-01..2018-06", {"from": "2018-01", "to": "2018-06"}),
    ("HT2018..VT2019", {"from": "HT2018", "to": "VT2019"}),
]

# Malformed period values must raise BEFORE any reg_meta lookup. SQLi /
# traversal / NUL / percent-encoded probes are not period tokens → reject.
_REJECT_PERIODS = [
    "",  # empty
    "2020'; DROP TABLE--",  # SQLi probe
    "../../etc/passwd",  # traversal probe
    "2020\x00",  # embedded NUL
    "2020%2f..",  # percent-encoded slash (Starlette decodes; even raw it's not a token)
    "2020-13",  # month out of range
    "HT20",  # year too short
    "2020-2021",  # a bare year-range without `..` is not a token
    "2018..2019..2020",  # double `..`
    "..2020",  # missing from-endpoint
    "2018..",  # missing to-endpoint
    "2018..badtoken",  # bad to-endpoint
    "badtoken..2020",  # bad from-endpoint
    "2020 ",  # trailing space
    "abc",  # not a period
]


@pytest.mark.parametrize(("raw", "expected"), _ACCEPT_PERIODS)
def test_parse_period_accepts(raw: str, expected):
    result = parse_period(raw)
    assert result == expected
    # A bare year must be an int (the documented year arm), every other single
    # token a str — pin the type, not just equality (2020 == 2020 but the int/str
    # distinction is the contract `resolve_at` keys on).
    if raw == "2020":
        assert isinstance(result, int)


@pytest.mark.parametrize("raw", _REJECT_PERIODS)
def test_parse_period_rejects(raw: str):
    with pytest.raises(PeriodParamError):
        parse_period(raw)


# ?variant ADMITS `_default` (a real register_variant slug) UNLIKE the
# path guard. Validates against the slug grammar otherwise.
_ACCEPT_VARIANTS = ["_default", "individer-15plus", "standard", "ht"]
_REJECT_VARIANTS = [
    "",
    "Standard",  # uppercase
    "in valid",  # space
    "../etc",  # traversal
    "x\x00",  # NUL
    "x'; DROP--",  # SQLi
    "class",  # reserved (classification prefix)
]


@pytest.mark.parametrize("raw", _ACCEPT_VARIANTS)
def test_parse_variant_accepts(raw: str):
    assert parse_variant(raw) == raw


@pytest.mark.parametrize("raw", _REJECT_VARIANTS)
def test_parse_variant_rejects(raw: str):
    with pytest.raises(VariantParamError):
        parse_variant(raw)


# [A5.3b] ?value_set_version is matched against the FREE-TEXT
# `value_set_version_label` via a Python `==` filter in resolve_at (no SQL), so
# the gate is a sanity check (non-empty, length-capped, no control chars) —
# NOT the slug grammar. Real labels carry spaces / commas / parens / case /
# non-ASCII and MUST pass.
_ACCEPT_VSV = [
    "sun2020",
    "_none",  # the empty/default-label sentinel (the handler maps it to "")
    "SUN 1996",
    "SUN 1996, 5 positioner, brutto",
    "SUN 2000 - Utbildningsnivå",
    "Utbildningsnivå (SUN 2000)",
    "_default",  # a literal label value, not a sentinel here — accepted as-is
    "x" * 200,  # exactly the length cap
]
_REJECT_VSV = [
    "",  # empty
    "   ",  # whitespace-only (not a real label)
    "x" * 201,  # over the length cap
    "x\x00y",  # NUL
    "a\tb",  # C0 control (tab)
    "a\nb",  # C0 control (newline)
    "a\x7fb",  # DEL
    "a\x85b",  # C1 control
]


@pytest.mark.parametrize("raw", _ACCEPT_VSV)
def test_parse_value_set_version_accepts(raw: str):
    assert parse_value_set_version(raw) == raw


@pytest.mark.parametrize("raw", _REJECT_VSV)
def test_parse_value_set_version_rejects(raw: str):
    with pytest.raises(ValueSetVersionParamError):
        parse_value_set_version(raw)
