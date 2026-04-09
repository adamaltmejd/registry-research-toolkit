"""Tests for the update module (version parsing)."""

import pytest

from regmeta.update import _parse_version


class TestParseVersion:
    """_parse_version produces comparable tuples for X.Y.Z, X.Y.ZaN, X.Y.Z.devN."""

    def test_final_release(self):
        assert _parse_version("0.4.0") == (0, 4, 0, 0, 0)

    def test_alpha(self):
        assert _parse_version("0.5.0a1") == (0, 5, 0, -1, 1)

    def test_dev(self):
        assert _parse_version("0.5.0.dev3") == (0, 5, 0, -2, 3)

    def test_strips_v_prefix(self):
        assert _parse_version("v1.2.3") == (1, 2, 3, 0, 0)

    def test_unparseable_returns_lowest(self):
        assert _parse_version("garbage") == (0, 0, 0, -99, 0)

    @pytest.mark.parametrize(
        "older, newer",
        [
            ("0.4.0.dev1", "0.4.0a1"),
            ("0.4.0a1", "0.4.0"),
            ("0.4.0", "0.5.0"),
            ("0.4.0.dev1", "0.4.0"),
            ("0.4.0a1", "0.4.0a2"),
            ("0.4.0.dev1", "0.4.0.dev2"),
            ("0.4.0", "1.0.0"),
        ],
    )
    def test_ordering(self, older: str, newer: str):
        assert _parse_version(older) < _parse_version(newer)
