"""Tests for the update module (version parsing and release resolution)."""

import pytest

from regmeta.download import _version_from_tag, _is_regmeta_release
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


class TestVersionFromTag:
    def test_prefixed_tag(self):
        assert _version_from_tag("regmeta/v0.5.0") == "0.5.0"

    def test_legacy_bare_tag(self):
        assert _version_from_tag("v0.4.0") == "0.4.0"

    def test_no_v_prefix(self):
        assert _version_from_tag("regmeta/0.5.0") == "0.5.0"


class TestIsRegmetaRelease:
    def test_prefixed_tag(self):
        assert _is_regmeta_release({"tag_name": "regmeta/v0.5.0"})

    def test_legacy_bare_tag(self):
        assert _is_regmeta_release({"tag_name": "v0.4.0"})

    def test_other_package_tag(self):
        assert not _is_regmeta_release({"tag_name": "mock-data-wizard/v0.4.0"})
