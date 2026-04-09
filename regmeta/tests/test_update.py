"""Tests for the update module (version parsing and release resolution)."""

import pytest

from regmeta.download import _is_regmeta_release, _pick_release, _version_from_tag
from regmeta.errors import RegmetaError
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

    def test_non_semver_v_tag(self):
        assert not _is_regmeta_release({"tag_name": "vNext"})


def _release(tag: str, has_db: bool = False) -> dict:
    """Build a minimal GitHub release dict for testing."""
    assets = [{"name": "regmeta.db.zst"}] if has_db else []
    return {"tag_name": tag, "assets": assets}


class TestPickRelease:
    def test_picks_latest_prefixed(self):
        releases = [
            _release("regmeta/v0.6.0"),
            _release("regmeta/v0.5.0", has_db=True),
        ]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.6.0"
        assert version == "0.6.0"
        assert db_tag == "regmeta/v0.5.0"

    def test_db_on_latest(self):
        releases = [_release("regmeta/v0.5.0", has_db=True)]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.5.0"
        assert db_tag == "regmeta/v0.5.0"

    def test_no_db_in_any_release(self):
        releases = [_release("regmeta/v0.6.0"), _release("regmeta/v0.5.0")]
        tag, _version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.6.0"
        assert db_tag is None

    def test_ignores_other_package_tags(self):
        releases = [
            _release("mock-data-wizard/v1.0.0", has_db=True),
            _release("regmeta/v0.5.0"),
        ]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.5.0"
        assert version == "0.5.0"
        assert db_tag is None

    def test_legacy_bare_tags(self):
        releases = [_release("v0.4.0", has_db=True)]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "v0.4.0"
        assert version == "0.4.0"
        assert db_tag == "v0.4.0"

    def test_prefers_prefixed_over_legacy(self):
        releases = [
            _release("regmeta/v0.6.0"),
            _release("v0.5.0", has_db=True),
        ]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.6.0"
        assert version == "0.6.0"
        assert db_tag == "v0.5.0"

    def test_empty_raises(self):
        with pytest.raises(RegmetaError) as exc_info:
            _pick_release([])
        assert exc_info.value.code == "no_releases"
