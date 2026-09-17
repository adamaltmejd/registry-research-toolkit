"""Concept-group input validation and offline accepted-candidate resolution.

Common grouping and persistence are exercised by test_catalog_dependencies.py and
test_resolved_metadata.py.
"""

from __future__ import annotations

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.concept_groups import (
    Accept,
    CodeLabelPair,
    CuratedGroup,
    CuratedMember,
    load_classification_groups,
    load_code_label_pairs,
    load_concept_group_accepts,
    load_concept_groups,
    resolve_accept,
)

_SCB = frozenset({"scb"})


class TestClassificationGroupLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_classification_groups(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_classification_groups(None) == ()
        assert load_classification_groups(tmp_path / "absent.toml") == ()

    def test_parses_valid_umbrella(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "sun-niva2020"
            value = "niva"
            label = "Nivå"
            [[classification_group.members]]
            classification = "sun-grupp2020"
            value = "grupp"
            label = "Grupp"
            """,
        )
        assert len(groups) == 1
        assert groups[0].key == "sun"
        # A provided axis is still accepted (the loader does not require it).
        assert groups[0].axis == "dimension"
        assert [m.classification for m in groups[0].members] == [
            "sun-niva2020",
            "sun-grupp2020",
        ]

    def test_axis_is_optional(self, tmp_path) -> None:
        # axis is now optional — an umbrella with no `axis` loads with axis=None
        # (axis-less), members keeping their value/label.
        groups = self._load(
            tmp_path,
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            [[classification_group.members]]
            classification = "sun-niva2020"
            value = "niva"
            label = "Nivå"
            [[classification_group.members]]
            classification = "sun-grupp2020"
            value = "grupp"
            label = "Grupp"
            """,
        )
        assert len(groups) == 1
        assert groups[0].axis is None
        assert groups[0].members[0].value == "niva"

    @pytest.mark.parametrize(
        "text",
        [
            # blank axis (present-but-blank is still drift, unlike absent)
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = ""
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
            # blank key
            """
            [[classification_group]]
            key = ""
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
            # missing members array
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            """,
            # only one member (< 2)
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            """,
            # member missing value
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
            # duplicate member slug
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "a"
            value = "2"
            label = "y"
            """,
        ],
    )
    def test_invalid_shapes_fail(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_keys_fail(self, tmp_path) -> None:
        text = """
        [[classification_group]]
        key = "sun"
        label = "SUN"
        axis = "dimension"
        [[classification_group.members]]
        classification = "a"
        value = "1"
        label = "x"
        [[classification_group.members]]
        classification = "b"
        value = "2"
        label = "y"
        [[classification_group]]
        key = "sun"
        label = "SUN2"
        axis = "dimension"
        [[classification_group.members]]
        classification = "c"
        value = "1"
        label = "x"
        [[classification_group.members]]
        classification = "d"
        value = "2"
        label = "y"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    def test_sibling_kinds_do_not_trip_top_level_guard(self, tmp_path) -> None:
        # A `[[classification_group]]` coexisting with `[[variable_group]]` /
        # `[[accept]]` parses cleanly — they're legal siblings, not typos.
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "vara"
            value = "1"
            label = "x"
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
        )
        assert [g.key for g in groups] == ["sun"]


class TestLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_concept_groups(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_concept_groups(None) == ()
        assert load_concept_groups(tmp_path / "absent.toml") == ()

    def test_parses_valid_family(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axis = "rank"
            [[variable_group.members]]
            variable = "agi1ink"
            value = "1"
            label = "största"
            [[variable_group.members]]
            variable = "extra"
            value = "2"
            label = "annan"
            """,
        )
        assert len(groups) == 1
        assert groups[0].provider == "scb"
        assert groups[0].members[0].variable == "agi1ink"
        assert groups[0].members[1].variable == "extra"

    def test_parses_axisless_variable_umbrella(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axes = []
            [[variable_group.members]]
            variable = "vara"
            [[variable_group.members]]
            variable = "varb"
            """,
        )
        assert groups[0].axes == ()
        assert groups[0].members == (
            CuratedMember(variable="vara", delivery_column=None, coords=()),
            CuratedMember(variable="varb", delivery_column=None, coords=()),
        )

    def test_parses_single_explicit_axis_with_flat_member_facets(
        self, tmp_path
    ) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axes = [{ axis = "rank", label = "Förvärvskälla" }]
            [[variable_group.members]]
            variable = "agi1ink"
            value = "1"
            label = "största"
            [[variable_group.members]]
            variable = "agi2ink"
            value = "2"
            label = "näst"
            """,
        )
        assert groups[0].axes == (("rank", "Förvärvskälla"),)
        assert groups[0].members[0].coords == (("rank", "1", "största"),)

    def test_parses_multi_axis_whole_variable_member(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axes = [
              { axis = "source", label = "Källa" },
              { axis = "rank", label = "Förvärvskälla" },
            ]
            [[variable_group.members]]
            variable = "agi1ink"
            coords = [
              { axis = "source", value = "agi", label = "AGI" },
              { axis = "rank", value = "1", label = "största" },
            ]
            [[variable_group.members]]
            variable = "ku1ink"
            coords = [
              { axis = "source", value = "ku", label = "KU" },
              { axis = "rank", value = "1", label = "största" },
            ]
            """,
        )
        assert groups[0].members[0].delivery_column is None
        assert groups[0].members[0].coords == (
            ("source", "agi", "AGI"),
            ("rank", "1", "största"),
        )

    def test_rejects_mixed_whole_and_representation_grain_per_variable(
        self, tmp_path
    ) -> None:
        text = """
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "Familj"
        axes = [
          { axis = "source", label = "Källa" },
          { axis = "rank", label = "Förvärvskälla" },
        ]
        [[variable_group.members]]
        variable = "agi1ink"
        coords = [
          { axis = "source", value = "agi", label = "AGI" },
          { axis = "rank", value = "all", label = "Alla" },
        ]
        [[variable_group.members]]
        variable = "agi1ink"
        delivery_column = "AGI1INK"
        coords = [
          { axis = "source", value = "agi", label = "AGI" },
          { axis = "rank", value = "1", label = "största" },
        ]
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    @pytest.mark.parametrize(
        "member_fields",
        [
            'value = "1"\nlabel = "x"',
            'coords = [{ axis = "rank", value = "1", label = "x" }]',
            'delivery_column = "COL"',
        ],
    )
    def test_axisless_variable_umbrella_rejects_facet_fields(
        self, tmp_path, member_fields: str
    ) -> None:
        text = f"""
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "Familj"
        axes = []
        [[variable_group.members]]
        variable = "vara"
        {member_fields}
        [[variable_group.members]]
        variable = "varb"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    @pytest.mark.parametrize(
        "text",
        [
            "[[variable_groups]]\n",  # misspelled top-level table
            # member missing the required `variable`
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            value = "1"
            label = "x"
            """,
            # 1-segment register
            """
            [[variable_group]]
            register = "lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "g"
            value = "1"
            label = "x"
            """,
            # missing members
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            """,
            # duplicate member reference
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "g"
            value = "1"
            label = "x"
            [[variable_group.members]]
            variable = "g"
            value = "2"
            label = "y"
            """,
        ],
    )
    def test_invalid_shapes_fail(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_group_keys_fail(self, tmp_path) -> None:
        text = """
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "F"
        axis = "a"
        [[variable_group.members]]
        variable = "g"
        value = "1"
        label = "x"
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "F2"
        axis = "b"
        [[variable_group.members]]
        variable = "h"
        value = "1"
        label = "x"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    def test_accept_sibling_does_not_trip_top_level_guard(self, tmp_path) -> None:
        # A `[[variable_group]]`, an `[[accept]]`, and a `[[classification_group]]`
        # coexist in one file: the variable_group parse must treat the latter two
        # as legal siblings, not unknown-top-level typos. (Case (g).)
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "vara"
            value = "1"
            label = "x"
            [[variable_group.members]]
            variable = "varb"
            value = "2"
            label = "y"
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
        )
        assert len(groups) == 1
        assert groups[0].key == "fam"


class TestAcceptLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_concept_group_accepts(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_concept_group_accepts(None) == ()
        assert load_concept_group_accepts(tmp_path / "absent.toml") == ()

    def test_parses_accept_with_overrides_and_exclude(self, tmp_path) -> None:
        accepts = self._load(
            tmp_path,
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            label = "Multipel dödsorsak"
            axis = "rank"
            exclude = ["morsak9"]
            """,
        )
        assert accepts == (
            Accept("sos", "dors", "morsak", "Multipel dödsorsak", "rank", ("morsak9",)),
        )

    def test_parses_minimal_accept(self, tmp_path) -> None:
        accepts = self._load(
            tmp_path,
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            """,
        )
        assert accepts == (Accept("sos", "dors", "morsak", None, None, ()),)

    def test_variable_group_sibling_does_not_trip_guard(self, tmp_path) -> None:
        # The accept loader must treat `[[variable_group]]` and
        # `[[classification_group]]` as legal siblings.
        accepts = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "v"
            value = "1"
            label = "x"
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            """,
        )
        assert accepts == (Accept("sos", "dors", "morsak", None, None, ()),)

    @pytest.mark.parametrize(
        "text",
        [
            # 1-segment register
            """
            [[accept]]
            register = "dors"
            key = "morsak"
            """,
            # 3-segment register (too many)
            """
            [[accept]]
            register = "sos/dors/morsak"
            key = "morsak"
            """,
            # empty key
            """
            [[accept]]
            register = "sos/dors"
            key = ""
            """,
            # blank label (present but empty — drift, not a fallback)
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            label = "  "
            """,
            # exclude with a non-string member
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            exclude = ["ok", 7]
            """,
            # exclude not a list
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            exclude = "morsak9"
            """,
        ],
    )
    def test_invalid_accept_shapes_fail(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_invalid"

    def test_duplicate_accept_fails(self, tmp_path) -> None:
        text = """
        [[accept]]
        register = "sos/dors"
        key = "morsak"
        [[accept]]
        register = "sos/dors"
        key = "morsak"
        axis = "rank"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"


# ── code↔label pairs (#923) ─────────────────────────────────────────────────


def _pair(code_slug: str = "partikod", label_slug: str = "partinamn") -> CodeLabelPair:
    return CodeLabelPair(
        code_provider="scb",
        code_register="lisa",
        code_variable=code_slug,
        label_provider="scb",
        label_register="lisa",
        label_variable=label_slug,
    )


class TestCodeLabelPairLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_code_label_pairs(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_code_label_pairs(None) == ()
        assert load_code_label_pairs(tmp_path / "absent.toml") == ()

    def test_parses_pair(self, tmp_path) -> None:
        pairs = self._load(
            tmp_path,
            """
            [[pair]]
            code  = "scb/lisa/partikod"
            label = "scb/lisa/partinamn"
            """,
        )
        assert pairs == (_pair(),)

    @pytest.mark.parametrize(
        "text",
        [
            # missing label
            """
            [[pair]]
            code = "scb/lisa/partikod"
            """,
            # missing code
            """
            [[pair]]
            label = "scb/lisa/partinamn"
            """,
            # 2-segment code FQID
            """
            [[pair]]
            code  = "scb/partikod"
            label = "scb/lisa/partinamn"
            """,
            # 2-segment label FQID
            """
            [[pair]]
            code  = "scb/lisa/partikod"
            label = "lisa/partinamn"
            """,
            # empty segment
            """
            [[pair]]
            code  = "scb//partikod"
            label = "scb/lisa/partinamn"
            """,
        ],
    )
    def test_malformed_entry_fails(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_unknown_top_level_key_fails(self, tmp_path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, '[[pairs]]\ncode = "a/b/c"\nlabel = "a/b/d"\n')
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_pair_fails(self, tmp_path) -> None:
        # Two identical (code, label) entries are drift — fail fast (EXIT_CONFIG,
        # the dedicated duplicate-pair code). The committed TOML is deduplicated, so
        # this guards future drift.
        text = """
        [[pair]]
        code  = "scb/lisa/partikod"
        label = "scb/lisa/partinamn"
        [[pair]]
        code  = "scb/lisa/partikod"
        label = "scb/lisa/partinamn"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_self_pair_fails(self, tmp_path) -> None:
        # A pair whose code and label FQID are identical is drift — a variable can't
        # decode itself. Fail fast at load with a clear error, rather than letting
        # the contradictory value_set guards fire at materialize time.
        text = """
        [[pair]]
        code  = "scb/lisa/partikod"
        label = "scb/lisa/partikod"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"


@pytest.mark.parametrize("override", [False, True])
def test_accepted_candidate_preserves_members_and_applies_overrides(override):
    auto = CuratedGroup(
        provider="scb",
        register="lisa",
        key="family",
        label="Family",
        axes=(("rank", "Rank"),),
        members=tuple(
            CuratedMember(
                variable=f"v{i}",
                delivery_column=None,
                coords=(("rank", str(i), str(i)),),
            )
            for i in range(3)
        ),
    )
    accept = Accept(
        "scb",
        "lisa",
        "family",
        "Override" if override else None,
        "part" if override else None,
        ("v2",),
    )
    result = resolve_accept(accept, {("scb", "lisa", "family"): auto})
    assert result.label == ("Override" if override else "Family")
    axis = "part" if override else "rank"
    assert result.axes == ((axis, axis),)
    assert tuple(m.variable for m in result.members) == ("v0", "v1")
    assert tuple(m.coords for m in result.members) == (
        ((axis, "0", "0"),),
        ((axis, "1", "1"),),
    )
    assert result.origin == "accept"


@pytest.mark.parametrize("failure", ["missing", "stale_exclude", "too_few"])
def test_accepted_candidate_refuses_drift(failure):
    auto = CuratedGroup(
        provider="scb",
        register="lisa",
        key="family",
        label="Family",
        axes=(("rank", "Rank"),),
        members=tuple(
            CuratedMember(
                variable=f"v{i}",
                delivery_column=None,
                coords=(("rank", str(i), str(i)),),
            )
            for i in range(2)
        ),
    )
    exclude = {"missing": (), "stale_exclude": ("absent",), "too_few": ("v0",)}[failure]
    accept = Accept("scb", "lisa", "family", None, None, exclude)
    with pytest.raises(RegMetaError) as exc:
        resolve_accept(
            accept, {} if failure == "missing" else {("scb", "lisa", "family"): auto}
        )
    assert exc.value.code == "concept_groups_unresolved"
    assert exc.value.exit_code == EXIT_CONFIG
