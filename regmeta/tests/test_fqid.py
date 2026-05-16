"""Tests for the FQID parser/emitter (REFACTOR_SPEC.md §5.2)."""

from __future__ import annotations

import pytest

from regmeta.fqid import (
    DEFAULT_VARIANT_SLUG,
    Fqid,
    FqidError,
    FqidKind,
    derive_variable_slug,
    is_period,
    is_slug,
    parse,
)


# ---------------------------------------------------------------------------
# Round-trip and segment-count discrimination
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.parametrize(
        "value,kind",
        [
            ("scb", FqidKind.PROVIDER),
            ("scb/lisa", FqidKind.REGISTER),
            ("scb/lisa/individer-15plus", FqidKind.REGISTER_VARIANT),
            ("scb/lisa/individer-15plus/2018", FqidKind.REGISTER_VERSION),
            ("scb/lisa/individer-15plus/2018/kon", FqidKind.VARIABLE_BINDING),
            ("class/sun/2020", FqidKind.CLASSIFICATION),
            ("class/lkf/2012", FqidKind.CLASSIFICATION),
            ("sos/lss/_default/2022", FqidKind.REGISTER_VERSION),
            ("scb/lisa/individer-15plus/HT2020", FqidKind.REGISTER_VERSION),
            ("scb/lisa/individer-15plus/2018-01", FqidKind.REGISTER_VERSION),
            ("scb/lisa/individer-15plus/2018-Q3", FqidKind.REGISTER_VERSION),
        ],
    )
    def test_parse_emit_identity(self, value: str, kind: FqidKind) -> None:
        f = parse(value)
        assert f.kind is kind
        assert str(f) == value
        # Idempotent across one more round-trip.
        assert str(parse(str(f))) == value


# ---------------------------------------------------------------------------
# Slug grammar
# ---------------------------------------------------------------------------


class TestSlugGrammar:
    @pytest.mark.parametrize(
        "slug",
        ["a", "ab", "kon", "lisa", "individer-15plus", "abc-d-e", "a1", "k0n"],
    )
    def test_valid_slugs(self, slug: str) -> None:
        assert is_slug(slug)

    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "A",
            "ab-",
            "-ab",
            "a--b",
            "1lisa",  # must start with letter
            "lisa_2018",  # underscore
            "Lisa",  # uppercase
            "lisåa",  # non-ASCII
        ],
    )
    def test_invalid_slugs(self, bad: str) -> None:
        assert not is_slug(bad)

    def test_class_token_rejected_as_slug_anywhere(self) -> None:
        # `class` is the discriminator; it may not appear as a slug in any
        # ordinary slot (§5.2 reserved-slug rule).
        for s in ("class", "scb/class", "scb/lisa/class", "scb/lisa/class/2020"):
            with pytest.raises(FqidError, match="class"):
                parse(s)

    def test_default_slug_only_in_variant_slot(self) -> None:
        # Allowed in the variant slot (synthesized for variant-less registers).
        assert parse("sos/lss/_default").kind is FqidKind.REGISTER_VARIANT
        assert parse("sos/lss/_default/2022").kind is FqidKind.REGISTER_VERSION
        # Rejected elsewhere.
        with pytest.raises(FqidError, match="_default"):
            parse("_default")
        with pytest.raises(FqidError, match="_default"):
            parse("scb/_default")
        with pytest.raises(FqidError, match="_default"):
            parse("scb/lisa/individer-15plus/2018/_default")

    def test_period_shaped_slugs_rejected_outside_period_slot(self) -> None:
        # Period grammar in non-period slot is rejected for legibility (§5.2).
        with pytest.raises(FqidError, match="period grammar"):
            parse("2020")  # provider slot
        with pytest.raises(FqidError, match="period grammar"):
            parse("scb/2020")  # register slot
        with pytest.raises(FqidError, match="period grammar"):
            parse("scb/lisa/2020")  # variant slot
        with pytest.raises(FqidError, match="period grammar"):
            parse("scb/lisa/v1/2020/2020")  # variable slot
        # Period slot accepts them.
        assert parse("scb/lisa/v1/2020").kind is FqidKind.REGISTER_VERSION


# ---------------------------------------------------------------------------
# Period grammar
# ---------------------------------------------------------------------------


class TestPeriodGrammar:
    @pytest.mark.parametrize(
        "period",
        [
            "2018",
            "1999",
            "2018-01",
            "2018-12",
            "HT2020",
            "VT2019",
            "2020-Q1",
            "2020-Q4",
        ],
    )
    def test_valid_periods(self, period: str) -> None:
        assert is_period(period)

    @pytest.mark.parametrize(
        "bad", ["", "20188", "2018-1", "Q1-2020", "HT20", "2020-Q0", "2020-Q5", "abc"]
    )
    def test_invalid_periods(self, bad: str) -> None:
        assert not is_period(bad)

    def test_register_version_requires_valid_period(self) -> None:
        with pytest.raises(FqidError, match="invalid period"):
            parse("scb/lisa/v1/q1-2020")


# ---------------------------------------------------------------------------
# Segment-count discrimination (kind is determined purely from segment count
# + the `class/` discriminator — no out-of-band lookup needed; §5.2).
# ---------------------------------------------------------------------------


class TestSegmentCount:
    def test_kind_from_segment_count(self) -> None:
        # Same slugs, different counts → different kinds.
        assert parse("scb").kind is FqidKind.PROVIDER
        assert parse("scb/lisa").kind is FqidKind.REGISTER
        assert parse("scb/lisa/v1").kind is FqidKind.REGISTER_VARIANT
        assert parse("scb/lisa/v1/2020").kind is FqidKind.REGISTER_VERSION
        assert parse("scb/lisa/v1/2020/kon").kind is FqidKind.VARIABLE_BINDING

    def test_class_prefix_forces_classification(self) -> None:
        # 3 segments with `class/` first → classification, not register_variant.
        assert parse("class/sun/2020").kind is FqidKind.CLASSIFICATION

    def test_six_segments_rejected(self) -> None:
        with pytest.raises(FqidError, match="6 segments"):
            parse("a/b/c/2020/e/f")

    def test_classification_wrong_arity_rejected(self) -> None:
        with pytest.raises(FqidError, match="3 segments"):
            parse("class/sun")
        with pytest.raises(FqidError, match="3 segments"):
            parse("class/sun/2020/extra")


# ---------------------------------------------------------------------------
# Empty/malformed input
# ---------------------------------------------------------------------------


class TestMalformed:
    def test_empty_raises(self) -> None:
        with pytest.raises(FqidError, match="empty"):
            parse("")

    def test_leading_slash_raises(self) -> None:
        with pytest.raises(FqidError, match="empty segment"):
            parse("/scb")

    def test_trailing_slash_raises(self) -> None:
        with pytest.raises(FqidError, match="empty segment"):
            parse("scb/")

    def test_double_slash_raises(self) -> None:
        with pytest.raises(FqidError, match="empty segment"):
            parse("scb//lisa")

    def test_elided_default_variant_rejected(self) -> None:
        # Stored FQIDs never elide (§5.2). Resolvers must reject the elided
        # display form `sos/lss/2022` — the period-slug ban makes 2022 invalid
        # in the variant slot, which is exactly the discrimination protection.
        with pytest.raises(FqidError, match="period grammar"):
            parse("sos/lss/2022")


# ---------------------------------------------------------------------------
# Factory constructors validate
# ---------------------------------------------------------------------------


class TestFactories:
    def test_register_factory(self) -> None:
        f = Fqid.register_fqid("scb", "lisa")
        assert str(f) == "scb/lisa"

    def test_variant_factory_accepts_default(self) -> None:
        f = Fqid.register_variant_fqid("sos", "lss", DEFAULT_VARIANT_SLUG)
        assert str(f) == "sos/lss/_default"

    def test_factory_rejects_bad_slug(self) -> None:
        with pytest.raises(FqidError):
            Fqid.register_fqid("SCB", "lisa")  # uppercase

    def test_classification_factory(self) -> None:
        f = Fqid.classification_fqid("sun", "2020")
        assert str(f) == "class/sun/2020"

    def test_classification_accepts_slug_version(self) -> None:
        # §5.3 examples use year versions; the slug-grammar branch covers
        # non-year tags like `v1` or `1-0`.
        assert str(Fqid.classification_fqid("sun", "v1")) == "class/sun/v1"
        assert str(parse("class/sun/v1")).endswith("/v1")

    def test_classification_rejects_reserved_version(self) -> None:
        with pytest.raises(FqidError, match="class"):
            Fqid.classification_fqid("sun", "class")
        with pytest.raises(FqidError, match="_default"):
            Fqid.classification_fqid("sun", "_default")

    def test_binding_factory(self) -> None:
        f = Fqid.binding_fqid("scb", "lisa", "v1", "2020", "kon")
        assert f.kind is FqidKind.VARIABLE_BINDING
        assert str(f) == "scb/lisa/v1/2020/kon"


# ---------------------------------------------------------------------------
# Variable slug derivation
# ---------------------------------------------------------------------------


class TestVariableSlugDerivation:
    @pytest.mark.parametrize(
        "kolumnnamn,expected",
        [
            ("Kon", "kon"),
            ("Kön", "kon"),
            ("KOMMUN", "kommun"),
            ("Födelseland_LISA", "fodelseland-lisa"),
            ("ÅÄÖVar", "aaovar"),
            ("Person-År", "person-ar"),
            ("Multi  spaces", "multi-spaces"),
            ("trim--edges--", "trim-edges"),
        ],
    )
    def test_derives_expected(self, kolumnnamn: str, expected: str) -> None:
        assert derive_variable_slug(kolumnnamn) == expected

    @pytest.mark.parametrize("empty", [None, "", "   ", "---", "_"])
    def test_empty_or_punctuation_yields_none(self, empty: str | None) -> None:
        assert derive_variable_slug(empty) is None

    def test_leading_digit_rejected(self) -> None:
        # Slug grammar requires a leading letter; "2018col" reduces to "2018col"
        # which fails the regex anchor.
        assert derive_variable_slug("2018col") is None

    def test_period_shaped_result_rejected(self) -> None:
        # If derivation yields something the period grammar would catch as
        # a slot-conflict (e.g. a bare year), reject — keeps FQID legible.
        assert derive_variable_slug("2018") is None


class TestDerivePeriod:
    @pytest.mark.parametrize(
        "version_name,expected",
        [
            ("LISA 2018", "2018"),
            ("LISA HT2020", "HT2020"),
            ("VT2019 cohort", "VT2019"),
            ("Survey 2020-Q1", "2020-Q1"),
            ("Census 2018-01", "2018-01"),
            ("Person-År", None),
            ("v19999", None),
            ("", None),
            (None, None),
        ],
    )
    def test_derives_most_specific(
        self, version_name: str | None, expected: str | None
    ) -> None:
        from regmeta.fqid import derive_period

        assert derive_period(version_name) == expected

    def test_prefers_specific_over_year(self) -> None:
        from regmeta.fqid import derive_period

        # "LISA HT2020" must not collapse to "2020" — keeps sub-year versions
        # from sharing an FQID with the year-only version of the same variant.
        assert derive_period("LISA HT2020") == "HT2020"
        assert derive_period("LISA 2020-Q1") == "2020-Q1"


# ---------------------------------------------------------------------------
# Stored FQIDs never elide (§5.2 paragraph "Stored FQIDs never elide")
# ---------------------------------------------------------------------------


class TestNoElision:
    def test_emit_keeps_default_variant(self) -> None:
        f = Fqid.binding_fqid("sos", "lss", DEFAULT_VARIANT_SLUG, "2022", "kon")
        assert str(f) == "sos/lss/_default/2022/kon"

    def test_no_silent_default_insertion_for_register(self) -> None:
        # A 2-segment FQID stays a register; we don't synthesise a _default
        # variant on emission.
        f = parse("sos/lss")
        assert str(f) == "sos/lss"
