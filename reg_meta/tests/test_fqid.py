"""Tests for the FQID parser/emitter (REFACTOR_SPEC.md §5.2)."""

from __future__ import annotations

import pytest
from reg_meta.fqid import (
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

    def test_default_slug_only_in_variant_or_version_slot(self) -> None:
        # Allowed in the variant slot (synthesized for variant-less registers).
        assert parse("sos/lss/_default").kind is FqidKind.REGISTER_VARIANT
        assert parse("sos/lss/_default/2022").kind is FqidKind.REGISTER_VERSION
        # Allowed in the version slot too (PR γ: curated `_default` slug on
        # an unperiodized aux version).
        assert (
            parse("scb/lisa/individer-15plus/_default").kind
            is FqidKind.REGISTER_VERSION
        )
        # Rejected elsewhere.
        with pytest.raises(FqidError, match="_default"):
            parse("_default")
        with pytest.raises(FqidError, match="_default"):
            parse("scb/_default")
        with pytest.raises(FqidError, match="_default"):
            parse("scb/lisa/individer-15plus/2018/_default")

    def test_version_slot_accepts_curated_slug(self) -> None:
        # §5.2: slot 4 accepts kebab-case slug for unperiodized aux versions
        # alongside the period grammar.
        f = parse("scb/ureg/gymnasieintyg/ackumulerat-register")
        assert f.kind is FqidKind.REGISTER_VERSION
        assert f.period == "ackumulerat-register"
        # And in 5-segment binding form too.
        f = parse("scb/ureg/gymnasieintyg/ackumulerat-register/kon")
        assert f.kind is FqidKind.VARIABLE_BINDING
        assert f.period == "ackumulerat-register"
        assert f.variable == "kon"

    def test_period_shaped_slugs_rejected_outside_period_slot(self) -> None:
        # Period grammar is rejected in slots that must be slugs (provider,
        # register, variable). The variant slot is special: a period there
        # is parsed as an elided `_default` (§5.2).
        with pytest.raises(FqidError, match="period grammar"):
            parse("2020")  # provider slot
        with pytest.raises(FqidError, match="period grammar"):
            parse("scb/2020")  # register slot
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
            # §5.2 half-year — `H` doesn't collide with `HT` because the
            # latter is two-character and prefixes the year.
            "2020-H1",
            "2020-H2",
            "1995-H1",
            # §5.2 ISO date — snapshot-date versions like `'2014-12-31'`.
            "2014-12-31",
            "2002-10-15",
            "2018-01-01",
            "2018-12-31",
            # Day bound is syntactic only — Feb 30 / Apr 31 parse; calendar
            # validity is the curator's responsibility (same as how year and
            # month bounds don't enforce SCB coverage).
            "2018-02-30",
            "2019-04-31",
        ],
    )
    def test_valid_periods(self, period: str) -> None:
        assert is_period(period)

    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "20188",
            "2018-1",
            "Q1-2020",
            "HT20",
            "2020-Q0",
            "2020-Q5",
            "abc",
            # Out-of-range constituents must be rejected even though their
            # shape matches: month 13, year outside 1900-2099, term/quarter
            # with non-19xx/20xx year. (Codex P2 on PR #80.)
            "2020-13",
            "2020-00",
            "9999",
            "1899",
            "HT9999",
            "9999-Q1",
            # Half-year bounds: H0/H3 reject the same way Q0/Q5 do; prefix
            # form `H1-2020` and out-of-range year all reject.
            "2020-H0",
            "2020-H3",
            "9999-H1",
            "H1-2020",
            # ISO-date bounds: day 00/32, month 13, year outside 19xx/20xx,
            # truncated forms.
            "2018-01-00",
            "2018-01-32",
            "2018-13-01",
            "9999-01-01",
            "2018-1-1",
            "2018-01-1",
        ],
    )
    def test_invalid_periods(self, bad: str) -> None:
        assert not is_period(bad)

    def test_register_version_requires_valid_period_or_slug(self) -> None:
        # §5.2: slot 4 accepts either period grammar or kebab-case slug.
        # `Q1-2020` fails both (uppercase Q breaks slug grammar, prefix
        # breaks period grammar).
        with pytest.raises(FqidError, match="invalid slug"):
            parse("scb/lisa/v1/Q1-2020")


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

    def test_elided_default_variant_parsed(self) -> None:
        # §5.2: a period in slot 3 signals an omitted `_default` variant.
        # Parser normalizes to the explicit 5-segment form; stringification
        # emits the canonical (non-elided) shape.
        f = parse("sos/lss/2022")
        assert f.kind is FqidKind.REGISTER_VERSION
        assert f.variant == DEFAULT_VARIANT_SLUG
        assert f.period == "2022"
        assert str(f) == "sos/lss/_default/2022"

        f = parse("scb/arbetskraftsbarometern/2020/kon")
        assert f.kind is FqidKind.VARIABLE_BINDING
        assert f.variant == DEFAULT_VARIANT_SLUG
        assert f.period == "2020"
        assert f.variable == "kon"
        assert str(f) == "scb/arbetskraftsbarometern/_default/2020/kon"

    @pytest.mark.parametrize(
        "elided,canonical",
        [
            ("sos/lss/2022", "sos/lss/_default/2022"),
            ("sos/lss/2022-01", "sos/lss/_default/2022-01"),
            ("sos/lss/HT2022", "sos/lss/_default/HT2022"),
            ("sos/lss/2022-Q3", "sos/lss/_default/2022-Q3"),
            ("scb/r/2022/kon", "scb/r/_default/2022/kon"),
        ],
    )
    def test_elided_forms_expand_for_each_period_shape(
        self, elided: str, canonical: str
    ) -> None:
        assert str(parse(elided)) == canonical

    @pytest.mark.parametrize("bad", [None, 123, b"scb/lisa", ["scb", "lisa"]])
    def test_non_string_input_typed_error(self, bad: object) -> None:
        # Non-string input used to surface as `AttributeError` on the
        # `.startswith()` call. Callers expect every grammar failure to come
        # out as `FqidError` so they can catch one exception type.
        with pytest.raises(FqidError, match="must be a string"):
            parse(bad)  # type: ignore[arg-type]


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
        "delivery_column_name,expected",
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
    def test_derives_expected(self, delivery_column_name: str, expected: str) -> None:
        assert derive_variable_slug(delivery_column_name) == expected

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
            # ISO date is the one auto-extracted §5.2 addition — SCB rows
            # with literal `YYYY-MM-DD` round-trip through derive_period.
            ("2014-12-31", "2014-12-31"),
            ("Snapshot 2014-12-31 enligt ny definition", "2014-12-31"),
            ("Person-År", None),
            ("v19999", None),
            ("", None),
            (None, None),
        ],
    )
    def test_derives_most_specific(
        self, version_name: str | None, expected: str | None
    ) -> None:
        from reg_meta.fqid import derive_period

        assert derive_period(version_name) == expected

    def test_prefers_specific_over_year(self) -> None:
        from reg_meta.fqid import derive_period

        # "LISA HT2020" must not collapse to "2020" — keeps sub-year versions
        # from sharing an FQID with the year-only version of the same variant.
        assert derive_period("LISA HT2020") == "HT2020"
        assert derive_period("LISA 2020-Q1") == "2020-Q1"
        # ISO date is more specific than YYYY-MM or YYYY; extract picks it.
        assert derive_period("Snapshot 2014-12-31") == "2014-12-31"

    def test_range_form_is_not_misread_as_month(self) -> None:
        from reg_meta.fqid import derive_period

        # `2018-2020` (a range) must not greedy-match the YYYY-MM pattern
        # as `2018-20` — falls through to the year pattern instead.
        assert derive_period("LISA 2018-2020") == "2018"

    def test_half_year_is_curated_only(self) -> None:
        from reg_meta.fqid import derive_period

        # Half-year tokens are accepted by the §5.2 grammar but NOT in
        # `_PERIOD_EXTRACT_PATTERNS` — same convention as `maj-2011` /
        # `kv1-2011`. Swedish source forms don't carry the bare `1995-H1`
        # substring, so curators set `YYYY-H1` / `YYYY-H2` explicitly.
        assert derive_period("Första halvåret 1995") == "1995"
        assert derive_period("2015 januari - juni") == "2015"

    @pytest.mark.parametrize(
        "version_name,expected",
        [
            # Trailing year (the kommunal vuxenutbildning shape).
            ("Höstterminen 1980", "HT1980"),
            ("Vårterminen 1980", "VT1980"),
            ("Hösttermin 1980", "HT1980"),
            ("Vårtermin 1980", "VT1980"),
            # Leading year (some SCB rows put it first).
            ("1980 höstterminen", "HT1980"),
            ("1980 vårterminen", "VT1980"),
            # Trailing whitespace (real data has stray spaces).
            ("1980 höstterminen ", "HT1980"),
            # Trailing modifier.
            ("Vårterminen 2010 - betyg", "VT2010"),
            # Cross-term academic-year span resolves to the leading (HT) form
            # so it doesn't collide with the bare termin row.
            ("Höstterminen 1980 - Vårterminen 1981", "HT1980"),
        ],
    )
    def test_swedish_termin_tokens(self, version_name: str, expected: str) -> None:
        from reg_meta.fqid import derive_period

        # 220 real SCB collision groups (607 rows) come from "1980 höstterminen"
        # + "1980 vårterminen" both falling back to bare "1980". The termin
        # patterns run before the year extractor to keep terms distinct.
        assert derive_period(version_name) == expected

    def test_termin_without_year_falls_through(self) -> None:
        from reg_meta.fqid import derive_period

        # Termin tokens without a usable year don't transform; the bare
        # extractor returns None.
        assert derive_period("Höstterminen") is None


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
