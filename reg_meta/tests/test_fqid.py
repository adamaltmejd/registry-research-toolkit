"""Tests for the FQID parser/emitter (see DESIGN.md → FQID grammar).

A2.6 grammar: 1-seg provider, 2-seg register, 3-seg variable binding (the FQID
names the variable). A2.6.1: `class/<slug>` is a classification (2-seg, vintage
baked into the slug). The variant and period are delivery coordinates, NOT FQID
segments — the variant and register_version FQID kinds are gone.
"""

from __future__ import annotations

import pytest
from reg_meta.fqid import (
    DEFAULT_VARIANT_SLUG,
    RESERVED_GROUP_SLUG,
    RESERVED_HTTP_SUFFIX_SLUGS,
    RESERVED_VARIANTS_SLUG,
    Fqid,
    FqidError,
    FqidKind,
    derive_variable_slug,
    is_period,
    is_slug,
    parse,
    period_token_for_bounds,
    period_token_to_bounds,
    validate_slug,
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
            # 3-seg is now a binding (the leaf is the variable slug), not a
            # variant — the variant FQID kind is gone.
            ("scb/lisa/kon", FqidKind.VARIABLE_BINDING),
            ("scb/lisa/individer-15plus", FqidKind.VARIABLE_BINDING),
            ("sos/lss/insatstyp", FqidKind.VARIABLE_BINDING),
            # A2.6.1: classification is 2-seg with the vintage baked into the
            # slug (`class/<slug>`), not 3-seg `class/<slug>/<version>`.
            ("class/sun2020", FqidKind.CLASSIFICATION),
            ("class/lkf2012", FqidKind.CLASSIFICATION),
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
            "lisa\n",  # trailing newline (Python `$` would accept this; `\Z` rejects)
            "lisa\r",  # trailing carriage return
            "li\nsa",  # embedded newline
        ],
    )
    def test_invalid_slugs(self, bad: str) -> None:
        assert not is_slug(bad)

    def test_class_token_rejected_as_slug_anywhere(self) -> None:
        # `class` is the discriminator; it may not appear as a slug in any
        # ordinary slot (reserved-slug rule; see DESIGN.md → FQID grammar).
        for s in ("class", "scb/class", "scb/lisa/class"):
            with pytest.raises(FqidError, match="class"):
                parse(s)

    def test_default_slug_rejected_in_binding(self) -> None:
        # A2.6: `_default` is a register_variant coordinate, not an FQID segment.
        # It may not appear in a binding (provider/register/variable) slot.
        with pytest.raises(FqidError, match="_default"):
            parse("_default")
        with pytest.raises(FqidError, match="_default"):
            parse("scb/_default")
        with pytest.raises(FqidError, match="_default"):
            parse("scb/lisa/_default")  # variable slot rejects _default

    def test_period_shaped_slugs_rejected_in_binding(self) -> None:
        # Period grammar is rejected in every slot — there is no period segment
        # in the grammar anymore (see DESIGN.md → FQID grammar). A period-shaped leaf is not a valid
        # variable slug.
        with pytest.raises(FqidError, match="period grammar"):
            parse("2020")  # provider slot
        with pytest.raises(FqidError, match="period grammar"):
            parse("scb/2020")  # register slot
        with pytest.raises(FqidError, match="period grammar"):
            parse("scb/lisa/2020")  # variable slot


# ---------------------------------------------------------------------------
# Reserved HTTP-suffix slugs (see DESIGN.md → FQID grammar): tokens that would shadow a live reg_webapp
# catalog sub-resource route. The 6 binding suffixes are reserved in the
# variable / register / classification slots; `variants` only in the variable
# slot. Provider and register_variant slots carry no reservation.
# ---------------------------------------------------------------------------


class TestReservedHttpSuffixSlugs:
    # The reserved variable-slot tokens = the binding suffixes + `variants`.
    ALL_VARIABLE_RESERVED = sorted(
        RESERVED_HTTP_SUFFIX_SLUGS | {RESERVED_VARIANTS_SLUG}
    )
    # `lineage_warnings` carries an underscore, so the slug grammar rejects it
    # BEFORE the reserved-token check runs — it can never be a slug regardless.
    # The other tokens are valid slug SHAPES whose only barrier IS the
    # reservation, so a "reserved" message is asserted only for these.
    SLUG_SHAPED_RESERVED = sorted(t for t in ALL_VARIABLE_RESERVED if is_slug(t))
    SLUG_SHAPED_SUFFIXES = sorted(t for t in RESERVED_HTTP_SUFFIX_SLUGS if is_slug(t))

    @pytest.mark.parametrize("token", ALL_VARIABLE_RESERVED)
    def test_variable_slot_rejects_all_reserved(self, token: str) -> None:
        # All are rejected as a variable slug (the slug-shaped suffixes + `variants`
        # via the reservation; `lineage_warnings` via the underscore grammar bar).
        with pytest.raises(FqidError):
            validate_slug(token, "variable")

    @pytest.mark.parametrize("token", SLUG_SHAPED_RESERVED)
    def test_variable_slot_reserved_message(self, token: str) -> None:
        with pytest.raises(FqidError, match="reserved"):
            validate_slug(token, "variable")

    @pytest.mark.parametrize("token", sorted(RESERVED_HTTP_SUFFIX_SLUGS))
    def test_register_slot_rejects_suffix_slugs(self, token: str) -> None:
        with pytest.raises(FqidError):
            validate_slug(token, FqidKind.REGISTER)

    @pytest.mark.parametrize("token", sorted(RESERVED_HTTP_SUFFIX_SLUGS))
    def test_classification_slot_rejects_suffix_slugs(self, token: str) -> None:
        with pytest.raises(FqidError):
            validate_slug(token, FqidKind.CLASSIFICATION)

    @pytest.mark.parametrize("token", SLUG_SHAPED_SUFFIXES)
    def test_register_and_classification_reserved_message(self, token: str) -> None:
        with pytest.raises(FqidError, match="reserved"):
            validate_slug(token, FqidKind.REGISTER)
        with pytest.raises(FqidError, match="reserved"):
            validate_slug(token, FqidKind.CLASSIFICATION)

    def test_variants_accepted_in_register_and_classification_slots(self) -> None:
        # `variants` only collides with the 3-seg variable leaf — a 2-seg
        # register (`scb/variants`) and a classification (`class/variants`) are
        # clean paths, so the register / classification slots must ACCEPT it.
        validate_slug(RESERVED_VARIANTS_SLUG, FqidKind.REGISTER)
        validate_slug(RESERVED_VARIANTS_SLUG, FqidKind.CLASSIFICATION)

    @pytest.mark.parametrize("token", SLUG_SHAPED_RESERVED)
    def test_reserved_tokens_allowed_in_provider_slot(self, token: str) -> None:
        # A provider is always the leading segment, never a colliding URL
        # position — no reservation applies. (`lineage_warnings` is excluded: it
        # fails the slug grammar in every slot, provider included.)
        validate_slug(token, FqidKind.PROVIDER)

    @pytest.mark.parametrize(
        "value",
        ["scb/lisa/states", "scb/lisa/related", "scb/lisa/variants"],
    )
    def test_parse_rejects_reserved_variable_leaf(self, value: str) -> None:
        with pytest.raises(FqidError, match="reserved"):
            parse(value)

    @pytest.mark.parametrize("value", ["scb/states", "class/states"])
    def test_parse_rejects_reserved_register_and_classification(
        self, value: str
    ) -> None:
        with pytest.raises(FqidError, match="reserved"):
            parse(value)

    def test_parse_accepts_variants_as_register(self) -> None:
        # `scb/variants` is a clean 2-seg register path (variants is reserved
        # only in the variable slot).
        f = parse("scb/variants")
        assert f.kind is FqidKind.REGISTER
        assert str(f) == "scb/variants"

    @pytest.mark.parametrize(
        "name", ["States", "Variants", "Lineage", "Related", "Class"]
    )
    def test_derive_variable_slug_rejects_folded_reserved(self, name: str) -> None:
        # A column name that folds to a reserved variable-slot token degrades to
        # None so the caller falls back to the name / last-resort slug instead of
        # minting a route-shadowing slug. "Class" pins the `class`-reservation
        # path now that the deriver delegates to validate_slug (the explicit
        # RESERVED_SLUGS check is gone).
        assert derive_variable_slug(name) is None


# ---------------------------------------------------------------------------
# Reserved PROVIDER-slot slug (#617, see DESIGN.md → FQID grammar): `group` would
# shadow the `/catalog/group/{provider}/{register}/{key}` concept-group subject
# route — the FIRST token reserved in the provider slot (a literal `group` prefix
# put `{provider}` at a non-leading position, so the provider slot CAN now collide).
# Reserved in the provider slot ONLY; a register / variable / classification named
# `group` is unaffected.
# ---------------------------------------------------------------------------


class TestReservedGroupSlug:
    def test_provider_slot_rejects_group(self) -> None:
        with pytest.raises(FqidError, match="reserved"):
            validate_slug(RESERVED_GROUP_SLUG, FqidKind.PROVIDER)

    def test_provider_fqid_rejects_group(self) -> None:
        # The construction path runs validate_slug on the provider segment.
        with pytest.raises(FqidError, match="reserved"):
            Fqid.provider_fqid(RESERVED_GROUP_SLUG)
        with pytest.raises(FqidError, match="reserved"):
            parse(RESERVED_GROUP_SLUG)  # 1-seg provider FQID

    def test_group_accepted_in_other_slots(self) -> None:
        # `group` only lands at the provider position in the subject route, so a
        # register / variable / classification slug `group` is a clean path.
        validate_slug(RESERVED_GROUP_SLUG, FqidKind.REGISTER)
        validate_slug(RESERVED_GROUP_SLUG, "variable")
        validate_slug(RESERVED_GROUP_SLUG, FqidKind.CLASSIFICATION)

    def test_group_parses_in_register_and_variable_leaf(self) -> None:
        # `scb/group` (register) and `scb/lisa/group` (variable leaf) are clean
        # FQIDs — only the PROVIDER (leading) position carries the reservation.
        assert parse("scb/group").kind is FqidKind.REGISTER
        assert parse("scb/lisa/group").kind is FqidKind.VARIABLE_BINDING
        assert parse("class/group").kind is FqidKind.CLASSIFICATION

    def test_group_rejected_as_provider_in_multiseg(self) -> None:
        # A 2-/3-seg FQID whose PROVIDER segment is `group` is rejected (the
        # provider-slot reservation fires on the leading segment).
        with pytest.raises(FqidError, match="reserved"):
            parse("group/lisa")
        with pytest.raises(FqidError, match="reserved"):
            parse("group/lisa/kon")


# ---------------------------------------------------------------------------
# Period grammar (survives: resolve_at + build-time coalescer use it; it is
# no longer an FQID segment)
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
            "2020-H1",
            "2020-H2",
            "1995-H1",
            "2014-12-31",
            "2002-10-15",
            "2018-01-01",
            "2018-12-31",
            "2020-02-29",  # 2020 IS a leap year — a real Feb 29
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
            "2020-13",
            "2020-00",
            "9999",
            "1899",
            "HT9999",
            "9999-Q1",
            "2020-H0",
            "2020-H3",
            "9999-H1",
            "H1-2020",
            "2018-01-00",
            "2018-01-32",
            "2018-13-01",
            "9999-01-01",
            "2018-1-1",
            "2018-01-1",
            "2020\n",  # trailing newline (Python `$` would accept this; `\Z` rejects)
            "HT2020\n",
            "2020-Q3\n",
            "2020-01-01\n",
            # Calendar-impossible author days: pass the syntactic 01-31 day regex
            # but are not real dates, so `is_period` rejects them (the
            # `_MONTH_LAST_DAY` Feb→29 over-count is for SYNTHESIZED bounds only,
            # never an author-supplied `YYYY-MM-DD` day).
            "2019-02-29",  # 2019 is NOT a leap year
            "2018-02-30",  # February never has 30 days
            "2021-04-31",  # April has 30 days
            "2019-04-31",  # April has 30 days
        ],
    )
    def test_invalid_periods(self, bad: str) -> None:
        assert not is_period(bad)

    @pytest.mark.parametrize("bad", ["2019-02-29", "2018-02-30", "2021-04-31"])
    def test_period_token_to_bounds_rejects_calendar_invalid_day(
        self, bad: str
    ) -> None:
        # `period_token_to_bounds` guards on `is_period` first, so a calendar-
        # impossible author day raises rather than expanding to nonsense bounds
        # (#239). The synthesized Feb→29 over-count for month/quarter forms is
        # unaffected — that is NOT an author-supplied day.
        with pytest.raises(FqidError):
            period_token_to_bounds(bad)


class TestPeriodTokenForBounds:
    @pytest.mark.parametrize(
        "token",
        # Every emittable token form round-trips bounds → token exactly. The
        # -H forms are deliberately absent: their bounds are the term forms'.
        [
            "2018",
            "VT2019",
            "HT2020",
            "2020-Q1",
            "2020-Q3",
            "2018-03",
            "2018-02",
            "2014-12-31",
        ],
    )
    def test_round_trip(self, token: str) -> None:
        assert period_token_for_bounds(*period_token_to_bounds(token)) == token

    def test_half_year_ties_break_to_term(self) -> None:
        # H1/H2 bounds are identical to VT/HT bounds; the term form wins and
        # the -H spelling is never emitted.
        assert period_token_for_bounds(*period_token_to_bounds("1995-H1")) == "VT1995"
        assert period_token_for_bounds(*period_token_to_bounds("1995-H2")) == "HT1995"

    def test_february_uses_synthesized_end(self) -> None:
        # The month window carries the intentional Feb-29 over-count from
        # `_MONTH_LAST_DAY` — the inverse must accept exactly that, leap year
        # or not, and a calendar-Feb-28 window is NOT a month token.
        assert period_token_for_bounds("2019-02-01", "2019-02-29") == "2019-02"
        assert period_token_for_bounds("2019-02-01", "2019-02-28") == (
            "2019-02-01..2019-02-28"
        )

    @pytest.mark.parametrize(
        "lo,hi",
        [
            ("2009-01-01", "2009-09-30"),  # Q1-Q3 union — no single token
            ("2009-04-01", "2009-12-31"),  # Q2-Q4 union
            ("2010-01-01", "2012-12-31"),  # multi-year span
            ("2009-07-01", "2010-06-30"),  # school-year span
        ],
    )
    def test_non_grammar_window_renders_explicit_range(self, lo: str, hi: str) -> None:
        # NEVER rounded down to a bare year — two sub-annual siblings both
        # reading "2009" would re-create the ambiguity #271 removes.
        assert period_token_for_bounds(lo, hi) == f"{lo}..{hi}"

    def test_single_day(self) -> None:
        assert period_token_for_bounds("2014-12-31", "2014-12-31") == "2014-12-31"
        # A calendar-impossible day can't be a token → explicit range.
        assert period_token_for_bounds("2019-02-29", "2019-02-29") == (
            "2019-02-29..2019-02-29"
        )


# ---------------------------------------------------------------------------
# Segment-count discrimination (kind is determined purely from segment count
# + the `class/` discriminator — no out-of-band lookup needed; see DESIGN.md → FQID grammar).
# ---------------------------------------------------------------------------


class TestSegmentCount:
    def test_kind_from_segment_count(self) -> None:
        # Same slugs, different counts → different kinds.
        assert parse("scb").kind is FqidKind.PROVIDER
        assert parse("scb/lisa").kind is FqidKind.REGISTER
        assert parse("scb/lisa/kon").kind is FqidKind.VARIABLE_BINDING

    def test_class_prefix_forces_classification(self) -> None:
        # 2 segments with `class/` first → classification, not a register.
        assert parse("class/sun2020").kind is FqidKind.CLASSIFICATION

    def test_four_plus_segments_rejected(self) -> None:
        # The old 4-seg version and 5-seg binding forms no longer parse.
        with pytest.raises(FqidError, match="4 segments"):
            parse("scb/lisa/individer-15plus/2018")
        with pytest.raises(FqidError, match="5 segments"):
            parse("scb/lisa/individer-15plus/2018/kon")

    def test_classification_wrong_arity_rejected(self) -> None:
        # A2.6.1: the canonical form is 2-seg `class/<slug>`. The old 3-seg
        # `class/<slug>/<version>` now raises (vintage baked into the slug).
        with pytest.raises(FqidError, match="2 segments"):
            parse("class/sun/2020")
        with pytest.raises(FqidError, match="2 segments"):
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

    def test_factory_rejects_bad_slug(self) -> None:
        with pytest.raises(FqidError):
            Fqid.register_fqid("SCB", "lisa")  # uppercase

    def test_classification_factory(self) -> None:
        # A2.6.1: single-arg factory; the slug bakes in the vintage.
        f = Fqid.classification_fqid("sun2020")
        assert str(f) == "class/sun2020"

    def test_classification_round_trips_baked_slug(self) -> None:
        # The version-baked slug round-trips: factory → str → parse → slug.
        assert str(Fqid.classification_fqid("lkf2007")) == "class/lkf2007"
        assert parse("class/sun2020").classification == "sun2020"

    def test_classification_rejects_reserved_slug(self) -> None:
        # `class` and `_default` are reserved and can't be a classification slug.
        with pytest.raises(FqidError, match="class"):
            Fqid.classification_fqid("class")
        with pytest.raises(FqidError, match="_default"):
            Fqid.classification_fqid("_default")

    def test_binding_factory(self) -> None:
        # A2.6: 3-arg binding factory (provider, register, variable).
        f = Fqid.binding_fqid("scb", "lisa", "kon")
        assert f.kind is FqidKind.VARIABLE_BINDING
        assert str(f) == "scb/lisa/kon"
        assert f.variable == "kon"

    def test_binding_factory_rejects_default_variable(self) -> None:
        # `_default` is a variant coordinate, not a variable slug.
        with pytest.raises(FqidError, match="_default"):
            Fqid.binding_fqid("sos", "lss", DEFAULT_VARIANT_SLUG)


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

        # "LISA HT2020" must not collapse to "2020" — keeps sub-year editions
        # distinct (the build coalescer + lineage linker rely on this).
        assert derive_period("LISA HT2020") == "HT2020"
        assert derive_period("LISA 2020-Q1") == "2020-Q1"
        assert derive_period("Snapshot 2014-12-31") == "2014-12-31"

    def test_range_form_is_not_misread_as_month(self) -> None:
        from reg_meta.fqid import derive_period

        # `2018-2020` (a range) must not greedy-match the YYYY-MM pattern
        # as `2018-20` — falls through to the year pattern instead.
        assert derive_period("LISA 2018-2020") == "2018"

    @pytest.mark.parametrize(
        "version_name,expected",
        [
            ("Höstterminen 1980", "HT1980"),
            ("Vårterminen 1980", "VT1980"),
            ("1980 höstterminen", "HT1980"),
            ("1980 vårterminen", "VT1980"),
            ("Vårterminen 2010 - betyg", "VT2010"),
            ("Höstterminen 1980 - Vårterminen 1981", "HT1980"),
        ],
    )
    def test_swedish_termin_tokens(self, version_name: str, expected: str) -> None:
        from reg_meta.fqid import derive_period

        assert derive_period(version_name) == expected

    @pytest.mark.parametrize(
        "version_name,expected",
        [
            ("Snapshot 2019-02-29", "2019-02"),  # non-leap Feb 29 → month token
            ("Snapshot 2018-02-30", "2018-02"),  # Feb 30 → month token
            ("Snapshot 2021-04-31", "2021-04"),  # April 31 → month token
            ("Snapshot 2020-02-29", "2020-02-29"),  # leap year IS a real date
        ],
    )
    def test_calendar_invalid_full_date_degrades_to_month(
        self, version_name: str, expected: str
    ) -> None:
        from reg_meta.fqid import derive_period

        # `is_period` now calendar-validates the author day, so a calendar-
        # impossible full-date substring skips the full-date extract pattern and
        # degrades to the most-specific VALID token (the month), instead of
        # returning a token `period_token_to_bounds` would later crash on.
        assert derive_period(version_name) == expected

    @pytest.mark.parametrize(
        "version_name",
        [
            "Snapshot 2019-02-29",
            "x 2021-04-31 y",
            "2018-02-30 cohort",
            "LISA HT2020",
            "Survey 2020-Q1",
            "Census 2018-01",
            "2014-12-31",
            "LISA 2018-2020",
            "Höstterminen 1980",
            "Person-År",  # no period — None is allowed
        ],
    )
    def test_extractor_output_always_satisfies_is_period(
        self, version_name: str
    ) -> None:
        from reg_meta.fqid import derive_period

        # The restored invariant: whatever `derive_period` returns (when non-None)
        # must itself pass `is_period` — the extractors and the validator agree on
        # what counts as a period, calendar-impossible dates included.
        out = derive_period(version_name)
        assert out is None or is_period(out), (version_name, out)


# ---------------------------------------------------------------------------
# Stored binding FQIDs (no variant/period segment; see DESIGN.md → FQID grammar)
# ---------------------------------------------------------------------------


class TestBindingFqid:
    def test_binding_is_three_segment(self) -> None:
        f = Fqid.binding_fqid("scb", "lisa", "kon")
        assert str(f) == "scb/lisa/kon"
        # No variant/period fields exist on the dataclass anymore.
        assert not hasattr(f, "variant")
        assert not hasattr(f, "period")

    def test_register_stays_two_segment(self) -> None:
        f = parse("sos/lss")
        assert f.kind is FqidKind.REGISTER
        assert str(f) == "sos/lss"


# ---------------------------------------------------------------------------
# Pydantic interop (#681): `Fqid` rides a `__get_pydantic_core_schema__` hook so
# a `BaseModel` with an `Fqid` field treats it as a STRING on the wire.
# ---------------------------------------------------------------------------


class TestPydanticSchema:
    def _model(self):
        from pydantic import BaseModel, ConfigDict

        class M(BaseModel):
            model_config = ConfigDict(frozen=True)
            fqid: Fqid
            opt: Fqid | None = None

        return M

    def test_json_schema_emits_type_string(self) -> None:
        schema = self._model().model_json_schema()
        assert schema["properties"]["fqid"] == {"title": "Fqid", "type": "string"}
        # `Fqid | None` → nullable string, not an object.
        assert {"type": "string"} in schema["properties"]["opt"]["anyOf"]

    def test_validate_from_str_parses(self) -> None:
        m = self._model()(fqid="scb/lisa/kon")
        assert isinstance(m.fqid, Fqid)
        assert m.fqid == Fqid.binding_fqid("scb", "lisa", "kon")

    def test_validate_accepts_an_existing_fqid_instance(self) -> None:
        existing = Fqid.register_fqid("scb", "lisa")
        assert self._model()(fqid=existing).fqid == existing

    def test_validate_rejects_a_malformed_fqid_string(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            self._model()(fqid="not//a/valid/fqid")

    def test_serializes_via_canonical_str(self) -> None:
        m = self._model()(fqid="scb/lisa/kon")
        assert m.model_dump_json() == '{"fqid":"scb/lisa/kon","opt":null}'

    def test_round_trips_str_to_fqid_to_str(self) -> None:
        m = self._model()(fqid="class/sun2020")
        assert str(m.fqid) == "class/sun2020"
