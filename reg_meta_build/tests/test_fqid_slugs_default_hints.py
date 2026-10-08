"""Default-slug candidate classification and hint formatting: pure functions of
register and variant names. Which registers of a built catalog the hint names is
`cases/cli/seed-slugs/`."""

from __future__ import annotations

from reg_meta_build.fqid_slugs import (
    classify_default_candidate,
    format_default_slug_hints,
)


class TestClassifyDefaultCandidate:
    def test_exact_mirror(self):
        assert (
            classify_default_candidate("Nybörjare i Komvux", "Nybörjare i Komvux")[0]
            == "exact"
        )

    def test_case_and_whitespace_insensitive(self):
        cls, _ = classify_default_candidate(
            "  Arbetskraftsbarometern  ", "ARBETSKRAFTSBAROMETERN"
        )
        assert cls == "exact"

    def test_paren_abbrev_stripped(self):
        cls, reason = classify_default_candidate(
            "Konjunkturstatistik, löner för statlig sektor (KLS)",
            "Konjunkturstatistik, löner för statlig sektor",
        )
        assert cls == "near"
        assert "paren" in reason

    def test_variant_matches_register_parenthetical(self):
        cls, reason = classify_default_candidate(
            "Registret för integrationsstudier (STATIV)", "STATIV"
        )
        assert cls == "near"
        assert "STATIV" in reason

    def test_survey_statistics_sibling(self):
        cls, reason = classify_default_candidate(
            "Continuing Vocational Training Statistics",
            "Continuing Vocational Training Survey",
        )
        assert cls == "near"
        assert "Survey/Statistics" in reason

    def test_canonical_substring(self):
        cls, _ = classify_default_candidate("Leveranser av fordonsgas", "Fordonsgas")
        assert cls == "near"

    def test_kept_when_genuinely_different(self):
        cls, _ = classify_default_candidate(
            "Mervärdesskatteregistret (Moms)", "Momsdeklarationsregistret"
        )
        assert cls == "kept"

    def test_kept_with_short_canonical_collision(self):
        # Two-letter overlap shouldn't qualify as a substring match.
        assert classify_default_candidate("Foo", "Bar")[0] == "kept"

    def test_missing_name(self):
        assert classify_default_candidate("", "Variant")[0] == "kept"


class TestFormatDefaultSlugHints:
    def _make(
        self,
        provider: str,
        register_slug: str,
        register_name: str,
        variant_name: str,
        classification: str,
        current_slug: str | None,
    ):
        from reg_meta_build.fqid_slugs import DefaultSlugCandidate

        return DefaultSlugCandidate(
            provider=provider,
            register_slug=register_slug,
            register_name=register_name,
            variant_name=variant_name,
            classification=classification,  # type: ignore[arg-type]
            reason="test",
            current_slug=current_slug,
        )

    def test_returns_none_when_no_actionable_candidates(self):
        # Both candidates already carry `_default` → nothing to suggest.
        cands = [
            self._make("scb", "komvux", "X", "X", "exact", "_default"),
            self._make("scb", "kls", "Y", "Y", "exact", "_default"),
        ]
        assert format_default_slug_hints(cands, all_hints=False) is None

    def test_skips_kept_candidates(self):
        cands = [self._make("scb", "boende", "A", "B", "kept", None)]
        assert format_default_slug_hints(cands, all_hints=False) is None

    def test_truncated_preview_by_default(self):
        cands = [
            self._make("scb", f"reg{i}", f"Reg{i}", f"Reg{i}", "exact", None)
            for i in range(1, 11)
        ]
        out = format_default_slug_hints(cands, all_hints=False)
        assert out is not None
        assert "10 single-variant register(s)" in out
        assert "scb/reg1 " in out
        assert "scb/reg5 " in out
        # Tail is omitted; sentinel mentions `--all-hints`.
        assert "scb/reg10 " not in out
        assert "--all-hints" in out
        assert "5 more" in out

    def test_all_hints_shows_full_list(self):
        cands = [
            self._make("scb", f"reg{i}", f"Reg{i}", f"Reg{i}", "exact", None)
            for i in range(1, 11)
        ]
        out = format_default_slug_hints(cands, all_hints=True)
        assert out is not None
        assert "scb/reg10 " in out
        assert "--all-hints" not in out

    def test_excludes_candidates_already_default(self):
        cands = [
            self._make("scb", "komvux", "X", "X", "exact", "_default"),
            self._make("scb", "kls", "Y", "Y", "exact", None),
        ]
        out = format_default_slug_hints(cands, all_hints=True)
        assert out is not None
        assert "1 single-variant register(s)" in out
        assert "scb/kls" in out
        assert "scb/komvux" not in out
