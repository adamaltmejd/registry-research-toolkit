"""Default-slug candidate classification, candidate iteration, and hint formatting for single-variant registers."""

from __future__ import annotations

from _fqid_slug_support import (
    add_single_variant_register as _add_single_variant_register,
)
from _slugged_db import (
    build_slugged_db,
)

from reg_meta_build.fqid_slugs import (
    classify_default_candidate,
    format_default_slug_hints,
    iter_default_slug_candidates,
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


class TestIterDefaultSlugCandidates:
    def test_skips_multi_variant_register(self):
        # The default fixture has one variant; only that register-variant
        # pair should show up (and it's `kept` since names diverge).
        conn = build_slugged_db()
        # Add a second variant under the same register → no longer single-variant.
        conn.execute(
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name) "
            "VALUES (?, ?, ?, ?)",
            (11, 1, "second", "Företag"),
        )
        candidates = list(iter_default_slug_candidates(conn))
        assert candidates == []

    def test_yields_three_classes(self):
        conn = build_slugged_db(
            register=None,
            variant=None,
            version=None,
            variable=None,
            classification=None,
        )
        _add_single_variant_register(
            conn,
            register_id=42,
            register_variant_id=124,
            name="Nybörjare i Komvux",
            variant_name="Nybörjare i Komvux",
            variant_slug="nyborjare-i-komvux",
        )
        _add_single_variant_register(
            conn,
            register_id=60,
            register_variant_id=168,
            name="Konjunkturstatistik, löner för statlig sektor (KLS)",
            variant_name="Konjunkturstatistik, löner för statlig sektor",
            variant_slug=None,
        )
        _add_single_variant_register(
            conn,
            register_id=346,
            register_variant_id=1158,
            name="Hushållens boende",
            variant_name="Individer",
            variant_slug="individer",
        )
        cands = list(iter_default_slug_candidates(conn))
        classes = {c.source_id: c.classification for c in cands}
        assert classes == {
            "42.124": "exact",
            "60.168": "near",
            "346.1158": "kept",
        }
        # Carries current slug so the hint can suppress already-applied rows.
        by_id = {c.source_id: c for c in cands}
        assert by_id["42.124"].current_slug == "nyborjare-i-komvux"
        assert by_id["60.168"].current_slug is None


class TestFormatDefaultSlugHints:
    def _make(
        self,
        provider: str,
        source_id: str,
        register_name: str,
        variant_name: str,
        classification: str,
        current_slug: str | None,
    ):
        from reg_meta_build.fqid_slugs import DefaultSlugCandidate

        return DefaultSlugCandidate(
            provider=provider,
            source_id=source_id,
            register_name=register_name,
            variant_name=variant_name,
            classification=classification,  # type: ignore[arg-type]
            reason="test",
            current_slug=current_slug,
        )

    def test_returns_none_when_no_actionable_candidates(self):
        # Both candidates already carry `_default` → nothing to suggest.
        cands = [
            self._make("scb", "42.124", "X", "X", "exact", "_default"),
            self._make("scb", "50.171", "Y", "Y", "exact", "_default"),
        ]
        assert format_default_slug_hints(cands, all_hints=False) is None

    def test_skips_kept_candidates(self):
        cands = [self._make("scb", "13.20", "A", "B", "kept", None)]
        assert format_default_slug_hints(cands, all_hints=False) is None

    def test_truncated_preview_by_default(self):
        cands = [
            self._make("scb", f"{i}.{i + 100}", f"Reg{i}", f"Reg{i}", "exact", None)
            for i in range(1, 11)
        ]
        out = format_default_slug_hints(cands, all_hints=False)
        assert out is not None
        assert "10 single-variant register(s)" in out
        assert "scb/1.101" in out
        assert "scb/5.105" in out
        # Tail is omitted; sentinel mentions `--all-hints`.
        assert "scb/10.110" not in out
        assert "--all-hints" in out
        assert "5 more" in out

    def test_all_hints_shows_full_list(self):
        cands = [
            self._make("scb", f"{i}.{i + 100}", f"Reg{i}", f"Reg{i}", "exact", None)
            for i in range(1, 11)
        ]
        out = format_default_slug_hints(cands, all_hints=True)
        assert out is not None
        assert "scb/10.110" in out
        assert "--all-hints" not in out

    def test_excludes_candidates_already_default(self):
        cands = [
            self._make("scb", "42.124", "X", "X", "exact", "_default"),
            self._make("scb", "50.171", "Y", "Y", "exact", None),
        ]
        out = format_default_slug_hints(cands, all_hints=True)
        assert out is not None
        assert "1 single-variant register(s)" in out
        assert "scb/50.171" in out
        assert "scb/42.124" not in out
