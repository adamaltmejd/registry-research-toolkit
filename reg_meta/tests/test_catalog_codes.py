"""Catalog code reads: `dense_integer_range` and `classification_codes`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import catalog_test_support
import pytest
from _slugged_db import (
    build_slugged_db,
)
from reg_meta.catalog import (
    Catalog,
    ClassificationCode,
    DenseIntegerRange,
    ValueSetMember,
    dense_integer_range,
)
from reg_meta.errors import RegMetaError

if TYPE_CHECKING:
    import sqlite3

# The shared fixture, bound by assignment: an imported name used only as a
# test parameter reads as an unused import redefined (ruff F401/F811).
slugged_conn = catalog_test_support.slugged_conn


class TestDenseIntegerRange:
    """`dense_integer_range` — the "these codes ARE the integers" test behind
    `ValueSetSummary.integer_range`. Lives here (not in the browser) because the
    verdict now ships in the binding payload: the members it reads are exactly
    what the leaf no longer carries."""

    @staticmethod
    def _members(pairs: list[tuple[str, str]]) -> list[ValueSetMember]:
        return [ValueSetMember(code=c, label=lbl) for c, lbl in pairs]

    def test_contiguous_age_run(self) -> None:
        members = self._members([(str(age), f"{age} år") for age in range(12)])
        assert dense_integer_range(members) == DenseIntegerRange(min=0, max=11)

    def test_small_gaps_are_still_a_run(self) -> None:
        ages = [0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11]
        members = self._members([(str(a), f"{a} år") for a in ages])
        assert dense_integer_range(members) == DenseIntegerRange(min=0, max=11)

    def test_too_few_members_stay_a_table(self) -> None:
        assert (
            dense_integer_range(self._members([(str(a), "") for a in range(9)])) is None
        )

    def test_sparse_scatter_is_not_a_run(self) -> None:
        evens = [(str(v), str(v)) for v in range(0, 20, 2)]
        assert dense_integer_range(self._members(evens)) is None

    def test_leading_zero_codes_are_not_canonical_integers(self) -> None:
        padded = [(f"{v:02d}", str(v)) for v in range(10)]
        assert dense_integer_range(self._members(padded)) is None

    def test_meaningful_labels_stay_a_table(self) -> None:
        # The labels ARE the content — rendering "0-9" would erase them.
        labelled = [(str(v), f"Category {v}") for v in range(10)]
        assert dense_integer_range(self._members(labelled)) is None

    def test_duplicate_values_are_rejected(self) -> None:
        # ` 7` and `7` denote one value with two codes: a set that cannot be a
        # faithful run.
        dupes = [(str(v), str(v)) for v in range(11)] + [(" 7", "7")]
        assert dense_integer_range(self._members(dupes)) is None

    def test_values_beyond_exact_json_round_trip_are_rejected(self) -> None:
        huge = 2**53
        members = self._members([(str(huge + v), str(huge + v)) for v in range(11)])
        assert dense_integer_range(members) is None

    def test_negative_and_alternate_age_phrasings(self) -> None:
        members = self._members(
            [(str(v), f"age {v}" if v % 2 else f"{v} years") for v in range(-5, 6)]
        )
        assert dense_integer_range(members) == DenseIntegerRange(min=-5, max=5)


class TestClassificationCodes:
    """#609: `classification_codes(fqid)` returns ONE edition's value-set codes
    (code-ordered), scoped to the resolved edition (per `classification_id`), with
    the canonical/unknown `is_valid` flag surfaced."""

    @staticmethod
    def _seed_codes(
        conn: sqlite3.Connection,
        slug: str,
        codes: list[tuple[str, str, int | None, int | None]],
    ) -> None:
        """Attach (code, label, level, is_valid) rows to the classification `slug`,
        minting `value_code` rows + `classification_code` links."""
        cls_id = conn.execute(
            "SELECT id FROM classification WHERE slug = ?", (slug,)
        ).fetchone()[0]
        for code, label, level, is_valid in codes:
            code_id = conn.execute(
                "INSERT INTO value_code (code, label) VALUES (?, ?)", (code, label)
            ).lastrowid
            conn.execute(
                "INSERT INTO classification_code "
                "(classification_id, code_id, level, is_valid) VALUES (?, ?, ?, ?)",
                (cls_id, code_id, level, is_valid),
            )
        conn.commit()

    def test_returns_codes_code_ordered_with_validity(self) -> None:
        conn = build_slugged_db()  # seeds the live sun2020 (no codes yet)
        # Inserted out of code order to prove the ORDER BY code.
        self._seed_codes(
            conn,
            "sun2020",
            [
                ("3", "Eftergymnasial", 1, 1),
                ("1", "Förgymnasial", 1, 1),
            ],
        )
        codes = Catalog(conn).classification_codes("class/sun2020")
        assert all(isinstance(c, ClassificationCode) for c in codes)
        assert [(c.code, c.label) for c in codes] == [
            ("1", "Förgymnasial"),
            ("3", "Eftergymnasial"),
        ]
        by_code = {c.code: c for c in codes}
        # is_valid coerces 1 → True for canonical rows.
        assert by_code["1"].is_valid is True
        assert by_code["1"].level == 1

    def test_null_is_valid_stays_none(self) -> None:
        # A classification with no canonical CSV has is_valid=NULL everywhere —
        # surfaced as None (validity unknown), not coerced to False.
        conn = build_slugged_db()
        self._seed_codes(conn, "sun2020", [("1", "Kod", None, None)])
        (code,) = Catalog(conn).classification_codes("class/sun2020")
        assert code.is_valid is None
        assert code.level is None

    def test_empty_when_no_codes(self) -> None:
        conn = build_slugged_db()  # sun2020 with no classification_code rows
        assert Catalog(conn).classification_codes("class/sun2020") == []

    def test_scoped_to_resolved_edition(self) -> None:
        # Codes are per-edition: sun2000's codes must NOT leak into sun2020's list.
        conn = build_slugged_db()
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SUN2000', 'SUN 2000', 'sun2000')"
        )
        self._seed_codes(conn, "sun2000", [("9", "Gammal kod", 1, 1)])
        self._seed_codes(conn, "sun2020", [("1", "Ny kod", 1, 1)])
        assert [
            c.code for c in Catalog(conn).classification_codes("class/sun2020")
        ] == ["1"]
        assert [
            c.code for c in Catalog(conn).classification_codes("class/sun2000")
        ] == ["9"]

    def test_resolves_through_same_as(self) -> None:
        # An alias slug cites its resolved target edition's codes.
        conn = build_slugged_db()  # ships sun2020
        for a_slug, b_slug in (("sun-eqf", "sun2020"), ("sun2020", "sun-eqf")):
            conn.execute(
                "INSERT INTO classification_same_as "
                "(a_provider, a_classification_slug, b_provider, b_classification_slug) "
                "VALUES ('scb', ?, 'scb', ?)",
                (a_slug, b_slug),
            )
        self._seed_codes(conn, "sun2020", [("1", "Kod", 1, 1)])
        codes = Catalog(conn).classification_codes("class/sun-eqf")
        assert [c.code for c in codes] == ["1"]

    def test_raises_on_non_classification_fqid(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).classification_codes("scb/lisa/kon")
        assert exc.value.code == "not_a_classification_fqid"

    def test_raises_on_unknown_classification(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).classification_codes("class/sun2099")
        assert exc.value.code == "fqid_not_found"
