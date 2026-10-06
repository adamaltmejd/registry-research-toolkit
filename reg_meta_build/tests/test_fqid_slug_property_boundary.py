"""Property cases for populated slugs, asserted on the artifact and the public
reader: any register's auto-derived `variable.slug` values pass the public slug
validator, are unique within the register and rebuild byte-identically; a
name-derived slug respects the length cap and re-derives to itself; a curated
panel key survives TOML -> `populate_slugs` -> `Catalog.list_variants`."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from _slugged_db import add_state, add_variable, build_slugged_db
from hypothesis import assume, example, given, settings, strategies as st
from reg_meta.catalog import Catalog
from reg_meta.fqid import derive_variable_slug, validate_slug

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    populate_slugs,
    populate_variable_slugs,
)

# Small pools make collisions (shared columns, shared names) common; free text
# covers the fold, the unit-parenthetical strip and the underivable arms.
_COLUMNS = ("Kon", "kön", "OBS_VALUE", "...", "states", "3DOMR", "2020", "Alder")
_NAMES = ("Kön", "Ålder", "Värde", "3D-område", "", "Sockerbetor (areal i ha)")
columns = st.one_of(st.sampled_from(_COLUMNS), st.text(max_size=12))
names = st.one_of(st.sampled_from(_NAMES), st.text(max_size=90))
variables = st.lists(
    st.tuples(names, st.lists(columns, min_size=1, max_size=3)), min_size=1, max_size=6
)


def _populate(
    spec: list[tuple[str, list[str]]], slug_dir: Path, first_id: int = 100
) -> dict[int, str]:
    """Register LISA with one variable per spec entry (var_id first_id+i, one era
    per column, earliest first); return var_id -> populated `variable.slug`."""
    conn = build_slugged_db(variable=None)
    for i, (name, cols) in enumerate(spec):
        add_variable(conn, register_id=1, var_id=first_id + i, name=name)
        for era, col in enumerate(cols):
            add_state(
                conn,
                register_id=1,
                var_id=first_id + i,
                register_variant_id=10,
                valid_from=f"{2000 + era}-01-01",
                valid_to=f"{2000 + era}-12-31",
                delivery_column_name=col,
            )
    conn.commit()
    populate_variable_slugs(conn, slug_dir)
    return {
        int(key): slug
        for key, slug in conn.execute(
            "SELECT provider_key, slug FROM variable WHERE register_id = 1"
        )
    }


# Each example builds and slugs a catalog; under machine load it overruns the default deadline.
@settings(deadline=None)
@given(variables)
# Four variables on one shared column and name: the fourth must skip every
# taken `-N` suffix, not just the first.
@example([("Värde", ["OBS_VALUE"])] * 4)
def test_auto_slugs_valid_unique_and_deterministic(
    spec: list[tuple[str, list[str]]],
) -> None:
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        first = _populate(spec, Path(a))
        again = _populate(spec, Path(b))
        auto_a = (Path(a) / f"scb{AUTO_FILE_SUFFIX}").read_bytes()
        auto_b = (Path(b) / f"scb{AUTO_FILE_SUFFIX}").read_bytes()
    assert set(first) == {100 + i for i in range(len(spec))}
    for slug in first.values():
        validate_slug(slug, "variable")
    assert len(set(first.values())) == len(first)
    assert again == first
    assert auto_a == auto_b


# Each example builds and slugs a catalog; under machine load it overruns the default deadline.
@settings(deadline=None)
@given(names)
@example("Utgifter för egen FoU efter finansieringskälla EU ramprogram forskning")
def test_name_slug_capped_and_stable(name: str) -> None:
    # A shared column forces the name arm for both variables.
    assume(not (derive_variable_slug(name) or "").startswith(("annat", "obs-value")))
    spec = [(name, ["OBS_VALUE"]), ("Annat värde", ["OBS_VALUE"])]
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        slug = _populate(spec, Path(a))[100]
        # Re-deriving from the slug itself returns the same slug.
        assert _populate([(slug, ["OBS_VALUE"]), *spec[1:]], Path(b))[100] == slug
    # Over the 60-char cap only when cutting back to a hyphen within the cap
    # would leave an unaddressable slug (a reserved word or a period).
    assert len(slug) <= 60 or derive_variable_slug(slug[:60].rsplit("-", 1)[0]) is None


@given(st.integers(min_value=0, max_value=10**18))
def test_underivable_variable_gets_v_provider_key(var_id: int) -> None:
    # Neither the column nor the name yields a slug (both lead with a digit).
    with tempfile.TemporaryDirectory() as d:
        slugs = _populate([("3D-område", ["3DOMR"])], Path(d), first_id=var_id)
    assert slugs == {var_id: f"v{var_id}"}


_slug = st.from_regex(r"[a-z](?:-?[a-z0-9]){0,10}", fullmatch=True).filter(
    lambda s: derive_variable_slug(s) == s and s != "period"
)
_panel_key = st.one_of(_slug, st.lists(_slug, min_size=1, max_size=4, unique=True))


# Each example builds and slugs a catalog; under machine load it overruns the default deadline.
@settings(deadline=None)
@given(_panel_key, st.one_of(st.just("period"), _panel_key))
def test_panel_keys_round_trip_to_reader(
    entity_key: str | list[str], time_key: str | list[str]
) -> None:
    conn = build_slugged_db()
    conn.execute("UPDATE register SET slug = NULL")
    conn.execute("UPDATE register_variant SET slug = NULL")
    conn.commit()
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "scb.toml").write_text(
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            f"panel_entity_key = {json.dumps(entity_key)}\n"
            f"panel_time_key = {json.dumps(time_key)}\n",
            encoding="utf-8",
        )
        populate_slugs(conn, Path(d), strict=True)
    [variant] = Catalog(conn).list_variants("scb", "lisa")

    def expect(key: str | list[str]) -> str | tuple[str, ...]:
        return tuple(key) if isinstance(key, list) else key

    assert variant.panel_entity_key == expect(entity_key)
    assert variant.panel_time_key == expect(time_key)
