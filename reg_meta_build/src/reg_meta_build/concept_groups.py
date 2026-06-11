"""Derived concept-group layer (#303).

Build-time grouping of near-identical catalog rows into PRESENTATION-ONLY
concept groups, so browse surfaces (webapp catalog, future CLI listings) can
collapse machine-stamped SCB column families into one row with a facet picker.
Identity is untouched: bindings/orders/stats keep leaf FQIDs, a group is not
FQID-addressable, and a wrong group is a cosmetic curation bug — not the
identity corruption that killed identity-level classification folding
(#223 part 2, 195 measured over-folds).

Three dimension sources, derived in priority order (a variable/classification
belongs to AT MOST ONE group — enforced by the member-table PKs; a later pass
never claims an already-grouped member):

0. ``edge`` — connected components of within-register
   ``same_definition_different_column`` sibling edges. Ground truth: the A2.2
   split machinery minted these edges between the delivery columns of ONE SCB
   variable definition, so folding them back into one browse row is
   zero-inference (e.g. ureg's sun2000inr/sun2020inr coding succession; on the
   real corpus ~2,200 components covering ~8,200 variables, 2,191/2,193
   sharing a single name).
1. ``token`` — exact curated vocabularies only (NO regex name-patterns, per
   the standing curation rule): Swedish month slug tails (both the short and
   full forms SCB mixes, e.g. lisa's agi1lonfinkjan…agi1lonfinkdec) for
   variables, and 4-digit vintage-year slug tails (lkf1980…, sni2007) for
   classifications. Guarded — a slug merely ending in "maj" must not fold:
   variables need >= ``_MIN_MONTH_SIBLINGS`` distinct months on one stem AND
   label-prefix agreement; classifications need >= ``_MIN_VINTAGE_SIBLINGS``
   vintages AND year-stripped-name agreement.
2. ``curated`` — maintainer TOML (``reg_meta_build/concept_groups.toml``):
   exact member lists that absorb token groups under an extra facet axis (the
   LISA agi{1,2,3} rank facet → one month × rank matrix) or attach single
   variables. Fails fast on unresolvable references (EXIT_CONFIG).

When the interval-native model (#271) lands its column→variable merges, the
month groups graduate into real single variables and this layer shrinks to
edge/rank/vintage duty.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

from ._components import DisjointSet
from ._curation import curation_error, load_curation_entries

# ── token vocabularies + guards ─────────────────────────────────────────────

# Exact slug-tail token → month number. SCB mixes short and full forms within
# one family (lisa agi1lonfink: jan, feb, mars, april, maj, juni, juli, aug,
# sep, okt, nov, dec), so both vocabularies are listed. No token is a suffix
# of another, so a slug tail matches at most one token. Exact strings only —
# no regex name-patterns (standing curation rule).
_MONTH_TOKENS: dict[str, int] = {
    "jan": 1,
    "januari": 1,
    "feb": 2,
    "februari": 2,
    "mar": 3,
    "mars": 3,
    "apr": 4,
    "april": 4,
    "maj": 5,
    "jun": 6,
    "juni": 6,
    "jul": 7,
    "juli": 7,
    "aug": 8,
    "augusti": 8,
    "sep": 9,
    "september": 9,
    "okt": 10,
    "oktober": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
# Canonical display label per month (the facet `label`; the facet `value` is
# the zero-padded number so lexicographic ordering is chronological).
_MONTH_LABELS: dict[int, str] = {
    1: "januari",
    2: "februari",
    3: "mars",
    4: "april",
    5: "maj",
    6: "juni",
    7: "juli",
    8: "augusti",
    9: "september",
    10: "oktober",
    11: "november",
    12: "december",
}

# Month-fold guards: >= 3 distinct month siblings on one stem AND a shared
# label prefix of >= 5 chars after trimming. A coincidental "…maj" slug has no
# sibling months and no shared label, so it never folds. Verified against the
# real corpus (2026-06-11): 8 groups / 96 variables, zero false folds.
_MIN_MONTH_SIBLINGS = 3
_MIN_LABEL_PREFIX = 5

# Classification vintage families need only 2 members (the catalog is tiny and
# curated — agarkat2000/2020, ssyk1996/2012 are genuine 2-vintage families)
# but the STRONGER name guard: every member's name must contain its vintage
# year, and the year-stripped names must all be identical.
_MIN_VINTAGE_SIBLINGS = 2
_VINTAGE_YEARS = range(1900, 2100)

_EDGE_RELATION_KIND = "same_definition_different_column"


# ── curated TOML ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedMember:
    """One member of a curated family: EITHER a derived token group to absorb
    (`group` = its stem key; absorbed variables keep their month facets and
    gain the family's facet) OR a single variable slug. `value`/`label` are
    this member's facet on the family's `axis`."""

    group: str | None
    variable: str | None
    value: str
    label: str


@dataclass(frozen=True)
class CuratedGroup:
    """One `[[variable_group]]` family from `concept_groups.toml`."""

    provider: str
    register: str
    key: str
    label: str
    axis: str
    members: tuple[CuratedMember, ...]


def repo_concept_groups_path() -> Path | None:
    """`reg_meta_build/concept_groups.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir
    is glob-loaded as provider-slug TOMLs; a file there would break the
    build)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "concept_groups.toml"
    return candidate if candidate.is_file() else None


def _require_str(entry: dict, field: str, context: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise curation_error(
            "concept_groups_invalid",
            f"concept_groups {context} needs `{field}` as a non-empty string, "
            f"got {value!r}.",
            f'Give `{field} = "<value>"` in reg_meta_build/concept_groups.toml.',
        )
    return value


def load_concept_groups(path: Path | None) -> tuple[CuratedGroup, ...]:
    """Parse the curated-family TOML. Empty when no file (synthetic test
    builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only
    `[[variable_group]]` top-level; `register` is a 2-segment
    `provider/register` FQID string; `key`/`label`/`axis` non-empty strings;
    each member sets exactly one of `group`/`variable` plus `value`/`label`;
    keys are unique. Reference RESOLUTION (register/group/variable exist)
    happens at materialize time against the built DB, not here."""
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="variable_group",
        label="concept-group",
        prefix="concept_groups",
        code_base="concept_groups",
        file_name="concept_groups.toml",
        entry_fields="register / key / label / axis / members",
    )
    out: list[CuratedGroup] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register", "[[variable_group]]")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups register {register_fqid!r} must be a 2-segment "
                "`provider/register` FQID.",
                'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
            )
        key = _require_str(entry, "key", "[[variable_group]]")
        label = _require_str(entry, "label", "[[variable_group]]")
        axis = _require_str(entry, "axis", "[[variable_group]]")
        scope_key = (parts[0], parts[1], key)
        if scope_key in seen_keys:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups duplicate key {key!r} under {register_fqid}.",
                "Group keys must be unique per register.",
            )
        seen_keys.add(scope_key)
        raw_members = entry.get("members", [])
        if not isinstance(raw_members, list) or not raw_members:
            raise curation_error(
                "concept_groups_invalid",
                f"concept_groups group {key!r} needs a non-empty "
                "`[[variable_group.members]]` array.",
                "List the family's members as `[[variable_group.members]]` tables.",
            )
        members: list[CuratedMember] = []
        seen_refs: set[tuple[str, str]] = set()
        for raw in raw_members:
            if not isinstance(raw, dict):
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} member {raw!r} must be a table.",
                    "Each member is a `[[variable_group.members]]` table.",
                )
            group_ref = raw.get("group")
            variable_ref = raw.get("variable")
            if (group_ref is None) == (variable_ref is None):
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} member {raw!r} must set exactly "
                    "one of `group` / `variable`.",
                    'Reference a derived token group (`group = "<stem>"`) or a '
                    'single variable (`variable = "<slug>"`), not both/neither.',
                )
            ref_field = "group" if group_ref is not None else "variable"
            ref = _require_str(raw, ref_field, f"group {key!r} member")
            if (ref_field, ref) in seen_refs:
                raise curation_error(
                    "concept_groups_invalid",
                    f"concept_groups group {key!r} references {ref_field} {ref!r} "
                    "twice.",
                    "List each member reference once.",
                )
            seen_refs.add((ref_field, ref))
            members.append(
                CuratedMember(
                    group=ref if ref_field == "group" else None,
                    variable=ref if ref_field == "variable" else None,
                    value=_require_str(raw, "value", f"group {key!r} member"),
                    label=_require_str(raw, "label", f"group {key!r} member"),
                )
            )
        out.append(
            CuratedGroup(
                provider=parts[0],
                register=parts[1],
                key=key,
                label=label,
                axis=axis,
                members=tuple(members),
            )
        )
    return tuple(out)


# ── derivation passes ───────────────────────────────────────────────────────


def _insert_group(
    conn: sqlite3.Connection,
    *,
    kind: str,
    register_id: int | None,
    group_key: str,
    label: str,
    source: str,
) -> int:
    cur = conn.execute(
        "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
        "VALUES (?, ?, ?, ?, ?)",
        (kind, register_id, group_key, label, source),
    )
    group_id = cur.lastrowid
    assert group_id is not None
    return group_id


def _derive_edge_groups(conn: sqlite3.Connection) -> int:
    """Dimension 0: one group per connected component of within-register
    `same_definition_different_column` edges. The edges are slug-anchored and
    stored both directions; resolve both endpoints back to variable_ids and
    union-find the components. Cross-register edges of this kind don't occur
    (the A2.2 split is register-local), but the WHERE guard keeps a future
    curated cross-register edge from minting an unrepresentable group."""
    rows = conn.execute(
        "SELECT va.variable_id AS a_id, vb.variable_id AS b_id "
        "FROM variable_related_to e "
        "JOIN provider pa ON pa.slug = e.a_provider "
        "JOIN register ra ON ra.provider_id = pa.provider_id "
        "  AND ra.slug = e.a_register "
        "JOIN variable va ON va.register_id = ra.register_id "
        "  AND va.slug = e.a_variable "
        "JOIN provider pb ON pb.slug = e.b_provider "
        "JOIN register rb ON rb.provider_id = pb.provider_id "
        "  AND rb.slug = e.b_register "
        "JOIN variable vb ON vb.register_id = rb.register_id "
        "  AND vb.slug = e.b_variable "
        "WHERE e.relation_kind = ? AND va.register_id = vb.register_id",
        (_EDGE_RELATION_KIND,),
    ).fetchall()
    if not rows:
        return 0

    ds: DisjointSet[int] = DisjointSet()
    for r in rows:
        a, b = r[0], r[1]
        ds.add(a)
        ds.add(b)
        ds.union(a, b)

    # Full slugged-variable scan rather than an IN(...) over the ~8k involved
    # ids — bounded (~50k rows on the real corpus) and immune to SQLite's
    # host-parameter cap.
    meta = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT variable_id, register_id, slug, name FROM variable "
            "WHERE slug IS NOT NULL"
        )
    }
    components = ds.components()

    # Deterministic order: by (register_id, min member slug). Key = min member
    # slug (components are disjoint, so it's scope-unique); label = the
    # min-slug member's name (the shared name on 2,191/2,193 real components),
    # falling back to the key itself.
    prepared = []
    for member_ids in components.values():
        members = sorted(member_ids, key=lambda v: meta[v][1])
        register_id = meta[members[0]][0]
        key = meta[members[0]][1]
        label = meta[members[0]][2] or key
        prepared.append((register_id, key, label, members))
    for register_id, key, label, members in sorted(
        prepared, key=lambda p: (p[0], p[1])
    ):
        group_id = _insert_group(
            conn,
            kind="variable",
            register_id=register_id,
            group_key=key,
            label=label,
            source="edge",
        )
        conn.executemany(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            [(vid, group_id) for vid in members],
        )
    return len(prepared)


def _common_prefix(strings: list[str]) -> str:
    prefix = strings[0]
    for s in strings[1:]:
        while not s.startswith(prefix):
            prefix = prefix[:-1]
    return prefix


def _trim_label(prefix: str) -> str:
    """Common label prefix → group label: trim trailing whitespace/punctuation
    and a dangling Swedish connector "i" ("Löne- eller företagarinkomst i
    {månad}…" → "Löne- eller företagarinkomst")."""
    trimmed = prefix.strip().rstrip(",;:").removesuffix(" i")
    return trimmed.strip().rstrip(",;:")


def _derive_month_groups(conn: sqlite3.Connection, warn: Callable[[str], None]) -> int:
    """Dimension 1 (variables): fold month-suffixed slug families. Candidates
    are ungrouped slugged variables whose slug ends in a month token; a
    (register, stem) family folds only past BOTH guards (>= 3 distinct months
    AND a usable shared label prefix). Members get a `month` facet. A family
    dropped on a group-key collision is reported via `warn` (never silent)."""
    rows = conn.execute(
        "SELECT v.variable_id, v.register_id, v.slug, v.name FROM variable v "
        "WHERE v.slug IS NOT NULL AND NOT EXISTS "
        "  (SELECT 1 FROM concept_group_variable m "
        "   WHERE m.variable_id = v.variable_id) "
        "ORDER BY v.register_id, v.slug"
    ).fetchall()
    # (register_id, stem) → [(month, slug, variable_id, name)]
    candidates: dict[tuple[int, str], list[tuple[int, str, int, str | None]]] = {}
    for variable_id, register_id, slug, name in rows:
        for token, month in _MONTH_TOKENS.items():
            if slug.endswith(token) and len(slug) > len(token):
                stem = slug[: -len(token)]
                candidates.setdefault((register_id, stem), []).append(
                    (month, slug, variable_id, name)
                )
                break  # no month token is a suffix of another — single match
    existing_keys = {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT register_id, group_key FROM concept_group WHERE kind = 'variable'"
        )
    }
    n_groups = 0
    for (register_id, stem), members in sorted(candidates.items()):
        if len({m[0] for m in members}) < _MIN_MONTH_SIBLINGS:
            continue
        names = [m[3] for m in members]
        if any(n is None for n in names):
            continue  # no labels to agree on — conservative skip
        label = _trim_label(_common_prefix([n for n in names if n is not None]))
        if len(label) < _MIN_LABEL_PREFIX:
            continue
        if (register_id, stem) in existing_keys:
            # An edge group already claimed this key (its min member slug
            # equals the stem). Cosmetic collision on a presentation key —
            # skip the fold rather than fail the build, but say so: the family
            # then renders as ~12 flat near-identical rows (the very symptom
            # #303 fixes) and the corpus floors only count totals, so without
            # this line the loss is invisible to the maintainer.
            warn(
                f"  WARN concept-groups: month family {stem!r} "
                f"(register_id {register_id}, {len(members)} variables) NOT "
                "folded — its stem collides with an existing group key"
            )
            continue
        group_id = _insert_group(
            conn,
            kind="variable",
            register_id=register_id,
            group_key=stem,
            label=label,
            source="token",
        )
        ordered = sorted(members)
        conn.executemany(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            [(vid, group_id) for _, _, vid, _ in ordered],
        )
        conn.executemany(
            "INSERT INTO concept_group_variable_facet (variable_id, axis, value, "
            "label) VALUES (?, 'month', ?, ?)",
            [
                (vid, f"{month:02d}", _MONTH_LABELS[month])
                for month, _, vid, _ in ordered
            ],
        )
        n_groups += 1
    return n_groups


def _derive_classification_vintage_groups(conn: sqlite3.Connection) -> int:
    """Dimension 1 (classifications): fold 4-digit vintage-year slug tails
    (lkf1980…lkf2026, sni2002/sni2007, agarkat2000/2020, …). Guard: every
    member's name contains its vintage year AND the year-stripped names all
    agree — that agreed name is the group label. Members carry the year as
    their single facet."""
    rows = conn.execute(
        "SELECT id, slug, name FROM classification WHERE slug IS NOT NULL ORDER BY slug"
    ).fetchall()
    # stem → [(year, slug, classification_id, name)]
    families: dict[str, list[tuple[int, str, int, str]]] = {}
    for cls_id, slug, name in rows:
        tail = slug[-4:]
        if len(slug) < 5 or not tail.isdigit():
            continue
        year = int(tail)
        stem = slug[:-4]
        # A digit-ending stem means the tail splits a longer number — not a
        # vintage year.
        if year not in _VINTAGE_YEARS or stem[-1].isdigit():
            continue
        families.setdefault(stem, []).append((year, slug, cls_id, name))
    n_groups = 0
    for stem in sorted(families):
        members = sorted(families[stem])
        if len(members) < _MIN_VINTAGE_SIBLINGS:
            continue
        stripped_names: set[str] = set()
        ok = True
        for year, _slug, _cls_id, name in members:
            if not name or str(year) not in name:
                ok = False
                break
            stripped_names.add(" ".join(name.replace(str(year), "", 1).split()))
        if not ok or len(stripped_names) != 1:
            continue
        label = next(iter(stripped_names))
        if not label:
            continue
        group_id = _insert_group(
            conn,
            kind="classification",
            register_id=None,
            group_key=stem,
            label=label,
            source="token",
        )
        conn.executemany(
            "INSERT INTO concept_group_classification "
            "(classification_id, group_id, facet_value, facet_label) "
            "VALUES (?, ?, ?, ?)",
            [
                (cls_id, group_id, f"{year:04d}", str(year))
                for year, _slug, cls_id, _name in members
            ],
        )
        n_groups += 1
    return n_groups


def _apply_curated_groups(
    conn: sqlite3.Connection, groups: tuple[CuratedGroup, ...]
) -> int:
    """Dimension 2: curated families. A `group` member absorbs a derived token
    group (its variables move over, keeping their month facets, and gain the
    family facet; the absorbed group row is deleted); a `variable` member
    attaches one ungrouped variable. Every dangling reference fails the build
    (EXIT_CONFIG) — curation drift must be fixed, not silently dropped."""
    n_groups = 0
    for g in groups:
        ctx = f"[[variable_group]] {g.key!r} ({g.provider}/{g.register})"
        reg = conn.execute(
            "SELECT r.register_id FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ?",
            (g.provider, g.register),
        ).fetchone()
        if reg is None:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: register does not resolve.",
                "Fix the `register` FQID in reg_meta_build/concept_groups.toml.",
            )
        register_id = reg[0]
        try:
            group_id = _insert_group(
                conn,
                kind="variable",
                register_id=register_id,
                group_key=g.key,
                label=g.label,
                source="curated",
            )
        except sqlite3.IntegrityError as exc:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: key collides with a derived group in the same register.",
                "Pick a curated `key` that no edge/token group already uses.",
            ) from exc
        n_members = 0
        for m in g.members:
            if m.group is not None:
                row = conn.execute(
                    "SELECT group_id FROM concept_group WHERE kind = 'variable' "
                    "AND register_id = ? AND group_key = ? AND source = 'token'",
                    (register_id, m.group),
                ).fetchone()
                if row is None:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: member references derived token group "
                        f"{m.group!r}, which this build did not derive (slug "
                        "rename, or the family no longer passes the fold guards).",
                        "Update the member's `group` stem in "
                        "reg_meta_build/concept_groups.toml.",
                    )
                absorbed_id = row[0]
                member_ids = [
                    r[0]
                    for r in conn.execute(
                        "SELECT variable_id FROM concept_group_variable "
                        "WHERE group_id = ? ORDER BY variable_id",
                        (absorbed_id,),
                    )
                ]
                conn.execute(
                    "UPDATE concept_group_variable SET group_id = ? WHERE group_id = ?",
                    (group_id, absorbed_id),
                )
                conn.execute(
                    "DELETE FROM concept_group WHERE group_id = ?", (absorbed_id,)
                )
                try:
                    conn.executemany(
                        "INSERT INTO concept_group_variable_facet "
                        "(variable_id, axis, value, label) VALUES (?, ?, ?, ?)",
                        [(vid, g.axis, m.value, m.label) for vid in member_ids],
                    )
                except sqlite3.IntegrityError as exc:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: axis {g.axis!r} collides with a facet the "
                        f"absorbed group {m.group!r} already assigned.",
                        "Pick a distinct `axis` for the curated family.",
                    ) from exc
                n_members += len(member_ids)
            else:
                var = conn.execute(
                    "SELECT variable_id FROM variable "
                    "WHERE register_id = ? AND slug = ?",
                    (register_id, m.variable),
                ).fetchone()
                if var is None:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: member variable {m.variable!r} does not resolve "
                        "in that register.",
                        "Fix the member's `variable` slug in "
                        "reg_meta_build/concept_groups.toml.",
                    )
                try:
                    conn.execute(
                        "INSERT INTO concept_group_variable (variable_id, group_id) "
                        "VALUES (?, ?)",
                        (var[0], group_id),
                    )
                except sqlite3.IntegrityError as exc:
                    raise curation_error(
                        "concept_groups_unresolved",
                        f"{ctx}: member variable {m.variable!r} already belongs "
                        "to an edge/token group — absorb that group instead.",
                        'Reference the derived group via `group = "<stem>"`.',
                    ) from exc
                conn.execute(
                    "INSERT INTO concept_group_variable_facet "
                    "(variable_id, axis, value, label) VALUES (?, ?, ?, ?)",
                    (var[0], g.axis, m.value, m.label),
                )
                n_members += 1
        if n_members < 2:
            raise curation_error(
                "concept_groups_unresolved",
                f"{ctx}: resolves to {n_members} member variable(s); a group "
                "needs >= 2.",
                "A single-member family is not a group — remove it or add members.",
            )
        n_groups += 1
    return n_groups


def materialize_concept_groups(
    conn: sqlite3.Connection,
    curated: tuple[CuratedGroup, ...] = (),
    *,
    providers: frozenset[str] = frozenset(),
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Derive the concept-group tables (#303). Ordering contract: runs after
    `populate_variable_slugs` + `_materialize_variable_related_to` (the edge
    pass resolves slug-anchored edges) and after `populate_slugs` /
    `populate_classifications` (classification slugs + rows). `providers`
    gates curated entries to the providers in this build (mirrors
    `populate_classifications`' provider gate, so a `--providers=sos` build
    doesn't fail on an scb family)."""
    _derive_edge_groups(conn)
    _derive_month_groups(conn, warn or (lambda _msg: None))
    _derive_classification_vintage_groups(conn)
    _apply_curated_groups(conn, tuple(g for g in curated if g.provider in providers))
    # Recount from the final table — a curated absorb DELETEs its token
    # groups, so per-pass return values would over-report the shipped state.
    by_bucket = {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            "SELECT source, kind, COUNT(*) FROM concept_group GROUP BY source, kind"
        )
    }
    counts = {
        "edge_groups": by_bucket.get(("edge", "variable"), 0),
        "month_groups": by_bucket.get(("token", "variable"), 0),
        "vintage_groups": by_bucket.get(("token", "classification"), 0),
        "curated_groups": by_bucket.get(("curated", "variable"), 0),
    }
    counts["grouped_variables"] = conn.execute(
        "SELECT COUNT(*) FROM concept_group_variable"
    ).fetchone()[0]
    counts["grouped_classifications"] = conn.execute(
        "SELECT COUNT(*) FROM concept_group_classification"
    ).fetchone()[0]
    return counts
