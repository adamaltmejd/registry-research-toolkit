"""Curated identity (`same_as`) edges + a candidate-inference generator (#417).

`variable_same_as` asserts that two variables are the SAME definition — one
concept, two FQIDs. Unlike `variable_related_to` (a weak "see also"), a same_as
edge is **resolver-load-bearing**: `Catalog.resolve` follows it transitively, and
the build cycle-checks the as-declared graph. A wrong edge therefore corrupts
resolution, so this surface is split into two halves with a human gate between:

  - The CURATED loader (`load_same_as` + `materialize_curated_same_as_edges`,
    merged into `materialize_same_as_edges` in `fqid_slugs.py`). Like
    `variable_related_to.toml` / `classification_links.toml` the file ships
    EMPTY — only confirmed identity ever loads into a build. NOTHING
    auto-materializes.
  - The GENERATOR (`infer_same_as_candidates` + `render_candidates_toml`, driven
    by `reg-meta-build same-as-candidates`). It reads structured signals off a
    BUILT DB — shared classification, shared value set, name agreement — and
    emits a tiered review worklist a maintainer curates into the curated file by
    hand. The generator never writes the curated file and never touches the DB.

The generator's output schema (a `[[same_as]]` TOML with `a`/`b`/`note`) is
exactly the curated loader's input schema, so a confirmed candidate is copied
across verbatim. They are co-located here for that reason.

Like the other curation TOMLs (`variable_related_to.toml`, `concept_groups.toml`)
the curated file is a maintainer artifact — absent in wheel installs and
synthetic test builds.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ._curation import curation_error, load_curation_entries

if TYPE_CHECKING:
    import sqlite3


@dataclass(frozen=True)
class CuratedSameAs:
    """One `[[same_as]]` identity edge from `variable_same_as.toml`: an unordered
    pair of variable FQIDs (`a_*` / `b_*`, each a 3-segment
    provider/register/variable), plus an optional `note`. Unlike
    `CuratedRelatedTo` there is NO `relation_kind` — same_as has no kind
    vocabulary; identity is identity. Endpoint resolution (provider+register
    exist) happens at materialize time against the built DB, not at load; the
    variable slug is NOT validated — same_as is slug-anchored and survives
    renames, consistent with the inline `same_as` path."""

    a_provider: str
    a_register: str
    a_variable: str
    b_provider: str
    b_register: str
    b_variable: str
    note: str | None


def repo_variable_same_as_path() -> Path | None:
    """`reg_meta_build/variable_same_as.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir is
    glob-loaded as provider-slug TOMLs; a file there would break the build)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "variable_same_as.toml"
    return candidate if candidate.is_file() else None


def _require_fqid(entry: dict, field: str) -> tuple[str, str, str]:
    """Parse a `provider/register/variable` 3-segment FQID string (mirrors
    `variable_related_to._require_fqid`)."""
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise curation_error(
            "variable_same_as_invalid",
            f"variable_same_as [[same_as]] needs `{field}` as a non-empty "
            f"string, got {value!r}.",
            f'Give `{field} = "scb/lisa/<variable>"`-style 3-segment FQIDs in '
            "reg_meta_build/variable_same_as.toml.",
        )
    parts = value.split("/")
    if len(parts) != 3 or not all(parts):
        raise curation_error(
            "variable_same_as_invalid",
            f"variable_same_as {field} {value!r} must be a 3-segment "
            "`provider/register/variable` FQID.",
            'Give `a = "scb/lisa/<variable>"`-style 3-segment FQIDs.',
        )
    return (parts[0], parts[1], parts[2])


def load_same_as(path: Path | None) -> tuple[CuratedSameAs, ...]:
    """Parse the curated identity TOML. Empty when no file (synthetic test
    builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): only `[[same_as]]`
    top-level; `a`/`b` are 3-segment `provider/register/variable` FQID strings;
    no self-edge (a == b); no duplicate UNORDERED FQID pair within the file;
    `note` optional but non-empty if present. There is NO relation_kind check —
    same_as carries no kind.

    Endpoint RESOLUTION (both providers/registers exist in the built DB) and the
    SHARED inline+curated cycle check happen at materialize time, not here — the
    same load/resolve split as `variable_related_to` / `concept_groups`."""
    entries = load_curation_entries(
        path,
        entry_key="same_as",
        label="variable-same-as",
        prefix="variable_same_as",
        code_base="variable_same_as",
        file_name="variable_same_as.toml",
        entry_fields="a / b",
    )
    out: list[CuratedSameAs] = []
    # Unordered FQID pairs already seen — frozenset of the two 3-tuples, so the
    # same pair in either a/b order collides. A duplicate is curation drift.
    seen_pairs: set[frozenset[tuple[str, str, str]]] = set()
    for entry in entries:
        a = _require_fqid(entry, "a")
        b = _require_fqid(entry, "b")
        if a == b:
            raise curation_error(
                "variable_same_as_invalid",
                f"variable_same_as entry relates {'/'.join(a)} to itself.",
                "A same_as edge connects two DISTINCT variable FQIDs; remove the "
                "self-edge.",
            )
        pair = frozenset({a, b})
        if pair in seen_pairs:
            raise curation_error(
                "variable_same_as_invalid",
                f"variable_same_as has a duplicate unordered pair "
                f"{{{'/'.join(a)}, {'/'.join(b)}}}.",
                "List each variable pair once (the edge is symmetric — a→b and "
                "b→a are the same pair).",
            )
        seen_pairs.add(pair)
        note = entry.get("note")
        if note is not None and (not isinstance(note, str) or not note):
            raise curation_error(
                "variable_same_as_invalid",
                f"variable_same_as entry {entry!r} `note` must be a non-empty "
                f"string when present, got {note!r}.",
                "Drop `note` or give it a non-empty value like "
                '`note = "candidate:tier1"`.',
            )
        out.append(
            CuratedSameAs(
                a_provider=a[0],
                a_register=a[1],
                a_variable=a[2],
                b_provider=b[0],
                b_register=b[1],
                b_variable=b[2],
                note=note,
            )
        )
    return tuple(out)


# ---------------------------------------------------------------------------
# Candidate inference (the `same-as-candidates` generator)
# ---------------------------------------------------------------------------

# Default value-set code-count floor: a shared generic hub (Ja/Nej = 2 codes) is
# meaningless evidence at ANY tier; a shared ≥15-code value set is a strong
# de-ambiguator that the value set genuinely matches.
_DEFAULT_MIN_VALUE_SET_CODES = 15

# Default per-signal distinct-REGISTER fanout cap. A signal (classification or
# value set) spanning more registers than this is a HUB — it generates O(N²)
# cross-register pairs that swamp the worklist (measured: an uncapped pass over
# the real corpus emits 53,368 candidates, mostly from hub classifications like
# SNI/LKF). A hub's pairs are suppressed UNLESS the two variables' names agree
# (the exemption keeps the strong name-corroborated pairs — a bare cap would drop
# ~17,720 of them: kommun/län/näringsgren/utbildningsnivå). Set to <=0 (CLI) /
# None (API) to disable (= ∞, the uncapped behaviour).
_DEFAULT_MAX_SIGNAL_FANOUT = 12


@dataclass(frozen=True)
class Candidate:
    """One inferred same_as candidate: an unordered cross-register variable pair
    (`a_fqid`/`b_fqid` sorted as "provider/register/variable" strings), the
    STRONGEST tier it qualifies for, and a short human `evidence` string for the
    review comment (e.g. "shared classification ICD-10-SE + value_set 12664 +
    name KÖN")."""

    a_fqid: str
    b_fqid: str
    tier: int
    evidence: str


@dataclass(frozen=True)
class InferenceResult:
    """The generator's output: the emitted `candidates` plus `hub_suppressed` —
    the count of pairs the fanout cap removed from the final output (generated
    from a hub signal, no name agreement, and not otherwise admitted by a
    non-hub signal). Surfaced so the cap is never a SILENT truncation
    (CLAUDE.md)."""

    candidates: list[Candidate]
    hub_suppressed: int


@dataclass(frozen=True)
class _VarSignals:
    """Per-variable inference signals read off the built DB."""

    fqid: str
    register_id: int
    norm_name: str | None  # TRIM+casefold of variable.name; None when name NULL
    classification_ids: frozenset[int]
    value_set_ids: frozenset[int]
    # value_set_id → whether ANY of this variable's states for that value set
    # carries a classification_id (so a Tier-4 shared set can require BOTH sides
    # classification-NULL on that set).
    value_set_classified: dict[int, bool]


def _load_var_signals(conn: sqlite3.Connection) -> dict[str, _VarSignals]:
    """Read per-variable identity + classification/value-set signals for every
    slugged variable. Keyed by FQID. `norm_name` is None when the name is NULL
    (so a NULL==NULL name never counts as agreement)."""
    rows = conn.execute(
        "SELECT v.variable_id, v.register_id, v.name, "
        "p.slug AS provider_slug, r.slug AS register_slug, v.slug AS variable_slug "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND p.slug IS NOT NULL"
    ).fetchall()

    by_var_id: dict[int, dict] = {}
    for row in rows:
        name = row["name"]
        norm_name = (
            name.strip().lower() if isinstance(name, str) and name.strip() else None
        )
        fqid = f"{row['provider_slug']}/{row['register_slug']}/{row['variable_slug']}"
        by_var_id[row["variable_id"]] = {
            "fqid": fqid,
            "register_id": row["register_id"],
            "norm_name": norm_name,
            "classification_ids": set(),
            "value_set_ids": set(),
            "value_set_classified": {},
        }

    # State-level signals: which classifications and value sets each variable's
    # states carry, and whether a given value_set is classified on that side.
    for vid, value_set_id, classification_id in conn.execute(
        "SELECT variable_id, value_set_id, classification_id FROM variable_state"
    ):
        acc = by_var_id.get(vid)
        if acc is None:
            continue
        if classification_id is not None:
            acc["classification_ids"].add(classification_id)
        if value_set_id is not None:
            acc["value_set_ids"].add(value_set_id)
            prior = acc["value_set_classified"].get(value_set_id, False)
            acc["value_set_classified"][value_set_id] = prior or (
                classification_id is not None
            )

    return {
        acc["fqid"]: _VarSignals(
            fqid=acc["fqid"],
            register_id=acc["register_id"],
            norm_name=acc["norm_name"],
            classification_ids=frozenset(acc["classification_ids"]),
            value_set_ids=frozenset(acc["value_set_ids"]),
            value_set_classified=acc["value_set_classified"],
        )
        for acc in by_var_id.values()
    }


def _load_value_set_code_counts(conn: sqlite3.Connection) -> dict[int, int]:
    return dict(
        conn.execute(
            "SELECT value_set_id, COUNT(*) FROM value_set_member GROUP BY value_set_id"
        )
    )


def _load_classification_short_names(conn: sqlite3.Connection) -> dict[int, str]:
    return dict(conn.execute("SELECT id, short_name FROM classification"))


def _load_existing_edges(conn: sqlite3.Connection) -> set[frozenset[str]]:
    """Unordered FQID pairs already present in `variable_same_as` — excluded from
    candidates so the worklist only surfaces NEW pairs."""
    existing: set[frozenset[str]] = set()
    for ap, ar, av, bp, br, bv in conn.execute(
        "SELECT a_provider, a_register, a_variable, "
        "b_provider, b_register, b_variable FROM variable_same_as"
    ):
        existing.add(frozenset({f"{ap}/{ar}/{av}", f"{bp}/{br}/{bv}"}))
    return existing


def _tier_and_evidence(
    a: _VarSignals,
    b: _VarSignals,
    *,
    code_counts: dict[int, int],
    class_names: dict[int, str],
    min_value_set_codes: int,
) -> tuple[int, str] | None:
    """Score one cross-register pair to its STRONGEST qualifying tier, or None.

    Tier 1: shared classification ∧ shared value set ∧ name agreement.
    Tier 2: shared classification ∧ name agreement (value sets may differ).
    Tier 3: shared classification ∧ shared value set (names differ).
    Tier 4: shared value set with ≥min_value_set_codes codes that is
            classification-NULL on BOTH sides — value-set-only identity the
            classification linkage (#416) hasn't reached yet.

    A shared value set counts as a corroborating signal at ANY tier ONLY when its
    code count ≥ `min_value_set_codes`. A small/generic set (e.g. a 2-code Ja/Nej
    hub) is not evidence: a pair that ALSO shares a classification already scores
    Tier 2 (via name) or is dropped, and the small value set never lifts it to
    Tier 1/3. This de-hubs the 2-code value sets without losing a pair — it only
    re-tiers it. Tier 4 already floored on the same constant, so its semantics are
    unchanged."""
    shared_class = a.classification_ids & b.classification_ids
    # Only value sets at/above the floor corroborate (tiers 1 & 3). Small sets
    # are dropped from the signal entirely — see docstring.
    shared_vsids = {
        vsid
        for vsid in (a.value_set_ids & b.value_set_ids)
        if code_counts.get(vsid, 0) >= min_value_set_codes
    }
    name_agree = a.norm_name is not None and a.norm_name == b.norm_name

    parts: list[str] = []
    if shared_class:
        names = sorted(class_names.get(c, str(c)) for c in shared_class)
        parts.append(f"classification {'/'.join(names)}")
    if shared_vsids:
        parts.append(f"value_set {'/'.join(str(v) for v in sorted(shared_vsids))}")
    if name_agree:
        parts.append(f"name {a.norm_name!r}")

    if shared_class and shared_vsids and name_agree:
        return 1, "shared " + " + ".join(parts)
    if shared_class and name_agree:
        return 2, "shared " + " + ".join(parts)
    if shared_class and shared_vsids:
        return 3, "shared " + " + ".join(parts)

    # Tier 4: a shared (floored) value set that is classification-NULL on both
    # sides (the #416 linkage hasn't bound it to a classification).
    tier4_sets = [
        vsid
        for vsid in shared_vsids
        if not a.value_set_classified.get(vsid, False)
        and not b.value_set_classified.get(vsid, False)
    ]
    if tier4_sets:
        joined = "/".join(str(v) for v in sorted(tier4_sets))
        return 4, (
            f"shared classification-NULL value_set {joined} "
            f"(≥{min_value_set_codes} codes)"
        )
    return None


def _distinct_register_fanout(
    members: list[str], signals: dict[str, _VarSignals]
) -> int:
    """Distinct registers among a signal group's member variables — the
    cross-register fanout that drives O(N²) pairs (a high-fanout signal is a
    hub)."""
    return len({signals[f].register_id for f in members})


def infer_same_as_candidates(
    conn: sqlite3.Connection,
    *,
    max_tier: int = 4,
    min_value_set_codes: int = _DEFAULT_MIN_VALUE_SET_CODES,
    max_signal_fanout: int | None = _DEFAULT_MAX_SIGNAL_FANOUT,
) -> InferenceResult:
    """Infer cross-register `same_as` candidates from structured signals on a
    BUILT DB. Each unordered cross-register pair (different `register_id`) is
    emitted ONCE at its strongest qualifying tier; pairs already in
    `variable_same_as` are excluded. NEVER mutates the DB — this is a read-only
    worklist generator; only the curated file's confirmed entries ever load.

    Generates candidate pairs by grouping variables on shared signals (cheaper
    and clearer than an O(n²) self-join, and the linked set is small):
    classification_id → cross-register pairs feed tiers 1-3; value_set_id →
    cross-register pairs feed tiers 3-4. Only value sets with code count ≥
    `min_value_set_codes` group (a small/generic set can't independently qualify
    a pair — a classification-sharing pair is already generated by `by_class`).
    Each pair is scored once at its strongest tier (`_tier_and_evidence`).
    `max_tier` caps which tiers emit.

    `max_signal_fanout` HUB-suppresses: a signal carried by more distinct
    registers than the cap is a hub, and its cross-register pairs are added ONLY
    when the two variables' names agree (the exemption keeps name-corroborated
    pairs — the analysis showed a bare cap would drop ~17,720 strong tier-1/2
    pairs). `None` disables the cap (= ∞). No silent truncation: the returned
    `InferenceResult.hub_suppressed` is the exact count of pairs the cap removed
    from the output (computed as the uncapped output minus the capped output)."""
    signals = _load_var_signals(conn)
    code_counts = _load_value_set_code_counts(conn)
    class_names = _load_classification_short_names(conn)
    existing = _load_existing_edges(conn)

    # Group FQIDs by each shared signal so we only score pairs that share at
    # least one. Small value sets never group — they can't corroborate a pair at
    # any tier (see `_tier_and_evidence`), so generating their O(N²) pairs only
    # to drop them at scoring is wasted work and re-hubs the 2-code sets.
    by_class: dict[int, list[str]] = {}
    by_vset: dict[int, list[str]] = {}
    for fqid, sig in signals.items():
        for cid in sig.classification_ids:
            by_class.setdefault(cid, []).append(fqid)
        for vsid in sig.value_set_ids:
            if code_counts.get(vsid, 0) >= min_value_set_codes:
                by_vset.setdefault(vsid, []).append(fqid)

    # Build two pair sets in one pass over the groups: `capped` honors the
    # fanout cap (hub groups contribute only name-agreeing pairs); `uncapped`
    # ignores it (every group contributes every cross-register pair). The
    # suppression count is the scored-output difference between them.
    capped_pairs: set[frozenset[str]] = set()
    uncapped_pairs: set[frozenset[str]] = set()
    for members in (*by_class.values(), *by_vset.values()):
        is_hub = (
            max_signal_fanout is not None
            and _distinct_register_fanout(members, signals) > max_signal_fanout
        )
        _add_cross_register_pairs(members, signals, uncapped_pairs, hub=False)
        _add_cross_register_pairs(members, signals, capped_pairs, hub=is_hub)

    capped_out = _score_pairs(
        capped_pairs,
        signals,
        existing=existing,
        code_counts=code_counts,
        class_names=class_names,
        min_value_set_codes=min_value_set_codes,
        max_tier=max_tier,
    )
    # `hub_suppressed` = (pairs that survive scoring at fanout=∞) − (capped
    # output). A pair suppressed by one hub signal but generated by a non-hub
    # signal is in `capped_out` already, so it is correctly NOT counted.
    if capped_pairs == uncapped_pairs:
        hub_suppressed = 0  # cap disabled or no hub removed anything
    else:
        uncapped_out = _score_pairs(
            uncapped_pairs,
            signals,
            existing=existing,
            code_counts=code_counts,
            class_names=class_names,
            min_value_set_codes=min_value_set_codes,
            max_tier=max_tier,
        )
        suppressed_pairs = {frozenset({c.a_fqid, c.b_fqid}) for c in uncapped_out} - {
            frozenset({c.a_fqid, c.b_fqid}) for c in capped_out
        }
        hub_suppressed = len(suppressed_pairs)

    capped_out.sort(key=lambda c: (c.tier, c.a_fqid, c.b_fqid))
    return InferenceResult(candidates=capped_out, hub_suppressed=hub_suppressed)


def _score_pairs(
    pairs: set[frozenset[str]],
    signals: dict[str, _VarSignals],
    *,
    existing: set[frozenset[str]],
    code_counts: dict[int, int],
    class_names: dict[int, str],
    min_value_set_codes: int,
    max_tier: int,
) -> list[Candidate]:
    """Score every candidate pair to its strongest tier, dropping already-edged
    pairs and pairs above `max_tier`. Unsorted (the caller sorts the final
    output)."""
    out: list[Candidate] = []
    for pair in pairs:
        if pair in existing:
            continue
        a_fqid, b_fqid = sorted(pair)
        scored = _tier_and_evidence(
            signals[a_fqid],
            signals[b_fqid],
            code_counts=code_counts,
            class_names=class_names,
            min_value_set_codes=min_value_set_codes,
        )
        if scored is None:
            continue
        tier, evidence = scored
        if tier > max_tier:
            continue
        out.append(
            Candidate(a_fqid=a_fqid, b_fqid=b_fqid, tier=tier, evidence=evidence)
        )
    return out


def _add_cross_register_pairs(
    members: list[str],
    signals: dict[str, _VarSignals],
    out: set[frozenset[str]],
    *,
    hub: bool,
) -> None:
    """Add every unordered CROSS-register pair among `members` to `out`. A
    within-register pair (same `register_id`) is never an identity candidate —
    two variables in one register are distinct columns.

    When `hub` is True the source signal is a high-fanout hub: only add a pair
    whose two variables' `norm_name` AGREE (the name-agreement exemption). A hub
    pair with no name agreement is suppressed (it's the O(N²) noise the cap
    targets); a name-agreeing hub pair is still strong and kept."""
    for i in range(len(members)):
        for j in range(i + 1, len(members)):
            a, b = members[i], members[j]
            sa, sb = signals[a], signals[b]
            if sa.register_id == sb.register_id:
                continue
            if hub and not (sa.norm_name is not None and sa.norm_name == sb.norm_name):
                continue
            out.add(frozenset({a, b}))


def render_candidates_toml(
    candidates: list[Candidate],
    *,
    counts_by_tier: dict[int, int],
    max_signal_fanout: int | None,
    hub_suppressed: int,
) -> str:
    """Render the candidate worklist as a `[[same_as]]` TOML string a maintainer
    curates from. Built by hand (not `tomli_w`) so the per-candidate `# tier N —
    <evidence>` comments survive — `tomli_w` drops comments. FQID slugs are bare
    identifiers (no escaping needed); `note` is simple ASCII.

    The header records the active `max_signal_fanout` and the `hub_suppressed`
    count (so the cap is never a silent truncation), then the per-tier counts.
    Entries are sorted (tier asc, a_fqid, b_fqid). Each `note = "candidate:tierN"`
    marks provenance; a maintainer copies CONFIRMED entries into
    `variable_same_as.toml` (dropping the candidate note or replacing it with a
    rationale)."""
    total = len(candidates)
    fanout_desc = (
        "disabled (hub-clique pairs included)"
        if max_signal_fanout is None
        else f"{max_signal_fanout} distinct registers"
    )
    lines = [
        "# GENERATED same_as candidate worklist — reg-meta-build same-as-candidates.",
        "#",
        "# These are INFERRED identity candidates, NOT confirmed edges. same_as is",
        "# resolver-load-bearing (Catalog.resolve follows it transitively), so a",
        "# wrong edge corrupts resolution. NOTHING here loads into a build — review",
        "# each pair and copy ONLY confirmed identities into",
        "# reg_meta_build/variable_same_as.toml (drop the candidate note or replace",
        "# it with a rationale).",
        "#",
        f"# hub fanout cap: {fanout_desc}",
        f"# hub-suppressed pairs (cap removed, names disagreed): {hub_suppressed}",
        "#   pass --max-signal-fanout 0 to include hub-clique pairs.",
        "#",
        f"# {total} candidate(s) by tier:",
    ]
    for tier in sorted(counts_by_tier):
        lines.append(f"#   tier {tier}: {counts_by_tier[tier]}")
    lines.append("")

    for c in candidates:
        lines.append(f"# tier {c.tier} — {c.evidence}")
        lines.append("[[same_as]]")
        lines.append(f'a = "{c.a_fqid}"')
        lines.append(f'b = "{c.b_fqid}"')
        lines.append(f'note = "candidate:tier{c.tier}"')
        lines.append("")

    return "\n".join(lines) + "\n"
