"""Derive group edges between already identified source-definition siblings.

No catalog identity or column equivalence is inferred here. A shared native
definition supplies group membership only after checked identity resolution.
A code column and its label column never fold, co-delivered or not; actual
edition co-delivery controls the shape guards.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations, product
from typing import TYPE_CHECKING, Literal

from reg_meta_build._components import DisjointSet
from reg_meta_build._curation import (
    _is_code_then_label,
    _looks_like_code_label_pair,
    fold_column,
    sibling_shape_conflict,
)
from reg_meta_build.source_coordinates import native_variable_key
from reg_meta_build.source_curation import ResolutionDiagnostic
from reg_meta_build.source_effects import record_ref

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.resolved_catalog import ResolvedState, ResolvedVariable
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import SourceRecordRef
    from reg_meta_build.source_occurrences import EffectiveOccurrence


@dataclass(frozen=True)
class SiblingPairDecision:
    family: NativeKey
    a: str
    b: str
    columns: tuple[str, str]
    kind: Literal["foldable", "code_label", "shape_conflict", "unresolved_shape"]
    refs: tuple[SourceRecordRef, ...]


@dataclass(frozen=True)
class SiblingResolution:
    pairs: tuple[tuple[str, str], ...]
    decisions: tuple[SiblingPairDecision, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def _column(occurrence: EffectiveOccurrence) -> str | None:
    if "column_name" in occurrence.withheld_fields:
        return None
    value = occurrence.fields.column_name
    if value is not None and value.status == "value" and value.value:
        assert isinstance(value.value, str)
        return value.value
    return None


def _shape(occurrence: EffectiveOccurrence) -> tuple[str | None, str | None]:
    data_type, data_length = (
        str(value.value) if value and value.status == "value" else None
        for value in (occurrence.fields.data_type, occurrence.fields.data_length)
    )
    return data_type, data_length


# The reader's search `q` cap: MAX_QUERY_CHARS in crates/reg-catalog/src/ops/mod.rs
# (conformance/api/operations.toml). Change both together.
SEARCH_QUERY_MAX_CHARS = 200

# A period ends a sentence unless the word it closes is an abbreviation: one with an
# inner period (t.ex., fr.o.m., m.m.), a single letter, or one of these.
_ABBREVIATIONS = frozenset(
    {"ca", "dvs", "exkl", "inkl", "jfr", "kap", "nr", "resp", "s", "st"}
)
_SENTENCE_BREAK = re.compile(r"[.?!] +(?=[^\W\d_])")


def _one_line(text: str | None) -> str | None:
    return (" ".join(text.split()) or None) if text is not None else None


def _first_sentence(text: str) -> str:
    """The first line's first sentence, without a closing period.

    A sentence ends at `.`, `?` or `!` followed by a space and an upper-case letter,
    and a period only where the word it closes is not an abbreviation.
    """
    line = next((" ".join(row.split()) for row in text.splitlines() if row.strip()), "")
    for match in _SENTENCE_BREAK.finditer(line):
        end = match.start()
        if not line[match.end()].isupper():
            continue
        if line[end] == ".":
            word = line[:end].rsplit(" ", 1)[-1]
            if "." in word or len(word) == 1 or word.casefold() in _ABBREVIATIONS:
                continue
            return line[:end].rstrip()
        return line[: end + 1]
    return line.removesuffix(".").rstrip()


def source_name(occurrences: Iterable[EffectiveOccurrence]) -> str | None:
    """The one name a variable's catalog source records carry, before corrections."""
    found = set()
    for occurrence in occurrences:
        if occurrence.use != "catalog":
            continue
        for record in occurrence.source_records:
            field = record.fields.name
            text = (
                field.value if field is not None and field.status == "value" else None
            )
            found.add(text.strip() or None if isinstance(text, str) else None)
    return found.pop() if len(found) == 1 else None


def _years(states: tuple[ResolvedState, ...]) -> str | None:
    starts = [s.valid_from[:4] for s in states if s.valid_from is not None]
    ends = [s.valid_to[:4] for s in states if s.valid_to is not None]
    if not starts:
        return None
    first, last = min(starts), max(ends)
    if last == "9999":
        return f"{first}–"
    return first if first == last else f"{first}–{last}"


def name_colliding_variables(
    variables: Mapping[NativeKey, ResolvedVariable],
    source_names: Mapping[NativeKey, str],
) -> dict[NativeKey, str]:
    """Name variables that share a source name in one variant apart (#1736).

    `source_names` holds each variable's one source name. Variables of one register
    that share it in a variant are a group; a variable in several groups is compared
    with all of their members. A member still named by its source name (no
    `[[errata.field]]` name correction) takes the first step that names it apart:

    1. `<name> — <operational definition>` when the definition is one text across its
       states that no step-1/2 sibling shares;
    2. else `<name> — <first sentence>` of that definition, when distinct;
    3. else `<name> (<COLUMN>)`, the latest state's column, when distinct (case-folded);
    4. else `<name> (<COLUMN>, <first>–<last year>)`, when distinct.

    No step writes a name over the search cap. A member none of them separates keeps its
    name; the caller refuses every variable whose name still collides.
    Returns the new name of each renamed variable.
    """
    groups: dict[tuple[str, str, str, str], set[NativeKey]] = defaultdict(set)
    for key, name in source_names.items():
        register = variables[key].register_ref
        for state in variables[key].states:
            groups[register.provider, register.slug, state.variant.slug, name].add(key)
    siblings: dict[NativeKey, set[NativeKey]] = defaultdict(set)
    for members in groups.values():
        for key in members:
            siblings[key] |= members - {key}
    pending = {
        key
        for key, others in siblings.items()
        if others and variables[key].name == source_names[key]
    }
    named: dict[NativeKey, str] = {}

    def latest(key: NativeKey) -> ResolvedState:
        return max(
            variables[key].states,
            key=lambda s: (
                s.valid_to or "",
                s.valid_from or "",
                s.delivery_column_name,
            ),
        )

    texts: dict[NativeKey, str] = {}
    for key in pending:
        variable = variables[key]
        found = {
            _one_line(s.operational_definition or variable.operational_definition)
            for s in variable.states
        }
        if len(found) == 1 and (text := found.pop()) is not None:
            texts[key] = text
    distinct = {
        key
        for key, text in texts.items()
        if all(texts.get(other) != text for other in siblings[key])
    }
    firsts = {
        key: _first_sentence(
            latest(key).operational_definition
            or variables[key].operational_definition
            or ""
        )
        for key in distinct
    }
    for key in distinct:
        name = source_names[key]
        rivals = {
            text
            for other in siblings[key] & distinct
            for text in (texts[other], firsts[other])
        }
        for text in (texts[key], firsts[key]):
            if (
                text
                and text not in rivals
                and len(f"{name} — {text}") <= SEARCH_QUERY_MAX_CHARS
            ):
                named[key] = f"{name} — {text}"
                break
    for label in (
        lambda key: latest(key).delivery_column_name,
        lambda key: (
            f"{latest(key).delivery_column_name}, {years}"
            if (years := _years(variables[key].states))
            else None
        ),
    ):
        left = pending - named.keys()
        labels = {key: label(key) for key in left}
        for key in left:
            mine = labels[key]
            if mine is None or any(
                (labels[other] or "").casefold() == mine.casefold()
                for other in siblings[key] & left
            ):
                continue
            if len(new := f"{source_names[key]} ({mine})") <= SEARCH_QUERY_MAX_CHARS:
                named[key] = new
    return named


def resolve_sibling_pairs(
    occurrences: tuple[EffectiveOccurrence, ...],
    *,
    identities: Mapping[NativeKey, str | None],
) -> SiblingResolution:
    """Apply grouping guards to exact existing partitions in a complete scope.

    Unknown identities remain None, never invented endpoints. Within each original
    family, every literal column of one known identity is paired with every column
    of another. Any code/label column pair keeps the two apart, co-delivered or
    not, and no group the foldable pairs form may hold a code and its label or a
    shape-disputed (conflicting or unresolved) pair. The pair is co-delivered when
    any column pair is, and the shape guards and evidence use exactly the
    co-delivered column pairs, the shapes compared within each edition that delivers
    both columns; the decision's `columns` name the first of them, else the first
    code/label pair, else each owner's lexically first column. Co-delivery is exact edition identity, never an
    overlap of year spans. An unresolved shape matters only when its alternatives
    disagree about whether the pair can be grouped.
    """
    families: dict[NativeKey, list[EffectiveOccurrence]] = defaultdict(list)
    for occurrence in occurrences:
        if occurrence.use != "catalog" or "identity" in occurrence.withheld_fields:
            continue
        if (
            occurrence.variable_key is not None
            and occurrence.variable_key not in identities
        ):
            raise ValueError("sibling resolution lacks an explicit identity outcome")
        for key in {native_variable_key(r) for r in occurrence.source_records} - {None}:
            assert key is not None
            if key != occurrence.variable_key and not occurrence.identity_checked:
                raise ValueError("sibling partition lacks a checked identity decision")
            families[key].append(occurrence)
    pairs, decisions, diagnostics = set(), [], []
    code_labels: set[tuple[str, str]] = set()
    for family, members in sorted(families.items(), key=lambda item: repr(item[0])):
        # Evidence is keyed by (owner, column): another owner's delivery of the same
        # spelling is not this owner's co-delivery or shape (#1760). An unowned
        # column (owner None) still counts toward whether the family co-delivers.
        by_column: dict[tuple[str | None, str], list[EffectiveOccurrence]] = (
            defaultdict(list)
        )
        owner_columns: dict[str, set[str]] = defaultdict(set)
        editions: dict[NativeKey, set[tuple[str | None, str]]] = defaultdict(set)
        # Shapes are compared edition by edition (#1783): a shape an owner
        # delivers in an edition the other owner does not share never disputes
        # the pair.
        shapes: dict[
            tuple[NativeKey, str | None, str], set[tuple[str | None, str | None]]
        ] = defaultdict(set)
        for occurrence in members:
            column = _column(occurrence)
            fqid = (
                identities.get(occurrence.variable_key)
                if occurrence.variable_key
                else None
            )
            if column is None:
                continue
            by_column[fqid, column].append(occurrence)
            if occurrence.edition_key is not None:
                editions[occurrence.edition_key].add((fqid, column))
                shapes[occurrence.edition_key, fqid, column].add(_shape(occurrence))
            if fqid is not None:
                owner_columns[fqid].add(column)
        codelivered = {
            frozenset((left, right))
            for owned in editions.values()
            for left, right in combinations(owned, 2)
            if left[1] != right[1]
        }
        if not codelivered or len(owner_columns) < 2:
            continue
        for a, b in combinations(sorted(owner_columns), 2):
            if a.rsplit("/", 1)[0] != b.rsplit("/", 1)[0]:
                raise ValueError(
                    "one source sibling family maps to different registers"
                )
            # Every spelling an owner holds is checked: co-delivery under any of them
            # makes the pair co-delivered (#1760).
            together = [
                (col_a, col_b)
                for col_a, col_b in product(
                    sorted(owner_columns[a]), sorted(owner_columns[b])
                )
                if frozenset(((a, col_a), (b, col_b))) in codelivered
            ]
            columns = (
                together[0]
                if together
                else (min(owner_columns[a]), min(owner_columns[b]))
            )
            evidence_columns = (
                {(a, col_a) for col_a, _ in together}
                | {(b, col_b) for _, col_b in together}
                if together
                else {(a, column) for column in owner_columns[a]}
                | {(b, column) for column in owner_columns[b]}
            )
            refs = tuple(
                sorted(
                    {
                        record_ref(record)
                        for owned in evidence_columns
                        for occurrence in by_column[owned]
                        for record in occurrence.evidence
                    },
                    key=repr,
                )
            )
            kind = "foldable"
            # A code and its label never fold, whether or not these two owners
            # co-deliver them: the guard reads every spelling pair (#1760, iku).
            code_label = [
                pair
                for pair in product(sorted(owner_columns[a]), sorted(owner_columns[b]))
                if _looks_like_code_label_pair(*pair)
            ]
            if code_label:
                kind = "code_label"
                if not together:
                    columns = code_label[0]
                col_a, col_b = code_label[0]
                code_labels.add(
                    (a, b)
                    if _is_code_then_label(fold_column(col_a), fold_column(col_b))
                    else (b, a)
                )
            elif together:
                outcomes = {
                    sibling_shape_conflict(left, right)
                    for edition, owned in editions.items()
                    for col_a, col_b in together
                    if (a, col_a) in owned and (b, col_b) in owned
                    for left, right in product(
                        shapes[edition, a, col_a], shapes[edition, b, col_b]
                    )
                }
                withheld_shape = any(
                    {"data_type", "data_length"} & set(row.withheld_fields)
                    for owned in evidence_columns
                    for row in by_column[owned]
                )
                if outcomes == {True} and not withheld_shape:
                    kind = "shape_conflict"
                elif len(outcomes) > 1 or withheld_shape:
                    kind = "unresolved_shape"
                    output = f"same_definition_pair:{a}:{b}"
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="ambiguous_sibling_shape",
                            severity="error",
                            subject=output,
                            detail="The sibling grouping guard has disputed or withheld source shape evidence.",
                            refs=refs,
                            fields=("data_type", "data_length"),
                            withheld_output=(output,),
                        )
                    )
            decisions.append(SiblingPairDecision(family, a, b, columns, kind, refs))
            if kind == "foldable":
                pairs.add((a, b))
    # The same accepted identities can be present in multiple original families.
    # A conflicting family must not be bypassed by an independently eligible edge.
    pairs.difference_update((d.a, d.b) for d in decisions if d.kind != "foldable")
    disputed = {
        (d.a, d.b)
        for d in decisions
        if d.kind in {"shape_conflict", "unresolved_shape"}
    }
    # Union across edges must not join a code and its label through a third owner
    # (#1760): the label owner loses its foldable edges until no such pair shares a
    # group, so the code keeps its own folds. A shape-disputed pair, conflicting or
    # unresolved, is kept apart the same way (#1885). With no code side to keep,
    # the owner on the most disputes inside one group loses its edges; on a tie,
    # the one with the fewest foldable edges, so the fewest folds are split; then
    # the lexically last. One owner per pass, so the counts are re-read.
    while True:
        groups: DisjointSet[str] = DisjointSet()
        for a, b in pairs:
            groups.add(a)
            groups.add(b)
            groups.union(a, b)
        linked = {owner for pair in pairs for owner in pair}
        joined = {
            label
            for code, label in code_labels
            if {code, label} <= linked and groups.find(code) == groups.find(label)
        }
        if not joined:
            counts = Counter(
                owner
                for a, b in disputed
                if {a, b} <= linked and groups.find(a) == groups.find(b)
                for owner in (a, b)
            )
            if not counts:
                break
            edges = Counter(owner for pair in pairs for owner in pair)
            joined = {
                max(counts, key=lambda owner: (counts[owner], -edges[owner], owner))
            }
        pairs = {pair for pair in pairs if not joined & set(pair)}
    return SiblingResolution(tuple(sorted(pairs)), tuple(decisions), tuple(diagnostics))
