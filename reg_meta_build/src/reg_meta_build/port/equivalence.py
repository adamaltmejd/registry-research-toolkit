"""Per-entry record-set equivalence of a converted tree against today's Python compile.

    uv run python -m reg_meta_build.port.equivalence --prepared P --input-commit C \\
        --input-manifest-sha256 M --registers SPEC ... --converted DIR --dump DUMP

`DIR` is `reg_meta_build.port.convert`'s output; `DUMP` is the Python side's
`check-curation --dump-decisions` directory for the same `--registers` set. For every
converted entry it compares what the neutral entry selects in the prepared root, through
`select.RegisterRecords` built from the converted file's own `table.assign` entries, with
what the old entry's case acts on in the dump. The expected sets come from Python's
compile, never from the converter. It also checks each entry's `legacy.head` against the
provenance Python publishes for that case.

It prints counts, case ids and verbs, never record content. Exit 0 when every entry is
equal; 1 otherwise. Deleted at phase 4 (`PORT.md`).
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta_build._curation import repo_curation_dir
from reg_meta_build.curation_compile import _naming_source_id
from reg_meta_build.port import convert
from reg_meta_build.port.select import Member, Ref, scope
from reg_meta_build.prepared_catalog import open_prepared_catalog_sources

if TYPE_CHECKING:
    from collections.abc import Iterable


def _ref(ref: dict[str, Any]) -> Ref:
    return ref["source"], tuple(ref["semantic_record_key"])


def _member(item: dict[str, Any]) -> Member:
    return Member(
        table=item.get("table"),
        column=item.get("column"),
        deliveries=tuple(item["deliveries"]) if "deliveries" in item else None,
        period=item.get("period"),
        where=tuple(sorted(item.get("where", {}).items())),
    )


class Dump:
    """The Python side: every case of every scope file, by case id."""

    def __init__(self, directory: Path) -> None:
        self.cases: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for path in sorted(directory.glob("scope-*.json")):
            for case in json.loads(path.read_text(encoding="utf-8"))["cases"]:
                self.cases[case["case_id"]].append(case)

    def of(self, ref: str) -> list[dict[str, Any]]:
        """The cases of one old entry: its root id and any `/period/<k>` suffix."""
        return [
            case
            for case_id, cases in self.cases.items()
            if case_id == ref or case_id.startswith(ref + "/")
            for case in cases
        ]

    def families(self, prefix: str) -> Iterable[dict[str, Any]]:
        for case_id, cases in self.cases.items():
            if case_id.startswith(prefix):
                yield from cases


def _targets(cases: list[dict[str, Any]]) -> set[Ref]:
    return {_ref(target["ref"]) for case in cases for target in case["targets"]}


def _head(cases: list[dict[str, Any]]) -> set[str | None]:
    return {
        (case["decision"].get("published") or "").split("\n")[0] or None
        for case in cases
    }


def converted_splits(
    entries: dict[str, list[dict]], variant_slugs: dict[str, str]
) -> dict[tuple[str, str], str]:
    """The converted file's own table assignments: a split moves the named deliveries
    of a member's source table to its table."""
    ids = {slug: id_ for id_, slug in variant_slugs.items()}
    splits = {}
    for entry in entries.get("table.assign", []):
        if "source_token" in entry:
            continue
        (table,) = entry["tables"]
        for member in entry["members"]:
            for delivery in member["deliveries"]:
                splits[(ids[member["table"]], delivery)] = ids[table]
    return splits


class Checker:
    def __init__(
        self, reg: convert.Register, entries: dict[str, list[dict]], dump: Dump
    ):
        self.reg = reg
        self.index = reg.index
        self.dump = dump
        self.entries = entries
        self.results: list[dict[str, Any]] = []

    def record(self, verb: str, case: str, ok: bool, **detail: Any) -> None:
        self.results.append({"verb": verb, "case": case, "ok": ok, **detail})

    def select(
        self, members: Iterable[dict[str, Any]], *, source: bool = False
    ) -> set[Ref]:
        out: set[Ref] = set()
        for member in members:
            out |= self.index.select(_member(member), source=source)
        return out

    def compare(
        self,
        verb: str,
        entry: dict,
        new: set[Any],
        old: set[Any],
        cases: list[dict] | None = None,
        **check: bool,
    ) -> None:
        """Equal sets and heads, plus every named `check` (a detail that is also
        reported)."""
        case = entry["legacy"]["ref"]
        cases = self.dump.of(case) if cases is None else cases
        head = entry["legacy"].get("head")
        heads = _head(cases) if cases else {None}
        self.record(
            verb,
            case,
            bool(cases) and new == old and heads == {head} and all(check.values()),
            cases=len(cases),
            new=len(new),
            old=len(old),
            missing=len(old - new),
            extra=len(new - old),
            head=head,
            published=sorted(h or "" for h in heads),
            **check,
        )

    # ── per verb ───────────────────────────────────────────────────────────────

    def occurrence(self, verb: str, entry: dict, members: list[dict]) -> None:
        """Per record where Python's effects name their refs: alternative records
        share a ref, and an effect reaches each alternative its `when` describes."""
        cases = self.dump.of(entry["legacy"]["ref"])
        effects = [
            effect
            for case in cases
            for effect in case["decision"].get("effects", ())
            if "ref" in effect and effect.get("kind") in ("field", "period")
        ]
        if not effects:
            # Coding cases carry no effects. A member that selects no source record
            # targets an occurrence only an addition creates (`PORT.md`): no oracle
            # yet, so never an ok.
            new_refs = self.select(members)
            self.compare(
                verb, entry, new_refs, _targets(cases), selects_source=bool(new_refs)
            )
            return
        by_ref: dict[Ref, list[Any]] = defaultdict(list)
        for rec in self.index.records:
            by_ref[rec.ref].append(rec)
        # An effect without `when` on a ref of several alternatives does not say which
        # one Python's guard picked (`_occurrence_correction_matches` checks
        # `expected_fields` per record): compare by ref, and say so.
        if any(
            not effect.get("when") and len(by_ref[_ref(effect["ref"])]) > 1
            for effect in effects
        ):
            self.compare(
                verb,
                entry,
                self.select(members),
                _targets(cases),
                alternative_by_ref=True,
            )
            return
        old = {
            id(rec)
            for effect in effects
            for rec in by_ref[_ref(effect["ref"])]
            if all(
                rec.stated(item["name"])
                == (item.get("value") if item["status"] == "value" else None)
                for item in effect.get("when") or ()
                if item["name"] in rec.FIELDS
            )
        }
        chosen = [_member(member) for member in members]
        new = {
            id(rec)
            for rec in self.index.records
            if any(self.index.matches(member, rec) for member in chosen)
        }
        self.compare(verb, entry, new, old, selects_source=bool(new))

    def addition(self, verb: str, entry: dict) -> None:
        """An addition lands in deliveries the source does not have: compare the
        (table, delivery) pairs, or the period, with Python's added occurrences."""
        cases = self.dump.of(entry["legacy"]["ref"])
        member = entry["member"]
        effects = [
            effect
            for case in cases
            for effect in case["decision"].get("effects", ())
            if effect.get("kind") == "addition"
        ]
        names = {
            rec.edition_id: rec.delivery
            for rec in self.index.records
            if rec.edition_id is not None
        }
        if "period" in member:
            new = {(member["table"], scope(member["period"]))}
            old = set()
            for effect in effects:
                variant = _naming_source_id(
                    "register_variant", tuple(effect["variant_key"])
                )
                old.add(
                    (
                        self.reg.variant_slugs.get(variant, variant),
                        scope(effect["edition_period_scope"]),
                    )
                )
        else:
            deliveries = member.get("deliveries")
            if deliveries is None:
                deliveries = sorted(
                    {
                        rec.delivery
                        for rec in self.index.records
                        if member["table"] in self.index.tables(rec) and rec.delivery
                    }
                )
            new = {(member["table"], d) for d in deliveries}
            old = set()
            for effect in effects:
                variant = _naming_source_id(
                    "register_variant", tuple(effect["variant_key"])
                )
                old.add(
                    (
                        self.reg.variant_slugs.get(variant, variant),
                        names.get(
                            effect["edition_key"][-1], str(effect["edition_key"][-1])
                        ),
                    )
                )
        if verb != "errata.delivered":
            self.compare(verb, entry, new, old)
            return
        # Python names a donor only where it copies the coding (`donor_coding`): one
        # same-edition record per added edition, each within the converted donor.
        if not entry.get("donor_coding"):
            self.compare(verb, entry, new, old)
            return
        donors = {_ref(effect["donor"]) for effect in effects if effect.get("donor")}
        donor_ok = bool(donors) and donors <= self.select([entry["donor"]])
        self.compare(verb, entry, new, old, donor=donor_ok)

    def assignments(self) -> None:
        """Owner per record: the converted `identity.assign` links against Python's
        accepted partitions and SoS identities, over every record either side owns.

        A record is matched to Python's effect on its ref whose `when` it satisfies
        (alternative records share a ref). An SCB partition's key ends in the owner's
        naming id, compared by slug. A SoS split or rename keys by its discriminator
        (`accepted-shape`, value) and binds the owner separately, so there the
        grouping is compared: each Python key maps to one owner and each owner to one
        key."""
        effects: dict[Ref, list[tuple[tuple, tuple[str, ...]]]] = defaultdict(list)
        for prefix in ("accepted-column-partitions:", "accepted-sos-identity:"):
            for case in self.dump.families(prefix):
                for effect in case["decision"].get("effects", ()):
                    key = effect.get("variable_key")
                    if effect.get("kind") != "identity" or not key:
                        continue
                    when = tuple(
                        (item["name"], item["value"])
                        for item in effect.get("when", ())
                        if item["status"] == "value"
                    )
                    effects[_ref(effect["ref"])].append(
                        (when, tuple(str(part) for part in key))
                    )
        python: dict[int, tuple[str, ...]] = {}
        for i, rec in enumerate(self.index.records):
            keys = {
                key
                for when, key in effects.get(rec.ref, ())
                if all(rec.stated(name) == value for name, value in when)
            }
            if len(keys) == 1:
                python[i] = keys.pop()
            elif keys:
                raise ValueError(f"{rec.ref}: {len(keys)} Python owners")

        def exact(key: tuple[str, ...]) -> str | None:
            if "accepted-shape" in key or "accepted-name" in key:
                return None
            return self.reg.variable_slugs.get(key[-1], key[-1])

        def selected(members: list[dict]) -> set[int]:
            chosen = [_member(member) for member in members]
            return {
                i
                for i, rec in enumerate(self.index.records)
                if any(self.index.matches(member, rec) for member in chosen)
            }

        covered: set[int] = set()
        for entry in self.entries.get("identity.assign", []):
            new: dict[int, str | bool] = {}
            for link in entry["link"]:
                for i in selected(link["members"]):
                    new[i] = link["owner"]
            covered |= set(new)
            owned = {i: owner for i, owner in new.items() if owner is not False}
            unassigned = {i for i, owner in new.items() if owner is False}
            pairs = {(python[i], owned[i]) for i in owned if i in python}
            mismatched = sum(
                1
                for i in owned
                if i in python
                and (want := exact(python[i])) is not None
                and want != owned[i]
            )
            keys = Counter(key for key, _ in pairs)
            owners = Counter(owner for _, owner in pairs)
            ambiguous = sum(1 for n in (*keys.values(), *owners.values()) if n > 1)
            self.record(
                "identity.assign",
                entry["legacy"]["ref"],
                set(owned) <= set(python)
                and not mismatched
                and not ambiguous
                and not (unassigned & set(python)),
                new=len(owned),
                not_owned_in_python=len(set(owned) - set(python)),
                unassigned=len(unassigned),
                mismatched=mismatched,
                ambiguous=ambiguous,
                unassigned_but_owned=len(unassigned & set(python)),
            )
        self.record(
            "identity.assign (coverage)",
            self.reg.file,
            set(python) <= covered,
            python_owned=len(python),
            not_converted=len(set(python) - covered),
        )

    def run(self) -> None:
        self.assignments()
        for entry in self.entries.get("table.assign", []):
            if "source_token" in entry:
                new = {
                    rec.ref
                    for rec in self.index.records
                    if rec.token == entry["source_token"]
                }
                # Python routes per variable (`accepted-sos-routes:<reg>:<var>`):
                # the cases whose effects reach this token's records.
                cases = [
                    case
                    for case in self.dump.families("accepted-sos-routes:")
                    if any(
                        _ref(effect["ref"]) in new
                        for effect in case["decision"].get("effects", ())
                    )
                ]
                old = {
                    _ref(effect["ref"])
                    for case in cases
                    for effect in case["decision"].get("effects", ())
                }
                self.compare("table.assign", entry, new, old, cases=cases)
                continue
            cases = self.dump.of(entry["legacy"]["ref"])
            old = {
                _ref(effect["ref"])
                for case in cases
                for effect in case["decision"].get("effects", ())
            }
            self.compare(
                "table.assign", entry, self.select(entry["members"], source=True), old
            )
        for path in ("errata.field", "errata.period", "errata.support"):
            for entry in self.entries.get(path, []):
                self.occurrence(path, entry, [entry["member"]])
        for entry in self.entries.get("errata.delivered", []):
            if entry["legacy"].get("head") == "errata:occurrence_carry":
                original = {k: v for k, v in entry["member"].items() if k != "period"}
                self.occurrence("errata.delivered (carry)", entry, [original])
            else:
                self.addition("errata.delivered", entry)
        for entry in self.entries.get("errata.column", []):
            self.addition("errata.column", entry)
        for entry in self.entries.get("representation.delivery_metadata", []):
            self.occurrence(
                "representation.delivery_metadata",
                entry,
                [column["member"] for column in entry["columns"]],
            )
        for entry in self.entries.get("representation.matrix", []):
            self.occurrence("representation.matrix", entry, [entry["member"]])
        for kind in convert.OLD_VERBS["coding"]:
            for entry in self.entries.get(f"coding.{kind}", []):
                self.occurrence(f"coding.{kind}", entry, [entry["member"]])
        for path in (
            "representation.parallel",
            "representation.period_family",
            "errata.delivery",
            "errata.table",
        ):
            for entry in self.entries.get(path, []):
                self.record(
                    path,
                    entry.get("legacy", {}).get("ref", self.reg.file),
                    False,
                    note="no oracle yet",
                )


def _entries(text: str) -> dict[str, list[dict]]:
    raw = tomllib.loads(text)
    return {
        f"{group}.{verb}": entries
        for group in ("identity", "table", "errata", "representation", "coding")
        for verb, entries in raw.get(group, {}).items()
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--input-commit", required=True)
    parser.add_argument("--input-manifest-sha256", required=True)
    parser.add_argument("--registers", action="append", required=True, metavar="SPEC")
    parser.add_argument("--converted", type=Path, required=True)
    parser.add_argument(
        "--curation-dir",
        type=Path,
        help="the tracked tree the conversion read (defaults to this checkout's)",
    )
    parser.add_argument("--dump", type=Path, required=True)
    parser.add_argument("--json", type=Path, help="write every result here")
    args = parser.parse_args(argv)
    prepared = open_prepared_catalog_sources(
        args.prepared,
        expected_sha256=args.input_manifest_sha256,
        input_commit=args.input_commit,
    )
    sources = {
        entry.revision.dataset
        for entry in prepared.manifest.inputs
        if entry.record_usage == "occurrence" and entry.revision is not None
    }
    records = convert.selected_records(prepared.records, sources, tuple(args.registers))
    tracked = args.curation_dir or repo_curation_dir()
    if tracked is None:
        parser.error("no curation tree; pass --curation-dir")
    converted = args.converted / "curation"
    tracked_files = convert.register_files(tracked)
    files = convert.register_files(converted)
    dump = Dump(args.dump)
    results = []
    for register_id in sorted(records):
        path = files[register_id]
        text = path.read_text(encoding="utf-8")
        entries = _entries(text)
        # The tracked routes stand in for the SoS adapter's token resolution (D7):
        # the converted file keeps only the ambiguous ones.
        routes = convert.old_routes(
            tomllib.loads(tracked_files[register_id].read_text(encoding="utf-8"))
        )
        splits = converted_splits(
            entries, convert.read_variant_slugs(tomllib.loads(text))
        )
        reg = convert.open_register(
            path,
            converted,
            register_id,
            records[register_id],
            routes=routes,
            splits=splits,
        )
        checker = Checker(reg, entries, dump)
        checker.run()
        results += checker.results
    failed = [r for r in results if not r["ok"]]
    summary = Counter((r["verb"], r["ok"]) for r in results)
    for (verb, ok), count in sorted(summary.items()):
        print(f"{'ok  ' if ok else 'FAIL'} {verb}: {count}")
    for result in failed[:40]:
        print(json.dumps(result, ensure_ascii=False))
    if args.json:
        args.json.write_text(
            json.dumps(results, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
