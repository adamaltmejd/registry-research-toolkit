"""The converter's output against the verb golden corpus, as parsed values.

    uv run python -m reg_meta_build.port.golden --registers-only
    uv run python -m reg_meta_build.port.golden --converted DIR

`--registers-only` prints the native ids of the registers the corpus pairs come from,
one per line, as `reg_meta_build.port.convert --registers` specs. `--converted DIR`
compares each pair's `after.toml` entry with the converted entry carrying the same
`legacy.ref` (a `[[variant]]` name pair: the equal `errata.table` entry). Exit 0 when
every pair is equal. Deleted at phase 4 (`PORT.md`).
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta_build._curation import repo_curation_dir
from reg_meta_build.port.convert import NEW_TABLES

if TYPE_CHECKING:
    from collections.abc import Iterator

CORPUS = Path(__file__).resolve().parents[4] / "crates/reg-build-model/tests/verbs"


def pairs() -> dict[str, str]:
    """Each pair directory by the case id its `before.toml` names on line 1."""
    return {
        d.name: (d / "before.toml")
        .read_text(encoding="utf-8")
        .splitlines()[0]
        .removeprefix("# case: ")
        .strip()
        for d in sorted(CORPUS.iterdir())
        if d.is_dir()
    }


def _entries(raw: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    for path in NEW_TABLES:
        group, verb = path.split(".")
        for entry in raw.get(group, {}).get(verb, []):
            yield path, entry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--registers-only", action="store_true")
    mode.add_argument("--converted", type=Path)
    args = parser.parse_args(argv)
    curation = repo_curation_dir()
    if curation is None:
        parser.error("no curation tree in this checkout")
    cases = pairs()
    if args.registers_only:
        files = {
            case.split("#")[0].removeprefix("curation/") for case in cases.values()
        }
        specs = set()
        for file in files:
            raw = tomllib.loads((curation / file).read_text(encoding="utf-8"))
            specs.add(str(raw["register"]["native_id"]))
        print("\n".join(sorted(specs)))
        return 0
    failed = 0
    for name, case in cases.items():
        after = tomllib.loads(
            (CORPUS / name / "after.toml").read_text(encoding="utf-8")
        )
        (want,) = _entries(after)
        file = args.converted / case.split("#")[0]
        got = [
            item
            for item in _entries(tomllib.loads(file.read_text(encoding="utf-8")))
            if item[1].get("legacy", {}).get("ref") == case
            or (name == "variant.name" and item == want)
        ]
        if got != [want]:
            failed += 1
            print(f"DIFF {name} {case}: {len(got)} converted entries")
            if len(got) == 1:
                for key in sorted(set(want[1]) | set(got[0][1])):
                    if want[1].get(key) != got[0][1].get(key):
                        print(f"  {key}: want {want[1].get(key)!r}")
                        print(f"  {key}: got  {got[0][1].get(key)!r}")
    print(f"{len(cases) - failed}/{len(cases)} pairs equal")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
