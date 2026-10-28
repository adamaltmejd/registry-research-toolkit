"""One-shot classification curation conversion; deleted at milestone F (REBUILD.md)."""

import re
import tomllib
from itertools import pairwise
from pathlib import Path

# Reviewed D-9 identity map. Versions retain their old slugs; dimensions and
# aggregates remain separate families, regardless of their browse umbrella.
FAMILIES = {
    **{f"lkf{year}": "lkf" for year in range(1980, 2027)},
    "agarkat2000": "agarkat",
    "agarkat2020": "agarkat",
    "atc": "atc",
    "distrikt2016": "distrikt",
    "drg": "drg",
    "icd-10-se": "icd",
    "icd-11-se": "icd",
    "icd-8-ks68": "icd",
    "icd-9-ks87": "icd",
    "icf": "icf",
    "isced-f2013": "isced-f",
    "isced2011": "isced",
    "jurform2000": "jurform",
    "jurform2020": "jurform",
    "ks87-p": "ks87-p",
    "ksi": "ksi",
    "kva": "kva",
    "mdc": "mdc",
    "niva-grovv1": "niva-grov",
    "niva-oldv1": "niva-old",
    "sektor2000": "sektor",
    "sektorkod": "sektorkod",
    "sni1969": "sni",
    "sni1992": "sni",
    "sni2002": "sni",
    "sni2007": "sni",
    "sni2025": "sni",
    "ssyk1996": "ssyk",
    "ssyk2012": "ssyk",
    "sun1996": "sun",
    "sun2000-grupp": "sun-grupp",
    "sun2020-grupp": "sun-grupp",
    "sun2000-inriktning": "sun-inriktning",
    "sun2020-inriktning": "sun-inriktning",
    "sun2000-niva": "sun-niva",
    "sun2020-niva": "sun-niva",
}


def fqid(definition):
    """Exact reviewed version reference; refuse unreviewed or changed families."""
    slug = definition["slug"]
    family = FAMILIES[slug]
    if definition.get("family", family) != family:
        raise ValueError(f"reviewed family differs from declared family: {slug}")
    return f"class/{family}/{slug}"


def load_books(root):
    """Read source books with original declaration and binding locations."""
    books = []
    for path in sorted(
        (Path(root) / "reg_meta_build/curation/classifications").glob("*.toml")
    ):
        text = path.read_text()
        data = tomllib.loads(text)
        lines = text.splitlines()
        books.append(
            {
                "source": str(path.relative_to(root)),
                "line": lines.index("[classification]") + 1,
                "definition": data["classification"],
                "bindings": data.get("binding", {}).get("variable", []),
                "binding_lines": [
                    i
                    for i, line in enumerate(lines, 1)
                    if line == "[[binding.variable]]"
                ],
                "data": data,
                "text": text,
            }
        )
    return books


def _outcome(source, line, category, outcome, **extra):
    return {
        "source": f"{source}:{line}",
        "category": category,
        "outcome": outcome,
        **extra,
    }


def inventory(root):
    """Return inventory records and located conversion/drop/refusal outcomes.

    Guarded legacy edition succession is materialized here once, never at build
    time. Its year/name guard is retained so source drift cannot invent an edge.
    """
    books = load_books(root)
    entries, outcomes = [], []
    by_slug = {}
    for book in books:
        definition = book["definition"]
        source, line = book["source"], book["line"]
        try:
            reference = fqid(definition)
        except (KeyError, ValueError) as error:
            outcomes.append(
                _outcome(source, line, "classification", "refused", reason=str(error))
            )
            continue
        entry: dict = {
            "family": FAMILIES[definition["slug"]],
            "version": definition["slug"],
            "value_set": f"catalog/source/classifications/{definition['codes_file']}",
            "evidence": f"{source}:{line}; reviewed D-9 family/version mapping",
            "name": definition["name"],
            "publisher": definition["publisher"],
        }
        start, end = definition.get("valid_from"), definition.get("valid_to")
        if start is not None or end is not None:
            entry["period"] = f"{start or ''}..{end or ''}"
        if definition.get("hierarchy") == "flat":
            entry["evidence"] += (
                "; declared flat hierarchy; reader config suppresses external parents"
            )
        entries.append(entry)
        by_slug[definition["slug"]] = (entry, book)
        outcomes.append(
            _outcome(
                source,
                line,
                "classification",
                "converted",
                short_name=definition["short_name"],
                fqid=reference,
            )
        )
        dropped = sorted(
            set(definition)
            & {
                "name_en",
                "url",
                "aliases",
                "family_aliases",
                "label_source",
                "description",
            }
        )
        if dropped:
            outcomes.append(
                _outcome(
                    source,
                    line,
                    "classification-presentation",
                    "dropped",
                    reason="fields omitted by #2016 inventory contract: "
                    + ", ".join(dropped),
                )
            )
        if definition.get("sentinel_codes"):
            outcomes.append(
                _outcome(
                    source,
                    line,
                    "classification-sentinels",
                    "dropped",
                    reason="sentinels deferred; not classification membership or bindings",
                    count=len(definition["sentinel_codes"]),
                )
            )

    # Publisher is family metadata in the new schema. Preserve differing edition
    # publishers verbatim in evidence rather than select one for the whole family.
    publishers = {}
    for entry in entries:
        publishers.setdefault(entry["family"], set()).add(entry["publisher"])
    for entry in entries:
        if len(publishers[entry["family"]]) > 1:
            publisher = entry.pop("publisher")
            entry["evidence"] += f"; edition publisher: {publisher}"
            _original, book = by_slug[entry["version"]]
            outcomes.append(
                _outcome(
                    book["source"],
                    book["line"],
                    "classification-publisher",
                    "converted",
                    reason="edition publishers differ; preserved verbatim in evidence, no family publisher guessed",
                )
            )

    def edge(predecessor, successor, source, line, evidence):
        if predecessor not in by_slug or successor not in by_slug:
            outcomes.append(
                _outcome(
                    source,
                    line,
                    "classification-succession",
                    "refused",
                    reason=f"uninventoried endpoint: {predecessor} -> {successor}",
                )
            )
            return
        target = by_slug[successor][0]
        reference = fqid(by_slug[predecessor][1]["definition"])
        target.setdefault("succeeds", []).append(reference)
        outcomes.append(
            _outcome(
                source,
                line,
                "classification-succession",
                "converted",
                predecessor=reference,
                successor=fqid(by_slug[successor][1]["definition"]),
                reason=evidence,
            )
        )

    families = {}
    for slug, (_entry, book) in by_slug.items():
        match = re.fullmatch(r"sun(\d{4})-(niva|inriktning|grupp)", slug)
        if match:
            stem, year = f"sun-{match[2]}", int(match[1])
        else:
            match = re.fullmatch(r"(.*\D)(\d{4})", slug)
            if not match:
                continue
            stem, year = match[1], int(match[2])
        if 1900 <= year < 2100:
            families.setdefault(stem, []).append((year, slug, book))
    for editions in families.values():
        editions.sort(key=lambda item: (item[0], item[1]))
        names = {
            " ".join(book["definition"]["name"].replace(str(year), "", 1).split())
            for year, _slug, book in editions
            if str(year) in book["definition"]["name"]
        }
        if len(editions) < 2 or len(names) != 1 or not next(iter(names)):
            continue
        if any(
            str(year) not in book["definition"]["name"]
            for year, _slug, book in editions
        ):
            continue
        for predecessor, successor in pairwise(editions):
            book = successor[2]
            edge(
                predecessor[1],
                successor[1],
                book["source"],
                book["line"],
                "materialized old guarded slug-year/name rule",
            )

    path = Path(root) / "reg_meta_build/curation/relations.toml"
    text = path.read_text()
    declarations = tomllib.loads(text).get("edge", [])
    lines = [i for i, line in enumerate(text.splitlines(), 1) if line == "[[edge]]"]
    for declaration, line in zip(declarations, lines, strict=True):
        if declaration.get("type") != "replaced_by":
            continue
        predecessor, successor = declaration.get("from", ""), declaration.get("to", "")
        if predecessor.startswith("class/") and successor.startswith("class/"):
            edge(
                predecessor.removeprefix("class/"),
                successor.removeprefix("class/"),
                str(path.relative_to(root)),
                line,
                "explicit legacy succession edge",
            )
    for entry in entries:
        if "succeeds" in entry:
            entry["succeeds"] = sorted(set(entry["succeeds"]))
    path = Path(root) / "reg_meta_build/curation/classification_groups.toml"
    text = path.read_text()
    groups = tomllib.loads(text).get("classification_group", [])
    lines = [
        i
        for i, line in enumerate(text.splitlines(), 1)
        if line == "[[classification_group]]"
    ]
    for group, line in zip(groups, lines, strict=True):
        outcomes.append(
            _outcome(
                str(path.relative_to(root)),
                line,
                "classification-umbrella",
                "dropped",
                reason="umbrella presentation omitted by #2016; members retain distinct families",
                key=group["key"],
                members=[m["classification"] for m in group["members"]],
            )
        )
    return entries, outcomes


def convert(root, *, register_public=None):
    """Default full-converter census; D-9 conversion uses --classifications-only."""
    register_public = register_public or {}
    outcomes = []
    for book in load_books(root):
        outcomes.append(
            (
                "",
                "",
                book["source"],
                book["line"],
                None,
                "classification conversion requires --classifications-only",
            )
        )
        for binding, line in zip(book["bindings"], book["binding_lines"], strict=True):
            provider, public, _variable = binding["variable"].split("/")
            register = register_public.get((provider, public))
            statement = None
            reason = (
                "classification binding requires reviewed whole-occurrence selection"
            )
            if register is None:
                reason = (
                    "classification binding register is absent from current ledgers"
                )
            outcomes.append(
                (provider, register or public, book["source"], line, statement, reason)
            )
    return outcomes
