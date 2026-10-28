"""One-shot classification curation conversion; deleted at milestone F (REBUILD.md)."""

import tomllib
from pathlib import Path


def convert(root, *, register_public=None):
    """Return located outcomes without inferring classification families or owners.

    One classification table is one source entry. Inline sentinel definitions remain
    part of that entry; each explicit binding.variable table is a separate entry.
    """
    register_public = register_public or {}
    outcomes = []
    for path in sorted(
        (Path(root) / "reg_meta_build/curation/classifications").glob("*.toml")
    ):
        text = path.read_text()
        data = tomllib.loads(text)
        definition = data["classification"]
        source = str(path.relative_to(root))
        line = next(
            i
            for i, value in enumerate(text.splitlines(), 1)
            if value == "[classification]"
        )
        outcomes.append(
            (
                "",
                "",
                source,
                line,
                None,
                "classification metadata and sentinel definitions have no complete four-kind mapping",
            )
        )
        bindings = data.get("binding", {}).get("variable", [])
        binding_lines = [
            i
            for i, value in enumerate(text.splitlines(), 1)
            if value == "[[binding.variable]]"
        ]
        for binding, line in zip(bindings, binding_lines, strict=True):
            provider, public, variable = binding["variable"].split("/")
            register = register_public.get((provider, public))
            family = definition.get("family")
            if not family:
                statement = None
                reason = (
                    "classification binding lacks an explicit family/version mapping"
                )
            elif register is None:
                statement = None
                reason = (
                    "classification binding register is absent from current ledgers"
                )
            else:
                statement = {
                    "kind": "set",
                    "subject": f"var:{variable}",
                    "fields": {
                        "classification": f"class/{family}/{definition['slug']}"
                    },
                    "evidence": binding.get("note") or definition.get("url") or source,
                }
                reason = ""
            outcomes.append(
                (provider, register or public, source, line, statement, reason)
            )
    return outcomes
