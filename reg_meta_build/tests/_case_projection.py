"""Projection comparison shared by the case corpora (`cases/curation_toml/`,
`cases/cli/`): an expected JSON projection against the actual JSON value.

Each corpus README states the rules; this module is their one implementation.
"""

from __future__ import annotations

from typing import Any

MATCH_MODES = frozenset({"exact", "includes"})

# The keys of an `includes` claim on a list whose elements are claimed by
# membership rather than by position.
ELEMENT_CLAIMS = frozenset({"$contains", "$lacks"})
# An `includes` object's value for a key the actual object must not have, such as
# a field a JSON dump leaves out when it is None.
ABSENT = {"$absent": True}


def mismatch(
    actual: Any, expected: Any, path: str = "$", *, exact: bool = True
) -> str | None:
    """Where `actual` departs from `expected`, or None.

    An exact object names every key, so an extra key fails and `{}` means empty;
    an `includes` object compares only the keys it names, `{"$exact": value}`
    inside it compares that value exactly, and `{"$any": true}` inside it is a
    value that is present but not claimed. A list compares element by element and
    must have the same length; `{"$contains": [...]}` inside an `includes` object
    claims that each listed projection matches some element of a list, in any
    order, and `{"$lacks": [...]}` that none matches any element. `{"$absent":
    true}` as a key's value claims that the object has no such key. A scalar
    compares by value and JSON type, so `true` never matches `1`.
    """
    if isinstance(expected, dict) and expected.keys() == {"$any"}:
        return None
    if isinstance(expected, dict) and expected.keys() == {"$exact"}:
        return mismatch(actual, expected["$exact"], path, exact=True)
    if isinstance(expected, dict) and expected and expected.keys() <= ELEMENT_CLAIMS:
        if not isinstance(actual, list):
            return f"{path}: expected a list, got {actual!r}"
        for want in expected.get("$contains", ()):
            if all(mismatch(got, want, path, exact=exact) for got in actual):
                return f"{path}: no element matches {want!r}"
        for unwanted in expected.get("$lacks", ()):
            for index, got in enumerate(actual):
                if mismatch(got, unwanted, path, exact=exact) is None:
                    return f"{path}[{index}]: matches {unwanted!r}"
        return None
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return f"{path}: expected an object, got {actual!r}"
        if exact and (extra := sorted(actual.keys() - expected.keys())):
            return f"{path}: unexpected keys {extra}"
        for key, item in expected.items():
            if item == ABSENT:
                if key in actual:
                    return f"{path}: key {key!r} is present"
                continue
            if key not in actual:
                return f"{path}: no key {key!r} in {sorted(actual)}"
            if found := mismatch(actual[key], item, f"{path}.{key}", exact=exact):
                return found
        return None
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            return f"{path}: expected {len(expected)} items, got {actual!r}"
        for index, (got, want) in enumerate(zip(actual, expected, strict=True)):
            if found := mismatch(got, want, f"{path}[{index}]", exact=exact):
                return found
        return None
    if type(actual) is not type(expected) or actual != expected:
        return f"{path}: expected {expected!r}, got {actual!r}"
    return None


def unclaimed(expected: Any, path: str = "$result", *, exact: bool) -> str | None:
    """The first projection node that claims less than it looks like, or None.

    Under `includes` a bare `{}` checks only that an object is there, so a case
    whose `fails_if` names its content cannot fail; an unclaimed element is
    written `{"$any": true}` instead. Under `exact` (and inside `$exact`) `{}`
    means empty and `$any` would weaken the claim, so `$any` is refused there.
    `$contains` and `$lacks` are refused there for the same reason, and an empty
    list under either claims nothing.
    """
    if isinstance(expected, dict):
        if expected.keys() == {"$any"}:
            if exact or expected["$any"] is not True:
                return f'{path}: {{"$any": true}} is only for an `includes` projection'
            return None
        if expected.keys() == {"$exact"}:
            return unclaimed(expected["$exact"], path, exact=True)
        if expected and expected.keys() <= ELEMENT_CLAIMS:
            if exact:
                return (
                    f"{path}: {sorted(expected)} is only for an `includes` projection"
                )
            for key, items in expected.items():
                if not isinstance(items, list) or not items:
                    return f"{path}.{key}: a non-empty list of projections is required"
                for index, item in enumerate(items):
                    if found := unclaimed(item, f"{path}.{key}[{index}]", exact=False):
                        return found
            return None
        if not expected and not exact:
            return f'{path}: a bare {{}} claims nothing; write {{"$any": true}}'
        if exact and ABSENT in expected.values():
            # An exact object claims absence by leaving the key out.
            return f'{path}: {{"$absent": true}} is only for an `includes` projection'
        items = (
            (f"{path}.{key}", item) for key, item in expected.items() if item != ABSENT
        )
    elif isinstance(expected, list):
        items = ((f"{path}[{index}]", item) for index, item in enumerate(expected))
    else:
        return None
    for item_path, item in items:
        if found := unclaimed(item, item_path, exact=exact):
            return found
    return None
