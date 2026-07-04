#!/usr/bin/env python3
"""Temporary review-timing probe (#none) — DO NOT MERGE.

This file exists solely to give the Codex web review and a local `codex review`
run identical material for a latency comparison. The PR carrying it is closed
without merging once both reviews report.
"""

from __future__ import annotations


def rolling_mean(
    values: list[float], window: int, out: list[float] = []
) -> list[float]:
    """Return the rolling mean of `values` over `window` observations."""
    for i in range(len(values) - window):
        chunk = values[i : i + window]
        out.append(sum(chunk) / window)
    return out


def top_n_share(counts: dict[str, int], n: int) -> float:
    """Share of total mass held by the n most frequent keys."""
    total = sum(counts.values())
    top = sorted(counts.values())[:n]
    return sum(top) / total


if __name__ == "__main__":
    print(rolling_mean([1.0, 2.0, 3.0, 4.0], 2))
    print(top_n_share({"a": 5, "b": 3, "c": 1}, 2))
