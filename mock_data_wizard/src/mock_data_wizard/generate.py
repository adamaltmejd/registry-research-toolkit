"""Generate mock CSV data from stats and enrichment."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from ._util import progress
from .enrich import EnrichedFile
from .stats import ProjectStats

_MANIFEST_FILENAME = "manifest.json"

@dataclass
class OutputFile:
    file_name: str
    relative_path: str
    row_count: int
    sha256: str


@dataclass
class Manifest:
    seed: int
    sample_pct: float
    output_dir: str
    files: list[OutputFile]


def _sub_seed(master_seed: int, file_name: str, column_name: str) -> int:
    """Derive a deterministic sub-seed from master seed, file, and column."""
    h = hashlib.sha256(f"{master_seed}:{file_name}:{column_name}".encode())
    return int.from_bytes(h.digest()[:4], "big")



def _generate_numeric(
    rng: np.random.Generator,
    n: int,
    stats: dict,
) -> np.ndarray:
    mean = stats.get("mean", 0.0)
    sd = stats.get("sd", 1.0)
    lo = stats.get("min", mean - 4 * sd)
    hi = stats.get("max", mean + 4 * sd)

    if sd == 0 or sd is None:
        values = np.full(n, mean)
    else:
        values = rng.normal(mean, sd, size=n)
        values = np.clip(values, lo, hi)

    is_int = stats.get("numeric_subtype", "double") == "integer"
    if is_int:
        values = np.round(values).astype(int)

    return values


def _generate_categorical(
    rng: np.random.Generator,
    n: int,
    stats: dict,
    value_codes: dict[str, str] | None,
) -> np.ndarray:
    frequencies = stats.get("frequencies", {})
    freq = {k: v for k, v in frequencies.items() if k != "_other"}
    other_weight = frequencies.get("_other", 0)

    if freq:
        codes = list(freq.keys())
        weights = np.array(list(freq.values()), dtype=float)

        if other_weight > 0:
            # Distribute censored count across unseen regmeta codes if available,
            # otherwise fold back proportionally into observed values
            unseen = [c for c in value_codes if c not in freq] if value_codes else []
            if unseen:
                per_unseen = other_weight / len(unseen)
                codes += unseen
                weights = np.append(weights, [per_unseen] * len(unseen))
            else:
                weights += other_weight * (weights / weights.sum())

        weights /= weights.sum()
        return rng.choice(codes, size=n, p=weights)

    if value_codes:
        codes = list(value_codes.keys())
        return rng.choice(codes, size=n)

    return np.array([f"cat_{i}" for i in range(n)])


def _generate_high_cardinality(
    rng: np.random.Generator,
    n: int,
    stats: dict,
    n_distinct: int,
) -> np.ndarray:
    pool_size = max(n_distinct, 1)
    pad = max(len(str(pool_size)), len(str(n)))
    indices = rng.integers(0, pool_size, size=n)
    return np.array([f"val_{i:0{pad}d}" for i in indices])


def _generate_date(
    rng: np.random.Generator,
    n: int,
    stats: dict,
) -> np.ndarray:
    min_str = stats.get("min", "2000-01-01")
    max_str = stats.get("max", "2025-12-31")
    fmt = stats.get("date_format", "%Y-%m-%d")

    try:
        d_min = date.fromisoformat(min_str)
        d_max = date.fromisoformat(max_str)
    except (ValueError, TypeError):
        d_min = date(2000, 1, 1)
        d_max = date(2025, 12, 31)

    span = (d_max - d_min).days
    if span <= 0:
        span = 1

    offsets = rng.integers(0, span + 1, size=n)
    # Use the format from the R script if available
    output_fmt = "%Y-%m-%d"
    if fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
        output_fmt = fmt

    return np.array(
        [(d_min + timedelta(days=int(o))).strftime(output_fmt) for o in offsets]
    )


def _make_id_pool(n_distinct: int, id_subtype: str) -> np.ndarray:
    pool_size = max(n_distinct, 1)
    if id_subtype == "integer":
        return np.arange(1, pool_size + 1)
    pad = len(str(pool_size))
    return np.array([f"ID_{i:0{pad}d}" for i in range(pool_size)])


def _generate_id(
    rng: np.random.Generator,
    n: int,
    n_distinct: int,
    id_subtype: str,
    pool: np.ndarray | None = None,
) -> np.ndarray:
    if pool is not None:
        return rng.choice(pool, size=n)
    return rng.choice(_make_id_pool(n_distinct, id_subtype), size=n)


def _apply_nulls(
    rng: np.random.Generator,
    values: np.ndarray,
    null_rate: float,
) -> list:
    """Apply null mask and convert to Python list."""
    result = values.tolist()
    if null_rate <= 0:
        return result
    mask = rng.random(len(result)) < null_rate
    for i in range(len(result)):
        if mask[i]:
            result[i] = ""
    return result


def _remove_stale_files(output_dir: Path, written_files: set[str]) -> list[str]:
    """Remove files from a previous run that are not in the current generation."""
    removed = []
    for path in sorted(output_dir.iterdir()):
        if path.name == _MANIFEST_FILENAME:
            continue
        if path.is_file() and path.name not in written_files:
            path.unlink()
            removed.append(path.name)
    return removed


def generate(
    stats: ProjectStats,
    enriched: list[EnrichedFile],
    seed: int,
    sample_pct: float = 1.0,
    output_dir: Path = Path("mock_data"),
    verbose: bool = False,
) -> Manifest:
    """Generate mock CSV files from stats and enrichment.

    Args:
        stats: Parsed stats JSON.
        enriched: Enriched file/column metadata.
        seed: Master random seed for deterministic generation.
        sample_pct: Fraction of original row count to generate (0.0-1.0].
        output_dir: Directory to write CSV files.
        verbose: Log per-file timing breakdown to stderr.

    Returns:
        Manifest describing generated files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine id_subtype per column name from the first file that has it
    id_subtypes: dict[str, str] = {}
    for file_stats in stats.files:
        for col in file_stats.columns:
            if col.inferred_type == "id" and col.column_name not in id_subtypes:
                id_subtypes[col.column_name] = col.stats["id_subtype"]

    # Build shared ID pools — sample the pool itself when sample_pct < 1
    # so that files sharing an ID column draw from the same reduced universe
    shared_pools: dict[str, np.ndarray] = {}
    for sc in stats.shared_columns:
        subtype = id_subtypes.get(sc.column_name, "string")
        pool_size = max(sc.max_n_distinct, 1)
        if sample_pct < 1.0:
            # Sample indices first, then materialize only the sampled IDs
            pool_n = max(1, int(pool_size * sample_pct))
            pool_rng = np.random.default_rng(
                _sub_seed(seed, "__pool__", sc.column_name)
            )
            sampled_indices = pool_rng.choice(pool_size, size=pool_n, replace=False)
            if subtype == "integer":
                shared_pools[sc.column_name] = sampled_indices + 1
            else:
                pad = len(str(pool_size))
                shared_pools[sc.column_name] = np.array(
                    [f"ID_{i:0{pad}d}" for i in sampled_indices]
                )
        else:
            shared_pools[sc.column_name] = _make_id_pool(pool_size, subtype)

    output_files: list[OutputFile] = []

    # Process files in lexical order for determinism
    file_pairs = sorted(
        zip(stats.files, enriched),
        key=lambda pair: pair[0].relative_path,
    )

    total_files = len(file_pairs)
    total_rows = sum(max(1, int(fs.row_count * sample_pct)) for fs, _ in file_pairs)
    t0 = time.monotonic()

    for file_idx, (file_stats, efile) in enumerate(file_pairs, 1):
        n_rows = max(1, int(file_stats.row_count * sample_pct))
        n_cols = len(efile.columns)
        progress(
            f"[{file_idx}/{total_files}] {file_stats.file_name} "
            f"({n_rows:,} rows × {n_cols} cols)"
        )

        t_file = time.monotonic()
        t_gen = 0.0
        columns_data: dict[str, list] = {}

        for ecol in efile.columns:
            t_col = time.monotonic()
            col_rng = np.random.default_rng(
                _sub_seed(seed, file_stats.file_name, ecol.column_name)
            )

            if ecol.inferred_type == "numeric":
                raw = _generate_numeric(col_rng, n_rows, ecol.stats)
            elif ecol.inferred_type == "categorical":
                raw = _generate_categorical(
                    col_rng, n_rows, ecol.stats, ecol.value_codes
                )
            elif ecol.inferred_type == "high_cardinality":
                raw = _generate_high_cardinality(
                    col_rng, n_rows, ecol.stats, ecol.n_distinct
                )
            elif ecol.inferred_type == "date":
                raw = _generate_date(col_rng, n_rows, ecol.stats)
            elif ecol.inferred_type == "id":
                pool = shared_pools.get(ecol.column_name)
                subtype = ecol.stats["id_subtype"]
                raw = _generate_id(col_rng, n_rows, ecol.n_distinct, subtype, pool=pool)
            else:
                raise ValueError(
                    f"Unknown inferred_type {ecol.inferred_type!r} "
                    f"for column {ecol.column_name!r}"
                )

            null_rng = np.random.default_rng(
                _sub_seed(seed, file_stats.file_name, f"{ecol.column_name}:nulls")
            )
            columns_data[ecol.column_name] = _apply_nulls(
                null_rng,
                raw,
                ecol.null_rate if ecol.nullable else 0.0,
            )
            t_gen += time.monotonic() - t_col

        # Write CSV — build in memory then flush once
        t_write = time.monotonic()
        out_path = output_dir / file_stats.file_name
        col_names = [ecol.column_name for ecol in efile.columns]
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(col_names)
        writer.writerows(zip(*(columns_data[c] for c in col_names)))
        content_bytes = buf.getvalue().encode("utf-8")
        out_path.write_bytes(content_bytes)
        t_write = time.monotonic() - t_write

        if verbose:
            t_total_file = time.monotonic() - t_file
            progress(
                f"  {t_total_file:.2f}s (generate {t_gen:.2f}s, write {t_write:.2f}s)"
            )

        output_files.append(
            OutputFile(
                file_name=file_stats.file_name,
                relative_path=file_stats.relative_path,
                row_count=n_rows,
                sha256=hashlib.sha256(content_bytes).hexdigest(),
            )
        )

    # Clean stale files from previous runs
    written_names = {f.file_name for f in output_files}
    removed = _remove_stale_files(output_dir, written_names)
    if removed:
        progress(f"Removed {len(removed)} stale file(s): {', '.join(removed)}")

    elapsed = time.monotonic() - t0
    progress(
        f"Generated {total_rows:,} rows across {total_files} files in {elapsed:.1f}s"
    )

    # Write manifest
    manifest = Manifest(
        seed=seed,
        sample_pct=sample_pct,
        output_dir=str(output_dir),
        files=output_files,
    )
    manifest_path = output_dir / _MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(
            {
                "seed": manifest.seed,
                "sample_pct": manifest.sample_pct,
                "output_dir": manifest.output_dir,
                "files": [
                    {
                        "file_name": f.file_name,
                        "relative_path": f.relative_path,
                        "row_count": f.row_count,
                        "sha256": f.sha256,
                    }
                    for f in manifest.files
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return manifest
