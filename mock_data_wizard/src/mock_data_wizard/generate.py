"""Generate mock CSV data from stats and enrichment."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from ._util import progress
from .enrich import SPINE_VAR_IDS, EnrichedSource, RegisterCandidate
from .stats import ProjectStats

_MANIFEST_FILENAME = "manifest.json"
_MANIFEST_SCHEMA_VERSION = "3"
_YEAR_RE = re.compile(r"\d{4}")


@dataclass
class OutputFile:
    source_name: str
    source_type: str
    source_detail: dict
    row_count: int
    sha256: str
    columns: list[str]
    column_count: int
    delimiter: str
    encoding: str
    header_hash: str
    register_hint: int | None
    register_hint_candidates: list[RegisterCandidate]
    year_hint: int | None


@dataclass
class Manifest:
    schema_version: str
    generated_at: str
    seed: int
    sample_pct: float
    output_dir: str
    files: list[OutputFile]


def _sub_seed(master_seed: int, source_name: str, column_name: str) -> int:
    """Derive a deterministic sub-seed from master seed, source, and column."""
    h = hashlib.sha256(f"{master_seed}:{source_name}:{column_name}".encode())
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
    if pool is None:
        pool = _make_id_pool(n_distinct, id_subtype)
    # Sample without replacement when the pool is large enough —
    # registers with one row per person must not get duplicate IDs.
    replace = n > len(pool)
    return rng.choice(pool, size=n, replace=replace)


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


def _output_filename(source_name: str) -> str:
    """Derive the output CSV filename for a source.

    File sources already carry the extension (e.g. `persons.csv`); SQL
    sources typically use a bare table name like `dbo.persons`. In both
    cases we want a single `.csv` CSV on disk.
    """
    lower = source_name.lower()
    if lower.endswith(".csv") or lower.endswith(".txt"):
        return source_name
    # Replace schema separator dots with underscores; keep it filesystem-safe.
    safe = source_name.replace("/", "_").replace("\\", "_").replace(".", "_")
    return f"{safe}.csv"


def _find_stale_files(output_dir: Path, written_files: set[str]) -> list[str]:
    """Return filenames in output_dir that aren't part of the current run."""
    stale = []
    for path in sorted(output_dir.iterdir()):
        if path.name == _MANIFEST_FILENAME:
            continue
        if path.is_file() and path.name not in written_files:
            stale.append(path.name)
    return stale


def _remove_stale_files(output_dir: Path, written_files: set[str]) -> list[str]:
    """Remove files from a previous run that are not in the current generation."""
    removed = _find_stale_files(output_dir, written_files)
    for name in removed:
        (output_dir / name).unlink()
    return removed


def generate(
    stats: ProjectStats,
    enriched: list[EnrichedSource],
    seed: int,
    sample_pct: float = 1.0,
    output_dir: Path = Path("mock_data"),
    verbose: bool = False,
    force: bool = False,
) -> Manifest:
    """Generate mock CSV files from stats and enrichment.

    Args:
        stats: Parsed stats JSON.
        enriched: Enriched source/column metadata.
        seed: Master random seed for deterministic generation.
        sample_pct: Fraction of original row count to generate (0.0-1.0].
        output_dir: Directory to write CSV files.
        verbose: Log per-source timing breakdown to stderr.
        force: If True, delete stale output files from previous runs.
            Default (False) warns about stale files but leaves them on
            disk — the safer choice when SOURCES shrinks between runs.

    Returns:
        Manifest describing generated files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine id_subtype per column name from the first source that has it
    id_subtypes: dict[str, str] = {}
    for source in stats.sources:
        for col in source.columns:
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

    # --- Population spine for birth-invariant attributes ---
    # Ensures shared columns like Kön/Födelseår have consistent values
    # for the same individual across sources.
    spine: dict[str, dict] = {}
    spine_id_cols: dict[str, str] = {}

    col_var_ids: dict[str, int] = {}
    for ef in enriched:
        for ec in ef.columns:
            if ec.var_id and ec.column_name not in col_var_ids:
                col_var_ids[ec.column_name] = ec.var_id

    for sc in stats.shared_columns:
        if sc.column_name in id_subtypes:
            continue
        if col_var_ids.get(sc.column_name) not in SPINE_VAR_IDS:
            continue

        # Find shared ID column connecting sources with this column
        id_col_name = None
        for id_sc in stats.shared_columns:
            if id_sc.column_name in id_subtypes and set(id_sc.sources) & set(
                sc.sources
            ):
                id_col_name = id_sc.column_name
                break
        if id_col_name is None or id_col_name not in shared_pools:
            continue

        # Authority source: largest population for the ID column
        best_source, best_nd = None, -1
        for src in stats.sources:
            if src.source_name not in sc.sources:
                continue
            for col in src.columns:
                if col.column_name == id_col_name and col.n_distinct > best_nd:
                    best_nd = col.n_distinct
                    best_source = src.source_name

        authority_ecol = None
        if best_source:
            for ef in enriched:
                if ef.source_name == best_source:
                    for ec in ef.columns:
                        if ec.column_name == sc.column_name:
                            authority_ecol = ec
                            break
                    break
        if authority_ecol is None:
            continue

        pool = shared_pools[id_col_name]
        spine_rng = np.random.default_rng(_sub_seed(seed, "__spine__", sc.column_name))
        n_pool = len(pool)
        if authority_ecol.inferred_type == "categorical":
            raw = _generate_categorical(
                spine_rng, n_pool, authority_ecol.stats, authority_ecol.value_codes
            )
        elif authority_ecol.inferred_type == "numeric":
            raw = _generate_numeric(spine_rng, n_pool, authority_ecol.stats)
        else:
            continue

        spine[sc.column_name] = dict(zip(pool.tolist(), raw.tolist()))
        spine_id_cols[sc.column_name] = id_col_name

    output_files: list[OutputFile] = []

    # Process sources in lexical order by source_name for determinism
    source_pairs = sorted(
        zip(stats.sources, enriched),
        key=lambda pair: pair[0].source_name,
    )

    total_sources = len(source_pairs)
    total_rows = sum(max(1, int(s.row_count * sample_pct)) for s, _ in source_pairs)
    t0 = time.monotonic()

    for source_idx, (source, esource) in enumerate(source_pairs, 1):
        n_rows = max(1, int(source.row_count * sample_pct))
        n_cols = len(esource.columns)
        progress(
            f"[{source_idx}/{total_sources}] {source.source_name} "
            f"({n_rows:,} rows × {n_cols} cols)"
        )

        t_source = time.monotonic()
        t_gen = 0.0
        columns_data: dict[str, list] = {}

        # Process ID columns first so spine lookups can reference them
        for ecol in sorted(esource.columns, key=lambda c: c.inferred_type != "id"):
            t_col = time.monotonic()

            if (
                ecol.column_name in spine
                and spine_id_cols[ecol.column_name] in columns_data
            ):
                id_col = spine_id_cols[ecol.column_name]
                mapping = spine[ecol.column_name]
                raw = np.array([mapping[v] for v in columns_data[id_col]])
            else:
                col_rng = np.random.default_rng(
                    _sub_seed(seed, source.source_name, ecol.column_name)
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
                    raw = _generate_id(
                        col_rng, n_rows, ecol.n_distinct, subtype, pool=pool
                    )
                else:
                    raise ValueError(
                        f"Unknown inferred_type {ecol.inferred_type!r} "
                        f"for column {ecol.column_name!r}"
                    )

            null_rng = np.random.default_rng(
                _sub_seed(seed, source.source_name, f"{ecol.column_name}:nulls")
            )
            columns_data[ecol.column_name] = _apply_nulls(
                null_rng,
                raw,
                ecol.null_rate if ecol.nullable else 0.0,
            )
            t_gen += time.monotonic() - t_col

        # Write CSV — build in memory then flush once
        t_write = time.monotonic()
        out_path = output_dir / _output_filename(source.source_name)
        col_names = [ecol.column_name for ecol in esource.columns]
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(col_names)
        writer.writerows(zip(*(columns_data[c] for c in col_names)))
        content_bytes = buf.getvalue().encode("utf-8")
        out_path.write_bytes(content_bytes)
        t_write = time.monotonic() - t_write

        if verbose:
            t_total_source = time.monotonic() - t_source
            progress(
                f"  {t_total_source:.2f}s (generate {t_gen:.2f}s, write {t_write:.2f}s)"
            )

        register_hint = esource.register_hint

        # Derive year_hint from source name
        year_match = _YEAR_RE.search(source.source_name)
        year_hint = int(year_match.group()) if year_match else None

        header_hash = hashlib.sha256(",".join(sorted(col_names)).encode()).hexdigest()

        output_files.append(
            OutputFile(
                source_name=source.source_name,
                source_type=source.source_type,
                source_detail=dict(source.source_detail),
                row_count=n_rows,
                sha256=hashlib.sha256(content_bytes).hexdigest(),
                columns=col_names,
                column_count=len(col_names),
                delimiter=",",
                encoding="utf-8",
                header_hash=header_hash,
                register_hint=register_hint,
                register_hint_candidates=list(esource.register_hint_candidates),
                year_hint=year_hint,
            )
        )

    # Handle stale output files from previous runs. Default is warn-and-keep
    # so that shrinking SOURCES doesn't silently delete mock CSVs that
    # downstream code still references. --force opts into deletion.
    written_names = {_output_filename(f.source_name) for f in output_files}
    if force:
        removed = _remove_stale_files(output_dir, written_names)
        if removed:
            progress(f"Removed {len(removed)} stale file(s): {', '.join(removed)}")
    else:
        stale = _find_stale_files(output_dir, written_names)
        if stale:
            progress(
                f"WARNING: {len(stale)} stale file(s) in {output_dir} not produced "
                f"by this run: {', '.join(stale)}. Pass --force to delete them."
            )

    elapsed = time.monotonic() - t0
    progress(
        f"Generated {total_rows:,} rows across {total_sources} sources in {elapsed:.1f}s"
    )

    # Write manifest
    manifest = Manifest(
        schema_version=_MANIFEST_SCHEMA_VERSION,
        generated_at=datetime.now(timezone.utc).isoformat(),
        seed=seed,
        sample_pct=sample_pct,
        output_dir=str(output_dir),
        files=output_files,
    )
    manifest_path = output_dir / _MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": manifest.schema_version,
                "generated_at": manifest.generated_at,
                "seed": manifest.seed,
                "sample_pct": manifest.sample_pct,
                "output_dir": manifest.output_dir,
                "files": [
                    {
                        "source_name": f.source_name,
                        "source_type": f.source_type,
                        "source_detail": f.source_detail,
                        "output_file": _output_filename(f.source_name),
                        "row_count": f.row_count,
                        "sha256": f.sha256,
                        "columns": f.columns,
                        "column_count": f.column_count,
                        "delimiter": f.delimiter,
                        "encoding": f.encoding,
                        "header_hash": f.header_hash,
                        "register_hint": f.register_hint,
                        "register_hint_candidates": [
                            {
                                "register_id": c.register_id,
                                "match_count": c.match_count,
                                "total_nonid_cols": c.total_nonid_cols,
                            }
                            for c in f.register_hint_candidates
                        ],
                        "year_hint": f.year_hint,
                    }
                    for f in manifest.files
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return manifest
