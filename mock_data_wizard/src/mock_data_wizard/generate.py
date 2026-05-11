"""Generate mock CSV data from stats and enrichment."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from ._util import progress
from .enrich import SPINE_VAR_IDS, EnrichedSource, RegisterCandidate
from .stats import Panel, ProjectStats

_MANIFEST_FILENAME = "manifest.json"
_MANIFEST_SCHEMA_VERSION = "3"
MOCK_DATA_DIRNAME = "mock_data"


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
    # `_other` may be null: extract nulls the bucket when 0 < suppressed_total
    # < SUPPRESS_K so the exact tiny count can't be inferred. Treat as 0 --
    # we never emit "_other" as a value, only use it to weight unseen codes.
    other_weight = frequencies.get("_other") or 0

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


def _generate_opaque(
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
    return np.array([(d_min + timedelta(days=int(o))).strftime(fmt) for o in offsets])


def _make_id_pool(n_distinct: int, id_subtype: str) -> np.ndarray:
    pool_size = max(n_distinct, 1)
    if id_subtype == "integer":
        return np.arange(1, pool_size + 1)
    pad = len(str(pool_size))
    return np.array([f"ID_{i:0{pad}d}" for i in range(pool_size)])


def _build_panel_pools(
    panels: list[Panel],
    id_subtypes: dict[str, str],
    seed: int,
    sample_pct: float,
) -> tuple[dict[str, np.ndarray], dict[tuple[str, int | str, str], np.ndarray]]:
    """Build per-entity_key id pools and per-(period, source) subsets.

    All panels sharing an ``entity_key`` share one id pool — in SCB
    register data, ``entity_key`` is typically the person identifier
    (e.g. ``P1105_LopNr_PersonNr``) and dozens of distinct registers
    legitimately reference the same id universe. Pool size is
    ``max(n_entity_ids)`` across every period of every panel using that
    key, and the shuffle seed derives from the key (not panel_id) so
    the universe is stable.

    Each ``(period, source)`` entry takes a *prefix* of the shuffled
    pool sized to that entry's ``n_entity_ids``. Strict prefix nesting
    gives stable cross-period overlap (panel persistence): sequential
    periods share ``min(n_entity_ids)`` of their ids, and per-source
    distinctness matches the source's stats. Cross-period attrition
    modelling (transition matrices, churn) is explicitly out of scope.

    Returned ``pools`` is keyed by ``panel_id`` for the caller's
    convenience — entries for panels sharing an ``entity_key`` reference
    the same underlying array.
    """
    by_key: dict[str, list[Panel]] = {}
    for panel in panels:
        if not panel.by_period:
            continue
        by_key.setdefault(panel.entity_key, []).append(panel)

    pools: dict[str, np.ndarray] = {}
    subsets: dict[tuple[str, int | str, str], np.ndarray] = {}
    for entity_key, key_panels in by_key.items():
        pool_size = max(p.n_entity_ids for pn in key_panels for p in pn.by_period)
        if sample_pct < 1.0:
            pool_size = max(1, int(pool_size * sample_pct))
        subtype = id_subtypes.get(entity_key, "string")
        pool_rng = np.random.default_rng(_sub_seed(seed, "__panel__", entity_key))
        pool = _make_id_pool(pool_size, subtype)
        pool_rng.shuffle(pool)
        for panel in key_panels:
            pools[panel.panel_id] = pool
            for ps in panel.by_period:
                subset_size = ps.n_entity_ids
                if sample_pct < 1.0:
                    subset_size = max(1, int(subset_size * sample_pct))
                subset_size = min(subset_size, len(pool))
                subsets[(panel.panel_id, ps.period, ps.source)] = pool[:subset_size]
    return pools, subsets


def _generate_merged_panel_columns(
    n_rows: int,
    panel: Panel,
    panel_subsets: dict[tuple[str, int | str, str], np.ndarray],
    rng: np.random.Generator,
    *,
    member_source: str,
) -> tuple[np.ndarray, np.ndarray] | None:
    """Co-generate ``(time_key_column, entity_key)`` for a column-member.

    Each row is assigned one of the periods that this member's source
    actually contributed to, weighted by its observed ``n_rows``. The
    entity-key value for that row is then drawn from the period's pool
    subset. Restricting to ``member_source`` keeps generated rows
    consistent with the extract output — a multi-source panel won't
    bleed periods from a sibling member into this file.

    Returns ``None`` when the source has no surviving periods (e.g.
    every period was suppressed by SUPPRESS_K but a sibling member
    survived), so the caller can fall through to normal column
    generation rather than emit uninitialised arrays.
    """
    member_periods = [p for p in panel.by_period if p.source == member_source]
    if not member_periods:
        return None
    period_list = [p.period for p in member_periods]
    periods = np.array(period_list, dtype=object)
    weights = np.array([p.n_rows for p in member_periods], dtype=float)
    weights /= weights.sum()
    period_idx = rng.choice(len(periods), size=n_rows, p=weights)
    period_values = periods[period_idx]
    entity_values = np.empty(n_rows, dtype=object)
    for i, period in enumerate(period_list):
        mask = period_idx == i
        count = int(mask.sum())
        if count == 0:
            continue
        subset = panel_subsets.get((panel.panel_id, period, member_source))
        if subset is None or len(subset) == 0:
            entity_values[mask] = ""
            continue
        replace = count > len(subset)
        entity_values[mask] = rng.choice(subset, size=count, replace=replace)
    return period_values, entity_values


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
    if null_rate <= 0:
        return values.tolist()
    mask = rng.random(len(values)) < null_rate
    result = values.astype(object)
    result[mask] = ""
    return result.tolist()


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
    output_dir: Path = Path(MOCK_DATA_DIRNAME),
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

    # Panel pools come first so we can route entity-key columns to them
    # when building shared_pools below. Panel ids replace the shared
    # pool for that column so spine + non-panel sources stay aligned
    # with the same id universe.
    panel_pools, panel_subsets = _build_panel_pools(
        stats.panels, id_subtypes, seed, sample_pct
    )
    panel_pool_for_col: dict[str, np.ndarray] = {}
    for panel in stats.panels:
        if panel.panel_id in panel_pools:
            panel_pool_for_col[panel.entity_key] = panel_pools[panel.panel_id]
    # panel_by_source maps a source name → (panel, period, time_key_column).
    # For a file-member, ``period`` is the literal period (int/str) and
    # ``time_key_column`` is None. For a column-member, ``period`` is
    # None and ``time_key_column`` names the column whose values are the
    # period. Distinguishes the two via member.time_key's runtime type.
    panel_by_source: dict[str, tuple[Panel, int | str | None, str | None]] = {}
    for panel in stats.panels:
        for member in panel.members:
            if isinstance(member.time_key, str):
                panel_by_source[member.source] = (panel, None, member.time_key)
            else:
                panel_by_source[member.source] = (panel, member.time_key, None)

    # Build shared ID pools — sample the pool itself when sample_pct < 1
    # so that files sharing an ID column draw from the same reduced universe
    shared_pools: dict[str, np.ndarray] = {}
    for sc in stats.shared_columns:
        # If this column is the entity_key of any panel, the panel pool
        # is authoritative -- sources outside the panel that share this
        # column then draw from the same universe.
        if sc.column_name in panel_pool_for_col:
            shared_pools[sc.column_name] = panel_pool_for_col[sc.column_name]
            continue
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

        # Pre-generate panel-managed columns for sources participating
        # in a panel. The main loop below skips columns already in
        # ``columns_data``; nullability is intentionally not applied to
        # panel columns (panel structure must be preserved row-for-row).
        # When a panel (or this source within it) has no surviving
        # periods, fall through to normal column generation --
        # overwriting time_key / panel_key with empty/garbage values
        # would be worse than losing panel structure.
        panel_info = panel_by_source.get(source.source_name)
        if panel_info is not None and panel_info[0].by_period:
            panel_obj, period, time_key = panel_info
            panel_rng = np.random.default_rng(
                _sub_seed(seed, source.source_name, "__panel__")
            )
            if time_key is not None:
                # Column-member: filter by_period to this source's
                # contributions before sampling so the (time_key column,
                # entity_key) pair stays internally consistent. Returns
                # None when this source has no surviving periods.
                result = _generate_merged_panel_columns(
                    n_rows,
                    panel_obj,
                    panel_subsets,
                    panel_rng,
                    member_source=source.source_name,
                )
                if result is not None:
                    time_vals, entity_vals = result
                    # time_key column may be typed int/categorical; emit
                    # whatever the period values are (typically int years).
                    columns_data[time_key] = time_vals.tolist()
                    columns_data[panel_obj.entity_key] = entity_vals.tolist()
            elif period is not None:  # file-member
                subset = panel_subsets.get(
                    (panel_obj.panel_id, period, source.source_name)
                )
                if subset is not None and len(subset) > 0:
                    replace = n_rows > len(subset)
                    entity_vals = panel_rng.choice(subset, size=n_rows, replace=replace)
                    columns_data[panel_obj.entity_key] = entity_vals.tolist()

        # Process ID columns first so spine lookups can reference them
        for ecol in sorted(esource.columns, key=lambda c: c.inferred_type != "id"):
            t_col = time.monotonic()

            if ecol.column_name in columns_data:
                # Pre-generated by the panel pass. Skip null injection
                # too: panel structure relies on row-for-row alignment.
                continue

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
                elif ecol.inferred_type == "opaque":
                    raw = _generate_opaque(col_rng, n_rows, ecol.stats, ecol.n_distinct)
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
        # extract.py populates source_detail["year"] (config override or
        # name-regex fallback); reuse it instead of re-running the regex.
        detail_year = source.source_detail.get("year")
        year_hint = int(detail_year) if detail_year is not None else None

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
