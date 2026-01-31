#!/usr/bin/env python3
"""
polars_parquet_smoketest.py

Small Polars smoke test for a Hive-partitioned Parquet dataset exported by `export_ht_to_parquet.py`.

Expected dataset layout:
  <dataset_root>/contig=chrY/part-*.parquet
  <dataset_root>/contig=chr1/part-*.parquet
  ...

This script:
- Scans the dataset lazily with Polars (no full materialization).
- Filters to a contig (default: chrY).
- Runs a few point and range position queries.
- Computes some aggregate stats (row count, min/max position, etc.).
- Prints timing for each query so you can sanity-check pruning/perf.

Requirements:
  pip install polars

Usage:
  python3 polars_parquet_smoketest.py --dataset ./gnomad.genomes.v4.1.sites.VRS.parquet --contig chrY
  python3 polars_parquet_smoketest.py --dataset ./gnomad.genomes.v4.1.sites.VRS.parquet --contig chrY --n-point-queries 10
  python3 polars_parquet_smoketest.py --dataset ./gnomad.genomes.v4.1.sites.VRS.parquet --contig chr1 --n-range-queries 8 --range-width 500000

How to use the dataset from DuckDB / Polars:
  DuckDB:
    SELECT *
    FROM parquet_scan('parquet_out/**/*.parquet', hive_partitioning=1)
    WHERE contig='chrY' AND position BETWEEN 20000000 AND 21000000;

  Polars:
    import polars as pl
    lf = pl.scan_parquet("parquet_out", hive_partitioning=True)
    lf.filter((pl.col("contig")=="chrY") & pl.col("position").is_between(20000000, 21000000)).collect()
"""

from __future__ import annotations

import argparse
import random
import statistics
import time
from pathlib import Path

import polars as pl


def _human_bytes(num_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


def _time_it(label: str, fn):
    t0 = time.perf_counter()
    out = fn()
    dt = time.perf_counter() - t0
    print(f"{label}: {dt:.3f}s")
    return out


def _collect(lf: pl.LazyFrame, *, streaming: bool) -> pl.DataFrame:
    # Older Polars versions may not support `streaming=...`; fall back.
    try:
        return lf.collect(streaming=streaming)
    except TypeError:
        return lf.collect()


def _parquet_file_stats(dataset_root: Path, contig: str) -> None:
    contig_dir = dataset_root / f"contig={contig}"
    if not contig_dir.exists():
        # If the user passed a contig dir directly, accept that too.
        contig_dir = dataset_root
    files = [p for p in contig_dir.rglob("*.parquet") if p.is_file()]
    if not files:
        print(f"No parquet files found under: {contig_dir}")
        return

    sizes = [p.stat().st_size for p in files]
    total = sum(sizes)
    print(f"Parquet files under {contig_dir}: {len(files)}")
    print(
        "File sizes:",
        f"total={_human_bytes(total)}",
        f"min={_human_bytes(min(sizes))}",
        f"p50={_human_bytes(int(statistics.median(sizes)))}",
        f"max={_human_bytes(max(sizes))}",
        sep=" ",
    )


def _list_contig_parquet_files(dataset_root: Path, contig: str) -> list[Path]:
    contig_dir = dataset_root / f"contig={contig}"
    if not contig_dir.exists():
        # If the user passed a contig dir directly, accept that too.
        contig_dir = dataset_root
    return sorted([p for p in contig_dir.rglob("*.parquet") if p.is_file()])


def _sample_existing_positions_from_files(
    parquet_files: list[Path],
    *,
    n_files: int,
    n_rows_per_file: int,
    seed: int,
) -> list[int]:
    """
    Return a list of positions known to exist in the dataset by sampling a small number of files.

    This avoids sampling via a full-table scan. Distribution depends on how the exporter laid out
    positions across files; for keyed-by-locus datasets this is usually "good enough".
    """
    if not parquet_files:
        return []

    rng = random.Random(seed)
    n_files = min(n_files, len(parquet_files))
    sampled_files = rng.sample(parquet_files, n_files)

    positions: list[int] = []
    for path in sampled_files:
        try:
            df = pl.read_parquet(path, columns=["position"], n_rows=n_rows_per_file)
        except Exception:
            # Skip unreadable/corrupt files without failing the smoke test.
            continue
        if df.is_empty():
            continue
        positions.extend(
            int(x) for x in df.get_column("position").to_list() if x is not None
        )

    return positions


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polars smoke test for contig-partitioned Parquet."
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Parquet dataset root (directory containing contig=... subdirs).",
    )
    parser.add_argument(
        "--contig", default="chrY", help="Contig to query (default: chrY)."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1,
        help="Random seed used for sampling files/positions (default: 1).",
    )
    parser.add_argument(
        "--n-point-queries",
        type=int,
        default=8,
        help="How many existing point positions to query (default: 8).",
    )
    parser.add_argument(
        "--sample-files",
        type=int,
        default=25,
        help="How many parquet files to sample for point positions (default: 25).",
    )
    parser.add_argument(
        "--rows-per-file-sample",
        type=int,
        default=100,
        help="How many rows to read from each sampled parquet file (default: 100).",
    )
    parser.add_argument(
        "--n-range-queries",
        type=int,
        default=5,
        help="How many range queries to run (default: 5).",
    )
    parser.add_argument(
        "--range-width",
        type=int,
        default=100_000,
        help="Width (bp) of each range query (default: 100000).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max rows to print for example queries (default: 5).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run more expensive stats (e.g. n_unique(position)).",
    )
    parser.add_argument(
        "--no-streaming",
        action="store_true",
        help="Disable Polars streaming collects (useful if your Polars build doesn't support it).",
    )
    args = parser.parse_args()

    dataset_root = Path(args.dataset)
    _parquet_file_stats(dataset_root, args.contig)
    parquet_files = _list_contig_parquet_files(dataset_root, args.contig)

    scan_path = str(dataset_root / "**" / "*.parquet")
    lf = pl.scan_parquet(scan_path, hive_partitioning=True)

    schema = lf.collect_schema()
    if "contig" not in schema:
        raise SystemExit(
            "No 'contig' column found. Expected Hive partitioning (contig=chrY directories)."
        )
    if "position" not in schema:
        raise SystemExit("No 'position' column found in Parquet dataset.")

    lf_contig = lf.filter(pl.col("contig") == args.contig)
    streaming = not args.no_streaming

    # Basic contig stats (should be safe even for large contigs).
    basic_stats = _time_it(
        f"{args.contig}: basic stats",
        lambda: _collect(
            lf_contig.select(
                pl.len().alias("n_rows"),
                pl.col("position").min().alias("pos_min"),
                pl.col("position").max().alias("pos_max"),
            ),
            streaming=streaming,
        ),
    )
    print(basic_stats)

    n_rows = int(basic_stats["n_rows"][0])
    pos_min = basic_stats["pos_min"][0]
    pos_max = basic_stats["pos_max"][0]
    if n_rows == 0 or pos_min is None or pos_max is None:
        raise SystemExit(f"No rows found for contig {args.contig}")
    pos_min = int(pos_min)
    pos_max = int(pos_max)
    print(f"{args.contig}: n_rows={n_rows} pos_min={pos_min} pos_max={pos_max}")

    # Quick list-length stats (useful sanity checks for multi-allelic / structural cases).
    len_exprs: list[pl.Expr] = []
    if "alleles" in schema:
        len_exprs.append(pl.col("alleles").list.len().alias("alleles_n"))
    if "VRS_Allele_IDs" in schema:
        len_exprs.append(pl.col("VRS_Allele_IDs").list.len().alias("vrs_ids_n"))

    if len_exprs:
        len_stats = _time_it(
            f"{args.contig}: list-length stats",
            lambda: _collect(
                lf_contig.select(
                    pl.min_horizontal(*len_exprs).alias("min_list_len"),
                    pl.max_horizontal(*len_exprs).alias("max_list_len"),
                ),
                streaming=streaming,
            ),
        )
        print(len_stats)

    if args.full:
        # Potentially expensive; may require a full pass.
        full_stats = _time_it(
            f"{args.contig}: full stats (n_unique(position))",
            lambda: _collect(
                lf_contig.select(
                    pl.col("position").n_unique().alias("n_unique_positions")
                ),
                streaming=False,
            ),
        )
        print(full_stats)

    # Choose a random assortment of existing positions (guaranteed to exist) without manual input.
    file_sample_positions = _sample_existing_positions_from_files(
        parquet_files,
        n_files=args.sample_files,
        n_rows_per_file=args.rows_per_file_sample,
        seed=args.seed,
    )

    # Always include min/max positions (guaranteed to exist by definition).
    candidate_positions = {pos_min, pos_max}
    candidate_positions.update(
        p for p in file_sample_positions if pos_min <= p <= pos_max
    )

    if len(candidate_positions) < max(3, args.n_point_queries, args.n_range_queries):
        # Fallback: read a small head slice from the contig scan (may bias toward low positions).
        sample_df = _time_it(
            f"{args.contig}: fallback sample positions (head)",
            lambda: _collect(
                lf_contig.select("position").head(2000), streaming=streaming
            ),
        )
        candidate_positions.update(
            int(x) for x in sample_df.get_column("position").to_list() if x is not None
        )

    candidate_positions_list = sorted(candidate_positions)
    if not candidate_positions_list:
        raise SystemExit(f"No positions found for contig {args.contig}")

    n_point_queries = max(0, int(args.n_point_queries))
    if n_point_queries > 0:
        rng = random.Random(args.seed)
        point_positions: list[int] = [pos_min]
        if pos_max != pos_min:
            point_positions.append(pos_max)
        remaining = [p for p in candidate_positions_list if p not in point_positions]
        rng.shuffle(remaining)
        point_positions.extend(
            remaining[: max(0, n_point_queries - len(point_positions))]
        )
        point_positions = sorted(set(point_positions))[:n_point_queries]
    else:
        point_positions = []

    print(
        f"{args.contig}: point query positions ({len(point_positions)}): {point_positions}"
    )

    # Point queries (positions guaranteed to exist).
    for pos in point_positions:
        count_df = _time_it(
            f"{args.contig}: point count position={pos}",
            lambda pos=pos: _collect(
                lf_contig.filter(pl.col("position") == pos).select(
                    pl.len().alias("n_rows")
                ),
                streaming=streaming,
            ),
        )
        print(count_df)

        example_cols = [
            c
            for c in ["contig", "position", "alleles", "VRS_Allele_IDs"]
            if c in schema
        ]
        if example_cols:
            examples = _time_it(
                f"{args.contig}: examples position={pos}",
                lambda pos=pos: _collect(
                    lf_contig.filter(pl.col("position") == pos)
                    .select(example_cols)
                    .limit(args.limit),
                    streaming=streaming,
                ),
            )
            print(examples)

    # Range queries distributed across the contig span.
    n_range_queries = max(0, int(args.n_range_queries))
    if n_range_queries > 0:
        range_width = max(1, int(args.range_width))

        # Choose range centers at roughly-even quantiles of known-existing positions to ensure hits.
        idxs = [
            round(i * (len(candidate_positions_list) - 1) / max(1, n_range_queries - 1))
            for i in range(n_range_queries)
        ]
        range_centers = [candidate_positions_list[i] for i in idxs]

        print(f"{args.contig}: range queries n={n_range_queries} width={range_width}")
        for center in range_centers:
            start = max(pos_min, center - range_width // 2)
            end = min(pos_max, start + range_width - 1)
            start = max(pos_min, min(start, end))

            range_count = _time_it(
                f"{args.contig}: range count {start}-{end}",
                lambda start=start, end=end: _collect(
                    lf_contig.filter(pl.col("position").is_between(start, end)).select(
                        pl.len().alias("n_rows")
                    ),
                    streaming=streaming,
                ),
            )
            print(range_count)

            example_cols = [
                c
                for c in ["contig", "position", "alleles", "VRS_Allele_IDs"]
                if c in schema
            ]
            if example_cols and args.limit > 0:
                examples = _time_it(
                    f"{args.contig}: examples range {start}-{end}",
                    lambda start=start, end=end: _collect(
                        lf_contig.filter(pl.col("position").is_between(start, end))
                        .select(example_cols)
                        .limit(args.limit),
                        streaming=streaming,
                    ),
                )
                print(examples)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
