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
- Avoids whole-contig aggregates (only queries with position predicates).
- Prints timing for each query so you can sanity-check pruning/perf.

Requirements:
  pip install polars
  # For gs:// datasets:
  pip install fsspec gcsfs

Usage:
  python3 polars_parquet_smoketest.py --dataset ./gnomad.genomes.v4.1.sites.VRS.parquet --contig chrY
  python3 polars_parquet_smoketest.py --dataset ./gnomad.genomes.v4.1.sites.VRS.parquet --contig chrY --n-point-queries 10
  python3 polars_parquet_smoketest.py --dataset ./gnomad.genomes.v4.1.sites.VRS.parquet --contig chr1 --n-range-queries 8 --range-width 500000
  python3 polars_parquet_smoketest.py --dataset gs://my-bucket/path/to/ds --contig chrY

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
from urllib.parse import urlparse

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
    # Polars deprecated `streaming=...` in favor of `engine=...` (e.g. engine="streaming").
    # Keep compatibility with older versions by trying engine first, then falling back.
    try:
        return lf.collect(engine="streaming" if streaming else "auto")
    except TypeError:
        try:
            return lf.collect(streaming=streaming)
        except TypeError:
            return lf.collect()


def _dataset_scheme(dataset: str) -> str:
    scheme = urlparse(dataset).scheme
    if scheme in ("", "file"):
        return "local"
    if scheme == "gs":
        return "gs"
    raise SystemExit(f"Unsupported dataset URL scheme: {scheme!r}")


def _contig_dir(dataset: str, contig: str | None) -> tuple[str, str]:
    dataset = dataset.rstrip("/")
    last = dataset.split("/")[-1]

    if last.startswith("contig="):
        dataset_contig = last.removeprefix("contig=")
        if contig is None:
            contig = dataset_contig
        elif contig != dataset_contig:
            raise SystemExit(
                f"--contig {contig!r} does not match dataset path contig {dataset_contig!r}"
            )
        return dataset, contig

    if contig is None:
        contig = "chrY"
    return f"{dataset}/contig={contig}", contig


def _list_parquet_files(contig_dir: str, *, scheme: str) -> list[str]:
    if scheme == "local":
        contig_path = Path(contig_dir)
        if not contig_path.exists():
            raise SystemExit(f"Path does not exist: {contig_path}")
        return sorted(str(p) for p in contig_path.rglob("*.parquet") if p.is_file())

    if scheme == "gs":
        try:
            import fsspec  # type: ignore[import-not-found]
        except ImportError as e:
            raise SystemExit(
                "gs:// support requires fsspec + gcsfs: pip install fsspec gcsfs"
            ) from e

        try:
            fs, path = fsspec.core.url_to_fs(contig_dir)
        except (ImportError, ModuleNotFoundError) as e:
            raise SystemExit(
                "gs:// support requires fsspec + gcsfs: pip install fsspec gcsfs"
            ) from e

        files = fs.find(path)
        return sorted(
            fs.unstrip_protocol(p) for p in files if str(p).endswith(".parquet")
        )

    raise AssertionError(f"Unhandled scheme: {scheme}")


def _local_parquet_file_stats(*, contig_dir: str, parquet_files: list[str]) -> None:
    if not parquet_files:
        print(f"No parquet files found under: {contig_dir}")
        return

    sizes = [Path(p).stat().st_size for p in parquet_files]
    total = sum(sizes)
    print(f"Parquet files under {contig_dir}: {len(parquet_files)}")
    print(
        "File sizes:",
        f"total={_human_bytes(total)}",
        f"min={_human_bytes(min(sizes))}",
        f"p50={_human_bytes(int(statistics.median(sizes)))}",
        f"max={_human_bytes(max(sizes))}",
        sep=" ",
    )


def _sample_existing_positions_from_files(
    parquet_files: list[str],
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
        "--contig",
        default=None,
        help="Contig to query (e.g. chrY). Defaults to chrY unless the dataset path is already contig=...",
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
        "--no-streaming",
        action="store_true",
        help="Disable Polars streaming collects (useful if your Polars build doesn't support it).",
    )
    parser.add_argument(
        "--local-file-stats",
        action="store_true",
        help="Print local-only parquet file size stats (ignored for gs://).",
    )
    args = parser.parse_args()

    dataset = args.dataset
    parsed = urlparse(dataset)
    scheme = _dataset_scheme(dataset)
    if parsed.scheme == "file":
        dataset = parsed.path

    contig_dir, contig = _contig_dir(dataset, args.contig)
    parquet_files = _list_parquet_files(contig_dir, scheme=scheme)
    if not parquet_files:
        raise SystemExit(f"No parquet files found under: {contig_dir}")

    if args.local_file_stats:
        if scheme == "local":
            _local_parquet_file_stats(contig_dir=contig_dir, parquet_files=parquet_files)
        else:
            print("--local-file-stats is only supported for local filesystem paths; skipping.")

    lf = pl.scan_parquet(parquet_files, hive_partitioning=True)

    schema = lf.collect_schema()
    if "position" not in schema:
        raise SystemExit("No 'position' column found in Parquet dataset.")

    lf_contig = lf.filter(pl.col("contig") == contig) if "contig" in schema else lf
    streaming = not args.no_streaming

    # Choose a random assortment of existing positions (guaranteed to exist) without manual input.
    file_sample_positions = _sample_existing_positions_from_files(
        parquet_files,
        n_files=args.sample_files,
        n_rows_per_file=args.rows_per_file_sample,
        seed=args.seed,
    )

    candidate_positions_list = sorted({int(p) for p in file_sample_positions})
    if not candidate_positions_list:
        raise SystemExit(
            f"No positions sampled for contig {contig}. Try increasing --sample-files and/or --rows-per-file-sample."
        )

    sampled_min = candidate_positions_list[0]
    sampled_max = candidate_positions_list[-1]
    print(
        f"{contig}: sampled positions={len(candidate_positions_list)} "
        f"sampled_min={sampled_min} sampled_max={sampled_max}"
    )

    n_point_queries = max(0, int(args.n_point_queries))
    if n_point_queries > 0:
        rng = random.Random(args.seed)
        point_positions: list[int] = [sampled_min]
        if sampled_max != sampled_min:
            point_positions.append(sampled_max)
        remaining = [p for p in candidate_positions_list if p not in point_positions]
        rng.shuffle(remaining)
        point_positions.extend(
            remaining[: max(0, n_point_queries - len(point_positions))]
        )
        point_positions = sorted(set(point_positions))[:n_point_queries]
    else:
        point_positions = []

    print(f"{contig}: point query positions ({len(point_positions)}): {point_positions}")

    # Point queries (positions guaranteed to exist).
    for pos in point_positions:
        count_df = _time_it(
            f"{contig}: point count position={pos}",
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
                f"{contig}: examples position={pos}",
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

        print(f"{contig}: range queries n={n_range_queries} width={range_width}")
        for center in range_centers:
            start = max(sampled_min, center - range_width // 2)
            end = min(sampled_max, start + range_width - 1)
            start = max(sampled_min, min(start, end))

            range_count = _time_it(
                f"{contig}: range count {start}-{end}",
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
                    f"{contig}: examples range {start}-{end}",
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
