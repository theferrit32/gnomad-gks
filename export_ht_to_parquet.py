#!/usr/bin/env python3
"""
export_ht_to_parquet.py

Export a large, keyed Hail Table of variant rows (e.g. gnomAD sites) to a Parquet dataset that is:

- Partitioned on-disk by contig using Hive-style directories:
    <out>/contig=chr1/part-*.parquet
    <out>/contig=chr2/part-*.parquet
    ...

- Friendly to DuckDB and Polars:
  - `locus` is flattened into a primitive `position` column (and contig comes from the directory name).
  - The `info` struct is flattened into top-level columns.
  - This avoids nested Hail-specific types in Parquet.

- Able to avoid full contig scans for point/range queries by relying on Parquet row-group statistics:
  - DuckDB/Polars can skip row groups whose (min(position), max(position)) cannot match the predicate.
  - This works best when each Parquet file / row group covers a relatively tight position range.
  - To preserve that locality, this script avoids dataset-wide shuffles and writes contig-by-contig via
    `hl.filter_intervals(...)` on the keyed Hail Table.

Output columns (per row):
  - position: int32
  - alleles: array<str>
  - VRS_Allele_IDs: array<str>
  - VRS_Error: array<str>
  - VRS_Starts: array<int32>
  - VRS_Ends: array<int32>
  - VRS_States: array<str>
  - VRS_Lengths: array<int32>
  - VRS_RepeatSubunitLengths: array<int32>

DuckDB usage (partition pruning + row-group pruning):
  >>> import duckdb
  >>> con = duckdb.connect()
  >>> con.execute(
  ...   \"\"\"
  ...   SELECT position, alleles, VRS_Error
  ...   FROM parquet_scan('parquet_out/**/*.parquet', hive_partitioning=1)
  ...   WHERE contig = 'chr1' AND position = 205000000
  ...   \"\"\"
  ... ).fetchall()

Polars usage (lazy scan; pass hive_partitioning=True):
  >>> import polars as pl
  >>> lf = pl.scan_parquet("parquet_out", hive_partitioning=True)
  >>> lf.filter((pl.col("contig") == "chr1") & (pl.col("position") == 205000000)).select(
  ...     "position", "alleles", "VRS_Error"
  ... ).collect()

Notes:
- You should filter on `contig` (directory partition) and `position` (row-group stats) for best performance.
- `--sort-within-partitions` can improve row-group pruning if you observe wide position ranges per file,
  but it may increase local spill during the write.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import hail as hl


def _init_hail(
    *,
    tmp_dir: Path,
    spark_local_dir: Path,
    driver_memory: str | None,
    executor_memory: str | None,
    parquet_compression: str,
):
    spark_local_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    spark_conf: dict[str, str] = {
        "spark.local.dir": str(spark_local_dir),
        "spark.sql.parquet.compression.codec": parquet_compression,
    }
    if driver_memory:
        spark_conf["spark.driver.memory"] = driver_memory
    if executor_memory:
        spark_conf["spark.executor.memory"] = executor_memory

    hl.init(tmp_dir=str(tmp_dir), spark_conf=spark_conf)


def _default_contigs() -> list[str]:
    return [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]


def _parse_contigs(contigs_arg: list[str] | None) -> list[str]:
    if not contigs_arg:
        return _default_contigs()
    contigs: list[str] = []
    for item in contigs_arg:
        for part in item.split(","):
            part = part.strip()
            if part:
                contigs.append(part)
    return contigs


def _flatten_for_parquet(ht: hl.Table) -> hl.Table:
    # Drop the key so we can drop Hail's `locus` field and flatten structs freely.
    ht = ht.key_by()
    return ht.select(
        position=ht.locus.position,
        alleles=ht.alleles,
        VRS_Allele_IDs=ht.info.VRS_Allele_IDs,
        VRS_Error=ht.info.VRS_Error,
        VRS_Starts=ht.info.VRS_Starts,
        VRS_Ends=ht.info.VRS_Ends,
        VRS_States=ht.info.VRS_States,
        VRS_Lengths=ht.info.VRS_Lengths,
        VRS_RepeatSubunitLengths=ht.info.VRS_RepeatSubunitLengths,
    )


def _success_file_exists(path: Path) -> bool:
    return (path / "_SUCCESS").is_file()


def _ensure_empty_output_dir(path: Path, *, overwrite: bool, resume: bool) -> bool:
    """
    Returns True if the caller should proceed with writing, False if the caller should skip.
    """
    if not path.exists():
        return True

    if resume and _success_file_exists(path):
        print(f"Output exists and looks complete; skipping: {path}")
        return False

    if overwrite:
        print(f"Deleting existing output directory: {path}")
        shutil.rmtree(path)
        return True

    raise SystemExit(
        f"Output path already exists (use --overwrite to replace or --resume to skip): {path}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export a keyed Hail Table to Hive-partitioned Parquet by contig."
    )
    parser.add_argument(
        "--ht-path",
        default="./gnomad.genomes.v4.1.sites.VRS.ht",
        help="Input Hail Table path (directory).",
    )
    parser.add_argument(
        "--out",
        default="./gnomad.genomes.v4.1.sites.VRS.parquet",
        help="Output root directory for Parquet dataset.",
    )
    parser.add_argument(
        "--reference",
        default="GRCh38",
        help="Reference genome name used by the Hail Table (default: GRCh38).",
    )
    parser.add_argument(
        "--contigs",
        action="append",
        help="Contigs to export. Can be repeated or comma-separated (default: chr1-22,chrX,chrY).",
    )
    parser.add_argument(
        "--compression",
        choices=["zstd", "snappy"],
        default="zstd",
        help="Parquet compression codec (default: zstd).",
    )
    parser.add_argument(
        "--sort-within-partitions",
        action="store_true",
        help="Sort within Spark partitions by position before writing (may increase local spill).",
    )
    parser.add_argument(
        "--max-files-per-contig",
        type=int,
        default=None,
        help="If set, coalesce (no shuffle) to at most this many output files per contig.",
    )
    parser.add_argument(
        "--min-files-per-contig",
        type=int,
        default=None,
        help=(
            "If set and a contig has fewer partitions, increase partitions using a range repartition "
            "on position (shuffle, but limited to that contig)."
        ),
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=None,
        help="Spark Parquet writer option to cap records per file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite any existing contig output directories.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip contigs whose output directory already contains a _SUCCESS marker.",
    )
    parser.add_argument(
        "--driver-memory", default="6g", help="Spark driver memory (e.g. 6g)."
    )
    parser.add_argument(
        "--executor-memory", default="6g", help="Spark executor memory (e.g. 6g)."
    )
    parser.add_argument(
        "--tmp-dir",
        default=str(Path("~/tmp/hail").expanduser()),
        help="Hail tmp dir (default: ~/tmp/hail).",
    )
    parser.add_argument(
        "--spark-local-dir",
        default=str(Path("~/tmp/spark-local").expanduser()),
        help="Spark local dir (default: ~/tmp/spark-local).",
    )
    args = parser.parse_args()

    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    _init_hail(
        tmp_dir=Path(args.tmp_dir),
        spark_local_dir=Path(args.spark_local_dir),
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
        parquet_compression=args.compression,
    )

    contigs = _parse_contigs(args.contigs)
    ht = hl.read_table(args.ht_path)

    # Basic schema expectation for this pipeline.
    missing = []
    if "locus" not in ht.row.dtype.fields:
        missing.append("locus")
    if "alleles" not in ht.row.dtype.fields:
        missing.append("alleles")
    if "info" not in ht.row.dtype.fields:
        missing.append("info")
    else:
        for field in [
            "VRS_Allele_IDs",
            "VRS_Error",
            "VRS_Starts",
            "VRS_Ends",
            "VRS_States",
            "VRS_Lengths",
            "VRS_RepeatSubunitLengths",
        ]:
            if field not in ht.info.dtype.fields:
                missing.append(f"info.{field}")
    if missing:
        raise SystemExit(
            f"Input table is missing expected fields: {', '.join(missing)}"
        )

    for contig in contigs:
        out_contig_dir = out_root / f"contig={contig}"
        if not _ensure_empty_output_dir(
            out_contig_dir, overwrite=args.overwrite, resume=args.resume
        ):
            continue

        print(f"Exporting contig: {contig}")
        interval = hl.parse_locus_interval(contig, reference_genome=args.reference)
        ht_contig = hl.filter_intervals(ht, [interval])

        # Preflight: useful for ensuring we'll generate multiple files per contig without assuming
        # a strict 1-partition == 1-file mapping.
        try:
            hail_parts = ht_contig.n_partitions()
        except Exception:
            hail_parts = None
        if hail_parts is not None:
            print(f"{contig}: Hail partitions after filter: {hail_parts}")

        ht_flat = _flatten_for_parquet(ht_contig)
        df = ht_flat.to_spark()
        spark_parts = df.rdd.getNumPartitions()
        print(f"{contig}: Spark partitions before write: {spark_parts}")

        if (
            args.max_files_per_contig is not None
            and spark_parts > args.max_files_per_contig
        ):
            print(f"{contig}: coalesce -> {args.max_files_per_contig}")
            df = df.coalesce(args.max_files_per_contig)
            spark_parts = df.rdd.getNumPartitions()
            print(f"{contig}: Spark partitions after coalesce: {spark_parts}")

        if (
            args.min_files_per_contig is not None
            and spark_parts < args.min_files_per_contig
        ):
            print(
                f"{contig}: repartitionByRange -> {args.min_files_per_contig} (shuffle, contig-scoped)"
            )
            df = df.repartitionByRange(args.min_files_per_contig, "position")
            spark_parts = df.rdd.getNumPartitions()
            print(f"{contig}: Spark partitions after repartitionByRange: {spark_parts}")

        if args.sort_within_partitions:
            df = df.sortWithinPartitions("position")

        writer = df.write.mode("overwrite").option("compression", args.compression)
        if args.max_records_per_file is not None:
            writer = writer.option("maxRecordsPerFile", args.max_records_per_file)

        writer.parquet(str(out_contig_dir))
        print(f"Wrote: {out_contig_dir}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
