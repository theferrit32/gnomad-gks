# Plan: Export `gnomad.genomes.v4.1.sites.VRS.ht` to Parquet (16GiB RAM VM)

## Goals / constraints
- Source: local Hail Table `./gnomad.genomes.v4.1.sites.VRS.ht` (~53GiB, ~10k partitions), keyed by `locus, alleles`.
- VM: 16GiB RAM, ~80GiB free disk; avoid a global shuffle that could spill tens of GiB.
- Target: Parquet dataset readable by Polars + DuckDB with good partition/file pruning (avoid full scans for typical genomic range queries).
- Output: ~10k-ish Parquet files total (similar order of magnitude to current partitions).
- Compression: prefer `zstd`, with a fallback to `snappy` if client environments lack zstd support.

## Parquet dataset layout (for pruning, without `pos_bin`)
Use Hive-style directory partitioning only on:
- `contig` (always)

Directory layout:
`parquet_out/contig=chr1/part-*.parquet`

How this still avoids “full contig scans”:
- Parquet stores per-row-group statistics (min/max) for columns like `position`.
- DuckDB/Polars can read Parquet metadata and skip row groups (and often whole files) whose `position` ranges cannot match the predicate.
- This relies on the Parquet files having *tight* `position` ranges (i.e., the data is clustered by genomic position within each contig).

Columns (flattened for Polars/DuckDB):
- `contig` (string)
- `position` (int32/int64)
- `alleles` (list[string])
- `VRS_Allele_IDs` (list[string])
- `VRS_Error` (list[string])
- `VRS_Starts`, `VRS_Ends`, `VRS_Lengths`, `VRS_RepeatSubunitLengths` (list[int32])
- `VRS_States` (list[string])

Keep `position` as a top-level primitive column so Parquet statistics can prune row groups for point/range queries.

## Export strategy (avoid a global shuffle)
Use a “Strategy A” approach: export contig subsets and preserve key-order locality; avoid global `partitionBy(...)` writes that shuffle.

Key idea:
- Hail tables keyed by `locus` can be subset with `hl.filter_intervals`, which allows Hail to avoid touching partitions that cannot overlap the interval.
- For each contig, read only relevant partitions and write Parquet in a way that preserves genomic locality (tight `position` min/max per file/row-group).

Algorithm:
1) `ht = hl.read_table("./gnomad.genomes.v4.1.sites.VRS.ht")`
2) For each contig in `chr1..chr22, chrX, chrY`:
   - `ht_contig = hl.filter_intervals(ht, [hl.parse_locus_interval(f"{contig}", reference_genome="GRCh38")])`
   - Transform schema (keep it simple / Spark-friendly):
     - Add `contig`, `position`
     - Flatten `info.*` into top-level columns
     - Drop original `locus` and `info` struct
   - Convert to Spark DataFrame and write Parquet under `parquet_out/contig={contig}/`.

Maintaining “tight ranges” for `position` (to enable pruning):
- Do not do any operation that destroys key ordering (e.g., a shuffle repartition without a range partitioner).
- Prefer “read keyed table -> filter intervals -> write” with no shuffle; this tends to keep each Spark partition (and thus each output file) as a contiguous locus range.
- Optionally add `sortWithinPartitions(["position"])` right before writing to improve row-group min/max locality without a shuffle (may spill some, but much less than a global shuffle).

File-count control (and ensuring >1 file per contig):
- Do not assume a strict “1 input partition = 1 parquet file” mapping: Spark generally writes one file per task/partition, but retries/speculation and file-splitting settings can change that.
- What you *can* rely on: if `ht_contig`/`df` has `N > 1` partitions and you do not coalesce to 1, you will get multiple output files for that contig.
- Add an explicit preflight check per contig:
  - Hail: `ht_contig.n_partitions()`
  - Spark: `df.rdd.getNumPartitions()`
- If a contig unexpectedly has only 1 partition and you want multiple files, increasing partitions requires a shuffle. Do it only for that contig (bounded cost) via a range repartition on `position` (and then `sortWithinPartitions`), not via a dataset-wide shuffle.
- Otherwise, to target ~10k total output files, prefer only *reducing* partitions with `coalesce(k)` (no shuffle) on small contigs; accept that very large contigs may produce many more files than very small contigs.

Key nuance: the table key includes `alleles` in addition to `locus`.
- Row order is lexicographic by the full key: `(locus, alleles)`. This preserves primary clustering by contig/position; `alleles` only affects the order *within the same locus*.
- This means `position` min/max ranges per partition/file are still driven by `locus` boundaries, not by allele ordering; allele tie-breaking does not by itself cause wide `position` ranges.

## Spark/Hail settings (match what worked in `hail_cleanup.py`)
Carry over the working settings:
- `spark.local.dir` on a large-ish disk mount (`~/tmp/spark-local`)
- `tmp_dir` on same disk (`~/tmp/hail`)
- `spark.driver.memory` / `spark.executor.memory` similar to what was stable for you (e.g. 6g).

Parquet compression settings:
- Prefer `zstd`:
  - Spark: set `spark.sql.parquet.compression.codec=zstd` (or writer option `compression=zstd`).
  - If client-side zstd is problematic, switch to `snappy` as a compatibility fallback.

Parquet row-group sizing (pruning granularity):
- Smaller row groups improve pruning for point/range lookups on `position` but increase metadata overhead.
- If the default (often ~128MiB) yields coarse pruning, consider reducing row group size (e.g. 64MiB) via Spark/Parquet settings.

## Verification / sanity checks (no full scans)
1) Verify dataset structure:
   - Ensure directories exist for `contig=.../`
   - Spot-check file sizes to catch extreme skew (a few huge files = imbalance):
     - `ls -lhS parquet_out/contig=chr1/part-*.parquet | head`
2) Smoke query in DuckDB (fast, uses partition pruning):
   - `parquet_scan('parquet_out/**/*.parquet', hive_partitioning=1)`
   - Filter `contig='chr1' AND position BETWEEN ... AND ...` (expect row-group pruning via Parquet stats)
3) Smoke query in Polars:
   - `pl.scan_parquet('parquet_out', hive_partitioning=True)` with the same filter pattern.

## Expected tradeoffs
- This avoids a single huge shuffle at the cost of running per-contig jobs.
- Pruning quality depends on how well the export preserves genomic locality: if `position` ranges per file/row-group are wide, engines may have to inspect many files/row-groups (metadata-only work, but still overhead).

## Next step
Implement `export_ht_to_parquet.py` that follows this plan with flags:
- `--ht-path`, `--out`, `--compression (zstd|snappy)`, `--driver-memory`, `--executor-memory`, `--coalesce-per-contig`, `--sort-within-partitions`.
