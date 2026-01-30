import shutil
from pathlib import Path

import hail as hl

spark_local_dir = Path("~/tmp/spark-local").expanduser()
hail_tmp_dir = Path("~/tmp/hail").expanduser()
spark_local_dir.mkdir(parents=True, exist_ok=True)
hail_tmp_dir.mkdir(parents=True, exist_ok=True)

hl.init(
    tmp_dir=str(hail_tmp_dir),
    spark_conf={
        "spark.local.dir": str(spark_local_dir),
        # Default is 1g. Seeing massive spillover to disk.
        # Running on a 16GiB RAM machine, so allocate 6g to driver and executor each.
        "spark.driver.memory": "6g",
        "spark.executor.memory": "6g",
    },
)


contigs = [str(i) for i in range(1, 23)] + ["X", "Y"]
# contigs = ["X", "Y"]
contigs = ["chr" + contig for contig in contigs]

local_paths = [f"./gnomad.genomes.v4.1.sites.{contig}.VRS.ht" for contig in contigs]


def restructure_table(ht: hl.MatrixTable) -> hl.Table:
    """
    Convert MatrixTable to Table with only the locus, alleles, and info field.
    """
    ht_out = ht.rows()
    ht_out = ht_out.key_by("locus", "alleles")
    ht_out = ht_out.select("info")
    return ht_out


# Start with the first
print(f"Reading first table from path: {local_paths[0]}")
ht_union = hl.read_matrix_table(local_paths[0])
ht_union = restructure_table(ht_union)

# Union the rest
for local_path in local_paths[1:]:
    print(f"Unioning in table from path: {local_path}")
    ht_to_union = hl.read_matrix_table(local_path)
    ht_to_union = restructure_table(ht_to_union)

    # Assert no overlap
    assert ht_union.join(ht_to_union, how="inner").count() == 0

    ht_union = ht_union.union(ht_to_union)


print(f"Count of unioned table: {ht_union.count()}")

# Repartition unioned table with shuffle to redistribute
# (current gnomad 4.1 genomes uses ~8k partitions)
# EDIT: turning off shuffle since input tables are 1000 each
# and so this is downsizing the partitions from 24000 to 10000
# which does not benefit from shuffling as much.
ht_union = ht_union.repartition(10000, shuffle=False)

# Delete this dir if it exists
output_path = Path("./gnomad.genomes.v4.1.sites.all_contigs.VRS.ht")
if output_path.is_dir():
    print("Deleting existing output path:", output_path)
    shutil.rmtree(output_path)
# Write the new table
print("Writing unioned Hail Table to path:", output_path)
ht_union.write(str(output_path), overwrite=True)
