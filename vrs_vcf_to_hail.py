"""
Notes:
Spark will try to spill memory to /tmp which may be too small.
  mkdir -p ~/tmp/spark-local ~/tmp/hail
  export SPARK_LOCAL_DIRS=~/tmp/spark-local
  # In Python, use Path("~").expanduser() (shown below) since Spark/Hail won't expand "~" for you.

"""

import shutil
import subprocess
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
        # Running on 16GiB RAM vm, so allocate 6g to driver and executor each.
        "spark.driver.memory": "6g",
        "spark.executor.memory": "6g",
    },
)

bucket = "kferrite-sandbox-storage"
source = "exomes"  # "genomes" or "exomes"
vcf_prefix = "vcf-vrs-fixed"
ht_prefix = "ht-vrs"

output_ht_url = f"gs://{bucket}/{ht_prefix}/gnomad.{source}.v4.1.sites.VRS.ht/"
# vcf_url_templ = (
#     f"gs://{bucket}/{vcf_prefix}/gnomad.{source}.v4.1.sites.chr{{}}.VRS.vcf.bgz"
# )
vcf_url_templ = f"./{vcf_prefix}/gnomad.{source}.v4.1.sites.chr{{}}.VRS.vcf.bgz"
contigs = [str(i) for i in range(1, 23)] + ["X", "Y"]

vcf_gcs_urls = [vcf_url_templ.format(contig) for contig in contigs]


def restructure_table(ht: hl.MatrixTable) -> hl.Table:
    """
    Convert MatrixTable to Table with only the locus, alleles, and info field.
    Drop the .info.VRS_Error field since it's empty for gnomad since there
    are no invalid values such as positions or sequences or refs or expressions.
    """
    ht_out = ht.rows()
    ht_out = ht_out.key_by("locus", "alleles")
    ht_out = ht_out.select("info")
    # Drop .info.VRS_Error since they're all empty for the gnomad use case
    if "VRS_Error" in ht_out.info.dtype.fields:
        ht_out = ht_out.annotate(info=ht_out.info.drop("VRS_Error"))
    return ht_out


def import_vcf_to_hail(vcf_gcs_url: str) -> hl.Table:
    matrix_table = hl.import_vcf(
        vcf_gcs_url,
        reference_genome="GRCh38",
        force_bgz=True,
        array_elements_required=False,
    )
    ht = restructure_table(matrix_table)
    return ht


union_table: hl.Table = None

# Import and restructure
for vcf_gcs_url in vcf_gcs_urls:
    print("Processing VCF GCS URL:", vcf_gcs_url)
    df = import_vcf_to_hail(vcf_gcs_url)
    # df = df.repartition(10)

    if union_table is None:
        union_table = df
    else:
        # Assert no overlap
        # assert union_table.join(df, how="inner").count() == 0
        print("Unioning in table from URL:", vcf_gcs_url)
        union_table = union_table.union(df)

# Repartition unioned table with shuffle to redistribute
# (current gnomad 4.1 genomes uses ~8k partitions)
union_table = union_table.repartition(10000, shuffle=True)

# Export table locally
local_path = f"gnomad.{source}.v4.1.sites.VRS.ht"
print("Persisting hail table locally to path:", local_path)
if Path(local_path).exists():
    shutil.rmtree(local_path)
union_table.write(local_path, overwrite=True)

# Read back from persisted local table. Just a sanity check.
union_table = hl.read_table(local_path)
print("Count of persisted unioned table:", union_table.count())

# rsync table to GCS
print("Writing persisted Hail Table to GCS path:", output_ht_url)

# Wipe out any existing files at that prefix with --delete-unmatched-destination-objects (careful!)
subprocess.run(
    [
        "gcloud",
        "storage",
        "rsync",
        "-r",
        "--delete-unmatched-destination-objects",
        f"./{local_path}/",
        output_ht_url,
    ],
    check=True,
)
