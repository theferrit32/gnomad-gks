"""
Notes:
Spark will try to spill memory to /tmp which may be too small.
  mkdir -p ~/tmp/spark-local ~/tmp/hail
  export SPARK_LOCAL_DIRS=~/tmp/spark-local
  # In Python, use Path("~").expanduser() (shown below) since Spark/Hail won't expand "~" for you.

"""

import subprocess
from pathlib import Path, PurePosixPath

import hail as hl

spark_local_dir = Path("~/tmp/spark-local").expanduser()
hail_tmp_dir = Path("~/tmp/hail").expanduser()
spark_local_dir.mkdir(parents=True, exist_ok=True)
hail_tmp_dir.mkdir(parents=True, exist_ok=True)

hl.init(
    tmp_dir=str(hail_tmp_dir),
    spark_conf={"spark.local.dir": str(spark_local_dir)},
)

# vcf_gcs_urls = [
#     "gs://kferrite-sandbox-storage/vcf-vrs/gnomad.genomes.v4.1.sites.chrY.VRS.vcf.bgz",
# ]

contigs = [str(i) for i in range(1, 22)] + ["X", "Y"]
# contigs = ["Y"]
vcf_gcs_urls = [
    f"gs://kferrite-sandbox-storage/vcf-vrs/gnomad.genomes.v4.1.sites.chr{contig}.VRS.vcf.bgz"
    for contig in contigs
]


def import_vcf_to_hail(vcf_gcs_url: str) -> hl.MatrixTable:
    return hl.import_vcf(
        vcf_gcs_url,
        reference_genome="GRCh38",
        force_bgz=True,
        array_elements_required=False,
    )


for vcf_gcs_url in vcf_gcs_urls:
    print("Processing VCF GCS URL:", vcf_gcs_url)
    df = import_vcf_to_hail(vcf_gcs_url)
    df = df.repartition(1000)
    df = df.key_rows_by(locus=df.locus, alleles=df.alleles)
    # Export the Hail Table to a GCS path directly (blows up with OOM even for chrY)
    # output_path = "gs://kferrite-sandbox-storage/ht-vrs/gnomad.genomes.v4.1.sites.chrY.VRS.ht"
    # df.write(output_path, overwrite=True)

    # For whatever reason, writing it locally does not blow up with OOM
    ht_local_path = PurePosixPath(vcf_gcs_url).name.removesuffix(".vcf.bgz") + ".ht"
    print(f"Writing Hail Table locally to path: {ht_local_path}")
    df.write(f"./{ht_local_path}", overwrite=True)

    # Read back from local path
    df_ht = hl.read_matrix_table(f"./{ht_local_path}")

    ht_gcs_path = vcf_gcs_url.replace(".vcf.bgz", ".ht/").replace("vcf-vrs", "ht-vrs")
    print(f"Writing persisted Hail Table to GCS path: {ht_gcs_path}")
    # Use gcloud CLI to copy local ht directory to GCS url (be careful with slashes for rsync)
    # Wipe out any existing files at that prefix with --delete-unmatched-destination-objects (careful!)
    # rsync local ht directory to GCS path
    subprocess.run(
        [
            "gcloud",
            "storage",
            "rsync",
            "-r",
            "--delete-unmatched-destination-objects",
            f"./{ht_local_path}/",
            ht_gcs_path,
        ],
        check=True,
    )
