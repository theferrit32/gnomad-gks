#!/bin/bash
# Entrypoint for gnomAD VRS annotation Cloud Run job.
#
# Required environment variables:
#   CONTIG        - Chromosome to process (e.g., chr1, chrX)
#   SOURCE        - gnomAD source type: "exomes" or "genomes"
#   OUTPUT_BUCKET - GCS bucket name for output (no gs:// prefix)
#
# Optional environment variables:
#   SEQREPO_PATH  - Path to seqrepo root (default: /seqrepo-GRCh38/master)

set -euo pipefail

# ── Validate inputs ──────────────────────────────────────────────
: "${CONTIG:?Error: CONTIG env var is required (e.g., chr1)}"
: "${SOURCE:?Error: SOURCE env var is required (exomes or genomes)}"
: "${OUTPUT_BUCKET:?Error: OUTPUT_BUCKET env var is required}"

SEQREPO_PATH="${SEQREPO_PATH:-/seqrepo}"

if [[ "$SOURCE" != "exomes" && "$SOURCE" != "genomes" ]]; then
    echo "Error: SOURCE must be 'exomes' or 'genomes', got: $SOURCE"
    exit 1
fi

# ── Derived variables ────────────────────────────────────────────
GNOMAD_URL="https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/vcf/${SOURCE}/gnomad.${SOURCE}.v4.1.sites.${CONTIG}.vcf.bgz"
WORKDIR="/tmp/work"
BASENAME="gnomad.${SOURCE}.v4.1.sites.${CONTIG}"

STRIPPED_VCF="${WORKDIR}/${BASENAME}.stripped.vcf.bgz"
ANNOTATED_VCF="${WORKDIR}/${BASENAME}.VRS.vcf.bgz"

GCS_STRIPPED="gs://${OUTPUT_BUCKET}/vcf-stripped"
GCS_VRS="gs://${OUTPUT_BUCKET}/vcf-vrs"

mkdir -p "$WORKDIR"

echo "========================================"
echo "gnomAD VRS Annotation Job"
echo "  CONTIG:        $CONTIG"
echo "  SOURCE:        $SOURCE"
echo "  OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "  SEQREPO_PATH:  $SEQREPO_PATH"
echo "  GNOMAD_URL:    $GNOMAD_URL"
echo "========================================"

# ── Step 1: Download and strip VCF in one pass ──────────────────
echo ""
echo "[Step 1/3] Downloading and stripping VCF to CHROM/POS/REF/ALT..."
START=$(date +%s)

curl -sSL "$GNOMAD_URL" \
    | bcftools annotate -x ID,INFO,FILTER -Oz -o "$STRIPPED_VCF"

ELAPSED=$(( $(date +%s) - START ))
echo "  Completed in ${ELAPSED}s ($(du -h "$STRIPPED_VCF" | cut -f1))"

# ── Step 2: VRS annotation ──────────────────────────────────────
echo ""
echo "[Step 2/3] Running VRS annotation..."
START=$(date +%s)

vrs-annotate vcf \
    --vrs-attributes \
    --dataproxy-uri "seqrepo+file://${SEQREPO_PATH}" \
    --vcf-out "$ANNOTATED_VCF" \
    "$STRIPPED_VCF"

ELAPSED=$(( $(date +%s) - START ))
echo "  Completed in ${ELAPSED}s ($(du -h "$ANNOTATED_VCF" | cut -f1))"

# ── Step 3: Upload to GCS ───────────────────────────────────────
echo ""
echo "[Step 3/3] Uploading results to GCS..."
START=$(date +%s)

gcloud storage cp "$STRIPPED_VCF" "${GCS_STRIPPED}/${BASENAME}.stripped.vcf.bgz"
gcloud storage cp "$ANNOTATED_VCF" "${GCS_VRS}/${BASENAME}.VRS.vcf.bgz"

if [[ -f "${ANNOTATED_VCF}.tbi" ]]; then
    gcloud storage cp "${ANNOTATED_VCF}.tbi" "${GCS_VRS}/${BASENAME}.VRS.vcf.bgz.tbi"
fi

ELAPSED=$(( $(date +%s) - START ))
echo "  Completed in ${ELAPSED}s"

echo ""
echo "========================================"
echo "Job complete."
echo "  Stripped:  ${GCS_STRIPPED}/${BASENAME}.stripped.vcf.bgz"
echo "  Annotated: ${GCS_VRS}/${BASENAME}.VRS.vcf.bgz"
echo "========================================"
