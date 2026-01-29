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

GCS_STRIPPED="gs://${OUTPUT_BUCKET}/vcf-stripped"
GCS_VRS="gs://${OUTPUT_BUCKET}/vcf-vrs"
GCS_ANNOTATED="${GCS_VRS}/${BASENAME}.VRS.vcf.gz"

mkdir -p "$WORKDIR"

echo "========================================"
echo "gnomAD VRS Annotation Job"
echo "  CONTIG:        $CONTIG"
echo "  SOURCE:        $SOURCE"
echo "  OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "  SEQREPO_PATH:  $SEQREPO_PATH"
echo "  GNOMAD_URL:    $GNOMAD_URL"
echo "========================================"

# ── Step 1: Get stripped VCF (download from cache or create) ────
echo ""
echo "[Step 1/2] Getting stripped VCF..."
START=$(date +%s)

# Check for existing stripped VCF in GCS (try new naming, then old naming)
GCS_STRIPPED_NEW="${GCS_STRIPPED}/${BASENAME}.stripped.vcf.bgz"
GCS_STRIPPED_OLD="${GCS_STRIPPED}/gnomad_${CONTIG}.vcf.bgz"

if gcloud storage ls "$GCS_STRIPPED_NEW" &>/dev/null; then
    echo "  Found cached stripped VCF: $GCS_STRIPPED_NEW"
    gcloud storage cp "$GCS_STRIPPED_NEW" "$STRIPPED_VCF"
    ELAPSED=$(( $(date +%s) - START ))
    echo "  Downloaded in ${ELAPSED}s ($(du -h "$STRIPPED_VCF" | cut -f1))"
elif gcloud storage ls "$GCS_STRIPPED_OLD" &>/dev/null; then
    echo "  Found cached stripped VCF (old naming): $GCS_STRIPPED_OLD"
    gcloud storage cp "$GCS_STRIPPED_OLD" "$STRIPPED_VCF"
    ELAPSED=$(( $(date +%s) - START ))
    echo "  Downloaded in ${ELAPSED}s ($(du -h "$STRIPPED_VCF" | cut -f1))"
else
    echo "  No cached VCF found, downloading and stripping from gnomAD..."
    curl -sSL "$GNOMAD_URL" \
        | bcftools annotate -x ID,INFO,FILTER -Oz -o "$STRIPPED_VCF"
    ELAPSED=$(( $(date +%s) - START ))
    echo "  Completed in ${ELAPSED}s ($(du -h "$STRIPPED_VCF" | cut -f1))"
    UPLOAD_STRIPPED=true
fi

# ── Count variants for progress estimation ─────────────────────
echo ""
echo "Counting variants in stripped VCF..."
VARIANT_COUNT=$(bcftools view -H "$STRIPPED_VCF" | wc -l)
echo "  Total variants: $VARIANT_COUNT"

# ── Step 2: VRS annotation + upload (streamed to GCS) ──────────
echo ""
echo "[Step 2/2] Running VRS annotation (streaming output to GCS)..."
echo "  Output: $GCS_ANNOTATED"
START=$(date +%s)

# Only upload stripped VCF if we created it (not if downloaded from cache)
if [[ "${UPLOAD_STRIPPED:-}" == "true" ]]; then
    echo "  Uploading stripped VCF..."
    gcloud storage cp "$STRIPPED_VCF" "${GCS_STRIPPED}/${BASENAME}.stripped.vcf.bgz"
fi

# Stream: vrs-annotate writes uncompressed VCF to stdout,
# bgzip compresses to BGZF format, gcloud streams to GCS.
# This avoids writing the (potentially multi-GB) output to tmpfs.
SEQREPO_FD_CACHE_MAXSIZE=None \
vrs-annotate vcf \
    --vrs-attributes \
    --dataproxy-uri "seqrepo+file://${SEQREPO_PATH}" \
    --vcf-out - \
    --log-every 100000 \
    "$STRIPPED_VCF" \
  | bgzip -c \
  | gcloud storage cp - "$GCS_ANNOTATED"

ELAPSED=$(( $(date +%s) - START ))
echo "  Completed in ${ELAPSED}s"

echo ""
echo "========================================"
echo "Job complete."
echo "  Annotated: $GCS_ANNOTATED"
if [[ "${UPLOAD_STRIPPED:-}" == "true" ]]; then
    echo "  Stripped:  ${GCS_STRIPPED}/${BASENAME}.stripped.vcf.bgz (uploaded)"
else
    echo "  Stripped:  (used cached version)"
fi
echo "========================================"
