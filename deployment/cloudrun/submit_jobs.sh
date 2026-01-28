#!/bin/bash
# Create and execute Cloud Run jobs for gnomAD VRS annotation.
#
# Usage:
#   ./submit_jobs.sh [OPTIONS] [CONTIGS...]
#
# Options:
#   --source exomes|genomes   Data source (default: genomes)
#   --dry-run                 Print commands without executing
#   --sleep SECONDS           Sleep between job submissions (default: 3)
#   --no-sleep                Disable sleeping between submissions
#   --tag TAG                 Image tag to use (e.g., v0.0.1)
#   --image IMAGE             Fully-qualified image ref (tag or digest)
#   --wait                    Wait for each execution to complete
#
# Examples:
#   ./submit_jobs.sh                              # All chroms, genomes
#   ./submit_jobs.sh --source exomes              # All chroms, exomes
#   ./submit_jobs.sh --source genomes chr1 chr2   # Specific chroms
#   ./submit_jobs.sh --dry-run                    # Preview commands

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/VERSION"

# ── Configuration ────────────────────────────────────────────────
PROJECT="kferrite-sandbox-26c7"
REGION="us-central1"
IMAGE_REPO="${REGION}-docker.pkg.dev/${PROJECT}/gnomad-gks/vrs-annotate"
IMAGE=""
SERVICE_ACCOUNT="cloudrun-gnomad@${PROJECT}.iam.gserviceaccount.com"
OUTPUT_BUCKET="kferrite-sandbox-storage"

MEMORY="2Gi"
CPU="1"
TIMEOUT="86400"
MAX_RETRIES="1"

# ── Parse arguments ──────────────────────────────────────────────
SOURCE="genomes"
DRY_RUN=false
SLEEP_SECONDS="3"
TAG=""
IMAGE_OVERRIDE=""
WAIT_FOR_COMPLETION=false
CONTIGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --source) SOURCE="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --sleep) SLEEP_SECONDS="$2"; shift 2 ;;
        --no-sleep) SLEEP_SECONDS="0"; shift ;;
        --tag) TAG="$2"; shift 2 ;;
        --image) IMAGE_OVERRIDE="$2"; shift 2 ;;
        --wait) WAIT_FOR_COMPLETION=true; shift ;;
        *) CONTIGS+=("$1"); shift ;;
    esac
done

if [[ ${#CONTIGS[@]} -eq 0 ]]; then
    CONTIGS=(chr{1..22} chrX chrY)
fi

SOURCE_LC="${SOURCE,,}"

if [[ -n "$IMAGE_OVERRIDE" && -n "$TAG" ]]; then
    echo "Error: use either --image or --tag, not both." >&2
    exit 1
fi

if [[ -n "$IMAGE_OVERRIDE" ]]; then
    IMAGE="$IMAGE_OVERRIDE"
else
    if [[ -z "$TAG" && -f "$VERSION_FILE" ]]; then
        TAG="$(tr -d ' \t\r\n' < "$VERSION_FILE")"
    fi
    if [[ -z "$TAG" ]]; then
        echo "Error: image tag is required (e.g., --tag v0.0.1) or set ${VERSION_FILE}, or pass --image." >&2
        exit 1
    fi
    IMAGE="${IMAGE_REPO}:${TAG}"
fi

echo "Submitting Cloud Run jobs"
echo "  Source:  $SOURCE"
echo "  Image:   $IMAGE"
echo "  Contigs: ${CONTIGS[*]}"
echo "  Sleep:   ${SLEEP_SECONDS}s"
echo "  Wait:    ${WAIT_FOR_COMPLETION}"
echo ""

# ── Submit jobs ──────────────────────────────────────────────────
for contig in "${CONTIGS[@]}"; do
    CONTIG_LC="${contig,,}"
    JOB_NAME="vrs-annotate-${SOURCE_LC}-${CONTIG_LC}"

    LABELS="app=gnomad-gks,component=vrs-annotate,source=${SOURCE_LC},contig=${CONTIG_LC}"
    ENV_VARS="CONTIG=${contig},SOURCE=${SOURCE_LC},OUTPUT_BUCKET=${OUTPUT_BUCKET}"

    if $DRY_RUN; then
        echo "[DRY RUN] gcloud run jobs describe \"$JOB_NAME\" --project \"$PROJECT\" --region \"$REGION\""
        echo "[DRY RUN] gcloud run jobs create|update \"$JOB_NAME\" --image \"$IMAGE\" --service-account \"$SERVICE_ACCOUNT\" --memory \"$MEMORY\" --cpu \"$CPU\" --task-timeout \"$TIMEOUT\" --max-retries \"$MAX_RETRIES\" --labels \"$LABELS\" --set-env-vars \"$ENV_VARS\" --project \"$PROJECT\" --region \"$REGION\""
        echo "[DRY RUN] gcloud run jobs execute \"$JOB_NAME\" --project \"$PROJECT\" --region \"$REGION\""
    else
	        echo "Checking job:  $JOB_NAME"
	        EXISTING_JOB="$(
	            gcloud run jobs list \
	                --project "$PROJECT" \
	                --region "$REGION" \
	                --filter "metadata.name=${JOB_NAME}" \
	                --format "value(metadata.name)" \
	                --limit 1
	        )"

        if [[ "$EXISTING_JOB" == "$JOB_NAME" ]]; then
            echo "Updating job:  $JOB_NAME"
            gcloud run jobs update "$JOB_NAME" \
                --project "$PROJECT" \
                --region "$REGION" \
                --image "$IMAGE" \
                --service-account "$SERVICE_ACCOUNT" \
                --memory "$MEMORY" \
                --cpu "$CPU" \
                --task-timeout "$TIMEOUT" \
                --max-retries "$MAX_RETRIES" \
                --labels "$LABELS" \
                --set-env-vars "$ENV_VARS"
        else
            echo "Creating job:  $JOB_NAME"
            gcloud run jobs create "$JOB_NAME" \
                --project "$PROJECT" \
                --region "$REGION" \
                --image "$IMAGE" \
                --service-account "$SERVICE_ACCOUNT" \
                --memory "$MEMORY" \
                --cpu "$CPU" \
                --task-timeout "$TIMEOUT" \
                --max-retries "$MAX_RETRIES" \
                --labels "$LABELS" \
                --set-env-vars "$ENV_VARS"
        fi

        echo "Executing job: $JOB_NAME"
        EXECUTE_FLAGS=()
        if $WAIT_FOR_COMPLETION; then
            EXECUTE_FLAGS+=(--wait)
        else
            # Returning when the execution has "started" can take a while (image pull / scheduling).
            # --async returns as soon as the execution request is submitted.
            EXECUTE_FLAGS+=(--async)
        fi

        gcloud run jobs execute "$JOB_NAME" \
            --project "$PROJECT" \
            --region "$REGION" \
            "${EXECUTE_FLAGS[@]}"

        if [[ "$SLEEP_SECONDS" != "0" ]]; then
            sleep "$SLEEP_SECONDS"
        fi
    fi
done
