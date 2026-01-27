#!/bin/bash
# Build and push the VRS annotation Docker image via Cloud Build.
#
# Usage:
#   ./build.sh [TAG]
#
# Cloud Build runs in GCP so the seqrepo download and image push happen
# at datacenter speeds rather than over a local connection.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/VERSION"

PROJECT="kferrite-sandbox-26c7"
REGION="us-central1"
REPO="${REGION}-docker.pkg.dev/${PROJECT}/gnomad-gks"
TAG="${1:-}"
if [[ -z "$TAG" && -f "$VERSION_FILE" ]]; then
    TAG="$(tr -d ' \t\r\n' < "$VERSION_FILE")"
fi
if [[ -z "$TAG" ]]; then
    echo "Error: tag is required (e.g., v0.0.1) or set ${VERSION_FILE}" >&2
    exit 1
fi
IMAGE="${REPO}/vrs-annotate:${TAG}"

echo "Building image: $IMAGE"
echo "Using Cloud Build (remote build)"

gcloud builds submit \
    --project "$PROJECT" \
    --region "$REGION" \
    --tag "$IMAGE" \
    --timeout=3600 \
    .

echo "Image pushed: $IMAGE"
