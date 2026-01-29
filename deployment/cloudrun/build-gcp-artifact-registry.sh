#!/bin/bash
# Build the VRS annotation image for amd64 and push to GCP Artifact Registry.
#
# Usage:
#   ./build-gcp-artifact-registry.sh [TAG]
#   ./build-gcp-artifact-registry.sh --impersonate-service-account SA_EMAIL [TAG]
#
# TAG defaults to the contents of ./VERSION (e.g., v0.0.1).
#
# Notes:
# - Requires `gcloud` authentication.
# - For podman, we login using an OAuth access token from gcloud.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/VERSION"

PROJECT="kferrite-sandbox-26c7"
REGION="us-central1"
REPOSITORY="gnomad-gks"
IMAGE_NAME="vrs-annotate"

IMPERSONATE_SA=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --impersonate-service-account) IMPERSONATE_SA="$2"; shift 2 ;;
        *) break ;;
    esac
done

TAG="${1:-}"
if [[ -z "$TAG" && -f "$VERSION_FILE" ]]; then
    TAG="$(tr -d ' \t\r\n' < "$VERSION_FILE")"
fi
if [[ -z "$TAG" ]]; then
    echo "Error: tag is required (e.g., v0.0.1) or set ${VERSION_FILE}" >&2
    exit 1
fi

REGISTRY_HOST="${REGION}-docker.pkg.dev"
IMAGE="${REGISTRY_HOST}/${PROJECT}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

echo "Building image: $IMAGE (linux/amd64)"
podman build \
    --platform linux/amd64 \
    --build-arg "CACHE_BUST=$(date +%s)" \
    -f "${SCRIPT_DIR}/Dockerfile" \
    -t "$IMAGE" \
    "${SCRIPT_DIR}"

echo "Logging in to: $REGISTRY_HOST"
GCLOUD_TOKEN_ARGS=()
if [[ -n "$IMPERSONATE_SA" ]]; then
    GCLOUD_TOKEN_ARGS+=(--impersonate-service-account="$IMPERSONATE_SA")
fi
gcloud auth print-access-token "${GCLOUD_TOKEN_ARGS[@]}" \
    | podman login \
        --username oauth2accesstoken \
        --password-stdin \
        "$REGISTRY_HOST"

echo "Pushing image: $IMAGE"
podman push "$IMAGE"

echo "Done: $IMAGE"
