#!/bin/bash
# Build the VRS annotation image for amd64 and push to GHCR.
#
# Usage:
#   ./build-ghcr.sh [TAG]
#
# Requires: podman login ghcr.io
#   cat ./theferrit32-github-classic.txt | podman login ghcr.io -u theferrit32 --password-stdin

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/VERSION"

GITHUB_USER="theferrit32"
REPO="ghcr.io/${GITHUB_USER}/gnomad-gks/vrs-annotate"
TAG="${1:-}"
if [[ -z "$TAG" && -f "$VERSION_FILE" ]]; then
    TAG="$(tr -d ' \t\r\n' < "$VERSION_FILE")"
fi
if [[ -z "$TAG" ]]; then
    echo "Error: tag is required (e.g., v0.0.1) or set ${VERSION_FILE}" >&2
    exit 1
fi
IMAGE="${REPO}:${TAG}"

echo "Building image: $IMAGE (linux/amd64)"

podman build \
    --platform linux/amd64 \
    -t "$IMAGE" \
    .

echo "Pushing image: $IMAGE"
podman push "$IMAGE"

echo "Done: $IMAGE"
