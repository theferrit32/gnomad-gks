# Stage 1: Download and unpack GRCh38-only seqrepo
FROM python:3.12-slim AS seqrepo-builder

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://pub-a70997b5b39342fe887c521b504d73de.r2.dev/seqrepo/seqrepo-2025-08-27-GRCh38.tar.gz \
    | tar -xz -C / \
    && mv /2025-08-27 /seqrepo

# Stage 2: Runtime image with vrs-python, bcftools, and gcloud CLI
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        bcftools \
        curl \
        apt-transport-https \
        ca-certificates \
        gnupg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt cloud-sdk main" \
       > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
       | tee /usr/share/keyrings/cloud.google.asc > /dev/null \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "ga4gh.vrs[extras]"

COPY --from=seqrepo-builder /seqrepo /seqrepo

ENV GA4GH_VRS_DATAPROXY_URI="seqrepo+file:///seqrepo"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
