# Build and package the diffed-places binary as a container.
#
# This file is used in continuous integration to automatically build
# containers; as a regular developer, you do not need to do this.
# Should you really want to build the container yourself, for example
# when testing a change to this file before sending out a pull/merge
# request, use podman:
#
#     podman build -t test-container -f Containerfile .
#     podman run -t test-container --help


# ----------------------------------------------------------------------------
#  Build Stage 1: Build, test, create Software Bill of Materials (SBOM)
# ----------------------------------------------------------------------------

FROM rust:1.92.0-alpine3.23 AS builder

WORKDIR /usr/diffed-places

COPY Cargo.toml Cargo.lock .
COPY src src
COPY tests tests

RUN apk update
RUN apk info syft
RUN apk add syft
RUN cargo build --release
RUN cargo test --release
RUN syft scan dir:. --source-name diffed-places -o cyclonedx-json=sbom.cdx.json


# ----------------------------------------------------------------------------
#  Build Stage 2: Package build artifacts into an otherwise empty container
# ----------------------------------------------------------------------------

FROM scratch

ARG BUILD_TIMESTAMP
ARG VCS_REF
ARG VCS_URL

COPY --from=builder --chown=1000:1000  \
    /usr/diffed-places/target/release/diffed-places /app/diffed-places
COPY --from=builder --chown=1000:1000 \
    /usr/diffed-places/sbom.cdx.json /sbom/sbom.cdx.json

USER 1000

ENTRYPOINT ["/app/diffed-places"]

LABEL  \
    org.opencontainers.image.authors="Sascha Brawer <sascha@brawer.ch>"  \
    org.opencontainers.image.created=$BUILD_TIMESTAMP \
    org.opencontainers.image.licenses="MIT"  \
    org.opencontainers.image.revision=$VCS_REF  \
    org.opencontainers.image.sbom="/sbom/sbom.cdx.json"  \
    org.opencontainers.image.source=$VCS_URL  \
    org.opencontainers.image.vendor="Sascha Brawer <sascha@brawer.ch>"
