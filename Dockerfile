# syntax=docker/dockerfile:1.26

ARG GO_VERSION=1.26
ARG ALPINE_VERSION=3.24

# Build Stage — native build per target platform (CGO needs target-arch vips-dev)
#
# Digest-pinned. The digest is the OCI INDEX (multi-arch), not a per-architecture
# child — pinning a child would build amd64 and break the arm64 release build.
# Verify with: docker buildx imagetools inspect <ref> -> lists linux/amd64 AND linux/arm64.
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@sha256:ce864e7223ac17b1775e6fd0b4c0db580c2eb50e7953a427916379e4b92a1628 AS builder

WORKDIR /app

# Install build dependencies (vips-dev for CGO image processing)
RUN apk add --no-cache git wget vips-dev gcc musl-dev

# Download the wait script (architecture-specific)
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      wget -O /wait https://github.com/ufoscout/docker-compose-wait/releases/download/2.12.1/wait_aarch64; \
    elif [ "$TARGETARCH" = "amd64" ]; then \
      wget -O /wait https://github.com/ufoscout/docker-compose-wait/releases/download/2.12.1/wait; \
    else \
      echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi && chmod +x /wait

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the serving binary and the separately removable one-off sweep binary.
# The image entrypoint remains the serving binary below.
RUN CGO_ENABLED=1 go build -tags vips -trimpath -ldflags "-s -w" -o /bin/file-service ./cmd/server/ \
    && CGO_ENABLED=1 go build -trimpath -ldflags "-s -w" -o /bin/sweep-ipfs-cids ./cmd/sweep-ipfs-cids/

# Runtime Stage — Alpine for lightweight runtime with vips.
#
# NOT distroless, deliberately. file-service builds CGO_ENABLED=1 against libvips
# (`-tags vips`, govips/antst fork), so the binary is DYNAMICALLY linked: `ldd` on
# the shipped image reports 80 entries including libvips.so.42, musl-linked.
# gcr.io/distroless/static-* ships CA certs and tzdata only — such a binary builds
# clean there and exits at container start. See alkem-io/file-service#67.
#
# Digest-pinned to the OCI INDEX (multi-arch), not a per-architecture child.
FROM alpine:${ALPINE_VERSION}@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

# Install libvips runtime only
RUN apk add --no-cache vips vips-heif vips-jxl vips-poppler

# Non-root user matching K8s securityContext (fsGroup: 65532)
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S -G nonroot nonroot

WORKDIR /app

COPY --from=builder /wait /wait
COPY --from=builder /bin/file-service /bin/file-service
COPY --from=builder /bin/sweep-ipfs-cids /bin/sweep-ipfs-cids

RUN mkdir /storage && chown nonroot:nonroot /storage
VOLUME /storage

# Remove the package manager from the runtime image. It is needed only by the
# `apk add` above, which has already run in an earlier layer.
#
# This is not theoretical: on the image running today, `apk` is present at
# /sbin/apk and FULLY FUNCTIONAL — `apk add --no-cache curl` succeeds inside the
# container. Anyone achieving code execution can fetch and install arbitrary
# tooling. Removing it costs nothing at runtime.
#
# /etc/apk and /lib/apk carry the package database and keys; without them a
# re-introduced apk binary still cannot install anything.
RUN rm -rf /sbin/apk /etc/apk /lib/apk /usr/share/apk /var/cache/apk

# NUMERIC user, not the `nonroot` name. The kubelet cannot resolve a name-based
# image user, so a Pod with `runAsNonRoot: true` and no explicit numeric
# `runAsUser` fails before the container starts:
#
#   CreateContainerConfigError: container has runAsNonRoot and image has
#   non-numeric user (nonroot), cannot verify user is non-root
#
# Reproduced on k8s-hetzner-sandbox. The `nonroot` account created above still
# provides the passwd entry and owns /storage; only the DECLARATION changes.
USER 65532:65532

EXPOSE 4003

ENTRYPOINT ["/bin/file-service"]
