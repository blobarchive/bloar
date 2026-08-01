# bloar: bloard (the archive daemon), bloar-edge (the public P2P edge),
# bloar-index (the indexers), bloar-kubo-replica (the standalone controller for
# an operator-owned Kubo archive store), and the local bloar-swarm-inspect
# census CLI.
#
# Build from the repository root through the Makefile so the OCI provenance
# labels and the binary's embedded VCS revision are checked against one another:
#
#     make docker IMAGE=bloar:dev
#
# The image runs bloard by default. The other commands share the same image and
# select their own entrypoint, which is why all five binaries are built:
#
#     docker run bloar:dev --config /etc/bloar/bloard.yaml
#     docker run --entrypoint /bloar-edge bloar:edge -config /etc/bloar/edge.yaml
#     docker run --entrypoint /bloar-index bloar:dev beacon --config /etc/bloar/beacon.yaml
#     docker run --entrypoint /bloar-kubo-replica bloar:dev -config /etc/bloar/kubo-replica.yaml
#     docker run --entrypoint /bloar-swarm-inspect bloar:dev -help

FROM --platform=$BUILDPLATFORM golang:1.26@sha256:3aff6657219a4d9c14e27fb1d8976c49c29fddb70ba835014f477e1c70636647 AS build

ARG VCS_REF
ARG MODULE_PATH
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /src
COPY . .

# CGO_ENABLED=0 is what makes the runtime stage possible: a static binary needs
# no libc, so the image can be distroless.
#
# It is also a real choice about KZG. go-ethereum's crypto/kzg4844 uses the cgo
# c-kzg-4844 library when cgo is on and the pure-Go gokzg4844 when it is not, so
# this build verifies blobs in Go. That is slower per blob -- it is the one hot
# path in ingest, and `ingest.kzg_verify_duration_seconds` is where it shows --
# and it is bought for a static binary with no C toolchain in the runtime image
# and no glibc version to match. An operator who wants the C implementation
# builds with CGO_ENABLED=1 and a runtime base that has libc; the conformance
# suite passes either way.
#
# -trimpath so the binary does not embed /src. No -X version stamping: neither
# main has a variable to stamp, and an -ldflags -X naming a symbol that does not
# exist is silently ignored, which is worse than not doing it -- it looks like
# the image is versioned when nothing in it is. -buildvcs=true asks Go to bind
# the real Git revision into every production binary, but it is necessary rather
# than sufficient: Go still emits an unstamped binary when repository metadata
# is unavailable. The explicit go-version comparisons below are the
# load-bearing enforcement which makes that case fail instead of silently
# producing an unverifiable image.
#
# The ordinary Docker context is deny-all. The clean source commit arrives as
# a minimal, one-commit shallow Git metadata snapshot in a BuildKit secret
# mount. It contains no remote, credentials, refs, reflogs, hooks, or prior
# history. It is extracted only into an ephemeral tmpfs mount, so it cannot
# enter a layer or registry cache while Go can still prove the exact clean
# revision.
RUN --mount=type=secret,id=vcs_snapshot,required=true \
    --mount=type=tmpfs,target=/src/.git \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "${VCS_REF}" && test -n "${MODULE_PATH}" && \
    test -n "${TARGETOS}" && test -n "${TARGETARCH}" && \
    tar -xf /run/secrets/vcs_snapshot -C /src && \
    test "$(git -C /src rev-parse --verify HEAD)" = "${VCS_REF}" && \
    test "$(git -C /src rev-list --count HEAD)" = "1" && \
    test -z "$(git -C /src status --porcelain --untracked-files=all)" && \
    cd /src && \
    go mod download && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -buildvcs=true -trimpath -ldflags "-s -w" -o /out/bloard ./cmd/bloard && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -buildvcs=true -trimpath -ldflags "-s -w" -o /out/bloar-edge ./cmd/bloar-edge && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -buildvcs=true -trimpath -ldflags "-s -w" -o /out/bloar-index ./cmd/bloar-index && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -buildvcs=true -trimpath -ldflags "-s -w" -o /out/bloar-kubo-replica ./cmd/bloar-kubo-replica && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -buildvcs=true -trimpath -ldflags "-s -w" -o /out/bloar-swarm-inspect ./cmd/bloar-swarm-inspect && \
    for binary in bloard bloar-edge bloar-index bloar-kubo-replica bloar-swarm-inspect; do \
        build_info="$(go version -m "/out/${binary}")"; \
        module_path="$(echo "${build_info}" | awk '$1 == "mod" { print $2; exit }')"; \
        if test "${module_path}" != "${MODULE_PATH}"; then \
            echo "${binary}: embedded module ${module_path:-<absent>} does not match ${MODULE_PATH}" >&2; \
            echo "${build_info}" >&2; \
            exit 1; \
        fi; \
        if ! echo "${build_info}" | grep -F "vcs.revision=${VCS_REF}" >/dev/null; then \
            echo "${binary}: embedded VCS revision is absent or does not match ${VCS_REF}" >&2; \
            echo "${build_info}" >&2; \
            exit 1; \
        fi; \
        if ! echo "${build_info}" | grep -F "vcs.modified=false" >/dev/null; then \
            echo "${binary}: refusing an image built from modified or unstamped source" >&2; \
            echo "${build_info}" >&2; \
            exit 1; \
        fi; \
    done
# The edge is a separate runtime target on purpose. It declares no VOLUME:
# deployment mounts the writer's blocks directory read-only and supplies only
# two narrow writable paths (/var/lib/bloar-edge and /run/bloar-edge). Reusing
# the monolith stage would cause Docker to create writable anonymous volumes
# around the read-only child mount.
FROM gcr.io/distroless/static-debian12@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639 AS edge

ARG VCS_REF
ARG BUILD_DATE
ARG SOURCE_URL

LABEL org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.created="${BUILD_DATE}"

COPY --from=build /out/bloar-edge /bloar-edge

EXPOSE 4005 9555
USER nonroot:nonroot
HEALTHCHECK --interval=30s --timeout=5s --start-period=2m --retries=3 \
    CMD ["/bloar-edge", "healthcheck", "-config", "/etc/bloar/edge.yaml"]
ENTRYPOINT ["/bloar-edge"]
CMD ["-config", "/etc/bloar/edge.yaml"]

# distroless/static rather than scratch: it brings ca-certificates (the indexers
# talk to beacon nodes and L1 RPCs over TLS, and a follower polls an archive over
# HTTPS), /etc/passwd with a nonroot user, and tzdata. Scratch would mean
# carrying all three by hand.
FROM gcr.io/distroless/static-debian12@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639 AS bloar

ARG VCS_REF
ARG BUILD_DATE
ARG SOURCE_URL

LABEL org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.created="${BUILD_DATE}"

COPY --from=build /out/bloard /bloard
COPY --from=build /out/bloar-index /bloar-index
COPY --from=build /out/bloar-kubo-replica /bloar-kubo-replica
COPY --from=build /out/bloar-swarm-inspect /bloar-swarm-inspect

# The embedded archive and the standalone replica's replay/ownership ledger
# are both durable state. Kubo's block repository is mounted separately into
# its own daemon and is intentionally not a volume of this image.
VOLUME ["/var/lib/bloar", "/var/lib/bloar-kubo-replica"]

# 8550 is the API of spec 12's server.listen, 9550 the embedded metrics
# listener, 9097 the standalone replica metrics/status listener, and 4001
# libp2p. EXPOSE documents; it does not publish.
EXPOSE 8550 9550 9097 4001

# nonroot (65532) from the base image. The store directory must be writable by
# this uid on the host; see docs/operations.md.
USER nonroot:nonroot

ENTRYPOINT ["/bloard"]
CMD ["--config", "/etc/bloar/bloard.yaml"]
