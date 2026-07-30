# bloar

A long-term archive for EIP-4844 blobs, built on IPFS, serving the
beacon-API `blobs` endpoint that Arbitrum Nitro nodes use to sync historical
batches after the beacon chain has pruned them.

- [Design](docs/design.md) -- what it is and why it's shaped this way
- [Code walkthrough](docs/code-walkthrough.md) -- a guided tour of the
  components, data flows, and trust boundaries
- [Spec](docs/spec.md) -- normative details for implementation
- [Data structures, illustrated](docs/data-structures.md) -- diagrams of the DAG
- [Independent writers](docs/multi-writer.md) -- operate multiple failure-domain
  independent writers for one finalized logical archive
- [Standalone Kubo archive replica](docs/kubo-replica.md) -- retain and serve
  complete published heads through an existing operator-owned Kubo node
- [Decentralized swarm census](docs/swarm-census.md) -- measure timestamped
  local lower bounds without a central registry or phone-home service

Go implementation; module path `github.com/blobarchive/bloar`.

## Run a follower

The public network service is libp2p/Bitswap plus authenticated DNSLink/IPNS
publication. Operators run a local follower or Kubo replica; there is no hosted
public beacon API.

The fastest first run is the
[embedded-follower Docker quickstart](deploy/quickstart/follower/). It uses the
built-in full-verification Arbitrum One live follow profile and binds the local
beacon and metrics endpoints to loopback.

Operators who already run Kubo 0.42.x can instead use the
[existing-Kubo replica quickstart](deploy/quickstart/kubo-replica/). Kubo stays
the sole block store and swarm host; the controller adds publication trust,
exact generation pins, and an optional local read-only beacon gateway.

Published images are multi-architecture. Discover a version through its signed
GitHub Release, then pull the immutable default-image digest from the attached
`bloar-v0.X.Y-image.env`:

```sh
docker pull ghcr.io/blobarchive/bloar@sha256:<digest-from-release>
```

The quickstarts require that digest and have no mutable default. The same
handoff also binds the Internet-facing `ghcr.io/blobarchive/bloar-edge`
artifact through `BLOAR_EDGE_IMAGE_DIGEST`. Both images carry OCI
source/revision/created labels, embedded Go VCS information, per-platform SPDX
SBOMs, BuildKit provenance, and GitHub/Sigstore attestations.

## Releases

Bloar uses semantic `v0.X.Y` tags during initial development. A signed stable
tag on protected `main`, followed by a separate protected-environment approval,
publishes `linux/amd64` and `linux/arm64` variants of both the default and
public-edge images to GHCR. The workflow verifies and attests both immutable
digests before updating either `latest` tag or creating the GitHub Release. See
[Release authority and verification](docs/releases.md).
The initial distribution is container-only; source remains buildable through
the Makefile.

## License

Bloar is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE) and [NOTICE](NOTICE).

    make build    # compile
    make test     # go test ./...
    make lint     # gofmt check, go vet, staticcheck

See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete development checks and
[SECURITY.md](SECURITY.md) for private vulnerability reporting.
