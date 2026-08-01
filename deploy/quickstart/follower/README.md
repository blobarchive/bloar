# Embedded follower quickstart

This lets a BlobArchive follower/replica operator start a full-retention
Arbitrum One follower with the bounded shared live tip and a local
beacon-compatible endpoint. That operator is commonly the same person or
organization as the L2 node operator that consumes the endpoint; it is not the
writer operator that derives and signs the published head.

Requirements: Linux, Docker Engine with Compose v2, and enough disk for the
selected complete archive.

This follower does **not** require an Ethereum execution client, consensus
client, validator, semi-supernode, or supernode. It replicates blobs that the
selected writer has already acquired and authenticated. Running a writer is a
different role with different upstream requirements.

## Start

From this directory:

```sh
BLOAR_VERSION=v0.1.0
curl -fsSL -O \
  https://github.com/blobarchive/bloar/releases/download/$BLOAR_VERSION/bloar-$BLOAR_VERSION-image.env
# Inspect it, then load its immutable BLOAR_IMAGE_DIGEST value.
set -a
. ./bloar-$BLOAR_VERSION-image.env
set +a
sudo install -d -m 0750 -o 65532 -g 65532 data
sudo install -m 0400 -o 65532 -g 65532 /dev/null token
openssl rand -base64 32 | sudo tee token >/dev/null
docker compose pull
docker compose up -d
```

Compose refuses to pull or start when `BLOAR_IMAGE_DIGEST` is absent. Verify
the release tag, attestation, and digest using
[`docs/releases.md`](../../../docs/releases.md) before loading the environment
file.

The container runs as uid/gid 65532 with no Linux capabilities, a read-only
root filesystem, `no-new-privileges`, bounded CPU/memory/PIDs/file descriptors,
and bounded local logs. The only durable writable path is `data/`; two small
tmpfs mounts cover temporary and unused image-volume paths. Defaults are 4
CPUs, 8 GiB memory, 512 PIDs, and 10 MiB × 3 log files. Set
`BLOAR_CPUS`, `BLOAR_MEMORY_LIMIT`, `BLOAR_PIDS_LIMIT`,
`BLOAR_LOG_MAX_SIZE`, or `BLOAR_LOG_MAX_FILES` before `docker compose up`
when the host needs lower bounds. A limit below the selected head's actual
working set will fail closed through readiness or process exit rather than
silently dropping archive state.

The HTTP and metrics endpoints bind to loopback:

```sh
curl --fail --silent --show-error http://127.0.0.1:9550/healthz
curl --fail --silent --show-error http://127.0.0.1:9550/readyz
curl --fail --silent --show-error http://127.0.0.1:8550/bloar/v1/heads
```

`readyz` remains unavailable until the selected generation is locally
serviceable. A completely fresh identity may spend a couple of quiet minutes
building its public-DHT routing table before it adopts the first head; the
production cold-start acceptance measured 2m19s. This is discovery, not the
full archive sync. Initial full retention may take substantial time and disk.
`docker compose ps` reports the same `/readyz` result as container health; an
`unhealthy` status during initial retention means “not yet serviceable,” not
that Compose has restarted or deleted the follower.

## Choose another production head

Change the scalar `follow` value in `bloard.yaml`, then inspect it before
restarting:

```sh
docker compose run --rm --no-deps follower \
  config-inspect -config /etc/bloar/bloard.yaml
```

The built-in selectors are:

```text
ethereum-mainnet-all-a
ethereum-mainnet-arb1-a
ethereum-mainnet-robinhood-a
ethereum-mainnet-base-a
ethereum-mainnet-all-live-a
ethereum-mainnet-arb1-live-a
ethereum-mainnet-robinhood-live-a
ethereum-mainnet-base-live-a
```

Their exact contracts are documented in
[`docs/follow-profiles.md`](../../../docs/follow-profiles.md). Names are opaque
keys; do not infer policy by parsing them.

## Inbound swarm serving

The quickstart publishes TCP 4001 by default so the follower can accept direct
inbound swarm connections. **Allow TCP 4001 through the host firewall and
forward it through the router whenever possible.** Publishing the container
port does not by itself create a route through either boundary.

If another IPFS node already occupies host TCP 4001, set
`BLOAR_P2P_PORT` to a free host port before `docker compose up`, then allow and
forward that port instead. The loopback HTTP/metrics mappings likewise fail
with a normal bind error when occupied; no fixed `container_name` can replace
another container.

## Stop and preserve state

```sh
docker compose down
```

This keeps `data/`, including archive blocks, the libp2p identity, publication
floors, and follower checkpoints. Do not use `down -v` or delete `data/` as an
outage-recovery step.
