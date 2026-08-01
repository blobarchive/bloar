# Existing-Kubo replica quickstart

This lets a BlobArchive follower/replica operator attach BlobArchive to a
stable Kubo 0.42.x daemon on the same Linux host. That operator is commonly the
same person or organization as the L2 node operator that consumes the endpoint;
it is not the writer operator that derives and signs the published head. Kubo
remains the sole block store, Peer ID, swarm host, pin database, provider, and
GC authority. The controller retains Arbitrum One plus the bounded shared live
tip and serves a loopback-only read gateway.

This replica does **not** require an Ethereum execution client, consensus
client, validator, semi-supernode, or supernode. It retains blobs that the
selected writer has already acquired and authenticated. Running a writer is a
different role with different upstream requirements.

This distributed sample is intentionally an Arbitrum One
finalized-plus-live replica. To retain another production selection, adapt the
head set and DNSLink authority using the strict
[`docs/kubo-replica.md`](../../../docs/kubo-replica.md) guide; do not infer
those values from a follow-profile name.

The controller does **not** use host networking. It reaches Kubo through one
stable, internal Docker bridge whose gateway is the only non-loopback address
on which Kubo's RPC listens. Kubo requires a bearer token and allowlists only
the RPC paths used by the replica. The controller keeps its ordinary Compose
network for DNS and public egress, but it cannot enter the host loopback
namespace. Do not attach unrelated containers to the control bridge.

## Prepare Kubo

Run these commands from this directory. First save the exact settings that a
permanent uninstall would restore, create the stable internal bridge, and
generate the shared RPC credential:

```sh
umask 077
ipfs config Addresses.API >.kubo-api-address.before
ipfs config --json API.Authorizations >.kubo-api-authorizations.before.json
./prepare-control-network.sh
openssl rand -base64 32 | tr -d '\n' >kubo-api-token
KUBO_API_TOKEN=$(cat kubo-api-token)
KUBO_CONTROL_GATEWAY=${KUBO_CONTROL_GATEWAY:-172.30.189.1}
```

The defaults use external Docker network `bloar-kubo-control`,
`172.30.189.0/28`, gateway `172.30.189.1`. To avoid a local subnet conflict,
export matching `KUBO_CONTROL_NETWORK`, `KUBO_CONTROL_SUBNET`, and
`KUBO_CONTROL_GATEWAY` values before both the script and every Compose command.
The script creates the network once, verifies an existing network exactly, and
refuses an unexpected listener on Kubo's common TCP 5001 RPC port.

Now configure the required bounded provider policy, an exact RPC allowlist,
and Kubo's bridge-only API listener:

```sh
ipfs version --number
ipfs config Provide.Enabled
ipfs config Provide.Strategy
ipfs config --json Provide.Enabled true
ipfs config Provide.Strategy roots

KUBO_ALLOWED_PATHS='["/api/v0/version","/api/v0/commands","/api/v0/id","/api/v0/block/get","/api/v0/block/put","/api/v0/pin/add","/api/v0/pin/ls","/api/v0/pin/rm","/api/v0/pin/update","/api/v0/provide/once","/api/v0/routing/get","/api/v0/swarm/connect"]'
KUBO_AUTH=$(printf '{"AuthSecret":"bearer:%s","AllowedPaths":%s}' \
  "$KUBO_API_TOKEN" "$KUBO_ALLOWED_PATHS")
ipfs config --json API.Authorizations.BlobArchive "$KUBO_AUTH"
ipfs config Addresses.API "/ip4/${KUBO_CONTROL_GATEWAY}/tcp/5001"
```

These settings affect every workload in that Kubo repository. The runtime
allowlist deliberately omits `/api/v0/config`: Kubo authorizes by path, so a
token able to read `Provide.*` could also mutate arbitrary configuration,
including its own authorization and API bind. The two native-host reads above
are therefore the provider-policy preflight; the container cannot repeat them.
Recheck them from the host before restarting after any Kubo configuration
change.

Restart Kubo with its normal service manager. Kubo must fail to start rather
than fall back to a wildcard/loopback API if the bridge address or port is not
available. Order a native Kubo service after `docker.service` so Docker has
restored the external bridge and its gateway before Kubo binds the RPC after a
host reboot. Then verify the authenticated runtime API:

```sh
ipfs version --number --api-auth "bearer:${KUBO_API_TOKEN}"
ipfs id --api-auth "bearer:${KUBO_API_TOKEN}" >/dev/null
```

Expected: stable `0.42.x`; an unauthenticated request to the bridge RPC must
return HTTP 403.

This bearer is least privilege only within Kubo's path-based API model. It
still intentionally grants repository-wide block put/get, recursive pin
add/list/update/remove, bounded provide, routing-get, and swarm-connect calls.
The normal BlobArchive client wrappers and durable ownership ledger constrain
which CIDs the controller changes, but Kubo cannot enforce those
content-specific rules on a compromised controller process holding the token.
Use a dedicated Kubo instance when unrelated pins require an RCE-grade
isolation boundary, not merely application-level ownership discipline.

Finally make only the controller's token copy readable by the image's
nonroot uid:

```sh
sudo chown 65532:65532 kubo-api-token
sudo chmod 0400 kubo-api-token
unset KUBO_API_TOKEN KUBO_AUTH
```

## Check and start

From this directory:

```sh
BLOAR_VERSION=v0.1.0
curl -fsSL -O \
  https://github.com/blobarchive/bloar/releases/download/$BLOAR_VERSION/bloar-$BLOAR_VERSION-image.env
# Inspect it, then load its immutable BLOAR_IMAGE_DIGEST value.
set -a
. ./bloar-$BLOAR_VERSION-image.env
set +a
sudo install -d -m 0750 -o 65532 -g 65532 state
docker compose pull
docker compose run --rm replica \
  -config /etc/bloar/kubo-replica.yaml -check
docker compose up -d
```

Compose refuses to pull or start when `BLOAR_IMAGE_DIGEST` is absent. Verify
the release tag, attestation, and digest using
[`docs/releases.md`](../../../docs/releases.md) before loading the environment
file.

The check verifies live Kubo version, the config-free runtime RPC profile, and
Peer ID before opening controller state. `provider_policy_check: external`
records that `Provide.Enabled=true` and `Provide.Strategy=roots` were validated
by the native-host preflight rather than by a config-admin container token.
Compose publishes only the read gateway and metrics listener, both on host
loopback. The native Kubo swarm ports remain entirely Kubo's responsibility;
this Compose file does not publish TCP 4001 or replace an existing IPFS
container.

The controller runs as uid/gid 65532 with no Linux capabilities, a read-only
root filesystem, `no-new-privileges`, bounded CPU/memory/PIDs/file descriptors,
and bounded local logs. The only durable writable path is `state/`; two small
tmpfs mounts cover temporary and unused image-volume paths. Defaults are 4
CPUs, 8 GiB memory, 512 PIDs, and 10 MiB × 3 log files. Override them with the
same `BLOAR_CPUS`, `BLOAR_MEMORY_LIMIT`, `BLOAR_PIDS_LIMIT`,
`BLOAR_LOG_MAX_SIZE`, and `BLOAR_LOG_MAX_FILES` variables documented by the
embedded follower.

Monitor the loopback endpoints:

```sh
curl --fail --silent --show-error http://127.0.0.1:9097/healthz
curl --fail --silent --show-error http://127.0.0.1:9097/readyz
curl --fail --silent --show-error http://127.0.0.1:9097/replica/status
```

The beacon-compatible gateway is `http://127.0.0.1:8550/live`. It reads only
the committed Kubo-local generation; a request miss never initiates Bitswap.
Mutation routes are absent.

`docker compose ps` reports the controller's real `/readyz` result as container
health. A non-ready status means the runtime Kubo capability/identity contract,
retained generation, followed heads, or the optional gateway is not yet
serviceable; Docker does not restart the controller merely because its health
is red. External provider-policy drift is not visible to the container.

## Stop without deleting pins

```sh
docker compose down
```

The external `bloar-kubo-control` network deliberately remains: Kubo's native
RPC is still bound to its gateway. Preserve `state/`, `kubo-api-token`, that
network, and the controller-owned Kubo generation pin. Do not run Kubo GC,
bulk-delete pins, restore the old API bind, or delete controller state as an
outage-recovery step. Permanent removal first follows the ownership-ledger
procedure in
[`docs/kubo-replica.md`](../../../docs/kubo-replica.md).
