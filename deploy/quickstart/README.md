# Docker quickstarts

These are the two supported first-run roles:

- [`follower/`](follower/) runs an embedded Bloar follower with its own archive
  store and libp2p host.
- [`kubo-replica/`](kubo-replica/) attaches the replica controller and optional
  local read gateway to an existing Kubo 0.42.x daemon.

Both use the public production follow authority and bind HTTP/metrics to
loopback. Neither exposes writer ingest or assigns a daemon-global
`container_name`. Both run as uid/gid 65532 with a read-only root filesystem,
all Linux capabilities dropped, `no-new-privileges`, bounded resources and
logs, and an in-container readiness probe. Only the documented state directory
and small named tmpfs mounts are writable.

Choose one role and follow its README. Both Compose files fail closed until
`BLOAR_IMAGE_DIGEST` names the immutable `sha256:...` digest attached to a
GitHub Release whose annotated tag and attestations verify. A version tag is
useful for discovery, but tags (including `latest`) are not durable deployment
references.

| Role | Compose project | Durable state | Local read endpoint |
| --- | --- | --- | --- |
| Embedded follower | [`follower/`](follower/) | `follower/data/` | `http://127.0.0.1:8550` |
| Existing-Kubo replica | [`kubo-replica/`](kubo-replica/) | `kubo-replica/state/` plus the follower/replica operator's Kubo repository | `http://127.0.0.1:8550/live` |

The Kubo role no longer uses host networking. It reaches an authenticated,
allowlisted Kubo RPC through a dedicated internal bridge; the controller cannot
reach the host's loopback namespace. The existing Kubo daemon remains the
blockstore, pin, provider, GC, and swarm authority.

The embedded sample defaults to `ethereum-mainnet-arb1-live-a`. The production
catalog also contains:

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

These are opaque follow-profile keys. Select one by exact name; do not derive
policy by parsing it. The Kubo sample spells out the equivalent Arbitrum One
finalized-plus-live selection because its controller consumes the signed
publication directly rather than expanding a follow profile.
