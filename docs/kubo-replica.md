# Standalone Kubo archive replica

## Why replica mode exists

Replica mode lets an operator contribute a complete Bloar archive through an
IPFS node they already run. Kubo remains the one multi-terabyte block store,
the one Peer ID, and the one place where peering, Bitswap, repository sizing,
pins, and garbage collection are operated. `bloar-kubo-replica` adds only the
Bloar-specific work: authenticating a writer's publication, refusing replay,
keeping the selected heads recursively pinned, and—when explicitly
enabled—serving those committed Kubo-local bytes through Bloar's read-only
beacon-compatible HTTP surface.

Use it when all of these are true:

- you want to retain and serve the complete DAG for one or more published
  heads;
- you already operate Kubo, or prefer Kubo's network and storage tooling to a
  second embedded IPFS stack;
- Kubo's node-wide providing policy can safely be set to
  `Provide.Enabled=true` and `Provide.Strategy=roots`; and
- contributing the archive over Bitswap is sufficient, or you also want an
  operator-controlled read-only Bloar endpoint without a second archive store.
  This command never runs a writer, indexer, or ingest endpoint.

The mode exists to increase the number of independent, full archive peers
without making every operator deploy a second multi-terabyte repository. It is
not a smaller cache or a light follower: every selected head uses full
retention. Selecting fewer heads is supported, and blocks shared by those heads
are naturally deduplicated by CID.

Do not use replica mode as an honesty audit. It proves that the bytes fetched
match the CIDs in an authenticated publication and refuses freshness rollback,
but it inherits the writer's decision about which blobs exist. An independent
beacon feed is still required to audit completeness.

## What it is—and is not

| Concern | Owner in replica mode |
| --- | --- |
| Block bytes and repository capacity | Kubo |
| Peer ID, transports, peering, DHT, and Bitswap | Kubo |
| Pin database and repository GC | Kubo/operator |
| Publication signature and replay floors | `bloar-kubo-replica` |
| Selected-head checkpoints | `bloar-kubo-replica` Pebble state |
| Full-generation retention intent | one named recursive Kubo pin |
| Stable discovery namespaces | one deterministic direct pin per selected head |
| Writer, ingest, indexing, and IPNS publication | not present |
| Optional public Bloar HTTP read API | `bloar-kubo-replica`, reading only the committed Kubo-local generation |

This is a standalone follower controller. It does not replace `bloard`'s
embedded blockstore with an exclusively managed Kubo repository.

The data path is deliberately narrow:

```text
authenticated heads document
           |
           v
  bloar-kubo-replica ---- bounded checkpoints/ownership ---- Pebble
           |                                   |
           | optional GET-only Bloar/beacon API|
           | block get/put + exact named generation pins
           | + bounded stable rendezvous direct pins
           v
   operator-owned Kubo ---- block bytes, Bitswap, DHT, pins, GC
```

The controller does not enumerate the Kubo repository, delete blocks, invoke
Kubo GC, manage Kubo keys, or publish IPNS records. Its blockstore wrapper is
append-only and rejects both deletion and full-repository enumeration. IPNS
following, when configured, reads raw records through Kubo's routing API and
retains the same sequence-number replay protection as an embedded follower; it
does not need the writer's private key.

The optional HTTP gateway receives only the Kubo **local** blockstore
capability. Every public archive read becomes `block/get?offline=true`; it
cannot invoke Bitswap, walk an unretained network DAG, mutate Kubo, or turn a
missing retained block into a successful response by fetching it on demand.
It serves the same registry snapshot that the retention controller has
committed, under the same reader lease which delays retirement of an older
generation until in-flight responses finish.

## Honest shared-Kubo trade-offs

Sharing a Kubo node is the main benefit, but it creates real coupling:

- `Provide.Strategy` is node-wide. `roots` prevents periodic DHT walks over
  every block in a multi-terabyte archive, but it also changes how unrelated
  content in that Kubo repository is advertised. Pin roots remain
  discoverable; arbitrary child CIDs are not independently advertised.
- Kubo GC and pin operations are scheduled by the Kubo operator. A long GC can
  delay a generation transition. The replica deliberately serves its previous
  committed generation instead of coordinating, disabling, or running GC.
- Any administrator of the Kubo node can change or remove pins behind the
  controller. The controller detects loss or ownership drift and fails closed;
  it cannot make a shared administrative API trustworthy.
- A recursive initial pin can take hours or days and can grow the existing
  repository by terabytes. Replica mode does not impose a storage quota.
- With `roots`, a new reader starts from an advertised root and normally
  continues fetching descendants from connected archive peers. An arbitrary
  child CID is less independently discoverable than it would be under `all` or
  `pinned`. This bounded-discovery trade-off is intentional: periodically
  advertising tens of millions of archive blocks is not an acceptable default.

If these node-wide choices would break another workload, use a separate Kubo
instance. That is an operational separation, not a Bloar requirement for an
exclusive repository.

## Requirements

- A stable Kubo **0.42.x** daemon. Prereleases and other release lines are
  rejected. SemVer build metadata is accepted.
- The Kubo 0.42 command and flag surface used by the replica. Startup checks
  capabilities as well as the version, so a restricted reverse proxy must pass
  the required RPCs.
- Enough local disk for every selected full archive, including transition
  over-retention and other Kubo workloads.
- At least one publication source: HTTPS (or loopback HTTP), direct IPNS, or
  one-hop DNSLink.
- The writer's 32-byte Ed25519 publication public key, except when DNSLink
  delegates the signer.
- A private metrics listener. Replica lag and a long pin must not be silent.
- If the optional gateway is enabled, an explicit listener/TLS boundary and
  measured connection, request, response-memory, and rate limits.

This standalone mode currently uses `follow.verify=cid` only: every fetched
block is content-address verified, and publication signatures/rollback floors
are enforced, but the optional full per-blob KZG recomputation mode is not
wired through external retention. Operators who require that additional audit
must run it separately; configuration cannot silently claim `verify=full`.

Kubo remains independently operated. Back it up, size its datastore, configure
peering and reachability, and monitor its GC with Kubo's own tooling.

## Configure Kubo safely

Replica startup requires these exact effective values:

```sh
ipfs config --json Provide.Enabled true
ipfs config Provide.Strategy roots
```

Restart Kubo using its normal service manager, then verify the persisted
values:

```sh
ipfs version --number
ipfs config Provide.Enabled
ipfs config Provide.Strategy
```

The expected outputs are a stable `0.42.x` version, `true`, and `roots`.
Record the prior values before changing them: both settings affect the whole
Kubo node, not just Bloar.

With the default `kubo.provider_policy_check: runtime`, the controller rechecks
the stable Kubo release/capabilities, Peer ID, and both provider values every
`replica.audit_interval`. Drift withdraws readiness until the exact contract is
restored. A least-privilege bridge deployment may set
`provider_policy_check: external`: the operator checks both provider values
from the native host before start, while the controller token omits Kubo's
read/write config RPC. That mode continues to audit capabilities and Peer ID
but cannot observe later provider-policy drift.

Why `roots` is mandatory:

- `all` periodically considers every stored block for providing;
- `pinned` traverses the descendants of recursive pins; and
- the replica archive may contain tens of millions of blocks.

Both archive-walking strategies turn routine DHT reprovide work into a
repository-scale operation. `roots` advertises only explicit pin roots. The
replica additionally performs bounded, non-recursive `provide once` calls for
its current generation entry points rather than enabling a per-block walk.
`Provide.Enabled=false` is rejected because it would retain data without
contributing discoverability.

The controller forces Kubo's fast-provide DAG/root/wait flags off on its long
pin operations. It never changes the two configuration keys itself; startup
only reads and validates them.

## Protect the Kubo RPC API

Kubo's RPC API is an administrative interface. Never expose it directly to an
untrusted network.

The replica accepts one of these arrangements:

- an unauthenticated Kubo API on loopback, with
  `allow_unauthenticated: true`;
- an HTTPS or loopback reverse proxy which validates a bearer token, with
  `bearer_token_file` pointing to a regular credential file; or
- an explicitly trusted non-loopback HTTP network, with bearer authentication
  and `allow_insecure_http: true`. HTTPS is preferred.

Unauthenticated access is accepted only for loopback hosts, over HTTP or HTTPS.
Remote HTTPS APIs require a bearer token. Tokens are never accepted inline by
the YAML format. Redirects are disabled so a token cannot be forwarded to
another authority.

A reverse proxy may allow the replica's checked RPC subset: version and command
discovery, node identity, block get/put, pin add/list/update/remove,
one-shot non-recursive provide, and routing get plus swarm connect for
authenticated publication address hints. Runtime provider-policy checking also
needs the config path. Kubo cannot authorize that path read-only, so omit it and
set `provider_policy_check: external` when the token must not mutate arbitrary
Kubo configuration. The subset never needs block removal, repository
enumeration, repository GC or verify, key generation, or name publication. Run
the binary's live `-check` after changing a proxy policy; the selected
capability profile is authoritative.

Path restriction is not CID restriction. The runtime token intentionally
permits repository-wide block and pin operations, and Kubo cannot enforce the
controller's ownership ledger against a compromised process holding that
token. Use a dedicated Kubo instance if unrelated pins need an RCE-grade
isolation boundary.

## Install and validate

Build the standalone command from the repository root:

```sh
go build -trimpath -o bloar-kubo-replica ./cmd/bloar-kubo-replica
```

The repository also ships:

- [`deploy/examples/kubo-replica.yaml`](../deploy/examples/kubo-replica.yaml),
  a strict configuration example; and
- [`deploy/systemd/bloar-kubo-replica.service`](../deploy/systemd/bloar-kubo-replica.service),
  a hardened service unit.

The unit expects `/etc/bloar/kubo-replica.yaml`, stores bounded controller state
under `/var/lib/bloar-kubo-replica`, and delivers
`/etc/bloar/kubo-api-token` as a systemd credential. If using an ordinary
unauthenticated loopback Kubo API, remove the unit's `LoadCredential=` line in
a local override and set `allow_unauthenticated: true`; otherwise a missing
credential source prevents systemd from starting the service.

### Container invocation

The bundled image defaults to the main `/bloard` entrypoint. Override it for a
replica, and persist `/var/lib/bloar-kubo-replica`: the state there is the replay
and ownership ledger, not disposable container cache. Kubo's repository remains
a volume of its separately operated daemon.

The image runs as uid/gid `65532`. Create a bind-mounted state directory with
that ownership before first start. Do not use host networking merely to reach a
native Kubo loopback API: it also gives the controller general host-loopback
reach. The distributed
[`deploy/quickstart/kubo-replica`](../deploy/quickstart/kubo-replica/) sample
uses a stable internal bridge, bridge-only Kubo RPC bind, bearer/path
allowlist, loopback-only published gateway/metrics, read-only root filesystem,
and explicit resource bounds. Use that Compose file as the container reference
deployment.

Before the first start, run the read-only live preflight:

```sh
bloar-kubo-replica -config /etc/bloar/kubo-replica.yaml -check
```

It validates the strict YAML, Kubo version, advertised RPC capabilities,
authentication, and Peer ID. In the default runtime policy-check mode it also
validates the exact provider policy. In external mode the native-host preflight
owns that check. It does not open replica state, fetch archive blocks, or
change a pin.

### Disposable GC/restart conformance

Developers can exercise the real Kubo retention transaction with a stable 0.42
`ipfs` binary:

```sh
BLOAR_KUBO_DISPOSABLE_INTEGRATION=1 \
  BLOAR_KUBO_BINARY=/path/to/ipfs \
  go test -race ./cmd/bloar-kubo-replica \
  -run TestDisposableKubo042GCAndRestartRetention -count=1 -v
```

The explicit gate is required because this test invokes `repo gc`. It accepts
no Kubo URL: the harness initializes and owns a temporary repository, binds its
daemon only to reserved loopback ports, disables bootstrap and mDNS, and removes
the repository through Go's test cleanup. It commits pair A, prepares pair B
add-before-remove, collects at both transition boundaries, restarts Kubo and the
controller metadata, then proves and collects pair B again. Never adapt this
test to run against a shared or production repository.

## Configuration

Start from the shipped example. Unknown YAML fields, multiple YAML documents,
and files over 1 MiB are rejected.

### Top level and replica identity

| Field | Meaning |
| --- | --- |
| `version` | `1` for the legacy finalized-only head list; `2` for explicit proof-aware head selections. |
| `net` | Publication network name; must match the signed document. |
| `replica.id` | Stable local identity: 1–64 lowercase ASCII letters, digits, `.`, `_`, or `-`. It is not a Peer ID. |
| `replica.state_path` | Absolute path to the controller's Pebble metadata. Never point it at the Kubo repository. |
| `replica.pin_name` | Reserved exact Kubo pin name. Empty derives `bloar-replica/v1/<id>`. |
| `replica.heads` | One to 64 unique publication head names. Version 1 uses a flat list; version 2 uses the structured map below. Each selected generation is retained in full. |
| `replica.audit_interval` | Interval between read-only proofs that the committed anchor remains an exact recursive pin, with the reserved name when controller-owned; defaults to `1m`. This is independent of publication polling. |

Keep `replica.id`, `state_path`, and `pin_name` stable. The Pebble directory
contains anti-replay floors, follower checkpoints, and the ownership ledger
which tells the controller which generation pins it may remove. Losing it is
not equivalent to losing a cache.

### Selecting finalized and live heads

Version 1 remains accepted for existing finalized-only replicas. Every name in
its flat list is pinned to the legacy `finalized-monotonic` contract:

```yaml
version: 1
replica:
  heads: [all]
```

This file-format version is independent of the generation-anchor and controller
state formats. Moving the YAML to version 2 does not rename
`bloar-replica/v1/<id>`, change the anchor schema, or require a new state path.

Use version 2 when selecting a mutable live head. Every entry must state its
expected signed ordering contract explicitly:

```yaml
version: 2
replica:
  heads:
    arbitrum-one:
      kind: finalized-monotonic
    unfinalized:
      kind: unfinalized-mutable
      handoff_head: all
      max_window_slots: 64
      overlay_finalized_head: arbitrum-one
```

This controller generation retains exactly `arbitrum-one` and the complete
bounded `unfinalized` generation; unrelated and borrowed Kubo pins remain
untouched. The signed publication's `all` entry authenticates the global
live-to-finalized handoff, but it is metadata only: because `all` is not a key
in `replica.heads`, it never enters the current generation anchor, readiness
gates, rendezvous initialization, or announcement targets.

An upgrade from an older configuration which selected `all` can leave that
name's tiny deterministic direct rendezvous pin in Kubo. The controller never
bulk-removes obsolete namespace pins, and Kubo's `roots` strategy can continue
reproviding it. It protects only the public namespace bytes, not the old `all`
archive DAG. Follow the exact-CID migration guidance below if the stale
discovery claim should be retired.

The mutable fields are fail-closed contracts:

- `handoff_head` is the finalized entry whose root and slot must match the
  mutable proof in the same authenticated publication document;
- `max_window_slots` is a positive hard limit, at most 4096, on the complete
  live snapshot the replica will fetch and retain; and
- `overlay_finalized_head` names the selected filtered-finalized head which
  must reach `window_start - 1` in that same document. It is required when the
  authenticated handoff is metadata-only. This prevents the retained
  finalized/live pair from opening a serving gap.

A selected handoff head must itself be `finalized-monotonic`. When the selected
handoff already supplies the retained finalized frontier, omit the redundant
overlay. A mutable selection also requires an explicit `source.pubkey` even
with DNSLink: mutable revision order is local to one signing authority, so
signer rotation cannot be delegated.

Proof, kind, window, and overlay-gap checks run before Kubo prepares a new
generation. They run again from the authenticated checkpoint on restart before
the command exposes the selected pair, and restart additionally requires one
exact recursive Kubo generation pin covering both selected heads. A refusal
therefore leaves the old anchor current; it cannot partly advance one side of
the pair.

Treat any change to the selected-head set in an existing state directory as an
exact-set migration. Once v3 document checkpoints exist, restart cannot mix the
old checkpoint/anchor set with the new configuration: the newly configured set
remains unexposed and its readiness gates stay red until the immediate
publication poll authenticates and atomically retains one exact new generation.
The prior Kubo generation remains safely pinned during that transition.
Preserve the state directory; do not erase replay or ownership history to force
the migration.

### Optional read-only Bloar/beacon gateway

`gateway.enabled` defaults to `false`. When enabled, the replica mounts only
the public GET surface:

- `/{head}/eth/v1/beacon/blobs/{slot}`;
- `/{head}/eth/v1/beacon/genesis` and `/{head}/eth/v1/config/spec`; and
- the read-only `/bloar/v1/heads...` metadata, manifest, and generation routes.

No bearer token or mutation authority exists in this mode. The blobs, refs,
truncate, manifest, and generation POST routes are not mounted; they return a
JSON 404. The gateway does not make this process a writer and does not add an
arbitrary-CID or Kubo proxy endpoint.

The shipped example contains a complete disabled block. Changing only
`enabled` to `true` is sufficient for its mainnet filtered-finalized plus
mutable selection:

```yaml
gateway:
  enabled: true
  listen: 127.0.0.1:8550
  beacon:
    genesis_time: 1606824023
    seconds_per_slot: 12
    genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
    genesis_fork_version: "0x00000000"
    spec_extra:
      DEPOSIT_CHAIN_ID: "1"
  live_heads:
    live:
      finalized_head: arbitrum-one
      unfinalized_head: unfinalized
      require_versioned_hashes: true
```

`live_heads` may name only selected physical heads. The finalized member must
be the retained frontier for the mutable member. When a chain-filtered
finalized overlay differs from the signed global handoff witness,
`require_versioned_hashes` must be true; Nitro already sends those hashes.

The remaining gateway fields bound public work:

| Field | Meaning |
| --- | --- |
| `max_query_hashes` | Maximum decoded `versioned_hashes` entries across repeated-key or comma-separated array encoding, at most the protocol slot ceiling. |
| `max_response_bytes_in_flight` | Global admission budget held from lookup through response rendering. It must admit at least one maximum request. |
| `immutable_horizon_slots` | Finalized response cache horizon, matching the ordinary server contract. |
| `read_header_timeout`, `read_timeout`, `write_timeout`, `idle_timeout` | Finite HTTP server and large-response deadlines. |
| `max_header_bytes`, `max_conns` | Header and accepted-connection bounds; zero selects finite defaults rather than an unbounded opt-out. |
| `public_read_admission` | Default-on global and per-client token buckets. Trusted forwarded-client headers require canonical proxy CIDRs. |

Bind the gateway to loopback or a private network unless a TLS/reverse proxy
is deliberately publishing it. The gateway itself has no authentication
because its routes are public reads; the safety controls are network exposure,
admission, deadlines, and the immutable content boundary.

Kubo-local absence returns a retryable service response and never initiates
Bitswap. A CID mismatch or malformed local block fails the request; the
gateway never heals corruption by asking the network. This behavior is
load-bearing: an endpoint which fetched on a request would let unauthenticated
clients turn the replica into a network-fetch oracle and would serve data not
yet covered by the committed recursive generation pin.

### Publication source

| Field | Meaning |
| --- | --- |
| `source.url` | HTTPS publisher base URL, or HTTP only on a literal loopback/`localhost` host. The follower appends `/bloar/v1/heads`; do not include that endpoint path in this field. |
| `source.ipns` | Direct IPNS name resolved through Kubo. Mutually exclusive with `dnslink`. |
| `source.dnslink` | One-hop DNSLink name which delegates the IPNS name and signer. Mutually exclusive with `ipns`. |
| `source.pubkey` | Writer's 32-byte Ed25519 public key as 64 hexadecimal characters. Required unless DNSLink delegates the signer. |
| `source.poll_interval` | Publication polling interval; defaults to `1m`. |
| `source.fetch_timeout` | Per-block fetch deadline; defaults to `30s`. Publication-document polls have their own fixed 30-second deadline. |

HTTPS and direct IPNS may be configured together. The follower evaluates both
channels, verifies the same publication authority, and keeps independent
freshness/replay floors. At least one of `url`, `ipns`, or `dnslink` is
required.

The one-minute default is part of the public freshness contract. Writers
publish authenticated generations in batches, so a live Kubo replica normally
adopts new blobs roughly one to two minutes after the source sees them rather
than once per Ethereum slot. DHT conditions and bounded retries can
occasionally extend that delay to several minutes. These are healthy-path
expectations, not a ceiling: publication can remain valid but frozen during a
longer outage. Monitor the age of the last accepted generation rather than
request errors alone. Initial recursive pinning and catch-up after downtime can
take much longer.

### Kubo RPC and long operations

| Field | Meaning |
| --- | --- |
| `kubo.api` | Kubo RPC authority, optionally with a reverse-proxy path prefix. The client adds the API endpoint paths. |
| `kubo.bearer_token_file` | Regular file read once at startup. Mutually exclusive with unauthenticated mode. |
| `kubo.allow_unauthenticated` | Explicitly permit a credential-free loopback API. Non-loopback APIs require bearer authentication even over HTTPS. |
| `kubo.allow_insecure_http` | Explicitly permit bearer auth over non-loopback HTTP. |
| `kubo.provider_policy_check` | `runtime` (default) reads and periodically audits `Provide.Enabled=true` and `Provide.Strategy=roots`; `external` omits the read/write config RPC from the runtime capability profile and makes the native-host preflight responsible for those values. |
| `kubo.request_timeout` | Ordinary bounded RPC timeout; defaults to `30s`. |
| `kubo.pin_timeout` | Explicit deadline for one archive-scale recursive add or incremental update; defaults to `24h`. |
| `kubo.announce_interval` | Periodic bounded announcement interval; defaults to `12h`. |
| `kubo.max_stream_items`, `max_stream_bytes` | Absolute decoded response ceilings for streamed RPCs. |
| `kubo.pin_progress_items`, `pin_progress_bytes` | Ceilings used for the initial recursive-pin progress response. |

The stream-byte ceilings bound encoded RPC responses, not the size of the
archive. The reported progress byte counter can therefore be much larger than
the number of HTTP response bytes. All ceilings are finite; increasing them
should be a measured response to a real limit error.

`metrics.listen` is required. Bind it to loopback or a private monitoring
network; it exposes operational state without authentication.

## Initial synchronization

1. Start Kubo and pass the live `-check`.
2. Start `bloar-kubo-replica`.
3. Watch its logs, `/readyz`, `/replica/status`, and the pin-progress metrics.
4. Do not consider the replica available until every selected head is ready and
   `/readyz` returns 200. With the gateway enabled, its listener is an
   additional readiness gate.

On the first valid publication, the controller creates a small deterministic
DAG-CBOR **generation anchor**. The anchor links every selected current head
root and manifest tip. One recursive named Kubo pin on that anchor therefore
protects the complete multi-head generation. The recursive pin fetches missing
descendants over IPFS, so first synchronization is intentionally the expensive
operation.

There is no partial-ready state: `/healthz` may return 200 while the process is
alive and an initial pin is still running, but `/readyz` remains 503 until all
configured heads have been adopted and protected. After recovery, restart first
identifies the one complete current or crash-pending generation which protects
the follower checkpoints it will expose. A read-only retention audit then runs
immediately and every `replica.audit_interval`; it proves that active anchor is
still an exact recursive pin and, when the controller owns it, still carries
the reserved name. An audit failure
independently makes `/readyz` return 503 even if publication polling is healthy,
and a later successful audit restores that gate. A separate audit at the same
interval withdraws readiness if the Kubo release/capabilities, Peer ID, or
provider policy drifts from the startup preflight. This command contributes
data over Kubo/Bitswap. If configured, the read-only gateway starts on its
declared listener and remains red in `/readyz` until that listener is serving.

If `pin_timeout` expires, the transition fails safely. The previous committed
generation, if any, remains pinned; on a first sync there is simply no ready
generation. Blocks Kubo already fetched may be reused by the retry, but until a
recursive pin commits they remain ordinary Kubo cache data and may be collected
according to Kubo's policy.

## Generation transaction and crash ordering

The order is designed so every interruption yields over-retention or lag, never
an exposed checkpoint whose DAG is unprotected:

1. Resolve and authenticate the publication; enforce document, IPNS sequence,
   per-head slot, and manifest ancestry floors.
2. Validate all selected heads with no checkpoint writes.
3. Write the deterministic generation-anchor block to Kubo.
4. Persist a synced `pending` generation intent and its intended pin ownership
   in Pebble. Pending is observable before the potentially long recursive pin
   finishes; it is not itself proof of protection.
5. Recursively pin the candidate. For a controller-owned predecessor, use
   `pin update old new --unpin=false`, so the old pin remains.
6. Verify the candidate is an exact recursive pin with the expected name.
7. Commit the selected follower checkpoints and freshness floor atomically.
8. Promote `pending` to `current` in the controller ledger.
9. Remove only superseded generation anchors the ledger records as
   controller-owned.

On restart, recovery validates the exact named pins against Pebble. A durable
`pending` record may legitimately have no pin if the process stopped after
writing the intent but before the recursive pin completed. Recovery leaves
that case as retryable intent; it does not count the record as protection, and
the next matching `Prepare` may safely resume the operation. If the follower
checkpoint was already committed before interruption, restart calls
`ProtectsAll` and requires one complete retained generation—never a mixture of
current and pending heads—to be an exact recursive pin before exposing any of
those checkpoints. A missing pin then fails closed. A still-pinned pending
generation can protect such a crash-interrupted checkpoint. Cleanup
failures remain as bounded cleanup debt and are retried; they do not make the
current generation unavailable.

Important failure outcomes:

| Failure point | Safe result |
| --- | --- |
| Before pending intent | old generation only; candidate is not authoritative |
| During initial pin/update | old generation remains pinned; publication is not committed |
| After candidate pin, before checkpoint | old and candidate both retained |
| After checkpoint, before controller commit | candidate remains pending and protects restart |
| During old-anchor cleanup | new generation is current; old generation is over-retained |

## Pin ownership and unrelated content

The derived pin namespace is `bloar-replica/v1/<replica.id>`. Give every
controller attached to one Kubo node a unique replica ID and pin name.

The controller queries only that exact pin name and exact candidate CIDs. It
does not enumerate all Kubo pins and never removes an unrelated or manual pin.
Kubo's `--name` filter is a substring match, so the client validates exact names
locally before treating a result as its own.

Ownership rules fail closed:

- a recursively pinned candidate which predates the controller under another
  name is **borrowed** and is never renamed or removed;
- an existing direct/non-recursive pin on the exact candidate is not silently
  upgraded;
- a pin using the reserved name without a matching durable ownership record is
  not adopted after state loss; and
- a missing or renamed controller-owned pin is ownership drift, not permission
  to recreate history by guessing.

A manual pin on a published head root is not the generation-anchor pin. It
remains untouched and may coexist with the replica. Likewise, Kubo GC respects
all ordinary pins from all applications; the replica owns no global pin policy.

The controller requires exclusive mutation authority over its reserved exact
pin name and every generation-anchor CID its ledger records as
controller-owned (`current`, `pending`, or cleanup debt) while the service is
running. Kubo 0.42 has no conditional operation meaning "remove this CID only
if it still has this exact name." Cleanup must therefore check a pin's name and
then remove it by CID in separate RPCs. An administrator racing that interval
could rename, remove, or repurpose the same CID between those calls. Do not use
other tooling to rename or remove the reserved pin, reuse its name, or pin the
same owned anchor CID under a different policy while the controller is active.
Stop the controller before an intentional mutation and verify the exact CID
and name first.

This exclusivity is narrow: other Kubo pins with other names and CIDs remain
safe to create, update, and remove concurrently. Manual pins on published head
roots and other unrelated archive CIDs are also outside the controller's
namespace. Do not delete the Pebble state first and expect the controller to
infer ownership from Kubo.

There is one intentional bounded exception to the rotating ownership ledger:
for every configured `(network, head)`, startup materializes the canonical tiny
raw rendezvous block and adds an unnamed **direct** pin. Stock Kubo 0.42 will not
run `provide once` for a CID absent from its local blockstore. These deterministic
pins contain only public namespace bytes, never rotate, and are never removed by
the generation controller. Include them in Kubo backups. Changing the network
or selected-head set may leave an obsolete tiny direct pin; an operator may
derive and inspect that exact old rendezvous CID before removing it. Never use a
bulk direct-pin deletion as migration or rollback.

## Kubo GC coexistence and lag

Kubo owns GC completely. `bloar-kubo-replica` never invokes `repo gc`, changes
auto-GC, deletes a block, or holds a Bloar-wide maintenance gate around Kubo.
Operators may retain their normal Kubo GC schedule.

The generation transaction is the coexistence mechanism. A GC or repository
operation may delay, block, or cause a candidate pin to exceed its deadline.
The controller then keeps serving the last committed generation and retries on
a later publication poll. This is visible lag, not data loss. On first sync,
readiness remains red because there is no prior committed generation.

Operational rules:

- never unpin the current generation to make GC faster;
- size `pin_timeout` for the largest expected update plus GC contention;
- monitor Kubo GC and repository capacity separately from the replica process;
- treat a long-lived pending generation as a storage/network/GC investigation,
  not as permission to remove the old pin; and
- remember that unrelated Kubo pins and cached data affect the same repository
  size and GC duration.

Kubo 0.42 snapshots its pin index before a reprovide cycle, avoiding the older
release's hours-long pin-index read lock, but a large repository can still make
pin traversal, GC, and storage I/O expensive. That is why the supported release
line and the `roots` strategy are both enforced.

## Providing and discoverability

The Kubo node serves the recursively pinned archive over Bitswap. Periodically,
the controller queues bounded non-recursive announcements for one stable
rendezvous CID per `(network, head)` and the current generation entry points:
the generation anchor, head roots, and manifest tips. The rendezvous CID is a
canonical tiny raw namespace block, stored and direct-pinned once so Kubo can
advertise it, and is stable across generations. Fetching that namespace block
proves no archive availability; the other targets let peers enter the currently
retained DAG. The
controller never asks Kubo to recursively announce every archive block.

These are ordinary IPFS provider records, not telemetry and not a centralized
report. The replica does not phone home. Provider records establish local
discoverability claims; they do not prove that a remote peer can reach the node
or that every descendant is intact. Monitor reachability and challenge actual
reads from independent vantage points when availability matters.

Static Kubo peering is still useful. In particular, stable connections between
writers and replicas make descendant fetches robust under the `roots`
announcement policy.

After a publication document passes signature and replay checks, its bounded
peer multiaddrs are also passed to Kubo `swarm/connect`. All peer attempts share
one finite `source.fetch_timeout` budget, divided across the remaining peers and
then across each peer's addresses. Malformed, oversized, stale, or unreachable
hints are logged and do not make the authenticated publication invalid; they are
connectivity hints, never trust input. This preserves the ordinary follower
behavior even though Kubo, rather than the controller process, owns the Bitswap
host.

## Monitoring, readiness, and status

The private listener exposes:

- `GET /healthz`: process liveness only;
- `GET /readyz`: 200 only after Kubo recovery, every selected head with a
  checkpoint is covered by one active generation and adopted, every configured
  head is ready, the latest independent retained-pin audit is healthy, and the
  periodic Kubo identity/capability audit is healthy (and, in runtime mode,
  the provider-policy audit is healthy). An
  enabled gateway must also be serving;
- `GET /metrics`: local Prometheus metrics; and
- `GET /replica/status`: bounded JSON with each current/pending anchor's
  ownership, durable retention timestamp, publication timestamp, and configured
  per-head root, manifest, and `synced_to`, plus transition age and cleanup-debt
  summary and `gateway_enabled`/`gateway_serving`. The generation format caps
  this detail at 64 heads.

An unexpected metrics-listener failure terminates the command so its service
manager can restart it; required readiness and lag signals never disappear
silently while replication continues.

Useful replica metrics:

| Metric | Interpretation |
| --- | --- |
| `bloar_replica_pin_progress_blocks` / `_bytes` | Latest observed initial recursive-pin progress snapshot. Pair with logs and pending state; it is not an `active` flag. |
| `bloar_replica_generation_current` / `_pending` | Whether durable current/pending generation state exists. Pending means a transition intent exists, not necessarily that its recursive pin has completed. |
| `bloar_replica_generation_retained_timestamp_seconds{state}` | Durable timestamp at which the current generation was promoted or the pending intent was first written. The pending timestamp survives same-generation retries and process restarts. |
| `bloar_replica_generation_head_present{state,head}` / `_synced_to{state,head}` | Per-configured-head progress in current and pending generations. Labels come only from configured heads; consult `head_present` before treating a zero floor as meaningful. |
| `bloar_replica_generation_pending` | `1` while a durable generation-transition intent exists. It may still be recursively pinning and is not, by itself, proof that the candidate is protected. |
| `bloar_replica_transition_in_progress` / `_started_timestamp_seconds` / `_age_seconds` | Scrape-time view of the durable pending intent. Age continues to advance during a silent multi-hour Kubo traversal because it is recomputed before each scrape. |
| `bloar_replica_cleanup_anchors` | Superseded owned anchors awaiting safe unpin. |
| `bloar_replica_cleanup_oldest_retained_timestamp_seconds` / `_age_seconds` | Former retention time and age of the oldest debt anchor. This is a conservative upper bound on debt duration, not the exact time cleanup first failed. |
| `bloar_replica_last_commit_timestamp_seconds` | Durable current-generation promotion time, preserved across restart; zero before the first. |
| `bloar_replica_transitions_total{operation,outcome}` | Prepare, commit, restart-protection, cleanup, retained-pin audit, and Kubo policy/identity audit outcomes. |
| `bloar_replica_last_transition_failure_timestamp_seconds{operation,class}` | Process-local timestamp of the latest bounded failure class (`cleanup`, ownership drift, unprotected, timeout, canceled, or other) for each operation. Error text and CIDs are never labels. |
| `bloar_replica_state_readable` | Whether the most recent controller-state refresh decoded successfully. `/replica/status` returns 503 when it cannot. |
| `bloar_replica_gateway_enabled` / `_serving` | Configuration intent and current listener state for the optional read-only gateway. Public read counters and latency use the ordinary `bloar_beacon_*` and admission metrics. |
| `bloar_replica_announcements_total{outcome}` | Bounded provider-announcement attempts. |
| `bloar_replica_last_announcement_timestamp_seconds` | Last successful bounded announcement; zero before the first. |

The shared follower metrics remain useful:

- `bloar_follow_head_ready{head}` and `/readyz` identify unserved selected
  heads;
- `bloar_head_synced_to{head}` shows locally adopted progress;
- `bloar_follow_polls_total{channel,outcome}` shows source resolution health;
- `bloar_follow_refusals_total{reason}` exposes anti-replay and consistency
  defences. In particular, `reason="handoff_blocked"` means the authenticated
  filtered-finalized frontier did not close the configured live overlay gap;
  kind/proof/window validation failures remain poll errors with detailed bounded
  logs; and
- `bloar_follow_synced_to_floor_lag{head}` exposes a writer regression window.

Recommended local alerts include: process absent; readiness false past the
expected initial-sync window; no successful source polls; a pending generation
older than the expected pin/GC window; cleanup debt which survives several
polls; failed retained-pin audits or a `kubo_replica` readiness gate which turns
false; a `kubo_runtime` readiness regression; transition or announcement errors; a stale last-announcement timestamp;
and `head_synced_to` remaining flat while an independently observed source
advances.

Also scrape Kubo. The replica intentionally does not invent or proxy repository
size, storage maximum, free-space, GC wait, provide-queue, connection, routing,
or Bitswap metrics from the RPC subset it owns: Kubo is the authority for those
facts. Neither metric endpoint reports to a central Bloar service; the operator
chooses where, or whether, to collect it.

## Migration

### From manual pins on an existing Kubo

Leave the manual pins in place, configure a new replica ID and state path, run
`-check`, then start the controller. Existing local blocks make the recursive
generation pin cheaper, while the manual pins remain unrelated and untouched.
Only remove manual pins after the replica is ready and the reserved generation
pin has been verified through status and Kubo.

### From an embedded Bloar follower

There is no in-place backend flip. Start the standalone Kubo replica as a new
follower with its own state path, let it reach readiness, and keep the embedded
node available until Kubo has a complete generation. The two repositories may
temporarily duplicate data. If the Kubo replica's gateway is disabled, preserve
a `bloard` reader. If it is enabled, prove exact beacon/Nitro behavior against
the Kubo-local endpoint—including source-offline, restart, missing-block, and
GC cases—before moving traffic; it still does not replace any writer or
indexer role.

Do not copy embedded Pebble state into the replica directory. A fresh
controller establishes its own authenticated replay floors and pin-ownership
ledger.

### Backup and restore

Back up the controller state and Kubo repository as one recovery point. The
safest portable sequence is:

1. Stop `bloar-kubo-replica` so its ownership ledger and replay floors stop
   changing.
2. Stop Kubo, or use a Kubo-supported snapshot mechanism which guarantees a
   consistent repository image.
3. Snapshot both the controller state directory and Kubo repository before
   either service restarts.
4. Start Kubo, verify its repository, then start the replica.

Restore both sides from the same paired capture, start Kubo first, and restore
the controller state before starting the replica against those pins. A
pin/repository image paired with an older or missing ownership ledger can fail
closed by design; never reconstruct authority from pin names alone.

## Rollback and removal

Rollback is deliberately recoverable:

1. Stop `bloar-kubo-replica`.
2. Preserve its state directory and leave its generation pins in place.
3. Restore or restart the prior follower/read service if one existed.
4. Revert the node-wide Kubo provide settings only if no remaining workload
   depends on them, then restart Kubo.

Stopping the controller does not unpin data. Safe over-retention is the correct
rollback default and allows the service to be restarted with the same state.
The bounded rendezvous direct pins also remain; leaving them during rollback is
safe and preserves discovery for a restarted replica.

Permanent removal needs an ownership audit. The current command has no
destructive uninstall subcommand. Keep the Pebble state. While the controller
is still running, capture `GET /replica/status` and inspect only the exact
reserved name:

```sh
ipfs pin ls --type=recursive --names --name 'bloar-replica/v1/archive-eu-1'
```

Kubo's name filter is substring-based. Do not pipe that output into `pin rm`, do
not use a prefix as proof of ownership, and do not remove borrowed or unrelated
pins. If the state and Kubo pins disagree, restore the state or investigate the
drift instead of guessing. Once every candidate has been proven
controller-owned, stop the controller, re-run the exact CID/name checks to rule
out a last-minute change, and only then remove those exact CIDs with ordinary
Kubo tooling. Run Kubo GC only under the operator's normal policy and delete the
controller state last. Never mutate a generation pin while the controller's
check/remove loop is live.

## Troubleshooting

### Live preflight fails

- **Unsupported version:** install a stable Kubo 0.42.x release. A newer minor
  line is not assumed compatible.
- **Missing capability:** the Kubo build or reverse proxy does not expose one of
  the checked replica RPCs/flags. Compare the proxy allowlist with the security
  section above.
- **`Provide.Enabled` false or strategy not exactly `roots`:** change the
  node-wide values, restart Kubo, and rerun `-check`.
- **401/403:** verify the reverse proxy and the credential file delivered to the
  service. The token file is read once at process start.
- **Kubo plain HTTP rejected:** use loopback, HTTPS, or explicitly acknowledge
  a trusted non-loopback network with `allow_insecure_http: true`.
- **Publication plain HTTP rejected:** use HTTPS, or a literal loopback address
  for a publisher on the same host.

### `/readyz` remains 503

Read its `waiting_on` list, then check logs and `/replica/status`.
`followed_head:<name>` usually means the first publication has not been
accepted, the named head is absent, or its full recursive pin has not finished.
Check source signature/network settings, Kubo connectivity and free space, pin
progress, and whether Kubo GC is active. A `kubo_replica` wait after a
previously healthy start means recovery, a transition, or the independent
retained-pin audit could not prove the active current or crash-pending anchor
is still the expected recursive pin (and, for an owned anchor, reserved name);
inspect its exact CID and ownership before changing it.

`kubo_runtime` means the stable 0.42.x capability profile or Peer ID no longer
matches the startup preflight. In runtime provider-policy-check mode it can
also mean `Provide.Enabled=true` or `Provide.Strategy=roots` drifted. External
mode deliberately cannot observe those config values; recheck them from the
native host after Kubo configuration changes. Restore the exact Kubo/proxy
contract and let the next audit recover readiness; do not restart repeatedly
to hide drift.

### A generation remains pending

Pending is a durable transition intent, not a claim that pinning has finished.
It is safe while the old generation remains current. Investigate Kubo pin/GC
contention, disk exhaustion, provider reachability, the configured
`pin_timeout`, and streamed-response limit errors. Do not remove the old
anchor. The next matching poll can retry an owned candidate whose interrupted
pin is absent. Restart accepts a still-pinned pending generation as protection
only after checking the exact recursive pin; if an already-committed follower
checkpoint needs a missing pending pin, restart fails closed instead.

### Ownership drift or an orphan reserved pin

Stop changing pins manually. Preserve both Pebble and Kubo state, compare the
exact reserved-name pins with the last known controller status, and restore the
matching controller backup. The controller intentionally will not adopt a
reserved-name pin merely because it exists.

### IPNS cannot resolve

Confirm Kubo is online in the DHT, the RPC proxy permits routing get, the direct
IPNS name is correct, and its raw record has not regressed below the durable
sequence floor. With DNSLink, also verify the one-hop delegation and signer.

### The archive is pinned but hard to discover

Confirm `Provide.Enabled=true`, `Provide.Strategy=roots`, recent successful
announcement metrics, public Kubo reachability, and useful peers. Remember that
`roots` intentionally does not advertise arbitrary descendants. Test from an
independent node by resolving an advertised root and fetching through the DAG;
a local `pin ls` proves retention, not network reachability.
