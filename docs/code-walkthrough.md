# Bloar code walkthrough

This guide is a newcomer-oriented map of the implementation. It explains how
the main processes and packages fit together, which component is trusted for
which claim, and where to start reading for a change.

It is descriptive rather than normative. Before changing an invariant, read
the corresponding section of the [specification](spec.md); before changing a
deployment, read the [operations guide](operations.md).

## The system in one page

Bloar is a long-lived, content-addressed archive for EIP-4844 blobs. Ethereum
consensus clients normally retain blob sidecars only for a bounded period.
Bloar preserves older blobs and exposes the small beacon API surface that a
rollup node such as Nitro needs:

```text
GET /{head}/eth/v1/beacon/blobs/{slot}
GET /{head}/eth/v1/beacon/genesis
GET /{head}/eth/v1/config/spec
```

There is deliberately no project-hosted public beacon API. A publication
authority signs an authenticated description of archive heads, and its public
edge serves immutable blocks over libp2p/Bitswap. Operators run a local
embedded follower or Kubo replica and point their consumers at that local read
API.

The complete data flow is:

```text
               trusted chain facts                       untrusted bytes
        +------------------------------+        +---------------------------+
        | beacon blocks / finality     |        | beacon blob endpoints     |
        | L1 posting transactions/logs |        | archives and blob mirrors |
        +---------------+--------------+        +-------------+-------------+
                        |                                     |
                        v                                     v
                  bloar-index  ---------------- validates KZG bindings
                        |
                        | authenticated blobs, refs, and complete generations
                        v
             private bloard writer
             +----------------------+       flatfs: immutable IPFS blocks
             | ingest + head engine |------ Pebble: selectors, pins, floors
             | signed publication   |
             +----------+-----------+
                        |
                        | already-signed document + IPNS record over AF_UNIX
                        v
              public bloar-edge
              +---------------------+
              | read-only blocks    |---- Bitswap ----+
              | DHT/IPNS/rendezvous |                 |
              +---------------------+                 v
                                               bloard follower
                                               or Kubo replica
                                                      |
                                                      | local beacon API
                                                      v
                                                 Nitro / other client
```

The split writer/edge deployment is the strongest form of this topology. The
code also supports a monolithic `bloard` with an embedded public libp2p host,
which is useful for followers and smaller deployments.

Three ideas make the rest of the repository easier to understand:

1. A **blob** has two names. Its CID hashes the raw 131,072-byte blob; its
   versioned hash derives from its KZG commitment. The index stores the explicit
   mapping because neither name can be derived from the other without the blob.
2. A **head** is a content-addressed index snapshot, not a server. Its root CID
   commits to coverage, directory pages, segments, versioned hashes, and blob
   CIDs.
3. A **publication document** is a signed claim about the current roots. It
   authenticates which roots to consider; CID verification and optional KZG
   verification still authenticate the blocks those roots reach.

For a visual introduction to the DAG, read
[data-structures.md](data-structures.md) before continuing.

## Commands and process roles

The repository builds several commands because the security and operational
boundaries are process boundaries, not merely Go interfaces.

| Command | Purpose | Main construction path |
| --- | --- | --- |
| `bloard` | Embedded archive daemon. It can write configured heads, follow remote heads, serve the beacon/Bloar APIs, retain blocks, and run GC/scrub. | [`cmd/bloard/main.go`](../cmd/bloard/main.go), [`cmd/bloard/serve.go`](../cmd/bloard/serve.go) |
| `bloar-index` | Stateless finalized beacon, chain-filter, and bounded unfinalized indexers. It writes through `bloard`'s authenticated API. | [`cmd/bloar-index/main.go`](../cmd/bloar-index/main.go), [`cmd/bloar-index/config.go`](../cmd/bloar-index/config.go) |
| `bloar-edge` | Public, non-authoritative libp2p process. It opens the writer's flatfs blocks read-only and accepts only already-signed publication bundles. | [`cmd/bloar-edge/main.go`](../cmd/bloar-edge/main.go) |
| `bloar-kubo-replica` | Controller that authenticates publications and owns exact generation pins in an operator-owned Kubo node. It can expose a read-only local beacon gateway. | [`cmd/bloar-kubo-replica/main.go`](../cmd/bloar-kubo-replica/main.go), [`replica/controller.go`](../replica/controller.go) |
| `bloar-swarm-inspect` | Bounded census/probe tool for measuring current and historical archive availability from one vantage. | [`cmd/bloar-swarm-inspect/main.go`](../cmd/bloar-swarm-inspect/main.go), [`census/census.go`](../census/census.go) |

The production image contains the ordinary commands; the public edge uses a
separate runtime target with no implicit writable volumes. See
[`deploy/Dockerfile`](../deploy/Dockerfile).

## The immutable archive DAG

### Wire types and canonical CIDs

The canonical DAG types live in [`schema/schema.go`](../schema/schema.go):

- `schema.Head` is the root of one physical head.
- `schema.DirNode` is one page in an implicit radix directory.
- `schema.Segment` covers one fixed power-of-two slot window.
- `schema.Row` contains the ordered blob references for one slot.
- `schema.RefEntry` binds a `VersionedHash` to a blob CID.

Encoding and decoding are centralized in
[`schema/encode.go`](../schema/encode.go) and
[`schema/decode.go`](../schema/decode.go). Both paths validate the same
invariants, reject unknown major versions, and use canonical DAG-CBOR. This is
what makes a logical object reproduce the same CID on every implementation.

Blob leaves are raw-codec IPFS blocks. A blob is exactly one block; there is no
UnixFS wrapper or chunk tree. Index and manifest objects are DAG-CBOR blocks
with real IPLD links.

Filtered chain heads have one additional content-addressed structure:
`schema.Manifest` in [`schema/manifest.go`](../schema/manifest.go). A manifest
records the complete L1 source schedule that defines a filter and links to its
predecessor. The manifest tip is published beside the head root rather than
linked from `schema.Head`, so retention treats it as a separate recursive pin.

### Loading, mutating, and reading a head

The in-memory engine is `archive.Head` in
[`archive/archive.go`](../archive/archive.go). `core.NodeStore[T]` in
[`core/core.go`](../core/core.go) is the typed boundary between Go objects,
canonical codecs, the decoded-node cache, and the blockstore.

The important operations are:

- `archive.New` creates an empty root with immutable head parameters.
- `archive.Load` reconstructs a reader from an existing root and validates its
  shape.
- `Head.ApplyRefs` in [`archive/apply.go`](../archive/apply.go) advances
  coverage, rewrites the open segment, seals completed windows, and rewrites
  only the rightmost directory spine.
- `Head.Lookup` and `Head.LookupVHs` in
  [`archive/lookup.go`](../archive/lookup.go) turn a slot into an arithmetic
  directory path, then select all entries or an exact requested hash list.
- `Head.Enumerate` in [`archive/enumerate.go`](../archive/enumerate.go)
  produces the bounded structural view used by retention and follower sync.

Mutation is copy-on-write. New descendants are written before the new root
becomes current. Old roots remain valid immutable snapshots and become
collectable only after no pin reaches them.

Coverage is part of the answer:

- below `origin_slot`: the head makes no claim;
- at or below `synced_to`: presence and absence are authoritative;
- above `synced_to`: the head has not answered yet.

This distinction later becomes HTTP 404 versus an empty 200 versus retryable
503. It is also why an indexer must advance coverage through blobless slots
instead of merely storing rows for slots which have blobs.

### Node-local state

`store.Open` in [`store/store.go`](../store/store.go) opens two coordinated but
different stores:

- flatfs contains immutable IPFS blocks, one file per multihash;
- Pebble contains small node-local state: the versioned-hash catalog, current
  root selectors, pin ledger, manifest tips, publication sequence/revision
  state, follower checkpoints, and replay floors.

`catalog.Catalog` in [`catalog/catalog.go`](../catalog/catalog.go) is the local
`versioned hash -> CID` lookup used while applying refs. `catalog.Ledger` in
[`catalog/ledger.go`](../catalog/ledger.go) is the only embedded pin database.

The blocks are sufficient to verify an already-known root, but they are not a
complete backup of node authority. Current selectors, IPNS sequence state,
follower replay floors, conflict evidence, and staging leases are not all
derivable by scanning the DAG. The precise backup contract is documented in
[operations.md](operations.md).

## Building archive heads

The indexers are intentionally separate from the archive daemon. They can
restart without owning private archive state: their resume point is the
archive's durable `synced_to`.

All indexers write through `index/archclient.Client` in
[`index/archclient/archclient.go`](../index/archclient/archclient.go). It
classifies transport/5xx failures separately from malformed requests and
conflicts, so retry policy cannot turn a fail-closed protocol refusal into an
infinite “healthy” loop.

### The `all` head

`index/beacon.Indexer` in [`index/beacon/beacon.go`](../index/beacon/beacon.go)
builds the identity-filter head containing every finalized blob.

In anchored mode, a trusted finalized checkpoint bounds the run and
slot-addressed beacon headers decide which commitments exist. Parent-root
continuity proves that an absent slot response belongs to the same canonical
chain; root-addressed optimistic ancestry is instead the unfinalized tracker's
job. Blob endpoints are only ordered byte sources. Returned bytes are accepted
only when their KZG commitments reproduce the block-derived versioned hashes.
A byte source's 404 is never evidence that a slot was empty.

Mirror mode instead reproduces another Bloar head's coverage decisions. It
proves deterministic replication and verifies every included blob, but it
inherits the source archive's omissions. That is a replication mode, not an
independent completeness audit.

The wire clients for blob/header sources are in
[`index/upstream/upstream.go`](../index/upstream/upstream.go). Every configured
source remains only a byte source: the trusted block feed and KZG bindings, not
the transport or provider, establish correctness.

### Filtered chain heads

`index/chain.Indexer` in [`index/chain/chain.go`](../index/chain/chain.go)
derives a head such as `arbitrum-one` from finalized L1 posting activity.
Sources can select inbox events or allowlisted blob transactions. The result is
an ordered, deduplicated set of versioned hashes per beacon slot.

A filtered head is an index over the same blob blocks, not another copy of the
blobs. When an `all` indexer is filling the same archive, the chain indexer can
wait for `all` coverage and post only refs. In standalone mode it fetches and
posts the selected blob bytes itself.

The source schedule is an append-only content-addressed manifest chain.
Conversion and upgrade validation live in
[`index/chain/manifest.go`](../index/chain/manifest.go). A schedule correction
for already-covered history requires truncate first, then a new manifest, then
resync; silently changing the configured filter would make the same head name
mean different histories.

### The bounded unfinalized head

`index/unfinalized.Tracker` in
[`index/unfinalized/tracker.go`](../index/unfinalized/tracker.go) maintains a
complete, replaceable generation near the optimistic beacon tip. Unlike a
finalized head, this physical head is allowed to move backward or replace rows,
but only under a higher signed publication revision and within a configured
window.

Each generation is anchored to:

- a trusted optimistic block/root snapshot;
- an authenticated finalized handoff head and frontier; and
- a hard maximum window size.

The tracker builds a candidate off to the side and commits it through the
generation compare-and-swap API. A source-root race, handoff change, or
incomplete candidate leaves the previously selected generation active.

## Ingest and writer-side publication

### Blobs first, refs second

`ingest.Ingester` in [`ingest/ingest.go`](../ingest/ingest.go) handles raw blob
uploads. It bounds the body, validates blob shape, recomputes KZG commitments,
writes the raw blocks, and updates the local versioned-hash catalog.

The mutation API is mounted by `server.Server`:

- route construction and public/private separation:
  [`server/server.go`](../server/server.go);
- authenticated blob, refs, manifest, truncate, and generation handlers:
  [`server/bloar.go`](../server/bloar.go);
- bearer authentication: [`server/auth.go`](../server/auth.go).

Blob upload and refs publication are separate requests. A durable staging pin
bridges the gap. `pinning.Staging` in
[`pinning/staging.go`](../pinning/staging.go) pins every accepted blob before
the upload returns success; after refs and the new root are durable, the
registry drops those temporary rows. Expired abandoned uploads are later
reclaimed by GC.

### The head registry is the publication boundary

`server.Heads` in [`server/heads.go`](../server/heads.go) is the process-wide
serving and publication registry. It distinguishes:

- heads this process writes;
- heads this process follows;
- finalized-monotonic heads;
- unfinalized-mutable heads; and
- local virtual live views, which are not physical published heads.

For a written mutation, the new immutable blocks and root selector become
durable before `Heads` rebuilds the signed publication document. For a follower
adoption, the authenticated checkpoint, compatibility mirrors, pin transition,
and visible registry generation are one ordered transition. Readers load an
immutable registry snapshot, so an in-flight response completes against the
generation it selected.

The signed wire contract is `server.Doc`, `server.Unsigned`, and
`server.HeadEntry` in
[`server/publication.go`](../server/publication.go). A document commits to the
network, archive identity, monotonic revision, physical head metadata, manifest
tips, mutable-generation proofs, and advertised libp2p addresses. The embedded
public key says who signed; local follower policy decides whether that key is
trusted.

Optional independent writers share an `archive_id` but use distinct stores,
keys, names, and endpoints. Followers compare finalized claims by
content-prefix and manifest ancestry, not by timestamps, arrival order, or
majority vote. The arbitration implementation is
[`follow/arbitration.go`](../follow/arbitration.go), with the operating model in
[multi-writer.md](multi-writer.md).

That arbitration is finalized-only. An `unfinalized-mutable` head is a complete
replaceable snapshot from exactly one configured authority; followers never
merge, vote on, or compare mutable generations from several writers. The
source-policy check is in
[`follow/source_config.go`](../follow/source_config.go).

## Publication and the public edge

### Three different identities

The split deployment keeps three identities conceptually separate:

| Identity | What it authenticates | Where its private key belongs |
| --- | --- | --- |
| Publication Ed25519 key | The semantic heads document | Private writer |
| IPNS key/name | The mutable name-to-document-CID record and sequence | Private writer |
| Public edge Peer ID | The libp2p endpoint which serves blocks and participates in discovery | Public edge |

Trusting one does not imply trusting the others. In particular, a Peer ID is
not authority to change archive roots, and a matching `archive_id` is not
authority to introduce a new publication signer.

### Writer side

`cmd/bloard` assembles publishing in
[`cmd/bloard/p2p.go`](../cmd/bloard/p2p.go). In split `required` mode it does
not create a public libp2p host. It:

1. signs the publication document;
2. uses the private IPNS key to construct an IPNS record naming the exact raw
   document CID; and
3. sends both immutable byte strings through `edge.ClientPolicy` over a bounded
   Unix-socket request.

The client protocol is implemented in
[`p2p/edge/client.go`](../p2p/edge/client.go). Its transaction, request, and
server-write deadlines are strictly ordered. Once a request crosses the control
socket, ordinary caller cancellation does not manufacture an error while the
edge may still complete the transaction.

In a monolithic deployment, `p2p.Publisher` in
[`p2p/ipns.go`](../p2p/ipns.go) performs the same high-level transaction through
a local publication policy.

### Edge side

`bloar-edge` opens only the writer's flatfs block directory through
`store.OpenReadOnlyBlocks` in
[`store/readonly.go`](../store/readonly.go). The Go wrapper rejects
`Put`, `PutMany`, and `DeleteBlock`; deployment should also mount that directory
read-only. The edge receives neither archive mutation token nor publication
signing key.

`edge.Sink` in [`p2p/edge/sink.go`](../p2p/edge/sink.go) accepts one bounded
control operation. Before touching network state it verifies:

- the IPNS record signature, name, sequence, and exact document CID;
- the publication document signature and contract;
- the configured document signer, network, and `archive_id`;
- the expected edge Peer ID in the advertised multiaddrs; and
- the allowlisted head names, kinds, handoff names, and required presence.

The commit order is load-bearing:

1. stage the verified raw document for serving;
2. notify Bitswap of the new block;
3. synchronously provide the document CID;
4. durably persist the exact document/record bundle when it changed;
5. put the IPNS record; and
6. only after success, replace auxiliary exact-pointer hints.

The edge-owned pointer schedule is implemented by
[`p2p/edge/pointers.go`](../p2p/edge/pointers.go), with its ownership and
failure-ordering contract exercised in
[`p2p/edge/pointers_test.go`](../p2p/edge/pointers_test.go).

Provider-before-record means a newly resolved IPNS value does not name a
document for which the publishing edge has not established discovery. Durable
state before `PutValue` means an edge restart can retry the exact signed bundle
after a DHT failure; persistence alone is not reported as a successful
publication.

## libp2p, Bitswap, and discovery

`p2p.Host` in [`p2p/p2p.go`](../p2p/p2p.go) owns the libp2p identity,
listeners, connection manager, resource manager, relay/DCUtR options, static
peers, and advertised addresses. Public inbound reachability is operationally
valuable: a follower which serves retained blocks is useful to strangers only
when peers can dial it directly or establish an alternate direct path.
Embedded hosts default to listening, and the follower quickstart publishes its
TCP swarm port by default. When the node is behind a firewall or router,
operators should allow and forward that TCP port; the exact quickstart behavior
is documented in
[`deploy/quickstart/follower/README.md`](../deploy/quickstart/follower/README.md)
and [`deploy/quickstart/follower/compose.yaml`](../deploy/quickstart/follower/compose.yaml).

Relay reservations and limited relay circuits are an introduction and DCUtR
control plane, not a blob-data fallback. They exist to help peers establish an
ordinary direct connection; Bitswap does not treat a limited relay circuit as
the transport for an encoded blob.

`p2p.Exchange` in [`p2p/bitswap.go`](../p2p/bitswap.go) is both the bounded
Bitswap server and the follower fetch source. It passes queue, outstanding-byte,
worker, and CID-size limits explicitly instead of inheriting library defaults.
A fetching blockstore stores a successfully fetched, CID-verified block in the
local blockstore before returning it.

`p2p.NewDHT` in [`p2p/dht.go`](../p2p/dht.go) joins the public Amino DHT when
configured, but the DHT is deliberately not installed as generic Bitswap
content routing. It is used for three narrow jobs:

- resolving and publishing IPNS records;
- discovering peers at a deterministic `(network, head)` rendezvous CID, from
  [`p2p/rendezvous.go`](../p2p/rendezvous.go); and
- finding providers for authenticated, semantically typed current pointers.

Rendezvous provider results are only untrusted addresses to try. They do not
select a head or authorize a publication. The bounded service is in
[`p2p/rendezvous_service.go`](../p2p/rendezvous_service.go).

Exact-pointer hints in
[`p2p/pointerhint/pointers.go`](../p2p/pointerhint/pointers.go) cover only
authenticated current root, manifest, and publication-document CIDs. The
provider and finder are
[`p2p/pointerhint/provider.go`](../p2p/pointerhint/provider.go) and
[`p2p/pointerhint/finder.go`](../p2p/pointerhint/finder.go). These hints improve
discovery; they are not a replacement for the synchronous document
provide-before-IPNS transaction, and a stale provider record cannot make stale
bytes pass a CID or publication check.

## Following a publication

`follow.Follower` in [`follow/follow.go`](../follow/follow.go) is a stateful
admission engine around otherwise immutable blocks.

### Resolve and authenticate

The resolution path is [`follow/resolve.go`](../follow/resolve.go). A follower
may use HTTPS plus one name authority:

- direct IPNS pins the name;
- one-hop DNSLink selects exactly one IPNS name;
- HTTPS is a redundant document channel, not a DNSLink signer-rotation
  authority.

For IPNS, the follower verifies the record against the name, obtains the exact
raw document CID, fetches that block with a fresh bounded timeout, independently
rehashes its bytes, and then verifies the publication signature and contract.
An explicit configured publication key is a signer pin. DNSLink without such a
pin may delegate a signer only through the complete
`DNSLink -> signed IPNS -> exact CID -> signed document` chain.

The follower persists independent floors for IPNS sequence, publication
revision/digest, per-head coverage, manifest ancestry, and any delegated name
and signer. A fresh outer record cannot hide an inner rollback. Same-revision
different-content publication is equivocation, not “pick the latest timestamp.”

Follow profiles are local, versioned configuration bundles which supply this
network/trust/head policy. They are expanded and then passed through the same
strict config validation as handwritten YAML. The implementation is
[`cmd/bloard/profiles.go`](../cmd/bloard/profiles.go); the operator-facing model
is [follow-profiles.md](follow-profiles.md).

### Prove, adopt, fetch, and retain

Before adoption, the follower:

1. checks the signed entry's immutable parameters and head kind;
2. loads and validates the claimed root;
3. proves manifest ancestry and mutable handoff constraints where applicable;
4. applies replay/equivocation floors;
5. protects the retention closure against a concurrent GC; and
6. durably checkpoints the complete admitted generation before exposing it.

The polling and admission machinery spans
[`follow/follow.go`](../follow/follow.go),
[`follow/source_admission.go`](../follow/source_admission.go), and
[`follow/source_poll.go`](../follow/source_poll.go). Multi-writer source-set
selection adds the partial-order proof in
[`follow/arbitration.go`](../follow/arbitration.go).

The daemon does not make that admission cadence wait for retained replication.
[`follow/run.go`](../follow/run.go) drives two bounded phases:

- the poll path resolves, authenticates, and atomically admits a complete
  generation immediately and at each configured poll boundary; and
- one background worker walks every configured head's current retained
  closure, with one buffered dirty bit rather than a revision queue.

If publications arrive while a closure walk is running, their wakeups
coalesce. The worker is not multiplied and historical roots are not queued.
When the current walk returns, one pending wake makes it snapshot the latest
admitted generation. The completion compare-and-swap in
[`follow/sync.go`](../follow/sync.go) prevents an old walk from stamping a
newer root or manifest tip complete. Content-addressed blocks fetched by that
old walk remain useful, but current-generation state wins.

`follow/sync.go` walks the snapshot according to its local retention policy.
Missing blocks are fetched over Bitswap and staged until normal head pins
protect them. Every fetched block must reproduce its CID. With
`follow.verify: full`, segment entries additionally cause KZG recomputation of
each blob's claimed versioned hash. A mismatch quarantines the head because it
is evidence against the writer's semantic claim, not a transient network miss.
Admission still owns all safety-critical structural, replay, GC-protection,
checkpoint, retention, and atomic-registry work; only the post-admission
closure fetch is scheduled independently.

Direct library callers retain a synchronous operation:
`follow.Follower.Poll` performs one admission attempt and one closure pass
before returning. It shares the worker's single sync permit, so a direct call
cannot accidentally overlap the daemon's closure walk.

The split is visible in metrics:

- `bloar_follow_admission_duration_seconds` and
  `bloar_follow_admission_last_success_timestamp_seconds` measure publication
  freshness without charging retained-replication time;
- `bloar_follow_sync_duration_seconds{head,outcome}` and
  `bloar_follow_sync_last_success_timestamp_seconds{head}` describe closure
  work, with `completed`, `noop`, `superseded`, and `error` outcomes; and
- `bloar_follow_sync_active` and `bloar_follow_sync_coalesced_total` show the
  one-worker bound and revisions folded into its pending dirty bit.

Only a compare-and-swap-stamped `completed` pass advances sync last-success.
A no-op or superseded pass is useful control flow, not evidence that the
current generation's retained closure is complete.

Kubo-backed external-retention followers are the deliberate exception to this
phase split. Their external store must prepare the retained generation before
admission exposes it, so that cost remains in admission telemetry and the
background worker normally reports no-op rather than a sync completion.

The key availability behavior is **last-good service**. A bad signature,
rollback, unavailable proof block, incomplete fetch, conflict, or mutable
generation race does not partially replace the current registry. The follower
keeps the last durably admitted coherent generation and reports the new
condition. Some failures are retryable; cryptographic or contract violations
are not silently retried as availability problems.

On an ordinary read, an index hit whose blob is outside local retention can
trigger one bounded on-demand Bitswap fetch. Failure is a retryable HTTP 503,
not a false 404.

## Retention, GC, and integrity

`pinning.Policy` in [`pinning/policy.go`](../pinning/policy.go) compiles each
head into a desired pin set:

- `full` recursively retains the current root;
- `window` retains the complete index and recursively retains only segments in
  the trailing window;
- `none` retains the complete index but no blob descendants.

`pinning.Reconciler` in
[`pinning/reconcile.go`](../pinning/reconcile.go) makes the durable ledger match
that desired set. Add-before-remove ordering means a crash may retain too much
but does not create an intentional unpinned gap.

`pinning.GC` in [`pinning/gc.go`](../pinning/gc.go) is online mark-and-sweep.
At a short gate-protected cut it reconciles pins, expires staging rows,
snapshots pin groups, and starts a blockstore protection epoch. The expensive
mark and sweep then run alongside application work. Blocks reachable from the
snapshot or touched by application reads/writes during the epoch survive.

The same component runs a separate integrity scrub which rehashes block bytes
without deleting or repairing them. A writer treats a missing/corrupt retained
block as a hard local failure. A follower may refetch a missing block which its
policy says should exist, but does not quietly repair a semantic KZG mismatch.

The gate is implemented in the library, not as HTTP middleware:
[`pinning/gate.go`](../pinning/gate.go), `server.Heads`, `ingest.Ingester`, and
the read path all participate. This keeps in-process stacks and tests subject
to the same crash/GC ordering as the daemon.

## Serving HTTP and the live view

### Public and mutation surfaces

`server.Server` in [`server/server.go`](../server/server.go) mounts:

- the public per-head beacon endpoints;
- public publication/head metadata;
- authenticated mutation endpoints on ordinary writer/follower daemons; or
- only GET routes when `ReadOnly` is selected, as in the Kubo gateway.

The blobs handler in [`server/beacon.go`](../server/beacon.go) preserves
requested versioned-hash order, bounds query count and response memory, and
distinguishes:

| State | Response meaning |
| --- | --- |
| slot below origin or unknown physical head | 404: this head does not cover the requested namespace |
| covered slot with no matching blobs | 200 with an empty `data` array |
| covered slot with blobs | 200 in canonical/requested order |
| head or blob not yet available | 503 with retry guidance |
| quarantined or incoherent live generation | 503 without pretending absence |

Socket deadlines, connection limits, weighted global/per-client admission, and
bounded response reservations are part of the server construction, not assumed
to exist at a reverse proxy. The public admission implementation is
[`server/public_read_limiter.go`](../server/public_read_limiter.go).

### Physical heads versus a virtual `live` view

A live view is local serving policy, not another signed archive head.
`server.LiveHead` pairs:

- a `finalized-monotonic` physical head; and
- an `unfinalized-mutable` physical head from the same authenticated
  publication generation.

For a slot at or below finalized coverage, finalized presence and absence win.
Above it, the mutable generation is selectable only when its durable window and
handoff proof cover the exact slot. A gap returns 503 and keeps the prior
generation; it never falls through to an optimistic “not found.”

For a chain-filtered finalized head over a global mutable head,
`RequireVersionedHashes` makes the provisional half an exact-hash overlay.
Clients can retrieve hashes they already know, but cannot enumerate unrelated
global provisional blobs through the filtered view.

Selection and response semantics are in
[`server/beacon.go`](../server/beacon.go), with registry proof handling in
[`server/heads.go`](../server/heads.go). The focused behavior tests are
[`server/live_test.go`](../server/live_test.go) and the real Nitro cases in
[`conformance/live_test.go`](../conformance/live_test.go).

## The Kubo replica path

The standalone replica reuses an existing Kubo repository instead of opening
Bloar's embedded flatfs store. Responsibilities remain explicit:

- Kubo owns block bytes, Peer ID, Bitswap, DHT, pin database, and GC.
- `bloar-kubo-replica` authenticates publication documents, enforces replay
  floors, constructs exact selected generations, and owns one named recursive
  generation anchor.
- The optional Bloar gateway reads only the committed generation through
  Kubo-local `offline=true` block access.

The HTTP/RPC client begins in [`kubo/client.go`](../kubo/client.go), alongside
constrained blockstore and routing adapters. `kubo.ReplicaBlockstore` in
[`kubo/replica_blockstore.go`](../kubo/replica_blockstore.go) allows the
fetch/put operations needed for replication while refusing delete and
full-repository enumeration.

`replica.Controller` in
[`replica/controller.go`](../replica/controller.go) implements the prepare,
pin, commit, audit, recovery, and cleanup transaction.
[`replica/generation.go`](../replica/generation.go) defines the
content-addressed generation anchor. The optional read gateway is assembled in
[`cmd/bloar-kubo-replica/gateway.go`](../cmd/bloar-kubo-replica/gateway.go).

The controller does not run Kubo GC, delete unrelated data, or turn a missing
local gateway block into a network fetch. It fails closed and leaves the prior
committed generation selected. Read [kubo-replica.md](kubo-replica.md) before
changing this boundary; shared Kubo provider policy and GC are node-wide
operational choices.

## Configuration

Configuration is role-defining and security-sensitive. The main structures and
validation are:

- embedded writer/follower: [`cmd/bloard/config.go`](../cmd/bloard/config.go);
- indexers: [`cmd/bloar-index/config.go`](../cmd/bloar-index/config.go);
- edge: config and validation in
  [`cmd/bloar-edge/main.go`](../cmd/bloar-edge/main.go);
- Kubo replica: [`cmd/bloar-kubo-replica/config.go`](../cmd/bloar-kubo-replica/config.go);

YAML decoding is strict: unknown keys, mutually exclusive authority modes,
impossible resource budgets, mismatched head kinds, and unsafe missing
credentials should fail before a store or listener opens. Follow-profile
expansion happens at the syntax-tree layer first, then the resulting config
passes through the same strict decoder and semantic checks.

Useful reference configurations are:

- [`deploy/examples/writer.yaml`](../deploy/examples/writer.yaml);
- [`deploy/examples/follower.yaml`](../deploy/examples/follower.yaml);
- [`deploy/examples/beacon-all.yaml`](../deploy/examples/beacon-all.yaml);
- [`deploy/examples/arbitrum-one.yaml`](../deploy/examples/arbitrum-one.yaml);
- [`deploy/examples/unfinalized.yaml`](../deploy/examples/unfinalized.yaml);
- [`deploy/examples/kubo-replica.yaml`](../deploy/examples/kubo-replica.yaml);
- the supported embedded-follower quickstart
  [`deploy/quickstart/follower/bloard.yaml`](../deploy/quickstart/follower/bloard.yaml).

Use `bloard config-inspect` to expand a follow profile and show the validated
effective configuration without opening the store or reading secret contents.
The code path is in [`cmd/bloard/main.go`](../cmd/bloard/main.go) and
[`cmd/bloard/profiles.go`](../cmd/bloard/profiles.go).

## Metrics, liveness, and readiness

The Prometheus registry is explicit rather than global:
[`metrics/metrics.go`](../metrics/metrics.go). Packages accept a
`*metrics.Metrics` and do nothing when it is nil. The major families cover head
coverage, reads, ingest, index progress, publication stages, follower
admission, pin reconciliation, GC/scrub, Bitswap, DHT/rendezvous, pointer
hints, and resource-manager state.

`metrics.Health` in [`metrics/health.go`](../metrics/health.go) deliberately
separates:

- `/healthz`: the process can answer HTTP;
- `/readyz`: every configured serving correctness gate is met.

For `bloard`, readiness includes the store, written-head registration, initial
pin reconciliation, the GC scheduler, and one gate per configured followed
head. A followed head that is not durably resumed/adopted or is quarantined
must not look ready merely because the process is alive. The daemon wiring is
in [`cmd/bloard/metrics.go`](../cmd/bloard/metrics.go).

Metrics have their own failure boundary: an expression over a missing series is
usually empty, which is indistinguishable from “no violation” unless monitoring
also checks scrape health and required-series presence. Tests should assert
that expected families appear, not infer instrumentation from the absence of an
alert.

## Security and failure boundaries

These distinctions recur throughout the code. Keeping them separate prevents
local success from being mistaken for a stronger system claim.

| Boundary | What success proves | What it does not prove |
| --- | --- | --- |
| Raw block CID | The bytes match the requested content address. | A `versioned hash -> CID` binding is honest. |
| KZG/versioned-hash check | The blob bytes match the chain commitment named by an index row. | The writer included every blob which should exist. |
| Publication document signature | The configured signer made the semantic claim. | The signer is authorized unless local policy says so; the claim matches external chain truth. |
| IPNS signature and sequence | The name owner selected an exact document CID without replaying its sequence. | The document signer is trusted, or the document contract is valid. |
| `archive_id` equality | Claims are in the same logical comparison namespace. | An unknown signer is authorized. |
| Rendezvous/provider result | A peer advertised for a bounded discovery key. | The peer has useful blocks or an authenticated current head. |
| Finalized head | Presence and absence through `synced_to` are monotonic archive claims. | Slots beyond coverage are absent. |
| Mutable generation | One signed, bounded optimistic snapshot passed its handoff proof. | It is final, monotonic, or safe to cache. |
| Follower poll failure | The new candidate was not admitted. | The last admitted generation stopped being usable. |
| Edge readiness | A complete provide-before-record transaction was restored or accepted. | The private writer, every archive block, or an HTTP follower is healthy. |
| Kubo generation pin | Kubo currently protects the exact selected DAG anchor. | Kubo's unrelated content or global provider/GC policy is under Bloar control. |
| Process liveness | The HTTP process is responding. | Its store, followed heads, publication, or serving generation is ready. |
| A quiet metric selector | No matching violating sample was returned. | The target and required metric family exist. |

Other important fail-closed choices include:

- strict config rejection rather than silently ignored YAML;
- immutable head parameters for the life of a head name;
- one mutation writer per physical head;
- durable root/checkpoint before publication or visibility;
- last-good follower service on candidate failure;
- 503 for uncertainty, never false absence;
- add-before-remove pin transitions and staging across unreferenced windows;
- edge blockstore and Kubo gateway capabilities narrower than their backing
  stores; and
- bounded network inputs, queues, walks, response memory, and deadlines at the
  component which owns them.

## How the tests are organized

The repository tests invariants at several layers:

1. **Schema and CID stability.** Golden vectors and strict decode cases live in
   [`schema/golden_test.go`](../schema/golden_test.go) and
   [`schema/decode_test.go`](../schema/decode_test.go).
2. **Archive algorithms.** Lookup, sealing, structural sharing, random mutation,
   and crash ordering are covered by
   [`archive/property_test.go`](../archive/property_test.go),
   [`archive/lookup_test.go`](../archive/lookup_test.go), and
   [`archive/crash_test.go`](../archive/crash_test.go).
3. **Retention and concurrent GC.** The mark/sweep, epoch, staging, and
   reconciliation suites accompany [`pinning/gc.go`](../pinning/gc.go) and
   [`pinning/reconcile.go`](../pinning/reconcile.go), especially
   [`pinning/gc_internal_test.go`](../pinning/gc_internal_test.go) and
   [`pinning/reconcile_test.go`](../pinning/reconcile_test.go).
4. **Publication and p2p.** IPNS policy, split-edge validation/ordering, Bitswap
   bounds, and pointer discovery are exercised in
   [`p2p/ipns_test.go`](../p2p/ipns_test.go),
   [`p2p/edge/sink_test.go`](../p2p/edge/sink_test.go), and
   [`p2p/pointerhint/finder_integration_test.go`](../p2p/pointerhint/finder_integration_test.go).
5. **Follower admission.** Replay, crash, GC, manifest, mutable-generation, and
   multi-writer cases accompany [`follow/follow.go`](../follow/follow.go). Good
   starting points are
   [`follow/verified_document_test.go`](../follow/verified_document_test.go),
   [`follow/document_atomic_gc_test.go`](../follow/document_atomic_gc_test.go),
   and [`follow/arbitration_test.go`](../follow/arbitration_test.go).
6. **HTTP behavior.** Focused API, live-view, read-only, and publication tests
   accompany [`server/server.go`](../server/server.go), including
   [`server/beacon_test.go`](../server/beacon_test.go),
   [`server/live_test.go`](../server/live_test.go), and
   [`server/publication_test.go`](../server/publication_test.go).
7. **End to end.** [`e2e/stack_test.go`](../e2e/stack_test.go) builds a real
   store/server/indexer stack against synthetic upstreams.
8. **Consumer conformance.** The separate `conformance` module imports Nitro's
   real client dependency graph. See
   [`conformance/nitro_test.go`](../conformance/nitro_test.go),
   [`conformance/follower_test.go`](../conformance/follower_test.go),
   [`conformance/live_test.go`](../conformance/live_test.go), and
   [`conformance/kubo_replica_gateway_test.go`](../conformance/kubo_replica_gateway_test.go).
9. **Configuration and deployment contracts.** Command config tests sit beside
   each `cmd`; quickstart confinement is checked by
   [`deploy/quickstart/compose_security_test.go`](../deploy/quickstart/compose_security_test.go).

The ordinary workflow is defined in [`Makefile`](../Makefile):

```text
make build
make test
make lint
make conformance
```

The conformance suite is separate because Nitro brings its own large dependency
graph and replace directives.

## Suggested reading paths

### To understand the whole system

1. [README.md](../README.md)
2. [data-structures.md](data-structures.md)
3. [`schema/schema.go`](../schema/schema.go)
4. [`archive/apply.go`](../archive/apply.go) and
   [`archive/lookup.go`](../archive/lookup.go)
5. [`server/heads.go`](../server/heads.go) and
   [`server/beacon.go`](../server/beacon.go)
6. [`cmd/bloard/serve.go`](../cmd/bloard/serve.go)
7. [spec.md](spec.md), using the implementation above as a map

### To change ingest or index construction

1. [`index/beacon/beacon.go`](../index/beacon/beacon.go) or
   [`index/chain/chain.go`](../index/chain/chain.go)
2. [`index/archclient/archclient.go`](../index/archclient/archclient.go)
3. [`ingest/ingest.go`](../ingest/ingest.go)
4. [`server/bloar.go`](../server/bloar.go)
5. [`archive/apply.go`](../archive/apply.go)
6. the relevant `e2e` and archive property tests

### To change publication or public networking

1. [`server/publication.go`](../server/publication.go)
2. [`p2p/ipns.go`](../p2p/ipns.go)
3. [`p2p/edge/client.go`](../p2p/edge/client.go)
4. [`p2p/edge/sink.go`](../p2p/edge/sink.go)
5. [`cmd/bloar-edge/main.go`](../cmd/bloar-edge/main.go)
6. [`p2p/p2p.go`](../p2p/p2p.go),
   [`p2p/bitswap.go`](../p2p/bitswap.go), and the pointer/rendezvous packages

### To change follower trust or adoption

1. [follow-profiles.md](follow-profiles.md)
2. [`follow/resolve.go`](../follow/resolve.go)
3. [`follow/follow.go`](../follow/follow.go)
4. [`follow/source_admission.go`](../follow/source_admission.go)
5. [`follow/sync.go`](../follow/sync.go)
6. [`server/heads.go`](../server/heads.go)
7. [`follow/verified_document_test.go`](../follow/verified_document_test.go),
   then the corresponding atomicity, arbitration, and GC tests beside it

### To change retention or storage

1. [operations.md](operations.md), especially the on-disk-state and backup
   sections
2. [`store/store.go`](../store/store.go)
3. [`catalog/ledger.go`](../catalog/ledger.go)
4. [`pinning/policy.go`](../pinning/policy.go)
5. [`pinning/reconcile.go`](../pinning/reconcile.go)
6. [`pinning/gc.go`](../pinning/gc.go)
7. the epoch, crash, staging, and shared-block tests

### To change live serving or Kubo integration

For live serving, start with [`server/beacon.go`](../server/beacon.go),
[`server/heads.go`](../server/heads.go), and
[`index/unfinalized/tracker.go`](../index/unfinalized/tracker.go), then run the
server and Nitro live conformance tests.

For Kubo, start with [kubo-replica.md](kubo-replica.md),
[`cmd/bloar-kubo-replica/main.go`](../cmd/bloar-kubo-replica/main.go),
[`replica/controller.go`](../replica/controller.go),
[`kubo/client.go`](../kubo/client.go), and
[`kubo/replica_blockstore.go`](../kubo/replica_blockstore.go). Keep the
distinction between controller metadata, generation pin ownership, Kubo's
node-wide policy, and the read-only gateway explicit in every change.
