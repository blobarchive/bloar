# bloar: Blob Archive Design

`bloar` is a long-term archive for EIP-4844 blobs, built on IPFS, designed to
serve Arbitrum Nitro nodes (and other rollup nodes) that need historical blobs
long after the beacon chain has pruned them.

This document explains the design and the reasoning behind it. The normative,
implementable details live in [spec.md](spec.md).

## Background

### The problem

Rollups post their batch data to Ethereum as EIP-4844 blobs. Consensus clients
only retain blobs for 4096 epochs (~18 days); after that, the standard
`/eth/v1/beacon/blobs/{slot}` endpoint returns nothing. Any Arbitrum node that
syncs from genesis (or from a snapshot older than the retention window) must
fetch historical blobs from an archive service. Nitro's own error message for
expired blobs tells operators to go find one.

### How Nitro consumes blobs (facts from the nitro codebase)

These are the load-bearing facts, verified against `util/headerreader/blob_client.go`,
`daprovider/reader.go`, and `arbstate/inbox.go`:

- Nitro fetches blobs from `GET /eth/v1/beacon/blobs/{slot}?versioned_hashes=0x...`.
  The old `blob_sidecars` endpoint is deprecated and effectively dead: the new
  endpoint strictly returns blob data, omitting KZG commitments and proofs.
- The response is `{"data": ["0x<blob hex>", ...]}`. Blobs must be returned in
  the same order as the requested versioned hashes, and the count must match
  exactly, or the client errors.
- Nitro verifies blobs by recomputing the KZG commitment from the raw blob
  bytes and comparing the derived versioned hash against the hashes it already
  knows from the sequencer inbox message (`sequencerMsg[41:]`, packed 32-byte
  hashes). Blobs are self-certifying; no verification material needs to be
  stored or served.
- The slot is computed client-side from the L1 block timestamp:
  `(header.Time - genesisTime) / secondsPerSlot`. Queries are by slot only.
- At startup, Nitro's `BlobClient.Initialize()` calls `/eth/v1/beacon/genesis`
  and `/eth/v1/config/spec`. An archive must serve these two (static) endpoints
  to be usable as a `beacon-url` or `secondary-beacon-url`.
- Batch recovery during sync is serial: one blob fetch per batch, in ascending
  slot order. Per-request latency directly gates sync speed; since archived
  content is immutable, aggressive HTTP caching is the mitigation.
- Nitro supports a primary and secondary beacon URL with failover, and an
  `Authorization` header. An archive can therefore slot in behind a real
  beacon node with zero Nitro changes.

### Access pattern

- **Syncing node**: strictly ascending slot order, one request per batch, each
  request naming the exact versioned hashes it wants. This is the pattern the
  archive must serve well.
- **Node starting from a snapshot**: same, but starting at an arbitrary
  historical slot. Random access into history must also be cheap.
- **Steady-state node**: reads near head only; normally served by a real
  beacon node, with the archive as fallback.

## Goals

1. Serve `GET /{head}/eth/v1/beacon/blobs/{slot}` (plus `genesis` and `spec`)
   as a drop-in Nitro beacon URL, per head.
2. Archive blobs forever, or per configurable retention policy.
3. **Multiple heads over shared data**: one head indexing ALL blobs, plus
   filtered heads per chain (arbitrum-one, ...), all referencing the same
   underlying blob objects. Adding a view of the data costs index space only.
4. Per-head retention/pinning policies (e.g. pin everything from ALL; pin
   arbitrum-one forever; pin some other head for a sliding 30-day window).
5. Mirrorable: anyone can replicate a head by fetching its root and pinning
   recursively, or by walking the HTTP API. Verifiable: heads are
   content-addressed and deterministically derivable from public chain data.
6. Boring operations: idempotent ingestion, stateless indexers, restart-safe
   everything.

### Non-goals

- Serving the deprecated `blob_sidecars` format, or storing KZG commitments
  or proofs. (Deprecated upstream; clients broke it; blobs are
  self-certifying. Commitments/proofs are pure functions of the blob if ever
  needed.)
- Treating unfinalized data as finalized archive history. A separate bounded,
  revisioned head may serve the optimistic tip, but the ALL head remains
  finalized and monotonic.
- General-purpose beacon API coverage. We serve exactly what blob consumers
  need.

## Design summary

```
                       parent chain (L1)                 beacon chain
                             |                                |
              +--------------+---------------+               |
              |                              |               |
   +-------------------+          +-------------------+      |
   | chain indexer     |          | beacon indexer    |      |
   | (per chain head)  |          | (ALL head)        |      |
   | SequencerInbox -> |          | every finalized   |      |
   | (slot, [vh]) refs |          | slot -> refs      |      |
   +---------+---------+          +---------+---------+      |
             |                              |                |
             |     refs                     |    refs        |
             |          +----------+        |                |
             +--------->|          |<-------+                |
                        |  bloard  |<---- blobs (fetcher) ---+
       reads  --------->|  daemon  |
   (nitro nodes,        |          |----> IPFS blockstore (embedded boxo)
    mirrors)            +----------+      pins = retention policy
```

One daemon (`bloard`) owns an embedded IPFS blockstore and exposes:

- the beacon-compatible read API, per head, at `/{head}/eth/v1/beacon/...`
- an authenticated ingest API: `put blobs` (raw bytes) and `put refs`
  (per-head `(slot, [versioned_hash])` rows)
- head publication: a signed JSON document mapping head name -> root CID

Indexer processes walk their source chain and push refs; a fetcher supplies
blob bytes. **Every head is a filter; ALL is the identity filter.** The same
machinery builds every head.

A deployment has exactly **one writer** bloard per head; any number of
**follower** bloards replicate published heads over IPFS and serve reads
(decision 7).

## Key decisions

### 1. IPFS as the storage layer

Blobs and index nodes are stored as IPFS blocks in an embedded (boxo)
blockstore. Rationale:

- Content addressing is native. Blob deduplication across heads is automatic
  and exact.
- Pinning + epoch mark-and-sweep GC replaces the reference-counting layer from
  the earlier CAS prototype entirely (see "Relationship to the arbloar
  prototype" below). Retention policy compiles to pin sets. A short epoch cut
  gives the collector a stable starting claim without stopping writers for the
  mark and sweep.
- Mirroring is native: pin a head root recursively and you have a full,
  self-verifying replica.

**Blobs are single raw blocks.** A 4844 blob is exactly 131,072 bytes
(128 KiB), comfortably below the 256 KiB default chunk size, the 1 MiB raw
block guidance, and the 2 MiB bitswap transfer ceiling. So each blob is one
raw-codec block: CID = hash of the exact blob bytes, no UnixFS wrapper, no
chunking, no envelope overhead. Chunking machinery exists for data larger
than a block; blobs are not. If a future fork ever raises blob size past the
block limit, only the blob leaf representation changes; the index schema
points at CIDs and does not care what they resolve to.

Note that a blob's CID (hash of blob bytes) and its versioned hash (hash of
its KZG commitment) are different values, neither derivable from the other
without the blob. The index therefore stores explicit `versioned_hash -> CID`
mappings, and the daemon keeps a node-local catalog for ingest-time lookups.

Index nodes (heads, directory pages, segments) are DAG-CBOR blocks with real
IPLD links, so recursive pins traverse head -> directory -> segments -> blobs
without any custom logic.

### 2. Multiple heads over shared data

A **head** is a complete, self-contained index of `(slot, versioned_hash) ->
blob` for one filter over the blob stream:

- `all`: every blob in every finalized slot (the identity filter).
- `arbitrum-one`: only versioned hashes referenced by the Arbitrum One
  SequencerInbox on L1. Filtering is at versioned-hash granularity, not slot
  granularity, because one slot routinely carries blobs from several rollups.
- Any other chain: same pattern, different inbox.

Heads share blob blocks by construction (same bytes -> same CID -> same
block). Index nodes are not shared between heads (different content), but
index overhead is ~80 bytes per blob reference vs 128 KiB of blob -- under
0.1%.

Each head is deterministically derivable from public data (the beacon chain,
plus the chain's inbox events), so a head root CID is an auditable claim
anyone can recompute and verify.

Each head carries its own `synced_to`. Heads advance independently; a chain
head that relies on the ALL head's blobs must not advance past it.

### 3. Coverage-based power-of-two segments; seal-and-append; no LSM cascade

The unit of index storage is the **segment**: all blob references for a fixed,
power-of-two-aligned window of slots (`2^seg_bits` slots per segment, chosen
per head). Segment boundaries are pure arithmetic: slot `S` lives in segment
ordinal `S >> seg_bits`.

The head structure is then:

- One **open segment** covering the current window, rewritten as refs arrive
  (it is small, this is cheap).
- When `synced_to` crosses the window boundary, the open segment is **sealed**:
  written once, immutable forever, its CID appended to the directory. A window
  with zero blob references seals to a null directory entry (nothing stored).
- The **directory** is an array of sealed-segment CIDs, paged into fixed-fanout
  pages (`2^fanout_bits` links per page) arranged as an implicit radix tree.
  Locating a segment is arithmetic on the ordinal -- no stored keys, no
  comparisons. Appending rewrites only the rightmost spine, O(depth) small
  pages. The tree grows a level when the root fills (capacity multiplies by
  the fanout each time, so this is rare and cheap).

This deliberately **dissolves the extent/skip-list/LSM design** from the
handwritten notes and the earlier prototype discussion. What happened to it:

- The LSM cascade existed to amortize copy-on-write rewrites of hot index
  structures. Coverage-based sealing makes sealed segments write-once; the
  only hot objects are the open segment and the directory spine. There is
  nothing left to amortize.
- Entry-merging compaction (merge 2^m level-n segments into one level-n+1
  segment; pure concatenation since ranges are disjoint and pre-determined)
  works, but hits the IPFS block-size ceiling after about two levels on the
  ALL head. The levels bought nothing that per-head segment sizing doesn't.
- The skip list was an index over data-dependent segment boundaries. With
  coverage-based boundaries there is nothing to look up: the "index" is a
  shift and a subtraction.

The one surviving knob from that design: **each head picks its own
`seg_bits`** to hit a target sealed-segment size (~1 MiB), because entry
density differs wildly between the ALL head (every slot, many blobs) and a
sparse filtered head. Sizing formula and defaults are in the spec.

Within a slot, blob references preserve the order the blobs appeared in the
beacon block, so unfiltered reads return blobs in canonical index order.

### 4. Retention = pinning; no reference counting

Retention policies are declarative, per head:

- `full`: recursively pin the head root. One pin swap per head update; the
  whole DAG (directory, segments, blobs) is protected.
- `window: <duration>`: recursively pin only the segments whose slot coverage
  intersects the trailing window (plus the open segment and directory pages,
  pinned directly). The daemon slides the window as heads advance.
- `none`: index is maintained and published, nothing is pinned locally
  (useful for an operator who serves the index but not the data).

GC is mark-and-sweep from the union of all pins, so a blob shared by the ALL
head and the arbitrum-one head survives as long as *either* head's policy still
reaches it. That is exactly the multi-head sharing semantics we want.

The collector is online. At a short consistency cut **T0**, under the ordinary
mutation/publication gate, it reconciles pins, expires abandoned staging rows,
snapshots the pin groups, and activates an in-memory epoch. It then releases the
gate for the expensive work. Let **M** be the multihashes reached from the T0
snapshot, and **T** the multihashes touched by application reads or writes after
T0 while the epoch is active. The safety invariant is:

```
retained during sweep = M ∪ T
```

The application sees a protected blockstore view which records T; the collector
uses an untracked view so its own scan does not protect every piece of garbage.
Both sets are keyed by multihash because flatfs is multihash-keyed and may
enumerate a block under a CID codec different from the link that reached it.
For an unmarked candidate, the final T check and the flatfs delete linearize
under the same per-key guard used to record a touch. Thus either the application
touch wins and the block survives, or deletion wins and a later application
operation observes the deletion (and a later put can recreate the block). A
short blockstore-lifecycle barrier ends the epoch after enumeration; it waits
for in-flight application block operations but does not stop a whole
publication transition.

Application enumeration is a special lifecycle operation. `AllKeysChan` cannot
join T atomically key by key and cannot surface a late iterator error, so an
application enumeration begun while idle holds a lifecycle read lease until its
channel drains or its context is cancelled; T0 waits. An attempt begun during an
epoch is refused. GC and scrub instead use the separate untracked,
error-preserving collector enumeration.

HTTP reads which materialize archive blocks have a separate, shorter reader
lease on the publication/GC gate. The blobs endpoint acquires it after response
memory admission and before choosing its physical root (including a virtual
live-head handoff); the manifest endpoint acquires it before choosing the tip.
The lease covers index traversal, follower fetches and verification, and blob
or manifest materialization. It is released immediately before the first
response write; any remaining serialization uses only the already-materialized
in-memory value. Thus a mutable generation or manifest tip may
still be replaced concurrently, but a fresh T0 cannot omit the retired root
until every already-selected request has finished reading it. A slow client is
outside the lease and remains bounded by the response write deadline. A request
which starts after T0 waits only for the short cut, then joins the active epoch
and protects its successful per-key operations in T.

The gate remains load-bearing even though it is no longer held for the full
walk. Root-changing operations must not straddle the initial cut without being
accounted for: ordinary refs application, `Truncate`, reconciliation, and the
follower's adoption or `Resume` root/checkpoint publication all use the same
gate. Blocks
written or found-present after the cut go through the protected application
view before a new root can publish. A mutation already complete at the cut is
in M; a mutation completing later is in T.

Reachability and integrity are deliberately separate jobs. Mark must read and
validate DAG-CBOR targets; recursive pins expand their links into M, while
direct pins validate only that one target. Raw targets are checked for local
existence, so a missing live block fails or heals under the head's role, but
their bytes do not have to be hashed to establish reachability. Hashing every
retained raw block made collector latency and write availability scale with
archive bytes. A separately scheduled validating scrub completely enumerates
flatfs and hashes every object it observes, including unreachable garbage,
without deleting or refetching anything. Concurrent additions which fall after
the iterator's view are checked by a later pass. That
preserves detection of corrupt raw leaves and direct pins without putting the
full-byte audit on the reclamation path; retained-block absence remains the
mark's responsibility.

The earlier prototype's reference-counting layer (and its commit-time
decrement-the-old-root GC) remains deliberately dropped. It cannot be a small
substitution here: blocks live in flatfs while roots, pins, staging state, and
other control facts live in Pebble, with no transaction spanning both stores.
Shared DAGs mean a block may be retained by several heads and policies; staging
expiry and follower adoption add provisional owners; and safely decrementing an
old root requires traversing the DAG anyway. Crash-safe reference counting would
therefore need a durable journal, root-swap protocol, and full count-repair pass.
An under-count loses live content, while an over-count leaks it. Recomputing M
from authoritative pins and conservatively adding T is simpler to prove and
self-repairs on the next run.

The epoch itself changes no flatfs, pin-ledger, root, or CID format; M, T, and
their per-key synchronization exist only for the running epoch. The optional
versioned sealed-Segment verification markers are new derived cache keys in the
existing follower Pebble namespace: their absence is valid and triggers
re-verification, so no migration is required. A process crash stops the
collector and discards its transient sets; the next run starts from a fresh
reconciled cut.

The safety argument divides every possible live block into two cases:

1. **Live at the cut.** Reconciliation and the pin snapshot occur while root
   publication is excluded. The subsequent traversal puts the complete closure
   of those recursive pins, plus every direct pin, in M before sweep starts.
2. **Made live after the cut.** Immutable subtrees reused from the cut's roots
   are already in M. New blocks are Put through the application view, and reused
   blocks which a publication newly depends on are read or presence-checked
   through that view, putting them in T. Forward refs application follows this
   naturally. A window-policy `Truncate` validating-reads every raw reference
   surviving in its rebuilt Segment and every sealed Segment closure which the
   rewind makes newly recursive. Full retention already had those closures in M;
   none does not retain them. A follower validating-reads the local root and
   manifest anchors before checkpoint/root exposure.

Follower presence proofs need one more distinction than ordinary application
access. The blockstore exposes both the currently active epoch and a monotonic
**collection generation**. Begin increments the generation before deletion can
start, and End does not reset it. A follower's subtree memo is stamped with that
generation, not merely with the active epoch ID, so a walk proved before a
completed collection never becomes trusted again just because collection is now
idle. An ordinary retention fetch which crosses a generation boundary neither
stamps its root fetched nor drops the staging pins it accumulated; a later poll
retries wholly in the new generation.

That generation-stamped cache is only a **presence and protection memo**. Full
verification has an independent `verifiedSegments` semantic proof. On the first
successful ordinary full-verification walk, every `RefEntry` in a Segment is
checked, including entries whose blob was already local. The Segment CID commits
to the complete binding list and each blob CID commits to its bytes, so that
proof remains valid across collection generations and refetches of the same
CIDs. Proofs for sealed Segments are stored under versioned Segment-CID keys in
the checksummed follower KV and survive ordinary restarts; losing a recent
best-effort marker only causes re-verification. The mutable open Segment is
memory-only so abandoned intermediate CIDs cannot grow the KV without bound; if
that CID later becomes sealed, its already-complete memory proof is promoted.
Changing the verification rule changes the key version. The protection-only
closure used during active-GC adoption or `Resume` performs no KZG semantic
verification and cannot populate either proof layer.

Publication uses the same generation as a proof token. Adoption and `Resume`
share this protocol. If no epoch is active, they skip the expensive closure
walk: commit holds the publication Gate
and requires the generation still to equal the observed token, so a collector
which cut in between causes refusal and retry before any durable publication. If
an epoch is active, the transition application-reads exactly the retention
policy's complete closure plus the manifest chain, staging anything fetched,
then commit
checks the unchanged generation under Gate and validating-reads the root and tip
before the first write. A cut crossed during the closure invalidates and repeats
the proof. This full walk is therefore paid only by the rare adoption or
concurrent `Resume` which overlaps an active collector, not by ordinary idle
publication.

Follower completion markers belong to a transition, not just to a CID. Exposing
A, then B, clears A's root and manifest fetched markers; a later return to A must
walk again. Otherwise A → B → GC → A could mistake the old A marker for proof
that A-only descendants survived while B was current. Collection generations
scope subtree presence within a transition, while this reset handles root/tip
recurrence across transitions.

The follower transition lock remains held during that rare active-epoch closure
proof so another Poll, Resume, or quarantine transition cannot replace the plan
being proved. Existing reads keep serving the last committed generation and the
collector keeps running, but another follower transition waits for the closure's
local I/O or fetches. This is an explicit liveness tradeoff for atomic admission,
not a return to whole-run GC write exclusion. A generic blockstore which cannot
expose collection generations uses the compatibility proof instead: walk the
retained closure while holding the whole publication/GC Gate. It disables the
shared presence memo because it has no monotonically changing token with which
to invalidate a pre-collection proof. Production uses the generation-aware
epoch store.

Sweep starts only after M is complete. For a candidate outside M, the shared
key guard makes touch-versus-delete a total order. If the successful application
operation comes first, T rejects deletion. If delete comes first, a later read
or existence check sees absence, while a later Put recreates and protects the
object before it can publish. Ending the epoch waits for in-flight block
operations, and after enumeration there is no collector deletion left to race.

The proof intentionally permits false positives. A public read, an abandoned
mutation, or a newly written object can enter T and survive one extra cycle even
if no final root needs it. That is bounded disk over-retention, not data loss;
the next fresh cut re-evaluates it. The HTTP reader lease closes the former
old-root availability sliver without retaining retired generations past their
last in-flight materialization.

This makes retention a reversible policy knob: "archive everything" can later
become "everything for 6 months, Arbitrum forever" by editing config, not by
migrating data.

### 5. Ingestion: indexers emit refs; a fetcher supplies bytes; the core assembles

Three roles, cleanly separated:

1. **Per-head indexers** (stateless processes): walk their source in finalized
   order and emit `(slot, [versioned_hash])` rows.
   - ALL: walk finalized beacon slots; every blob in the slot.
   - Chain heads: walk the parent chain's SequencerInbox batch-delivery
     events (importing nitro's own parsing where possible), take blob tx
     versioned hashes, compute the slot from the L1 block timestamp exactly
     as Nitro does.
2. **Blob fetching**: raw blob bytes are pushed via `put blobs` from a beacon
   node (within retention), another bloar archive (its own read API is the
   backfill protocol), or any other source. The daemon verifies every blob at
   ingest by computing its KZG commitment and derived versioned hash; it is
   impossible to store a blob under a wrong versioned hash.
3. **The core** (`bloard`): applies refs monotonically per head, seals
   segments, maintains directories, publishes roots, manages pins.

Rules that keep this boring:

- `put blobs` is idempotent by nature (content addressing) and carries no
  metadata: the server derives versioned hashes itself during verification.
- `put refs` is strictly monotonic per head, but an exact replay of
  already-applied rows is an idempotent no-op, so a crashed-and-retrying
  indexer cannot wedge the archive.
- Refs may only reference blobs the daemon already has; `put refs` fails
  listing missing hashes otherwise. Order of operations is always
  blobs-then-refs.
- Indexers are stateless: on startup they read the head's `synced_to` from
  the archive and resume. The archive is the single source of progress truth.
- `synced_to` advances through blobless and empty slots, so within covered
  range, "no entry" provably means "no blob", never "archive gap".
- Finalized heads are finalized-only. Reorgs of finalized data imply an Ethereum
  consensus failure; the emergency `truncate` admin operation exists for that
  and for operator error, and is expected never to run. The optional
  `unfinalized-mutable` head is a distinct, bounded complete snapshot ordered by
  signed publication revision, so ordinary beacon reorgs replace it without
  changing the ALL head's contract.

### 6. Go implementation

The earlier CAS prototype was Rust; production bloar is Go:

- boxo lets us embed the blockstore, pinner, and GC in-process. The Rust IPFS
  ecosystem effectively requires driving a kubo daemon over RPC.
- The indexers and conformance tests want nitro's own packages
  (`util/headerreader`, `util/blobs`, kzg4844 bindings, inbox parsing). The
  archive's CI can literally run Nitro's `BlobClient` against `bloard` as a
  conformance test.
- The `Pointer[T]` pattern (cid / loaded / dirty) ports to a small Go generic;
  without refcounting, the CAS core shrinks dramatically. The Rust
  prototype's structural-sharing and laziness tests get ported as the spec
  for the Go core.

### 7. One writer per head; followers replicate via IPFS

Mutation was already single-writer per head (decision 5's monotonic refs);
this makes it a topology: one writer node produces and signs each head, and
every other node is a **follower** -- no indexers, no ingest API, no
mutation code. A follower resolves the signed publication document (HTTPS
poll, or an **IPNS** record: a signed, sequence-numbered pointer published
under the archive's key, resolvable over the DHT with no server in the
loop), verifies the signature, adopts the new root, and runs the *same pin
reconciler the writer uses for retention* -- except over a bitswap-backed
blockstore, where adding a pin fetches the DAG behind it. Pin reconciliation
**is** the replication protocol; there is no second sync mechanism to build
or trust. Per-head pin policy composes for free: a follower with
`window: 720h` on arbitrum-one is a lean regional mirror by config alone.

A second deployment shape is the
[standalone Kubo archive replica](kubo-replica.md). It follows the same signed,
anti-replay publication boundary but retains every selected head in full through
one generation-anchor pin on an existing operator-owned Kubo node. Kubo remains
the block store and libp2p host. This shape contributes a multi-terabyte
Bitswap replica without running ingest or mutation and may optionally expose
the same public GET-only Bloar/beacon API from Kubo-local committed blocks. It
does not replace the embedded writer backend.

The HTTP capability is intentionally narrower than the follower's fetch
capability. Publication following may fetch and then pin missing descendants;
an unauthenticated read receives only Kubo's `offline=true` local view. A miss
does not become Bitswap work and cannot serve ahead of the committed generation
anchor. The HTTP reader and generation retirement share one lease boundary, so
Kubo cannot be asked to unpin the selected generation while a response still
needs it.

The same shape can retain a filtered finalized archive plus the bounded global
live tip. Its generation anchor contains only the selected physical heads; the
global finalized entry which authenticates the live handoff can remain signed
metadata. A selected filtered frontier closes the local boundary, so proof or
gap failure leaves the previous two-head generation pinned instead of advancing
either side alone.

The trust surface this leaves is small and precise. Content addressing
makes the index and blob bytes unforgeable, `verify: full` (or ultimately
Nitro's own KZG check) makes the vh->blob bindings unforgeable, so the
writer's signature vouches only for what no hash can prove:
**completeness** (nothing censored) and **freshness** (`synced_to` honest,
mitigated by followers refusing regressions across both channels). And
because every published root is a CID, heads are **permissionlessly
forkable**: anyone holding a replica can continue building from the last
good root under a new key, structurally reusing every existing block. A
dead or misbehaving writer is routed around, not negotiated with; failover
is "hand a follower the signing key".

An archive is scoped to a single network. Its publication document carries one
`net`, and every head it signs is a filter over that one network's blob stream
(decision 2); the signing key is the archive's identity, and that identity is
per-network. A second network -- sepolia beside mainnet -- is a second archive,
with its own store, signing key, and publication document, never extra heads or
sources added to the first. The core is `net`-agnostic, so running both is the
same binary run twice under two configs, not one daemon spanning two chains.

### 8. Scheduled sources, and a filter as verifiable as the data

Decision 2 made every head a filter over the blob stream, and decision 5 made
each head deterministically derivable from public data. Two facts pull against
that once a real chain's history is long enough:

- **Posting mechanisms change mid-history.** Arbitrum posts batches as blob
  transactions to its SequencerInbox today; it is expected to move to a
  Base-style arrangement -- blob transactions sent to a plain EOA. A head is
  supposed to mean "the blobs this chain posted", and that meaning has to survive
  the chain changing *how* it posts without the head becoming a different head.
- **A filter that isn't stated isn't verifiable.** Decision 5's re-derivation
  claim -- two independent runs over the same finalized data produce the same
  root -- is empty for a filtered head if the filter itself is just however the
  operator happened to configure the indexer. Two honest re-derivers could pick
  slightly different inbox addresses or cut-over blocks and each believe they had
  reproduced the head.

So a chain head's filter is an **ordered schedule of sources**, not a single
rule. Each source names a way blobs entered the chain's history -- an inbox event
stream, or blob transactions to an allowlisted EOA -- over an inclusive L1 block
range; the head's rows are the deduplicated union of what the sources select. A
migration is two sources over adjacent ranges, and the head keeps its name and
its meaning across it. `blob-txs` sources carry a mandatory sender allowlist,
because a blob transaction can be sent to any address by anyone: an allowlist-free
EOA source is a public write handle to the chain's recorded history, so it is
refused rather than trusted.

And the schedule is itself published as a **manifest chain**: a content-addressed,
append-only chain of `{sources, prev}` blocks whose tip CID rides in the signed
publication document beside the head root. This is the load-bearing move. It makes
the filter as immutable and reproducible as the data: the data is a CID you can
recompute, and now the filter is one too, so two honest re-derivers apply the same
schedule and reach the same root. What a CID does NOT establish is that the schedule
names every real posting source -- an omitted source is still a valid, reproducible
schedule -- so a filtered head's completeness is only ever RELATIVE TO its
advertised schedule. An upgrade is a compare-and-swap
on the tip; the server that accepts it compares only CIDs (it cannot see L1), and
the indexer that does see L1 enforces the real rule -- everything at or behind the
head's current position is frozen, only the future may change.

That append-only rule is why schedule changes are **close-and-add, never edit**.
Editing a source that has already covered ground would rewrite the meaning of
history the head has already served to followers and re-derivers; it is precisely
the attack the manifest chain exists to make impossible. Closing the old source at
a block and adding a new one ahead of the position expresses the same intent as an
append, which is always legal. Correcting something already covered therefore
requires moving the position back first -- `truncate` -- which turns
truncate-then-manifest-then-resync into a mechanically enforced recovery order
rather than a convention.

A head with no manifest chain remains valid: it is the single configured filter
it always was, making no verifiable claim about that filter -- the status of every
head built before this decision. A genesis manifest, whose sources describe the
existing filter and whose `prev` is null, bootstraps the claim onto such a head
without rebuilding it.

## Deployment shapes

- **Full archive**: beacon indexer (ALL head) + chain indexers + `full` pins.
  Order of 10-15 GB/day of blob data at mid-2026 blob throughput (target 14
  blobs/slot under the current BPO schedule, subject to further increases);
  index overhead is negligible (<0.1%).
- **Lean chain archive**: only a chain indexer in fetch mode (it fetches just
  the referenced blobs); order of 0.5-1.5 GB/day for Arbitrum One. Same code,
  different config.
- **Follower (mirror)**: follow the signed publication doc (HTTPS/IPNS),
  pin-reconcile over bitswap per local policy, serve the read API. No
  indexers at all; a full or lean mirror is just a `follow:` config block.

## Relationship to the arbloar prototype

The Rust prototype (`arbloar/archiver`, see its `docs/architecture.md`)
de-risked the core data model: content-addressed immutable nodes, the
three-state reference (`Hash | Loaded | Dirty`), lazy loading with
upgrade-in-place caching, copy-on-write mutation, bottom-up commit. All of
that carries over into the Go core as `Pointer[T]` and the head mutation
engine.

Two parts of the prototype are deliberately dropped:

- **Reference counting / commit-time GC**: replaced by pinning and online epoch
  mark-and-sweep (decision 4). The split flatfs/Pebble durability domain and
  shared DAGs make an exact crash-safe count materially more complex than the
  M-union-T invariant.
- **Extents and skip lists** (from the handwritten design notes): replaced by
  coverage-based segments with arithmetic addressing (decision 3).

## Future work

- DHT content-routing announcements (provide records) so followers can
  discover block holders beyond static peering; IPNS-over-pubsub for fast
  publication propagation; gateway exposure from `bloard`.
- SSZ response encoding on the read API if clients ever want it.
- Multi-writer / HA `bloard` (v1 is single-writer per archive; read replicas
  are just mirrors).
- If some future client genuinely needs commitments/proofs, a
  compute-and-cache layer can derive them from blobs; they will not enter the
  storage schema.
